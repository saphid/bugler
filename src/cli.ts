import { readFile } from "node:fs/promises";
import { createApp } from "./app";
import { hasValidTokenChecksum, hashToken, makeAdminKey } from "./auth";
import type { AppendEventInput, Source } from "./domain";
import { runListener } from "./listen";
import { serveMcpStdio } from "./mcp";
import { PollEngine } from "./poll";
import { RelayRuntime } from "./runtime";
import { EnvironmentFileSecretResolver } from "./secrets";
import { createCosmosContainer, CosmosStorageDriver } from "./storage/cosmos";
import { DynamoDbStorageDriver, requireDynamoDbTable } from "./storage/dynamodb";
import type { StorageDriver } from "./storage/driver";
import { FirestoreStorageDriver } from "./storage/firestore";
import { SqliteStorageDriver } from "./storage/sqlite";
import { DynamoDBClient, type DynamoDBClientConfig } from "@aws-sdk/client-dynamodb";
import { newUlid, parseDurationSeconds } from "./util";
import { productEnvironment, productEnvironmentVariable } from "./environment";

interface Arguments {
  positionals: string[];
  flags: Map<string, string | boolean>;
}

function parseArguments(values: string[]): Arguments {
  const positionals: string[] = [];
  const flags = new Map<string, string | boolean>();
  for (let index = 0; index < values.length; index += 1) {
    const value = values[index]!;
    if (!value.startsWith("--")) {
      positionals.push(value);
      continue;
    }
    const equal = value.indexOf("=");
    if (equal > 0) {
      flags.set(value.slice(2, equal), value.slice(equal + 1));
      continue;
    }
    const name = value.slice(2);
    const next = values[index + 1];
    if (next && !next.startsWith("--")) {
      flags.set(name, next);
      index += 1;
    } else flags.set(name, true);
  }
  return { positionals, flags };
}

function stringFlag(args: Arguments, name: string, fallback?: string): string | undefined {
  const value = args.flags.get(name);
  return typeof value === "string" ? value : fallback;
}

function numberFlag(args: Arguments, name: string, fallback: number): number {
  const value = Number(stringFlag(args, name, String(fallback)));
  if (!Number.isFinite(value) || value < 0) throw new Error(`--${name} must be a non-negative number`);
  return value;
}

function databasePath(args: Arguments): string {
  return stringFlag(args, "db", productEnvironment("DB") ?? "bugler.db")!;
}

async function storageDriver(args: Arguments): Promise<StorageDriver> {
  const name = productEnvironment("STORAGE") ?? "sqlite";
  if (name === "sqlite") return new SqliteStorageDriver(databasePath(args));
  if (name === "dynamodb") {
    const tableName = productEnvironment("DYNAMODB_TABLE");
    if (!tableName) throw new Error("BUGLER_DYNAMODB_TABLE is required for DynamoDB storage");
    const endpoint = productEnvironment("DYNAMODB_ENDPOINT");
    const clientConfig: DynamoDBClientConfig = {
      region: process.env.AWS_REGION ?? process.env.AWS_DEFAULT_REGION ?? "us-east-1",
      ...(endpoint ? { endpoint } : {}),
    };
    const setupClient = new DynamoDBClient(clientConfig);
    try {
      await requireDynamoDbTable(setupClient, tableName);
    } finally {
      setupClient.destroy();
    }
    return new DynamoDbStorageDriver({ tableName, clientConfig });
  }
  if (name === "cosmos") {
    const endpoint = productEnvironment("COSMOS_ENDPOINT");
    const key = productEnvironment("COSMOS_KEY");
    if (!endpoint || !key) throw new Error("BUGLER_COSMOS_ENDPOINT and BUGLER_COSMOS_KEY are required for Cosmos storage");
    const { client, container } = await createCosmosContainer({
      clientOptions: { endpoint, key },
      databaseId: productEnvironment("COSMOS_DATABASE") ?? "bugler",
      containerId: productEnvironment("COSMOS_CONTAINER") ?? "relay",
    });
    return new CosmosStorageDriver({ container, client });
  }
  if (name === "firestore") {
    const projectId = productEnvironment("FIRESTORE_PROJECT");
    return new FirestoreStorageDriver({
      ...(projectId ? { projectId } : {}),
      databaseId: productEnvironment("FIRESTORE_DATABASE") ?? "(default)",
      namespace: productEnvironment("FIRESTORE_NAMESPACE") ?? "bugler",
    });
  }
  throw new Error(`unsupported BUGLER_STORAGE ${name}`);
}

function usage(): string {
  return `Bugler core

Usage:
  bugler init [--db bugler.db]
  bugler serve [--db bugler.db] [--host 127.0.0.1] [--port 8787]
  bugler listen --relay URL --sub ID [--wait 30s] [--once] -- COMMAND [ARGS]
  bugler mcp --relay URL --sub ID
  bugler poll --url URL --to INGEST_URL --token-ref env:NAME --source-id ID [options]

Listen options:
  --token-env NAME         Read subscriber token from NAME, default BUGLER_TOKEN
  --wait DURATION          Long-poll wait, default 30s
  --once                   Process one returned batch and exit

Poll options:
  --state FILE             Local restart-safe state database
  --interval SECONDS       Poll interval, default 300
  --type EVENT_TYPE        Local transition type
  --subject SUBJECT        Optional local transition subject
  --json-pointer POINTER   Compare one JSON field
  --fire-on-first          Emit the initial observation
  --once                   Perform one poll and exit
  --deny-private           Reject private address literals
  --config FILE            JSON file containing {source,to,token_ref,state}
`;
}

async function listenCommand(args: Arguments, command: string[]): Promise<void> {
  const relay = stringFlag(args, "relay");
  const subscriptionId = stringFlag(args, "sub");
  if (!relay || !subscriptionId) throw new Error("listen requires --relay and --sub");
  if (args.flags.has("token")) throw new Error("listen tokens are accepted only through an environment variable");
  const tokenEnvironment = stringFlag(args, "token-env", "BUGLER_TOKEN")!;
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(tokenEnvironment)) throw new Error("--token-env must be an environment variable name");
  const token = productEnvironmentVariable(tokenEnvironment);
  if (!token) throw new Error(`listen token environment variable ${tokenEnvironment} is missing`);
  const relayUrl = new URL(relay);
  if (relayUrl.protocol !== "http:" && relayUrl.protocol !== "https:") throw new Error("--relay must use http or https");
  if (relayUrl.search || relayUrl.hash) throw new Error("--relay must not contain a query string or fragment");
  const waitSeconds = parseDurationSeconds(stringFlag(args, "wait", "30s")!);
  await runListener({
    relay: relayUrl.toString(),
    subscriptionId,
    token,
    tokenEnvironment,
    waitSeconds,
    once: args.flags.has("once"),
    command,
  });
}

async function mcpCommand(args: Arguments): Promise<void> {
  const relay = stringFlag(args, "relay");
  const subscriptionId = stringFlag(args, "sub");
  if (!relay || !subscriptionId) throw new Error("mcp requires --relay and --sub");
  if (args.flags.has("token")) throw new Error("mcp tokens are accepted only through BUGLER_TOKEN");
  const token = productEnvironment("TOKEN");
  if (!token) throw new Error("mcp token environment variable BUGLER_TOKEN is missing");
  const relayUrl = new URL(relay);
  if (relayUrl.protocol !== "http:" && relayUrl.protocol !== "https:") throw new Error("--relay must use http or https");
  if (relayUrl.search || relayUrl.hash) throw new Error("--relay must not contain a query string or fragment");
  await serveMcpStdio({ relay: relayUrl.toString(), subscriptionId, token });
}

async function initCommand(args: Arguments): Promise<void> {
  const driver = await storageDriver(args);
  try {
    if ((await driver.listAdminKeys()).length > 0) throw new Error("an admin key already exists; mint another through POST /admin-keys");
    const { key, token } = makeAdminKey();
    await driver.putAdminKey(key);
    process.stdout.write(`Admin key created. Store it now; it will not be shown again.\n${token}\n`);
  } finally {
    driver.close();
  }
}

async function serveCommand(args: Arguments): Promise<void> {
  const driver = await storageDriver(args);
  const bootstrapToken = productEnvironment("BOOTSTRAP_ADMIN_TOKEN");
  if (driver.capabilities.name !== "bun:sqlite" && (await driver.listAdminKeys()).length === 0) {
    if (!bootstrapToken || !hasValidTokenChecksum(bootstrapToken, "adm")) {
      await driver.close();
      throw new Error("storage has no admin key; run bugler init or supply a valid BUGLER_BOOTSTRAP_ADMIN_TOKEN");
    }
    const now = new Date();
    await driver.putAdminKey({
      id: newUlid(now.getTime()),
      keyHash: hashToken(bootstrapToken),
      keyPrefix: bootstrapToken.slice(0, 18),
      createdAt: now.toISOString(),
    });
  }
  const secrets = new EnvironmentFileSecretResolver();
  const health = { retentionPressure: false, secretFailures: new Set<string>() };
  const { app, delivery } = createApp({ driver, secrets, health });
  const runtime = new RelayRuntime(driver, secrets, health, {
    retentionMaxAgeMs: Number(productEnvironment("RETENTION_AGE_DAYS") ?? 30) * 86_400_000,
    retentionMaxCount: Number(productEnvironment("RETENTION_MAX_COUNT") ?? 100_000),
    retentionMaxBytes: Number(productEnvironment("RETENTION_MAX_BYTES") ?? 1024 * 1024 * 1024),
    onEvent: () => delivery.notifyNewEvent(),
  });
  const port = numberFlag(args, "port", Number(process.env.PORT ?? 8787));
  const hostname = stringFlag(args, "host", process.env.HOST ?? "127.0.0.1")!;
  const server = Bun.serve({ port, hostname, fetch: app.fetch });
  runtime.start();
  process.stdout.write(`Bugler listening on http://${hostname}:${server.port} using ${driver.capabilities.name}\n`);
  let stopping = false;
  const stop = () => {
    if (stopping) return;
    stopping = true;
    runtime.stop();
    server.stop(true);
    driver.close();
    process.exit(0);
  };
  process.on("SIGINT", stop);
  process.on("SIGTERM", stop);
}

interface PollFileConfig {
  source: Source;
  to: string;
  token_ref: string;
  state?: string;
}

async function loadPollConfiguration(args: Arguments): Promise<PollFileConfig> {
  const configPath = stringFlag(args, "config");
  if (configPath) return JSON.parse(await readFile(configPath, "utf8")) as PollFileConfig;
  const url = stringFlag(args, "url");
  const to = stringFlag(args, "to");
  const tokenRef = stringFlag(args, "token-ref");
  const sourceId = stringFlag(args, "source-id");
  if (!url || !to || !tokenRef || !sourceId) throw new Error("poll requires --url, --to, --token-ref, and --source-id");
  const interval = numberFlag(args, "interval", 300);
  const pointer = stringFlag(args, "json-pointer");
  const type = stringFlag(args, "type", "com.bugler.poll.changed")!;
  const subject = stringFlag(args, "subject");
  return {
    to,
    token_ref: tokenRef,
    state: stringFlag(args, "state", ".bugler-poll.db")!,
    source: {
      id: sourceId,
      kind: "poll",
      verification: { primitive: "none" },
      mapping: {
        source: `poll/${sourceId}`,
        type: { static: type },
        ...(subject ? { subject: { static: subject } } : {}),
      },
      poll: {
        url,
        interval_s: interval,
        ...(pointer ? { change_detect: { json_pointer: pointer } } : {}),
        ...(args.flags.has("fire-on-first") ? { fire_on_first: true } : {}),
        ...(args.flags.has("deny-private") ? { deny_private_addresses: true } : {}),
      },
      createdAt: new Date().toISOString(),
    },
  };
}

function outboundBody(event: AppendEventInput): Blob {
  if (event.data !== undefined) return new Blob([JSON.stringify(event.data)]);
  if (event.data_base64 !== undefined) return new Blob([Uint8Array.from(Buffer.from(event.data_base64, "base64")).buffer]);
  return new Blob([]);
}

async function pollCommand(args: Arguments): Promise<void> {
  const configuration = await loadPollConfiguration(args);
  const driver = new SqliteStorageDriver(configuration.state ?? ".bugler-poll.db");
  const resolver = new EnvironmentFileSecretResolver();
  const token = await resolver.resolve(configuration.token_ref);
  const engine = new PollEngine(driver, {
    beforeCommit: async (event) => {
      const response = await fetch(configuration.to, {
        method: "POST",
        headers: {
          authorization: `Bearer ${token}`,
          "content-type": event.datacontenttype ?? "application/octet-stream",
          "x-bugler-dedupe-key": event.dedupekey!,
          "x-bugler-generation": event.dedupekey?.split("/").at(-1) ?? "0",
        },
        body: outboundBody(event),
        redirect: "manual",
        signal: AbortSignal.timeout(30_000),
      });
      if (!response.ok) throw new Error(`relay ingest returned ${response.status}`);
    },
  });
  try {
    do {
      const result = await engine.run(configuration.source);
      process.stdout.write(`${new Date().toISOString()} ${result.status} generation=${result.generation}${result.error ? ` error=${result.error}` : ""}\n`);
      if (args.flags.has("once")) break;
      const state = await driver.getPollState(configuration.source.id);
      const waitMs = Math.max(100, (state?.nextRunAt ? Date.parse(state.nextRunAt) : Date.now() + configuration.source.poll!.interval_s * 1000) - Date.now());
      await Bun.sleep(waitMs);
    } while (true);
  } finally {
    driver.close();
  }
}

export async function main(argv = process.argv.slice(2)): Promise<void> {
  const [command, ...rest] = argv;
  if (!command || command === "help" || command === "--help" || command === "-h") {
    process.stdout.write(usage());
    return;
  }
  if (command === "listen") {
    const separator = rest.indexOf("--");
    if (separator < 0) throw new Error("listen requires -- before the command");
    return listenCommand(parseArguments(rest.slice(0, separator)), rest.slice(separator + 1));
  }
  const args = parseArguments(rest);
  if (command === "init") return initCommand(args);
  if (command === "serve") return serveCommand(args);
  if (command === "mcp") return mcpCommand(args);
  if (command === "poll") return pollCommand(args);
  throw new Error(`unknown command: ${command}\n\n${usage()}`);
}
