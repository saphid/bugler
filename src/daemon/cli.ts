import { chmod, mkdir, open, readFile, rm, stat, writeFile } from "node:fs/promises";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import {
  daemonAccountConfig,
  loadDaemonAccount,
  persistDaemonAccount,
  readSecretFile,
  subscriberTokenPath,
  writeSecretFile,
  type DaemonPathOverrides,
} from "./account";
import { startLocalDaemonServer } from "./server";
import { SqliteLocalStore } from "./sqlite-store";
import { HostedMirrorClient, OssLongPollMirrorClient } from "./upstream";
import { LocalActionManager, LocalActionRunner } from "./actions";
import { recoverDaemonFromHosted, restoreDaemonBackup } from "./recovery";
import { startLocalRetention } from "./retention";

const DAEMON_VERSION = "0.1.0";

export function daemonServiceArguments(argv = process.argv, executable = process.execPath): string[] {
  const sourceEntry = argv[1]?.endsWith(".ts") ? argv[1] : undefined;
  return [executable, ...(sourceEntry ? [sourceEntry] : [])];
}

function xml(value: string): string {
  return value.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
}

export function daemonServiceDefinition(
  platform: "darwin" | "linux",
  accountName: string,
  accountKey: string,
  serviceCommand: string[],
): string {
  return platform === "darwin"
    ? `<?xml version="1.0" encoding="UTF-8"?>\n<plist version="1.0"><dict><key>Label</key><string>dev.bugler.daemon.${accountKey}</string><key>ProgramArguments</key><array>${serviceCommand.map((item) => `<string>${xml(item)}</string>`).join("")}</array><key>RunAtLoad</key><true/><key>KeepAlive</key><dict><key>SuccessfulExit</key><false/></dict><key>ThrottleInterval</key><integer>10</integer></dict></plist>\n`
    : `[Unit]\nDescription=Bugler daemon ${accountName}\n[Service]\nExecStart=${serviceCommand.map((item) => JSON.stringify(item)).join(" ")}\nRestart=on-failure\nRestartSec=10s\n[Install]\nWantedBy=default.target\n`;
}

interface Parsed {
  positionals: string[];
  flags: Map<string, string | boolean>;
}

function parse(values: string[]): Parsed {
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
    const next = values[index + 1];
    if (next && !next.startsWith("--")) {
      flags.set(value.slice(2), next);
      index += 1;
    } else flags.set(value.slice(2), true);
  }
  return { positionals, flags };
}

function flag(args: Parsed, name: string): string | undefined {
  const value = args.flags.get(name);
  return typeof value === "string" ? value : undefined;
}

function required(args: Parsed, name: string): string {
  const value = flag(args, name);
  if (!value) throw new Error(`--${name} is required`);
  return value;
}

function overrides(args: Parsed): DaemonPathOverrides {
  return {
    ...(flag(args, "config-root") ? { configRoot: flag(args, "config-root")! } : {}),
    ...(flag(args, "state-root") ? { stateRoot: flag(args, "state-root")! } : {}),
    ...(flag(args, "runtime-root") ? { runtimeRoot: flag(args, "runtime-root")! } : {}),
  };
}

async function account(args: Parsed) {
  return loadDaemonAccount(required(args, "account"), overrides(args));
}

async function request(config: Awaited<ReturnType<typeof account>>, path: string, init: RequestInit, token: string): Promise<Response> {
  const headers = new Headers(init.headers);
  headers.set("authorization", `Bearer ${token}`);
  headers.set("x-bugler-local-client", "cli");
  if (init.body) headers.set("content-type", "application/json");
  const status: { port?: number } = await readFile(config.statusPath, "utf8").then((value) => JSON.parse(value) as { port?: number }).catch(() => ({}));
  if (status.port) {
    headers.set("origin", `http://127.0.0.1:${status.port}`);
    return fetch(`http://127.0.0.1:${status.port}/v1${path}`, { ...init, headers });
  }
  return fetch(`http://localhost/v1${path}`, { ...init, headers, unix: config.socketPath });
}

async function managementToken(config: Awaited<ReturnType<typeof account>>): Promise<string> {
  return (await readFile(config.managementTokenFile, "utf8")).trim();
}

async function acquireDaemonLock(path: string, accountName: string) {
  const create = async () => {
    const handle = await open(path, "wx", 0o600);
    await handle.writeFile(`${process.pid}\n`);
    await handle.sync();
    return handle;
  };
  try {
    return await create();
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
    const pid = Number((await readFile(path, "utf8").catch(() => "")).trim());
    let running = Number.isSafeInteger(pid) && pid > 0;
    if (running) {
      try { process.kill(pid, 0); } catch (probe) { if ((probe as NodeJS.ErrnoException).code === "ESRCH") running = false; }
    }
    if (running) {
      const command = process.platform === "linux"
        ? await readFile(`/proc/${pid}/cmdline`, "utf8").catch(() => "")
        : Bun.spawnSync(["ps", "-p", String(pid), "-o", "command="]).stdout.toString();
      const normalized = command.replaceAll("\0", " ");
      const accountFlag = new RegExp(`(?:^|\\s)--account(?:=|\\s+)${accountName}(?:\\s|$)`);
      running = normalized.includes("daemon") && normalized.includes("start") && accountFlag.test(normalized);
    }
    if (running) throw new Error(`daemon account ${accountName} is already running`);
    await rm(path, { force: true });
    return create();
  }
}

export function daemonUsage(): string {
  return `Bugler local daemon

Usage:
  bugler daemon init --account NAME --origin URL --tenant ID --principal ID [--credential-env NAME]
  bugler daemon start --account NAME
  bugler daemon status --account NAME [--json]
  bugler daemon install --account NAME
  bugler daemon uninstall --account NAME
  bugler daemon credential <rotate|revoke> --account NAME --id ID --platform URL
  bugler daemon recover --account NAME <--from-hosted|--from-backup PATH --yes>
  bugler local mirror add --account NAME --sub ID [--token-env NAME]
  bugler local consumer create --account NAME --id ID --mirror ID
  bugler local consumer rotate --account NAME --id ID [--overlap 86400]
  bugler local rewind --account NAME --consumer ID --seq N
  bugler local events --account NAME --consumer ID [--wait 45s]
  bugler local actions install FILE --account NAME
  bugler local actions validate --account NAME
  bugler local dead-letter <list|replay> --account NAME [--id ID]
`;
}

export async function daemonCli(argv: string[], output: NodeJS.WritableStream = process.stdout, errorOutput: NodeJS.WritableStream = process.stderr): Promise<void> {
  const [command, ...rest] = argv;
  const args = parse(rest);
  if (!command || command === "help" || command === "--help") {
    output.write(daemonUsage());
    return;
  }
  if (command === "init") {
    const config = daemonAccountConfig({
      account: required(args, "account"),
      origin: required(args, "origin"),
      tenantId: required(args, "tenant"),
      principalId: required(args, "principal"),
    }, overrides(args));
    const credentialEnvironment = flag(args, "credential-env");
    const credential = credentialEnvironment ? process.env[credentialEnvironment] : undefined;
    if (credentialEnvironment && !credential) throw new Error(`${credentialEnvironment} is missing or empty`);
    await persistDaemonAccount(config);
    const store = new SqliteLocalStore(config.databasePath, { accountKey: config.accountKey });
    const token = await store.mintManagementToken();
    store.close();
    await writeSecretFile(config.managementTokenFile, token);
    if (credentialEnvironment && credential) {
      await writeSecretFile(config.hostedCredentialFile, credential);
      delete process.env[credentialEnvironment];
    }
    output.write(`Daemon account ${config.account} initialized (${config.accountKey}).\nManagement token, shown once:\n${token}\n`);
    return;
  }
  const config = await account(args);
  if (command === "status") {
    const status: Record<string, unknown> = await readFile(config.statusPath, "utf8")
      .then((value) => JSON.parse(value) as Record<string, unknown>)
      .catch(async (error: unknown) => {
        if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
        const listening = await stat(config.socketPath).then((entry) => entry.isSocket()).catch(() => false);
        return listening
          ? { account: config.account, account_key: config.accountKey, socket: config.socketPath, version: DAEMON_VERSION, link_state: "starting" }
          : { account: config.account, account_key: config.accountKey, version: DAEMON_VERSION, link_state: "stopped" };
      });
    const live = await managementToken(config)
      .then((token) => request(config, "/status", { method: "GET", signal: AbortSignal.timeout(1_000) }, token))
      .then((response) => response.ok)
      .catch(() => false);
    const observed: Record<string, unknown> = live
      ? { ...status, live: true }
      : status.link_state === "stopped"
        ? { ...status, live: false }
        : { ...status, link_state: "stale", link_detail: "daemon endpoint is not responding", live: false };
    output.write(args.flags.has("json") ? `${JSON.stringify(observed)}\n` : `Bugler daemon ${config.account}: ${String(observed["link_state"])}\n`);
    return;
  }
  if (command === "recover") {
    const backup = flag(args, "from-backup");
    if (backup) {
      if (!args.flags.has("yes")) throw new Error("backup recovery requires --yes after preserving the suspect database");
      const restored = await restoreDaemonBackup({ config, backupPath: backup });
      output.write(`Verified backup restored.${restored.suspectPath ? ` Suspect database preserved at ${restored.suspectPath}.` : ""}\n`);
      return;
    }
    if (!args.flags.has("from-hosted")) throw new Error("daemon recover requires --from-backup PATH or --from-hosted");
    const recovered = await recoverDaemonFromHosted({ config });
    await writeSecretFile(config.managementTokenFile, recovered.managementToken);
    output.write(`${recovered.lossDisclosure}\nRecovered ${recovered.subscriptions} hosted subscriptions.\nManagement token, shown once:\n${recovered.managementToken}\n`);
    return;
  }
  if (command === "install" || command === "uninstall") {
    if (process.platform !== "darwin" && process.platform !== "linux") throw new Error("daemon service installation supports macOS and Linux only");
    const defaultDestination = process.platform === "darwin"
      ? join(homedir(), "Library/LaunchAgents", `dev.bugler.daemon.${config.accountKey}.plist`)
      : join(homedir(), ".config/systemd/user", `bugler-daemon-${config.accountKey}.service`);
    const destination = flag(args, "output") ?? defaultDestination;
    if (command === "uninstall") {
      if (args.flags.has("dry-run")) {
        output.write(`${process.platform === "darwin" ? `launchctl bootout gui/${process.getuid?.() ?? 0} ${destination}` : `systemctl --user disable --now ${destination.split("/").at(-1)}`}` + `\nremove ${destination}\n`);
        return;
      }
      if (!flag(args, "output")) {
        if (process.platform === "darwin") {
          const domain = `gui/${process.getuid?.() ?? 0}`;
          const loaded = Bun.spawn(["launchctl", "print", `${domain}/dev.bugler.daemon.${config.accountKey}`], { stdout: "ignore", stderr: "ignore" });
          if (await loaded.exited === 0) {
            const child = Bun.spawn(["launchctl", "bootout", domain, destination], { stdout: "inherit", stderr: "inherit" });
            if (await child.exited !== 0) throw new Error("daemon service disable failed");
          }
        } else {
          const child = Bun.spawn(["systemctl", "--user", "disable", "--now", destination.split("/").at(-1)!], { stdout: "inherit", stderr: "inherit" });
          if (await child.exited !== 0) throw new Error("daemon service disable failed");
        }
      }
      await rm(destination, { force: true });
      output.write(`${destination}\n`);
      return;
    }
    await mkdir(dirname(destination), { recursive: true, mode: 0o700 });
    const launcher = daemonServiceArguments();
    const serviceCommand = [...launcher, "daemon", "start", "--account", config.account];
    const content = daemonServiceDefinition(process.platform === "darwin" ? "darwin" : "linux", config.account, config.accountKey, serviceCommand);
    if (args.flags.has("dry-run")) {
      output.write(`write ${destination}\n${process.platform === "darwin" ? `launchctl bootstrap gui/${process.getuid?.() ?? 0} ${destination}` : `systemctl --user daemon-reload\nsystemctl --user enable --now ${destination.split("/").at(-1)}`}\n`);
      return;
    }
    await writeFile(destination, content, { mode: 0o600 });
    if (!flag(args, "output") && !args.flags.has("dry-run")) {
      const child = process.platform === "darwin"
        ? Bun.spawn(["launchctl", "bootstrap", `gui/${process.getuid?.() ?? 0}`, destination], { stdout: "inherit", stderr: "inherit" })
        : Bun.spawn(["systemctl", "--user", "daemon-reload"], { stdout: "inherit", stderr: "inherit" });
      if (await child.exited !== 0) throw new Error("daemon service installation failed");
      if (process.platform === "linux") {
        const enabled = Bun.spawn(["systemctl", "--user", "enable", "--now", destination.split("/").at(-1)!], { stdout: "inherit", stderr: "inherit" });
        if (await enabled.exited !== 0) throw new Error("daemon service enable failed");
      }
    }
    output.write(`${destination}\n`);
    return;
  }
  if (command === "credential") {
    const operation = args.positionals[0];
    if (operation !== "rotate" && operation !== "revoke") throw new Error("credential requires rotate or revoke");
    const platformToken = process.env.BUGLER_PLATFORM_TOKEN;
    if (!platformToken) throw new Error("BUGLER_PLATFORM_TOKEN is required for credential lifecycle commands");
    const endpoint = `${required(args, "platform").replace(/\/$/, "")}/api/daemon-credentials/${encodeURIComponent(required(args, "id"))}${operation === "rotate" ? "/rotate" : ""}`;
    const response = await fetch(endpoint, { method: operation === "rotate" ? "POST" : "DELETE", redirect: "manual", headers: { authorization: `Bearer ${platformToken}` } });
    if (!response.ok) throw new Error(`daemon credential ${operation} failed with ${response.status}`);
    if (operation === "rotate") {
      const body = await response.json<{ token: string }>();
      await writeSecretFile(config.hostedCredentialFile, body.token);
    } else await rm(config.hostedCredentialFile, { force: true });
    output.write(`${operation}d\n`);
    return;
  }
  if (command !== "start" && command !== "run") throw new Error(`unknown daemon command: ${command}`);
  await mkdir(config.runtimeDirectory, { recursive: true, mode: 0o700 });
  const lock = await acquireDaemonLock(config.lockPath, config.account);
  await rm(config.socketPath, { force: true });
  const store = new SqliteLocalStore(config.databasePath, { accountKey: config.accountKey });
  const actionManager = new LocalActionManager({ actionsDirectory: config.actionsDirectory, stateDirectory: config.stateDirectory, store });
  await actionManager.reconcileFiles();
  const actionRunner = new LocalActionRunner({ store, stateDirectory: config.stateDirectory });
  const actionAbort = new AbortController();
  const localhost = args.flags.has("localhost");
  const portValue = flag(args, "port");
  const port = portValue === undefined ? 0 : Number(portValue);
  if (localhost && (!Number.isSafeInteger(port) || port < 0 || port > 65_535)) throw new Error("--port must be between 0 and 65535");
  const allowedOrigin = flag(args, "allowed-origin");
  if (localhost && !allowedOrigin) throw new Error("--allowed-origin is required with --localhost");
  const local = startLocalDaemonServer({
    store,
    actionManager,
    actionRunner,
    ...(localhost ? { port, hostname: "127.0.0.1", allowedOrigin: allowedOrigin! } : { unix: config.socketPath }),
  });
  const status: { account: string; account_key: string; pid: number; version: string; link_state: string; link_detail?: string; socket?: string; port?: number } = {
    account: config.account, account_key: config.accountKey, pid: process.pid, version: DAEMON_VERSION, link_state: "starting",
    ...(localhost ? { port: local.server.port! } : { socket: config.socketPath }),
  };
  let statusWrite = Promise.resolve();
  const updateLinkState = (state: string, detail?: string) => {
    status.link_state = state;
    if (detail) status.link_detail = detail;
    else delete status.link_detail;
    statusWrite = statusWrite.then(() => writeFile(config.statusPath, `${JSON.stringify(status)}\n`, { mode: 0o600 })).then(() => undefined);
    errorOutput.write(`Bugler daemon ${config.account}: ${state}${detail ? ` (${detail})` : ""}\n`);
  };
  const hasHostedCredential = await stat(config.hostedCredentialFile).then((entry) => entry.isFile()).catch(() => false);
  const ossUpstream = new OssLongPollMirrorClient({
    origin: hasHostedCredential
      ? `${config.origin.replace(/\/$/, "")}/t/${encodeURIComponent(config.tenantId)}`
      : config.origin,
    store,
    notifyNewEvent: local.notifyNewEvent,
    tokenFor: async (subscriptionId) => readSecretFile(subscriberTokenPath(config, subscriptionId)).catch(() => undefined),
    onState: updateLinkState,
  });
  const upstream = hasHostedCredential ? new HostedMirrorClient({
    origin: config.origin,
    tenantId: config.tenantId,
    credentialFile: config.hostedCredentialFile,
    store,
    notifyNewEvent: local.notifyNewEvent,
    fallback: ossUpstream,
    onState: updateLinkState,
  }) : ossUpstream;
  const actionTimer = setInterval(() => { void actionRunner.runDue(4, actionAbort.signal); }, 250);
  const backupTimer = setInterval(() => {
    void store.createVerifiedBackup(`${config.databasePath}.backup-1`, `${config.databasePath}.backup-2`)
      .catch((error: unknown) => errorOutput.write(`Bugler daemon ${config.account}: backup_failed (${error instanceof Error ? error.name : "Error"})\n`));
  }, 24 * 60 * 60_000);
  backupTimer.unref();
  const retention = startLocalRetention(store, {
    onError: (error: unknown) => errorOutput.write(`Bugler daemon ${config.account}: retention_failed (${error instanceof Error ? error.name : "Error"})\n`),
  });
  if (!localhost) await chmod(config.socketPath, 0o600);
  await writeFile(config.statusPath, `${JSON.stringify(status)}\n`, { mode: 0o600 });
  const upstreamRun = upstream.run();
  output.write(`Bugler daemon ${config.account} listening on ${localhost ? local.url : config.socketPath}\n`);
  await new Promise<void>((resolve) => {
    let stopped = false;
    const stop = async () => {
      if (stopped) return;
      stopped = true;
      local.server.stop(true);
      clearInterval(actionTimer);
      clearInterval(backupTimer);
      retention.stop();
      actionAbort.abort();
      upstream.stop();
      store.close();
      await lock.close();
      await rm(config.lockPath, { force: true });
      await rm(config.socketPath, { force: true });
      await statusWrite;
      await writeFile(config.statusPath, `${JSON.stringify({ account: config.account, account_key: config.accountKey, pid: process.pid, version: DAEMON_VERSION, link_state: "stopped" })}\n`, { mode: 0o600 });
      await upstreamRun.catch(() => undefined);
      resolve();
    };
    process.once("SIGINT", stop);
    process.once("SIGTERM", stop);
  });
  errorOutput.write("");
}

export async function localCli(argv: string[], output: NodeJS.WritableStream = process.stdout): Promise<void> {
  const resource = argv[0];
  const consumesDirectly = resource === "events" || resource === "ack" || resource === "rewind";
  const operation = consumesDirectly ? undefined : argv[1];
  const rest = argv.slice(consumesDirectly ? 1 : 2);
  const args = parse(rest);
  const config = await account(args);
  const management = await managementToken(config);
  if (resource === "mirror" && operation === "add") {
    const subscriptionId = required(args, "sub");
    const tokenEnvironment = flag(args, "token-env");
    if (!tokenEnvironment) throw new Error(`--token-env is required so subscriber-token fallback can authenticate ${subscriptionId}`);
    const subscriberToken = process.env[tokenEnvironment];
    if (!subscriberToken) throw new Error(`${tokenEnvironment} is missing or empty`);
    const response = await request(config, "/mirrors", { method: "POST", body: JSON.stringify({ id: subscriptionId }) }, management);
    if (!response.ok) throw new Error(await response.text());
    await mkdir(config.subscriberTokenDirectory, { recursive: true, mode: 0o700 });
    await writeSecretFile(subscriberTokenPath(config, subscriptionId), subscriberToken);
    delete process.env[tokenEnvironment];
    output.write(`${JSON.stringify(await response.json())}\n`);
    return;
  }
  if (resource === "mirror" && operation === "list") {
    const response = await request(config, "/mirrors", {}, management);
    output.write(`${JSON.stringify(await response.json())}\n`);
    return;
  }
  if (resource === "actions" && operation === "install") {
    const path = args.positionals[0];
    if (!path) throw new Error("actions install requires a definition file");
    const definition = JSON.parse(await readFile(path, "utf8")) as unknown;
    const response = await request(config, "/actions", { method: "POST", body: JSON.stringify(definition) }, management);
    if (!response.ok) throw new Error(await response.text());
    output.write(`${JSON.stringify(await response.json())}\n`);
    return;
  }
  if (resource === "actions" && operation === "list") {
    const response = await request(config, "/actions", {}, management);
    output.write(`${JSON.stringify(await response.json())}\n`);
    return;
  }
  if (resource === "actions" && operation === "reload") {
    const response = await request(config, "/actions/reload", { method: "POST", body: "{}" }, management);
    if (!response.ok) throw new Error(await response.text());
    output.write(`${JSON.stringify(await response.json())}\n`);
    return;
  }
  if (resource === "actions" && operation === "validate") {
    const response = await request(config, "/actions/reload", { method: "POST", body: "{}" }, management);
    if (!response.ok) throw new Error(await response.text());
    const result = await response.json<{ failed: number }>();
    if (result.failed > 0) throw new Error(`${result.failed} action definitions failed validation`);
    output.write(`${JSON.stringify(result)}\n`);
    return;
  }
  if (resource === "dead-letter" && operation === "list") {
    const query = new URLSearchParams();
    if (flag(args, "since")) query.set("since", flag(args, "since")!);
    if (flag(args, "limit")) query.set("limit", flag(args, "limit")!);
    const response = await request(config, `/dead-letter${query.size ? `?${query}` : ""}`, {}, management);
    if (!response.ok) throw new Error(await response.text());
    output.write(`${JSON.stringify(await response.json())}\n`);
    return;
  }
  if (resource === "dead-letter" && operation === "replay") {
    const response = await request(config, `/dead-letter/${required(args, "id")}/replay`, { method: "POST", body: "{}" }, management);
    if (!response.ok) throw new Error(await response.text());
    output.write(`${JSON.stringify(await response.json())}\n`);
    return;
  }
  if (resource === "consumer" && operation === "create") {
    const response = await request(config, "/consumers", { method: "POST", body: JSON.stringify({ id: required(args, "id"), mirrors: [required(args, "mirror")], start: flag(args, "start") ?? "latest" }) }, management);
    if (!response.ok) throw new Error(await response.text());
    output.write(`${JSON.stringify(await response.json())}\n`);
    return;
  }
  if (resource === "consumer" && operation === "rotate") {
    const response = await request(config, `/consumers/${required(args, "id")}/token/rotate`, {
      method: "POST", body: JSON.stringify({ overlap_s: Number(flag(args, "overlap") ?? 86_400) }),
    }, management);
    if (!response.ok) throw new Error(await response.text());
    output.write(`${JSON.stringify(await response.json())}\n`);
    return;
  }
  if (resource === "rewind") {
    const response = await request(config, `/consumers/${required(args, "consumer")}/cursor`, {
      method: "POST", body: JSON.stringify({ seq: Number(required(args, "seq")) }),
    }, management);
    if (!response.ok) throw new Error(await response.text());
    output.write(`${JSON.stringify(await response.json())}\n`);
    return;
  }
  const consumer = required(args, "consumer");
  const consumerToken = process.env.BUGLER_LOCAL_TOKEN;
  if (!consumerToken) throw new Error("BUGLER_LOCAL_TOKEN is required for consume commands");
  if (resource === "events") {
    const response = await request(config, `/consumers/${consumer}/events`, { method: "POST", body: JSON.stringify({ wait: flag(args, "wait") ?? "45s" }) }, consumerToken);
    if (!response.ok) throw new Error(await response.text());
    output.write(`${JSON.stringify(await response.json())}\n`);
    return;
  }
  if (resource === "ack") {
    const response = await request(config, `/consumers/${consumer}/cursor`, { method: "POST", body: JSON.stringify({ seq: Number(required(args, "seq")), epoch: Number(required(args, "epoch")) }) }, consumerToken);
    if (!response.ok) throw new Error(await response.text());
    output.write(`${JSON.stringify(await response.json())}\n`);
    return;
  }
  throw new Error(`unknown local command: ${resource ?? ""} ${operation ?? ""}`);
}
