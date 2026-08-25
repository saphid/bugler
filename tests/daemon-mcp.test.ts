import { afterEach, describe, expect, it } from "bun:test";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createBuglerMcpServer } from "../src/mcp";
import { MemoryLocalStore } from "../src/daemon/memory-store";
import { createLocalConsumerApp } from "../src/daemon/server";
import type { StoredEvent } from "../src/domain";
import { daemonAccountConfig, persistDaemonAccount } from "../src/daemon/account";
import { SqliteLocalStore } from "../src/daemon/sqlite-store";
import { startLocalDaemonServer } from "../src/daemon/server";

const scratch: string[] = [];
afterEach(async () => { for (const path of scratch.splice(0)) await rm(path, { recursive: true, force: true }); });

async function fixture() {
  const store = new MemoryLocalStore();
  await store.createMirror("hosted");
  const consumer = await store.createConsumer({ id: "codex", mirrors: ["hosted"] });
  const local = createLocalConsumerApp(store);
  const fetcher = async (input: string | URL | Request, init?: RequestInit) => local.app.request(new URL(String(input)).pathname, init);
  const server = createBuglerMcpServer({
    eventsEndpoint: "http://localhost/v1/consumers/codex/events", subscriptionId: "codex", token: consumer.token, fetch: fetcher,
  });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  const client = new Client({ name: "daemon-mcp-conformance", version: "1.0.0" });
  await Promise.all([server.connect(serverTransport), client.connect(clientTransport)]);
  return { store, client, server, clientTransport };
}

function event(seq = 1): StoredEvent {
  return {
    specversion: "1.0", id: `event-${seq}`, source: "source/a", sourceId: "source/a", type: "dev.bugler.test", seq,
    receivedat: new Date(1_700_000_000_000 + seq).toISOString(), verificationmethod: "none", payloadverified: false, rawSize: 1,
    data: { untrusted: true },
  };
}

describe("daemon MCP conformance", () => {
  it("negotiates, discovers exactly two tools, drains, and acknowledges local coordinates", async () => {
    const test = await fixture();
    const tools = await test.client.listTools();
    expect(tools.tools.map((tool) => tool.name).sort()).toEqual(["check_events", "wait_for_event"]);
    expect(tools.tools.every((tool) => tool.description?.includes("untrusted"))).toBe(true);
    await test.store.commitHostedFrame({ kind: "events", subscriptionId: "hosted", cursorEpoch: 0, events: [event()] });
    const drained = await test.client.callTool({ name: "check_events", arguments: {} });
    expect(drained.structuredContent).toMatchObject({ events: [expect.objectContaining({ id: "event-1", seq: 1 })], cursor_epoch: 0 });
    const acknowledged = await test.client.callTool({ name: "check_events", arguments: { ack: 1, ack_epoch: 0 } });
    expect(acknowledged.structuredContent).toMatchObject({ events: [], cursor_epoch: 0 });
    await test.clientTransport.close();
    await test.server.close();
  });

  it("bounds waits, rejects malformed tool arguments, supports overlapping ids, and cancels cleanly", async () => {
    const test = await fixture();
    const [first, second] = await Promise.all([
      test.client.callTool({ name: "check_events", arguments: {} }),
      test.client.callTool({ name: "check_events", arguments: {} }),
    ]);
    expect(first.structuredContent).toMatchObject({ events: [] });
    expect(second.structuredContent).toMatchObject({ events: [] });
    const invalid = await test.client.callTool({ name: "wait_for_event", arguments: { timeout_s: "invalid" as unknown as number } });
    expect(invalid.isError).toBe(true);
    const controller = new AbortController();
    const waited = test.client.callTool({ name: "wait_for_event", arguments: { timeout_s: 50 } }, undefined, { signal: controller.signal });
    controller.abort();
    await expect(waited).rejects.toThrow();
    await test.clientTransport.close();
    await test.server.close();
  });

  it("passes the native stdio process lifecycle with pure protocol stdout and restart", async () => {
    const root = await mkdtemp(join(tmpdir(), "bugler-mcp-stdio-"));
    scratch.push(root);
    const paths = { configRoot: join(root, "config"), stateRoot: join(root, "state"), runtimeRoot: join(root, "runtime") };
    const config = daemonAccountConfig({ account: "stdio", origin: "https://relay.example", tenantId: "tenant", principalId: "principal" }, paths);
    await persistDaemonAccount(config);
    const store = new SqliteLocalStore(config.databasePath, { accountKey: config.accountKey });
    await store.createMirror("hosted");
    const consumer = await store.createConsumer({ id: "codex", mirrors: ["hosted"] });
    const local = startLocalDaemonServer({ store, unix: config.socketPath });
    const connect = async () => {
      const transport = new StdioClientTransport({
        command: process.execPath,
        args: ["run", join(import.meta.dir, "../src/bugler.ts"), "mcp", "--account", "stdio", "--consumer", "codex", "--config-root", paths.configRoot, "--state-root", paths.stateRoot, "--runtime-root", paths.runtimeRoot],
        env: { ...process.env, BUGLER_LOCAL_TOKEN: consumer.token } as Record<string, string>,
        stderr: "pipe",
      });
      const client = new Client({ name: "bugler-native-stdio-test", version: "1.0.0" });
      await client.connect(transport);
      return { client, transport };
    };
    const first = await connect();
    expect((await first.client.listTools()).tools.map((tool) => tool.name).sort()).toEqual(["check_events", "wait_for_event"]);
    await first.transport.close();
    await store.commitHostedFrame({ kind: "events", subscriptionId: "hosted", cursorEpoch: 0, events: [event()] });
    const restarted = await connect();
    expect((await restarted.client.callTool({ name: "check_events", arguments: {} })).structuredContent).toMatchObject({ events: [expect.objectContaining({ seq: 1 })] });
    await restarted.transport.close();
    local.server.stop(true);
    store.close();
  });
});
