import { mkdtemp, rm, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "bun:test";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { createApp } from "../src/app";
import { makeAdminKey, mintToken } from "../src/auth";
import type { AppendEventInput, Subscription } from "../src/domain";
import { runListener } from "../src/listen";
import { EnvironmentFileSecretResolver } from "../src/secrets";
import { MemoryStorageDriver } from "../src/storage/memory";

const scratch: string[] = [];
const servers: Array<ReturnType<typeof Bun.serve>> = [];

afterEach(async () => {
  for (const server of servers.splice(0)) server.stop(true);
  for (const path of scratch.splice(0)) await rm(path, { recursive: true, force: true });
});

async function relayFixture() {
  const driver = new MemoryStorageDriver();
  const admin = makeAdminKey(new Date("2026-08-23T00:00:00.000Z"));
  await driver.putAdminKey(admin.key);
  const minted = mintToken("sub");
  const subscription: Subscription = {
    id: "wake-sub",
    filter: { exact: { source: "test/wake" } },
    transport: { type: "pull" },
    tokenHash: minted.hash,
    tokenPrefix: minted.prefix,
    cursor: 0,
    cursorEpoch: 0,
    push: { failureCount: 0, parked: 0 },
    createdAt: "2026-08-23T00:00:00.000Z",
    lastSeenAt: "2026-08-23T00:00:00.000Z",
  };
  await driver.putSubscription(subscription);
  const { app, delivery } = createApp({ driver, secrets: new EnvironmentFileSecretResolver() });
  const server = Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: app.fetch });
  servers.push(server);
  return { driver, delivery, admin, token: minted.token, relay: `http://127.0.0.1:${server.port}`, subscription };
}

function inheritedEnvironment(extra: Record<string, string>): Record<string, string> {
  return { ...Object.fromEntries(Object.entries(process.env).filter((entry): entry is [string, string] => entry[1] !== undefined)), ...extra };
}

async function connectMcp(relay: string, subscriptionId: string, token: string) {
  const transport = new StdioClientTransport({
    command: process.execPath,
    args: ["run", "src/bugler.ts", "mcp", "--relay", relay, "--sub", subscriptionId],
    cwd: process.cwd(),
    env: inheritedEnvironment({ BUGLER_TOKEN: token }),
    stderr: "pipe",
  });
  const client = new Client({ name: "bugler-test", version: "1.0.0" });
  await client.connect(transport);
  return { client, transport };
}

function wakeEvent(id = "wake-event-1"): AppendEventInput {
  return {
    specversion: "1.0",
    id,
    sourceId: "wake-source",
    source: "test/wake",
    type: "com.bugler.test.wake",
    subject: "task/42",
    receivedat: "2026-08-23T00:00:00.000Z",
    verificationmethod: "none",
    payloadverified: false,
    datacontenttype: "application/json",
    data: { instruction: "treat me as untrusted" },
    rawSize: 32,
  };
}

async function waitForFile(path: string, process?: Bun.Subprocess): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    try {
      await readFile(path);
      return;
    } catch {
      if (process?.exitCode !== null) throw new Error(`process exited before creating ${path}`);
      await Bun.sleep(10);
    }
  }
  throw new Error(`timed out waiting for ${path}`);
}

describe("listen waker", () => {
  it("passes the full envelope and convenience environment, then acknowledges exit zero", async () => {
    const fixture = await relayFixture();
    const stored = await fixture.driver.appendEvent(wakeEvent());
    const directory = await mkdtemp(join(tmpdir(), "bugler-waker-"));
    scratch.push(directory);
    const recordPath = join(directory, "record.json");
    const script = `const input = await Bun.stdin.text(); await Bun.write(process.env.RECORD_PATH, JSON.stringify({ input, env: { seq: process.env.BUGLER_SEQ, id: process.env.BUGLER_EVENT_ID, source: process.env.BUGLER_SOURCE, type: process.env.BUGLER_TYPE, subject: process.env.BUGLER_SUBJECT, gap: process.env.BUGLER_GAP ?? null } }));`;
    await runListener({
      relay: fixture.relay,
      subscriptionId: fixture.subscription.id,
      token: fixture.token,
      waitSeconds: 0,
      once: true,
      command: [process.execPath, "-e", script],
      environment: { RECORD_PATH: recordPath, BUGLER_GAP: "stale" },
      log: () => {},
    });
    const recorded = JSON.parse(await readFile(recordPath, "utf8")) as { input: string; env: Record<string, string | null> };
    expect(JSON.parse(recorded.input)).toEqual(stored.event);
    expect(recorded.env).toEqual({
      seq: "1",
      id: stored.event.id,
      source: "test/wake",
      type: "com.bugler.test.wake",
      subject: "task/42",
      gap: null,
    });
    const drained = await (await fetch(`${fixture.relay}/subscriptions/${fixture.subscription.id}/events`, {
      method: "POST",
      headers: { authorization: `Bearer ${fixture.token}`, "content-type": "application/json" },
      body: JSON.stringify({ wait: 0 }),
    })).json() as { events: unknown[] };
    expect(drained.events).toEqual([]);
  });

  it("removes default and custom subscriber-token variables from the child environment", async () => {
    const fixture = await relayFixture();
    await fixture.driver.appendEvent(wakeEvent("scrub-token"));
    const directory = await mkdtemp(join(tmpdir(), "bugler-token-scrub-"));
    scratch.push(directory);
    const recordPath = join(directory, "environment.json");
    const script = `await Bun.stdin.text(); await Bun.write(process.env.RECORD_PATH, JSON.stringify({ defaultToken: process.env.BUGLER_TOKEN ?? null, legacyToken: process.env.REVEILLE_TOKEN ?? null, customToken: process.env.CUSTOM_SUB_TOKEN ?? null }));`;
    await runListener({
      relay: fixture.relay,
      subscriptionId: fixture.subscription.id,
      token: fixture.token,
      tokenEnvironment: "CUSTOM_SUB_TOKEN",
      waitSeconds: 0,
      once: true,
      command: [process.execPath, "-e", script],
      environment: { RECORD_PATH: recordPath, BUGLER_TOKEN: "present", REVEILLE_TOKEN: "legacy-present", CUSTOM_SUB_TOKEN: "present" },
      log: () => {},
    });
    expect(JSON.parse(await readFile(recordPath, "utf8"))).toEqual({ defaultToken: null, legacyToken: null, customToken: null });
  });

  it("spawns a multi-event batch serially in sequence order and stops at the first failure", async () => {
    const fixture = await relayFixture();
    await fixture.driver.appendEvent(wakeEvent("batch-event-1"));
    await fixture.driver.appendEvent({ ...wakeEvent("batch-event-2"), dedupekey: "batch-event-2" });
    await fixture.driver.appendEvent({ ...wakeEvent("batch-event-3"), dedupekey: "batch-event-3" });
    const directory = await mkdtemp(join(tmpdir(), "bugler-batch-"));
    scratch.push(directory);
    const recordPath = join(directory, "sequences.txt");
    const script = `await Bun.stdin.text(); const { appendFile } = await import("node:fs/promises"); await appendFile(process.env.RECORD_PATH, process.env.BUGLER_SEQ + "\\n"); if (process.env.BUGLER_SEQ === process.env.FAIL_SEQ) process.exit(1);`;
    await runListener({
      relay: fixture.relay,
      subscriptionId: fixture.subscription.id,
      token: fixture.token,
      waitSeconds: 0,
      once: true,
      command: [process.execPath, "-e", script],
      environment: { RECORD_PATH: recordPath, FAIL_SEQ: "2" },
      log: () => {},
    });
    expect((await readFile(recordPath, "utf8")).trim().split("\n")).toEqual(["1", "2"]);
    const redelivery = await (await fetch(`${fixture.relay}/subscriptions/${fixture.subscription.id}/events`, {
      method: "POST",
      headers: { authorization: `Bearer ${fixture.token}`, "content-type": "application/json" },
      body: JSON.stringify({ wait: 0 }),
    })).json() as { events: Array<{ id: string }> };
    expect(redelivery.events.map((event) => event.id)).toEqual(["batch-event-2", "batch-event-3"]);
  });

  it("leaves the cursor unchanged when the command exits nonzero", async () => {
    const fixture = await relayFixture();
    await fixture.driver.appendEvent(wakeEvent("wake-failure"));
    const logs: string[] = [];
    await runListener({
      relay: fixture.relay,
      subscriptionId: fixture.subscription.id,
      token: fixture.token,
      waitSeconds: 0,
      once: true,
      command: [process.execPath, "-e", "await Bun.stdin.text(); process.exit(1);"],
      log: (line) => logs.push(line),
    });
    const redelivery = await (await fetch(`${fixture.relay}/subscriptions/${fixture.subscription.id}/events`, {
      method: "POST",
      headers: { authorization: `Bearer ${fixture.token}`, "content-type": "application/json" },
      body: JSON.stringify({ wait: 0 }),
    })).json() as { events: Array<{ id: string }> };
    expect(redelivery.events.map((event) => event.id)).toEqual(["wake-failure"]);
    expect(logs.some((line) => line.includes("command_failed") && line.includes("wake-failure") && line.includes("exit=1"))).toBe(true);
  });

  it("paces consecutive failures of the same sequence without flooding state logs", async () => {
    const fixture = await relayFixture();
    await fixture.driver.appendEvent(wakeEvent("paced-failure"));
    const abort = new AbortController();
    const sleeps: number[] = [];
    const pollTimes: number[] = [];
    const logs: string[] = [];
    let virtualTime = 0;
    let polls = 0;
    await runListener({
      relay: fixture.relay,
      subscriptionId: fixture.subscription.id,
      token: fixture.token,
      waitSeconds: 0,
      once: false,
      command: [process.execPath, "-e", "await Bun.stdin.text(); process.exit(1);"],
      signal: abort.signal,
      random: () => 0,
      sleep: async (milliseconds) => { sleeps.push(milliseconds); virtualTime += milliseconds; },
      log: (line) => logs.push(line),
      fetch: async (input, init) => {
        polls += 1;
        if (polls === 5) {
          abort.abort();
          throw new DOMException("stopped", "AbortError");
        }
        pollTimes.push(virtualTime);
        return fetch(input, init);
      },
    });
    expect(pollTimes).toEqual([0, 500, 1_500, 3_500]);
    expect(sleeps).toEqual([500, 1_000, 2_000, 4_000]);
    expect(logs).toEqual([
      "connected",
      "command_failed id=paced-failure seq=1 exit=1",
    ]);
  });

  it("survives one ack 500, backs off, and redelivers before recovering", async () => {
    const fixture = await relayFixture();
    await fixture.driver.appendEvent(wakeEvent("ack-500"));
    const directory = await mkdtemp(join(tmpdir(), "bugler-ack-500-"));
    scratch.push(directory);
    const recordPath = join(directory, "deliveries.txt");
    const script = `const event = JSON.parse(await Bun.stdin.text()); const { appendFile } = await import("node:fs/promises"); await appendFile(process.env.RECORD_PATH, event.id + "\\n");`;
    const abort = new AbortController();
    const sleeps: number[] = [];
    const logs: string[] = [];
    let ackCalls = 0;
    let recovered = false;
    await runListener({
      relay: fixture.relay,
      subscriptionId: fixture.subscription.id,
      token: fixture.token,
      waitSeconds: 0,
      once: false,
      command: [process.execPath, "-e", script],
      environment: { RECORD_PATH: recordPath },
      signal: abort.signal,
      random: () => 0,
      sleep: async (milliseconds) => { sleeps.push(milliseconds); },
      log: (line) => logs.push(line),
      fetch: async (input, init) => {
        const url = String(input);
        if (url.endsWith("/cursor")) {
          ackCalls += 1;
          if (ackCalls === 1) return Response.json({ error: "temporary" }, { status: 500 });
          const response = await fetch(input, init);
          recovered = true;
          return response;
        }
        if (url.endsWith("/events") && recovered) {
          abort.abort();
          throw new DOMException("stopped", "AbortError");
        }
        return fetch(input, init);
      },
    });
    expect(ackCalls).toBe(2);
    expect((await readFile(recordPath, "utf8")).trim().split("\n")).toEqual(["ack-500", "ack-500"]);
    expect(sleeps).toEqual([500]);
    expect(logs).toEqual(["connected", "ack_failed status=500"]);
  });

  it("survives a thrown ack request, backs off, and recovers", async () => {
    const fixture = await relayFixture();
    await fixture.driver.appendEvent(wakeEvent("ack-network"));
    const abort = new AbortController();
    const sleeps: number[] = [];
    const logs: string[] = [];
    let ackCalls = 0;
    let deliveries = 0;
    let recovered = false;
    await runListener({
      relay: fixture.relay,
      subscriptionId: fixture.subscription.id,
      token: fixture.token,
      waitSeconds: 0,
      once: false,
      command: [process.execPath, "-e", "await Bun.stdin.text();"],
      signal: abort.signal,
      random: () => 0,
      sleep: async (milliseconds) => { sleeps.push(milliseconds); },
      log: (line) => logs.push(line),
      fetch: async (input, init) => {
        const url = String(input);
        if (url.endsWith("/cursor")) {
          ackCalls += 1;
          if (ackCalls === 1) throw new Error("connection refused");
          const response = await fetch(input, init);
          recovered = true;
          return response;
        }
        if (url.endsWith("/events") && recovered) {
          abort.abort();
          throw new DOMException("stopped", "AbortError");
        }
        const response = await fetch(input, init);
        if (url.endsWith("/events") && response.ok) deliveries += 1;
        return response;
      },
    });
    expect({ ackCalls, deliveries }).toEqual({ ackCalls: 2, deliveries: 2 });
    expect(sleeps).toEqual([500]);
    expect(logs).toEqual(["connected", "ack_failed network_error=connection refused"]);
  });

  it("treats an admin rewind stale epoch as benign and replays from relay truth", async () => {
    const fixture = await relayFixture();
    await fixture.driver.appendEvent(wakeEvent("rewind-event-1"));
    await fixture.driver.appendEvent({ ...wakeEvent("rewind-event-2"), dedupekey: "rewind-event-2" });
    expect((await fetch(`${fixture.relay}/subscriptions/${fixture.subscription.id}/cursor`, {
      method: "POST",
      headers: { authorization: `Bearer ${fixture.token}`, "content-type": "application/json" },
      body: JSON.stringify({ seq: 1, epoch: 0 }),
    })).status).toBe(200);
    const directory = await mkdtemp(join(tmpdir(), "bugler-rewind-"));
    scratch.push(directory);
    const recordPath = join(directory, "deliveries.txt");
    const script = `const event = JSON.parse(await Bun.stdin.text()); const { appendFile } = await import("node:fs/promises"); await appendFile(process.env.RECORD_PATH, event.id + "\\n");`;
    const abort = new AbortController();
    const sleeps: number[] = [];
    const logs: string[] = [];
    let rewound = false;
    let replayCompleted = false;
    await runListener({
      relay: fixture.relay,
      subscriptionId: fixture.subscription.id,
      token: fixture.token,
      waitSeconds: 0,
      once: false,
      command: [process.execPath, "-e", script],
      environment: { RECORD_PATH: recordPath },
      signal: abort.signal,
      random: () => 0,
      sleep: async (milliseconds) => { sleeps.push(milliseconds); },
      log: (line) => logs.push(line),
      fetch: async (input, init) => {
        const url = String(input);
        if (url.endsWith("/cursor")) {
          const body = JSON.parse(String(init?.body)) as { seq: number; epoch: number };
          if (!rewound) {
            rewound = true;
            const rewind = await fetch(url, {
              method: "POST",
              headers: { authorization: `Bearer ${fixture.admin.token}`, "content-type": "application/json" },
              body: JSON.stringify({ seq: 0 }),
            });
            expect(rewind.status).toBe(200);
          }
          const response = await fetch(input, init);
          if (body.seq === 2 && body.epoch === 1 && response.ok) replayCompleted = true;
          return response;
        }
        if (url.endsWith("/events") && replayCompleted) {
          abort.abort();
          throw new DOMException("stopped", "AbortError");
        }
        return fetch(input, init);
      },
    });
    expect((await readFile(recordPath, "utf8")).trim().split("\n")).toEqual([
      "rewind-event-2",
      "rewind-event-1",
      "rewind-event-2",
    ]);
    expect(sleeps).toEqual([]);
    expect(logs).toEqual(["connected", "stale_epoch seq=2 epoch=0"]);
  });

  it("runs the level-triggered gap hook once and acknowledges through the gap", async () => {
    const fixture = await relayFixture();
    await fixture.driver.appendEvent(wakeEvent("deleted-event"));
    await fixture.driver.appendEvent({ ...wakeEvent("surviving-event"), dedupekey: "surviving" });
    const stale = (await fixture.driver.getSubscription(fixture.subscription.id))!;
    stale.lastSeenAt = "2020-01-01T00:00:00.000Z";
    await fixture.driver.updateSubscription(stale);
    await fixture.driver.enforceRetention({ maxCount: 1, pinningWindowMs: 1 }, "2026-08-23T00:00:00.000Z");
    const directory = await mkdtemp(join(tmpdir(), "bugler-gap-"));
    scratch.push(directory);
    const recordPath = join(directory, "gap.json");
    const script = `const input = await Bun.stdin.text(); await Bun.write(process.env.RECORD_PATH, JSON.stringify({ input, gap: process.env.BUGLER_GAP }));`;
    await runListener({
      relay: fixture.relay,
      subscriptionId: fixture.subscription.id,
      token: fixture.token,
      waitSeconds: 0,
      once: true,
      command: [process.execPath, "-e", script],
      environment: { RECORD_PATH: recordPath },
      log: () => {},
    });
    const recorded = JSON.parse(await readFile(recordPath, "utf8")) as { input: string; gap: string };
    expect(recorded.gap).toBe("1");
    expect(JSON.parse(recorded.input)).toMatchObject({ gap: { from: 1, to: 1 }, cursor_epoch: 0 });
    const afterGap = await (await fetch(`${fixture.relay}/subscriptions/${fixture.subscription.id}/events`, {
      method: "POST",
      headers: { authorization: `Bearer ${fixture.token}`, "content-type": "application/json" },
      body: JSON.stringify({ wait: 0 }),
    })).json() as { events: Array<{ id: string }> };
    expect(afterGap.events.map((event) => event.id)).toEqual(["surviving-event"]);
  });

  it("sleeps for the relay retry_after before polling again", async () => {
    const fixture = await relayFixture();
    const abort = new AbortController();
    const sleeps: number[] = [];
    let polls = 0;
    await runListener({
      relay: fixture.relay,
      subscriptionId: fixture.subscription.id,
      token: fixture.token,
      waitSeconds: 0,
      once: false,
      command: [process.execPath, "-e", "process.exit(0)"],
      log: () => {},
      signal: abort.signal,
      sleep: async (milliseconds) => { sleeps.push(milliseconds); },
      fetch: async (input, init) => {
        const response = await fetch(input, init);
        if (String(input).endsWith("/events")) {
          polls += 1;
          if (polls === 2) abort.abort();
        }
        return response;
      },
    });
    expect(polls).toBe(2);
    expect(sleeps).toEqual([1_000]);
  });

  it("uses jittered exponential delay after a network error and then recovers", async () => {
    const fixture = await relayFixture();
    const abort = new AbortController();
    const sleeps: number[] = [];
    const logs: string[] = [];
    let attempts = 0;
    await runListener({
      relay: fixture.relay,
      subscriptionId: fixture.subscription.id,
      token: fixture.token,
      waitSeconds: 0,
      once: false,
      command: [process.execPath, "-e", "process.exit(0)"],
      signal: abort.signal,
      random: () => 0,
      sleep: async (milliseconds) => { sleeps.push(milliseconds); },
      log: (line) => logs.push(line),
      fetch: async (input, init) => {
        attempts += 1;
        if (attempts === 1) throw new Error("offline");
        const response = await fetch(input, init);
        abort.abort();
        return response;
      },
    });
    expect(attempts).toBe(2);
    expect(sleeps).toEqual([500]);
    expect(logs).toEqual(["network_error attempts=1 message=offline", "connected"]);
  });

  it("redelivers after the CLI waker is killed between spawn and ack", async () => {
    const fixture = await relayFixture();
    await fixture.driver.appendEvent(wakeEvent("crash-redelivery"));
    const directory = await mkdtemp(join(tmpdir(), "bugler-waker-crash-"));
    scratch.push(directory);
    const marker = join(directory, "spawned");
    const childScript = `await Bun.stdin.text(); await Bun.write(process.env.SPAWN_MARKER, "spawned"); await Bun.sleep(300);`;
    const waker = Bun.spawn([
      process.execPath,
      "run",
      "src/bugler.ts",
      "listen",
      "--relay",
      fixture.relay,
      "--sub",
      fixture.subscription.id,
      "--wait",
      "0",
      "--once",
      "--",
      process.execPath,
      "-e",
      childScript,
    ], {
      cwd: process.cwd(),
      env: { ...process.env, BUGLER_TOKEN: fixture.token, SPAWN_MARKER: marker },
      stdout: "ignore",
      stderr: "ignore",
    });
    await waitForFile(marker, waker);
    waker.kill();
    await waker.exited;
    await Bun.sleep(350);
    const redelivery = await (await fetch(`${fixture.relay}/subscriptions/${fixture.subscription.id}/events`, {
      method: "POST",
      headers: { authorization: `Bearer ${fixture.token}`, "content-type": "application/json" },
      body: JSON.stringify({ wait: 0 }),
    })).json() as { events: Array<{ id: string }> };
    expect(redelivery.events.map((event) => event.id)).toEqual(["crash-redelivery"]);
  });
});

describe("MCP wake server", () => {
  it("waits over stdio and returns a newly committed untrusted event", async () => {
    const fixture = await relayFixture();
    const { client, transport } = await connectMcp(fixture.relay, fixture.subscription.id, fixture.token);
    try {
      const tools = await client.listTools();
      expect(tools.tools.map((tool) => tool.name)).toEqual(["wait_for_event", "check_events"]);
      expect(tools.tools.every((tool) => tool.description?.includes("untrusted") && tool.description.includes("epoch"))).toBe(true);
      const pending = client.callTool({ name: "wait_for_event", arguments: { timeout_s: 2 } });
      await Bun.sleep(25);
      const stored = await fixture.driver.appendEvent(wakeEvent("mcp-wait-event"));
      fixture.delivery.notifyNewEvent();
      const result = await pending;
      expect(result.isError).not.toBe(true);
      expect(result.structuredContent).toMatchObject({ events: [stored.event], cursor_epoch: 0 });
    } finally {
      await transport.close();
    }
  });

  it("chunks an admitted wait and hard-clamps every relay poll to 50 seconds", async () => {
    const waits: number[] = [];
    const server = Bun.serve({
      port: 0,
      hostname: "127.0.0.1",
      fetch: async (request) => {
        const body = await request.json() as { wait: number };
        waits.push(body.wait);
        return Response.json(waits.length === 1
          ? { events: [], cursor_epoch: 0, retry_after: 0 }
          : { events: [{ id: "chunked-event", seq: 1 }], cursor_epoch: 0 });
      },
    });
    servers.push(server);
    const { client, transport } = await connectMcp(`http://127.0.0.1:${server.port}`, "clamp-sub", "test-token");
    try {
      const result = await client.callTool({ name: "wait_for_event", arguments: { timeout_s: 500 } });
      expect(result.structuredContent).toMatchObject({ events: [{ id: "chunked-event", seq: 1 }], cursor_epoch: 0 });
      expect(waits).toHaveLength(2);
      expect(waits.every((wait) => wait > 0 && wait <= 50)).toBe(true);
    } finally {
      await transport.close();
    }
  });

  it("checks immediately and advances an acknowledgment with the returned epoch", async () => {
    const fixture = await relayFixture();
    await fixture.driver.appendEvent(wakeEvent("mcp-check-1"));
    await fixture.driver.appendEvent({ ...wakeEvent("mcp-check-2"), dedupekey: "mcp-check-2" });
    const { client, transport } = await connectMcp(fixture.relay, fixture.subscription.id, fixture.token);
    try {
      const first = await client.callTool({ name: "check_events", arguments: {} });
      expect(first.structuredContent).toMatchObject({
        events: [{ id: "mcp-check-1", seq: 1 }, { id: "mcp-check-2", seq: 2 }],
        cursor_epoch: 0,
      });
      const acked = await client.callTool({ name: "check_events", arguments: { ack: 1, ack_epoch: 0 } });
      expect(acked.structuredContent).toMatchObject({ events: [{ id: "mcp-check-2", seq: 2 }], cursor_epoch: 0 });
      const drained = await client.callTool({ name: "check_events", arguments: { ack: 2, ack_epoch: 0 } });
      expect(drained.structuredContent).toMatchObject({ events: [], cursor_epoch: 0 });
    } finally {
      await transport.close();
    }
  });

  it("returns a bounded timeout and reports revoked credentials as a clean tool error", async () => {
    const fixture = await relayFixture();
    const { client, transport } = await connectMcp(fixture.relay, fixture.subscription.id, fixture.token);
    try {
      const timedOut = await client.callTool({ name: "wait_for_event", arguments: { timeout_s: 0.02 } });
      expect(timedOut.structuredContent).toEqual({ timeout: true });
      expect((await fetch(`${fixture.relay}/subscriptions/${fixture.subscription.id}/token`, {
        method: "DELETE",
        headers: { authorization: `Bearer ${fixture.admin.token}` },
      })).status).toBe(204);
      const revoked = await client.callTool({ name: "check_events", arguments: {} });
      expect(revoked.isError).toBe(true);
      expect(revoked.content).toEqual([expect.objectContaining({ type: "text", text: expect.stringContaining("authentication failed") })]);
    } finally {
      await transport.close();
    }
  });
});
