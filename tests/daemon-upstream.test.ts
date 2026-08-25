import { chmod, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "bun:test";
import { MemoryLocalStore } from "../src/daemon/memory-store";
import { HostedMirrorClient, OssLongPollMirrorClient, type DaemonSocketFactory } from "../src/daemon/upstream";

const scratch: string[] = [];
afterEach(async () => {
  for (const path of scratch.splice(0)) await rm(path, { recursive: true, force: true });
});

class FakeSocket {
  readyState = 0;
  readonly sent: string[] = [];
  private readonly listeners = new Map<string, Array<(event: never) => void>>();

  addEventListener(type: string, listener: (event: never) => void): void {
    const listeners = this.listeners.get(type) ?? [];
    listeners.push(listener);
    this.listeners.set(type, listeners);
  }

  send(data: string): void {
    this.sent.push(data);
  }

  close(code = 1000, reason = ""): void {
    this.readyState = 3;
    this.emit("close", { code, reason });
  }

  open(): void {
    this.readyState = 1;
    this.emit("open", {});
  }

  message(value: unknown): void {
    this.emit("message", { data: JSON.stringify(value) });
  }

  fail(): void {
    this.emit("error", {});
  }

  private emit(type: string, event: unknown): void {
    for (const listener of this.listeners.get(type) ?? []) listener(event as never);
  }
}

async function credentialFile(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "bugler-upstream-"));
  scratch.push(directory);
  const path = join(directory, "credential");
  await writeFile(path, "bgl_dmn_test\n", { mode: 0o600 });
  await chmod(path, 0o600);
  return path;
}

describe("hosted daemon mirror client", () => {
  it("sends resume and acknowledges only after the local durable commit", async () => {
    const store = new MemoryLocalStore();
    await store.createMirror("hosted", 0, 0);
    const socket = new FakeSocket();
    let socketReady!: () => void;
    const created = new Promise<void>((resolve) => { socketReady = resolve; });
    const fetcher = Object.assign(async () => Response.json({
      session_id: "dms_test", websocket_url: "wss://relay.test/connect", websocket_id: "dws_test",
      device_token: "bgl_dvc_test", expires_at: new Date(Date.now() + 86_400_000).toISOString(), reconnect_after: 120,
    }), { preconnect: fetch.preconnect });
    const client = new HostedMirrorClient({
      origin: "https://relay.test", tenantId: "tenant", credentialFile: await credentialFile(), store,
      notifyNewEvent: () => undefined,
      fetcher,
      socketFactory: ((_url, headers) => {
        expect(headers.authorization).toBe("Bearer bgl_dvc_test");
        socketReady();
        return socket;
      }) as DaemonSocketFactory,
    });
    const running = client.run();
    await created;
    socket.open();
    socket.message({ kind: "ready", protocol: 1, productive_ack_interval: 0, ack_grace_ms: 250 });
    socket.message({
      kind: "events", protocol: 1, channel: "hosted", cursor_epoch: 0,
      events: [{ specversion: "1.0", seq: 1, id: "evt-1", source: "test/source", type: "test.event", time: "2026-08-25T00:00:00.000Z", receivedat: "2026-08-25T00:00:00.000Z", dedupekey: "one", verificationmethod: "none", payloadverified: false, data: { ok: true } }],
    });
    await client.whenIdle();
    expect(JSON.parse(socket.sent[0]!)).toEqual({ kind: "resume", protocol: 1, channels: [{ id: "hosted", ack: 0, ack_epoch: 0 }] });
    expect(JSON.parse(socket.sent[1]!)).toEqual({ kind: "ack", protocol: 1, acks: [{ channel: "hosted", seq: 1, epoch: 0 }] });
    expect((await store.listMirrors())[0]).toMatchObject({ hostedCursor: 1, cursorEpoch: 0 });
    client.stop();
    await running;
  });

  it("fails closed without an ack when the durable commit fails", async () => {
    const store = new MemoryLocalStore({ fault: (phase) => { if (phase === "after_events") throw new Error("disk full"); } });
    await store.createMirror("hosted", 0, 0);
    const socket = new FakeSocket();
    let socketReady!: () => void;
    const created = new Promise<void>((resolve) => { socketReady = resolve; });
    const fetcher = Object.assign(async () => Response.json({
      session_id: "dms_test", websocket_url: "wss://relay.test/connect", websocket_id: "dws_test",
      device_token: "bgl_dvc_test", expires_at: new Date(Date.now() + 86_400_000).toISOString(), reconnect_after: 120,
    }), { preconnect: fetch.preconnect });
    const client = new HostedMirrorClient({
      origin: "https://relay.test", tenantId: "tenant", credentialFile: await credentialFile(), store,
      notifyNewEvent: () => undefined, fetcher,
      socketFactory: (() => { socketReady(); return socket; }) as DaemonSocketFactory,
    });
    const running = client.run();
    await created;
    socket.open();
    socket.message({ kind: "ready", protocol: 1, productive_ack_interval: 0 });
    socket.message({
      kind: "events", protocol: 1, channel: "hosted", cursor_epoch: 0,
      events: [{ specversion: "1.0", seq: 1, id: "evt-1", source: "test/source", type: "test.event", time: "2026-08-25T00:00:00.000Z", receivedat: "2026-08-25T00:00:00.000Z", dedupekey: "one", verificationmethod: "none", payloadverified: false, data: {} }],
    });
    await client.whenIdle();
    expect(socket.sent.map((value) => JSON.parse(value) as { kind: string }).map((frame) => frame.kind)).toEqual(["resume"]);
    expect(socket.readyState).toBe(3);
    client.stop();
    await running;
  });

  it("switches from failed WebSockets to subscriber-token fallback and probes WebSocket again", async () => {
    const store = new MemoryLocalStore();
    await store.createMirror("hosted", 0, 0);
    let now = 0;
    let socketAttempts = 0;
    const states: string[] = [];
    const requests: string[] = [];
    let client!: HostedMirrorClient;
    const fetcher = Object.assign(async (input: string | URL | Request) => {
      const url = new URL(String(input));
      requests.push(url.pathname);
      if (url.pathname.endsWith("/daemon/sessions")) return Response.json({
        session_id: "dms_switch", websocket_url: "wss://relay.test/connect", websocket_id: "dws_switch",
        device_token: "bgl_dvc_test", expires_at: "2099-01-01T00:00:00.000Z", reconnect_after: 1,
      });
      return Response.json({ events: [], cursor_epoch: 0 });
    }, { preconnect: fetch.preconnect });
    const fallback = new OssLongPollMirrorClient({
      origin: "https://relay.test/t/tenant", store, fetcher, notifyNewEvent() {}, tokenFor: async () => "bgl_sub_test",
    });
    client = new HostedMirrorClient({
      origin: "https://relay.test", tenantId: "tenant", credentialFile: await credentialFile(), store, fetcher, fallback,
      notifyNewEvent() {}, now: () => now, fallbackInitialProbeMs: 500, fallbackProbeMs: 500,
      setTimer: ((callback: () => void, milliseconds: number) => {
        const timer = setTimeout(() => { now += milliseconds; callback(); }, 0);
        return timer;
      }),
      socketFactory: (() => {
        socketAttempts += 1;
        const socket = new FakeSocket();
        queueMicrotask(() => {
          if (socketAttempts <= 3) socket.fail();
          else {
            socket.open();
            socket.message({ kind: "ready", protocol: 1, productive_ack_interval: 0 });
          }
        });
        return socket;
      }) as DaemonSocketFactory,
      onState(state) {
        states.push(state);
        if (state === "websocket" && states.includes("fallback")) client.stop();
      },
    });
    await client.run();
    expect(states).toContain("fallback");
    expect(states.at(-1)).toBe("websocket");
    expect(socketAttempts).toBe(4);
    expect(requests).toContain("/t/tenant/subscriptions/hosted/events");
  });

  it("reports the exact subscriptions that make hosted fallback unavailable", async () => {
    const store = new MemoryLocalStore();
    await store.createMirror("missing-token");
    const states: Array<{ state: string; detail?: string }> = [];
    let client!: HostedMirrorClient;
    const fallback = new OssLongPollMirrorClient({
      origin: "https://relay.test/t/tenant", store, notifyNewEvent() {}, tokenFor: async () => undefined,
    });
    const fetcher = Object.assign(async () => Response.json({
      session_id: "dms_missing", websocket_url: "wss://relay.test/connect", websocket_id: "dws_missing",
      device_token: "bgl_dvc_test", expires_at: "2099-01-01T00:00:00.000Z", reconnect_after: 1,
    }), { preconnect: fetch.preconnect });
    client = new HostedMirrorClient({
      origin: "https://relay.test", tenantId: "tenant", credentialFile: await credentialFile(), store, fetcher, fallback,
      notifyNewEvent() {}, fallbackInitialProbeMs: 60_000,
      setTimer: ((callback: () => void) => setTimeout(callback, 0)),
      socketFactory: (() => {
        const socket = new FakeSocket();
        queueMicrotask(() => socket.fail());
        return socket;
      }) as DaemonSocketFactory,
      onState(state, detail) {
        states.push({ state, ...(detail ? { detail } : {}) });
        if (state === "fallback_unavailable") client.stop();
      },
    });
    await client.run();
    expect(states.some(({ state }) => state === "fallback")).toBe(false);
    expect(states.at(-1)).toEqual({ state: "fallback_unavailable", detail: "subscriber tokens required for: missing-token" });
  });

  for (const control of [
    { frame: { kind: "retry", retry_after: 17 }, close: 4408, state: "backoff" },
    { frame: { kind: "stop", reason: "revoked" }, close: 4403, state: "reauthenticate" },
    { frame: { kind: "channel_error", channel: "hosted", code: "stale_epoch" }, close: 1011, state: "backoff" },
  ] as const) {
    it(`handles ${control.frame.kind} control frames explicitly`, async () => {
      const store = new MemoryLocalStore();
      await store.createMirror("hosted");
      const socket = new FakeSocket();
      const states: string[] = [];
      let socketReady!: () => void;
      const created = new Promise<void>((resolve) => { socketReady = resolve; });
      const fetcher = Object.assign(async () => Response.json({
        session_id: "dms_control", websocket_url: "wss://relay.test/connect", websocket_id: "dws_control",
        device_token: "bgl_dvc_test", expires_at: "2099-01-01T00:00:00.000Z", reconnect_after: 1,
      }), { preconnect: fetch.preconnect });
      let client!: HostedMirrorClient;
      client = new HostedMirrorClient({
        origin: "https://relay.test", tenantId: "tenant", credentialFile: await credentialFile(), store, fetcher,
        notifyNewEvent() {}, socketFactory: (() => { socketReady(); return socket; }) as DaemonSocketFactory,
        onState(state) { states.push(state); },
      });
      const running = client.run();
      await created;
      socket.open();
      socket.message({ kind: "ready", protocol: 1, productive_ack_interval: 0 });
      socket.message(control.frame);
      await client.whenIdle();
      expect(socket.readyState).toBe(3);
      expect(states).toContain(control.state);
      client.stop();
      await running;
    });
  }

  it("bounds session-mint rate-limit retries to one delayed retry", async () => {
    const store = new MemoryLocalStore();
    await store.createMirror("hosted");
    let calls = 0;
    let client!: HostedMirrorClient;
    const fetcher = Object.assign(async () => {
      calls += 1;
      return new Response("rate limited", { status: 429, headers: { "retry-after": "1" } });
    }, { preconnect: fetch.preconnect });
    client = new HostedMirrorClient({
      origin: "https://relay.test", tenantId: "tenant", credentialFile: await credentialFile(), store, fetcher,
      notifyNewEvent() {}, setTimer: ((callback: () => void) => setTimeout(callback, 0)),
      onState(state, detail) {
        if (state === "backoff" && detail === "Error") client.stop();
      },
    });
    await client.run();
    expect(calls).toBe(2);
  });
});
