import type { LocalStore } from "./store";
import type { DurableHostedAck, HostedFrame, LocalMirror } from "./types";
import { readSecretFile } from "./account";

interface SessionResponse {
  session_id: string;
  websocket_url: string;
  websocket_id: string;
  device_token: string;
  expires_at: string;
  reconnect_after: number;
}

interface SocketLike {
  readonly readyState: number;
  send(data: string): void;
  close(code?: number, reason?: string): void;
  addEventListener(type: "open", listener: () => void, options?: { once?: boolean }): void;
  addEventListener(type: "message", listener: (event: { data: unknown }) => void): void;
  addEventListener(type: "close", listener: (event: { code: number; reason: string }) => void, options?: { once?: boolean }): void;
  addEventListener(type: "error", listener: () => void, options?: { once?: boolean }): void;
}

export type DaemonSocketFactory = (url: string, headers: Record<string, string>) => SocketLike;

export interface HostedMirrorClientOptions {
  origin: string;
  tenantId: string;
  credentialFile: string;
  store: LocalStore;
  notifyNewEvent(): void;
  fetcher?: typeof fetch;
  socketFactory?: DaemonSocketFactory;
  now?: () => number;
  setTimer?: (callback: () => void, milliseconds: number) => ReturnType<typeof setTimeout>;
  clearTimer?: (timer: ReturnType<typeof setTimeout>) => void;
  onState?: (state: string, detail?: string) => void;
  fallback?: OssLongPollMirrorClient;
  fallbackInitialProbeMs?: number;
  fallbackProbeMs?: number;
}

export interface OssLongPollMirrorClientOptions {
  origin: string;
  store: LocalStore;
  tokenFor(subscriptionId: string): Promise<string | undefined>;
  notifyNewEvent(): void;
  fetcher?: (input: string | URL | Request, init?: RequestInit) => Promise<Response>;
  waitSeconds?: number;
  setTimer?: (callback: () => void, milliseconds: number) => ReturnType<typeof setTimeout>;
  clearTimer?: (timer: ReturnType<typeof setTimeout>) => void;
  onState?: (state: string, detail?: string) => void;
}

export class FallbackTokenError extends Error {
  constructor(readonly subscriptions: string[]) {
    super(`subscriber tokens required for: ${subscriptions.join(", ")}`);
    this.name = "FallbackTokenError";
  }
}

export class OssLongPollMirrorClient {
  private readonly fetcher: NonNullable<OssLongPollMirrorClientOptions["fetcher"]>;
  private readonly setTimer: NonNullable<OssLongPollMirrorClientOptions["setTimer"]>;
  private readonly clearTimer: NonNullable<OssLongPollMirrorClientOptions["clearTimer"]>;
  private stopped = false;
  private timer?: ReturnType<typeof setTimeout>;
  private state = "starting";

  constructor(private readonly options: OssLongPollMirrorClientOptions) {
    this.fetcher = options.fetcher ?? fetch;
    this.setTimer = options.setTimer ?? setTimeout;
    this.clearTimer = options.clearTimer ?? clearTimeout;
  }

  stop(): void {
    this.stopped = true;
    if (this.timer) this.clearTimer(this.timer);
  }

  async run(): Promise<void> {
    let failures = 0;
    while (!this.stopped) {
      try {
        const mirrors = (await this.options.store.listMirrors()).filter((mirror) => !mirror.id.startsWith("internal:"));
        if (mirrors.length === 0) {
          await this.wait(1_000);
          continue;
        }
        const results = await this.pollCycle(this.options.waitSeconds ?? 30);
        this.transition("fallback", "subscriber-token long poll active");
        failures = 0;
        const retryAfter = Math.max(0, ...results.map((result) => result.retryAfter ?? 0));
        if (retryAfter > 0) await this.wait(retryAfter * 1_000);
      } catch (error) {
        if (this.stopped) return;
        failures += 1;
        const delay = Math.min(300, 2 ** Math.min(failures - 1, 8));
        if (error instanceof FallbackTokenError) this.transition("fallback_unavailable", error.message);
        else this.transition("backoff", error instanceof Error ? error.message : "unknown upstream error");
        await this.wait(delay * 1_000);
      }
    }
  }

  async pollCycle(waitSeconds = this.options.waitSeconds ?? 30): Promise<Array<{ retryAfter?: number }>> {
    const mirrors = (await this.options.store.listMirrors()).filter((mirror) => !mirror.id.startsWith("internal:"));
    const missing = (await Promise.all(mirrors.map(async (mirror) => ({ id: mirror.id, token: await this.options.tokenFor(mirror.id) }))))
      .filter((item) => !item.token)
      .map((item) => item.id);
    if (missing.length > 0) throw new FallbackTokenError(missing);
    return Promise.all(mirrors.map((mirror) => this.pollOnce(mirror.id, waitSeconds)));
  }

  async pollOnce(subscriptionId: string, waitSeconds = this.options.waitSeconds ?? 30): Promise<{ retryAfter?: number }> {
    const token = await this.options.tokenFor(subscriptionId);
    if (!token) throw new Error(`subscriber token unavailable for ${subscriptionId}`);
    const mirror = (await this.options.store.listMirrors()).find((item) => item.id === subscriptionId);
    if (!mirror) throw new Error(`mirror ${subscriptionId} not found`);
    const endpoint = `${this.options.origin.replace(/\/$/, "")}/subscriptions/${encodeURIComponent(subscriptionId)}`;
    const poll = async (includeAck: boolean) => this.fetcher(`${endpoint}/events`, {
      method: "POST",
      redirect: "manual",
      headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
      body: JSON.stringify({ wait: Math.max(0, Math.min(waitSeconds, 50)), limit: 100, ...(includeAck && mirror.hostedCursor > 0 ? { ack: mirror.hostedCursor, ack_epoch: mirror.cursorEpoch } : {}) }),
    });
    let response = await poll(true);
    if (response.status === 409) response = await poll(false);
    if (response.status === 429) return { retryAfter: Math.max(1, Number(response.headers.get("retry-after") ?? 1)) };
    if (!response.ok) throw new Error(`OSS relay poll failed with ${response.status}`);
    const body = await response.json() as {
      events?: import("../domain").StoredEvent[];
      gap?: { from: number; to: number };
      cursor_epoch: number;
      retry_after?: number;
    };
    let ack: DurableHostedAck | undefined;
    if (body.gap) {
      ack = await this.options.store.commitHostedFrame({ kind: "gap", subscriptionId, cursorEpoch: body.cursor_epoch, gap: body.gap });
    } else if (body.events?.length) {
      const first = body.events[0]!.seq;
      const expected = body.cursor_epoch === mirror.cursorEpoch ? mirror.receivedCursor + 1 : 1;
      if (first > expected) {
        await this.options.store.commitHostedFrame({ kind: "gap", subscriptionId, cursorEpoch: body.cursor_epoch, gap: { from: expected, to: first - 1 } });
      }
      ack = await this.options.store.commitHostedFrame({ kind: "events", subscriptionId, cursorEpoch: body.cursor_epoch, events: body.events });
    }
    if (ack) {
      this.options.notifyNewEvent();
      const acknowledged = await this.fetcher(`${endpoint}/cursor`, {
        method: "POST",
        redirect: "manual",
        headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
        body: JSON.stringify({ seq: ack.seq, epoch: ack.epoch }),
      });
      if (!acknowledged.ok && acknowledged.status !== 409) throw new Error(`OSS relay ack failed with ${acknowledged.status}`);
    }
    return { ...(body.retry_after !== undefined ? { retryAfter: body.retry_after } : {}) };
  }

  private wait(milliseconds: number): Promise<void> {
    if (this.stopped) return Promise.resolve();
    return new Promise((resolve) => {
      this.timer = this.setTimer(() => {
        delete this.timer;
        resolve();
      }, milliseconds);
    });
  }

  private transition(state: string, detail?: string): void {
    if (this.state === state) return;
    this.state = state;
    this.options.onState?.(state, detail);
  }
}

function defaultSocketFactory(url: string, headers: Record<string, string>): SocketLike {
  const Constructor = WebSocket as unknown as new (url: string, options: { headers: Record<string, string>; protocols: string[] }) => WebSocket;
  return new Constructor(url, { headers, protocols: ["bugler.v1"] });
}

function frameToHosted(frame: Record<string, unknown>): HostedFrame | undefined {
  const subscriptionId = typeof frame.channel === "string" ? frame.channel : undefined;
  const cursorEpoch = Number(frame.cursor_epoch);
  if (!subscriptionId || !Number.isSafeInteger(cursorEpoch) || cursorEpoch < 0) return undefined;
  if (frame.kind === "events" && Array.isArray(frame.events)) {
    return { kind: "events", subscriptionId, cursorEpoch, events: frame.events as import("../domain").StoredEvent[] };
  }
  if ((frame.kind === "gap" || frame.kind === "cursor_jump") && frame.gap && typeof frame.gap === "object") {
    const gap = frame.gap as { from?: unknown; to?: unknown };
    if (Number.isSafeInteger(gap.from) && Number.isSafeInteger(gap.to)) {
      return { kind: "gap", subscriptionId, cursorEpoch, gap: { from: Number(gap.from), to: Number(gap.to) } };
    }
  }
  return undefined;
}

export class HostedMirrorClient {
  private readonly fetcher: typeof fetch;
  private readonly socketFactory: DaemonSocketFactory;
  private readonly now: () => number;
  private readonly setTimer: NonNullable<HostedMirrorClientOptions["setTimer"]>;
  private readonly clearTimer: NonNullable<HostedMirrorClientOptions["clearTimer"]>;
  private socket?: SocketLike;
  private stopped = false;
  private session?: SessionResponse;
  private productiveIntervalMs = 60_000;
  private ackGraceMs = 250;
  private lastAckAt = 0;
  private pendingAcks = new Map<string, DurableHostedAck>();
  private ackTimer?: ReturnType<typeof setTimeout>;
  private frameTail = Promise.resolve();
  private state = "starting";
  private readonly waits = new Map<ReturnType<typeof setTimeout>, () => void>();

  constructor(private readonly options: HostedMirrorClientOptions) {
    this.fetcher = options.fetcher ?? fetch;
    this.socketFactory = options.socketFactory ?? defaultSocketFactory;
    this.now = options.now ?? Date.now;
    this.setTimer = options.setTimer ?? setTimeout;
    this.clearTimer = options.clearTimer ?? clearTimeout;
  }

  stop(): void {
    this.stopped = true;
    if (this.ackTimer) this.clearTimer(this.ackTimer);
    for (const [timer, resolve] of this.waits) {
      this.clearTimer(timer);
      resolve();
    }
    this.waits.clear();
    this.socket?.close(1000, "daemon stopping");
    this.options.fallback?.stop();
  }

  async whenIdle(): Promise<void> {
    await this.frameTail;
  }

  async run(): Promise<void> {
    let failureCount = 0;
    let fallbackUntil = 0;
    let usedFallback = false;
    while (!this.stopped) {
      try {
        if (failureCount >= 3 && this.options.fallback && this.now() < fallbackUntil) {
          let results: Array<{ retryAfter?: number }>;
          try {
            results = await this.options.fallback.pollCycle(30);
          } catch (error) {
            if (!(error instanceof FallbackTokenError)) throw error;
            this.transition("fallback_unavailable", error.message);
            await this.wait(Math.min(60_000, Math.max(1_000, fallbackUntil - this.now())));
            continue;
          }
          this.transition("fallback");
          const pause = Math.max(1, ...results.map((result) => result.retryAfter ?? 0));
          await this.wait(pause * 1_000);
          continue;
        }
        if (failureCount >= 3 && this.options.fallback) {
          failureCount = 0;
          usedFallback = true;
        }
        const mirrors = (await this.options.store.listMirrors()).filter((mirror) => !mirror.id.startsWith("internal:"));
        if (mirrors.length === 0) {
          this.transition("offline", "no mirrored subscriptions");
          await this.wait(1_000);
          continue;
        }
        if (!this.session || Date.parse(this.session.expires_at) - this.now() < 5 * 60_000) this.session = await this.mintSession(mirrors);
        this.transition("connecting");
        const closed = await this.connect(this.session, mirrors);
        if (this.stopped) return;
        if (closed.code === 4401) delete this.session;
        if (closed.code === 4403 || closed.code === 4409) {
          this.transition("reauthenticate", `${closed.code}`);
          return;
        }
        failureCount += 1;
        if (failureCount === 3 && this.options.fallback) {
          fallbackUntil = this.now() + (usedFallback ? this.options.fallbackProbeMs ?? 6 * 60 * 60_000 : this.options.fallbackInitialProbeMs ?? 30 * 60_000);
          continue;
        }
        const floor = this.session?.reconnect_after ?? 60;
        const backoff = Math.min(300, Math.max(floor, 2 ** Math.min(failureCount - 1, 8)));
        this.transition("backoff", `${backoff}s`);
        await this.wait(backoff * 1_000);
      } catch (error) {
        if (this.stopped) return;
        failureCount += 1;
        if (failureCount === 3 && this.options.fallback) {
          fallbackUntil = this.now() + (usedFallback ? this.options.fallbackProbeMs ?? 6 * 60 * 60_000 : this.options.fallbackInitialProbeMs ?? 30 * 60_000);
          continue;
        }
        const backoff = Math.min(300, 2 ** Math.min(failureCount - 1, 8));
        this.transition("backoff", error instanceof Error ? error.name : "Error");
        await this.wait(backoff * 1_000);
      }
    }
  }

  private async mintSession(mirrors: LocalMirror[]): Promise<SessionResponse> {
    const credential = await readSecretFile(this.options.credentialFile);
    for (let attempt = 0; attempt < 2; attempt += 1) {
      const response = await this.fetcher(`${this.relayBase()}/daemon/sessions`, {
        method: "POST",
        redirect: "manual",
        headers: { authorization: `Bearer ${credential}`, "content-type": "application/json" },
        body: JSON.stringify({ protocol: 1, channels: mirrors.map((mirror) => ({ id: mirror.id })) }),
      });
      if (response.status === 429 && attempt === 0) {
        const retryAfter = Number(response.headers.get("retry-after") ?? 1);
        await this.wait(Math.max(1, retryAfter) * 1_000);
        continue;
      }
      if (response.status === 429) throw new Error("daemon session mint remained rate limited after its single delayed retry");
      if (!response.ok) throw new Error(`daemon session mint failed with ${response.status}`);
      return response.json<SessionResponse>();
    }
    throw new Error("daemon session mint retry exhausted");
  }

  private connect(session: SessionResponse, mirrors: LocalMirror[]): Promise<{ code: number; reason: string }> {
    return new Promise((resolve, reject) => {
      const socket = this.socketFactory(session.websocket_url, {
        authorization: `Bearer ${session.device_token}`,
        "x-bugler-websocket-id": session.websocket_id,
        "sec-websocket-protocol": "bugler.v1",
      });
      this.socket = socket;
      let opened = false;
      socket.addEventListener("open", () => { opened = true; }, { once: true });
      socket.addEventListener("message", (event) => {
        this.frameTail = this.frameTail.then(() => this.handleMessage(socket, event.data, mirrors)).catch(() => socket.close(1011, "frame processing failed"));
      });
      socket.addEventListener("error", () => {
        if (!opened) reject(new Error("daemon WebSocket connection failed"));
      }, { once: true });
      socket.addEventListener("close", (event) => {
        if (this.socket === socket) delete this.socket;
        resolve({ code: event.code, reason: event.reason });
      }, { once: true });
    });
  }

  private async handleMessage(socket: SocketLike, data: unknown, mirrors: LocalMirror[]): Promise<void> {
    const text = typeof data === "string" ? data : data instanceof ArrayBuffer ? new TextDecoder().decode(data) : String(data);
    const frame = JSON.parse(text) as Record<string, unknown>;
    if (frame.kind === "ready") {
      this.productiveIntervalMs = Math.max(0, Number(frame.productive_ack_interval ?? 60)) * 1_000;
      this.ackGraceMs = Math.max(0, Number(frame.ack_grace_ms ?? 250));
      this.transition("websocket");
      socket.send(JSON.stringify({
        kind: "resume", protocol: 1,
        channels: mirrors.map((mirror) => ({ id: mirror.id, ack: mirror.hostedCursor, ack_epoch: mirror.cursorEpoch })),
      }));
      return;
    }
    if (frame.kind === "limits_updated") {
      this.productiveIntervalMs = Math.max(0, Number(frame.productive_ack_interval ?? 60)) * 1_000;
      this.ackGraceMs = Math.max(0, Number(frame.ack_grace_ms ?? 250));
      if (this.session) this.session.reconnect_after = Math.max(1, Number(frame.reconnect_after ?? this.session.reconnect_after));
      return;
    }
    if (frame.kind === "retry") {
      if (this.session) this.session.reconnect_after = Math.max(1, Number(frame.retry_after ?? this.session.reconnect_after));
      this.transition("backoff", `server retry ${this.session?.reconnect_after ?? 1}s`);
      socket.close(4408, "server retry");
      return;
    }
    if (frame.kind === "stop") {
      this.transition("reauthenticate", typeof frame.reason === "string" ? frame.reason : "server stopped session");
      socket.close(4403, "server stopped session");
      return;
    }
    if (frame.kind === "channel_error") {
      const channel = typeof frame.channel === "string" ? frame.channel : "unknown";
      const code = typeof frame.code === "string" ? frame.code : "unknown";
      this.transition("backoff", `channel ${channel}: ${code}`);
      socket.close(1011, "channel error");
      return;
    }
    const hosted = frameToHosted(frame);
    if (!hosted) return;
    const ack = await this.options.store.commitHostedFrame(hosted);
    this.options.notifyNewEvent();
    this.pendingAcks.set(ack.subscriptionId, ack);
    this.scheduleAck(socket);
  }

  private scheduleAck(socket: SocketLike): void {
    if (this.ackTimer) return;
    const due = Math.max(this.now(), this.lastAckAt + this.productiveIntervalMs - this.ackGraceMs);
    const flush = () => {
      delete this.ackTimer;
      if (this.pendingAcks.size === 0 || socket.readyState > 1) return;
      const acks = [...this.pendingAcks.values()].map((ack) => ({ channel: ack.subscriptionId, seq: ack.seq, epoch: ack.epoch }));
      this.pendingAcks.clear();
      socket.send(JSON.stringify({ kind: "ack", protocol: 1, acks }));
      this.lastAckAt = this.now();
    };
    if (due <= this.now()) flush();
    else this.ackTimer = this.setTimer(flush, due - this.now());
  }

  private relayBase(): string {
    return `${this.options.origin.replace(/\/$/, "")}/t/${encodeURIComponent(this.options.tenantId)}`;
  }

  private transition(state: string, detail?: string): void {
    if (this.state === state) return;
    this.state = state;
    this.options.onState?.(state, detail);
  }

  private wait(milliseconds: number): Promise<void> {
    if (this.stopped) return Promise.resolve();
    return new Promise((resolve) => {
      const timer = this.setTimer(() => {
        this.waits.delete(timer);
        resolve();
      }, milliseconds);
      this.waits.set(timer, resolve);
    });
  }
}
