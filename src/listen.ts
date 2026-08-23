import type { StoredEvent } from "./domain";

interface EventBatch {
  events: StoredEvent[];
  cursor_epoch: number;
  retry_after?: number;
}

interface GapBatch {
  gap: { from: number; to: number };
  cursor_epoch: number;
  retry_after?: number;
}

type ListenerBatch = EventBatch | GapBatch;
type ListenerFetch = (input: string | URL | Request, init?: RequestInit) => Promise<Response>;
type AcknowledgeResult =
  | { status: "ok" }
  | { status: "stale_epoch" }
  | { status: "network_error"; message: string }
  | { status: "server_error"; code: number };

export interface ListenerOptions {
  relay: string;
  subscriptionId: string;
  token: string;
  tokenEnvironment?: string;
  waitSeconds: number;
  once: boolean;
  command: string[];
  environment?: Record<string, string>;
  log?: (line: string) => void;
  fetch?: ListenerFetch;
  sleep?: (milliseconds: number) => Promise<void>;
  random?: () => number;
  signal?: AbortSignal;
}

function subscriptionUrl(options: ListenerOptions, suffix: string): string {
  return `${options.relay.replace(/\/$/, "")}/subscriptions/${encodeURIComponent(options.subscriptionId)}/${suffix}`;
}

async function spawnDelivery(options: ListenerOptions, delivery: StoredEvent | GapBatch): Promise<number> {
  const gap = "gap" in delivery;
  const deliveryEnvironment = gap ? { BUGLER_GAP: "1" } : {
    BUGLER_SEQ: String(delivery.seq),
    BUGLER_EVENT_ID: delivery.id,
    BUGLER_SOURCE: delivery.source,
    BUGLER_TYPE: delivery.type,
    ...(delivery.subject !== undefined ? { BUGLER_SUBJECT: delivery.subject } : {}),
  };
  const environment: Record<string, string | undefined> = { ...process.env, ...options.environment };
  for (const name of [
    "BUGLER_TOKEN",
    "REVEILLE_TOKEN",
    options.tokenEnvironment ?? "BUGLER_TOKEN",
    "BUGLER_GAP",
    "BUGLER_SEQ",
    "BUGLER_EVENT_ID",
    "BUGLER_SOURCE",
    "BUGLER_TYPE",
    "BUGLER_SUBJECT",
    "REVEILLE_GAP",
    "REVEILLE_SEQ",
    "REVEILLE_EVENT_ID",
    "REVEILLE_SOURCE",
    "REVEILLE_TYPE",
    "REVEILLE_SUBJECT",
  ]) {
    delete environment[name];
  }
  Object.assign(environment, deliveryEnvironment);
  const child = Bun.spawn(options.command, {
    stdin: "pipe",
    stdout: "inherit",
    stderr: "inherit",
    env: environment,
  });
  child.stdin.write(JSON.stringify(delivery));
  child.stdin.end();
  return child.exited;
}

async function acknowledge(options: ListenerOptions, seq: number, epoch: number): Promise<AcknowledgeResult> {
  let response: Response;
  try {
    response = await (options.fetch ?? fetch)(subscriptionUrl(options, "cursor"), {
      method: "POST",
      headers: { authorization: `Bearer ${options.token}`, "content-type": "application/json" },
      body: JSON.stringify({ seq, epoch }),
      redirect: "manual",
      ...(options.signal ? { signal: options.signal } : {}),
    });
  } catch (error) {
    return { status: "network_error", message: error instanceof Error ? error.message : String(error) };
  }
  if (response.ok) return { status: "ok" };
  if (response.status === 409) return { status: "stale_epoch" };
  if (response.status >= 500) return { status: "server_error", code: response.status };
  if (response.status === 401 || response.status === 403) throw new Error(`relay authentication failed with ${response.status}`);
  throw new Error(`relay cursor acknowledgment returned ${response.status}`);
}

function retryAfterSeconds(response: Response, body?: Partial<ListenerBatch> & { retry_after?: number }): number | undefined {
  if (typeof body?.retry_after === "number" && Number.isFinite(body.retry_after) && body.retry_after >= 0) return body.retry_after;
  const rawHeader = response.headers.get("retry-after");
  if (rawHeader === null) return undefined;
  const header = Number(rawHeader);
  return Number.isFinite(header) && header >= 0 ? header : undefined;
}

async function defaultSleep(milliseconds: number, signal?: AbortSignal): Promise<void> {
  if (milliseconds <= 0 || signal?.aborted) return;
  await new Promise<void>((resolve) => {
    const timer = setTimeout(finish, milliseconds);
    function finish() {
      clearTimeout(timer);
      signal?.removeEventListener("abort", finish);
      resolve();
    }
    signal?.addEventListener("abort", finish, { once: true });
  });
}

function backoffMilliseconds(failureCount: number, random: () => number): number {
  const ceiling = Math.min(1_000 * 2 ** (failureCount - 1), 30_000);
  return Math.round(ceiling * (0.5 + random() * 0.5));
}

export async function runListener(options: ListenerOptions): Promise<void> {
  if (options.command.length === 0) throw new Error("listen requires a command after --");
  const fetcher = options.fetch ?? fetch;
  const sleep = options.sleep ?? ((milliseconds: number) => defaultSleep(milliseconds, options.signal));
  const random = options.random ?? Math.random;
  const log = options.log ?? ((line: string) => process.stderr.write(`${line}\n`));
  let connectionState: string | undefined;
  let handlerState = "healthy";
  let acknowledgeState = "healthy";
  let networkFailures = 0;
  let acknowledgeFailures = 0;
  let handlerFailure: { key: string; count: number } | undefined;
  const logConnectionState = (state: string, line: string) => {
    if (connectionState === state) return;
    connectionState = state;
    log(line);
  };
  const logHandlerState = (state: string, line: string) => {
    if (handlerState === state) return;
    handlerState = state;
    log(line);
  };
  const logAcknowledgeState = (state: string, line: string) => {
    if (acknowledgeState === state) return;
    acknowledgeState = state;
    log(line);
  };
  const acknowledgeDelivery = async (seq: number, epoch: number): Promise<"ok" | "drop_batch"> => {
    const result = await acknowledge(options, seq, epoch);
    if (result.status === "ok") {
      acknowledgeFailures = 0;
      acknowledgeState = "healthy";
      return "ok";
    }
    if (result.status === "stale_epoch") {
      acknowledgeFailures = 0;
      logAcknowledgeState(`stale_epoch:${seq}:${epoch}`, `stale_epoch seq=${seq} epoch=${epoch}`);
      return "drop_batch";
    }
    if (options.signal?.aborted) return "drop_batch";
    acknowledgeFailures += 1;
    if (result.status === "server_error") {
      logAcknowledgeState(`ack_failed:${result.code}`, `ack_failed status=${result.code}`);
    } else {
      logAcknowledgeState("ack_failed:network", `ack_failed network_error=${result.message}`);
    }
    await sleep(backoffMilliseconds(acknowledgeFailures, random));
    return "drop_batch";
  };
  while (!options.signal?.aborted) {
    let response: Response;
    try {
      response = await fetcher(subscriptionUrl(options, "events"), {
        method: "POST",
        headers: { authorization: `Bearer ${options.token}`, "content-type": "application/json" },
        body: JSON.stringify({ wait: options.waitSeconds }),
        redirect: "manual",
        ...(options.signal ? { signal: options.signal } : {}),
      });
    } catch (error) {
      if (options.signal?.aborted) return;
      networkFailures += 1;
      logConnectionState("network_error", `network_error attempts=${networkFailures} message=${error instanceof Error ? error.message : String(error)}`);
      await sleep(backoffMilliseconds(networkFailures, random));
      continue;
    }
    if (response.status === 401 || response.status === 403) throw new Error(`relay authentication failed with ${response.status}`);
    if (response.status === 429) {
      const body = await response.json().catch(() => ({})) as { retry_after?: number };
      const retryAfter = retryAfterSeconds(response, body) ?? 1;
      logConnectionState("rate_limited", `rate_limited retry_after=${retryAfter}`);
      await sleep(retryAfter * 1000);
      continue;
    }
    if (!response.ok) {
      networkFailures += 1;
      logConnectionState("relay_error", `relay_error status=${response.status}`);
      await sleep(backoffMilliseconds(networkFailures, random));
      continue;
    }
    networkFailures = 0;
    logConnectionState("connected", "connected");
    let batch: ListenerBatch;
    try {
      batch = await response.json() as ListenerBatch;
    } catch (error) {
      if (options.signal?.aborted) return;
      throw error;
    }
    let handlerBackoff: number | undefined;
    if ("gap" in batch) {
      const exitCode = await spawnDelivery(options, batch);
      if (exitCode === 0) {
        handlerFailure = undefined;
        handlerState = "healthy";
        await acknowledgeDelivery(batch.gap.to, batch.cursor_epoch);
      } else {
        const key = `gap:${batch.gap.to}`;
        handlerFailure = { key, count: handlerFailure?.key === key ? handlerFailure.count + 1 : 1 };
        logHandlerState(`command_failed:${key}`, `command_failed gap=${batch.gap.from}-${batch.gap.to} exit=${exitCode}`);
        handlerBackoff = backoffMilliseconds(handlerFailure.count, random);
      }
    } else {
      for (const event of [...batch.events].sort((left, right) => left.seq - right.seq)) {
        const exitCode = await spawnDelivery(options, event);
        if (exitCode !== 0) {
          const key = String(event.seq);
          handlerFailure = { key, count: handlerFailure?.key === key ? handlerFailure.count + 1 : 1 };
          logHandlerState(`command_failed:${key}`, `command_failed id=${event.id} seq=${event.seq} exit=${exitCode}`);
          handlerBackoff = backoffMilliseconds(handlerFailure.count, random);
          break;
        }
        handlerFailure = undefined;
        handlerState = "healthy";
        if ((await acknowledgeDelivery(event.seq, batch.cursor_epoch)) === "drop_batch") break;
      }
    }
    if (options.once) return;
    if (options.signal?.aborted) return;
    if (handlerBackoff !== undefined) {
      await sleep(handlerBackoff);
      continue;
    }
    const retryAfter = retryAfterSeconds(response, batch);
    if (retryAfter !== undefined && ("gap" in batch || batch.events.length === 0)) await sleep(retryAfter * 1000);
  }
}
