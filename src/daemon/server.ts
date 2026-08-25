import { Hono } from "hono";
import { chmodSync } from "node:fs";
import { bearerToken } from "../auth";
import { validateFilter } from "../filter";
import { asErrorMessage, isRecord, parseDurationSeconds } from "../util";
import { LocalStoreError, type LocalStore } from "./store";
import type { LocalActionManager, LocalActionRunner } from "./actions";

export interface LocalServerProfile {
  defaultWaitSeconds: number;
  maxWaitSeconds: number;
  maxBatch: number;
}

export const LOCAL_DELIVERY_PROFILE: LocalServerProfile = {
  defaultWaitSeconds: 45,
  maxWaitSeconds: 50,
  maxBatch: 100,
};

interface Waiter {
  finish(reason: "event" | "timeout" | "superseded" | "aborted"): void;
  timer: ReturnType<typeof setTimeout>;
}

function jsonObject(value: unknown): Record<string, unknown> {
  if (!isRecord(value)) throw new Error("body must be an object");
  return value;
}

export function createLocalConsumerApp(
  store: LocalStore,
  options: {
    profile?: LocalServerProfile;
    transport?: "unix" | "localhost";
    allowedOrigin?: string;
    scheduleWork?: (work: () => void) => void;
    ownerUid?: number;
    peerUid?: (request: Request) => number | undefined;
    actionManager?: LocalActionManager;
    actionRunner?: LocalActionRunner;
  } = {},
): { app: Hono; notifyNewEvent(): void } {
  const profile = options.profile ?? LOCAL_DELIVERY_PROFILE;
  const waiters = new Map<string, Waiter>();
  const app = new Hono();
  const scheduleWork = options.scheduleWork ?? ((work: () => void) => { setTimeout(work, 0); });

  const notifyNewEvent = () => {
    for (const [id, waiter] of waiters) {
      clearTimeout(waiter.timer);
      waiters.delete(id);
      waiter.finish("event");
    }
  };
  const continueBackfills = () => {
    scheduleWork(() => {
      void store.advanceBackfills(1_000).then((remaining) => {
        if (remaining > 0) continueBackfills();
        notifyNewEvent();
      });
    });
  };

  app.use("*", async (context, next) => {
    const url = new URL(context.req.url);
    if (["token", "key", "api_key", "access_token"].some((key) => url.searchParams.has(key))) {
      return context.json({ error: "tokens are not accepted in query strings" }, 400);
    }
    if (options.transport === "unix" && options.peerUid) {
      const peer = options.peerUid(context.req.raw);
      if (peer !== undefined && peer !== (options.ownerUid ?? process.getuid?.())) return context.json({ error: "peer uid rejected" }, 403);
    }
    if (options.transport === "localhost") {
      const host = context.req.header("host")?.split(":")[0];
      if (host !== "127.0.0.1" && host !== "localhost") return context.json({ error: "invalid host" }, 403);
      const origin = context.req.header("origin");
      if (origin !== options.allowedOrigin && context.req.header("x-bugler-local-client") !== "cli") return context.json({ error: "invalid origin" }, 403);
    }
    await next();
  });

  app.onError((error, context) => {
    if (error instanceof LocalStoreError) {
      const statuses = { not_found: 404, already_exists: 409, stale_epoch: 409, beyond_newest: 422, backfilling: 409, epoch_regression: 409, sequence_gap: 409, corrupt: 500 } as const;
      return context.json({ error: error.code }, statuses[error.code]);
    }
    return context.json({ error: asErrorMessage(error) }, 500);
  });

  const token = (headers: Headers): string | undefined => bearerToken(headers.get("authorization") ?? undefined)
    ?? headers.get("x-bugler-key") ?? undefined;
  const management = async (headers: Headers) => store.authenticateManagement(token(headers) ?? "");
  const consume = async (id: string, headers: Headers) => store.authenticateConsumer(id, token(headers) ?? "");

  app.get("/v1/status", async (context) => {
    if (!(await management(context.req.raw.headers))) return context.json({ error: "unauthorized" }, 401);
    const mirrors = (await store.listMirrors()).map((mirror) => ({
      id: mirror.id,
      state: mirror.state,
      lag: Math.max(0, mirror.hostedCursor - mirror.receivedCursor),
    }));
    return context.json({
      ok: true,
      mirrors,
      consumers: (await store.listConsumers()).length,
      actions: (await store.listActions()).length,
      dead_letter: (await store.listDeadLetters()).length,
    });
  });

  app.post("/v1/mirrors", async (context) => {
    if (!(await management(context.req.raw.headers))) return context.json({ error: "unauthorized" }, 401);
    const body = jsonObject(await context.req.json());
    if (typeof body.id !== "string" || !body.id) return context.json({ error: "id is required" }, 422);
    return context.json({ mirror: await store.createMirror(body.id, Number(body.cursor ?? 0), Number(body.epoch ?? 0)) }, 201);
  });
  app.get("/v1/mirrors", async (context) => {
    if (!(await management(context.req.raw.headers))) return context.json({ error: "unauthorized" }, 401);
    return context.json({ mirrors: await store.listMirrors() });
  });

  app.post("/v1/consumers", async (context) => {
    if (!(await management(context.req.raw.headers))) return context.json({ error: "unauthorized" }, 401);
    const body = jsonObject(await context.req.json());
    if (typeof body.id !== "string" || !Array.isArray(body.mirrors) || !body.mirrors.every((item) => typeof item === "string")) {
      return context.json({ error: "id and mirrors are required" }, 422);
    }
    if (body.filter !== undefined) validateFilter(body.filter);
    try {
      const created = await store.createConsumer({
        id: body.id,
        mirrors: body.mirrors as string[],
        ...(body.filter ? { filter: body.filter as import("../domain").Filter } : {}),
        ...(body.start === "oldest" ? { start: "oldest" as const } : {}),
      });
      if (created.consumer.state === "backfilling") {
        continueBackfills();
        return context.json({
          consumer_id: created.consumer.id,
          state: "backfilling",
          mapped: created.consumer.mapped ?? 0,
          total_estimate: created.consumer.totalEstimate ?? 0,
          token: created.token,
        }, 202);
      }
      return context.json(created, 201);
    } catch (error) {
      if (error instanceof LocalStoreError && error.code === "already_exists") return context.json({ error: "consumer id already exists" }, 409);
      throw error;
    }
  });
  app.get("/v1/consumers", async (context) => {
    if (!(await management(context.req.raw.headers))) return context.json({ error: "unauthorized" }, 401);
    return context.json({ consumers: await store.listConsumers() });
  });
  app.get("/v1/consumers/:id", async (context) => {
    if (!(await management(context.req.raw.headers))) return context.json({ error: "unauthorized" }, 401);
    const consumer = await store.getConsumer(context.req.param("id"));
    return consumer ? context.json({ consumer }) : context.json({ error: "not_found" }, 404);
  });

  app.post("/v1/consumers/:id/events", async (context) => {
    const id = context.req.param("id");
    if (!(await consume(id, context.req.raw.headers))) return context.json({ error: "unauthorized" }, 401);
    const body = jsonObject(await context.req.json().catch(() => ({})));
    if (typeof body.ack === "number") {
      if (typeof body.ack_epoch !== "number") return context.json({ error: "ack_epoch_required" }, 422);
      const result = await store.acknowledgeConsumer(id, body.ack, body.ack_epoch);
      if (result.status === "not_found") return context.json({ error: "not_found" }, 404);
      if (result.status === "stale_epoch") return context.json({ error: "stale_epoch" }, 409);
      if (result.status === "beyond_newest") return context.json({ error: "beyond_newest" }, 422);
    }
    const limit = typeof body.limit === "number" ? Math.max(1, Math.min(Math.trunc(body.limit), profile.maxBatch)) : profile.maxBatch;
    let payload = await store.readConsumer(id, limit);
    if ("gap" in payload || payload.events.length > 0) return context.json(payload);
    const previous = waiters.get(id);
    if (previous) {
      clearTimeout(previous.timer);
      waiters.delete(id);
      previous.finish("superseded");
    }
    const wait = Math.max(0, Math.min(body.wait === undefined ? profile.defaultWaitSeconds : parseDurationSeconds(body.wait), profile.maxWaitSeconds));
    if (wait <= 0) return context.json(payload);
    const reason = await new Promise<"event" | "timeout" | "superseded" | "aborted">((resolve) => {
      const timer = setTimeout(() => {
        if (waiters.get(id)?.timer === timer) waiters.delete(id);
        resolve("timeout");
      }, wait * 1_000);
      const abort = () => {
        if (waiters.get(id)?.timer === timer) waiters.delete(id);
        clearTimeout(timer);
        resolve("aborted");
      };
      context.req.raw.signal.addEventListener("abort", abort, { once: true });
      waiters.set(id, { finish: (value) => {
        context.req.raw.signal.removeEventListener("abort", abort);
        resolve(value);
      }, timer });
    });
    if (reason === "superseded") return context.json({ ...payload, superseded: true });
    if (reason === "aborted") return new Response(null, { status: 499 });
    payload = await store.readConsumer(id, limit);
    return context.json(payload);
  });

  app.post("/v1/consumers/:id/cursor", async (context) => {
    const id = context.req.param("id");
    const isManagement = await management(context.req.raw.headers);
    if (!isManagement && !(await consume(id, context.req.raw.headers))) return context.json({ error: "unauthorized" }, 401);
    const body = jsonObject(await context.req.json());
    if (!Number.isSafeInteger(body.seq) || Number(body.seq) < 0) return context.json({ error: "seq must be a non-negative integer" }, 422);
    const current = await store.getConsumer(id);
    if (!current) return context.json({ error: "not_found" }, 404);
    if (isManagement && Number(body.seq) < current.cursor) {
      const rewound = await store.rewindConsumer(id, Number(body.seq));
      return context.json(rewound);
    }
    if (!Number.isSafeInteger(body.epoch)) return context.json({ error: "epoch is required" }, 422);
    const result = await store.acknowledgeConsumer(id, Number(body.seq), Number(body.epoch));
    if (result.status === "stale_epoch") return context.json({ error: "stale_epoch" }, 409);
    if (result.status === "beyond_newest") return context.json({ error: "beyond_newest" }, 422);
    if (result.status === "not_found") return context.json({ error: "not_found" }, 404);
    return context.json(result);
  });

  app.get("/v1/consumers/:id/events", async (context) => {
    const id = context.req.param("id");
    if (!(await consume(id, context.req.raw.headers))) return context.json({ error: "unauthorized" }, 401);
    const number = (value: string | undefined) => value === undefined ? undefined : Number(value);
    const sinceSeq = number(context.req.query("since_seq"));
    const limit = number(context.req.query("limit"));
    return context.json(await store.history(id, {
      ...(context.req.query("source") ? { source: context.req.query("source")! } : {}),
      ...(context.req.query("type") ? { type: context.req.query("type")! } : {}),
      ...(context.req.query("subject") ? { subject: context.req.query("subject")! } : {}),
      ...(sinceSeq !== undefined ? { sinceSeq } : {}),
      ...(limit !== undefined ? { limit } : {}),
    }));
  });

  app.post("/v1/consumers/:id/token/rotate", async (context) => {
    if (!(await management(context.req.raw.headers))) return context.json({ error: "unauthorized" }, 401);
    const body = jsonObject(await context.req.json().catch(() => ({})));
    const token = await store.rotateConsumerToken(context.req.param("id"), Number(body.overlap_s ?? 86_400));
    return token ? context.json({ token }) : context.json({ error: "not_found" }, 404);
  });
  app.delete("/v1/consumers/:id/token", async (context) => {
    if (!(await management(context.req.raw.headers))) return context.json({ error: "unauthorized" }, 401);
    return (await store.revokeConsumerToken(context.req.param("id"))) ? context.body(null, 204) : context.json({ error: "not_found" }, 404);
  });

  app.get("/v1/actions", async (context) => {
    if (!(await management(context.req.raw.headers))) return context.json({ error: "unauthorized" }, 401);
    return context.json({ actions: await store.listActions() });
  });
  app.post("/v1/actions", async (context) => {
    if (!(await management(context.req.raw.headers))) return context.json({ error: "unauthorized" }, 401);
    if (!options.actionManager) return context.json({ error: "actions unavailable" }, 503);
    return context.json({ action: await options.actionManager.install(await context.req.json()) }, 201);
  });
  app.delete("/v1/actions/:id", async (context) => {
    if (!(await management(context.req.raw.headers))) return context.json({ error: "unauthorized" }, 401);
    if (!options.actionManager) return context.json({ error: "actions unavailable" }, 503);
    return (await options.actionManager.remove(context.req.param("id"))) ? context.body(null, 204) : context.json({ error: "not_found" }, 404);
  });
  app.post("/v1/actions/reload", async (context) => {
    if (!(await management(context.req.raw.headers))) return context.json({ error: "unauthorized" }, 401);
    if (!options.actionManager) return context.json({ error: "actions unavailable" }, 503);
    return context.json(await options.actionManager.reconcileFiles());
  });
  app.get("/v1/dead-letter", async (context) => {
    if (!(await management(context.req.raw.headers))) return context.json({ error: "unauthorized" }, 401);
    const since = context.req.query("since");
    const limit = Math.max(1, Math.min(1_000, Number(context.req.query("limit") ?? 100)));
    const ordered = (await store.listDeadLetters()).sort((left, right) => left.createdAt.localeCompare(right.createdAt) || left.id.localeCompare(right.id));
    const start = since ? Math.max(0, ordered.findIndex((item) => item.id === since) + 1) : 0;
    const page = ordered.slice(start, start + limit);
    return context.json({ dead_letters: page, ...(start + page.length < ordered.length && page.at(-1) ? { next_since: page.at(-1)!.id } : {}) });
  });
  app.post("/v1/dead-letter/:id/replay", async (context) => {
    if (!(await management(context.req.raw.headers))) return context.json({ error: "unauthorized" }, 401);
    if (!options.actionRunner) return context.json({ error: "actions unavailable" }, 503);
    const result = await options.actionRunner.replayDeadLetter(context.req.param("id"), context.req.raw.signal);
    if (result === "not_found") return context.json({ error: "not_found" }, 404);
    if (result === "unavailable") return context.json({ error: "replay_unavailable" }, 409);
    return context.json({ status: result }, result === "succeeded" ? 200 : 502);
  });

  app.notFound((context) => context.json({ error: "not_found" }, 404));
  return { app, notifyNewEvent };
}

export function startLocalDaemonServer(input: {
  store: LocalStore;
  unix?: string;
  port?: number;
  hostname?: string;
  allowedOrigin?: string;
  actionManager?: LocalActionManager;
  actionRunner?: LocalActionRunner;
}): { server: Bun.Server<undefined>; notifyNewEvent(): void; url: string } {
  const local = createLocalConsumerApp(input.store, {
    transport: input.unix ? "unix" : "localhost",
    ...(input.allowedOrigin ? { allowedOrigin: input.allowedOrigin } : {}),
    ...(input.actionManager ? { actionManager: input.actionManager } : {}),
    ...(input.actionRunner ? { actionRunner: input.actionRunner } : {}),
  });
  const server = input.unix
    ? Bun.serve({ unix: input.unix, fetch: local.app.fetch })
    : Bun.serve({ port: input.port ?? 0, hostname: input.hostname ?? "127.0.0.1", fetch: local.app.fetch });
  if (input.unix) chmodSync(input.unix, 0o600);
  return { server, notifyNewEvent: local.notifyNewEvent, url: input.unix ? "http://localhost" : `http://127.0.0.1:${server.port}` };
}
