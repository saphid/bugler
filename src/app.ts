import { Hono } from "hono";
import { streamSSE } from "hono/streaming";
import {
  authenticateAdmin,
  authenticateAnySubscriber,
  authenticateSubscriber,
  bearerToken,
  mintToken,
} from "./auth";
import { BINARY_DELIVERY_PROFILE, DeliveryCoordinator, DeliveryError, type DeliveryProfile, type EventEntitlement } from "./delivery";
import type {
  AdminKey,
  EventQuery,
  Filter,
  MappingConfig,
  Source,
  Subscription,
  Transport,
  VerificationConfig,
} from "./domain";
import { validateFilter } from "./filter";
import { queryHistory } from "./history";
import { mapEnvelope } from "./mapping";
import { OPENAPI_DOCUMENT } from "./openapi";
import { pushSecretReferences, PushEngine } from "./push";
import type { SecretResolver } from "./secrets";
import { SecretReferenceError, validateSecretReferences } from "./secrets";
import { StoragePayloadTooLargeError, type StorageDriver } from "./storage/driver";
import { asErrorMessage, isRecord, newUlid, parseDurationSeconds } from "./util";
import { secretReferences, verifyRequest } from "./verification";

export interface RelayHealth {
  retentionPressure: boolean;
  secretFailures: Set<string>;
}

export interface CreateAppOptions {
  driver: StorageDriver;
  secrets: SecretResolver;
  deliveryProfile?: DeliveryProfile;
  maxPayloadBytes?: number;
  now?: () => Date;
  health?: RelayHealth;
  pushEngine?: PushEngine;
  sseMaxLifetimeMs?: number;
  sseRecheckMs?: number;
  onEventsDelivered?: (subscriptionId: string, seqs: number[], at: string) => Promise<void>;
  onSubscriberAuthChanged?: (subscription: Subscription) => Promise<void>;
  onSubscriberDeleted?: (subscriptionId: string) => Promise<void>;
  onSourceChanged?: (source: Source) => Promise<void>;
  onSourceDeleted?: (sourceId: string) => Promise<void>;
  onAdminAuthChanged?: (key: AdminKey) => Promise<void>;
  onAdminDeleted?: (keyId: string) => Promise<void>;
  onEventCommitted?: (sourceId: string, seq: number) => Promise<void>;
  subscriberStreamAllowed?: (subscriptionId: string, admissionId?: string) => Promise<boolean>;
  eventEntitlement?: (request: Request, subscription?: Subscription) => Promise<EventEntitlement>;
}

function publicSubscription(subscription: Subscription): Omit<Subscription, "tokenHash" | "oldTokenHash"> {
  const { tokenHash: _tokenHash, oldTokenHash: _oldTokenHash, ...visible } = subscription;
  return visible;
}

function publicAdminKey(key: Awaited<ReturnType<StorageDriver["listAdminKeys"]>>[number]) {
  return { id: key.id, key_prefix: key.keyPrefix, created_at: key.createdAt, ...(key.expiresAt ? { expires_at: key.expiresAt } : {}) };
}

function parseJsonObject(value: unknown, label: string): Record<string, unknown> {
  if (!isRecord(value)) throw new Error(`${label} must be an object`);
  return value;
}

function assertNoInlineSecrets(value: unknown, path = "config"): void {
  if (Array.isArray(value)) {
    value.forEach((item, index) => assertNoInlineSecrets(item, `${path}[${index}]`));
    return;
  }
  if (!isRecord(value)) return;
  for (const [key, child] of Object.entries(value)) {
    const normalized = key.toLowerCase();
    const allowedReference = normalized.endsWith("_ref") || normalized.endsWith("_refs");
    if (!allowedReference && /(^|_)(secret|password|credential|token|api_key)($|_)/.test(normalized)) {
      throw new Error(`plaintext secret field is forbidden at ${path}.${key}; use a secret reference`);
    }
    assertNoInlineSecrets(child, `${path}.${key}`);
  }
}

function validateMapping(value: unknown): MappingConfig {
  const mapping = parseJsonObject(value, "mapping");
  if (typeof mapping.source !== "string" || !mapping.source) throw new Error("mapping.source is required");
  if ("id" in mapping) throw new Error("relay event id is not mappable");
  if (mapping.preset !== "github" && mapping.type === undefined) throw new Error("mapping.type is required without a preset");
  return mapping as unknown as MappingConfig;
}

function validateVerification(value: unknown): VerificationConfig {
  const config = parseJsonObject(value, "verification");
  if (!("preset" in config) && !("primitive" in config)) throw new Error("verification requires a preset or primitive");
  return config as unknown as VerificationConfig;
}

function validateTransport(value: unknown): Transport {
  const transport = parseJsonObject(value, "transport");
  if (transport.type === "pull") return { type: "pull" };
  if (transport.type !== "push" || typeof transport.url !== "string" || !Array.isArray(transport.whsec_refs) || !transport.whsec_refs.every((item) => typeof item === "string")) {
    throw new Error("invalid transport");
  }
  const url = new URL(transport.url);
  if (url.protocol !== "http:" && url.protocol !== "https:") throw new Error("push URL must use http or https");
  const normalized = { ...transport } as Record<string, unknown>;
  const auth = transport.destination_auth;
  if (auth === undefined || auth === "none") normalized.destination_auth = { type: "none" };
  else if (isRecord(auth) && auth.type === "none") normalized.destination_auth = { type: "none" };
  else if (isRecord(auth) && auth.type === "bearer" && typeof auth.secret_ref === "string") {
    normalized.destination_auth = { type: "bearer", secret_ref: auth.secret_ref };
  } else if (isRecord(auth) && auth.type === "header" && typeof auth.name === "string" && typeof auth.secret_ref === "string") {
    normalized.destination_auth = { type: "header", name: auth.name, secret_ref: auth.secret_ref };
  }
  else if (isRecord(auth) && isRecord(auth.bearer) && typeof auth.bearer.secret_ref === "string") {
    normalized.destination_auth = { type: "bearer", secret_ref: auth.bearer.secret_ref };
  } else if (isRecord(auth) && isRecord(auth.header) && typeof auth.header.name === "string" && typeof auth.header.secret_ref === "string") {
    normalized.destination_auth = { type: "header", name: auth.header.name, secret_ref: auth.header.secret_ref };
  } else throw new Error("invalid destination_auth");
  return normalized as unknown as Transport;
}

function numberParam(value: string | undefined, name: string): number | undefined {
  if (value === undefined) return undefined;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) throw new Error(`${name} must be a non-negative integer`);
  return parsed;
}

async function readBodyCapped(request: Request, cap: number): Promise<Uint8Array> {
  const declared = Number(request.headers.get("content-length"));
  if (Number.isFinite(declared) && declared > cap) throw new DeliveryError("beyond_newest", 413);
  if (!request.body) return new Uint8Array();
  const reader = request.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    total += value.byteLength;
    if (total > cap) {
      await reader.cancel();
      throw new DeliveryError("beyond_newest", 413);
    }
    chunks.push(value);
  }
  const body = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return body;
}

function queryHasToken(url: URL): boolean {
  return ["token", "access_token", "api_key", "key", "authorization"].some((name) => url.searchParams.has(name));
}

export function createApp(options: CreateAppOptions): { app: Hono; delivery: DeliveryCoordinator; health: RelayHealth } {
  const { driver, secrets } = options;
  const now = options.now ?? (() => new Date());
  const health = options.health ?? { retentionPressure: false, secretFailures: new Set<string>() };
  const delivery = new DeliveryCoordinator(driver, options.deliveryProfile ?? BINARY_DELIVERY_PROFILE, now);
  const pushEngine = options.pushEngine ?? new PushEngine(driver, secrets, { now });
  const bodyCap = Math.min(options.maxPayloadBytes ?? 5 * 1024 * 1024, driver.capabilities.maxPayloadBytes ?? Number.POSITIVE_INFINITY);
  const sseMaxLifetimeMs = options.sseMaxLifetimeMs ?? 5 * 60 * 1000;
  const sseRecheckMs = options.sseRecheckMs ?? 15_000;
  const app = new Hono();

  app.use("*", async (context, next) => {
    if (queryHasToken(new URL(context.req.url))) return context.json({ error: "tokens are not accepted in query strings" }, 400);
    await next();
  });

  app.onError((error, context) => {
    if (error instanceof StoragePayloadTooLargeError) return context.json({ error: "payload too large" }, 413);
    if (error instanceof DeliveryError) {
      if (error.retryAfter !== undefined) context.header("Retry-After", String(error.retryAfter));
      return context.json({ error: error.code, ...(error.retryAfter !== undefined ? { retry_after: error.retryAfter } : {}) }, error.status as 400);
    }
    return context.json({ error: asErrorMessage(error) }, 500);
  });

  app.get("/healthz", async (context) => {
    const sources = await driver.listSources();
    const subscriptions = await driver.listSubscriptions();
    const warnings = new Set<string>();
    if (health.retentionPressure) warnings.add("retention_size_override");
    for (const failure of health.secretFailures) warnings.add(`secret_unresolved:${failure}`);
    for (const source of sources) {
      if (source.poll && (await driver.getPollState(source.id))?.failureCount) warnings.add(`poll_failures:${source.id}`);
      if ((source.reconcile?.failure_count ?? 0) > 0) warnings.add(`reconcile_failures:${source.id}`);
    }
    for (const subscription of subscriptions) if (subscription.push.failureCount > 0 || subscription.push.parked > 0) warnings.add(`push_failures:${subscription.id}`);
    return context.json({
      ok: warnings.size === 0,
      storage_driver: driver.capabilities.name,
      oldest_available_seq: await driver.oldestAvailableSeq(),
      newest_seq: await driver.newestSeq(),
      retention_pressure: health.retentionPressure,
      warnings: [...warnings].sort(),
    });
  });

  app.get("/openapi.json", (context) => context.json(OPENAPI_DOCUMENT));

  app.post("/ingest/:sourceId", async (context) => {
    const source = await driver.getSource(context.req.param("sourceId"));
    if (!source || source.kind !== "http") return context.json({ error: "source not found" }, 404);
    const encoding = context.req.header("content-encoding");
    if (encoding && encoding.toLowerCase() !== "identity") return context.json({ error: "only identity content-encoding is supported" }, 415);
    let body: Uint8Array;
    try {
      body = await readBodyCapped(context.req.raw, bodyCap);
    } catch {
      return context.json({ error: "payload too large" }, 413);
    }
    let verification;
    try {
      verification = await verifyRequest(source.verification, context.req.raw.headers, body, secrets, now());
      for (const reference of secretReferences(source.verification)) health.secretFailures.delete(reference);
    } catch (error) {
      if (error instanceof SecretReferenceError) health.secretFailures.add(error.reference);
      const status = "failure_status" in source.verification ? source.verification.failure_status ?? 401 : 401;
      return context.json({ accepted: false, error: "verification unavailable" }, status);
    }
    if (!verification.ok) return context.json({ accepted: false, error: "verification failed" }, verification.failureStatus);
    let mapped;
    try {
      const contentType = context.req.header("content-type");
      mapped = mapEnvelope({
        sourceId: source.id,
        mapping: source.mapping,
        headers: context.req.raw.headers,
        rawBody: body,
        ...(contentType ? { contentType } : {}),
        verificationMethod: verification.method,
        ...(verification.keyId ? { verificationKeyId: verification.keyId } : {}),
        now: now(),
      });
    } catch (error) {
      return context.json({ error: asErrorMessage(error) }, 422);
    }
    const committed = await driver.appendEvent(mapped);
    delivery.notifyNewEvent();
    if (options.onEventCommitted) await options.onEventCommitted(source.id, committed.event.seq);
    return context.json({ accepted: true, inserted: committed.inserted, seq: committed.event.seq, id: committed.event.id }, 200);
  });

  async function requireAdmin(context: Parameters<Parameters<typeof app.use>[1]>[0]) {
    const key = await authenticateAdmin(driver, bearerToken(context.req.header("authorization")), now());
    if (!key) return undefined;
    return key;
  }

  app.post("/sources", async (context) => {
    if (!(await requireAdmin(context))) return context.json({ error: "unauthorized" }, 401);
    try {
      const body = parseJsonObject(await context.req.json(), "source");
      assertNoInlineSecrets(body);
      if (typeof body.id !== "string" || !/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(body.id)) throw new Error("invalid source id");
      if (body.kind !== "http" && body.kind !== "poll") throw new Error("source kind must be http or poll");
      const verification = validateVerification(body.verification);
      const mapping = validateMapping(body.mapping);
      const references = [...secretReferences(verification)];
      if (isRecord(body.reconcile) && typeof body.reconcile.credential_ref === "string") references.push(body.reconcile.credential_ref);
      await validateSecretReferences(secrets, references);
      const source: Source = {
        id: body.id,
        kind: body.kind,
        verification,
        mapping,
        createdAt: now().toISOString(),
        ...(isRecord(body.poll) ? { poll: body.poll as unknown as NonNullable<Source["poll"]> } : {}),
        ...(isRecord(body.reconcile) ? { reconcile: body.reconcile as unknown as NonNullable<Source["reconcile"]> } : {}),
      };
      if (source.kind === "poll" && !source.poll) throw new Error("poll source requires poll config");
      await driver.putSource(source);
      if (options.onSourceChanged) await options.onSourceChanged(source);
      return context.json({ source }, 201);
    } catch (error) {
      return context.json({ error: asErrorMessage(error) }, 422);
    }
  });

  app.get("/sources", async (context) => {
    if (!(await requireAdmin(context))) return context.json({ error: "unauthorized" }, 401);
    return context.json({ sources: await driver.listSources() });
  });
  app.get("/sources/:id", async (context) => {
    if (!(await requireAdmin(context))) return context.json({ error: "unauthorized" }, 401);
    const source = await driver.getSource(context.req.param("id"));
    return source ? context.json({ source }) : context.json({ error: "not found" }, 404);
  });
  app.patch("/sources/:id", async (context) => {
    if (!(await requireAdmin(context))) return context.json({ error: "unauthorized" }, 401);
    const source = await driver.getSource(context.req.param("id"));
    if (!source) return context.json({ error: "not found" }, 404);
    try {
      const body = parseJsonObject(await context.req.json(), "source patch");
      assertNoInlineSecrets(body);
      if (body.kind !== undefined && body.kind !== "http" && body.kind !== "poll") throw new Error("source kind must be http or poll");
      if (body.kind) source.kind = body.kind;
      if (body.verification !== undefined) source.verification = validateVerification(body.verification);
      if (body.mapping !== undefined) source.mapping = validateMapping(body.mapping);
      if (body.poll === null) delete source.poll;
      else if (isRecord(body.poll)) source.poll = body.poll as unknown as NonNullable<Source["poll"]>;
      if (body.reconcile === null) delete source.reconcile;
      else if (isRecord(body.reconcile)) source.reconcile = body.reconcile as unknown as NonNullable<Source["reconcile"]>;
      if (source.kind === "poll" && !source.poll) throw new Error("poll source requires poll config");
      const references = [...secretReferences(source.verification)];
      if (source.reconcile?.credential_ref) references.push(source.reconcile.credential_ref);
      await validateSecretReferences(secrets, references);
      await driver.putSource(source);
      if (options.onSourceChanged) await options.onSourceChanged(source);
      return context.json({ source });
    } catch (error) {
      return context.json({ error: asErrorMessage(error) }, 422);
    }
  });
  app.delete("/sources/:id", async (context) => {
    if (!(await requireAdmin(context))) return context.json({ error: "unauthorized" }, 401);
    const id = context.req.param("id");
    if (!(await driver.deleteSource(id))) return context.json({ error: "not found" }, 404);
    if (options.onSourceDeleted) await options.onSourceDeleted(id);
    return context.body(null, 204);
  });

  app.post("/subscriptions", async (context) => {
    if (!(await requireAdmin(context))) return context.json({ error: "unauthorized" }, 401);
    try {
      const body = parseJsonObject(await context.req.json(), "subscription");
      assertNoInlineSecrets(body.transport);
      validateFilter(body.filter);
      const transport = validateTransport(body.transport ?? { type: "pull" });
      await validateSecretReferences(secrets, pushSecretReferences(transport));
      const minted = mintToken("sub");
      const subscription: Subscription = {
        id: typeof body.id === "string" ? body.id : newUlid(now().getTime()),
        ...(typeof body.name === "string" ? { name: body.name } : {}),
        filter: body.filter as Filter,
        transport,
        ...(typeof body.action === "string" ? { action: body.action } : {}),
        tokenHash: minted.hash,
        tokenPrefix: minted.prefix,
        cursor: 0,
        cursorEpoch: 0,
        push: { failureCount: 0, parked: 0 },
        createdAt: now().toISOString(),
        lastSeenAt: now().toISOString(),
      };
      if (!(await driver.createSubscription(subscription))) {
        return context.json({ error: "subscription id already exists" }, 409);
      }
      if (options.onSubscriberAuthChanged) await options.onSubscriberAuthChanged(subscription);
      return context.json({ subscription: publicSubscription(subscription), token: minted.token }, 201);
    } catch (error) {
      return context.json({ error: asErrorMessage(error) }, 422);
    }
  });

  app.get("/subscriptions", async (context) => {
    if (!(await requireAdmin(context))) return context.json({ error: "unauthorized" }, 401);
    return context.json({ subscriptions: (await driver.listSubscriptions()).map(publicSubscription) });
  });
  app.get("/subscriptions/:id", async (context) => {
    if (!(await requireAdmin(context))) return context.json({ error: "unauthorized" }, 401);
    const subscription = await driver.getSubscription(context.req.param("id"));
    return subscription ? context.json({ subscription: publicSubscription(subscription) }) : context.json({ error: "not found" }, 404);
  });
  app.patch("/subscriptions/:id", async (context) => {
    if (!(await requireAdmin(context))) return context.json({ error: "unauthorized" }, 401);
    const subscription = await driver.getSubscription(context.req.param("id"));
    if (!subscription) return context.json({ error: "not found" }, 404);
    try {
      const body = parseJsonObject(await context.req.json(), "subscription patch");
      if (body.filter !== undefined) {
        validateFilter(body.filter);
        subscription.filter = body.filter as Filter;
        delete subscription.push.scannedThroughSeq;
        delete subscription.push.scannedFilter;
      }
      if (body.transport !== undefined) {
        const transport = validateTransport(body.transport);
        assertNoInlineSecrets(body.transport);
        await validateSecretReferences(secrets, pushSecretReferences(transport));
        subscription.transport = transport;
        delete subscription.push.scannedThroughSeq;
        delete subscription.push.scannedFilter;
      }
      if (body.name === null) delete subscription.name;
      else if (typeof body.name === "string") subscription.name = body.name;
      if (body.action === null) delete subscription.action;
      else if (typeof body.action === "string") subscription.action = body.action;
      await driver.updateSubscription(subscription);
      return context.json({ subscription: publicSubscription(subscription) });
    } catch (error) {
      return context.json({ error: asErrorMessage(error) }, 422);
    }
  });
  app.delete("/subscriptions/:id", async (context) => {
    if (!(await requireAdmin(context))) return context.json({ error: "unauthorized" }, 401);
    const id = context.req.param("id");
    if (!(await driver.deleteSubscription(id))) return context.json({ error: "not found" }, 404);
    if (options.onSubscriberDeleted) await options.onSubscriberDeleted(id);
    return context.body(null, 204);
  });

  app.post("/subscriptions/:id/token/rotate", async (context) => {
    if (!(await requireAdmin(context))) return context.json({ error: "unauthorized" }, 401);
    const subscription = await driver.getSubscription(context.req.param("id"));
    if (!subscription) return context.json({ error: "not found" }, 404);
    const body = await context.req.json().catch(() => ({})) as Record<string, unknown>;
    const overlapSeconds = Math.max(0, Number(body.overlap_s ?? 86_400));
    const minted = mintToken("sub");
    subscription.oldTokenHash = subscription.tokenHash;
    subscription.oldTokenExpiresAt = new Date(now().getTime() + overlapSeconds * 1000).toISOString();
    subscription.tokenHash = minted.hash;
    subscription.tokenPrefix = minted.prefix;
    subscription.tokenRevoked = false;
    await driver.updateSubscription(subscription);
    if (options.onSubscriberAuthChanged) await options.onSubscriberAuthChanged(subscription);
    return context.json({ token: minted.token, old_token_expires_at: subscription.oldTokenExpiresAt });
  });
  app.delete("/subscriptions/:id/token", async (context) => {
    if (!(await requireAdmin(context))) return context.json({ error: "unauthorized" }, 401);
    const subscription = await driver.getSubscription(context.req.param("id"));
    if (!subscription) return context.json({ error: "not found" }, 404);
    subscription.tokenRevoked = true;
    delete subscription.oldTokenHash;
    delete subscription.oldTokenExpiresAt;
    await driver.updateSubscription(subscription);
    if (options.onSubscriberAuthChanged) await options.onSubscriberAuthChanged(subscription);
    return context.body(null, 204);
  });

  app.post("/subscriptions/:id/events", async (context) => {
    const token = bearerToken(context.req.header("authorization"));
    if (!(await authenticateSubscriber(driver, context.req.param("id"), token, now()))) return context.json({ error: "unauthorized" }, 401);
    const subscription = await driver.getSubscription(context.req.param("id"));
    const entitlement = options.eventEntitlement ? await options.eventEntitlement(context.req.raw, subscription) : "allow";
    const raw = await context.req.json().catch(() => ({}));
    const body = isRecord(raw) ? raw : {};
    const wait = body.wait === undefined ? undefined : parseDurationSeconds(body.wait);
    const ack = typeof body.ack === "number" ? body.ack : undefined;
    const ackEpoch = typeof body.ack_epoch === "number" ? body.ack_epoch : undefined;
    const limit = typeof body.limit === "number" ? body.limit : undefined;
    const payload = await delivery.poll(context.req.param("id"), {
      ...(wait !== undefined ? { wait } : {}),
      ...(ack !== undefined ? { ack } : {}),
      ...(ackEpoch !== undefined ? { ack_epoch: ackEpoch } : {}),
      ...(limit !== undefined ? { limit } : {}),
    }, entitlement);
    if ("events" in payload && payload.events.length > 0 && options.onEventsDelivered) {
      await options.onEventsDelivered(context.req.param("id"), payload.events.map((event) => event.seq), now().toISOString());
    }
    return context.json(payload);
  });

  app.get("/subscriptions/:id/stream", async (context) => {
    const token = bearerToken(context.req.header("authorization"));
    const id = context.req.param("id");
    if (!(await authenticateSubscriber(driver, id, token, now()))) return context.json({ error: "unauthorized" }, 401);
    const lastEventId = Number(context.req.header("last-event-id"));
    let afterSeq = Number.isSafeInteger(lastEventId) && lastEventId >= 0 ? lastEventId : undefined;
    return streamSSE(context, async (stream) => {
      const abort = new AbortController();
      stream.onAbort(() => abort.abort());
      const requestedLifetime = Number(context.req.header("x-bugler-stream-lifetime-ms"));
      const streamLifetime = Number.isFinite(requestedLifetime) && requestedLifetime > 0
        ? Math.min(sseMaxLifetimeMs, requestedLifetime)
        : sseMaxLifetimeMs;
      const expiresAt = Date.now() + streamLifetime;
      let gapSentTo: number | undefined;
      let cursorEpoch: number | undefined;
      const admissionId = context.req.header("x-bugler-stream-admission-id");
      while (!abort.signal.aborted && Date.now() < expiresAt) {
        if (options.subscriberStreamAllowed && !(await options.subscriberStreamAllowed(id, admissionId))) break;
        if (!(await authenticateSubscriber(driver, id, token, now()))) break;
        const observedGeneration = delivery.currentSignalGeneration();
        const subscription = await driver.getSubscription(id);
        const entitlement = options.eventEntitlement ? await options.eventEntitlement(context.req.raw, subscription) : "allow";
        const payload = await delivery.readForSse(id, afterSeq, 100, entitlement);
        if (cursorEpoch !== undefined && payload.cursor_epoch !== cursorEpoch) {
          afterSeq = undefined;
          gapSentTo = undefined;
          cursorEpoch = payload.cursor_epoch;
          continue;
        }
        cursorEpoch = payload.cursor_epoch;
        if ("gap" in payload) {
          if (gapSentTo !== payload.gap.to) {
            await stream.writeSSE({ event: "gap", id: String(payload.gap.to), data: JSON.stringify(payload) });
            gapSentTo = payload.gap.to;
          }
        } else if (payload.events.length > 0) {
          gapSentTo = undefined;
          for (const event of payload.events) {
            if (options.onEventsDelivered) await options.onEventsDelivered(id, [event.seq], now().toISOString());
            await stream.writeSSE({ event: "event", id: String(event.seq), data: JSON.stringify(event) });
            afterSeq = event.seq;
          }
          continue;
        }
        const remaining = expiresAt - Date.now();
        if (remaining <= 0) break;
        const reason = await delivery.waitForSignal(observedGeneration, Math.min(sseRecheckMs, remaining), abort.signal);
        if (reason === "aborted") break;
        if (reason === "timeout") await stream.write(": keepalive\n\n");
      }
    });
  });

  app.post("/subscriptions/:id/cursor", async (context) => {
    const id = context.req.param("id");
    const token = bearerToken(context.req.header("authorization"));
    const admin = await authenticateAdmin(driver, token, now());
    const subscriber = admin ? undefined : await authenticateSubscriber(driver, id, token, now());
    if (!admin && !subscriber) return context.json({ error: "unauthorized" }, 401);
    const body = parseJsonObject(await context.req.json(), "cursor");
    if (!Number.isSafeInteger(body.seq) || Number(body.seq) < 0) return context.json({ error: "seq must be a non-negative integer" }, 422);
    const current = await driver.getSubscription(id);
    if (!current) return context.json({ error: "not found" }, 404);
    if (admin && Number(body.seq) < current.cursor) {
      const rewound = await driver.rewindCursor(id, Number(body.seq));
      delivery.notifyCursorAdvanced();
      return context.json({ cursor: rewound!.cursor, epoch: rewound!.cursorEpoch });
    }
    if (!Number.isSafeInteger(body.epoch)) return context.json({ error: "epoch is required" }, 422);
    const result = await driver.advanceCursor(id, Number(body.seq), Number(body.epoch));
    if (result.status === "stale_epoch") return context.json({ error: "stale_epoch" }, 409);
    if (result.status === "beyond_newest") return context.json({ error: "beyond_newest" }, 422);
    if (result.status === "not_found") return context.json({ error: "not found" }, 404);
    await driver.refreshLiveness(id, now().toISOString(), 60 * 60 * 1000);
    delivery.notifyCursorAdvanced();
    if (result.status === "advanced" || result.status === "unchanged") {
      return context.json({ cursor: result.subscription.cursor, epoch: result.subscription.cursorEpoch, status: result.status });
    }
    return context.json({ error: "cursor update failed" }, 500);
  });

  app.get("/subscriptions/:id/push-status", async (context) => {
    if (!(await requireAdmin(context))) return context.json({ error: "unauthorized" }, 401);
    const subscription = await driver.getSubscription(context.req.param("id"));
    return subscription ? context.json({ push: subscription.push }) : context.json({ error: "not found" }, 404);
  });
  app.post("/subscriptions/:id/push/unpark", async (context) => {
    if (!(await requireAdmin(context))) return context.json({ error: "unauthorized" }, 401);
    const subscription = await pushEngine.unpark(context.req.param("id"));
    return subscription ? context.json({ push: subscription.push }) : context.json({ error: "not found" }, 404);
  });

  app.get("/events", async (context) => {
    const token = bearerToken(context.req.header("authorization"));
    const admin = await authenticateAdmin(driver, token, now());
    let subscriberFilter: Filter | undefined;
    if (!admin) {
      const subscription = await authenticateAnySubscriber(driver, token, now());
      if (!subscription) return context.json({ error: "unauthorized" }, 401);
      subscriberFilter = subscription.filter;
      await driver.refreshLiveness(subscription.id, now().toISOString(), 60 * 60 * 1000);
    }
    try {
      const source = context.req.query("source");
      const type = context.req.query("type");
      const subject = context.req.query("subject");
      const sinceSeq = numberParam(context.req.query("since_seq"), "since_seq");
      const since = context.req.query("since");
      const until = context.req.query("until");
      const eventSince = context.req.query("event_since");
      const eventUntil = context.req.query("event_until");
      const query: EventQuery = {
        limit: Math.max(1, Math.min(numberParam(context.req.query("limit"), "limit") ?? 100, 1000)),
        ...(source ? { source } : {}),
        ...(type ? { type } : {}),
        ...(subject ? { subject } : {}),
        ...(sinceSeq !== undefined ? { sinceSeq } : {}),
        ...(since ? { since } : {}),
        ...(until ? { until } : {}),
        ...(eventSince ? { eventSince } : {}),
        ...(eventUntil ? { eventUntil } : {}),
      };
      const entitlement = options.eventEntitlement ? await options.eventEntitlement(context.req.raw) : "allow";
      return context.json(await queryHistory(driver, query, subscriberFilter, entitlement));
    } catch (error) {
      return context.json({ error: asErrorMessage(error) }, 422);
    }
  });

  app.get("/admin-keys", async (context) => {
    if (!(await requireAdmin(context))) return context.json({ error: "unauthorized" }, 401);
    return context.json({ admin_keys: (await driver.listAdminKeys()).map(publicAdminKey) });
  });
  app.post("/admin-keys", async (context) => {
    if (!(await requireAdmin(context))) return context.json({ error: "unauthorized" }, 401);
    const body = await context.req.json().catch(() => ({})) as Record<string, unknown>;
    const minted = mintToken("adm");
    const createdAt = now().toISOString();
    const key = {
      id: newUlid(now().getTime()), keyHash: minted.hash, keyPrefix: minted.prefix, createdAt,
      ...(typeof body.expires_at === "string" ? { expiresAt: body.expires_at } : {}),
    };
    await driver.putAdminKey(key);
    if (options.onAdminAuthChanged) await options.onAdminAuthChanged(key);
    return context.json({ admin_key: publicAdminKey(key), token: minted.token }, 201);
  });
  app.patch("/admin-keys/:id", async (context) => {
    if (!(await requireAdmin(context))) return context.json({ error: "unauthorized" }, 401);
    const key = (await driver.listAdminKeys()).find((candidate) => candidate.id === context.req.param("id"));
    if (!key) return context.json({ error: "not found" }, 404);
    const body = parseJsonObject(await context.req.json(), "admin key patch");
    if (body.expires_at === null) delete key.expiresAt;
    else if (typeof body.expires_at === "string" && Number.isFinite(Date.parse(body.expires_at))) key.expiresAt = body.expires_at;
    else return context.json({ error: "expires_at must be an ISO timestamp or null" }, 422);
    await driver.putAdminKey(key);
    if (options.onAdminAuthChanged) await options.onAdminAuthChanged(key);
    return context.json({ admin_key: publicAdminKey(key) });
  });
  app.delete("/admin-keys/:id", async (context) => {
    const admin = await requireAdmin(context);
    if (!admin) return context.json({ error: "unauthorized" }, 401);
    if (admin.id === context.req.param("id") && (await driver.listAdminKeys()).filter((key) => !key.expiresAt || Date.parse(key.expiresAt) > now().getTime()).length === 1) {
      return context.json({ error: "cannot revoke the last active admin key" }, 409);
    }
    const id = context.req.param("id");
    if (!(await driver.deleteAdminKey(id))) return context.json({ error: "not found" }, 404);
    if (options.onAdminDeleted) await options.onAdminDeleted(id);
    return context.body(null, 204);
  });

  return { app, delivery, health };
}
