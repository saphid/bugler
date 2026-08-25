import { createHmac, randomUUID } from "node:crypto";
import type { StoredEvent, Subscription, Transport } from "./domain";
import type { EventEntitlement } from "./delivery";
import { matchesFilter } from "./filter";
import type { SecretResolver } from "./secrets";
import type { StorageDriver } from "./storage/driver";
import { asErrorMessage } from "./util";

export interface PushRequestShape {
  id: string;
  headers: Headers;
  body: string;
}

export interface PushAttemptResult {
  status: "success" | "failed" | "parked" | "gap_parked" | "busy" | "empty";
  error?: string;
}

export function pushAttemptDue(
  subscription: Pick<Subscription, "transport" | "push" | "cursor" | "filter">,
  newestSeq: number,
  oldestAvailableSeq: number,
  now: string,
): boolean {
  if (subscription.transport.type !== "push" || subscription.push.parked !== 0) return false;
  if (subscription.push.nextAttemptAt) return subscription.push.nextAttemptAt <= now;
  if (oldestAvailableSeq > subscription.cursor + 1) return true;
  const scannedThroughSeq = subscription.push.scannedFilter === JSON.stringify(subscription.filter)
    ? subscription.push.scannedThroughSeq ?? subscription.cursor
    : subscription.cursor;
  return newestSeq > Math.max(subscription.cursor, scannedThroughSeq);
}

function standardSecret(secret: string): Buffer | string {
  return secret.startsWith("whsec_") ? Buffer.from(secret.slice(6), "base64") : secret;
}

function promptText(delivery: unknown): string {
  return `Bugler untrusted delivery envelope follows as JSON.\n${JSON.stringify(delivery)}`;
}

export async function buildPushRequest(
  subscription: Subscription,
  delivery: StoredEvent | { gap: { from: number; to: number }; cursor_epoch: number },
  id: string,
  resolver: SecretResolver,
  timestamp = Math.floor(Date.now() / 1000),
): Promise<PushRequestShape> {
  if (subscription.transport.type !== "push") throw new Error("subscription does not use push transport");
  const transport = subscription.transport;
  const adapter = transport.adapter ?? "generic";
  const body = adapter === "generic"
    ? JSON.stringify(delivery)
    : adapter === "routines"
      ? JSON.stringify({ text: promptText(delivery) })
      : JSON.stringify({ text: promptText(delivery), mode: transport.mode ?? "now" });
  const headers = new Headers({
    "content-type": "application/json",
    "webhook-id": id,
    "webhook-timestamp": String(timestamp),
  });
  if (adapter === "routines") {
    headers.set("anthropic-beta", "experimental-cc-routine-2026-04-01");
    headers.set("anthropic-version", "2023-06-01");
  }
  const signed = `${id}.${timestamp}.${body}`;
  const signatures: string[] = [];
  for (const reference of transport.whsec_refs) {
    const secret = await resolver.resolve(reference);
    const signature = createHmac("sha256", standardSecret(secret)).update(signed).digest("base64");
    signatures.push(`v1,${signature}`);
  }
  headers.set("webhook-signature", signatures.join(" "));
  const auth = transport.destination_auth ?? { type: "none" };
  if (auth.type === "bearer") headers.set("authorization", `Bearer ${await resolver.resolve(auth.secret_ref)}`);
  if (auth.type === "header") headers.set(auth.name, await resolver.resolve(auth.secret_ref));
  return { id, headers, body };
}

export function pushSecretReferences(transport: Transport): string[] {
  if (transport.type !== "push") return [];
  const references = [...transport.whsec_refs];
  const auth = transport.destination_auth;
  if (auth?.type === "bearer" || auth?.type === "header") references.push(auth.secret_ref);
  return references;
}

export interface PushEngineOptions {
  fetch?: (input: string | URL | Request, init?: RequestInit) => Promise<Response>;
  now?: () => Date;
  timeoutMs?: number;
  leaseMs?: number;
  onDelivered?: (subscriptionId: string, seq: number, at: string) => Promise<void>;
  eventEntitlement?: (subscription: Subscription) => Promise<EventEntitlement>;
}

export class PushEngine {
  private readonly fetcher: (input: string | URL | Request, init?: RequestInit) => Promise<Response>;
  private readonly now: () => Date;
  private readonly timeoutMs: number;
  private readonly leaseMs: number;
  private readonly onDelivered: PushEngineOptions["onDelivered"];
  private readonly eventEntitlement: PushEngineOptions["eventEntitlement"];

  constructor(
    private readonly driver: StorageDriver,
    private readonly resolver: SecretResolver,
    options: PushEngineOptions = {},
  ) {
    this.fetcher = options.fetch ?? fetch;
    this.now = options.now ?? (() => new Date());
    this.timeoutMs = options.timeoutMs ?? 30_000;
    this.leaseMs = options.leaseMs ?? 60_000;
    this.onDelivered = options.onDelivered;
    this.eventEntitlement = options.eventEntitlement;
  }

  private clearLease(subscription: Subscription): void {
    delete subscription.push.leaseToken;
    delete subscription.push.leaseExpiresAt;
  }

  async attempt(subscriptionId: string): Promise<PushAttemptResult> {
    const now = this.now();
    const initial = await this.driver.getSubscription(subscriptionId);
    if (!initial || initial.transport.type !== "push") return { status: "empty" };
    if (initial.push.parked !== 0) return { status: initial.push.parked === 2 ? "gap_parked" : "parked" };
    if (initial.push.nextAttemptAt && initial.push.nextAttemptAt > now.toISOString()) return { status: "empty" };
    const leaseToken = randomUUID();
    const subscription = await this.driver.claimPushLease(
      subscriptionId,
      leaseToken,
      new Date(now.getTime() + this.leaseMs).toISOString(),
      now.toISOString(),
    );
    if (!subscription) return { status: "busy" };
    if (subscription.transport.type !== "push") return { status: "empty" };
    const transport = subscription.transport;

    const oldest = await this.driver.oldestAvailableSeq();
    let delivery: StoredEvent | { gap: { from: number; to: number }; cursor_epoch: number };
    let id: string;
    let gapTo: number | undefined;
    if (oldest > subscription.cursor + 1) {
      gapTo = oldest - 1;
      delivery = { gap: { from: subscription.cursor + 1, to: gapTo }, cursor_epoch: subscription.cursorEpoch };
      id = `gap:${subscription.id}:${gapTo}`;
    } else {
      let event: StoredEvent | undefined;
      const entitlement = this.eventEntitlement ? await this.eventEntitlement(subscription) : "allow";
      if (entitlement === "deny") {
        this.clearLease(subscription);
        const committed = await this.driver.commitPushMutation({
          subscriptionId,
          expectedLeaseToken: leaseToken,
          expectedCursorEpoch: subscription.cursorEpoch,
          push: subscription.push,
        });
        return { status: committed.status === "committed" ? "empty" : "busy" };
      }
      let scanCursor = subscription.cursor;
      let fullyScanned = false;
      const newestAtScan = await this.driver.newestSeq();
      for (let page = 0; page < 100 && !event; page += 1) {
        const rows = await this.driver.queryEvents({ sinceSeq: scanCursor, untilSeq: newestAtScan, limit: 1_000 });
        event = rows.find((row) => matchesFilter(row, subscription.filter)
          && (entitlement === "allow" || matchesFilter(row, entitlement)));
        if (event) break;
        if (rows.length < 1_000) {
          fullyScanned = true;
          break;
        }
        scanCursor = rows[rows.length - 1]!.seq;
      }
      if (!event) {
        const scannedThroughSeq = fullyScanned ? newestAtScan : scanCursor;
        subscription.push.scannedThroughSeq = Math.max(subscription.push.scannedThroughSeq ?? 0, scannedThroughSeq);
        subscription.push.scannedFilter = JSON.stringify(subscription.filter);
        this.clearLease(subscription);
        const committed = await this.driver.commitPushMutation({
          subscriptionId,
          expectedLeaseToken: leaseToken,
          expectedCursorEpoch: subscription.cursorEpoch,
          push: subscription.push,
        });
        return { status: committed.status === "committed" ? "empty" : "busy" };
      }
      delivery = event;
      id = event.id;
    }

    try {
      const request = await buildPushRequest(subscription, delivery, id, this.resolver, Math.floor(now.getTime() / 1000));
      const response = await this.fetcher(transport.url, {
        method: "POST",
        headers: request.headers,
        body: request.body,
        redirect: "manual",
        signal: AbortSignal.timeout(this.timeoutMs),
      });
      if (response.status < 200 || response.status >= 300) throw new Error(`destination returned ${response.status}`);
      if (gapTo !== undefined) {
        subscription.push.parked = 2;
        subscription.push.gapTo = gapTo;
      } else {
        subscription.cursor = (delivery as StoredEvent).seq;
        subscription.push.parked = 0;
        delete subscription.push.gapTo;
      }
      subscription.push.failureCount = 0;
      delete subscription.push.failedSince;
      delete subscription.push.nextAttemptAt;
      delete subscription.push.lastError;
      this.clearLease(subscription);
      subscription.lastSeenAt = now.toISOString();
      const committed = await this.driver.commitPushMutation({
        subscriptionId,
        expectedLeaseToken: leaseToken,
        expectedCursorEpoch: subscription.cursorEpoch,
        push: subscription.push,
        ...(gapTo === undefined ? { cursor: (delivery as StoredEvent).seq } : {}),
        lastSeenAt: subscription.lastSeenAt,
      });
      if (committed.status === "committed" && gapTo === undefined && this.onDelivered) {
        await this.onDelivered(subscriptionId, (delivery as StoredEvent).seq, now.toISOString());
      }
      return committed.status === "committed" ? { status: gapTo === undefined ? "success" : "gap_parked" } : { status: "busy" };
    } catch (error) {
      const message = asErrorMessage(error);
      const failedSince = subscription.push.failedSince ?? now.toISOString();
      subscription.push.failedSince = failedSince;
      subscription.push.failureCount += 1;
      subscription.push.lastError = message;
      if (now.getTime() - Date.parse(failedSince) >= 24 * 60 * 60 * 1000) {
        subscription.push.parked = 1;
        delete subscription.push.nextAttemptAt;
      } else {
        const delaySeconds = Math.min(30 * 2 ** (subscription.push.failureCount - 1), 3600);
        subscription.push.nextAttemptAt = new Date(now.getTime() + delaySeconds * 1000).toISOString();
      }
      this.clearLease(subscription);
      const committed = await this.driver.commitPushMutation({
        subscriptionId,
        expectedLeaseToken: leaseToken,
        expectedCursorEpoch: subscription.cursorEpoch,
        push: subscription.push,
      });
      if (committed.status !== "committed") return { status: "busy" };
      return { status: subscription.push.parked === 1 ? "parked" : "failed", error: message };
    }
  }

  async unpark(subscriptionId: string): Promise<Subscription | undefined> {
    const subscription = await this.driver.getSubscription(subscriptionId);
    if (!subscription || subscription.transport.type !== "push") return undefined;
    if (subscription.push.parked !== 1) return subscription;
    subscription.push = { failureCount: 0, parked: 0 };
    await this.driver.updateSubscription(subscription);
    return subscription;
  }
}
