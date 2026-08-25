import type { StoredEvent, Subscription } from "./domain";
import { matchesFilter } from "./filter";
import type { StorageDriver } from "./storage/driver";

/** Hosted callers must choose explicitly. Direct seam omission is fail-closed; self-hosted routes pass allow. */
export type EventEntitlement = Subscription["filter"] | "allow" | "deny";

export interface DeliveryProfile {
  defaultWaitSeconds: number;
  minWaitSeconds: number;
  maxWaitSeconds: number;
  admissionCadenceSeconds: number;
  maxBatch: number;
  scanLimit: number;
}

export const BINARY_DELIVERY_PROFILE: DeliveryProfile = {
  defaultWaitSeconds: 45,
  minWaitSeconds: 0,
  maxWaitSeconds: 55,
  admissionCadenceSeconds: 1,
  maxBatch: 100,
  scanLimit: 10_000,
};

export const WORKERS_DELIVERY_PROFILE: DeliveryProfile = {
  defaultWaitSeconds: 240,
  minWaitSeconds: 0,
  maxWaitSeconds: 240,
  admissionCadenceSeconds: 240,
  maxBatch: 100,
  scanLimit: 1_000,
};

export type DeliveryPayload =
  | { events: StoredEvent[]; cursor_epoch: number; oldest_available_seq: number; retry_after?: number; superseded?: boolean }
  | { gap: { from: number; to: number }; cursor_epoch: number; oldest_available_seq: number; retry_after?: number };

export class DeliveryError extends Error {
  constructor(readonly code: "stale_epoch" | "beyond_newest" | "ack_epoch_required" | "not_found" | "rate_limited", readonly status: number, readonly retryAfter?: number) {
    super(code);
  }
}

interface Waiter {
  resolve: (reason: "event" | "timeout" | "superseded") => void;
  timer: ReturnType<typeof setTimeout>;
}

export class DeliveryCoordinator {
  private readonly waiters = new Map<string, Waiter>();
  private readonly streamWaiters = new Set<() => void>();
  private readonly readTails = new Map<string, Promise<void>>();
  private readonly admission = new Map<string, { nextAt: number; graceUsed: boolean }>();
  private signalGeneration = 0;

  constructor(
    private readonly driver: StorageDriver,
    private readonly profile: DeliveryProfile = BINARY_DELIVERY_PROFILE,
    private readonly now: () => Date = () => new Date(),
  ) {}

  notifyNewEvent(): void {
    this.broadcastSignal();
    for (const [id, waiter] of this.waiters) {
      clearTimeout(waiter.timer);
      this.waiters.delete(id);
      waiter.resolve("event");
    }
  }

  notifyCursorAdvanced(): void {
    this.broadcastSignal();
  }

  private broadcastSignal(): void {
    this.signalGeneration += 1;
    for (const resolve of this.streamWaiters) resolve();
    this.streamWaiters.clear();
  }

  currentSignalGeneration(): number {
    return this.signalGeneration;
  }

  waitForSignal(observedGeneration: number, timeoutMs: number, signal?: AbortSignal): Promise<"changed" | "timeout" | "aborted"> {
    if (this.signalGeneration !== observedGeneration) return Promise.resolve("changed");
    return new Promise((resolve) => {
      let finished = false;
      const finish = (reason: "changed" | "timeout" | "aborted") => {
        if (finished) return;
        finished = true;
        clearTimeout(timer);
        this.streamWaiters.delete(changed);
        signal?.removeEventListener("abort", aborted);
        resolve(reason);
      };
      const changed = () => finish("changed");
      const aborted = () => finish("aborted");
      const timer = setTimeout(() => finish("timeout"), timeoutMs);
      this.streamWaiters.add(changed);
      signal?.addEventListener("abort", aborted, { once: true });
      if (signal?.aborted) aborted();
      else if (this.signalGeneration !== observedGeneration) changed();
    });
  }

  private async serialized<T>(id: string, operation: () => Promise<T>): Promise<T> {
    const previous = this.readTails.get(id) ?? Promise.resolve();
    let release!: () => void;
    const current = new Promise<void>((resolve) => (release = resolve));
    const tail = previous.then(() => current);
    this.readTails.set(id, tail);
    await previous;
    try {
      return await operation();
    } finally {
      release();
      if (this.readTails.get(id) === tail) this.readTails.delete(id);
    }
  }

  private async read(subscription: Subscription, limit: number, afterSeq = subscription.cursor, entitlement: EventEntitlement = "deny"): Promise<DeliveryPayload> {
    const oldest = await this.driver.oldestAvailableSeq();
    if (oldest > subscription.cursor + 1) {
      return {
        gap: { from: subscription.cursor + 1, to: oldest - 1 },
        cursor_epoch: subscription.cursorEpoch,
        oldest_available_seq: oldest,
      };
    }
    if (entitlement === "deny") {
      return { events: [], cursor_epoch: subscription.cursorEpoch, oldest_available_seq: oldest };
    }
    const events: StoredEvent[] = [];
    let scanCursor = Math.max(subscription.cursor, afterSeq);
    for (let page = 0; page < 100 && events.length < limit; page += 1) {
      const scanned = await this.driver.queryEvents({ sinceSeq: scanCursor, limit: this.profile.scanLimit });
      events.push(...scanned.filter((event) => matchesFilter(event, subscription.filter)
        && (entitlement === "allow" || matchesFilter(event, entitlement))).slice(0, limit - events.length));
      if (scanned.length < this.profile.scanLimit) break;
      scanCursor = scanned[scanned.length - 1]!.seq;
    }
    return { events, cursor_epoch: subscription.cursorEpoch, oldest_available_seq: oldest };
  }

  private hasDelivery(payload: DeliveryPayload): boolean {
    return "gap" in payload || payload.events.length > 0;
  }

  private admissionDecision(subscriptionId: string): { allowed: boolean; retryAfter: number; grace: boolean } {
    const now = this.now().getTime();
    const current = this.admission.get(subscriptionId);
    const cadenceMs = this.profile.admissionCadenceSeconds * 1000;
    if (!current || now >= current.nextAt) {
      this.admission.set(subscriptionId, { nextAt: now + cadenceMs, graceUsed: false });
      return { allowed: true, retryAfter: this.profile.admissionCadenceSeconds, grace: false };
    }
    const retryAfter = Math.max(1, Math.ceil((current.nextAt - now) / 1000));
    if (!current.graceUsed) {
      current.graceUsed = true;
      return { allowed: true, retryAfter, grace: true };
    }
    return { allowed: false, retryAfter, grace: false };
  }

  private park(id: string, waitMs: number): Promise<"event" | "timeout" | "superseded"> {
    const existing = this.waiters.get(id);
    if (existing) {
      clearTimeout(existing.timer);
      existing.resolve("superseded");
    }
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        if (this.waiters.get(id)?.timer === timer) this.waiters.delete(id);
        resolve("timeout");
      }, waitMs);
      this.waiters.set(id, { resolve, timer });
    });
  }

  private supersedeParked(id: string): void {
    const existing = this.waiters.get(id);
    if (!existing) return;
    clearTimeout(existing.timer);
    this.waiters.delete(id);
    existing.resolve("superseded");
  }

  async poll(
    subscriptionId: string,
    body: { wait?: number; ack?: number; ack_epoch?: number; limit?: number },
    entitlement: EventEntitlement = "deny",
  ): Promise<DeliveryPayload> {
    const prepared = await this.serialized(subscriptionId, async () => {
      let subscription = await this.driver.getSubscription(subscriptionId);
      if (!subscription) throw new DeliveryError("not_found", 404);
      if (body.ack !== undefined) {
        if (body.ack_epoch === undefined) throw new DeliveryError("ack_epoch_required", 422);
        const updated = await this.driver.advanceCursor(subscriptionId, body.ack, body.ack_epoch);
        if (updated.status === "stale_epoch") throw new DeliveryError("stale_epoch", 409);
        if (updated.status === "beyond_newest") throw new DeliveryError("beyond_newest", 422);
        if (updated.status === "not_found") throw new DeliveryError("not_found", 404);
        if (updated.status === "advanced" || updated.status === "unchanged") subscription = updated.subscription;
      }
      await this.driver.refreshLiveness(subscriptionId, this.now().toISOString(), 60 * 60 * 1000);
      const limit = Math.max(1, Math.min(Math.trunc(body.limit ?? this.profile.maxBatch), this.profile.maxBatch));
      return { subscription, limit, payload: await this.read(subscription, limit, subscription.cursor, entitlement) };
    });
    let { subscription, limit, payload } = prepared;
    this.supersedeParked(subscriptionId);
    if (this.hasDelivery(payload)) return payload;
    const admission = this.admissionDecision(subscriptionId);
    if (!admission.allowed) throw new DeliveryError("rate_limited", 429, admission.retryAfter);
    if (admission.grace) return { ...payload, retry_after: admission.retryAfter };
    const waitSeconds = Math.max(this.profile.minWaitSeconds, Math.min(body.wait ?? this.profile.defaultWaitSeconds, this.profile.maxWaitSeconds));
    if (waitSeconds <= 0) return { ...payload, retry_after: admission.retryAfter };
    const reason = await this.park(subscriptionId, waitSeconds * 1000);
    if (reason === "superseded") return { ...payload, retry_after: admission.retryAfter, superseded: true };
    return this.serialized(subscriptionId, async () => {
      const refreshed = await this.driver.getSubscription(subscriptionId);
      if (!refreshed) throw new DeliveryError("not_found", 404);
      subscription = refreshed;
      payload = await this.read(subscription, limit, subscription.cursor, entitlement);
      return this.hasDelivery(payload) ? payload : { ...payload, retry_after: admission.retryAfter };
    });
  }

  async readForSse(subscriptionId: string, afterSeq?: number, limit = 100, entitlement: EventEntitlement = "deny"): Promise<DeliveryPayload> {
    return this.serialized(subscriptionId, async () => {
      const subscription = await this.driver.getSubscription(subscriptionId);
      if (!subscription) throw new DeliveryError("not_found", 404);
      await this.driver.refreshLiveness(subscriptionId, this.now().toISOString(), 60 * 60 * 1000);
      return this.read(subscription, Math.min(limit, this.profile.maxBatch), afterSeq ?? subscription.cursor, entitlement);
    });
  }
}
