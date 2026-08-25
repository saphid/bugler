import type {
  AdminKey,
  AppendEventInput,
  AppendPollTransition,
  CursorUpdateResult,
  EventQuery,
  PollState,
  PushCommitInput,
  PushCommitResult,
  RetentionPolicy,
  RetentionResult,
  Source,
  StoredEvent,
  Subscription,
} from "../domain";
import { cloneValue, type StorageDriver } from "./driver";

export class MemoryStorageDriver implements StorageDriver {
  readonly capabilities = {
    name: "memory",
    maxPayloadBytes: 5 * 1024 * 1024,
    queryNotes: {
      seqRange: "ordered in-memory sequence scan",
      sourceSubject: "bounded in-memory predicate scan",
      sourceType: "bounded in-memory predicate scan",
      receivedTime: "bounded in-memory predicate scan followed by sequence order",
    },
    transactionNotes: "single serialized critical section",
    sizeMeasurementNotes: "UTF-8 JSON byte count of persisted semantic values",
  } as const;

  private events = new Map<number, StoredEvent>();
  private dedupe = new Map<string, number>();
  private sources = new Map<string, Source>();
  private subscriptions = new Map<string, Subscription>();
  private adminKeys = new Map<string, AdminKey>();
  private pollStates = new Map<string, PollState>();
  private nextSeq = 1;
  private writeTail: Promise<void> = Promise.resolve();

  close(): void {}

  private async write<T>(operation: () => T | Promise<T>): Promise<T> {
    const previous = this.writeTail;
    let release!: () => void;
    this.writeTail = new Promise<void>((resolve) => (release = resolve));
    await previous;
    try {
      return await operation();
    } finally {
      release();
    }
  }

  private appendUnlocked(input: AppendEventInput): { event: StoredEvent; inserted: boolean } {
    if (input.dedupekey !== undefined) {
      const existingSeq = this.dedupe.get(`${input.sourceId}\u0000${input.dedupekey}`);
      if (existingSeq !== undefined) {
        const existing = this.events.get(existingSeq);
        if (existing) return { event: cloneValue(existing), inserted: false };
      }
    }
    const event: StoredEvent = { ...cloneValue(input), seq: this.nextSeq++ };
    this.events.set(event.seq, event);
    if (event.dedupekey !== undefined) {
      this.dedupe.set(`${event.sourceId}\u0000${event.dedupekey}`, event.seq);
    }
    return { event: cloneValue(event), inserted: true };
  }

  appendEvent(input: AppendEventInput): Promise<{ event: StoredEvent; inserted: boolean }> {
    return this.write(() => this.appendUnlocked(input));
  }

  appendPollTransition(input: AppendPollTransition): Promise<{ event?: StoredEvent; committed: boolean }> {
    return this.write(() => {
      const current = this.pollStates.get(input.state.sourceId);
      if (!current || current.leaseToken !== input.expectedLeaseToken) return { committed: false };
      const appended = input.event ? this.appendUnlocked(input.event).event : undefined;
      this.pollStates.set(input.state.sourceId, cloneValue(input.state));
      return appended ? { event: appended, committed: true } : { committed: true };
    });
  }

  async newestSeq(): Promise<number> {
    return this.nextSeq - 1;
  }

  async oldestAvailableSeq(): Promise<number> {
    return this.events.size === 0 ? this.nextSeq : Math.min(...this.events.keys());
  }

  async queryEvents(query: EventQuery): Promise<StoredEvent[]> {
    const rows = [...this.events.values()].filter((event) => {
      if (query.source !== undefined && event.source !== query.source) return false;
      if (query.type !== undefined && event.type !== query.type) return false;
      if (query.subject !== undefined && event.subject !== query.subject) return false;
      if (query.sinceSeq !== undefined && event.seq <= query.sinceSeq) return false;
      if (query.untilSeq !== undefined && event.seq > query.untilSeq) return false;
      if (query.since !== undefined && event.receivedat < query.since) return false;
      if (query.until !== undefined && event.receivedat > query.until) return false;
      if (query.eventSince !== undefined && (event.time === undefined || event.time < query.eventSince)) return false;
      if (query.eventUntil !== undefined && (event.time === undefined || event.time > query.eventUntil)) return false;
      return true;
    });
    rows.sort((a, b) => a.seq - b.seq);
    return cloneValue(rows.slice(0, query.limit));
  }

  async getEventBySeq(seq: number): Promise<StoredEvent | undefined> {
    const event = this.events.get(seq);
    return event ? cloneValue(event) : undefined;
  }

  putSource(source: Source): Promise<void> {
    return this.write(() => this.sources.set(source.id, cloneValue(source))).then(() => undefined);
  }
  async getSource(id: string): Promise<Source | undefined> {
    const value = this.sources.get(id);
    return value ? cloneValue(value) : undefined;
  }
  async listSources(): Promise<Source[]> {
    return cloneValue([...this.sources.values()].sort((a, b) => a.id.localeCompare(b.id)));
  }
  deleteSource(id: string): Promise<boolean> {
    return this.write(() => this.sources.delete(id));
  }

  putSubscription(subscription: Subscription): Promise<void> {
    return this.write(() => this.subscriptions.set(subscription.id, cloneValue(subscription))).then(() => undefined);
  }
  createSubscription(subscription: Subscription): Promise<boolean> {
    return this.write(() => {
      if (this.subscriptions.has(subscription.id)) return false;
      this.subscriptions.set(subscription.id, cloneValue(subscription));
      return true;
    });
  }
  async getSubscription(id: string): Promise<Subscription | undefined> {
    const value = this.subscriptions.get(id);
    return value ? cloneValue(value) : undefined;
  }
  async listSubscriptions(): Promise<Subscription[]> {
    return cloneValue([...this.subscriptions.values()].sort((a, b) => a.id.localeCompare(b.id)));
  }
  deleteSubscription(id: string): Promise<boolean> {
    return this.write(() => this.subscriptions.delete(id));
  }
  updateSubscription(subscription: Subscription): Promise<void> {
    return this.putSubscription(subscription);
  }

  advanceCursor(id: string, seq: number, epoch: number): Promise<CursorUpdateResult> {
    return this.write(() => {
      const subscription = this.subscriptions.get(id);
      if (!subscription) return { status: "not_found" };
      if (subscription.cursorEpoch !== epoch) return { status: "stale_epoch" };
      if (seq > this.nextSeq - 1) return { status: "beyond_newest" };
      if (seq <= subscription.cursor) return { status: "unchanged", subscription: cloneValue(subscription) };
      subscription.cursor = seq;
      if (subscription.push.parked === 2 && subscription.push.gapTo !== undefined && seq >= subscription.push.gapTo) {
        subscription.push.parked = 0;
        delete subscription.push.gapTo;
        delete subscription.push.nextAttemptAt;
      }
      return { status: "advanced", subscription: cloneValue(subscription) };
    });
  }

  rewindCursor(id: string, seq: number): Promise<Subscription | undefined> {
    return this.write(() => {
      const subscription = this.subscriptions.get(id);
      if (!subscription) return undefined;
      subscription.cursor = Math.max(0, seq);
      subscription.cursorEpoch += 1;
      delete subscription.push.scannedThroughSeq;
      delete subscription.push.scannedFilter;
      return cloneValue(subscription);
    });
  }

  refreshLiveness(id: string, now: string, coalesceMs: number): Promise<void> {
    return this.write(() => {
      const subscription = this.subscriptions.get(id);
      if (!subscription) return;
      const previous = subscription.lastSeenAt ? Date.parse(subscription.lastSeenAt) : 0;
      if (!previous || Date.parse(now) - previous >= coalesceMs) subscription.lastSeenAt = now;
    });
  }

  claimPushLease(id: string, token: string, expiresAt: string, now: string): Promise<Subscription | undefined> {
    return this.write(() => {
      const subscription = this.subscriptions.get(id);
      if (!subscription) return undefined;
      if (subscription.push.leaseExpiresAt && subscription.push.leaseExpiresAt > now && subscription.push.leaseToken !== token) return undefined;
      subscription.push.leaseToken = token;
      subscription.push.leaseExpiresAt = expiresAt;
      return cloneValue(subscription);
    });
  }

  commitPushMutation(input: PushCommitInput): Promise<PushCommitResult> {
    return this.write(() => {
      const current = this.subscriptions.get(input.subscriptionId);
      if (!current || current.push.leaseToken !== input.expectedLeaseToken) return { status: "stale_lease" };
      if (current.cursorEpoch !== input.expectedCursorEpoch) {
        delete current.push.leaseToken;
        delete current.push.leaseExpiresAt;
        return { status: "stale_epoch" };
      }
      current.push = cloneValue(input.push);
      if (input.cursor !== undefined && input.cursor > current.cursor) current.cursor = input.cursor;
      if (input.lastSeenAt !== undefined) current.lastSeenAt = input.lastSeenAt;
      if (current.push.parked === 2 && current.push.gapTo !== undefined && current.cursor >= current.push.gapTo) {
        current.push.parked = 0;
        delete current.push.gapTo;
        delete current.push.nextAttemptAt;
      }
      return { status: "committed" };
    });
  }

  putAdminKey(key: AdminKey): Promise<void> {
    return this.write(() => this.adminKeys.set(key.id, cloneValue(key))).then(() => undefined);
  }
  async listAdminKeys(): Promise<AdminKey[]> {
    return cloneValue([...this.adminKeys.values()].sort((a, b) => a.createdAt.localeCompare(b.createdAt)));
  }
  deleteAdminKey(id: string): Promise<boolean> {
    return this.write(() => this.adminKeys.delete(id));
  }

  async getPollState(sourceId: string): Promise<PollState | undefined> {
    const state = this.pollStates.get(sourceId);
    return state ? cloneValue(state) : undefined;
  }
  putPollState(state: PollState): Promise<void> {
    return this.write(() => this.pollStates.set(state.sourceId, cloneValue(state))).then(() => undefined);
  }
  claimPollLease(sourceId: string, token: string, expiresAt: string, now: string): Promise<PollState | undefined> {
    return this.write(() => {
      const state = this.pollStates.get(sourceId) ?? { sourceId, generation: 0, failureCount: 0 };
      if (state.leaseExpiresAt && state.leaseExpiresAt > now && state.leaseToken !== token) return undefined;
      state.leaseToken = token;
      state.leaseExpiresAt = expiresAt;
      this.pollStates.set(sourceId, state);
      return cloneValue(state);
    });
  }

  async measureSizeBytes(): Promise<number> {
    return new TextEncoder().encode(
      JSON.stringify({
        events: [...this.events.values()],
        sources: [...this.sources.values()],
        subscriptions: [...this.subscriptions.values()],
        adminKeys: [...this.adminKeys.values()],
        pollStates: [...this.pollStates.values()],
      }),
    ).byteLength;
  }

  enforceRetention(policy: RetentionPolicy, now: string): Promise<RetentionResult> {
    return this.write(async () => {
      const nowMs = Date.parse(now);
      const liveCursors = [...this.subscriptions.values()]
        .filter((sub) => {
          if (!sub.lastSeenAt) return false;
          return nowMs - Date.parse(sub.lastSeenAt) <= (policy.pinningWindowMs ?? policy.maxAgeMs ?? 30 * 86_400_000);
        })
        .map((sub) => sub.cursor);
      const pinnedThrough = liveCursors.length ? Math.min(...liveCursors) : Number.POSITIVE_INFINITY;
      const ordered = () => [...this.events.values()].sort((a, b) => a.seq - b.seq);
      const remove = (event: StoredEvent) => {
        this.events.delete(event.seq);
        if (event.dedupekey !== undefined) this.dedupe.delete(`${event.sourceId}\u0000${event.dedupekey}`);
      };
      let deleted = 0;
      for (const event of ordered()) {
        const overAge = policy.maxAgeMs !== undefined && nowMs - Date.parse(event.receivedat) > policy.maxAgeMs;
        const overCount = policy.maxCount !== undefined && this.events.size > policy.maxCount;
        if ((overAge || overCount) && event.seq <= pinnedThrough) {
          remove(event);
          deleted += 1;
        }
      }
      let size = await this.measureSizeBytes();
      let sizeOverride = false;
      if (policy.maxBytes !== undefined && size > policy.maxBytes) {
        sizeOverride = true;
        for (const event of ordered()) {
          if (size <= policy.maxBytes) break;
          remove(event);
          deleted += 1;
          size = await this.measureSizeBytes();
        }
      }
      return {
        deleted,
        sizeOverride,
        oldestAvailableSeq: this.events.size ? Math.min(...this.events.keys()) : this.nextSeq,
        sizeBytes: size,
      };
    });
  }
}
