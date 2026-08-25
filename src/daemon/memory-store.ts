import { timingSafeEqual } from "node:crypto";
import { hashToken, hasValidLocalTokenChecksum, mintLocalToken } from "../auth";
import type { StoredEvent } from "../domain";
import { matchesFilter } from "../filter";
import { newUlid } from "../util";
import { LocalStoreError, type LocalStore } from "./store";
import type {
  DurableHostedAck,
  HostedFrame,
  LocalActionRecord,
  LocalActionAttempt,
  LocalConsumer,
  LocalConsumerCreate,
  LocalCursorResult,
  LocalDeadLetter,
  LocalDeliveryPayload,
  LocalHistoryResult,
  LocalMirror,
  LocalRetentionPolicy,
  LocalRetentionResult,
  LocalRecoveryNotice,
  LocalStoreOptions,
  LocalTokenRecord,
} from "./types";

type MirrorItem =
  | { kind: "event"; subscriptionId: string; epoch: number; seq: number; event: StoredEvent; createdAt: string }
  | { kind: "gap"; subscriptionId: string; epoch: number; from: number; to: number; createdAt: string };

type ConsumerEntry =
  | { kind: "event"; localSeq: number; mirrorKey: string; createdAt: string }
  | { kind: "gap"; localSeq: number; mirrorKey?: string; subscriptionId: string; hostedFrom: number; hostedTo: number; createdAt: string };

interface MemorySnapshot {
  mirrors: Map<string, LocalMirror>;
  mirrorItems: Map<string, MirrorItem>;
  consumers: Map<string, LocalConsumer>;
  entries: Map<string, Map<number, ConsumerEntry>>;
  tokens: Map<string, LocalTokenRecord>;
  actions: Map<string, LocalActionRecord>;
  deadLetters: Map<string, LocalDeadLetter>;
  actionAttempts: Map<string, LocalActionAttempt>;
  recoveryNotices: Map<string, LocalRecoveryNotice>;
  backfillScanned: Map<string, Set<string>>;
}

function cloneMap<T>(value: Map<string, T>): Map<string, T> {
  return structuredClone(value) as Map<string, T>;
}

function hashesEqual(left: string, right: string): boolean {
  if (left.length !== right.length) return false;
  return timingSafeEqual(Buffer.from(left), Buffer.from(right));
}

export class MemoryLocalStore implements LocalStore {
  private mirrors = new Map<string, LocalMirror>();
  private mirrorItems = new Map<string, MirrorItem>();
  private consumers = new Map<string, LocalConsumer>();
  private entries = new Map<string, Map<number, ConsumerEntry>>();
  private tokens = new Map<string, LocalTokenRecord>();
  private actions = new Map<string, LocalActionRecord>();
  private deadLetters = new Map<string, LocalDeadLetter>();
  private actionAttempts = new Map<string, LocalActionAttempt>();
  private recoveryNotices = new Map<string, LocalRecoveryNotice>();
  private backfillScanned = new Map<string, Set<string>>();
  private writeTail: Promise<void> = Promise.resolve();
  protected readonly now: () => Date;
  protected readonly fault?: LocalStoreOptions["fault"];

  constructor(options: LocalStoreOptions = {}) {
    this.now = options.now ?? (() => new Date());
    this.fault = options.fault;
  }

  close(): void {}

  private snapshot(): MemorySnapshot {
    return {
      mirrors: cloneMap(this.mirrors),
      mirrorItems: cloneMap(this.mirrorItems),
      consumers: cloneMap(this.consumers),
      entries: structuredClone(this.entries) as Map<string, Map<number, ConsumerEntry>>,
      tokens: cloneMap(this.tokens),
      actions: cloneMap(this.actions),
      deadLetters: cloneMap(this.deadLetters),
      actionAttempts: cloneMap(this.actionAttempts),
      recoveryNotices: cloneMap(this.recoveryNotices),
      backfillScanned: structuredClone(this.backfillScanned) as Map<string, Set<string>>,
    };
  }

  private restore(snapshot: MemorySnapshot): void {
    this.mirrors = snapshot.mirrors;
    this.mirrorItems = snapshot.mirrorItems;
    this.consumers = snapshot.consumers;
    this.entries = snapshot.entries;
    this.tokens = snapshot.tokens;
    this.actions = snapshot.actions;
    this.deadLetters = snapshot.deadLetters;
    this.actionAttempts = snapshot.actionAttempts;
    this.recoveryNotices = snapshot.recoveryNotices;
    this.backfillScanned = snapshot.backfillScanned;
  }

  private async write<T>(operation: () => Promise<T> | T): Promise<T> {
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

  async createMirror(id: string, cursor = 0, epoch = 0): Promise<LocalMirror> {
    return this.write(() => {
      const existing = this.mirrors.get(id);
      if (existing) return structuredClone(existing);
      const mirror: LocalMirror = { id, hostedCursor: cursor, cursorEpoch: epoch, receivedCursor: cursor, state: "caught_up" };
      this.mirrors.set(id, mirror);
      return structuredClone(mirror);
    });
  }

  async listMirrors(): Promise<LocalMirror[]> {
    return [...this.mirrors.values()].sort((a, b) => a.id.localeCompare(b.id)).map((item) => structuredClone(item));
  }

  async createConsumer(input: LocalConsumerCreate): Promise<{ consumer: LocalConsumer; token: string }> {
    return this.write(() => {
      if (this.consumers.has(input.id)) throw new LocalStoreError("already_exists");
      for (const mirror of input.mirrors) if (!this.mirrors.has(mirror)) throw new LocalStoreError("not_found");
      const consumer: LocalConsumer = {
        id: input.id,
        mirrors: [...new Set(input.mirrors)],
        ...(input.filter ? { filter: structuredClone(input.filter) } : {}),
        cursor: 0,
        cursorEpoch: 0,
        nextLocalSeq: 1,
        lastSeenAt: this.now().toISOString(),
        state: input.start === "oldest" ? "backfilling" : "ready",
        kind: "agent",
      };
      this.consumers.set(input.id, consumer);
      this.entries.set(input.id, new Map());
      this.backfillScanned.set(input.id, new Set());
      const recovered = consumer.mirrors.filter((id) => this.recoveryNotices.has(id));
      if (recovered.length > 0) {
        this.entries.get(input.id)!.set(1, { kind: "gap", localSeq: 1, subscriptionId: recovered[0]!, hostedFrom: 0, hostedTo: 0, createdAt: this.now().toISOString() });
        consumer.nextLocalSeq = 2;
      }
      if (input.start === "oldest") {
        consumer.mapped = 0;
        consumer.totalEstimate = [...this.mirrorItems.values()]
          .filter((item) => consumer.mirrors.includes(item.subscriptionId)).length;
      }
      const minted = mintLocalToken("lcon");
      this.tokens.set(`consume:${input.id}`, {
        id: `consume:${input.id}`,
        consumerId: input.id,
        kind: "consume",
        currentHash: minted.hash,
      });
      return { consumer: structuredClone(consumer), token: minted.token };
    });
  }

  async advanceBackfills(limit = 1_000): Promise<number> {
    return this.write(() => {
      for (const consumer of this.consumers.values()) {
        if (consumer.state !== "backfilling") continue;
        const target = this.entries.get(consumer.id)!;
        const mappedKeys = this.backfillScanned.get(consumer.id) ?? new Set<string>();
        this.backfillScanned.set(consumer.id, mappedKeys);
        const pending = [...this.mirrorItems.entries()]
          .filter(([key, item]) => consumer.mirrors.includes(item.subscriptionId) && !mappedKeys.has(key))
          .sort(([, left], [, right]) => left.subscriptionId.localeCompare(right.subscriptionId)
            || left.epoch - right.epoch
            || (left.kind === "event" ? left.seq : left.from) - (right.kind === "event" ? right.seq : right.from)
            || (left.kind === "gap" ? 0 : 1));
        for (const [key, item] of pending.slice(0, Math.max(1, Math.min(limit, 1_000)))) {
          this.mapItem(consumer, key, item);
          mappedKeys.add(key);
        }
        consumer.mapped = consumer.nextLocalSeq - 1;
        if (pending.length <= limit) consumer.state = "ready";
      }
      return [...this.consumers.values()].filter((consumer) => consumer.state === "backfilling").length;
    });
  }

  async getConsumer(id: string): Promise<LocalConsumer | undefined> {
    const value = this.consumers.get(id);
    return value ? structuredClone(value) : undefined;
  }

  async listConsumers(): Promise<LocalConsumer[]> {
    return [...this.consumers.values()].sort((a, b) => a.id.localeCompare(b.id)).map((value) => structuredClone(value));
  }

  private mirrorKey(subscriptionId: string, epoch: number, seq: number): string {
    return `${subscriptionId}\u0000${epoch}\u0000${seq}`;
  }

  private gapKey(subscriptionId: string, epoch: number, from: number, to: number): string {
    return `${subscriptionId}\u0000${epoch}\u0000gap\u0000${from}\u0000${to}`;
  }

  private mapItem(consumer: LocalConsumer, key: string, item: MirrorItem): void {
    const target = this.entries.get(consumer.id)!;
    if ([...target.values()].some((entry) => entry.mirrorKey === key)) return;
    if (item.kind === "event" && consumer.filter && !matchesFilter(item.event, consumer.filter)) return;
    const localSeq = consumer.nextLocalSeq++;
    if (item.kind === "event") target.set(localSeq, { kind: "event", localSeq, mirrorKey: key, createdAt: item.createdAt });
    else target.set(localSeq, {
      kind: "gap",
      mirrorKey: key,
      localSeq,
      subscriptionId: item.subscriptionId,
      hostedFrom: item.from,
      hostedTo: item.to,
      createdAt: item.createdAt,
    });
  }

  async commitHostedFrame(frame: HostedFrame): Promise<DurableHostedAck> {
    return this.write(async () => {
      const snapshot = this.snapshot();
      let committed = false;
      try {
        const mirror = this.mirrors.get(frame.subscriptionId);
        if (!mirror) throw new LocalStoreError("not_found");
        if (frame.cursorEpoch < mirror.cursorEpoch) {
          mirror.state = "epoch_regression";
          throw new LocalStoreError("epoch_regression");
        }
        if (frame.cursorEpoch > mirror.cursorEpoch) {
          mirror.cursorEpoch = frame.cursorEpoch;
          mirror.receivedCursor = 0;
        }
        const expected = mirror.receivedCursor + 1;
        const createdAt = this.now().toISOString();
        let acknowledged = mirror.receivedCursor;
        const inserted: Array<[string, MirrorItem]> = [];
        if (frame.kind === "events") {
          if (frame.events.length === 0) return { subscriptionId: frame.subscriptionId, seq: acknowledged, epoch: mirror.cursorEpoch };
          const ordered = [...frame.events].sort((a, b) => a.seq - b.seq);
          if (ordered[0]!.seq > expected) throw new LocalStoreError("sequence_gap");
          for (let index = 1; index < ordered.length; index += 1) {
            if (ordered[index]!.seq !== ordered[index - 1]!.seq + 1) throw new LocalStoreError("sequence_gap");
          }
          for (const event of ordered) {
            const key = this.mirrorKey(frame.subscriptionId, frame.cursorEpoch, event.seq);
            const existing = this.mirrorItems.get(key);
            if (existing) {
              if (existing.kind !== "event" || JSON.stringify(existing.event) !== JSON.stringify(event)) throw new LocalStoreError("corrupt");
            } else if (event.seq >= expected) {
              const item: MirrorItem = { kind: "event", subscriptionId: frame.subscriptionId, epoch: frame.cursorEpoch, seq: event.seq, event: structuredClone(event), createdAt };
              this.mirrorItems.set(key, item);
              inserted.push([key, item]);
            }
            acknowledged = Math.max(acknowledged, event.seq);
          }
        } else {
          if (frame.gap.to < expected) return { subscriptionId: frame.subscriptionId, seq: mirror.receivedCursor, epoch: mirror.cursorEpoch };
          if (frame.gap.from > expected || frame.gap.to < frame.gap.from) throw new LocalStoreError("sequence_gap");
          const key = this.gapKey(frame.subscriptionId, frame.cursorEpoch, frame.gap.from, frame.gap.to);
          let item = this.mirrorItems.get(key);
          if (!item) {
            item = { kind: "gap", subscriptionId: frame.subscriptionId, epoch: frame.cursorEpoch, from: frame.gap.from, to: frame.gap.to, createdAt };
            this.mirrorItems.set(key, item);
            inserted.push([key, item]);
          }
          acknowledged = frame.gap.to;
        }
        await this.fault?.("after_events");
        for (const consumer of this.consumers.values()) {
          if (!consumer.mirrors.includes(frame.subscriptionId)) continue;
          for (const [key, item] of inserted) this.mapItem(consumer, key, item);
        }
        await this.fault?.("after_mappings");
        mirror.receivedCursor = Math.max(mirror.receivedCursor, acknowledged);
        mirror.hostedCursor = Math.max(mirror.hostedCursor, acknowledged);
        mirror.state = "caught_up";
        committed = true;
        await this.fault?.("after_commit");
        return { subscriptionId: frame.subscriptionId, seq: acknowledged, epoch: mirror.cursorEpoch };
      } catch (error) {
        if (!committed) {
          this.restore(snapshot);
          if (error instanceof LocalStoreError && error.code === "epoch_regression") {
            const mirror = this.mirrors.get(frame.subscriptionId);
            if (mirror) mirror.state = "epoch_regression";
          }
        }
        throw error;
      }
    });
  }

  private localEvent(entry: Extract<ConsumerEntry, { kind: "event" }>): StoredEvent {
    const item = this.mirrorItems.get(entry.mirrorKey);
    if (!item || item.kind !== "event") throw new LocalStoreError("corrupt");
    return { ...structuredClone(item.event), seq: entry.localSeq };
  }

  async readConsumer(id: string, limit = 100): Promise<LocalDeliveryPayload> {
    const consumer = this.consumers.get(id);
    if (!consumer) throw new LocalStoreError("not_found");
    if (consumer.state !== "ready") throw new LocalStoreError("backfilling");
    const entries = this.entries.get(id)!;
    const ordered = [...entries.values()].sort((a, b) => a.localSeq - b.localSeq);
    const oldest = ordered[0]?.localSeq ?? consumer.nextLocalSeq;
    if (oldest > consumer.cursor + 1) {
      return { gap: { from: consumer.cursor + 1, to: oldest - 1 }, cursor_epoch: consumer.cursorEpoch, oldest_available_seq: oldest };
    }
    const next = ordered.find((entry) => entry.localSeq > consumer.cursor);
    if (next?.kind === "gap") {
      return { gap: { from: next.localSeq, to: next.localSeq }, cursor_epoch: consumer.cursorEpoch, oldest_available_seq: oldest };
    }
    const nextGap = ordered.find((entry) => entry.kind === "gap" && entry.localSeq > consumer.cursor)?.localSeq ?? Number.POSITIVE_INFINITY;
    const events = ordered
      .filter((entry): entry is Extract<ConsumerEntry, { kind: "event" }> => entry.kind === "event" && entry.localSeq > consumer.cursor && entry.localSeq < nextGap)
      .slice(0, Math.max(1, Math.min(limit, 100)))
      .map((entry) => this.localEvent(entry));
    return { events, cursor_epoch: consumer.cursorEpoch, oldest_available_seq: oldest };
  }

  async acknowledgeConsumer(id: string, seq: number, epoch: number): Promise<LocalCursorResult> {
    return this.write(() => {
      const consumer = this.consumers.get(id);
      if (!consumer) return { status: "not_found" };
      if (epoch !== consumer.cursorEpoch) return { status: "stale_epoch" };
      if (seq > consumer.nextLocalSeq - 1) return { status: "beyond_newest" };
      if (seq <= consumer.cursor) return { status: "unchanged", cursor: consumer.cursor, epoch: consumer.cursorEpoch };
      consumer.cursor = seq;
      consumer.lastSeenAt = this.now().toISOString();
      return { status: "advanced", cursor: consumer.cursor, epoch: consumer.cursorEpoch };
    });
  }

  async rewindConsumer(id: string, seq: number): Promise<{ cursor: number; epoch: number } | undefined> {
    return this.write(() => {
      const consumer = this.consumers.get(id);
      if (!consumer) return undefined;
      consumer.cursor = Math.max(0, Math.min(seq, consumer.nextLocalSeq - 1));
      consumer.cursorEpoch += 1;
      return { cursor: consumer.cursor, epoch: consumer.cursorEpoch };
    });
  }

  async history(id: string, query: { source?: string; type?: string; subject?: string; sinceSeq?: number; limit?: number } = {}): Promise<LocalHistoryResult> {
    const consumer = this.consumers.get(id);
    if (!consumer) throw new LocalStoreError("not_found");
    const entries = [...this.entries.get(id)!.values()].sort((a, b) => a.localSeq - b.localSeq);
    const oldest = entries[0]?.localSeq ?? consumer.nextLocalSeq;
    const limit = Math.max(1, Math.min(query.limit ?? 100, 1_000));
    const matching = entries
      .filter((entry): entry is Extract<ConsumerEntry, { kind: "event" }> => entry.kind === "event" && entry.localSeq > (query.sinceSeq ?? 0))
      .map((entry) => this.localEvent(entry))
      .filter((event) => (!query.source || event.source === query.source) && (!query.type || event.type === query.type) && (!query.subject || event.subject === query.subject));
    const events = matching.slice(0, limit);
    return {
      events,
      oldest_available_seq: oldest,
      ...(matching.length > events.length && events.length ? { next_since_seq: events.at(-1)!.seq } : {}),
    };
  }

  async mintManagementToken(): Promise<string> {
    return this.write(() => {
      const minted = mintLocalToken("lmg");
      this.tokens.set("management", { id: "management", kind: "management", currentHash: minted.hash });
      return minted.token;
    });
  }

  async authenticateManagement(token: string): Promise<boolean> {
    if (!hasValidLocalTokenChecksum(token, "lmg")) return false;
    const record = this.tokens.get("management");
    return Boolean(record && !record.revokedAt && hashesEqual(record.currentHash, hashToken(token)));
  }

  async authenticateConsumer(id: string, token: string): Promise<boolean> {
    if (!hasValidLocalTokenChecksum(token, "lcon")) return false;
    const record = this.tokens.get(`consume:${id}`);
    if (!record || record.revokedAt) return false;
    const hash = hashToken(token);
    if (hashesEqual(record.currentHash, hash)) return true;
    return Boolean(record.nextHash && record.overlapUntil && Date.parse(record.overlapUntil) > this.now().getTime() && hashesEqual(record.nextHash, hash));
  }

  async rotateConsumerToken(id: string, overlapSeconds: number): Promise<string | undefined> {
    return this.write(() => {
      const record = this.tokens.get(`consume:${id}`);
      if (!record) return undefined;
      const minted = mintLocalToken("lcon");
      record.nextHash = record.currentHash;
      record.overlapUntil = new Date(this.now().getTime() + Math.max(0, overlapSeconds) * 1_000).toISOString();
      record.currentHash = minted.hash;
      delete record.revokedAt;
      return minted.token;
    });
  }

  async revokeConsumerToken(id: string): Promise<boolean> {
    return this.write(() => {
      const record = this.tokens.get(`consume:${id}`);
      if (!record) return false;
      record.revokedAt = this.now().toISOString();
      delete record.nextHash;
      delete record.overlapUntil;
      return true;
    });
  }

  async applyRetention(policy: LocalRetentionPolicy): Promise<LocalRetentionResult> {
    return this.write(() => {
      let deleted = 0;
      const now = this.now().getTime();
      const ageCutoff = policy.maxAgeMs === undefined ? undefined : now - policy.maxAgeMs;
      const pinCutoff = policy.pinningWindowMs === undefined ? undefined : now - policy.pinningWindowMs;
      const isPinned = (consumer: LocalConsumer) => {
        if (pinCutoff === undefined) return false;
        if (consumer.kind === "action") {
          const action = this.actions.get(consumer.id.replace(/^action:/, ""));
          if (action?.enabled) return true;
        }
        return Date.parse(consumer.lastSeenAt) >= pinCutoff;
      };
      const sliceLimit = 1_000;
      for (const [consumerId, entries] of this.entries) {
        const consumer = this.consumers.get(consumerId)!;
        if (ageCutoff !== undefined) {
          for (const entry of [...entries.values()].sort((left, right) => left.localSeq - right.localSeq)) {
            if (Date.parse(entry.createdAt) >= ageCutoff) continue;
            if (entry.localSeq > consumer.cursor && isPinned(consumer)) continue;
            if (entries.delete(entry.localSeq)) deleted += 1;
            if (deleted >= sliceLimit) break;
          }
          if (deleted >= sliceLimit) break;
        }
      }
      const maximum = policy.maxCount;
      if (maximum !== undefined) {
        const events = [...this.mirrorItems.entries()]
          .filter(([, item]) => item.kind === "event")
          .sort(([, left], [, right]) => Date.parse(left.createdAt) - Date.parse(right.createdAt)
            || left.subscriptionId.localeCompare(right.subscriptionId)
            || left.epoch - right.epoch
            || (left.kind === "event" ? left.seq : 0) - (right.kind === "event" ? right.seq : 0));
        let excess = Math.max(0, events.length - maximum);
        for (const [key] of events) {
          if (excess <= 0 || deleted >= sliceLimit) break;
          const pinned = [...this.entries.entries()].some(([consumerId, entries]) => {
            const consumer = this.consumers.get(consumerId)!;
            return isPinned(consumer) && [...entries.values()].some((entry) => entry.kind === "event" && entry.mirrorKey === key && entry.localSeq > consumer.cursor);
          });
          if (pinned) continue;
          for (const entries of this.entries.values()) {
            for (const entry of entries.values()) {
              if (entry.kind === "event" && entry.mirrorKey === key && entries.delete(entry.localSeq)) deleted += 1;
            }
          }
          this.mirrorItems.delete(key);
          excess -= 1;
        }
      }
      const bytesForEntry = (entry: ConsumerEntry) => {
        if (entry.kind === "gap") return 64;
        const item = this.mirrorItems.get(entry.mirrorKey);
        return item?.kind === "event" ? Buffer.byteLength(JSON.stringify(item.event)) : 0;
      };
      let measuredBytes = [...this.entries.values()].reduce((total, entries) => total + [...entries.values()].reduce((sum, entry) => sum + bytesForEntry(entry), 0), 0)
        + [...this.deadLetters.values()].reduce((total, dead) => total + Buffer.byteLength(JSON.stringify(dead)), 0);
      const sizeOverride = policy.maxBytes !== undefined && measuredBytes > policy.maxBytes;
      if (sizeOverride) {
        const candidates = [...this.entries.entries()].flatMap(([consumerId, entries]) => [...entries.values()].map((entry) => ({ consumerId, entry })))
          .sort((left, right) => Date.parse(left.entry.createdAt) - Date.parse(right.entry.createdAt) || left.entry.localSeq - right.entry.localSeq);
        for (const candidate of candidates) {
          if (measuredBytes <= policy.maxBytes! || deleted >= sliceLimit) break;
          if (this.entries.get(candidate.consumerId)!.delete(candidate.entry.localSeq)) {
            measuredBytes -= bytesForEntry(candidate.entry);
            deleted += 1;
          }
        }
      }
      if (policy.deadLetterMaxAgeMs !== undefined) {
        const cutoff = now - policy.deadLetterMaxAgeMs;
        for (const [id, dead] of this.deadLetters) if (Date.parse(dead.createdAt) < cutoff) this.deadLetters.delete(id);
      }
      const referenced = new Set([...this.entries.values()].flatMap((entries) => [...entries.values()].flatMap((entry) => entry.kind === "event" ? [entry.mirrorKey] : [])));
      for (const [key, item] of this.mirrorItems) {
        if (!referenced.has(key) && (sizeOverride || (ageCutoff !== undefined && Date.parse(item.createdAt) < ageCutoff))) this.mirrorItems.delete(key);
      }
      return { deleted, sizeOverride };
    });
  }

  putAction(action: LocalActionRecord): Promise<void> {
    return this.write(() => this.actions.set(action.id, structuredClone(action))).then(() => undefined);
  }

  async listActions(): Promise<LocalActionRecord[]> {
    return [...this.actions.values()].map((value) => structuredClone(value));
  }

  async getAction(id: string): Promise<LocalActionRecord | undefined> {
    const value = this.actions.get(id);
    return value ? structuredClone(value) : undefined;
  }

  deleteAction(id: string): Promise<boolean> {
    return this.write(() => this.actions.delete(id));
  }

  putActionAttempt(attempt: LocalActionAttempt): Promise<void> {
    const key = `${attempt.actionId}\u0000${attempt.localSeq}\u0000${attempt.replayGeneration}\u0000${attempt.attempt}`;
    return this.write(() => this.actionAttempts.set(key, structuredClone(attempt))).then(() => undefined);
  }

  async listActionAttempts(actionId: string, localSeq: number): Promise<LocalActionAttempt[]> {
    return [...this.actionAttempts.values()]
      .filter((attempt) => attempt.actionId === actionId && attempt.localSeq === localSeq)
      .sort((a, b) => a.replayGeneration - b.replayGeneration || a.attempt - b.attempt)
      .map((attempt) => structuredClone(attempt));
  }

  async listRecentActionAttempts(actionId: string, since: string): Promise<LocalActionAttempt[]> {
    const cutoff = Date.parse(since);
    return [...this.actionAttempts.values()]
      .filter((attempt) => attempt.actionId === actionId && Date.parse(attempt.startedAt ?? attempt.dueAt) >= cutoff)
      .sort((left, right) => Date.parse(left.startedAt ?? left.dueAt) - Date.parse(right.startedAt ?? right.dueAt))
      .map((attempt) => structuredClone(attempt));
  }

  putDeadLetter(deadLetter: LocalDeadLetter): Promise<void> {
    return this.write(() => this.deadLetters.set(deadLetter.id || newUlid(), structuredClone(deadLetter))).then(() => undefined);
  }

  async getDeadLetter(id: string): Promise<LocalDeadLetter | undefined> {
    const value = this.deadLetters.get(id);
    return value ? structuredClone(value) : undefined;
  }

  async listDeadLetters(): Promise<LocalDeadLetter[]> {
    return [...this.deadLetters.values()].map((value) => structuredClone(value));
  }

  markDeadLetterReplayed(id: string, at: string): Promise<boolean> {
    return this.write(() => {
      const deadLetter = this.deadLetters.get(id);
      if (!deadLetter) return false;
      deadLetter.replayedAt = at;
      return true;
    });
  }

  putRecoveryNotice(notice: LocalRecoveryNotice): Promise<void> {
    return this.write(() => this.recoveryNotices.set(notice.subscriptionId, structuredClone(notice))).then(() => undefined);
  }

  async listRecoveryNotices(): Promise<LocalRecoveryNotice[]> {
    return [...this.recoveryNotices.values()].map((notice) => structuredClone(notice));
  }

  async setConsumerFilter(id: string, filter: import("../domain").Filter | undefined): Promise<void> {
    await this.write(() => {
      const consumer = this.consumers.get(id);
      if (!consumer) throw new LocalStoreError("not_found");
      if (filter) consumer.filter = structuredClone(filter);
      else delete consumer.filter;
    });
  }

  async debugEventCount(): Promise<number> {
    return [...this.mirrorItems.values()].filter((item) => item.kind === "event").length;
  }

  async debugMappingCount(consumerId: string): Promise<number> {
    return this.entries.get(consumerId)?.size ?? 0;
  }

  async debugEvent(localConsumerId: string, localSeq: number): Promise<StoredEvent | undefined> {
    const entry = this.entries.get(localConsumerId)?.get(localSeq);
    return entry?.kind === "event" ? this.localEvent(entry) : undefined;
  }
}
