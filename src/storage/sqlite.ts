import { Database } from "bun:sqlite";
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
import type { StorageDriver } from "./driver";

type Row = Record<string, unknown>;

export const SQLITE_RETENTION_AGE_DELETE_SQL =
  "DELETE FROM events INDEXED BY events_received WHERE received_at < ? AND seq <= ?";

export class SqliteStorageDriver implements StorageDriver {
  readonly capabilities = {
    name: "bun:sqlite",
    maxPayloadBytes: 5 * 1024 * 1024,
    queryNotes: {
      seqRange: "events primary key range",
      sourceSubject: "events_source_subject composite index",
      sourceType: "events_source_type composite index",
      receivedTime: "events_received index then sequence ordering",
    },
    transactionNotes: "SQLite transaction with WAL and synchronous=FULL",
    sizeMeasurementNotes: "page_count multiplied by page_size, including free pages until incremental vacuum",
  } as const;

  readonly db: Database;

  constructor(path: string) {
    this.db = new Database(path, { create: true, strict: true });
    const hasTables = Number((this.db.query("SELECT count(*) AS count FROM sqlite_master WHERE type='table'").get() as Row).count) > 0;
    if (!hasTables) this.db.exec("PRAGMA auto_vacuum=INCREMENTAL");
    this.db.exec("PRAGMA journal_mode=WAL");
    this.db.exec("PRAGMA synchronous=FULL");
    this.db.exec("PRAGMA foreign_keys=ON");
    this.migrate();
    this.assertPragmas();
  }

  private migrate(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS events (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        id TEXT NOT NULL UNIQUE,
        source_id TEXT NOT NULL,
        source TEXT NOT NULL,
        type TEXT NOT NULL,
        subject TEXT,
        time TEXT,
        received_at TEXT NOT NULL,
        dedupe_key TEXT,
        verification_method TEXT NOT NULL,
        verification_key_id TEXT,
        payload_verified INTEGER NOT NULL,
        datacontenttype TEXT,
        data_json TEXT,
        data_base64 TEXT,
        raw_size INTEGER NOT NULL,
        UNIQUE(source_id, dedupe_key)
      );
      CREATE INDEX IF NOT EXISTS events_source_subject ON events(source, subject, seq);
      CREATE INDEX IF NOT EXISTS events_source_type ON events(source, type, seq);
      CREATE INDEX IF NOT EXISTS events_received ON events(received_at);

      CREATE TABLE IF NOT EXISTS sources (
        id TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        created_at TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS subscriptions (
        id TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        token_hash TEXT NOT NULL,
        token_prefix TEXT NOT NULL,
        old_token_hash TEXT,
        old_token_expires_at TEXT,
        cursor INTEGER NOT NULL,
        cursor_epoch INTEGER NOT NULL,
        last_seen_at TEXT,
        created_at TEXT NOT NULL
      );
      CREATE INDEX IF NOT EXISTS subscriptions_liveness ON subscriptions(last_seen_at, cursor);
      CREATE TABLE IF NOT EXISTS admin_keys (
        id TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        key_hash TEXT NOT NULL,
        key_prefix TEXT NOT NULL,
        created_at TEXT NOT NULL,
        expires_at TEXT
      );
      CREATE TABLE IF NOT EXISTS poll_state (
        source_id TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        generation INTEGER NOT NULL,
        failure_count INTEGER NOT NULL,
        lease_token TEXT,
        lease_expires_at TEXT
      );
    `);
  }

  private assertPragmas(): void {
    const journal = String((this.db.query("PRAGMA journal_mode").get() as Row).journal_mode).toLowerCase();
    const synchronous = Number((this.db.query("PRAGMA synchronous").get() as Row).synchronous);
    const autoVacuum = Number((this.db.query("PRAGMA auto_vacuum").get() as Row).auto_vacuum);
    if (journal !== "wal" || synchronous !== 2 || autoVacuum !== 2) {
      throw new Error(`SQLite durability pragmas unavailable: journal_mode=${journal}, synchronous=${synchronous}, auto_vacuum=${autoVacuum}`);
    }
  }

  close(): void {
    this.db.close();
  }

  private eventFromRow(row: Row): StoredEvent {
    const event: StoredEvent = {
      specversion: "1.0",
      id: String(row.id),
      sourceId: String(row.source_id),
      source: String(row.source),
      type: String(row.type),
      seq: Number(row.seq),
      receivedat: String(row.received_at),
      verificationmethod: String(row.verification_method) as StoredEvent["verificationmethod"],
      payloadverified: Boolean(row.payload_verified),
      rawSize: Number(row.raw_size),
    };
    if (row.subject !== null) event.subject = String(row.subject);
    if (row.time !== null) event.time = String(row.time);
    if (row.dedupe_key !== null) event.dedupekey = String(row.dedupe_key);
    if (row.verification_key_id !== null) event.verificationkeyid = String(row.verification_key_id);
    if (row.datacontenttype !== null) event.datacontenttype = String(row.datacontenttype);
    if (row.data_json !== null) event.data = JSON.parse(String(row.data_json));
    if (row.data_base64 !== null) event.data_base64 = String(row.data_base64);
    return event;
  }

  private insertEvent(input: AppendEventInput): { event: StoredEvent; inserted: boolean } {
    if (input.dedupekey !== undefined) {
      const row = this.db.query("SELECT * FROM events WHERE source_id = ? AND dedupe_key = ?").get(input.sourceId, input.dedupekey) as Row | null;
      if (row) return { event: this.eventFromRow(row), inserted: false };
    }
    const result = this.db
      .query(`INSERT INTO events
        (id, source_id, source, type, subject, time, received_at, dedupe_key,
         verification_method, verification_key_id, payload_verified, datacontenttype,
         data_json, data_base64, raw_size)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(
        input.id,
        input.sourceId,
        input.source,
        input.type,
        input.subject ?? null,
        input.time ?? null,
        input.receivedat,
        input.dedupekey ?? null,
        input.verificationmethod,
        input.verificationkeyid ?? null,
        input.payloadverified ? 1 : 0,
        input.datacontenttype ?? null,
        input.data === undefined ? null : JSON.stringify(input.data),
        input.data_base64 ?? null,
        input.rawSize,
      );
    const row = this.db.query("SELECT * FROM events WHERE seq = ?").get(Number(result.lastInsertRowid)) as Row;
    return { event: this.eventFromRow(row), inserted: true };
  }

  async appendEvent(input: AppendEventInput): Promise<{ event: StoredEvent; inserted: boolean }> {
    return this.db.transaction(() => this.insertEvent(input)).immediate();
  }

  async appendPollTransition(input: AppendPollTransition): Promise<{ event?: StoredEvent; committed: boolean }> {
    return this.db.transaction(() => {
      const row = this.db.query("SELECT lease_token FROM poll_state WHERE source_id = ?").get(input.state.sourceId) as Row | null;
      if (!row || row.lease_token !== input.expectedLeaseToken) return { committed: false };
      const event = input.event ? this.insertEvent(input.event).event : undefined;
      this.writePollState(input.state);
      return event ? { event, committed: true } : { committed: true };
    }).immediate();
  }

  async newestSeq(): Promise<number> {
    return Number((this.db.query("SELECT coalesce((SELECT seq FROM sqlite_sequence WHERE name='events'), 0) AS seq").get() as Row).seq);
  }
  async oldestAvailableSeq(): Promise<number> {
    const row = this.db.query("SELECT min(seq) AS oldest, coalesce((SELECT seq FROM sqlite_sequence WHERE name='events'), 0) + 1 AS next FROM events").get() as Row;
    return row.oldest === null ? Number(row.next) : Number(row.oldest);
  }
  async getEventBySeq(seq: number): Promise<StoredEvent | undefined> {
    const row = this.db.query("SELECT * FROM events WHERE seq = ?").get(seq) as Row | null;
    return row ? this.eventFromRow(row) : undefined;
  }

  async queryEvents(query: EventQuery): Promise<StoredEvent[]> {
    const clauses: string[] = [];
    const values: Array<string | number> = [];
    const add = (sql: string, value: string | number | undefined) => {
      if (value !== undefined) {
        clauses.push(sql);
        values.push(value);
      }
    };
    add("source = ?", query.source);
    add("type = ?", query.type);
    add("subject = ?", query.subject);
    add("seq > ?", query.sinceSeq);
    add("seq <= ?", query.untilSeq);
    add("received_at >= ?", query.since);
    add("received_at <= ?", query.until);
    add("time >= ?", query.eventSince);
    add("time <= ?", query.eventUntil);
    values.push(query.limit);
    const sql = `SELECT * FROM events ${clauses.length ? `WHERE ${clauses.join(" AND ")}` : ""} ORDER BY seq LIMIT ?`;
    return (this.db.query(sql).all(...values) as Row[]).map((row) => this.eventFromRow(row));
  }

  async putSource(source: Source): Promise<void> {
    this.db.query("INSERT INTO sources(id, value, created_at) VALUES (?, ?, ?) ON CONFLICT(id) DO UPDATE SET value=excluded.value").run(source.id, JSON.stringify(source), source.createdAt);
  }
  async getSource(id: string): Promise<Source | undefined> {
    const row = this.db.query("SELECT value FROM sources WHERE id = ?").get(id) as Row | null;
    return row ? JSON.parse(String(row.value)) as Source : undefined;
  }
  async listSources(): Promise<Source[]> {
    return (this.db.query("SELECT value FROM sources ORDER BY id").all() as Row[]).map((row) => JSON.parse(String(row.value)) as Source);
  }
  async deleteSource(id: string): Promise<boolean> {
    return this.db.query("DELETE FROM sources WHERE id = ?").run(id).changes > 0;
  }

  private writeSubscription(subscription: Subscription): void {
    this.db.query(`INSERT INTO subscriptions
      (id, value, token_hash, token_prefix, old_token_hash, old_token_expires_at, cursor, cursor_epoch, last_seen_at, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(id) DO UPDATE SET value=excluded.value, token_hash=excluded.token_hash,
        token_prefix=excluded.token_prefix, old_token_hash=excluded.old_token_hash,
        old_token_expires_at=excluded.old_token_expires_at, cursor=excluded.cursor,
        cursor_epoch=excluded.cursor_epoch, last_seen_at=excluded.last_seen_at`)
      .run(subscription.id, JSON.stringify(subscription), subscription.tokenHash, subscription.tokenPrefix,
        subscription.oldTokenHash ?? null, subscription.oldTokenExpiresAt ?? null, subscription.cursor,
        subscription.cursorEpoch, subscription.lastSeenAt ?? null, subscription.createdAt);
  }
  private subscriptionFromRow(row: Row): Subscription {
    const subscription = JSON.parse(String(row.value)) as Subscription;
    subscription.cursor = Number(row.cursor);
    subscription.cursorEpoch = Number(row.cursor_epoch);
    if (row.last_seen_at === null) delete subscription.lastSeenAt;
    else subscription.lastSeenAt = String(row.last_seen_at);
    return subscription;
  }
  async putSubscription(subscription: Subscription): Promise<void> { this.writeSubscription(subscription); }
  async createSubscription(subscription: Subscription): Promise<boolean> {
    return this.db.query(`INSERT INTO subscriptions
      (id, value, token_hash, token_prefix, old_token_hash, old_token_expires_at, cursor, cursor_epoch, last_seen_at, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(id) DO NOTHING`)
      .run(subscription.id, JSON.stringify(subscription), subscription.tokenHash, subscription.tokenPrefix,
        subscription.oldTokenHash ?? null, subscription.oldTokenExpiresAt ?? null, subscription.cursor,
        subscription.cursorEpoch, subscription.lastSeenAt ?? null, subscription.createdAt).changes > 0;
  }
  async updateSubscription(subscription: Subscription): Promise<void> { this.writeSubscription(subscription); }
  async getSubscription(id: string): Promise<Subscription | undefined> {
    const row = this.db.query("SELECT * FROM subscriptions WHERE id = ?").get(id) as Row | null;
    return row ? this.subscriptionFromRow(row) : undefined;
  }
  async listSubscriptions(): Promise<Subscription[]> {
    return (this.db.query("SELECT * FROM subscriptions ORDER BY id").all() as Row[]).map((row) => this.subscriptionFromRow(row));
  }
  async deleteSubscription(id: string): Promise<boolean> { return this.db.query("DELETE FROM subscriptions WHERE id = ?").run(id).changes > 0; }

  async advanceCursor(id: string, seq: number, epoch: number): Promise<CursorUpdateResult> {
    return this.db.transaction(() => {
      const row = this.db.query("SELECT * FROM subscriptions WHERE id = ?").get(id) as Row | null;
      if (!row) return { status: "not_found" } as const;
      if (Number(row.cursor_epoch) !== epoch) return { status: "stale_epoch" } as const;
      const newest = Number((this.db.query("SELECT coalesce((SELECT seq FROM sqlite_sequence WHERE name='events'), 0) AS seq").get() as Row).seq);
      if (seq > newest) return { status: "beyond_newest" } as const;
      const subscription = this.subscriptionFromRow(row);
      if (seq <= subscription.cursor) return { status: "unchanged", subscription } as const;
      subscription.cursor = seq;
      if (subscription.push.parked === 2 && subscription.push.gapTo !== undefined && seq >= subscription.push.gapTo) {
        subscription.push.parked = 0;
        delete subscription.push.gapTo;
        delete subscription.push.nextAttemptAt;
      }
      this.writeSubscription(subscription);
      return { status: "advanced", subscription } as const;
    }).immediate();
  }

  async rewindCursor(id: string, seq: number): Promise<Subscription | undefined> {
    return this.db.transaction(() => {
      const row = this.db.query("SELECT * FROM subscriptions WHERE id = ?").get(id) as Row | null;
      if (!row) return undefined;
      const subscription = this.subscriptionFromRow(row);
      subscription.cursor = Math.max(0, seq);
      subscription.cursorEpoch += 1;
      delete subscription.push.scannedThroughSeq;
      delete subscription.push.scannedFilter;
      this.writeSubscription(subscription);
      return subscription;
    }).immediate();
  }

  async refreshLiveness(id: string, now: string, coalesceMs: number): Promise<void> {
    const row = this.db.query("SELECT * FROM subscriptions WHERE id = ?").get(id) as Row | null;
    if (!row) return;
    const lastSeen = row.last_seen_at === null ? 0 : Date.parse(String(row.last_seen_at));
    if (lastSeen && Date.parse(now) - lastSeen < coalesceMs) return;
    const subscription = this.subscriptionFromRow(row);
    subscription.lastSeenAt = now;
    this.writeSubscription(subscription);
  }

  async claimPushLease(id: string, token: string, expiresAt: string, now: string): Promise<Subscription | undefined> {
    return this.db.transaction(() => {
      const row = this.db.query("SELECT * FROM subscriptions WHERE id = ?").get(id) as Row | null;
      if (!row) return undefined;
      const subscription = this.subscriptionFromRow(row);
      if (subscription.push.leaseExpiresAt && subscription.push.leaseExpiresAt > now && subscription.push.leaseToken !== token) return undefined;
      subscription.push.leaseToken = token;
      subscription.push.leaseExpiresAt = expiresAt;
      this.writeSubscription(subscription);
      return subscription;
    }).immediate();
  }

  async commitPushMutation(input: PushCommitInput): Promise<PushCommitResult> {
    return this.db.transaction(() => {
      const row = this.db.query("SELECT * FROM subscriptions WHERE id = ?").get(input.subscriptionId) as Row | null;
      if (!row) return { status: "stale_lease" } as const;
      const current = this.subscriptionFromRow(row);
      if (current.push.leaseToken !== input.expectedLeaseToken) return { status: "stale_lease" } as const;
      if (current.cursorEpoch !== input.expectedCursorEpoch) {
        delete current.push.leaseToken;
        delete current.push.leaseExpiresAt;
        this.writeSubscription(current);
        return { status: "stale_epoch" } as const;
      }
      current.push = structuredClone(input.push);
      if (input.cursor !== undefined && input.cursor > current.cursor) current.cursor = input.cursor;
      if (input.lastSeenAt !== undefined) current.lastSeenAt = input.lastSeenAt;
      if (current.push.parked === 2 && current.push.gapTo !== undefined && current.cursor >= current.push.gapTo) {
        current.push.parked = 0;
        delete current.push.gapTo;
        delete current.push.nextAttemptAt;
      }
      this.writeSubscription(current);
      return { status: "committed" } as const;
    }).immediate();
  }

  async putAdminKey(key: AdminKey): Promise<void> {
    this.db.query(`INSERT INTO admin_keys(id, value, key_hash, key_prefix, created_at, expires_at)
      VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET value=excluded.value,
      key_hash=excluded.key_hash, key_prefix=excluded.key_prefix, expires_at=excluded.expires_at`)
      .run(key.id, JSON.stringify(key), key.keyHash, key.keyPrefix, key.createdAt, key.expiresAt ?? null);
  }
  async listAdminKeys(): Promise<AdminKey[]> {
    return (this.db.query("SELECT value FROM admin_keys ORDER BY created_at").all() as Row[]).map((row) => JSON.parse(String(row.value)) as AdminKey);
  }
  async deleteAdminKey(id: string): Promise<boolean> { return this.db.query("DELETE FROM admin_keys WHERE id = ?").run(id).changes > 0; }

  private writePollState(state: PollState): void {
    this.db.query(`INSERT INTO poll_state(source_id, value, generation, failure_count, lease_token, lease_expires_at)
      VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(source_id) DO UPDATE SET value=excluded.value,
      generation=excluded.generation, failure_count=excluded.failure_count,
      lease_token=excluded.lease_token, lease_expires_at=excluded.lease_expires_at`)
      .run(state.sourceId, JSON.stringify(state), state.generation, state.failureCount, state.leaseToken ?? null, state.leaseExpiresAt ?? null);
  }
  async putPollState(state: PollState): Promise<void> { this.writePollState(state); }
  async getPollState(sourceId: string): Promise<PollState | undefined> {
    const row = this.db.query("SELECT value FROM poll_state WHERE source_id = ?").get(sourceId) as Row | null;
    return row ? JSON.parse(String(row.value)) as PollState : undefined;
  }
  async claimPollLease(sourceId: string, token: string, expiresAt: string, now: string): Promise<PollState | undefined> {
    return this.db.transaction(() => {
      const row = this.db.query("SELECT value FROM poll_state WHERE source_id = ?").get(sourceId) as Row | null;
      const state = row ? JSON.parse(String(row.value)) as PollState : { sourceId, generation: 0, failureCount: 0 };
      if (state.leaseExpiresAt && state.leaseExpiresAt > now && state.leaseToken !== token) return undefined;
      state.leaseToken = token;
      state.leaseExpiresAt = expiresAt;
      this.writePollState(state);
      return state;
    }).immediate();
  }

  async measureSizeBytes(): Promise<number> {
    const pageCount = Number((this.db.query("PRAGMA page_count").get() as Row).page_count);
    const pageSize = Number((this.db.query("PRAGMA page_size").get() as Row).page_size);
    return pageCount * pageSize;
  }

  async enforceRetention(policy: RetentionPolicy, now: string): Promise<RetentionResult> {
    return this.db.transaction(() => {
      const nowMs = Date.parse(now);
      const pinWindow = policy.pinningWindowMs ?? policy.maxAgeMs ?? 30 * 86_400_000;
      const liveCutoff = new Date(nowMs - pinWindow).toISOString();
      const pinned = (this.db.query("SELECT min(cursor) AS cursor FROM subscriptions WHERE last_seen_at >= ?").get(liveCutoff) as Row).cursor;
      const pinnedThrough = pinned === null ? Number.MAX_SAFE_INTEGER : Number(pinned);
      let deleted = 0;
      if (policy.maxAgeMs !== undefined) {
        const ageCutoff = new Date(nowMs - policy.maxAgeMs).toISOString();
        deleted += this.db.query(SQLITE_RETENTION_AGE_DELETE_SQL).run(ageCutoff, pinnedThrough).changes;
      }
      if (policy.maxCount !== undefined) {
        const count = Number((this.db.query("SELECT count(*) AS count FROM events").get() as Row).count);
        const excess = Math.max(0, count - policy.maxCount);
        if (excess > 0) {
          deleted += this.db.query(
            "DELETE FROM events WHERE seq IN (SELECT seq FROM events WHERE seq <= ? ORDER BY seq LIMIT ?)",
          ).run(pinnedThrough, excess).changes;
        }
      }
      let size = this.measureSizeBytesSync();
      let sizeOverride = false;
      if (policy.maxBytes !== undefined && size > policy.maxBytes) {
        sizeOverride = true;
        const remaining = this.db.query("SELECT seq FROM events ORDER BY seq").all() as Row[];
        for (const row of remaining) {
          if (size <= policy.maxBytes) break;
          this.db.query("DELETE FROM events WHERE seq = ?").run(Number(row.seq));
          deleted += 1;
          this.db.exec("PRAGMA incremental_vacuum(1)");
          size = this.measureSizeBytesSync();
        }
      }
      const oldestRow = this.db.query("SELECT min(seq) AS oldest FROM events").get() as Row;
      const next = Number((this.db.query("SELECT coalesce((SELECT seq FROM sqlite_sequence WHERE name='events'), 0) + 1 AS next").get() as Row).next);
      return { deleted, sizeOverride, oldestAvailableSeq: oldestRow.oldest === null ? next : Number(oldestRow.oldest), sizeBytes: size };
    }).immediate();
  }

  private measureSizeBytesSync(): number {
    const pageCount = Number((this.db.query("PRAGMA page_count").get() as Row).page_count);
    const pageSize = Number((this.db.query("PRAGMA page_size").get() as Row).page_size);
    return pageCount * pageSize;
  }
}
