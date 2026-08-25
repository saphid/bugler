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

type SqlValue = ArrayBuffer | string | number | null;
type Row = Record<string, SqlValue>;

interface SqlCursor<T> {
  toArray(): T[];
  one(): T;
  readonly rowsRead: number;
  readonly rowsWritten: number;
}

interface SqlStorageLike {
  exec<T extends Row>(query: string, ...bindings: SqlValue[]): SqlCursor<T>;
  readonly databaseSize: number;
}

export interface DurableObjectStorageLike {
  readonly sql: SqlStorageLike;
  transactionSync<T>(closure: () => T): T;
}

export const DURABLE_OBJECT_RETENTION_AGE_DELETE_SQL =
  "DELETE FROM events INDEXED BY events_received WHERE received_at < ? AND seq <= ?";

export class DurableObjectStorageDriver implements StorageDriver {
  readonly capabilities = {
    name: "durable-object-sqlite",
    maxPayloadBytes: 1_500_000,
    queryNotes: {
      seqRange: "events primary key range in Durable Object SQLite",
      sourceSubject: "events_source_subject composite index in Durable Object SQLite",
      sourceType: "events_source_type composite index in Durable Object SQLite",
      receivedTime: "events_received index followed by sequence ordering in Durable Object SQLite",
    },
    transactionNotes: "Durable Object storage transactionSync around SQLite mutations",
    sizeMeasurementNotes: "Durable Object SqlStorage.databaseSize",
  } as const;

  private readonly sql: SqlStorageLike;
  lastRetentionRowsRead = 0;

  constructor(private readonly storage: DurableObjectStorageLike) {
    this.sql = storage.sql;
    this.migrate();
  }

  private migrate(): void {
    const statements = [
      `CREATE TABLE IF NOT EXISTS events (
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
      )`,
      "CREATE INDEX IF NOT EXISTS events_source_subject ON events(source, subject, seq)",
      "CREATE INDEX IF NOT EXISTS events_source_type ON events(source, type, seq)",
      "CREATE INDEX IF NOT EXISTS events_received ON events(received_at)",
      `CREATE TABLE IF NOT EXISTS sources (
        id TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        created_at TEXT NOT NULL
      )`,
      `CREATE TABLE IF NOT EXISTS subscriptions (
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
      )`,
      "CREATE INDEX IF NOT EXISTS subscriptions_liveness ON subscriptions(last_seen_at, cursor)",
      `CREATE TABLE IF NOT EXISTS admin_keys (
        id TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        key_hash TEXT NOT NULL,
        key_prefix TEXT NOT NULL,
        created_at TEXT NOT NULL,
        expires_at TEXT
      )`,
      `CREATE TABLE IF NOT EXISTS poll_state (
        source_id TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        generation INTEGER NOT NULL,
        failure_count INTEGER NOT NULL,
        lease_token TEXT,
        lease_expires_at TEXT
      )`,
    ];
    this.storage.transactionSync(() => {
      for (const statement of statements) this.sql.exec(statement);
    });
  }

  close(): void {}

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
      const existing = this.sql.exec<Row>("SELECT * FROM events WHERE source_id = ? AND dedupe_key = ?", input.sourceId, input.dedupekey).toArray()[0];
      if (existing) return { event: this.eventFromRow(existing), inserted: false };
    }
    this.sql.exec(
      `INSERT INTO events
        (id, source_id, source, type, subject, time, received_at, dedupe_key,
         verification_method, verification_key_id, payload_verified, datacontenttype,
         data_json, data_base64, raw_size)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
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
    const row = this.sql.exec<Row>("SELECT * FROM events WHERE seq = last_insert_rowid()").one();
    return { event: this.eventFromRow(row), inserted: true };
  }

  async appendEvent(input: AppendEventInput): Promise<{ event: StoredEvent; inserted: boolean }> {
    return this.storage.transactionSync(() => this.insertEvent(input));
  }

  async appendPollTransition(input: AppendPollTransition): Promise<{ event?: StoredEvent; committed: boolean }> {
    return this.storage.transactionSync(() => {
      const row = this.sql.exec<Row>("SELECT lease_token FROM poll_state WHERE source_id = ?", input.state.sourceId).toArray()[0];
      if (!row || row.lease_token !== input.expectedLeaseToken) return { committed: false };
      const event = input.event ? this.insertEvent(input.event).event : undefined;
      this.writePollState(input.state);
      return event ? { event, committed: true } : { committed: true };
    });
  }

  async newestSeq(): Promise<number> {
    return Number(this.sql.exec<Row>("SELECT coalesce((SELECT seq FROM sqlite_sequence WHERE name='events'), 0) AS seq").one().seq);
  }

  async oldestAvailableSeq(): Promise<number> {
    const row = this.sql.exec<Row>("SELECT min(seq) AS oldest, coalesce((SELECT seq FROM sqlite_sequence WHERE name='events'), 0) + 1 AS next FROM events").one();
    return row.oldest === null ? Number(row.next) : Number(row.oldest);
  }

  async getEventBySeq(seq: number): Promise<StoredEvent | undefined> {
    const row = this.sql.exec<Row>("SELECT * FROM events WHERE seq = ?", seq).toArray()[0];
    return row ? this.eventFromRow(row) : undefined;
  }

  async queryEvents(query: EventQuery): Promise<StoredEvent[]> {
    const clauses: string[] = [];
    const values: SqlValue[] = [];
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
    return this.sql.exec<Row>(`SELECT * FROM events ${clauses.length ? `WHERE ${clauses.join(" AND ")}` : ""} ORDER BY seq LIMIT ?`, ...values).toArray().map((row) => this.eventFromRow(row));
  }

  async putSource(source: Source): Promise<void> {
    this.sql.exec("INSERT INTO sources(id, value, created_at) VALUES (?, ?, ?) ON CONFLICT(id) DO UPDATE SET value=excluded.value", source.id, JSON.stringify(source), source.createdAt);
  }

  async getSource(id: string): Promise<Source | undefined> {
    const row = this.sql.exec<Row>("SELECT value FROM sources WHERE id = ?", id).toArray()[0];
    return row ? JSON.parse(String(row.value)) as Source : undefined;
  }

  async listSources(): Promise<Source[]> {
    return this.sql.exec<Row>("SELECT value FROM sources ORDER BY id").toArray().map((row) => JSON.parse(String(row.value)) as Source);
  }

  async deleteSource(id: string): Promise<boolean> {
    return this.sql.exec("DELETE FROM sources WHERE id = ?", id).rowsWritten > 0;
  }

  private writeSubscription(subscription: Subscription): void {
    this.sql.exec(
      `INSERT INTO subscriptions
       (id, value, token_hash, token_prefix, old_token_hash, old_token_expires_at, cursor, cursor_epoch, last_seen_at, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(id) DO UPDATE SET value=excluded.value, token_hash=excluded.token_hash,
         token_prefix=excluded.token_prefix, old_token_hash=excluded.old_token_hash,
         old_token_expires_at=excluded.old_token_expires_at, cursor=excluded.cursor,
         cursor_epoch=excluded.cursor_epoch, last_seen_at=excluded.last_seen_at`,
      subscription.id,
      JSON.stringify(subscription),
      subscription.tokenHash,
      subscription.tokenPrefix,
      subscription.oldTokenHash ?? null,
      subscription.oldTokenExpiresAt ?? null,
      subscription.cursor,
      subscription.cursorEpoch,
      subscription.lastSeenAt ?? null,
      subscription.createdAt,
    );
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
    return this.sql.exec(
      `INSERT INTO subscriptions
       (id, value, token_hash, token_prefix, old_token_hash, old_token_expires_at, cursor, cursor_epoch, last_seen_at, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(id) DO NOTHING`,
      subscription.id,
      JSON.stringify(subscription),
      subscription.tokenHash,
      subscription.tokenPrefix,
      subscription.oldTokenHash ?? null,
      subscription.oldTokenExpiresAt ?? null,
      subscription.cursor,
      subscription.cursorEpoch,
      subscription.lastSeenAt ?? null,
      subscription.createdAt,
    ).rowsWritten > 0;
  }
  async updateSubscription(subscription: Subscription): Promise<void> { this.writeSubscription(subscription); }

  async getSubscription(id: string): Promise<Subscription | undefined> {
    const row = this.sql.exec<Row>("SELECT * FROM subscriptions WHERE id = ?", id).toArray()[0];
    return row ? this.subscriptionFromRow(row) : undefined;
  }

  async listSubscriptions(): Promise<Subscription[]> {
    return this.sql.exec<Row>("SELECT * FROM subscriptions ORDER BY id").toArray().map((row) => this.subscriptionFromRow(row));
  }

  async deleteSubscription(id: string): Promise<boolean> {
    return this.sql.exec("DELETE FROM subscriptions WHERE id = ?", id).rowsWritten > 0;
  }

  async advanceCursor(id: string, seq: number, epoch: number): Promise<CursorUpdateResult> {
    return this.storage.transactionSync(() => {
      const row = this.sql.exec<Row>("SELECT * FROM subscriptions WHERE id = ?", id).toArray()[0];
      if (!row) return { status: "not_found" } as const;
      if (Number(row.cursor_epoch) !== epoch) return { status: "stale_epoch" } as const;
      const newest = Number(this.sql.exec<Row>("SELECT coalesce((SELECT seq FROM sqlite_sequence WHERE name='events'), 0) AS seq").one().seq);
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
    });
  }

  async rewindCursor(id: string, seq: number): Promise<Subscription | undefined> {
    return this.storage.transactionSync(() => {
      const row = this.sql.exec<Row>("SELECT * FROM subscriptions WHERE id = ?", id).toArray()[0];
      if (!row) return undefined;
      const subscription = this.subscriptionFromRow(row);
      subscription.cursor = Math.max(0, seq);
      subscription.cursorEpoch += 1;
      delete subscription.push.scannedThroughSeq;
      delete subscription.push.scannedFilter;
      this.writeSubscription(subscription);
      return subscription;
    });
  }

  async refreshLiveness(id: string, now: string, coalesceMs: number): Promise<void> {
    const row = this.sql.exec<Row>("SELECT * FROM subscriptions WHERE id = ?", id).toArray()[0];
    if (!row) return;
    const lastSeen = row.last_seen_at === null ? 0 : Date.parse(String(row.last_seen_at));
    if (lastSeen && Date.parse(now) - lastSeen < coalesceMs) return;
    const subscription = this.subscriptionFromRow(row);
    subscription.lastSeenAt = now;
    this.writeSubscription(subscription);
  }

  async claimPushLease(id: string, token: string, expiresAt: string, now: string): Promise<Subscription | undefined> {
    return this.storage.transactionSync(() => {
      const row = this.sql.exec<Row>("SELECT * FROM subscriptions WHERE id = ?", id).toArray()[0];
      if (!row) return undefined;
      const subscription = this.subscriptionFromRow(row);
      if (subscription.push.leaseExpiresAt && subscription.push.leaseExpiresAt > now && subscription.push.leaseToken !== token) return undefined;
      subscription.push.leaseToken = token;
      subscription.push.leaseExpiresAt = expiresAt;
      this.writeSubscription(subscription);
      return subscription;
    });
  }

  async commitPushMutation(input: PushCommitInput): Promise<PushCommitResult> {
    return this.storage.transactionSync(() => {
      const row = this.sql.exec<Row>("SELECT * FROM subscriptions WHERE id = ?", input.subscriptionId).toArray()[0];
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
    });
  }

  async putAdminKey(key: AdminKey): Promise<void> {
    this.sql.exec(
      `INSERT INTO admin_keys(id, value, key_hash, key_prefix, created_at, expires_at)
       VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET value=excluded.value,
       key_hash=excluded.key_hash, key_prefix=excluded.key_prefix, expires_at=excluded.expires_at`,
      key.id,
      JSON.stringify(key),
      key.keyHash,
      key.keyPrefix,
      key.createdAt,
      key.expiresAt ?? null,
    );
  }

  async listAdminKeys(): Promise<AdminKey[]> {
    return this.sql.exec<Row>("SELECT value FROM admin_keys ORDER BY created_at").toArray().map((row) => JSON.parse(String(row.value)) as AdminKey);
  }

  async deleteAdminKey(id: string): Promise<boolean> {
    return this.sql.exec("DELETE FROM admin_keys WHERE id = ?", id).rowsWritten > 0;
  }

  private writePollState(state: PollState): void {
    this.sql.exec(
      `INSERT INTO poll_state(source_id, value, generation, failure_count, lease_token, lease_expires_at)
       VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(source_id) DO UPDATE SET value=excluded.value,
       generation=excluded.generation, failure_count=excluded.failure_count,
       lease_token=excluded.lease_token, lease_expires_at=excluded.lease_expires_at`,
      state.sourceId,
      JSON.stringify(state),
      state.generation,
      state.failureCount,
      state.leaseToken ?? null,
      state.leaseExpiresAt ?? null,
    );
  }

  async putPollState(state: PollState): Promise<void> { this.writePollState(state); }

  async getPollState(sourceId: string): Promise<PollState | undefined> {
    const row = this.sql.exec<Row>("SELECT value FROM poll_state WHERE source_id = ?", sourceId).toArray()[0];
    return row ? JSON.parse(String(row.value)) as PollState : undefined;
  }

  async claimPollLease(sourceId: string, token: string, expiresAt: string, now: string): Promise<PollState | undefined> {
    return this.storage.transactionSync(() => {
      const row = this.sql.exec<Row>("SELECT value FROM poll_state WHERE source_id = ?", sourceId).toArray()[0];
      const state = row ? JSON.parse(String(row.value)) as PollState : { sourceId, generation: 0, failureCount: 0 };
      if (state.leaseExpiresAt && state.leaseExpiresAt > now && state.leaseToken !== token) return undefined;
      state.leaseToken = token;
      state.leaseExpiresAt = expiresAt;
      this.writePollState(state);
      return state;
    });
  }

  async measureSizeBytes(): Promise<number> {
    return this.sql.databaseSize;
  }

  async enforceRetention(policy: RetentionPolicy, now: string): Promise<RetentionResult> {
    const cursors: SqlCursor<Row>[] = [];
    const exec = <T extends Row>(query: string, ...bindings: SqlValue[]): SqlCursor<T> => {
      const cursor = this.sql.exec<T>(query, ...bindings);
      cursors.push(cursor as unknown as SqlCursor<Row>);
      return cursor;
    };
    const result = this.storage.transactionSync(() => {
      const nowMs = Date.parse(now);
      const pinWindow = policy.pinningWindowMs ?? policy.maxAgeMs ?? 30 * 86_400_000;
      const liveCutoff = new Date(nowMs - pinWindow).toISOString();
      const pinned = exec<Row>("SELECT min(cursor) AS cursor FROM subscriptions WHERE last_seen_at >= ?", liveCutoff).one().cursor;
      const pinnedThrough = pinned === null ? Number.MAX_SAFE_INTEGER : Number(pinned);
      let deleted = 0;
      if (policy.maxAgeMs !== undefined) {
        const ageCutoff = new Date(nowMs - policy.maxAgeMs).toISOString();
        deleted += exec(DURABLE_OBJECT_RETENTION_AGE_DELETE_SQL, ageCutoff, pinnedThrough).rowsWritten;
      }
      if (policy.maxCount !== undefined) {
        const count = Number(exec<Row>("SELECT count(*) AS count FROM events").one().count);
        const excess = Math.max(0, count - policy.maxCount);
        if (excess > 0) {
          deleted += exec(
            "DELETE FROM events WHERE seq IN (SELECT seq FROM events WHERE seq <= ? ORDER BY seq LIMIT ?)",
            pinnedThrough,
            excess,
          ).rowsWritten;
        }
      }
      let size = this.sql.databaseSize;
      let sizeOverride = false;
      if (policy.maxBytes !== undefined && size > policy.maxBytes) {
        sizeOverride = true;
        for (const row of exec<Row>("SELECT seq FROM events ORDER BY seq").toArray()) {
          if (size <= policy.maxBytes) break;
          exec("DELETE FROM events WHERE seq = ?", Number(row.seq));
          deleted += 1;
          size = this.sql.databaseSize;
        }
      }
      const oldest = exec<Row>("SELECT min(seq) AS oldest FROM events").one().oldest;
      const next = Number(exec<Row>("SELECT coalesce((SELECT seq FROM sqlite_sequence WHERE name='events'), 0) + 1 AS next").one().next);
      return { deleted, sizeOverride, oldestAvailableSeq: oldest === null ? next : Number(oldest), sizeBytes: size };
    });
    this.lastRetentionRowsRead = cursors.reduce((total, cursor) => total + cursor.rowsRead, 0);
    return result;
  }
}
