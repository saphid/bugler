import { Database } from "bun:sqlite";
import { timingSafeEqual } from "node:crypto";
import { rename, rm } from "node:fs/promises";
import { hashToken, hasValidLocalTokenChecksum, mintLocalToken } from "../auth";
import type { Filter, StoredEvent } from "../domain";
import { matchesFilter } from "../filter";
import { LocalStoreError, type LocalStore } from "./store";
import type {
  DurableHostedAck,
  HostedFrame,
  LocalActionRecord,
  LocalActionAttempt,
  LocalCommitPhase,
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
} from "./types";

interface MirrorRow {
  subscription_id: string;
  hosted_cursor: number;
  hosted_cursor_epoch: number;
  received_cursor: number;
  link_state: LocalMirror["state"];
}

interface ConsumerRow {
  consumer_id: string;
  state: LocalConsumer["state"];
  next_local_seq: number;
  cursor: number;
  cursor_epoch: number;
  filter_json: string | null;
  last_seen_at: string;
  kind: "agent" | "action" | "management";
  backfill_subscription_id: string | null;
  backfill_hosted_cursor_epoch: number | null;
  backfill_hosted_seq: number | null;
}

interface EntryRow {
  local_seq: number;
  kind: "event" | "hosted_gap";
  envelope_json: string | null;
}

interface TokenRow {
  current_hash: string;
  next_hash: string | null;
  overlap_until: string | null;
  revoked_at: string | null;
}

function hashesEqual(left: string, right: string): boolean {
  if (left.length !== right.length) return false;
  return timingSafeEqual(Buffer.from(left), Buffer.from(right));
}

function stringify(value: unknown): string {
  return JSON.stringify(value);
}

export class SqliteLocalStore implements LocalStore {
  readonly db: Database;
  private readonly now: () => Date;
  private readonly fault?: LocalStoreOptions["fault"];

  constructor(path: string, private readonly options: LocalStoreOptions = {}) {
    this.db = new Database(path, { create: true, strict: true });
    this.now = options.now ?? (() => new Date());
    this.fault = options.fault;
    this.db.exec("PRAGMA journal_mode=WAL");
    this.db.exec("PRAGMA synchronous=FULL");
    this.db.exec("PRAGMA foreign_keys=ON");
    this.db.exec("PRAGMA busy_timeout=5000");
    const quick = this.db.query("PRAGMA quick_check").get() as { quick_check: string };
    if (quick.quick_check !== "ok") {
      this.db.close();
      throw new LocalStoreError("corrupt");
    }
    const hadMeta = this.db.query("SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name='daemon_meta'").get() !== null;
    const cleanShutdown = hadMeta
      ? Number((this.db.query("SELECT clean_shutdown FROM daemon_meta WHERE singleton=1").get() as { clean_shutdown?: number } | null)?.clean_shutdown ?? 0)
      : 1;
    if (hadMeta && cleanShutdown !== 1) {
      const integrity = this.db.query("PRAGMA integrity_check").get() as { integrity_check: string };
      if (integrity.integrity_check !== "ok") {
        this.db.close();
        throw new LocalStoreError("corrupt");
      }
    }
    const autoVacuum = (this.db.query("PRAGMA auto_vacuum").get() as { auto_vacuum: number }).auto_vacuum;
    if (autoVacuum === 0) {
      this.db.exec("PRAGMA auto_vacuum=INCREMENTAL");
      this.db.exec("VACUUM");
    }
    this.migrate();
    this.assertMonotonicInvariants();
  }

  private assertMonotonicInvariants(): void {
    const invalid = this.db.query(`SELECT 1 AS invalid FROM consumer_streams c
      WHERE c.next_local_seq <= c.cursor OR c.next_local_seq <= COALESCE((SELECT MAX(e.local_seq) FROM consumer_entries e WHERE e.consumer_id=c.consumer_id),0)
      LIMIT 1`).get();
    if (invalid) {
      this.db.close();
      throw new LocalStoreError("corrupt");
    }
  }

  private migrate(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS daemon_meta (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        schema_version INTEGER NOT NULL,
        instance_id TEXT NOT NULL,
        account_key TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL DEFAULT '',
        clean_shutdown INTEGER NOT NULL DEFAULT 0
      );
      INSERT OR IGNORE INTO daemon_meta(singleton,schema_version,instance_id,clean_shutdown)
        VALUES(1,1,lower(hex(randomblob(16))),0);

      CREATE TABLE IF NOT EXISTS mirror_subscriptions (
        subscription_id TEXT PRIMARY KEY,
        token_auth_version INTEGER NOT NULL DEFAULT 0,
        hosted_cursor INTEGER NOT NULL,
        hosted_cursor_epoch INTEGER NOT NULL,
        received_cursor INTEGER NOT NULL,
        link_state TEXT NOT NULL,
        last_error TEXT,
        updated_at TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS mirror_events (
        subscription_id TEXT NOT NULL,
        hosted_cursor_epoch INTEGER NOT NULL,
        hosted_seq INTEGER NOT NULL,
        event_id TEXT NOT NULL,
        envelope_json TEXT NOT NULL,
        received_at TEXT NOT NULL,
        byte_size INTEGER NOT NULL,
        PRIMARY KEY(subscription_id,hosted_cursor_epoch,hosted_seq)
      );
      CREATE INDEX IF NOT EXISTS local_mirror_events_received ON mirror_events(received_at);
      CREATE INDEX IF NOT EXISTS local_mirror_events_id ON mirror_events(event_id);
      CREATE TABLE IF NOT EXISTS mirror_gaps (
        subscription_id TEXT NOT NULL,
        hosted_cursor_epoch INTEGER NOT NULL,
        hosted_from INTEGER NOT NULL,
        hosted_to INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        PRIMARY KEY(subscription_id,hosted_cursor_epoch,hosted_from,hosted_to)
      );
      CREATE TABLE IF NOT EXISTS consumer_streams (
        consumer_id TEXT PRIMARY KEY,
        kind TEXT NOT NULL DEFAULT 'agent' CHECK (kind IN ('agent','action','management')),
        state TEXT NOT NULL,
        next_local_seq INTEGER NOT NULL,
        cursor INTEGER NOT NULL,
        cursor_epoch INTEGER NOT NULL,
        filter_json TEXT,
        backfill_subscription_id TEXT,
        backfill_hosted_cursor_epoch INTEGER,
        backfill_hosted_seq INTEGER,
        last_seen_at TEXT NOT NULL,
        created_at TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS consumer_mirrors (
        consumer_id TEXT NOT NULL REFERENCES consumer_streams(consumer_id) ON DELETE CASCADE,
        subscription_id TEXT NOT NULL REFERENCES mirror_subscriptions(subscription_id),
        PRIMARY KEY(consumer_id,subscription_id)
      );
      CREATE TABLE IF NOT EXISTS consumer_entries (
        consumer_id TEXT NOT NULL REFERENCES consumer_streams(consumer_id) ON DELETE CASCADE,
        local_seq INTEGER NOT NULL,
        kind TEXT NOT NULL CHECK (kind IN ('event','hosted_gap')),
        subscription_id TEXT NOT NULL,
        hosted_cursor_epoch INTEGER NOT NULL,
        hosted_seq INTEGER,
        hosted_gap_from INTEGER,
        hosted_gap_to INTEGER,
        envelope_json TEXT,
        created_at TEXT NOT NULL,
        byte_size INTEGER NOT NULL,
        PRIMARY KEY(consumer_id,local_seq),
        UNIQUE(consumer_id,subscription_id,hosted_cursor_epoch,hosted_seq),
        UNIQUE(consumer_id,subscription_id,hosted_cursor_epoch,hosted_gap_from,hosted_gap_to)
      );
      CREATE INDEX IF NOT EXISTS local_consumer_entries_created ON consumer_entries(consumer_id,created_at);
      CREATE TABLE IF NOT EXISTS local_tokens (
        token_id TEXT PRIMARY KEY,
        consumer_id TEXT,
        class TEXT NOT NULL,
        operations TEXT NOT NULL DEFAULT '',
        current_hash TEXT NOT NULL,
        next_hash TEXT,
        overlap_until TEXT,
        revoked_at TEXT,
        expires_at TEXT
      );
      CREATE INDEX IF NOT EXISTS local_tokens_current_hash ON local_tokens(current_hash);
      CREATE INDEX IF NOT EXISTS local_tokens_next_hash ON local_tokens(next_hash);
      CREATE TABLE IF NOT EXISTS action_registry (
        action_id TEXT PRIMARY KEY,
        file_path TEXT UNIQUE,
        definition_json TEXT NOT NULL,
        enabled INTEGER NOT NULL,
        digest TEXT NOT NULL,
        load_error TEXT,
        loaded_at TEXT
      );
      CREATE TABLE IF NOT EXISTS action_attempts (
        action_id TEXT NOT NULL,
        local_seq INTEGER NOT NULL,
        replay_generation INTEGER NOT NULL,
        attempt INTEGER NOT NULL,
        state TEXT NOT NULL,
        due_at TEXT NOT NULL,
        started_at TEXT,
        finished_at TEXT,
        exit_status INTEGER,
        signal TEXT,
        stdout_tail BLOB,
        stderr_tail BLOB,
        PRIMARY KEY(action_id,local_seq,replay_generation,attempt)
      );
      CREATE INDEX IF NOT EXISTS local_action_attempts_due ON action_attempts(state,due_at);
      CREATE TABLE IF NOT EXISTS dead_letters (
        id TEXT PRIMARY KEY,
        action_id TEXT NOT NULL,
        local_seq INTEGER NOT NULL,
        replay_generation INTEGER NOT NULL DEFAULT 0,
        envelope_snapshot TEXT,
        summary_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        replayed_at TEXT
      );
      CREATE INDEX IF NOT EXISTS local_dead_letters_created ON dead_letters(created_at);
      CREATE TABLE IF NOT EXISTS recovery_notices (
        subscription_id TEXT PRIMARY KEY,
        recovered_at TEXT NOT NULL,
        hosted_cursor INTEGER NOT NULL,
        hosted_cursor_epoch INTEGER NOT NULL,
        oldest_available_seq INTEGER NOT NULL,
        acknowledged_at TEXT
      );
    `);
    const ensureColumn = (table: string, name: string, definition: string) => {
      const present = (this.db.query(`PRAGMA table_info(${table})`).all() as Array<{ name: string }>).some((column) => column.name === name);
      if (!present) this.db.exec(`ALTER TABLE ${table} ADD COLUMN ${name} ${definition}`);
    };
    ensureColumn("daemon_meta", "account_key", "TEXT NOT NULL DEFAULT ''");
    ensureColumn("daemon_meta", "created_at", "TEXT NOT NULL DEFAULT ''");
    ensureColumn("mirror_subscriptions", "token_auth_version", "INTEGER NOT NULL DEFAULT 0");
    ensureColumn("consumer_streams", "kind", "TEXT NOT NULL DEFAULT 'agent'");
    ensureColumn("consumer_streams", "backfill_subscription_id", "TEXT");
    ensureColumn("consumer_streams", "backfill_hosted_cursor_epoch", "INTEGER");
    ensureColumn("consumer_streams", "backfill_hosted_seq", "INTEGER");
    ensureColumn("local_tokens", "operations", "TEXT NOT NULL DEFAULT ''");
    ensureColumn("local_tokens", "expires_at", "TEXT");
    ensureColumn("action_registry", "file_path", "TEXT");
    ensureColumn("action_registry", "load_error", "TEXT");
    ensureColumn("action_registry", "loaded_at", "TEXT");
    ensureColumn("action_attempts", "started_at", "TEXT");
    ensureColumn("action_attempts", "finished_at", "TEXT");
    ensureColumn("action_attempts", "exit_status", "INTEGER");
    ensureColumn("action_attempts", "signal", "TEXT");
    ensureColumn("action_attempts", "stdout_tail", "BLOB");
    ensureColumn("action_attempts", "stderr_tail", "BLOB");
    ensureColumn("dead_letters", "replay_generation", "INTEGER NOT NULL DEFAULT 0");
    ensureColumn("dead_letters", "replayed_at", "TEXT");
    if (this.options.accountKey) {
      const existing = this.db.query("SELECT account_key FROM daemon_meta WHERE singleton=1").get() as { account_key: string };
      if (existing.account_key && existing.account_key !== this.options.accountKey) {
        this.db.close();
        throw new LocalStoreError("corrupt");
      }
      this.db.query("UPDATE daemon_meta SET account_key=?,created_at=CASE WHEN created_at='' THEN ? ELSE created_at END WHERE singleton=1")
        .run(this.options.accountKey, this.now().toISOString());
      this.db.query(`INSERT OR IGNORE INTO mirror_subscriptions
        (subscription_id,token_auth_version,hosted_cursor,hosted_cursor_epoch,received_cursor,link_state,updated_at)
        VALUES('internal:management',0,0,0,0,'local_only',?)`).run(this.now().toISOString());
    }
    this.db.query("UPDATE daemon_meta SET clean_shutdown=0 WHERE singleton=1").run();
  }

  close(): void {
    this.db.query("UPDATE daemon_meta SET clean_shutdown=1 WHERE singleton=1").run();
    this.db.close();
  }

  async createVerifiedBackup(primaryPath: string, secondaryPath: string): Promise<void> {
    const temporary = `${primaryPath}.tmp`;
    await rm(temporary, { force: true });
    try {
      this.db.query("VACUUM INTO ?").run(temporary);
      const verification = new Database(temporary, { readonly: true, strict: true });
      try {
        const result = verification.query("PRAGMA integrity_check").get() as { integrity_check: string };
        if (result.integrity_check !== "ok") throw new LocalStoreError("corrupt");
      } finally {
        verification.close();
      }
      await rm(secondaryPath, { force: true });
      await rename(primaryPath, secondaryPath).catch((error: NodeJS.ErrnoException) => {
        if (error.code !== "ENOENT") throw error;
      });
      await rename(temporary, primaryPath);
    } catch (error) {
      await rm(temporary, { force: true });
      throw error;
    }
  }

  private invokeFault(phase: LocalCommitPhase): void {
    const result = this.fault?.(phase);
    if (result instanceof Promise) throw new Error("SqliteLocalStore fault hooks must be synchronous");
  }

  private mirror(row: MirrorRow): LocalMirror {
    return {
      id: row.subscription_id,
      hostedCursor: row.hosted_cursor,
      cursorEpoch: row.hosted_cursor_epoch,
      receivedCursor: row.received_cursor,
      state: row.link_state,
    };
  }

  async createMirror(id: string, cursor = 0, epoch = 0): Promise<LocalMirror> {
    this.db.query(`INSERT OR IGNORE INTO mirror_subscriptions
      (subscription_id,hosted_cursor,hosted_cursor_epoch,received_cursor,link_state,updated_at)
      VALUES(?,?,?,?,?,?)`).run(id, cursor, epoch, cursor, "caught_up", this.now().toISOString());
    return this.mirror(this.db.query("SELECT * FROM mirror_subscriptions WHERE subscription_id=?").get(id) as MirrorRow);
  }

  async listMirrors(): Promise<LocalMirror[]> {
    return (this.db.query("SELECT * FROM mirror_subscriptions ORDER BY subscription_id").all() as MirrorRow[]).map((row) => this.mirror(row));
  }

  private consumer(row: ConsumerRow): LocalConsumer {
    const mirrors = (this.db.query("SELECT subscription_id FROM consumer_mirrors WHERE consumer_id=? ORDER BY subscription_id").all(row.consumer_id) as Array<{ subscription_id: string }>).map((item) => item.subscription_id);
    const totalEstimate = row.state === "backfilling"
      ? Number((this.db.query(`SELECT COUNT(*) AS count FROM mirror_events e JOIN consumer_mirrors m
          ON m.subscription_id=e.subscription_id WHERE m.consumer_id=?`).get(row.consumer_id) as { count: number }).count)
      : undefined;
    return {
      id: row.consumer_id,
      mirrors,
      ...(row.filter_json ? { filter: JSON.parse(row.filter_json) as Filter } : {}),
      cursor: row.cursor,
      cursorEpoch: row.cursor_epoch,
      nextLocalSeq: row.next_local_seq,
      lastSeenAt: row.last_seen_at,
      state: row.state,
      kind: row.kind,
      ...(totalEstimate !== undefined ? { mapped: row.next_local_seq - 1, totalEstimate } : {}),
    };
  }

  async createConsumer(input: LocalConsumerCreate): Promise<{ consumer: LocalConsumer; token: string }> {
    const minted = mintLocalToken("lcon");
    const createdAt = this.now().toISOString();
    const create = this.db.transaction(() => {
      if (this.db.query("SELECT 1 FROM consumer_streams WHERE consumer_id=?").get(input.id)) throw new LocalStoreError("already_exists");
      for (const mirror of input.mirrors) {
        if (!this.db.query("SELECT 1 FROM mirror_subscriptions WHERE subscription_id=?").get(mirror)) throw new LocalStoreError("not_found");
      }
      this.db.query(`INSERT INTO consumer_streams
        (consumer_id,kind,state,next_local_seq,cursor,cursor_epoch,filter_json,last_seen_at,created_at)
        VALUES(?,?,?,?,?,?,?,?,?)`).run(input.id, "agent", input.start === "oldest" ? "backfilling" : "ready", 1, 0, 0, input.filter ? stringify(input.filter) : null, createdAt, createdAt);
      for (const mirror of [...new Set(input.mirrors)]) this.db.query("INSERT INTO consumer_mirrors VALUES(?,?)").run(input.id, mirror);
      const recovery = this.db.query(`SELECT r.* FROM recovery_notices r JOIN consumer_mirrors m
        ON m.subscription_id=r.subscription_id WHERE m.consumer_id=? ORDER BY r.subscription_id LIMIT 1`).get(input.id) as {
          subscription_id: string; hosted_cursor_epoch: number;
        } | null;
      if (recovery) {
        this.db.query(`INSERT INTO consumer_entries
          (consumer_id,local_seq,kind,subscription_id,hosted_cursor_epoch,hosted_gap_from,hosted_gap_to,created_at,byte_size)
          VALUES(?,1,'hosted_gap',?,?,0,0,?,0)`).run(input.id, recovery.subscription_id, recovery.hosted_cursor_epoch, createdAt);
        this.db.query("UPDATE consumer_streams SET next_local_seq=2 WHERE consumer_id=?").run(input.id);
      }
      this.db.query(`INSERT INTO local_tokens(token_id,consumer_id,class,current_hash)
        VALUES(?,?,?,?)`).run(`consume:${input.id}`, input.id, "consume", minted.hash);
    });
    create();
    return { consumer: (await this.getConsumer(input.id))!, token: minted.token };
  }

  async advanceBackfills(limit = 1_000): Promise<number> {
    const slice = Math.max(1, Math.min(limit, 1_000));
    const ids = (this.db.query("SELECT consumer_id FROM consumer_streams WHERE state='backfilling' ORDER BY consumer_id").all() as Array<{ consumer_id: string }>).map((row) => row.consumer_id);
    for (const id of ids) {
      const transaction = this.db.transaction(() => {
        const consumer = this.getConsumerSync(id)!;
        const row = this.db.query("SELECT * FROM consumer_streams WHERE consumer_id=?").get(id) as ConsumerRow;
        const items = this.db.query(`SELECT * FROM (
            SELECT 'event' AS item_kind,e.subscription_id,e.hosted_cursor_epoch,e.hosted_seq AS item_seq,
              e.hosted_seq,e.envelope_json,e.received_at AS created_at,NULL AS hosted_from,NULL AS hosted_to
            FROM mirror_events e JOIN consumer_mirrors m ON m.subscription_id=e.subscription_id
            WHERE m.consumer_id=?
            UNION ALL
            SELECT 'gap' AS item_kind,g.subscription_id,g.hosted_cursor_epoch,g.hosted_from AS item_seq,
              NULL,NULL,g.created_at,g.hosted_from,g.hosted_to
            FROM mirror_gaps g JOIN consumer_mirrors m ON m.subscription_id=g.subscription_id
            WHERE m.consumer_id=?
          ) WHERE ? IS NULL OR subscription_id>? OR
            (subscription_id=? AND hosted_cursor_epoch>?) OR
            (subscription_id=? AND hosted_cursor_epoch=? AND item_seq>?)
          ORDER BY subscription_id,hosted_cursor_epoch,item_seq,item_kind LIMIT ?`)
          .all(id, id, row.backfill_subscription_id, row.backfill_subscription_id, row.backfill_subscription_id,
            row.backfill_hosted_cursor_epoch ?? 0, row.backfill_subscription_id, row.backfill_hosted_cursor_epoch ?? 0,
            row.backfill_hosted_seq ?? 0, slice) as Array<{
            item_kind: "event" | "gap"; subscription_id: string; hosted_cursor_epoch: number; hosted_seq: number | null;
            item_seq: number; envelope_json: string | null; created_at: string; hosted_from: number | null; hosted_to: number | null;
          }>;
        for (const item of items) {
          if (item.item_kind === "event") {
            const event = JSON.parse(item.envelope_json!) as StoredEvent;
            if (!consumer.filter || matchesFilter(event, consumer.filter)) {
              this.insertMapping(consumer, item.subscription_id, item.hosted_cursor_epoch, item.hosted_seq!, event, item.created_at);
            }
          } else {
            const localSeq = consumer.nextLocalSeq;
            const inserted = this.db.query(`INSERT OR IGNORE INTO consumer_entries
              (consumer_id,local_seq,kind,subscription_id,hosted_cursor_epoch,hosted_gap_from,hosted_gap_to,created_at,byte_size)
              VALUES(?,?,?,?,?,?,?,?,0)`).run(id, localSeq, "hosted_gap", item.subscription_id, item.hosted_cursor_epoch, item.hosted_from, item.hosted_to, item.created_at);
            if (inserted.changes > 0) consumer.nextLocalSeq += 1;
          }
        }
        const last = items.at(-1);
        this.db.query(`UPDATE consumer_streams SET next_local_seq=?,state=?,backfill_subscription_id=?,
          backfill_hosted_cursor_epoch=?,backfill_hosted_seq=? WHERE consumer_id=?`).run(
          consumer.nextLocalSeq, items.length < slice ? "ready" : "backfilling",
          last?.subscription_id ?? row.backfill_subscription_id,
          last?.hosted_cursor_epoch ?? row.backfill_hosted_cursor_epoch,
          last?.item_seq ?? row.backfill_hosted_seq, id,
        );
      });
      transaction();
    }
    return Number((this.db.query("SELECT COUNT(*) AS count FROM consumer_streams WHERE state='backfilling'").get() as { count: number }).count);
  }

  private getConsumerSync(id: string): LocalConsumer | undefined {
    const row = this.db.query("SELECT * FROM consumer_streams WHERE consumer_id=?").get(id) as ConsumerRow | null;
    return row ? this.consumer(row) : undefined;
  }

  async getConsumer(id: string): Promise<LocalConsumer | undefined> {
    return this.getConsumerSync(id);
  }

  async listConsumers(): Promise<LocalConsumer[]> {
    return (this.db.query("SELECT * FROM consumer_streams ORDER BY consumer_id").all() as ConsumerRow[]).map((row) => this.consumer(row));
  }

  private insertMapping(consumer: LocalConsumer, subscriptionId: string, epoch: number, seq: number, event: StoredEvent, createdAt: string): void {
    const localSeq = consumer.nextLocalSeq;
    const local = { ...event, seq: localSeq };
    const inserted = this.db.query(`INSERT OR IGNORE INTO consumer_entries
      (consumer_id,local_seq,kind,subscription_id,hosted_cursor_epoch,hosted_seq,envelope_json,created_at,byte_size)
      VALUES(?,?,?,?,?,?,?,?,?)`).run(consumer.id, localSeq, "event", subscriptionId, epoch, seq, stringify(local), createdAt, Buffer.byteLength(stringify(local)));
    if (inserted.changes > 0) consumer.nextLocalSeq += 1;
  }

  async commitHostedFrame(frame: HostedFrame): Promise<DurableHostedAck> {
    let ack!: DurableHostedAck;
    const transaction = this.db.transaction(() => {
      const row = this.db.query("SELECT * FROM mirror_subscriptions WHERE subscription_id=?").get(frame.subscriptionId) as MirrorRow | null;
      if (!row) throw new LocalStoreError("not_found");
      if (frame.cursorEpoch < row.hosted_cursor_epoch) throw new LocalStoreError("epoch_regression");
      let received = frame.cursorEpoch > row.hosted_cursor_epoch ? 0 : row.received_cursor;
      const expected = received + 1;
      const createdAt = this.now().toISOString();
      const insertedEvents: Array<{ seq: number; event: StoredEvent }> = [];
      let insertedGap: { from: number; to: number } | undefined;
      if (frame.kind === "events") {
        const ordered = [...frame.events].sort((a, b) => a.seq - b.seq);
        if (ordered.length && ordered[0]!.seq > expected) throw new LocalStoreError("sequence_gap");
        for (let index = 1; index < ordered.length; index += 1) if (ordered[index]!.seq !== ordered[index - 1]!.seq + 1) throw new LocalStoreError("sequence_gap");
        for (const event of ordered) {
          const existing = this.db.query(`SELECT envelope_json FROM mirror_events
            WHERE subscription_id=? AND hosted_cursor_epoch=? AND hosted_seq=?`).get(frame.subscriptionId, frame.cursorEpoch, event.seq) as { envelope_json: string } | null;
          if (existing && existing.envelope_json !== stringify(event)) throw new LocalStoreError("corrupt");
          if (!existing && event.seq >= expected) {
            this.db.query(`INSERT INTO mirror_events
              (subscription_id,hosted_cursor_epoch,hosted_seq,event_id,envelope_json,received_at,byte_size)
              VALUES(?,?,?,?,?,?,?)`).run(frame.subscriptionId, frame.cursorEpoch, event.seq, event.id, stringify(event), event.receivedat, Buffer.byteLength(stringify(event)));
            insertedEvents.push({ seq: event.seq, event });
          }
          received = Math.max(received, event.seq);
        }
      } else {
        if (frame.gap.to >= expected) {
          if (frame.gap.from > expected || frame.gap.to < frame.gap.from) throw new LocalStoreError("sequence_gap");
          const result = this.db.query(`INSERT OR IGNORE INTO mirror_gaps
            (subscription_id,hosted_cursor_epoch,hosted_from,hosted_to,created_at) VALUES(?,?,?,?,?)`)
            .run(frame.subscriptionId, frame.cursorEpoch, frame.gap.from, frame.gap.to, createdAt);
          if (result.changes > 0) insertedGap = frame.gap;
          received = frame.gap.to;
        }
      }
      this.invokeFault("after_events");
      const consumers = this.db.query(`SELECT s.* FROM consumer_streams s
        JOIN consumer_mirrors m ON m.consumer_id=s.consumer_id WHERE m.subscription_id=?`).all(frame.subscriptionId) as ConsumerRow[];
      for (const consumerRow of consumers) {
        const consumer = this.consumer(consumerRow);
        for (const inserted of insertedEvents) {
          if (!consumer.filter || matchesFilter(inserted.event, consumer.filter)) this.insertMapping(consumer, frame.subscriptionId, frame.cursorEpoch, inserted.seq, inserted.event, createdAt);
        }
        if (insertedGap) {
          const localSeq = consumer.nextLocalSeq;
          const result = this.db.query(`INSERT OR IGNORE INTO consumer_entries
            (consumer_id,local_seq,kind,subscription_id,hosted_cursor_epoch,hosted_gap_from,hosted_gap_to,created_at,byte_size)
            VALUES(?,?,?,?,?,?,?,?,?)`).run(consumer.id, localSeq, "hosted_gap", frame.subscriptionId, frame.cursorEpoch, insertedGap.from, insertedGap.to, createdAt, 0);
          if (result.changes > 0) consumer.nextLocalSeq += 1;
        }
        this.db.query("UPDATE consumer_streams SET next_local_seq=? WHERE consumer_id=?").run(consumer.nextLocalSeq, consumer.id);
      }
      this.invokeFault("after_mappings");
      this.db.query(`UPDATE mirror_subscriptions SET hosted_cursor=?,hosted_cursor_epoch=?,received_cursor=?,link_state='caught_up',last_error=NULL,updated_at=?
        WHERE subscription_id=?`).run(received, frame.cursorEpoch, received, createdAt, frame.subscriptionId);
      ack = { subscriptionId: frame.subscriptionId, seq: received, epoch: frame.cursorEpoch };
    });
    try {
      transaction();
    } catch (error) {
      if (error instanceof LocalStoreError && error.code === "epoch_regression") {
        this.db.query("UPDATE mirror_subscriptions SET link_state='epoch_regression',last_error='epoch_regression',updated_at=? WHERE subscription_id=?")
          .run(this.now().toISOString(), frame.subscriptionId);
      }
      throw error;
    }
    this.invokeFault("after_commit");
    return ack;
  }

  async readConsumer(id: string, limit = 100): Promise<LocalDeliveryPayload> {
    const consumer = this.getConsumerSync(id);
    if (!consumer) throw new LocalStoreError("not_found");
    if (consumer.state !== "ready") throw new LocalStoreError("backfilling");
    const oldestRow = this.db.query("SELECT MIN(local_seq) AS oldest FROM consumer_entries WHERE consumer_id=?").get(id) as { oldest: number | null };
    const oldest = oldestRow.oldest ?? consumer.nextLocalSeq;
    if (oldest > consumer.cursor + 1) return { gap: { from: consumer.cursor + 1, to: oldest - 1 }, cursor_epoch: consumer.cursorEpoch, oldest_available_seq: oldest };
    const first = this.db.query("SELECT * FROM consumer_entries WHERE consumer_id=? AND local_seq>? ORDER BY local_seq LIMIT 1").get(id, consumer.cursor) as EntryRow | null;
    if (first?.kind === "hosted_gap") return { gap: { from: first.local_seq, to: first.local_seq }, cursor_epoch: consumer.cursorEpoch, oldest_available_seq: oldest };
    const nextGap = this.db.query(`SELECT MIN(local_seq) AS seq FROM consumer_entries
      WHERE consumer_id=? AND local_seq>? AND kind='hosted_gap'`).get(id, consumer.cursor) as { seq: number | null };
    const rows = this.db.query(`SELECT local_seq,kind,envelope_json FROM consumer_entries
      WHERE consumer_id=? AND local_seq>? AND local_seq<? AND kind='event' ORDER BY local_seq LIMIT ?`)
      .all(id, consumer.cursor, nextGap.seq ?? Number.MAX_SAFE_INTEGER, Math.max(1, Math.min(limit, 100))) as EntryRow[];
    return { events: rows.map((row) => JSON.parse(row.envelope_json!) as StoredEvent), cursor_epoch: consumer.cursorEpoch, oldest_available_seq: oldest };
  }

  async acknowledgeConsumer(id: string, seq: number, epoch: number): Promise<LocalCursorResult> {
    const consumer = this.getConsumerSync(id);
    if (!consumer) return { status: "not_found" };
    if (epoch !== consumer.cursorEpoch) return { status: "stale_epoch" };
    if (seq > consumer.nextLocalSeq - 1) return { status: "beyond_newest" };
    if (seq <= consumer.cursor) return { status: "unchanged", cursor: consumer.cursor, epoch: consumer.cursorEpoch };
    this.db.query("UPDATE consumer_streams SET cursor=?,last_seen_at=? WHERE consumer_id=?").run(seq, this.now().toISOString(), id);
    return { status: "advanced", cursor: seq, epoch: consumer.cursorEpoch };
  }

  async rewindConsumer(id: string, seq: number): Promise<{ cursor: number; epoch: number } | undefined> {
    const consumer = this.getConsumerSync(id);
    if (!consumer) return undefined;
    const cursor = Math.max(0, Math.min(seq, consumer.nextLocalSeq - 1));
    const epoch = consumer.cursorEpoch + 1;
    this.db.query("UPDATE consumer_streams SET cursor=?,cursor_epoch=? WHERE consumer_id=?").run(cursor, epoch, id);
    return { cursor, epoch };
  }

  async history(id: string, query: { source?: string; type?: string; subject?: string; sinceSeq?: number; limit?: number } = {}): Promise<LocalHistoryResult> {
    const consumer = this.getConsumerSync(id);
    if (!consumer) throw new LocalStoreError("not_found");
    const oldest = (this.db.query("SELECT MIN(local_seq) AS oldest FROM consumer_entries WHERE consumer_id=?").get(id) as { oldest: number | null }).oldest ?? consumer.nextLocalSeq;
    const rows = this.db.query(`SELECT envelope_json FROM consumer_entries WHERE consumer_id=? AND kind='event' AND local_seq>? ORDER BY local_seq`)
      .all(id, query.sinceSeq ?? 0) as Array<{ envelope_json: string }>;
    const all = rows.map((row) => JSON.parse(row.envelope_json) as StoredEvent)
      .filter((event) => (!query.source || event.source === query.source) && (!query.type || event.type === query.type) && (!query.subject || event.subject === query.subject));
    const events = all.slice(0, Math.max(1, Math.min(query.limit ?? 100, 1_000)));
    return { events, oldest_available_seq: oldest, ...(all.length > events.length && events.length ? { next_since_seq: events.at(-1)!.seq } : {}) };
  }

  async mintManagementToken(): Promise<string> {
    const minted = mintLocalToken("lmg");
    this.db.query(`INSERT INTO local_tokens(token_id,class,current_hash) VALUES('management','management',?)
      ON CONFLICT(token_id) DO UPDATE SET current_hash=excluded.current_hash,next_hash=NULL,overlap_until=NULL,revoked_at=NULL`).run(minted.hash);
    return minted.token;
  }

  private token(id: string): TokenRow | null {
    return this.db.query("SELECT current_hash,next_hash,overlap_until,revoked_at FROM local_tokens WHERE token_id=?").get(id) as TokenRow | null;
  }

  async authenticateManagement(token: string): Promise<boolean> {
    if (!hasValidLocalTokenChecksum(token, "lmg")) return false;
    const row = this.token("management");
    return Boolean(row && !row.revoked_at && hashesEqual(row.current_hash, hashToken(token)));
  }

  async authenticateConsumer(id: string, token: string): Promise<boolean> {
    if (!hasValidLocalTokenChecksum(token, "lcon")) return false;
    const row = this.token(`consume:${id}`);
    if (!row || row.revoked_at) return false;
    const hash = hashToken(token);
    return hashesEqual(row.current_hash, hash) || Boolean(row.next_hash && row.overlap_until && Date.parse(row.overlap_until) > this.now().getTime() && hashesEqual(row.next_hash, hash));
  }

  async rotateConsumerToken(id: string, overlapSeconds: number): Promise<string | undefined> {
    const row = this.token(`consume:${id}`);
    if (!row) return undefined;
    const minted = mintLocalToken("lcon");
    this.db.query(`UPDATE local_tokens SET current_hash=?,next_hash=?,overlap_until=?,revoked_at=NULL WHERE token_id=?`)
      .run(minted.hash, row.current_hash, new Date(this.now().getTime() + Math.max(0, overlapSeconds) * 1_000).toISOString(), `consume:${id}`);
    return minted.token;
  }

  async revokeConsumerToken(id: string): Promise<boolean> {
    return this.db.query("UPDATE local_tokens SET revoked_at=?,next_hash=NULL,overlap_until=NULL WHERE token_id=?").run(this.now().toISOString(), `consume:${id}`).changes > 0;
  }

  async applyRetention(policy: LocalRetentionPolicy): Promise<LocalRetentionResult> {
    let deleted = 0;
    const now = this.now().getTime();
    const pinCutoff = policy.pinningWindowMs === undefined ? undefined : new Date(now - policy.pinningWindowMs).toISOString();
    const canDelete = pinCutoff === undefined
      ? "1=1"
      : `(e.local_seq<=c.cursor OR (c.last_seen_at<? AND NOT (c.kind='action' AND EXISTS (
          SELECT 1 FROM action_registry a WHERE a.action_id=substr(c.consumer_id,8) AND a.enabled=1
        ))))`;
    const pinArguments = pinCutoff === undefined ? [] : [pinCutoff];
    if (policy.maxAgeMs !== undefined) {
      const cutoff = new Date(now - policy.maxAgeMs).toISOString();
      const result = this.db.query(`DELETE FROM consumer_entries WHERE rowid IN (
        SELECT e.rowid FROM consumer_entries e JOIN consumer_streams c ON c.consumer_id=e.consumer_id
        WHERE e.created_at<? AND ${canDelete} ORDER BY e.created_at,e.consumer_id,e.local_seq LIMIT 1000
      )`).run(cutoff, ...pinArguments);
      deleted += result.changes;
    }
    if (policy.maxCount !== undefined) {
      const victims = this.db.query(`SELECT subscription_id,hosted_cursor_epoch,hosted_seq FROM mirror_events
        WHERE rowid IN (SELECT rowid FROM mirror_events ORDER BY received_at DESC,hosted_cursor_epoch DESC,hosted_seq DESC LIMIT -1 OFFSET ?)
          AND NOT EXISTS (
            SELECT 1 FROM consumer_entries e JOIN consumer_streams c ON c.consumer_id=e.consumer_id
            WHERE e.subscription_id=mirror_events.subscription_id AND e.hosted_cursor_epoch=mirror_events.hosted_cursor_epoch
              AND e.hosted_seq=mirror_events.hosted_seq AND e.local_seq>c.cursor
              ${pinCutoff === undefined ? "AND 0" : `AND (c.last_seen_at>=? OR (c.kind='action' AND EXISTS (
                SELECT 1 FROM action_registry a WHERE a.action_id=substr(c.consumer_id,8) AND a.enabled=1)))`}
          ) ORDER BY received_at,hosted_cursor_epoch,hosted_seq LIMIT 1000`)
        .all(Math.max(0, policy.maxCount), ...(pinCutoff === undefined ? [] : [pinCutoff])) as Array<{
          subscription_id: string; hosted_cursor_epoch: number; hosted_seq: number;
        }>;
      for (const victim of victims) {
        deleted += this.db.query(`DELETE FROM consumer_entries WHERE subscription_id=? AND hosted_cursor_epoch=? AND hosted_seq=?`)
          .run(victim.subscription_id, victim.hosted_cursor_epoch, victim.hosted_seq).changes;
        this.db.query(`DELETE FROM mirror_events WHERE subscription_id=? AND hosted_cursor_epoch=? AND hosted_seq=?`)
          .run(victim.subscription_id, victim.hosted_cursor_epoch, victim.hosted_seq);
      }
    }
    const databaseBytes = () => {
      const pages = (this.db.query("PRAGMA page_count").get() as { page_count: number }).page_count;
      const pageSize = (this.db.query("PRAGMA page_size").get() as { page_size: number }).page_size;
      return pages * pageSize;
    };
    const sizeOverride = policy.maxBytes !== undefined && databaseBytes() > policy.maxBytes;
    if (sizeOverride) {
      const result = this.db.query(`DELETE FROM consumer_entries WHERE rowid IN (
        SELECT rowid FROM consumer_entries ORDER BY created_at,consumer_id,local_seq LIMIT 1000
      )`).run();
      deleted += result.changes;
      this.db.query("UPDATE dead_letters SET envelope_snapshot=NULL WHERE envelope_snapshot IS NOT NULL").run();
    }
    if (policy.deadLetterMaxAgeMs !== undefined) {
      this.db.query("DELETE FROM dead_letters WHERE created_at<?")
        .run(new Date(now - policy.deadLetterMaxAgeMs).toISOString());
    }
    if (policy.maxAgeMs !== undefined || sizeOverride) {
      const eventCutoff = sizeOverride ? "9999-12-31T23:59:59.999Z" : new Date(now - policy.maxAgeMs!).toISOString();
      this.db.query(`DELETE FROM mirror_events WHERE received_at<? AND NOT EXISTS (
        SELECT 1 FROM consumer_entries e WHERE e.subscription_id=mirror_events.subscription_id
          AND e.hosted_cursor_epoch=mirror_events.hosted_cursor_epoch AND e.hosted_seq=mirror_events.hosted_seq
      )`).run(eventCutoff);
      this.db.query(`DELETE FROM mirror_gaps WHERE created_at<? AND NOT EXISTS (
        SELECT 1 FROM consumer_entries e WHERE e.subscription_id=mirror_gaps.subscription_id
          AND e.hosted_cursor_epoch=mirror_gaps.hosted_cursor_epoch AND e.hosted_gap_from=mirror_gaps.hosted_from
          AND e.hosted_gap_to=mirror_gaps.hosted_to
      )`).run(eventCutoff);
    }
    if (deleted || sizeOverride) this.db.exec("PRAGMA incremental_vacuum(1000)");
    return { deleted, sizeOverride };
  }

  async putAction(action: LocalActionRecord): Promise<void> {
    this.db.query(`INSERT INTO action_registry(action_id,file_path,definition_json,enabled,digest,load_error,loaded_at) VALUES(?,?,?,?,?,?,?)
      ON CONFLICT(action_id) DO UPDATE SET file_path=excluded.file_path,definition_json=excluded.definition_json,
        enabled=excluded.enabled,digest=excluded.digest,load_error=excluded.load_error,loaded_at=excluded.loaded_at`)
      .run(action.id, action.filePath ?? null, stringify(action.definition), action.enabled ? 1 : 0, action.digest, action.loadError ?? null, action.loadedAt ?? this.now().toISOString());
  }

  async listActions(): Promise<LocalActionRecord[]> {
    return (this.db.query("SELECT * FROM action_registry ORDER BY action_id").all() as Array<{
      action_id: string; file_path: string | null; definition_json: string; enabled: number; digest: string; load_error: string | null; loaded_at: string | null;
    }>).map((row) => ({
      id: row.action_id, definition: JSON.parse(row.definition_json), enabled: row.enabled === 1, digest: row.digest,
      ...(row.file_path ? { filePath: row.file_path } : {}), ...(row.load_error ? { loadError: row.load_error } : {}),
      ...(row.loaded_at ? { loadedAt: row.loaded_at } : {}),
    }));
  }

  async getAction(id: string): Promise<LocalActionRecord | undefined> {
    return (await this.listActions()).find((action) => action.id === id);
  }

  async deleteAction(id: string): Promise<boolean> {
    return this.db.query("DELETE FROM action_registry WHERE action_id=?").run(id).changes > 0;
  }

  async putActionAttempt(value: LocalActionAttempt): Promise<void> {
    this.db.query(`INSERT OR REPLACE INTO action_attempts
      (action_id,local_seq,replay_generation,attempt,state,due_at,started_at,finished_at,exit_status,signal,stdout_tail,stderr_tail)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`).run(
      value.actionId, value.localSeq, value.replayGeneration, value.attempt, value.state, value.dueAt,
      value.startedAt ?? null, value.finishedAt ?? null, value.exitStatus ?? null, value.signal ?? null,
      value.stdoutTail ?? null, value.stderrTail ?? null,
    );
  }

  async listActionAttempts(actionId: string, localSeq: number): Promise<LocalActionAttempt[]> {
    return (this.db.query(`SELECT * FROM action_attempts WHERE action_id=? AND local_seq=?
      ORDER BY replay_generation,attempt`).all(actionId, localSeq) as Array<Record<string, unknown>>).map((row) => ({
      actionId: String(row.action_id), localSeq: Number(row.local_seq), replayGeneration: Number(row.replay_generation), attempt: Number(row.attempt),
      state: String(row.state) as LocalActionAttempt["state"], dueAt: String(row.due_at),
      ...(row.started_at ? { startedAt: String(row.started_at) } : {}), ...(row.finished_at ? { finishedAt: String(row.finished_at) } : {}),
      ...(row.exit_status !== null ? { exitStatus: Number(row.exit_status) } : {}), ...(row.signal ? { signal: String(row.signal) } : {}),
      ...(row.stdout_tail ? { stdoutTail: String(row.stdout_tail) } : {}), ...(row.stderr_tail ? { stderrTail: String(row.stderr_tail) } : {}),
    }));
  }

  async listRecentActionAttempts(actionId: string, since: string): Promise<LocalActionAttempt[]> {
    return (this.db.query(`SELECT * FROM action_attempts WHERE action_id=? AND COALESCE(started_at,due_at)>=?
      ORDER BY COALESCE(started_at,due_at),local_seq,replay_generation,attempt`).all(actionId, since) as Array<Record<string, unknown>>)
      .map((row) => ({
        actionId: String(row.action_id), localSeq: Number(row.local_seq), replayGeneration: Number(row.replay_generation), attempt: Number(row.attempt),
        state: String(row.state) as LocalActionAttempt["state"], dueAt: String(row.due_at),
        ...(row.started_at ? { startedAt: String(row.started_at) } : {}), ...(row.finished_at ? { finishedAt: String(row.finished_at) } : {}),
        ...(row.exit_status !== null ? { exitStatus: Number(row.exit_status) } : {}), ...(row.signal ? { signal: String(row.signal) } : {}),
        ...(row.stdout_tail ? { stdoutTail: String(row.stdout_tail) } : {}), ...(row.stderr_tail ? { stderrTail: String(row.stderr_tail) } : {}),
      }));
  }

  async putDeadLetter(value: LocalDeadLetter): Promise<void> {
    this.db.query(`INSERT OR REPLACE INTO dead_letters(id,action_id,local_seq,replay_generation,envelope_snapshot,summary_json,created_at,replayed_at)
      VALUES(?,?,?,?,?,?,?,?)`).run(value.id, value.actionId, value.localSeq, value.replayGeneration ?? 0,
      value.envelopeSnapshot ? stringify(value.envelopeSnapshot) : null, stringify(value.summary), value.createdAt, value.replayedAt ?? null);
  }

  async getDeadLetter(id: string): Promise<LocalDeadLetter | undefined> {
    return (await this.listDeadLetters()).find((deadLetter) => deadLetter.id === id);
  }

  async listDeadLetters(): Promise<LocalDeadLetter[]> {
    return (this.db.query("SELECT * FROM dead_letters ORDER BY created_at").all() as Array<{ id: string; action_id: string; local_seq: number; replay_generation: number; envelope_snapshot: string | null; summary_json: string; created_at: string; replayed_at: string | null }>)
      .map((row) => ({ id: row.id, actionId: row.action_id, localSeq: row.local_seq, replayGeneration: row.replay_generation,
        ...(row.envelope_snapshot ? { envelopeSnapshot: JSON.parse(row.envelope_snapshot) as StoredEvent } : {}), summary: JSON.parse(row.summary_json),
        createdAt: row.created_at, ...(row.replayed_at ? { replayedAt: row.replayed_at } : {}) }));
  }

  async markDeadLetterReplayed(id: string, at: string): Promise<boolean> {
    return this.db.query("UPDATE dead_letters SET replayed_at=? WHERE id=?").run(at, id).changes > 0;
  }

  async putRecoveryNotice(value: LocalRecoveryNotice): Promise<void> {
    this.db.query(`INSERT OR REPLACE INTO recovery_notices
      (subscription_id,recovered_at,hosted_cursor,hosted_cursor_epoch,oldest_available_seq,acknowledged_at)
      VALUES(?,?,?,?,?,?)`).run(value.subscriptionId, value.recoveredAt, value.hostedCursor, value.hostedCursorEpoch,
      value.oldestAvailableSeq, value.acknowledgedAt ?? null);
  }

  async listRecoveryNotices(): Promise<LocalRecoveryNotice[]> {
    return (this.db.query("SELECT * FROM recovery_notices ORDER BY subscription_id").all() as Array<{
      subscription_id: string; recovered_at: string; hosted_cursor: number; hosted_cursor_epoch: number; oldest_available_seq: number; acknowledged_at: string | null;
    }>).map((row) => ({
      subscriptionId: row.subscription_id, recoveredAt: row.recovered_at, hostedCursor: row.hosted_cursor,
      hostedCursorEpoch: row.hosted_cursor_epoch, oldestAvailableSeq: row.oldest_available_seq,
      ...(row.acknowledged_at ? { acknowledgedAt: row.acknowledged_at } : {}),
    }));
  }

  async setConsumerFilter(id: string, filter: Filter | undefined): Promise<void> {
    if (this.db.query("UPDATE consumer_streams SET filter_json=? WHERE consumer_id=?").run(filter ? stringify(filter) : null, id).changes === 0) throw new LocalStoreError("not_found");
  }

  async debugEventCount(): Promise<number> {
    return (this.db.query("SELECT COUNT(*) AS count FROM mirror_events").get() as { count: number }).count;
  }

  async debugMappingCount(consumerId: string): Promise<number> {
    return (this.db.query("SELECT COUNT(*) AS count FROM consumer_entries WHERE consumer_id=?").get(consumerId) as { count: number }).count;
  }

  async debugEvent(consumerId: string, localSeq: number): Promise<StoredEvent | undefined> {
    const row = this.db.query("SELECT envelope_json FROM consumer_entries WHERE consumer_id=? AND local_seq=? AND kind='event'").get(consumerId, localSeq) as { envelope_json: string } | null;
    return row ? JSON.parse(row.envelope_json) as StoredEvent : undefined;
  }
}
