import { mkdtemp, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "bun:test";
import { SqliteLocalStore } from "../src/daemon/sqlite-store";
import type { LocalCommitPhase } from "../src/daemon/types";
import type { StoredEvent } from "../src/domain";

const scratch: string[] = [];

afterEach(async () => {
  for (const path of scratch.splice(0)) await rm(path, { recursive: true, force: true });
});

async function database(): Promise<{ store: SqliteLocalStore; path: string }> {
  const directory = await mkdtemp(join(tmpdir(), "bugler-daemon-store-"));
  scratch.push(directory);
  const path = join(directory, "daemon.db");
  return { store: new SqliteLocalStore(path), path };
}

function event(seq: number, id = `event-${seq}`, bytes = 1): StoredEvent {
  return {
    specversion: "1.0",
    id,
    source: "source/a",
    sourceId: "source/a",
    type: "dev.bugler.test",
    seq,
    receivedat: new Date(Date.parse("2026-08-25T00:00:00Z") + seq).toISOString(),
    verificationmethod: "none",
    payloadverified: false,
    rawSize: bytes,
    data: { value: "x".repeat(bytes) },
  };
}

describe("sqlite local store", () => {
  it("installs the normative daemon schema and local-only management mirror", async () => {
    const store = new SqliteLocalStore(":memory:", { accountKey: "account-test" });
    const columns = (table: string) => (store.db.query(`PRAGMA table_info(${table})`).all() as Array<{ name: string }>).map((row) => row.name);
    expect(columns("daemon_meta")).toEqual(expect.arrayContaining(["account_key", "created_at"]));
    expect(columns("mirror_subscriptions")).toContain("token_auth_version");
    expect(columns("consumer_streams")).toEqual(expect.arrayContaining(["kind", "backfill_subscription_id", "backfill_hosted_cursor_epoch", "backfill_hosted_seq"]));
    expect(columns("action_registry")).toEqual(expect.arrayContaining(["file_path", "load_error", "loaded_at"]));
    expect(columns("action_attempts")).toEqual(expect.arrayContaining(["started_at", "finished_at", "exit_status", "signal", "stdout_tail", "stderr_tail"]));
    expect(columns("dead_letters")).toEqual(expect.arrayContaining(["replay_generation", "replayed_at"]));
    expect(await store.listMirrors()).toContainEqual(expect.objectContaining({ id: "internal:management", state: "local_only" }));
    store.close();
  });
  it("asserts durable pragmas, schema indexes, and persists the mirror mapping", async () => {
    const { store, path } = await database();
    expect((store.db.query("PRAGMA journal_mode").get() as { journal_mode: string }).journal_mode).toBe("wal");
    expect((store.db.query("PRAGMA synchronous").get() as { synchronous: number }).synchronous).toBe(2);
    expect((store.db.query("PRAGMA auto_vacuum").get() as { auto_vacuum: number }).auto_vacuum).toBe(2);
    await store.createMirror("hosted");
    const created = await store.createConsumer({ id: "agent", mirrors: ["hosted"] });
    await store.commitHostedFrame({ kind: "events", subscriptionId: "hosted", cursorEpoch: 0, events: [event(1), event(2)] });
    const management = await store.mintManagementToken();
    expect(await store.authenticateManagement(management)).toBe(true);
    expect(await store.authenticateConsumer("agent", created.token)).toBe(true);
    store.close();

    const reopened = new SqliteLocalStore(path);
    expect(await reopened.debugEventCount()).toBe(2);
    expect(await reopened.debugMappingCount("agent")).toBe(2);
    expect(await reopened.readConsumer("agent")).toMatchObject({ events: [expect.objectContaining({ seq: 1 }), expect.objectContaining({ seq: 2 })] });
    const plans = JSON.stringify([
      reopened.db.query("EXPLAIN QUERY PLAN SELECT * FROM local_tokens WHERE current_hash=?").all("hash"),
      reopened.db.query("EXPLAIN QUERY PLAN SELECT * FROM local_tokens WHERE next_hash=?").all("hash"),
    ]);
    expect(plans).toContain("local_tokens_current_hash");
    expect(plans).toContain("local_tokens_next_hash");
    reopened.close();
  });

  for (const phase of ["after_events", "after_mappings", "after_commit"] as LocalCommitPhase[]) {
    it(`survives SIGKILL at ${phase} without violating commit-before-ACK`, async () => {
      const { store, path } = await database();
      store.close();
      const child = Bun.spawn([process.execPath, join(import.meta.dir, "fixtures/daemon-crash-worker.ts"), path, phase], {
        stdout: "pipe",
        stderr: "pipe",
      });
      expect(await child.exited).not.toBe(0);
      const reopened = new SqliteLocalStore(path);
      const committed = phase === "after_commit";
      expect(await reopened.debugEventCount()).toBe(committed ? 1 : 0);
      expect(await reopened.debugMappingCount("agent")).toBe(committed ? 1 : 0);
      if (committed) {
        expect(await reopened.commitHostedFrame({ kind: "events", subscriptionId: "hosted", cursorEpoch: 0, events: [event(1, "crash-event")] }))
          .toEqual({ subscriptionId: "hosted", seq: 1, epoch: 0 });
        expect(await reopened.debugMappingCount("agent")).toBe(1);
      }
      reopened.close();
    });
  }

  it("rolls back a real SQLITE_FULL failure and returns no durable ack state", async () => {
    const { store } = await database();
    await store.createMirror("hosted");
    await store.createConsumer({ id: "agent", mirrors: ["hosted"] });
    store.db.exec("PRAGMA wal_checkpoint(TRUNCATE)");
    const pages = (store.db.query("PRAGMA page_count").get() as { page_count: number }).page_count;
    store.db.exec(`PRAGMA max_page_count=${pages + 1}`);
    await expect(store.commitHostedFrame({ kind: "events", subscriptionId: "hosted", cursorEpoch: 0, events: [event(1, "large", 128 * 1024)] }))
      .rejects.toThrow();
    expect(await store.debugEventCount()).toBe(0);
    expect(await store.debugMappingCount("agent")).toBe(0);
    expect((await store.listMirrors())[0]?.receivedCursor).toBe(0);
    store.close();
  });

  it("keeps rewind epochs distinct and persists action and dead-letter state", async () => {
    const { store, path } = await database();
    await store.createMirror("hosted");
    await store.createConsumer({ id: "agent", mirrors: ["hosted"] });
    await store.commitHostedFrame({ kind: "events", subscriptionId: "hosted", cursorEpoch: 0, events: [event(1, "same")] });
    await store.commitHostedFrame({ kind: "events", subscriptionId: "hosted", cursorEpoch: 1, events: [event(1, "same")] });
    expect(await store.debugMappingCount("agent")).toBe(2);
    await expect(store.commitHostedFrame({ kind: "events", subscriptionId: "hosted", cursorEpoch: 0, events: [event(2)] })).rejects.toMatchObject({ code: "epoch_regression" });
    expect((await store.listMirrors())[0]?.state).toBe("epoch_regression");
    await store.putAction({ id: "wake", enabled: true, digest: "abc", definition: { run: ["/bin/true"] } });
    await store.putDeadLetter({ id: "dead-1", actionId: "wake", localSeq: 2, envelopeSnapshot: { ...event(1), seq: 2 }, summary: { exit: 1 }, createdAt: "2026-08-25T00:00:00Z" });
    store.close();
    const reopened = new SqliteLocalStore(path);
    expect(await reopened.listActions()).toEqual([
      expect.objectContaining({ id: "wake", enabled: true, digest: "abc", definition: { run: ["/bin/true"] } }),
    ]);
    expect(await reopened.listDeadLetters()).toEqual([expect.objectContaining({ id: "dead-1", localSeq: 2, envelopeSnapshot: expect.objectContaining({ seq: 2 }) })]);
    reopened.close();
  });

  it("creates only verified rotating online backups", async () => {
    const { store, path } = await database();
    await store.createMirror("hosted");
    await store.commitHostedFrame({ kind: "events", subscriptionId: "hosted", cursorEpoch: 0, events: [event(1)] });
    const first = `${path}.backup-1`;
    const second = `${path}.backup-2`;
    await store.createVerifiedBackup(first, second);
    expect((await stat(first)).isFile()).toBe(true);
    await store.commitHostedFrame({ kind: "events", subscriptionId: "hosted", cursorEpoch: 0, events: [event(2)] });
    await store.createVerifiedBackup(first, second);
    expect((await stat(second)).isFile()).toBe(true);
    const previous = new SqliteLocalStore(second);
    expect(await previous.debugEventCount()).toBe(1);
    previous.close();
    store.close();
  });
});
