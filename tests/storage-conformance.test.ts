import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "bun:test";
import { MemoryStorageDriver } from "../src/storage/memory";
import { SQLITE_RETENTION_AGE_DELETE_SQL, SqliteStorageDriver } from "../src/storage/sqlite";
import { storageConformanceCases, type StorageFactory } from "./storage-conformance-suite";

const scratch: string[] = [];

afterEach(async () => {
  for (const path of scratch.splice(0)) await rm(path, { recursive: true, force: true });
});

async function memory() {
  return new MemoryStorageDriver();
}

async function sqlite() {
  const directory = await mkdtemp(join(tmpdir(), "bugler-storage-"));
  scratch.push(directory);
  return new SqliteStorageDriver(join(directory, "bugler.db"));
}

function conformance(name: string, factory: StorageFactory): void {
  describe(`${name} storage conformance`, () => {
    for (const testCase of storageConformanceCases) it(testCase.name, () => testCase.run(factory));
  });
}

conformance("memory", memory);
conformance("sqlite", sqlite);

describe("sqlite startup", () => {
  it("asserts WAL, FULL synchronous mode, and incremental auto-vacuum", async () => {
    const driver = await sqlite();
    expect((driver.db.query("PRAGMA journal_mode").get() as { journal_mode: string }).journal_mode).toBe("wal");
    expect((driver.db.query("PRAGMA synchronous").get() as { synchronous: number }).synchronous).toBe(2);
    expect((driver.db.query("PRAGMA auto_vacuum").get() as { auto_vacuum: number }).auto_vacuum).toBe(2);
    driver.close();
  });

  it("uses the intended indexes for the normative query shapes", async () => {
    const driver = await sqlite();
    const plans = [
      driver.db.query("EXPLAIN QUERY PLAN SELECT * FROM events WHERE seq > ? ORDER BY seq LIMIT 10").all(1),
      driver.db.query("EXPLAIN QUERY PLAN SELECT * FROM events WHERE source = ? AND subject = ? ORDER BY seq LIMIT 10").all("s", "x"),
      driver.db.query("EXPLAIN QUERY PLAN SELECT * FROM events WHERE source = ? AND type = ? ORDER BY seq LIMIT 10").all("s", "t"),
      driver.db.query("EXPLAIN QUERY PLAN SELECT * FROM events WHERE received_at >= ?").all("2026-01-01T00:00:00Z"),
    ].map((rows) => JSON.stringify(rows));
    expect(plans[0]).toContain("PRIMARY KEY");
    expect(plans[1]).toContain("events_source_subject");
    expect(plans[2]).toContain("events_source_type");
    expect(plans[3]).toContain("events_received");
    const retentionPlan = JSON.stringify(driver.db.query(`EXPLAIN QUERY PLAN ${SQLITE_RETENTION_AGE_DELETE_SQL}`).all(
      "2026-01-01T00:00:00Z",
      Number.MAX_SAFE_INTEGER,
    ));
    expect(retentionPlan).toContain("events_received");
    driver.close();
  });
});
