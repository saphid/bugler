import { afterEach, describe, expect, it } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { MemoryLocalStore } from "../src/daemon/memory-store";
import { LOCAL_RETENTION_DEFAULTS, startLocalRetention } from "../src/daemon/retention";
import { SqliteLocalStore } from "../src/daemon/sqlite-store";
import type { LocalStore } from "../src/daemon/store";
import type { StoredEvent } from "../src/domain";

const scratch: string[] = [];
afterEach(async () => { for (const path of scratch.splice(0)) await rm(path, { recursive: true, force: true }); });

function event(seq: number): StoredEvent {
  return {
    specversion: "1.0", id: `retention-${seq}`, source: "source/retention", sourceId: "source/retention", type: "dev.bugler.retention",
    seq, receivedat: new Date(Date.parse("2026-08-25T00:00:00Z") + seq).toISOString(), verificationmethod: "none", payloadverified: false,
    rawSize: 512, data: { payload: "x".repeat(512) },
  };
}

async function stores(now?: () => Date): Promise<Array<{ name: string; store: LocalStore }>> {
  const directory = await mkdtemp(join(tmpdir(), "bugler-local-retention-"));
  scratch.push(directory);
  return [
    { name: "memory", store: new MemoryLocalStore({ ...(now ? { now } : {}) }) },
    { name: "sqlite", store: new SqliteLocalStore(join(directory, "daemon.db"), { ...(now ? { now } : {}) }) },
  ];
}

describe("daemon local retention", () => {
  it("publishes the normative section-14 defaults", () => {
    expect(LOCAL_RETENTION_DEFAULTS).toEqual({
      maxAgeMs: 30 * 24 * 60 * 60_000,
      maxCount: 1_000_000,
      maxBytes: 2 * 1024 * 1024 * 1024,
      pinningWindowMs: 7 * 24 * 60 * 60_000,
      deadLetterMaxAgeMs: 30 * 24 * 60 * 60_000,
    });
  });

  it("runs bounded scheduled slices and surfaces a size-override gap on both stores", async () => {
    for (const test of await stores()) {
      await test.store.createMirror("hosted");
      await test.store.createConsumer({ id: "agent", mirrors: ["hosted"] });
      await test.store.commitHostedFrame({ kind: "events", subscriptionId: "hosted", cursorEpoch: 0, events: [event(1), event(2)] });
      let scheduled: (() => void) | undefined;
      let result: { deleted: number; sizeOverride: boolean } | undefined;
      const retention = startLocalRetention(test.store, {
        policy: { ...LOCAL_RETENTION_DEFAULTS, maxBytes: 1 },
        setInterval: (callback) => { scheduled = callback; return 1 as unknown as ReturnType<typeof setInterval>; },
        clearInterval: () => undefined,
        onResult: (value) => { result = value; },
      });
      expect(scheduled, test.name).toBeDefined();
      scheduled!();
      await retention.whenIdle();
      expect(result, test.name).toMatchObject({ sizeOverride: true });
      expect(result!.deleted, test.name).toBeGreaterThan(0);
      expect(await test.store.readConsumer("agent"), test.name).toEqual({
        gap: { from: 1, to: 2 }, cursor_epoch: 0, oldest_available_seq: 3,
      });
      retention.stop();
      await test.store.close();
    }
  });

  it("enforces the global mirror-event count and emits gaps for stale unacked consumers", async () => {
    let now = Date.parse("2026-08-01T00:00:00.000Z");
    for (const test of await stores(() => new Date(now))) {
      const selected = test.store;
      await selected.createMirror("mirror");
      await selected.createConsumer({ id: "stale", mirrors: ["mirror"] });
      await selected.commitHostedFrame({ kind: "events", subscriptionId: "mirror", cursorEpoch: 0, events: [event(1), event(2), event(3)] });
      now += LOCAL_RETENTION_DEFAULTS.pinningWindowMs + 1;
      const retained = await selected.applyRetention({ maxCount: 1, pinningWindowMs: LOCAL_RETENTION_DEFAULTS.pinningWindowMs });
      expect(retained.deleted).toBe(2);
      expect(await selected.debugEventCount()).toBe(1);
      expect(await selected.readConsumer("stale")).toEqual({ gap: { from: 1, to: 2 }, cursor_epoch: 0, oldest_available_seq: 3 });
      await selected.close();
    }
  });
});
