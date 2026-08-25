import { describe, expect, it } from "bun:test";
import { MemoryLocalStore } from "../src/daemon/memory-store";
import { SqliteLocalStore } from "../src/daemon/sqlite-store";
import type { LocalStore } from "../src/daemon/store";
import type { HostedEventsFrame, LocalCommitPhase } from "../src/daemon/types";
import type { StoredEvent } from "../src/domain";

function event(seq: number, id = `event-${seq}`, source = "source/a"): StoredEvent {
  return {
    specversion: "1.0",
    id,
    source,
    sourceId: source,
    type: "dev.bugler.test",
    seq,
    receivedat: new Date(Date.parse("2026-08-25T00:00:00Z") + seq).toISOString(),
    verificationmethod: "none",
    payloadverified: false,
    rawSize: 1,
    data: { id },
  };
}

async function seeded(store: LocalStore): Promise<{ token: string }> {
  await store.createMirror("hosted");
  const created = await store.createConsumer({ id: "agent", mirrors: ["hosted"], start: "latest" });
  return { token: created.token };
}

describe("memory local store", () => {
  it("commits mirror events and dense consumer mappings before returning the hosted ack", async () => {
    const store = new MemoryLocalStore();
    await seeded(store);
    const ack = await store.commitHostedFrame({ kind: "events", subscriptionId: "hosted", cursorEpoch: 0, events: [event(1), event(2)] });
    expect(ack).toEqual({ subscriptionId: "hosted", seq: 2, epoch: 0 });
    expect(await store.debugEventCount()).toBe(2);
    expect(await store.debugMappingCount("agent")).toBe(2);
    expect(await store.readConsumer("agent")).toMatchObject({
      events: [expect.objectContaining({ seq: 1, id: "event-1" }), expect.objectContaining({ seq: 2, id: "event-2" })],
      cursor_epoch: 0,
      oldest_available_seq: 1,
    });
  });

  for (const phase of ["after_events", "after_mappings"] as LocalCommitPhase[]) {
    it(`rolls back without an ack when a commit fails at ${phase}`, async () => {
      const store = new MemoryLocalStore({ fault: (at) => { if (at === phase) throw new Error("injected"); } });
      await seeded(store);
      await expect(store.commitHostedFrame({ kind: "events", subscriptionId: "hosted", cursorEpoch: 0, events: [event(1)] })).rejects.toThrow("injected");
      expect(await store.debugEventCount()).toBe(0);
      expect(await store.debugMappingCount("agent")).toBe(0);
      expect((await store.listMirrors())[0]?.receivedCursor).toBe(0);
    });
  }

  it("deduplicates redelivery after commit-before-ack failure", async () => {
    let fail = true;
    const store = new MemoryLocalStore({ fault: (at) => { if (at === "after_commit" && fail) throw new Error("ack path died"); } });
    await seeded(store);
    const frame: HostedEventsFrame = { kind: "events", subscriptionId: "hosted", cursorEpoch: 0, events: [event(1)] };
    await expect(store.commitHostedFrame(frame)).rejects.toThrow("ack path died");
    fail = false;
    expect(await store.commitHostedFrame(frame)).toEqual({ subscriptionId: "hosted", seq: 1, epoch: 0 });
    expect(await store.debugEventCount()).toBe(1);
    expect(await store.debugMappingCount("agent")).toBe(1);
  });

  it("allocates new local positions for rewind epochs and fails stop on epoch regression", async () => {
    const store = new MemoryLocalStore();
    await seeded(store);
    await store.commitHostedFrame({ kind: "events", subscriptionId: "hosted", cursorEpoch: 0, events: [event(1, "A")] });
    await store.commitHostedFrame({ kind: "events", subscriptionId: "hosted", cursorEpoch: 1, events: [event(1, "A")] });
    expect(await store.debugMappingCount("agent")).toBe(2);
    expect((await store.debugEvent("agent", 2))?.id).toBe("A");
    await expect(store.commitHostedFrame({ kind: "events", subscriptionId: "hosted", cursorEpoch: 0, events: [event(2)] }))
      .rejects.toEqual(expect.objectContaining({ code: "epoch_regression" }));
  });

  it("maps A-B-A as three deliveries and turns hosted gaps into one local gap position", async () => {
    const store = new MemoryLocalStore();
    await seeded(store);
    await store.commitHostedFrame({ kind: "events", subscriptionId: "hosted", cursorEpoch: 0, events: [event(1, "A"), event(2, "B"), event(3, "A")] });
    expect((await store.history("agent")).events.map((item) => item.id)).toEqual(["A", "B", "A"]);
    await store.commitHostedFrame({ kind: "gap", subscriptionId: "hosted", cursorEpoch: 0, gap: { from: 4, to: 9 } });
    expect(await store.acknowledgeConsumer("agent", 3, 0)).toMatchObject({ status: "advanced" });
    expect(await store.readConsumer("agent")).toEqual({ gap: { from: 4, to: 4 }, cursor_epoch: 0, oldest_available_seq: 1 });
  });
});

for (const [name, factory] of [
  ["memory", () => new MemoryLocalStore()],
  ["sqlite", () => new SqliteLocalStore(":memory:")],
] as const) {
  describe(`${name} local gap ordering`, () => {
    it("stops an unread event batch at an interleaved hosted gap", async () => {
      const store = factory();
      await store.createMirror("hosted");
      const consumer = await store.createConsumer({ id: "agent", mirrors: ["hosted"] });
      await store.commitHostedFrame({ kind: "events", subscriptionId: "hosted", cursorEpoch: 0, events: [event(1)] });
      await store.commitHostedFrame({ kind: "gap", subscriptionId: "hosted", cursorEpoch: 0, gap: { from: 2, to: 2 } });
      await store.commitHostedFrame({ kind: "events", subscriptionId: "hosted", cursorEpoch: 0, events: [event(3)] });

      expect(await store.readConsumer("agent")).toMatchObject({ events: [{ seq: 1 }] });
      await store.acknowledgeConsumer("agent", 1, consumer.consumer.cursorEpoch);
      expect(await store.readConsumer("agent")).toEqual({ gap: { from: 2, to: 2 }, cursor_epoch: 0, oldest_available_seq: 1 });
      await store.acknowledgeConsumer("agent", 2, 0);
      expect(await store.readConsumer("agent")).toMatchObject({ events: [{ seq: 3 }] });
      await store.close();
    });

    it("backfills numeric hosted order and existing gaps without duplication", async () => {
      const store = factory();
      await store.createMirror("hosted");
      await store.commitHostedFrame({ kind: "events", subscriptionId: "hosted", cursorEpoch: 0, events: Array.from({ length: 5 }, (_, index) => event(index + 1)) });
      await store.commitHostedFrame({ kind: "gap", subscriptionId: "hosted", cursorEpoch: 0, gap: { from: 6, to: 9 } });
      await store.commitHostedFrame({ kind: "events", subscriptionId: "hosted", cursorEpoch: 0, events: [event(10), event(11), event(12)] });
      const created = await store.createConsumer({ id: "backfill", mirrors: ["hosted"], start: "oldest" });
      while ((await store.advanceBackfills(2)) > 0) {}
      expect((await store.readConsumer("backfill", 20) as { events: StoredEvent[] }).events.map((item) => item.id))
        .toEqual(["event-1", "event-2", "event-3", "event-4", "event-5"]);
      await store.acknowledgeConsumer("backfill", 5, created.consumer.cursorEpoch);
      expect(await store.readConsumer("backfill")).toEqual({ gap: { from: 6, to: 6 }, cursor_epoch: 0, oldest_available_seq: 1 });
      await store.acknowledgeConsumer("backfill", 6, 0);
      expect((await store.readConsumer("backfill", 20) as { events: StoredEvent[] }).events.map((item) => item.id))
        .toEqual(["event-10", "event-11", "event-12"]);
      await store.close();
    });
  });
}
