import { createApp } from "../src/app";
import type { AppendEventInput, Source, Subscription } from "../src/domain";
import type { SecretResolver } from "../src/secrets";
import type { StorageDriver } from "../src/storage/driver";

export type StorageFactory = () => Promise<StorageDriver>;

export const conformanceSource: Source = {
  id: "source-1",
  kind: "http",
  verification: { primitive: "none" },
  mapping: { source: "test/source", type: { static: "test.changed" } },
  createdAt: "2026-08-23T00:00:00.000Z",
};

export function conformanceSubscription(id = "sub-1"): Subscription {
  return {
    id,
    filter: { exact: { source: "test/source" } },
    transport: { type: "pull" },
    tokenHash: "hash",
    tokenPrefix: "bgl_sub_test",
    cursor: 0,
    cursorEpoch: 0,
    push: { failureCount: 0, parked: 0 },
    createdAt: "2026-08-23T00:00:00.000Z",
    lastSeenAt: "2026-08-23T00:00:00.000Z",
  };
}

export function conformanceEvent(id: string, dedupekey?: string, receivedat = "2026-08-23T00:00:00.000Z"): AppendEventInput {
  const value: AppendEventInput = {
    specversion: "1.0",
    id,
    sourceId: conformanceSource.id,
    source: conformanceSource.mapping.source,
    type: "test.changed",
    subject: "thing/1",
    receivedat,
    verificationmethod: "none",
    payloadverified: false,
    datacontenttype: "application/json",
    data: { id },
    rawSize: 16,
  };
  if (dedupekey !== undefined) value.dedupekey = dedupekey;
  return value;
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

function equal(actual: unknown, expected: unknown, message: string): void {
  const left = JSON.stringify(actual);
  const right = JSON.stringify(expected);
  if (left !== right) throw new Error(`${message}: expected ${right}, received ${left}`);
}

function payloadDataAtSerializedSize(size: number, escapeHeavy: boolean): { payload: string } {
  const encoder = new TextEncoder();
  const emptySize = encoder.encode(JSON.stringify({ payload: "" })).byteLength;
  assert(size >= emptySize, "payload boundary must fit the JSON wrapper");
  const unit = escapeHeavy ? "\"\\" : "x";
  const unitSize = encoder.encode(JSON.stringify({ payload: unit })).byteLength - emptySize;
  const units = Math.floor((size - emptySize) / unitSize);
  const payload = unit.repeat(units);
  const used = encoder.encode(JSON.stringify({ payload })).byteLength;
  const data = { payload: payload + "x".repeat(size - used) };
  equal(encoder.encode(JSON.stringify(data)).byteLength, size, "boundary fixture must have the advertised raw JSON size");
  return data;
}

const noSecrets: SecretResolver = {
  async resolve(reference: string): Promise<string> {
    throw new Error(`unexpected secret resolution: ${reference}`);
  },
};

export const storageConformanceCases: Array<{ name: string; run: (factory: StorageFactory) => Promise<void> }> = [
  {
    name: "allocates monotonic sequences and returns the existing deduplicated row",
    async run(factory) {
      const driver = await factory();
      try {
        await driver.putSource(conformanceSource);
        const first = await driver.appendEvent(conformanceEvent("id-1", "same"));
        const duplicate = await driver.appendEvent(conformanceEvent("id-2", "same"));
        const second = await driver.appendEvent(conformanceEvent("id-3", "other"));
        assert(first.inserted, "first event must be inserted");
        equal(duplicate, { event: first.event, inserted: false }, "dedupe must return the existing event");
        assert(second.event.seq > first.event.seq, "sequence must increase");
        await driver.enforceRetention({ maxCount: 0 }, "2026-08-24T00:00:00.000Z");
        equal(await driver.newestSeq(), second.event.seq, "high-water sequence must survive retention");
        const third = await driver.appendEvent(conformanceEvent("id-4", "third"));
        assert(third.event.seq > second.event.seq, "deleted sequences must never be reused");
      } finally {
        await driver.close();
      }
    },
  },
  {
    name: "acknowledges every event in a concurrent append storm",
    async run(factory) {
      const driver = await factory();
      try {
        const results = await Promise.all(Array.from({ length: 16 }, (_, index) =>
          driver.appendEvent(conformanceEvent(`storm-${index}`, `storm-${index}`))));
        assert(results.every((result) => result.inserted), "every distinct storm event must be inserted");
        const sequences = results.map((result) => result.event.seq);
        equal(new Set(sequences).size, 16, "every acknowledged storm event must have a unique sequence");
        const stored = await driver.queryEvents({ limit: 100 });
        equal(new Set(stored.map((event) => event.id)).size, 16, "every acknowledged storm event must be retrievable");
        equal(await driver.newestSeq(), Math.max(...sequences), "the counter high-water mark must match acknowledged sequences");
        const measured = await driver.measureSizeBytes();
        const retained = await driver.enforceRetention({}, "2026-08-23T00:00:00.000Z");
        equal(retained.sizeBytes, measured, "retention and direct size measurement must agree");
      } finally {
        await driver.close();
      }
    },
  },
  {
    name: "collapses a concurrent dedupe storm to one stored event",
    async run(factory) {
      const driver = await factory();
      try {
        const results = await Promise.all(Array.from({ length: 12 }, (_, index) =>
          driver.appendEvent(conformanceEvent(`dedupe-storm-${index}`, "one-key"))));
        equal(results.filter((result) => result.inserted).length, 1, "exactly one dedupe contender must insert");
        equal(new Set(results.map((result) => result.event.seq)).size, 1, "all dedupe contenders must return the winner");
        const stored = await driver.queryEvents({ limit: 100 });
        equal(stored.length, 1, "dedupe storm must persist exactly one event");
        equal(stored[0], results[0]!.event, "every returned row must be the durable winner");
      } finally {
        await driver.close();
      }
    },
  },
  {
    name: "serves every normative query shape in sequence order",
    async run(factory) {
      const driver = await factory();
      try {
        await driver.appendEvent(conformanceEvent("id-1", "1", "2026-08-20T00:00:00.000Z"));
        const second = await driver.appendEvent({ ...conformanceEvent("id-2", "2", "2026-08-21T00:00:00.000Z"), type: "test.other", subject: "thing/2" });
        await driver.appendEvent(conformanceEvent("id-3", "3", "2026-08-22T00:00:00.000Z"));
        equal((await driver.queryEvents({ sinceSeq: 1, limit: 10 })).map((row) => row.id), ["id-2", "id-3"], "sequence range query");
        equal((await driver.queryEvents({ source: "test/source", subject: "thing/1", limit: 10 })).map((row) => row.id), ["id-1", "id-3"], "source and subject query");
        equal(await driver.queryEvents({ source: "test/source", type: "test.other", limit: 10 }), [second.event], "source and type query");
        equal((await driver.queryEvents({ since: "2026-08-21T00:00:00.000Z", until: "2026-08-22T00:00:00.000Z", limit: 10 })).map((row) => row.id), ["id-2", "id-3"], "received time query");
      } finally {
        await driver.close();
      }
    },
  },
  {
    name: "atomically fences cursor updates and admin rewinds",
    async run(factory) {
      const driver = await factory();
      try {
        const subscription = conformanceSubscription();
        subscription.push.scannedThroughSeq = 2;
        await driver.putSubscription(subscription);
        await driver.appendEvent(conformanceEvent("id-1", "1"));
        await driver.appendEvent(conformanceEvent("id-2", "2"));
        const results = await Promise.all([driver.advanceCursor("sub-1", 1, 0), driver.advanceCursor("sub-1", 2, 0)]);
        assert(results.some((result) => result.status === "advanced"), "a concurrent cursor update must advance");
        equal((await driver.getSubscription("sub-1"))?.cursor, 2, "cursor must reach the maximum ack");
        equal((await driver.advanceCursor("sub-1", 1, 0)).status, "unchanged", "lower ack must be unchanged");
        equal((await driver.advanceCursor("sub-1", 3, 0)).status, "beyond_newest", "future ack must be rejected");
        const rewound = await driver.rewindCursor("sub-1", 0);
        equal(rewound?.cursorEpoch, 1, "rewind must bump the epoch");
        equal(rewound?.push.scannedThroughSeq, undefined, "rewind must make the push tail replayable");
        equal((await driver.advanceCursor("sub-1", 2, 0)).status, "stale_epoch", "old epoch must be rejected");
        equal((await driver.advanceCursor("sub-1", 999, 0)).status, "stale_epoch", "stale epoch must precede newest validation");
        equal((await driver.advanceCursor("missing", 999, 0)).status, "not_found", "missing subscription must take precedence");
      } finally {
        await driver.close();
      }
    },
  },
  {
    name: "commits poll event and state together under a fenced lease",
    async run(factory) {
      const driver = await factory();
      try {
        const claimed = await driver.claimPollLease("source-1", "lease-a", "2026-08-23T00:01:00.000Z", "2026-08-23T00:00:00.000Z");
        equal(claimed?.leaseToken, "lease-a", "poll lease must be claimed");
        const stale = await driver.appendPollTransition({ expectedLeaseToken: "lease-b", state: { ...claimed!, fingerprint: "B", generation: 1 }, event: conformanceEvent("poll-1", "source-1/1") });
        equal(stale.committed, false, "stale poll lease must not commit");
        equal(await driver.newestSeq(), 0, "stale transition must not append");
        const committed = await driver.appendPollTransition({ expectedLeaseToken: "lease-a", state: { ...claimed!, fingerprint: "B", generation: 1 }, event: conformanceEvent("poll-1", "source-1/1") });
        equal(committed.committed, true, "current poll lease must commit");
        equal((await driver.getPollState("source-1"))?.generation, 1, "poll generation must advance atomically");
        equal(await driver.newestSeq(), 1, "poll event must commit atomically");
      } finally {
        await driver.close();
      }
    },
  },
  {
    name: "rejects the losing stale poll batch without partially advancing state",
    async run(factory) {
      const driver = await factory();
      try {
        const claimed = await driver.claimPollLease("source-1", "lease-a", "2026-08-23T00:01:00.000Z", "2026-08-23T00:00:00.000Z");
        assert(claimed, "poll lease must be claimed");
        const transitions = [1, 2].map((generation) => driver.appendPollTransition({
          expectedLeaseToken: "lease-a",
          state: { ...claimed, leaseToken: `lease-next-${generation}`, fingerprint: `value-${generation}`, generation },
          event: conformanceEvent(`poll-race-${generation}`, `poll-race-${generation}`),
        }));
        const results = await Promise.all(transitions);
        equal(results.filter((result) => result.committed).length, 1, "only one ETag or lease contender may commit");
        const winner = results.findIndex((result) => result.committed) + 1;
        const state = await driver.getPollState("source-1");
        equal(state?.generation, winner, "poll state must match the one acknowledged transition");
        equal(state?.leaseToken, `lease-next-${winner}`, "the losing transition must leave poll fencing untouched");
        equal((await driver.queryEvents({ limit: 10 })).map((event) => event.id), [`poll-race-${winner}`], "only the winning poll event may exist");
      } finally {
        await driver.close();
      }
    },
  },
  {
    name: "retries a poll append when unrelated appends race its counter",
    async run(factory) {
      const driver = await factory();
      try {
        const claimed = await driver.claimPollLease("source-1", "lease-a", "2026-08-23T00:01:00.000Z", "2026-08-23T00:00:00.000Z");
        assert(claimed, "poll lease must be claimed");
        const poll = driver.appendPollTransition({
          expectedLeaseToken: "lease-a",
          state: { ...claimed, fingerprint: "poll-won", generation: 1 },
          event: conformanceEvent("poll-under-ingest", "poll-under-ingest"),
        });
        const unrelated = Array.from({ length: 12 }, (_, index) =>
          driver.appendEvent(conformanceEvent(`unrelated-${index}`, `unrelated-${index}`)));
        const [pollResult] = await Promise.all([poll, ...unrelated]);
        equal(pollResult.committed, true, "a counter race must not be reported as a lost poll lease");
        equal((await driver.getPollState("source-1"))?.generation, 1, "the valid poll transition must advance state");
        assert((await driver.queryEvents({ limit: 100 })).some((event) => event.id === "poll-under-ingest"), "the acknowledged poll event must exist");
      } finally {
        await driver.close();
      }
    },
  },
  {
    name: "fences concurrent push lease owners",
    async run(factory) {
      const driver = await factory();
      try {
        await driver.putSubscription(conformanceSubscription());
        const claimed = await driver.claimPushLease("sub-1", "push-a", "2026-08-23T00:01:00.000Z", "2026-08-23T00:00:00.000Z");
        equal(claimed?.push.leaseToken, "push-a", "push lease must be claimed");
        equal(await driver.claimPushLease("sub-1", "push-b", "2026-08-23T00:02:00.000Z", "2026-08-23T00:00:30.000Z"), undefined, "live push lease must fence another owner");
        claimed!.push.failureCount = 1;
        delete claimed!.push.leaseToken;
        delete claimed!.push.leaseExpiresAt;
        equal((await driver.commitPushMutation({ subscriptionId: "sub-1", expectedLeaseToken: "push-b", expectedCursorEpoch: 0, push: claimed!.push })).status, "stale_lease", "wrong push lease must fail");
        equal((await driver.commitPushMutation({ subscriptionId: "sub-1", expectedLeaseToken: "push-a", expectedCursorEpoch: 0, push: claimed!.push })).status, "committed", "current push lease must commit");
      } finally {
        await driver.close();
      }
    },
  },
  {
    name: "treats liveness refresh racing subscription deletion as a no-op",
    async run(factory) {
      const driver = await factory();
      try {
        for (let index = 0; index < 12; index += 1) {
          const id = `liveness-delete-${index}`;
          await driver.putSubscription(conformanceSubscription(id));
          await Promise.all([
            driver.refreshLiveness(id, "2026-08-23T02:00:00.000Z", 0),
            driver.deleteSubscription(id),
          ]);
          equal(await driver.getSubscription(id), undefined, "a liveness refresh must not recreate a deleted subscription");
        }
      } finally {
        await driver.close();
      }
    },
  },
  {
    name: "pins live cursors for age and count retention",
    async run(factory) {
      const driver = await factory();
      try {
        await driver.putSubscription(conformanceSubscription());
        await driver.appendEvent(conformanceEvent("id-1", "1", "2026-08-01T00:00:00.000Z"));
        await driver.appendEvent(conformanceEvent("id-2", "2", "2026-08-02T00:00:00.000Z"));
        const result = await driver.enforceRetention({ maxAgeMs: 1, maxCount: 0, pinningWindowMs: 30 * 86_400_000 }, "2026-08-23T00:00:00.000Z");
        equal(result.deleted, 0, "live cursor must pin retention");
        await driver.advanceCursor("sub-1", 1, 0);
        const afterAck = await driver.enforceRetention({ maxAgeMs: 1, maxCount: 0, pinningWindowMs: 30 * 86_400_000 }, "2026-08-23T00:00:00.000Z");
        equal(afterAck.deleted, 1, "acked event may be retained away");
        equal(afterAck.oldestAvailableSeq, 2, "oldest sequence must advance");
      } finally {
        await driver.close();
      }
    },
  },
  {
    name: "accounts for a concurrently selected retention victim only once",
    async run(factory) {
      const driver = await factory();
      try {
        await driver.appendEvent(conformanceEvent("double-delete", "double-delete"));
        const results = await Promise.all([
          driver.enforceRetention({ maxCount: 0 }, "2026-08-23T00:00:00.000Z"),
          driver.enforceRetention({ maxCount: 0 }, "2026-08-23T00:00:00.000Z"),
        ]);
        equal(results.reduce((total, result) => total + result.deleted, 0), 1, "one durable event may be counted as deleted only once");
        equal(await driver.queryEvents({ limit: 10 }), [], "the selected event must be absent");
        assert((await driver.measureSizeBytes()) >= 0, "repeated retention must not drive size accounting negative");
      } finally {
        await driver.close();
      }
    },
  },
  {
    name: "overrides a live cursor for the size cap and surfaces retention pressure",
    async run(factory) {
      const driver = await factory();
      try {
        await driver.putSubscription(conformanceSubscription());
        await driver.appendEvent({ ...conformanceEvent("large-1", "large-1"), data: { payload: "x".repeat(128_000) }, rawSize: 128_000 });
        await driver.appendEvent({ ...conformanceEvent("large-2", "large-2"), data: { payload: "y".repeat(128_000) }, rawSize: 128_000 });
        const before = await driver.measureSizeBytes();
        const result = await driver.enforceRetention({ maxBytes: before - 1, pinningWindowMs: 30 * 86_400_000 }, "2026-08-23T00:00:00.000Z");
        assert(result.sizeOverride, "size cap must override cursor pinning");
        assert(result.deleted > 0, "size override must delete events");
        assert(result.oldestAvailableSeq > 1, "size override must cross the cursor pin");
        equal((await driver.getSubscription("sub-1"))?.cursor, 0, "retention must not mutate the cursor");
        const health = { retentionPressure: result.sizeOverride, secretFailures: new Set<string>() };
        const { app } = createApp({ driver, secrets: noSecrets, health });
        const body = await (await app.request("/healthz")).json() as Record<string, unknown>;
        equal({ ok: body.ok, retention_pressure: body.retention_pressure, warnings: body.warnings }, { ok: false, retention_pressure: true, warnings: ["retention_size_override"] }, "health must surface size override");
      } finally {
        await driver.close();
      }
    },
  },
  {
    name: "stores an event at the driver-declared payload boundary",
    async run(factory) {
      const driver = await factory();
      try {
        const cap = driver.capabilities.maxPayloadBytes;
        assert(cap !== undefined && cap > 0, "every driver must declare its accepted payload boundary");
        const appended = await driver.appendEvent({
          ...conformanceEvent("payload-boundary", "payload-boundary"),
          data: payloadDataAtSerializedSize(cap, false),
          rawSize: cap,
        });
        assert(appended.inserted, "a payload at the advertised boundary must be accepted");
        equal((await driver.getEventBySeq(appended.event.seq))?.id, "payload-boundary", "the boundary event must be durable");
        const escaped = await driver.appendEvent({
          ...conformanceEvent("payload-boundary-escaped", "payload-boundary-escaped"),
          data: payloadDataAtSerializedSize(cap, true),
          rawSize: cap,
        });
        assert(escaped.inserted, "an escape-heavy payload at the advertised boundary must be accepted");
        equal((await driver.getEventBySeq(escaped.event.seq))?.id, "payload-boundary-escaped", "the escape-heavy boundary event must be durable");
      } finally {
        await driver.close();
      }
    },
  },
];
