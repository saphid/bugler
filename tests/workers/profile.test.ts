import { env, exports } from "cloudflare:workers";
import { reset, runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { afterEach, describe, expect, it } from "vitest";
import { makeAdminKey, mintToken } from "../../src/auth";
import type { Source } from "../../src/domain";
import type { BuglerDurableObject } from "../../src/workers/index";
import worker from "../../src/workers/index";
import { WorkersDueWorkScheduler } from "../../src/workers/scheduler";
import { conformanceEvent, conformanceSource, conformanceSubscription } from "../storage-conformance-suite";

function singleton() {
  return env.BUGLER.get(env.BUGLER.idFromName("reveille"));
}

afterEach(() => reset());

async function withDriver<T>(operation: (instance: BuglerDurableObject, state: DurableObjectState) => Promise<T>): Promise<T> {
  return runInDurableObject(singleton(), operation);
}

describe("Workers profile", () => {
  it("isolates path-prefixed and subdomain tenant routes while preserving the default relay", async () => {
    const alpha = env.BUGLER.get(env.BUGLER.idFromName("alpha"));
    const beta = env.BUGLER.get(env.BUGLER.idFromName("beta"));
    await runInDurableObject(alpha, async (instance: BuglerDurableObject) => {
      await instance.driver.appendEvent(conformanceEvent("alpha-event", "alpha-event"));
    });
    await runInDurableObject(beta, async (instance: BuglerDurableObject) => {
      await instance.driver.appendEvent(conformanceEvent("beta-event-1", "beta-event-1"));
      await instance.driver.appendEvent(conformanceEvent("beta-event-2", "beta-event-2"));
    });

    const alphaResponse = await worker.fetch(new Request("https://relay.example/t/alpha/healthz"), {
      BUGLER: env.BUGLER,
    });
    expect(alphaResponse.status).toBe(200);
    expect(await alphaResponse.json()).toMatchObject({ newest_seq: 1 });

    const betaResponse = await worker.fetch(new Request("https://beta.relay.example/healthz"), {
      BUGLER: env.BUGLER,
      BUGLER_TENANT_HOST_SUFFIX: ".relay.example",
    });
    expect(betaResponse.status).toBe(200);
    expect(await betaResponse.json()).toMatchObject({ newest_seq: 2 });

    const defaultResponse = await worker.fetch(new Request("https://relay.example/healthz"), {
      BUGLER: env.BUGLER,
      BUGLER_LEGACY_DO: "1",
    });
    expect(defaultResponse.status).toBe(200);
    expect(await defaultResponse.json()).toMatchObject({ newest_seq: 0 });
  });

  it("routes compatibility deployments to the legacy named Durable Object", async () => {
    const legacy = env.BUGLER.get(env.BUGLER.idFromName("reveille"));
    await runInDurableObject(legacy, async (instance: BuglerDurableObject) => {
      await instance.driver.appendEvent(conformanceEvent("legacy-event", "legacy-event"));
    });

    const response = await worker.fetch(new Request("https://bugler.example/healthz"), {
      BUGLER: env.BUGLER,
      BUGLER_LEGACY_DO: "1",
    });
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({ newest_seq: 1 });
  });

  it("routes through the named Durable Object and enforces the 1.5 MB ingest cap", async () => {
    await withDriver(async (instance) => instance.driver.putSource(conformanceSource));
    const cap = 1_500_000;
    const framingBytes = new TextEncoder().encode(JSON.stringify({ payload: "" })).byteLength;
    const acceptedBody = JSON.stringify({ payload: "x".repeat(cap - framingBytes) });
    expect(new TextEncoder().encode(acceptedBody).byteLength).toBe(cap);
    const accepted = await exports.default.fetch(new Request("https://bugler.example/ingest/source-1", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: acceptedBody,
    }));
    expect(accepted.status).toBe(200);
    const rejected = await exports.default.fetch(new Request("https://bugler.example/ingest/source-1", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: `${acceptedBody} `,
    }));
    expect(rejected.status).toBe(413);
    const health = await exports.default.fetch(new Request("https://bugler.example/healthz"));
    expect(await health.json()).toMatchObject({ storage_driver: "durable-object-sqlite" });
  });

  it("advertises the 240 second cadence and rate limits empty spins", async () => {
    const token = mintToken("sub");
    await withDriver(async (instance) => instance.driver.putSubscription({
      ...conformanceSubscription("workers-sub"),
      tokenHash: token.hash,
      tokenPrefix: token.prefix,
    }));
    const poll = () => exports.default.fetch(new Request("https://bugler.example/subscriptions/workers-sub/events", {
      method: "POST",
      headers: { authorization: `Bearer ${token.token}`, "content-type": "application/json" },
      body: JSON.stringify({ wait: 0 }),
    }));
    const first = await poll();
    expect(first.status).toBe(200);
    expect(await first.json()).toMatchObject({ events: [], retry_after: 240 });
    const grace = await poll();
    expect(grace.status).toBe(200);
    expect(await grace.json()).toMatchObject({ events: [], retry_after: 240 });
    const limited = await poll();
    expect(limited.status).toBe(429);
    expect(limited.headers.get("retry-after")).toBe("240");
  });

  it("reuses lean due-time snapshots on fetches that do not mutate schedules", async () => {
    await withDriver(async (instance, state) => {
      const scheduler = new WorkersDueWorkScheduler(state.storage, instance.driver, {
        async resolve(reference: string): Promise<string> {
          throw new Error(`unexpected secret reference: ${reference}`);
        },
      }, instance.health);
      await scheduler.armNext();
      let sourceListings = 0;
      let subscriptionListings = 0;
      const listSources = instance.driver.listSources.bind(instance.driver);
      const listSubscriptions = instance.driver.listSubscriptions.bind(instance.driver);
      instance.driver.listSources = async () => {
        sourceListings += 1;
        return listSources();
      };
      instance.driver.listSubscriptions = async () => {
        subscriptionListings += 1;
        return listSubscriptions();
      };
      await scheduler.armNext();
      await scheduler.armNext();
      expect(sourceListings).toBe(0);
      expect(subscriptionListings).toBe(0);
    });
  });

  it("does not rebuild schedule snapshots after a rejected subscription mutation", async () => {
    await withDriver(async (instance) => {
      await instance.fetch(new Request("https://bugler.example/healthz"));
      let subscriptionListings = 0;
      const listSubscriptions = instance.driver.listSubscriptions.bind(instance.driver);
      instance.driver.listSubscriptions = async () => {
        subscriptionListings += 1;
        return listSubscriptions();
      };
      const response = await instance.fetch(new Request("https://bugler.example/subscriptions", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: "{}",
      }));
      expect(response.status).toBe(401);
      expect(subscriptionListings).toBe(0);
    });
  });

  it("rebuilds the subscription schedule snapshot after a successful mutation", async () => {
    await withDriver(async (instance) => {
      const admin = makeAdminKey();
      await instance.driver.putAdminKey(admin.key);
      await instance.fetch(new Request("https://bugler.example/healthz"));
      let subscriptionListings = 0;
      const listSubscriptions = instance.driver.listSubscriptions.bind(instance.driver);
      instance.driver.listSubscriptions = async () => {
        subscriptionListings += 1;
        return listSubscriptions();
      };
      const response = await instance.fetch(new Request("https://bugler.example/subscriptions", {
        method: "POST",
        headers: { authorization: `Bearer ${admin.token}`, "content-type": "application/json" },
        body: JSON.stringify({
          id: "successful-refresh",
          filter: { exact: { source: "test/source" } },
          transport: { type: "pull" },
          action: "Reconcile current state.",
        }),
      }));
      expect(response.status).toBe(201);
      expect(subscriptionListings).toBeGreaterThan(0);
    });
  });

  it("defaults retention work to a thirty minute cadence", async () => {
    const now = new Date("2026-08-23T12:00:00.000Z");
    await withDriver(async (instance, state) => {
      await state.storage.delete("bugler:scheduler:retention-due");
      const scheduler = new WorkersDueWorkScheduler(state.storage, instance.driver, {
        async resolve(reference: string): Promise<string> {
          throw new Error(`unexpected secret reference: ${reference}`);
        },
      }, instance.health, { now: () => now });
      await scheduler.initialize();
      expect(await state.storage.get("bugler:scheduler:retention-due")).toBe(now.getTime() + 30 * 60_000);
    });
  });

  it("drains due work in six-item slices and persists an immediate continuation", async () => {
    await withDriver(async (instance) => {
      for (let index = 0; index < 7; index += 1) {
        const source: Source = {
          ...structuredClone(conformanceSource),
          id: `reconcile-${index}`,
          reconcile: { hook: "1", credential_ref: "env:MISSING", interval_s: 3600 },
        };
        await instance.driver.putSource(source);
      }
    });
    const afterSlice = await withDriver(async (instance, state) => {
      await instance.alarm();
      return {
        failures: (await instance.driver.listSources()).filter((source) => source.reconcile?.failure_count === 1).length,
        continuation: await state.storage.get("bugler:scheduler:last-continuation"),
        alarm: await state.storage.getAlarm(),
      };
    });
    expect(afterSlice.failures).toBe(6);
    expect(afterSlice.continuation).toMatchObject({ sliceSize: 6, remaining: 1 });
    expect(afterSlice.alarm).not.toBeNull();
  });

  it("does not immediately re-arm a caught-up push subscription", async () => {
    const startedAt = Date.now();
    const alarm = await withDriver(async (instance, state) => {
      await instance.driver.putSubscription({
        ...conformanceSubscription("idle-push"),
        transport: { type: "push", url: "https://push.example/events", whsec_refs: [] },
      });
      await instance.alarm();
      return state.storage.getAlarm();
    });
    expect(alarm).not.toBeNull();
    expect(alarm!).toBeGreaterThanOrEqual(startedAt + 30_000);
  });

  it("does not immediately re-arm a push subscription on a non-matching tail", async () => {
    const startedAt = Date.now();
    const alarm = await withDriver(async (instance, state) => {
      await instance.driver.putSubscription({
        ...conformanceSubscription("filtered-push"),
        filter: { exact: { type: "test.other" } },
        transport: { type: "push", url: "https://push.example/events", whsec_refs: [] },
      });
      await instance.driver.appendEvent(conformanceEvent("non-matching-tail"));
      await instance.alarm();
      return state.storage.getAlarm();
    });
    expect(alarm).not.toBeNull();
    expect(alarm!).toBeGreaterThanOrEqual(startedAt + 30_000);
  });

  it("promptly arms and delivers a new matching push event", async () => {
    const now = new Date("2026-08-23T12:00:00.000Z");
    const deliveries: string[] = [];
    await withDriver(async (instance, state) => {
      await instance.driver.putSubscription({
        ...conformanceSubscription("matching-push"),
        transport: { type: "push", url: "https://push.example/events", whsec_refs: [] },
      });
      await state.storage.put("bugler:scheduler:retention-due", now.getTime() + 30 * 60_000);
      const scheduler = new WorkersDueWorkScheduler(state.storage, instance.driver, {
        async resolve(reference: string): Promise<string> {
          throw new Error(`unexpected secret reference: ${reference}`);
        },
      }, instance.health, {
        now: () => now,
        retentionEveryMs: 30 * 60_000,
        pushFetch: async (_input, init) => {
          deliveries.push(String(init?.body));
          return new Response(null, { status: 204 });
        },
      });
      await scheduler.armNext();
      expect(await state.storage.getAlarm()).toBeGreaterThan(now.getTime());
      const appended = await instance.driver.appendEvent(conformanceEvent("matching-new"));
      await scheduler.armNext();
      expect(await state.storage.getAlarm()).toBe(now.getTime());
      await scheduler.runSlice();
      expect((await instance.driver.getSubscription("matching-push"))?.cursor).toBe(appended.event.seq);
    });
    expect(deliveries).toHaveLength(1);
    expect(JSON.parse(deliveries[0]!)).toMatchObject({ id: "matching-new" });
  });

  it("keeps repeated alarm delivery idempotent", async () => {
    await withDriver(async (instance, state) => {
      await instance.driver.appendEvent(conformanceEvent("alarm-1", "alarm-1"));
      await instance.driver.appendEvent(conformanceEvent("alarm-2", "alarm-2"));
      await instance.driver.appendEvent(conformanceEvent("alarm-3", "alarm-3"));
      await state.storage.put("bugler:scheduler:retention-due", Date.now() - 1);
      await state.storage.setAlarm(Date.now() + 60_000);
    });
    expect(await runDurableObjectAlarm(singleton())).toBe(true);
    await withDriver(async (_instance, state) => {
      await state.storage.put("bugler:scheduler:retention-due", Date.now() - 1);
      await state.storage.setAlarm(Date.now() + 60_000);
    });
    expect(await runDurableObjectAlarm(singleton())).toBe(true);
    const remaining = await withDriver(async (instance) => instance.driver.queryEvents({ sinceSeq: 0, limit: 10 }));
    expect(remaining.map((event) => event.id)).toEqual(["alarm-3"]);
  });
});
