import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { DURABLE_OBJECT_RETENTION_AGE_DELETE_SQL, DurableObjectStorageDriver } from "../../src/storage/durable-object";
import type { BuglerDurableObject } from "../../src/workers/index";
import { conformanceEvent, storageConformanceCases } from "../storage-conformance-suite";

describe("Durable Object SQLite storage conformance", () => {
  for (const [index, testCase] of storageConformanceCases.entries()) {
    it(testCase.name, async () => {
      const stub = env.BUGLER.get(env.BUGLER.idFromName(`conformance-${index}`));
      await runInDurableObject(stub, async (_instance: BuglerDurableObject, state) => {
        await testCase.run(async () => new DurableObjectStorageDriver(state.storage));
      });
    });
  }

  it("uses the received-time index for age retention", async () => {
    const stub = env.BUGLER.get(env.BUGLER.idFromName("retention-age-plan"));
    await runInDurableObject(stub, async (_instance: BuglerDurableObject, state) => {
      const plan = state.storage.sql.exec(
        `EXPLAIN QUERY PLAN ${DURABLE_OBJECT_RETENTION_AGE_DELETE_SQL}`,
        "2026-01-01T00:00:00Z",
        Number.MAX_SAFE_INTEGER,
      ).toArray();
      expect(JSON.stringify(plan)).toContain("events_received");
    });
  });

  it("bounds row reads when no events have crossed the age cutoff", async () => {
    const stub = env.BUGLER.get(env.BUGLER.idFromName("retention-age-rows-read"));
    await runInDurableObject(stub, async (_instance: BuglerDurableObject, state) => {
      const driver = new DurableObjectStorageDriver(state.storage);
      for (let index = 0; index < 200; index += 1) {
        await driver.appendEvent(conformanceEvent(`young-${index}`, `young-${index}`, "2026-08-23T00:00:00.000Z"));
      }
      await driver.enforceRetention({ maxAgeMs: 30 * 86_400_000 }, "2026-08-24T00:00:00.000Z");
      expect(driver.lastRetentionRowsRead).toBeLessThan(20);
      expect(await driver.newestSeq()).toBe(200);
    });
  });
});
