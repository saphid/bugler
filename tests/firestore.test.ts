import { randomUUID } from "node:crypto";
import { Firestore } from "@google-cloud/firestore";
import { afterAll, describe, expect, test } from "bun:test";
import { createApp } from "../src/app";
import type { Source } from "../src/domain";
import { FirestoreStorageDriver } from "../src/storage/firestore";
import { storageConformanceCases } from "./storage-conformance-suite";

const host = process.env.FIRESTORE_EMULATOR_HOST;
const projectId = process.env.FIRESTORE_PROJECT_ID ?? "bugler-firestore-test";

if (!host) {
  test.skip("Firestore emulator conformance requires FIRESTORE_EMULATOR_HOST; run bun run test:firestore", () => {});
} else {
  const firestore = new Firestore({ projectId, databaseId: "(default)" });

  afterAll(async () => {
    await firestore.terminate();
  });

  describe("Firestore emulator storage conformance", () => {
    for (const testCase of storageConformanceCases) {
      test(testCase.name, async () => {
        const namespace = `case-${randomUUID()}`;
        await testCase.run(async () => new FirestoreStorageDriver({ firestore, namespace }));
      }, 60_000);
    }

    test("returns 413 when the approximated Firestore event exceeds the document limit", async () => {
      const driver = new FirestoreStorageDriver({ firestore, namespace: `case-${randomUUID()}` });
      try {
        const source: Source = {
          id: "exact-size",
          kind: "http",
          verification: { primitive: "none" },
          mapping: {
            source: "test/source",
            type: { static: "test.changed" },
            subject: { header: "x-test-subject" },
          },
          createdAt: "2026-08-23T00:00:00.000Z",
        };
        await driver.putSource(source);
        const { app } = createApp({
          driver,
          secrets: { resolve: async () => { throw new Error("unexpected secret lookup"); } },
        });
        const response = await app.request("/ingest/exact-size", {
          method: "POST",
          headers: {
            "content-type": "application/json",
            "x-test-subject": "x".repeat(1024 * 1024),
          },
          body: "{}",
        });
        expect(response.status).toBe(413);
        expect(await response.json() as unknown).toEqual({ error: "payload too large" });
      } finally {
        await driver.close();
      }
    }, 60_000);
  });
}
