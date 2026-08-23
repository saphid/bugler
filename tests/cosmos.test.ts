import { randomUUID } from "node:crypto";
import { CosmosClient } from "@azure/cosmos";
import { afterAll, describe, expect, test } from "bun:test";
import { createApp } from "../src/app";
import type { Source } from "../src/domain";
import { CosmosStorageDriver, createCosmosContainer } from "../src/storage/cosmos";
import { storageConformanceCases } from "./storage-conformance-suite";

const endpoint = process.env.BUGLER_COSMOS_ENDPOINT;
const key = process.env.BUGLER_COSMOS_KEY;

if (!endpoint || !key) {
  test.skip("Cosmos emulator conformance requires BUGLER_COSMOS_ENDPOINT and BUGLER_COSMOS_KEY; run bun run test:cosmos", () => {});
} else {
  const databaseId = `bugler-${randomUUID()}`;
  const cleanupClient = new CosmosClient({ endpoint, key });

  afterAll(async () => {
    await cleanupClient.database(databaseId).delete();
    cleanupClient.dispose();
  });

  describe("Cosmos vNext emulator storage conformance", () => {
    for (const testCase of storageConformanceCases) {
      test(testCase.name, async () => {
        const { client, container } = await createCosmosContainer({
          clientOptions: { endpoint, key },
          databaseId,
          containerId: `case-${randomUUID()}`,
        });
        await testCase.run(async () => new CosmosStorageDriver({ container }));
        client.dispose();
      });
    }

    test("returns 413 when the exact serialized Cosmos event exceeds the item limit", async () => {
      const { client, container } = await createCosmosContainer({
        clientOptions: { endpoint, key },
        databaseId,
        containerId: `case-${randomUUID()}`,
      });
      const driver = new CosmosStorageDriver({ container });
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
            "x-test-subject": "x".repeat(2 * 1024 * 1024),
          },
          body: "{}",
        });
        expect(response.status).toBe(413);
        expect(await response.json() as unknown).toEqual({ error: "payload too large" });
      } finally {
        await driver.close();
        client.dispose();
      }
    });
  });
}
