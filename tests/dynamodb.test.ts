import { randomUUID } from "node:crypto";
import { DeleteTableCommand, DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { afterAll, describe, expect, test } from "bun:test";
import { classifyDynamoTransactionCancellation, createDynamoDbTable, DynamoDbStorageDriver } from "../src/storage/dynamodb";
import { storageConformanceCases } from "./storage-conformance-suite";

const endpoint = process.env.BUGLER_DYNAMODB_ENDPOINT;

function canceled(...codes: string[]): Error {
  const error = new Error("transaction canceled") as Error & { CancellationReasons: Array<{ Code: string }> };
  error.name = "TransactionCanceledException";
  error.CancellationReasons = codes.map((Code) => ({ Code }));
  return error;
}

describe("DynamoDB transaction cancellation classification", () => {
  test("reserves fencing semantics for pure conditional failures", () => {
    expect(classifyDynamoTransactionCancellation(canceled("ConditionalCheckFailed", "None"))).toBe("condition");
  });

  test("surfaces conflicts, throttling, mixed, and unknown cancellations as busy", () => {
    expect(classifyDynamoTransactionCancellation(canceled("TransactionConflict"))).toBe("busy");
    expect(classifyDynamoTransactionCancellation(canceled("ProvisionedThroughputExceeded"))).toBe("busy");
    expect(classifyDynamoTransactionCancellation(canceled("ThrottlingError"))).toBe("busy");
    expect(classifyDynamoTransactionCancellation(canceled("ConditionalCheckFailed", "TransactionConflict"))).toBe("busy");
    expect(classifyDynamoTransactionCancellation(canceled("ItemCollectionSizeLimitExceeded"))).toBe("busy");
  });
});

if (!endpoint) {
  test.skip("DynamoDB Local conformance requires BUGLER_DYNAMODB_ENDPOINT; run bun run test:dynamodb", () => {});
} else {
  const client = new DynamoDBClient({
    endpoint,
    region: "local",
    credentials: { accessKeyId: "local", secretAccessKey: "local" },
  });
  const tables: string[] = [];

  afterAll(async () => {
    for (const tableName of tables) await client.send(new DeleteTableCommand({ TableName: tableName }));
    client.destroy();
  });

  describe("DynamoDB Local storage conformance", () => {
    for (const testCase of storageConformanceCases) {
      test(testCase.name, async () => {
        const tableName = `bugler-${randomUUID()}`;
        tables.push(tableName);
        await createDynamoDbTable(client, tableName);
        await testCase.run(async () => new DynamoDbStorageDriver({ client, tableName }));
      });
    }

    test("serve refuses to create a missing table implicitly", async () => {
      const tableName = `bugler-missing-${randomUUID()}`;
      try {
        const child = Bun.spawn(["bun", "run", "src/bugler.ts", "serve"], {
          cwd: new URL("..", import.meta.url).pathname,
          env: {
            ...process.env,
            BUGLER_STORAGE: "dynamodb",
            BUGLER_DYNAMODB_TABLE: tableName,
            BUGLER_DYNAMODB_ENDPOINT: endpoint,
            AWS_REGION: "local",
            AWS_ACCESS_KEY_ID: "local",
            AWS_SECRET_ACCESS_KEY: "local",
          },
          stdout: "pipe",
          stderr: "pipe",
        });
        const [exitCode, stderr] = await Promise.all([child.exited, new Response(child.stderr).text()]);
        if (exitCode === 0) child.kill();
        expect(exitCode).not.toBe(0);
        expect(stderr).toContain("deploy/aws/setup.sh");
      } finally {
        await client.send(new DeleteTableCommand({ TableName: tableName })).catch(() => undefined);
      }
    });
  });
}
