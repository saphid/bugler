import { describe, expect, it } from "bun:test";
import { createApp } from "../src/app";
import { mintToken } from "../src/auth";
import { MemoryLocalStore } from "../src/daemon/memory-store";
import { OssLongPollMirrorClient } from "../src/daemon/upstream";
import type { AppendEventInput, Subscription } from "../src/domain";
import { EnvironmentFileSecretResolver } from "../src/secrets";
import { MemoryStorageDriver } from "../src/storage/memory";

async function setup() {
  const driver = new MemoryStorageDriver();
  const subscriber = mintToken("sub");
  const subscription: Subscription = {
    id: "plain-oss", filter: { exact: { source: "source/a" } }, transport: { type: "pull" },
    tokenHash: subscriber.hash, tokenPrefix: subscriber.prefix, cursor: 0, cursorEpoch: 0,
    push: { failureCount: 0, parked: 0 }, createdAt: "2026-08-25T00:00:00.000Z", lastSeenAt: "2026-08-25T00:00:00.000Z",
  };
  await driver.createSubscription(subscription);
  const relay = createApp({ driver, secrets: new EnvironmentFileSecretResolver(), deliveryProfile: {
    defaultWaitSeconds: 0, minWaitSeconds: 0, maxWaitSeconds: 1, admissionCadenceSeconds: 0, maxBatch: 100, scanLimit: 1_000,
  } });
  const store = new MemoryLocalStore();
  await store.createMirror("plain-oss");
  const consumer = await store.createConsumer({ id: "agent", mirrors: ["plain-oss"] });
  const fetcher = async (input: string | URL | Request, init?: RequestInit) => relay.app.request(new URL(String(input)).pathname, init);
  const client = new OssLongPollMirrorClient({
    origin: "https://oss.test", store, fetcher, notifyNewEvent() {}, tokenFor: async () => subscriber.token,
  });
  return { driver, store, consumer, client };
}

function input(seq: number): AppendEventInput {
  return {
    specversion: "1.0", id: `event-${seq}`, source: "source/a", sourceId: "source/a", type: "dev.bugler.test",
    receivedat: new Date(1_700_000_000_000 + seq).toISOString(), verificationmethod: "none", payloadverified: false,
    rawSize: 1, data: { seq },
  };
}

describe("plain OSS relay long-poll upstream", () => {
  it("commits locally before acknowledging the subscriber cursor", async () => {
    const test = await setup();
    await test.driver.appendEvent(input(1));
    await test.client.pollOnce("plain-oss", 0);
    expect(await test.store.readConsumer("agent")).toMatchObject({ events: [expect.objectContaining({ id: "event-1", seq: 1 })] });
    expect(await test.driver.getSubscription("plain-oss")).toMatchObject({ cursor: 1, cursorEpoch: 0 });
  });

  it("mirrors a relay retention gap as a level-triggered local gap", async () => {
    const test = await setup();
    await test.driver.appendEvent(input(1));
    await test.driver.appendEvent(input(2));
    const subscription = (await test.driver.getSubscription("plain-oss"))!;
    subscription.lastSeenAt = "2020-01-01T00:00:00.000Z";
    await test.driver.updateSubscription(subscription);
    await test.driver.enforceRetention({ maxCount: 0, pinningWindowMs: 1 }, "2026-08-25T00:00:00.000Z");
    await test.client.pollOnce("plain-oss", 0);
    expect(await test.store.readConsumer("agent")).toEqual({ gap: { from: 1, to: 1 }, cursor_epoch: 0, oldest_available_seq: 1 });
    expect(await test.driver.getSubscription("plain-oss")).toMatchObject({ cursor: 2 });
  });
});
