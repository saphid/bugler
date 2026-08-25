import { describe, it } from "bun:test";
import { createApp } from "../src/app";
import { makeAdminKey, mintToken } from "../src/auth";
import type { AppendEventInput, Subscription } from "../src/domain";
import { EnvironmentFileSecretResolver } from "../src/secrets";
import { MemoryStorageDriver } from "../src/storage/memory";
import { MemoryLocalStore } from "../src/daemon/memory-store";
import { createLocalConsumerApp } from "../src/daemon/server";
import { OssLongPollMirrorClient } from "../src/daemon/upstream";
import {
  deliveryConformanceCases,
  type DeliveryConformanceAdapter,
  type DeliveryResponse,
} from "./delivery-conformance-suite";

async function hostedAdapter(): Promise<DeliveryConformanceAdapter> {
  const driver = new MemoryStorageDriver();
  const admin = makeAdminKey(new Date("2026-08-25T00:00:00Z"));
  await driver.putAdminKey(admin.key);
  const { app, delivery } = createApp({
    driver,
    secrets: new EnvironmentFileSecretResolver(),
    deliveryProfile: {
      defaultWaitSeconds: 0,
      minWaitSeconds: 0,
      maxWaitSeconds: 1,
      admissionCadenceSeconds: 0,
      maxBatch: 100,
      scanLimit: 1_000,
    },
    now: () => new Date("2026-08-25T00:00:00Z"),
  });
  const adminHeaders = { authorization: `Bearer ${admin.token}`, "content-type": "application/json" };
  let eventNumber = 0;
  const response = async (value: Response): Promise<DeliveryResponse> => ({
    status: value.status,
    body: value.status === 204 ? undefined : await value.json(),
    ...(value.headers.get("retry-after") ? { retryAfter: value.headers.get("retry-after")! } : {}),
  });
  return {
    profile: { tokenPrefix: "bgl_sub_", maxWaitSeconds: 1 },
    async createConsumer(id, filterSource = "source/a") {
      const minted = mintToken("sub");
      const subscription: Subscription = {
        id,
        filter: { exact: { source: filterSource } },
        transport: { type: "pull" },
        tokenHash: minted.hash,
        tokenPrefix: minted.prefix,
        cursor: 0,
        cursorEpoch: 0,
        push: { failureCount: 0, parked: 0 },
        createdAt: "2026-08-25T00:00:00.000Z",
        lastSeenAt: "2026-08-01T00:00:00.000Z",
      };
      await driver.createSubscription(subscription);
      return { token: minted.token, epoch: 0 };
    },
    async append(source = "source/a") {
      eventNumber += 1;
      const input: AppendEventInput = {
        specversion: "1.0",
        id: `event-${eventNumber}`,
        source,
        sourceId: source,
        type: "dev.bugler.test",
        receivedat: new Date(Date.parse("2026-08-25T00:00:00Z") + eventNumber).toISOString(),
        verificationmethod: "none",
        payloadverified: false,
        rawSize: 1,
        data: { eventNumber },
      };
      const appended = await driver.appendEvent(input);
      delivery.notifyNewEvent();
      return appended.event.seq;
    },
    async gapNext() {
      eventNumber += 1;
      const appended = await driver.appendEvent({
        specversion: "1.0", id: `event-${eventNumber}`, source: "source/a", sourceId: "source/a", type: "dev.bugler.test",
        receivedat: new Date(Date.parse("2026-08-25T00:00:00Z") + eventNumber).toISOString(), verificationmethod: "none",
        payloadverified: false, rawSize: 1, data: { eventNumber },
      });
      delivery.notifyNewEvent();
      for (const subscription of await driver.listSubscriptions()) {
        subscription.lastSeenAt = "2020-01-01T00:00:00.000Z";
        await driver.updateSubscription(subscription);
      }
      await driver.enforceRetention({ maxCount: 0, pinningWindowMs: 1 }, "2026-08-25T00:00:00.000Z");
      return appended.event.seq;
    },
    async poll(id, token, body) {
      return response(await app.request(`/subscriptions/${id}/events`, {
        method: "POST",
        headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
        body: JSON.stringify(body),
      }));
    },
    async cursor(id, token, seq, epoch) {
      return response(await app.request(`/subscriptions/${id}/cursor`, {
        method: "POST",
        headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
        body: JSON.stringify({ seq, epoch }),
      }));
    },
    async rewind(id, seq) {
      const result = await response(await app.request(`/subscriptions/${id}/cursor`, {
        method: "POST",
        headers: adminHeaders,
        body: JSON.stringify({ seq, epoch: 0 }),
      }));
      return { epoch: (result.body as { epoch: number }).epoch };
    },
    async retainNewest(count) {
      for (const subscription of await driver.listSubscriptions()) {
        subscription.lastSeenAt = "2020-01-01T00:00:00.000Z";
        await driver.updateSubscription(subscription);
      }
      await driver.enforceRetention({ maxCount: count, pinningWindowMs: 1 }, "2026-08-25T00:00:00.000Z");
    },
    async history(token, source) {
      return response(await app.request(`/events${source ? `?source=${encodeURIComponent(source)}` : ""}`, {
        headers: { authorization: `Bearer ${token}` },
      }));
    },
    async rotate(id, overlapSeconds) {
      const result = await response(await app.request(`/subscriptions/${id}/token/rotate`, {
        method: "POST",
        headers: adminHeaders,
        body: JSON.stringify({ overlap_s: overlapSeconds }),
      }));
      return { token: (result.body as { token: string }).token };
    },
    async revoke(id) {
      await app.request(`/subscriptions/${id}/token`, { method: "DELETE", headers: adminHeaders });
    },
    async close() {
      driver.close();
    },
  };
}

describe("hosted delivery conformance", () => {
  for (const testCase of deliveryConformanceCases) {
    it(testCase.name, () => testCase.run(hostedAdapter));
  }
});

async function localAdapter(): Promise<DeliveryConformanceAdapter> {
  const store = new MemoryLocalStore({ now: () => new Date("2026-08-25T00:00:00Z") });
  const management = await store.mintManagementToken();
  const local = createLocalConsumerApp(store, { profile: { defaultWaitSeconds: 0, maxWaitSeconds: 1, maxBatch: 100 } });
  const managementHeaders = { authorization: `Bearer ${management}`, "content-type": "application/json" };
  let hostedSeq = 0;
  const response = async (value: Response): Promise<DeliveryResponse> => ({
    status: value.status,
    body: value.status === 204 ? undefined : await value.json(),
    ...(value.headers.get("retry-after") ? { retryAfter: value.headers.get("retry-after")! } : {}),
  });
  return {
    profile: { tokenPrefix: "bgl_lcon_", maxWaitSeconds: 1 },
    async createConsumer(id, filterSource = "source/a") {
      await store.createMirror("hosted");
      const result = await response(await local.app.request("/v1/consumers", {
        method: "POST",
        headers: managementHeaders,
        body: JSON.stringify({ id, mirrors: ["hosted"], filter: { exact: { source: filterSource } } }),
      }));
      return { token: (result.body as { token: string }).token, epoch: 0 };
    },
    async append(source = "source/a") {
      hostedSeq += 1;
      const item: AppendEventInput & { seq: number } = {
        specversion: "1.0",
        id: `event-${hostedSeq}`,
        source,
        sourceId: source,
        type: "dev.bugler.test",
        seq: hostedSeq,
        receivedat: new Date(Date.parse("2026-08-25T00:00:00Z") + hostedSeq).toISOString(),
        verificationmethod: "none",
        payloadverified: false,
        rawSize: 1,
        data: { eventNumber: hostedSeq },
      };
      await store.commitHostedFrame({ kind: "events", subscriptionId: "hosted", cursorEpoch: 0, events: [item] });
      local.notifyNewEvent();
      return hostedSeq;
    },
    async gapNext() {
      hostedSeq += 1;
      await store.commitHostedFrame({ kind: "gap", subscriptionId: "hosted", cursorEpoch: 0, gap: { from: hostedSeq, to: hostedSeq } });
      local.notifyNewEvent();
      return hostedSeq;
    },
    async poll(id, token, body) {
      return response(await local.app.request(`/v1/consumers/${id}/events`, {
        method: "POST",
        headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
        body: JSON.stringify(body),
      }));
    },
    async cursor(id, token, seq, epoch) {
      return response(await local.app.request(`/v1/consumers/${id}/cursor`, {
        method: "POST",
        headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
        body: JSON.stringify({ seq, epoch }),
      }));
    },
    async rewind(id, seq) {
      const result = await response(await local.app.request(`/v1/consumers/${id}/cursor`, {
        method: "POST",
        headers: managementHeaders,
        body: JSON.stringify({ seq, epoch: 0 }),
      }));
      return { epoch: (result.body as { epoch: number }).epoch };
    },
    async retainNewest(count) {
      await store.applyRetention({ maxCount: count });
    },
    async history(token, source) {
      return response(await local.app.request(`/v1/consumers/auth/events${source ? `?source=${encodeURIComponent(source)}` : ""}`, {
        headers: { authorization: `Bearer ${token}` },
      }));
    },
    async rotate(id, overlapSeconds) {
      const result = await response(await local.app.request(`/v1/consumers/${id}/token/rotate`, {
        method: "POST",
        headers: managementHeaders,
        body: JSON.stringify({ overlap_s: overlapSeconds }),
      }));
      return { token: (result.body as { token: string }).token };
    },
    async revoke(id) {
      await local.app.request(`/v1/consumers/${id}/token`, { method: "DELETE", headers: managementHeaders });
    },
    async close() {
      store.close();
    },
  };
}

describe("daemon local-wire delivery conformance", () => {
  for (const testCase of deliveryConformanceCases) {
    it(testCase.name, () => testCase.run(localAdapter));
  }
});

async function ossOriginAdapter(): Promise<DeliveryConformanceAdapter> {
  const driver = new MemoryStorageDriver();
  const upstreamToken = mintToken("sub");
  await driver.createSubscription({
    id: "upstream", filter: { prefix: { source: "source/" } }, transport: { type: "pull" },
    tokenHash: upstreamToken.hash, tokenPrefix: upstreamToken.prefix, cursor: 0, cursorEpoch: 0,
    push: { failureCount: 0, parked: 0 }, createdAt: "2026-08-25T00:00:00.000Z", lastSeenAt: "2026-08-25T00:00:00.000Z",
  });
  const relay = createApp({ driver, secrets: new EnvironmentFileSecretResolver(), deliveryProfile: {
    defaultWaitSeconds: 0, minWaitSeconds: 0, maxWaitSeconds: 1, admissionCadenceSeconds: 0, maxBatch: 100, scanLimit: 1_000,
  } });
  const store = new MemoryLocalStore({ now: () => new Date("2026-08-25T00:00:00Z") });
  await store.createMirror("upstream");
  const management = await store.mintManagementToken();
  const local = createLocalConsumerApp(store, { profile: { defaultWaitSeconds: 0, maxWaitSeconds: 1, maxBatch: 100 } });
  const managementHeaders = { authorization: `Bearer ${management}`, "content-type": "application/json" };
  const sync = new OssLongPollMirrorClient({
    origin: "https://oss.test", store, tokenFor: async () => upstreamToken.token, notifyNewEvent: local.notifyNewEvent,
    fetcher: async (input, init) => relay.app.request(new URL(String(input)).pathname, init),
  });
  let eventNumber = 0;
  const response = async (value: Response): Promise<DeliveryResponse> => ({ status: value.status, body: value.status === 204 ? undefined : await value.json() });
  return {
    profile: { tokenPrefix: "bgl_lcon_", maxWaitSeconds: 1 },
    async createConsumer(id, filterSource = "source/a") {
      const result = await response(await local.app.request("/v1/consumers", {
        method: "POST", headers: managementHeaders,
        body: JSON.stringify({ id, mirrors: ["upstream"], filter: { exact: { source: filterSource } } }),
      }));
      return { token: (result.body as { token: string }).token, epoch: 0 };
    },
    async append(source = "source/a") {
      eventNumber += 1;
      const appended = await driver.appendEvent({
        specversion: "1.0", id: `oss-event-${eventNumber}`, source, sourceId: source, type: "dev.bugler.test",
        receivedat: new Date(Date.parse("2026-08-25T00:00:00Z") + eventNumber).toISOString(), verificationmethod: "none",
        payloadverified: false, rawSize: 1, data: { eventNumber },
      });
      await sync.pollOnce("upstream", 0);
      return appended.event.seq;
    },
    async gapNext() {
      eventNumber += 1;
      await driver.appendEvent({
        specversion: "1.0", id: `oss-gap-${eventNumber}`, source: "source/a", sourceId: "source/a", type: "dev.bugler.test",
        receivedat: new Date(Date.parse("2026-08-25T00:00:00Z") + eventNumber).toISOString(), verificationmethod: "none", payloadverified: false, rawSize: 1,
      });
      const subscription = (await driver.getSubscription("upstream"))!;
      subscription.lastSeenAt = "2020-01-01T00:00:00.000Z";
      await driver.updateSubscription(subscription);
      await driver.enforceRetention({ maxCount: 0, pinningWindowMs: 1 }, "2026-08-25T00:00:00.000Z");
      await sync.pollOnce("upstream", 0);
      return eventNumber;
    },
    async poll(id, token, body) {
      return response(await local.app.request(`/v1/consumers/${id}/events`, { method: "POST", headers: { authorization: `Bearer ${token}`, "content-type": "application/json" }, body: JSON.stringify(body) }));
    },
    async cursor(id, token, seq, epoch) {
      return response(await local.app.request(`/v1/consumers/${id}/cursor`, { method: "POST", headers: { authorization: `Bearer ${token}`, "content-type": "application/json" }, body: JSON.stringify({ seq, epoch }) }));
    },
    async rewind(id, seq) {
      const result = await response(await local.app.request(`/v1/consumers/${id}/cursor`, { method: "POST", headers: managementHeaders, body: JSON.stringify({ seq, epoch: 0 }) }));
      return { epoch: (result.body as { epoch: number }).epoch };
    },
    async retainNewest(count) {
      const subscription = (await driver.getSubscription("upstream"))!;
      subscription.lastSeenAt = "2020-01-01T00:00:00.000Z";
      await driver.updateSubscription(subscription);
      await driver.enforceRetention({ maxCount: count, pinningWindowMs: 1 }, "2026-08-25T00:00:00.000Z");
      await sync.pollOnce("upstream", 0);
      await store.applyRetention({ maxCount: count });
    },
    async history(token, source) {
      return response(await local.app.request(`/v1/consumers/auth/events${source ? `?source=${encodeURIComponent(source)}` : ""}`, { headers: { authorization: `Bearer ${token}` } }));
    },
    async rotate(id, overlapSeconds) {
      const result = await response(await local.app.request(`/v1/consumers/${id}/token/rotate`, { method: "POST", headers: managementHeaders, body: JSON.stringify({ overlap_s: overlapSeconds }) }));
      return { token: (result.body as { token: string }).token };
    },
    async revoke(id) { await local.app.request(`/v1/consumers/${id}/token`, { method: "DELETE", headers: managementHeaders }); },
    async close() { sync.stop(); store.close(); driver.close(); },
  };
}

describe("plain OSS origin through daemon delivery conformance", () => {
  for (const testCase of deliveryConformanceCases) it(testCase.name, () => testCase.run(ossOriginAdapter));
});
