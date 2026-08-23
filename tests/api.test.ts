import { createHmac } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "bun:test";
import { createApp } from "../src/app";
import { makeAdminKey } from "../src/auth";
import type { AppendEventInput } from "../src/domain";
import { EnvironmentFileSecretResolver } from "../src/secrets";
import { MemoryStorageDriver } from "../src/storage/memory";
import { SqliteStorageDriver } from "../src/storage/sqlite";

async function setup(maxPayloadBytes = 5 * 1024 * 1024, now: () => Date = () => new Date("2026-08-23T00:00:00Z")) {
  const driver = new MemoryStorageDriver();
  const admin = makeAdminKey(new Date("2026-08-23T00:00:00Z"));
  await driver.putAdminKey(admin.key);
  const built = createApp({
    driver,
    secrets: new EnvironmentFileSecretResolver(),
    maxPayloadBytes,
    deliveryProfile: {
      defaultWaitSeconds: 0,
      minWaitSeconds: 0,
      maxWaitSeconds: 0,
      admissionCadenceSeconds: 60,
      maxBatch: 100,
      scanLimit: 1_000,
    },
    now,
  });
  const adminHeaders = { authorization: `Bearer ${admin.token}`, "content-type": "application/json" };
  return { driver, ...built, admin, adminHeaders };
}

async function createGithubSource(app: ReturnType<typeof createApp>["app"], adminHeaders: Record<string, string>) {
  return app.request("/sources", {
    method: "POST",
    headers: adminHeaders,
    body: JSON.stringify({
      id: "github-widgets",
      kind: "http",
      verification: { preset: "github", secret_refs: ["env:TEST_API_GH_SECRET"] },
      mapping: { preset: "github", source: "github/acme/widgets" },
    }),
  });
}

async function createSubscription(app: ReturnType<typeof createApp>["app"], adminHeaders: Record<string, string>, subject = "pr/42") {
  const response = await app.request("/subscriptions", {
    method: "POST",
    headers: adminHeaders,
    body: JSON.stringify({
      id: `sub-${subject.replace("/", "-")}`,
      filter: { all: [{ exact: { source: "github/acme/widgets" } }, { exact: { subject } }] },
      transport: { type: "pull" },
      action: "Reconcile current state.",
    }),
  });
  return response.json() as Promise<{ subscription: { id: string; cursorEpoch: number }; token: string }>;
}

function signedGithub(body: string, secret: string): Headers {
  return new Headers({
    "content-type": "application/json",
    "x-github-event": "pull_request_review",
    "x-github-delivery": "delivery-1",
    "x-hub-signature-256": `sha256=${createHmac("sha256", secret).update(body).digest("hex")}`,
  });
}

async function readSseUntil(
  reader: ReadableStreamDefaultReader<Uint8Array>,
  expected: string,
  timeoutMs = 250,
): Promise<string> {
  const decoder = new TextDecoder();
  let text = "";
  const deadline = Date.now() + timeoutMs;
  while (!text.includes(expected)) {
    const remaining = deadline - Date.now();
    if (remaining <= 0) throw new Error(`timed out waiting for SSE text: ${expected}`);
    const result = await Promise.race([
      reader.read(),
      Bun.sleep(remaining).then(() => "timeout" as const),
    ]);
    if (result === "timeout") throw new Error(`timed out waiting for SSE text: ${expected}`);
    if (result.done) throw new Error(`SSE stream closed before text: ${expected}`);
    text += decoder.decode(result.value, { stream: true });
  }
  return text;
}

describe("HTTP API boundaries", () => {
  it("returns 422 for dangling secret references at configuration time", async () => {
    delete process.env.TEST_API_GH_SECRET;
    const { app, adminHeaders } = await setup();
    const response = await createGithubSource(app, adminHeaders);
    expect(response.status).toBe(422);
    expect(await response.json()).toMatchObject({ error: expect.stringContaining("missing") });
  });

  it("rejects plaintext secret fields instead of persisting them", async () => {
    process.env.TEST_API_GH_SECRET = "configured-secret";
    const { app, adminHeaders } = await setup();
    const response = await app.request("/sources", {
      method: "POST",
      headers: adminHeaders,
      body: JSON.stringify({
        id: "unsafe", kind: "http",
        verification: { preset: "github", secret_refs: ["env:TEST_API_GH_SECRET"], secret: "plaintext" },
        mapping: { preset: "github", source: "github/acme/widgets" },
      }),
    });
    expect(response.status).toBe(422);
    expect(await response.json()).toMatchObject({ error: expect.stringContaining("plaintext") });
    delete process.env.TEST_API_GH_SECRET;
  });

  it("enforces 415, 413, and 401 before storage and fails closed when a secret disappears", async () => {
    process.env.TEST_API_GH_SECRET = "api-secret";
    const { app, driver, adminHeaders } = await setup(64);
    expect((await createGithubSource(app, adminHeaders)).status).toBe(201);
    expect((await app.request("/ingest/github-widgets", {
      method: "POST", headers: { "content-encoding": "gzip" }, body: "compressed",
    })).status).toBe(415);
    expect((await app.request("/ingest/github-widgets", {
      method: "POST", headers: { "content-length": "65" }, body: "x".repeat(65),
    })).status).toBe(413);
    expect((await app.request("/ingest/github-widgets", {
      method: "POST", headers: { "content-type": "application/json" }, body: "{}",
    })).status).toBe(401);
    expect(await driver.newestSeq()).toBe(0);
    delete process.env.TEST_API_GH_SECRET;
    const missing = await app.request("/ingest/github-widgets", {
      method: "POST", headers: { "content-type": "application/json", "x-hub-signature-256": "sha256=x" }, body: "{}",
    });
    expect(missing.status).toBe(401);
    expect(await (await app.request("/healthz")).json()).toMatchObject({ ok: false });
  });

  it("commits a signed event before ACK, then supports delivery, ack, history, and SSE replay", async () => {
    process.env.TEST_API_GH_SECRET = "api-secret";
    const { app, driver, adminHeaders } = await setup();
    await createGithubSource(app, adminHeaders);
    const subscription = await createSubscription(app, adminHeaders);
    const payload = JSON.stringify({
      action: "submitted",
      pull_request: { number: 42 },
      review: { submitted_at: "2026-08-22T04:11:09Z" },
    });
    const ingest = await app.request("/ingest/github-widgets", { method: "POST", headers: signedGithub(payload, "api-secret"), body: payload });
    expect(ingest.status).toBe(200);
    expect(await driver.newestSeq()).toBe(1);
    const auth = { authorization: `Bearer ${subscription.token}`, "content-type": "application/json" };
    const first = await (await app.request(`/subscriptions/${subscription.subscription.id}/events`, {
      method: "POST", headers: auth, body: JSON.stringify({ wait: 0 }),
    })).json() as { events: Array<{ seq: number }>; cursor_epoch: number };
    expect(first.events).toHaveLength(1);
    const history = await (await app.request("/events?source=github%2Facme%2Fwidgets&subject=pr%2F42", {
      headers: { authorization: `Bearer ${subscription.token}` },
    })).json() as { events: unknown[]; oldest_available_seq: number };
    expect(history.events).toHaveLength(1);
    const ack = await app.request(`/subscriptions/${subscription.subscription.id}/cursor`, {
      method: "POST", headers: auth, body: JSON.stringify({ seq: 1, epoch: first.cursor_epoch }),
    });
    expect(ack.status).toBe(200);
    expect((await driver.getSubscription(subscription.subscription.id))?.cursor).toBe(1);
    delete process.env.TEST_API_GH_SECRET;
  });

  it("keeps one SSE connection open for backlog and newly committed events", async () => {
    process.env.TEST_API_GH_SECRET = "api-secret";
    const { app, adminHeaders } = await setup();
    await createGithubSource(app, adminHeaders);
    const subscription = await createSubscription(app, adminHeaders);
    const firstBody = JSON.stringify({ action: "submitted", pull_request: { number: 42 }, review: { id: 1 } });
    await app.request("/ingest/github-widgets", { method: "POST", headers: signedGithub(firstBody, "api-secret"), body: firstBody });
    const response = await app.request(`/subscriptions/${subscription.subscription.id}/stream`, {
      headers: { authorization: `Bearer ${subscription.token}` },
    });
    const reader = response.body!.getReader();
    expect(await readSseUntil(reader, "id: 1")).toContain("id: 1");
    const secondBody = JSON.stringify({ action: "submitted", pull_request: { number: 42 }, review: { id: 2 } });
    const secondHeaders = signedGithub(secondBody, "api-secret");
    secondHeaders.set("x-github-delivery", "delivery-2");
    await app.request("/ingest/github-widgets", { method: "POST", headers: secondHeaders, body: secondBody });
    expect(await readSseUntil(reader, "id: 2")).toContain("id: 2");
    await reader.cancel();
    delete process.env.TEST_API_GH_SECRET;
  });

  it("ends an open SSE stream when its token is revoked before a new event", async () => {
    process.env.TEST_API_GH_SECRET = "api-secret";
    const { app, adminHeaders } = await setup();
    await createGithubSource(app, adminHeaders);
    const subscription = await createSubscription(app, adminHeaders);
    const response = await app.request(`/subscriptions/${subscription.subscription.id}/stream`, {
      headers: { authorization: `Bearer ${subscription.token}` },
    });
    const reader = response.body!.getReader();
    expect((await app.request(`/subscriptions/${subscription.subscription.id}/token`, {
      method: "DELETE",
      headers: adminHeaders,
    })).status).toBe(204);
    const body = JSON.stringify({ action: "submitted", pull_request: { number: 42 }, review: { id: "revoked" } });
    expect((await app.request("/ingest/github-widgets", {
      method: "POST",
      headers: signedGithub(body, "api-secret"),
      body,
    })).status).toBe(200);
    const result = await reader.read();
    expect(result.done).toBe(true);
    expect(result.value).toBeUndefined();
    delete process.env.TEST_API_GH_SECRET;
  });

  it("ends an open SSE stream when its old token passes the rotation overlap", async () => {
    process.env.TEST_API_GH_SECRET = "api-secret";
    let clock = new Date("2026-08-23T00:00:00Z");
    const { app, adminHeaders } = await setup(5 * 1024 * 1024, () => clock);
    await createGithubSource(app, adminHeaders);
    const subscription = await createSubscription(app, adminHeaders);
    const response = await app.request(`/subscriptions/${subscription.subscription.id}/stream`, {
      headers: { authorization: `Bearer ${subscription.token}` },
    });
    const reader = response.body!.getReader();
    expect((await app.request(`/subscriptions/${subscription.subscription.id}/token/rotate`, {
      method: "POST",
      headers: adminHeaders,
      body: JSON.stringify({ overlap_s: 1 }),
    })).status).toBe(200);
    clock = new Date("2026-08-23T00:00:02Z");
    const body = JSON.stringify({ action: "submitted", pull_request: { number: 42 }, review: { id: "rotated-away" } });
    expect((await app.request("/ingest/github-widgets", {
      method: "POST",
      headers: signedGithub(body, "api-secret"),
      body,
    })).status).toBe(200);
    const result = await reader.read();
    expect(result.done).toBe(true);
    expect(result.value).toBeUndefined();
    delete process.env.TEST_API_GH_SECRET;
  });

  it("uses Last-Event-ID as a replay hint without advancing the cursor", async () => {
    process.env.TEST_API_GH_SECRET = "api-secret";
    const { app, driver, adminHeaders } = await setup();
    await createGithubSource(app, adminHeaders);
    const subscription = await createSubscription(app, adminHeaders);
    for (const id of [1, 2]) {
      const body = JSON.stringify({ action: "submitted", pull_request: { number: 42 }, review: { id } });
      const headers = signedGithub(body, "api-secret");
      headers.set("x-github-delivery", `delivery-${id}`);
      await app.request("/ingest/github-widgets", { method: "POST", headers, body });
    }
    const response = await app.request(`/subscriptions/${subscription.subscription.id}/stream`, {
      headers: { authorization: `Bearer ${subscription.token}`, "last-event-id": "1" },
    });
    const reader = response.body!.getReader();
    const text = await readSseUntil(reader, "id: 2");
    expect(text).not.toContain("id: 1\n");
    expect((await driver.getSubscription(subscription.subscription.id))?.cursor).toBe(0);
    await reader.cancel();
    delete process.env.TEST_API_GH_SECRET;
  });

  it("emits one gap frame, pauses, and resumes only after gap ack", async () => {
    process.env.TEST_API_GH_SECRET = "api-secret";
    const { app, driver, adminHeaders } = await setup();
    await createGithubSource(app, adminHeaders);
    const subscription = await createSubscription(app, adminHeaders);
    for (const id of [1, 2]) {
      const body = JSON.stringify({ action: "submitted", pull_request: { number: 42 }, review: { id } });
      const headers = signedGithub(body, "api-secret");
      headers.set("x-github-delivery", `delivery-${id}`);
      await app.request("/ingest/github-widgets", { method: "POST", headers, body });
    }
    const stored = (await driver.getSubscription(subscription.subscription.id))!;
    stored.lastSeenAt = "2020-01-01T00:00:00Z";
    await driver.updateSubscription(stored);
    await driver.enforceRetention({ maxCount: 1, pinningWindowMs: 1 }, "2026-08-23T00:00:00Z");
    const response = await app.request(`/subscriptions/${subscription.subscription.id}/stream`, {
      headers: { authorization: `Bearer ${subscription.token}` },
    });
    const reader = response.body!.getReader();
    expect(await readSseUntil(reader, "event: gap")).toContain('"to":1');
    const pendingRead = reader.read();
    const beforeAck = await Promise.race([pendingRead, Bun.sleep(20).then(() => "timeout" as const)]);
    expect(beforeAck).toBe("timeout");
    const ack = await app.request(`/subscriptions/${subscription.subscription.id}/cursor`, {
      method: "POST",
      headers: { authorization: `Bearer ${subscription.token}`, "content-type": "application/json" },
      body: JSON.stringify({ seq: 1, epoch: 0 }),
    });
    expect(ack.status).toBe(200);
    const resumed = await pendingRead;
    expect(new TextDecoder().decode(resumed.value)).toContain("id: 2");
    await reader.cancel();
    delete process.env.TEST_API_GH_SECRET;
  });

  it("scopes subscriber history and rejects tokens in query strings", async () => {
    process.env.TEST_API_GH_SECRET = "api-secret";
    const { app, adminHeaders } = await setup();
    await createGithubSource(app, adminHeaders);
    const pr42 = await createSubscription(app, adminHeaders, "pr/42");
    const pr99 = await createSubscription(app, adminHeaders, "pr/99");
    for (const number of [42, 99]) {
      const payload = JSON.stringify({ action: "submitted", pull_request: { number }, review: { submitted_at: "2026-08-22T04:11:09Z" } });
      const headers = signedGithub(payload, "api-secret");
      headers.set("x-github-delivery", `delivery-${number}`);
      await app.request("/ingest/github-widgets", { method: "POST", headers, body: payload });
    }
    const history = await (await app.request("/events", { headers: { authorization: `Bearer ${pr42.token}` } })).json() as { events: Array<{ subject: string }> };
    expect(history.events.map((event) => event.subject)).toEqual(["pr/42"]);
    expect((await app.request(`/events?token=${encodeURIComponent(pr99.token)}`)).status).toBe(400);
    delete process.env.TEST_API_GH_SECRET;
  });

  it("returns one retry hint and then 429 with Retry-After", async () => {
    const { app, adminHeaders } = await setup();
    const subscription = await createSubscription(app, adminHeaders);
    const request = () => app.request(`/subscriptions/${subscription.subscription.id}/events`, {
      method: "POST",
      headers: { authorization: `Bearer ${subscription.token}`, "content-type": "application/json" },
      body: JSON.stringify({ wait: 0 }),
    });
    expect((await request()).status).toBe(200);
    expect((await request()).status).toBe(200);
    const limited = await request();
    expect(limited.status).toBe(429);
    expect(limited.headers.get("retry-after")).toBe("60");
  });

  it("supports dual-valid subscriber rotation and the admin key lifecycle", async () => {
    const { app, adminHeaders } = await setup();
    const subscription = await createSubscription(app, adminHeaders);
    const rotatedResponse = await app.request(`/subscriptions/${subscription.subscription.id}/token/rotate`, {
      method: "POST", headers: adminHeaders, body: JSON.stringify({ overlap_s: 60 }),
    });
    const rotated = await rotatedResponse.json() as { token: string };
    expect((await app.request("/events", { headers: { authorization: `Bearer ${subscription.token}` } })).status).toBe(200);
    expect((await app.request("/events", { headers: { authorization: `Bearer ${rotated.token}` } })).status).toBe(200);
    const immediateResponse = await app.request(`/subscriptions/${subscription.subscription.id}/token/rotate`, {
      method: "POST", headers: adminHeaders, body: JSON.stringify({ overlap_s: 0 }),
    });
    const immediate = await immediateResponse.json() as { token: string };
    expect((await app.request("/events", { headers: { authorization: `Bearer ${rotated.token}` } })).status).toBe(401);
    expect((await app.request("/events", { headers: { authorization: `Bearer ${immediate.token}` } })).status).toBe(200);

    const mintedResponse = await app.request("/admin-keys", { method: "POST", headers: adminHeaders, body: "{}" });
    const minted = await mintedResponse.json() as { admin_key: { id: string }; token: string };
    expect(minted.token).toStartWith("bgl_adm_");
    expect((await app.request(`/admin-keys/${minted.admin_key.id}`, {
      method: "PATCH", headers: adminHeaders, body: JSON.stringify({ expires_at: "2026-08-24T00:00:00Z" }),
    })).status).toBe(200);
    expect((await app.request(`/admin-keys/${minted.admin_key.id}`, { method: "DELETE", headers: adminHeaders })).status).toBe(204);
  });

  it("applies the Workers 1.5 MB cap on memory and SQLite drivers", async () => {
    process.env.TEST_API_GH_SECRET = "api-secret";
    const directory = await mkdtemp(join(tmpdir(), "bugler-api-cap-"));
    const drivers = [new MemoryStorageDriver(), new SqliteStorageDriver(join(directory, "cap.db"))];
    const cap = Math.floor(1.5 * 1024 * 1024);
    try {
      for (const driver of drivers) {
        const admin = makeAdminKey();
        await driver.putAdminKey(admin.key);
        const { app } = createApp({ driver, secrets: new EnvironmentFileSecretResolver(), maxPayloadBytes: cap });
        const adminHeaders = { authorization: `Bearer ${admin.token}`, "content-type": "application/json" };
        expect((await createGithubSource(app, adminHeaders)).status).toBe(201);
        const acceptedBody = JSON.stringify({ action: "submitted", pull_request: { number: 42 }, padding: "x".repeat(cap - 200) });
        expect((await app.request("/ingest/github-widgets", {
          method: "POST", headers: signedGithub(acceptedBody, "api-secret"), body: acceptedBody,
        })).status).toBe(200);
        const rejectedBody = "x".repeat(cap + 1);
        expect((await app.request("/ingest/github-widgets", {
          method: "POST", headers: { "content-type": "application/octet-stream", "content-length": String(cap + 1) }, body: rejectedBody,
        })).status).toBe(413);
        await driver.close();
      }
    } finally {
      await rm(directory, { recursive: true, force: true });
      delete process.env.TEST_API_GH_SECRET;
    }
  });
});

describe("ingest durability ordering", () => {
  it("does not resolve the HTTP ACK until appendEvent resolves", async () => {
    process.env.TEST_API_GH_SECRET = "api-secret";
    let release!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    class GatedDriver extends MemoryStorageDriver {
      override async appendEvent(input: AppendEventInput) {
        await gate;
        return super.appendEvent(input);
      }
    }
    const driver = new GatedDriver();
    const admin = makeAdminKey();
    await driver.putAdminKey(admin.key);
    const { app } = createApp({ driver, secrets: new EnvironmentFileSecretResolver() });
    const adminHeaders = { authorization: `Bearer ${admin.token}`, "content-type": "application/json" };
    await createGithubSource(app, adminHeaders);
    const payload = JSON.stringify({ action: "submitted", pull_request: { number: 42 } });
    let settled = false;
    const responsePromise = Promise.resolve(app.request("/ingest/github-widgets", { method: "POST", headers: signedGithub(payload, "api-secret"), body: payload })).then((response) => {
      settled = true;
      return response;
    });
    await Bun.sleep(5);
    expect(settled).toBe(false);
    release();
    expect((await responsePromise).status).toBe(200);
    expect(await driver.newestSeq()).toBe(1);
    delete process.env.TEST_API_GH_SECRET;
  });
});
