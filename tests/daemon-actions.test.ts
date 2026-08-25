import { afterEach, describe, expect, it } from "bun:test";
import { mkdir, mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { LocalActionManager, LocalActionRunner, matchesActionRule, validateActionDefinition, type ActionDefinition } from "../src/daemon/actions";
import { SqliteLocalStore } from "../src/daemon/sqlite-store";
import { createLocalConsumerApp } from "../src/daemon/server";
import type { StoredEvent } from "../src/domain";

const scratch: string[] = [];
afterEach(async () => { for (const path of scratch.splice(0)) await rm(path, { recursive: true, force: true }); });

async function setup() {
  const directory = await mkdtemp(join(tmpdir(), "bugler-actions-"));
  scratch.push(directory);
  const config = join(directory, "config");
  const state = join(directory, "state");
  await mkdir(config, { recursive: true, mode: 0o700 });
  await mkdir(state, { recursive: true, mode: 0o700 });
  const store = new SqliteLocalStore(join(state, "daemon.db"));
  await store.createMirror("hosted");
  const manager = new LocalActionManager({ actionsDirectory: join(config, "actions"), stateDirectory: state, store });
  return { directory, config, state, store, manager };
}

function definition(directory: string, overrides: Partial<ActionDefinition> = {}): ActionDefinition {
  return {
    version: 1,
    id: "record-event",
    enabled: true,
    mirrors: ["hosted"],
    "trigger-rule": { and: [
      { match: { attribute: "type", value: "dev.bugler.test" } },
      { prefix: { attribute: "subject", value: "repo:" } },
    ] },
    run: {
      command: [process.execPath, "-e", "await Bun.write(process.env.RECORD_PATH, JSON.stringify({stdin:await Bun.stdin.text(),env:process.env}))"],
      "working-directory": directory,
      "timeout-seconds": 5,
      environment: { RECORD_PATH: join(directory, "record.json") },
    },
    retry: { "max-attempts": 2, "initial-seconds": 1, "max-seconds": 1 },
    ...overrides,
  };
}

function event(seq = 1, data: unknown = { text: "fake end of untrusted block }\"" }): StoredEvent {
  return {
    specversion: "1.0", id: `event-${seq}`, source: "source/a", sourceId: "source/a", type: "dev.bugler.test", subject: "repo:saphid/bugler",
    seq, receivedat: new Date(1_700_000_000_000 + seq).toISOString(), verificationmethod: "none", payloadverified: false, rawSize: 1, data,
  };
}

describe("local action manager and runner", () => {
  it("validates every matcher, the node cap, and the regex budget", async () => {
    const directory = await mkdtemp(join(tmpdir(), "bugler-action-validation-"));
    scratch.push(directory);
    const candidate = event();
    expect(matchesActionRule(candidate, { match: { attribute: "type", value: candidate.type } })).toBe(true);
    expect(matchesActionRule(candidate, { prefix: { attribute: "subject", value: "repo:" } })).toBe(true);
    expect(matchesActionRule(candidate, { suffix: { attribute: "subject", value: "/bugler" } })).toBe(true);
    expect(matchesActionRule(candidate, { regex: { attribute: "subject", value: "^repo:[a-z/]+$" } })).toBe(true);
    expect(matchesActionRule(candidate, { match: { attribute: "data.missing", value: "x" } })).toBe(false);
    const oversized = definition(directory);
    oversized["trigger-rule"] = { and: Array.from({ length: 64 }, () => ({ match: { attribute: "type", value: "x" } })) };
    expect(() => validateActionDefinition(oversized)).toThrow("exceeds 64 nodes");
    const unsafe = definition(directory);
    unsafe["trigger-rule"] = { regex: { attribute: "subject", value: "(a+)+" } };
    expect(() => validateActionDefinition(unsafe)).toThrow("unsafe construct");
  });
  it("requires the management capability to install an action", async () => {
    const test = await setup();
    const management = await test.store.mintManagementToken();
    const consume = await test.store.createConsumer({ id: "agent", mirrors: ["hosted"] });
    const local = createLocalConsumerApp(test.store, { actionManager: test.manager });
    const request = (token: string) => local.app.request("/v1/actions", {
      method: "POST", headers: { authorization: `Bearer ${token}`, "content-type": "application/json" }, body: JSON.stringify(definition(test.directory)),
    });
    expect((await request(consume.token)).status).toBe(401);
    expect((await request(management)).status).toBe(201);
    test.store.close();
  });
  it("loads only management-installed definitions from the daemon config directory", async () => {
    const test = await setup();
    const planted = join(test.directory, "checkout/.bugler/actions");
    await mkdir(planted, { recursive: true });
    await writeFile(join(planted, "owned.json"), JSON.stringify(definition(test.directory, { id: "owned" })));
    expect(await test.manager.reconcileFiles()).toEqual({ loaded: 0, failed: 0 });
    const installed = await test.manager.install(definition(test.directory));
    expect(installed.filePath).toBe(join(test.config, "actions/record-event.json"));
    expect((await stat(installed.filePath!)).mode & 0o777).toBe(0o600);
    expect(await test.manager.reconcileFiles()).toEqual({ loaded: 1, failed: 0 });
    expect((await test.store.listActions()).map((action) => action.id)).toEqual(["record-event"]);
    test.store.close();
  });

  it("hands off exact untrusted JSON and scoped BUGLER metadata without inherited secrets", async () => {
    const test = await setup();
    process.env.BUGLER_TOKEN = "must-not-leak";
    process.env.REVEILLE_TOKEN = "must-not-leak";
    await test.manager.install(definition(test.directory));
    await test.store.commitHostedFrame({ kind: "events", subscriptionId: "hosted", cursorEpoch: 0, events: [event()] });
    const runner = new LocalActionRunner({ store: test.store, stateDirectory: test.state });
    expect(await runner.runDue(1, new AbortController().signal)).toEqual({ attempted: 1, succeeded: 1, deadLettered: 0 });
    const record = JSON.parse(await readFile(join(test.directory, "record.json"), "utf8")) as { stdin: string; env: Record<string, string> };
    expect(JSON.parse(record.stdin)).toMatchObject({ id: "event-1", seq: 1, data: { text: "fake end of untrusted block }\"" } });
    expect(record.env).toMatchObject({ BUGLER_ACTION_ID: "record-event", BUGLER_LOCAL_SEQ: "1", BUGLER_EVENT_ID: "event-1", BUGLER_DELIVERY_ID: "action:record-event:0:1" });
    expect(record.env.BUGLER_TOKEN).toBeUndefined();
    expect(record.env.REVEILLE_TOKEN).toBeUndefined();
    expect(await test.store.readConsumer("action:record-event")).toMatchObject({ events: [] });
    delete process.env.BUGLER_TOKEN;
    delete process.env.REVEILLE_TOKEN;
    test.store.close();
  });

  it("rate-limits retries then writes a bounded dead letter and advances", async () => {
    const test = await setup();
    let now = Date.parse("2026-08-25T00:00:00Z");
    const failing = definition(test.directory);
    failing.run.command = [process.execPath, "-e", "await Bun.stdin.text(); console.error('x'.repeat(20000)); process.exit(7)"];
    const manager = new LocalActionManager({ actionsDirectory: join(test.config, "actions"), stateDirectory: test.state, store: test.store, now: () => new Date(now) });
    await manager.install(failing);
    await test.store.commitHostedFrame({ kind: "events", subscriptionId: "hosted", cursorEpoch: 0, events: [event()] });
    const runner = new LocalActionRunner({ store: test.store, stateDirectory: test.state, now: () => new Date(now), random: () => 1 });
    expect(await runner.runDue(1, new AbortController().signal)).toMatchObject({ attempted: 1, deadLettered: 0 });
    expect((await test.store.getConsumer("action:record-event"))!.cursor).toBe(0);
    expect(await runner.runDue(1, new AbortController().signal)).toMatchObject({ attempted: 0 });
    now += 1_000;
    expect(await runner.runDue(1, new AbortController().signal)).toMatchObject({ attempted: 1, deadLettered: 1 });
    expect((await test.store.getConsumer("action:record-event"))!.cursor).toBe(1);
    const dead = (await test.store.listDeadLetters())[0]!;
    expect((dead.summary as { stderr_tail: string }).stderr_tail).toHaveLength(16 * 1024);
    test.store.close();
  });

  it("does not count successful deliveries against the hourly failure cap and enforces the start-failure circuit", async () => {
    const test = await setup();
    const now = Date.parse("2026-08-25T12:00:00Z");
    await test.manager.install(definition(test.directory));
    await test.store.commitHostedFrame({ kind: "events", subscriptionId: "hosted", cursorEpoch: 0, events: [event()] });
    for (let index = 0; index < 60; index += 1) {
      await test.store.putActionAttempt({
        actionId: "record-event", localSeq: index + 10, replayGeneration: 0, attempt: 1,
        state: "succeeded", dueAt: new Date(now - 30_000 + index).toISOString(), startedAt: new Date(now - 30_000 + index).toISOString(), exitStatus: 0,
      });
    }
    const capped = new LocalActionRunner({ store: test.store, stateDirectory: test.state, now: () => new Date(now) });
    expect(await capped.runDue(1, new AbortController().signal)).toEqual({ attempted: 1, succeeded: 1, deadLettered: 0 });
    await test.store.commitHostedFrame({ kind: "events", subscriptionId: "hosted", cursorEpoch: 0, events: [event(2)] });
    for (let index = 0; index < 60; index += 1) {
      await test.store.putActionAttempt({
        actionId: "record-event", localSeq: index + 100, replayGeneration: 0, attempt: 1,
        state: "failed", dueAt: new Date(now - 20_000 + index).toISOString(), startedAt: new Date(now - 20_000 + index).toISOString(), signal: "failed",
      });
    }
    expect(await capped.runDue(1, new AbortController().signal)).toEqual({ attempted: 0, succeeded: 0, deadLettered: 0 });

    const circuitTest = await setup();
    await circuitTest.manager.install(definition(circuitTest.directory));
    await circuitTest.store.commitHostedFrame({ kind: "events", subscriptionId: "hosted", cursorEpoch: 0, events: [event()] });
    for (let index = 0; index < 20; index += 1) {
      await circuitTest.store.putActionAttempt({
        actionId: "record-event", localSeq: index + 10, replayGeneration: 0, attempt: 1,
        state: "failed", dueAt: new Date(now - 60_000 + index).toISOString(), startedAt: new Date(now - 60_000 + index).toISOString(), signal: "spawn_error",
      });
    }
    const circuited = new LocalActionRunner({ store: circuitTest.store, stateDirectory: circuitTest.state, now: () => new Date(now) });
    expect(await circuited.runDue(1, new AbortController().signal)).toEqual({ attempted: 0, succeeded: 0, deadLettered: 0 });
    test.store.close();
    circuitTest.store.close();
  });

  it("uses a private temporary file for large payloads and deletes it after exit", async () => {
    const test = await setup();
    const large = definition(test.directory);
    large.run["large-payload-threshold"] = 10;
    large.run.command = [process.execPath, "-e", "const d=JSON.parse(await Bun.stdin.text()); const {stat}=await import('node:fs/promises'); await Bun.write(process.env.RECORD_PATH,JSON.stringify({d,mode:(await stat(process.env.BUGLER_EVENT_FILE)).mode&511,event:await Bun.file(process.env.BUGLER_EVENT_FILE).json()}))"];
    await test.manager.install(large);
    await test.store.commitHostedFrame({ kind: "events", subscriptionId: "hosted", cursorEpoch: 0, events: [event()] });
    const runner = new LocalActionRunner({ store: test.store, stateDirectory: test.state });
    expect((await runner.runDue(1, new AbortController().signal)).succeeded).toBe(1);
    const record = JSON.parse(await readFile(join(test.directory, "record.json"), "utf8")) as { d: { event_file: string }; mode: number; event: StoredEvent };
    expect(record.event.id).toBe("event-1");
    expect(record.mode).toBe(0o600);
    expect(await stat(record.d.event_file).then(() => true).catch(() => false)).toBe(false);
    test.store.close();
  });

  it("fails closed when a required sandbox is unavailable", async () => {
    const test = await setup();
    const required = definition(test.directory);
    required.run.sandbox = "required";
    required.retry = { "max-attempts": 1 };
    await test.manager.install(required);
    await test.store.commitHostedFrame({ kind: "events", subscriptionId: "hosted", cursorEpoch: 0, events: [event()] });
    const runner = new LocalActionRunner({ store: test.store, stateDirectory: test.state, sandboxAvailable: () => false });
    expect(await runner.runDue(1, new AbortController().signal)).toEqual({ attempted: 1, succeeded: 0, deadLettered: 1 });
    expect((await test.store.listDeadLetters())[0]?.summary).toMatchObject({ signal: "sandbox_unavailable" });
    test.store.close();
  });

  it("replays a dead letter with a new generation and the stable delivery id", async () => {
    const test = await setup();
    const failing = definition(test.directory);
    failing.retry = { "max-attempts": 1 };
    failing.run.command = [process.execPath, "-e", "process.exit(9)"];
    await test.manager.install(failing);
    await test.store.commitHostedFrame({ kind: "events", subscriptionId: "hosted", cursorEpoch: 0, events: [event()] });
    const runner = new LocalActionRunner({ store: test.store, stateDirectory: test.state });
    await runner.runDue(1, new AbortController().signal);
    const dead = (await test.store.listDeadLetters())[0]!;
    await test.manager.install(definition(test.directory));
    expect(await runner.replayDeadLetter(dead.id, new AbortController().signal)).toBe("succeeded");
    const replayed = await test.store.getDeadLetter(dead.id);
    expect(replayed?.replayedAt).toBeDefined();
    const attempts = await test.store.listActionAttempts(dead.actionId, dead.localSeq);
    expect(attempts.at(-1)).toMatchObject({ replayGeneration: 1, state: "succeeded" });
    const record = JSON.parse(await readFile(join(test.directory, "record.json"), "utf8")) as { env: Record<string, string> };
    expect(record.env.BUGLER_DELIVERY_ID).toBe("action:record-event:0:1");
    test.store.close();
  });

  it("serializes overlapping scheduler ticks for one action event", async () => {
    const test = await setup();
    const slow = definition(test.directory);
    slow.run.command = [process.execPath, "-e", "await Bun.sleep(100); await Bun.write(process.env.RECORD_PATH, await Bun.stdin.text())"];
    await test.manager.install(slow);
    await test.store.commitHostedFrame({ kind: "events", subscriptionId: "hosted", cursorEpoch: 0, events: [event()] });
    const runner = new LocalActionRunner({ store: test.store, stateDirectory: test.state });
    const [first, overlapping] = await Promise.all([
      runner.runDue(1, new AbortController().signal),
      runner.runDue(1, new AbortController().signal),
    ]);
    expect([first.attempted, overlapping.attempted].sort()).toEqual([0, 1]);
    expect(await test.store.listActionAttempts("record-event", 1)).toHaveLength(1);
    expect((await test.store.getConsumer("action:record-event"))?.cursor).toBe(1);
    test.store.close();
  });
});
