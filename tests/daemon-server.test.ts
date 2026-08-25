import { mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it } from "bun:test";
import { daemonAccountConfig, daemonAccountKey, daemonDefaultRuntimeRoot, loadDaemonAccount } from "../src/daemon/account";
import { daemonCli, daemonServiceArguments, daemonServiceDefinition, localCli } from "../src/daemon/cli";
import { LocalActionManager } from "../src/daemon/actions";
import { createLocalConsumerApp, startLocalDaemonServer } from "../src/daemon/server";
import { SqliteLocalStore } from "../src/daemon/sqlite-store";

const scratch: string[] = [];

afterEach(async () => {
  for (const path of scratch.splice(0)) await rm(path, { recursive: true, force: true });
});

async function roots() {
  const directory = await mkdtemp(join(tmpdir(), "bugler-daemon-server-"));
  scratch.push(directory);
  return {
    directory,
    args: ["--config-root", join(directory, "config"), "--state-root", join(directory, "state"), "--runtime-root", join(directory, "run")],
  };
}

function capture(): { stream: PassThrough; text(): string } {
  const stream = new PassThrough();
  let value = "";
  stream.on("data", (chunk) => { value += chunk.toString(); });
  return { stream, text: () => value };
}

describe("daemon local server and lifecycle", () => {
  it("generates source and compiled service launchers without an argv duplication", () => {
    expect(daemonServiceArguments(["/usr/local/bin/bugler", "daemon", "install"], "/usr/local/bin/bugler"))
      .toEqual(["/usr/local/bin/bugler"]);
    expect(daemonServiceArguments(["/opt/bun", "/repo/src/bugler.ts", "daemon"], "/opt/bun"))
      .toEqual(["/opt/bun", "/repo/src/bugler.ts"]);
    const launchd = daemonServiceDefinition("darwin", "work", "account-key", ["/usr/local/bin/bugler", "daemon", "start", "--account", "work"]);
    expect(launchd).toContain("<key>KeepAlive</key><dict><key>SuccessfulExit</key><false/></dict>");
    expect(launchd).toContain("<key>ThrottleInterval</key><integer>10</integer>");
    const systemd = daemonServiceDefinition("linux", "work", "account-key", ["/usr/local/bin/bugler", "daemon", "start", "--account", "work"]);
    expect(systemd).toContain("Restart=on-failure");
    expect(systemd).toContain("RestartSec=10s");
    expect(daemonDefaultRuntimeRoot("darwin", "/test-home", { TMPDIR: "/private/session-a" })).toBe("/test-home/Library/Application Support");
    expect(daemonDefaultRuntimeRoot("darwin", "/test-home", { TMPDIR: "/private/session-b" })).toBe("/test-home/Library/Application Support");
  });
  it("serves scoped management and consume APIs over a mode-0600 unix socket", async () => {
    const test = await roots();
    const output = capture();
    await daemonCli(["init", "--account", "work", "--origin", "https://relay.bugler.dev", "--tenant", "ten_1", "--principal", "member_1", ...test.args], output.stream);
    expect(output.text()).toContain("Management token, shown once");
    const config = await loadDaemonAccount("work", { configRoot: join(test.directory, "config"), stateRoot: join(test.directory, "state"), runtimeRoot: join(test.directory, "run") });
    const management = (await readFile(config.managementTokenFile, "utf8")).trim();
    expect((await stat(config.managementTokenFile)).mode & 0o777).toBe(0o600);
    const store = new SqliteLocalStore(config.databasePath);
    const local = startLocalDaemonServer({ store, unix: config.socketPath });
    expect((await stat(config.socketPath)).isSocket()).toBe(true);
    expect((await stat(config.socketPath)).mode & 0o777).toBe(0o600);
    const call = (path: string, init: RequestInit = {}) => {
      const headers = new Headers(init.headers);
      headers.set("authorization", `Bearer ${management}`);
      if (init.body) headers.set("content-type", "application/json");
      return fetch(`http://localhost/v1${path}`, { ...init, headers, unix: config.socketPath });
    };
    expect((await call("/mirrors", { method: "POST", body: '{"id":"hosted"}' })).status).toBe(201);
    const created = await (await call("/consumers", { method: "POST", body: '{"id":"agent","mirrors":["hosted"]}' })).json() as { token: string };
    expect(created.token.startsWith("bgl_lcon_")).toBe(true);
    expect((await call("/consumers/agent/events", { method: "POST", body: '{"wait":0}' })).status).toBe(401);
    const consume = await fetch("http://localhost/v1/consumers/agent/events", {
      method: "POST",
      headers: { authorization: `Bearer ${created.token}`, "content-type": "application/json" },
      body: '{"wait":0}',
      unix: config.socketPath,
    });
    expect(consume.status).toBe(200);
    expect(await consume.json() as unknown).toEqual({ events: [], cursor_epoch: 0, oldest_available_seq: 1 });
    local.server.stop(true);
    store.close();
  });

  it("fails closed on localhost Host and Origin while preserving capability auth", async () => {
    const { store } = (() => ({ store: new SqliteLocalStore(":memory:") }))();
    const management = await store.mintManagementToken();
    const local = createLocalConsumerApp(store, { transport: "localhost", allowedOrigin: "http://127.0.0.1:3000" });
    expect((await local.app.request("http://evil.test/v1/mirrors", { headers: { host: "evil.test", authorization: `Bearer ${management}` } })).status).toBe(403);
    expect((await local.app.request("http://localhost/v1/mirrors", { headers: { host: "localhost", origin: "http://evil.test", authorization: `Bearer ${management}` } })).status).toBe(403);
    expect((await local.app.request("http://localhost/v1/mirrors", { headers: { host: "localhost", authorization: `Bearer ${management}` } })).status).toBe(403);
    expect((await local.app.request("http://localhost/v1/mirrors", { headers: { host: "localhost", origin: "http://127.0.0.1:3000", authorization: `Bearer ${management}` } })).status).toBe(200);
    store.close();
  });

  it("rejects a unix peer uid that differs from the daemon owner when peer identity is exposed", async () => {
    const store = new SqliteLocalStore(":memory:");
    const management = await store.mintManagementToken();
    const local = createLocalConsumerApp(store, { transport: "unix", ownerUid: 501, peerUid: () => 502 });
    expect((await local.app.request("/v1/status", { headers: { authorization: `Bearer ${management}` } })).status).toBe(403);
    store.close();
  });

  it("requires management auth for status and exposes only local health coordinates", async () => {
    const store = new SqliteLocalStore(":memory:");
    const management = await store.mintManagementToken();
    await store.createMirror("hosted", 37, 9);
    const local = createLocalConsumerApp(store);
    expect((await local.app.request("/v1/status")).status).toBe(401);
    const response = await local.app.request("/v1/status", { headers: { authorization: `Bearer ${management}` } });
    expect(response.status).toBe(200);
    const body = await response.json() as { mirrors: unknown[] };
    expect(body.mirrors).toEqual([{ id: "hosted", state: "caught_up", lag: 0 }]);
    expect(JSON.stringify(body)).not.toContain("hostedCursor");
    expect(JSON.stringify(body)).not.toContain("cursorEpoch");
    expect(JSON.stringify(body)).not.toContain("receivedCursor");
    store.close();
  });

  it("returns 409 without mutating an existing consumer", async () => {
    const store = new SqliteLocalStore(":memory:");
    const management = await store.mintManagementToken();
    await store.createMirror("hosted");
    const local = createLocalConsumerApp(store);
    const headers = { authorization: `Bearer ${management}`, "content-type": "application/json" };
    expect((await local.app.request("/v1/consumers", { method: "POST", headers, body: '{"id":"agent","mirrors":["hosted"]}' })).status).toBe(201);
    expect((await local.app.request("/v1/consumers", { method: "POST", headers, body: '{"id":"agent","mirrors":["hosted"],"start":"oldest"}' })).status).toBe(409);
    expect(await store.getConsumer("agent")).toMatchObject({ cursor: 0, cursorEpoch: 0, state: "ready" });
    store.close();
  });

  it("exposes token rotation, rewind, action validation, and dead-letter pagination through the CLI", async () => {
    const test = await roots();
    await daemonCli(["init", "--account", "work", "--origin", "http://127.0.0.1:8787", "--tenant", "default", "--principal", "operator", ...test.args], capture().stream);
    const config = await loadDaemonAccount("work", { configRoot: join(test.directory, "config"), stateRoot: join(test.directory, "state"), runtimeRoot: join(test.directory, "run") });
    const store = new SqliteLocalStore(config.databasePath);
    await store.createMirror("hosted");
    const created = await store.createConsumer({ id: "agent", mirrors: ["hosted"] });
    await store.commitHostedFrame({ kind: "events", subscriptionId: "hosted", cursorEpoch: 0, events: [{
      specversion: "1.0", id: "cli-event", source: "source/a", sourceId: "source/a", type: "dev.bugler.test", seq: 1,
      receivedat: "2026-08-25T00:00:00.000Z", verificationmethod: "none", payloadverified: false, rawSize: 1,
    }] });
    await store.acknowledgeConsumer("agent", 1, 0);
    await store.putDeadLetter({ id: "dlq-a", actionId: "missing", localSeq: 1, summary: {}, createdAt: "2026-08-25T00:00:00.000Z" });
    await store.putDeadLetter({ id: "dlq-b", actionId: "missing", localSeq: 2, summary: {}, createdAt: "2026-08-25T00:00:01.000Z" });
    const manager = new LocalActionManager({ actionsDirectory: config.actionsDirectory, stateDirectory: config.stateDirectory, store });
    const local = startLocalDaemonServer({ store, unix: config.socketPath, actionManager: manager });

    const rotated = capture();
    await localCli(["consumer", "rotate", "--account", "work", "--id", "agent", "--overlap", "60", ...test.args], rotated.stream);
    const replacement = JSON.parse(rotated.text()) as { token: string };
    expect(await store.authenticateConsumer("agent", created.token)).toBe(true);
    expect(await store.authenticateConsumer("agent", replacement.token)).toBe(true);
    const rewound = capture();
    await localCli(["rewind", "--account", "work", "--consumer", "agent", "--seq", "0", ...test.args], rewound.stream);
    expect(JSON.parse(rewound.text())).toEqual({ cursor: 0, epoch: 1 });
    const validated = capture();
    await localCli(["actions", "validate", "--account", "work", ...test.args], validated.stream);
    expect(JSON.parse(validated.text())).toEqual({ loaded: 0, failed: 0 });
    const dead = capture();
    await localCli(["dead-letter", "list", "--account", "work", "--limit", "1", ...test.args], dead.stream);
    expect(JSON.parse(dead.text())).toMatchObject({ dead_letters: [{ id: "dlq-a" }], next_since: "dlq-a" });
    local.server.stop(true);
    store.close();
  });

  it("returns 202 and advances oldest backfill in interruptible thousand-row slices", async () => {
    const store = new SqliteLocalStore(":memory:");
    const management = await store.mintManagementToken();
    await store.createMirror("hosted");
    for (let seq = 1; seq <= 1_001; seq += 1) {
      await store.commitHostedFrame({ kind: "events", subscriptionId: "hosted", cursorEpoch: 0, events: [{
        specversion: "1.0", id: `event-${seq}`, source: "source/a", sourceId: "source/a", type: "dev.bugler.test",
        seq, receivedat: new Date(1_700_000_000_000 + seq).toISOString(), verificationmethod: "none", payloadverified: false, rawSize: 1,
      }] });
    }
    const scheduled: Array<() => void> = [];
    const local = createLocalConsumerApp(store, { scheduleWork: (work) => scheduled.push(work) });
    const headers = { authorization: `Bearer ${management}`, "content-type": "application/json" };
    const response = await local.app.request("/v1/consumers", {
      method: "POST", headers, body: '{"id":"archive","mirrors":["hosted"],"start":"oldest"}',
    });
    expect(response.status).toBe(202);
    const body = await response.json() as { consumer_id: string; state: string; mapped: number; total_estimate: number; token: string };
    expect(body).toMatchObject({ consumer_id: "archive", state: "backfilling", mapped: 0, total_estimate: 1_001 });
    expect(body.token.startsWith("bgl_lcon_")).toBe(true);
    expect((await local.app.request("/v1/consumers/archive/events", {
      method: "POST", headers: { authorization: `Bearer ${body.token}`, "content-type": "application/json" }, body: '{"wait":0}',
    })).status).toBe(409);
    expect(scheduled).toHaveLength(1);
    scheduled.shift()!();
    await Bun.sleep(0);
    expect(await store.getConsumer("archive")).toMatchObject({ state: "backfilling", mapped: 1_000, totalEstimate: 1_001 });
    expect(scheduled).toHaveLength(1);
    scheduled.shift()!();
    await Bun.sleep(0);
    expect(await store.getConsumer("archive")).toMatchObject({ state: "ready", nextLocalSeq: 1_002 });
    store.close();
  });

  it("namespaces multiple daemon accounts by the complete origin-tenant-principal tuple", async () => {
    const test = await roots();
    const base = { origin: "https://relay.bugler.dev", tenantId: "ten_1" };
    const first = daemonAccountConfig({ account: "work", ...base, principalId: "member_a" }, { configRoot: join(test.directory, "config"), stateRoot: join(test.directory, "state"), runtimeRoot: join(test.directory, "run") });
    const second = daemonAccountConfig({ account: "personal", ...base, principalId: "member_b" }, { configRoot: join(test.directory, "config"), stateRoot: join(test.directory, "state"), runtimeRoot: join(test.directory, "run") });
    expect(first.accountKey).not.toBe(second.accountKey);
    expect(first.socketPath).not.toBe(second.socketPath);
    expect(daemonAccountKey(base.origin, base.tenantId, "member_a")).toBe(first.accountKey);
    const alias = daemonAccountConfig({ account: "alias", ...base, principalId: "member_a" }, { configRoot: join(test.directory, "config"), stateRoot: join(test.directory, "state"), runtimeRoot: join(test.directory, "run") });
    expect(alias.accountKey).toBe(first.accountKey);
  });

  it("imports the hosted mint credential from an environment name into a private file and scrubs it", async () => {
    const test = await roots();
    process.env.BUGLER_TEST_DAEMON_CREDENTIAL = "bgl_dmn_obviously-fake-test-value";
    await daemonCli([
      "init", "--account", "hosted", "--origin", "https://relay.bugler.dev", "--tenant", "ten_1", "--principal", "member_1",
      "--credential-env", "BUGLER_TEST_DAEMON_CREDENTIAL", ...test.args,
    ], capture().stream);
    const config = await loadDaemonAccount("hosted", { configRoot: join(test.directory, "config"), stateRoot: join(test.directory, "state"), runtimeRoot: join(test.directory, "run") });
    expect(await readFile(config.hostedCredentialFile, "utf8")).toBe("bgl_dmn_obviously-fake-test-value\n");
    expect((await stat(config.hostedCredentialFile)).mode & 0o777).toBe(0o600);
    expect(process.env.BUGLER_TEST_DAEMON_CREDENTIAL).toBeUndefined();
    await expect(localCli(["mirror", "add", "--account", "hosted", "--sub", "github-starter", ...test.args], capture().stream))
      .rejects.toThrow("--token-env is required so subscriber-token fallback can authenticate github-starter");
  });

  it("rotates and revokes the stored hosted credential through the platform seam", async () => {
    const test = await roots();
    process.env.TEST_DAEMON_CREDENTIAL = "bgl_dmn_old";
    await daemonCli(["init", "--account", "hosted", "--origin", "https://relay.bugler.dev", "--tenant", "ten_1", "--principal", "member_1", "--credential-env", "TEST_DAEMON_CREDENTIAL", ...test.args], capture().stream);
    const config = await loadDaemonAccount("hosted", { configRoot: join(test.directory, "config"), stateRoot: join(test.directory, "state"), runtimeRoot: join(test.directory, "run") });
    const methods: string[] = [];
    const platform = Bun.serve({ port: 0, fetch(request) {
      expect(request.headers.get("authorization")).toBe("Bearer platform-test-token");
      methods.push(request.method);
      return request.method === "POST" ? Response.json({ token: "bgl_dmn_new" }) : new Response(null, { status: 204 });
    } });
    process.env.BUGLER_PLATFORM_TOKEN = "platform-test-token";
    await daemonCli(["credential", "rotate", "--account", "hosted", "--id", "dcr_1", "--platform", platform.url.origin, ...test.args], capture().stream);
    expect((await readFile(config.hostedCredentialFile, "utf8")).trim()).toBe("bgl_dmn_new");
    await daemonCli(["credential", "revoke", "--account", "hosted", "--id", "dcr_1", "--platform", platform.url.origin, ...test.args], capture().stream);
    expect(await stat(config.hostedCredentialFile).then(() => true).catch(() => false)).toBe(false);
    expect(methods).toEqual(["POST", "DELETE"]);
    delete process.env.BUGLER_PLATFORM_TOKEN;
    platform.stop(true);
  });

  it("starts, reports status, rejects a second instance, and emits an install unit", async () => {
    const test = await roots();
    await daemonCli(["init", "--account", "work", "--origin", "https://relay.bugler.dev", "--tenant", "ten_1", "--principal", "member_1", ...test.args], capture().stream);
    const config = await loadDaemonAccount("work", { configRoot: join(test.directory, "config"), stateRoot: join(test.directory, "state"), runtimeRoot: join(test.directory, "run") });
    // A reused live pid that does not belong to this daemon account is stale, not a lock holder.
    await writeFile(config.lockPath, `${process.pid}\n`, { mode: 0o600 });
    const child = Bun.spawn([process.execPath, join(import.meta.dir, "../src/bugler.ts"), "daemon", "start", "--account=work", ...test.args], { stdout: "pipe", stderr: "pipe" });
    for (let attempt = 0; attempt < 100; attempt += 1) {
      if (await stat(config.socketPath).then(() => true).catch(() => false)) break;
      await Bun.sleep(10);
    }
    expect((await stat(config.socketPath)).isSocket()).toBe(true);
    const status = capture();
    await daemonCli(["status", "--account", "work", "--json", ...test.args], status.stream);
    expect(JSON.parse(status.text())).toMatchObject({ account: "work", link_state: "starting", version: "0.1.0", socket: config.socketPath });
    const second = Bun.spawnSync([process.execPath, join(import.meta.dir, "../src/bugler.ts"), "daemon", "start", "--account", "work", ...test.args]);
    expect(second.exitCode).not.toBe(0);
    expect(second.stderr.toString()).toContain("already running");
    child.kill("SIGTERM");
    expect(await child.exited).toBe(0);
    const install = capture();
    const unit = join(test.directory, "bugler-daemon.unit");
    await daemonCli(["install", "--account", "work", "--output", unit, ...test.args], install.stream);
    const installed = await readFile(unit, "utf8");
    expect(installed).toContain("daemon");
    expect(installed).toContain("start");
    expect(installed).toContain("--account");
    expect(installed).toContain("work");
    const preview = capture();
    await daemonCli(["install", "--account", "work", "--dry-run", ...test.args], preview.stream);
    expect(preview.text()).toMatch(/launchctl bootstrap|systemctl --user daemon-reload/);
    const uninstall = capture();
    await daemonCli(["uninstall", "--account", "work", "--dry-run", ...test.args], uninstall.stream);
    expect(uninstall.text()).toMatch(/launchctl bootout|systemctl --user disable/);
  });

  it("reports a killed daemon as stale and reclaims its lock and unix socket immediately", async () => {
    const test = await roots();
    await daemonCli(["init", "--account", "crash", "--origin", "http://127.0.0.1:8787", "--tenant", "default", "--principal", "operator", ...test.args], capture().stream);
    const config = await loadDaemonAccount("crash", { configRoot: join(test.directory, "config"), stateRoot: join(test.directory, "state"), runtimeRoot: join(test.directory, "run") });
    const command = [process.execPath, join(import.meta.dir, "../src/bugler.ts"), "daemon", "start", "--account=crash", ...test.args];
    const first = Bun.spawn(command, { stdout: "pipe", stderr: "pipe" });
    for (let attempt = 0; attempt < 100; attempt += 1) {
      const pid = await readFile(config.statusPath, "utf8").then((value) => (JSON.parse(value) as { pid?: number }).pid).catch(() => undefined);
      if (pid === first.pid) break;
      await Bun.sleep(10);
    }
    first.kill("SIGKILL");
    await first.exited;

    const stale = capture();
    await daemonCli(["status", "--account", "crash", "--json", ...test.args], stale.stream);
    expect(JSON.parse(stale.text())).toMatchObject({ account: "crash", link_state: "stale", live: false });

    const restarted = Bun.spawn(command, { stdout: "pipe", stderr: "pipe" });
    let restartedStatus: { pid?: number; link_state?: string; live?: boolean } = {};
    for (let attempt = 0; attempt < 100; attempt += 1) {
      const status = capture();
      await daemonCli(["status", "--account", "crash", "--json", ...test.args], status.stream);
      restartedStatus = JSON.parse(status.text()) as typeof restartedStatus;
      if (restartedStatus.pid === restarted.pid && restartedStatus.live === true) break;
      await Bun.sleep(10);
    }
    expect(restartedStatus).toMatchObject({ pid: restarted.pid, live: true });
    expect(restartedStatus.link_state).not.toBe("stale");
    restarted.kill("SIGTERM");
    expect(await restarted.exited).toBe(0);
  });

  it("executes the README local consume command verbatim", async () => {
    const test = await roots();
    const root = join(import.meta.dir, "..");
    const readmeCommand = "bun run bugler local events --account work --consumer codex --wait 45s";
    expect(await readFile(join(root, "README.md"), "utf8")).toContain(readmeCommand);
    await daemonCli(["init", "--account", "work", "--origin", "http://127.0.0.1:8787", "--tenant", "default", "--principal", "operator", ...test.args], capture().stream);
    const overrides = { configRoot: join(test.directory, "config"), stateRoot: join(test.directory, "state"), runtimeRoot: join(test.directory, "run") };
    const config = await loadDaemonAccount("work", overrides);
    const store = new SqliteLocalStore(config.databasePath, { accountKey: config.accountKey });
    await store.createMirror("hosted");
    const consumer = await store.createConsumer({ id: "codex", mirrors: ["hosted"] });
    await store.commitHostedFrame({ kind: "events", subscriptionId: "hosted", cursorEpoch: 0, events: [{
      specversion: "1.0", id: "readme-event", source: "source/readme", sourceId: "source/readme", type: "dev.bugler.readme",
      seq: 1, receivedat: "2026-08-25T00:00:00.000Z", verificationmethod: "none", payloadverified: false, rawSize: 1,
    }] });
    store.close();
    const daemon = Bun.spawn([process.execPath, join(root, "src/bugler.ts"), "daemon", "start", "--account", "work", ...test.args], { stdout: "pipe", stderr: "pipe" });
    for (let attempt = 0; attempt < 100; attempt += 1) {
      if (await stat(config.socketPath).then((entry) => entry.isSocket()).catch(() => false)) break;
      await Bun.sleep(10);
    }
    const command = Bun.spawn([process.execPath, "run", "bugler", ...readmeCommand.split(" ").slice(3), ...test.args], {
      cwd: root,
      env: {
        ...process.env,
        BUGLER_LOCAL_TOKEN: consumer.token,
        XDG_CONFIG_HOME: overrides.configRoot,
        XDG_STATE_HOME: overrides.stateRoot,
        XDG_RUNTIME_DIR: overrides.runtimeRoot,
      },
      stdout: "pipe",
      stderr: "pipe",
    });
    const [exitCode, stdout, stderr] = await Promise.all([command.exited, new Response(command.stdout).text(), new Response(command.stderr).text()]);
    expect(exitCode, stderr).toBe(0);
    expect(JSON.parse(stdout)).toMatchObject({ events: [expect.objectContaining({ id: "readme-event", seq: 1 })], cursor_epoch: 0 });
    let status: { link_state?: string; link_detail?: string } = {};
    for (let attempt = 0; attempt < 100; attempt += 1) {
      status = JSON.parse(await readFile(config.statusPath, "utf8")) as typeof status;
      if (status.link_state === "fallback_unavailable") break;
      await Bun.sleep(10);
    }
    expect(status).toMatchObject({ link_state: "fallback_unavailable", link_detail: "subscriber tokens required for: hosted" });
    daemon.kill("SIGTERM");
    expect(await daemon.exited).toBe(0);
  }, 15_000);
});
