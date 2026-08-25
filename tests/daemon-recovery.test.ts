import { afterEach, describe, expect, it } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { daemonAccountConfig, persistDaemonAccount, writeSecretFile } from "../src/daemon/account";
import { recoverDaemonFromHosted, restoreDaemonBackup } from "../src/daemon/recovery";
import { SqliteLocalStore } from "../src/daemon/sqlite-store";

const scratch: string[] = [];
afterEach(async () => { for (const path of scratch.splice(0)) await rm(path, { recursive: true, force: true }); });

describe("daemon total-loss recovery", () => {
  it("validates a fresh empty session, re-lists mirrors, and gives recreated consumers a local recovery gap", async () => {
    const directory = await mkdtemp(join(tmpdir(), "bugler-recovery-"));
    scratch.push(directory);
    const config = daemonAccountConfig({ account: "work", origin: "https://relay.bugler.test", tenantId: "ten_1", principalId: "member_1" }, {
      configRoot: join(directory, "config"), stateRoot: join(directory, "state"), runtimeRoot: join(directory, "run"),
    });
    await persistDaemonAccount(config);
    await writeSecretFile(config.hostedCredentialFile, "bgl_dmn_obviously-fake-recovery-token");
    const calls: string[] = [];
    const fetcher = async (input: string | URL | Request, init?: RequestInit) => {
      const url = new URL(String(input));
      calls.push(`${init?.method ?? "GET"} ${url.pathname}`);
      expect(new Headers(init?.headers).get("authorization")).toBe("Bearer bgl_dmn_obviously-fake-recovery-token");
      if (url.pathname.endsWith("/daemon/sessions")) {
        expect(JSON.parse(String(init?.body))).toEqual({ protocol: 1, channels: [] });
        return Response.json({ session_id: "recovery" }, { status: 201 });
      }
      return Response.json({ subscriptions: [{ id: "github-starter", cursor: 42, cursor_epoch: 3, oldest_available_seq: 10, revoked: false }] });
    };
    const recovered = await recoverDaemonFromHosted({ config, fetcher, now: () => new Date("2026-08-25T00:00:00.000Z") });
    expect(calls).toEqual(["POST /t/ten_1/daemon/sessions", "GET /t/ten_1/daemon/subscriptions"]);
    expect(recovered.managementToken.startsWith("bgl_lmg_")).toBe(true);
    expect(recovered.lossDisclosure).toContain("were lost");
    const store = new SqliteLocalStore(config.databasePath, { accountKey: config.accountKey });
    expect(await store.listMirrors()).toContainEqual(expect.objectContaining({ id: "github-starter", hostedCursor: 42, cursorEpoch: 3 }));
    const consumer = await store.createConsumer({ id: "rebuilt-agent", mirrors: ["github-starter"] });
    expect(await store.readConsumer("rebuilt-agent")).toEqual({ gap: { from: 1, to: 1 }, cursor_epoch: 0, oldest_available_seq: 1 });
    expect(await store.authenticateManagement(recovered.managementToken)).toBe(true);
    expect(consumer.consumer.nextLocalSeq).toBe(2);
    store.close();
  });

  it("verifies a backup and preserves a suspect database before restoring", async () => {
    const directory = await mkdtemp(join(tmpdir(), "bugler-backup-recovery-"));
    scratch.push(directory);
    const config = daemonAccountConfig({ account: "work", origin: "https://relay.bugler.test", tenantId: "ten_1", principalId: "member_1" }, {
      configRoot: join(directory, "config"), stateRoot: join(directory, "state"), runtimeRoot: join(directory, "run"),
    });
    await persistDaemonAccount(config);
    const store = new SqliteLocalStore(config.databasePath, { accountKey: config.accountKey });
    await store.createMirror("before-backup");
    const backup = `${config.databasePath}.backup-1`;
    await store.createVerifiedBackup(backup, `${config.databasePath}.backup-2`);
    await store.createMirror("after-backup");
    store.close();
    const restored = await restoreDaemonBackup({ config, backupPath: backup, now: () => new Date("2026-08-25T00:00:00Z") });
    expect(restored.suspectPath).toContain("suspect-2026-08-25T00-00-00-000Z");
    const reopened = new SqliteLocalStore(config.databasePath, { accountKey: config.accountKey });
    expect((await reopened.listMirrors()).map((mirror) => mirror.id)).toEqual(expect.arrayContaining(["internal:management", "before-backup"]));
    expect((await reopened.listMirrors()).map((mirror) => mirror.id)).not.toContain("after-backup");
    reopened.close();
  });
});
