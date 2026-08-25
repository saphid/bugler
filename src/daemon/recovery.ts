import { copyFile, mkdir, readFile, readdir, rename, rm, stat } from "node:fs/promises";
import { dirname, join } from "node:path";
import type { DaemonAccountConfig } from "./account";
import { readSecretFile } from "./account";
import { SqliteLocalStore } from "./sqlite-store";
import { LocalActionManager } from "./actions";

interface HostedSubscriptionMetadata {
  id: string;
  cursor: number;
  cursor_epoch: number;
  oldest_available_seq: number;
  revoked: boolean;
}

export async function recoverDaemonFromHosted(options: {
  config: DaemonAccountConfig;
  fetcher?: (input: string | URL | Request, init?: RequestInit) => Promise<Response>;
  now?: () => Date;
}): Promise<{ managementToken: string; subscriptions: number; lossDisclosure: string }> {
  const fetcher = options.fetcher ?? fetch;
  const now = options.now ?? (() => new Date());
  if (await stat(options.config.databasePath).then(() => true).catch(() => false)) {
    throw new Error("total-loss recovery requires the local database to be absent; preserve or move the suspect file first");
  }
  const credential = await readSecretFile(options.config.hostedCredentialFile);
  const base = `${options.config.origin.replace(/\/$/, "")}/t/${encodeURIComponent(options.config.tenantId)}`;
  const headers = { authorization: `Bearer ${credential}`, "content-type": "application/json" };
  const session = await fetcher(`${base}/daemon/sessions`, {
    method: "POST", redirect: "manual", headers, body: '{"protocol":1,"channels":[]}',
  });
  if (!session.ok) throw new Error(`recovery session validation failed with ${session.status}`);
  const metadataResponse = await fetcher(`${base}/daemon/subscriptions`, { redirect: "manual", headers });
  if (!metadataResponse.ok) throw new Error(`recovery subscription listing failed with ${metadataResponse.status}`);
  const metadata = await metadataResponse.json() as { subscriptions?: HostedSubscriptionMetadata[] };
  if (!Array.isArray(metadata.subscriptions)) throw new Error("recovery subscription listing was malformed");
  const active = metadata.subscriptions.filter((subscription) => !subscription.revoked);
  await mkdir(dirname(options.config.databasePath), { recursive: true, mode: 0o700 });
  const temporary = `${options.config.databasePath}.recovering`;
  await rm(temporary, { force: true });
  await rm(`${temporary}-wal`, { force: true });
  await rm(`${temporary}-shm`, { force: true });
  const store = new SqliteLocalStore(temporary, { accountKey: options.config.accountKey, now });
  let managementToken: string;
  try {
    for (const subscription of active) {
      await store.createMirror(subscription.id, subscription.cursor, subscription.cursor_epoch);
      await store.putRecoveryNotice({
        subscriptionId: subscription.id,
        recoveredAt: now().toISOString(),
        hostedCursor: subscription.cursor,
        hostedCursorEpoch: subscription.cursor_epoch,
        oldestAvailableSeq: subscription.oldest_available_seq,
      });
    }
    const manager = new LocalActionManager({ actionsDirectory: options.config.actionsDirectory, stateDirectory: options.config.stateDirectory, store, now });
    for (const entry of await readdir(options.config.actionsDirectory, { withFileTypes: true }).catch(() => [])) {
      if (!entry.isFile() || !entry.name.endsWith(".json")) continue;
      await manager.install(JSON.parse(await readFile(join(options.config.actionsDirectory, entry.name), "utf8")) as unknown);
    }
    managementToken = await store.mintManagementToken();
  } finally {
    store.close();
  }
  await rename(temporary, options.config.databasePath);
  return {
    managementToken,
    subscriptions: active.length,
    lossDisclosure: "Local consumer definitions, tokens, cursors, action attempts, and dead letters were lost. Hosted events at or before each recovered cursor may not be replayable.",
  };
}

export async function restoreDaemonBackup(options: {
  config: DaemonAccountConfig;
  backupPath: string;
  now?: () => Date;
}): Promise<{ suspectPath?: string }> {
  if (!(await stat(options.backupPath).then((entry) => entry.isFile()).catch(() => false))) throw new Error("backup file not found");
  const now = options.now ?? (() => new Date());
  const temporary = `${options.config.databasePath}.restoring`;
  const databaseExists = await stat(options.config.databasePath).then((entry) => entry.isFile()).catch(() => false);
  const suspectPath = databaseExists ? `${options.config.databasePath}.suspect-${now().toISOString().replace(/[:.]/g, "-")}` : undefined;
  await rm(temporary, { force: true });
  await copyFile(options.backupPath, temporary);
  const verification = new SqliteLocalStore(temporary, { accountKey: options.config.accountKey, now });
  verification.close();
  if (suspectPath) await copyFile(options.config.databasePath, suspectPath);
  await rm(`${options.config.databasePath}-wal`, { force: true });
  await rm(`${options.config.databasePath}-shm`, { force: true });
  await rename(temporary, options.config.databasePath);
  return { ...(suspectPath ? { suspectPath } : {}) };
}
