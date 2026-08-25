import { createHash } from "node:crypto";
import { constants } from "node:fs";
import { chmod, lstat, mkdir, open, readFile, readdir, rename, stat, writeFile } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";

export interface DaemonAccountConfig {
  account: string;
  accountKey: string;
  origin: string;
  tenantId: string;
  principalId: string;
  configDirectory: string;
  stateDirectory: string;
  runtimeDirectory: string;
  socketPath: string;
  databasePath: string;
  managementTokenFile: string;
  hostedCredentialFile: string;
  subscriberTokenDirectory: string;
  actionsDirectory: string;
  statusPath: string;
  lockPath: string;
}

export interface DaemonPathOverrides {
  configRoot?: string;
  stateRoot?: string;
  runtimeRoot?: string;
}

const ACCOUNT_NAME = /^[a-z][a-z0-9-]{0,31}$/;
const BASE32 = "abcdefghijklmnopqrstuvwxyz234567";

function base32(bytes: Uint8Array): string {
  let bits = 0;
  let value = 0;
  let output = "";
  for (const byte of bytes) {
    value = (value << 8) | byte;
    bits += 8;
    while (bits >= 5) {
      output += BASE32[(value >>> (bits - 5)) & 31];
      bits -= 5;
    }
  }
  if (bits > 0) output += BASE32[(value << (5 - bits)) & 31];
  return output;
}

export function canonicalDaemonOrigin(value: string): string {
  const url = new URL(value);
  if (url.protocol !== "http:" && url.protocol !== "https:") throw new Error("daemon origin must use http or https");
  if (url.pathname !== "/" || url.search || url.hash || url.username || url.password) throw new Error("daemon origin must not contain path, credentials, query, or fragment");
  return url.origin.toLowerCase();
}

export function daemonAccountKey(origin: string, tenantId: string, principalId: string): string {
  return base32(createHash("sha256").update(`${canonicalDaemonOrigin(origin)}\0${tenantId}\0${principalId}`).digest()).slice(0, 20);
}

export function daemonDefaultRuntimeRoot(
  platform = process.platform,
  home = homedir(),
  environment: Record<string, string | undefined> = process.env,
): string {
  if (platform === "darwin") return join(home, "Library/Application Support");
  return environment.XDG_RUNTIME_DIR ?? join("/tmp", `bugler-${process.getuid?.() ?? 0}`);
}

function defaults(overrides: DaemonPathOverrides = {}): Required<DaemonPathOverrides> {
  const configRoot = overrides.configRoot ?? process.env.XDG_CONFIG_HOME ?? (process.platform === "darwin" ? join(homedir(), "Library/Application Support") : join(homedir(), ".config"));
  const stateRoot = overrides.stateRoot ?? process.env.XDG_STATE_HOME ?? (process.platform === "darwin" ? join(homedir(), "Library/Application Support") : join(homedir(), ".local/state"));
  const runtimeRoot = overrides.runtimeRoot ?? daemonDefaultRuntimeRoot();
  return { configRoot, stateRoot, runtimeRoot };
}

export function daemonAccountConfig(input: {
  account: string;
  origin: string;
  tenantId: string;
  principalId: string;
}, overrides: DaemonPathOverrides = {}): DaemonAccountConfig {
  if (!ACCOUNT_NAME.test(input.account)) throw new Error("account must match ^[a-z][a-z0-9-]{0,31}$");
  const root = defaults(overrides);
  const origin = canonicalDaemonOrigin(input.origin);
  const accountKey = daemonAccountKey(origin, input.tenantId, input.principalId);
  const configDirectory = join(root.configRoot, "bugler/accounts", accountKey);
  const stateDirectory = join(root.stateRoot, "bugler/accounts", accountKey);
  const runtimeDirectory = join(root.runtimeRoot, "bugler");
  return {
    account: input.account,
    accountKey,
    origin,
    tenantId: input.tenantId,
    principalId: input.principalId,
    configDirectory,
    stateDirectory,
    runtimeDirectory,
    socketPath: join(runtimeDirectory, `${accountKey}.sock`),
    databasePath: join(stateDirectory, "daemon.db"),
    managementTokenFile: join(configDirectory, "management-token"),
    hostedCredentialFile: join(configDirectory, "hosted-credential"),
    subscriberTokenDirectory: join(configDirectory, "subscriber-tokens"),
    actionsDirectory: join(configDirectory, "actions"),
    statusPath: join(stateDirectory, "status.json"),
    lockPath: join(stateDirectory, "daemon.lock"),
  };
}

export function subscriberTokenPath(config: DaemonAccountConfig, subscriptionId: string): string {
  const name = createHash("sha256").update(subscriptionId).digest("hex");
  return join(config.subscriberTokenDirectory, `${name}.token`);
}

export async function persistDaemonAccount(config: DaemonAccountConfig): Promise<void> {
  for (const directory of [config.configDirectory, config.stateDirectory, config.runtimeDirectory]) {
    await mkdir(directory, { recursive: true, mode: 0o700 });
    await chmod(directory, 0o700);
  }
  const path = join(config.configDirectory, "config.json");
  const existing = await readFile(path, "utf8").then((value) => JSON.parse(value) as DaemonAccountConfig).catch(() => undefined);
  if (existing && (existing.account !== config.account || existing.accountKey !== config.accountKey)) {
    throw new Error(`daemon identity is already configured as account ${existing.account}`);
  }
  await writeFile(path, `${JSON.stringify(config, null, 2)}\n`, { mode: 0o600 });
  await chmod(path, 0o600);
}

export async function loadDaemonAccount(account: string, overrides: DaemonPathOverrides = {}): Promise<DaemonAccountConfig> {
  const root = defaults(overrides);
  const accounts = join(root.configRoot, "bugler/accounts");
  for (const entry of await readdir(accounts, { withFileTypes: true }).catch(() => [])) {
    if (!entry.isDirectory()) continue;
    const path = join(accounts, entry.name, "config.json");
    const parsed = JSON.parse(await readFile(path, "utf8")) as DaemonAccountConfig;
    if (parsed.account !== account) continue;
    const expected = daemonAccountConfig({ account, origin: parsed.origin, tenantId: parsed.tenantId, principalId: parsed.principalId }, overrides);
    if (expected.accountKey !== parsed.accountKey) throw new Error("daemon account identity does not match its account key");
    return expected;
  }
  throw new Error(`daemon account ${account} not found`);
}

export async function writeSecretFile(path: string, value: string): Promise<void> {
  const handle = await open(`${path}.tmp`, "w", 0o600);
  try {
    await handle.writeFile(`${value}\n`);
    await handle.sync();
  } finally {
    await handle.close();
  }
  await rename(`${path}.tmp`, path);
  await chmod(path, 0o600);
}

export async function assertPrivatePath(path: string, mode: number): Promise<void> {
  const details = await stat(path);
  if ((details.mode & 0o777) !== mode) throw new Error(`${path} must have mode ${mode.toString(8)}`);
  if (typeof process.getuid === "function" && details.uid !== process.getuid()) throw new Error(`${path} must be owned by the current user`);
}

export async function readSecretFile(path: string): Promise<string> {
  const details = await lstat(path);
  if (details.isSymbolicLink() || !details.isFile()) throw new Error(`${path} must be a regular file, not a symlink`);
  if (details.nlink !== 1) throw new Error(`${path} must have exactly one hard link`);
  await assertPrivatePath(path, 0o600);
  const handle = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    const value = (await handle.readFile("utf8")).trim();
    if (!value) throw new Error(`${path} is empty`);
    return value;
  } finally {
    await handle.close();
  }
}
