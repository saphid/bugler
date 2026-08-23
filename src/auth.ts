import { createHash, randomBytes, timingSafeEqual } from "node:crypto";
import { Buffer } from "node:buffer";
import type { AdminKey, Subscription } from "./domain";
import type { StorageDriver } from "./storage/driver";
import { newUlid } from "./util";

type TokenKind = "adm" | "sub";

function crc32(input: string): string {
  let crc = 0xffffffff;
  for (const byte of new TextEncoder().encode(input)) {
    crc ^= byte;
    for (let bit = 0; bit < 8; bit += 1) crc = (crc >>> 1) ^ (0xedb88320 & -(crc & 1));
  }
  return ((crc ^ 0xffffffff) >>> 0).toString(16).padStart(8, "0");
}

export function mintToken(kind: TokenKind): { token: string; hash: string; prefix: string } {
  const body = `bgl_${kind}_${Buffer.from(randomBytes(24)).toString("base64url")}`;
  const token = `${body}_${crc32(body)}`;
  return { token, hash: hashToken(token), prefix: token.slice(0, 18) };
}

export function hasValidTokenChecksum(token: string, kind?: TokenKind): boolean {
  if (kind && !token.startsWith(`bgl_${kind}_`) && !token.startsWith(`rvl_${kind}_`)) return false;
  if (!kind && !token.startsWith("bgl_adm_") && !token.startsWith("bgl_sub_") && !token.startsWith("rvl_adm_") && !token.startsWith("rvl_sub_")) return false;
  const split = token.lastIndexOf("_");
  if (split < 8) return false;
  const body = token.slice(0, split);
  const actual = token.slice(split + 1);
  const expected = crc32(body);
  if (actual.length !== expected.length) return false;
  return timingSafeEqual(Buffer.from(actual), Buffer.from(expected));
}

export function hashToken(token: string): string {
  return createHash("sha256").update(token).digest("hex");
}

function equalHash(left: string, right: string): boolean {
  if (left.length !== right.length) return false;
  return timingSafeEqual(Buffer.from(left), Buffer.from(right));
}

export function bearerToken(header: string | undefined): string | undefined {
  if (!header?.startsWith("Bearer ")) return undefined;
  const token = header.slice(7).trim();
  return token || undefined;
}

export async function authenticateAdmin(driver: StorageDriver, token: string | undefined, now = new Date()): Promise<AdminKey | undefined> {
  if (!token || !hasValidTokenChecksum(token, "adm")) return undefined;
  const hash = hashToken(token);
  const keys = await driver.listAdminKeys();
  return keys.find((key) => (!key.expiresAt || Date.parse(key.expiresAt) > now.getTime()) && equalHash(key.keyHash, hash));
}

export async function authenticateSubscriber(
  driver: StorageDriver,
  subscriptionId: string,
  token: string | undefined,
  now = new Date(),
): Promise<Subscription | undefined> {
  if (!token || !hasValidTokenChecksum(token, "sub")) return undefined;
  const subscription = await driver.getSubscription(subscriptionId);
  if (!subscription || subscription.tokenRevoked) return undefined;
  const hash = hashToken(token);
  if (equalHash(subscription.tokenHash, hash)) return subscription;
  if (subscription.oldTokenHash && subscription.oldTokenExpiresAt && Date.parse(subscription.oldTokenExpiresAt) > now.getTime() && equalHash(subscription.oldTokenHash, hash)) {
    return subscription;
  }
  return undefined;
}

export async function authenticateAnySubscriber(driver: StorageDriver, token: string | undefined, now = new Date()): Promise<Subscription | undefined> {
  if (!token || !hasValidTokenChecksum(token, "sub")) return undefined;
  const hash = hashToken(token);
  return (await driver.listSubscriptions()).find((subscription) => {
    if (subscription.tokenRevoked) return false;
    if (equalHash(subscription.tokenHash, hash)) return true;
    return Boolean(
      subscription.oldTokenHash &&
      subscription.oldTokenExpiresAt &&
      Date.parse(subscription.oldTokenExpiresAt) > now.getTime() &&
      equalHash(subscription.oldTokenHash, hash),
    );
  });
}

export function makeAdminKey(now = new Date()): { key: AdminKey; token: string } {
  const minted = mintToken("adm");
  return {
    token: minted.token,
    key: {
      id: newUlid(now.getTime()),
      keyHash: minted.hash,
      keyPrefix: minted.prefix,
      createdAt: now.toISOString(),
    },
  };
}
