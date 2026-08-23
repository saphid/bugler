import { createHash, randomBytes } from "node:crypto";

const CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";

export function newUlid(now = Date.now()): string {
  let time = BigInt(now);
  let encodedTime = "";
  for (let index = 0; index < 10; index += 1) {
    encodedTime = CROCKFORD[Number(time & 31n)] + encodedTime;
    time >>= 5n;
  }
  const bytes = randomBytes(10);
  let random = 0n;
  for (const byte of bytes) random = (random << 8n) | BigInt(byte);
  let encodedRandom = "";
  for (let index = 0; index < 16; index += 1) {
    encodedRandom = CROCKFORD[Number(random & 31n)] + encodedRandom;
    random >>= 5n;
  }
  return encodedTime + encodedRandom;
}

export function sha256(value: string | Uint8Array): string {
  return createHash("sha256").update(value).digest("hex");
}

export function parseDurationSeconds(value: unknown, fallback = 0): number {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value !== "string") return fallback;
  const match = /^([0-9]+(?:\.[0-9]+)?)(ms|s|m|h)?$/.exec(value.trim());
  if (!match) return fallback;
  const amount = Number(match[1]);
  const multiplier = match[2] === "ms" ? 0.001 : match[2] === "m" ? 60 : match[2] === "h" ? 3600 : 1;
  return amount * multiplier;
}

export function asErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export function isoTimestamp(date: Date): string {
  return date.toISOString().replace(".000Z", "Z");
}

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
