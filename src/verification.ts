import { createHmac, timingSafeEqual } from "node:crypto";
import type { VerificationConfig, VerificationMethod } from "./domain";
import type { SecretResolver } from "./secrets";

export interface VerificationResult {
  ok: boolean;
  method: VerificationMethod;
  keyId?: string;
  failureStatus: 200 | 401;
  reason?: string;
}

export const VERIFIER_PRESETS = {
  github: { primitive: "hmac", header: "x-hub-signature-256", encoding: "hex", prefix: "sha256=", signed: "body" },
  gitlab: { primitive: "standard-webhooks", header: "webhook-signature", signed: "id.timestamp.body" },
  "standard-webhooks": { primitive: "standard-webhooks", header: "webhook-signature", signed: "id.timestamp.body" },
  stripe: { primitive: "hmac", header: "stripe-signature", encoding: "hex", signed: "timestamp.body" },
  circleci: { primitive: "hmac", header: "circleci-signature", encoding: "hex", prefix: "v1=", signed: "body" },
  sentry: { primitive: "hmac", header: "sentry-hook-signature", encoding: "hex", signed: "body" },
  "generic-token": { primitive: "token", header: "authorization" },
  "buildkite-signed": { primitive: "hmac", header: "x-buildkite-signature", encoding: "hex", signed: "timestamp+body", experimental: true },
} as const;

function equalText(actual: string, expected: string): boolean {
  const left = Buffer.from(actual);
  const right = Buffer.from(expected);
  if (left.length !== right.length) return false;
  return timingSafeEqual(left, right);
}

function hmac(secret: string | Buffer, content: Uint8Array | string, encoding: "hex" | "base64"): string {
  return createHmac("sha256", secret).update(content).digest(encoding);
}

function standardSecret(secret: string): Buffer | string {
  if (!secret.startsWith("whsec_")) return secret;
  try { return Buffer.from(secret.slice(6), "base64"); } catch { return secret; }
}

async function activeSecrets(resolver: SecretResolver, references: string[]): Promise<Array<{ reference: string; secret: string }>> {
  const values: Array<{ reference: string; secret: string }> = [];
  for (const reference of references) values.push({ reference, secret: await resolver.resolve(reference) });
  return values;
}

function failed(config: VerificationConfig, method: VerificationMethod, reason: string): VerificationResult {
  return { ok: false, method, failureStatus: "failure_status" in config ? config.failure_status ?? 401 : 401, reason };
}

export function secretReferences(config: VerificationConfig): string[] {
  if ("secret_refs" in config) return config.secret_refs;
  if ("primitive" in config && config.primitive === "basic-auth") return [config.username_ref, config.password_ref];
  return [];
}

export async function verifyRequest(
  config: VerificationConfig,
  headers: Headers,
  body: Uint8Array,
  resolver: SecretResolver,
  now = new Date(),
): Promise<VerificationResult> {
  if ("primitive" in config && config.primitive === "none") return { ok: true, method: "none", failureStatus: 401 };
  if ("primitive" in config && config.primitive === "basic-auth") {
    const authorization = headers.get("authorization");
    if (!authorization?.startsWith("Basic ")) return failed(config, "basic-auth", "missing basic authorization");
    const username = await resolver.resolve(config.username_ref);
    const password = await resolver.resolve(config.password_ref);
    const expected = Buffer.from(`${username}:${password}`).toString("base64");
    return equalText(authorization.slice(6), expected)
      ? { ok: true, method: "basic-auth", keyId: `${config.username_ref}+${config.password_ref}`, failureStatus: 401 }
      : failed(config, "basic-auth", "basic authorization mismatch");
  }
  if ("primitive" in config && config.primitive === "api-key") {
    const actual = headers.get(config.header);
    const secrets = await activeSecrets(resolver, config.secret_refs);
    const match = actual && secrets.find(({ secret }) => equalText(actual, secret));
    return match
      ? { ok: true, method: "api-key", keyId: match.reference, failureStatus: 401 }
      : failed(config, "api-key", "API key mismatch");
  }

  if ("preset" in config && config.preset === "generic-token") {
    const actual = headers.get("authorization");
    const secrets = await activeSecrets(resolver, config.secret_refs);
    const match = actual?.startsWith("Bearer ") && secrets.find(({ secret }) => equalText(actual.slice(7), secret));
    return match
      ? { ok: true, method: "token", keyId: match.reference, failureStatus: 401 }
      : failed(config, "token", "bearer token mismatch");
  }

  if ("preset" in config && (config.preset === "gitlab" || config.preset === "standard-webhooks")) {
    const id = headers.get("webhook-id");
    const timestamp = headers.get("webhook-timestamp");
    const signatureHeader = headers.get("webhook-signature");
    if (!id || !timestamp || !signatureHeader) return failed(config, "hmac-sha256", "missing Standard Webhooks headers");
    const tolerance = config.tolerance_s ?? 300;
    const timestampNumber = Number(timestamp);
    if (!/^\d+$/.test(timestamp) || !Number.isFinite(timestampNumber) || Math.abs(now.getTime() / 1000 - timestampNumber) > tolerance) {
      return failed(config, "hmac-sha256", "webhook timestamp outside replay tolerance");
    }
    const content = Buffer.concat([Buffer.from(`${id}.${timestamp}.`), Buffer.from(body)]);
    const signatures = signatureHeader.split(/\s+/).map((value) => value.replace(/^v1[,=]/, ""));
    const secrets = await activeSecrets(resolver, config.secret_refs);
    for (const item of secrets) {
      const expected = hmac(standardSecret(item.secret), content, "base64");
      if (signatures.some((signature) => equalText(signature, expected))) {
        return { ok: true, method: "hmac-sha256", keyId: item.reference, failureStatus: 401 };
      }
    }
    return failed(config, "hmac-sha256", "Standard Webhooks signature mismatch");
  }

  if ("preset" in config && config.preset === "stripe") {
    const signatureHeader = headers.get("stripe-signature");
    if (!signatureHeader) return failed(config, "hmac-sha256", "missing Stripe signature");
    const values = Object.fromEntries(signatureHeader.split(",").map((part) => part.trim().split("=", 2) as [string, string]));
    const timestamp = values.t;
    const signatures = signatureHeader.split(",").filter((part) => part.trim().startsWith("v1=")).map((part) => part.trim().slice(3));
    const timestampNumber = Number(timestamp);
    if (!timestamp || !/^\d+$/.test(timestamp) || !Number.isFinite(timestampNumber) || Math.abs(now.getTime() / 1000 - timestampNumber) > (config.tolerance_s ?? 300)) {
      return failed(config, "hmac-sha256", "Stripe timestamp outside replay tolerance");
    }
    const content = Buffer.concat([Buffer.from(`${timestamp}.`), Buffer.from(body)]);
    const secrets = await activeSecrets(resolver, config.secret_refs);
    for (const item of secrets) {
      const expected = hmac(item.secret, content, "hex");
      if (signatures.some((signature) => equalText(signature, expected))) return { ok: true, method: "hmac-sha256", keyId: item.reference, failureStatus: 401 };
    }
    return failed(config, "hmac-sha256", "Stripe signature mismatch");
  }

  if (!("preset" in config)) return failed(config, "none", "unsupported verifier");
  const preset = VERIFIER_PRESETS[config.preset];
  const signature = headers.get(preset.header);
  if (!signature) return failed(config, "hmac-sha256", "missing signature header");
  const prefix = "prefix" in preset ? preset.prefix : "";
  const actual = signature.startsWith(prefix) ? signature.slice(prefix.length) : signature;
  let content: Uint8Array | string = body;
  if (config.preset === "buildkite-signed") {
    const timestamp = headers.get("x-buildkite-timestamp") ?? "";
    content = Buffer.concat([Buffer.from(timestamp), Buffer.from(body)]);
  }
  const secrets = await activeSecrets(resolver, config.secret_refs);
  for (const item of secrets) {
    if (equalText(actual, hmac(item.secret, content, "hex"))) return { ok: true, method: "hmac-sha256", keyId: item.reference, failureStatus: 401 };
  }
  return failed(config, "hmac-sha256", "signature mismatch");
}
