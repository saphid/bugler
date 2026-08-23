import { createHash, randomUUID } from "node:crypto";
import { isIP } from "node:net";
import { lookup } from "node:dns/promises";
import type { AppendEventInput, PollConfig, PollState, Source } from "./domain";
import { jsonPointer, mapEnvelope } from "./mapping";
import type { StorageDriver } from "./storage/driver";
import { asErrorMessage } from "./util";

export interface PollRunResult {
  status: "baseline" | "changed" | "unchanged" | "busy" | "failed";
  generation: number;
  event?: Awaited<ReturnType<StorageDriver["appendEvent"]>>["event"];
  error?: string;
}

export interface PollEngineOptions {
  fetch?: (input: string | URL | Request, init?: RequestInit) => Promise<Response>;
  now?: () => Date;
  maxPayloadBytes?: number;
  timeoutMs?: number;
  maxRedirects?: number;
  leaseMs?: number;
  beforeCommit?: (event: AppendEventInput) => Promise<void>;
}

function isPrivateIpv4(host: string): boolean {
  const parts = host.split(".").map(Number);
  if (parts.length !== 4 || parts.some((part) => !Number.isInteger(part) || part < 0 || part > 255)) return false;
  return parts[0] === 10 || parts[0] === 127 || (parts[0] === 169 && parts[1] === 254) ||
    (parts[0] === 172 && (parts[1] ?? 0) >= 16 && (parts[1] ?? 0) <= 31) ||
    (parts[0] === 192 && parts[1] === 168);
}

export function assertPollUrlAllowed(rawUrl: string, denyPrivate: boolean): URL {
  const url = new URL(rawUrl);
  if (url.protocol !== "http:" && url.protocol !== "https:") throw new Error("poll URL must use http or https");
  if (denyPrivate) {
    const host = url.hostname.toLowerCase();
    if (host === "localhost" || host.endsWith(".localhost") || isPrivateIpv4(host) || (isIP(host) === 6 && (host === "::1" || host.startsWith("fc") || host.startsWith("fd") || host.startsWith("fe80")))) {
      throw new Error("poll URL resolves to a denied private address literal");
    }
  }
  return url;
}

async function assertResolvedAddressAllowed(url: URL, denyPrivate: boolean): Promise<void> {
  if (!denyPrivate) return;
  const addresses = await lookup(url.hostname, { all: true, verbatim: true });
  for (const { address, family } of addresses) {
    if ((family === 4 && isPrivateIpv4(address)) || (family === 6 && (address === "::1" || address.startsWith("fc") || address.startsWith("fd") || address.toLowerCase().startsWith("fe80")))) {
      throw new Error("poll URL resolves to a denied private address");
    }
  }
}

async function readCapped(response: Response, cap: number): Promise<Uint8Array> {
  const declared = Number(response.headers.get("content-length"));
  if (Number.isFinite(declared) && declared > cap) throw new Error("poll response exceeds payload cap");
  if (!response.body) return new Uint8Array();
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value.byteLength;
    if (total > cap) {
      await reader.cancel();
      throw new Error("poll response exceeds payload cap");
    }
    chunks.push(value);
  }
  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return bytes;
}

function fingerprint(config: PollConfig, response: Response, body: Uint8Array): string {
  const etag = response.headers.get("etag");
  const lastModified = response.headers.get("last-modified");
  if ((config.change_detect === "etag" || config.change_detect === undefined) && etag) return `etag:${etag}`;
  if (config.change_detect === undefined && lastModified) return `last-modified:${lastModified}`;
  if (typeof config.change_detect === "object") {
    const parsed = JSON.parse(new TextDecoder().decode(body));
    const selected = jsonPointer(parsed, config.change_detect.json_pointer);
    return `json:${createHash("sha256").update(JSON.stringify(selected)).digest("hex")}`;
  }
  return `body:${createHash("sha256").update(body).digest("hex")}`;
}

function withoutLease(state: PollState): PollState {
  const next = { ...state };
  delete next.leaseToken;
  delete next.leaseExpiresAt;
  return next;
}

export class PollEngine {
  private readonly fetcher: (input: string | URL | Request, init?: RequestInit) => Promise<Response>;
  private readonly now: () => Date;
  private readonly maxPayloadBytes: number;
  private readonly timeoutMs: number;
  private readonly maxRedirects: number;
  private readonly leaseMs: number;
  private readonly beforeCommit: ((event: AppendEventInput) => Promise<void>) | undefined;

  constructor(private readonly driver: StorageDriver, options: PollEngineOptions = {}) {
    this.fetcher = options.fetch ?? fetch;
    this.now = options.now ?? (() => new Date());
    this.maxPayloadBytes = Math.min(options.maxPayloadBytes ?? 5 * 1024 * 1024, driver.capabilities.maxPayloadBytes ?? Number.POSITIVE_INFINITY);
    this.timeoutMs = options.timeoutMs ?? 30_000;
    this.maxRedirects = options.maxRedirects ?? 3;
    this.leaseMs = options.leaseMs ?? 60_000;
    this.beforeCommit = options.beforeCommit;
  }

  private async fetchWithRedirects(url: URL, headers: Headers, denyPrivate: boolean): Promise<Response> {
    let current = url;
    for (let redirects = 0; redirects <= this.maxRedirects; redirects += 1) {
      assertPollUrlAllowed(current.toString(), denyPrivate);
      await assertResolvedAddressAllowed(current, denyPrivate);
      const response = await this.fetcher(current, {
        method: "GET",
        headers,
        redirect: "manual",
        signal: AbortSignal.timeout(this.timeoutMs),
      });
      if (![301, 302, 303, 307, 308].includes(response.status)) return response;
      const location = response.headers.get("location");
      if (!location || redirects === this.maxRedirects) throw new Error("poll redirect limit exceeded");
      current = new URL(location, current);
    }
    throw new Error("poll redirect limit exceeded");
  }

  async run(source: Source): Promise<PollRunResult> {
    if (source.kind !== "poll" || !source.poll) throw new Error("source is not a poll source");
    const started = this.now();
    const leaseToken = randomUUID();
    const claimed = await this.driver.claimPollLease(
      source.id,
      leaseToken,
      new Date(started.getTime() + this.leaseMs).toISOString(),
      started.toISOString(),
    );
    if (!claimed) return { status: "busy", generation: (await this.driver.getPollState(source.id))?.generation ?? 0 };

    try {
      const url = assertPollUrlAllowed(source.poll.url, source.poll.deny_private_addresses ?? false);
      const headers = new Headers({ accept: "*/*" });
      if (claimed.etag) headers.set("if-none-match", claimed.etag);
      if (claimed.lastModified) headers.set("if-modified-since", claimed.lastModified);
      const response = await this.fetchWithRedirects(url, headers, source.poll.deny_private_addresses ?? false);
      const completed = this.now();
      if (response.status === 304) {
        const state = withoutLease({
          ...claimed,
          lastSuccessAt: completed.toISOString(),
          nextRunAt: new Date(completed.getTime() + source.poll.interval_s * 1000).toISOString(),
          failureCount: 0,
        });
        const result = await this.driver.appendPollTransition({ state, expectedLeaseToken: leaseToken });
        return { status: result.committed ? "unchanged" : "busy", generation: state.generation };
      }
      if (!response.ok) throw new Error(`poll target returned ${response.status}`);
      const body = await readCapped(response, this.maxPayloadBytes);
      const nextFingerprint = fingerprint(source.poll, response, body);
      const first = claimed.fingerprint === undefined;
      const changed = !first && claimed.fingerprint !== nextFingerprint;
      const shouldFire = changed || (first && (source.poll.fire_on_first ?? false));
      const generation = shouldFire ? claimed.generation + 1 : claimed.generation;
      const responseEtag = response.headers.get("etag");
      const responseLastModified = response.headers.get("last-modified");
      const nextState = withoutLease({
        ...claimed,
        ...(responseEtag ? { etag: responseEtag } : {}),
        ...(responseLastModified ? { lastModified: responseLastModified } : {}),
        fingerprint: nextFingerprint,
        generation,
        lastSuccessAt: completed.toISOString(),
        nextRunAt: new Date(completed.getTime() + source.poll.interval_s * 1000).toISOString(),
        failureCount: 0,
      });
      let event: AppendEventInput | undefined;
      if (shouldFire) {
        const contentType = response.headers.get("content-type");
        event = mapEnvelope({
          sourceId: source.id,
          mapping: source.mapping,
          headers: response.headers,
          rawBody: body,
          ...(contentType ? { contentType } : {}),
          verificationMethod: "poll",
          now: completed,
        });
        event.dedupekey = `${source.id}/${generation}`;
        if (this.beforeCommit) await this.beforeCommit(event);
      }
      const committed = await this.driver.appendPollTransition({
        state: nextState,
        expectedLeaseToken: leaseToken,
        ...(event ? { event } : {}),
      });
      if (!committed.committed) return { status: "busy", generation: claimed.generation };
      if (committed.event) return { status: "changed", generation, event: committed.event };
      return { status: first ? "baseline" : "unchanged", generation };
    } catch (error) {
      const failedAt = this.now();
      const failureCount = claimed.failureCount + 1;
      const delaySeconds = Math.min(source.poll.interval_s * 2 ** (failureCount - 1), 3600);
      const state = withoutLease({
        ...claimed,
        failureCount,
        nextRunAt: new Date(failedAt.getTime() + delaySeconds * 1000).toISOString(),
      });
      await this.driver.appendPollTransition({ state, expectedLeaseToken: leaseToken });
      return { status: "failed", generation: claimed.generation, error: asErrorMessage(error) };
    }
  }
}
