import { DurableObject } from "cloudflare:workers";
import { createApp, type RelayHealth } from "../app";
import { hashToken, hasValidTokenChecksum } from "../auth";
import { WORKERS_DELIVERY_PROFILE } from "../delivery";
import { DurableObjectStorageDriver } from "../storage/durable-object";
import { WorkersDueWorkScheduler } from "./scheduler";
import { WorkersSecretResolver } from "./secrets";
import { createProductEnvironmentReader } from "../environment";

export interface WorkersEnv {
  BUGLER: DurableObjectNamespace<BuglerDurableObject>;
  REVEILLE?: DurableObjectNamespace<BuglerDurableObject>;
  BUGLER_LEGACY_DO?: string;
  BUGLER_TENANT_HOST_SUFFIX?: string;
  [name: string]: unknown;
}

export class BuglerDurableObject extends DurableObject<WorkersEnv> {
  readonly driver: DurableObjectStorageDriver;
  readonly health: RelayHealth = { retentionPressure: false, secretFailures: new Set<string>() };
  private readonly app;
  private readonly scheduler: WorkersDueWorkScheduler;

  constructor(ctx: DurableObjectState, env: WorkersEnv) {
    super(ctx, env);
    this.driver = new DurableObjectStorageDriver(ctx.storage);
    const secrets = new WorkersSecretResolver(env);
    const productEnvironment = createProductEnvironmentReader(env);
    const built = createApp({
      driver: this.driver,
      secrets,
      deliveryProfile: WORKERS_DELIVERY_PROFILE,
      maxPayloadBytes: 1_500_000,
      health: this.health,
    });
    this.app = built.app;
    this.scheduler = new WorkersDueWorkScheduler(ctx.storage, this.driver, secrets, this.health, {
      onEvent: () => built.delivery.notifyNewEvent(),
      retentionEveryMs: numberBinding(productEnvironment("RETENTION_INTERVAL_MS"), 30 * 60_000),
      retentionMaxAgeMs: numberBinding(productEnvironment("RETENTION_MAX_AGE_MS"), 30 * 86_400_000),
      retentionMaxCount: numberBinding(productEnvironment("RETENTION_MAX_COUNT"), 100_000),
      retentionMaxBytes: numberBinding(productEnvironment("RETENTION_MAX_BYTES"), 512 * 1024 * 1024),
      maxSlice: numberBinding(productEnvironment("ALARM_SLICE"), 6),
    });
    ctx.blockConcurrencyWhile(async () => {
      await this.ensureBootstrapAdmin(productEnvironment("BOOTSTRAP_ADMIN_TOKEN"));
      await this.scheduler.initialize();
    });
  }

  override async fetch(request: Request): Promise<Response> {
    const response = await this.app.fetch(request);
    await this.scheduler.refreshAfterRequest(request, response.status);
    await this.scheduler.armNext();
    return response;
  }

  override alarm(): Promise<void> {
    return this.scheduler.runSlice();
  }

  private async ensureBootstrapAdmin(value: unknown): Promise<void> {
    if (typeof value !== "string" || !value) return;
    if (!hasValidTokenChecksum(value, "adm")) throw new Error("BUGLER_BOOTSTRAP_ADMIN_TOKEN is not a valid Bugler admin token");
    const keyHash = hashToken(value);
    const current = (await this.driver.listAdminKeys()).find((key) => key.id === "workers-bootstrap");
    if (current?.keyHash === keyHash) return;
    await this.driver.putAdminKey({
      id: "workers-bootstrap",
      keyHash,
      keyPrefix: value.slice(0, 18),
      createdAt: new Date().toISOString(),
    });
  }
}

function numberBinding(value: unknown, fallback: number): number {
  const parsed = typeof value === "string" || typeof value === "number" ? Number(value) : Number.NaN;
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

export default {
  fetch(request: Request, env: WorkersEnv): Promise<Response> {
    const namespace = env.BUGLER ?? env.REVEILLE;
    if (!namespace) throw new Error("BUGLER Durable Object binding is missing");
    const routed = tenantRoute(request, env.BUGLER_TENANT_HOST_SUFFIX);
    if (routed instanceof Response) return Promise.resolve(routed);
    const instanceName = routed.tenant ?? (env.BUGLER_LEGACY_DO === "1" ? "reveille" : "bugler");
    const id = namespace.idFromName(instanceName);
    return namespace.get(id).fetch(routed.request);
  },
};

function tenantRoute(request: Request, hostSuffix: string | undefined): { tenant?: string; request: Request } | Response {
  const url = new URL(request.url);
  const pathMatch = /^\/t\/([^/]+)(\/.*)?$/.exec(url.pathname);
  let tenant: string | undefined;
  if (pathMatch) {
    tenant = decodeURIComponent(pathMatch[1]!);
    url.pathname = pathMatch[2] || "/";
  } else if (hostSuffix) {
    const suffix = hostSuffix.toLowerCase().startsWith(".") ? hostSuffix.toLowerCase() : `.${hostSuffix.toLowerCase()}`;
    const hostname = url.hostname.toLowerCase();
    if (hostname.endsWith(suffix)) {
      const prefix = hostname.slice(0, -suffix.length);
      if (prefix && !prefix.includes(".")) tenant = prefix;
    }
  }
  if (!tenant) return { request };
  if (!/^[a-z0-9](?:[a-z0-9-]{0,62})$/.test(tenant)) {
    return Response.json({ error: "invalid tenant id" }, { status: 400 });
  }
  const headers = new Headers(request.headers);
  headers.set("x-bugler-tenant-id", tenant);
  return { tenant, request: new Request(url, { method: request.method, headers, body: request.body, redirect: request.redirect }) };
}
