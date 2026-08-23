import type { ReconcileConfig, Source } from "./domain";
import type { SecretResolver } from "./secrets";
import type { StorageDriver } from "./storage/driver";
import { asErrorMessage } from "./util";

interface GitHubDelivery {
  id: number;
  guid: string;
  status: string;
  delivered_at: string;
}

export interface ReconcileResult {
  status: "success" | "failed" | "disabled";
  scanned?: number;
  redelivered?: number;
  error?: string;
}

export class GitHubReconciler {
  constructor(
    private readonly driver: StorageDriver,
    private readonly resolver: SecretResolver,
    private readonly fetcher: (input: string | URL | Request, init?: RequestInit) => Promise<Response> = fetch,
    private readonly now: () => Date = () => new Date(),
  ) {}

  private deliveriesUrl(source: Source, config: ReconcileConfig): string {
    if (config.hook.startsWith("http://") || config.hook.startsWith("https://")) return config.hook.replace(/\/$/, "");
    if (!source.mapping.source.startsWith("github/")) throw new Error("GitHub source mapping must identify github/owner/repo");
    const repository = source.mapping.source.slice("github/".length);
    return `https://api.github.com/repos/${repository}/hooks/${encodeURIComponent(config.hook)}/deliveries`;
  }

  async run(source: Source): Promise<ReconcileResult> {
    if (!source.reconcile) return { status: "disabled" };
    const config = source.reconcile;
    try {
      const credential = await this.resolver.resolve(config.credential_ref);
      const base = this.deliveriesUrl(source, config);
      const intervalSeconds = config.interval_s ?? 3600;
      const cutoff = config.watermark
        ? new Date(Date.parse(config.watermark) - intervalSeconds * 1000).toISOString()
        : new Date(this.now().getTime() - 3 * 86_400_000).toISOString();
      const deliveries: GitHubDelivery[] = [];
      let nextPageUrl: URL | undefined;
      for (let page = 0; page < 100; page += 1) {
        const url = nextPageUrl ?? new URL(base);
        if (!nextPageUrl) url.searchParams.set("per_page", "100");
        const response = await this.fetcher(url, {
          headers: {
            accept: "application/vnd.github+json",
            authorization: `Bearer ${credential}`,
            "x-github-api-version": "2022-11-28",
          },
          redirect: "manual",
          signal: AbortSignal.timeout(30_000),
        });
        if (!response.ok) throw new Error(`GitHub deliveries returned ${response.status}`);
        const pageRows = await response.json() as GitHubDelivery[];
        deliveries.push(...pageRows.filter((row) => row.delivered_at >= cutoff));
        if (pageRows.length < 100 || pageRows.some((row) => row.delivered_at < cutoff)) break;
        const cursor = response.headers.get("x-next-cursor");
        const link = response.headers.get("link")?.match(/<([^>]+)>;\s*rel="next"/i)?.[1];
        if (link) nextPageUrl = new URL(link);
        else if (cursor) {
          nextPageUrl = new URL(base);
          nextPageUrl.searchParams.set("per_page", "100");
          nextPageUrl.searchParams.set("cursor", cursor);
        } else break;
      }
      const groups = new Map<string, GitHubDelivery[]>();
      for (const delivery of deliveries) {
        const group = groups.get(delivery.guid) ?? [];
        group.push(delivery);
        groups.set(delivery.guid, group);
      }
      let redelivered = 0;
      for (const group of groups.values()) {
        if (group.some((delivery) => delivery.status === "OK")) continue;
        const newest = group.sort((a, b) => b.delivered_at.localeCompare(a.delivered_at))[0]!;
        const response = await this.fetcher(`${base}/${newest.id}/attempts`, {
          method: "POST",
          headers: {
            accept: "application/vnd.github+json",
            authorization: `Bearer ${credential}`,
            "x-github-api-version": "2022-11-28",
          },
          redirect: "manual",
          signal: AbortSignal.timeout(30_000),
        });
        if (!response.ok) throw new Error(`GitHub redelivery returned ${response.status}`);
        redelivered += 1;
      }
      const completed = this.now().toISOString();
      source.reconcile = {
        ...config,
        watermark: deliveries.reduce((latest, delivery) => delivery.delivered_at > latest ? delivery.delivered_at : latest, config.watermark ?? completed),
        last_success_at: completed,
        failure_count: 0,
        next_run_at: new Date(Date.parse(completed) + intervalSeconds * 1000).toISOString(),
      };
      await this.driver.putSource(source);
      return { status: "success", scanned: deliveries.length, redelivered };
    } catch (error) {
      const message = asErrorMessage(error);
      source.reconcile = {
        ...config,
        failure_count: (config.failure_count ?? 0) + 1,
        next_run_at: new Date(this.now().getTime() + (config.interval_s ?? 3600) * 1000).toISOString(),
      };
      await this.driver.putSource(source);
      return { status: "failed", error: message };
    }
  }
}
