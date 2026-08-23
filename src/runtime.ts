import type { RelayHealth } from "./app";
import { PollEngine } from "./poll";
import { pushAttemptDue, PushEngine } from "./push";
import { GitHubReconciler } from "./reconcile";
import type { SecretResolver } from "./secrets";
import type { StorageDriver } from "./storage/driver";

export interface RuntimeOptions {
  tickMs?: number;
  retentionEveryMs?: number;
  retentionMaxAgeMs?: number;
  retentionMaxCount?: number;
  retentionMaxBytes?: number;
  pinningWindowMs?: number;
  now?: () => Date;
  onEvent?: () => void;
}

export class RelayRuntime {
  private timer?: ReturnType<typeof setInterval>;
  private running = false;
  private lastRetentionAt = 0;
  private readonly poll: PollEngine;
  private readonly push: PushEngine;
  private readonly reconcile: GitHubReconciler;
  private readonly now: () => Date;
  private readonly runtimeOptions: Required<Omit<RuntimeOptions, "now" | "onEvent">>;
  private readonly onEvent: (() => void) | undefined;

  constructor(
    private readonly driver: StorageDriver,
    secrets: SecretResolver,
    private readonly health: RelayHealth,
    options: RuntimeOptions = {},
  ) {
    this.now = options.now ?? (() => new Date());
    this.onEvent = options.onEvent;
    this.runtimeOptions = {
      tickMs: options.tickMs ?? 1_000,
      retentionEveryMs: options.retentionEveryMs ?? 60_000,
      retentionMaxAgeMs: options.retentionMaxAgeMs ?? 30 * 86_400_000,
      retentionMaxCount: options.retentionMaxCount ?? 100_000,
      retentionMaxBytes: options.retentionMaxBytes ?? 1024 * 1024 * 1024,
      pinningWindowMs: options.pinningWindowMs ?? options.retentionMaxAgeMs ?? 30 * 86_400_000,
    };
    this.poll = new PollEngine(driver, { now: this.now });
    this.push = new PushEngine(driver, secrets, { now: this.now });
    this.reconcile = new GitHubReconciler(driver, secrets, fetch, this.now);
  }

  start(): void {
    if (this.timer) return;
    void this.tick().catch((error) => process.stderr.write(`bugler runtime: ${String(error)}\n`));
    this.timer = setInterval(() => void this.tick().catch((error) => process.stderr.write(`bugler runtime: ${String(error)}\n`)), this.runtimeOptions.tickMs);
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    delete this.timer;
  }

  async tick(): Promise<void> {
    if (this.running) return;
    this.running = true;
    try {
      const now = this.now();
      const sources = await this.driver.listSources();
      for (const source of sources) {
        if (source.kind === "poll" && source.poll) {
          const state = await this.driver.getPollState(source.id);
          if (!state?.nextRunAt || state.nextRunAt <= now.toISOString()) {
            const result = await this.poll.run(source);
            if (result.event) this.onEvent?.();
          }
        }
        if (source.reconcile && (!source.reconcile.next_run_at || source.reconcile.next_run_at <= now.toISOString())) {
          await this.reconcile.run(source);
        }
      }
      const newestSeq = await this.driver.newestSeq();
      const oldestAvailableSeq = await this.driver.oldestAvailableSeq();
      for (const subscription of await this.driver.listSubscriptions()) {
        if (pushAttemptDue(subscription, newestSeq, oldestAvailableSeq, now.toISOString())) {
          await this.push.attempt(subscription.id);
        }
      }
      if (now.getTime() - this.lastRetentionAt >= this.runtimeOptions.retentionEveryMs) {
        const result = await this.driver.enforceRetention({
          maxAgeMs: this.runtimeOptions.retentionMaxAgeMs,
          maxCount: this.runtimeOptions.retentionMaxCount,
          maxBytes: this.runtimeOptions.retentionMaxBytes,
          pinningWindowMs: this.runtimeOptions.pinningWindowMs,
        }, now.toISOString());
        this.health.retentionPressure = result.sizeOverride;
        this.lastRetentionAt = now.getTime();
      }
    } finally {
      this.running = false;
    }
  }
}
