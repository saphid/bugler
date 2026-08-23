import type { RelayHealth } from "../app";
import type { Source, Subscription } from "../domain";
import { PollEngine } from "../poll";
import { pushAttemptDue, PushEngine, type PushEngineOptions } from "../push";
import { GitHubReconciler } from "../reconcile";
import type { SecretResolver } from "../secrets";
import type { StorageDriver } from "../storage/driver";

interface AlarmStorage {
  get<T>(key: string): Promise<T | undefined>;
  put<T>(key: string, value: T): Promise<void>;
  delete(key: string): Promise<boolean>;
  getAlarm(): Promise<number | null>;
  setAlarm(time: number | Date): Promise<void>;
}

export interface WorkersSchedulerOptions {
  now?: () => Date;
  retentionEveryMs?: number;
  retentionMaxAgeMs?: number;
  retentionMaxCount?: number;
  retentionMaxBytes?: number;
  pinningWindowMs?: number;
  maxSlice?: number;
  onEvent?: () => void;
  pushFetch?: PushEngineOptions["fetch"];
  onPushDelivered?: PushEngineOptions["onDelivered"];
  retentionPolicy?: () => Promise<{ maxAgeMs?: number; maxCount?: number; maxBytes?: number; pinningWindowMs?: number }>;
}

type DueWork =
  | { kind: "poll"; id: string; dueAt: number }
  | { kind: "push"; id: string; dueAt: number }
  | { kind: "reconcile"; id: string; dueAt: number }
  | { kind: "retention"; id: "retention"; dueAt: number };

const RETENTION_DUE_KEY = "bugler:scheduler:retention-due";
const CONTINUATION_KEY = "bugler:scheduler:continuation";
const LAST_CONTINUATION_KEY = "bugler:scheduler:last-continuation";

interface SourceSchedule {
  id: string;
  hasPoll: boolean;
  hasReconcile: boolean;
  pollNextRunAt?: string;
  reconcileNextRunAt?: string;
}

interface PushSchedule {
  id: string;
  cursor: number;
  filter: Subscription["filter"];
  transport: Subscription["transport"];
  push: Subscription["push"];
}

function dueTime(value: string | undefined, fallback: number): number {
  if (!value) return fallback;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

export class WorkersDueWorkScheduler {
  private readonly now: () => Date;
  private readonly retentionEveryMs: number;
  private readonly retentionMaxAgeMs: number;
  private readonly retentionMaxCount: number;
  private readonly retentionMaxBytes: number;
  private readonly pinningWindowMs: number;
  private readonly maxSlice: number;
  private readonly onEvent: (() => void) | undefined;
  private readonly poll: PollEngine;
  private readonly push: PushEngine;
  private readonly reconcile: GitHubReconciler;
  private readonly retentionPolicy: WorkersSchedulerOptions["retentionPolicy"];
  private sourceSchedules: Map<string, SourceSchedule> | undefined;
  private pushSchedules: Map<string, PushSchedule> | undefined;

  constructor(
    private readonly alarmStorage: AlarmStorage,
    private readonly driver: StorageDriver,
    secrets: SecretResolver,
    private readonly health: RelayHealth,
    options: WorkersSchedulerOptions = {},
  ) {
    this.now = options.now ?? (() => new Date());
    this.retentionEveryMs = options.retentionEveryMs ?? 30 * 60_000;
    this.retentionMaxAgeMs = options.retentionMaxAgeMs ?? 30 * 86_400_000;
    this.retentionMaxCount = options.retentionMaxCount ?? 100_000;
    this.retentionMaxBytes = options.retentionMaxBytes ?? 512 * 1024 * 1024;
    this.pinningWindowMs = options.pinningWindowMs ?? this.retentionMaxAgeMs;
    this.maxSlice = Math.max(1, Math.min(6, Math.trunc(options.maxSlice ?? 6)));
    this.onEvent = options.onEvent;
    this.retentionPolicy = options.retentionPolicy;
    this.poll = new PollEngine(driver, { now: this.now, maxPayloadBytes: 1_500_000 });
    this.push = new PushEngine(driver, secrets, {
      now: this.now,
      ...(options.pushFetch ? { fetch: options.pushFetch } : {}),
      ...(options.onPushDelivered ? { onDelivered: options.onPushDelivered } : {}),
    });
    this.reconcile = new GitHubReconciler(driver, secrets, fetch, this.now);
  }

  async initialize(): Promise<void> {
    if ((await this.alarmStorage.get<number>(RETENTION_DUE_KEY)) === undefined) {
      await this.alarmStorage.put(RETENTION_DUE_KEY, this.now().getTime() + this.retentionEveryMs);
    }
    await this.refreshSchedules();
    await this.armNext();
  }

  private async sourceSchedule(source: Source): Promise<SourceSchedule> {
    const state = source.kind === "poll" && source.poll ? await this.driver.getPollState(source.id) : undefined;
    return {
      id: source.id,
      hasPoll: source.kind === "poll" && source.poll !== undefined,
      hasReconcile: source.reconcile !== undefined,
      ...(state?.nextRunAt ? { pollNextRunAt: state.nextRunAt } : {}),
      ...(source.reconcile?.next_run_at ? { reconcileNextRunAt: source.reconcile.next_run_at } : {}),
    };
  }

  private pushSchedule(subscription: Subscription): PushSchedule {
    return {
      id: subscription.id,
      cursor: subscription.cursor,
      filter: structuredClone(subscription.filter),
      transport: structuredClone(subscription.transport),
      push: structuredClone(subscription.push),
    };
  }

  private async refreshSchedules(): Promise<void> {
    const sources = await this.driver.listSources();
    this.sourceSchedules = new Map(await Promise.all(sources.map(async (source) => [source.id, await this.sourceSchedule(source)] as const)));
    this.pushSchedules = new Map((await this.driver.listSubscriptions()).map((subscription) => [subscription.id, this.pushSchedule(subscription)]));
  }

  private async ensureSchedules(): Promise<void> {
    if (!this.sourceSchedules || !this.pushSchedules) await this.refreshSchedules();
  }

  async refreshAfterRequest(request: Request, responseStatus: number): Promise<void> {
    if (responseStatus < 200 || responseStatus >= 400) return;
    const method = request.method.toUpperCase();
    if (method === "GET" || method === "HEAD" || method === "OPTIONS") return;
    const segments = new URL(request.url).pathname.split("/").filter(Boolean);
    if (segments[0] === "sources") {
      if (!segments[1]) {
        this.sourceSchedules = undefined;
        return;
      }
      await this.ensureSchedules();
      const source = await this.driver.getSource(segments[1]);
      if (source) this.sourceSchedules!.set(source.id, await this.sourceSchedule(source));
      else this.sourceSchedules!.delete(segments[1]);
      return;
    }
    if (segments[0] === "subscriptions") {
      if (!segments[1]) {
        this.pushSchedules = undefined;
        return;
      }
      await this.ensureSchedules();
      const subscription = await this.driver.getSubscription(segments[1]);
      if (subscription) this.pushSchedules!.set(subscription.id, this.pushSchedule(subscription));
      else this.pushSchedules!.delete(segments[1]);
    }
  }

  private async allWork(nowMs: number): Promise<DueWork[]> {
    await this.ensureSchedules();
    const work: DueWork[] = [];
    for (const source of this.sourceSchedules!.values()) {
      if (source.hasPoll) work.push({ kind: "poll", id: source.id, dueAt: dueTime(source.pollNextRunAt, nowMs) });
      if (source.hasReconcile) work.push({ kind: "reconcile", id: source.id, dueAt: dueTime(source.reconcileNextRunAt, nowMs) });
    }
    const newestSeq = await this.driver.newestSeq();
    const oldestAvailableSeq = await this.driver.oldestAvailableSeq();
    for (const subscription of this.pushSchedules!.values()) {
      if (subscription.transport.type !== "push" || subscription.push.parked !== 0) continue;
      if (subscription.push.nextAttemptAt) {
        work.push({ kind: "push", id: subscription.id, dueAt: dueTime(subscription.push.nextAttemptAt, nowMs) });
      } else if (pushAttemptDue(subscription, newestSeq, oldestAvailableSeq, new Date(nowMs).toISOString())) {
        work.push({ kind: "push", id: subscription.id, dueAt: nowMs });
      }
    }
    const retentionDue = (await this.alarmStorage.get<number>(RETENTION_DUE_KEY)) ?? nowMs;
    work.push({ kind: "retention", id: "retention", dueAt: retentionDue });
    return work.sort((left, right) => left.dueAt - right.dueAt || left.kind.localeCompare(right.kind) || left.id.localeCompare(right.id));
  }

  async armNext(): Promise<void> {
    const nowMs = this.now().getTime();
    const work = await this.allWork(nowMs);
    const next = work[0]?.dueAt ?? nowMs + this.retentionEveryMs;
    const current = await this.alarmStorage.getAlarm();
    if (current === null || current !== next) await this.alarmStorage.setAlarm(next);
  }

  private async runWork(work: DueWork): Promise<void> {
    if (work.kind === "poll") {
      const source = await this.driver.getSource(work.id);
      if (!source) return;
      const result = await this.poll.run(source);
      if (result.event) this.onEvent?.();
      return;
    }
    if (work.kind === "push") {
      await this.push.attempt(work.id);
      return;
    }
    if (work.kind === "reconcile") {
      const source = await this.driver.getSource(work.id);
      if (source) await this.reconcile.run(source);
      return;
    }
    const override = await this.retentionPolicy?.() ?? {};
    const result = await this.driver.enforceRetention({
      maxAgeMs: override.maxAgeMs ?? this.retentionMaxAgeMs,
      maxCount: override.maxCount ?? this.retentionMaxCount,
      maxBytes: override.maxBytes ?? this.retentionMaxBytes,
      pinningWindowMs: override.pinningWindowMs ?? this.pinningWindowMs,
    }, this.now().toISOString());
    this.health.retentionPressure = result.sizeOverride;
    await this.alarmStorage.put(RETENTION_DUE_KEY, this.now().getTime() + this.retentionEveryMs);
  }

  async runSlice(): Promise<void> {
    await this.refreshSchedules();
    const nowMs = this.now().getTime();
    const due = (await this.allWork(nowMs)).filter((work) => work.dueAt <= nowMs);
    const slice = due.slice(0, this.maxSlice);
    if (due.length > slice.length) {
      await this.alarmStorage.put(CONTINUATION_KEY, nowMs);
      await this.alarmStorage.put(LAST_CONTINUATION_KEY, {
        scheduledAt: nowMs,
        sliceSize: slice.length,
        remaining: due.length - slice.length,
      });
      await this.alarmStorage.setAlarm(nowMs);
    }
    await Promise.all(slice.map((work) => this.runWork(work)));
    if (due.length <= slice.length) await this.alarmStorage.delete(CONTINUATION_KEY);
    await this.refreshSchedules();
    await this.armNext();
  }
}
