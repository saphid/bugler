import type { LocalStore } from "./store";
import type { LocalRetentionPolicy, LocalRetentionResult } from "./types";

type RetentionTimer = ReturnType<typeof setInterval> | number;

export const LOCAL_RETENTION_DEFAULTS: Required<LocalRetentionPolicy> = {
  maxAgeMs: 30 * 24 * 60 * 60_000,
  maxCount: 1_000_000,
  maxBytes: 2 * 1024 * 1024 * 1024,
  pinningWindowMs: 7 * 24 * 60 * 60_000,
  deadLetterMaxAgeMs: 30 * 24 * 60 * 60_000,
};

export function startLocalRetention(store: LocalStore, options: {
  policy?: LocalRetentionPolicy;
  intervalMs?: number;
  setInterval?: (callback: () => void, milliseconds: number) => RetentionTimer;
  clearInterval?: (timer: RetentionTimer) => void;
  onResult?: (result: LocalRetentionResult) => void;
  onError?: (error: unknown) => void;
} = {}): { stop(): void; whenIdle(): Promise<void> } {
  const schedule = options.setInterval ?? setInterval;
  const clear: (timer: RetentionTimer) => void = options.clearInterval
    ?? ((timer) => clearInterval(timer as NodeJS.Timeout));
  let tail = Promise.resolve();
  const run = () => {
    tail = tail.then(async () => {
      const result = await store.applyRetention(options.policy ?? LOCAL_RETENTION_DEFAULTS);
      options.onResult?.(result);
    }).catch((error: unknown) => options.onError?.(error));
  };
  const timer = schedule(run, options.intervalMs ?? 60_000);
  if (typeof timer === "object" && timer && "unref" in timer) timer.unref();
  return { stop: () => clear(timer), whenIdle: () => tail };
}
