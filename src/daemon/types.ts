import type { Filter, StoredEvent } from "../domain";
import type { DeliveryPayload } from "../delivery";

export interface HostedEventsFrame {
  kind: "events";
  subscriptionId: string;
  cursorEpoch: number;
  events: StoredEvent[];
}

export interface HostedGapFrame {
  kind: "gap";
  subscriptionId: string;
  cursorEpoch: number;
  gap: { from: number; to: number };
}

export type HostedFrame = HostedEventsFrame | HostedGapFrame;

export interface DurableHostedAck {
  subscriptionId: string;
  seq: number;
  epoch: number;
}

export interface LocalMirror {
  id: string;
  hostedCursor: number;
  cursorEpoch: number;
  receivedCursor: number;
  state: "caught_up" | "epoch_regression" | "corrupt" | "shared_cursor" | "local_only";
}

export interface LocalConsumer {
  id: string;
  mirrors: string[];
  filter?: Filter;
  cursor: number;
  cursorEpoch: number;
  nextLocalSeq: number;
  lastSeenAt: string;
  state: "ready" | "backfilling";
  kind?: "agent" | "action" | "management";
  mapped?: number;
  totalEstimate?: number;
}

export interface LocalConsumerCreate {
  id: string;
  mirrors: string[];
  filter?: Filter;
  start?: "latest" | "oldest";
}

export type LocalDeliveryPayload = DeliveryPayload;

export type LocalCursorResult =
  | { status: "advanced" | "unchanged"; cursor: number; epoch: number }
  | { status: "not_found" | "stale_epoch" | "beyond_newest" };

export interface LocalHistoryResult {
  events: StoredEvent[];
  oldest_available_seq: number;
  next_since_seq?: number;
}

export interface LocalTokenRecord {
  id: string;
  consumerId?: string;
  kind: "management" | "consume";
  currentHash: string;
  nextHash?: string;
  overlapUntil?: string;
  revokedAt?: string;
}

export interface LocalActionRecord {
  id: string;
  definition: unknown;
  enabled: boolean;
  digest: string;
  filePath?: string;
  loadError?: string;
  loadedAt?: string;
}

export interface LocalActionAttempt {
  actionId: string;
  localSeq: number;
  replayGeneration: number;
  attempt: number;
  state: "queued" | "running" | "succeeded" | "failed" | "dead_lettered";
  dueAt: string;
  startedAt?: string;
  finishedAt?: string;
  exitStatus?: number;
  signal?: string;
  stdoutTail?: string;
  stderrTail?: string;
}

export interface LocalDeadLetter {
  id: string;
  actionId: string;
  localSeq: number;
  envelopeSnapshot?: StoredEvent;
  summary: unknown;
  createdAt: string;
  replayGeneration?: number;
  replayedAt?: string;
}

export interface LocalRetentionPolicy {
  maxAgeMs?: number;
  maxCount?: number;
  maxBytes?: number;
  pinningWindowMs?: number;
  deadLetterMaxAgeMs?: number;
}

export interface LocalRetentionResult {
  deleted: number;
  sizeOverride: boolean;
}

export interface LocalRecoveryNotice {
  subscriptionId: string;
  recoveredAt: string;
  hostedCursor: number;
  hostedCursorEpoch: number;
  oldestAvailableSeq: number;
  acknowledgedAt?: string;
}

export type LocalCommitPhase = "after_events" | "after_mappings" | "after_commit";

export interface LocalStoreOptions {
  now?: () => Date;
  fault?: (phase: LocalCommitPhase) => void | Promise<void>;
  accountKey?: string;
}
