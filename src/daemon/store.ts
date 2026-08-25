import type { Filter, StoredEvent } from "../domain";
import type {
  DurableHostedAck,
  HostedFrame,
  LocalActionRecord,
  LocalConsumer,
  LocalConsumerCreate,
  LocalCursorResult,
  LocalDeadLetter,
  LocalDeliveryPayload,
  LocalHistoryResult,
  LocalMirror,
  LocalRetentionPolicy,
  LocalRetentionResult,
} from "./types";

export class LocalStoreError extends Error {
  constructor(readonly code: "not_found" | "already_exists" | "stale_epoch" | "beyond_newest" | "epoch_regression" | "sequence_gap" | "corrupt" | "backfilling") {
    super(code);
    this.name = "LocalStoreError";
  }
}

export interface LocalStore {
  close(): void | Promise<void>;
  createMirror(id: string, cursor?: number, epoch?: number): Promise<LocalMirror>;
  listMirrors(): Promise<LocalMirror[]>;
  createConsumer(input: LocalConsumerCreate): Promise<{ consumer: LocalConsumer; token: string }>;
  advanceBackfills(limit?: number): Promise<number>;
  getConsumer(id: string): Promise<LocalConsumer | undefined>;
  listConsumers(): Promise<LocalConsumer[]>;
  commitHostedFrame(frame: HostedFrame): Promise<DurableHostedAck>;
  readConsumer(id: string, limit?: number): Promise<LocalDeliveryPayload>;
  acknowledgeConsumer(id: string, seq: number, epoch: number): Promise<LocalCursorResult>;
  rewindConsumer(id: string, seq: number): Promise<{ cursor: number; epoch: number } | undefined>;
  history(id: string, query?: { source?: string; type?: string; subject?: string; sinceSeq?: number; limit?: number }): Promise<LocalHistoryResult>;
  authenticateManagement(token: string): Promise<boolean>;
  authenticateConsumer(id: string, token: string): Promise<boolean>;
  mintManagementToken(): Promise<string>;
  rotateConsumerToken(id: string, overlapSeconds: number): Promise<string | undefined>;
  revokeConsumerToken(id: string): Promise<boolean>;
  applyRetention(policy: LocalRetentionPolicy): Promise<LocalRetentionResult>;
  putAction(action: LocalActionRecord): Promise<void>;
  getAction(id: string): Promise<LocalActionRecord | undefined>;
  listActions(): Promise<LocalActionRecord[]>;
  deleteAction(id: string): Promise<boolean>;
  putActionAttempt(attempt: import("./types").LocalActionAttempt): Promise<void>;
  listActionAttempts(actionId: string, localSeq: number): Promise<import("./types").LocalActionAttempt[]>;
  listRecentActionAttempts(actionId: string, since: string): Promise<import("./types").LocalActionAttempt[]>;
  putDeadLetter(deadLetter: LocalDeadLetter): Promise<void>;
  getDeadLetter(id: string): Promise<LocalDeadLetter | undefined>;
  listDeadLetters(): Promise<LocalDeadLetter[]>;
  markDeadLetterReplayed(id: string, at: string): Promise<boolean>;
  putRecoveryNotice(notice: import("./types").LocalRecoveryNotice): Promise<void>;
  listRecoveryNotices(): Promise<import("./types").LocalRecoveryNotice[]>;
  setConsumerFilter(id: string, filter: Filter | undefined): Promise<void>;
  debugEventCount(): Promise<number>;
  debugMappingCount(consumerId: string): Promise<number>;
  debugEvent(localConsumerId: string, localSeq: number): Promise<StoredEvent | undefined>;
}
