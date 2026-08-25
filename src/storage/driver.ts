import type {
  AdminKey,
  AppendEventInput,
  AppendPollTransition,
  CursorUpdateResult,
  EventQuery,
  PollState,
  PushCommitInput,
  PushCommitResult,
  RetentionPolicy,
  RetentionResult,
  Source,
  StoredEvent,
  Subscription,
} from "../domain";

export interface StorageCapabilities {
  name: string;
  maxPayloadBytes?: number;
  queryNotes: Record<"seqRange" | "sourceSubject" | "sourceType" | "receivedTime", string>;
  transactionNotes: string;
  sizeMeasurementNotes: string;
}

export class StorageBusyError extends Error {
  readonly retryable = true;

  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "StorageBusyError";
  }
}

export class StoragePayloadTooLargeError extends Error {
  constructor(readonly actualBytes: number, readonly limitBytes: number) {
    super(`serialized storage item is ${actualBytes} bytes; limit is ${limitBytes} bytes`);
    this.name = "StoragePayloadTooLargeError";
  }
}

export interface StorageDriver {
  readonly capabilities: StorageCapabilities;
  close(): void | Promise<void>;

  appendEvent(input: AppendEventInput): Promise<{ event: StoredEvent; inserted: boolean }>;
  appendPollTransition(input: AppendPollTransition): Promise<{ event?: StoredEvent; committed: boolean }>;
  newestSeq(): Promise<number>;
  oldestAvailableSeq(): Promise<number>;
  queryEvents(query: EventQuery): Promise<StoredEvent[]>;
  getEventBySeq(seq: number): Promise<StoredEvent | undefined>;

  putSource(source: Source): Promise<void>;
  getSource(id: string): Promise<Source | undefined>;
  listSources(): Promise<Source[]>;
  deleteSource(id: string): Promise<boolean>;

  putSubscription(subscription: Subscription): Promise<void>;
  /** Atomically inserts a subscription only when its id is absent. Existing rows are never mutated. */
  createSubscription(subscription: Subscription): Promise<boolean>;
  getSubscription(id: string): Promise<Subscription | undefined>;
  listSubscriptions(): Promise<Subscription[]>;
  deleteSubscription(id: string): Promise<boolean>;
  updateSubscription(subscription: Subscription): Promise<void>;
  /**
   * Cursor rejection precedence is normative across every driver:
   * not_found, then stale_epoch, then beyond_newest, then unchanged or advanced.
   */
  advanceCursor(id: string, seq: number, epoch: number): Promise<CursorUpdateResult>;
  rewindCursor(id: string, seq: number): Promise<Subscription | undefined>;
  refreshLiveness(id: string, now: string, coalesceMs: number): Promise<void>;
  claimPushLease(id: string, token: string, expiresAt: string, now: string): Promise<Subscription | undefined>;
  /** Applies only push-owned fields under lease and cursor-epoch fencing. */
  commitPushMutation(input: PushCommitInput): Promise<PushCommitResult>;

  putAdminKey(key: AdminKey): Promise<void>;
  listAdminKeys(): Promise<AdminKey[]>;
  deleteAdminKey(id: string): Promise<boolean>;

  getPollState(sourceId: string): Promise<PollState | undefined>;
  putPollState(state: PollState): Promise<void>;
  claimPollLease(sourceId: string, token: string, expiresAt: string, now: string): Promise<PollState | undefined>;

  measureSizeBytes(): Promise<number>;
  /** Persistent drivers must enforce the age cap through their received-time access path, without a full event-table scan. */
  enforceRetention(policy: RetentionPolicy, now: string): Promise<RetentionResult>;
}

export function cloneValue<T>(value: T): T {
  return structuredClone(value);
}
