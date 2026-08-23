export const VERIFICATION_METHODS = [
  "hmac-sha256",
  "token",
  "basic-auth",
  "api-key",
  "poll",
  "none",
] as const;

export type VerificationMethod = (typeof VERIFICATION_METHODS)[number];

export interface EventEnvelope {
  specversion: "1.0";
  id: string;
  source: string;
  type: string;
  subject?: string;
  time?: string;
  datacontenttype?: string;
  seq: number;
  receivedat: string;
  dedupekey?: string;
  verificationmethod: VerificationMethod;
  verificationkeyid?: string;
  payloadverified: boolean;
  data?: unknown;
  data_base64?: string;
}

export interface StoredEvent extends EventEnvelope {
  sourceId: string;
  rawSize: number;
}

export type MappingValue =
  | { header: string }
  | { json_pointer: string }
  | { static: string }
  | { template: string };

export interface MappingConfig {
  preset?: "github";
  source: string;
  type?: MappingValue;
  subject?: MappingValue;
  dedupekey?: MappingValue;
  time?: MappingValue;
}

export type VerificationConfig =
  | { preset: "github" | "circleci" | "sentry"; secret_refs: string[]; failure_status?: 200 | 401 }
  | { preset: "gitlab" | "standard-webhooks"; secret_refs: string[]; tolerance_s?: number; failure_status?: 200 | 401 }
  | { preset: "stripe"; secret_refs: string[]; tolerance_s?: number; failure_status?: 200 | 401 }
  | { preset: "buildkite-signed"; secret_refs: string[]; experimental: true; failure_status?: 200 | 401 }
  | { preset: "generic-token"; secret_refs: string[]; failure_status?: 200 | 401 }
  | { primitive: "basic-auth"; username_ref: string; password_ref: string; failure_status?: 200 | 401 }
  | { primitive: "api-key"; header: string; secret_refs: string[]; failure_status?: 200 | 401 }
  | { primitive: "none" };

export interface PollConfig {
  url: string;
  interval_s: number;
  change_detect?: "etag" | "body_hash" | { json_pointer: string };
  fire_on_first?: boolean;
  deny_private_addresses?: boolean;
  token_ref?: string;
}

export interface ReconcileConfig {
  hook: string;
  credential_ref: string;
  interval_s?: number;
  watermark?: string;
  last_success_at?: string;
  failure_count?: number;
  next_run_at?: string;
}

export interface Source {
  id: string;
  kind: "http" | "poll";
  verification: VerificationConfig;
  mapping: MappingConfig;
  poll?: PollConfig;
  reconcile?: ReconcileConfig;
  createdAt: string;
}

export type FilterAttribute = "source" | "type" | "subject" | "id" | "time";
export type StringPredicate = Partial<Record<FilterAttribute, string>>;
export type Filter =
  | { exact: StringPredicate }
  | { prefix: StringPredicate }
  | { suffix: StringPredicate }
  | { all: Filter[] }
  | { any: Filter[] }
  | { not: Filter };

export type DestinationAuth =
  | { type: "none" }
  | { type: "bearer"; secret_ref: string }
  | { type: "header"; name: string; secret_ref: string };

export type Transport =
  | { type: "pull" }
  | {
      type: "push";
      url: string;
      whsec_refs: string[];
      destination_auth?: DestinationAuth;
      adapter?: "generic" | "routines" | "openclaw";
      mode?: string;
    };

export interface PushState {
  nextAttemptAt?: string;
  scannedThroughSeq?: number;
  scannedFilter?: string;
  failedSince?: string;
  failureCount: number;
  parked: 0 | 1 | 2;
  lastError?: string;
  leaseToken?: string;
  leaseExpiresAt?: string;
  gapTo?: number;
}

export interface PushCommitInput {
  subscriptionId: string;
  expectedLeaseToken: string;
  expectedCursorEpoch: number;
  push: PushState;
  cursor?: number;
  lastSeenAt?: string;
}

export type PushCommitResult = { status: "committed" | "stale_lease" | "stale_epoch" };

export interface Subscription {
  id: string;
  name?: string;
  filter: Filter;
  transport: Transport;
  action?: string;
  tokenHash: string;
  tokenPrefix: string;
  oldTokenHash?: string;
  oldTokenExpiresAt?: string;
  cursor: number;
  cursorEpoch: number;
  push: PushState;
  createdAt: string;
  lastSeenAt?: string;
  tokenRevoked?: boolean;
}

export interface AdminKey {
  id: string;
  keyHash: string;
  keyPrefix: string;
  createdAt: string;
  expiresAt?: string;
}

export interface PollState {
  sourceId: string;
  etag?: string;
  lastModified?: string;
  fingerprint?: string;
  generation: number;
  lastSuccessAt?: string;
  nextRunAt?: string;
  failureCount: number;
  leaseToken?: string;
  leaseExpiresAt?: string;
}

export interface EventQuery {
  source?: string;
  type?: string;
  subject?: string;
  sinceSeq?: number;
  untilSeq?: number;
  since?: string;
  until?: string;
  eventSince?: string;
  eventUntil?: string;
  limit: number;
}

export interface AppendEventInput extends Omit<StoredEvent, "seq"> {}

export interface AppendPollTransition {
  event?: AppendEventInput;
  state: PollState;
  expectedLeaseToken: string;
}

export type CursorUpdateResult =
  | { status: "advanced" | "unchanged"; subscription: Subscription }
  | { status: "stale_epoch" | "beyond_newest" | "not_found" };

export interface RetentionPolicy {
  maxAgeMs?: number;
  maxCount?: number;
  maxBytes?: number;
  pinningWindowMs?: number;
}

export interface RetentionResult {
  deleted: number;
  sizeOverride: boolean;
  oldestAvailableSeq: number;
  sizeBytes: number;
}
