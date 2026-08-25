import {
  BulkOperationType,
  CosmosClient,
  type Container,
  type CosmosClientOptions,
  type ItemDefinition,
  type OperationInput,
} from "@azure/cosmos";
import { createDefaultHttpClient, type HttpClient } from "@azure/core-rest-pipeline";
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
import { StorageBusyError, StoragePayloadTooLargeError, type StorageDriver } from "./driver";

const PARTITION = "relay";
const COSMOS_ITEM_LIMIT_BYTES = 2 * 1024 * 1024;
const STORAGE_MARGIN_BYTES = 32 * 1024;

interface CosmosRecord extends ItemDefinition {
  id: string;
  partitionKey: typeof PARTITION;
  kind: string;
  key?: string;
  value?: unknown;
  valueJson?: string;
  seq?: number;
  source?: string;
  type?: string;
  subject?: string;
  receivedAt?: string;
  eventTime?: string;
  bytes?: number;
  _etag?: string;
}

function documentId(kind: string, key: string): string {
  return `${kind}:${Buffer.from(key).toString("base64url")}`;
}

function valueRecord<T>(kind: string, key: string, value: T): CosmosRecord {
  return { id: documentId(kind, key), partitionKey: PARTITION, kind, key, value: structuredClone(value) };
}

function eventRecord(event: StoredEvent): CosmosRecord {
  return {
    id: documentId("event", String(event.seq)),
    partitionKey: PARTITION,
    kind: "event",
    value: structuredClone(event),
    seq: event.seq,
    source: event.source,
    type: event.type,
    ...(event.subject !== undefined ? { subject: event.subject } : {}),
    receivedAt: event.receivedat,
    ...(event.time !== undefined ? { eventTime: event.time } : {}),
    bytes: eventBytes(event),
  };
}

function eventValue(record: CosmosRecord | undefined): StoredEvent | undefined {
  if (typeof record?.valueJson === "string") return JSON.parse(record.valueJson) as StoredEvent;
  return record?.value !== undefined ? structuredClone(record.value as StoredEvent) : undefined;
}

function recordValue<T>(record: CosmosRecord | undefined): T | undefined {
  return record?.value === undefined ? undefined : structuredClone(record.value as T);
}

function eventBytes(event: StoredEvent): number {
  return new TextEncoder().encode(JSON.stringify(event)).byteLength;
}

function measuredCosmosEnvelopeBytes(): number {
  const probe: StoredEvent = {
    specversion: "1.0",
    id: "01K00000000000000000000000",
    sourceId: "source-probe",
    source: "probe/source",
    type: "probe.changed",
    subject: "probe/subject",
    receivedat: "2026-08-23T00:00:00.000Z",
    verificationmethod: "none",
    payloadverified: false,
    datacontenttype: "application/json",
    data: { payload: "" },
    rawSize: 0,
    seq: Number.MAX_SAFE_INTEGER,
  };
  return new TextEncoder().encode(JSON.stringify(eventRecord(probe))).byteLength;
}

export const COSMOS_MAX_PAYLOAD_BYTES = COSMOS_ITEM_LIMIT_BYTES - measuredCosmosEnvelopeBytes() - STORAGE_MARGIN_BYTES;

function serializedBytes(value: unknown): number {
  return new TextEncoder().encode(JSON.stringify(value)).byteLength;
}

function statusOf(error: unknown): number | undefined {
  return typeof error === "object" && error !== null && "code" in error ? Number((error as { code: unknown }).code) : undefined;
}

export type CosmosBatchClassification =
  | { kind: "operations"; codes: number[]; topLevelCode?: number }
  | { kind: "top_level_failure"; code: number }
  | { kind: "retryable" };

function recordOf(value: unknown): Record<string, unknown> | undefined {
  return typeof value === "object" && value !== null ? value as Record<string, unknown> : undefined;
}

function operationResults(value: unknown): Array<{ statusCode: number }> | undefined {
  if (!Array.isArray(value)) return undefined;
  const results = value.map((item) => recordOf(item)).filter((item): item is Record<string, unknown> => item !== undefined);
  if (results.length !== value.length || results.some((item) => !Number.isInteger(item.statusCode))) return undefined;
  return results.map((item) => ({ statusCode: Number(item.statusCode) }));
}

export function classifyCosmosBatchResponse(value: unknown, expectedOperations: number): CosmosBatchClassification {
  const record = recordOf(value);
  const nestedBody = recordOf(record?.body);
  let candidate: unknown;
  let hasOperationResults = false;
  if (record && Object.hasOwn(record, "result")) {
    candidate = record.result;
    hasOperationResults = true;
  } else if (Array.isArray(record?.body)) {
    candidate = record.body;
    hasOperationResults = true;
  } else if (nestedBody && Object.hasOwn(nestedBody, "result")) {
    candidate = nestedBody.result;
    hasOperationResults = true;
  }
  const results = operationResults(candidate);
  const topLevelCode = Number(record?.code ?? record?.statusCode ?? record?.status);
  if (hasOperationResults) {
    if (!results || results.length !== expectedOperations) return { kind: "retryable" };
    return {
      kind: "operations",
      codes: results.map((result) => result.statusCode),
      ...(Number.isInteger(topLevelCode) ? { topLevelCode } : {}),
    };
  }
  if (record?.cause !== undefined && record.cause !== value) return classifyCosmosBatchResponse(record.cause, expectedOperations);
  if (Number.isInteger(topLevelCode) && topLevelCode >= 400) return { kind: "top_level_failure", code: topLevelCode };
  return { kind: "retryable" };
}

export function cosmosBatchHttpStatus(status: number, bodyAsText: string | null | undefined): number {
  if (status < 400 || !bodyAsText) return status;
  try {
    return operationResults(JSON.parse(bodyAsText)) ? 207 : status;
  } catch {
    return status;
  }
}

function batchAwareHttpClient(delegate: HttpClient): HttpClient {
  return {
    async sendRequest(request) {
      const response = await delegate.sendRequest(request);
      if (request.headers.get("x-ms-cosmos-is-batch-request")?.toLowerCase() === "true") {
        response.status = cosmosBatchHttpStatus(response.status, response.bodyAsText);
      }
      return response;
    },
  };
}

function successfulBatch(codes: number[]): boolean {
  return codes.every((code) => code >= 200 && code < 300);
}

export interface CosmosStorageOptions {
  container: Container;
  client?: CosmosClient;
}

export class CosmosStorageDriver implements StorageDriver {
  readonly capabilities = {
    name: "cosmosdb",
    maxPayloadBytes: COSMOS_MAX_PAYLOAD_BYTES,
    queryNotes: {
      seqRange: "single-partition indexed query on kind and numeric sequence",
      sourceSubject: "single-partition indexed query on source, subject, and sequence",
      sourceType: "single-partition indexed query on source, type, and sequence",
      receivedTime: "single-partition indexed range query on receivedAt ordered by sequence",
    },
    transactionNotes: "single logical partition batches counter increment, deterministic creates, size accounting, and poll-state replacement",
    sizeMeasurementNotes: "transactionally maintained semantic event-byte counter",
  } as const;

  constructor(private readonly options: CosmosStorageOptions) {}

  close(): void {
    this.options.client?.dispose();
  }

  private get container(): Container {
    return this.options.container;
  }

  private async read(kind: string, key: string): Promise<CosmosRecord | undefined> {
    try {
      return (await this.container.item(documentId(kind, key), PARTITION).read<CosmosRecord>()).resource as CosmosRecord | undefined;
    } catch (error) {
      if (statusOf(error) === 404) return undefined;
      throw error;
    }
  }

  private async replace(record: CosmosRecord, etag: string): Promise<CosmosRecord | undefined> {
    try {
      return (await this.container.item(record.id, PARTITION).replace(record, {
        accessCondition: { type: "IfMatch", condition: etag },
      })).resource as CosmosRecord | undefined;
    } catch (error) {
      if (statusOf(error) === 404 || statusOf(error) === 412) return undefined;
      throw error;
    }
  }

  private async putValue<T>(kind: string, key: string, value: T): Promise<void> {
    await this.container.items.upsert(valueRecord(kind, key, value));
  }

  private async listValues<T>(kind: string): Promise<T[]> {
    const response = await this.container.items.query<CosmosRecord>({
      query: "SELECT * FROM c WHERE c.partitionKey = @partition AND c.kind = @kind",
      parameters: [{ name: "@partition", value: PARTITION }, { name: "@kind", value: kind }],
    }, { partitionKey: PARTITION }).fetchAll();
    return response.resources.map((record) => recordValue<T>(record)).filter((value): value is T => value !== undefined);
  }

  private async counter(): Promise<CosmosRecord> {
    const existing = await this.read("meta", "counter");
    if (existing) return existing;
    try {
      return (await this.container.items.create({
        id: documentId("meta", "counter"), partitionKey: PARTITION, kind: "meta", key: "counter", value: 0,
      })).resource! as CosmosRecord;
    } catch (error) {
      if (statusOf(error) !== 409) throw error;
      return (await this.read("meta", "counter"))!;
    }
  }

  private async ensureSize(): Promise<void> {
    try {
      await this.container.items.create({ id: documentId("meta", "size"), partitionKey: PARTITION, kind: "meta", key: "size", value: 0 });
    } catch (error) {
      if (statusOf(error) !== 409) throw error;
    }
  }

  private async existingDedupe(sourceId: string, dedupeKey: string): Promise<StoredEvent | undefined> {
    const marker = await this.read("dedupe", `${sourceId}\u0000${dedupeKey}`);
    return typeof marker?.seq === "number" ? this.getEventBySeq(marker.seq) : undefined;
  }

  private eventOperations(event: StoredEvent): OperationInput[] {
    const record = eventRecord(event);
    const recordBytes = serializedBytes(record);
    if (recordBytes > COSMOS_ITEM_LIMIT_BYTES) throw new StoragePayloadTooLargeError(recordBytes, COSMOS_ITEM_LIMIT_BYTES);
    const operations: OperationInput[] = [{ operationType: BulkOperationType.Create, resourceBody: record }];
    if (event.dedupekey !== undefined) {
      operations.push({
        operationType: BulkOperationType.Create,
        resourceBody: {
          id: documentId("dedupe", `${event.sourceId}\u0000${event.dedupekey}`),
          partitionKey: PARTITION,
          kind: "dedupe",
          seq: event.seq,
        },
      });
    }
    return operations;
  }

  private async appendBatch(input: AppendEventInput, poll?: { state: PollState; etag: string }): Promise<{ event?: StoredEvent; status: "inserted" | "conflict" }> {
    await this.ensureSize();
    for (;;) {
      const counter = await this.counter();
      const previous = Number(counter.value ?? 0);
      const event: StoredEvent = { ...structuredClone(input), seq: previous + 1 };
      const operations: OperationInput[] = [
        {
          operationType: BulkOperationType.Patch,
          id: counter.id,
          resourceBody: {
            operations: [{ op: "incr", path: "/value", value: 1 }],
            condition: `from c where c.value = ${previous}`,
          },
        },
        ...this.eventOperations(event),
        {
          operationType: BulkOperationType.Patch,
          id: documentId("meta", "size"),
          resourceBody: [{ op: "incr", path: "/value", value: eventBytes(event) }],
        },
      ];
      if (poll) {
        operations.push({
          operationType: BulkOperationType.Replace,
          id: documentId("poll", poll.state.sourceId),
          ifMatch: poll.etag,
          resourceBody: valueRecord("poll", poll.state.sourceId, poll.state),
        });
      }
      let classification: CosmosBatchClassification;
      try {
        classification = classifyCosmosBatchResponse(await this.container.items.batch(operations, PARTITION), operations.length);
      } catch (error) {
        classification = classifyCosmosBatchResponse(error, operations.length);
      }
      if (classification.kind === "retryable") throw new StorageBusyError("Cosmos transactional append returned an unclassifiable response");
      if (classification.kind === "top_level_failure") {
        if (classification.code === 409 && event.dedupekey !== undefined) return { status: "conflict" };
        if (classification.code === 412) {
          if (!poll) continue;
          const latestPoll = await this.read("poll", poll.state.sourceId);
          if (latestPoll?._etag !== poll.etag) return { status: "conflict" };
          continue;
        }
        if (classification.code === 413) throw new StoragePayloadTooLargeError(serializedBytes(eventRecord(event)), COSMOS_ITEM_LIMIT_BYTES);
        throw new StorageBusyError(`Cosmos transactional append failed with top-level status ${classification.code}`);
      }
      const { codes } = classification;
      if (successfulBatch(codes)) return { event, status: "inserted" };
      if (codes[0] === 412) continue;
      const dedupeIndex = event.dedupekey === undefined ? undefined : 2;
      if (dedupeIndex !== undefined && codes[dedupeIndex] === 409) return { status: "conflict" };
      const pollIndex = poll ? operations.length - 1 : undefined;
      if (pollIndex !== undefined && codes[pollIndex] === 412) return { status: "conflict" };
      throw new StorageBusyError(`Cosmos transactional append failed: ${codes.join(",")}`);
    }
  }

  async appendEvent(input: AppendEventInput): Promise<{ event: StoredEvent; inserted: boolean }> {
    if (input.dedupekey !== undefined) {
      const existing = await this.existingDedupe(input.sourceId, input.dedupekey);
      if (existing) return { event: existing, inserted: false };
    }
    for (;;) {
      const result = await this.appendBatch(input);
      if (result.event) return { event: result.event, inserted: true };
      if (input.dedupekey !== undefined) {
        const existing = await this.existingDedupe(input.sourceId, input.dedupekey);
        if (existing) return { event: existing, inserted: false };
      }
    }
  }

  async appendPollTransition(input: AppendPollTransition): Promise<{ event?: StoredEvent; committed: boolean }> {
    for (;;) {
      const record = await this.read("poll", input.state.sourceId);
      const current = recordValue<PollState>(record);
      if (!record?._etag || !current || current.leaseToken !== input.expectedLeaseToken) return { committed: false };
      if (!input.event) {
        const replaced = await this.replace(valueRecord("poll", input.state.sourceId, input.state), record._etag);
        if (replaced) return { committed: true };
        continue;
      }
      if (input.event.dedupekey !== undefined) {
        const existing = await this.existingDedupe(input.event.sourceId, input.event.dedupekey);
        if (existing) {
          const replaced = await this.replace(valueRecord("poll", input.state.sourceId, input.state), record._etag);
          return replaced ? { event: existing, committed: true } : { committed: false };
        }
      }
      const result = await this.appendBatch(input.event, { state: input.state, etag: record._etag });
      if (result.event) return { event: result.event, committed: true };
      if (input.event.dedupekey !== undefined) {
        const existing = await this.existingDedupe(input.event.sourceId, input.event.dedupekey);
        if (existing) return this.appendPollTransition(input);
      }
      return { committed: false };
    }
  }

  async newestSeq(): Promise<number> { return Number((await this.counter()).value ?? 0); }

  async oldestAvailableSeq(): Promise<number> {
    const response = await this.container.items.query<CosmosRecord>({
      query: "SELECT TOP 1 * FROM c WHERE c.kind = 'event' ORDER BY c.seq ASC",
    }, { partitionKey: PARTITION }).fetchAll();
    return response.resources[0]?.seq ?? (await this.newestSeq()) + 1;
  }

  async queryEvents(query: EventQuery): Promise<StoredEvent[]> {
    const clauses = ["c.kind = 'event'"];
    const parameters: Array<{ name: string; value: string | number }> = [];
    const add = (field: string, operator: string, name: string, value: string | number | undefined) => {
      if (value === undefined) return;
      clauses.push(`c.${field} ${operator} @${name}`);
      parameters.push({ name: `@${name}`, value });
    };
    add("seq", ">", "sinceSeq", query.sinceSeq);
    add("seq", "<=", "untilSeq", query.untilSeq);
    add("source", "=", "source", query.source);
    add("type", "=", "type", query.type);
    add("subject", "=", "subject", query.subject);
    add("receivedAt", ">=", "since", query.since);
    add("receivedAt", "<=", "until", query.until);
    add("eventTime", ">=", "eventSince", query.eventSince);
    add("eventTime", "<=", "eventUntil", query.eventUntil);
    const response = await this.container.items.query<CosmosRecord>({
      query: `SELECT * FROM c WHERE ${clauses.join(" AND ")} ORDER BY c.seq ASC OFFSET 0 LIMIT ${Math.max(1, query.limit)}`,
      parameters,
    }, { partitionKey: PARTITION }).fetchAll();
    return response.resources.map((record) => eventValue(record)).filter((event): event is StoredEvent => event !== undefined);
  }

  async getEventBySeq(seq: number): Promise<StoredEvent | undefined> { return eventValue(await this.read("event", String(seq))); }

  putSource(source: Source): Promise<void> { return this.putValue("source", source.id, source); }
  async getSource(id: string): Promise<Source | undefined> { return recordValue<Source>(await this.read("source", id)); }
  async listSources(): Promise<Source[]> { return (await this.listValues<Source>("source")).sort((a, b) => a.id.localeCompare(b.id)); }
  deleteSource(id: string): Promise<boolean> { return this.deleteValue("source", id); }

  putSubscription(subscription: Subscription): Promise<void> { return this.putValue("subscription", subscription.id, subscription); }
  async createSubscription(subscription: Subscription): Promise<boolean> {
    try {
      await this.container.items.create(valueRecord("subscription", subscription.id, subscription));
      return true;
    } catch (error) {
      if (statusOf(error) === 409) return false;
      throw error;
    }
  }
  updateSubscription(subscription: Subscription): Promise<void> { return this.putSubscription(subscription); }
  async getSubscription(id: string): Promise<Subscription | undefined> { return recordValue<Subscription>(await this.read("subscription", id)); }
  async listSubscriptions(): Promise<Subscription[]> { return (await this.listValues<Subscription>("subscription")).sort((a, b) => a.id.localeCompare(b.id)); }
  deleteSubscription(id: string): Promise<boolean> { return this.deleteValue("subscription", id); }

  async advanceCursor(id: string, seq: number, epoch: number): Promise<CursorUpdateResult> {
    for (;;) {
      const record = await this.read("subscription", id);
      const current = recordValue<Subscription>(record);
      if (!record?._etag || !current) return { status: "not_found" };
      if (current.cursorEpoch !== epoch) return { status: "stale_epoch" };
      if (seq > await this.newestSeq()) return { status: "beyond_newest" };
      if (seq <= current.cursor) return { status: "unchanged", subscription: current };
      current.cursor = seq;
      if (current.push.parked === 2 && current.push.gapTo !== undefined && seq >= current.push.gapTo) {
        current.push.parked = 0;
        delete current.push.gapTo;
        delete current.push.nextAttemptAt;
      }
      if (await this.replace(valueRecord("subscription", id, current), record._etag)) return { status: "advanced", subscription: current };
    }
  }

  async rewindCursor(id: string, seq: number): Promise<Subscription | undefined> {
    for (;;) {
      const record = await this.read("subscription", id);
      const current = recordValue<Subscription>(record);
      if (!record?._etag || !current) return undefined;
      current.cursor = Math.max(0, seq);
      current.cursorEpoch += 1;
      delete current.push.scannedThroughSeq;
      delete current.push.scannedFilter;
      if (await this.replace(valueRecord("subscription", id, current), record._etag)) return current;
    }
  }

  async refreshLiveness(id: string, now: string, coalesceMs: number): Promise<void> {
    for (;;) {
      const record = await this.read("subscription", id);
      const current = recordValue<Subscription>(record);
      if (!record?._etag || !current) return;
      const previous = current.lastSeenAt ? Date.parse(current.lastSeenAt) : 0;
      if (previous && Date.parse(now) - previous < coalesceMs) return;
      current.lastSeenAt = now;
      if (await this.replace(valueRecord("subscription", id, current), record._etag)) return;
    }
  }

  async claimPushLease(id: string, token: string, expiresAt: string, now: string): Promise<Subscription | undefined> {
    for (;;) {
      const record = await this.read("subscription", id);
      const current = recordValue<Subscription>(record);
      if (!record?._etag || !current) return undefined;
      if (current.push.leaseExpiresAt && current.push.leaseExpiresAt > now && current.push.leaseToken !== token) return undefined;
      current.push.leaseToken = token;
      current.push.leaseExpiresAt = expiresAt;
      if (await this.replace(valueRecord("subscription", id, current), record._etag)) return current;
    }
  }

  async commitPushMutation(input: PushCommitInput): Promise<PushCommitResult> {
    for (;;) {
      const record = await this.read("subscription", input.subscriptionId);
      const current = recordValue<Subscription>(record);
      if (!record?._etag || !current || current.push.leaseToken !== input.expectedLeaseToken) return { status: "stale_lease" };
      if (current.cursorEpoch !== input.expectedCursorEpoch) {
        delete current.push.leaseToken;
        delete current.push.leaseExpiresAt;
        if (await this.replace(valueRecord("subscription", current.id, current), record._etag)) return { status: "stale_epoch" };
        continue;
      }
      current.push = structuredClone(input.push);
      if (input.cursor !== undefined) current.cursor = input.cursor;
      if (input.lastSeenAt !== undefined) current.lastSeenAt = input.lastSeenAt;
      if (await this.replace(valueRecord("subscription", current.id, current), record._etag)) return { status: "committed" };
    }
  }

  putAdminKey(key: AdminKey): Promise<void> { return this.putValue("admin", key.id, key); }
  async listAdminKeys(): Promise<AdminKey[]> { return (await this.listValues<AdminKey>("admin")).sort((a, b) => a.createdAt.localeCompare(b.createdAt)); }
  deleteAdminKey(id: string): Promise<boolean> { return this.deleteValue("admin", id); }

  async getPollState(sourceId: string): Promise<PollState | undefined> { return recordValue<PollState>(await this.read("poll", sourceId)); }
  putPollState(state: PollState): Promise<void> { return this.putValue("poll", state.sourceId, state); }

  async claimPollLease(sourceId: string, token: string, expiresAt: string, now: string): Promise<PollState | undefined> {
    for (;;) {
      const record = await this.read("poll", sourceId);
      const current = recordValue<PollState>(record) ?? { sourceId, generation: 0, failureCount: 0 };
      if (current.leaseExpiresAt && current.leaseExpiresAt > now && current.leaseToken !== token) return undefined;
      current.leaseToken = token;
      current.leaseExpiresAt = expiresAt;
      if (!record?._etag) {
        try {
          return recordValue<PollState>((await this.container.items.create(valueRecord("poll", sourceId, current))).resource)!;
        } catch (error) {
          if (statusOf(error) === 409) continue;
          throw error;
        }
      }
      if (await this.replace(valueRecord("poll", sourceId, current), record._etag)) return current;
    }
  }

  async measureSizeBytes(): Promise<number> {
    await this.ensureSize();
    return Number((await this.read("meta", "size"))?.value ?? 0);
  }

  private async deleteEvent(event: StoredEvent): Promise<boolean> {
    const operations: OperationInput[] = [
      { operationType: BulkOperationType.Delete, id: documentId("event", String(event.seq)) },
      ...(event.dedupekey === undefined ? [] : [{ operationType: BulkOperationType.Delete as "Delete", id: documentId("dedupe", `${event.sourceId}\u0000${event.dedupekey}`) }]),
      {
        operationType: BulkOperationType.Patch,
        id: documentId("meta", "size"),
        resourceBody: [{ op: "incr", path: "/value", value: -eventBytes(event) }],
      },
    ];
    let classification: CosmosBatchClassification;
    try {
      classification = classifyCosmosBatchResponse(await this.container.items.batch(operations, PARTITION), operations.length);
    } catch (error) {
      classification = classifyCosmosBatchResponse(error, operations.length);
    }
    if (classification.kind === "retryable") throw new StorageBusyError("Cosmos retention delete returned an unclassifiable response");
    if (classification.kind === "top_level_failure") {
      if (classification.code === 404 && !await this.getEventBySeq(event.seq)) return false;
      throw new StorageBusyError(`Cosmos retention delete failed with top-level status ${classification.code}`);
    }
    const { codes } = classification;
    if (successfulBatch(codes)) return true;
    if (codes[0] === 404) return false;
    throw new StorageBusyError(`Cosmos retention delete failed: ${codes.join(",")}`);
  }

  async enforceRetention(policy: RetentionPolicy, now: string): Promise<RetentionResult> {
    const nowMs = Date.parse(now);
    const pinWindow = policy.pinningWindowMs ?? policy.maxAgeMs ?? 30 * 86_400_000;
    const live = (await this.listSubscriptions()).filter((subscription) =>
      subscription.lastSeenAt !== undefined && nowMs - Date.parse(subscription.lastSeenAt) <= pinWindow);
    const pinnedThrough = live.length ? Math.min(...live.map((subscription) => subscription.cursor)) : Number.MAX_SAFE_INTEGER;
    let deleted = 0;
    if (policy.maxAgeMs !== undefined) {
      const cutoff = new Date(nowMs - policy.maxAgeMs).toISOString();
      const response = await this.container.items.query<CosmosRecord>({
        query: "SELECT * FROM c WHERE c.kind = 'event' AND c.receivedAt < @cutoff ORDER BY c.receivedAt ASC",
        parameters: [{ name: "@cutoff", value: cutoff }],
      }, { partitionKey: PARTITION }).fetchAll();
      for (const record of response.resources) {
        const event = eventValue(record);
        if (event && event.seq <= pinnedThrough) {
          if (await this.deleteEvent(event)) deleted += 1;
        }
      }
    }
    const allEvents = async () => (await this.container.items.query<CosmosRecord>({
      query: "SELECT * FROM c WHERE c.kind = 'event' ORDER BY c.seq ASC",
    }, { partitionKey: PARTITION }).fetchAll()).resources.map(eventValue).filter((event): event is StoredEvent => event !== undefined);
    if (policy.maxCount !== undefined) {
      const events = await allEvents();
      const excess = Math.max(0, events.length - policy.maxCount);
      for (const event of events.filter((candidate) => candidate.seq <= pinnedThrough).slice(0, excess)) {
        if (await this.deleteEvent(event)) deleted += 1;
      }
    }
    let size = await this.measureSizeBytes();
    let sizeOverride = false;
    if (policy.maxBytes !== undefined && size > policy.maxBytes) {
      sizeOverride = true;
      for (const event of await allEvents()) {
        if (size <= policy.maxBytes) break;
        if (await this.deleteEvent(event)) deleted += 1;
        size = await this.measureSizeBytes();
      }
    }
    return { deleted, sizeOverride, oldestAvailableSeq: await this.oldestAvailableSeq(), sizeBytes: size };
  }

  private async deleteValue(kind: string, key: string): Promise<boolean> {
    try {
      await this.container.item(documentId(kind, key), PARTITION).delete();
      return true;
    } catch (error) {
      if (statusOf(error) === 404) return false;
      throw error;
    }
  }
}

export async function createCosmosContainer(options: {
  clientOptions: CosmosClientOptions;
  databaseId: string;
  containerId: string;
}): Promise<{ client: CosmosClient; container: Container }> {
  const client = new CosmosClient({
    ...options.clientOptions,
    httpClient: batchAwareHttpClient(options.clientOptions.httpClient ?? createDefaultHttpClient()),
  });
  const { database } = await client.databases.createIfNotExists({ id: options.databaseId });
  const { container } = await database.containers.createIfNotExists({
    id: options.containerId,
    partitionKey: { paths: ["/partitionKey"] },
  });
  return { client, container };
}
