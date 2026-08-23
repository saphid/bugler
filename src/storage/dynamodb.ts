import {
  CreateTableCommand,
  DescribeTableCommand,
  DynamoDBClient,
  type DynamoDBClientConfig,
} from "@aws-sdk/client-dynamodb";
import {
  DeleteCommand,
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
  QueryCommand,
  TransactWriteCommand,
  UpdateCommand,
  type NativeAttributeValue,
} from "@aws-sdk/lib-dynamodb";
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
import { StorageBusyError, type StorageDriver } from "./driver";

type Item = Record<string, NativeAttributeValue>;

const EVENT_PK = "EVENT";
const META_PK = "META";
const COUNTER_SK = "COUNTER";
const SIZE_SK = "SIZE";
const RECEIVED_PK = "RECEIVED";
const DYNAMODB_ITEM_LIMIT_BYTES = 400 * 1024;
const STORAGE_MARGIN_BYTES = 16 * 1024;
const MAX_TRANSACTION_BUSY_RETRIES = 8;

function seqKey(seq: number): string {
  return String(seq).padStart(20, "0");
}

function sourceSubjectKey(source: string, subject: string | undefined): string {
  return `SS#${source}\u0000${subject ?? ""}`;
}

function sourceTypeKey(source: string, type: string): string {
  return `ST#${source}\u0000${type}`;
}

function recordItem<T>(pk: string, sk: string, value: T): Item {
  return { pk, sk, value: structuredClone(value) };
}

export type DynamoTransactionCancellation = "condition" | "busy" | undefined;

export function classifyDynamoTransactionCancellation(error: unknown): DynamoTransactionCancellation {
  if (!(error instanceof Error) || error.name !== "TransactionCanceledException") return undefined;
  const reasons = (error as Error & { CancellationReasons?: Array<{ Code?: string }> }).CancellationReasons ?? [];
  const codes = reasons.map((reason) => reason.Code).filter((code): code is string => Boolean(code && code !== "None"));
  if (codes.length > 0 && codes.every((code) => code === "ConditionalCheckFailed")) return "condition";
  return "busy";
}

function conditionalFailure(error: unknown): boolean {
  if (!(error instanceof Error)) return false;
  return error.name === "ConditionalCheckFailedException" || classifyDynamoTransactionCancellation(error) === "condition";
}

function itemValue<T>(item: Item | undefined): T | undefined {
  return item?.value === undefined ? undefined : structuredClone(item.value as T);
}

function eventValue(item: Item | undefined): StoredEvent | undefined {
  return typeof item?.valueJson === "string" ? JSON.parse(item.valueJson) as StoredEvent : undefined;
}

function eventBytes(event: StoredEvent): number {
  return new TextEncoder().encode(JSON.stringify(event)).byteLength;
}

function measuredDynamoEnvelopeBytes(): number {
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
  const item = {
    pk: EVENT_PK,
    sk: seqKey(probe.seq),
    valueJson: JSON.stringify(probe),
    seq: probe.seq,
    source: probe.source,
    type: probe.type,
    subject: probe.subject,
    receivedAt: probe.receivedat,
    gsi1pk: RECEIVED_PK,
    gsi1sk: `${probe.receivedat}#${seqKey(probe.seq)}`,
    bytes: eventBytes(probe),
  };
  return new TextEncoder().encode(JSON.stringify(item)).byteLength;
}

export const DYNAMODB_MAX_PAYLOAD_BYTES = DYNAMODB_ITEM_LIMIT_BYTES - measuredDynamoEnvelopeBytes() - STORAGE_MARGIN_BYTES;

export interface DynamoDbStorageOptions {
  tableName: string;
  client?: DynamoDBClient;
  clientConfig?: DynamoDBClientConfig;
}

export class DynamoDbStorageDriver implements StorageDriver {
  readonly capabilities = {
    name: "dynamodb",
    maxPayloadBytes: DYNAMODB_MAX_PAYLOAD_BYTES,
    queryNotes: {
      seqRange: "EVENT partition ordered by zero-padded sequence sort keys",
      sourceSubject: "materialized source-subject partition ordered by sequence",
      sourceType: "materialized source-type partition ordered by sequence",
      receivedTime: "one received-time GSI ordered by received_at and sequence",
    },
    transactionNotes: "DynamoDB condition expressions and TransactWriteItems; sequence values come from an atomic ADD counter",
    sizeMeasurementNotes: "transactionally maintained semantic event-byte counter",
  } as const;

  private readonly baseClient: DynamoDBClient;
  private readonly client: DynamoDBDocumentClient;
  private readonly ownsClient: boolean;

  constructor(private readonly options: DynamoDbStorageOptions) {
    this.ownsClient = options.client === undefined;
    this.baseClient = options.client ?? new DynamoDBClient(options.clientConfig ?? {});
    this.client = DynamoDBDocumentClient.from(this.baseClient, {
      marshallOptions: { removeUndefinedValues: true },
    });
  }

  close(): void {
    if (this.ownsClient) this.baseClient.destroy();
  }

  private get tableName(): string {
    return this.options.tableName;
  }

  private async getItem(pk: string, sk: string): Promise<Item | undefined> {
    return (await this.client.send(new GetCommand({
      TableName: this.tableName,
      Key: { pk, sk },
      ConsistentRead: true,
    }))).Item as Item | undefined;
  }

  private async queryPartition(pk: string, options: { from?: string; until?: string; limit?: number } = {}): Promise<Item[]> {
    const items: Item[] = [];
    let startKey: Record<string, NativeAttributeValue> | undefined;
    do {
      const names: Record<string, string> = { "#pk": "pk" };
      const values: Record<string, NativeAttributeValue> = { ":pk": pk };
      let keyCondition = "#pk = :pk";
      if (options.from !== undefined && options.until !== undefined) {
        names["#sk"] = "sk";
        values[":from"] = options.from;
        values[":until"] = options.until;
        keyCondition += " AND #sk BETWEEN :from AND :until";
      } else if (options.from !== undefined) {
        names["#sk"] = "sk";
        values[":from"] = options.from;
        keyCondition += " AND #sk > :from";
      } else if (options.until !== undefined) {
        names["#sk"] = "sk";
        values[":until"] = options.until;
        keyCondition += " AND #sk <= :until";
      }
      const response = await this.client.send(new QueryCommand({
        TableName: this.tableName,
        KeyConditionExpression: keyCondition,
        ExpressionAttributeNames: names,
        ExpressionAttributeValues: values,
        ExclusiveStartKey: startKey,
        ...(options.limit !== undefined ? { Limit: Math.max(1, options.limit - items.length) } : {}),
        ConsistentRead: true,
      }));
      items.push(...(response.Items as Item[] | undefined ?? []));
      startKey = response.LastEvaluatedKey;
    } while (startKey && (options.limit === undefined || items.length < options.limit));
    return items;
  }

  private async queryReceived(since: string | undefined, until: string | undefined): Promise<Item[]> {
    const items: Item[] = [];
    let startKey: Record<string, NativeAttributeValue> | undefined;
    const from = `${since ?? ""}#`;
    const through = `${until ?? "\uffff"}#\uffff`;
    do {
      const response = await this.client.send(new QueryCommand({
        TableName: this.tableName,
        IndexName: "received-index",
        KeyConditionExpression: "gsi1pk = :pk AND gsi1sk BETWEEN :from AND :through",
        ExpressionAttributeValues: { ":pk": RECEIVED_PK, ":from": from, ":through": through },
        ExclusiveStartKey: startKey,
      }));
      items.push(...(response.Items as Item[] | undefined ?? []));
      startKey = response.LastEvaluatedKey;
    } while (startKey);
    return items;
  }

  private async allocateSeq(): Promise<number> {
    const response = await this.client.send(new UpdateCommand({
      TableName: this.tableName,
      Key: { pk: META_PK, sk: COUNTER_SK },
      UpdateExpression: "ADD #value :one",
      ExpressionAttributeNames: { "#value": "value" },
      ExpressionAttributeValues: { ":one": 1 },
      ReturnValues: "UPDATED_NEW",
    }));
    return Number(response.Attributes?.value);
  }

  private eventWriteItems(event: StoredEvent, requireDedupe: boolean): Item[] {
    const key = seqKey(event.seq);
    const base: Item = {
      pk: EVENT_PK,
      sk: key,
      valueJson: JSON.stringify(event),
      seq: event.seq,
      source: event.source,
      type: event.type,
      ...(event.subject !== undefined ? { subject: event.subject } : {}),
      receivedAt: event.receivedat,
      gsi1pk: RECEIVED_PK,
      gsi1sk: `${event.receivedat}#${key}`,
      bytes: eventBytes(event),
    };
    const indexed = [
      base,
      { pk: sourceSubjectKey(event.source, event.subject), sk: key, valueJson: JSON.stringify(event) },
      { pk: sourceTypeKey(event.source, event.type), sk: key, valueJson: JSON.stringify(event) },
    ];
    if (requireDedupe && event.dedupekey !== undefined) {
      indexed.push({
        pk: `DEDUPE#${event.sourceId}`,
        sk: event.dedupekey,
        seq: event.seq,
      });
    }
    return indexed;
  }

  private async existingDedupe(sourceId: string, dedupekey: string): Promise<StoredEvent | undefined> {
    const marker = await this.getItem(`DEDUPE#${sourceId}`, dedupekey);
    return marker?.seq === undefined ? undefined : this.getEventBySeq(Number(marker.seq));
  }

  private async transactEvent(event: StoredEvent, extra: Item[] = []): Promise<void> {
    const writes = this.eventWriteItems(event, event.dedupekey !== undefined);
    await this.client.send(new TransactWriteCommand({
      TransactItems: [
        ...extra,
        ...writes.map((item) => ({
          Put: {
            TableName: this.tableName,
            Item: item,
            ...(item.pk.toString().startsWith("DEDUPE#") ? { ConditionExpression: "attribute_not_exists(pk)" } : {}),
          },
        })),
        {
          Update: {
            TableName: this.tableName,
            Key: { pk: META_PK, sk: SIZE_SK },
            UpdateExpression: "ADD #value :bytes",
            ExpressionAttributeNames: { "#value": "value" },
            ExpressionAttributeValues: { ":bytes": eventBytes(event) },
          },
        },
      ],
    }));
  }

  async appendEvent(input: AppendEventInput): Promise<{ event: StoredEvent; inserted: boolean }> {
    if (input.dedupekey !== undefined) {
      const existing = await this.existingDedupe(input.sourceId, input.dedupekey);
      if (existing) return { event: existing, inserted: false };
    }
    let busyAttempts = 0;
    for (;;) {
      const event: StoredEvent = { ...structuredClone(input), seq: await this.allocateSeq() };
      try {
        await this.transactEvent(event);
        return { event, inserted: true };
      } catch (error) {
        const cancellation = classifyDynamoTransactionCancellation(error);
        if (cancellation === "busy") {
          busyAttempts += 1;
          if (busyAttempts >= MAX_TRANSACTION_BUSY_RETRIES) throw new StorageBusyError("DynamoDB append remained transaction-busy", { cause: error });
          continue;
        }
        if (!conditionalFailure(error) || input.dedupekey === undefined) throw error;
        const existing = await this.existingDedupe(input.sourceId, input.dedupekey);
        if (existing) return { event: existing, inserted: false };
      }
    }
  }

  async appendPollTransition(input: AppendPollTransition, busyAttempts = 0): Promise<{ event?: StoredEvent; committed: boolean }> {
    const current = await this.getPollState(input.state.sourceId);
    if (!current || current.leaseToken !== input.expectedLeaseToken) return { committed: false };
    if (!input.event) {
      try {
        await this.client.send(new UpdateCommand({
          TableName: this.tableName,
          Key: { pk: "POLL", sk: input.state.sourceId },
          UpdateExpression: "SET #value = :value, generation = :generation, failureCount = :failureCount, leaseToken = :leaseToken, leaseExpiresAt = :leaseExpiresAt",
          ConditionExpression: "leaseToken = :expected",
          ExpressionAttributeNames: { "#value": "value" },
          ExpressionAttributeValues: {
            ":value": input.state,
            ":generation": input.state.generation,
            ":failureCount": input.state.failureCount,
            ":leaseToken": input.state.leaseToken ?? "",
            ":leaseExpiresAt": input.state.leaseExpiresAt ?? "",
            ":expected": input.expectedLeaseToken,
          },
        }));
        return { committed: true };
      } catch (error) {
        if (conditionalFailure(error)) return { committed: false };
        throw error;
      }
    }
    if (input.event.dedupekey !== undefined) {
      const existing = await this.existingDedupe(input.event.sourceId, input.event.dedupekey);
      if (existing) {
        const committed = await this.appendPollTransition({
          state: input.state,
          expectedLeaseToken: input.expectedLeaseToken,
        });
        return committed.committed ? { event: existing, committed: true } : { committed: false };
      }
    }
    const event: StoredEvent = { ...structuredClone(input.event), seq: await this.allocateSeq() };
    const pollUpdate: Item = {
      Update: {
        TableName: this.tableName,
        Key: { pk: "POLL", sk: input.state.sourceId },
        UpdateExpression: "SET #value = :value, generation = :generation, failureCount = :failureCount, leaseToken = :leaseToken, leaseExpiresAt = :leaseExpiresAt",
        ConditionExpression: "leaseToken = :expected",
        ExpressionAttributeNames: { "#value": "value" },
        ExpressionAttributeValues: {
          ":value": input.state,
          ":generation": input.state.generation,
          ":failureCount": input.state.failureCount,
          ":leaseToken": input.state.leaseToken ?? "",
          ":leaseExpiresAt": input.state.leaseExpiresAt ?? "",
          ":expected": input.expectedLeaseToken,
        },
      },
    };
    try {
      await this.transactEvent(event, [pollUpdate]);
      return { event, committed: true };
    } catch (error) {
      const cancellation = classifyDynamoTransactionCancellation(error);
      if (cancellation === "busy") {
        if (busyAttempts + 1 >= MAX_TRANSACTION_BUSY_RETRIES) throw new StorageBusyError("DynamoDB poll append remained transaction-busy", { cause: error });
        return this.appendPollTransition(input, busyAttempts + 1);
      }
      if (!conditionalFailure(error)) throw error;
      if (input.event.dedupekey !== undefined) {
        const existing = await this.existingDedupe(input.event.sourceId, input.event.dedupekey);
        if (existing) return this.appendPollTransition(input);
      }
      return { committed: false };
    }
  }

  async newestSeq(): Promise<number> {
    return Number((await this.getItem(META_PK, COUNTER_SK))?.value ?? 0);
  }

  async oldestAvailableSeq(): Promise<number> {
    const rows = await this.queryPartition(EVENT_PK, { limit: 1 });
    return rows[0]?.seq === undefined ? (await this.newestSeq()) + 1 : Number(rows[0].seq);
  }

  async queryEvents(query: EventQuery): Promise<StoredEvent[]> {
    let rows: Item[];
    if (query.source !== undefined && query.subject !== undefined) {
      rows = await this.queryPartition(sourceSubjectKey(query.source, query.subject), {
        ...(query.sinceSeq !== undefined ? { from: seqKey(query.sinceSeq) } : {}),
        ...(query.untilSeq !== undefined ? { until: seqKey(query.untilSeq) } : {}),
      });
    } else if (query.source !== undefined && query.type !== undefined) {
      rows = await this.queryPartition(sourceTypeKey(query.source, query.type), {
        ...(query.sinceSeq !== undefined ? { from: seqKey(query.sinceSeq) } : {}),
        ...(query.untilSeq !== undefined ? { until: seqKey(query.untilSeq) } : {}),
      });
    } else if (query.since !== undefined || query.until !== undefined) {
      rows = await this.queryReceived(query.since, query.until);
    } else {
      rows = await this.queryPartition(EVENT_PK, {
        ...(query.sinceSeq !== undefined ? { from: seqKey(query.sinceSeq) } : {}),
        ...(query.untilSeq !== undefined ? { until: seqKey(query.untilSeq) } : {}),
      });
    }
    const events = rows.map((row) => eventValue(row)).filter((event): event is StoredEvent => event !== undefined)
      .filter((event) => {
        if (query.source !== undefined && event.source !== query.source) return false;
        if (query.type !== undefined && event.type !== query.type) return false;
        if (query.subject !== undefined && event.subject !== query.subject) return false;
        if (query.sinceSeq !== undefined && event.seq <= query.sinceSeq) return false;
        if (query.untilSeq !== undefined && event.seq > query.untilSeq) return false;
        if (query.since !== undefined && event.receivedat < query.since) return false;
        if (query.until !== undefined && event.receivedat > query.until) return false;
        if (query.eventSince !== undefined && (event.time === undefined || event.time < query.eventSince)) return false;
        if (query.eventUntil !== undefined && (event.time === undefined || event.time > query.eventUntil)) return false;
        return true;
      });
    events.sort((left, right) => left.seq - right.seq);
    return events.slice(0, query.limit);
  }

  async getEventBySeq(seq: number): Promise<StoredEvent | undefined> {
    return eventValue(await this.getItem(EVENT_PK, seqKey(seq)));
  }

  private async putValue<T>(pk: string, sk: string, value: T, extra: Item = {}): Promise<void> {
    await this.client.send(new PutCommand({ TableName: this.tableName, Item: { ...recordItem(pk, sk, value), ...extra } }));
  }

  private async listValues<T>(pk: string): Promise<T[]> {
    return (await this.queryPartition(pk)).map((item) => itemValue<T>(item)).filter((value): value is T => value !== undefined);
  }

  putSource(source: Source): Promise<void> { return this.putValue("SOURCE", source.id, source); }
  async getSource(id: string): Promise<Source | undefined> { return itemValue<Source>(await this.getItem("SOURCE", id)); }
  async listSources(): Promise<Source[]> { return (await this.listValues<Source>("SOURCE")).sort((a, b) => a.id.localeCompare(b.id)); }
  async deleteSource(id: string): Promise<boolean> { return this.deleteValue("SOURCE", id); }

  putSubscription(subscription: Subscription): Promise<void> {
    return this.putValue("SUB", subscription.id, subscription, {
      cursor: subscription.cursor,
      cursorEpoch: subscription.cursorEpoch,
      ...(subscription.lastSeenAt !== undefined ? { lastSeenAt: subscription.lastSeenAt } : {}),
      ...(subscription.push.leaseToken !== undefined ? { pushLeaseToken: subscription.push.leaseToken } : {}),
      ...(subscription.push.leaseExpiresAt !== undefined ? { pushLeaseExpiresAt: subscription.push.leaseExpiresAt } : {}),
    });
  }
  updateSubscription(subscription: Subscription): Promise<void> { return this.putSubscription(subscription); }
  async getSubscription(id: string): Promise<Subscription | undefined> { return itemValue<Subscription>(await this.getItem("SUB", id)); }
  async listSubscriptions(): Promise<Subscription[]> { return (await this.listValues<Subscription>("SUB")).sort((a, b) => a.id.localeCompare(b.id)); }
  async deleteSubscription(id: string): Promise<boolean> { return this.deleteValue("SUB", id); }

  async advanceCursor(id: string, seq: number, epoch: number): Promise<CursorUpdateResult> {
    for (;;) {
      const current = await this.getSubscription(id);
      if (!current) return { status: "not_found" };
      if (current.cursorEpoch !== epoch) return { status: "stale_epoch" };
      if (seq > await this.newestSeq()) return { status: "beyond_newest" };
      if (seq <= current.cursor) return { status: "unchanged", subscription: current };
      const clearsGap = current.push.parked === 2 && current.push.gapTo !== undefined && seq >= current.push.gapTo;
      try {
        const response = await this.client.send(new UpdateCommand({
          TableName: this.tableName,
          Key: { pk: "SUB", sk: id },
          UpdateExpression: clearsGap
            ? "SET #cursor = :seq, #value.#cursor = :seq, #value.#push.parked = :zero REMOVE #value.#push.gapTo, #value.#push.nextAttemptAt"
            : "SET #cursor = :seq, #value.#cursor = :seq",
          ConditionExpression: "#cursorEpoch = :epoch AND #cursor = :current",
          ExpressionAttributeNames: {
            "#value": "value",
            "#cursor": "cursor",
            "#cursorEpoch": "cursorEpoch",
            ...(clearsGap ? { "#push": "push" } : {}),
          },
          ExpressionAttributeValues: {
            ":seq": seq,
            ":epoch": epoch,
            ":current": current.cursor,
            ...(clearsGap ? { ":zero": 0 } : {}),
          },
          ReturnValues: "ALL_NEW",
        }));
        return { status: "advanced", subscription: itemValue<Subscription>(response.Attributes as Item)! };
      } catch (error) {
        if (!conditionalFailure(error)) throw error;
      }
    }
  }

  async rewindCursor(id: string, seq: number): Promise<Subscription | undefined> {
    try {
      const response = await this.client.send(new UpdateCommand({
        TableName: this.tableName,
        Key: { pk: "SUB", sk: id },
        UpdateExpression: "SET #cursor = :cursor, #value.#cursor = :cursor, #cursorEpoch = #cursorEpoch + :one, #value.#cursorEpoch = #value.#cursorEpoch + :one REMOVE #value.#push.scannedThroughSeq, #value.#push.scannedFilter",
        ConditionExpression: "attribute_exists(pk)",
        ExpressionAttributeNames: {
          "#value": "value",
          "#push": "push",
          "#cursorEpoch": "cursorEpoch",
          "#cursor": "cursor",
        },
        ExpressionAttributeValues: { ":cursor": Math.max(0, seq), ":one": 1 },
        ReturnValues: "ALL_NEW",
      }));
      return itemValue<Subscription>(response.Attributes as Item);
    } catch (error) {
      if (conditionalFailure(error)) return undefined;
      throw error;
    }
  }

  async refreshLiveness(id: string, now: string, coalesceMs: number): Promise<void> {
    const current = await this.getSubscription(id);
    if (!current) return;
    const previous = current.lastSeenAt ? Date.parse(current.lastSeenAt) : 0;
    if (previous && Date.parse(now) - previous < coalesceMs) return;
    try {
      await this.client.send(new UpdateCommand({
        TableName: this.tableName,
        Key: { pk: "SUB", sk: id },
        UpdateExpression: "SET lastSeenAt = :now, #value.lastSeenAt = :now",
        ConditionExpression: "attribute_exists(pk)",
        ExpressionAttributeNames: { "#value": "value" },
        ExpressionAttributeValues: { ":now": now },
      }));
    } catch (error) {
      if (conditionalFailure(error)) return;
      throw error;
    }
  }

  async claimPushLease(id: string, token: string, expiresAt: string, now: string): Promise<Subscription | undefined> {
    try {
      const response = await this.client.send(new UpdateCommand({
        TableName: this.tableName,
        Key: { pk: "SUB", sk: id },
        UpdateExpression: "SET pushLeaseToken = :token, pushLeaseExpiresAt = :expires, #value.#push.leaseToken = :token, #value.#push.leaseExpiresAt = :expires",
        ConditionExpression: "attribute_exists(pk) AND (attribute_not_exists(pushLeaseExpiresAt) OR pushLeaseExpiresAt <= :now OR pushLeaseToken = :token)",
        ExpressionAttributeNames: { "#value": "value", "#push": "push" },
        ExpressionAttributeValues: { ":token": token, ":expires": expiresAt, ":now": now },
        ReturnValues: "ALL_NEW",
      }));
      return itemValue<Subscription>(response.Attributes as Item);
    } catch (error) {
      if (conditionalFailure(error)) return undefined;
      throw error;
    }
  }

  async commitPushMutation(input: PushCommitInput): Promise<PushCommitResult> {
    const set = ["#value.#push = :push"];
    const remove = ["pushLeaseToken", "pushLeaseExpiresAt"];
    const values: Record<string, NativeAttributeValue> = {
      ":push": input.push,
      ":lease": input.expectedLeaseToken,
      ":epoch": input.expectedCursorEpoch,
    };
    if (input.cursor !== undefined) {
      set.push("#cursor = :cursor", "#value.#cursor = :cursor");
      values[":cursor"] = input.cursor;
    }
    if (input.lastSeenAt !== undefined) {
      set.push("lastSeenAt = :lastSeenAt", "#value.lastSeenAt = :lastSeenAt");
      values[":lastSeenAt"] = input.lastSeenAt;
    }
    try {
      await this.client.send(new UpdateCommand({
        TableName: this.tableName,
        Key: { pk: "SUB", sk: input.subscriptionId },
        UpdateExpression: `SET ${set.join(", ")} REMOVE ${remove.join(", ")}`,
        ConditionExpression: "pushLeaseToken = :lease AND #cursorEpoch = :epoch",
        ExpressionAttributeNames: {
          "#value": "value",
          "#push": "push",
          "#cursorEpoch": "cursorEpoch",
          ...(input.cursor !== undefined ? { "#cursor": "cursor" } : {}),
        },
        ExpressionAttributeValues: values,
      }));
      return { status: "committed" };
    } catch (error) {
      if (!conditionalFailure(error)) throw error;
      const current = await this.getSubscription(input.subscriptionId);
      if (current?.cursorEpoch !== input.expectedCursorEpoch) {
        await this.client.send(new UpdateCommand({
          TableName: this.tableName,
          Key: { pk: "SUB", sk: input.subscriptionId },
          UpdateExpression: "REMOVE pushLeaseToken, pushLeaseExpiresAt, #value.#push.leaseToken, #value.#push.leaseExpiresAt",
          ConditionExpression: "pushLeaseToken = :lease",
          ExpressionAttributeNames: { "#value": "value", "#push": "push" },
          ExpressionAttributeValues: { ":lease": input.expectedLeaseToken },
        })).catch(() => undefined);
        return { status: "stale_epoch" };
      }
      return { status: "stale_lease" };
    }
  }

  putAdminKey(key: AdminKey): Promise<void> { return this.putValue("ADMIN", key.id, key); }
  async listAdminKeys(): Promise<AdminKey[]> { return (await this.listValues<AdminKey>("ADMIN")).sort((a, b) => a.createdAt.localeCompare(b.createdAt)); }
  async deleteAdminKey(id: string): Promise<boolean> { return this.deleteValue("ADMIN", id); }

  async getPollState(sourceId: string): Promise<PollState | undefined> { return itemValue<PollState>(await this.getItem("POLL", sourceId)); }
  putPollState(state: PollState): Promise<void> {
    return this.putValue("POLL", state.sourceId, state, {
      generation: state.generation,
      failureCount: state.failureCount,
      ...(state.leaseToken !== undefined ? { leaseToken: state.leaseToken } : {}),
      ...(state.leaseExpiresAt !== undefined ? { leaseExpiresAt: state.leaseExpiresAt } : {}),
    });
  }

  async claimPollLease(sourceId: string, token: string, expiresAt: string, now: string): Promise<PollState | undefined> {
    const current = await this.getPollState(sourceId) ?? { sourceId, generation: 0, failureCount: 0 };
    try {
      const response = await this.client.send(new UpdateCommand({
        TableName: this.tableName,
        Key: { pk: "POLL", sk: sourceId },
        UpdateExpression: "SET #value = :value, generation = :generation, failureCount = :failureCount, leaseToken = :token, leaseExpiresAt = :expires",
        ConditionExpression: "attribute_not_exists(leaseExpiresAt) OR leaseExpiresAt <= :now OR leaseToken = :token",
        ExpressionAttributeNames: { "#value": "value" },
        ExpressionAttributeValues: {
          ":value": { ...current, leaseToken: token, leaseExpiresAt: expiresAt },
          ":generation": current.generation,
          ":failureCount": current.failureCount,
          ":token": token,
          ":expires": expiresAt,
          ":now": now,
        },
        ReturnValues: "ALL_NEW",
      }));
      return itemValue<PollState>(response.Attributes as Item);
    } catch (error) {
      if (conditionalFailure(error)) return undefined;
      throw error;
    }
  }

  async measureSizeBytes(): Promise<number> {
    return Number((await this.getItem(META_PK, SIZE_SK))?.value ?? 0);
  }

  private async deleteEvent(event: StoredEvent): Promise<boolean> {
    const key = seqKey(event.seq);
    const deletes = [
      { pk: EVENT_PK, sk: key },
      { pk: sourceSubjectKey(event.source, event.subject), sk: key },
      { pk: sourceTypeKey(event.source, event.type), sk: key },
      ...(event.dedupekey !== undefined ? [{ pk: `DEDUPE#${event.sourceId}`, sk: event.dedupekey }] : []),
    ];
    let busyAttempts = 0;
    for (;;) {
      try {
        await this.client.send(new TransactWriteCommand({
          TransactItems: [
            ...deletes.map((keyValue, index) => ({
              Delete: {
                TableName: this.tableName,
                Key: keyValue,
                ...(index === 0 ? { ConditionExpression: "attribute_exists(pk)" } : {}),
              },
            })),
            {
              Update: {
                TableName: this.tableName,
                Key: { pk: META_PK, sk: SIZE_SK },
                UpdateExpression: "ADD #value :bytes",
                ExpressionAttributeNames: { "#value": "value" },
                ExpressionAttributeValues: { ":bytes": -eventBytes(event) },
              },
            },
          ],
        }));
        return true;
      } catch (error) {
        const cancellation = classifyDynamoTransactionCancellation(error);
        if (cancellation === "busy") {
          busyAttempts += 1;
          if (busyAttempts >= MAX_TRANSACTION_BUSY_RETRIES) throw new StorageBusyError("DynamoDB retention remained transaction-busy", { cause: error });
          continue;
        }
        if (conditionalFailure(error)) return false;
        throw error;
      }
    }
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
      const expired = await this.queryReceived(undefined, cutoff);
      for (const item of expired) {
        const event = eventValue(item);
        if (event && event.receivedat < cutoff && event.seq <= pinnedThrough) {
          if (await this.deleteEvent(event)) deleted += 1;
        }
      }
    }
    if (policy.maxCount !== undefined) {
      const remaining = (await this.queryPartition(EVENT_PK)).map((item) => eventValue(item)).filter((event): event is StoredEvent => event !== undefined);
      const excess = Math.max(0, remaining.length - policy.maxCount);
      for (const event of remaining.filter((candidate) => candidate.seq <= pinnedThrough).slice(0, excess)) {
        if (await this.deleteEvent(event)) deleted += 1;
      }
    }
    let size = await this.measureSizeBytes();
    let sizeOverride = false;
    if (policy.maxBytes !== undefined && size > policy.maxBytes) {
      sizeOverride = true;
      for (const item of await this.queryPartition(EVENT_PK)) {
        if (size <= policy.maxBytes) break;
        const event = eventValue(item);
        if (!event) continue;
        if (await this.deleteEvent(event)) deleted += 1;
        size = await this.measureSizeBytes();
      }
    }
    return { deleted, sizeOverride, oldestAvailableSeq: await this.oldestAvailableSeq(), sizeBytes: size };
  }

  private async deleteValue(pk: string, sk: string): Promise<boolean> {
    const response = await this.client.send(new DeleteCommand({
      TableName: this.tableName,
      Key: { pk, sk },
      ReturnValues: "ALL_OLD",
    }));
    return response.Attributes !== undefined;
  }
}

export async function createDynamoDbTable(client: DynamoDBClient, tableName: string): Promise<void> {
  try {
    await client.send(new DescribeTableCommand({ TableName: tableName }));
    return;
  } catch (error) {
    if (!(error instanceof Error) || error.name !== "ResourceNotFoundException") throw error;
  }
  await client.send(new CreateTableCommand({
    TableName: tableName,
    BillingMode: "PAY_PER_REQUEST",
    AttributeDefinitions: [
      { AttributeName: "pk", AttributeType: "S" },
      { AttributeName: "sk", AttributeType: "S" },
      { AttributeName: "gsi1pk", AttributeType: "S" },
      { AttributeName: "gsi1sk", AttributeType: "S" },
    ],
    KeySchema: [
      { AttributeName: "pk", KeyType: "HASH" },
      { AttributeName: "sk", KeyType: "RANGE" },
    ],
    GlobalSecondaryIndexes: [{
      IndexName: "received-index",
      KeySchema: [
        { AttributeName: "gsi1pk", KeyType: "HASH" },
        { AttributeName: "gsi1sk", KeyType: "RANGE" },
      ],
      Projection: { ProjectionType: "ALL" },
    }],
  }));
  for (let attempt = 0; attempt < 50; attempt += 1) {
    const status = (await client.send(new DescribeTableCommand({ TableName: tableName }))).Table?.TableStatus;
    if (status === "ACTIVE") return;
    await Bun.sleep(20);
  }
  throw new Error(`DynamoDB table ${tableName} did not become active`);
}

export async function requireDynamoDbTable(client: DynamoDBClient, tableName: string): Promise<void> {
  try {
    const table = (await client.send(new DescribeTableCommand({ TableName: tableName }))).Table;
    if (!table?.GlobalSecondaryIndexes?.some((index) => index.IndexName === "received-index")) {
      throw new Error(`DynamoDB table ${tableName} is missing received-index; repair it with deploy/aws/setup.sh`);
    }
  } catch (error) {
    if (error instanceof Error && error.name === "ResourceNotFoundException") {
      throw new Error(`DynamoDB table ${tableName} does not exist; create it with deploy/aws/setup.sh`, { cause: error });
    }
    throw error;
  }
}
