import { createHash } from "node:crypto";
import {
  Firestore,
  type DocumentData,
  type DocumentReference,
  type DocumentSnapshot,
  type Query,
  type QueryDocumentSnapshot,
  type Transaction,
} from "@google-cloud/firestore";
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

const FIRESTORE_DOCUMENT_LIMIT_BYTES = 1024 * 1024;
const FIRESTORE_FIELD_LIMIT_BYTES = 1024 * 1024 - 89;
const STORAGE_MARGIN_BYTES = 32 * 1024;
const DOCUMENT_NAME_RESERVE_BYTES = 512;
const TRANSACTION_ATTEMPTS = 20;
const RETENTION_BATCH_SIZE = 8;

interface EventDocument extends DocumentData {
  valueJson: string;
  seq: number;
  source: string;
  type: string;
  subject?: string;
  receivedAt: string;
  eventTime?: string;
  bytes: number;
}

interface ValueDocument extends DocumentData {
  valueJson: string;
}

function utf8Bytes(value: string): number {
  return new TextEncoder().encode(value).byteLength;
}

function eventBytes(event: StoredEvent): number {
  return utf8Bytes(JSON.stringify(event));
}

function firestoreValueApproxBytes(value: unknown): number {
  if (value === null) return 1;
  if (typeof value === "boolean") return 1;
  if (typeof value === "number") return 8;
  if (typeof value === "string") return utf8Bytes(value) + 1;
  if (Array.isArray(value)) return value.reduce((total, item) => total + firestoreValueApproxBytes(item), 0);
  if (typeof value === "object") {
    return Object.entries(value as Record<string, unknown>)
      .reduce((total, [name, item]) => total + utf8Bytes(name) + 1 + firestoreValueApproxBytes(item), 32);
  }
  return 0;
}

function eventDocument(event: StoredEvent): EventDocument {
  return {
    valueJson: JSON.stringify(event),
    seq: event.seq,
    source: event.source,
    type: event.type,
    ...(event.subject !== undefined ? { subject: event.subject } : {}),
    receivedAt: event.receivedat,
    ...(event.time !== undefined ? { eventTime: event.time } : {}),
    bytes: eventBytes(event),
  };
}

function eventValue(snapshot: DocumentSnapshot | QueryDocumentSnapshot): StoredEvent | undefined {
  const valueJson = snapshot.data()?.valueJson;
  return typeof valueJson === "string" ? JSON.parse(valueJson) as StoredEvent : undefined;
}

function valueDocument<T>(value: T): ValueDocument {
  return { valueJson: JSON.stringify(value) };
}

function documentValue<T>(snapshot: DocumentSnapshot | QueryDocumentSnapshot): T | undefined {
  const valueJson = snapshot.data()?.valueJson;
  return typeof valueJson === "string" ? JSON.parse(valueJson) as T : undefined;
}

function measuredFirestoreEnvelopeBytes(): number {
  const probeData = { payload: "" };
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
    data: probeData,
    rawSize: 0,
    seq: Number.MAX_SAFE_INTEGER,
  };
  return DOCUMENT_NAME_RESERVE_BYTES + firestoreValueApproxBytes(eventDocument(probe)) - utf8Bytes(JSON.stringify(probeData));
}

export const FIRESTORE_MAX_PAYLOAD_BYTES = FIRESTORE_DOCUMENT_LIMIT_BYTES - measuredFirestoreEnvelopeBytes() - STORAGE_MARGIN_BYTES;

function documentKey(value: string): string {
  return createHash("sha256").update(value).digest("base64url");
}

function seqKey(seq: number): string {
  return String(seq).padStart(20, "0");
}

function firestoreCode(error: unknown): number | string | undefined {
  if (typeof error !== "object" || error === null || !("code" in error)) return undefined;
  return (error as { code?: number | string }).code;
}

function transactionFailure(error: unknown, operation: string): never {
  const code = firestoreCode(error);
  if (code === 4 || code === 8 || code === 10 || code === 13 || code === 14 || code === "ABORTED" || code === "DEADLINE_EXCEEDED" || code === "RESOURCE_EXHAUSTED" || code === "INTERNAL" || code === "UNAVAILABLE") {
    throw new StorageBusyError(`Firestore ${operation} remained transaction-busy`, { cause: error });
  }
  throw error;
}

export interface FirestoreStorageOptions {
  firestore?: Firestore;
  projectId?: string;
  databaseId?: string;
  namespace?: string;
}

export class FirestoreStorageDriver implements StorageDriver {
  readonly capabilities = {
    name: "firestore",
    maxPayloadBytes: FIRESTORE_MAX_PAYLOAD_BYTES,
    queryNotes: {
      seqRange: "events collection ordered by numeric sequence; queries outside the four indexed shapes (sequence range, source plus subject, source plus type, received time) fall back to reading the whole events collection and filtering in memory, costing document reads proportional to collection size against Firestore's free tier of 50,000 document reads per day",
      sourceSubject: "composite source, subject, and sequence index",
      sourceType: "composite source, type, and sequence index",
      receivedTime: "receivedAt range index, paged in bounded retention batches",
    },
    transactionNotes: "Firestore transactions fence the sequence counter, deterministic dedupe marker, event append, size counter, and optional poll state",
    sizeMeasurementNotes: "transactionally maintained UTF-8 JSON event-byte approximation; Firestore document sizing reserves field, document-name, and 32 KiB safety overhead",
  } as const;

  private readonly firestore: Firestore;
  private readonly ownsFirestore: boolean;
  private readonly root: DocumentReference;

  constructor(options: FirestoreStorageOptions = {}) {
    this.ownsFirestore = options.firestore === undefined;
    this.firestore = options.firestore ?? new Firestore({
      ...(options.projectId !== undefined ? { projectId: options.projectId } : {}),
      ...(options.databaseId !== undefined ? { databaseId: options.databaseId } : {}),
    });
    this.root = this.firestore.collection("buglerNamespaces").doc(documentKey(options.namespace ?? "bugler"));
  }

  async close(): Promise<void> {
    if (this.ownsFirestore) await this.firestore.terminate();
  }

  private collection(name: string) {
    return this.root.collection(name);
  }

  private eventRef(seq: number): DocumentReference<EventDocument> {
    return this.collection("events").doc(seqKey(seq)) as DocumentReference<EventDocument>;
  }

  private valueRef(kind: string, key: string): DocumentReference<ValueDocument> {
    return this.collection(kind).doc(documentKey(key)) as DocumentReference<ValueDocument>;
  }

  private dedupeRef(sourceId: string, dedupeKey: string): DocumentReference {
    return this.collection("dedupe").doc(documentKey(`${sourceId}\u0000${dedupeKey}`));
  }

  private counterRef(): DocumentReference {
    return this.collection("meta").doc("counter");
  }

  private sizeRef(): DocumentReference {
    return this.collection("meta").doc("size");
  }

  private assertEventFits(event: StoredEvent): EventDocument {
    const document = eventDocument(event);
    const actualBytes = DOCUMENT_NAME_RESERVE_BYTES + firestoreValueApproxBytes(document);
    if (actualBytes > FIRESTORE_DOCUMENT_LIMIT_BYTES || utf8Bytes(document.valueJson) > FIRESTORE_FIELD_LIMIT_BYTES) {
      throw new StoragePayloadTooLargeError(actualBytes, FIRESTORE_DOCUMENT_LIMIT_BYTES);
    }
    return document;
  }

  private async runTransaction<T>(operation: string, callback: (transaction: Transaction) => Promise<T>): Promise<T> {
    try {
      return await this.firestore.runTransaction(callback, { maxAttempts: TRANSACTION_ATTEMPTS });
    } catch (error) {
      return transactionFailure(error, operation);
    }
  }

  private async appendInTransaction(transaction: Transaction, input: AppendEventInput): Promise<{ event: StoredEvent; inserted: boolean }> {
    const dedupe = input.dedupekey === undefined ? undefined : this.dedupeRef(input.sourceId, input.dedupekey);
    const dedupeSnapshot = dedupe ? await transaction.get(dedupe) : undefined;
    if (dedupeSnapshot?.exists) {
      const existingSeq = Number(dedupeSnapshot.data()?.seq);
      const existingSnapshot = await transaction.get(this.eventRef(existingSeq));
      const existing = eventValue(existingSnapshot);
      if (existing) return { event: existing, inserted: false };
    }

    const [counter, size] = await transaction.getAll(this.counterRef(), this.sizeRef());
    const seq = Number(counter!.data()?.value ?? 0) + 1;
    const event: StoredEvent = { ...structuredClone(input), seq };
    const document = this.assertEventFits(event);
    transaction.set(this.counterRef(), { value: seq });
    transaction.set(this.sizeRef(), { value: Number(size!.data()?.value ?? 0) + document.bytes });
    transaction.create(this.eventRef(seq), document);
    if (dedupe) transaction.set(dedupe, { seq });
    return { event, inserted: true };
  }

  appendEvent(input: AppendEventInput): Promise<{ event: StoredEvent; inserted: boolean }> {
    return this.runTransaction("append", (transaction) => this.appendInTransaction(transaction, input));
  }

  appendPollTransition(input: AppendPollTransition): Promise<{ event?: StoredEvent; committed: boolean }> {
    return this.runTransaction("poll append", async (transaction) => {
      const pollRef = this.valueRef("pollStates", input.state.sourceId);
      const current = documentValue<PollState>(await transaction.get(pollRef));
      if (!current || current.leaseToken !== input.expectedLeaseToken) return { committed: false };
      const appended = input.event ? await this.appendInTransaction(transaction, input.event) : undefined;
      transaction.set(pollRef, valueDocument(input.state));
      return appended ? { event: appended.event, committed: true } : { committed: true };
    });
  }

  async newestSeq(): Promise<number> {
    return Number((await this.counterRef().get()).data()?.value ?? 0);
  }

  async oldestAvailableSeq(): Promise<number> {
    const snapshot = await this.collection("events").orderBy("seq", "asc").limit(1).get();
    return snapshot.empty ? (await this.newestSeq()) + 1 : Number(snapshot.docs[0]!.data().seq);
  }

  async queryEvents(query: EventQuery): Promise<StoredEvent[]> {
    let candidate: Query = this.collection("events");
    let canLimit = true;
    if (query.source !== undefined && query.subject !== undefined) {
      candidate = candidate.where("source", "==", query.source).where("subject", "==", query.subject).orderBy("seq", "asc");
      canLimit = query.type === undefined && query.since === undefined && query.until === undefined && query.eventSince === undefined && query.eventUntil === undefined;
    } else if (query.source !== undefined && query.type !== undefined) {
      candidate = candidate.where("source", "==", query.source).where("type", "==", query.type).orderBy("seq", "asc");
      canLimit = query.subject === undefined && query.since === undefined && query.until === undefined && query.eventSince === undefined && query.eventUntil === undefined;
    } else if (query.since !== undefined || query.until !== undefined) {
      if (query.since !== undefined) candidate = candidate.where("receivedAt", ">=", query.since);
      if (query.until !== undefined) candidate = candidate.where("receivedAt", "<=", query.until);
      candidate = candidate.orderBy("receivedAt", "asc");
      canLimit = false;
    } else {
      candidate = candidate.orderBy("seq", "asc");
      canLimit = query.source === undefined && query.type === undefined && query.subject === undefined && query.eventSince === undefined && query.eventUntil === undefined;
    }
    if (query.sinceSeq !== undefined && !((query.since !== undefined || query.until !== undefined))) candidate = candidate.where("seq", ">", query.sinceSeq);
    if (query.untilSeq !== undefined && !((query.since !== undefined || query.until !== undefined))) candidate = candidate.where("seq", "<=", query.untilSeq);
    if (canLimit) candidate = candidate.limit(Math.max(1, query.limit));

    const events = (await candidate.get()).docs.map(eventValue).filter((event): event is StoredEvent => event !== undefined)
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
    return eventValue(await this.eventRef(seq).get());
  }

  private async putValue<T>(kind: string, key: string, value: T): Promise<void> {
    await this.valueRef(kind, key).set(valueDocument(value));
  }

  private async getValue<T>(kind: string, key: string): Promise<T | undefined> {
    return documentValue<T>(await this.valueRef(kind, key).get());
  }

  private async listValues<T>(kind: string): Promise<T[]> {
    return (await this.collection(kind).get()).docs.map(documentValue<T>).filter((value): value is T => value !== undefined);
  }

  private deleteValue(kind: string, key: string): Promise<boolean> {
    return this.runTransaction("delete", async (transaction) => {
      const reference = this.valueRef(kind, key);
      const snapshot = await transaction.get(reference);
      if (!snapshot.exists) return false;
      transaction.delete(reference);
      return true;
    });
  }

  putSource(source: Source): Promise<void> { return this.putValue("sources", source.id, source); }
  getSource(id: string): Promise<Source | undefined> { return this.getValue("sources", id); }
  async listSources(): Promise<Source[]> { return (await this.listValues<Source>("sources")).sort((left, right) => left.id.localeCompare(right.id)); }
  deleteSource(id: string): Promise<boolean> { return this.deleteValue("sources", id); }

  putSubscription(subscription: Subscription): Promise<void> { return this.putValue("subscriptions", subscription.id, subscription); }
  getSubscription(id: string): Promise<Subscription | undefined> { return this.getValue("subscriptions", id); }
  async listSubscriptions(): Promise<Subscription[]> { return (await this.listValues<Subscription>("subscriptions")).sort((left, right) => left.id.localeCompare(right.id)); }
  deleteSubscription(id: string): Promise<boolean> { return this.deleteValue("subscriptions", id); }
  updateSubscription(subscription: Subscription): Promise<void> { return this.putSubscription(subscription); }

  advanceCursor(id: string, seq: number, epoch: number): Promise<CursorUpdateResult> {
    return this.runTransaction("cursor advance", async (transaction) => {
      const subscriptionRef = this.valueRef("subscriptions", id);
      const [subscriptionSnapshot, counterSnapshot] = await transaction.getAll(subscriptionRef, this.counterRef());
      const subscription = documentValue<Subscription>(subscriptionSnapshot!);
      if (!subscription) return { status: "not_found" };
      if (subscription.cursorEpoch !== epoch) return { status: "stale_epoch" };
      if (seq > Number(counterSnapshot!.data()?.value ?? 0)) return { status: "beyond_newest" };
      if (seq <= subscription.cursor) return { status: "unchanged", subscription };
      subscription.cursor = seq;
      if (subscription.push.parked === 2 && subscription.push.gapTo !== undefined && seq >= subscription.push.gapTo) {
        subscription.push.parked = 0;
        delete subscription.push.gapTo;
        delete subscription.push.nextAttemptAt;
      }
      transaction.set(subscriptionRef, valueDocument(subscription));
      return { status: "advanced", subscription };
    });
  }

  rewindCursor(id: string, seq: number): Promise<Subscription | undefined> {
    return this.runTransaction("cursor rewind", async (transaction) => {
      const reference = this.valueRef("subscriptions", id);
      const subscription = documentValue<Subscription>(await transaction.get(reference));
      if (!subscription) return undefined;
      subscription.cursor = Math.max(0, seq);
      subscription.cursorEpoch += 1;
      delete subscription.push.scannedThroughSeq;
      delete subscription.push.scannedFilter;
      transaction.set(reference, valueDocument(subscription));
      return subscription;
    });
  }

  refreshLiveness(id: string, now: string, coalesceMs: number): Promise<void> {
    return this.runTransaction("liveness refresh", async (transaction) => {
      const reference = this.valueRef("subscriptions", id);
      const subscription = documentValue<Subscription>(await transaction.get(reference));
      if (!subscription) return;
      const previous = subscription.lastSeenAt ? Date.parse(subscription.lastSeenAt) : 0;
      if (previous && Date.parse(now) - previous < coalesceMs) return;
      subscription.lastSeenAt = now;
      transaction.set(reference, valueDocument(subscription));
    });
  }

  claimPushLease(id: string, token: string, expiresAt: string, now: string): Promise<Subscription | undefined> {
    return this.runTransaction("push lease claim", async (transaction) => {
      const reference = this.valueRef("subscriptions", id);
      const subscription = documentValue<Subscription>(await transaction.get(reference));
      if (!subscription) return undefined;
      if (subscription.push.leaseExpiresAt && subscription.push.leaseExpiresAt > now && subscription.push.leaseToken !== token) return undefined;
      subscription.push.leaseToken = token;
      subscription.push.leaseExpiresAt = expiresAt;
      transaction.set(reference, valueDocument(subscription));
      return subscription;
    });
  }

  commitPushMutation(input: PushCommitInput): Promise<PushCommitResult> {
    return this.runTransaction("push mutation", async (transaction) => {
      const reference = this.valueRef("subscriptions", input.subscriptionId);
      const current = documentValue<Subscription>(await transaction.get(reference));
      if (!current || current.push.leaseToken !== input.expectedLeaseToken) return { status: "stale_lease" };
      if (current.cursorEpoch !== input.expectedCursorEpoch) {
        delete current.push.leaseToken;
        delete current.push.leaseExpiresAt;
        transaction.set(reference, valueDocument(current));
        return { status: "stale_epoch" };
      }
      current.push = structuredClone(input.push);
      if (input.cursor !== undefined && input.cursor > current.cursor) current.cursor = input.cursor;
      if (input.lastSeenAt !== undefined) current.lastSeenAt = input.lastSeenAt;
      if (current.push.parked === 2 && current.push.gapTo !== undefined && current.cursor >= current.push.gapTo) {
        current.push.parked = 0;
        delete current.push.gapTo;
        delete current.push.nextAttemptAt;
      }
      transaction.set(reference, valueDocument(current));
      return { status: "committed" };
    });
  }

  putAdminKey(key: AdminKey): Promise<void> { return this.putValue("adminKeys", key.id, key); }
  async listAdminKeys(): Promise<AdminKey[]> { return (await this.listValues<AdminKey>("adminKeys")).sort((left, right) => left.createdAt.localeCompare(right.createdAt)); }
  deleteAdminKey(id: string): Promise<boolean> { return this.deleteValue("adminKeys", id); }

  getPollState(sourceId: string): Promise<PollState | undefined> { return this.getValue("pollStates", sourceId); }
  putPollState(state: PollState): Promise<void> { return this.putValue("pollStates", state.sourceId, state); }

  claimPollLease(sourceId: string, token: string, expiresAt: string, now: string): Promise<PollState | undefined> {
    return this.runTransaction("poll lease claim", async (transaction) => {
      const reference = this.valueRef("pollStates", sourceId);
      const state = documentValue<PollState>(await transaction.get(reference)) ?? { sourceId, generation: 0, failureCount: 0 };
      if (state.leaseExpiresAt && state.leaseExpiresAt > now && state.leaseToken !== token) return undefined;
      state.leaseToken = token;
      state.leaseExpiresAt = expiresAt;
      transaction.set(reference, valueDocument(state));
      return state;
    });
  }

  async measureSizeBytes(): Promise<number> {
    return Math.max(0, Number((await this.sizeRef().get()).data()?.value ?? 0));
  }

  private deleteEvents(snapshots: QueryDocumentSnapshot[]): Promise<{ deleted: number; bytes: number }> {
    return this.runTransaction("retention delete", async (transaction) => {
      const references = snapshots.map((snapshot) => snapshot.ref);
      const currentSnapshots = references.length > 0 ? await transaction.getAll(...references) : [];
      const sizeSnapshot = await transaction.get(this.sizeRef());
      let deleted = 0;
      let bytes = 0;
      for (const snapshot of currentSnapshots) {
        const event = eventValue(snapshot);
        if (!event) continue;
        deleted += 1;
        bytes += eventBytes(event);
        transaction.delete(snapshot.ref);
        if (event.dedupekey !== undefined) transaction.delete(this.dedupeRef(event.sourceId, event.dedupekey));
      }
      if (deleted > 0) transaction.set(this.sizeRef(), { value: Math.max(0, Number(sizeSnapshot.data()?.value ?? 0) - bytes) });
      return { deleted, bytes };
    });
  }

  async enforceRetention(policy: RetentionPolicy, now: string): Promise<RetentionResult> {
    const nowMs = Date.parse(now);
    const pinWindow = policy.pinningWindowMs ?? policy.maxAgeMs ?? 30 * 86_400_000;
    const live = (await this.listSubscriptions()).filter((subscription) =>
      subscription.lastSeenAt !== undefined && nowMs - Date.parse(subscription.lastSeenAt) <= pinWindow);
    const pinnedThrough = live.length > 0 ? Math.min(...live.map((subscription) => subscription.cursor)) : Number.MAX_SAFE_INTEGER;
    let deleted = 0;

    if (policy.maxAgeMs !== undefined) {
      const cutoff = new Date(nowMs - policy.maxAgeMs).toISOString();
      let cursor: QueryDocumentSnapshot | undefined;
      for (;;) {
        let query: Query = this.collection("events").where("receivedAt", "<", cutoff).orderBy("receivedAt", "asc").limit(RETENTION_BATCH_SIZE);
        if (cursor) query = query.startAfter(cursor);
        const page = await query.get();
        if (page.empty) break;
        const victims = page.docs.filter((snapshot) => Number(snapshot.data().seq) <= pinnedThrough);
        if (victims.length > 0) deleted += (await this.deleteEvents(victims)).deleted;
        cursor = page.docs.at(-1);
        if (page.size < RETENTION_BATCH_SIZE) break;
      }
    }

    if (policy.maxCount !== undefined) {
      const count = (await this.collection("events").count().get()).data().count;
      let excess = Math.max(0, count - policy.maxCount);
      while (excess > 0) {
        const page = await this.collection("events").where("seq", "<=", pinnedThrough).orderBy("seq", "asc").limit(Math.min(excess, RETENTION_BATCH_SIZE)).get();
        if (page.empty) break;
        const result = await this.deleteEvents(page.docs);
        deleted += result.deleted;
        excess -= result.deleted;
        if (result.deleted === 0) break;
      }
    }

    let size = await this.measureSizeBytes();
    let sizeOverride = false;
    if (policy.maxBytes !== undefined && size > policy.maxBytes) {
      sizeOverride = true;
      while (size > policy.maxBytes) {
        const page = await this.collection("events").orderBy("seq", "asc").limit(RETENTION_BATCH_SIZE).get();
        if (page.empty) break;
        const result = await this.deleteEvents(page.docs);
        deleted += result.deleted;
        size = await this.measureSizeBytes();
        if (result.deleted === 0) break;
      }
    }
    return { deleted, sizeOverride, oldestAvailableSeq: await this.oldestAvailableSeq(), sizeBytes: size };
  }
}
