import { expect } from "bun:test";
import type { DeliveryPayload } from "../src/delivery";

export interface DeliveryConformanceProfile {
  tokenPrefix: string;
  maxWaitSeconds: number;
}

export interface DeliveryResponse {
  status: number;
  body: unknown;
  retryAfter?: string;
}

export interface DeliveryConformanceAdapter {
  readonly profile: DeliveryConformanceProfile;
  createConsumer(id: string, filterSource?: string): Promise<{ token: string; epoch: number }>;
  append(source?: string): Promise<number>;
  gapNext(): Promise<number>;
  poll(id: string, token: string, body: Record<string, unknown>): Promise<DeliveryResponse>;
  cursor(id: string, token: string, seq: number, epoch: number): Promise<DeliveryResponse>;
  rewind(id: string, seq: number): Promise<{ epoch: number }>;
  retainNewest(count: number): Promise<void>;
  history(token: string, source?: string): Promise<DeliveryResponse>;
  rotate(id: string, overlapSeconds: number): Promise<{ token: string }>;
  revoke(id: string): Promise<void>;
  close(): Promise<void>;
}

export interface DeliveryConformanceCase {
  name: string;
  run(factory: () => Promise<DeliveryConformanceAdapter>): Promise<void>;
}

function payload(body: unknown): DeliveryPayload {
  return body as DeliveryPayload;
}

export const deliveryConformanceCases: DeliveryConformanceCase[] = [
  {
    name: "returns the exact batch shape and advances only acknowledged events",
    async run(factory) {
      const adapter = await factory();
      try {
        const consumer = await adapter.createConsumer("batch");
        expect(consumer.token.startsWith(adapter.profile.tokenPrefix)).toBe(true);
        await adapter.append();
        await adapter.append();
        const first = await adapter.poll("batch", consumer.token, { wait: 0, limit: 1 });
        expect(first.status).toBe(200);
        expect(first.body).toEqual({
          events: [expect.objectContaining({ seq: 1, source: "source/a" })],
          cursor_epoch: consumer.epoch,
          oldest_available_seq: 1,
        });
        const acknowledged = await adapter.poll("batch", consumer.token, {
          wait: 0,
          ack: 1,
          ack_epoch: consumer.epoch,
        });
        expect(acknowledged.status).toBe(200);
        expect(payload(acknowledged.body)).toMatchObject({
          events: [expect.objectContaining({ seq: 2 })],
          cursor_epoch: consumer.epoch,
          oldest_available_seq: 1,
        });
      } finally {
        await adapter.close();
      }
    },
  },
  {
    name: "uses normative cursor rejection precedence and epoch fencing",
    async run(factory) {
      const adapter = await factory();
      try {
        const consumer = await adapter.createConsumer("cursor");
        await adapter.append();
        expect((await adapter.cursor("cursor", consumer.token, 1, consumer.epoch)).status).toBe(200);
        const rewound = await adapter.rewind("cursor", 0);
        const staleAndBeyond = await adapter.cursor("cursor", consumer.token, 999, consumer.epoch);
        expect(staleAndBeyond.status).toBe(409);
        expect(staleAndBeyond.body).toEqual({ error: "stale_epoch" });
        const beyond = await adapter.cursor("cursor", consumer.token, 999, rewound.epoch);
        expect(beyond.status).toBe(422);
        expect(beyond.body).toEqual({ error: "beyond_newest" });
      } finally {
        await adapter.close();
      }
    },
  },
  {
    name: "supersedes an older parked read without weakening the payload",
    async run(factory) {
      const adapter = await factory();
      try {
        const consumer = await adapter.createConsumer("supersede");
        const older = adapter.poll("supersede", consumer.token, { wait: adapter.profile.maxWaitSeconds });
        await Bun.sleep(5);
        const newer = adapter.poll("supersede", consumer.token, { wait: 0 });
        const oldResponse = await older;
        expect(oldResponse.status).toBe(200);
        expect(oldResponse.body).toMatchObject({
          events: [],
          cursor_epoch: consumer.epoch,
          oldest_available_seq: 1,
          superseded: true,
        });
        expect((await newer).status).toBe(200);
      } finally {
        await adapter.close();
      }
    },
  },
  {
    name: "delivers and acknowledges a retention gap before resuming",
    async run(factory) {
      const adapter = await factory();
      try {
        const consumer = await adapter.createConsumer("gap");
        await adapter.append();
        await adapter.append();
        await adapter.append();
        await adapter.retainNewest(1);
        const gapResponse = await adapter.poll("gap", consumer.token, { wait: 0 });
        expect(gapResponse.status).toBe(200);
        expect(gapResponse.body).toEqual({
          gap: { from: 1, to: 2 },
          cursor_epoch: consumer.epoch,
          oldest_available_seq: 3,
        });
        const resumed = await adapter.poll("gap", consumer.token, {
          wait: 0,
          ack: 2,
          ack_epoch: consumer.epoch,
        });
        expect(resumed.status).toBe(200);
        expect(payload(resumed.body)).toMatchObject({ events: [expect.objectContaining({ seq: 3 })] });
      } finally {
        await adapter.close();
      }
    },
  },
  {
    name: "does not let an event batch cross an interleaved upstream gap",
    async run(factory) {
      const adapter = await factory();
      try {
        const consumer = await adapter.createConsumer("interleaved");
        await adapter.append();
        const first = await adapter.poll("interleaved", consumer.token, { wait: 0 });
        expect(payload(first.body)).toMatchObject({ events: [expect.objectContaining({ seq: 1 })] });
        await adapter.gapNext();
        await adapter.append();
        const gap = await adapter.poll("interleaved", consumer.token, { wait: 0, ack: 1, ack_epoch: consumer.epoch });
        expect(gap.body).toMatchObject({ gap: { from: 2, to: 2 }, cursor_epoch: consumer.epoch });
        const resumed = await adapter.poll("interleaved", consumer.token, { wait: 0, ack: 2, ack_epoch: consumer.epoch });
        expect(payload(resumed.body)).toMatchObject({ events: [expect.objectContaining({ seq: 3 })] });
      } finally {
        await adapter.close();
      }
    },
  },
  {
    name: "filters history and enforces dual-valid rotation then revocation",
    async run(factory) {
      const adapter = await factory();
      try {
        const consumer = await adapter.createConsumer("auth", "source/a");
        await adapter.append("source/a");
        await adapter.append("source/b");
        const history = await adapter.history(consumer.token);
        expect(history.status).toBe(200);
        expect(history.body).toMatchObject({ events: [expect.objectContaining({ source: "source/a" })] });
        expect((history.body as { events: unknown[] }).events).toHaveLength(1);
        const replacement = await adapter.rotate("auth", 60);
        expect((await adapter.poll("auth", consumer.token, { wait: 0 })).status).toBe(200);
        expect((await adapter.poll("auth", replacement.token, { wait: 0 })).status).toBe(200);
        await adapter.revoke("auth");
        expect((await adapter.poll("auth", replacement.token, { wait: 0 })).status).toBe(401);
      } finally {
        await adapter.close();
      }
    },
  },
];
