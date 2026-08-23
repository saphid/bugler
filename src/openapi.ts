export const OPENAPI_DOCUMENT = {
  openapi: "3.1.0",
  info: {
    title: "Bugler API",
    version: "0.1.0",
    description: "Verified event ingest, cursor delivery, filtered history, source management, and credential lifecycle.",
  },
  components: {
    securitySchemes: {
      bearer: { type: "http", scheme: "bearer" },
    },
    schemas: {
      Cursor: {
        type: "object",
        required: ["seq", "epoch"],
        properties: { seq: { type: "integer", minimum: 0 }, epoch: { type: "integer", minimum: 0 } },
      },
      Poll: {
        type: "object",
        properties: {
          wait: { oneOf: [{ type: "number" }, { type: "string" }] },
          ack: { type: "integer", minimum: 0 },
          ack_epoch: { type: "integer", minimum: 0 },
          limit: { type: "integer", minimum: 1 },
        },
      },
      Gap: {
        type: "object",
        required: ["gap", "cursor_epoch", "oldest_available_seq"],
        properties: {
          gap: { type: "object", required: ["from", "to"], properties: { from: { type: "integer" }, to: { type: "integer" } } },
          cursor_epoch: { type: "integer" },
          oldest_available_seq: { type: "integer" },
        },
      },
    },
  },
  paths: {
    "/healthz": { get: { summary: "Relay health and retention watermark", responses: { "200": { description: "Health report" } } } },
    "/ingest/{source_id}": {
      post: {
        summary: "Verify, map, deduplicate, durably commit, then acknowledge an event",
        parameters: [{ in: "path", name: "source_id", required: true, schema: { type: "string" } }],
        responses: { "200": { description: "Committed or deduplicated" }, "401": { description: "Verification failed" }, "413": { description: "Payload too large" }, "415": { description: "Unsupported content encoding" } },
      },
    },
    "/sources": {
      get: { summary: "List sources", security: [{ bearer: [] }], responses: { "200": { description: "Source list" } } },
      post: { summary: "Create or replace a source", security: [{ bearer: [] }], responses: { "201": { description: "Source created" }, "422": { description: "Invalid config or secret reference" } } },
    },
    "/sources/{id}": {
      get: { summary: "Get source", security: [{ bearer: [] }], responses: { "200": { description: "Source" } } },
      patch: { summary: "Update source", security: [{ bearer: [] }], responses: { "200": { description: "Source updated" }, "422": { description: "Invalid config or secret reference" } } },
      delete: { summary: "Delete source", security: [{ bearer: [] }], responses: { "204": { description: "Deleted" } } },
    },
    "/subscriptions": {
      get: { summary: "List subscriptions", security: [{ bearer: [] }], responses: { "200": { description: "Subscription list" } } },
      post: { summary: "Create subscription and show token once", security: [{ bearer: [] }], responses: { "201": { description: "Subscription and token" }, "422": { description: "Invalid filter, transport, or secret reference" } } },
    },
    "/subscriptions/{id}": {
      get: { summary: "Get subscription", security: [{ bearer: [] }], responses: { "200": { description: "Subscription" } } },
      patch: { summary: "Update subscription", security: [{ bearer: [] }], responses: { "200": { description: "Updated" } } },
      delete: { summary: "Delete subscription", security: [{ bearer: [] }], responses: { "204": { description: "Deleted" } } },
    },
    "/subscriptions/{id}/events": {
      post: { summary: "Long-poll events and optionally acknowledge", security: [{ bearer: [] }], requestBody: { content: { "application/json": { schema: { $ref: "#/components/schemas/Poll" } } } }, responses: { "200": { description: "Batch, empty response, or gap" }, "429": { description: "Admission cadence exceeded" } } },
    },
    "/subscriptions/{id}/stream": { get: { summary: "SSE delivery with sequence event IDs", security: [{ bearer: [] }], responses: { "200": { description: "SSE stream" } } } },
    "/subscriptions/{id}/cursor": {
      post: { summary: "Acknowledge, skip, or rewind with admin auth", security: [{ bearer: [] }], requestBody: { content: { "application/json": { schema: { $ref: "#/components/schemas/Cursor" } } } }, responses: { "200": { description: "Cursor state" }, "409": { description: "Stale epoch" }, "422": { description: "Beyond newest sequence" } } },
    },
    "/subscriptions/{id}/token/rotate": { post: { summary: "Rotate subscriber token with overlap", security: [{ bearer: [] }], responses: { "200": { description: "Show-once replacement token" } } } },
    "/subscriptions/{id}/token": { delete: { summary: "Revoke subscriber token", security: [{ bearer: [] }], responses: { "204": { description: "Revoked" } } } },
    "/subscriptions/{id}/push-status": { get: { summary: "Inspect push state", security: [{ bearer: [] }], responses: { "200": { description: "Push state" } } } },
    "/subscriptions/{id}/push/unpark": { post: { summary: "Clear push retry state", security: [{ bearer: [] }], responses: { "200": { description: "Unparked" } } } },
    "/events": { get: { summary: "Query sequence-ordered history", security: [{ bearer: [] }], responses: { "200": { description: "Filtered page and continuation" } } } },
    "/admin-keys": {
      get: { summary: "List admin key metadata", security: [{ bearer: [] }], responses: { "200": { description: "Admin keys" } } },
      post: { summary: "Mint and show an admin key once", security: [{ bearer: [] }], responses: { "201": { description: "Admin key and token" } } },
    },
    "/admin-keys/{id}": {
      patch: { summary: "Set or clear admin key expiry", security: [{ bearer: [] }], responses: { "200": { description: "Updated key metadata" } } },
      delete: { summary: "Revoke admin key", security: [{ bearer: [] }], responses: { "204": { description: "Revoked" }, "409": { description: "Last active key protected" } } },
    },
  },
} as const;
