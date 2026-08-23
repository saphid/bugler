# Bugler implementation notes

This document maps the public implementation and records the decisions that matter when extending or operating Bugler. The storage contract in `src/storage/driver.ts` and its shared conformance suite are the primary compatibility boundary.

## Structure map

- `src/bugler.ts`: package and compiled-binary entry point.
- `src/cli.ts`: implementations of `init`, `serve`, `poll`, `listen`, and `mcp`.
- `src/domain.ts`: storage-neutral records and wire-domain unions.
- `src/storage/driver.ts`: normative semantic storage interface.
- `src/storage/memory.ts`: pure in-memory driver.
- `src/storage/sqlite.ts`: Bun SQLite schema, SQL, pragmas, indexes, transactions, and size measurement.
- `src/storage/durable-object.ts`: Durable Object SQLite migrations and storage contract implementation.
- `src/storage/dynamodb.ts`: DynamoDB key layout, conditions, transactions, and received-time index.
- `src/storage/cosmos.ts`: Cosmos DB documents, transactional batches, ETag fencing, and indexed queries.
- `src/mapping.ts`, `src/verification.ts`, `src/filter.ts`: ingest mapping, verifier presets, and CloudEvents filters.
- `src/auth.ts`, `src/secrets.ts`, `src/environment.ts`: show-once credentials, hashing, secret resolution, and environment compatibility.
- `src/delivery.ts`, `src/push.ts`, `src/history.ts`: pull, SSE, signed push, gaps, cursor state, and filtered history.
- `src/poll.ts`, `src/reconcile.ts`, `src/runtime.ts`: poll transitions, GitHub reconciliation, retention, and due-work loops.
- `src/listen.ts`: stateless waker loop, child execution, cursor acknowledgment, and retry pacing.
- `src/mcp.ts`: bounded stdio MCP server exposing `wait_for_event` and `check_events`.
- `src/app.ts`: shared Hono routes and HTTP boundary behavior.
- `src/workers/`: outer Worker router, Durable Object runtime, bindings, and single-alarm scheduler.
- `tests/storage-conformance-suite.ts`: shared storage correctness cases used by every driver.
- `tests/workers/`: workerd storage conformance and Workers profile tests.
- `tests/core.test.ts`, `tests/api.test.ts`, `tests/wake.test.ts`: state-machine and public-boundary coverage.
- `scripts/smoke.sh`: scratch-database, signed GitHub webhook, delivery, acknowledgment, and history flow.
- `deploy/`, `Dockerfile`, `docker-compose.yml`: supported setup and container paths.

## Notable decisions

### Storage and durability

Core code depends only on `StorageDriver`. SQL, key layouts, engine APIs, size accounting, and engine-specific limits stay inside `src/storage/`. Sequence high-water marks survive retention deletion, so sequence numbers are never reused and a gap acknowledgment can advance through a deleted tail.

The shared suite defines sequence allocation, dedupe, cursor rejection precedence, lease fencing, transactional append pairs, retention pinning, size accounting, query shapes, boundary payloads, and concurrency behavior. A semantic storage change must update this suite and pass unchanged for memory, Bun SQLite, Durable Object SQLite, DynamoDB, and Cosmos DB.

Bun SQLite sets `auto_vacuum=INCREMENTAL` before first schema creation and asserts WAL, `synchronous=FULL`, and incremental auto-vacuum on every open. It refuses startup when those durability settings are unavailable.

The Durable Object build uses one named object as the log and coordinator. The outer Worker only routes. Timed poll, push, reconciliation, and retention work shares one persisted alarm. Each alarm turn processes a bounded slice of six jobs and persists an immediate continuation before outbound work when more work remains. Leases, dedupe, fencing, and transactional state transitions make alarm redelivery idempotent.

The DynamoDB driver uses one table with `pk` and `sk`. Events use `pk=EVENT` with zero-padded sequence sort keys. Materialized source-subject and source-type partitions provide the matching query shapes. Deterministic dedupe rows use `DEDUPE#source_id`. `META/COUNTER` allocates sequences, and `META/SIZE` changes in the same transaction as event mutation. The `received-index` GSI uses `gsi1pk=RECEIVED` and `gsi1sk=received_at#sequence`, so history and retention use bounded queries. Startup requires a table provisioned by `deploy/aws/setup.sh`.

The Cosmos driver keeps all records in the logical partition `partitionKey=relay`. IDs encode record kind and semantic key. Append batches contain the fenced sequence increment, event create, optional dedupe create, size increment, and optional poll-state replacement. Every per-operation result must succeed. Counter contention is retryable; only the operation that owns a lease or ETag fence can report fence loss. The exact serialized event item is measured before write so JSON escaping cannot bypass the payload limit.

DynamoDB Local and the Cosmos Linux vNext emulator run the shared conformance cases. Cosmos emulator coverage includes its returned-207 batch-failure shape. The real service's documented first-failure top-level shape is covered by synthetic response tests because the emulator emits 207 only. Cosmos service integration outside the emulator remains an operator acceptance check.

### Delivery and wake behavior

Long-poll and cursor acknowledgment use epoch-fenced monotonic compare-and-set updates. Rejection precedence is `not_found`, `stale_epoch`, `beyond_newest`, then success. An admin rewind increments the epoch so stale deliveries cannot overwrite replay state.

SSE keeps a connection-local replay position, continuously flushes new matching commits, and parks on generation-counted signals. `Last-Event-ID` is a replay hint only. A gap frame pauses delivery until cursor acknowledgment covers the gap. The default server-side connection lifetime is five minutes.

Push completion uses a lease and epoch-fenced driver mutation that can change only cursor, push-owned state, and liveness. It cannot replace filters, actions, transports, token hashes, or revocation state. Routines and OpenClaw request text contains provenance and the untrusted envelope only. Trusted actions belong in the harness-side saved prompt. Standard Webhooks identifiers remain stable across retries.

`bugler listen` keeps no local state. It processes events serially, passes each envelope on stdin, and acknowledges only after a zero child exit. Command failures for the same sequence and relay network failures use capped jittered backoff. A stale acknowledgment epoch drops the current batch and re-polls relay truth. The listener removes the resolved relay token and Bugler delivery metadata from the child environment before adding the intended convenience fields.

`bugler mcp` registers only `wait_for_event` and `check_events`. Waits default to 45 seconds and clamp to 50 seconds. Both tools describe event payloads as untrusted and require the epoch from a previous delivery when acknowledging.

Standalone polling and in-process poll sources share `PollEngine`. The standalone local state is SQLite-backed. It posts before advancing generation state, so a crash after POST repeats the same generation dedupe key and remains idempotent at the relay.

### Runtime and deployment decisions

The Workers profile caps ingest and poll bodies at 1.5 MB, advertises a preferred 240 second wait, and defaults retention scheduling to 30 minutes. Workers secrets resolve through encrypted `env:` bindings. Local `file:` references are intentionally unavailable in that runtime.

Cloud storage may bootstrap with a checksummed `BUGLER_BOOTSTRAP_ADMIN_TOKEN`; only its hash is persisted. The container entry point initializes a local database only for SQLite, avoiding independent key creation by scaled cloud revisions.

AWS setup provisions DynamoDB and a table-scoped IAM identity only. Compute is operator supplied because the complete long-polling relay has no honest always-free AWS host. Azure setup makes the operator choose scale-to-zero or an always-on minimum, provisions Cosmos free tier and Container Apps, and passes secrets through Key Vault references.

### Upgrade continuity

For one compatibility release, current `BUGLER_*` variables take precedence while corresponding `REVEILLE_*` variables remain deprecated inputs with one warning per process. Stored product-prefixed `env:` references use the same fallback. New tokens use `bgl_adm_` and `bgl_sub_`; verification continues to accept checksummed `rvl_` tokens.

Cloudflare keeps `BUGLER_LEGACY_DO=1` in `wrangler.toml` for this release. When enabled, the outer Worker routes to `idFromName("reveille")`, preserving the named Durable Object used by existing deployments while the class migration keeps its attached storage. An automatic emptiness probe was not chosen because probing a new object can initialize it and make adoption ambiguous. New deployments therefore use the compatibility name during this release; a later explicit migration can remove the binding.

The Docker entry point uses an explicit `BUGLER_DB` first, then the deprecated `REVEILLE_DB`, then `/data/reveille.db` when that file exists, and otherwise `/data/bugler.db`. It logs the selected path without exposing credentials. The GCP VM upgrade command stops and disables an installed `reveille.service` before enabling and restarting `bugler.service`.

## Public export

`scripts/export-public.sh` archives `HEAD`, removes the internal project record and historical design archive, preserves `site/index.html`, and scans the result for stale product naming, complete tokens, GitHub credential prefixes, local hostnames, and machine-specific paths. It creates a fresh repository with one `Bugler v0.1.0` commit. Compatibility identifiers are narrowly allowlisted only where implementation, migration documentation, or regression tests require them.

## Intentional deviations

The local suite uses `bun test` because Node and Vite cannot load Bun's native `bun:sqlite` module. Workers tests use Vitest with `@cloudflare/vitest-pool-workers`, which runs the Durable Object and SQLite storage under workerd. `bun run test` aggregates both runners and the Docker-backed emulator suites.

No other intentional design deviations are known.

## Future driver notes

- Firestore should allocate sequence and append within transactions, use composite indexes for source-subject and source-type queries, and document its size approximation and retention query bounds.
- Postgres should use an identity sequence that is never reset, transactional dedupe return, conditional epoch-aware cursor updates, and database-size measurement. Notifications may only nudge readers; stored rows remain truth.
- Any new driver must declare a conservative payload boundary, measure the exact representation it writes, implement bounded receive-time retention queries, distinguish retryable engine contention from semantic fence loss, and pass the shared conformance suite unchanged.
