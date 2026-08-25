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
- `src/storage/firestore.ts`: Firestore transactions, composite query indexes, fenced state updates, and bounded retention pages.
- `src/mapping.ts`, `src/verification.ts`, `src/filter.ts`: ingest mapping, verifier presets, and CloudEvents filters.
- `src/auth.ts`, `src/secrets.ts`, `src/environment.ts`: show-once credentials, hashing, secret resolution, and environment compatibility.
- `src/delivery.ts`, `src/push.ts`, `src/history.ts`: pull, SSE, signed push, gaps, cursor state, and filtered history.
- `src/poll.ts`, `src/reconcile.ts`, `src/runtime.ts`: poll transitions, GitHub reconciliation, retention, and due-work loops.
- `src/listen.ts`: stateless waker loop, child execution, cursor acknowledgment, and retry pacing.
- `src/mcp.ts`: bounded stdio MCP server exposing `wait_for_event` and `check_events`.
- `src/daemon/`: account namespacing, local capability server, durable mirror store, hosted and OSS upstream clients, local actions, and loss recovery.
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

The shared suite defines sequence allocation, dedupe, cursor rejection precedence, lease fencing, transactional append pairs, retention pinning, size accounting, query shapes, boundary payloads, and concurrency behavior. A semantic storage change must update this suite and pass unchanged for memory, Bun SQLite, Durable Object SQLite, DynamoDB, Cosmos DB, and Firestore.

Bun SQLite sets `auto_vacuum=INCREMENTAL` before first schema creation and asserts WAL, `synchronous=FULL`, and incremental auto-vacuum on every open. It refuses startup when those durability settings are unavailable.

The Durable Object build uses one named object as the log and coordinator. The outer Worker only routes. Timed poll, push, reconciliation, and retention work shares one persisted alarm. Each alarm turn processes a bounded slice of six jobs and persists an immediate continuation before outbound work when more work remains. Leases, dedupe, fencing, and transactional state transitions make alarm redelivery idempotent.

The DynamoDB driver uses one table with `pk` and `sk`. Events use `pk=EVENT` with zero-padded sequence sort keys. Materialized source-subject and source-type partitions provide the matching query shapes. Deterministic dedupe rows use `DEDUPE#source_id`. `META/COUNTER` allocates sequences, and `META/SIZE` changes in the same transaction as event mutation. The `received-index` GSI uses `gsi1pk=RECEIVED` and `gsi1sk=received_at#sequence`, so history and retention use bounded queries. Startup requires a table provisioned by `deploy/aws/setup.sh`.

The Cosmos driver keeps all records in the logical partition `partitionKey=relay`. IDs encode record kind and semantic key. Append batches contain the fenced sequence increment, event create, optional dedupe create, size increment, and optional poll-state replacement. Every per-operation result must succeed. Counter contention is retryable; only the operation that owns a lease or ETag fence can report fence loss. The exact serialized event item is measured before write so JSON escaping cannot bypass the payload limit.

The Firestore driver stores each namespace under one root document with subcollections for semantic record kinds. A Firestore transaction reads and fences the sequence and size counters, deterministic hashed dedupe marker, event document, and optional poll state before writing. Subscription and poll lease operations use the same transaction retry boundary. Source-subject and source-type queries use the composite indexes declared in `firestore.indexes.json`; age retention pages through the single-field `receivedAt` index without scanning unrelated events. Retention deletes at most eight selected events per transaction so worst-case 1 MiB documents stay below Firestore's transaction request limit. Size accounting is the UTF-8 JSON length of semantic events. The declared payload cap reserves the measured document field envelope, document-name overhead, and 32 KiB beneath Firestore's 1 MiB document limit.

DynamoDB Local, the Cosmos Linux vNext emulator, and the official Google Cloud Firestore emulator run the shared conformance cases. Cosmos emulator coverage includes its returned-207 batch-failure shape. The real service's documented first-failure top-level shape is covered by synthetic response tests because the emulator emits 207 only. Firestore's emulator does not enforce every production transaction and size limit, so the driver applies its conservative document limit before writes and real-service integration remains an operator acceptance check.

### Delivery and wake behavior

Long-poll and cursor acknowledgment use epoch-fenced monotonic compare-and-set updates. Rejection precedence is `not_found`, `stale_epoch`, `beyond_newest`, then success. An admin rewind increments the epoch so stale deliveries cannot overwrite replay state.

SSE keeps a connection-local replay position, continuously flushes new matching commits, and parks on generation-counted signals. `Last-Event-ID` is a replay hint only. A gap frame pauses delivery until cursor acknowledgment covers the gap. The default server-side connection lifetime is five minutes.

Push completion uses a lease and epoch-fenced driver mutation that can change only cursor, push-owned state, and liveness. It cannot replace filters, actions, transports, token hashes, or revocation state. Routines and OpenClaw request text contains provenance and the untrusted envelope only. Trusted actions belong in the harness-side saved prompt. Standard Webhooks identifiers remain stable across retries.

`bugler listen` keeps no local state. It processes events serially, passes each envelope on stdin, and acknowledges only after a zero child exit. Command failures for the same sequence and relay network failures use capped jittered backoff. A stale acknowledgment epoch drops the current batch and re-polls relay truth. The listener removes the resolved relay token and Bugler delivery metadata from the child environment before adding the intended convenience fields.

`bugler mcp` registers only `wait_for_event` and `check_events`. Waits default to 45 seconds and clamp to 50 seconds. Both tools describe event payloads as untrusted and require the epoch from a previous delivery when acknowledging.

`bugler daemon` is one `(origin, tenant, principal)` account per process. Its account key namespaces the SQLite file, mode-0600 Unix socket, local capabilities, status, and hosted mint credential, so independent daemons coexist without sharing authority. Hosted frames commit the mirror event or gap and every matching local sequence mapping in one SQLite transaction before the client can send an upstream ACK. Local consumers use their own dense `(consumer_id, local_seq)` stream and never advance a hosted cursor.

The local delivery HTTP adapter and the hosted relay run the same consumer contract suite. Wait limits and token prefixes are adapter profile values; batch, gap, epoch, ACK, history, rotation, and supersession shapes are shared. The local store has separate memory and Bun SQLite implementations. SQLite crash tests use real `SIGKILL` points before commit, inside event and mapping work, and after commit, plus a real `SQLITE_FULL` failure.

Hosted gaps occupy an ordinary local sequence and form a hard batch boundary. Events before the marker may be acknowledged first, but no read can cross it. Plain OSS origins use subscriber-token long-polling. Hosted accounts prefer `bugler.v1` and switch to the same durable long-poll client after repeated socket failures. Mirror registration always imports the scoped subscriber token into a mode-0600 account file so fallback is available by construction. Missing files produce a named `fallback_unavailable` status. Both upstream paths commit locally before the upstream ACK.

Action definitions are installed with local management authority and copied into the daemon-owned account configuration directory. The daemon never discovers project or repository files. The runner serializes scheduler ticks, uses argv without a shell, a clean environment, bounded output, private large-payload files, process-group timeout cleanup, jittered retries, a durable 60-failure hourly retry ceiling, a 20-start-failure circuit, dead letters, and stable delivery ids. Successful first attempts never consume the retry ceiling. A required unavailable sandbox fails closed. Dead-letter replay increments its replay generation without changing the stable delivery id.

Local retention runs once per minute in serialized 1,000-row slices with defaults of 30 days, one million mappings, 2 GiB measured store size, seven-day liveness pinning, and 30-day dead letters. Age and count respect live consumer and enabled-action pins. Size overrides pins, vacuums incrementally, and exposes the resulting deletion through the ordinary local gap contract. Memory and SQLite stores share the same tests, including numeric backfill order and pre-existing hosted gaps.

The local MCP mode reuses the two-tool MCP adapter over the scoped local consumer interface. CI runs protocol conformance through the SDK and a real child-process stdio lifecycle, including restart and stdout purity. Product-specific harness validation remains a manual release gate recorded in `docs/daemon-mcp-compatibility.md`; pending harnesses are not presented as tested.

SQLite performs startup quick checks, runs a full integrity check after an unclean shutdown, validates monotonic local counters, and rotates two verified online backups daily. Backup recovery preserves the suspect database. Total-loss hosted recovery validates the mint-only credential before mutation, re-lists allowlisted subscriptions, rebuilds at their authoritative cursors, mints new local management authority, and emits a level-triggered recovery gap for every recreated local consumer. The disclosed loss window includes all hosted data already acknowledged by the lost store.

Daemon service install and uninstall target the per-user launchd or systemd directory. Account-scoped unit names permit multiple daemons. launchd and systemd restart failed processes with a ten-second throttle. macOS sockets use the stable per-user Application Support path rather than session `TMPDIR`. Status authenticates a socket probe, reports dead persisted state as stale, and startup reclaims a stale lock and socket after verifying that the recorded pid is not the account owner. The CLI also exposes hosted credential rotation and revocation, local consumer-token rotation, management rewind, action validation, and paginated dead-letter inspection.

Standalone polling and in-process poll sources share `PollEngine`. The standalone local state is SQLite-backed. It posts before advancing generation state, so a crash after POST repeats the same generation dedupe key and remains idempotent at the relay.

### Runtime and deployment decisions

The Workers profile caps ingest and poll bodies at 1.5 MB, advertises a preferred 240 second wait, and defaults retention scheduling to 30 minutes. Workers secrets resolve through encrypted `env:` bindings. Local `file:` references are intentionally unavailable in that runtime.

Hosted deployments route an optional tenant path or host to one named Durable Object per tenant. The default route remains the unchanged single-tenant self-hosted relay. Hosted policy is injected through runtime callbacks for metering and tier configuration, while the relay continues to depend only on the storage and delivery interfaces. The closed control-plane implementation is deliberately excluded from public exports.

The hosted outer Worker authenticates and admission-controls pull, SSE, cursor, and subscriber-history requests before tenant-object execution. It also verifies indexed ingest signatures at the edge. Invalid credentials, rejected admissions, and invalid signatures do not wake a tenant object. Solo denies SSE; paid streams use tier lifetimes, parked-connection budgets, and replaceable registry leases. Index updates that fail after a tenant commit are reconciled by persisted alarm work with a 60 second maximum backoff.

Hosted daemon credentials have only `daemon:session_mint` authority. The registry validates their allowlisted subscription auth versions, enforces a 16-active-credential principal cap, mints 24-hour device sessions, and applies reconnect, socket, channel, suspension, and shared parked-connection limits before tenant-object execution. Accepted sockets use Durable Object WebSocket Hibernation with `bugler.v1`. One cumulative frame can ACK all ready channels, but each socket has the server-enforced productive cadence exported from `platform/economics.ts`. Subscriber authorization is delivery-reconciled at a 15 second floor with no idle alarm. A detected channel mismatch removes only that channel; credential, principal, and tenant failures close the connection.

The daemon WebSocket cost model prices the incoming-frame ceiling, reconnect writes, Worker and Durable Object requests, delivered-event volume, and up to ten seconds of measured post-wake residency. Physical socket slots consume the existing tenant parked-connection units. The combined worst-case totals remain $4.195087 for Pro and $9.368281 for Team, leaving 12.6 percent and 19.2 percent headroom inside the 40 percent infrastructure budgets. Solo is an explicit bounded $2.345448 acquisition cost.

The implemented hosted economics profile is stricter than the initial research profile. A parked request can supersede only an actively waiting predecessor; after completion, Solo enforces a 60 second reconnect floor, Pro enforces 20 seconds, and idle tenants enforce 300 seconds. The cost model starts from full duty and applies only these server-enforced gaps. The closed platform suite prices actual Worker requests, registry calls, tenant calls, row writes, delivery volume, and duration at list rates, and requires at least 10 percent headroom beneath the 40 percent paid-tier budget. Hosted custom domains belong only to the closed platform Worker; the public relay configuration intentionally claims none of them.

Hosted authorization is injected at one core event-selection seam shared by pull, SSE, history, and push. The seam accepts only explicit allow, deny, or filter decisions and defaults an omitted decision to deny. The hosted layer reserves unfiltered allow for a registry-resolved tenant owner or admin; absent, malformed, removed, and unresolved principals deny. A relay admin history request receives that explicit allow only after the outer Worker verifies the admin token and resolves a current tenant authority. Invalid admin credentials stop before tenant execution. The public 32-node filter validator remains unchanged. The hosted layer may add a larger internal intersection wrapper compiled from organization membership, upstream resolver results, and direct grants. Self-hosted routes explicitly select allow and retain the existing behavior.

The closed hosted platform can receive GitHub App webhooks at one global signed endpoint. Its registry assigns each installation to one tenant and tracks the active monitored repositories. Content retains the original signed bytes and enters the existing mapper, dedupe, storage, and entitlement seams after global signature verification. The install callback validates the GitHub user-to-server token against the returned installation before atomically claiming it, then creates one logical GitHub App source and a member-owned starter subscription. Subscription creation is an atomic insert-if-absent operation in every storage driver. Existing ids return 409 without mutation, and the hosted public API reserves `github-starter` for the install flow. Consequently, later callbacks and generic create requests preserve the starter token, cursor, epoch, and principal. Installation, repository, member, and membership changes invalidate the tenant's GitHub entitlement cache.

A Clerk-verified GitHub social account can pre-link hosted identity metadata without storing an access token or granting entitlements. App installation still supplies the user-to-server token required by the resolver. Matching GitHub user ids merge the social and authorized identity; mismatches remain distinct and the explicitly authorized account is active. Platform outbound requests use workerd-supported manual redirect handling and reject unexpected statuses explicitly.

Hosted billing uses provider adapters over one registry ownership and tier-assignment seam. Stripe is primary: customer ownership is stored before Checkout, linked customers change their single subscription in place, and signed events plus the success return run one full-state subscription synchronizer. Only that synchronizer calls `setTier`. The Paddle receiver is hard-disabled while its adapter code remains dormant for a future full-state migration path.

Cloud storage may bootstrap with a checksummed `BUGLER_BOOTSTRAP_ADMIN_TOKEN`; only its hash is persisted. The container entry point initializes a local database only for SQLite, avoiding independent key creation by scaled cloud revisions.

AWS setup provisions DynamoDB and a table-scoped IAM identity only. Compute is operator supplied because the complete long-polling relay has no honest always-free AWS host. Azure setup makes the operator choose scale-to-zero or an always-on minimum, provisions Cosmos free tier and Container Apps, and passes secrets through Key Vault references. GCP setup offers either the SQLite VM or Firestore-backed Cloud Run with zero minimum instances. The Cloud Run recipe prints that cold starts can miss webhook deadlines and that scheduled work cannot originate while no instance exists.

### Upgrade continuity

For one compatibility release, current `BUGLER_*` variables take precedence while corresponding `REVEILLE_*` variables remain deprecated inputs with one warning per process. Stored product-prefixed `env:` references use the same fallback. New tokens use `bgl_adm_` and `bgl_sub_`; verification continues to accept checksummed `rvl_` tokens.

Cloudflare keeps `BUGLER_LEGACY_DO=1` in `wrangler.toml` for this release. When enabled, the outer Worker routes to `idFromName("reveille")`, preserving the named Durable Object used by existing deployments while the class migration keeps its attached storage. An automatic emptiness probe was not chosen because probing a new object can initialize it and make adoption ambiguous. New deployments therefore use the compatibility name during this release; a later explicit migration can remove the binding.

The Docker entry point uses an explicit `BUGLER_DB` first, then the deprecated `REVEILLE_DB`, then `/data/reveille.db` when that file exists, and otherwise `/data/bugler.db`. It logs the selected path without exposing credentials. The GCP VM upgrade command stops and disables an installed `reveille.service` before enabling and restarting `bugler.service`.

## Public export

`scripts/export-public.sh` archives `HEAD`, removes the internal project record and historical design archive, preserves `site/index.html`, prunes closed-platform dependencies, regenerates the public lockfile, verifies a frozen install, and scans the result for stale product naming, complete tokens, GitHub credential prefixes, local hostnames, and machine-specific paths. It creates a fresh repository with one `Bugler v0.1.0` commit. Compatibility identifiers are narrowly allowlisted only where implementation, migration documentation, or regression tests require them.

## Intentional deviations

The local suite uses `bun test` because Node and Vite cannot load Bun's native `bun:sqlite` module. Workers tests use Vitest with `@cloudflare/vitest-pool-workers`, which runs the Durable Object and SQLite storage under workerd. `bun run test` aggregates both runners and the Docker-backed emulator suites.

No other intentional design deviations are known.

## Future driver notes

- Postgres should use an identity sequence that is never reset, transactional dedupe return, conditional epoch-aware cursor updates, and database-size measurement. Notifications may only nudge readers; stored rows remain truth.
- Any new driver must declare a conservative payload boundary, measure the exact representation it writes, implement bounded receive-time retention queries, distinguish retryable engine contention from semantic fence loss, and pass the shared conformance suite unchanged.
