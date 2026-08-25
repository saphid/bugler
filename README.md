# Bugler

Bugler is a storage-neutral event relay that verifies webhooks, records CloudEvents in one ordered log, filters them per logical consumer, and wakes agents through long-poll, SSE, or signed push. Waiting spends no inference tokens. Delivery is at least once within retention, with explicit gap notifications when retention overtakes a cursor.

## Quickstart

Requirements: Bun 1.4.0 or newer.

```bash
bun install
export GH_WEBHOOK_SECRET='provision-this-outside-the-repository'

# Create the database and first show-once admin key.
bun run bugler init --db bugler.db

# Start the relay.
bun run bugler serve --db bugler.db --host 127.0.0.1 --port 8787
```

Save the `bgl_adm_...` token printed by `init`. Create a GitHub source:

```bash
export BUGLER_ADMIN_TOKEN='bgl_adm_...'
curl -X POST http://127.0.0.1:8787/sources \
  -H "Authorization: Bearer $BUGLER_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "id":"github-widgets",
    "kind":"http",
    "verification":{"preset":"github","secret_refs":["env:GH_WEBHOOK_SECRET"]},
    "mapping":{"preset":"github","source":"github/acme/widgets"}
  }'
```

Create one subscription per consumer. The returned `bgl_sub_...` token is shown once:

```bash
curl -X POST http://127.0.0.1:8787/subscriptions \
  -H "Authorization: Bearer $BUGLER_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "id":"pr-42-agent",
    "filter":{"all":[
      {"exact":{"source":"github/acme/widgets"}},
      {"exact":{"subject":"pr/42"}},
      {"any":[
        {"prefix":{"type":"com.github.pull_request_review"}},
        {"prefix":{"type":"com.github.check_run"}}
      ]}
    ]},
    "transport":{"type":"pull"},
    "action":"Reconcile PR 42 against current GitHub state."
  }'
```

For a provider without a preset, `mapping.source` is the CloudEvents source string while every other mapped attribute is an object selecting `static`, `header`, `json_pointer`, or `template`. This complete generic-token example maps an event type from a constant, a subject from JSON, and a dedupe key from a header:

```bash
export CUSTOM_WEBHOOK_TOKEN='replace-me'
curl -X POST http://127.0.0.1:8787/sources \
  -H "Authorization: Bearer $BUGLER_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "id":"custom-alerts",
    "kind":"http",
    "verification":{"preset":"generic-token","secret_refs":["env:CUSTOM_WEBHOOK_TOKEN"]},
    "mapping":{
      "source":"custom/acme-alerts",
      "type":{"static":"com.acme.alert"},
      "subject":{"json_pointer":"/service/name"},
      "dedupekey":{"header":"x-event-id"}
    }
  }'
```

Send a signed sample event so the delivery commands have one sequence to process:

```bash
PAYLOAD='{"action":"submitted","pull_request":{"number":42},"review":{"id":1},"repository":{"full_name":"acme/widgets"}}'
SIGNATURE=$(printf '%s' "$PAYLOAD" | bun -e 'import { createHmac } from "node:crypto"; const body=await Bun.stdin.text(); console.log("sha256="+createHmac("sha256",process.env.GH_WEBHOOK_SECRET).update(body).digest("hex"))')
curl -X POST http://127.0.0.1:8787/ingest/github-widgets \
  -H 'Content-Type: application/json' \
  -H 'X-GitHub-Event: pull_request_review' \
  -H 'X-GitHub-Delivery: quickstart-1' \
  -H "X-Hub-Signature-256: $SIGNATURE" \
  --data-binary "$PAYLOAD"
```

Poll, then acknowledge the highest processed sequence using the returned epoch:

```bash
export BUGLER_SUB_TOKEN='bgl_sub_...'
POLL_RESPONSE=$(curl -sS -X POST http://127.0.0.1:8787/subscriptions/pr-42-agent/events \
  -H "Authorization: Bearer $BUGLER_SUB_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"wait":"45s","limit":20}')
printf '%s\n' "$POLL_RESPONSE"
SEQ=$(printf '%s' "$POLL_RESPONSE" | bun -e 'const r=await Bun.stdin.json(); console.log(r.events.at(-1).seq)')
EPOCH=$(printf '%s' "$POLL_RESPONSE" | bun -e 'const r=await Bun.stdin.json(); console.log(r.cursor_epoch)')

curl -X POST http://127.0.0.1:8787/subscriptions/pr-42-agent/cursor \
  -H "Authorization: Bearer $BUGLER_SUB_TOKEN" \
  -H 'Content-Type: application/json' \
  -d "{\"seq\":$SEQ,\"epoch\":$EPOCH}"
```

SSE connections continuously deliver backlog and newly committed matching events. `Last-Event-ID` is only a replay hint and never advances the durable cursor. Each server connection is bounded to five minutes by default, so clients should reconnect with their last received event id. A gap frame pauses that connection until the gap is acknowledged through the cursor endpoint.

Query filtered history on relay receive time:

```bash
curl 'http://127.0.0.1:8787/events?source=github%2Facme%2Fwidgets&subject=pr%2F42&since=2026-08-19T00%3A00%3A00Z' \
  -H "Authorization: Bearer $BUGLER_SUB_TOKEN"
```

Run the complete local flow with a real-shaped signed GitHub review payload:

```bash
bun run smoke
```

## Hosted early access

The hosted control plane at [app.bugler.dev](https://app.bugler.dev) provisions an isolated relay after Clerk sign-in. Select **Add GitHub**, choose the organization and repositories on GitHub, and approve the App. On first setup, the callback creates the tenant's GitHub App source, monitored-repository set, and starter subscription, links the installing member for repository entitlements, and reveals the subscriber token once. Repeating the flow preserves the existing starter token, cursor, epoch, and principal. Stripe, Sentry, GitLab, and CircleCI remain available in the same source wizard as paste-one-URL presets. The dashboard also tails recent history and shows monthly usage. Agents can mint a hashed-at-rest platform API key and use the same control-plane operations without a browser. Machine-readable setup commands are available at `https://bugler.dev/auth.md` and `https://app.bugler.dev/auth.md`.

Hosted tenant state is isolated by one Durable Object per tenant. The public relay remains storage-neutral and single-tenant by default. Hosted routing and policy are an optional layer and do not change self-hosted paths or delivery semantics.

Hosted Clerk organizations map one-to-one to tenants with owner, admin, and member roles. Enterprise SAML and OIDC connections are configured in Clerk. Members can link GitHub through the GitHub App authorization flow; Bugler mirrors their user-scoped installation repositories and intersects that entitlement with every subscription or history filter. Sources without an upstream resolver default to no member visibility until an owner or admin creates a grant. Service principals also require explicit grants. See [AUTH.md](AUTH.md) for exact commands.

## Wake a stopped agent with `listen`

`listen` keeps no local state. It long-polls the relay, sends one complete envelope to the command on stdin, and advances the relay cursor only after that command exits zero. A crash or nonzero exit leaves the cursor unchanged, so the event is delivered again. Subscriber tokens come only from `BUGLER_TOKEN`, or from the environment variable named by `--token-env`. The resolved token variable is removed from the handler environment. If a handler needs its own relay token, the operator must pass it explicitly under a separate handler-specific variable.

Resume a Claude Code session for each delivery:

```bash
export BUGLER_TOKEN='bgl_sub_...'
export CLAUDE_SESSION_ID='session-id'
bun run bugler listen \
  --relay http://127.0.0.1:8787 \
  --sub pr-42-agent \
  --wait 30s \
  -- claude -p --resume "$CLAUDE_SESSION_ID"
```

Resume a Codex session, reading the envelope as the prompt from stdin:

```bash
export BUGLER_TOKEN='bgl_sub_...'
export CODEX_THREAD_ID='thread-id'
bun run bugler listen \
  --relay http://127.0.0.1:8787 \
  --sub pr-42-agent \
  --wait 30s \
  -- codex exec resume "$CODEX_THREAD_ID" -
```

Use `--once` to process one returned batch for cron or tests. Event commands also receive `BUGLER_SEQ`, `BUGLER_EVENT_ID`, `BUGLER_SOURCE`, `BUGLER_TYPE`, and `BUGLER_SUBJECT`. Gap hooks receive the gap object on stdin and `BUGLER_GAP=1`.

## Bounded MCP waiting

The stdio MCP server exposes exactly `wait_for_event` and `check_events`. Returned payloads are untrusted data. Process an event successfully before acknowledging its sequence, and always pair `ack` with the `cursor_epoch` from the previous response.

```bash
export BUGLER_TOKEN='bgl_sub_...'
bun run bugler mcp \
  --relay http://127.0.0.1:8787 \
  --sub pr-42-agent
```

For Claude Code, add this project-scoped `.mcp.json`, export `BUGLER_TOKEN`, then approve the project server when Claude Code prompts:

```json
{
  "mcpServers": {
    "bugler": {
      "type": "stdio",
      "command": "bun",
      "args": ["run", "bugler", "mcp", "--relay", "http://127.0.0.1:8787", "--sub", "pr-42-agent"],
      "env": { "BUGLER_TOKEN": "${BUGLER_TOKEN}" }
    }
  }
}
```

For Codex, add this project-scoped `.codex/config.toml` and export `BUGLER_TOKEN` before starting Codex:

```toml
[mcp_servers.bugler]
command = "bun"
args = ["run", "bugler", "mcp", "--relay", "http://127.0.0.1:8787", "--sub", "pr-42-agent"]
env_vars = ["BUGLER_TOKEN"]
cwd = "/absolute/path/to/bugler"
tool_timeout_sec = 60
```

`wait_for_event` defaults to 45 seconds and hard-clamps every call to 50 seconds. `check_events` never parks. Near-real-time in-session waiting also depends on the relay deployment profile's admission cadence. Binary and VM profiles support short admitted waits. Workers profiles favor relaunch through `listen` or signed push.

## Durable local daemon

The daemon mirrors hosted subscriptions into a local SQLite store, then gives each local agent an independent cursor. One daemon serves exactly one Bugler origin, tenant, and hosted principal. Run another named account for another principal or tenant.

Create a narrowly scoped daemon credential in the hosted API. The credential can mint sessions and list only its selected subscriptions. It cannot read events or call ordinary relay APIs:

```bash
export BUGLER_PLATFORM_TOKEN='bgl_plt_replace_me'
export BUGLER_DAEMON_TOKEN="$(
  curl -fsS https://app.bugler.dev/api/daemon-credentials \
    -H "Authorization: Bearer $BUGLER_PLATFORM_TOKEN" \
    -H 'Content-Type: application/json' \
    -d '{"subscription_ids":["github-starter"]}' | jq -r .token
)"

bun run bugler daemon init \
  --account work \
  --origin https://relay.bugler.dev \
  --tenant tenant_123 \
  --principal user_123 \
  --credential-env BUGLER_DAEMON_TOKEN

bun run bugler daemon start --account work
```

In another shell, register the hosted mirror and create a local consumer. Consumer creation reveals one scoped local token:

```bash
export BUGLER_HOSTED_SUB_TOKEN='bgl_sub_replace_me'
bun run bugler local mirror add --account work --sub github-starter \
  --token-env BUGLER_HOSTED_SUB_TOKEN
bun run bugler local consumer create --account work --id codex --mirror github-starter
export BUGLER_LOCAL_TOKEN='bgl_lcon_replace_me'
bun run bugler local events --account work --consumer codex --wait 45s
```

The daemon acknowledges upstream only after the full frame and local consumer mappings commit durably. The hosted credential is imported from the named environment variable into a mode-0600 account file, then removed from the daemon process environment. Local management and consume capabilities are separate. The Unix socket and every account path are derived from the complete account tuple, so multiple daemons coexist safely.

A plain self-hosted relay uses the same daemon without hosted credentials. Initialize and start it in one shell:

```bash
bun run bugler daemon init --account local --origin http://127.0.0.1:8787 \
  --tenant default --principal operator
bun run bugler daemon start --account local
```

In another shell, give each mirror its ordinary subscriber token by environment-variable name:

```bash
export BUGLER_SUB_TOKEN='bgl_sub_replace_me'
bun run bugler local mirror add --account local --sub pr-agent \
  --token-env BUGLER_SUB_TOKEN
```

This mode long-polls the OSS relay within its admitted wait and retry hints. Hosted accounts prefer the `bugler.v1` WebSocket and use the same per-subscription long-poll path after repeated socket failures. Every mirror therefore requires its subscriber token at registration, including hosted mirrors. Bugler stores each token in a private mode-0600 account file. `daemon status --json` reports `fallback_unavailable` plus the exact mirrors missing tokens instead of silently retrying.

Local actions are JSON files copied into the daemon-owned account configuration directory only. Project checkouts are never searched. Install an action with the local management capability, then inspect or replay failures:

```bash
bun run bugler local actions install ./wake-codex.json --account work
bun run bugler local actions list --account work
bun run bugler local dead-letter list --account work
bun run bugler local dead-letter replay --account work --id dlq_id
```

Actions receive the local CloudEvent as JSON on stdin, or through a private temporary file when large, plus scoped `BUGLER_*` metadata. They run without a shell and with a clean environment. They still have the daemon user's filesystem and network authority unless the Linux action explicitly requires an available sandbox. Execution is at least once; use `BUGLER_DELIVERY_ID` as the idempotency key.

The local MCP adapter exposes the same two bounded tools over the daemon consumer socket:

```bash
export BUGLER_LOCAL_TOKEN='bgl_lcon_replace_me'
bun run bugler mcp --account work --consumer codex
```

Total local database loss can rebuild mirror positions from hosted truth, but cannot recover locally acknowledged payloads, consumer tokens, attempts, or dead letters:

```bash
bun run bugler daemon recover --account work --from-hosted
```

Verified daily backups rotate beside the database. Restoring one requires an explicit confirmation flag and preserves the suspect database:

```bash
bun run bugler daemon recover --account work \
  --from-backup /path/to/daemon.db.backup-1 --yes
```

## Beyond GitHub

The verifier and mapping layers are provider-neutral. Useful patterns include:

- Stripe: select the `stripe` verifier preset, map `type` from `/type`, and dedupe from `/id`.
- Sentry: select the `sentry` preset, map alert type and subject from headers or JSON pointers, and keep the ingest path fast for Sentry's response deadline.
- GitLab: select `gitlab` for Standard Webhooks signatures. The same implementation accepts concurrent signing keys during rotation.
- CircleCI: select `circleci`, then map workflow or job identity from its JSON body.
- Jenkins and bridge scripts: use `generic-token`. A queue bridge for SQS, Pub/Sub, NATS, or Redis only needs to POST each message with a stable dedupe header.
- Poll-only systems: use an in-process `poll` source or the bundled standalone agent. The standalone form keeps restart-safe generation state locally and POSTs transitions to a generic-token source:

```bash
export BUGLER_INGEST_TOKEN='source-token'
bun run bugler poll \
  --url https://status.example.com/api/v2/status.json \
  --to https://relay.example.com/ingest/status-upstream \
  --token-ref env:BUGLER_INGEST_TOKEN \
  --source-id status-upstream \
  --json-pointer /status/indicator \
  --interval 300
```

For a generic standalone poll target, configure relay mapping with a static source and type plus `dedupekey: {"header":"X-Bugler-Dedupe-Key"}`. Duplicate POSTs after a crash collapse on `(source_id, dedupe_key)`.

## Storage drivers

| Driver | Intended target | Transactions and queries | Payload limit |
| --- | --- | --- | --- |
| `bun:sqlite` | Local, VM, container | WAL, `synchronous=FULL`, atomic local transactions, indexed sequence/source/subject/type/receive-time queries | 5 MB default |
| Pure memory | Tests and embedding | Serialized critical sections and bounded in-memory scans | 5 MB default |
| Durable Object SQLite | Cloudflare Workers | One named SQLite Durable Object, synchronous transactions, indexed query shapes, and `databaseSize` measurement | 1.5 MB |
| DynamoDB | AWS | Atomic counter, conditional writes, transactional append pairs, materialized source query keys, and one receive-time GSI | 392,546 bytes |
| Cosmos DB NoSQL | Azure | One logical partition, transactional batches, deterministic conflict keys, ETag CAS, and indexed query shapes | 2,063,812 bytes |
| Firestore | GCP Cloud Run | Transactional sequence, append, dedupe, and fencing; composite source query indexes; bounded receive-time retention queries | 1,014,804 bytes |
| Bring-your-own Postgres | Teams with an existing database | Identity sequence, conditional cursor update, per-driver migrations | Driver-declared |

No SQL or storage-engine behavior is visible outside `src/storage`. The shared conformance suite defines correctness for every driver.

The cloud-driver limits reserve measured serialized-envelope overhead plus a safety margin beneath each engine's item limit. This lets the HTTP boundary reject an oversized raw payload with 413 before storage instead of admitting a nominally in-cap payload that the engine cannot represent.

## Deploy

### Local and Docker

The Bun quickstart above is the simplest local path. For the container path, the first start initializes `/data/bugler.db` and prints the show-once admin key in the service logs:

```bash
docker compose up --build -d
docker compose logs bugler
curl --fail http://127.0.0.1:8787/healthz
```

The Compose service uses one persistent volume and checks `/healthz`. Put source secrets into the container environment or a container secret mechanism, then reference them as `env:NAME`. Do not bake secrets into the image or Compose file.

The bootstrap admin token remains in the container runtime's logs after retrieval. Treat those logs as credentials. Immediately mint a replacement admin key with `POST /admin-keys`, verify it, then list key metadata with `GET /admin-keys` and delete the bootstrap key with `DELETE /admin-keys/{id}`. Operators who cannot restrict or promptly rotate the logged token should use the local binary initialization flow instead.

### Cloudflare Workers

The Workers build uses one named SQLite Durable Object as the log and coordinator. The outer Worker only routes requests. Generate a valid bootstrap admin token without creating a local database, then run the setup preview:

```bash
export BUGLER_ADMIN_TOKEN=$(bun -e 'import { mintToken } from "./src/auth"; console.log(mintToken("adm").token)')
bash deploy/cloudflare/setup.sh --dry-run
```

After reviewing the transcript, run `bash deploy/cloudflare/setup.sh`. It checks Wrangler authentication and the generated binding types, deploys the configured SQLite Durable Object migration once, securely prompts for `BUGLER_BOOTSTRAP_ADMIN_TOKEN` and `BUGLER_GITHUB_SECRET`, updates them through stdin, then deploys again to activate the secrets. It prints the ingest URL plus a first-source request. The bootstrap token is hashed into Durable Object storage and remains show-once from the operator's perspective. Workers supports `env:` secret bindings, not local `file:` references.

The Workers profile caps ingest and poll bodies at 1.5 MB, advertises a preferred 240 second wait, and uses a persisted single-alarm scheduler. Due work runs in slices of at most six concurrent jobs. When more due work remains, an immediate continuation alarm is persisted before outbound work begins.

### AWS DynamoDB plus your compute

AWS has no honest always-free host for the complete long-polling core. App Runner has no free tier, Lambda has a 15 minute invocation ceiling and fits only an ingest shim, and ECS, Fargate, and Lightsail have compute costs. Bugler therefore ships an honest storage-only setup for AWS:

```bash
bash deploy/aws/setup.sh --region us-east-1 --table bugler --dry-run
bash deploy/aws/setup.sh --region us-east-1 --table bugler --yes
```

The script creates or reuses a provisioned DynamoDB table and a least-privilege IAM user for only that table and its `received-index`. It prints access credentials once only when it creates the user's first key. Run the standard container on compute you already operate with:

```bash
export BUGLER_STORAGE=dynamodb
export BUGLER_DYNAMODB_TABLE=bugler
export AWS_REGION=us-east-1
export BUGLER_BOOTSTRAP_ADMIN_TOKEN='bgl_adm_...'
docker run --rm -p 8787:8787 \
  -e BUGLER_STORAGE -e BUGLER_DYNAMODB_TABLE -e AWS_REGION \
  -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
  -e BUGLER_BOOTSTRAP_ADMIN_TOKEN your-registry/bugler:latest
```

The relay requires this table to exist and refuses to create an on-demand table during startup. A missing table error points back to `deploy/aws/setup.sh`, preserving the setup script's provisioned-capacity cost model.

DynamoDB's allowance is bounded and current account rules vary. Storage or request use beyond the allowance can incur charges.

### Azure Container Apps and Cosmos DB

The Azure path uses Container Apps Consumption plus a Cosmos DB NoSQL free-tier account. Choose the cost and reliability mode explicitly:

```bash
# Exactly $0 while idle, with cold-start risk on webhook ingest.
bash deploy/azure/setup.sh \
  --resource-group bugler-prod \
  --key-vault bugler-prod-kv \
  --image yourregistry.azurecr.io/bugler:latest \
  --mode scale-to-zero \
  --dry-run

# Recommended always-warm profile, roughly $3 to $5 per month when mostly idle.
bash deploy/azure/setup.sh \
  --resource-group bugler-prod \
  --image yourregistry.azurecr.io/bugler:latest \
  --mode always-on \
  --yes
```

The script provisions the lifetime free-tier Cosmos account at shared 1000 RU/s, one `/partitionKey` container, a Container Apps environment, and one app. Raw Cosmos and bootstrap credentials are streamed into Key Vault and Container Apps receives only Key Vault references. Key Vault names are globally unique; use `--key-vault` when the default is already owned. Re-runs reuse the existing bootstrap secret, recover only a matching soft-deleted vault, and never purge a protected vault. Scale-to-zero can cold start beyond GitHub's 10 second response deadline, and GitHub does not automatically retry those failures. The always-on mode avoids that exposure but is not strictly free. A continuously active population of long polls can cost more.

The Cosmos driver is verified under Microsoft's Linux vNext emulator on ARM64. All relay records share one logical partition at Bugler's intended volume, allowing the sequence counter, event create, dedupe marker, size counter, and poll advance to commit atomically. The `serve` and `init` commands select it with `BUGLER_STORAGE=cosmos`; the Azure script supplies the remaining endpoint, key, database, and container settings.

### GCP Cloud Run and VM

Preview the Firestore-backed Cloud Run scale-to-zero profile:

```bash
bash deploy/gcp/setup-cloudrun.sh --project YOUR_PROJECT --dry-run
```

After reviewing the plan, run it with `--yes`. The script enables the required APIs, creates a Firestore Native database and the two composite event indexes, stores the bootstrap admin token in Secret Manager, builds and pushes the Docker image through Cloud Build and Artifact Registry, and deploys `BUGLER_STORAGE=firestore` with a dedicated `roles/datastore.user` service identity. Re-runs reuse the database, indexes, service identity, registry, and enabled secret version.

Cloud Run uses zero minimum instances and one maximum instance. This can be exactly idle when unused, but a cold start can exceed GitHub's 10 second response deadline and GitHub does not automatically retry a failed delivery. While no instance exists, Bugler also cannot originate scheduled poll or retention work. An inbound webhook, pull, or health request must start an instance first. Keep one active listener or use the VM profile when timely webhook admission and autonomous schedules matter.

The Firestore driver keeps the sequence counter, event, deterministic dedupe marker, semantic size counter, and optional poll state in one transaction. Cursor and push/poll lease updates are transactionally fenced. Receive-time retention walks the `receivedAt` index in bounded pages, and source-subject plus source-type history uses the declared composite indexes in `firestore.indexes.json`. The size result is a transactionally maintained UTF-8 JSON event-byte approximation, not Firestore billing usage.

Preview the SQLite VM profile:

```bash
bash deploy/gcp/setup-vm.sh --project YOUR_PROJECT --dry-run
```

The real command requires an already linked billing account, enables only the Compute Engine API, cross-compiles the Linux x64 binary, and creates or reuses one `e2-micro` with a 30 GB standard disk in an eligible free-tier region. It installs Bugler under systemd and prints Cloudflare Tunnel, Tailscale Funnel, and firewall guidance. It never changes billing settings. See [deploy/gcp/README.md](deploy/gcp/README.md) for the Cloud Run tradeoffs and external IPv4 cost caveat.

### GitHub repository webhook

Preview the create-or-update operation:

```bash
bash deploy/github/wire-repo.sh owner/repo https://relay.example/ingest/github-owner-repo --dry-run
```

The real command requires `--yes`. It sends the generated webhook secret to `gh api` through stdin, prints it once, and prints the matching source request using `env:BUGLER_GITHUB_SECRET`.

## Security and delivery contract

Admin and subscriber bearer tokens have CRC32 typo checks, are shown once, and are stored only as SHA-256 hashes. Source and push signing secrets remain `env:` or `file:` references and resolve at use time. Missing references fail configuration with 422 and fail closed later. Tokens in query strings are rejected.

New credentials use `bgl_adm_` and `bgl_sub_`. For one compatibility release, checksummed legacy `rvl_` tokens remain valid. `REVEILLE_*` configuration inputs remain deprecated aliases for `BUGLER_*`; Bugler prefers the new name and warns once when it falls back to a legacy variable.

Verified means authentic, never safe to obey. Provider-controlled `type`, `subject`, `time`, `dedupekey`, and payload fields remain untrusted data. Stored actions are admin-controlled text.

The generic push adapter sends the envelope as its JSON body. The Routines and OpenClaw adapters send provenance plus the envelope JSON inside the harness's untrusted `text` field. For those two adapters, configure the stored action as trusted harness-side instructions in the Routine prompt or OpenClaw hook mapping. Bugler does not copy the action into inbound untrusted text.

See [How Bugler works](docs/how-bugler-works.html), the [harness integration guide](docs/integration-guide.md), and [AUTH.md](AUTH.md) for the public operating contract.

Bugler is available under the MIT License. Contributions follow [CONTRIBUTING.md](CONTRIBUTING.md), and security reports follow [SECURITY.md](SECURITY.md).
