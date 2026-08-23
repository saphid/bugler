#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
SCRATCH_DIR=$(mktemp -d "${TMPDIR:-/tmp}/bugler-smoke.XXXXXX")
PORT=${BUGLER_SMOKE_PORT:-$((20000 + RANDOM % 20000))}
BASE_URL="http://127.0.0.1:${PORT}"
SERVER_PID=""
WAKER_PID=""

cleanup() {
  if [[ -n "$WAKER_PID" ]]; then
    kill "$WAKER_PID" 2>/dev/null || true
    wait "$WAKER_PID" 2>/dev/null || true
  fi
  if [[ -n "$SERVER_PID" ]]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -rf "$SCRATCH_DIR"
}
trap cleanup EXIT INT TERM

export BUGLER_SMOKE_GH_SECRET="smoke-github-secret"

cd "$PROJECT_DIR"
bun run src/bugler.ts serve --db "$SCRATCH_DIR/bugler.db" --port "$PORT" >"$SCRATCH_DIR/server.log" 2>&1 &
SERVER_PID=$!

for _attempt in $(seq 1 100); do
  if curl --silent --fail "$BASE_URL/healthz" >/dev/null; then
    break
  fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    cat "$SCRATCH_DIR/server.log" >&2
    exit 1
  fi
  sleep 0.05
done

ADMIN_OUTPUT=$(bun run src/bugler.ts init --db "$SCRATCH_DIR/bugler.db")
ADMIN_TOKEN=$(printf '%s\n' "$ADMIN_OUTPUT" | tail -n 1)
AUTH_HEADER="Authorization: Bearer $ADMIN_TOKEN"

curl --silent --fail-with-body -X POST "$BASE_URL/sources" \
  -H "$AUTH_HEADER" \
  -H 'Content-Type: application/json' \
  -d '{
    "id":"github-widgets",
    "kind":"http",
    "verification":{"preset":"github","secret_refs":["env:BUGLER_SMOKE_GH_SECRET"]},
    "mapping":{"preset":"github","source":"github/acme/widgets"}
  }' >"$SCRATCH_DIR/source.json"

SUBSCRIPTION_RESPONSE=$(curl --silent --fail-with-body -X POST "$BASE_URL/subscriptions" \
  -H "$AUTH_HEADER" \
  -H 'Content-Type: application/json' \
  -d '{
    "id":"pr-42-smoke",
    "name":"PR 42 smoke subscriber",
    "filter":{"all":[
      {"exact":{"source":"github/acme/widgets"}},
      {"exact":{"subject":"pr/42"}},
      {"prefix":{"type":"com.github.pull_request_review"}}
    ]},
    "transport":{"type":"pull"},
    "action":"Reconcile PR 42 from current GitHub state."
  }')
SUBSCRIBER_TOKEN=$(printf '%s' "$SUBSCRIPTION_RESPONSE" | bun -e 'const value = await new Response(Bun.stdin.stream()).json(); console.log(value.token)')

WAKE_SUBSCRIPTION_RESPONSE=$(curl --silent --fail-with-body -X POST "$BASE_URL/subscriptions" \
  -H "$AUTH_HEADER" \
  -H 'Content-Type: application/json' \
  -d '{
    "id":"pr-42-wake-smoke",
    "name":"PR 42 wake smoke subscriber",
    "filter":{"all":[
      {"exact":{"source":"github/acme/widgets"}},
      {"exact":{"subject":"pr/42"}},
      {"prefix":{"type":"com.github.pull_request_review"}}
    ]},
    "transport":{"type":"pull"},
    "action":"Reconcile PR 42 from current GitHub state."
  }')
WAKE_TOKEN=$(printf '%s' "$WAKE_SUBSCRIPTION_RESPONSE" | bun -e 'const value = await new Response(Bun.stdin.stream()).json(); console.log(value.token)')
export BUGLER_TOKEN="$WAKE_TOKEN"
export BUGLER_WAKE_RECORD="$SCRATCH_DIR/wake.json"
bun run src/bugler.ts listen --relay "$BASE_URL" --sub pr-42-wake-smoke --wait 30s --once -- \
  bun -e 'const input = await Bun.stdin.text(); await Bun.write(process.env.BUGLER_WAKE_RECORD, JSON.stringify({event: JSON.parse(input), seq: process.env.BUGLER_SEQ, id: process.env.BUGLER_EVENT_ID}));' \
  >"$SCRATCH_DIR/waker.log" 2>&1 &
WAKER_PID=$!
sleep 0.1

WEBHOOK_BODY='{
  "action":"submitted",
  "review":{"id":12345,"state":"approved","body":"Looks good","submitted_at":"2026-08-22T04:11:09Z","user":{"login":"reviewer"}},
  "pull_request":{"number":42,"title":"Ship Bugler","html_url":"https://github.com/acme/widgets/pull/42"},
  "repository":{"id":9876,"full_name":"acme/widgets"},
  "sender":{"login":"reviewer"}
}'
SIGNATURE=$(WEBHOOK_BODY="$WEBHOOK_BODY" bun -e 'import { createHmac } from "node:crypto"; console.log(createHmac("sha256", process.env.BUGLER_SMOKE_GH_SECRET).update(process.env.WEBHOOK_BODY).digest("hex"))')

curl --silent --fail-with-body -X POST "$BASE_URL/ingest/github-widgets" \
  -H 'Content-Type: application/json' \
  -H 'X-GitHub-Event: pull_request_review' \
  -H 'X-GitHub-Delivery: 72d3162e-cc78-11e3-81ab-4c9367dc0958' \
  -H "X-Hub-Signature-256: sha256=$SIGNATURE" \
  --data-binary "$WEBHOOK_BODY" >"$SCRATCH_DIR/ingest.json"

for _attempt in $(seq 1 100); do
  if [[ -f "$BUGLER_WAKE_RECORD" ]]; then
    break
  fi
  if ! kill -0 "$WAKER_PID" 2>/dev/null; then
    cat "$SCRATCH_DIR/waker.log" >&2
    exit 1
  fi
  sleep 0.05
done
if [[ ! -f "$BUGLER_WAKE_RECORD" ]]; then
  cat "$SCRATCH_DIR/waker.log" >&2
  exit 1
fi
wait "$WAKER_PID"
WAKER_PID=""
WAKE_SEQ=$(bun -e 'const value = await Bun.file(process.env.BUGLER_WAKE_RECORD).json(); if (value.event.type !== "com.github.pull_request_review.submitted" || value.event.subject !== "pr/42" || value.seq !== String(value.event.seq) || value.id !== value.event.id) process.exit(1); console.log(value.event.seq)')
WAKE_DRAIN=$(curl --silent --fail-with-body -X POST "$BASE_URL/subscriptions/pr-42-wake-smoke/events" \
  -H "Authorization: Bearer $WAKE_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"wait":0}')
printf '%s' "$WAKE_DRAIN" | WAKE_SEQ="$WAKE_SEQ" bun -e 'const value = await new Response(Bun.stdin.stream()).json(); if (value.events?.length !== 0 || Number(process.env.WAKE_SEQ) !== 1) process.exit(1)'

POLL_RESPONSE=$(curl --silent --fail-with-body -X POST "$BASE_URL/subscriptions/pr-42-smoke/events" \
  -H "Authorization: Bearer $SUBSCRIBER_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"wait":0,"limit":10}')
SEQ=$(printf '%s' "$POLL_RESPONSE" | bun -e 'const value = await new Response(Bun.stdin.stream()).json(); if (value.events?.[0]?.type !== "com.github.pull_request_review.submitted") process.exit(1); console.log(value.events[0].seq)')
EPOCH=$(printf '%s' "$POLL_RESPONSE" | bun -e 'const value = await new Response(Bun.stdin.stream()).json(); console.log(value.cursor_epoch)')

curl --silent --fail-with-body -X POST "$BASE_URL/subscriptions/pr-42-smoke/cursor" \
  -H "Authorization: Bearer $SUBSCRIBER_TOKEN" \
  -H 'Content-Type: application/json' \
  -d "{\"seq\":$SEQ,\"epoch\":$EPOCH}" >"$SCRATCH_DIR/ack.json"

HISTORY_RESPONSE=$(curl --silent --fail-with-body "$BASE_URL/events?source=github%2Facme%2Fwidgets&subject=pr%2F42&since=2026-08-22T00%3A00%3A00Z" \
  -H "Authorization: Bearer $SUBSCRIBER_TOKEN")
printf '%s' "$HISTORY_RESPONSE" | bun -e 'const value = await new Response(Bun.stdin.stream()).json(); if (value.events?.length !== 1 || value.events[0].seq !== 1) process.exit(1)'

printf 'PASS\n'
