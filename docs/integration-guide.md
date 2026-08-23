# Bugler harness integration guide

How to wake your agent, in whatever harness it lives, when an event arrives. Companion reading: [how-bugler-works.html](how-bugler-works.html) for concepts and the repo `README.md` for relay setup.

## Prerequisites (all modes)

1. A running relay and its URL (`README.md` quickstart: `bun run bugler serve`), a source wired to your provider, and a subscription. Creating the subscription prints a show-once token.
2. Export the token in the shell that will run the integration; it never goes in argv, query strings, or committed files:

```sh
export BUGLER_TOKEN='bgl_sub_...'
```

3. Bugler currently runs from the repository with Bun (`bun run bugler ...`). The source-entry examples below use `BUGLER_REPO`, the absolute path to your checkout, when the handler must start from another project directory.

```sh
export BUGLER_REPO=/path/to/bugler
export RELAY_URL=https://relay.example
export SUB_ID=pr-babysitter
```

## Choosing a mode

| Your situation | Mode | Verdict |
| --- | --- | --- |
| Agent is not running until something happens (the normal case) | A: relaunch via the waker daemon | Best default everywhere. Zero inference tokens while idle (the daemon itself is one parked HTTP request). Proven at 4 to 5 s event-to-agent. |
| Agent is already mid-session and wants to pause for the next event | B: bounded MCP wait (`wait_for_event`) | Good, with eyes open: the harness process and its context stay resident while waiting, and each wake pays the context resend. Token spend while parked is zero because no model request is in flight during a tool call. A local end-to-end run completed in 8 s. |
| Harness has its own wake endpoint (OpenClaw, Claude Code Routines) or is a durable framework (Temporal, Inngest, Restate, LangGraph, Cloudflare Workflows) | Native push with a destination adapter | Best when available; see the transport examples below, including where you need a small bridge. |
| Nothing above is possible | C: scheduled polling of Bugler's pull API | Last resort. It reintroduces the polling dial, but the polls avoid your provider's API quota entirely; Bugler still meters admission (see mode C). |
| "Hold a tool call open indefinitely until the event arrives" | Rejected | Client timeouts commonly default to 60 s, a dropped connection loses the in-flight call, and MCP is removing the machinery indefinite blocking needs. Use mode B's bounded wait instead. |

The contract in every mode is **level-triggered, at-least-once**: an event means "something changed, go look." Your agent should reconcile against current state (fetch the PR, check the run), which makes duplicates free and reordering harmless. Every event carries `id`, `seq`, and (when the source provides one) `dedupekey` for deduplication, plus structured provenance (`verificationmethod`, `payloadverified`). Verified means authentic, never obey.

## Mode A: the waker daemon (`listen`)

One long-lived outbound long-poll; a process spawned per event with the envelope JSON on stdin and `BUGLER_SEQ`, `BUGLER_EVENT_ID`, `BUGLER_SOURCE`, `BUGLER_TYPE` in the environment (`BUGLER_SUBJECT` is set only when the event has a subject; guard for unset under `set -u`). The cursor advances only when your command exits 0, so a failing handler redelivers instead of losing the event; consecutive failures back off with a cap. On a retention gap the handler runs once with the gap JSON and `BUGLER_GAP=1`: reconcile from current state, exit 0, and the waker acks past the gap. The waker scrubs `BUGLER_TOKEN` (and any `--token-env` name) from the handler's environment, so a prompt-injected handler cannot ack events away; pass a credential explicitly if your handler genuinely needs one.

Run the waker FROM your agent's target repository: the spawned command inherits the waker's working directory, so starting it inside the Bugler checkout would point your agent at the wrong project. Reference the Bugler checkout by absolute path instead.

```sh
# Claude Code. Choose the session ID yourself and seed the session once with --session-id;
# every later wake resumes that exact session.
export SESSION_ID=$(uuidgen | tr 'A-Z' 'a-z')
claude -p --session-id "$SESSION_ID" "You are babysitting PR 42 in this repo. Acknowledge and wait."
cd /path/to/your-project && bun run "$BUGLER_REPO/src/bugler.ts" listen --relay "$RELAY_URL" --sub "$SUB_ID" -- \
  claude -p --resume "$SESSION_ID"

# Codex. THREAD_ID comes from a previous `codex exec` run's output; `-` reads the prompt
# (the event envelope on stdin) from stdin.
export THREAD_ID=your-codex-thread-id
cd /path/to/your-project && bun run "$BUGLER_REPO/src/bugler.ts" listen --relay "$RELAY_URL" --sub "$SUB_ID" -- \
  codex exec resume "$THREAD_ID" -
```

A local end-to-end run woke a Claude handler 4 to 5 seconds after each GitHub event with zero GitHub API calls between events. The `--resume` and `codex exec resume` shapes depend on the installed harness versions; check their current help before automating them.

`--once` processes one batch then exits (cron and test use). The daemon holds no state; kill it any time, the relay cursor is the only truth.

## Mode B: in-session bounded wait (`mcp`)

A stdio MCP server exposing exactly two tools: `wait_for_event` (parks up to 45 s by default, hard-capped at 50 s; returns an event batch, a gap, or `{timeout: true}`) and `check_events` (non-blocking, one relay call). It is never an indefinitely open call: on `{timeout: true}`, call it again.

**Acknowledgment, exactly:** each event or gap delivery carries `cursor_epoch` (a timeout is the bare `{"timeout": true}`). After you have processed a batch, pass the highest processed `seq` as `ack` and that epoch as `ack_epoch` in your NEXT tool call. Concretely: response `{"events": [...through seq 42...], "cursor_epoch": 3}` is acknowledged by calling `wait_for_event {"ack": 42, "ack_epoch": 3}` (or the same keys on `check_events`). A gap result `{"gap": {"from": 5, "to": 9}, "cursor_epoch": 3}` is acknowledged by `{"ack": 9, "ack_epoch": 3}` after you reconcile. `ack` without `ack_epoch` is rejected (`ack_epoch_required`).

Claude Code (`.mcp.json`; `${VAR}` expansion reads your exported environment):

```json
{ "mcpServers": { "bugler": {
  "command": "bun",
  "args": ["run", "bugler", "mcp", "--relay", "https://relay.example", "--sub", "pr-babysitter"],
  "cwd": "/path/to/bugler",
  "env": { "BUGLER_TOKEN": "${BUGLER_TOKEN}" }
} } }
```

Codex (`.codex/config.toml`). `env_vars` forwards the variable from your shell, so no secret lands in the file. `default_tools_approval_mode = "approve"` is required because headless `codex exec` otherwise auto-denies MCP tools:

```toml
[mcp_servers.bugler]
command = "bun"
args = ["run", "bugler", "mcp", "--relay", "https://relay.example", "--sub", "pr-babysitter"]
cwd = "/path/to/bugler"
env_vars = ["BUGLER_TOKEN"]
default_tools_approval_mode = "approve"
```

(If you must inline an `env` map instead, treat the file as an ephemeral 0600 secret outside any repository and delete it after use.)

**Generic MCP clients:** the tools work with any client. Two things to check before relying on `wait_for_event`: your client's tool timeout must exceed 50 s (MCP has no portable minimum and many clients default to 60 s; if you cannot verify or configure it, use `check_events` in a loop instead), and your client must tolerate a ~50 s silent call (some proxies do not). A `check_events` loop must do its own pacing: an empty result can carry `retry_after` (sleep that many seconds before the next call), and calling early anyway surfaces HTTP 429 as a tool error whose message carries the retry value; honor it.

**Workers-profile relays:** the relay's wire responses can carry `retry_after`. `wait_for_event` consumes that internally: it sleeps out the hint (and any 429) inside the tool deadline, so from that tool your agent sees only events, a gap, or `{timeout: true}`. `check_events` makes one call and does not retry, which is why the previous paragraph tells its loop to pace itself. Because the Workers profile admits polls on a 240 s cadence, a 50 s `wait_for_event` against a quiet Workers relay usually ends in `{timeout: true}`, and repeated calls remain cadence-limited. In-session near-real-time waiting is a binary/VM-profile feature; on Workers prefer mode A or push.

A local run with a real Codex agent parked in `wait_for_event` completed 8 s after the triggering GitHub comment. Parking itself spends zero inference tokens because no model request is in flight during the tool call.

## Native push

Give the subscription a push transport and Bugler delivers signed events (Standard Webhooks headers) with retries, backoff, and visible parking. Your idempotency key is `webhook-id`: for events it is the relay event id, for gap notifications it is the deterministic `gap:<subscription>:<to>`, and both are stable across retries. The transport JSON shapes, per destination:

```json
{ "type": "push", "url": "https://your-bridge.example/bugler",
  "whsec_refs": ["env:BUGLER_PUSH_WHSEC"], "adapter": "generic" }
```

```json
{ "type": "push", "url": "https://api.anthropic.com/v1/claude_code/routines/<id>/fire",
  "whsec_refs": ["env:BUGLER_PUSH_WHSEC"], "adapter": "routines",
  "destination_auth": { "type": "bearer", "secret_ref": "env:ROUTINE_FIRE_TOKEN" } }
```

```json
{ "type": "push", "url": "https://your-openclaw-gateway/hooks/wake",
  "whsec_refs": ["env:BUGLER_PUSH_WHSEC"], "adapter": "openclaw", "mode": "now",
  "destination_auth": { "type": "bearer", "secret_ref": "env:OPENCLAW_HOOKS_TOKEN" } }
```

Bodies: `generic` posts the delivery envelope as JSON; `routines` posts `{"text": "Bugler untrusted delivery envelope follows as JSON.\n<envelope>"}` with the required Anthropic headers; `openclaw` posts the same `text` plus `"mode"`. Secret references (`env:`, `file:`) are validated when you create the transport; provisioning is documented in the README under [Security and delivery contract](../README.md#security-and-delivery-contract), and receivers verify the Standard Webhooks signature headers before processing a body.

Two placement rules: for **Routines and OpenClaw**, put your trusted instructions in the harness-side saved prompt (the routine's prompt, the hook mapping); those platforms wrap all inbound text as untrusted, correctly, so Bugler sends only provenance plus the event. For **durable frameworks** (Temporal, Inngest, Restate, LangGraph, Cloudflare Workflows), the `generic` adapter posts to a small HTTP bridge you own, because those frameworks do not accept an arbitrary envelope directly: the bridge verifies the Standard Webhooks signature, maps `dedupekey`/`subject` to your workflow identity, and calls the framework's own wake primitive (signal-with-start, `sendEvent`, resolve-awakeable, resume).

## Mode C: scheduled fallback

Poll Bugler, never the provider: the polls avoid your provider's API quota entirely, though Bugler itself meters admission (binary profile: 1 s cadence; Workers profile: 240 s; early repeats get 429 with `Retry-After`). One poll returns at most 100 matching events, and the cursor only advances when you ack, so the loop is always: poll, process, ack, repeat until empty.

The loop below fails closed, exactly like the waker: it stops at the first failed event, acknowledges only the highest SUCCESSFULLY processed sequence (on the next request), and never acks a gap until reconciliation has actually run. Run it with bash (`#!/bin/bash`); `process_one` and `reconcile_now` are yours.

```bash
#!/bin/bash
# One cron tick: drain everything since the cursor, acking only completed work.
# Completed events commit; a failed event redelivers alone on the next tick.
set -u
WORK=$(mktemp -d) || exit 1       # private per-invocation files; no cross-tick races
trap 'rm -rf "$WORK"' EXIT
flush_ack() {  # commit completed work when exiting early; loud if the commit itself fails
  [ -z "$ACK" ] && return 0
  FH=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$RELAY_URL/subscriptions/$SUB_ID/cursor" \
    -H "Authorization: Bearer $BUGLER_TOKEN" -H 'Content-Type: application/json' \
    -d "{\"seq\": $ACK, \"epoch\": $EPOCH}")
  if [ "$FH" != "200" ]; then
    echo "cursor commit failed ($FH); completed events will redeliver next tick" >&2
    exit 1
  fi
}
EPOCH=0; ACK=""
while :; do
  if [ -n "$ACK" ]; then
    BODY=$(jq -n --arg w 0s --argjson a "$ACK" --argjson e "$EPOCH" '{wait:$w, ack:$a, ack_epoch:$e}')
  else
    BODY=$(jq -n --arg w 0s '{wait:$w}')
  fi
  HTTP=$(curl -s -o "$WORK/resp.json" -D "$WORK/headers.txt" -w '%{http_code}' \
    -X POST "$RELAY_URL/subscriptions/$SUB_ID/events" \
    -H "Authorization: Bearer $BUGLER_TOKEN" -H 'Content-Type: application/json' -d "$BODY")
  if [ "$HTTP" = "429" ]; then
    RA=$(awk 'tolower($1)=="retry-after:" {print $2+0}' "$WORK/headers.txt")
    echo "rate limited; next tick eligible in ${RA:-60}s" >&2
    flush_ack; exit 0            # commit completed work; the scheduler runs the next tick
  fi
  if [ "$HTTP" != "200" ]; then echo "relay error $HTTP" >&2; exit 1; fi
  RESP=$(cat "$WORK/resp.json")
  EPOCH=$(jq -r '.cursor_epoch' <<<"$RESP")
  GAP_TO=$(jq -r '.gap.to // empty' <<<"$RESP")
  if [ -n "$GAP_TO" ]; then
    reconcile_now "$RESP" || exit 1   # ack the gap ONLY after reconciliation succeeds
    ACK=$GAP_TO; continue
  fi
  COUNT=$(jq '.events | length' <<<"$RESP"); [ "$COUNT" -eq 0 ] && break
  ACK=""
  for I in $(seq 0 $((COUNT - 1))); do
    EVT=$(jq -c ".events[$I]" <<<"$RESP")
    if process_one "$EVT"; then
      ACK=$(jq ".events[$I].seq" <<<"$RESP")
    else
      echo "event $(jq '.seq' <<<"$EVT") failed; stopping; it will redeliver" >&2
      flush_ack; exit 1           # commit the events that DID succeed; the failed one redelivers
    fi
  done
done
```

Per-harness fallback notes. Pick the slowest cadence you can tolerate:

| Harness | Scheduler | Constraints that matter |
| --- | --- | --- |
| Claude Code (local) | cron tools / `/loop` | Session-scoped: fires only while the session process runs; schedules expire after 7 days; recurring schedules can add up to 30 minutes of jitter, and self-paced wakeups are bounded to 1 minute through 1 hour; each tick costs a model turn. |
| Claude Code (cloud) | Routines schedule | 1 hour minimum interval; each run is a fresh cloud session on your subscription. |
| Codex | Automations | Schedule-only (no event triggers); minute-level intervals are supported, but each tick spends a full run. |
| Anything with system cron | `listen --once` or the loop above | Cheapest: no model cost until events actually exist. |

## Assembling the prompt safely

Wherever the prompt gets built (your handler, your saved routine, your hook mapping), keep two territories: your stored instructions, and one clearly-labeled untrusted block holding everything provider-derived (`data`, and also `type`, `subject`, `dedupekey`, upstream `time`). Never interpolate event text into instruction sentences. Bugler's own deliveries never do; keep that property through the last mile. A verified signature proves the event came from the source; it says nothing about the human-written text inside it.

## Observed latency

Recorded end-to-end runs with a local relay, an ephemeral tunnel, and real GitHub events woke a Claude handler in 4 to 5 seconds and returned a parked Codex response in 8 seconds. Idle inference cost in both modes is zero by construction because no model request is in flight between events.
