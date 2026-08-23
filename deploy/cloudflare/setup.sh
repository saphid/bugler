#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
DRY_RUN=0

usage() {
  printf 'Usage: %s [--dry-run]\n' "$0"
}

while (($# > 0)); do
  case "$1" in
    --dry-run) DRY_RUN=1 ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'Unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
  shift
done

if ((DRY_RUN)); then
  printf '%s\n' \
    'DRY RUN: bunx wrangler whoami' \
    'DRY RUN: bunx wrangler types validates the BUGLER Durable Object binding' \
    'DRY RUN: bunx wrangler deploy --dry-run validates the SQLite migration' \
    'DRY RUN: bunx wrangler deploy creates or updates the Worker before secrets' \
    'DRY RUN: prompt securely for BUGLER_BOOTSTRAP_ADMIN_TOKEN' \
    'DRY RUN: bunx wrangler secret put BUGLER_BOOTSTRAP_ADMIN_TOKEN via stdin' \
    'DRY RUN: prompt securely for BUGLER_GITHUB_SECRET' \
    'DRY RUN: bunx wrangler secret put BUGLER_GITHUB_SECRET via stdin' \
    'DRY RUN: bunx wrangler deploy activates the supplied secrets' \
    'DRY RUN: print the workers.dev ingest URL and this first-source request:' \
    "POST /sources with Authorization: Bearer \$BUGLER_ADMIN_TOKEN and secret reference env:BUGLER_GITHUB_SECRET"
  exit 0
fi

cd "$PROJECT_DIR"
command -v bun >/dev/null 2>&1 || { printf 'bun is required\n' >&2; exit 1; }
bunx wrangler whoami >/dev/null

VALIDATION_DIR=$(mktemp -d)
BINDINGS_FILE="$VALIDATION_DIR/bindings.d.ts"
trap 'rm -f "$BINDINGS_FILE"; rmdir "$VALIDATION_DIR"' EXIT
bunx wrangler types "$BINDINGS_FILE" --env-interface WranglerBindings --include-runtime false >/dev/null
if ! BINDINGS_FILE="$BINDINGS_FILE" bun -e '
  const contents = await Bun.file(Bun.env.BINDINGS_FILE).text();
  if (!/BUGLER:\s*DurableObjectNamespace<[^>]*BuglerDurableObject/.test(contents)) process.exit(1);
'; then
  printf 'Wrangler did not resolve BUGLER as the BuglerDurableObject binding\n' >&2
  exit 1
fi
bunx wrangler deploy --dry-run >/dev/null

INITIAL_DEPLOY_OUTPUT=$(bunx wrangler deploy)
printf '%s\n' "$INITIAL_DEPLOY_OUTPUT"

put_secret() {
  local name=$1
  local value
  printf 'Enter %s. Input is hidden: ' "$name" >&2
  IFS= read -r -s value
  printf '\n' >&2
  if [[ -z "$value" ]]; then
    printf '%s cannot be empty\n' "$name" >&2
    exit 1
  fi
  if [[ "$name" == "BUGLER_BOOTSTRAP_ADMIN_TOKEN" && ! "$value" =~ ^bgl_adm_ ]]; then
    printf '%s must be a Bugler bgl_adm_ token\n' "$name" >&2
    exit 1
  fi
  printf '%s' "$value" | bunx wrangler secret put "$name"
  unset value
}

put_secret BUGLER_BOOTSTRAP_ADMIN_TOKEN
put_secret BUGLER_GITHUB_SECRET

DEPLOY_OUTPUT=$(bunx wrangler deploy)
printf '%s\n' "$DEPLOY_OUTPUT"
WORKER_URL=$(printf '%s\n' "$DEPLOY_OUTPUT" | grep -Eo 'https://[^[:space:]]+\.workers\.dev' | tail -n 1 || true)
if [[ -z "$WORKER_URL" ]]; then
  WORKER_URL='https://<your-worker>.workers.dev'
fi

printf '\nIngest URL: %s/ingest/github-example\n' "$WORKER_URL"
printf '%s\n' 'Create the first source after exporting the admin token you supplied:'
cat <<EOF
curl --fail-with-body -X POST '$WORKER_URL/sources' \\
  -H "Authorization: Bearer \$BUGLER_ADMIN_TOKEN" \\
  -H 'Content-Type: application/json' \\
  -d '{"id":"github-example","kind":"http","verification":{"preset":"github","secret_refs":["env:BUGLER_GITHUB_SECRET"]},"mapping":{"preset":"github","source":"github/owner/repo"}}'
EOF
