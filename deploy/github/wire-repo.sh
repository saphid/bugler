#!/usr/bin/env bash
set -euo pipefail

DRY_RUN=0
YES=0
POSITIONAL=()

usage() {
  printf 'Usage: %s OWNER/REPO RELAY_INGEST_URL [--yes] [--dry-run]\n' "$0"
}

while (($# > 0)); do
  case "$1" in
    --dry-run) DRY_RUN=1 ;;
    --yes) YES=1 ;;
    -h|--help) usage; exit 0 ;;
    *) POSITIONAL+=("$1") ;;
  esac
  shift
done

if ((${#POSITIONAL[@]} != 2)); then
  usage >&2
  exit 2
fi
REPOSITORY=${POSITIONAL[0]}
INGEST_URL=${POSITIONAL[1]}
if [[ ! "$REPOSITORY" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]]; then
  printf 'Repository must be owner/repo\n' >&2
  exit 2
fi
if [[ "$INGEST_URL" != https://* || "$INGEST_URL" == *$'\n'* || "$INGEST_URL" == *\"* || "$INGEST_URL" == *\\* ]]; then
  printf 'Relay ingest URL must be a safe https URL on one line\n' >&2
  exit 2
fi
if ((!DRY_RUN && !YES)); then
  printf '%s\n' '--yes is required because this command changes a GitHub repository webhook' >&2
  exit 2
fi

SOURCE_ID=${INGEST_URL##*/ingest/}
if [[ "$SOURCE_ID" == "$INGEST_URL" || ! "$SOURCE_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]]; then
  printf 'Relay ingest URL must end in /ingest/SOURCE_ID\n' >&2
  exit 2
fi
EVENTS='pull_request,pull_request_review,check_run,issue_comment'

if ((DRY_RUN)); then
  printf '%s\n' \
    "DRY RUN: gh api GET repos/$REPOSITORY/hooks" \
    "DRY RUN: create or update webhook with events: $EVENTS" \
    'DRY RUN: generate one webhook secret, send it only through gh api stdin, and print it once' \
    'DRY RUN: matching Bugler source uses secret reference env:BUGLER_GITHUB_SECRET' \
    "DRY RUN: source id $SOURCE_ID maps to github/$REPOSITORY"
  exit 0
fi

command -v gh >/dev/null 2>&1 || { printf 'gh is required\n' >&2; exit 1; }
command -v openssl >/dev/null 2>&1 || { printf 'openssl is required\n' >&2; exit 1; }
command -v bun >/dev/null 2>&1 || { printf 'bun is required\n' >&2; exit 1; }
gh auth status >/dev/null

HOOK_ID=$(HOOK_URL="$INGEST_URL" gh api "repos/$REPOSITORY/hooks" --paginate | bun -e '
  const hooks = await new Response(Bun.stdin.stream()).json();
  const hook = hooks.find((candidate) => candidate?.config?.url === process.env.HOOK_URL);
  if (hook) console.log(hook.id);
')
WEBHOOK_SECRET=$(openssl rand -hex 32)
PAYLOAD=$(printf '{"name":"web","active":true,"events":["pull_request","pull_request_review","check_run","issue_comment"],"config":{"url":"%s","content_type":"json","insecure_ssl":"0","secret":"%s"}}' "$INGEST_URL" "$WEBHOOK_SECRET")
if [[ -n "$HOOK_ID" ]]; then
  printf '%s' "$PAYLOAD" | gh api --method PATCH "repos/$REPOSITORY/hooks/$HOOK_ID" --input - >/dev/null
  printf 'Updated webhook %s for %s.\n' "$HOOK_ID" "$REPOSITORY"
else
  printf '%s' "$PAYLOAD" | gh api --method POST "repos/$REPOSITORY/hooks" --input - >/dev/null
  printf 'Created webhook for %s.\n' "$REPOSITORY"
fi
unset PAYLOAD

printf 'Webhook secret, shown once: %s\n' "$WEBHOOK_SECRET"
unset WEBHOOK_SECRET
printf '%s\n' 'Store that value in the relay credential named BUGLER_GITHUB_SECRET, then create the matching source:'
cat <<EOF
curl --fail-with-body -X POST "\${BUGLER_RELAY_URL:?set BUGLER_RELAY_URL}/sources" \\
  -H "Authorization: Bearer \${BUGLER_ADMIN_TOKEN:?set BUGLER_ADMIN_TOKEN}" \\
  -H 'Content-Type: application/json' \\
  -d '{"id":"$SOURCE_ID","kind":"http","verification":{"preset":"github","secret_refs":["env:BUGLER_GITHUB_SECRET"]},"mapping":{"preset":"github","source":"github/$REPOSITORY"}}'
EOF
