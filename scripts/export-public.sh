#!/usr/bin/env bash
set -euo pipefail

# Public export exclusions:
# CHARTER.md, KICKOFF.md, PLAN.md, STATUS.md, design/, research/, reviews/,
# verification/, orchestration/, prompts/, platform/, and site/drafts/. site/index.html is kept.

PROJECT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
DESTINATION=${BUGLER_EXPORT_DIR:-/tmp/bugler-public}

case "$DESTINATION" in
  /*bugler-public*) ;;
  *) printf 'Refusing unsafe export destination: %s\n' "$DESTINATION" >&2; exit 1 ;;
esac

STAGING=$(mktemp -d "${TMPDIR:-/tmp}/bugler-public-stage.XXXXXX")
cleanup() {
  rm -rf "$STAGING"
}
trap cleanup EXIT INT TERM

cd "$PROJECT_DIR"
git archive HEAD | tar -x -C "$STAGING"

rm -rf \
  "$STAGING/CHARTER.md" \
  "$STAGING/KICKOFF.md" \
  "$STAGING/PLAN.md" \
  "$STAGING/STATUS.md" \
  "$STAGING/design" \
  "$STAGING/research" \
  "$STAGING/reviews" \
  "$STAGING/verification" \
  "$STAGING/orchestration" \
  "$STAGING/prompts" \
  "$STAGING/platform" \
  "$STAGING/site/drafts"

INTERNAL_PATHS=(
  CHARTER.md KICKOFF.md PLAN.md STATUS.md design research reviews verification
  orchestration prompts platform site/drafts
)
for path in "${INTERNAL_PATHS[@]}"; do
  if [[ -e "$STAGING/$path" ]]; then
    printf 'Internal path escaped export filtering: %s\n' "$path" >&2
    exit 1
  fi
done
if [[ ! -f "$STAGING/site/index.html" ]]; then
  printf '%s\n' 'site/index.html is missing from the public export' >&2
  exit 1
fi

cd "$STAGING"
if find . -path ./.git -prune -o -iname '*reveille*' -print | grep -q .; then
  printf '%s\n' 'Legacy product name remains in a public path:' >&2
  find . -path ./.git -prune -o -iname '*reveille*' -print >&2
  exit 1
fi

LEGACY_MATCHES=$(grep -RInE 'reveille|rvl_' . \
  --exclude-dir=.git \
  --exclude='*.db' || true)
if [[ -n "$LEGACY_MATCHES" ]]; then
  UNEXPECTED_LEGACY=$(printf '%s\n' "$LEGACY_MATCHES" | grep -Ev '^\./(AUTH\.md|README\.md|IMPLEMENTATION-NOTES\.md|deploy/docker/entrypoint\.sh|deploy/gcp/setup-vm\.sh|scripts/export-public\.sh|src/auth\.ts|src/environment\.ts|src/listen\.ts|src/workers/index\.ts|tests/core\.test\.ts|tests/deploy\.test\.ts|tests/wake\.test\.ts|tests/workers/profile\.test\.ts|wrangler\.toml):' || true)
  if [[ -n "$UNEXPECTED_LEGACY" ]]; then
    printf '%s\n' 'Legacy name remains in a product-naming position:' >&2
    printf '%s\n' "$UNEXPECTED_LEGACY" >&2
    exit 1
  fi
fi

if grep -RInE '/Users/|gh[ops]_[A-Za-z0-9_]+|github_pat_[A-Za-z0-9_]+|[A-Za-z0-9-]+\.local([^A-Za-z0-9-]|$)|AKIA[0-9A-Z]{16}' . --exclude-dir=.git --exclude=export-public.sh; then
  printf '%s\n' 'A secret-like value, local hostname, or machine-specific path remains in the public tree' >&2
  exit 1
fi
if grep -RInE '(bgl|rvl)_(adm|sub)_[A-Za-z0-9_-]{20,}_[a-f0-9]{8}' . --exclude-dir=.git; then
  printf '%s\n' 'A complete Bugler token remains in the public tree' >&2
  exit 1
fi

git init -q
git add --all
git -c user.name='Bugler' -c user.email='hello@bugler.dev' commit -q -m 'Bugler v0.1.0'

if [[ -e "$DESTINATION" ]]; then
  rm -rf "$DESTINATION"
fi
mkdir -p "$(dirname "$DESTINATION")"
mv "$STAGING" "$DESTINATION"
trap - EXIT INT TERM

printf 'Public tree created at %s\n' "$DESTINATION"
(
  cd "$DESTINATION"
  find . -path ./.git -prune -o -print | sort
)
