#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
BODY="$PROJECT_DIR/docs/_body.html"
DOCUMENT="$PROJECT_DIR/docs/how-bugler-works.html"
TEMP_FILE=$(mktemp "${TMPDIR:-/tmp}/bugler-docs.XXXXXX")
trap 'rm -f "$TEMP_FILE"' EXIT

awk '{ print } /<article class="document-flow">/ { exit }' "$DOCUMENT" >"$TEMP_FILE"
sed -n '1,$p' "$BODY" >>"$TEMP_FILE"
awk 'found || /<\/article>/ { found=1; print }' "$DOCUMENT" >>"$TEMP_FILE"
mv "$TEMP_FILE" "$DOCUMENT"
trap - EXIT

printf 'Built %s from %s\n' "$DOCUMENT" "$BODY"
