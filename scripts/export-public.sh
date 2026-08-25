#!/usr/bin/env bash
set -euo pipefail

# Public export exclusions:
# CHARTER.md, KICKOFF.md, PLAN.md, STATUS.md, design/, research/, reviews/,
# verification/, orchestration/, prompts/, platform/, hosted-only deploy/README.md,
# and site/drafts/. site/index.html is kept.

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
  "$STAGING/deploy/README.md" \
  "$STAGING/site/drafts"

# Keep hosted authentication dependencies and test commands in the private tree.
python3 - "$STAGING/package.json" <<'PY'
import json
import pathlib
import sys

manifest_path = pathlib.Path(sys.argv[1])
manifest = json.loads(manifest_path.read_text())
hosted_only = {
    "dependencies": {"@clerk/backend"},
    "devDependencies": {"jose"},
}
for section, names in hosted_only.items():
    values = manifest.get(section, {})
    for name in names:
        values.pop(name, None)

scripts = manifest.get("scripts", {})
scripts.pop("test:platform", None)
if "test" in scripts:
    scripts["test"] = " && ".join(
        command for command in scripts["test"].split(" && ")
        if command != "bun run test:platform"
    )

manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
PY

# The private lockfile includes hosted-only dependencies removed above. Rebuild
# it from the exported manifest, then prove the public CI frozen install works.
(
  cd "$STAGING"
  bun install --lockfile-only --ignore-scripts
  if [[ ${BUGLER_EXPORT_SKIP_FROZEN_CHECK:-0} != 1 ]]; then
    bun install --frozen-lockfile --ignore-scripts
  fi
)
rm -rf "$STAGING/node_modules"

# Keep the public Worker warning useful without naming the excluded hosted tree.
sed -i.bak 's|# belong exclusively.*|# are intentionally not claimed by this self-hosted Worker.|' "$STAGING/wrangler.toml"
rm -f "$STAGING/wrangler.toml.bak"

INTERNAL_PATHS=(
  CHARTER.md KICKOFF.md PLAN.md STATUS.md design research reviews verification
  orchestration prompts platform deploy/README.md site/drafts
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
python3 - package.json <<'PY'
import json
import pathlib
import re
import sys

root = pathlib.Path(".")
manifest = json.loads(pathlib.Path(sys.argv[1]).read_text())
declared = set()
for section in ("dependencies", "devDependencies", "peerDependencies", "optionalDependencies"):
    declared.update(manifest.get(section, {}))

patterns = (
    re.compile(r'''\b(?:import|export)\s+(?:type\s+)?(?:[^;"']*?\s+from\s+)?["']([^"']+)["']'''),
    re.compile(r'''\bimport\s*\(\s*["']([^"']+)["']\s*\)'''),
    re.compile(r'''\brequire\s*\(\s*["']([^"']+)["']\s*\)'''),
)
extensions = {".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs"}
missing = []

for path in sorted(root.rglob("*")):
    if not path.is_file() or path.suffix not in extensions or ".git" in path.parts:
        continue
    source = path.read_text(errors="replace")
    for pattern in patterns:
        for match in pattern.finditer(source):
            specifier = match.group(1)
            if specifier.startswith((".", "/", "#")) or ":" in specifier.split("/", 1)[0]:
                continue
            parts = specifier.split("/")
            package = "/".join(parts[:2]) if specifier.startswith("@") else parts[0]
            if package not in declared:
                line = source.count("\n", 0, match.start()) + 1
                missing.append(f"{path.as_posix()}:{line}: {specifier}")

if missing:
    print("Exported source imports a dependency absent from package.json:", file=sys.stderr)
    for item in sorted(set(missing)):
        print(item, file=sys.stderr)
    raise SystemExit(1)
PY

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

if grep -RInE '/Users/|gh[ops]_[A-Za-z0-9_]+|github_pat_[A-Za-z0-9_]+|(^|[^A-Za-z0-9_.-])[A-Za-z0-9-]+\.local([^A-Za-z0-9_-]|$)|AKIA[0-9A-Z]{16}' . --exclude-dir=.git --exclude=export-public.sh; then
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
