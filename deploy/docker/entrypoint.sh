#!/usr/bin/env sh
set -eu

LEGACY_USED=0
LEGACY_DATABASE=0
if [ -n "${BUGLER_DB:-}" ]; then
  DATABASE=$BUGLER_DB
elif [ -n "${REVEILLE_DB:-}" ]; then
  LEGACY_USED=1
  DATABASE=$REVEILLE_DB
elif [ -f /data/reveille.db ]; then
  DATABASE=/data/reveille.db
  LEGACY_DATABASE=1
else
  DATABASE=/data/bugler.db
fi
if [ -n "${BUGLER_STORAGE:-}" ]; then
  STORAGE=$BUGLER_STORAGE
elif [ -n "${REVEILLE_STORAGE:-}" ]; then
  LEGACY_USED=1
  STORAGE=$REVEILLE_STORAGE
else
  STORAGE=sqlite
fi
if [ "$LEGACY_USED" -eq 1 ]; then
  printf '%s\n' 'BUGLER: REVEILLE_* environment variables are deprecated; use BUGLER_* instead.' >&2
fi
if [ "$LEGACY_DATABASE" -eq 1 ]; then
  printf '%s\n' 'Using legacy database /data/reveille.db for upgrade continuity.' >&2
else
  printf 'Using database %s.\n' "$DATABASE" >&2
fi
if [ "$STORAGE" = "sqlite" ] && [ ! -f "$DATABASE" ]; then
  /usr/local/bin/bugler init --db "$DATABASE"
fi
exec /usr/local/bin/bugler serve --db "$DATABASE" --host 0.0.0.0 --port "${PORT:-8787}"
