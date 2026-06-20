#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

if [[ -z "${DAGSTER_PG_URL:-}" && -f ".env" ]]; then
  DAGSTER_PG_URL="$(grep -m 1 '^DAGSTER_PG_URL=' .env | cut -d '=' -f 2- || true)"
  export DAGSTER_PG_URL
fi

if [[ -z "${DAGSTER_PG_URL:-}" ]]; then
  cat >&2 <<'EOF'
DAGSTER_PG_URL is required.

Example:
  export DAGSTER_PG_URL='postgresql://dagster:dagster@companycollect:5432/dagster_v3'
  ./scripts/dagster-dev.sh
EOF
  exit 1
fi

export DAGSTER_HOME="${DAGSTER_HOME:-$PWD}"

# Keep Dagster's connection pool small: this Postgres (max_connections=100) is
# shared with Temporal and PostgREST. The previous overflow of 50 left ~50 idle
# connections held by the dev server, so backfills (which open a burst of
# event-log connections) hit "remaining connection slots reserved for SUPERUSER".
# Overridable via DAGSTER_DB_POOL_MAX_OVERFLOW.
exec uv run dg dev --db-pool-max-overflow "${DAGSTER_DB_POOL_MAX_OVERFLOW:-5}" "$@"
