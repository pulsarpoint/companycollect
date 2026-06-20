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

# Connection-pool overflow for the dev server on a Postgres (max_connections=100)
# shared with Temporal/PostgREST. Too high (e.g. 50) holds ~50 idle connections
# and starves the shared DB; too low (e.g. 5) starves the daemon's OWN pool
# ("QueuePool limit ... reached") because it runs many sensors + the run queue
# concurrently. 20 is a balance: enough per process, while leaving headroom on
# the shared DB (partition backfills are throttled via multi_run + the
# exchange_rates_v2_duckdb pool + concurrency.runs.max_concurrent_runs, so they
# no longer spike connections). Overridable via DAGSTER_DB_POOL_MAX_OVERFLOW.
exec uv run dg dev --db-pool-max-overflow "${DAGSTER_DB_POOL_MAX_OVERFLOW:-20}" "$@"
