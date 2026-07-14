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
  export DAGSTER_PG_URL='postgresql://DAGSTER_USER:DAGSTER_PASSWORD@companycollect:6432/dagster'
  ./scripts/dagster-dev.sh
EOF
  exit 1
fi

export DAGSTER_HOME="${DAGSTER_HOME:-$PWD}"

# Client-side connection-pool overflow for the dev server. Postgres now allows
# 600 connections, so the daemon/webserver can have a comfortable per-process
# pool: too low (e.g. 5-20) starves the daemon's OWN pool ("QueuePool limit ...
# reached") because it runs many sensors + the run queue concurrently. 100 gives
# plenty of headroom; with max_connections=600 the idle connections are
# negligible. Overridable via DAGSTER_DB_POOL_MAX_OVERFLOW.
exec uv run dg dev --db-pool-max-overflow "${DAGSTER_DB_POOL_MAX_OVERFLOW:-100}" "$@"
