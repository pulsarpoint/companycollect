#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

# Shared CommonCrawl config (single source of truth for COMMONCRAWL_EMBED_* + CLICKHOUSE_*, also
# read by commoncrawl/cc-enrich-worker) — so the reference-embedding assets and the Go worker
# always hit the same served model. Keep these vars in commoncrawl/.env, not dagster_v3/.env.
if [[ -f "../commoncrawl/.env" ]]; then
  set -a; source "../commoncrawl/.env"; set +a
fi

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

# Client-side connection-pool overflow for the dev server. Postgres now allows
# 600 connections, so the daemon/webserver can have a comfortable per-process
# pool: too low (e.g. 5-20) starves the daemon's OWN pool ("QueuePool limit ...
# reached") because it runs many sensors + the run queue concurrently. 100 gives
# plenty of headroom; with max_connections=600 the idle connections are
# negligible. Overridable via DAGSTER_DB_POOL_MAX_OVERFLOW.
exec uv run dg dev --db-pool-max-overflow "${DAGSTER_DB_POOL_MAX_OVERFLOW:-100}" "$@"
