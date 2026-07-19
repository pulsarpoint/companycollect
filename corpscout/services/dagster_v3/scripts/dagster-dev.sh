#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

readonly DAGSTER_LOCAL_POSTGRES_COMPOSE="$PWD/docker-compose.local.yml"
readonly DAGSTER_LOCAL_PG_URL="postgresql://dagster_local:dagster_local@127.0.0.1:55432/dagster_local"

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker is required for the isolated local Dagster metadata database." >&2
  exit 1
fi

docker compose -f "$DAGSTER_LOCAL_POSTGRES_COMPOSE" up -d --wait

# Never inherit the server metadata URL into a local daemon. A local dg dev
# process attached to the server queue can claim and execute server-submitted
# runs with workstation paths and local DuckDB files.
export DAGSTER_PG_URL="$DAGSTER_LOCAL_PG_URL"

export DAGSTER_HOME="${DAGSTER_HOME:-$PWD}"

# Client-side connection-pool overflow for the dev server. Postgres now allows
# 600 connections, so the daemon/webserver can have a comfortable per-process
# pool: too low (e.g. 5-20) starves the daemon's OWN pool ("QueuePool limit ...
# reached") because it runs many sensors + the run queue concurrently. 100 gives
# plenty of headroom; with max_connections=600 the idle connections are
# negligible. Overridable via DAGSTER_DB_POOL_MAX_OVERFLOW.
exec uv run dg dev --db-pool-max-overflow "${DAGSTER_DB_POOL_MAX_OVERFLOW:-100}" "$@"
