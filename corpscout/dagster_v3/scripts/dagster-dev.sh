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

exec uv run dg dev --db-pool-max-overflow 50 "$@"
