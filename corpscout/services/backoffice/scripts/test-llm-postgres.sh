#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
container="backoffice-llm-test-$$"
trap 'docker rm -f "$container" >/dev/null 2>&1 || true' EXIT
docker run -d --rm --name "$container" -e POSTGRES_PASSWORD=test-only -p 127.0.0.1::5432 postgres:17.10-bookworm >/dev/null
for attempt in {1..30}; do
  if docker exec "$container" pg_isready -U postgres >/dev/null 2>&1; then break; fi
  sleep 1
done
docker exec "$container" createdb -U postgres llm_lifecycle_test
docker exec "$container" psql -v ON_ERROR_STOP=1 -U postgres -d llm_lifecycle_test -c 'CREATE SCHEMA processing' >/dev/null
docker exec -i "$container" psql -v ON_ERROR_STOP=1 -U postgres -d llm_lifecycle_test < ../../database/migrations/000127_llm_lifecycle.up.sql >/dev/null
port=$(docker port "$container" 5432 | cut -d: -f2)
export LLM_TEST_PG_URL="postgresql://postgres:test-only@127.0.0.1:$port/llm_lifecycle_test"
pnpm exec vitest run tests/llm-settings.server.test.ts
(cd ../dagster_v3 && uv run pytest tests/test_llm_lifecycle.py)
