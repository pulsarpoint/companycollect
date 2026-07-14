# AGENTS.md — dagster_v3

Agent guidance for this project (Dagster company open-data pipelines) lives in **[CLAUDE.md](./CLAUDE.md)**
in this directory. It is the single source of truth — read and follow it.

It covers: the per-source pipeline shape (dlt → per-source DuckDB → dbt → ClickHouse), DuckDB single-writer
pools and the file-stem-vs-dataset-name rule, the no-`from __future__ import annotations` asset gotcha, the
migration-owned ClickHouse export pattern, resilient HTTP downloads (dlt requests session + streaming retry),
monthly partitions + `multi_run` throttled backfills, ClickHouse migration rules, separate-step EUR→USD
currency conversion, PgBouncer connection scaling for 100+ sources, and troubleshooting (stale dbt locks,
connection exhaustion).

(The monorepo-wide Go/agent conventions are in the repository-root `AGENTS.md`; this file is the dagster_v3
sub-project supplement.)
