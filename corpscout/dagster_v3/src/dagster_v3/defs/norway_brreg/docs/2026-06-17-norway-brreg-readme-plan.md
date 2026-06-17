# Norway Brreg README Documentation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create comprehensive source-local documentation for the Norway BRREG Dagster processing flow.

**Architecture:** Add `src/dagster_v3/defs/norway_brreg/docs/README.md` as the durable package documentation. It should describe source APIs, Dagster assets, DuckDB staging tables, Temporal translation workflow, ClickHouse exports, dependencies, schemas, configuration, and operational run order.

**Tech Stack:** Dagster, dlt, DuckDB, Temporal, ClickHouse, Markdown.

---

## Tasks

### Task 1: Create README

- [ ] Create `src/dagster_v3/defs/norway_brreg/docs/README.md`.
- [ ] Include a Mermaid graph showing the end-to-end flow.
- [ ] Document each Dagster asset with upstreams, outputs, storage path, and purpose.
- [ ] Document DuckDB and ClickHouse tables.
- [ ] Document translation workflow status/sensor behavior.
- [ ] Document what is translated and what is intentionally not translated.
- [ ] Document operational run order and validation commands.

### Task 2: Verify

- [ ] Run:

```bash
uv run dg check defs
```

- [ ] Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py -q
```

## Self-Review

- The README must describe the current graph, not old multi-asset behavior.
- The README must explain that `norway_brreg_clickhouse_financial_statements` depends on `norway_brreg_financial_statements_duckdb`, not translations.
- The README must identify the real Dagster asset key `norway_brreg_financial_statements_duckdb`, while noting that the Python function is `norway_brreg_financial_statements_duckdb_asset`.
- The README must explain Temporal UI state via `norway_brreg_translation_workflow_status`.
