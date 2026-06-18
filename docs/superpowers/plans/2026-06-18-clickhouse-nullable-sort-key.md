# ClickHouse Nullable Sort Key Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the Norway resolved ClickHouse migration so it runs on ClickHouse instances where `allow_nullable_key` is disabled.

**Architecture:** Keep `fiscal_year` nullable as data, but do not use the nullable column directly in the MergeTree sorting key. Add a regression test against the migration SQL, then change the sorting key to an `ifNull(...)` expression.

**Tech Stack:** ClickHouse SQL migrations, pytest migration-contract tests.

---

### Task 1: Reproduce With A Migration Contract Test

**Files:**
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_clickhouse_migrations.py`
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/clickhouse/migrations/000012_corpscout_norway_resolved_and_domains.up.sql`

- [ ] **Step 1: Write the failing test**

Add a test that rejects the exact broken sorting key and requires the non-null expression:

```python
def test_norway_financial_statements_sort_key_avoids_nullable_fiscal_year() -> None:
    sql = _migration_sql("000012_corpscout_norway_resolved_and_domains.up.sql")

    assert "ORDER BY (org_number, fiscal_year, accounts_type, source_record_id)" not in sql
    assert "ORDER BY (org_number, ifNull(fiscal_year, 0), accounts_type, source_record_id)" in sql
```

- [ ] **Step 2: Run the test and verify red**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_clickhouse_migrations.py::test_norway_financial_statements_sort_key_avoids_nullable_fiscal_year -q
```

Expected: fail because the migration still uses `fiscal_year` directly in `ORDER BY`.

- [ ] **Step 3: Update the migration**

Change:

```sql
ORDER BY (org_number, fiscal_year, accounts_type, source_record_id);
```

to:

```sql
ORDER BY (org_number, ifNull(fiscal_year, 0), accounts_type, source_record_id);
```

- [ ] **Step 4: Verify green**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_clickhouse_migrations.py::test_norway_financial_statements_sort_key_avoids_nullable_fiscal_year tests/test_clickhouse_migrations.py::test_norway_resolved_migration_covers_exported_columns -q
```

Expected: both tests pass.

- [ ] **Step 5: Verify the actual migration command**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make clickhouse-migrate-up
```

Expected: migration 12 no longer fails with ClickHouse code 44.

- [ ] **Step 6: Final Dagster checks**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_clickhouse_migrations.py -q
uv run dg check defs
```

Expected: migration tests pass and Dagster definitions load.
