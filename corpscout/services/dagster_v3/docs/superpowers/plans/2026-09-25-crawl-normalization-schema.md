# Crawl normalization: database migration implementation plan

> **For agentic workers:** Execute this plan task-by-task in the current session using the executing-plans skill. Steps use checkbox syntax for tracking.

**Goal:** Install the typed ClickHouse contracts for the nine deterministic crawl datasets, including purpose, site types, business activities and structured jobs.

**Architecture:** Append detail rows under a normalization UUID and publish an attempt by inserting its scan row last. Serving views join that exact UUID. Keep latest attempt and latest usable attempt separate, with independent selection for full, jobs and basic-info crawls.

**Tech Stack:** ClickHouse 26.5, golang-migrate SQL, pytest and the existing disposable clickhouse-local test runner.

**Spec:** [Approved design](../../../src/dagster_v3/defs/website_crawl/docs/website-crawl-normalization-design.md).

## Global constraints

- Migration-owned schema in `corpscout/clickhouse/migrations`, registered in `EXPECTED_MIGRATIONS`.
- Preserve existing results and archives. No crawl, model call, company update or automatic backfill in this task.
- All rows retain `(domain, crawl_type, request_id, attempt)` and a normalization UUID.
- Original-language strings and source evidence are retained. Unsupported values stay nullable.
- This plan implements delivery step 1 only. Parser/assets, automation/backfill, offline text-job extraction and company enrichment follow as separately reviewable tasks in the approved design.

## Task 1: schema and query behavior

**Files:**
- Create `corpscout/clickhouse/migrations/000452_corpscout_website_crawl_normalized.up.sql` and `.down.sql`.
- Create `tests/test_website_crawl_normalized_migration.py` in dagster_v3.
- Modify `tests/test_clickhouse_migrations.py` to register migration 452.
- Update the source design with actual publication/view contracts and validation results.

**Interface:** Nine `website_crawl_*` tables: scans, site_profiles, business_activities, pages, contacts, identifiers, structured_data, links, jobs. Each detail table has `_published` (all published attempts) and `_current` (latest usable attempt) views. Scans expose `_latest`, `_latest_usable` and `_latest_profile` views. Profiles and activities use the last source-matched classification, including a skipped full crawl.

- [x] Write real-engine tests before DDL. Exercise unpublished detail rows, two normalizations of one attempt, late older revisions, a newer failure, skipped full crawls, independent crawl types, valid empty replacement, duplicate detail inserts, nullable values, invalid records and additive up/down/up.
- [x] Run `uv run pytest tests/test_website_crawl_normalized_migration.py -q` and confirm the missing migration fails.
- [x] Add migration tables and views. Use `ReplacingMergeTree(normalization_revision)` on scans and `ReplacingMergeTree` on detail keys including normalization UUID, page ID and row index. Use `FINAL` in serving views; filter eligibility only after scan revision resolution.

```sql
CREATE VIEW corpscout.website_crawl_scans_latest AS
SELECT * FROM corpscout.website_crawl_scans FINAL
ORDER BY finished_at DESC, request_id DESC, attempt DESC
LIMIT 1 BY domain, crawl_type;
```

- [x] Require completed, successful, error-free scans for usable data, with `crawl_status='finished'` or `crawl_type='site_info' AND crawl_status='skip_crawling'`. Keep all other outcomes queryable through history and latest-attempt views.
- [x] Constrain positive attempts/revisions, nonempty identities, nonzero normalization UUIDs, valid enum states, truthful success, and structured-data scalar value types. Explicit null is a supported scalar kind. Empty arrays are valid and are not inferred absence.
- [x] Pin the entire native column order/type contract for all nine tables through real `DESCRIBE TABLE` checks. These are the contracts the later exporter must adopt.
- [x] Run `uv run pytest tests/test_website_crawl_normalized_migration.py tests/test_clickhouse_migrations.py -q` and `uv run dg check defs`.
- [x] Verify only task paths changed and commit them explicitly.
- [x] Read the live migration version, apply this additive migration through the existing Makefile target if preceding versions are already applied, and verify the nine empty tables plus publication/current views. Never advance unrelated migrations or rewind the live ledger.

## Completion evidence

- `uv run pytest tests/test_website_crawl_normalized_migration.py tests/test_clickhouse_migrations.py -q`: 168 passed, 20 existing Dagster beta warnings.
- `uv run dg check defs`: component YAML and all definitions loaded successfully.
- Live ledger was 451 with no conflicting target table names. `make clickhouse-migrate-up-one` applied 452 successfully; `make clickhouse-migrate-version` returned clean version 452.
- Read-only live verification compared all nine complete native schemas against the test contract and queried all 19 views. Every new table and view is empty, as expected before parser assets exist.
- The first real-engine run exposed ClickHouse's constraint-CNF expansion limit. Independent type/presence constraints replaced the large disjunction; all scalar and rejection tests pass. Regression tests additionally verify exact number text and a skipped full crawl updating classification while preserving older deep facts.

This migration does not claim parsed data or running assets; those are the next task.
