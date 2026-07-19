# Sweden Incremental (Never-Replace) Financial Exports Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the full-table-replace exports of `se_financial_reports` and `se_financial_facts` with **partition-scoped incremental upserts** (delete-own-scope + insert, the Norway updates precedent), so no host ever needs the full history locally and the 2026-07-19 cross-host wipe becomes structurally impossible — implementing the user's architecture decision: *never replace; insert new data; rely on keys/merge semantics for idempotency*.

**Architecture:** The source is append-shaped (immutable weekly archives; parsed reports never change; nothing is deleted upstream). The export assets become **partitioned like their parse counterparts** (backfill years + current weeklies): each run exports ONLY its partition's rows from the local DuckDB, first deleting exactly its own scope in ClickHouse (`ALTER TABLE ... DELETE WHERE source_archive_key IN (<this partition's archive keys>) SETTINGS mutations_sync = 1` — usually a no-op for new archives), then inserting. Reads stay exact (no FINAL needed — the tables are `ReplacingMergeTree(resolved_at)` but consumers like the metrics rebuild scan raw, so we keep the table free of live duplicates rather than relying on async merges). Derived tables (`se_financial_metrics`, `se_financial_history`) STAY rebuild-from-source — correct for derivations, single-host CH→CH, no cross-host trap. The shrink guard remains only on the metrics rebuild (a full replace) and becomes obsolete for reports/facts (nothing replaces them anymore).

**Tech Stack:** dagster_v3 (partitioned assets, clickhouse-driver mutations + inserts, pytest). NO ClickHouse migration — engines/schemas unchanged.

## Sequencing gate — SATISFIED (2026-07-19, late)

The restore chain completed and was verified: `se_financial_reports` 2,206,788 (all years incl. complete 2026), `se_financial_facts` 290,754,496, metrics/history/summary/companies_all rebuilt on the server (runs 9690ef2b + a54186f8). The per-year DuckDB files (2020–2026) live on the prod dagster host. Task 3 is UNGATED.

## Post-plan module changes to account for (2026-07-19, late)

- New downstream assets `se_company_officers_clickhouse` (officers.py) and `se_company_audits_clickhouse` (audits.py) both declare `deps=["sweden_financial_facts_clickhouse"]` and are members of `SWEDEN_FINANCIAL_CLICKHOUSE_SELECTION` (which now holds reports/facts/metrics/history/officers/audits). Task 2's dep-alignment scope includes them.
- `CLICKHOUSE_LEAVES` in `common/clickhouse_checks.py` now lists se_company_officers/se_company_audits alongside the originals.
- A PARTIAL Task 1 implementation exists as REFERENCE ONLY (from an interrupted earlier attempt; superseded by module evolution — officers/audits/etc. landed since): stashed working copies extracted to the session scratchpad `si_stash_reference/clickhouse.stashed.py` (upsert_sweden_financial_reports_partition/facts twin, ~lines 344+: scope resolution, delete-by-archive-keys with mutations_sync, explicit-column insert) and `storage.stashed.py` (sweden_financial_year_duckdb_connection single-year read-only helper). Read them, reuse what is correct, but the CURRENT committed files are the base — do not apply the stash wholesale.

## Global Constraints

- **Scope key = the partition's archive set.** Each partitioned export computes its archive keys from the local DuckDB catalog for ITS partition (`source_archive_key` values), deletes CH rows `WHERE source_archive_key IN (...)`, then inserts that partition's rows. A partition run touches NOTHING outside its own archives — cross-host safe by construction.
- Delete uses `SETTINGS mutations_sync = 1` (norway precedent `financial_statements.py:514-516`); insert uses explicit column lists; per-run metadata: partition, archives in scope, rows deleted (pre-count), rows inserted.
- Idempotency invariant (test-pinned): running the same partition export twice yields identical CH row counts for that scope (delete-own-scope makes re-runs clean; ReplacingMergeTree versioning is defense-in-depth, not the primary mechanism).
- The parse assets and their partitions definitions are UNTOUCHED; the export assets change from non-partitioned full-replace to partitioned scoped-upsert with the SAME partitions as their upstream parse assets (`SWEDEN_FINANCIAL_BACKFILL_PARTITIONS` for the backfill pair, `SWEDEN_FINANCIAL_CURRENT_PARTITIONS` for current), deps updated to the matching parse asset, `BackfillPolicy.multi_run(max_partitions_per_run=1)` like their upstreams.
- `sweden_financial_metrics_clickhouse` (+ its shrink guard + `archive_ingest_complete` check) keeps working unchanged: it's a derived full rebuild reading CH facts — verify its deps now point at the new partitioned exports (unpartitioned-on-partitioned dep is fine, matches companies_all precedent).
- The old full-replace exporter functions are DELETED, not kept as dead code (the jobs/schedules that referenced the old asset names must be updated; `sweden_financial_clickhouse_job` selection updated; `common/clickhouse_checks.py` leaf entries re-pointed if they name the old assets).
- ClickHouse `IN` list size: a year partition holds ≤ ~300 archive keys — bind as an Array param, never interpolate.
- Local DuckDB may legitimately hold ONLY some partitions: the export for partition P must FAIL LOUDLY (ValueError) if the local catalog has no rows for P (never silently export nothing), mirroring the empty-input rule.
- dagster_v3 conventions bind; explicit-path commits; `uv run dg check defs`; TDD; trailer `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`; dev 5183 USER-OWNED; SELECT-only CH outside the pipeline.

## Ground truth (2026-07-19)

- `se_financial_reports`/`se_financial_facts`: `ReplacingMergeTree(resolved_at)` (migration 000090 — implementer verifies ORDER BY keys and records them; no schema change needed since `source_archive_key` exists on both).
- Current exporters: `sweden_financial/clickhouse.py` `export_sweden_financial_reports_clickhouse`/`..._facts_clickhouse` — stage + EXCHANGE full replace + shrink guard (guard added 906df992 after the incident; for reports/facts it dies with this plan).
- Norway scoped-upsert precedent: `norway_brreg_financial/assets/financial_statements.py:505-520` (delete-by-key-set with `mutations_sync = 1`, then insert).
- Partition defs: `sweden_financial/assets.py:41-59` (backfill years 2020–2026; current weeklies from 2026-07-04).
- The parse catalog in the local DuckDB records per-report `source_archive_key` + partition year — the export derives its scope from it (implementer confirms exact table/column names in `parsing.py`/`storage.py`).
- Incident context: `.superpowers/sdd/progress.md` "INCIDENT + RECOVERY" entry (2026-07-19).

## Out of scope (logged)

- Migrating snapshot-shaped exports (se_companies etc.) — full-replace with guards remains correct there (deletion propagation).
- PARTITION BY re-tabling for DROP/REPLACE PARTITION semantics — cleaner long-term but needs a data-copy migration; revisit if mutation cost ever bites (weekly deltas make deletes near-no-ops in practice).
- Applying the same incremental pattern to other append-shaped sources (UK accounts already do it; audit others later).

---

### Task 1: Scoped-upsert exporters + partitioned assets

**Files:**
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_financial/clickhouse.py` (replace the two full-replace exporters with scoped-upsert functions)
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_financial/assets.py` (partitioned export assets, deps, jobs, defs)
- Test: `corpscout/services/dagster_v3/tests/test_sweden_financial_clickhouse.py`, `tests/test_sweden_financial_assets.py`

**Interfaces:**
- `upsert_sweden_financial_reports_partition(duckdb_connection, clickhouse, partition_key, log) -> {archives, deleted, inserted}` and the facts twin — each: resolve the partition's archive keys from the local catalog (ValueError if none), count+delete CH rows in scope (Array-param `IN`, mutations_sync=1), insert the partition's rows with explicit columns, return metadata.
- Assets `sweden_financial_reports_clickhouse` / `sweden_financial_facts_clickhouse` become PARTITIONED (two pairs mirroring parse assets' backfill/current split IF that's how the module structures parse partitions — mirror EXACTLY the existing parse-asset partition layout, whatever it is; keep asset names stable if possible so `clickhouse_checks.py`/metrics deps survive, else update every reference).

- [ ] **Step 1 (TDD):** unit tests first with a fake CH client + seeded DuckDB fixture: scope resolution per partition; ValueError on missing partition; delete-then-insert call order with Array-param binding (no interpolated keys); idempotency (second run: delete count == first run's insert count, final state identical); explicit column lists match the CH schema constants; the OTHER partition's rows are untouched by a run.
- [ ] **Step 2:** implement, wiring the partitioned assets (deps → matching parse assets; BackfillPolicy multi_run; pools NOT needed — CH-write only reads local duckdb read-only? The duckdb READ still needs the module's duckdb pool if writes could collide — mirror how the current exporters handle duckdb access and keep the same pool discipline).
- [ ] **Step 3:** update `sweden_financial_clickhouse_job` selection + any schedule/job/check references; `archive_ingest_complete` stays attached to the reports export (verify asset_check attachment works on a partitioned asset — if not, move it to `sweden_financial_metrics_clickhouse`); DELETE the old full-replace exporter functions + their shrink-guard wiring for reports/facts (metrics keeps its guard).
- [ ] **Step 4:** gates — the sweden_financial test files green; `uv run dg check defs` clean. Commit: `feat(dagster): partition-scoped incremental sweden financial exports`.

---

### Task 2: History/metrics dep + docs alignment

**Files:**
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_financial/{metrics.py,history.py,assets.py}` (only if dep names changed)
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_financial/docs/*design*.md` (the design doc's export section: full-replace → scoped upsert, with the incident as rationale)
- Test: wiring tests updated

- [ ] Verify metrics/history assets' deps resolve against the new export assets; update the module design doc (append-shaped rationale, the never-replace decision, the one remaining full-replace = derived metrics rebuild and why); gates + commit: `docs(dagster): sweden export architecture notes` (or fold into Task 1's commit if trivial — implementer's call, explicit paths).

---

### Task 3: Validation materialization (GATED — after the restore chain completes and is verified)

- [ ] With CH fully restored (reports ≈ 2.25M incl. 2026, facts ≈ 296M): run ONE partition export end-to-end as validation (e.g. `--partition 2025` for both partitioned exports): confirm metadata shows delete count == that partition's prior rows, insert count equal-or-newer, total table counts unchanged-or-grown, OTHER years' counts byte-identical before/after (the never-touch invariant, verified with per-year count snapshots).
- [ ] Then a current-weekly partition run (this week's) to validate the weekly path.
- [ ] Record in the report; clean up. The controller then adds the prod-deploy note: after deploying this, prod may safely run Sweden exports again (each host exports only partitions it parsed).
