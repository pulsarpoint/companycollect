# ESEF Pipeline Simplification Implementation Plan

> Superseded on 2026-08-24 by
> `src/dagster_v3/defs/esef_filings/docs/esef-parsing-migration-plan.md`.
> This file is retained only as historical decision context; its target graph
> and asset names are not current runtime contracts.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate `defs/esef_filings/` from a 27-asset, weekly-partitioned pipeline to a ~9-asset, unpartitioned sweep-and-diff pipeline, deleting the finished OIM migration scaffolding and ~30% accidental complexity along the way.

**Architecture:** Discovery is a cheap full index sweep (~126 API pages); "new work" is computed by content-address (package SHA-256 in S3) and ledger anti-joins (ClickHouse), not by partition bookkeeping. Raw packages and parsed Arelle artifacts stay in S3 as the rebuild source; parsed rows land in ClickHouse (`ReplacingMergeTree` idempotency); derived tables stay single SQL SELECTs. Arelle parses each package exactly once (process pool, skip-existing by SHA). LLM enrichment is one asset in its own paid job.

**Tech Stack:** Dagster (dg CLI), ClickHouse (`ReplacingMergeTree(resolved_at)` on `esef_filings`/`esef_facts`/`esef_financial_metrics`; plain `MergeTree` on the document tables), S3 object store, Arelle, DeepSeek. Existing DuckDB staging is retired for this source (Decision D1).

**Spec:** This plan is its own spec — the "Target architecture" and "Asset mapping" sections below. Background: `src/dagster_v3/defs/esef_filings/docs/esef_filings-design.md` (as-built) and the 2026-08-21 four-agent complexity analysis (memory: `esef-simplification-analysis`).

## Global Constraints

- Validate with `uv run dg check defs` and the relevant `uv run pytest tests/...` before declaring any task done.
- No `from __future__ import annotations` in modules defining Dagster assets.
- The ClickHouse migration owns the schema; Python only asserts tables exist. Never duplicate DDL.
- Non-nullable CH `String` columns get `''`, never `NULL`; `Date32` in `ORDER BY` gets the `1970-01-01` sentinel.
- Keep: crawl-completeness guard (90% of `meta.count`), the `period_duration_end`/OIM end-of-day date fixes in `facts.py`, the 404/410 skip-and-count classifier, `scope='consolidated_ifrs'`, the Arelle process pool + temp-file artifact handoff.
- Commit by explicit path (never `git add -A`); Conventional Commits.
- Tests run against `FakeClickHouseClient`/`FakeObjectStore` + short-lived local files — no network, no live ClickHouse.
- Heavy runs (backfills, exports) execute on the prod dagster server (`companycollect`), launched from its UI — never relocate in-flight work.

## Decisions (agreed 2026-08-21 unless marked open)

| # | Decision | Status |
|---|---|---|
| D1 | Retire DuckDB staging for this source. S3 artifacts are the rebuild source; CH is the serving copy. Deviates from repo convention deliberately, for this source only. | Agreed in principle; final call inside Phase 2's detailed plan |
| D2 | Delete `esef_report_xhtml_s3`. Zero consumers; the same XHTML lives inside the SHA-256-archived report packages. The future embeddings corpus extracts from packages. | Recommended — confirm before Phase 1b Task 6 |
| D3 | Retire the legacy OIM fact path + `fact_parity.py` + `compare_artifact_to_oim` + parity half of `segment_cli.py`. | Agreed, gated on G1 |
| D4 | LLM extracts narrative company information only; numeric facts always come from Arelle. LLM enrichment collapses to one asset in its own manually-triggered job. | Agreed |

## Gates

- **G0 — backfill drained.** The four in-flight backfills (`crujszxc`, `ketxmdjy`, `qlpyshry`, `ufzgsmzd`; 163 runs queued/started at 2026-08-21) must complete. Cancel stragglers cleanly (`scripts/dagster-health-check.py --fix` frees leaked pool slots) BEFORE any `partitions_def` change — a queued partition run starting after partitions vanish fails with `RUN_EXCEPTION` and leaks its pool slot.
- **G1 — artifact/OIM parity at scale.** ClickHouse currently holds OIM-produced facts (export of 2026-07-23): **14,399,251 facts / 25,061 filings / 3,802 source documents** (baseline snapshot 2026-08-21). The first artifact-based export is compared against aggregate snapshots of this baseline (Phase 0). Phase 1b and Phase 2 start only after G1 passes.

## Target architecture (the end state)

Unpartitioned assets, one `esef_filings` op pool for anything touching shared local state, `esef_arelle` pool for parse workers:

| # | Asset | Does | Idempotency |
|---|---|---|---|
| 1 | `esef_filings_index` | Full index sweep (~126 pages, JSON:API), completeness guard, insert all rows into `corpscout.esef_filings` | `ReplacingMergeTree(resolved_at)` keyed `(lei, period_end, fxo_id)`; re-insert supersedes on merge |
| 2 | `esef_packages_s3` | Download report packages missing from S3 | skip-existing by `package_sha256` object key |
| 3 | `esef_artifacts_s3` | Arelle-parse packages with no schema-v5 artifact in S3 (process pool) | skip-existing by SHA; artifact key embeds `ARTIFACT_SCHEMA_VERSION` |
| 4 | `esef_facts_load` | For `fxo_id`s in the index but absent from the ledger: read artifact from S3, emit fact rows to `corpscout.esef_facts` | worklist = anti-join on the ledger (`esef_source_documents.extraction_status`), so zero-fact filings don't loop; retries superseded by `ReplacingMergeTree(resolved_at)` |
| 5 | `esef_document_evidence_load` | Same worklist/artifact read: source documents, contact+website candidates, concept labels, fact disclosures → CH | per-`source_document_id` delete-free re-insert on `MergeTree` requires an explicit per-doc `ALTER DELETE`-free strategy: stage rows for the worklist docs only and `INSERT`; ledger row written last marks the doc done |
| 6 | `esef_entity_registry_map` + `esef_financial_metrics` | The existing single CH SELECTs (stage + shrink guard + `EXCHANGE`) | full derived rebuild (unchanged) |
| 7 | `esef_document_company_information` | One LLM asset: anti-join on `(source_document_id, model_name, prompt_version)`, evidence from S3 artifact, DeepSeek call, result rows + one content-addressed response artifact in S3. Four independent `esef_filings` assets project the normalized observation tables from this result | the existing 6-line LEFT JOIN skip |
| 8–9 | `esef_document_concept_official_translations` + `esef_document_concept_translation_load` (+ WARN coverage check) | unchanged concept-translation pair, rebuilt on the shared `translator_load` builders | unchanged |

Jobs: `esef_filings_refresh_job` (1–6, unpartitioned, weekly schedule), `esef_document_company_information_job` (7, manual, paid). The reconciliation job disappears — the sweep IS the reconciliation. Facts consumers (`metrics.py` anchor CTE) must read `esef_facts` merge-tolerantly (`FINAL` or `LIMIT 1 BY (fxo_id, fact_id)`) once inserts replace `EXCHANGE`-swaps.

## Asset mapping (current 26 → target)

| Current asset | Fate |
|---|---|
| `esef_filings_index_duckdb` | → target 1 (direct CH, no DuckDB) |
| `esef_filings_index_reconciliation_duckdb` | deleted — subsumed by full sweep |
| `esef_filing_facts_json_s3` | deleted (Phase 1b, D3) |
| `esef_filing_facts_duckdb` | → target 4 |
| `esef_report_xhtml_s3` | deleted (D2) |
| `esef_filings_clickhouse` / `esef_facts_clickhouse` | subsumed by targets 1 / 4 |
| `esef_entity_registry_map_clickhouse` / `esef_financial_metrics_clickhouse` | → target 6 (unchanged logic) |
| `esef_document_extraction_manifest_s3` | deleted — worklist computed in-asset |
| `esef_document_artifacts_s3` | → targets 2+3 (download and parse split) |
| `esef_source_documents_duckdb` / `esef_document_contact_candidates_duckdb` / `esef_document_concept_labels_duckdb` | → target 5 |
| `esef_source_documents_clickhouse` / `esef_document_contact_candidates_clickhouse` / `esef_document_concept_labels_clickhouse` | subsumed by target 5 |
| `esef_fact_disclosure_inputs_s3` / `esef_fact_disclosure_artifacts_s3` / `esef_fact_disclosures_duckdb` / `esef_fact_disclosures_clickhouse` | collapsed into target 5 (disclosures are a pure function of artifact `raw_value` — no S3 round-trips) |
| `esef_document_concept_official_translations_clickhouse` / `esef_document_concept_translation_load` | → targets 8–9 |
| `esef_document_company_information_duckdb` / `_clickhouse` | → target 7 |
| `esef_document_observations_clickhouse` | deleted — split into four independently materializable projection assets in `esef_filings` |
| `esef_company_source_records_clickhouse` (group `company_source_records`) | kept pending the separate cross-source provenance cleanup |

Module-file consolidations riding along: `contact_candidates.py` + `website_candidates.py` merge into one `candidates.py` (they are one file forked; output already lands in one table); report XHTML parsed once per member and the tree passed to all extractors; the 4 `allow_shrink` Config classes become 1; the 5 export bodies become 1 helper; `_json_text`/`_mapping`/`_validated_sha256` hoisted to one `_validation.py`.

---

# Phase 0 — Gate verification (runs on the prod server; no code changes)

### Task 0.1: Confirm backfills drained (G0)

**Files:** none (operational).

- [ ] **Step 1:** Check run queue on the server:

```bash
ssh companycollect "docker exec ppoint-postgres psql -U dagster -d dagster -t -A -c \
  \"SELECT status, count(*) FROM runs WHERE status IN ('STARTED','QUEUED','STARTING') GROUP BY status;\""
```

Expected: zero rows (or rows unrelated to ESEF — verify via `run_tags key='dagster/backfill'` against the four ids `crujszxc`, `ketxmdjy`, `qlpyshry`, `ufzgsmzd`).

- [ ] **Step 2:** Run the health check; fix leaked slots if reported:

```bash
ssh companycollect "cd /path/to/dagster_v3 && uv run python scripts/dagster-health-check.py" || \
ssh companycollect "cd /path/to/dagster_v3 && uv run python scripts/dagster-health-check.py --fix"
```

- [ ] **Step 3:** Confirm partition coverage: every weekly partition from `2023-01-01` to the last closed week has an `esef_filing_facts_duckdb` + `esef_document_artifacts_s3` materialization (Dagster UI asset partition view). Record any gaps; backfill them before proceeding.

### Task 0.2: Snapshot the OIM baseline aggregates (before anything overwrites them)

- [ ] **Step 1:** Create aggregate snapshot tables in ClickHouse (aggregates, not full copies):

```sql
CREATE TABLE corpscout.esef_facts_oim_baseline_by_year ENGINE = MergeTree ORDER BY fiscal_year AS
SELECT toYear(period_end) AS fiscal_year, count() AS fact_count,
       uniqExact(fxo_id) AS filing_count, uniqExact(lei) AS lei_count
FROM corpscout.esef_facts GROUP BY fiscal_year;

CREATE TABLE corpscout.esef_metrics_oim_baseline_sample ENGINE = MergeTree ORDER BY fxo_id AS
SELECT lei, fxo_id, period_end, revenue_amount_original, total_assets_amount_original,
       equity_amount_original, profit_loss_amount_original, currency
FROM corpscout.esef_financial_metrics;
```

- [ ] **Step 2:** Verify totals match the recorded baseline: `SELECT sum(fact_count) FROM corpscout.esef_facts_oim_baseline_by_year;` → expected **14,399,251**.

### Task 0.3: Run the artifact-based exports and compare (G1)

- [ ] **Step 1:** From the prod Dagster UI, materialize in order: `esef_filings_clickhouse`, `esef_facts_clickhouse`, `esef_entity_registry_map_clickhouse`, `esef_financial_metrics_clickhouse`. The shrink guards protect the swap; do NOT pass `allow_shrink` on the first attempt — a refusal is signal, not an obstacle.
- [ ] **Step 2:** Compare per-year fact counts:

```sql
SELECT b.fiscal_year, b.fact_count AS oim, n.fact_count AS arelle,
       round(n.fact_count / b.fact_count, 3) AS ratio
FROM corpscout.esef_facts_oim_baseline_by_year b
FULL JOIN (SELECT toYear(period_end) AS fiscal_year, count() AS fact_count,
           uniqExact(fxo_id) AS filing_count FROM corpscout.esef_facts GROUP BY fiscal_year) n
USING (fiscal_year) ORDER BY fiscal_year;
```

Expected: ratios near 1.0 per year (the 2026-08-04 five-package validation matched 4,582/4,582 facts; at scale small deviations from extension-concept handling are acceptable — investigate any year off by >2%).

- [ ] **Step 3:** Spot-check 10 filings' metrics against the baseline sample:

```sql
SELECT b.fxo_id, b.revenue_amount_original AS oim_rev, m.revenue_amount_original AS new_rev
FROM corpscout.esef_metrics_oim_baseline_sample b
JOIN corpscout.esef_financial_metrics m USING (fxo_id)
WHERE b.revenue_amount_original != m.revenue_amount_original LIMIT 20;
```

Expected: empty (or explainable diffs only). Record the verdict + counts in this plan file under G1. **G1 passes → Phases 1b and 2 unlock.**

#### G1 fact-variance decision (2026-08-24)

The `2026-04-19` shadow partition produced 34,069 facts in both paths. Three
semantic rows differed:

- `ifrs-full:DateOfEndOfReportingPeriod2013` used `2025-12-31` in OIM and
  `2025-12-31T00:00:00` in the Arelle artifact. Date-typed artifact values are
  normalized to `YYYY-MM-DD` when created and when an existing schema-v5
  artifact is projected into `esef_facts`.
- Two `ifrs-full:AverageNumberOfEmployees` facts (`2243.0`, decimals 3, and
  `2.0`, decimals 0) were unitless text in OIM but numeric `xbrli:pure` facts in
  Arelle. This is an approved data-quality correction: the parity report records
  the approval and raw metric mismatch while allowing the cutover gate to pass.
  Approval requires the value and every semantic field except unit to match, so
  it cannot mask a changed employee count.

The separate `2025-05-04` shadow partition matched all 536 facts semantically.
Operational `source_run_id` differences and deterministic fact-ID differences
are tracked separately and are not semantic fact variances. This variance
decision does not by itself mark the full-corpus G1 gate complete.

---

# Phase 1a — Bug fixes and dead-code deletions safe before G1

Each task: branch from `main`, commit by explicit path. All in `src/dagster_v3/defs/esef_filings/` unless noted. Line numbers are as of 2026-08-21 — locate by symbol (`rg -n '<symbol>'`) if drifted.

### Task 1.1: Fix the latent liabilities-fallback bug in `metrics.py`

**Files:** Modify: `metrics.py:394-403`; Test: `tests/test_esef_filings_metrics.py`.

**Interfaces:** Produces: `build_esef_financial_metrics_select()` output where the balance-equation fallback uses the same coalesced candidate expressions as the main columns.

- [ ] **Step 1: Write the failing test** — assert the rendered SELECT's fallback no longer hardcodes first candidates:

```python
def test_liabilities_fallback_uses_coalesced_candidates() -> None:
    sql = build_esef_financial_metrics_select(run_id="test")
    assert "facts.total_assets_c0 IS NOT NULL AND facts.equity_c0 IS NOT NULL" not in sql
```

(Today `total_assets`/`equity` each have one candidate so `_coalesce_candidates_sql` renders `facts.total_assets_c0` — the *guard clause shape* is what distinguishes old from new. Adjust the assertion to the actual old string if it differs.)

- [ ] **Step 2:** Run: `uv run pytest tests/test_esef_filings_metrics.py -k liabilities -v` → FAIL.
- [ ] **Step 3:** In `metrics.py` replace the fallback (currently lines 397–401) with the coalesced expressions already in scope:

```python
                        if(
                            ({total_assets_sql}) IS NOT NULL AND ({equity_sql}) IS NOT NULL,
                            ({total_assets_sql}) - ({equity_sql}),
                            NULL
                        )
```

- [ ] **Step 4:** `uv run pytest tests/test_esef_filings_metrics.py -v` → PASS (all — rendered SQL is byte-identical today except the guard shape, so existing SQL-text tests may need the same string update).
- [ ] **Step 5:** Commit: `fix(esef): use coalesced candidates in liabilities fallback`

### Task 1.2: Drop the duplicate full SELECT in the metrics rebuild

**Files:** Modify: `metrics.py:505-514`; Test: `tests/test_esef_filings_metrics.py` (the refuse-on-empty test, ~line 397).

- [ ] **Step 1:** Update the refuse-on-empty test to expect refusal AFTER staging (the `ValueError` message stays identical; the fake client will now see `CREATE TABLE`/`INSERT` before the refusal — adjust the fake's expected statement sequence).
- [ ] **Step 2:** Run it → FAIL (refusal currently happens pre-stage).
- [ ] **Step 3:** In `replace_esef_financial_metrics_clickhouse`: delete the `would_be_row_count` execute + `if` block (lines 505–514); after `staged_row_count = clickhouse_table_row_count(...)` (line 525) insert:

```python
            if staged_row_count == 0:
                raise ValueError(
                    "ESEF financial metrics SELECT would insert 0 rows -- "
                    "refusing to replace "
                    f"{tables.QUALIFIED_ESEF_FINANCIAL_METRICS_TABLE} "
                    "(refuse-to-replace-on-empty)."
                )
```

(The existing `except`/cleanup path already drops the stage table on error — verify by reading the `finally` block.)

- [ ] **Step 4:** `uv run pytest tests/test_esef_filings_metrics.py -v` → PASS.
- [ ] **Step 5:** Commit: `perf(esef): stop executing the metrics SELECT twice per rebuild`

### Task 1.3: Translation load — skip unknown languages, WARN coverage severity

**Files:** Modify: `document_publish.py` (`:399-424`, `:482-496`); Test: `tests/test_esef_filings_publish.py`.

- [ ] **Step 1:** Write two failing tests: (a) an untranslated row with language `"tr"` (not in `_ESEF_LANGUAGE_NAMES`) does NOT raise `dg.Failure` — the asset completes and reports the skipped language in metadata; (b) `esef_document_concept_translation_coverage` returns `severity == dg.AssetCheckSeverity.WARN` when counts mismatch.
- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** Replace the `raise dg.Failure(...)` block (`:409-415`) with `context.log.warning(...)` + a `skipped_unsupported_languages` metadata entry; delete the unreachable `raise AssertionError` (`:423-424`, guaranteed dead by the loop above it); add `severity=dg.AssetCheckSeverity.WARN` to the `AssetCheckResult` at `:482-496` (mirrors the deliberate repo-wide decision documented in `defs/translator_load/coverage.py:14-21`).
- [ ] **Step 4:** `uv run pytest tests/test_esef_filings_publish.py -v` → PASS.
- [ ] **Step 5:** Commit: `fix(esef): warn instead of fail on unknown label languages and coverage gaps`

### Task 1.4: Delete unreachable `_validate_evidence_citations`

**Files:** Modify: `llm_enrichment.py` (delete `:735-812` + the call at `:922`).

- [ ] **Step 1:** Verify nothing else references it: `rg -n '_validate_evidence_citations' src tests` → only the definition and the one call.
- [ ] **Step 2:** Delete both. (`_normalize_evidence_citations` runs immediately before and already removes everything the validator checks — it cannot raise.)
- [ ] **Step 3:** `uv run pytest tests/test_esef_llm_enrichment.py -v && uv run dg check defs` → PASS.
- [ ] **Step 4:** Commit: `refactor(esef): drop unreachable citation validator`

### Task 1.5: Delete dead `replace_disclosure_partition_rows`

**Files:** Modify: `disclosure_parser.py:129-165`; Test: `tests/test_esef_fact_disclosures.py:122-133` (delete that test).

- [ ] **Step 1:** Verify: `rg -n 'replace_disclosure_partition_rows' src tests` → only definition + the one test (production inlines its own sequence in `disclosure_assets.publish_disclosure_partition`).
- [ ] **Step 2:** Delete function and test. Run `uv run pytest tests/test_esef_fact_disclosures.py -v` → PASS.
- [ ] **Step 3:** Commit: `refactor(esef): remove unused disclosure row replacer`

### Task 1.6: Remove the never-produced `'complete'` extraction status

**Files:** Modify: `src/dagster_v3/defs/company_source_records/sql.py:320, 352, 380, 427`.

- [ ] **Step 1:** Verify no historical rows carry it (prod): `ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT count() FROM corpscout.esef_document_company_information WHERE extraction_status='complete'\""` → expected `0`. If nonzero, STOP and keep the filter.
- [ ] **Step 2:** Remove `'complete'` from the four `IN (...)` filters (the writers only ever produce `'enriched'`, `'reused'`, `'no_evidence'` — `llm_enrichment_assets.py:204,231,576`).
- [ ] **Step 3:** `uv run pytest tests/ -k company_source_records -v && uv run dg check defs` → PASS. Commit: `refactor(esef): drop never-written 'complete' extraction status`

---

# Phase 1b — Post-G1 deletions (Decision D3; do not start before G1 passes)

### Task 2.1: Delete the legacy OIM fact path in `assets.py`

**Files:** Modify: `assets.py`; Delete tests in `tests/test_esef_filings_assets.py` that reference the deleted symbols.

Symbols to delete (locate each with `rg -n`): `esef_filing_facts_json_s3` (asset, `:1993-2021`, and its entry in `Definitions` `:2622`), `run_esef_filing_facts_partition` (`:1693-1990`), `run_esef_filing_facts_json_partition` (`:757-865`), `_archive_filing_fact_json` (`:706-729`), `_log_fact_json_progress` (`:732-754`), `_FactParseTask` (`:892-902`), `_parse_filing_facts_worker` (`:1053-1098`), `OIM_FACTS_PARSER_CONTRACT` (`:109`), `ESEF_FACT_JSON_PREFIX` (`:96`), `_fact_json_object_key` (`:238`), `_IndexFilingRow` (`:647`, only used by the deleted loop), plus the inline OIM checkpoint reader inside the deleted function.

- [ ] **Step 1:** Confirm none of these has a caller outside the deletion set: `for s in esef_filing_facts_json_s3 run_esef_filing_facts_partition _parse_filing_facts_worker _FactParseTask _archive_filing_fact_json OIM_FACTS_PARSER_CONTRACT; do rg -n "$s" src | rg -v assets.py; done` → empty. (`facts.iter_oim_facts` STAYS until Task 2.2 removes its last consumer.)
- [ ] **Step 2:** Delete the symbols and their tests (`tests/test_esef_filings_assets.py:747, :1560` and any other referencing test — find with `rg -n 'facts_json|run_esef_filing_facts_partition' tests/`).
- [ ] **Step 3:** `uv run dg check defs && uv run pytest tests/test_esef_filings_assets.py tests/test_esef_filings_facts.py -v` → PASS.
- [ ] **Step 4:** Commit: `refactor(esef): retire legacy OIM fact ingestion path` — one commit, cleanly revertable (this IS the emergency-fallback story now).

### Task 2.2: Delete `fact_parity.py`, `compare_artifact_to_oim`, and trim `segment_cli.py`

**Files:** Delete: `fact_parity.py`. Modify: `segment_parser.py` (delete `compare_artifact_to_oim` `:480-518` + helpers `:1302-1350`), `segment_cli.py` (delete the `--reference-oim`/`--lei`/`--period-end`/`--parity-sample-limit` args, `_period_end_from_fxo_id`, and both comparison branches — keep the ~90-line "parse one local ZIP → artifact JSON" tool), `tests/test_esef_ixbrl_segments.py` (delete `:567-793`, `:828-863` and the parity-related CLI tests), `facts.py` (delete `parse_oim_facts` and `iter_oim_facts` if `rg` shows no remaining consumer after the above).

- [ ] **Step 1:** `rg -n 'fact_parity|build_fact_parity_report|compare_artifact_to_oim' src tests` → confirm the only referencers are the files listed above.
- [ ] **Step 2:** Delete; update `docs/esef-ixbrl-segment-parser.md` to note the parity gates completed 2026-08-04 and were removed (keep the corpus table for the record).
- [ ] **Step 3:** `uv run dg check defs && uv run pytest tests/test_esef_ixbrl_segments.py tests/test_esef_filings_facts.py -v` → PASS.
- [ ] **Step 4:** Commit: `refactor(esef): remove completed OIM cutover parity harness`

### Task 2.3: Delete `esef_report_xhtml_s3` (Decision D2 — confirm with owner first)

**Files:** Modify: `assets.py` (asset `:2264-2292`, `run_esef_report_xhtml_partition` `:2127-2261`, `_archive_filing_report_xhtml` `:2060-2092`, `_log_report_xhtml_progress` `:2095-2124`, its entries in the refresh/backfill selections `:2509`, `:2545`, and `Definitions`); delete its tests.

- [ ] **Step 1:** Re-verify zero consumers: `rg -rn 'ESEF_REPORT_XHTML_PREFIX|report_xhtml' src tests --glob '!assets.py'` → only test references.
- [ ] **Step 2:** Delete symbols + tests; `uv run dg check defs && uv run pytest tests/test_esef_filings_assets.py -v` → PASS.
- [ ] **Step 3:** Commit: `refactor(esef): drop per-fxo_id XHTML archive (bytes live in the SHA-256 package archive)`
- [ ] **Step 4:** (Optional, later) S3 lifecycle-delete the `esef_filings/report_xhtml/` prefix after confirming the embeddings plan reads from packages.

### Task 2.4: Design-doc update

- [ ] **Step 1:** Update `docs/esef_filings-design.md` §2/§10/§11: the OIM archive and report-XHTML assets are retired (with dates and the G1 verdict); the reconstruction story is "S3 packages + artifacts → re-parse/re-derive".
- [ ] **Step 2:** Commit: `docs(esef): record OIM path retirement`

---

# Phase 2 — De-partition + sweep-and-diff core (targets 1–6, Decision D1)

**This phase gets its own detailed plan** (write it with superpowers:writing-plans when G1 passes and Phase 1b is merged) — its task decomposition depends on the G1 outcome and the D1 final call. The design contract it must honor:

1. **One index asset**, unpartitioned: full sweep with the existing completeness guard (`_check_crawl_completeness`, `EsefFilingsClient.last_reported_total`), inserting ALL rows into `corpscout.esef_filings` each run with fresh `resolved_at`. `ReplacingMergeTree(lei, period_end, fxo_id)` supersedes prior rows on merge; consumers read `FINAL`/`LIMIT 1 BY`. Refuse the write entirely on a failed guard. Delete `esef_filings_index_reconciliation_duckdb`, the windowed fetch, and the separate reconciliation job/schedule.
2. **Worklists by anti-join, never by partition.** The ledger is `corpscout.esef_source_documents` (one row per `fxo_id` with `extraction_status`, including failed/empty states) — a filing is "new" if its `fxo_id` has no ledger row for the current `ARTIFACT_SCHEMA_VERSION`. Zero-fact filings therefore do not loop.
3. **Facts insert, not exchange:** target 4 inserts per-filing fact rows; `ReplacingMergeTree(resolved_at)` absorbs retries. The metrics anchor CTE and any other `esef_facts` reader gains merge-tolerance (`FINAL` or `LIMIT 1 BY (fxo_id, fact_id)`). The DuckDB→CH exporters, the 4 `allow_shrink` Config classes, and `publish.py`'s twin export bodies are deleted with the replace pattern (the derived-table rebuilds in target 6 keep stage+guard+`EXCHANGE` unchanged).
4. **Partition retirement procedure:** G0 confirmed no in-flight backfills → change assets to unpartitioned in one commit → `uv run dg check defs` → deploy → verify no queued runs reference old partitions (`bulk_actions`/`run_tags key='dagster/backfill'`). Cumulative DuckDB files and S3 data are untouched by the definition change; DuckDB files are deleted only after the first two green weekly runs of the new shape (D1).
5. **Schedule:** keep `10 5 * * 0` Europe/Belgrade, stopped-by-default until validated; the run is now "sweep + process whatever is new" (~140 filings/week steady state).
6. Keep `BackfillPolicy`/pool discipline only where state is still shared: the `esef_arelle` pool for parse workers; a single `esef_filings` pool if any local state remains.

# Phase 3 — Document-evidence consolidation (target 5)

**Separate detailed plan.** Design contract:

1. Merge `contact_candidates.py` + `website_candidates.py` into `candidates.py`: one `_visible_blocks`, one `_ROLE_PATTERNS`, one parser config, kind-tagged emitters (~1,436 → ~900 lines). Reuse `dagster_v3/domains.py` for host/eTLD+1 (extend it with `include_psl_private_domains` rather than keeping a second `TLDExtract`); adopt `contact_extraction._rejected_as_abbreviation` in place of the one-entry `_KNOWN_FALSE_DOMAINS` stub.
2. Parse each report member ONCE into an lxml tree in `parse_esef_report_package`; pass the tree to contacts/websites/visible-sections/language extractors (removes 2 full parses + 1 duplicate block walk per file on the hottest asset). Hoist `_css_rules` (computed twice in `visible_sections.py`).
3. Collapse the disclosure chain: emit disclosure rows from the artifact-processing worker alongside fact rows (disclosures are a pure function of `raw_value`); delete `disclosure_assets.py`'s 3 assets and both S3 round-trips. Gate the swap on a row-count parity check against the current `esef_fact_disclosures` table.
4. Hoist `_json_text` (4 copies), `_mapping` (3), `_validated_sha256` (2) into `esef_filings/_validation.py`; share the stage-manifest reader; drop the `_ARELLE_SESSION_LOCK` (each pool worker is its own process); extract a `partitions.py`-style module boundary so `assets` ↔ `segment_assets` lose their circular import.

# Phase 4 — LLM enrichment collapse (target 7, Decision D4)

**Separate detailed plan**, executed at the next `PROMPT_VERSION` bump (the natural moment to abandon the old S3 keyspace). Design contract:

1. One asset, one job (manual/paid, unchanged trigger discipline). Selection = the existing `(source_document_id, model_name, prompt_version)` LEFT JOIN anti-join; the `preferred_filing_artifact_rank` inner window is deleted after confirming `SELECT count(), uniqExact(source_document_id) FROM corpscout.esef_source_documents` are equal in prod.
2. Kill the JSON trombone: the fresh-call path builds rows from the typed `EsefLlmEnrichmentResult`; only the reuse-from-S3 path parses JSON (re-validated with `model_validate`). Merge `_no_evidence_row` into `_information_row`.
3. Drop the separate S3 request object; fold the ~6 unique request fields into the response artifact; flatten the response key to `schema=v1/request_sha256=<h>/artifact.json`. `llm_request_object_key` columns default to `''`.
4. The LLM asset writes ONLY `esef_document_company_information` to CH. Four independent `esef_filings` assets project `company_description_observations`, `esef_document_people`, `esef_document_business_items`, and `esef_document_group_relationships` directly from it. The deterministic `source_record_uid` carried by the input row is sufficient; these projections do not depend on materializing the generic source-record spine first. Rename or split `reprocess_existing_without_model` into `reselect_processed` + `force_model_call`.
5. Disambiguate `extraction_status='reused'` (parsed-artifact reuse vs LLM-response reuse) across the two tables while touching them.

---

## Execution order & verification summary

```
Phase 0 (G0+G1 gates, prod)  ──►  Phase 1b (OIM retirement)  ──►  Phase 2 (de-partition core)
Phase 1a (bug fixes)  — anytime, now                              ──►  Phase 3 (evidence consolidation)
                                                                  ──►  Phase 4 (LLM, at PROMPT_VERSION bump)
```

After every task: `uv run dg check defs` + the named pytest files. After each phase: one full `uv run pytest tests/ -k esef -v`. Never combine a `partitions_def` change with logic changes in one commit.
