# ESEF Backfill Completion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. This is an operational runbook-style plan: most tasks are prod checks and UI actions, not code.

**Goal:** Drive the ESEF fact parse backfill to full corpus coverage, then run the post-completion gate sequence: metrics rebuild → serving verification → re-enable the weekly schedule and freshness expectations.

**Architecture:** Facts flow `esef_document_artifacts_s3` → four DuckDB parse assets → the non-subsettable `esef_parsing_clickhouse` multi-asset (emits `esef_facts_clickhouse`, `esef_document_contact_candidates_clickhouse`, `esef_document_concept_labels_clickhouse`, `esef_disclosures_clickhouse`). All are weekly-partitioned on the filing's `processed_at` week (`ESEF_PROCESSED_WEEK_PARTITIONS`, start 2023-01-01, Sunday cron, `assets.py:82-88`), backfilled one partition per run via `esef_filings_backfill_job` (`assets.py:1490-1493`, 11-asset selection `:1471-1483`). `esef_financial_metrics_clickhouse` is deliberately in **no job or schedule** (`assets.py:1369-1391`; excluded from every selection) — it must be materialized manually after the corpus lands. The weekly schedule `esef_filings_refresh_weekly` is `STOPPED` in code (`assets.py:1530`, "until Task 8 validates a live run and flips it on in the UI").

**Tech Stack:** Dagster (prod UI on companycollect), ClickHouse, Dagster Postgres (`ppoint-postgres`, db `dagster`).

**Spec:** State verified 2026-08-31:
- `esef_facts` covers 6,815 of 25,061 cataloged filings (~27%); by `date_added` week: 64 weeks with zero facts, 85 partial, 34 complete; `max(resolved_at)` = 2026-08-26.
- Backfill `eikemvvu` = COMPLETED_SUCCESS 2026-08-26 (bounded partition set over `esef_document_artifacts_s3`); **new backfill tranches were REQUESTED on 2026-08-31** (`jwwtmzxm`, `tqalsdfc`, `zhfwjhqx`, `vbcryjlz`) — the owner confirms filings are still processing. This plan therefore MONITORS and finishes; it does not restart anything unless monitoring shows a genuine stall.
- Reference company: Svenska Handelsbanken (5020077862) — FY2021/2022/2024 filings still show `source_fact_count: 0` on the Financial tab ("Filed years without standardized values").

## Global Constraints

- Heavy runs execute on the prod Dagster server, launched from its UI — never relocated in-flight, never killed to move elsewhere.
- One partition per run (`BackfillPolicy.multi_run(max_partitions_per_run=1)`); parse pool bounded (`parse_workers` default 4, max 8).
- Completion gates from `src/dagster_v3/defs/esef_filings/docs/esef-parsing-migration-plan.md:47-56`: every DuckDB file carries a completed `_partition_status` row; ClickHouse publication refuses mismatched stage/target counts; re-materializing a week is idempotent.
- Do not enable the schedule before the backfill drains — a scheduled run and a backfill run on the same partition contend for the pools.

---

### Task 1: Inventory what is in flight

- [ ] **Step 1:** List active/pending ESEF backfills and runs:

```bash
ssh companycollect 'docker exec ppoint-postgres psql -U dagster -d dagster -tAc \
  "SELECT key, status, timestamp FROM bulk_actions WHERE status IN (\$\$REQUESTED\$\$, \$\$IN_PROGRESS\$\$) ORDER BY timestamp"'
ssh companycollect 'docker exec ppoint-postgres psql -U dagster -d dagster -tAc \
  "SELECT status, count(*) FROM runs WHERE status IN (\$\$QUEUED\$\$, \$\$STARTED\$\$, \$\$STARTING\$\$) GROUP BY 1"'
```

- [ ] **Step 2:** In the prod Dagster UI, confirm which asset selection those backfills target (expect the parse chain: `esef_document_artifacts_s3` and/or `esef_filings_backfill_job` assets) and which processed-week ranges they cover.
- [ ] **Step 3:** If ANY requested backfill has sat with zero launched runs for >2h, run `scripts/dagster-health-check.py --fix` on the prod host to free leaked pool slots (per the simplification plan's G0 gate), then re-check.

### Task 2: Monitor to completion

- [ ] **Step 1:** Track coverage with this query (the same shape as the `per_week_facts_coverage` check, `partitioned_publish.py:304-342`); repeat daily or per-tranche:

```sql
SELECT
  countIf(with_facts = 0) AS weeks_missing_all,
  countIf(with_facts > 0 AND with_facts < filings) AS weeks_partial,
  countIf(with_facts >= filings) AS weeks_complete,
  sum(filings) AS total_filings,
  sum(with_facts) AS filings_with_facts
FROM (
  SELECT toStartOfWeek(f.date_added, 1) AS wk, count() AS filings,
         countIf(f.fxo_id IN (SELECT DISTINCT fxo_id FROM corpscout.esef_facts)) AS with_facts
  FROM corpscout.esef_filings f
  GROUP BY wk
);
```

Baseline 2026-08-31: `64 / 85 / 34 / 25061 / 6793`.

- [ ] **Step 2:** When the in-flight tranches finish, enumerate remaining unmaterialized weeks in the UI (asset partition view for `esef_facts_clickhouse`) and launch the next `esef_filings_backfill_job` tranche over exactly those weeks. Repeat until every closed week `2023-01-01 → last complete UTC week` is materialized.
- [ ] **Step 3:** For partial weeks (facts < filings), re-materialize the week — the parse fails an incomplete partition rather than silently skipping (validation contract), and a re-run replaces only that week idempotently. Investigate any week that fails twice: pull the run error; the known poison-document class (bracketed URL text) was fixed 2026-08-26 (`47e4df0c`) — a new class needs its own fix before that week can pass.

### Task 3: Post-completion asset-check gates

- [ ] **Step 1:** Execute both checks on `esef_facts_clickhouse` from the UI:
  - `fxo_id_single_processed_week` (ERROR, `partitioned_publish.py:250-296`) — MUST pass; a failure means cross-week fact duplication and the metrics rebuild would double-count. Fix duplicated fxo_ids before proceeding (the check's sample SQL names them).
  - `per_week_facts_coverage` (WARN, threshold 0.90, `:359-430`) — expect ≥0.90 every week; the worst-week list bounds any residue.
- [ ] **Step 2:** Confirm final corpus: `SELECT uniqExact(fxo_id) FROM corpscout.esef_facts` ≈ 25,061 (minus filings legitimately without JSON facts — compare `countIf(has_json_facts=1)` on `esef_filings`).

### Task 4: Metrics rebuild + serving verification

- [ ] **Step 1:** Materialize `esef_financial_metrics_clickhouse` from the prod UI (manual-only asset; atomic stage+EXCHANGE; `allow_shrink` stays False — the row count only grows).
  - If the metric-mapping-expansion plan (`2026-08-31-esef-metric-mapping-expansion.md`) has been deployed, this single rebuild picks up both the new corpus and the new mapping.
- [ ] **Step 2:** Verify serving (view updates instantly; no asset to run):

```sql
SELECT fiscal_year, currency, revenue_amount_original, source_fact_count
FROM corpscout.se_financials_esef_current
WHERE company_id = '5020077862' ORDER BY fiscal_year;
-- Expect FY2021/2022/2024 rows now populated (source_fact_count > 0), not just FY2023.
```

- [ ] **Step 3:** Spot-check the backoffice page `http://localhost:5183/admin/se/company/5020077862/financial` — "Filed years without standardized values" should shrink to zero (or only years whose filings genuinely lack the mapped concepts). `se_companies_serving` refreshes itself within 15 min (refreshable MV, migration 000335); force with the `sweden_companies_current_clickhouse` asset if needed.

### Task 5: Re-enable steady state

- [ ] **Step 1:** In the prod Dagster UI, START `esef_filings_refresh_weekly` (code default stays STOPPED per `assets.py:1530` and its pinning test `tests/test_esef_filings_assets.py:944-951` — the flip is a UI action, exactly as the inline comment prescribes).
- [ ] **Step 2:** Confirm the next Sunday run processes the last closed week end-to-end (all four ClickHouse outputs land, checks pass).
- [ ] **Step 3:** Re-enable/verify freshness expectations for the ESEF leaves in `common/clickhouse_checks.py` that were relaxed during the migration (the metrics leaf at `:69` has cadence `None`; see Step 4).
- [ ] **Step 4 (owner decision, small code change):** `esef_financial_metrics_clickhouse` currently never refreshes automatically — filings parsed by the weekly refresh will not reach the Financial tab until someone rebuilds metrics by hand. Proposal: append it to `ESEF_FILINGS_REFRESH_SELECTION` (`assets.py:1464-1466`) so the weekly job rebuilds metrics after each parse (cheap: single atomic SELECT over `esef_facts`), and give its freshness leaf a WEEKLY cadence. Present this to the owner; implement only on approval (edit the selection + `tests/test_esef_filings_assets.py` job-selection pins, commit `feat(esef): rebuild financial metrics in the weekly refresh`).

## Self-Review

- Monitors rather than restarts (owner: "esef filings are still processing"); restart criteria explicit (Task 1 Step 3, Task 2 Step 3). ✔
- Gate order preserved: parse → checks → metrics → serving → schedule. ✔
- The one code change is fenced behind an explicit owner decision. ✔
