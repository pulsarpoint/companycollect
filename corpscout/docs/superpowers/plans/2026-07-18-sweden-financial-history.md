# Sweden Financial History (Prior-Year Comparatives) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract the prior-year comparatives (flerårsöversikt — the mandatory multi-year overview) already stored in the 244.7M Swedish XBRL facts into a per-company-year `se_financial_history` table, multiplying SE history depth without new downloads — a company with one 2024 filing yields ~2021–2024 figures — and surface the extra years on the backoffice detail page with an honest "(from later filing)" marker.

**Architecture:** New CH table `corpscout.se_financial_history` (migration 000140) built by a CH→CH asset in `defs/sweden_financial`: facts with context ids matching `(?i)^(period|balans)(\d+)$` map to `target_year = report.fiscal_year − n` for the flerårsöversikt concept set; a **per-statement trust guard** drops any statement whose comparatives disagree with a direct filing by >0.5% on ANY overlap year (kills the measured 2.8% of restated/odd-fiscal-year statements); per (company, year) the newest trusted observation wins with `n = 0` (reported) preferred over `n ≥ 1` (comparative). USD via the `exchange_rates` join pattern the SE metrics builder already uses, at each target year's shifted period end. The backoffice SE `financialsQuery` unions comparative-only years beneath the reported ones.

**Tech Stack:** dagster_v3 (ClickHouse CH→CH, golang-migrate, pytest), backoffice (registry SQL + FinancialsSection marker, live vitest).

## Global Constraints

- **Empirical grounding (measured 2026-07-18, sample n=9,305 overlap pairs):** period-1 comparatives vs direct prior filings — 70.8% exact, +26.4% within 0.5% (presentation rounding to thousands), 2.8% genuinely off. The trust guard's tolerance is therefore **relative 0.5%**; a statement with ≥1 overlap-year disagreement beyond that loses ALL its comparative rows. The build must emit a metadata metric: overlap agreement rate (target ≥ 97% pre-guard, ≥ 99.5% post-guard).
- Context-year inference: `n` from `(?i)^(period|balans)(\d+)$` (covers ~100% of monetary facts: `balans`/`period` lower 1.31M statements + upper 361k; `INGAENDE*` opening-balance contexts and the 6 `period_`-style statements are EXCLUDED). `n = 0` → observation `'reported'`, `n ≥ 1` → `'comparative'`; cap `n ≤ 4` (flerårsöversikt max) — larger n is noise, drop.
- Document scope mirrors the metrics inclusion rule (post-49fb7fb8): statements only — exclude `/ar/rar%` entirely and `/misc/race/` docs with zero mapped facts.
- Concept set: the implementer DISCOVERS exact local names by frequency on `n ≥ 1` contexts (expected: `Nettoomsattning` → revenue; a result-after-financial-items concept (verify exact spelling, e.g. `ResultatEfterFinansiellaPoster`); `Soliditet` → solidity %; plus a balance-total concept on `balans{n}` if one appears at scale). Contract columns: `revenue_amount_original/usd`, `result_after_financial_items_amount_original/usd`, `solidity_pct`, plus `total_assets_amount_original/usd` IF the discovery finds a balance-total concept covering ≥30% of statements (else omit the columns from population, keep them in schema as NULL).
- Schema (exact, migration 000140): `company_id String, fiscal_year Int32, observation LowCardinality(String), source_statement_key String, source_fiscal_year Int32, currency LowCardinality(String), revenue_amount_original Nullable(Float64), revenue_amount_usd Nullable(Float64), result_after_financial_items_amount_original Nullable(Float64), result_after_financial_items_amount_usd Nullable(Float64), solidity_pct Nullable(Float64), total_assets_amount_original Nullable(Float64), total_assets_amount_usd Nullable(Float64), resolved_at DateTime64(3, 'UTC')` — `ENGINE = MergeTree ORDER BY (company_id, fiscal_year)`.
- Selection per (company_id, fiscal_year): `ORDER BY observation = 'reported' DESC, source_fiscal_year DESC, source_statement_key DESC` → `LIMIT 1 BY company_id, fiscal_year` (reported beats comparative; newest filing wins ties; deterministic).
- USD: join `corpscout.exchange_rates` the way `sweden_financial/metrics.py` does, rate date = `addYears(report_period_end, -n)` with that pattern's nearest-rate semantics. Comparative USD is approximate by nature — acceptable, documented.
- Build = stage + `EXCHANGE TABLES` with non-empty guard (the repo standard); asset joins the existing sweden_financial job selections so scheduled runs refresh it; sanity guard: every `fiscal_year` within `[source_fiscal_year − 4, source_fiscal_year]`.
- **Ordering:** Tasks 1–2 (code) can land any time; **Task 3 (materialize) MUST wait for the in-flight 2026 archive re-ingest to complete** (fiscal-2025 statements are landing — building history before it finishes would immediately be stale). The controller confirms re-ingest completion before dispatching Task 3.
- Backoffice: fidelity rule — comparative years render with their available columns and a muted "(from later filing)" marker on the year; missing columns show the standard dashes; `FinancialYearRow` gains optional `observation`; net_result stays NULL for comparative years (result-after-financial-items is NOT net result — no silent relabeling).
- dagster_v3 conventions bind (uv run, explicit-path commits, EXPECTED_MIGRATIONS, dg check, TDD); dev 5183 USER-OWNED; trailer `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.

## Out of scope (logged)

- Extending other countries' history the same way (NO/FI/EE filings may carry comparatives too — evaluate after SE proves the pattern).
- Employees/equity per comparative year (not in the flerårsöversikt).
- ESEF ingestion for listed groups; representative/beneficial-owner APIs.

---

### Task 1: Migration 000140 + history SQL module + tests

**Files:**
- Create: `corpscout/clickhouse/migrations/000140_corpscout_se_financial_history.up.sql` / `.down.sql`
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_financial/history.py`
- Modify: `corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py` (EXPECTED_MIGRATIONS + coverage test, fi-pattern)
- Test: `corpscout/services/dagster_v3/tests/test_sweden_financial_history.py`

**Interfaces:** `history.py` exports `SE_FINANCIAL_HISTORY_TABLE`, `SE_FINANCIAL_HISTORY_COLUMNS` (schema order), `HISTORY_CONCEPTS` (the discovered concept→column map), and `build_history_insert_sql(qualified_stage_table: str) -> str` (Task 2's asset wraps it; explicit column list).

- [ ] **Step 1: Concept discovery (read-only CH), recorded in the module as constants with a dated comment**: frequency query over `n ≥ 1` contexts grouped by `concept_local_name`; pick the revenue/result/solidity (+optional balance-total) names per the Global Constraints rule; paste the frequency evidence into the report.
- [ ] **Step 2 (TDD):** unit tests first — the built SQL contains: the case-insensitive context regex with n extraction and `n <= 4` cap; the document-scope exclusions; the trust-guard join (statements disqualified on >0.5% relative overlap disagreement); the LIMIT 1 BY selection with the exact ORDER BY; the exchange-rates join; every `SE_FINANCIAL_HISTORY_COLUMNS` alias. Then implement. Migration + coverage test per the fi pattern.
- [ ] **Step 3:** `uv run pytest tests/test_sweden_financial_history.py tests/test_clickhouse_migrations.py -q` green; apply migration 000140 (local migrate CLI as before), verify SHOW CREATE. Commit (explicit paths): `feat(dagster): sweden financial history table and comparative extraction sql`.

---

### Task 2: Build asset + wiring

**Files:**
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_financial/assets.py`
- Test: extend `corpscout/services/dagster_v3/tests/test_sweden_financial_history.py` (+ the assets wiring test file)

- [ ] Asset `se_financial_history_clickhouse` (CH→CH, no pool): deps on `sweden_financial_reports_clickhouse`, `sweden_financial_facts_clickhouse`, `sweden_financial_metrics_clickhouse`; stage-AS-target + explicit-column INSERT via `build_history_insert_sql` + non-empty guard + EXCHANGE + finally-drop; MaterializeResult metadata: total rows, reported vs comparative counts, distinct companies, overlap-agreement rate (pre/post guard), statements disqualified by the guard.
- [ ] Add the asset to the module's job selections so the existing schedules refresh it after metrics; register in `defs`. Wiring tests (asset exists, deps pinned, in-job). `uv run dg check defs` clean. Commit: `feat(dagster): sweden financial history build asset`.

---

### Task 3: Materialize + verify (operational — GATED on the 2026 re-ingest completing)

- [ ] Confirm with the controller that the archive re-ingest + metrics/summary/companies_all rebuilds are done. Then `./scripts/dagster-dev.sh` + `uv run dg launch --assets se_financial_history_clickhouse`.
- [ ] Verify (verbatim): total rows; reported count ≈ post-reingest metrics statement-years; comparative-only years added (expect ≥ 1M new company-years); history-depth distribution before/after (baseline: 78% of companies 2+, 1-year companies 114k — expect the 1-year bucket to collapse); overlap agreement ≥ 99.5% post-guard; spot-checks: a single-filing company now showing 3–4 years; `5561632232` gaining pre-2023 years; guard metadata sane. Clean up.

---

### Task 4: Backoffice — extended SE history on the detail page

**Files:**
- Modify: `corpscout/services/backoffice/app/lib/countries.ts` (se financialsQuery)
- Modify: `corpscout/services/backoffice/app/lib/queries.server.ts` (`FinancialYearRow.observation?: string`)
- Modify: `corpscout/services/backoffice/app/components/detail/financials-section.tsx` (year marker)
- Test: `corpscout/services/backoffice/tests/queries.server.test.ts`, `app/lib/countries.test.ts` (canonical-alias list if extended)

- [ ] se `financialsQuery` becomes a UNION: the existing metrics query (reported years, full columns, `'reported' AS observation`) + history rows where `observation = 'comparative'` AND the year is absent from the company's metrics (`fiscal_year NOT IN (...)` subquery or `LIMIT 1 BY fiscal_year` with reported-first ordering across the union — pick the cleaner; canonical aliases preserved; comparative rows: revenue orig/usd filled, net_result/total-assets-per-availability, employees NULL).
- [ ] Component: when `observation === 'comparative'`, the Year cell gains a muted `*` with a legend line under the table ("* from a later filing's multi-year overview"). Chart includes comparative revenue (it's the same measure) — result bars only where present.
- [ ] Tests: live test on a company with comparative-extended history (find one whose earliest year comes from history — assert extra years + observation flag); canonical sweep still green. Full `pnpm typecheck && pnpm test`; SSR check on a throwaway port. Commit: `feat(backoffice): sweden multi-year history from filing comparatives`.
