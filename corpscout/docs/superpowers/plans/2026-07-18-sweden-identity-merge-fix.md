# Sweden Identity-Merge Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Collapse the ~745k phantom duplicate Swedish companies created by the unstripped `16` century prefix: SCB keys legal entities as 12-digit `16`+orgnr while Bolagsverket uses the bare 10-digit orgnr, and the `sweden_company` merge joins on raw digits — so the same company appears twice (a Bolagsverket copy without industries + an SCB copy with them). Normalize the identity at derivation, rebuild the chain, and align the backoffice registry expressions that currently work around the broken id space.

**Architecture:** One normalization rule applied at every identity-derivation site in `sweden_company/normalized_duckdb.py`: after stripping non-digits, a 12-digit value starting `16` becomes its last 10 digits (the orgnr); 12-digit person-keyed values (`19`/`20` prefixes — sole traders) stay as-is. That single rule collapses the twins in the `companies` merge (Bolagsverket legal identity + SCB economics on ONE row, per the existing coalesce preferences), keys `company_industry_codes` and `company_addresses` consistently, and — as a free consequence — makes `se_companies.company_id` finally match `se_financial_metrics.company_id` (Bolagsverket-format 10-digit). Then: materialize the sweden chain + `companies_all`, and update the backoffice registry's SE `financialsAggregates` entry (its `substring(…,3)`/`startsWith('16')` workaround would MISS everything against fixed data — data and registry must flip in the same wave).

**Tech Stack:** dagster_v3 (DuckDB SQL inside Python, pytest, `uv run`), ClickHouse (no migration — schemas unchanged), backoffice (TypeScript registry + live vitest).

## Global Constraints

- **The normalization rule (exact):** `digits = regexp_replace(coalesce(<raw>, ''), '[^0-9]', '', 'g')`; identity = `CASE WHEN length(digits) = 12 AND digits LIKE '16%' THEN substring(digits, 3) ELSE digits END`. Applied to BOTH sources (the Bolagsverket file also mixes 10- and 12-digit identities — 612k BV rows are 12-digit today, some `16`-prefixed), at ALL 8 derivation sites in `normalized_duckdb.py`: companies (bolagsverket + scb CTEs), addresses (both branches), industries (Ng1..Ng5 branches). Implement it ONCE as a Python helper returning the SQL fragment (e.g. `_identity_sql(raw_column: str) -> str`) and interpolate it everywhere — 8 hand-copies of the CASE is how the next bug happens. `16` can never prefix a real personnummer (no one born in the 1600s files Swedish taxes), so the rule is safe for sole traders.
- Merge preference rules (Bolagsverket-first coalesces, BV-only status/dissolution/activity_description) stay EXACTLY as they are — this fix changes only the join key. SCB-only rows keep today's `status='active'` hardcoding (pre-existing; logged follow-up to consult SCB's FtgStat/JEStat).
- No ClickHouse migration: `se_companies`/`se_company_addresses`/`se_industries` schemas are unchanged; the exports atomically replace content as always.
- **Deploy/rebuild ordering is load-bearing:** dagster code fix → sweden chain materialization → `companies_all` rebuild → backoffice registry change + tests. Between the data flip and the registry change, SE financial-aggregate industry views are briefly broken on a live dev server (old `startsWith('16')` exprs against new 10-digit keys) — acceptable dev window, do NOT leave it overnight: Tasks 2 and 3 run back-to-back.
- Expected magnitudes for verification (measured 2026-07-18): se_companies 4,135,692 today; −744,975 known 10-digit-BV↔`16`-SCB twins ⇒ ≈3,390,717, MINUS any additional BV-internal 12-digit-`16` collisions the fix also collapses (unknown count — measure, don't assume). `se_industries` row count unchanged (2,443,310) but its `16`-prefixed keys (1,465,716 rows / 1,014,276 companies) become 10-digit; `19`/`20` person keys (940,551 + 37,043 rows) unchanged. Twin proof-case: org 5565257747 must end as ONE row carrying the Bolagsverket name AND an industry.
- dagster_v3 conventions bind: `uv run`, explicit-path commits, `dg check defs`, TDD, never `+leaf` (explicit asset lists). Trailer `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`. Dev server 5183 USER-OWNED. ClickHouse read-only outside the pipeline.

## Ground truth (audited 2026-07-18; file:line refs)

- Bug sites: `sweden_company/normalized_duckdb.py` — `_replace_companies_table` :73-180 (bolagsverket CTE :79-85, scb CTE :114, union+left-joins on `company_id` :130-177, `registration_number = company_id` :137); `_replace_company_addresses_table` :183-265 (BV :189-194, SCB :221, union all :260-262 — post-fix a merged company keeps BOTH address rows under one id, which is correct); `_replace_company_industry_codes_table` :268-349 (identical SCB expr at :278/:289/:300/:311/:322). Count helpers `_bolagsverket_company_count` :417-430 / `_scb_company_count` :433-441 recompute the raw expression — update them consistently or their coherence checks lie.
- Tests: `tests/test_sweden_company_normalized_duckdb.py` — fixture builder `_create_raw_tables` :384 uses only 10-digit ids (`'5560000000$ORGNR-IDORG'` :416/:439, PeOrgNr `'5560000000'`/`'9999999999'` :507/:533); the 3-way scenario test :14 pins exact rows/counts. NO fixture exercises a 12-digit `16` id — that's why the bug was invisible.
- Chain & job: assets `sweden_company_raw_snapshot_s3` → `sweden_company_raw_duckdb` → `sweden_company_normalized_duckdb` (pool `sweden_company_duckdb`) → `sweden_company_{companies,addresses,industries}_clickhouse`; job `sweden_company_refresh_job` (heavy-bulk); schedule `sweden_company_refresh_weekly` cron `15 6 * * 1` is `default_status=STOPPED` (flag, don't change here). Fix needs NO raw re-download: relaunch `sweden_company_normalized_duckdb,sweden_company_companies_clickhouse,sweden_company_addresses_clickhouse,sweden_company_industries_clickhouse` then `companies_all_clickhouse`.
- Downstream consumers: `companies_all/sql.py` se entry :112-139 — needs NO functional change (`registration_number` and `c.company_id` both normalize; joins simply start matching) but its docstring :15-34 describes se `company_id` as a distinct synthetic id — update the wording. `company_financials_latest/sql.py` se reads `se_financial_metrics` (filename-parsed 10-digit ids, independent) — untouched; the summary table itself doesn't change. `common/clickhouse_checks.py` — table/asset pairings only, untouched.
- Backoffice: `app/lib/countries.ts` se `financialsAggregates.nace` (~:328-335) — `companyKeyExpr: "substring(toString(company_id), 3)"` + `filterExpr: "... AND startsWith(toString(company_id), '16')"` are the workaround that must become `companyKeyExpr: "toString(company_id)"` and drop the startsWith conjunct (keep `is_primary = 1 AND nace_rev2_class_code != ''`). se `financialsLatest.companyKeyExpr: "company_id"` and `industryJoinKeyExpr: "company_id"` stay (they become MORE correct). The parity sweep + financial-aggregates tests derive from the registry — they follow automatically once data + registry agree.
- Expected wins to verify at the end: SE industry coverage in companies_all (39.1% → measure; active-only 62.6% → expect a large jump), SE has_financials matches (381,495 → expect ≈500k of 525,494 summary rows), sweden financial-aggregates NACE coverage (417,119 → expect similar-or-better via the clean join), README count 116,333,029 → update to the new verified total.

## Out of scope (logged)

- SCB-only rows' hardcoded `status='active'` (consult FtgStat/JEStat) — pre-existing.
- Enabling `sweden_company_refresh_weekly` (STOPPED today — SE data refreshes only manually; recommend flipping to RUNNING as a separate decision).
- Exposing `se_company_addresses` in the backoffice (4.34M rows exist, no registry addressQuery yet — natural follow-up, better post-fix since addresses unify per company).
- Legal-form vocabulary unification (ORGFO codes vs SCB numeric on merged rows — merge preference keeps BV codes; facet vocabulary mixing is pre-existing).

---

### Task 1: The normalization fix + tests (dagster)

**Files:**
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/normalized_duckdb.py`
- Modify: `corpscout/services/dagster_v3/tests/test_sweden_company_normalized_duckdb.py`
- Modify (docstring only): `corpscout/services/dagster_v3/src/dagster_v3/defs/companies_all/sql.py`

**Interfaces:** No signature changes — `replace_sweden_company_normalized_tables(...)` and the exporters keep their contracts; only the derived `company_id`/`registration_number` VALUES change.

- [ ] **Step 1 (TDD — prove the bug first):** extend `_create_raw_tables` with the twin scenario: a Bolagsverket row `organisationsidentitet = '5565257747$ORGNR-IDORG'` (name `'Twin AB med firma Twin AB'`, a verksamhetsbeskrivning, registreringsdatum) AND an SCB row `PeOrgNr = '165565257747'` (Namn `'TWIN AB'`, JurForm `'49'`, an `Ng1` SNI code, RegDatKtid). Plus: an SCB sole-trader row `PeOrgNr = '195001011234'` (with an Ng1) and a Bolagsverket 12-digit row `organisationsidentitet = '165565258888'` with a 10-digit BV sibling `'5565258888$...'` (the BV-internal collision case). New tests assert:
  - ONE companies row for `5565257747`: `company_id = registration_number = '5565257747'`, legal_name from Bolagsverket, `activity_description` present, `legal_form_code` from Bolagsverket, incorporation date BV-preferred;
  - `company_industry_codes` keys that company's SNI rows as `'5565257747'`;
  - `company_addresses` carries BOTH sources' rows under `'5565257747'`;
  - the sole trader keeps `company_id = '195001011234'` and keeps its industry row under that 12-digit key;
  - the BV-internal pair collapses to one `'5565258888'` row;
  - existing 10-digit-only fixtures keep their exact current expectations (update the pinned counts in the 3-way scenario test to include the new fixture rows — deliberately, listing old→new counts in the test comments).
  Run: `uv run pytest tests/test_sweden_company_normalized_duckdb.py -q` → the new tests FAIL against current code (twin asserts see two rows) — capture the RED output.
- [ ] **Step 2: implement.** Add near the top of `normalized_duckdb.py`:

```python
def _identity_sql(raw_column: str) -> str:
    """Normalized Swedish organization identity from a raw source id column.

    Strips non-digits, then removes the '16' century prefix SCB (and some
    Bolagsverket rows) put in front of a 10-digit organisationsnummer in
    12-digit PeOrgNr form. 12-digit person-keyed ids (19/20 birth-century
    prefixes, sole traders) pass through unchanged -- '16' can never prefix
    a real personnummer. Without this, the same company appears once per
    source (~745k phantom duplicates measured 2026-07-18).
    """
    digits = f"regexp_replace(coalesce({raw_column}, ''), '[^0-9]', '', 'g')"
    return (
        f"case when length({digits}) = 12 and {digits} like '16%' "
        f"then substring({digits}, 3) else {digits} end"
    )
```

Replace all 8 inline `regexp_replace(...)` identity expressions with `{_identity_sql('organisationsidentitet')}` / `{_identity_sql('PeOrgNr')}` interpolations (companies ×2, addresses ×2, industries ×5 — count them; also the two count-helper functions `_bolagsverket_company_count`/`_scb_company_count`). Keep each site's surrounding dedup (`row_number` partitions now partition by the normalized id — that's exactly what collapses the BV-internal pairs).
- [ ] **Step 3:** `uv run pytest tests/test_sweden_company_normalized_duckdb.py tests/test_sweden_company_clickhouse.py tests/test_sweden_company_assets.py -q` → all green; `uv run dg check defs` clean. Update the `companies_all/sql.py` docstring lines about se's `company_id` (it now equals `registration_number`'s id space: 10-digit orgnr for legal entities, 12-digit person ids for sole traders; the industries/financials joins rely on that).
- [ ] **Step 4: Commit** (explicit paths, all three files): `fix(dagster): normalize swedish 16-prefixed organization identities`.

---

### Task 2: Materialize + verify (operational)

**Files:** none (report carries evidence)

- [ ] **Step 1:** `./scripts/dagster-dev.sh` (background); relaunch WITHOUT raw re-download: `uv run dg launch --assets sweden_company_normalized_duckdb,sweden_company_companies_clickhouse,sweden_company_addresses_clickhouse,sweden_company_industries_clickhouse` (foreground, generous timeout — the normalize rebuilds from the existing raw DuckDB). Then `uv run dg launch --assets companies_all_clickhouse` (its per-country count guard must pass with the NEW se count).
- [ ] **Step 2: Verify** (read-only, all outputs verbatim in the report):
  - `SELECT count(), uniqExact(company_id), uniqExact(registration_number) FROM corpscout.se_companies` — all three equal; total ≈ 3.39M minus the BV-internal collapses (record the exact number and the delta from 4,135,692).
  - Twin proof: `SELECT company_id, legal_name, legal_form_code, scb_company_id_raw != '', bolagsverket_company_id_raw != '' FROM corpscout.se_companies WHERE registration_number IN ('5565257747','165565257747')` → exactly ONE row, id `5565257747`, both source flags set.
  - Zero remaining phantom pairs: the Task-diagnosis query (`bv-only rows whose concat('16', registration_number) exists as an scb row`) returns 0.
  - `se_industries`: count still 2,443,310; `substring(company_id,1,2)` distribution shows NO `16` bucket (moved to 10-digit), `19`/`20` unchanged.
  - Industry join coverage: companies_all SE `countIf(industry_code != '')` and the active-only percentage (record before/after: 39.1% / 62.6% baselines).
  - Financials: `SELECT countIf(has_financials = 1) FROM corpscout.companies_all WHERE country_code = 'se'` (baseline 381,495 — expect ≈500k).
  - `SELECT count() FROM corpscout.companies_all` — new total (baseline 116,333,029; expect ≈ −745k−X).
- [ ] **Step 3:** Clean up the dev instance you started. No commits. IMPORTANT: proceed to Task 3 immediately (registry is now out of sync with the data).

---

### Task 3: Backoffice registry alignment + gates

**Files:**
- Modify: `corpscout/services/backoffice/app/lib/countries.ts` (se `financialsAggregates.nace` only)
- Modify: `corpscout/services/backoffice/README.md` (row-count references + a line in the companies_all notes)

- [ ] **Step 1:** se `financialsAggregates.nace` becomes:

```ts
nace: {
  industriesTable: "se_industries",
  companyKeyExpr: "toString(company_id)",
  naceCodeExpr: "nace_rev2_class_code",
  filterExpr: "is_primary = 1 AND nace_rev2_class_code != ''",
},
```

(Drop the `substring`/`startsWith('16')` workaround and its stale comment; add one line noting ids are normalized at the dagster layer since 2026-07-18.)
- [ ] **Step 2: Gates.** `pnpm typecheck`; full `pnpm test` — the parity sweep re-derives from the registry against the rebuilt data (se count parity, field parity, financials parity all live); `tests/financial-aggregates.server.test.ts` (real-estate division still spans no+se; global overview intact). Record the new SE numbers the tests observe. If any live test pins a stale SE magnitude that legitimately moved (counts shrank ~18%), update it DELIBERATELY with a dated comment — list every such change in the report.
- [ ] **Step 3:** README: update the companies_all row-count reference to the new verified total, and add one line to the intentional-changes/notes area: Swedish identities are normalized (16-prefix stripped) as of 2026-07-18 — ~745k phantom duplicates collapsed; SE counts dropped accordingly.
- [ ] **Step 4:** Quick SSR sanity on a throwaway port: `/companies?f_country=se&f_has_financials=true` total reflects the improved match; `/financials/country/se` division breakdown healthy; a merged company's detail page (`/company/se/5565257747`) shows BV name + industries. Kill your server.
- [ ] **Step 5: Commit** (explicit paths): `fix(backoffice): drop swedish id-prefix workaround after identity normalization`.
