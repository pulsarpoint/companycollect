# Company People (SE Officers + Cross-Country Search) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract directors/officers/auditors from the Swedish XBRL signature facts already stored in `se_financial_facts` into `se_company_officers`, expose a cross-country people-search layer `company_people_all`, and show a Management section (+ same-name matches) on the backoffice detail page.

**Architecture:** No document re-parsing — everything derives in ClickHouse SQL from `corpscout.se_financial_facts` (the signature block of every filing is tagged: first name / surname / role as consecutive facts in document order). Person rows are reconstructed by walking `fact_ordinal` per statement (the proven `se_financial_history` sequential technique). `company_people_all` is a per-source-row search table like `companies_all` — identity resolution is deliberately NOT part of it: same-identifier hard links and name-based "possible matches" stay separate layers; SE rows carry no person identifier (personnummer is not public in filings), so SE↔anything links are name matches only, always labeled as such.

**Tech Stack:** ClickHouse migrations (golang-migrate files under `corpscout/clickhouse/migrations/`), dagster asset in `sweden_financial`, new `company_people` defs module, backoffice registry queries.

## Global Constraints

- Derived tables rebuild with the module's standard stage + `EXCHANGE TABLES` atomic replace, refuse-empty guard, and `guard_against_clickhouse_table_shrink` (`SHRINK_GUARD_MIN_RATIO = 0.5`, `allow_shrink` config override defaulting to `False`) — identical wiring to `se_financial_history`.
- Migration owns all CH schema; next free numbers: **000143** (`se_company_officers`), **000144** (`company_people_all`). Column order in Python schema constants MUST match the migration's column order (contract-test pattern used by history.py).
- `company_id` in officers rows = normalized 10-digit orgnr = `se_companies.registration_number`.
- No `from __future__ import annotations` in asset modules. `uv run` for every command. `uv run dg check defs` green before any commit. Commits by explicit path (shared tree).
- Backoffice: registry-driven queries in `countries.ts` only; named CH params; readonly client untouched.
- Name-based matches are NEVER presented as "same person" — UI copy must say "same name".

---

### Task 1: Migrations + officers build SQL + asset (dagster)

**Files:**
- Create: `corpscout/clickhouse/migrations/000143_corpscout_se_company_officers.up.sql` / `.down.sql`
- Create: `corpscout/clickhouse/migrations/000144_corpscout_company_people_all.up.sql` / `.down.sql`
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_financial/officers.py`
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_financial/assets.py` (new asset at the end, after `se_financial_history_clickhouse`)
- Test: `corpscout/services/dagster_v3/tests/test_sweden_financial_officers.py`

**Interfaces:**
- Produces: table `corpscout.se_company_officers`; function `replace_se_company_officers_clickhouse(clickhouse, *, source_run_id, resolved_at, log, allow_shrink) -> dict` (metadata counts); asset `se_company_officers_clickhouse` (deps: `sweden_financial_facts_clickhouse`).

- [ ] **Step 1: Migration 000143**

```sql
-- 000143_corpscout_se_company_officers.up.sql
CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.se_company_officers
(
    company_id String,
    fiscal_year Int32,
    statement_key String,
    signatory_kind LowCardinality(String), -- 'board_signature' | 'certification' | 'auditor'
    person_seq UInt16,
    first_name String,
    last_name String,
    role_original String,
    role_kind LowCardinality(String), -- normalized, see Step 3 mapping; 'unknown' fallback
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (company_id, fiscal_year, statement_key, signatory_kind, person_seq);
```
`.down.sql`: `DROP TABLE IF EXISTS corpscout.se_company_officers;`

- [ ] **Step 2: Failing wiring test** — `test_sweden_financial_officers.py`: contract test that greps migration 000143 for each column in `officers.py`'s `SE_COMPANY_OFFICERS_COLUMNS` in order (copy the history.py contract-test shape); test that `build_officers_insert_sql()` contains the four concept-triple sets and the running-sum person grouping; wiring test that the asset exists with dep `sweden_financial_facts_clickhouse`. Run: `uv run pytest tests/test_sweden_financial_officers.py -q` → FAIL (module missing).

- [ ] **Step 3: `officers.py`** — mirror `history.py`'s structure (`QUALIFIED_..._TABLE`, columns constant, `build_..._insert_sql`, `replace_..._clickhouse` with stage+exchange+guards). Core SELECT (INSERT INTO stage):

```sql
WITH sig AS (
    SELECT
        statement_key,
        company_id,
        toInt32(coalesce(toYear(report_period_end), 0)) AS fiscal_year,
        fact_ordinal,
        concept_local_name,
        trim(coalesce(text_value, raw_value)) AS v,
        multiIf(
            concept_local_name LIKE 'UnderskriftRevisionsberattelseRevisor%', 'auditor',
            concept_local_name LIKE 'UnderskriftFaststallelseintygForetradare%', 'certification',
            'board_signature'
        ) AS signatory_kind,
        concept_local_name IN (
            'UnderskriftHandlingTilltalsnamn',
            'UnderskriftArsredovisningForetradareTilltalsnamn',
            'UnderskriftFaststallelseintygForetradareTilltalsnamn',
            'UnderskriftRevisionsberattelseRevisorTilltalsnamn'
        ) AS is_first_name,
        concept_local_name IN (
            'UnderskriftHandlingEfternamn',
            'UnderskriftArsredovisningForetradareEfternamn',
            'UnderskriftFaststallelseintygForetradareEfternamn',
            'UnderskriftRevisionsberattelseRevisorEfternamn'
        ) AS is_last_name,
        concept_local_name IN (
            'UnderskriftHandlingRoll',
            'UnderskriftArsredovisningForetradareForetradarroll',
            'UnderskriftFaststallelseintygForetradareForetradarroll',
            'UnderskriftRevisionsberattelseRevisorTitel'
        ) AS is_role
    FROM corpscout.se_financial_facts
    WHERE concept_local_name IN (
        'UnderskriftHandlingTilltalsnamn','UnderskriftHandlingEfternamn','UnderskriftHandlingRoll',
        'UnderskriftArsredovisningForetradareTilltalsnamn','UnderskriftArsredovisningForetradareEfternamn','UnderskriftArsredovisningForetradareForetradarroll',
        'UnderskriftFaststallelseintygForetradareTilltalsnamn','UnderskriftFaststallelseintygForetradareEfternamn','UnderskriftFaststallelseintygForetradareForetradarroll',
        'UnderskriftRevisionsberattelseRevisorTilltalsnamn','UnderskriftRevisionsberattelseRevisorEfternamn','UnderskriftRevisionsberattelseRevisorTitel'
    )
),
grouped AS (
    SELECT *,
        sum(is_first_name) OVER (
            PARTITION BY statement_key, signatory_kind
            ORDER BY fact_ordinal
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS person_seq
    FROM sig
)
SELECT
    company_id,
    fiscal_year,
    statement_key,
    signatory_kind,
    toUInt16(person_seq) AS person_seq,
    anyIf(v, is_first_name = 1) AS first_name,
    anyIf(v, is_last_name = 1) AS last_name,
    coalesce(anyIf(v, is_role = 1), '') AS role_original,
    multiIf(
        role_original ILIKE '%ordförande%', 'chairman',
        role_original ILIKE '%verkställande direktör%' OR role_original ILIKE 'VD%', 'ceo',
        role_original ILIKE '%suppleant%', 'deputy_board_member',
        role_original ILIKE '%styrelseledamot%' OR role_original ILIKE '%ledamot%', 'board_member',
        role_original ILIKE '%likvidator%', 'liquidator',
        role_original ILIKE '%revisor%', 'auditor',
        signatory_kind = 'auditor', 'auditor',
        role_original = '', 'unknown',
        'other'
    ) AS role_kind,
    parseDateTime64BestEffort({resolved_at:String}, 3) AS resolved_at
FROM grouped
WHERE person_seq > 0
GROUP BY company_id, fiscal_year, statement_key, signatory_kind, person_seq
HAVING first_name != '' OR last_name != ''
```

Note: `multiIf` on `role_original` references the aggregate alias — ClickHouse allows alias reuse in the same SELECT; if the implementer hits an alias-scope error, wrap the GROUP BY select in an outer SELECT that adds `role_kind`. Empty-role rows stay (`'unknown'`) — row 14 of the Axfood example shows real filings omit the role.

- [ ] **Step 4: asset in assets.py** — `se_company_officers_clickhouse`, deps `["sweden_financial_facts_clickhouse"]`, `SwedenFinancialClickhouseExportConfig` (reuse `allow_shrink`), kinds `{"python","clickhouse","xbrl"}`, calls `replace_se_company_officers_clickhouse`, returns counts metadata. Asset check `clickhouse_tables_non_empty` pattern if the module applies it to history — match history's checks exactly.
- [ ] **Step 5:** `uv run pytest tests/test_sweden_financial_officers.py -q` → PASS; `uv run dg check defs` → green.
- [ ] **Step 6: Commit** (explicit paths: both migration pairs, officers.py, assets.py, test file) — `feat(dagster): se_company_officers extracted from XBRL signature facts`.

### Task 2: `company_people_all` search layer (dagster)

**Files:**
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/company_people/__init__.py`, `.../company_people/tables.py`, `.../company_people/assets.py`
- Test: `corpscout/services/dagster_v3/tests/test_company_people_all.py`

**Interfaces:**
- Consumes: `corpscout.se_company_officers` (Task 1).
- Produces: table `corpscout.company_people_all`; asset `company_people_all_clickhouse` (deps: `se_company_officers_clickhouse`); per-source SELECT registry `PEOPLE_SOURCES: dict[str, str]` in tables.py so future sources (NO roles, EE officers, BR sócios) are added as one SELECT each.

- [ ] **Step 1: Migration 000144**

```sql
CREATE TABLE IF NOT EXISTS corpscout.company_people_all
(
    country_iso2 LowCardinality(String),
    company_id String,
    company_name String,
    first_name String,
    last_name String,
    full_name_normalized String, -- lowerUTF8(trim(first || ' ' || last))
    role_original String,
    role_kind LowCardinality(String),
    signatory_kind LowCardinality(String),
    fiscal_year Int32,
    identifier_kind LowCardinality(String), -- '' for SE (no public person id)
    identifier_value String,
    source LowCardinality(String), -- 'se_xbrl_signatures'
    source_statement_key String,
    resolved_at DateTime64(3, 'UTC'),
    INDEX idx_people_name full_name_normalized TYPE ngrambf_v1(3, 65536, 3, 7) GRANULARITY 4
)
ENGINE = MergeTree
ORDER BY (full_name_normalized, country_iso2, company_id, fiscal_year);
```

- [ ] **Step 2: failing tests** — contract test vs migration 000144; test that the SE source SELECT dedupes to one row per (company, fiscal_year, person, signatory_kind) and joins `se_companies` for `company_name`.
- [ ] **Step 3: build SQL** — SE source SELECT joins `se_company_officers o LEFT JOIN se_companies c ON c.registration_number = o.company_id` (take `c.legal_name`), `full_name_normalized = lowerUTF8(trim(concat(first_name, ' ', last_name)))`, `identifier_kind=''`, `source='se_xbrl_signatures'`. Stage + exchange + refuse-empty + shrink guard, same as Task 1.
- [ ] **Step 4:** asset + `dg check defs` + tests PASS.
- [ ] **Step 5: Commit** — `feat(dagster): company_people_all cross-country people search table`.

### Task 3: Materialize on the server + verify

- [ ] Deploy via `cd corpscout/services/dagster_v3/ansible && ansible-playbook -i inventory.ini light_sync.yml` (migrations run per the module's standard migration path — follow however 000141/000142 were applied; if manual, apply 000143+000144 the same way before launching).
- [ ] Launch `se_company_officers_clickhouse,company_people_all_clickhouse` on the server via the Dagster GraphQL launchRun (established pattern). Runtime expectation: minutes (window scan over ~10M signature facts).
- [ ] Verify: `SELECT count() FROM corpscout.se_company_officers` — expect roughly 2.0–2.5M board-signature persons + ~2M certifications + ~0.3M auditors ≈ **4–5M rows**; spot-check `company_id='5560003575' AND fiscal_year=2023` reproduces the known people (Balkow chairman, Pettersson CEO certification, Roos auditor); `company_people_all` count ≈ officers count; name search `full_name_normalized LIKE '%klas balkow%'` returns Axfood Snabbgross.

### Task 4: Backoffice Management section + same-name matches

**Files:**
- Modify: `corpscout/services/backoffice/app/lib/countries.ts` (SE `officersQuery`; new optional `CountryDetailConfig` field)
- Modify: `corpscout/services/backoffice/app/lib/queries.server.ts` (`OfficerRow`, wire into `getCompanyDetail` like `secondaryNamesQuery`)
- Create: `corpscout/services/backoffice/app/components/detail/management-section.tsx`
- Modify: `corpscout/services/backoffice/app/routes/country-company-detail.tsx`

**Interfaces:**
- Consumes: `corpscout.se_company_officers`, `corpscout.company_people_all`.

- [ ] **Step 1: SE `officersQuery`** — latest fiscal year with people, board + certification merged per person (a person signing both shows once with their best role), auditor listed separately:

```sql
SELECT first_name, last_name,
  argMax(role_original, role_kind != 'unknown') AS role_original,
  argMax(role_kind, role_kind != 'unknown') AS role_kind,
  signatory_kind, fiscal_year
FROM corpscout.se_company_officers
WHERE company_id = {id:String}
  AND fiscal_year = (SELECT max(fiscal_year) FROM corpscout.se_company_officers WHERE company_id = {id:String})
GROUP BY first_name, last_name, signatory_kind, fiscal_year
ORDER BY signatory_kind = 'auditor', role_kind != 'chairman', role_kind != 'ceo', last_name
LIMIT 100
```

- [ ] **Step 2: `ManagementSection`** — card "Management · fiscal YYYY": people with role badges (chairman/CEO highlighted), auditor row at the bottom labeled Auditor. Empty → render nothing.
- [ ] **Step 3: same-name matches** — for each displayed person, a small expandable "other companies with this name" fetched from `company_people_all` (`full_name_normalized = {name}` , exclude current company, LIMIT 10, grouped by country+company with role+year). UI copy MUST read "same name — may be a different person". Implement as one batched loader query (`full_name_normalized IN {names}`), not per-person fetches.
- [ ] **Step 4:** `npm run typecheck` clean; verify on `http://localhost:5184/company/se/5560003575` (expect Balkow/Pettersson/Lexmon/Sundström/Stenbeck + auditor Roos) and one small company; screenshot check.
- [ ] **Step 5: Commit** — `feat(backoffice): management section from XBRL signatories with same-name matches`.

## Explicitly deferred (follow-on plans)

- **NO BRREG roles ingestion** (new source; registry roles with dates — richer than signatures) → then added to `PEOPLE_SOURCES` as one SELECT.
- **EE / BR person sources** with real identifiers → enables the hard-link layer (`identifier_kind` already in schema).
- **Identity-resolution / linkage table** (match evidence, reversible merges) — separate design once ≥2 identifier-bearing sources exist.
- Officer names in `companies_all` search text.

## Self-review notes

- Person reconstruction correctness risk: filings that interleave name/role order oddly. Mitigation: grouping keys on `is_first_name` running sum; `HAVING first_name != '' OR last_name != ''` drops degenerate groups; Task 3 spot-check against a known filing gates the result.
- `UnderskriftArsredovisning*` and `UnderskriftHandling*` both map to `board_signature`; if a filing tags both families the same person may appear twice within a statement — acceptable for v1 (distinct `person_seq`), the Task 4 query dedupes per person for display.
