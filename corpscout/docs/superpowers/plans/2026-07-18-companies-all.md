# companies_all Core Table Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Dagster-built ClickHouse table `corpscout.companies_all` (~116M rows: one row per company across all 10 countries, with normalized search/facet/industry/financial columns) that replaces the backoffice's 10-branch UNION search — making name search and revenue sort fast, killing the 400-page cap, and serving as the schema prototype for the client platform's search core.

**Architecture:** New dagster_v3 module `defs/companies_all/` (CH→CH, mirrors `defs/company_financials_latest` + `defs/domains`): a per-country `SOURCES` spec materializes each registry expression (status/legal_form/place/size), the primary-industry label (today's page-time `industryQuery` logic moves into the build), and the financials join (revenue/fiscal_year/employees/has_financials) into one uniform row; 10 sequential `INSERT INTO stage SELECT` legs, per-country row-count guards, then `EXCHANGE TABLES`. The backoffice `unified.server.ts` + `facets.server.ts` then rewrite onto the single table, preserving today's user-visible semantics (capability exclusion becomes natural: countries without a column carry `''`, which `IN` filters never match). Migration 000139 owns the schema and introduces the repo's first skip index (`ngrambf_v1` on `name_normalized`).

**Tech Stack:** dagster_v3 (Python, clickhouse-driver, golang-migrate, pytest, `uv run`), backoffice (React Router 8, TypeScript, vitest live-CH tests).

## Global Constraints

- dagster_v3 conventions bind (its CLAUDE.md): migration owns schema; stage + `EXCHANGE TABLES`; refuse to replace on empty/short input; non-nullable String gets `''` never NULL; no `from __future__ import annotations` in asset modules; `uv run` everywhere; commit by explicit path; `uv run dg check defs` before done.
- **Migration number 000139** (`000139_corpscout_companies_all`); append to `EXPECTED_MIGRATIONS`.
- Uniform schema (exact):
  `country_code LowCardinality(String), company_id String, name String, name_normalized String, is_active UInt8, status String, legal_form String, place String, size String, industry_code String, industry_label String, revenue_usd Nullable(Float64), fiscal_year Nullable(Int32), employees Nullable(Float64), has_financials UInt8, resolved_at DateTime64(3, 'UTC')` — `ENGINE = MergeTree ORDER BY (country_code, company_id)` plus `INDEX idx_name_ngram name_normalized TYPE ngrambf_v1(3, 262144, 3, 0) GRANULARITY 4`. All String columns coalesced to `''`.
- `name_normalized = lowerUTF8(name)`; the backoffice search binds `name_normalized LIKE {pattern:String}` with a lowercased `%q%` pattern (preserves today's ILIKE semantics and lets the ngram index prune).
- **Semantics preservation is the contract for the backoffice switch** (from the 2026-07-18 registry audit): capability exclusion (e.g. `size` filter → only BR rows can match because everyone else has `size=''`; `ee`+`size` → 0 rows); status/legal_form facet values keep today's per-country vocabularies and merge across countries by literal string (GROUP BY does this naturally); default sort stays `country_code ASC, company_id ASC` (br first); `q` stays case-insensitive substring; industry filter = `industry_code IN {f_industry:Array(String)}` (same value space: today's per-country primary-industry codes); `has_financials` becomes a real `= 1` predicate. INTENTIONAL changes (update tests deliberately, list them in reports): `MAX_UNIFIED_PAGE` cap removed; `has_financials` facet returns real counts (stub retired); rows' `industry_code/industry_label` come from the table (page-time enrichment deleted); LV auto-joins the industry filter/facet the day `lv_companies_nace` gets data (its `industry_code` is `''` today).
- Per-country expressions in the dagster `SOURCES` spec are **duplicated by design** from `backoffice/app/lib/countries.ts` (Python can't import the TS registry). The guard is the live parity test in Task 5 — never "simplify" an expression on one side only.
- The build job carries `tags=HEAVY_BULK_RUN_TAGS` (`defs/common/tags.py:10`); schedule daily with `default_status=dg.DefaultScheduleStatus.RUNNING` + per-asset `AutomationCondition.eager()` with the same truthful comment as `company_financials_latest` (eager is dormant until the automation sensor is enabled).
- Insert strategy: 10 sequential per-country `INSERT INTO <stage> (<explicit column list>) SELECT ...` statements (bounded memory, per-country progress logs) — no client-side batching (matches every existing CH→CH precedent; BR's 68.6M-row leg is the biggest single INSERT SELECT in the repo — log its duration). After all legs: assert per-country stage counts EQUAL the source `xx_companies` counts (joins must not duplicate or drop rows — industry subquery is `LIMIT 1 BY`, financials is one-row-per-company), then EXCHANGE.
- Dev server 5183 USER-OWNED. ClickHouse read-only outside the pipeline (http://companycollect:8123, password in corpscout/.env). Conventional Commits + trailer `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.

## Ground truth (audited 2026-07-18; two exploration reports + live probes)

- Row counts: br 68.63M · fr 29.68M · gb 5.70M · se 4.14M · cz 3.51M · sk 2.19M · no 1.17M · lv 485k · fi 461k · ee 373k = **116,333,029**.
- Perf baseline (no index): substring scan over br (68M) = 1.5s; name-sorted top-N on br = 1.3s. Today's unified name sort ≈ 9.5s. No skip index exists anywhere in the schema yet — 000139 introduces the first.
- **Upstream asset keys** (deps): companies exports — `norway_brreg_entities_snapshot_clickhouse` + `norway_brreg_entity_updates_clickhouse` (both write no_companies), `finland_ytj_resolved_clickhouse`, `sweden_company_companies_clickhouse`, `estonia_ar_clickhouse_companies`, `latvia_ur_clickhouse_companies`, `uk_companies_house_clickhouse_companies`, `france_sirene_clickhouse_companies`, `brazil_comp_rfb_clickhouse_companies` (PARTITIONED — dep on a partitioned asset from an unpartitioned one is fine), `czech_ares_clickhouse_companies`, `slovakia_rpo_clickhouse_companies`; plus the 8 `{code}_company_financials_latest_clickhouse` assets. The implementer MUST verify each key against `uv run dg list defs` and additionally include any SEPARATE industries-table export asset if a country exports `xx_industries` from a different asset than its companies export (check per module; e.g. brazil establishments).
- **Per-country registry expressions** (verbatim from countries.ts, the SOURCES spec below encodes them; line refs in the audit): see Task 1's SOURCES table — id/name/active/status/legal_form/place/size per country, industry logic per country (BR via `br_establishments` `is_headquarters=1`; SE industries keyed on `se_companies.company_id` with `ORDER BY is_primary DESC, sequence ASC`; FI labels via `substring(source_industry_code,1,4)` against `nace_categories` `is_current=1`; LV via `lv_companies_nace`, currently all-empty), financials join key per country (se → `company_id`, br → `cnpj_basico`, others = idColumn).
- Backoffice current behaviors pinned in the audit: ILIKE `%q%` on nameColumn; sorts {country, name, revenue}; per-branch `LIMIT page*pageSize` + outer merge; `MAX_UNIFIED_PAGE=400` (countries.ts:9); page-time industry enrichment via `industryQuery` `{ids:Array(String)}`; facet cache 24h keyed `${code}:${key}`; `facetSql` whitelist-throws on unknown keys (injection guard — preserve); `UNIFIED_FACET_KEYS = ["country","has_financials","legal_form","status","place","size","industry"]`.
- Blast-radius tests that change INTENTIONALLY: `tests/unified.server.test.ts` (has_financials stub expectation :111-117; any cap references), `tests/facets.server.test.ts` (per-country cache reference identity :73-77 — keep the per-country cache API, now `WHERE country_code=` scoped, so this may survive unchanged; verify). Tests that must KEEP passing byte-for-byte in spirit: br-first default sort, total >100M, ee count band, capability exclusion (size→br-only; ee+size→0), petrobras search, revenue monotonic, no+has_financials band, fr+has_financials=0, every row has revenue_usd, country facet 10 options with br>60M, merged status facet desc, typeahead esto→ee, unknown-facet throws.
- `queries.server.ts` (per-country search) stays — it serves detail pages and live test sweeps. Registry `columns` stay for facet definitions/labels.

## Out of scope (logged)

- Status *normalization* (canonical active/inactive vocabulary) — companies_all carries today's raw per-country values; a `status_normalized` column is the natural v2 once a mapping table exists.
- `registration_date`, website, contacts/domains flags — add columns later as consumers appear.
- Scoped facet counts (counts respecting current filters) — the single table makes this a small toggle later; this pass preserves today's unscoped semantics.
- Name-sort/revenue-sort projections — measure first; add only if the single-table sorts disappoint.
- SE id-space reconciliation (10- vs '16'-prefixed 12-digit) — replicate today's registry joins verbatim; the known trap stays logged.

---

### Task 1: Migration 000139 + module scaffolding + per-country SQL spec

**Files:**
- Create: `corpscout/clickhouse/migrations/000139_corpscout_companies_all.up.sql` / `.down.sql`
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/companies_all/__init__.py` (empty)
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/companies_all/tables.py`
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/companies_all/sql.py`
- Modify: `corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py` (EXPECTED_MIGRATIONS + a companies_all coverage test)
- Test: `corpscout/services/dagster_v3/tests/test_companies_all.py`

**Interfaces:**
- Produces: CH table `corpscout.companies_all` (schema = Global Constraints, 4-space column lines); `COMPANIES_ALL_TABLE = "companies_all"`, `COMPANIES_ALL_COLUMNS` tuple (exact insert order), `COMPANIES_ALL_COUNTRIES = ("no","fi","se","ee","lv","gb","fr","br","cz","sk")`, and `build_country_insert_select(code: str) -> str` in `sql.py` (Task 2's asset wraps each in `INSERT INTO <stage> (<columns>) <select>`).

- [ ] **Step 1: Migration.** `up.sql`: `CREATE DATABASE IF NOT EXISTS corpscout;` + one `CREATE TABLE IF NOT EXISTS corpscout.companies_all` with the exact Global-Constraints schema, `ENGINE = MergeTree ORDER BY (country_code, company_id)`, and the index INSIDE the column list block:

```sql
    INDEX idx_name_ngram name_normalized TYPE ngrambf_v1(3, 262144, 3, 0) GRANULARITY 4
```

`down.sql`: `DROP TABLE IF EXISTS corpscout.companies_all;`. Append `"000139_corpscout_companies_all"` to `EXPECTED_MIGRATIONS`; add `test_companies_all_migration_covers_columns` (mirror the fi pattern: every `COMPANIES_ALL_COLUMNS` entry appears as `    {column} ` in the up.sql; plus assert the `INDEX idx_name_ngram` line and `ORDER BY (country_code, company_id)` are present).

- [ ] **Step 2: `tables.py`** — the three constants above; `COMPANIES_ALL_COLUMNS` in exactly the schema order.

- [ ] **Step 3: `sql.py`** — a `SOURCES` dict + builder. Every SELECT emits ALL columns in `COMPANIES_ALL_COLUMNS` order. Template (shared; per-country fragments below):

```sql
SELECT
  '{code}' AS country_code,
  toString({id}) AS company_id,
  coalesce({name}, '') AS name,
  lowerUTF8(coalesce({name}, '')) AS name_normalized,
  toUInt8({active}) AS is_active,
  coalesce(toString({status}), '') AS status,
  coalesce(toString({legal_form}), '') AS legal_form,
  coalesce(toString({place}), '') AS place,
  coalesce(toString({size}), '') AS size,
  coalesce(ind.industry_code, '') AS industry_code,
  coalesce(ind.industry_label, '') AS industry_label,
  fin.revenue_amount_usd AS revenue_usd,
  fin.fiscal_year AS fiscal_year,
  fin.employees AS employees,
  toUInt8(fin.company_id != '') AS has_financials,
  now64(3) AS resolved_at
FROM corpscout.{companies_table} AS c
LEFT JOIN ({industry_subquery}) AS ind ON ind.company_id = toString({industry_join_key})
LEFT JOIN corpscout.{financials_table} AS fin ON fin.company_id = toString({financials_join_key})
```

Countries WITHOUT a financials table (fr, cz): drop the fin join and emit `CAST(NULL AS Nullable(Float64))`, `CAST(NULL AS Nullable(Int32))`, `CAST(NULL AS Nullable(Float64))`, `toUInt8(0)` for the four financial columns. NOTE: `fin.revenue_amount_usd`/`fiscal_year`/`employees` are already Nullable in the summary tables and LEFT JOIN misses yield NULL (verified in the financials-latest work); `fin.company_id != ''` is the has-row test because `company_id` is non-nullable String (miss → `''`).

Per-country fragments (VERBATIM from the registry audit — column exprs reference `c.`-prefixed columns where ambiguous):

| code | companies_table / id / name | active | status | legal_form | place | size | industry_subquery (one row per company) | industry_join_key | financials table / join key |
|---|---|---|---|---|---|---|---|---|---|
| no | no_companies / org_number / name | is_active = 1 | lifecycle_status | coalesce(legal_form_description_original, legal_form_code) | '' | '' | `SELECT toString(i.org_number) AS company_id, i.nace_normalized_code AS industry_code, coalesce(nullIf(n.description_en,''), i.description_en, i.description_original, i.nace_normalized_code) AS industry_label FROM corpscout.no_industries AS i LEFT JOIN corpscout.nace_categories AS n ON n.normalized_code = substring(i.nace_normalized_code,1,4) AND n.is_current = 1 ORDER BY i.is_primary DESC LIMIT 1 BY i.org_number` | org_number | no_company_financials_latest / org_number |
| fi | fi_companies / business_id / name | is_active = 1 | lifecycle_status | coalesce(legal_form_description_en, legal_form_description_original, legal_form_code) | '' | '' | fi_industries analog: `coalesce(i.source_industry_code,'')` as code, label via `n.normalized_code = substring(coalesce(i.source_industry_code,''),1,4)`, `ORDER BY i.is_primary DESC LIMIT 1 BY i.business_id` | business_id | fi_company_financials_latest / business_id |
| se | se_companies / registration_number / legal_name | status = 'active' | status | legal_form_code | '' | '' | `se_industries`: code `i.nace_rev2_class_code`, label via `n.normalized_code = i.nace_rev2_class_code AND n.is_current = 1`, `ORDER BY i.is_primary DESC, i.sequence ASC LIMIT 1 BY i.company_id` (company_id key) | **c.company_id** | se_company_financials_latest / **c.company_id** |
| ee | ee_companies / reg_code / name | is_active = 1 | coalesce(nullIf(status_en,''), status_original) | coalesce(nullIf(legal_form_en,''), legal_form_original) | location | '' | ee_industries analog of no (label join on full `nace_normalized_code`) `LIMIT 1 BY i.reg_code` | reg_code | ee_company_financials_latest / reg_code |
| lv | lv_companies / regcode / legal_name | is_active = 1 | status | coalesce(nullIf(legal_form_description_en,''), legal_form_text) | coalesce(address_city_name,'') | '' | `SELECT toString(regcode) AS company_id, coalesce(nace_code,'') AS industry_code, coalesce(nullIf(nace_label,''), nace_code, '') AS industry_label FROM corpscout.lv_companies_nace LIMIT 1 BY regcode` (all-empty today; auto-fills later) | regcode | lv_company_financials_latest / regcode |
| gb | gb_companies / company_number / name | is_active = 1 | company_status | company_category | city | '' | gb_industries analog of ee, `LIMIT 1 BY i.company_number` | company_number | gb_company_financials_latest / company_number |
| fr | fr_companies / siren / name | is_active = 1 | status_en | legal_form_en | city | '' | fr_industries analog of ee, `LIMIT 1 BY i.siren` | siren | (none) |
| br | br_companies / cnpj_basico / legal_name | is_active = 1 | status_en | '' | concat(municipality_name, ' / ', state) | company_size_en | `SELECT toString(e.cnpj_basico) AS company_id, e.primary_cnae_code AS industry_code, coalesce(nullIf(m.nace_description_en,''), e.primary_cnae_code) AS industry_label FROM corpscout.br_establishments AS e LEFT JOIN corpscout.br_cnae_to_nace AS m ON m.cnae_normalized_code = e.primary_cnae_code WHERE e.is_headquarters = 1 ORDER BY e.primary_cnae_code != '' DESC LIMIT 1 BY e.cnpj_basico` | cnpj_basico | br_company_financials_latest / cnpj_basico |
| cz | cz_companies / ico / name | is_active = 1 | if(is_active = 1, 'active', 'inactive') | legal_form_en | city | '' | cz_industries analog of ee, `LIMIT 1 BY i.ico` | ico | (none) |
| sk | sk_companies / ico / name | is_active = 1 | if(is_active = 1, 'active', 'inactive') | coalesce(nullIf(legal_form_en,''), legal_form_original) | city | '' | sk_industries analog of ee, `LIMIT 1 BY i.ico` | ico | sk_company_financials_latest / ico |

The implementer MUST verify each referenced column exists via `system.columns` (read-only) before finalizing — where the audit and reality disagree, reality wins and the report records it. IMPORTANT on the place/size ambiguity: `br.place` uses `concat(municipality_name, ' / ', state)` from `br_companies` — verify those columns live on br_companies (the audit says the registry expr binds against companiesTable; if they actually live on establishments, replicate whatever the registry's SELECT context implies and record it).

- [ ] **Step 4: Unit tests** (`test_companies_all.py`): for every code — the built SELECT mentions the right companies table, aliases `country_code` to the code literal, emits every `COMPANIES_ALL_COLUMNS` alias, fr/cz have the NULL-literal financial columns and `toUInt8(0) AS has_financials`, se's industry join key is `c.company_id` (not registration_number), unknown code raises ValueError. `uv run pytest tests/test_companies_all.py tests/test_clickhouse_migrations.py -q` → green.

- [ ] **Step 5: Apply migration** (same mechanism as 000137/000138 — local migrate CLI acceptable, report it) → verify table exists with the index: `SHOW CREATE TABLE corpscout.companies_all` contains `idx_name_ngram`.

- [ ] **Step 6: Commit** — the migration pair, module files, both test files, by explicit path: `feat(dagster): companies_all core table migration and sql spec`.

---

### Task 2: The asset — build job, guards, schedule

**Files:**
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/companies_all/assets.py`
- Test: extend `corpscout/services/dagster_v3/tests/test_companies_all.py`

**Interfaces:**
- Consumes Task 1's `sql.py`/`tables.py`. Produces asset `companies_all_clickhouse`, job `companies_all_job` (tagged `HEAVY_BULK_RUN_TAGS`), schedule `companies_all_schedule` (daily `15 7 * * *` Europe/Oslo — after the 06:30 financials-latest run — `default_status=RUNNING`).

- [ ] **Step 1:** Mirror `company_financials_latest/assets.py` + `domains/assets.py`: single asset `companies_all_clickhouse`; `deps` = the 11 companies-export keys + the 8 financials-latest keys from Ground truth, EACH VERIFIED against `uv run dg list defs` (fix names to reality; add separate industries-export assets where a module exports `xx_industries`/`br_establishments` from a different asset — check and record); `automation_condition=dg.AutomationCondition.eager()` with the truthful dormant-sensor comment; `kinds={"clickhouse"}`; no pool (pure CH). Body: `assert_clickhouse_tables_exist` (target + all 10 companies tables), uuid stage `CREATE TABLE ... AS` target, then FOR EACH code (sequential): `INSERT INTO stage (<COMPANIES_ALL_COLUMNS>) {build_country_insert_select(code)}`, log per-country rows+duration; guards: per-country stage count == `SELECT count() FROM corpscout.<companies_table>` (exact equality — raise ValueError naming the country on mismatch), total > 0; EXCHANGE; stage dropped in finally. MaterializeResult metadata: per-country rows + total + build seconds.
- [ ] **Step 2:** Job (`dg.define_asset_job("companies_all_job", tags=HEAVY_BULK_RUN_TAGS, selection=dg.AssetSelection.assets("companies_all_clickhouse"))`) + schedule as specified; module-level `defs = dg.Definitions(...)`.
- [ ] **Step 3:** Unit tests: asset name/deps cover all expected keys (pin the dep list); job carries the heavy-bulk tag; schedule RUNNING + cron. `uv run pytest tests/test_companies_all.py -q` green; `uv run dg check defs` clean (validates the dep keys resolve — the real test of Step 1's verification).
- [ ] **Step 4:** Commit by explicit path: `feat(dagster): companies_all build asset with per-country guards`.

---

### Task 3: Materialize + verify (operational)

**Files:** none (report carries evidence)

- [ ] **Step 1:** `./scripts/dagster-dev.sh` (background) → `uv run dg launch --assets companies_all_clickhouse` (upstreams already materialized — launch ONLY the leaf). Expect the longest single build so far (BR leg dominates); run foreground with a 30-minute timeout; log per-country durations from the run output.
- [ ] **Step 2:** Verify (read-only): total = 116,333,029 ±(whatever the sources now hold — must EQUAL `sum` of the 10 source counts taken in the same session); per-country counts match sources exactly; `count() = uniqExact(country_code, company_id)`; spot checks — `SELECT ... WHERE country_code='br' AND name_normalized LIKE '%petrobras%'` returns rows and uses the ngram index (`EXPLAIN indexes = 1` shows the skip index pruning); Equinor row has `has_financials=1`, revenue ≈ $72.5bn; `ee` status facet `GROUP BY status` matches the old per-country facet's top values; fr rows all `has_financials=0` and NULL revenue; a NO NUF company (983096077 is quality-flagged — pick 921416873 AWS) carries its summary-row revenue (lists-keep semantics — the exclusion rule is an aggregates-layer concern, NOT companies_all's).
- [ ] **Step 3:** Measure and record: full-table name search (`name_normalized LIKE '%petrobras%'` no country filter), name-sorted top-25 (`ORDER BY name_normalized LIMIT 25`), revenue-sorted top-25, count with 2 filters. Targets: search < 1.5s, sorts < 3s (vs 9.5s baseline). Clean up the dev instance you started (verify no strays).

---

### Task 4: Backoffice switch — unified layer + facets on companies_all

**Files:**
- Modify: `corpscout/services/backoffice/app/lib/unified.server.ts` (rewrite)
- Modify: `corpscout/services/backoffice/app/lib/facets.server.ts`
- Modify: `corpscout/services/backoffice/app/lib/filters.ts` (only if key lists move)
- Modify: `corpscout/services/backoffice/app/lib/countries.ts` (remove `MAX_UNIFIED_PAGE`)
- Modify: `corpscout/services/backoffice/app/routes/companies.tsx` + `app/components/data-table/pagination.tsx` (drop maxPage/"(capped)")
- Test: `corpscout/services/backoffice/tests/unified.server.test.ts`, `tests/facets.server.test.ts`

**Interfaces:**
- `searchUnifiedCompanies(opts)` keeps its EXACT external signature and `UnifiedRow` shape (now `industry_code`/`industry_label` come from the table; `__ik` and the enrichment pass die). Facet function signatures keep their shapes; `getFacetOptions(country, key)` becomes a `WHERE country_code =` scoped query on companies_all (cache keys unchanged).

- [ ] **Step 1 (tests first):** Update the two test files to the intentional changes ONLY (cap removal; has_financials facet realness — assert `value:"true"` with `count > 1_000_000` instead of the stub; everything else keeps its current assertions). Run → the suite fails against the old implementation where expectations changed (RED for the changes).
- [ ] **Step 2: unified.server.ts rewrite.** One table, one query path:
  - WHERE assembly from `parseUnifiedFilters` output: `country` → `country_code IN {f_country:Array(String)}`; `has_financials` → `has_financials = 1`; `industry` → `industry_code IN {f_industry:Array(String)}`; column keys (status/legal_form/place/size) → `<column> IN {f_<key>:Array(String)}` — the column NAME comes from a fixed module map `{status: "status", legal_form: "legal_form", place: "place", size: "size"}` (never from user input); `q` → `name_normalized LIKE {pattern:String}` with `%${q.toLowerCase()}%`.
  - Count: `SELECT count() FROM companies_all WHERE ...`. Pagination: clamp to `lastPage` only (cap gone).
  - Sorts: country → `country_code {dir}, company_id {dir}`; name → `name_normalized = '' ASC, name_normalized {dir}, country_code, company_id`; revenue → `isNull(revenue_usd) ASC, revenue_usd {dir}, country_code, company_id`. `LIMIT {pageSize} OFFSET ...`.
  - Row SELECT: `country_code, company_id AS id, name, is_active AS active, industry_code, industry_label, revenue_usd, fiscal_year` (map `''` industry to null in TS to preserve the row contract).
  - `getUnifiedFacetOptions(key)`: country → `GROUP BY country_code` (labels from registry, cached 24h); has_financials → `SELECT 'true' AS value, 'yes' AS label, countIf(has_financials = 1) AS cnt` (real); others → `GROUP BY <column> WHERE <column> != '' ORDER BY cnt DESC LIMIT 50000` (cached; industry groups `industry_code` with `any(industry_label)` as label). Whitelist-throw on unknown keys preserved verbatim.
  - `searchUnifiedFacetOptions` unchanged (rankFacetOptions over the new lists).
- [ ] **Step 3: facets.server.ts** — `getFacetOptions(country, key)`/`searchFacetOptions` re-point to companies_all with `country_code = {code:String}` + the same fixed column map (industry: `GROUP BY industry_code` scoped to the country). Keep the `${code}:${key}` cache and the unknown-facet throw (the injection-guard tests must pass untouched).
- [ ] **Step 4:** Gate: `pnpm typecheck`; full `pnpm test` → all green (updated + preserved assertions); measure and REPORT: default page, name sort deep page, petrobras search, revenue sort — compare against the Task 3 raw numbers and the old 9.5s baseline. Throwaway dev server: /companies renders, all facets open (incl. real has_financials count), filters compose, pagination beyond 400 now reachable (e.g. `?page=500` on an unfiltered view). Kill it.
- [ ] **Step 5:** Commit by explicit path (all touched backoffice files): `feat(backoffice): unified search on companies_all`.

---

### Task 5: Parity sweep + README

**Files:**
- Test: `corpscout/services/backoffice/tests/companies-all-parity.test.ts` (new)
- Modify: `corpscout/services/backoffice/README.md`

- [ ] **Step 1: Parity tests (live).** For EACH of the 10 countries: (a) `companies_all` count for the country == `count()` of its `companiesTable`; (b) sample 25 ids from companies_all for the country and assert `status`/`legal_form`/`place`/`size` values equal the registry exprs evaluated per-country (`SELECT <expr> FROM <companiesTable> WHERE <idColumn> IN {ids}` — build the comparison FROM the registry so drift in either side fails the test); (c) for a country with financials (no): revenue_usd parity vs the summary table for 10 sampled has_financials rows; (d) for a country with industries (ee): industry_label parity vs the registry `industryQuery` for 10 sampled ids. This is THE guard for the duplicated-spec risk — it must run in the live suite permanently.
- [ ] **Step 2: README** — companies_all section: what it is, build cadence, the duplicated-spec parity contract, the intentional semantic changes (cap gone, real has_financials counts, LV industry auto-join), and that `queries.server.ts` remains for detail pages.
- [ ] **Step 3:** Full gate `pnpm typecheck && pnpm test`; commit: `test(backoffice): companies_all parity sweep` + `docs(backoffice): companies_all notes` (or one commit, implementer's call — explicit paths).
