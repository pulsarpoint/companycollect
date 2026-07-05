# Canonical Contact/Domain Tables — Phase C (Brazil) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert Brazil RFB contact output to the canonical `br_company_contacts` / `br_company_domains` pair and consolidate its email-provider denylist onto the shared module, while keeping the graph-consumed legacy `br_websites` byte-identical until Phase E.

**Architecture:** The DuckDB pipeline keeps its internal `company_contact_info` stage (the websites build reads it) and gains two canonical-shaped stage tables built in SQL — `company_contacts` (13 canonical columns: phones with area code folded in, per-company dedupe, `source_field` provenance) and `company_domains` (15 canonical columns from the existing accepted-email-domain + election CTEs). ClickHouse gets migration `0000NN`: drop `br_company_contact_info` (zero consumers), create the canonical pair; `br_websites` untouched. Exports swap accordingly. The local 24-entry denylist is deleted in favor of the shared 48-entry union.

**Tech Stack:** Python 3.14 (`uv run`), DuckDB SQL stages, ClickHouse (golang-migrate), existing `export_duckdb_connection_table_to_clickhouse` stage/EXCHANGE pattern.

**Spec:** `corpscout/docs/superpowers/specs/2026-07-04-company-contacts-domains-standard-design.md` (Brazil rows in "Per-source conversion inventory" + decisions 1, 4, 6). Reference canonical migrations: `000088`/`000089`. Reference conformance helper: `tests/canonical_contact_tables.py`.

## Global Constraints

- Work dir `corpscout/dagster_v3` (`uv run`); migrations in `corpscout/clickhouse/migrations/` (number = highest + 1 at execution; 000091 was highest at planning).
- Canonical DDL exactly per spec; the new migration must pass `assert_canonical_contacts_ddl` / `assert_canonical_domains_ddl` unchanged — no Brazil carve-outs.
- Brazil mapping (spec decisions): `registry_id = cnpj_basico`; `source_record_id` keeps establishment traceability (the existing `concat(cnpj,':',…)` synthetic ids); `country_iso2 = 'BR'` (already on establishment rows); `source_slug = 'brazil_rfb'` (existing); phone/fax `contact_value` = area code folded in as `"<ddd> <number>"` when the area code is non-empty; `contact_type_raw = ''` (RFB has no source-side labels — our `contact_type_en` was our own label, it dies with the old table); `source_field` ∈ {`correio_eletronico`, `telefone_1`, `telefone_2`, `fax`}; `valid_to = NULL`; `source_url = ''`.
- Domains rows: `domain_source='email'`, `validation_method=''`, `confidence = EMAIL_UNIQUE_CONFIDENCE (0.9)` imported from the shared module; `website_*` columns `''`; election = existing SQL (`is_current desc, length(domain), domain` per company) — this equals the spec rule when all rows are email-sourced at equal confidence; keep it in SQL (50M-row scale — do NOT route through the Python `elect_primary_domains`).
- **Grain change (deliberate, spec-conform):** canonical contacts are company-grain (`ORDER BY (registry_id, contact_type, contact_value)`), so identical contacts shared by multiple establishments of one company collapse to one row. The stage SQL must dedupe deterministically: one row per `(cnpj_basico, contact_type, folded contact_value)`, preferring `is_current desc, cnpj asc`.
- Denylist consolidation: delete the local `EMAIL_PROVIDER_DENYLIST` / `EMAIL_DOMAIN_MAX_COMPANIES`; import both from `dagster_v3.contact_extraction`. Known, approved behavior change: the shared 48-entry union also rejects Estonia-sphere webmail (mail.ru, inbox.lv, …) — correct for Brazil too. Update the drift-guard test in `tests/test_contact_extraction.py` accordingly (the `<=` subset assertions become trivially-true identity — replace the BR side with an `is` identity assertion).
- **`br_websites` stays byte-identical** (table, build SQL output, export, asset) — the domain graph consumes it until Phase E. `test_domains_assets.py` must remain untouched and green.
- NO live re-materialization: rebuilding requires the multi-GB RFB DuckDB stages that live on the prod box; the canonical tables stay empty until the next `brazil_comp_rfb_resolve_job` run (nothing consumes them before Phase E). The migration IS applied live. Synthetic-DuckDB tests are the correctness gate (the existing test style in `tests/test_brazil_comp_rfb_transforms.py`).
- Verification per task: `uv run pytest tests/test_brazil_comp_rfb_transforms.py tests/test_brazil_comp_rfb_clickhouse.py tests/test_contact_extraction.py tests/test_canonical_contact_migrations.py tests/test_clickhouse_migrations.py -q` green; `uv run dg check defs` green after asset changes; ruff clean. Full-suite excludes: `--ignore=tests/test_exchange_rates_v2_dbt.py --ignore=tests/test_finland_xbrl_parsed_assets.py --ignore=tests/test_sweden_company_normalized_duckdb.py`.
- Conventional Commits.

---

### Task 1: Migration — canonical `br_company_contacts` + `br_company_domains`, drop `br_company_contact_info`

**Files:**
- Create: `corpscout/clickhouse/migrations/0000NN_corpscout_br_canonical_contacts.up.sql` + `.down.sql` (NN = highest + 1)
- Modify: `tests/test_clickhouse_migrations.py` (append entry; ALSO delete/adjust the old-shape pin tests around lines ~1250-1262 that assert `br_company_contact_info` columns — read them first; the ones pinning `br_websites` stay)
- Modify: `tests/test_canonical_contact_migrations.py` (add the br conformance test)

**Interfaces:**
- Produces: live `corpscout.br_company_contacts` (canonical 13-col) + `corpscout.br_company_domains` (canonical 15-col); `br_company_contact_info` gone.

- [ ] **Step 1: Write the migration**

Up: `CREATE DATABASE IF NOT EXISTS corpscout;` + `DROP TABLE IF EXISTS corpscout.br_company_contact_info;` + the two canonical CREATEs copied from `000088_corpscout_cz_canonical_contacts.up.sql` with `cz_` → `br_` (byte-identical otherwise — the conformance test enforces this). Down: drop both canonical tables + recreate `br_company_contact_info` verbatim from `000055_corpscout_br_rfb_contact_domains.up.sql` (ONLY that table — `br_websites` is not touched in either direction).

- [ ] **Step 2: Conformance test**

Add to `tests/test_canonical_contact_migrations.py` (mirroring the cz/lv tests):

```python
def test_br_canonical_migration_conforms():
    sql = _read("*_corpscout_br_canonical_contacts.up.sql")
    assert_canonical_contacts_ddl(sql, "br_company_contacts")
    assert_canonical_domains_ddl(sql, "br_company_domains")
```

- [ ] **Step 3: Ledger, old-pin cleanup, live apply, smoke**

Append the entry to `EXPECTED_MIGRATIONS`; remove/adjust the old `br_company_contact_info` column-pin test in `test_clickhouse_migrations.py` (the migration file 000055 stays on disk — only pins asserting the TABLE's current relevance need adjusting; read what the test actually asserts before touching). Run the migration tests. Apply live (`cd corpscout && make clickhouse-migrate-up`, creds from main checkout `corpscout/dagster_v3/.env`); smoke: canonical pair exists with 0 rows, `br_company_contact_info` gone, `br_websites` untouched with its existing row count (record it).

- [ ] **Step 4: Commit**

```bash
git add corpscout/clickhouse/migrations/ corpscout/dagster_v3/tests/
git commit -m "feat(clickhouse): canonical br company contacts and domains tables"
```

---

### Task 2: DuckDB build — canonical stages + denylist consolidation

**Files:**
- Modify: `src/dagster_v3/defs/brazil_companies/rfb/contacts.py`
- Modify: `src/dagster_v3/defs/brazil_companies/rfb/tables.py` (add stage-table + export-column constants; read it first — it owns ALL name constants)
- Modify: `tests/test_brazil_comp_rfb_transforms.py`, `tests/test_contact_extraction.py` (drift-guard adjustment)

**Interfaces:**
- Consumes: `dagster_v3.contact_extraction.EMAIL_PROVIDER_DENYLIST`, `EMAIL_DOMAIN_MAX_COMPANIES`, `EMAIL_UNIQUE_CONFIDENCE`, `COMPANY_CONTACTS_COLUMNS`, `COMPANY_DOMAINS_COLUMNS`.
- Produces (consumed by Task 3): DuckDB stage tables `brazil_rfb.company_contacts` (canonical 13-col layout, column names = `COMPANY_CONTACTS_COLUMNS`) and `brazil_rfb.company_domains` (canonical 15-col, names = `COMPANY_DOMAINS_COLUMNS`); `build_brazil_rfb_contact_info` counts dict gains `"contact_facts"`; `build_brazil_rfb_websites` counts dict gains `"company_domains"` and `"primary_domains"`.

- [ ] **Step 1: Denylist swap**

Delete the local `EMAIL_PROVIDER_DENYLIST` and `EMAIL_DOMAIN_MAX_COMPANIES` from `contacts.py`; import both (plus `EMAIL_UNIQUE_CONFIDENCE`) from `dagster_v3.contact_extraction`. `_denylist_sql()` keeps working off the import. In `tests/test_contact_extraction.py`'s `test_shared_denylist_superset_of_country_copies`: the BR assertions become identity (`br.EMAIL_PROVIDER_DENYLIST is contact_extraction.EMAIL_PROVIDER_DENYLIST`), Estonia's stay subset (Phase B pending); rename/adjust the docstring comment accordingly.

- [ ] **Step 2: Extend `build_brazil_rfb_contact_info`**

Keep the internal `company_contact_info` stage EXACTLY as-is (the websites build and legacy export depend on it) with ONE addition: each `base` UNION branch gains a `source_field` literal (`'correio_eletronico'` / `'telefone_1'` / `'telefone_2'` / `'fax'`) carried through to the final select (appended column — verify the legacy ClickHouse export names its columns explicitly rather than `select *`; it does, via `BR_COMPANY_CONTACT_INFO_EXPORT_COLUMNS`, so an extra stage column is invisible to the legacy export — confirm by reading `rfb/clickhouse.py`).

Then, after the existing stage build, create the canonical contacts stage in the same function:

```sql
create or replace table {company_contacts_stage} as
with folded as (
    select
        country_iso2,
        source_slug,
        source_run_id,
        source_record_id,
        cnpj,
        cnpj_basico as registry_id,
        contact_type,
        '' as contact_type_raw,
        case
            when contact_area_code <> '' then contact_area_code || ' ' || contact_value
            else contact_value
        end as contact_value,
        source_field,
        is_current,
        cast(null as date) as valid_to,
        '' as source_url,
        resolved_at
    from {contact_info_stage}
),
deduped as (
    select
        *,
        row_number() over (
            partition by registry_id, contact_type, contact_value
            order by is_current desc, cnpj
        ) as rn
    from folded
)
select
    country_iso2, source_slug, source_run_id, source_record_id,
    registry_id, contact_type, contact_type_raw, contact_value,
    source_field, is_current, valid_to, source_url, resolved_at
from deduped
where rn = 1
```

(Stage/table name constants go in `tables.py`: `COMPANY_CONTACTS_STAGE_TABLE = "company_contacts"` etc. — follow the file's naming conventions.) Extend the returned counts with `"contact_facts": count(*) of the canonical stage`.

- [ ] **Step 3: Extend `build_brazil_rfb_websites`**

After the (unchanged) `websites` stage build, add the canonical domains stage — derived from the SAME `picked`/`primaried` logic; simplest correct form is a second `create or replace table` reusing the same CTE chain (duplicate the CTE text; both read the attached contact-info stage):

```sql
create or replace table {company_domains_stage} as
with src as ( ... identical src/deduped/picked/primaried CTEs as the websites build ... )
select
    country_iso2,
    source_slug,
    source_run_id,
    concat('br_company_domains:', cnpj_basico, ':', root_domain) as source_record_id,
    cnpj_basico as registry_id,
    root_domain as domain,
    domain_source,
    '' as validation_method,
    cast({email_unique_confidence} as float) as confidence,
    '' as website_url,
    '' as website_normalized_url,
    '' as website_host,
    is_current,
    is_primary,
    now() as resolved_at
from primaried
```

(`{email_unique_confidence}` interpolated from the imported `EMAIL_UNIQUE_CONFIDENCE` constant.) Extend counts with `"company_domains"` (row count) and `"primary_domains"` (`sum(is_primary)` — must equal `count(distinct registry_id)`; assert it in the build and raise on mismatch, mirroring the fail-loud convention).

- [ ] **Step 4: Tests**

In `tests/test_brazil_comp_rfb_transforms.py`, extend `test_build_contact_info_and_websites_extracts_unique_email_domains`'s synthetic fixture (read it first — it builds establishments in DuckDB and asserts exact rows) to additionally assert:
- the canonical contacts stage: column names == `contact_extraction.COMPANY_CONTACTS_COLUMNS` (query `describe`), a phone row's `contact_value` is `"11 34567890"`-style folded, `source_field` values correct, a contact shared by two establishments of one company appears ONCE (add such a fixture row), denylisted-provider emails still appear as FACTS (gmail row present in contacts stage) while absent from the domains stage;
- the canonical domains stage: columns == `COMPANY_DOMAINS_COLUMNS`, only accepted email domains, `confidence == 0.9`, `validation_method == ''`, one `is_primary` per registry;
- the legacy `websites` stage output is UNCHANGED by all of this (existing assertions keep passing untouched).

- [ ] **Step 5: Verify + commit**

```bash
uv run pytest tests/test_brazil_comp_rfb_transforms.py tests/test_contact_extraction.py -q
uv run ruff check src/dagster_v3/defs/brazil_companies/rfb/ tests/
git add src/dagster_v3/defs/brazil_companies/rfb/ tests/
git commit -m "feat(dagster): brazil canonical contact/domain stages; shared denylist"
```

---

### Task 3: ClickHouse exports + assets + docs

**Files:**
- Modify: `src/dagster_v3/defs/brazil_companies/rfb/clickhouse.py` (contact_info export → `br_company_contacts` export from the canonical stage; NEW `br_company_domains` export; websites export untouched)
- Modify: `src/dagster_v3/defs/brazil_companies/rfb/tables.py` (export column tuples: reuse the shared `COMPANY_CONTACTS_COLUMNS`/`COMPANY_DOMAINS_COLUMNS` — import them rather than redeclaring)
- Modify: `src/dagster_v3/defs/brazil_companies/rfb/assets.py` (rename the contact-info CH asset to `brazil_comp_rfb_clickhouse_company_contacts`; add `brazil_comp_rfb_clickhouse_company_domains` with `deps` on the websites duckdb asset which builds the domains stage; both stay in group `brazil_comp_rfb` so the job selection auto-covers them; update `defs = dg.Definitions(...)` asset list; update the preflight `assert_clickhouse_tables_exist` call(s) to name both canonical tables)
- Modify: `tests/test_brazil_comp_rfb_clickhouse.py`, and the standard spec's inventory row is NOT edited (status tracking lives in plans/ledgers); `docs/data-source-guidelines.md` gets one line only if it names `br_company_contact_info` anywhere (check; likely not)

**Interfaces:**
- Consumes: Task 2's stage tables and Task 1's live tables.
- Produces: assets `brazil_comp_rfb_clickhouse_company_contacts` / `brazil_comp_rfb_clickhouse_company_domains` exporting the canonical stages via the existing `export_duckdb_connection_table_to_clickhouse(truncate=True)` pattern.

- [ ] **Step 1: Rework exports** — mirror the existing `export_brazil_comp_rfb_clickhouse_contact_info` shape for the two canonical exports (read `rfb/clickhouse.py`; same truncate/stage/EXCHANGE call, new source stage + target table + column tuple). Delete the contact_info export function.

- [ ] **Step 2: Assets** — per Files above. The old `brazil_comp_rfb_clickhouse_contact_info` asset name disappears; note in the commit body that any Dagster UI bookmarks/run history references simply age out (no schedules reference asset names directly — the job selects the group).

- [ ] **Step 3: Tests** — update `test_brazil_comp_rfb_clickhouse.py`'s export test to drive both canonical exports against the synthetic stages (columns == shared tuples; row counts) and keep the websites export assertions untouched. Add a column-order pin: the export tuples used by clickhouse.py ARE the shared tuples (identity assertion), so DuckDB stage order, export order, and migration DDL can never diverge.

- [ ] **Step 4: Verify + commit**

```bash
uv run pytest tests/test_brazil_comp_rfb_transforms.py tests/test_brazil_comp_rfb_clickhouse.py tests/test_canonical_contact_migrations.py tests/test_clickhouse_migrations.py tests/test_contact_extraction.py tests/test_domains_assets.py -q
uv run dg check defs 2>&1 | tail -1
uv run pytest --ignore=tests/test_exchange_rates_v2_dbt.py --ignore=tests/test_finland_xbrl_parsed_assets.py --ignore=tests/test_sweden_company_normalized_duckdb.py -q 2>&1 | tail -2
uv run ruff check src/dagster_v3/defs/brazil_companies/ tests/
git add src/dagster_v3/defs/brazil_companies/ tests/ docs/
git commit -m "feat(dagster): brazil exports canonical contact/domain pair to clickhouse"
```

---

## Deployment note (not a code task)

Migration applies in Task 1 (drops `br_company_contact_info` — acceptable: zero consumers, and the canonical replacement fills on the next full `brazil_comp_rfb_resolve_job` run on the prod box, which is also when `br_company_contacts`/`br_company_domains` first populate). `br_websites` and the domain graph are completely unaffected. After this phase: B (Estonia) and D (NO/FI/wikidata) remain, then E (graph switch — its plan must include the "domains swaps last" write-order requirement and the row-shape assert the Phase A review suggested).
