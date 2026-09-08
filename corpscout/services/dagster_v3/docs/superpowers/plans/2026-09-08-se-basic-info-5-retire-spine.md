# SE Basic Info Slice 5: Retire the `se_companies` Spine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move every reader of `corpscout.se_companies` to `se_company_basic_info` (plus the two register tables where record identity is needed), delete the spine's builder, empty its DDL from the ledger, and hand the owner a gated drop of the table and its translation rows.

**Architecture:** Anchor, name, status, form and date reads go to `se_company_basic_info FINAL`; register record identity and stamps go to `se_bolagsverket_companies` and `se_scb_companies` through one shared uid expression (a Python helper and a dbt macro pinned equal). Dagster dependencies on the spine builder move to the basic-info fold; the `identities_normalized` check moves to the SCB export. The public SE company shell is re-projected under its old column names. No new ClickHouse migration: the spine's DDL leaves 000084/000244/000281 per the ledger policy and the drop is owner-run.

**Tech Stack:** Python 3.14 / Dagster / dbt-clickhouse / pytest (`uv run pytest`), clickhouse-local or docker for integration tests, TypeScript / React Router / vitest (`npx vitest run`, `npm run typecheck`).

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-08-se-basic-info-5-retire-spine-design.md`

## Global Constraints

- **Repo root:** `/Users/graovic/pulsarpoint/ppoint/companycollect`. Dagster commands from `corpscout/services/dagster_v3` with `uv run`; backoffice commands from `corpscout/services/backoffice`.
- **Branch:** `se-basic-info-5-retire-spine`, up to date with main (`a4e8079f3`, which carries the other session's 000392 serving swap). Commit by explicit path only.
- **Definitions-loading tests** need `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix`.
- **Integration tests** (`*_clickhouse_local.py`, `test_se_companies_serving_sql.py`, the dbt local run) need docker; a skip is not a pass.
- **Successor names (verbatim):** `corpscout.se_company_basic_info` (columns `company_id`, `legal_name` non-null String, `legal_form_code` Nullable, `status`, `status_source`, `incorporation_date` Nullable(Date32), `description`, `description_language`, `description_sv`, `folded_at`, `source_run_id`); `corpscout.se_bolagsverket_companies` (`deregistration_date`, `deregistration_reason`, `legal_name_raw`, `source_record_id`, `source_payload_hash`, `observed_at`, `has_company`); `corpscout.se_scb_companies` (`source_record_id`, `source_payload_hash`, `observed_at`, `has_company`).
- **Uid expression (verbatim, both languages):** `lower(hex(SHA256(concat('company-source-record-v1\nstructured\n', '<slug>', '\nregistry_company\n', <alias>.source_record_id, '\n', lowerUTF8(<alias>.source_payload_hash)))))` with `<slug>` = `sweden_bolagsverket` or `sweden_scb`.
- **Not touched:** `se_companies_serving` (already re-based by 000391/000392), the fold, the Info tab, the observation cache, the DuckDB normalised layer.
- **Commit footer** on every commit:
  ```
  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01UJnba4eXZta4f9KaKJxhY9
  ```

---

## File map

| File | Responsibility |
|---|---|
| `src/dagster_v3/defs/se_company/common.py` (modify) | `register_record_uid_sql(source_slug, alias)`; `bolagsverket_record_uid_sql` becomes a thin wrapper. |
| `src/dagster_v3/defs/sweden_uhm_procurement/clickhouse.py`, `assets.py` (modify) | Anchor join and dep. |
| `src/dagster_v3/defs/sweden_platsbanken/clickhouse.py`, `assets.py` (modify) | Anchor CTE, table list, dep. |
| `src/dagster_v3/defs/company_serving/publish.py` (modify) | Anchor checks. |
| `src/dagster_v3/defs/company_signals/rules.py`, `company_identifier/rules.py`, `company_identifier/assets.py` (modify) | Table names, upstream key. |
| `src/dagster_v3/defs/company_domain_suggestions/inputs.py`, `dbt_run.py`, `assets.py` (modify) | Companies read, count, table list. |
| `src/dagster_v3/defs/sweden_ratsit/assets.py` (modify) | Active-companies table and dep. |
| `src/dagster_v3/defs/sweden_company/assets.py`, `clickhouse.py`, `tables.py` (modify) | Spine export removed; `identities_normalized` re-targeted; wikidata seed spine key. |
| `src/dagster_v3/defs/common/clickhouse_checks.py` (modify) | Spine leaf removed. |
| dbt: `company_serving/dbt/models/sources.yml`, 7 models, `macros/company_serving_tests.sql`, new `macros/se_register_record_uid.sql`; `company_domain_suggestions/dbt/models/sources.yml`, `staging/stg_se_company_match_features.sql` (modify/create) | Sources and models re-pointed. |
| tests (modify): `test_company_identifier.py`, `test_company_signals_rules.py`, `test_sweden_uhm_procurement_source.py`, `test_sweden_platsbanken_assets.py`, `test_sweden_platsbanken_clickhouse_local.py`, `test_sweden_ratsit_pilot.py`, `test_sweden_company_assets.py`, `test_clickhouse_leaf_checks.py`, `test_company_domain_suggestions_dbt.py`, `fixtures/company_domain_suggestions/clickhouse.sql`, `test_clickhouse_migrations.py`, new `test_se_register_record_uid.py` | Pins follow. |
| backoffice `app/lib/countries.ts`, `address-companies.server.ts`, `address-quality.server.ts`, `company-domains.server.ts`, `technologies.server.ts`, `se-ratsit-results.server.ts`, `se-company-shell.server.ts` (modify); tests `app/lib/countries.test.ts`, `tests/address-quality.test.ts`, `tests/company-serving-sections.test.ts`, `tests/queries.server.test.ts`, `tests/se-company-tabs.server.test.ts`, `tests/technologies.server.test.ts` (modify) | Queries and pins. |
| `corpscout/clickhouse/migrations/000084`, `000244`, `000281` (modify) | Spine DDL leaves the files. |
| `corpscout/clickhouse/operations/se_companies_retire.md` (create) | Owner-run gated drop. |

---

### Task 1: Dagster readers move to the main table

**Files:**
- Modify: `src/dagster_v3/defs/sweden_uhm_procurement/clickhouse.py` (lines 117, 132, 168), `src/dagster_v3/defs/sweden_uhm_procurement/assets.py` (line 136)
- Modify: `src/dagster_v3/defs/sweden_platsbanken/clickhouse.py` (lines 313, 461), `src/dagster_v3/defs/sweden_platsbanken/assets.py` (line 501)
- Modify: `src/dagster_v3/defs/company_serving/publish.py` (lines 236, 242, 247, 255)
- Modify: `src/dagster_v3/defs/company_signals/rules.py` (line 100), `src/dagster_v3/defs/company_identifier/rules.py` (line 40), `src/dagster_v3/defs/company_identifier/assets.py` (line 16)
- Modify: `src/dagster_v3/defs/company_domain_suggestions/inputs.py` (lines 62-63), `dbt_run.py` (line 27), `assets.py` (line 116)
- Modify: `src/dagster_v3/defs/sweden_ratsit/assets.py` (lines 62, 1010)
- Modify tests: `tests/test_company_identifier.py` (45, 51, 56), `tests/test_company_signals_rules.py` (18), `tests/test_sweden_uhm_procurement_source.py` (181), `tests/test_sweden_platsbanken_assets.py` (119), `tests/test_sweden_platsbanken_clickhouse_local.py` (33), `tests/test_sweden_ratsit_pilot.py` (283, 674, 971)

**Interfaces:**
- Consumes: `se_company_basic_info` as listed in Global Constraints; asset key `se_company_basic_info_fold`.
- Produces: nothing new; every listed module names `se_company_basic_info` where it named `se_companies`.

- [x] **Step 1: Re-pin the tests**

Apply these exact substitutions:

- `tests/test_company_identifier.py`: line 45 `assert _SE.register_table == "se_companies"` becomes `assert _SE.register_table == "se_company_basic_info"`; line 51 docstring `"""se_companies is a ReplacingMergeTree; a raw join fans out."""` becomes `"""se_company_basic_info is a ReplacingMergeTree; a raw join fans out."""`; line 56 `assert "INNER JOIN corpscout.se_companies AS r" not in sql` becomes `assert "INNER JOIN corpscout.se_company_basic_info AS r" not in sql`.
- `tests/test_company_signals_rules.py`: line 18 `assert rule.companies_table == "se_companies"` becomes `assert rule.companies_table == "se_company_basic_info"`. Line 241 stays: it reads the historical 000182 text.
- `tests/test_sweden_uhm_procurement_source.py`: line 181 `assert "LEFT ANY JOIN corpscout.se_companies" in sql` becomes `assert "LEFT ANY JOIN corpscout.se_company_basic_info" in sql`.
- `tests/test_sweden_platsbanken_assets.py`: line 119 `dg.AssetKey("sweden_company_companies_clickhouse"),` becomes `dg.AssetKey("se_company_basic_info_fold"),`.
- `tests/test_sweden_platsbanken_clickhouse_local.py`: the stub `CREATE TABLE corpscout.se_companies (company_id String)` becomes `CREATE TABLE corpscout.se_company_basic_info (company_id String)` (engine and order unchanged).
- `tests/test_sweden_ratsit_pilot.py`: lines 283 and 674 `"FROM corpscout.se_companies FINAL"` become `"FROM corpscout.se_company_basic_info FINAL"`; line 971 `{dg.AssetKey("sweden_company_companies_clickhouse")}` becomes `{dg.AssetKey("se_company_basic_info_fold")}`.

- [x] **Step 2: Run them to verify they fail**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run pytest tests/test_company_identifier.py tests/test_company_signals_rules.py tests/test_sweden_uhm_procurement_source.py tests/test_sweden_platsbanken_assets.py tests/test_sweden_ratsit_pilot.py -q -k "register or companies_table or LEFT_ANY or parent_keys or selection or spine or scan_dispatch or sweden_reads" 2>&1 | tail -3`
Expected: FAIL on the re-pinned assertions.

- [x] **Step 3: Re-point the modules**

Exact substitutions (each string occurs once unless noted):

- `sweden_uhm_procurement/clickhouse.py`: both `LEFT ANY JOIN corpscout.se_companies AS c` (two occurrences) become `LEFT ANY JOIN corpscout.se_company_basic_info AS c`; `tables=(tables.AWARDS_TABLE, "se_companies"),` becomes `tables=(tables.AWARDS_TABLE, "se_company_basic_info"),`; the docstring `"""Publish every UHM observation and annotate exact ``se_companies`` matches."""` becomes `...exact ``se_company_basic_info`` matches."""`.
- `sweden_uhm_procurement/assets.py`: `dg.AssetKey("sweden_company_companies_clickhouse"),` becomes `dg.AssetKey("se_company_basic_info_fold"),`.
- `sweden_platsbanken/clickhouse.py`: `FROM corpscout.se_companies FINAL` becomes `FROM corpscout.se_company_basic_info FINAL`; in the table tuple `"se_companies",` becomes `"se_company_basic_info",`.
- `sweden_platsbanken/assets.py`: `dg.AssetKey("sweden_company_companies_clickhouse"),` becomes `dg.AssetKey("se_company_basic_info_fold"),`.
- `company_serving/publish.py`: `"LEFT JOIN corpscout.se_companies AS company FINAL "` becomes `"LEFT JOIN corpscout.se_company_basic_info AS company FINAL "`; `rows without se_companies anchor` becomes `rows without se_company_basic_info anchor`; `"SELECT count() FROM corpscout.se_companies FINAL"` becomes `"SELECT count() FROM corpscout.se_company_basic_info FINAL"`; `reconciliation failed: se_companies={anchors}` becomes `reconciliation failed: se_company_basic_info={anchors}`.
- `company_signals/rules.py`: `companies_table="se_companies",` becomes `companies_table="se_company_basic_info",`; the comment `se_companies keys on company_id,` becomes `se_company_basic_info keys on company_id,`.
- `company_identifier/rules.py`: `register_table="se_companies",` becomes `register_table="se_company_basic_info",`; the comment `They disagree — se_companies.company_id,` becomes `They disagree — se_company_basic_info.company_id,`.
- `company_identifier/assets.py`: `"sweden_company_companies_clickhouse",` in `COMPANY_IDENTIFIER_UPSTREAM_ASSET_KEYS` becomes `"se_company_basic_info_fold",`.
- `company_domain_suggestions/inputs.py`: the `COMPANIES_SQL` body becomes
  ```
  SELECT company_id, legal_name
  FROM corpscout.se_company_basic_info FINAL
  WHERE company_id != ''
  ORDER BY company_id
  ```
- `company_domain_suggestions/dbt_run.py`: `"SELECT count() FROM corpscout.se_companies FINAL WHERE company_id != ''",` becomes `"SELECT count() FROM corpscout.se_company_basic_info FINAL WHERE company_id != ''",`.
- `company_domain_suggestions/assets.py`: `"se_companies",` in the table tuple becomes `"se_company_basic_info",`.
- `sweden_ratsit/assets.py`: `RATSIT_ACTIVE_COMPANIES_TABLE = "se_companies"` becomes `RATSIT_ACTIVE_COMPANIES_TABLE = "se_company_basic_info"`; the asset dep `deps=[dg.AssetKey("sweden_company_companies_clickhouse")],` becomes `deps=[dg.AssetKey("se_company_basic_info_fold")],`; the description `"Selects one of 128 stable CRC32 buckets from active corpscout.se_companies, "` becomes `"...from active corpscout.se_company_basic_info, "`.

- [x] **Step 4: Run the tests**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run pytest tests/test_company_identifier.py tests/test_company_signals_rules.py tests/test_sweden_uhm_procurement_source.py tests/test_sweden_platsbanken_assets.py tests/test_sweden_ratsit_pilot.py tests/test_company_domain_suggestions_dbt.py tests/test_company_serving_dbt.py -q 2>&1 | tail -3`
Expected: PASS except `test_company_domain_suggestions_dbt.py` items that pin dbt sources (Task 3). Then `uv run pytest tests/test_sweden_platsbanken_clickhouse_local.py -q` (docker): PASS.

- [x] **Step 5: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_uhm_procurement corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_platsbanken corpscout/services/dagster_v3/src/dagster_v3/defs/company_serving/publish.py corpscout/services/dagster_v3/src/dagster_v3/defs/company_signals/rules.py corpscout/services/dagster_v3/src/dagster_v3/defs/company_identifier corpscout/services/dagster_v3/src/dagster_v3/defs/company_domain_suggestions/inputs.py corpscout/services/dagster_v3/src/dagster_v3/defs/company_domain_suggestions/dbt_run.py corpscout/services/dagster_v3/src/dagster_v3/defs/company_domain_suggestions/assets.py corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_ratsit/assets.py corpscout/services/dagster_v3/tests/test_company_identifier.py corpscout/services/dagster_v3/tests/test_company_signals_rules.py corpscout/services/dagster_v3/tests/test_sweden_uhm_procurement_source.py corpscout/services/dagster_v3/tests/test_sweden_platsbanken_assets.py corpscout/services/dagster_v3/tests/test_sweden_platsbanken_clickhouse_local.py corpscout/services/dagster_v3/tests/test_sweden_ratsit_pilot.py
git commit -m "refactor(dagster): SE anchor, name and status reads move from se_companies to se_company_basic_info

UHM, Platsbanken, the serving publish checks, the signals and identifier
rules, domain suggestions and the Ratsit selector read the folded main
table; their lineage points at the basic-info fold."
```
(append the footer)

---

### Task 2: Spine builder removed, check re-targeted, leaf and job updated

**Files:**
- Modify: `src/dagster_v3/defs/sweden_company/assets.py` (asset at lines 136-159; check at 361-380; job selection line 390; `defs` assets list line 414)
- Modify: `src/dagster_v3/defs/sweden_company/clickhouse.py` (delete `export_sweden_company_clickhouse_companies`, lines 15-50)
- Modify: `src/dagster_v3/defs/sweden_company/tables.py` (lines 8, 16, 49, 112: `COMPANIES_TABLE_CH`, `QUALIFIED_COMPANIES_TABLE`, `spine_asset_key`, `SE_COMPANIES_EXPORT_COLUMNS`)
- Modify: `src/dagster_v3/defs/common/clickhouse_checks.py` (line 189)
- Modify tests: `tests/test_sweden_company_assets.py` (24, 42, 85), `tests/test_clickhouse_leaf_checks.py` (46), `tests/test_clickhouse_migrations.py` (the `expected_columns_by_table` map at line 1458)

**Interfaces:**
- Consumes: `tables.QUALIFIED_SCB_COMPANIES_TABLE` (exists).
- Produces: `identities_normalized` attached to `sweden_company_scb_companies_clickhouse`; no `sweden_company_companies_clickhouse` asset; `WIKIDATA_REGISTRY_SEED_SPEC.spine_asset_key == "se_company_basic_info_fold"`.

- [x] **Step 1: Re-pin the tests**

- `tests/test_sweden_company_assets.py`: remove `"sweden_company_companies_clickhouse",` from the job's expected key set (line 24) and from the group loop tuple (line 42); line 85 becomes `assert ("sweden_company_scb_companies_clickhouse", "identities_normalized") in names`.
- `tests/test_clickhouse_leaf_checks.py`: remove the line `"sweden_company_companies_clickhouse",`.
- `tests/test_clickhouse_migrations.py`: in `test_sweden_company_registry_migration_covers_exported_columns`, delete the entry `sweden_company_tables.COMPANIES_TABLE_CH: sweden_company_tables.SE_COMPANIES_EXPORT_COLUMNS,` from `expected_columns_by_table`.

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run pytest tests/test_sweden_company_assets.py tests/test_clickhouse_leaf_checks.py -q 2>&1 | tail -2`
Expected: FAIL (the asset and leaf still exist).

- [x] **Step 2: Remove the builder and re-target the check**

- `sweden_company/assets.py`: delete the whole `sweden_company_companies_clickhouse` asset (decorator through `return dg.MaterializeResult(...)`, lines 136-159). Change the check:
  ```python
  @dg.asset_check(
      asset="sweden_company_scb_companies_clickhouse",
      name="identities_normalized",
  )
  def identities_normalized(clickhouse: ClickhouseResource) -> dg.AssetCheckResult:
      """Guard against re-publishing 16-prefixed duplicate identities.

      A dagster deploy running pre-2026-07-18 code would export SCB rows keyed as
      12-digit '16'+orgnr, silently re-splitting ~745k companies and breaking every
      downstream join. Checked on the SCB register export, where the ids originate,
      since the se_companies spine retired (basic-info slice 5, 2026-09-08).
      """
      with clickhouse.get_connection() as client:
          [(prefixed, total, distinct)] = client.execute(
              "SELECT countIf(length(company_id) = 12 AND startsWith(company_id, '16')), "
              "count(), uniqExact(company_id) "
              f"FROM {tables.QUALIFIED_SCB_COMPANIES_TABLE} FINAL"
          )
  ```
  (the `return dg.AssetCheckResult(...)` block is unchanged). Remove `"sweden_company_companies_clickhouse",` from the `sweden_company_refresh_job` selection and `sweden_company_companies_clickhouse,` from the `defs` assets list. Delete the now-unused import of `export_sweden_company_clickhouse_companies` if it is imported by name.
- `sweden_company/clickhouse.py`: delete `export_sweden_company_clickhouse_companies` (lines 15-50) and any import only it used (`export_duckdb_connection_table_to_clickhouse` stays if another function uses it; check with `rg -n "export_duckdb_connection_table_to_clickhouse" src/dagster_v3/defs/sweden_company/clickhouse.py`).
- `sweden_company/tables.py`: delete `COMPANIES_TABLE_CH = "se_companies"`, `QUALIFIED_COMPANIES_TABLE = ...`, and the `SE_COMPANIES_EXPORT_COLUMNS = (...)` tuple; `spine_asset_key="sweden_company_companies_clickhouse",` becomes `spine_asset_key="se_company_basic_info_fold",`.
- `common/clickhouse_checks.py`: delete the line `ClickhouseLeaf("sweden_company_companies_clickhouse", ("se_companies",), WEEKLY),`.

Then `rg -n "COMPANIES_TABLE_CH\b|QUALIFIED_COMPANIES_TABLE|SE_COMPANIES_EXPORT_COLUMNS|sweden_company_companies_clickhouse" src tests` must return only the other countries' identically named constants (slovakia_rpo, czech_ares, france_sirene, uk_companies_house, norway_brreg) and `tests/test_wikidata_assets.py:380` (a `not in` assertion that stays true).

- [x] **Step 3: Check definitions and run the tests**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run dg check defs 2>&1 | tail -1 && WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run pytest tests/test_sweden_company_assets.py tests/test_clickhouse_leaf_checks.py tests/test_clickhouse_migrations.py tests/test_wikidata_assets.py tests/test_sweden_company_source_tables.py -q 2>&1 | tail -2`
Expected: "All definitions loaded successfully." and PASS.

- [x] **Step 4: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company corpscout/services/dagster_v3/src/dagster_v3/defs/common/clickhouse_checks.py corpscout/services/dagster_v3/tests/test_sweden_company_assets.py corpscout/services/dagster_v3/tests/test_clickhouse_leaf_checks.py corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py
git commit -m "refactor(dagster): retire the se_companies spine export

The identities_normalized check guards the SCB register export instead;
the freshness leaf, the job entry and the export column list go."
```
(append the footer)

---

### Task 3: dbt sources and models, shared uid macro

**Files:**
- Modify: `src/dagster_v3/defs/se_company/common.py` (`bolagsverket_record_uid_sql`)
- Create: `src/dagster_v3/defs/company_serving/dbt/macros/se_register_record_uid.sql`
- Modify: `src/dagster_v3/defs/company_serving/dbt/models/sources.yml`, `macros/company_serving_tests.sql`, the seven models (`company_contract_current_build`, `company_description_current_build`, `company_domains_build`, `company_external_identifier_current_build`, `company_management_current_build`, `company_section_presence_current_build`, `se_company_industry_display_current_build`) and `company_section_item_source_links_build.sql`
- Modify: `src/dagster_v3/defs/company_domain_suggestions/dbt/models/sources.yml`, `models/staging/stg_se_company_match_features.sql`
- Modify tests: `tests/fixtures/company_domain_suggestions/clickhouse.sql`, `tests/test_company_domain_suggestions_dbt.py` (lines 91, 428), `tests/test_company_serving_dbt.py` (line 61, 178)
- Create: `tests/test_se_register_record_uid.py`

**Interfaces:**
- Produces: `register_record_uid_sql(source_slug: str, alias: str) -> str` in `common.py`; dbt macro `se_register_record_uid(source_slug, alias)` rendering the identical text.

- [x] **Step 1: Write the parity test and re-pin the dbt tests**

Create `tests/test_se_register_record_uid.py`:

```python
"""The company-source-record uid is rendered in two languages -- Python for the
basic-info extractors and the serving builder, Jinja for the company_serving dbt models --
and the two must never drift: a uid that differs by one byte links nothing."""

from pathlib import Path

from dagster_v3.defs.se_company.common import bolagsverket_record_uid_sql, register_record_uid_sql

MACRO = (
    Path(__file__).resolve().parents[1]
    / "src/dagster_v3/defs/company_serving/dbt/macros/se_register_record_uid.sql"
)


def _macro_render(source_slug: str, alias: str) -> str:
    body = MACRO.read_text(encoding="utf-8")
    start = body.index("lower(hex(SHA256(")
    end = body.index("{%- endmacro %}")
    return body[start:end].strip().replace("{{ source_slug }}", source_slug).replace("{{ alias }}", alias)


def test_python_and_dbt_render_the_same_uid_expression() -> None:
    for slug, alias in (("sweden_bolagsverket", "b"), ("sweden_scb", "s")):
        assert _macro_render(slug, alias) == register_record_uid_sql(slug, alias)


def test_bolagsverket_helper_is_the_general_one_with_its_slug() -> None:
    assert bolagsverket_record_uid_sql("register") == register_record_uid_sql("sweden_bolagsverket", "register")
```

`tests/test_company_domain_suggestions_dbt.py`: line 91 `"se_companies",` in the sources tuple becomes `"se_company_basic_info",`; line 428 `if "FROM corpscout.se_companies" in normalized_sql:` becomes `if "FROM corpscout.se_company_basic_info" in normalized_sql:`.

`tests/test_company_serving_dbt.py`: line 61 `"dagster/table_name": "corpscout.se_companies",` becomes `"dagster/table_name": "corpscout.se_company_basic_info",`; line 178 `assert "source('corpscout', 'se_companies')" in model` becomes `assert "source('corpscout', 'se_company_basic_info')" in model`. Read the surrounding test first: if it iterates every model asserting the anchor source, the assertion text is the only change.

`tests/fixtures/company_domain_suggestions/clickhouse.sql`: the `CREATE TABLE corpscout.se_companies (company_id String, legal_name Nullable(String))` block becomes `CREATE TABLE corpscout.se_company_basic_info (company_id String, legal_name String) ENGINE = ReplacingMergeTree ORDER BY company_id;` and `INSERT INTO corpscout.se_companies VALUES` becomes `INSERT INTO corpscout.se_company_basic_info VALUES`.

Run: `uv run pytest tests/test_se_register_record_uid.py tests/test_company_domain_suggestions_dbt.py tests/test_company_serving_dbt.py -q 2>&1 | tail -3`
Expected: FAIL (`register_record_uid_sql` missing; sources still name `se_companies`).

- [x] **Step 2: The shared helper and macro**

In `src/dagster_v3/defs/se_company/common.py` replace `bolagsverket_record_uid_sql` with:

```python
def register_record_uid_sql(source_slug: str, alias: str) -> str:
    """A register row's company-source-record uid, as SQL over `alias`.

    The one expression the basic-info register extractors write as source_record_uid,
    the serving view renders as bolagsverket_source_record_uid, and the company_serving
    dbt macro se_register_record_uid renders for its source records, so none can drift:
    sha256 of the fixed envelope, the source slug, the register's source_record_id and
    its lower-cased payload hash.
    """
    return (
        "lower(hex(SHA256(concat('company-source-record-v1\\nstructured\\n', "
        f"'{source_slug}', '\\nregistry_company\\n', {alias}.source_record_id, '\\n', "
        f"lowerUTF8({alias}.source_payload_hash)))))"
    )


def bolagsverket_record_uid_sql(alias: str) -> str:
    """`register_record_uid_sql` for the Bolagsverket register."""
    return register_record_uid_sql("sweden_bolagsverket", alias)
```

Confirm the Bolagsverket rendering is byte-identical to before: `uv run pytest tests/test_se_company_basic_info_extractors_sql.py tests/test_se_companies_serving_mv.py -q` must pass unchanged (the extractor pin and the 000391/000392 drift pins would fail on any change).

Create `src/dagster_v3/defs/company_serving/dbt/macros/se_register_record_uid.sql`:

```sql
{#- The company-source-record uid of a Swedish register row, byte-identical to
    dagster_v3.defs.se_company.common.register_record_uid_sql (tests/test_se_register_record_uid.py
    pins the two equal). source_slug is 'sweden_bolagsverket' or 'sweden_scb'; alias names a row
    of se_bolagsverket_companies or se_scb_companies. -#}
{%- macro se_register_record_uid(source_slug, alias) -%}
lower(hex(SHA256(concat('company-source-record-v1\nstructured\n', '{{ source_slug }}', '\nregistry_company\n', {{ alias }}.source_record_id, '\n', lowerUTF8({{ alias }}.source_payload_hash)))))
{%- endmacro %}
```

- [x] **Step 3: Sources and models**

`company_serving/dbt/models/sources.yml`: replace `      - name: se_companies` with three lines `      - name: se_company_basic_info`, `      - name: se_bolagsverket_companies`, `      - name: se_scb_companies`.

`company_domain_suggestions/dbt/models/sources.yml`: replace `      - name: se_companies` with `      - name: se_company_basic_info`.

In each of the seven company_serving models and in `company_serving_tests.sql`, replace `{{ source('corpscout', 'se_companies') }}` with `{{ source('corpscout', 'se_company_basic_info') }}` (the anchor CTEs and the anchor test read `company_id` only).

`stg_se_company_match_features.sql`: the companies CTE becomes

```sql
WITH companies AS (
    SELECT
        company_id,
        legal_name AS company_name
    FROM {{ source('corpscout', 'se_company_basic_info') }} FINAL
    WHERE match(company_id, '^[0-9]{10}$')
),
```

`company_section_item_source_links_build.sql`: replace the two register branches of `source_records_raw` (the Bolagsverket `SELECT ... FROM se_companies FINAL WHERE bolagsverket_source_record_uid != ''` and the SCB one) with:

```sql
    SELECT
        {{ se_register_record_uid('sweden_bolagsverket', 'b') }} AS source_record_uid,
        'registry_company' AS record_kind,
        lowerUTF8(b.source_payload_hash) AS content_sha256,
        b.observed_at AS first_seen_at,
        b.observed_at AS last_seen_at,
        'sweden_bolagsverket' AS source_slug,
        b.source_record_id AS source_record_key,
        '' AS source_url,
        '' AS source_object_key,
        lowerUTF8(b.source_payload_hash) AS payload_sha256,
        b.observed_at AS retrieved_at,
        b.source_run_id
    FROM {{ source('corpscout', 'se_bolagsverket_companies') }} AS b FINAL
    WHERE b.has_company = 1 AND b.source_payload_hash != ''
    UNION ALL
    SELECT
        {{ se_register_record_uid('sweden_scb', 's') }}, 'registry_company', lowerUTF8(s.source_payload_hash),
        s.observed_at, s.observed_at, 'sweden_scb',
        s.source_record_id, '', '', lowerUTF8(s.source_payload_hash),
        s.observed_at, s.source_run_id
    FROM {{ source('corpscout', 'se_scb_companies') }} AS s FINAL
    WHERE s.has_company = 1 AND s.source_payload_hash != ''
```

Replace the `registry_source_uids` CTE with:

```sql
registry_source_uids AS (
    SELECT b.company_id AS company_id, {{ se_register_record_uid('sweden_bolagsverket', 'b') }} AS source_record_uid
    FROM {{ source('corpscout', 'se_bolagsverket_companies') }} AS b FINAL
    WHERE b.has_company = 1 AND b.source_payload_hash != ''
    UNION ALL
    SELECT s.company_id, {{ se_register_record_uid('sweden_scb', 's') }}
    FROM {{ source('corpscout', 'se_scb_companies') }} AS s FINAL
    WHERE s.has_company = 1 AND s.source_payload_hash != ''
),
```

and the ESEF join `INNER JOIN {{ source('corpscout', 'se_companies') }} AS companies FINAL ON companies.registration_number = mappings.registry_id` becomes `INNER JOIN {{ source('corpscout', 'se_company_basic_info') }} AS companies FINAL ON companies.company_id = mappings.registry_id`. The `company_anchors` CTE at the top follows the seven-model rule.

Then `rg -n "se_companies'" src/dagster_v3/defs/company_serving/dbt src/dagster_v3/defs/company_domain_suggestions/dbt` must print nothing.

- [x] **Step 4: Parse and test**

Run: `uv run --frozen --no-sync dbt parse --project-dir src/dagster_v3/defs/company_serving/dbt --profiles-dir src/dagster_v3/defs/company_serving/dbt 2>&1 | tail -2 && uv run --frozen --no-sync dbt parse --project-dir src/dagster_v3/defs/company_domain_suggestions/dbt --profiles-dir src/dagster_v3/defs/company_domain_suggestions/dbt 2>&1 | tail -2` (if a project has no profiles dir beside it, use the one the `.local_defs_state` refresh uses; `ls src/dagster_v3/defs/*/dbt/profiles.yml` lists them).
Expected: both parse.

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run pytest tests/test_se_register_record_uid.py tests/test_company_domain_suggestions_dbt.py tests/test_company_serving_dbt.py tests/test_se_company_basic_info_extractors_sql.py tests/test_se_companies_serving_mv.py -q 2>&1 | tail -2`
Expected: PASS, none skipped (the domain-suggestions dbt local run uses docker).

- [x] **Step 5: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/common.py corpscout/services/dagster_v3/src/dagster_v3/defs/company_serving/dbt corpscout/services/dagster_v3/src/dagster_v3/defs/company_domain_suggestions/dbt corpscout/services/dagster_v3/tests/test_se_register_record_uid.py corpscout/services/dagster_v3/tests/test_company_domain_suggestions_dbt.py corpscout/services/dagster_v3/tests/test_company_serving_dbt.py corpscout/services/dagster_v3/tests/fixtures/company_domain_suggestions/clickhouse.sql
git commit -m "refactor(dbt): company_serving and domain-suggestions read se_company_basic_info and the register tables

Register source records come from se_bolagsverket_companies and
se_scb_companies through the se_register_record_uid macro, pinned equal to
the Python helper."
```
(append the footer)

---

### Task 4: Backoffice readers and the public SE shell

**Files:**
- Modify: `app/lib/countries.ts` (line 750 `companiesTable`, lines 861-873 `companyShellQuery`), `app/lib/address-companies.server.ts` (66-73), `app/lib/address-quality.server.ts` (311-315), `app/lib/company-domains.server.ts` (591, 620), `app/lib/technologies.server.ts` (424-428), `app/lib/se-ratsit-results.server.ts` (121-125), `app/lib/se-company-shell.server.ts` (71-78 and the comments at 16, 57, 264 of the tabs test)
- Modify tests: `app/lib/countries.test.ts` (22, 249-256), `tests/company-serving-sections.test.ts` (118-124), `tests/se-company-tabs.server.test.ts` (132), `tests/technologies.server.test.ts` (325), `tests/address-quality.test.ts` (51), `tests/queries.server.test.ts` (779)

**Interfaces:**
- Consumes: the main table and register columns from Global Constraints.
- Produces: the SE `companyShellQuery` projecting `company_id`, `registration_number`, `legal_name`, `legal_name_raw`, `legal_form_code`, `status`, `status_source`, `status_reason`, `dissolution_date`, `activity_description_original`, `registration_date`, `source_run_id`, `updated_from_raw_at` and the six `__shell_*` fields.

- [x] **Step 1: Re-pin the tests**

- `app/lib/countries.test.ts`: line 22 `expect(se?.companiesTable).toBe("se_companies");` becomes `toBe("se_company_basic_info")`; line 249 `toContain("FROM se_companies AS c")` becomes `toContain("FROM se_company_basic_info AS i FINAL")`; line 251-253 `"activity_description AS activity_description_original"` becomes `"i.description_sv AS activity_description_original"`; line 254-256 `"incorporation_date AS registration_date"` stays (it matches `i.incorporation_date AS registration_date`).
- `tests/company-serving-sections.test.ts`: `"FROM se_companies AS c"` becomes `"FROM se_company_basic_info AS i FINAL"` and `"PREWHERE c.company_id = {id:String}"` becomes `"WHERE i.company_id = {id:String}"`.
- `tests/se-company-tabs.server.test.ts`: line 132 `"corpscout.se_companies AS c FINAL",` becomes `"corpscout.se_company_basic_info AS c FINAL",`; the comment at 264 `and se_companies carries` becomes `and the register fallback carries`.
- `tests/technologies.server.test.ts`: line 325 `"FROM corpscout.se_companies FINAL",` becomes `"FROM corpscout.se_company_basic_info FINAL",`; the test title at 296 `enriches SE rows with se_companies FINAL names` becomes `enriches SE rows with se_company_basic_info FINAL names`.
- `tests/address-quality.test.ts`: line 51 `sql.includes("FROM corpscout.se_companies")` becomes `sql.includes("FROM corpscout.se_company_basic_info")`.
- `tests/queries.server.test.ts` (live): the query at 779 becomes
  ```
  SELECT company_id AS id FROM se_company_basic_info FINAL
       WHERE company_id IN (
         SELECT company_id
         FROM se_company_addresses_current
         WHERE has_address = 1 AND street_address != ''
       )
       ORDER BY company_id LIMIT 1
  ```

Run: `npx vitest run app/lib/countries.test.ts tests/company-serving-sections.test.ts tests/se-company-tabs.server.test.ts tests/technologies.server.test.ts tests/address-quality.test.ts 2>&1 | rg "Tests |FAIL" | head -3`
Expected: failures on the re-pinned assertions.

- [x] **Step 2: Re-point the queries**

- `countries.ts` line 750: `companiesTable: "se_companies",` becomes `companiesTable: "se_company_basic_info",`; reword the comment above `idColumn` to `// The canonical company_id is the normalized 10- or 12-digit organization number and the table's sorting key.` Replace the whole `companyShellQuery` template (from ``companyShellQuery: `SELECT c.* EXCEPT`` through ``LIMIT 1`,``) with:
  ```ts
      companyShellQuery: `SELECT
    i.company_id AS company_id,
    i.company_id AS registration_number,
    i.legal_name AS legal_name,
    b.legal_name_raw AS legal_name_raw,
    i.legal_form_code AS legal_form_code,
    i.status AS status,
    i.status_source AS status_source,
    b.deregistration_reason AS status_reason,
    b.deregistration_date AS dissolution_date,
    i.description_sv AS activity_description_original,
    i.incorporation_date AS registration_date,
    i.source_run_id AS source_run_id,
    i.folded_at AS updated_from_raw_at,
    i.company_id AS __shell_id,
    i.legal_name AS __shell_name,
    i.legal_form_code AS __shell_legal_form,
    i.status AS __shell_status,
    toString(i.incorporation_date) AS __shell_registered,
    toUInt8(i.status = 'active') AS __shell_active,
    i.company_id AS __shell_industry_key
  FROM se_company_basic_info AS i FINAL
  LEFT JOIN (
    SELECT company_id, legal_name_raw, deregistration_reason, deregistration_date
    FROM se_bolagsverket_companies FINAL
    WHERE has_company = 1
  ) AS b ON b.company_id = i.company_id
  WHERE i.company_id = {id:String}
  LIMIT 1`,
  ```
  and replace the comment block above it with: `// The shell reads the folded basic-info row plus the Bolagsverket register fields the page has always shown (dissolution date, status reason, raw name), under the column names the record card and its lineage bucket already know. The translated activity is served lazily through company_description_current.`
- `address-companies.server.ts`: the SELECT becomes
  ```
  SELECT
    company_id,
    legal_name AS company_name,
    status AS status
  FROM corpscout.se_company_basic_info FINAL
  WHERE company_id IN matching_company_ids
    AND company_id != {id:String}
  ORDER BY lowerUTF8(company_name), company_id
  LIMIT 51`;
  ```
- `address-quality.server.ts`: the SELECT becomes `SELECT company_id, legal_name AS company_name FROM corpscout.se_company_basic_info FINAL WHERE company_id IN {companyIds:Array(String)}` (keep the template's line breaks).
- `company-domains.server.ts`: both `INNER JOIN se_companies AS companies FINAL` become `INNER JOIN se_company_basic_info AS companies FINAL`.
- `technologies.server.ts` and `se-ratsit-results.server.ts`: `FROM corpscout.se_companies FINAL` becomes `FROM corpscout.se_company_basic_info FINAL`.
- `se-company-shell.server.ts`: in `SHELL_REGISTER_SQL`, `FROM corpscout.se_companies AS c FINAL` becomes `FROM corpscout.se_company_basic_info AS c FINAL`; comments: line 16 `and se_companies carries` becomes `and the fallback carries`; line 57 `se_companies are ReplacingMergeTrees` becomes `the fallback read the same ReplacingMergeTree`.

Then `rg -n "\bse_companies\b" app tests --glob '!*serving*'` must print only comment lines that describe history (`address-quality.server.ts` docs, `countries.ts` comments about translations) or nothing; fix any query it finds.

- [x] **Step 3: Typecheck and tests**

Run: `npm run typecheck 2>&1 | rg "error TS" | head -3; npx vitest run app/lib/countries.test.ts tests/company-serving-sections.test.ts tests/se-company-tabs.server.test.ts tests/technologies.server.test.ts tests/address-quality.test.ts 2>&1 | rg "Tests |FAIL" | head -3`
Expected: no TS errors; the five files pass.

- [x] **Step 4: Commit**

```bash
git add app/lib/countries.ts app/lib/address-companies.server.ts app/lib/address-quality.server.ts app/lib/company-domains.server.ts app/lib/technologies.server.ts app/lib/se-ratsit-results.server.ts app/lib/se-company-shell.server.ts app/lib/countries.test.ts tests/company-serving-sections.test.ts tests/se-company-tabs.server.test.ts tests/technologies.server.test.ts tests/address-quality.test.ts tests/queries.server.test.ts
git commit -m "refactor(backoffice): SE company reads move from se_companies to se_company_basic_info

The public SE company shell projects the folded row plus the Bolagsverket
register's dissolution date, reason and raw name under its old column
names; address, domains, technologies and Ratsit lookups read the main
table."
```
(append the footer)

---

### Task 5: Ledger edits and the drop script

**Files:**
- Modify: `corpscout/clickhouse/migrations/000084_corpscout_se_company_registry.{up,down}.sql`, `000244_corpscout_company_source_records.{up,down}.sql`, `000281_corpscout_se_company_presentation_fields.{up,down}.sql`
- Modify: `tests/test_clickhouse_migrations.py` (`EMPTIED_MIGRATIONS`; delete `test_sweden_company_presentation_fields_are_migrated`; the 000084 export-column test already lost its spine entry in Task 2)
- Create: `corpscout/clickhouse/operations/se_companies_retire.md`

- [x] **Step 1: Ledger**

- 000084 up: delete the `CREATE TABLE IF NOT EXISTS corpscout.se_companies (...) ENGINE = ReplacingMergeTree(updated_from_raw_at) ORDER BY (company_id);` block (lines 3-29) and add above the next statement the comment `-- se_companies (the old Sweden company spine) was dropped by hand in basic-info slice 5 (2026-09-08) and its DDL left this file per the dev-phase ledger policy.` 000084 down: delete `DROP TABLE IF EXISTS corpscout.se_companies;`.
- 000244 up: delete the `ALTER TABLE corpscout.se_companies ADD COLUMN ... scb_source_record_uid ... AFTER scb_source_payload_hash;` statement (lines 192-210). 000244 down: delete its `ALTER TABLE corpscout.se_companies` statement (line 20 onward to its semicolon).
- 000281 up and down: replaced by the three-line comment plus `CREATE DATABASE IF NOT EXISTS corpscout;` (the emptied shape from slice 4), and `"000281_corpscout_se_company_presentation_fields"` joins `EMPTIED_MIGRATIONS` with the comment `# Basic-info slice 5 (2026-09-08): the se_companies spine was dropped by hand.`
- Delete `test_sweden_company_presentation_fields_are_migrated` from `tests/test_clickhouse_migrations.py`.
- Check no comment line gained a semicolon: `rg -n "^\s*--.*;" corpscout/clickhouse/migrations/000084_* corpscout/clickhouse/migrations/000244_* corpscout/clickhouse/migrations/000281_*` prints nothing.

Run: `uv run pytest tests/test_clickhouse_migrations.py tests/test_company_signals_rules.py tests/test_sweden_company_source_tables.py -q 2>&1 | tail -2`
Expected: PASS.

- [x] **Step 2: Drop script**

Create `corpscout/clickhouse/operations/se_companies_retire.md`:

```markdown
# Retire the se_companies spine (basic-info slice 5)

Owner-run, on the companycollect ClickHouse, AFTER the slice-5 dagster_v3 deploy, the
dbt rebuilds (company_domain_suggestions staging, company_serving publish) and the
backoffice merge are live and smoke-tested.

## Gates

```sql
-- 1. No view or materialized view names the spine (stg_se_company_match_features is
--    rebuilt by dbt from se_company_basic_info before this runs).
SELECT count() = 0 AS no_readers FROM system.tables
WHERE database = 'corpscout' AND engine IN ('View', 'MaterializedView')
  AND match(create_table_query, 'se_companies([^_a-z]|$)');
-- 2. Row counts recorded.
SELECT count() FROM corpscout.se_companies;
SELECT count() FROM corpscout.text_translations
WHERE source_table = 'corpscout.se_companies' AND source_column = 'activity_description';
```

Dagster gates: the code location has no `sweden_company_companies_clickhouse` asset, and
`sweden_company_translation_load`'s field names `corpscout.se_bolagsverket_companies`.

## Drops

```sql
DROP TABLE IF EXISTS corpscout.se_companies;
ALTER TABLE corpscout.text_translations
    DELETE WHERE source_table = 'corpscout.se_companies' AND source_column = 'activity_description';
```

The DELETE is a lightweight delete over 2,176,663 rows; watch it with
`SELECT * FROM system.mutations WHERE table = 'text_translations' AND NOT is_done`.
```

- [x] **Step 3: Commit**

```bash
git add corpscout/clickhouse/migrations/000084_corpscout_se_company_registry.up.sql corpscout/clickhouse/migrations/000084_corpscout_se_company_registry.down.sql corpscout/clickhouse/migrations/000244_corpscout_company_source_records.up.sql corpscout/clickhouse/migrations/000244_corpscout_company_source_records.down.sql corpscout/clickhouse/migrations/000281_corpscout_se_company_presentation_fields.up.sql corpscout/clickhouse/migrations/000281_corpscout_se_company_presentation_fields.down.sql corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py corpscout/clickhouse/operations/se_companies_retire.md
git commit -m "chore(clickhouse): the se_companies spine's DDL leaves the ledger; gated drop script"
```
(append the footer)

---

### Task 6: Whole-suite verification and merge

- [x] **Step 1:** `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run pytest -q -m "not integration" --deselect tests/test_schedule_cron_contracts.py::test_every_schedule_fires_on_a_unique_minute_hour_pair -p no:cacheprovider 2>&1 | tail -6` from `corpscout/services/dagster_v3`. Expected: only the four failures already on main.
- [x] **Step 2:** `uv run pytest tests/test_sweden_platsbanken_clickhouse_local.py tests/test_company_domain_suggestions_dbt.py tests/test_se_companies_serving_sql.py -q`. Expected: PASS, none skipped.
- [x] **Step 3:** backoffice `npm run typecheck && npx vitest run 2>&1 | rg "Tests |×"`. Expected: only the live-database timeouts already on main.
- [x] **Step 4:** `git checkout main && git merge --no-ff se-basic-info-5-retire-spine -m "Merge branch 'se-basic-info-5-retire-spine'"` (append the footer).

---

### Task 7: Prod rollout (owner-named)

- [x] **Step 1:** dbt state: from `corpscout/services/dagster_v3`, run the two `dbt parse` commands for `finland_ytj` and `exchange_rates_v2` and `uv run --frozen --no-sync dg utils refresh-defs-state` (the company_serving and domain-suggestions component projects changed), then `uv run --frozen --no-sync dg check defs`.
- [x] **Step 2:** deploy: `cd ansible && ANSIBLE_BECOME_TIMEOUT=60 ansible-playbook -i inventory.ini light_sync.yml`; verify `sweden_company_companies_clickhouse` is gone from the code location.
- [x] **Step 3:** materialise the company_domain_suggestions staging dbt assets (recreates `stg_se_company_match_features`) and the company_serving publish job; both must finish green (the publish job's anchor checks now count the main table).
- [x] **Step 4:** smoke `http://localhost:5183/company/se/5020077862`, the SE address list, the domains review queue and the technologies page.
- [x] **Step 5:** run `corpscout/clickhouse/operations/se_companies_retire.md`: gates, then the DROP and the DELETE.

## Rollout record (2026-09-08, UTC)

- 13:48 dbt state refreshed locally (3/3 states, both manifests); `dg check defs` clean.
- 14:38 light_sync deployed by the owner (a first attempt never reached the host; verified by
  md5 of the host tree, not by the code-location reload stamp). Code location: 636 assets, no
  `sweden_company_companies_clickhouse`, UHM and identifier exports depend on the fold.
- Run `0415b82a`: `stg_se_company_match_features` rebuilt from `se_company_basic_info` (SUCCESS).
- Run `3144bbe9`: company_serving dbt build + publish FAILED after 27 min on two anchor tests
  that predate this slice: `company_contact_current_build` (140 rows with company_id '' from
  ten unresolved ESEF LEIs, 5 rows for 5565401493) and `company_management_current_build`
  (24 ESEF rows for 5565401493, Rizzo Group AB (publ.), in no register). The ESEF sources grew
  after the last green publish (2026-08-25); neither id was ever in the spine. Owner decision:
  no change to the serving models; fix the ESEF people extraction and LEI mapping upstream.
  The publish stays red until then; the current serving tables keep the 2026-08-25 data.
- Gate: no view, MV or dictionary reads `se_companies` (the stg view's provenance literal
  `'se_companies.legal_name'` was renamed in `0ee29f509`, on the host with the next deploy;
  the gate regex matches FROM/JOIN). Counts: 3,523,558 spine rows, 2,176,663 spine-keyed
  translation rows.
- ~15:35 `DROP TABLE corpscout.se_companies` and the lightweight DELETE of the spine-keyed
  `text_translations` rows executed (owner: "Drop now"). Backoffice smoke after the drop green.
