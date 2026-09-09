# ESEF Slice 1: Country-Agnostic Products and the `se_esef_*` Views Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every ESEF product LEI-and-document keyed, give the entity map a register-verified `link_status`, create the eight `se_esef_*` views that expose `company_id` for Sweden, and move every Swedish consumer onto them, so the company_serving publish is green without touching its models and a mapping fix never needs a re-parse.

**Architecture:** The parse and model stages stop writing `country_iso2` / `country_code` / `company_id`; the model stage selects per LEI with the admitted LEIs resolved through the map at selection time. `esef_entity_registry_map` gains `link_status` (`register_verified` / `unverified` / `gleif`), verified with one LEFT JOIN per `COUNTRY_IDENTITY_RULES` country. A Python renderer (`esef_filings/country_views.py`) builds each `se_esef_<table>` view from the module's column tuples; migration 000395 embeds the rendered SQL and a drift test pins the two equal, the way `company_people/source_views.py` and its test pin the person views. The three model-output tables are recreated with `lei` and a document-keyed sorting key; the four big tables keep their stamp columns until an owner-run drop.

**Tech Stack:** Python 3.14 / Dagster / pytest (`uv run pytest`), ClickHouse (golang-migrate ledger), dbt-clickhouse, TypeScript / React Router / vitest (`npx vitest run`, `npm run typecheck`).

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-08-esef-entity-link-people-addresses-design.md` (revised 2026-09-09), section "1. Country-agnostic products, one map, the `se_esef_*` views".

## Global Constraints

- **Repo root:** `/Users/graovic/pulsarpoint/ppoint/companycollect`. Dagster commands from `corpscout/services/dagster_v3` with `uv run`; backoffice commands from `corpscout/services/backoffice`. Always `cd` with absolute paths (the shell's cwd drifts between calls).
- **Branch:** `esef-1-country-agnostic-products` from main. Commit by explicit path only, never `git add -A`.
- **Definitions-loading tests** need `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix`.
- **Migration number:** 000395 (000394 is applied on prod, the ledger is at 394). Register it in `EXPECTED_MIGRATIONS` in `tests/test_clickhouse_migrations.py`. Migration comments must not contain `;`.
- **View names (verbatim):** `se_esef_filings`, `se_esef_facts`, `se_esef_disclosures`, `se_esef_document_contact_candidates`, `se_esef_document_company_information`, `se_esef_document_people`, `se_esef_document_business_items`, `se_esef_document_group_relationships`. Every view: `company_id` first, then the product's own columns, joined on `lei` to `esef_entity_registry_map FINAL WHERE country_iso2 = 'SE' AND link_status = 'register_verified'`. Products that are ReplacingMergeTree (filings, facts, people, business items, group relationships) are read with `FINAL` inside the view; consumers never write `FINAL` after a view name.
- **`link_status` values (verbatim):** `register_verified`, `unverified`, `gleif`.
- **Ledger policy (dev phase):** stamp columns leave the CREATE blocks of 000243, 000246, 000316 and 000244 in place (files stay); the real column drops are owner-run from `corpscout/clickhouse/operations/esef_stamps_retire.md`.
- **Commit footer** on every commit:
  ```
  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01UJnba4eXZta4f9KaKJxhY9
  ```

---

## File map

| File | Change |
|---|---|
| `corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/tables.py` | stamps leave seven column tuples; `lei` enters the three model-output tuples; `link_status` enters the map tuple; new `SE_ESEF_VIEWS` registry |
| `corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/country_views.py` (new) | `build_se_esef_view_sql(view)` renders one `CREATE OR REPLACE VIEW` |
| `corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/publish.py:249-299` | map SELECT gains `link_status` |
| `corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/segment_assets.py` | manifest, document, contact-candidate and label rows without stamps; `load_esef_company_links` and `_company_id_for_index_row` deleted |
| `corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/segment_parser.py:196-203` | `EsefArtifactSource` loses `country` and `company_id` |
| `corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/disclosure_parser.py:115-130, 192-205` | disclosure rows without stamps |
| `corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/llm_enrichment_assets.py` | selection per LEI through the map; `link_statuses` config; information rows without stamps |
| `corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/company_information_projections.py` | projections write `lei`, no stamps |
| `corpscout/services/dagster_v3/src/dagster_v3/defs/company_people/source_views.py` | the esef person view reads `se_esef_document_people` |
| `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/basic_info/esef.py` | reads `se_esef_document_company_information` |
| `corpscout/services/dagster_v3/src/dagster_v3/defs/company_serving/dbt/models/sources.yml`, `company_section_item_source_links_build.sql`, `company_domains_build.sql`, `company_description_current_build.sql`, `company_management_current_build.sql`, `company_contact_current_build.sql` | ESEF legs read the views |
| `corpscout/clickhouse/migrations/000395_corpscout_esef_country_agnostic_products.{up,down}.sql` (new) | map column, three table recreations, eight views, the person view re-issued |
| `corpscout/clickhouse/migrations/000243, 000244, 000246, 000316` | stamp columns leave the CREATE blocks; 000244's three blocks take the new shape |
| `corpscout/clickhouse/operations/esef_stamps_retire.md` (new) | owner-run column drops and `_legacy` drops |
| `corpscout/services/backoffice/app/lib/se-company-esef.server.ts`, `app/lib/queries.server.ts` | Swedish ESEF queries read the views |
| tests: `tests/test_esef_country_views.py` (new), `test_esef_filings_publish.py`, `test_esef_llm_enrichment.py`, `test_esef_company_information_projections.py`, `test_esef_fact_disclosures.py`, `test_se_company_person_views.py`, `test_company_serving_dbt.py`, `test_clickhouse_migrations.py`; backoffice `tests/se-company-esef.server.test.ts`, `tests/source-information-sections.test.tsx` | re-pinned |
| `corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/docs/esef_filings-design.md`, `docs/esef-ixbrl-segment-parser.md` | amended |

---

## Task 1: Column tuples, the view renderer and migration 000395

**Files:**
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/tables.py`
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/country_views.py`
- Create: `corpscout/clickhouse/migrations/000395_corpscout_esef_country_agnostic_products.up.sql`, `.down.sql`
- Modify: `corpscout/clickhouse/migrations/000243_corpscout_esef_source_documents.up.sql:46-47,72-73`, `000246_corpscout_esef_document_concept_labels.up.sql:6-7`, `000316_corpscout_esef_disclosures.up.sql:14-15,66-67`, `000244_corpscout_company_source_records.up.sql:92-165`
- Test: `corpscout/services/dagster_v3/tests/test_esef_country_views.py` (new), `tests/test_clickhouse_migrations.py`

**Interfaces:**
- Produces: `tables.SE_ESEF_VIEWS: tuple[SeEsefView, ...]` where `SeEsefView(view: str, table: str, columns: tuple[str, ...], final: bool)`; `country_views.build_se_esef_view_sql(view: SeEsefView) -> str`; `tables.ESEF_ENTITY_MAP_EXPORT_COLUMNS` with `link_status` after `match_source`; the three model-output tuples with `lei` after `source_document_id` and without `country_code` / `company_id`; the four parse tuples without `country_iso2` / `company_id`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_esef_country_views.py
"""The se_esef_* views: rendered from the module's column tuples, pinned to migration 000395."""

import re
from pathlib import Path

from dagster_v3.defs.esef_filings import tables
from dagster_v3.defs.esef_filings.country_views import build_se_esef_view_sql

MIGRATION = (
    Path(__file__).resolve().parents[3]
    / "clickhouse" / "migrations" / "000395_corpscout_esef_country_agnostic_products.up.sql"
)


def _normalized(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip().rstrip(";")


def test_eight_views_one_per_swedish_consumer() -> None:
    assert [v.view for v in tables.SE_ESEF_VIEWS] == [
        "se_esef_filings", "se_esef_facts", "se_esef_disclosures",
        "se_esef_document_contact_candidates", "se_esef_document_company_information",
        "se_esef_document_people", "se_esef_document_business_items",
        "se_esef_document_group_relationships",
    ]
    for view in tables.SE_ESEF_VIEWS:
        assert view.view == f"se_{view.table}"  # se_ + esef_<table>
        assert "country_iso2" not in view.columns and "company_id" not in view.columns
        assert "lei" in view.columns


def test_view_sql_joins_the_verified_swedish_link_and_reads_replacing_tables_final() -> None:
    people = next(v for v in tables.SE_ESEF_VIEWS if v.table == "esef_document_people")
    sql = _normalized(build_se_esef_view_sql(people))
    assert sql.startswith("CREATE OR REPLACE VIEW corpscout.se_esef_document_people AS SELECT m.registry_id AS company_id,")
    assert "FROM corpscout.esef_document_people AS t FINAL" in sql
    assert ("INNER JOIN (SELECT lei, registry_id FROM corpscout.esef_entity_registry_map FINAL "
            "WHERE country_iso2 = 'SE' AND link_status = 'register_verified') AS m ON m.lei = t.lei") in sql
    facts = next(v for v in tables.SE_ESEF_VIEWS if v.table == "esef_disclosures")
    assert "AS t INNER JOIN" in _normalized(build_se_esef_view_sql(facts))  # MergeTree: no FINAL


def test_migration_000395_embeds_every_rendered_view() -> None:
    up = _normalized(MIGRATION.read_text(encoding="utf-8"))
    for view in tables.SE_ESEF_VIEWS:
        assert _normalized(build_se_esef_view_sql(view)) in up, view.view
    assert "ALTER TABLE corpscout.esef_entity_registry_map ADD COLUMN IF NOT EXISTS link_status LowCardinality(String) DEFAULT 'gleif' AFTER match_source" in up


def test_tuples_lost_the_stamps_and_the_model_outputs_gained_lei() -> None:
    for columns in (
        tables.ESEF_DOCUMENT_CONTACT_CANDIDATES_EXPORT_COLUMNS,
        tables.ESEF_DOCUMENT_CONCEPT_LABELS_EXPORT_COLUMNS,
        tables.ESEF_DOCUMENT_COMPANY_INFORMATION_EXPORT_COLUMNS,
        tables.ESEF_DISCLOSURES_EXPORT_COLUMNS,
    ):
        assert "country_iso2" not in columns and "company_id" not in columns
    for columns in (
        tables.ESEF_DOCUMENT_PEOPLE_COLUMNS,
        tables.ESEF_DOCUMENT_BUSINESS_ITEM_COLUMNS,
        tables.ESEF_DOCUMENT_GROUP_RELATIONSHIP_COLUMNS,
    ):
        assert columns[:4] == ("candidate_uid", "source_record_uid", "source_document_id", "lei")
        assert "country_code" not in columns and "company_id" not in columns
    assert tables.ESEF_ENTITY_MAP_EXPORT_COLUMNS == (
        "lei", "country_iso2", "registry_id_raw", "registry_id", "match_source", "link_status", "source_run_id",
    )
```

Also add `"000395_corpscout_esef_country_agnostic_products"` after 000394 in `EXPECTED_MIGRATIONS`.

- [ ] **Step 2: Run them to verify they fail**

Run: `cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/dagster_v3 && uv run pytest tests/test_esef_country_views.py tests/test_clickhouse_migrations.py -q -p no:warnings`
Expected: FAIL (`country_views` missing, tuples still carry stamps, 000395 missing).

- [ ] **Step 3: Edit `tables.py`**

Remove `"country_iso2", "company_id"` from `ESEF_DOCUMENT_CONTACT_CANDIDATES_EXPORT_COLUMNS` (keep its `country_code`: it is the phone's region), `ESEF_DOCUMENT_CONCEPT_LABELS_EXPORT_COLUMNS`, `ESEF_DOCUMENT_COMPANY_INFORMATION_EXPORT_COLUMNS`, `ESEF_DISCLOSURES_EXPORT_COLUMNS`. In `ESEF_DOCUMENT_PEOPLE_COLUMNS`, `ESEF_DOCUMENT_BUSINESS_ITEM_COLUMNS`, `ESEF_DOCUMENT_GROUP_RELATIONSHIP_COLUMNS` replace `"country_code", "company_id"` with nothing and insert `"lei"` after `"source_document_id"`. Insert `"link_status"` after `"match_source"` in `ESEF_ENTITY_MAP_EXPORT_COLUMNS`. Append:

```python
from dataclasses import dataclass


@dataclass(frozen=True)
class SeEsefView:
    """One Swedish view over an ESEF product: the product's columns behind a company_id
    resolved through the register-verified link (spec 2026-09-09, section 1)."""

    table: str
    columns: tuple[str, ...]
    final: bool  # ReplacingMergeTree products are read FINAL inside the view

    @property
    def view(self) -> str:
        return f"se_{self.table}"


# esef_filings and esef_facts are not exported through a tuple here, so their column lists
# are spelled out from the DDL (000149).
_ESEF_FILINGS_COLUMNS = (
    "lei", "entity_name", "fxo_id", "country", "period_end", "date_added", "processed_at",
    "json_url", "package_url", "report_url", "viewer_url", "package_sha256", "error_count",
    "warning_count", "inconsistency_count", "has_json_facts", "source_url", "source_run_id",
    "resolved_at",
)
_ESEF_FACTS_COLUMNS = (
    "lei", "fxo_id", "period_end", "fact_id", "concept_qname", "concept_namespace",
    "concept_local_name", "period_start", "period_instant", "period_duration_end", "unit",
    "currency", "value_kind", "raw_value", "amount_original", "decimals", "dimensions",
    "language", "source_run_id", "processed_week", "resolved_at",
)

SE_ESEF_VIEWS: tuple[SeEsefView, ...] = (
    SeEsefView("esef_filings", _ESEF_FILINGS_COLUMNS, final=True),
    SeEsefView("esef_facts", _ESEF_FACTS_COLUMNS, final=True),
    SeEsefView("esef_disclosures", (*ESEF_DISCLOSURES_EXPORT_COLUMNS, "processed_week", "resolved_at"), final=False),
    SeEsefView("esef_document_contact_candidates", (*ESEF_DOCUMENT_CONTACT_CANDIDATES_EXPORT_COLUMNS, "processed_week", "resolved_at"), final=False),
    SeEsefView("esef_document_company_information", (*ESEF_DOCUMENT_COMPANY_INFORMATION_EXPORT_COLUMNS, "resolved_at"), final=False),
    SeEsefView("esef_document_people", (*ESEF_DOCUMENT_PEOPLE_COLUMNS, "person_profile_hash", "person_role_hash"), final=True),
    SeEsefView("esef_document_business_items", ESEF_DOCUMENT_BUSINESS_ITEM_COLUMNS, final=True),
    SeEsefView("esef_document_group_relationships", ESEF_DOCUMENT_GROUP_RELATIONSHIP_COLUMNS, final=True),
)
```

Check each product's trailing columns against `DESCRIBE` before committing (contact candidates and disclosures carry `processed_week` and `resolved_at`; company information carries `resolved_at`; people carry the two materialised hashes from 000289): `curl -s 'http://companycollect:8123/?database=corpscout' --user "default:$(sed -n 's/^CLICKHOUSE_PASSWORD=//p' corpscout/services/backoffice/.env)" --data-binary "DESCRIBE corpscout.<table> FORMAT TSV" | cut -f1`.

- [ ] **Step 4: Write `country_views.py`**

```python
"""The se_esef_* views: Sweden's slice of each ESEF product (spec 2026-09-09, section 1).

The ESEF products carry no country and no company id. One view per product Sweden reads
joins the product to the register-verified Swedish link of esef_entity_registry_map and
puts the registry id first as company_id. Migration 000395 embeds the rendering;
tests/test_esef_country_views.py pins the two equal so the map's contract cannot drift
from the DDL. A consumer never writes FINAL after a view name: the view already reads a
ReplacingMergeTree product FINAL.
"""

from dagster_v3.defs.esef_filings import tables

VERIFIED_SWEDISH_LINK_SQL = (
    "SELECT lei, registry_id FROM corpscout.esef_entity_registry_map FINAL "
    "WHERE country_iso2 = 'SE' AND link_status = 'register_verified'"
)


def build_se_esef_view_sql(view: tables.SeEsefView) -> str:
    columns = ",\n    ".join(f"t.{column}" for column in view.columns)
    final = " FINAL" if view.final else ""
    return (
        f"CREATE OR REPLACE VIEW corpscout.{view.view} AS\n"
        "SELECT\n"
        "    m.registry_id AS company_id,\n"
        f"    {columns}\n"
        f"FROM corpscout.{view.table} AS t{final}\n"
        f"INNER JOIN ({VERIFIED_SWEDISH_LINK_SQL}) AS m ON m.lei = t.lei"
    )


def render_all_se_esef_views_sql() -> str:
    return ";\n\n".join(build_se_esef_view_sql(view) for view in tables.SE_ESEF_VIEWS) + ";\n"
```

- [ ] **Step 5: Write migration 000395**

Render the views with `uv run python -c "from dagster_v3.defs.esef_filings.country_views import render_all_se_esef_views_sql as r; print(r())"` and paste them verbatim. The file, in this order:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- THE ESEF PRODUCTS STOP CARRYING A COUNTRY OR A COMPANY ID (spec 2026-09-08 revised
-- 2026-09-09, section 1). One link table, register-verified, and per-country views.

-- 1. The link's verification status: register_verified when the jurisdiction has a rule in
--    COUNTRY_IDENTITY_RULES and the id exists in that register, unverified when it does not,
--    gleif when the jurisdiction has no register here. The builder fills it on the next
--    rebuild, existing rows read the default until then.
ALTER TABLE corpscout.esef_entity_registry_map
    ADD COLUMN IF NOT EXISTS link_status LowCardinality(String) DEFAULT 'gleif' AFTER match_source;

-- 2. The three model-output tables carried the stamps as the head of their sorting key.
--    They are pure projections of esef_document_company_information, refilled by their
--    assets on the next run, so each is recreated under the document-keyed shape and the
--    old one parks as _legacy until the owner drops it (operations/esef_stamps_retire.md).
RENAME TABLE corpscout.esef_document_people TO corpscout.esef_document_people_legacy;
CREATE TABLE IF NOT EXISTS corpscout.esef_document_people
(
    candidate_uid FixedString(64),
    source_record_uid FixedString(64),
    source_document_id String,
    lei String,
    fiscal_year UInt16,
    name String,
    role String,
    role_category LowCardinality(String),
    organization String,
    status LowCardinality(String),
    effective_from Nullable(Date32),
    effective_to Nullable(Date32),
    confidence Float32,
    evidence_ids Array(String),
    model_provider LowCardinality(String),
    model_name String,
    prompt_version String,
    source_run_id String,
    extracted_at DateTime64(3, 'UTC'),
    person_profile_hash FixedString(64) MATERIALIZED <copy the expression from 000289 lines 30-35>,
    person_role_hash FixedString(64) MATERIALIZED <copy the expression from 000289 lines 36-46>
)
ENGINE = ReplacingMergeTree(extracted_at)
ORDER BY (lei, fiscal_year, source_record_uid, candidate_uid);
```

then the same rename-and-create for `esef_document_business_items` (columns as in 000244 lines 118-137 with `lei String` in place of the two stamps, `ORDER BY (lei, item_kind, fiscal_year, source_record_uid, candidate_uid)`) and `esef_document_group_relationships` (`ORDER BY (lei, relationship_type, fiscal_year, source_record_uid, candidate_uid)`), then the eight rendered views, then:

```sql
-- 4. The Swedish person source view reads its country slice instead of filtering itself
--    (it was CREATE OR REPLACEd by 000331, re-issued here with the new FROM).
CREATE OR REPLACE VIEW corpscout.se_company_person_esef AS
SELECT
    company_id,
    source_record_uid,
    person_profile_hash,
    person_role_hash,
    name AS full_name,
    role,
    role_category,
    organization,
    status,
    effective_from,
    effective_to,
    confidence,
    fiscal_year,
    extracted_at AS source_observed_at,
    candidate_uid
FROM corpscout.se_esef_document_people;
```

Down: drop the eight views, re-issue 000331's `se_company_person_esef` text, drop the three new tables and rename the `_legacy` ones back, drop `link_status`.

Copy the two MATERIALIZED expressions exactly from 000289 (`sed -n 29,47p corpscout/clickhouse/migrations/000289_corpscout_company_person_semantic_hashes.up.sql`); they hash `name`, `role`, `role_category`, `organization`, `status` and the effective dates and do not reference the dropped columns.

- [ ] **Step 6: Edit the older ledger files per the policy**

In 000243 delete lines 46-47 (`country_iso2`, `company_id` of `esef_document_contact_candidates`) and 72-73 (of `esef_document_company_information`); in 000246 delete lines 6-7; in 000316 delete lines 14-15 of the CREATE and lines 66-67 of the INSERT ... SELECT column list (and the matching two SELECT expressions further down: grep `country_iso2` in the file and remove every occurrence). In 000244 rewrite the three CREATE blocks (lines 92-165) to the exact new shape written in 000395 minus the MATERIALIZED columns (000289 still adds those). Run `uv run pytest tests/test_clickhouse_migrations.py -q -p no:warnings` and re-pin whatever asserts the old shape (the file has assertions on 000244 around lines 3130-3200 and 3522).

- [ ] **Step 7: Run the tests**

Run: `uv run pytest tests/test_esef_country_views.py tests/test_clickhouse_migrations.py -q -p no:warnings`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add corpscout/clickhouse/migrations/000395_corpscout_esef_country_agnostic_products.up.sql corpscout/clickhouse/migrations/000395_corpscout_esef_country_agnostic_products.down.sql corpscout/clickhouse/migrations/000243_corpscout_esef_source_documents.up.sql corpscout/clickhouse/migrations/000244_corpscout_company_source_records.up.sql corpscout/clickhouse/migrations/000246_corpscout_esef_document_concept_labels.up.sql corpscout/clickhouse/migrations/000316_corpscout_esef_disclosures.up.sql corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/tables.py corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/country_views.py corpscout/services/dagster_v3/tests/test_esef_country_views.py corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py
git commit -m "feat(clickhouse): 000395 verifies the ESEF link and creates the se_esef_* views"
```

---

## Task 2: The map builder verifies against the registers

**Files:**
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/publish.py:249-299`
- Test: `corpscout/services/dagster_v3/tests/test_esef_filings_publish.py:669-720`

**Interfaces:**
- Consumes: `COUNTRY_IDENTITY_RULES` from `dagster_v3.defs.company_identifier.rules` (`register_table`, `id_column`, `identifier_length` per country code).
- Produces: `build_esef_entity_registry_map_select(source_run_id)` returning a SELECT whose columns are `ESEF_ENTITY_MAP_EXPORT_COLUMNS` in order, `link_status` included; `LINK_STATUS_REGISTER_VERIFIED = "register_verified"`, `LINK_STATUS_UNVERIFIED = "unverified"`, `LINK_STATUS_GLEIF = "gleif"`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_esef_filings_publish.py`:

```python
def test_build_esef_entity_registry_map_select_verifies_rule_countries_against_their_registers() -> None:
    sql = build_esef_entity_registry_map_select("run-1")
    # One LEFT JOIN per COUNTRY_IDENTITY_RULES country, both sides digits-only.
    assert "LEFT JOIN (SELECT DISTINCT replaceRegexpAll(company_id, '[^0-9]', '') AS id FROM corpscout.se_company_basic_info) AS reg_se" in sql
    assert "LEFT JOIN (SELECT DISTINCT replaceRegexpAll(org_number, '[^0-9]', '') AS id FROM corpscout.no_companies) AS reg_no" in sql
    assert "ON reg_se.id = digits AND primary_country_iso2 = 'SE'" in sql
    assert ("multiIf(primary_country_iso2 NOT IN ('SE', 'NO', 'FI', 'FR'), 'gleif', "
            "reg_se.id != '' OR reg_no.id != '' OR reg_fi.id != '' OR reg_fr.id != '', "
            "'register_verified', 'unverified') AS link_status") in sql
    assert sql.index("AS match_source") < sql.index("AS link_status") < sql.index("AS source_run_id")
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/test_esef_filings_publish.py -q -p no:warnings -k registry_map`
Expected: FAIL (`link_status` absent).

- [ ] **Step 3: Implement**

In `publish.py` add near `MATCH_SOURCE_GLEIF_REGISTERED_AS`:

```python
from dagster_v3.defs.company_identifier.rules import COUNTRY_IDENTITY_RULES

LINK_STATUS_REGISTER_VERIFIED = "register_verified"
LINK_STATUS_UNVERIFIED = "unverified"
LINK_STATUS_GLEIF = "gleif"


def _register_verification_sql() -> tuple[str, str]:
    """(joins, link_status expression): one LEFT JOIN per rule country on the digits-only
    id, the way company_identifier verifies, and the three-valued status."""
    joins = []
    hits = []
    for code, rule in sorted(COUNTRY_IDENTITY_RULES.items()):
        alias = f"reg_{code.lower()}"
        joins.append(
            f"LEFT JOIN (SELECT DISTINCT replaceRegexpAll({rule.id_column}, '[^0-9]', '') AS id "
            f"FROM corpscout.{rule.register_table}) AS {alias} "
            f"ON {alias}.id = digits AND primary_country_iso2 = '{code}'"
        )
        hits.append(f"{alias}.id != ''")
    countries = ", ".join(f"'{code}'" for code in sorted(COUNTRY_IDENTITY_RULES))
    status = (
        f"multiIf(primary_country_iso2 NOT IN ({countries}), '{LINK_STATUS_GLEIF}', "
        f"{' OR '.join(hits)}, '{LINK_STATUS_REGISTER_VERIFIED}', '{LINK_STATUS_UNVERIFIED}') AS link_status"
    )
    return "\n        ".join(joins), status
```

Then in `build_esef_entity_registry_map_select` wrap the existing SELECT as a subquery that also computes `replaceRegexpAll(registered_as, '[^0-9]', '') AS digits` and `primary_country_iso2`, and select from it with the joins:

```python
    joins, status = _register_verification_sql()
    return f"""
        SELECT
            lei,
            country_iso2,
            registry_id_raw,
            registry_id,
            '{MATCH_SOURCE_GLEIF_REGISTERED_AS}' AS match_source,
            {status},
            '{run_id_literal}' AS source_run_id
        FROM (
            SELECT
                lei,
                coalesce(primary_country_iso2, '') AS country_iso2,
                primary_country_iso2,
                coalesce(registered_as, '') AS registry_id_raw,
                <the existing multiIf(...) AS registry_id, unchanged>,
                replaceRegexpAll(registered_as, '[^0-9]', '') AS digits
            FROM {GLEIF_LEI_RECORDS_QUALIFIED_TABLE}
            WHERE registered_as != ''
              AND lei IN (SELECT DISTINCT lei FROM {tables.QUALIFIED_ESEF_FILINGS_TABLE})
            ORDER BY lei, resolved_at DESC
            LIMIT 1 BY lei
        ) AS mapped
        {joins}
    """
```

Note for the FR rule: `fr_companies.siren` is already nine digits; the digits-only normalisation is a no-op there. The join alias `digits` is unqualified on purpose: it belongs to `mapped`, the only non-joined source.

- [ ] **Step 4: Run the publish tests**

Run: `uv run pytest tests/test_esef_filings_publish.py -q -p no:warnings`
Expected: PASS (the existing scope-and-normalizers test still finds the FI/SE branches inside the subquery; if it asserts the exact old text, re-pin it to the subquery form).

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/publish.py corpscout/services/dagster_v3/tests/test_esef_filings_publish.py
git commit -m "feat(esef): the entity map verifies rule countries against their registers (link_status)"
```

---

## Task 3: The parse products stop carrying a country and a company id

**Files:**
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/segment_assets.py:226-275, 372-392, 418-445, 596-612, 862-876, 965-980, 1066-1080, 1244-1276`
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/segment_parser.py:196-203`
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/disclosure_parser.py:115-130, 192-205`
- Test: `corpscout/services/dagster_v3/tests/test_esef_fact_disclosures.py:80-90, 190-200`, `tests/test_esef_llm_enrichment.py:40-50`, and whichever `tests/test_esef_*` construct `EsefArtifactSource(... country=..., company_id=...)` (grep `company_id=` under `tests/test_esef_`).

**Interfaces:**
- Produces: `EsefArtifactSource(fxo_id, source_url, object_key, source_run_id, expected_package_sha256)`; `run_esef_document_manifest_partition(...)` without `company_links`; row dictionaries from `_document_row`, `_unavailable_document_row`, the contact-candidate `common` dict, the concept-label `common` dict, `tagged_fact_disclosure_row` and `visible_section_disclosure_row` without `country_iso2` / `company_id`.

- [ ] **Step 1: Re-pin the tests**

In `test_esef_fact_disclosures.py` remove `"country_iso2": "SE"` and `"company_id": "5566000000"` from the source dictionaries at lines 85-86 and 193-194 and from any expected row; add:

```python
def test_disclosure_rows_carry_no_country_and_no_company_id() -> None:
    row = tagged_fact_disclosure_row(source=SOURCE, fact=FACT)  # the fixtures the file already builds
    assert "country_iso2" not in row and "company_id" not in row
    assert row["lei"] == SOURCE["lei"]
```

In `test_esef_llm_enrichment.py` line 46 delete `assert evidence_input.source.company_id == "5566692850"` and assert instead `assert evidence_input.source.fxo_id` is the fixture's document id. In every test constructing `EsefArtifactSource`, drop the `country=` and `company_id=` keyword arguments. Add to `tests/test_esef_segment_assets.py` (create it if absent):

```python
from dagster_v3.defs.esef_filings import segment_assets


def test_manifest_and_parse_rows_never_stamp_a_company() -> None:
    assert not hasattr(segment_assets, "load_esef_company_links")
    assert not hasattr(segment_assets, "_company_id_for_index_row")
    index_row = {
        "source_document_id": "LEI-2024-12-31-ESEF-SE-0", "package_sha256": "AB" * 32, "lei": "LEI",
        "entity_name": "X AB", "country_iso2": "SE", "period_end": "2024-12-31", "package_url": "u",
        "report_url": "", "viewer_url": "",
    }
    common = segment_assets._contact_candidate_common(index_row, fiscal_year=2024, source_run_id="r", extracted_at="2026-01-01T00:00:00Z")
    assert "country_iso2" not in common and "company_id" not in common
    assert common["lei"] == "LEI"
```

(`_contact_candidate_common` is the helper Step 3 extracts from the inline `common = {...}` at lines 1066-1080; the contact-candidate builder at 965-980 gets the same treatment.)

- [ ] **Step 2: Run to verify they fail**

Run: `uv run pytest tests/test_esef_fact_disclosures.py tests/test_esef_llm_enrichment.py tests/test_esef_segment_assets.py -q -p no:warnings`
Expected: FAIL.

- [ ] **Step 3: Implement**

- `segment_parser.py`: delete the `country` and `company_id` fields of `EsefArtifactSource`; grep the module for `source.country` / `source.company_id` and delete those artifact keys (`"country"`, `"company_id"` under the artifact's `source` object). Keep `artifact_schema_version` unchanged: readers tolerate the absent keys (Task 4 removes the last reader).
- `segment_assets.py`: delete `load_esef_company_links` and `_company_id_for_index_row`; `run_esef_document_manifest_partition` loses its `company_links` parameter and builds `documents = [dict(index_row) for index_row in index_rows]`; the asset at 1244-1276 stops loading links and no longer needs `clickhouse` (drop the resource parameter if nothing else in the function uses it). Remove the `company_id=` arguments and the `"country_iso2"` / `"company_id"` keys from `_unavailable_document_row`, `_document_row`, the representative `EsefArtifactSource(...)` construction, and both `common` dictionaries; extract the contact-candidate common dictionary into `_contact_candidate_common(index_row, *, fiscal_year, source_run_id, extracted_at)` so the test above can call it.
- `disclosure_parser.py`: delete the two `"country_iso2"` and `"company_id"` entries.

- [ ] **Step 4: Run the ESEF unit tests**

Run: `uv run pytest tests/test_esef_fact_disclosures.py tests/test_esef_llm_enrichment.py tests/test_esef_segment_assets.py tests/test_esef_filings_publish.py -q -p no:warnings`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/segment_assets.py corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/segment_parser.py corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/disclosure_parser.py corpscout/services/dagster_v3/tests/test_esef_fact_disclosures.py corpscout/services/dagster_v3/tests/test_esef_llm_enrichment.py corpscout/services/dagster_v3/tests/test_esef_segment_assets.py
git commit -m "feat(esef): parse products are keyed by LEI and document only"
```

---

## Task 4: The model stage selects per LEI through the map

**Files:**
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/llm_enrichment_assets.py:60-100, 151-215, 465-500, 585-700, 790-805, 1028-1050, 1150-1172`
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/company_information_projections.py:21-135`
- Test: `corpscout/services/dagster_v3/tests/test_esef_llm_enrichment.py:496-560`, `tests/test_esef_company_information_projections.py`

**Interfaces:**
- Consumes: `LINK_STATUS_REGISTER_VERIFIED` from `publish.py` (Task 2).
- Produces: `EsefLlmEnrichmentConfig.link_statuses: list[str] = ["register_verified"]`; `_load_latest_source_documents(clickhouse, *, model, provider, prompt_version, link_statuses, country_iso2s, company_ids, source_document_ids, max_documents)` returning documents with keys `source_document_id, package_sha256, lei, period_end, fiscal_year, package_url, artifact_schema_version`; `_information_identity(document)` without stamps; projections that write `lei`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_esef_llm_enrichment.py (append)
def test_selection_is_per_lei_and_admits_leis_through_the_map() -> None:
    from dagster_v3.defs.esef_filings.llm_enrichment_assets import _selection_query
    sql, params = _selection_query(
        provider="deepseek", model="deepseek-v4-flash", prompt_version=PROMPT_VERSION,
        link_statuses={"register_verified"}, country_iso2s={"SE"}, company_ids={"5020077862"},
        source_document_ids=set(),
    )
    assert "PARTITION BY lei" in sql
    assert "disclosures.company_id" not in sql and "disclosures.country_iso2" not in sql
    assert ("disclosures.lei IN (SELECT lei FROM corpscout.esef_entity_registry_map FINAL "
            "WHERE link_status IN %(link_statuses)s AND country_iso2 IN %(country_iso2s)s "
            "AND registry_id IN %(company_ids)s)") in sql
    assert params["link_statuses"] == ("register_verified",)


def test_config_admits_register_verified_leis_by_default() -> None:
    config = EsefLlmEnrichmentConfig(provider="deepseek", model="deepseek-v4-flash")
    assert config.link_statuses == ["register_verified"]
```

Re-pin the runtime-profile test at lines 496-540: its expected `run_esef_llm_enrichment` kwargs gain `"link_statuses": ["register_verified"]`. In `test_esef_company_information_projections.py` add:

```python
def test_projections_write_lei_and_no_stamps() -> None:
    for sql in (esef_document_people_sql(), esef_document_business_items_sql(), esef_document_group_relationships_sql()):
        assert "info.lei," in sql
        assert "info.country_iso2" not in sql and "info.company_id" not in sql
```

- [ ] **Step 2: Run to verify they fail**

Run: `uv run pytest tests/test_esef_llm_enrichment.py tests/test_esef_company_information_projections.py -q -p no:warnings`
Expected: FAIL.

- [ ] **Step 3: Implement**

- Config: add `link_statuses: list[str] = Field(default_factory=lambda: [LINK_STATUS_REGISTER_VERIFIED])` after `country_iso2s`; pass it through the asset (line ~1160) and `run_esef_llm_enrichment` (new keyword `link_statuses: Sequence[str]`), validating each value is one of the three constants. The `company_ids` rule "exactly one country_iso2" stays (registry ids are country-scoped).
- Split `_load_latest_source_documents` into `_selection_query(...) -> tuple[str, dict]` (pure) and the executing wrapper. The query: `columns = ("source_document_id", "package_sha256", "lei", "period_end", "fiscal_year", "package_url", "artifact_schema_version")`; `document_filters` drop the two stamp filters and gain one membership filter built from the map:

```python
    link_filters = ["link_status IN %(link_statuses)s"]
    parameters["link_statuses"] = tuple(sorted(link_statuses))
    if country_iso2s:
        link_filters.append("country_iso2 IN %(country_iso2s)s")
        parameters["country_iso2s"] = tuple(sorted(country_iso2s))
    if company_ids:
        link_filters.append("registry_id IN %(company_ids)s")
        parameters["company_ids"] = tuple(sorted(company_ids))
    document_filters.append(
        "disclosures.lei IN (SELECT lei FROM corpscout.esef_entity_registry_map FINAL "
        f"WHERE {' AND '.join(link_filters)})"
    )
```

  The window becomes `PARTITION BY lei`, the aggregate loses the two `argMax` stamp columns, the final `ORDER BY documents.lei, documents.source_document_id`, and the rank alias `latest_lei_report_rank`.
- Metadata: `"selection_method": "latest_xbrl_per_lei"`, `"selected_lei_count": len({str(d["lei"]) for d in documents})` in place of the company count; drop `selection_country_iso2s` only if nothing reads it (grep the backoffice for it first: `rg selection_country_iso2s corpscout/services/backoffice/app`).
- `_information_identity` and the disclosure-input `source` (lines 790-805) lose `country` / `company_id`.
- Projections: replace `info.country_iso2, info.company_id,` with `info.lei,` in the three SELECTs (the tuples already moved in Task 1).

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/test_esef_llm_enrichment.py tests/test_esef_company_information_projections.py -q -p no:warnings`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/llm_enrichment_assets.py corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/company_information_projections.py corpscout/services/dagster_v3/tests/test_esef_llm_enrichment.py corpscout/services/dagster_v3/tests/test_esef_company_information_projections.py
git commit -m "feat(esef): the model stage selects per LEI admitted through the register-verified map"
```

---

## Task 5: Dagster and dbt consumers read the views

**Files:**
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/company_people/source_views.py:80-100`
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/basic_info/esef.py:15-66`
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/company_serving/dbt/models/sources.yml:25-45`, `company_section_item_source_links_build.sql:181-205, 306-335, 386-400`, `company_domains_build.sql:92-112`, `company_description_current_build.sql:38-58`, `company_management_current_build.sql:126-150`, `company_contact_current_build.sql`
- Test: `tests/test_se_company_person_views.py`, `tests/test_se_company_basic_info_extractors_sql.py:78-97`, `tests/test_company_serving_dbt.py:83-160`

**Interfaces:**
- Consumes: the eight views (Task 1); `se_company_person_esef` now reads `se_esef_document_people` (000395).
- Produces: dbt sources `se_esef_filings`, `se_esef_document_contact_candidates`, `se_esef_document_company_information`, `se_esef_document_people` (each with the producing asset's key in `meta.dagster.asset_key`, so lineage still points at the product's asset).

- [ ] **Step 1: Re-pin the tests**

`test_se_company_person_views.py`: the drift pin's migration constant moves from 000331 to 000395 for the esef view (the other two views keep 000331); the rendered esef view must contain `FROM corpscout.se_esef_document_people` and no `FINAL`, no `WHERE country_code`. `test_se_company_basic_info_extractors_sql.py::test_esef_select_takes_the_newest_filing_per_company`: `FROM corpscout.se_esef_document_company_information` in both SQLs, no `country_iso2`. `test_company_serving_dbt.py`: line 153 expects `source('corpscout', 'se_esef_document_contact_candidates')` in `company_domains_build`; add

```python
    for model in ("company_contact_current_build", "company_description_current_build", "company_management_current_build", "company_section_item_source_links_build", "company_domains_build"):
        text = (models / f"{model}.sql").read_text()
        assert "esef_document_" not in text.replace("se_esef_document_", "")  # only the views remain
        assert "esef_entity_registry_map" not in text
        assert "se_esef_document_people FINAL" not in text
```

The partition-mapping test at lines 83-118 keeps `esef_document_contact_candidates_clickhouse` as the asset key behind the new source name.

- [ ] **Step 2: Run to verify they fail**

Run: `uv run pytest tests/test_se_company_person_views.py tests/test_se_company_basic_info_extractors_sql.py tests/test_company_serving_dbt.py -q -p no:warnings`
Expected: FAIL.

- [ ] **Step 3: Implement**

- `source_views.py`: the esef builder renders `FROM corpscout.se_esef_document_people` with no `FINAL` and no country filter (the columns are unchanged).
- `basic_info/esef.py`: `_FILTER` drops `country_iso2 = 'SE'`; both SQLs read `corpscout.se_esef_document_company_information`; the asset's `deps` stay on `esef_document_company_information_clickhouse` (the view has no asset) and gain `dg.AssetKey("esef_entity_registry_map_clickhouse")`.
- `sources.yml`: rename the four ESEF sources to their `se_esef_` names keeping each `meta.dagster.asset_key`; drop the `esef_entity_registry_map` source (no model reads it any more).
- `company_section_item_source_links_build.sql`: `esef_sources` reads `se_esef_filings AS filings` joined to `se_company_basic_info AS companies FINAL ON companies.company_id = filings.company_id` (the map join and its country filter go; `match_method` becomes the literal `'gleif_registered_as'`, `country_code` the literal `'{{ var("country_code") }}'`); `esef_domains` joins `se_esef_document_contact_candidates AS candidates ON candidates.company_id = current.company_id AND candidates.registrable_domain = current.root_domain AND candidates.candidate_kind = 'website'` (no `country_iso2` predicate); `contacts` reads the same view; the management leg joins `se_esef_document_people AS esef ON esef.candidate_uid = current.management_id` without `FINAL`.
- `company_domains_build.sql`: `FROM se_esef_document_contact_candidates AS candidates`, drop `candidates.country_iso2 = ...` and `candidates.company_id != ''`.
- `company_description_current_build.sql`: `FROM se_esef_document_company_information AS info`, drop `info.country_iso2 = ...`.
- `company_management_current_build.sql`: `FROM se_esef_document_people`, no `FINAL`, no `WHERE country_code`.
- `company_contact_current_build.sql`: `FROM se_esef_document_contact_candidates`, drop `WHERE country_iso2 = ...`.

- [ ] **Step 4: Parse and test**

Run: `uv run pytest tests/test_se_company_person_views.py tests/test_se_company_basic_info_extractors_sql.py tests/test_company_serving_dbt.py -q -p no:warnings` then `uv run --frozen --no-sync dbt parse --project-dir src/dagster_v3/defs/company_serving/dbt --profiles-dir src/dagster_v3/defs/company_serving/dbt` and `uv run dg check defs`.
Expected: PASS, parse clean, definitions load.

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/company_people/source_views.py corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/basic_info/esef.py corpscout/services/dagster_v3/src/dagster_v3/defs/company_serving/dbt/models/sources.yml corpscout/services/dagster_v3/src/dagster_v3/defs/company_serving/dbt/models/company_section_item_source_links_build.sql corpscout/services/dagster_v3/src/dagster_v3/defs/company_serving/dbt/models/company_domains_build.sql corpscout/services/dagster_v3/src/dagster_v3/defs/company_serving/dbt/models/company_description_current_build.sql corpscout/services/dagster_v3/src/dagster_v3/defs/company_serving/dbt/models/company_management_current_build.sql corpscout/services/dagster_v3/src/dagster_v3/defs/company_serving/dbt/models/company_contact_current_build.sql corpscout/services/dagster_v3/tests/test_se_company_person_views.py corpscout/services/dagster_v3/tests/test_se_company_basic_info_extractors_sql.py corpscout/services/dagster_v3/tests/test_company_serving_dbt.py
git commit -m "refactor(serving): Swedish ESEF consumers read the se_esef_* views"
```

---

## Task 6: The backoffice reads the views

**Files:**
- Modify: `corpscout/services/backoffice/app/lib/se-company-esef.server.ts:50-90`, `app/lib/queries.server.ts:2530-2600, 3400-3470`
- Test: `corpscout/services/backoffice/tests/se-company-esef.server.test.ts:25-45`, `tests/source-information-sections.test.tsx:155-170`

- [ ] **Step 1: Re-pin the tests**

`se-company-esef.server.test.ts` lines 29-30 become `expect(ESEF_TAB_CONTACTS_SQL).toContain("FROM corpscout.se_esef_document_contact_candidates")` and `expect(ESEF_TAB_PEOPLE_SQL).toContain("FROM corpscout.se_esef_document_people")`, plus `expect(ESEF_TAB_PEOPLE_SQL).not.toContain("FINAL")` and `not.toContain("country_code")`. `source-information-sections.test.tsx` lines 160-167: the four queries name `se_esef_document_people`, `se_esef_document_business_items`, `se_esef_document_group_relationships`, `se_esef_document_contact_candidates`, none with `FINAL`, none with `country_code = {country:String}`.

- [ ] **Step 2: Run to verify they fail**

Run: `cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/backoffice && npx vitest run tests/se-company-esef.server.test.ts tests/source-information-sections.test.tsx`
Expected: FAIL.

- [ ] **Step 3: Implement**

In `se-company-esef.server.ts` the five product queries read `corpscout.se_esef_<table>` and keep only `WHERE company_id = {companyId:String}`; the filings query keeps its `company_identifier` join (it is register-verified already and the ESEF tab is Swedish). In `queries.server.ts` the four public-page ESEF section queries read the `se_esef_*` views with `WHERE company_id = {id:String}` and the callers pass `country` no more; the sections render only when `country === "se"` (a non-Swedish company has no Swedish view), which the section loader already implies since only Sweden serves ESEF sections.

- [ ] **Step 4: Typecheck and test**

Run: `npm run typecheck && npx vitest run tests/se-company-esef.server.test.ts tests/source-information-sections.test.tsx tests/se-company-tabs.server.test.ts`
Expected: typecheck clean, tests PASS.

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/backoffice/app/lib/se-company-esef.server.ts corpscout/services/backoffice/app/lib/queries.server.ts corpscout/services/backoffice/tests/se-company-esef.server.test.ts corpscout/services/backoffice/tests/source-information-sections.test.tsx
git commit -m "refactor(backoffice): Swedish ESEF queries read the se_esef_* views"
```

---

## Task 7: Docs and the owner-run drop script

**Files:**
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/docs/esef_filings-design.md`, `corpscout/services/dagster_v3/docs/esef-ixbrl-segment-parser.md`
- Create: `corpscout/clickhouse/operations/esef_stamps_retire.md`

- [ ] **Step 1: Write the docs**

`esef_filings-design.md` gains a section "Country-agnostic products (2026-09-09)": keys, the map's `link_status`, the `se_esef_*` views and the rule that a consumer never writes `FINAL` after a view. `esef-ixbrl-segment-parser.md`: the artifact's `source` object no longer carries `country` or `company_id`; the model stage selects per LEI.

`esef_stamps_retire.md`:

```markdown
# Retire the ESEF stamp columns (spec 2026-09-09, slice 1)

Owner-run on the companycollect ClickHouse AFTER 000395 is applied, dagster_v3 is deployed,
the map is rebuilt, the three projections have refilled their new tables, and the
company_serving publish is green.

## Gates
```sql
SELECT count() FROM corpscout.esef_document_people;                 -- > 0 (refilled)
SELECT count() FROM corpscout.esef_document_business_items;         -- > 0
SELECT count() FROM corpscout.esef_document_group_relationships;    -- > 0
SELECT name FROM system.tables WHERE database = 'corpscout' AND engine IN ('View','MaterializedView')
  AND match(create_table_query, '(country_iso2|country_code|company_id)') AND name LIKE 'esef%';  -- none
```

## Drops
```sql
ALTER TABLE corpscout.esef_disclosures DROP COLUMN IF EXISTS country_iso2, DROP COLUMN IF EXISTS company_id;
ALTER TABLE corpscout.esef_document_concept_labels DROP COLUMN IF EXISTS country_iso2, DROP COLUMN IF EXISTS company_id;
ALTER TABLE corpscout.esef_document_contact_candidates DROP COLUMN IF EXISTS country_iso2, DROP COLUMN IF EXISTS company_id;
ALTER TABLE corpscout.esef_document_company_information DROP COLUMN IF EXISTS country_iso2, DROP COLUMN IF EXISTS company_id;
DROP TABLE IF EXISTS corpscout.esef_document_people_legacy;
DROP TABLE IF EXISTS corpscout.esef_document_business_items_legacy;
DROP TABLE IF EXISTS corpscout.esef_document_group_relationships_legacy;
```
```

- [ ] **Step 2: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/docs/esef_filings-design.md corpscout/services/dagster_v3/docs/esef-ixbrl-segment-parser.md corpscout/clickhouse/operations/esef_stamps_retire.md
git commit -m "docs(esef): country-agnostic products, the se_esef_* views and the stamp retirement script"
```

---

## Task 8: Verify and merge

- [ ] **Step 1:** `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run pytest -q -m "not integration" --deselect tests/test_schedule_cron_contracts.py::test_every_schedule_fires_on_a_unique_minute_hour_pair -p no:cacheprovider -p no:warnings --color=no -rf 2>&1 | rg "^FAILED|passed"`. Expected: only the four failures already on main (`test_backfill_policy_contracts`, `test_duckdb_bulk_loading_contract`, `test_nace_categories`, `test_sweden_address_geocoding::test_lantmateriet_credentials_are_documented_without_values`).
- [ ] **Step 2:** `uv run pytest tests/test_company_domain_suggestions_dbt.py tests/test_se_company_basic_info_extractors_clickhouse_local.py -q` (docker). Expected: PASS.
- [ ] **Step 3:** backoffice `npm run typecheck && npx vitest run 2>&1 | rg "Tests |×"`. Expected: only the known live-DB timeouts, the ESEF tab "renders every section" test and the address-casing test from the address workstream. The Swedish ESEF live tests will fail with "unknown table se_esef_*" until 000395 is applied: that is expected before Task 9 step 1, so run this step again after it.
- [ ] **Step 4:** Merge AFTER Task 9 step 1 (the dev server runs main and the ESEF tab would query views that do not exist yet): `git checkout main && git merge --no-ff esef-1-country-agnostic-products -m "Merge branch 'esef-1-country-agnostic-products'"` (footer).

---

## Task 9: Rollout (owner-run steps marked)

- [ ] **Step 1 (owner):** `cd corpscout && make clickhouse-migrate-up-one` (000395: the map column, the three renames and creates, the eight views, the person view). Verify: `SELECT name FROM system.tables WHERE database = 'corpscout' AND name LIKE 'se_esef_%'` lists eight rows and the ledger reads 395.
- [ ] **Step 2:** merge (Task 8 step 4); backoffice smoke of the ESEF tab (`/admin/se/company/5020077862/esef`) and the public page.
- [ ] **Step 3 (owner):** dbt-state refresh (the company_serving project changed) + light_sync deploy; verify on the host that `.local_defs_state/DbtProjectComponent__dbt____company-serving/project/models/company_domains_build.sql` names `se_esef_document_contact_candidates`.
- [ ] **Step 4:** materialise `esef_entity_registry_map_clickhouse`; verify `SELECT link_status, count() FROM corpscout.esef_entity_registry_map FINAL WHERE country_iso2 = 'SE' GROUP BY 1` gives 403 register_verified and 1 unverified (Rizzo).
- [ ] **Step 5:** materialise `esef_document_people_clickhouse`, `esef_document_business_items_clickhouse`, `esef_document_group_relationships_clickhouse` (refill the new tables); verify `SELECT count() FROM corpscout.se_esef_document_people` is close to the 9,634 Swedish rows minus Rizzo's 24.
- [ ] **Step 6:** run the company_serving dbt build plus `company_serving_current` for SE (the same 15-asset selection as run 3144bbe9). Expected: green, no anchor failure.
- [ ] **Step 7 (owner):** run `corpscout/clickhouse/operations/esef_stamps_retire.md`.
