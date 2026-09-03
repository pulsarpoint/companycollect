# SE Basic Info, Slice 0: Register Source Tables Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the two Swedish register sources their own ClickHouse tables — `corpscout.se_scb_companies` and `corpscout.se_bolagsverket_companies`, each holding the whole source record per company in that source's own organisation — published by two new assets in the existing `sweden_company` register load, move the domain-suggestions dbt model onto their union, and retire `se_company_registry_observations` / `se_company_registry_current`.

**Architecture:** Two new normalised DuckDB tables (`sweden_company.scb_companies`, `sweden_company.bolagsverket_companies`) are built straight from the raw DuckDB tables by `normalized_duckdb.py`, with the same identity normalisation and one-row-per-company ranking `company_registry_states` already uses. Two new ClickHouse export assets publish them through a stage table, inserting only the companies whose `source_payload_hash` differs from their currently published row. `sweden_company_profile_history_clickhouse` keeps publishing proceedings and stops publishing the registry pair. The retired pair is dropped by a gated migration written at its apply step, never before.

**Tech Stack:** Python 3.14 / uv, Dagster 1.13.9, DuckDB, ClickHouse 26.5 (golang-migrate ledger; `clickhouse-local` via `docker run clickhouse/clickhouse-server:26.5` for the integration harness), dbt-clickhouse, pytest.

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-03-se-company-basic-info-design.md` — this plan implements section 3.1 (source layer), the slice-0 bullet of section 6, slice 0 of section 10, and the source-table names of section 11. Binding authority for every name below.

## Global Constraints

- Python 3.14 project managed by uv. Every command runs from `corpscout/services/dagster_v3` unless the step says otherwise. Tests: `uv run --frozen --no-sync pytest <file> -q -p no:warnings`.
- Definitions-loading tests and `dg check defs` need `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix` in the environment.
- No `from __future__ import annotations` in a module that defines a `@dg.asset` — it stringizes the `context: AssetExecutionContext` hint and breaks Dagster's op context-type validation. `sweden_company/assets.py` has none today; keep it that way.
- Every asset that opens `data/sweden_company_source.duckdb` — writers and read-only exporters alike — carries `pool="sweden_company_duckdb"` (`assets.py:35`, `SWEDEN_COMPANY_DUCKDB_POOL`). The two new exports must too.
- The migration owns the ClickHouse schema. Code asserts the table exists (`assert_clickhouse_tables_exist`) and never duplicates DDL in Python. Column order is pinned by a contract test that reads the migration file.
- Migrations (`corpscout/clickhouse/migrations/`): first line `CREATE DATABASE IF NOT EXISTS corpscout;`; the file's last non-blank line ends with `;` (a statement, never a comment); no `;` inside any `--` comment; no `TRUNCATE TABLE` in an up file; **no `DROP TABLE` in an up file that has to wait for a deploy** (the 2026-08-25 ruling, enforced by `test_se_company_info_field_value_replaces_the_correction_ledger` and followed by 000371 → 000372); every up file has a matching down file; every name added to `EXPECTED_MIGRATIONS` in `tests/test_clickhouse_migrations.py`.
- Migration numbers are **000373** (`se_scb_companies`), **000374** (`se_bolagsverket_companies`) and **000375** (the gated retirement). Before Task 1, `ls corpscout/clickhouse/migrations | tail -2` must show `000372_corpscout_retire_se_company_info_correction.{down,up}.sql`. If the ledger moved, renumber all three consistently everywhere in this plan.
- `ORDER BY` cannot contain `Nullable` columns (`allow_nullable_key` is off).
- A non-nullable ClickHouse `String` / `LowCardinality(String)` column must receive `''`, never `NULL` — the driver calls `.encode()` per value and dies on `None`. Coalesce non-nullable string columns in the producing DuckDB SQL.
- Commit by explicit path; the working tree may carry unrelated WIP — never `git add -A`.
- `source_payload_hash` is exported on purpose, against the usual "don't export the payload hash" rule in `CLAUDE.md`: these two tables are hash-based incremental (the hash is the change key of the anti-join in Task 3), which is exactly the exception that rule names.
- Task 7 touches production. Every step there needs the owner's go-ahead; commands the auto-mode classifier blocks (`make clickhouse-migrate-*`, `ssh companycollect …`, `ssh dagster …`) are posted for the owner to run with the `!` prefix and the output pasted back. Conventional Commits with these two trailers, each on its own line, after a blank line:
  `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`
  `Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5`

## Design decisions fixed here (read before Task 1)

These were decided from the code, not invented. Later tasks depend on them verbatim.

1. **`company_registry_states` filtered by source is NOT enough.** It is missing three things each new table needs, so the two DuckDB tables are built from `scb_raw` / `bolagsverket_raw` directly:
   - SCB's five SNI columns `Ng1`..`Ng5` and its four address columns `COAdress` / `Gatuadress` / `PostNr` / `PostOrt` live in `company_industry_states` and `company_addresses`, not in `company_registry_states` (`normalized_duckdb.py:143-190` selects neither).
   - Bolagsverket's `postadress` is likewise absent from `company_registry_states` (it goes to `company_addresses`, `normalized_duckdb.py:427-441`).
   - `company_registry_states` carries `derived_status`, which section 3.1 forbids ("no derived status"), plus each source's always-NULL twin of the other source's columns.
2. **Column names are the plain English rename of the source field**, reusing the name `normalized_duckdb.py` already gives it where one exists. `avregistreringsdatum` → `deregistration_date` (not `dissolution_date`), `registreringsdatum` / `RegDatKtid` → `registration_date` (not `incorporation_date`): those are entity concepts the slice-1 fold produces, not source facts. `Ng1`..`Ng5` → `ng1_code`..`ng5_code`, matching `company_industry_states`. `COAdress` → `care_of`, `Gatuadress` → `street_address`, `PostNr` → `postal_code`, `PostOrt` → `post_town`, matching `company_addresses`. `FtgStat` → `source_status_code` and `JEStat` → `source_secondary_status_code`, matching `company_registry_states`.
3. **SCB's `Foretagsnamn` is kept as `alternate_name`.** The design's "company name" covers SCB's two name fields; the domain-suggestions model builds an alternate-name feature out of it today (`stg_se_company_match_features.sql:39-47`) and Task 6 must keep that output column.
4. **`ForAndrTyp` is not published.** It is in `SCB_SOURCE_COLUMNS` (`tables.py:69`) but not in `SCB_REQUIRED_COLUMNS`, not in `company_registry_states`, and not in section 3.1's enumeration: it is a per-delivery change-type marker, not a company attribute. `scb_raw` and the S3 snapshot keep it. The `m*` previous-value twins are dropped for the same reason, as section 3.1 says explicitly.
5. **Types are 000257's** (`000257_corpscout_se_company_profile_history.up.sql`) for every column that table also had, **except the dates, which are `Nullable(Date32)`.** 000257 used `Nullable(Date)`, whose range starts at 1970-01-01; Swedish registration dates predate it, and the spec's own basic-info tables (§3.2, §3.3) use `Date32`. New columns take the type the same concept already has elsewhere in the SE schema: `postal_address` / `street_address` / `care_of` / `postal_code` / `post_town` are `Nullable(String)` as in `000084_corpscout_se_company_registry.up.sql:37-40`.
6. **Provenance is exactly the four columns section 3.1 names**: `source_run_id`, `source_record_id`, `source_payload_hash`, `observed_at`. No `source_record_uid DEFAULT` column (000257's registry tables had one; the slice-1 extractors compute the uid themselves from `source_record_id` + `source_payload_hash`, the same formula 000257 encodes), no `raw_record`, no `has_observation`, no `*_fingerprint`.
7. **`observed_at` is the module's existing observation instant**: the `loaded_at` that `sweden_company_normalized_duckdb` computes as `datetime.now(UTC)` (`assets.py:111`) and passes into `replace_sweden_company_normalized_tables`, the same value `company_registry_states.observed_at` gets.
8. **The change-only publish helper is new, named `publish_sweden_company_source_table`**, and it anti-joins on `(company_id, source_payload_hash)` against `argMax(source_payload_hash, observed_at) GROUP BY company_id` — not against the whole target table. Reasons, from reading both existing helpers:
   - `se_company/common.publish_with_stage(..., new_versions_only=True)` (`common.py:128-147`) anti-joins the target table directly. It can, because its targets are ordered by `(company_id, source_record_uid)` where every version is its own key. These tables are `ORDER BY company_id`, one row per company, so a whole-table anti-join would suppress an A → B → A revert and leave the stale B row as current.
   - `sweden_company/clickhouse.py::_publish_changed_snapshot` (`clickhouse.py:155-248`) needs a physical `*_current` twin to diff against and swaps it by `RENAME TABLE`. Section 3.1 gives these tables no twin ("No history table: S3 is the archive"), so it does not apply.

## File map

| Path (relative to `corpscout/services/dagster_v3` unless noted) | Responsibility |
| --- | --- |
| `../../clickhouse/migrations/000373_corpscout_se_scb_companies.{up,down}.sql` | `corpscout.se_scb_companies` DDL |
| `../../clickhouse/migrations/000374_corpscout_se_bolagsverket_companies.{up,down}.sql` | `corpscout.se_bolagsverket_companies` DDL |
| `../../clickhouse/migrations/000375_corpscout_retire_se_company_registry.{up,down}.sql` | gated drop of the retired registry pair, written at its apply step |
| `src/dagster_v3/defs/sweden_company/tables.py` | table-name constants + the two export column tuples; registry constants removed |
| `src/dagster_v3/defs/sweden_company/normalized_duckdb.py` | `_replace_scb_companies_table`, `_replace_bolagsverket_companies_table` |
| `src/dagster_v3/defs/sweden_company/clickhouse.py` | `publish_sweden_company_source_table`, `sweden_company_source_anti_join_sql`; `publish_sweden_company_profile_history` loses its registry half |
| `src/dagster_v3/defs/sweden_company/assets.py` | the two new export assets, job selection, `defs` |
| `src/dagster_v3/defs/common/clickhouse_checks.py` | one `ClickhouseLeaf` per new export asset |
| `src/dagster_v3/defs/company_domain_suggestions/dbt/models/sources.yml` | source list: registry out, the two new tables in |
| `src/dagster_v3/defs/company_domain_suggestions/dbt/models/staging/stg_se_company_match_features.sql` | name features read the union of the two new tables |
| `src/dagster_v3/defs/sweden_company/docs/sweden_company-design.md`, `docs/sweden-data-sources.md` | module docs follow the retirement |
| `tests/test_sweden_company_source_tables.py` (new) | DDL layout pins for the two tables |
| `tests/test_sweden_company_source_tables_clickhouse_local.py` (new) | the anti-join executed on a real engine |
| `tests/test_clickhouse_migrations.py`, `tests/test_sweden_company_normalized_duckdb.py`, `tests/test_sweden_company_clickhouse.py`, `tests/test_sweden_company_assets.py`, `tests/test_sweden_company_profile_history.py`, `tests/test_clickhouse_leaf_checks.py`, `tests/test_company_domain_suggestions_dbt.py` | existing tests extended / retargeted |

---

### Task 1: Migrations 000373 / 000374 and the export column tuples

**Files:**
- Create: `corpscout/clickhouse/migrations/000373_corpscout_se_scb_companies.up.sql`
- Create: `corpscout/clickhouse/migrations/000373_corpscout_se_scb_companies.down.sql`
- Create: `corpscout/clickhouse/migrations/000374_corpscout_se_bolagsverket_companies.up.sql`
- Create: `corpscout/clickhouse/migrations/000374_corpscout_se_bolagsverket_companies.down.sql`
- Modify: `src/dagster_v3/defs/sweden_company/tables.py` (add four name constants after line 41; add two column tuples after line 137)
- Modify: `tests/test_clickhouse_migrations.py` (two lines appended to `EXPECTED_MIGRATIONS` after line 387; one test appended at the end of the file, after `test_every_migration_ends_with_a_statement_not_a_comment`, which ends at line 4080)
- Test: `tests/test_sweden_company_source_tables.py` (new)

**Interfaces:**
- Consumes: `tests/se_company_ddl.py` helpers `declared_columns(table) -> list[str]` and `table_block(table) -> str` (they locate the one migration whose `CREATE TABLE IF NOT EXISTS corpscout.<table>\n` declares the table and return its statement up to the terminating `;`); `_migration_sql(file_name) -> str` in `tests/test_clickhouse_migrations.py`.
- Produces (fixed names every later task imports):
  - `tables.SCB_COMPANIES_TABLE_CH = "se_scb_companies"`, `tables.BOLAGSVERKET_COMPANIES_TABLE_CH = "se_bolagsverket_companies"`
  - `tables.QUALIFIED_SCB_COMPANIES_TABLE = "corpscout.se_scb_companies"`, `tables.QUALIFIED_BOLAGSVERKET_COMPANIES_TABLE = "corpscout.se_bolagsverket_companies"`
  - `tables.SE_SCB_COMPANIES_EXPORT_COLUMNS: tuple[str, ...]` — 22 names, DDL order
  - `tables.SE_BOLAGSVERKET_COMPANIES_EXPORT_COLUMNS: tuple[str, ...]` — 17 names, DDL order

**Verify at execution:** run `ls corpscout/clickhouse/migrations | tail -2` from the repo root. Expected output is exactly
```
000372_corpscout_retire_se_company_info_correction.down.sql
000372_corpscout_retire_se_company_info_correction.up.sql
```
If it is not, the ledger moved: renumber 000373/000374/000375 consistently in every task of this plan before continuing.

- [ ] **Step 1: Write the failing migration-ledger test**

Append to `tests/test_clickhouse_migrations.py`, after `test_every_migration_ends_with_a_statement_not_a_comment`:

```python
def test_se_register_source_tables_are_created_by_000373_and_000374() -> None:
    """2026-09-03 SE basic-info design, section 3.1: one table per register source, holding
    that source's whole record in the source's own organisation, keyed on company_id alone
    and replaced only when the source record changes. Neither up file drops anything --
    se_company_registry_observations and se_company_registry_current are retired by 000375,
    written at its apply step, because a DROP that has to wait for a deploy must not sit in
    the sequential ledger (2026-08-25 ruling, and the 000371 -> 000372 precedent)."""
    scb_up = _migration_sql("000373_corpscout_se_scb_companies.up.sql")
    scb_down = _migration_sql("000373_corpscout_se_scb_companies.down.sql")
    bolagsverket_up = _migration_sql("000374_corpscout_se_bolagsverket_companies.up.sql")
    bolagsverket_down = _migration_sql(
        "000374_corpscout_se_bolagsverket_companies.down.sql"
    )

    assert "CREATE TABLE IF NOT EXISTS corpscout.se_scb_companies\n" in scb_up
    assert "DROP TABLE IF EXISTS corpscout.se_scb_companies" in scb_down
    assert (
        "CREATE TABLE IF NOT EXISTS corpscout.se_bolagsverket_companies\n"
        in bolagsverket_up
    )
    assert (
        "DROP TABLE IF EXISTS corpscout.se_bolagsverket_companies" in bolagsverket_down
    )

    for up, columns in (
        (scb_up, sweden_company_tables.SE_SCB_COMPANIES_EXPORT_COLUMNS),
        (
            bolagsverket_up,
            sweden_company_tables.SE_BOLAGSVERKET_COMPANIES_EXPORT_COLUMNS,
        ),
    ):
        for column in columns:
            assert f"\n    {column} " in up, column
        assert "ENGINE = ReplacingMergeTree(observed_at)\nORDER BY company_id;" in up
        assert (
            "CONSTRAINT has_company CHECK match(company_id, "
            "'^([0-9]{10}|[0-9]{12})$')"
        ) in up
        # No derived status and no merge: each table keeps its source's own codes.
        assert "derived_status" not in up
        # The DROPs of the retired registry pair belong to 000375 alone.
        assert "DROP TABLE" not in up
        assert "TRUNCATE TABLE" not in up.upper()

    # The m* previous-value twins of the SCB delivery file are not published: the S3
    # snapshots keep every file as delivered (section 3.1).
    for twin in ("mNamn", "mFtgStat", "mJurForm", "mNg1", "mPostNr", "mRegDatKtid"):
        assert twin not in scb_up
    # ForAndrTyp is a per-delivery change-type marker, not a company attribute.
    assert "ForAndrTyp" not in scb_up
```

And extend `EXPECTED_MIGRATIONS`, immediately after `"000372_corpscout_retire_se_company_info_correction",`:

```python
    "000373_corpscout_se_scb_companies",
    "000374_corpscout_se_bolagsverket_companies",
```

- [ ] **Step 2: Write the failing DDL-layout test**

Create `tests/test_sweden_company_source_tables.py`:

```python
"""The two register source tables of the 2026-09-03 SE basic-info design (spec 3.1).

The column pins read the migration DDL through tests/se_company_ddl.py -- the same helper
test_se_company_layout.py uses -- so the tuples in sweden_company/tables.py and the
deployed DDL cannot drift apart. The exporter binds those tuples positionally.
"""

from dagster_v3.defs.sweden_company import tables
from tests.se_company_ddl import declared_columns, table_block


def test_se_scb_companies_declares_the_whole_scb_record_in_export_order() -> None:
    assert declared_columns("se_scb_companies") == list(
        tables.SE_SCB_COMPANIES_EXPORT_COLUMNS
    )
    block = table_block("se_scb_companies")

    assert block.count("CREATE TABLE") == 1 and block.endswith(";")
    assert "ENGINE = ReplacingMergeTree(observed_at)" in block
    assert "ORDER BY company_id" in block
    # SCB's own codes, kept as delivered: turning FtgStat into an entity status is the scb
    # suggestion extractor's job (slice 1), never a source table's.
    assert "source_status_code LowCardinality(Nullable(String))" in block
    assert "source_secondary_status_code LowCardinality(Nullable(String))" in block
    assert "derived_status" not in block
    # Date32, not 000257's Date: Date starts at 1970-01-01 and registration dates do not.
    assert "registration_date Nullable(Date32)" in block
    # The address and SNI columns company_registry_states never carried.
    for column in ("ng1_code", "ng5_code", "care_of", "street_address", "post_town"):
        assert f"    {column} " in block, column


def test_se_bolagsverket_companies_declares_the_whole_bolagsverket_record() -> None:
    assert declared_columns("se_bolagsverket_companies") == list(
        tables.SE_BOLAGSVERKET_COMPANIES_EXPORT_COLUMNS
    )
    block = table_block("se_bolagsverket_companies")

    assert block.count("CREATE TABLE") == 1 and block.endswith(";")
    assert "ENGINE = ReplacingMergeTree(observed_at)" in block
    assert "ORDER BY company_id" in block
    assert "registration_date Nullable(Date32)" in block
    assert "deregistration_date Nullable(Date32)" in block
    # The packed postal address exactly as Bolagsverket delivers it: parsing belongs to the
    # address entity, which keeps its own tables until its own slice.
    assert "postal_address Nullable(String)" in block
    assert "derived_status" not in block


def test_neither_source_table_carries_provenance_it_does_not_need() -> None:
    """Provenance is the four columns section 3.1 names. No source_record_uid DEFAULT
    (000257's registry tables had one; the slice-1 extractors compute the uid themselves),
    no raw_record, no *_current bookkeeping."""
    for table in ("se_scb_companies", "se_bolagsverket_companies"):
        block = table_block(table)
        assert "source_run_id String" in block, table
        assert "source_record_id String" in block, table
        assert "source_payload_hash String" in block, table
        assert "observed_at DateTime64(3, 'UTC')" in block, table
        assert "source_record_uid" not in block, table
        assert "raw_record" not in block, table
        assert "has_observation" not in block, table
        assert "observation_fingerprint" not in block, table
        assert "state_fingerprint" not in block, table
```

- [ ] **Step 3: Run both tests to verify they fail**

Run: `cd corpscout/services/dagster_v3 && uv run --frozen --no-sync pytest tests/test_sweden_company_source_tables.py tests/test_clickhouse_migrations.py -q -p no:warnings`
Expected: FAIL — `AttributeError: module 'dagster_v3.defs.sweden_company.tables' has no attribute 'SE_SCB_COMPANIES_EXPORT_COLUMNS'` from the new tests, and `test_clickhouse_migration_files_are_explicit` failing because `EXPECTED_MIGRATIONS` now names two files that do not exist.

- [ ] **Step 4: Write `000373_corpscout_se_scb_companies.up.sql`**

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- One row per company: the whole SCB register record, in SCB's own organisation
-- (2026-09-03 SE basic-info design, section 3.1). English column names where the name is a
-- plain rename, SCB's own status codes untouched, no derived status and no merge with
-- Bolagsverket -- turning FtgStat into an entity status is the job of the scb suggestion
-- extractor, not of a source table. The m* previous-value twins of the delivery file and
-- the per-delivery change-type marker are not published: the S3 snapshots keep every file
-- exactly as delivered, and scb_raw keeps every column in DuckDB.
--
-- Column types are 000257's for every column that table also had, except company_id_raw,
-- which is String rather than Nullable(String): a row exists only when company_id was
-- derived from a non-empty raw identifier, so the raw value is never NULL. registration_date is
-- Date32 rather than 000257's Date because Date starts at 1970-01-01 and Swedish
-- registration dates predate it. The design's own basic-info tables use Date32 too.
--
-- This replaces the SCB half of se_company_registry_observations and
-- se_company_registry_current. Those two are NOT dropped here: a DROP that has to wait for
-- a deploy must not sit in the sequential ledger (2026-08-25 ruling), so their retirement
-- is migration 000375, written and applied after this table holds rows and the
-- domain-suggestions dbt model has been rebuilt against it.
CREATE TABLE IF NOT EXISTS corpscout.se_scb_companies
(
    company_id String,
    company_id_raw String,
    legal_name Nullable(String),
    alternate_name Nullable(String),
    legal_form_code LowCardinality(Nullable(String)),
    source_status_code LowCardinality(Nullable(String)),
    source_secondary_status_code LowCardinality(Nullable(String)),
    registration_date Nullable(Date32),
    ng1_code LowCardinality(Nullable(String)),
    ng2_code LowCardinality(Nullable(String)),
    ng3_code LowCardinality(Nullable(String)),
    ng4_code LowCardinality(Nullable(String)),
    ng5_code LowCardinality(Nullable(String)),
    care_of Nullable(String),
    street_address Nullable(String),
    postal_code Nullable(String),
    post_town Nullable(String),
    marketing_block_code LowCardinality(Nullable(String)),
    source_run_id String,
    source_record_id String,
    source_payload_hash String,
    observed_at DateTime64(3, 'UTC'),

    CONSTRAINT has_company CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY company_id;
```

- [ ] **Step 5: Write `000373_corpscout_se_scb_companies.down.sql`**

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

DROP TABLE IF EXISTS corpscout.se_scb_companies;
```

- [ ] **Step 6: Write `000374_corpscout_se_bolagsverket_companies.up.sql`**

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- One row per company: the whole Bolagsverket register record, in Bolagsverket's own
-- organisation (2026-09-03 SE basic-info design, section 3.1). Identity, name-protection
-- sequence, registration country, name, legal form, deregistration date and reason, the
-- pending-proceedings field, registration date, activity description, postal address.
-- legal_name is the first segment of the packed organisationsnamn and legal_name_raw is
-- that packed string as delivered, exactly as company_registry_states already split them.
-- postal_address is the packed postadress as delivered -- parsing it belongs to the address
-- entity, which keeps its own tables and assets until its own slice.
--
-- No derived status: deriving one from avregistreringsdatum is the job of the bolagsverket
-- suggestion extractor. Column types are 000257's for every column that table also had.
-- The two dates are Date32 rather than 000257's Date because Date starts at 1970-01-01 and
-- Swedish registration dates predate it.
--
-- This replaces the Bolagsverket half of se_company_registry_observations and
-- se_company_registry_current, retired by migration 000375 at its apply step -- never here,
-- per the 2026-08-25 ruling on DROPs that have to wait for a deploy.
CREATE TABLE IF NOT EXISTS corpscout.se_bolagsverket_companies
(
    company_id String,
    company_id_raw String,
    name_protection_sequence Nullable(String),
    registration_country_code LowCardinality(Nullable(String)),
    legal_name Nullable(String),
    legal_name_raw Nullable(String),
    legal_form_code LowCardinality(Nullable(String)),
    registration_date Nullable(Date32),
    deregistration_date Nullable(Date32),
    deregistration_reason LowCardinality(Nullable(String)),
    proceedings_raw Nullable(String),
    activity_description Nullable(String),
    postal_address Nullable(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash String,
    observed_at DateTime64(3, 'UTC'),

    CONSTRAINT has_company CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY company_id;
```

- [ ] **Step 7: Write `000374_corpscout_se_bolagsverket_companies.down.sql`**

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

DROP TABLE IF EXISTS corpscout.se_bolagsverket_companies;
```

- [ ] **Step 8: Add the name constants to `tables.py`**

Insert after line 41 (immediately after the `QUALIFIED_COMPANY_INDUSTRY_CURRENT_TABLE` block closes and before the blank line preceding `SWEDEN_COMPANY_DUCKDB_PATH`):

```python
SCB_COMPANIES_TABLE_CH = "se_scb_companies"
BOLAGSVERKET_COMPANIES_TABLE_CH = "se_bolagsverket_companies"
QUALIFIED_SCB_COMPANIES_TABLE = f"{SWEDEN_DATABASE}.{SCB_COMPANIES_TABLE_CH}"
QUALIFIED_BOLAGSVERKET_COMPANIES_TABLE = (
    f"{SWEDEN_DATABASE}.{BOLAGSVERKET_COMPANIES_TABLE_CH}"
)
```

- [ ] **Step 9: Add the export column tuples to `tables.py`**

Insert after `SE_COMPANIES_EXPORT_COLUMNS` (ends line 137), before `SE_COMPANY_ADDRESS_BASE_COLUMNS`:

```python
# The whole SCB register record per company, in the DDL order of migration 000373. Bound
# positionally by the exporter, pinned against the migration by
# tests/test_sweden_company_source_tables.py.
SE_SCB_COMPANIES_EXPORT_COLUMNS = (
    "company_id",
    "company_id_raw",
    "legal_name",
    "alternate_name",
    "legal_form_code",
    "source_status_code",
    "source_secondary_status_code",
    "registration_date",
    "ng1_code",
    "ng2_code",
    "ng3_code",
    "ng4_code",
    "ng5_code",
    "care_of",
    "street_address",
    "postal_code",
    "post_town",
    "marketing_block_code",
    "source_run_id",
    "source_record_id",
    "source_payload_hash",
    "observed_at",
)

# The whole Bolagsverket register record per company, in the DDL order of migration 000374.
SE_BOLAGSVERKET_COMPANIES_EXPORT_COLUMNS = (
    "company_id",
    "company_id_raw",
    "name_protection_sequence",
    "registration_country_code",
    "legal_name",
    "legal_name_raw",
    "legal_form_code",
    "registration_date",
    "deregistration_date",
    "deregistration_reason",
    "proceedings_raw",
    "activity_description",
    "postal_address",
    "source_run_id",
    "source_record_id",
    "source_payload_hash",
    "observed_at",
)
```

- [ ] **Step 10: Run the tests to verify they pass**

Run: `cd corpscout/services/dagster_v3 && uv run --frozen --no-sync pytest tests/test_sweden_company_source_tables.py tests/test_clickhouse_migrations.py tests/test_se_company_layout.py -q -p no:warnings`
Expected: PASS, no failures. (`test_se_company_layout.py` is included because it shares `tests/se_company_ddl.py`; the new tables must not disturb `_migration_for`'s "exactly one migration creates this table" rule for any existing table.)

- [ ] **Step 11: Commit**

```bash
git add corpscout/clickhouse/migrations/000373_corpscout_se_scb_companies.up.sql \
        corpscout/clickhouse/migrations/000373_corpscout_se_scb_companies.down.sql \
        corpscout/clickhouse/migrations/000374_corpscout_se_bolagsverket_companies.up.sql \
        corpscout/clickhouse/migrations/000374_corpscout_se_bolagsverket_companies.down.sql \
        corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/tables.py \
        corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py \
        corpscout/services/dagster_v3/tests/test_sweden_company_source_tables.py
git commit -m "$(cat <<'EOF'
feat(dagster): add se_scb_companies and se_bolagsverket_companies tables

Migrations 000373/000374 create one ClickHouse table per Swedish register
source, each holding that source's whole record per company in the source's
own organisation, ReplacingMergeTree(observed_at) ORDER BY company_id. No
derived status, no merge, no *_current twin -- S3 remains the archive.
Neither up file drops the registry pair they replace: that retirement is
000375, written at its apply step.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
EOF
)"
```

---

### Task 2: The two normalised DuckDB tables

**Files:**
- Modify: `src/dagster_v3/defs/sweden_company/normalized_duckdb.py:51-93` (call the two new builders, count them) and add two functions after `_replace_company_registry_states_table` (which ends at line 234, before `_replace_company_proceedings_table` at line 236)
- Modify: `src/dagster_v3/defs/sweden_company/assets.py:117-132` (two new metadata keys)
- Test: `tests/test_sweden_company_normalized_duckdb.py` (update the counts assertion at line 108; append two tests before `_create_raw_tables` at line 834)

**Interfaces:**
- Consumes: `tables.SE_SCB_COMPANIES_EXPORT_COLUMNS`, `tables.SE_BOLAGSVERKET_COMPANIES_EXPORT_COLUMNS` (Task 1); `sweden_identity_sql(raw_column) -> str` from `sweden_company/identity.py`.
- Produces: DuckDB tables `sweden_company.scb_companies` and `sweden_company.bolagsverket_companies`, whose columns are exactly those two tuples in that order; `replace_sweden_company_normalized_tables(...)` returns a dict that additionally contains the keys `"scb_companies"` and `"bolagsverket_companies"`.

- [ ] **Step 1: Write the failing DuckDB tests**

Append to `tests/test_sweden_company_normalized_duckdb.py`, immediately before `def _create_raw_tables(...)`:

```python
def test_normalized_snapshot_builds_the_two_register_source_tables(
    tmp_path: Path,
) -> None:
    """2026-09-03 SE basic-info design, section 3.1: one table per register source, holding
    that source's whole record. company_registry_states cannot simply be filtered into
    these -- it carries neither SCB's Ng1..Ng5 and address columns nor Bolagsverket's packed
    postal address, and it carries a derived_status these tables must not have."""
    database_path = tmp_path / "sweden_company_source.duckdb"
    loaded_at = datetime(2026, 9, 3, 6, 0, tzinfo=UTC)

    with duckdb.connect(str(database_path)) as connection:
        _create_raw_tables(connection)

        replace_sweden_company_normalized_tables(
            connection=connection,
            loaded_at=loaded_at,
        )

        scb = connection.execute(
            f"""
            select
                company_id,
                company_id_raw,
                legal_name,
                alternate_name,
                legal_form_code,
                source_status_code,
                source_secondary_status_code,
                registration_date::varchar,
                ng1_code,
                ng2_code,
                ng3_code,
                ng4_code,
                ng5_code,
                care_of,
                street_address,
                postal_code,
                post_town,
                marketing_block_code,
                source_run_id,
                source_record_id,
                source_payload_hash,
                observed_at
            from {tables.DLT_DATASET_NAME}.scb_companies
            where company_id = '5560000000'
            """
        ).fetchone()
        bolagsverket = connection.execute(
            f"""
            select
                company_id,
                company_id_raw,
                name_protection_sequence,
                registration_country_code,
                legal_name,
                legal_name_raw,
                legal_form_code,
                registration_date::varchar,
                deregistration_date::varchar,
                deregistration_reason,
                proceedings_raw,
                activity_description,
                postal_address,
                source_run_id,
                source_record_id,
                source_payload_hash,
                observed_at
            from {tables.DLT_DATASET_NAME}.bolagsverket_companies
            where company_id = '5560000000'
            """
        ).fetchall()
        closed = connection.execute(
            f"""
            select
                deregistration_date::varchar,
                deregistration_reason,
                legal_name
            from {tables.DLT_DATASET_NAME}.bolagsverket_companies
            where company_id = '5561111111'
            """
        ).fetchone()
        collision = connection.execute(
            f"""
            select
                company_id_raw,
                legal_name,
                registration_date::varchar,
                postal_address
            from {tables.DLT_DATASET_NAME}.bolagsverket_companies
            where company_id = '5565258888'
            """
        ).fetchall()

    assert scb == (
        "5560000000",
        "5560000000",
        "ACME SCB",
        None,
        "49",
        "0",
        "1",
        "2020-01-01",
        "62010",
        "70220",
        None,
        None,
        None,
        "c/o ACME",
        "Main Street 1",
        "11122",
        "STOCKHOLM",
        "1",
        "run-1",
        "5560000000",
        "scb-hash-1",
        loaded_at,
    )
    assert bolagsverket == [
        (
            "5560000000",
            "5560000000$ORGNR-IDORG",
            "1",
            "SE-LAND",
            "Acme AB",
            "Acme AB$FORETAGSNAMN-ORGNAM$2020-01-01",
            "AB-ORGFO",
            "2020-01-01",
            None,
            None,
            None,
            "Runs acme.se",
            "Box 1$c/o CFO$STOCKHOLM$11122$SE-LAND",
            "run-1",
            "5560000000$ORGNR-IDORG",
            "bolag-hash-1",
            loaded_at,
        )
    ]
    # A deregistered company keeps the source's own date and reason -- no derived status.
    assert closed == ("2025-02-03", "OVERK-AVORG", "Closed AB")
    # One row per company: the '16'-prefixed duplicate collapses onto the 10-digit identity
    # and the winner is the smaller source_record_id, exactly as company_registry_states
    # ranks its candidates.
    assert collision == [
        (
            "165565258888",
            "Bv8888 AB",
            "2017-07-01",
            "Bv8888 Street 5$$SIBLINGTOWN$55555$SE-LAND",
        )
    ]


def test_the_register_source_duckdb_tables_match_the_export_column_tuples(
    tmp_path: Path,
) -> None:
    """The exporter binds tables.SE_*_EXPORT_COLUMNS positionally against the migration's
    column order, so the DuckDB tables must offer exactly those names in exactly that
    order."""
    database_path = tmp_path / "sweden_company_source.duckdb"

    with duckdb.connect(str(database_path)) as connection:
        _create_raw_tables(connection)
        replace_sweden_company_normalized_tables(
            connection=connection,
            loaded_at=datetime(2026, 9, 3, 6, 0, tzinfo=UTC),
        )

        for duckdb_table, columns in (
            ("scb_companies", tables.SE_SCB_COMPANIES_EXPORT_COLUMNS),
            ("bolagsverket_companies", tables.SE_BOLAGSVERKET_COMPANIES_EXPORT_COLUMNS),
        ):
            names = [
                str(row[0])
                for row in connection.execute(
                    """
                    select column_name
                    from information_schema.columns
                    where table_schema = ? and table_name = ?
                    order by ordinal_position
                    """,
                    [tables.DLT_DATASET_NAME, duckdb_table],
                ).fetchall()
            ]
            assert names == list(columns), duckdb_table
```

Also replace the counts assertion at line 108 with:

```python
    assert counts == {
        # Old -> new pins after adding the twin pair (5565257747), the
        # 19-prefixed sole trader (195001011234), and the BV-internal
        # 12-digit '16' collision pair (5565258888) to the fixtures:
        "companies": 6,  # old 3
        "company_addresses": 8,  # old 4
        "company_registry_states": 8,
        # One row per company per source, the source's whole record (spec 3.1). They add up
        # to company_registry_states because the same identity + rank-1 dedup builds all
        # three.
        "scb_companies": 4,
        "bolagsverket_companies": 4,
        "company_proceedings": 0,
        "company_industry_states": 4,
        "company_industry_codes": 6,  # old 4
        "bolagsverket_company_count": 4,  # old 2
        "scb_company_count": 4,  # old 2
        "companies_with_sni_count": 4,  # old 2
        "unknown_sni_count": 2,  # unchanged -- new SCB rows add no invalid SNI codes
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd corpscout/services/dagster_v3 && uv run --frozen --no-sync pytest tests/test_sweden_company_normalized_duckdb.py -q -p no:warnings`
Expected: FAIL — `duckdb.CatalogException: Table with name scb_companies does not exist!` and the counts assertion failing on the two missing keys.

- [ ] **Step 3: Add the two builders to `normalized_duckdb.py`**

Insert after `_replace_company_registry_states_table` (ends line 234), before `_replace_company_proceedings_table`:

```python
def _replace_scb_companies_table(*, connection: Any, loaded_at: datetime) -> None:
    """The whole SCB register record per company, in SCB's own organisation.

    Built from scb_raw rather than filtered out of company_registry_states: that table
    carries neither the five SNI columns nor the four address columns, and it carries a
    derived_status the source layer must not have (2026-09-03 basic-info design, 3.1).
    """
    scb_identity = sweden_identity_sql("PeOrgNr")
    connection.execute(
        f"""
        create or replace table sweden_company.scb_companies as
        with scb_source as (
            select
                *,
                {scb_identity} as company_id,
                row_number() over (
                    partition by {scb_identity}
                    order by source_record_id, source_line_number, source_payload_hash
                ) as company_rank
            from sweden_company.scb_raw
            where {scb_identity} != ''
        )
        select
            company_id,
            coalesce(PeOrgNr, '') as company_id_raw,
            nullif(trim(Namn), '') as legal_name,
            nullif(trim(Foretagsnamn), '') as alternate_name,
            nullif(trim(JurForm), '') as legal_form_code,
            nullif(trim(FtgStat), '') as source_status_code,
            nullif(trim(JEStat), '') as source_secondary_status_code,
            try_strptime(nullif(trim(RegDatKtid), ''), '%Y%m%d')::date
                as registration_date,
            nullif(trim(Ng1), '') as ng1_code,
            nullif(trim(Ng2), '') as ng2_code,
            nullif(trim(Ng3), '') as ng3_code,
            nullif(trim(Ng4), '') as ng4_code,
            nullif(trim(Ng5), '') as ng5_code,
            nullif(trim(COAdress), '') as care_of,
            nullif(trim(Gatuadress), '') as street_address,
            nullif(trim(PostNr), '') as postal_code,
            nullif(trim(PostOrt), '') as post_town,
            nullif(trim(Reklamsparrtyp), '') as marketing_block_code,
            source_run_id,
            source_record_id,
            source_payload_hash,
            ? as observed_at
        from scb_source
        where company_rank = 1
        """,
        [loaded_at],
    )


def _replace_bolagsverket_companies_table(
    *, connection: Any, loaded_at: datetime
) -> None:
    """The whole Bolagsverket register record per company, in Bolagsverket's own
    organisation.

    Built from bolagsverket_raw rather than filtered out of company_registry_states: that
    table does not carry the packed postadress, and it carries a derived_status the source
    layer must not have (2026-09-03 basic-info design, 3.1).
    """
    bolagsverket_identity = sweden_identity_sql("organisationsidentitet")
    connection.execute(
        f"""
        create or replace table sweden_company.bolagsverket_companies as
        with bolagsverket_source as (
            select
                *,
                {bolagsverket_identity} as company_id,
                row_number() over (
                    partition by {bolagsverket_identity}
                    order by source_record_id, source_line_number, source_payload_hash
                ) as company_rank
            from sweden_company.bolagsverket_raw
            where {bolagsverket_identity} != ''
        )
        select
            company_id,
            coalesce(organisationsidentitet, '') as company_id_raw,
            nullif(trim(namnskyddslopnummer), '') as name_protection_sequence,
            nullif(trim(registreringsland), '') as registration_country_code,
            nullif(trim(split_part(organisationsnamn, '$', 1)), '') as legal_name,
            organisationsnamn as legal_name_raw,
            nullif(trim(organisationsform), '') as legal_form_code,
            try_strptime(
                nullif(trim(registreringsdatum), ''), '%Y-%m-%d'
            )::date as registration_date,
            try_strptime(
                nullif(trim(avregistreringsdatum), ''), '%Y-%m-%d'
            )::date as deregistration_date,
            nullif(trim(avregistreringsorsak), '') as deregistration_reason,
            nullif(
                trim(pagandeAvvecklingsEllerOmstruktureringsforfarande), ''
            ) as proceedings_raw,
            nullif(trim(verksamhetsbeskrivning), '') as activity_description,
            postadress as postal_address,
            source_run_id,
            source_record_id,
            source_payload_hash,
            ? as observed_at
        from bolagsverket_source
        where company_rank = 1
        """,
        [loaded_at],
    )
```

- [ ] **Step 4: Call them and count them**

In `replace_sweden_company_normalized_tables`, insert after the `_replace_company_registry_states_table(...)` call (lines 60-62) and before `_replace_company_proceedings_table(...)`:

```python
        _replace_scb_companies_table(connection=connection, loaded_at=loaded_at)
        _replace_bolagsverket_companies_table(
            connection=connection, loaded_at=loaded_at
        )
```

In the `counts` dict (line 73), insert after the `"company_registry_states"` entry:

```python
            "scb_companies": _table_count(connection, "scb_companies"),
            "bolagsverket_companies": _table_count(
                connection, "bolagsverket_companies"
            ),
```

- [ ] **Step 5: Surface the two counts in the asset metadata**

In `src/dagster_v3/defs/sweden_company/assets.py`, in `sweden_company_normalized_duckdb`'s `dg.MaterializeResult` metadata (lines 118-131), insert after `"registry_state_count": counts["company_registry_states"],`:

```python
            "scb_company_row_count": counts["scb_companies"],
            "bolagsverket_company_row_count": counts["bolagsverket_companies"],
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cd corpscout/services/dagster_v3 && uv run --frozen --no-sync pytest tests/test_sweden_company_normalized_duckdb.py tests/test_sweden_company_code_quality.py -q -p no:warnings`
Expected: PASS, no failures.

- [ ] **Step 7: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/normalized_duckdb.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/assets.py \
        corpscout/services/dagster_v3/tests/test_sweden_company_normalized_duckdb.py
git commit -m "$(cat <<'EOF'
feat(dagster): build per-source Sweden register tables in DuckDB

sweden_company.scb_companies and sweden_company.bolagsverket_companies hold
one row per company with that source's whole record, built from the raw
tables with the identity normalisation and rank-1 dedup company_registry_states
already uses. company_registry_states could not be filtered into them: it
carries neither SCB's SNI and address columns nor Bolagsverket's packed
postal address, and it carries a derived status the source layer must not.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
EOF
)"
```

---

### Task 3: `sweden_company_scb_companies_clickhouse`

**Files:**
- Modify: `src/dagster_v3/defs/sweden_company/clickhouse.py` (add `SwedenSourceTablePublishResult`, `sweden_company_source_anti_join_sql`, `publish_sweden_company_source_table` after `publish_sweden_company_industry_history`, which ends at line 152, before `_publish_changed_snapshot` at line 155)
- Modify: `src/dagster_v3/defs/sweden_company/assets.py` (import, new asset, job selection at lines 319-328, `defs` assets list at lines 340-351)
- Modify: `src/dagster_v3/defs/common/clickhouse_checks.py:188-193` (one leaf in the `sweden_company` block)
- Test: `tests/test_sweden_company_clickhouse.py` (extend `FakeClickHouseClient.execute`, add one test), `tests/test_sweden_company_assets.py:20-29`, `tests/test_clickhouse_leaf_checks.py:23-56`, `tests/test_sweden_company_source_tables_clickhouse_local.py` (new)

**Interfaces:**
- Consumes: `tables.SCB_COMPANIES_TABLE_CH`, `tables.QUALIFIED_SCB_COMPANIES_TABLE`, `tables.SE_SCB_COMPANIES_EXPORT_COLUMNS` (Task 1); DuckDB table `sweden_company.scb_companies` (Task 2); `assert_clickhouse_tables_exist`, `export_duckdb_connection_table_to_clickhouse` from `dagster_v3.defs.clickhouse.resolved`; `read_only_duckdb_connection` from `dagster_v3.defs.common.duckdb_resources`.
- Produces:
  - `sweden_company_source_anti_join_sql(*, qualified_stage: str, qualified_target: str) -> str`
  - `SwedenSourceTablePublishResult(candidates: int, inserted: int)` — frozen slotted dataclass
  - `publish_sweden_company_source_table(*, duckdb_connection, clickhouse: ClickhouseResource, duckdb_table: str, clickhouse_table: str, columns: tuple[str, ...], log=None) -> SwedenSourceTablePublishResult`
  - Dagster asset `sweden_company_scb_companies_clickhouse`, group `sweden_company`, pool `sweden_company_duckdb`, member of `sweden_company_refresh_job`

- [ ] **Step 1: Write the failing unit test**

In `tests/test_sweden_company_clickhouse.py`, first extend the fake so the anti-join count has an answer. Insert into `FakeClickHouseClient.execute`, immediately after the `if "AS snapshot_removals" in sql:` branch (line 34-35):

```python
        if "AS new_versions" in sql:
            return [(1,)]
```

Then add to the import block at the top:

```python
from dagster_v3.defs.sweden_company.clickhouse import (
    export_sweden_company_clickhouse_companies,
    export_sweden_company_clickhouse_industries,
    publish_sweden_company_industry_history,
    publish_sweden_company_profile_history,
    publish_sweden_company_source_table,
    sweden_company_source_anti_join_sql,
)
```

Add the DuckDB fixture table. Inside `_sweden_company_duckdb`, after the `company_industry_states` create/insert pair and before `yield connection`:

```python
        connection.execute(
            f"""
            create table {tables.DLT_DATASET_NAME}.scb_companies (
                company_id varchar,
                company_id_raw varchar,
                legal_name varchar,
                alternate_name varchar,
                legal_form_code varchar,
                source_status_code varchar,
                source_secondary_status_code varchar,
                registration_date date,
                ng1_code varchar,
                ng2_code varchar,
                ng3_code varchar,
                ng4_code varchar,
                ng5_code varchar,
                care_of varchar,
                street_address varchar,
                postal_code varchar,
                post_town varchar,
                marketing_block_code varchar,
                source_run_id varchar,
                source_record_id varchar,
                source_payload_hash varchar,
                observed_at timestamp
            )
            """
        )
        connection.execute(
            f"""
            insert into {tables.DLT_DATASET_NAME}.scb_companies
            values (
                '5560000000', '5560000000', 'ACME SCB', null, '49', '0', '1',
                '2020-01-01', '62010', '70220', null, null, null,
                'c/o ACME', 'Main Street 1', '11122', 'STOCKHOLM', '1',
                'run-1', '5560000000', 'scb-hash-1', '2026-09-03 06:00:00'
            )
            """
        )
```

And add the test, after `test_publish_sweden_company_industry_history_tracks_complete_sni_state`:

```python
def test_publish_sweden_company_source_table_inserts_only_changed_payloads(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """The SCB source table is ReplacingMergeTree(observed_at) ORDER BY company_id, so the
    anti-join's right side must be the CURRENT row per company (argMax over observed_at),
    not the whole table: a whole-table anti-join would suppress an A -> B -> A revert and
    leave the stale B row current."""
    client = FakeClickHouseClient()
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(self: ClickhouseResource) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    with _sweden_company_duckdb(tmp_path) as connection:
        result = publish_sweden_company_source_table(
            duckdb_connection=connection,
            clickhouse=resource,
            duckdb_table="scb_companies",
            clickhouse_table=tables.SCB_COMPANIES_TABLE_CH,
            columns=tables.SE_SCB_COMPANIES_EXPORT_COLUMNS,
        )

    assert result.candidates == 1
    assert result.inserted == 1
    assert client.table_checks == [(tables.SCB_COMPANIES_TABLE_CH,)]
    assert (
        f"CREATE TABLE corpscout._tmp_{tables.SCB_COMPANIES_TABLE_CH}_"
        in client.statements[1]
    )
    # Staged first, then copied with the anti-join -- never straight into the target.
    # The fake records every INSERT statement; the target copy names the stage table in
    # its FROM clause too, so match the staged data insert by its prefix.
    staged_inserts = [
        sql
        for sql, _ in client.insert_calls
        if sql.lstrip().startswith(
            f"INSERT INTO `{tables.SWEDEN_DATABASE}`.`_tmp_{tables.SCB_COMPANIES_TABLE_CH}_"
        )
    ]
    assert len(staged_inserts) == 1
    target_inserts = [
        statement
        for statement in client.statements
        if statement.lstrip().startswith(
            f"INSERT INTO {tables.QUALIFIED_SCB_COMPANIES_TABLE} ("
        )
    ]
    assert len(target_inserts) == 1
    assert "LEFT ANTI JOIN" in target_inserts[0]
    assert "argMax(source_payload_hash, observed_at)" in target_inserts[0]
    assert ", ".join(tables.SE_SCB_COMPANIES_EXPORT_COLUMNS) in target_inserts[0]
    # The stage table is always dropped.
    assert any(
        statement.lstrip().startswith("DROP TABLE IF EXISTS corpscout._tmp_")
        for statement in client.statements
    )


def test_sweden_company_source_anti_join_reads_the_current_row_per_company() -> None:
    sql = sweden_company_source_anti_join_sql(
        qualified_stage="corpscout.stage",
        qualified_target="corpscout.se_scb_companies",
    )

    assert "FROM corpscout.stage AS candidate" in sql
    assert "LEFT ANTI JOIN (" in sql
    assert (
        "SELECT company_id, argMax(source_payload_hash, observed_at) "
        "AS source_payload_hash" in sql
    )
    assert "FROM corpscout.se_scb_companies" in sql
    assert "GROUP BY company_id" in sql
    assert "ON current.company_id = candidate.company_id" in sql
    assert "AND current.source_payload_hash = candidate.source_payload_hash" in sql
    # FINAL is not used: it would read the whole table through the merge logic, and argMax
    # states the intent (one current row per company) without depending on merge state.
    assert "FINAL" not in sql
```

Also extend `tests/test_sweden_company_assets.py` (line 20-29) to expect the new asset:

```python
    assert asset_keys == {
        "sweden_company_raw_snapshot_s3",
        "sweden_company_raw_duckdb",
        "sweden_company_normalized_duckdb",
        "sweden_company_companies_clickhouse",
        "sweden_company_scb_companies_clickhouse",
        "sweden_company_profile_history_clickhouse",
        "sweden_company_addresses_clickhouse",
        "sweden_company_industries_clickhouse",
        "sweden_company_industry_history_clickhouse",
    }
```

and add the new key to the pooled-exporter loop at lines 39-43:

```python
    for asset_key in (
        "sweden_company_companies_clickhouse",
        "sweden_company_scb_companies_clickhouse",
        "sweden_company_addresses_clickhouse",
        "sweden_company_industries_clickhouse",
    ):
```

and add one line to the spot-check list in `tests/test_clickhouse_leaf_checks.py` (inside `test_registry_covers_the_known_scheduled_leaves`, after `"sweden_company_companies_clickhouse",`):

```python
        "sweden_company_scb_companies_clickhouse",
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd corpscout/services/dagster_v3 && WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_sweden_company_clickhouse.py tests/test_sweden_company_assets.py tests/test_clickhouse_leaf_checks.py -q -p no:warnings`
Expected: FAIL — `ImportError: cannot import name 'publish_sweden_company_source_table'`, and the asset/leaf tests failing on the missing `sweden_company_scb_companies_clickhouse` key.

- [ ] **Step 3: Add the publish helper to `clickhouse.py`**

Insert after `publish_sweden_company_industry_history` (ends line 152), before `_publish_changed_snapshot`:

```python
@dataclass(frozen=True, slots=True)
class SwedenSourceTablePublishResult:
    candidates: int
    inserted: int


def sweden_company_source_anti_join_sql(
    *,
    qualified_stage: str,
    qualified_target: str,
) -> str:
    """The left-anti-join that keeps only the rows whose payload hash differs from the
    company's CURRENT published row.

    Exposed as a function so the clickhouse-local harness executes this exact text rather
    than a paraphrase of it.
    """
    return (
        f"FROM {qualified_stage} AS candidate\n"
        "LEFT ANTI JOIN (\n"
        "    SELECT company_id, argMax(source_payload_hash, observed_at) "
        "AS source_payload_hash\n"
        f"    FROM {qualified_target}\n"
        "    GROUP BY company_id\n"
        ") AS current\n"
        "ON current.company_id = candidate.company_id\n"
        "AND current.source_payload_hash = candidate.source_payload_hash"
    )


def publish_sweden_company_source_table(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    duckdb_table: str,
    clickhouse_table: str,
    columns: tuple[str, ...],
    log: Callable[..., object] | None = None,
) -> SwedenSourceTablePublishResult:
    """Insert only the register rows whose source payload hash changed.

    The target is ``ReplacingMergeTree(observed_at) ORDER BY company_id``: one row per
    company survives a merge, so the anti-join key has to be the CURRENT row per company
    and not every version ever written. ``se_company/common.publish_with_stage`` can
    anti-join its target directly (``new_versions_only=True``) because those targets are
    ordered by ``(company_id, source_record_uid)``, where every version is its own key.
    Here a company that went A -> B -> A would have its third state suppressed by a
    whole-table anti-join and the stale B row would stay current.

    ``_publish_changed_snapshot`` is not reused either: it needs a physical ``*_current``
    twin to diff against and swaps it by rename. The source layer deliberately has no twin
    -- one table per source, and S3 is the archive (2026-09-03 basic-info design, 3.1).
    """
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.SWEDEN_DATABASE,
        tables=(clickhouse_table,),
    )
    qualified_target = f"{tables.SWEDEN_DATABASE}.{clickhouse_table}"
    stage_table = f"_tmp_{clickhouse_table}_{uuid.uuid4().hex}"
    qualified_stage = f"{tables.SWEDEN_DATABASE}.{stage_table}"
    with clickhouse.get_connection() as client:
        primary_error: BaseException | None = None
        try:
            client.execute(f"CREATE TABLE {qualified_stage} AS {qualified_target}")
            candidates = export_duckdb_connection_table_to_clickhouse(
                duckdb_connection=duckdb_connection,
                clickhouse_client=client,
                duckdb_schema=tables.DLT_DATASET_NAME,
                duckdb_table=duckdb_table,
                clickhouse_database=tables.SWEDEN_DATABASE,
                clickhouse_table=stage_table,
                columns=columns,
                truncate=False,
                log=log,
            )
            if candidates == 0:
                raise ValueError(
                    f"DuckDB table {tables.DLT_DATASET_NAME}.{duckdb_table} has 0 rows; "
                    f"refusing to publish {qualified_target}."
                )
            anti_join = sweden_company_source_anti_join_sql(
                qualified_stage=qualified_stage,
                qualified_target=qualified_target,
            )
            inserted = int(
                client.execute(f"SELECT count() AS new_versions {anti_join}")[0][0]
            )
            if inserted > 0:
                column_list = ", ".join(columns)
                selected = ", ".join(f"candidate.{column}" for column in columns)
                client.execute(
                    f"INSERT INTO {qualified_target} ({column_list})\n"
                    f"SELECT {selected} {anti_join}"
                )
            if log is not None:
                log(
                    "Published Sweden register source table: table=%s candidates=%d "
                    "inserted=%d",
                    qualified_target,
                    candidates,
                    inserted,
                )
            return SwedenSourceTablePublishResult(
                candidates=candidates,
                inserted=inserted,
            )
        except BaseException as exc:
            primary_error = exc
            raise
        finally:
            try:
                client.execute(f"DROP TABLE IF EXISTS {qualified_stage}")
            except Exception as cleanup_error:
                if primary_error is not None:
                    primary_error.add_note(
                        f"Failed to drop temporary table {qualified_stage}: "
                        f"{cleanup_error}"
                    )
                else:
                    raise
```

- [ ] **Step 4: Add the asset**

In `src/dagster_v3/defs/sweden_company/assets.py`, extend the import from `.clickhouse` (lines 13-19) to:

```python
from dagster_v3.defs.sweden_company.clickhouse import (
    export_sweden_company_clickhouse_companies,
    export_sweden_company_clickhouse_industries,
    publish_sweden_company_clickhouse_addresses,
    publish_sweden_company_industry_history,
    publish_sweden_company_profile_history,
    publish_sweden_company_source_table,
)
```

Insert the asset after `sweden_company_companies_clickhouse` (ends line 156), before `sweden_company_profile_history_clickhouse`:

```python
@dg.asset(
    deps=["sweden_company_normalized_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "scb"},
    pool=SWEDEN_COMPANY_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_SCB_COMPANIES_TABLE},
    description=(
        "Publishes the whole SCB register record per company to ClickHouse "
        "corpscout.se_scb_companies, inserting only the companies whose SCB payload "
        "hash changed."
    ),
)
def sweden_company_scb_companies_clickhouse(
    context: dg.AssetExecutionContext,
    sweden_company_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    with read_only_duckdb_connection(sweden_company_duckdb) as connection:
        result = publish_sweden_company_source_table(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            duckdb_table="scb_companies",
            clickhouse_table=tables.SCB_COMPANIES_TABLE_CH,
            columns=tables.SE_SCB_COMPANIES_EXPORT_COLUMNS,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            "table": tables.QUALIFIED_SCB_COMPANIES_TABLE,
            "candidates": result.candidates,
            "inserted": result.inserted,
        }
    )
```

Add `"sweden_company_scb_companies_clickhouse",` to the `sweden_company_refresh_job` selection (after `"sweden_company_companies_clickhouse",` at line 322) and `sweden_company_scb_companies_clickhouse,` to the `defs` assets list (after `sweden_company_companies_clickhouse,` at line 344).

- [ ] **Step 5: Add the leaf check**

In `src/dagster_v3/defs/common/clickhouse_checks.py`, in the `# sweden_company — weekly Mon` block, insert after the `sweden_company_companies_clickhouse` leaf (line 189):

```python
    ClickhouseLeaf(
        "sweden_company_scb_companies_clickhouse", ("se_scb_companies",), WEEKLY
    ),
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cd corpscout/services/dagster_v3 && WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_sweden_company_clickhouse.py tests/test_sweden_company_assets.py tests/test_clickhouse_leaf_checks.py tests/test_sweden_company_code_quality.py -q -p no:warnings`
Expected: PASS, no failures.

- [ ] **Step 7: Write the clickhouse-local integration test**

The unit test above runs against a fake client, so it proves the SQL is *built* correctly but not that ClickHouse *runs* it. Two claims need a real engine: that the anti-join re-inserts an A → B → A revert (the whole reason for `argMax`), and that a pre-1970 `Date32` value survives a round trip. Create `tests/test_sweden_company_source_tables_clickhouse_local.py`:

```python
"""The register source tables' change-only publish, executed on a real ClickHouse.

Two claims a fake client cannot settle:

1. The anti-join's right side is the CURRENT row per company. A company whose payload goes
   A -> B -> A must get its third row inserted; a whole-table anti-join would suppress it
   and ReplacingMergeTree(observed_at) would keep the stale B row as current.
2. registration_date is Date32, not 000257's Date. A pre-1970 registration date has to
   survive insertion and read-back unclamped -- Date starts at 1970-01-01.

The script applies the real migration DDL and the real anti-join text
(sweden_company_source_anti_join_sql), never a paraphrase.
"""

import subprocess
from pathlib import Path

import pytest

from dagster_v3.defs.sweden_company import tables
from dagster_v3.defs.sweden_company.clickhouse import (
    sweden_company_source_anti_join_sql,
)
from tests.test_se_company_person_clickhouse_local import _clickhouse_local_command

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATIONS = (
    "000373_corpscout_se_scb_companies.up.sql",
    "000374_corpscout_se_bolagsverket_companies.up.sql",
)
APPLIED_PREFIXES = ("CREATE DATABASE", "CREATE TABLE")


def _schema_statements() -> list[str]:
    statements: list[str] = []
    for name in MIGRATIONS:
        text = (MIGRATIONS_DIR / name).read_text(encoding="utf-8")
        for raw in text.split(";"):
            statement = "\n".join(
                line for line in raw.splitlines() if not line.strip().startswith("--")
            ).strip()
            if statement.upper().startswith(APPLIED_PREFIXES):
                statements.append(statement)
    return statements


def _run(script_statements: list[str]) -> list[str]:
    script = ";\n".join(script_statements) + ";\n"
    completed = subprocess.run(
        _clickhouse_local_command(),
        input=script,
        capture_output=True,
        text=True,
        timeout=900,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
    return [line for line in completed.stdout.splitlines() if line.strip()]


def _scb_row(payload_hash: str, legal_name: str, observed_at: str) -> str:
    """One se_scb_companies row: only legal_name, source_payload_hash and observed_at vary."""
    return (
        "('5560000000', '5560000000', "
        f"'{legal_name}', NULL, '49', '1', '1', toDate32('1962-03-04'), "
        "'62010', NULL, NULL, NULL, NULL, "
        "NULL, 'Main Street 1', '11122', 'STOCKHOLM', '1', "
        f"'run-1', '5560000000', '{payload_hash}', "
        f"toDateTime64('{observed_at}', 3, 'UTC'))"
    )


def _publish(stage_rows: str) -> list[str]:
    """Stage the given rows and run the real anti-join insert against the real target."""
    columns = ", ".join(tables.SE_SCB_COMPANIES_EXPORT_COLUMNS)
    anti_join = sweden_company_source_anti_join_sql(
        qualified_stage="corpscout.stage_scb",
        qualified_target=tables.QUALIFIED_SCB_COMPANIES_TABLE,
    )
    selected = ", ".join(
        f"candidate.{column}" for column in tables.SE_SCB_COMPANIES_EXPORT_COLUMNS
    )
    return [
        "DROP TABLE IF EXISTS corpscout.stage_scb",
        f"CREATE TABLE corpscout.stage_scb AS {tables.QUALIFIED_SCB_COMPANIES_TABLE}",
        f"INSERT INTO corpscout.stage_scb ({columns}) VALUES {stage_rows}",
        f"SELECT count() AS new_versions {anti_join}",
        f"INSERT INTO {tables.QUALIFIED_SCB_COMPANIES_TABLE} ({columns})\n"
        f"SELECT {selected} {anti_join}",
    ]


def test_the_anti_join_reinserts_a_reverted_payload_and_skips_an_unchanged_one() -> None:
    output = _run(
        [
            *_schema_statements(),
            # Publish A, then B, then A again, then B again unchanged.
            *_publish(_scb_row("hash-a", "Acme AB", "2026-09-01 06:00:00")),
            *_publish(_scb_row("hash-b", "Acme Bolag AB", "2026-09-02 06:00:00")),
            *_publish(_scb_row("hash-a", "Acme AB", "2026-09-03 06:00:00")),
            *_publish(_scb_row("hash-a", "Acme AB", "2026-09-04 06:00:00")),
            f"SELECT count() FROM {tables.QUALIFIED_SCB_COMPANIES_TABLE}",
            "SELECT legal_name, toString(source_payload_hash) "
            f"FROM {tables.QUALIFIED_SCB_COMPANIES_TABLE} FINAL",
        ]
    )

    # The four anti-join counts, then the row count, then the current row.
    assert output[:4] == ["1", "1", "1", "0"]
    assert output[4] == "3"
    assert output[5] == "Acme AB\thash-a"


def test_a_pre_1970_registration_date_survives_the_round_trip() -> None:
    output = _run(
        [
            *_schema_statements(),
            *_publish(_scb_row("hash-a", "Acme AB", "2026-09-01 06:00:00")),
            "SELECT toString(registration_date) "
            f"FROM {tables.QUALIFIED_SCB_COMPANIES_TABLE} FINAL",
            "SELECT toString(registration_date), toString(deregistration_date) "
            f"FROM {tables.QUALIFIED_BOLAGSVERKET_COMPANIES_TABLE} FINAL",
        ]
    )

    assert output[0] == "1"  # the anti-join count
    assert output[1] == "1962-03-04"
    # The Bolagsverket table exists and is empty -- its own publish is Task 4's.
    assert output[2:] == []
```

- [ ] **Step 8: Run the integration test**

Run: `cd corpscout/services/dagster_v3 && uv run --frozen --no-sync pytest tests/test_sweden_company_source_tables_clickhouse_local.py -q -p no:warnings -m integration`
Expected: PASS (2 passed). If Docker is unavailable the harness calls `pytest.skip("docker is installed but not running")` or `pytest.skip("no clickhouse-local binary and no docker to run one")` — a skip is acceptable here only if the same command is re-run before Task 7's production step.

- [ ] **Step 9: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/clickhouse.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/assets.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/common/clickhouse_checks.py \
        corpscout/services/dagster_v3/tests/test_sweden_company_clickhouse.py \
        corpscout/services/dagster_v3/tests/test_sweden_company_assets.py \
        corpscout/services/dagster_v3/tests/test_clickhouse_leaf_checks.py \
        corpscout/services/dagster_v3/tests/test_sweden_company_source_tables_clickhouse_local.py
git commit -m "$(cat <<'EOF'
feat(dagster): publish se_scb_companies from the Sweden register load

sweden_company_scb_companies_clickhouse stages the normalised SCB record and
copies only the companies whose source_payload_hash differs from their current
published row. The anti-join reads argMax(source_payload_hash, observed_at)
per company rather than the whole table, so an A -> B -> A revert is published
instead of being suppressed under ReplacingMergeTree(observed_at).

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
EOF
)"
```

---

### Task 4: `sweden_company_bolagsverket_companies_clickhouse`

**Files:**
- Modify: `src/dagster_v3/defs/sweden_company/assets.py` (one asset, job selection, `defs`)
- Modify: `src/dagster_v3/defs/common/clickhouse_checks.py` (one leaf)
- Test: `tests/test_sweden_company_clickhouse.py`, `tests/test_sweden_company_assets.py`, `tests/test_clickhouse_leaf_checks.py`, `tests/test_sweden_company_source_tables_clickhouse_local.py`

**Interfaces:**
- Consumes: `publish_sweden_company_source_table(*, duckdb_connection, clickhouse, duckdb_table, clickhouse_table, columns, log=None) -> SwedenSourceTablePublishResult` and `sweden_company_source_anti_join_sql(*, qualified_stage, qualified_target) -> str` (Task 3); `tables.BOLAGSVERKET_COMPANIES_TABLE_CH`, `tables.QUALIFIED_BOLAGSVERKET_COMPANIES_TABLE`, `tables.SE_BOLAGSVERKET_COMPANIES_EXPORT_COLUMNS` (Task 1); DuckDB table `sweden_company.bolagsverket_companies` (Task 2).
- Produces: Dagster asset `sweden_company_bolagsverket_companies_clickhouse`, group `sweden_company`, pool `sweden_company_duckdb`, member of `sweden_company_refresh_job`.

- [ ] **Step 1: Write the failing tests**

Add the DuckDB fixture table to `_sweden_company_duckdb` in `tests/test_sweden_company_clickhouse.py`, after the `scb_companies` insert and before `yield connection`:

```python
        connection.execute(
            f"""
            create table {tables.DLT_DATASET_NAME}.bolagsverket_companies (
                company_id varchar,
                company_id_raw varchar,
                name_protection_sequence varchar,
                registration_country_code varchar,
                legal_name varchar,
                legal_name_raw varchar,
                legal_form_code varchar,
                registration_date date,
                deregistration_date date,
                deregistration_reason varchar,
                proceedings_raw varchar,
                activity_description varchar,
                postal_address varchar,
                source_run_id varchar,
                source_record_id varchar,
                source_payload_hash varchar,
                observed_at timestamp
            )
            """
        )
        connection.execute(
            f"""
            insert into {tables.DLT_DATASET_NAME}.bolagsverket_companies
            values (
                '5560000000', '5560000000$ORGNR-IDORG', '1', 'SE-LAND',
                'Acme AB', 'Acme AB$FORETAGSNAMN-ORGNAM$2020-01-01', 'AB-ORGFO',
                '2020-01-01', null, null, null, 'Runs acme.se',
                'Box 1$c/o CFO$STOCKHOLM$11122$SE-LAND',
                'run-1', '5560000000$ORGNR-IDORG', 'bolag-hash-1',
                '2026-09-03 06:00:00'
            )
            """
        )
```

Add the test, after `test_publish_sweden_company_source_table_inserts_only_changed_payloads`:

```python
def test_publish_sweden_company_source_table_publishes_the_bolagsverket_record(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """The same helper drives both register sources -- only the DuckDB table, the ClickHouse
    table and the column tuple differ, so a second wrapper would be pure forwarding."""
    client = FakeClickHouseClient()
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(self: ClickhouseResource) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    with _sweden_company_duckdb(tmp_path) as connection:
        result = publish_sweden_company_source_table(
            duckdb_connection=connection,
            clickhouse=resource,
            duckdb_table="bolagsverket_companies",
            clickhouse_table=tables.BOLAGSVERKET_COMPANIES_TABLE_CH,
            columns=tables.SE_BOLAGSVERKET_COMPANIES_EXPORT_COLUMNS,
        )

    assert result.candidates == 1
    assert result.inserted == 1
    assert client.table_checks == [(tables.BOLAGSVERKET_COMPANIES_TABLE_CH,)]
    target_inserts = [
        statement
        for statement in client.statements
        if statement.lstrip().startswith(
            f"INSERT INTO {tables.QUALIFIED_BOLAGSVERKET_COMPANIES_TABLE} ("
        )
    ]
    assert len(target_inserts) == 1
    assert "LEFT ANTI JOIN" in target_inserts[0]
    assert (
        ", ".join(tables.SE_BOLAGSVERKET_COMPANIES_EXPORT_COLUMNS) in target_inserts[0]
    )
```

Extend `tests/test_sweden_company_assets.py`'s `asset_keys` set with `"sweden_company_bolagsverket_companies_clickhouse",` and the pooled-exporter loop with the same key, and add `"sweden_company_bolagsverket_companies_clickhouse",` to the spot-check list in `tests/test_clickhouse_leaf_checks.py`.

Finally, replace the last assertion of `test_a_pre_1970_registration_date_survives_the_round_trip` in `tests/test_sweden_company_source_tables_clickhouse_local.py` so the Bolagsverket pair is exercised too. Add these helpers after `_publish`:

```python
def _bolagsverket_row(payload_hash: str, observed_at: str) -> str:
    """One se_bolagsverket_companies row with both dates before 1970."""
    return (
        "('5561111111', '5561111111$ORGNR-IDORG', '1', 'SE-LAND', "
        "'Closed AB', 'Closed AB$FORETAGSNAMN-ORGNAM$1955-01-01', 'AB-ORGFO', "
        "toDate32('1955-01-01'), toDate32('1968-12-31'), 'OVERK-AVORG', "
        "NULL, 'Closed activity', 'Closed Street 2$$GOTEBORG$41111$SE-LAND', "
        f"'run-1', '5561111111$ORGNR-IDORG', '{payload_hash}', "
        f"toDateTime64('{observed_at}', 3, 'UTC'))"
    )


def _publish_bolagsverket(stage_rows: str) -> list[str]:
    columns = ", ".join(tables.SE_BOLAGSVERKET_COMPANIES_EXPORT_COLUMNS)
    anti_join = sweden_company_source_anti_join_sql(
        qualified_stage="corpscout.stage_bolagsverket",
        qualified_target=tables.QUALIFIED_BOLAGSVERKET_COMPANIES_TABLE,
    )
    selected = ", ".join(
        f"candidate.{column}"
        for column in tables.SE_BOLAGSVERKET_COMPANIES_EXPORT_COLUMNS
    )
    return [
        "DROP TABLE IF EXISTS corpscout.stage_bolagsverket",
        "CREATE TABLE corpscout.stage_bolagsverket AS "
        f"{tables.QUALIFIED_BOLAGSVERKET_COMPANIES_TABLE}",
        f"INSERT INTO corpscout.stage_bolagsverket ({columns}) VALUES {stage_rows}",
        f"SELECT count() AS new_versions {anti_join}",
        f"INSERT INTO {tables.QUALIFIED_BOLAGSVERKET_COMPANIES_TABLE} ({columns})\n"
        f"SELECT {selected} {anti_join}",
    ]
```

and replace the body of `test_a_pre_1970_registration_date_survives_the_round_trip` with:

```python
def test_a_pre_1970_registration_date_survives_the_round_trip() -> None:
    output = _run(
        [
            *_schema_statements(),
            *_publish(_scb_row("hash-a", "Acme AB", "2026-09-01 06:00:00")),
            *_publish_bolagsverket(_bolagsverket_row("bv-hash-a", "2026-09-01 06:00:00")),
            "SELECT toString(registration_date) "
            f"FROM {tables.QUALIFIED_SCB_COMPANIES_TABLE} FINAL",
            "SELECT toString(registration_date), toString(deregistration_date) "
            f"FROM {tables.QUALIFIED_BOLAGSVERKET_COMPANIES_TABLE} FINAL",
        ]
    )

    assert output[0] == "1"  # the SCB anti-join count
    assert output[1] == "1"  # the Bolagsverket anti-join count
    assert output[2] == "1962-03-04"
    assert output[3] == "1955-01-01\t1968-12-31"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd corpscout/services/dagster_v3 && WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_sweden_company_clickhouse.py tests/test_sweden_company_assets.py tests/test_clickhouse_leaf_checks.py -q -p no:warnings`
Expected: FAIL — the asset and leaf tests fail on the missing `sweden_company_bolagsverket_companies_clickhouse` key.

- [ ] **Step 3: Add the asset**

In `src/dagster_v3/defs/sweden_company/assets.py`, insert immediately after `sweden_company_scb_companies_clickhouse`:

```python
@dg.asset(
    deps=["sweden_company_normalized_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "bolagsverket"},
    pool=SWEDEN_COMPANY_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_BOLAGSVERKET_COMPANIES_TABLE},
    description=(
        "Publishes the whole Bolagsverket register record per company to ClickHouse "
        "corpscout.se_bolagsverket_companies, inserting only the companies whose "
        "Bolagsverket payload hash changed."
    ),
)
def sweden_company_bolagsverket_companies_clickhouse(
    context: dg.AssetExecutionContext,
    sweden_company_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    with read_only_duckdb_connection(sweden_company_duckdb) as connection:
        result = publish_sweden_company_source_table(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            duckdb_table="bolagsverket_companies",
            clickhouse_table=tables.BOLAGSVERKET_COMPANIES_TABLE_CH,
            columns=tables.SE_BOLAGSVERKET_COMPANIES_EXPORT_COLUMNS,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            "table": tables.QUALIFIED_BOLAGSVERKET_COMPANIES_TABLE,
            "candidates": result.candidates,
            "inserted": result.inserted,
        }
    )
```

Add `"sweden_company_bolagsverket_companies_clickhouse",` to the `sweden_company_refresh_job` selection (after the SCB key) and `sweden_company_bolagsverket_companies_clickhouse,` to the `defs` assets list (after the SCB asset).

- [ ] **Step 4: Add the leaf check**

In `src/dagster_v3/defs/common/clickhouse_checks.py`, immediately after the SCB leaf:

```python
    ClickhouseLeaf(
        "sweden_company_bolagsverket_companies_clickhouse",
        ("se_bolagsverket_companies",),
        WEEKLY,
    ),
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd corpscout/services/dagster_v3 && WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_sweden_company_clickhouse.py tests/test_sweden_company_assets.py tests/test_clickhouse_leaf_checks.py -q -p no:warnings`
Expected: PASS, no failures.

- [ ] **Step 6: Re-run the integration harness**

Run: `cd corpscout/services/dagster_v3 && uv run --frozen --no-sync pytest tests/test_sweden_company_source_tables_clickhouse_local.py -q -p no:warnings -m integration`
Expected: PASS (2 passed).

- [ ] **Step 7: Check the definitions load**

Run: `cd corpscout/services/dagster_v3 && WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync dg check defs`
Expected: `All components validated successfully.` / a success line, exit code 0.

- [ ] **Step 8: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/assets.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/common/clickhouse_checks.py \
        corpscout/services/dagster_v3/tests/test_sweden_company_clickhouse.py \
        corpscout/services/dagster_v3/tests/test_sweden_company_assets.py \
        corpscout/services/dagster_v3/tests/test_clickhouse_leaf_checks.py \
        corpscout/services/dagster_v3/tests/test_sweden_company_source_tables_clickhouse_local.py
git commit -m "$(cat <<'EOF'
feat(dagster): publish se_bolagsverket_companies from the Sweden register load

sweden_company_bolagsverket_companies_clickhouse publishes the whole
Bolagsverket record per company through the same change-only helper as the SCB
export, and joins the weekly register job and the leaf-check registry beside it.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
EOF
)"
```

---

### Task 5: The profile-history asset stops writing the registry tables

**Files:**
- Modify: `src/dagster_v3/defs/sweden_company/clickhouse.py:71-121` (delete `SwedenProfilePublishResult`, rewrite `publish_sweden_company_profile_history`)
- Modify: `src/dagster_v3/defs/sweden_company/tables.py` (delete lines 12-13, 24-29 and `SE_COMPANY_REGISTRY_OBSERVATION_COLUMNS` at lines 163-190)
- Modify: `src/dagster_v3/defs/sweden_company/assets.py:159-200` (asset metadata)
- Modify: `src/dagster_v3/defs/sweden_company/docs/sweden_company-design.md`, `docs/sweden-data-sources.md`
- Test: `tests/test_sweden_company_clickhouse.py`, `tests/test_sweden_company_profile_history.py`

**Interfaces:**
- Consumes: nothing new.
- Produces: `publish_sweden_company_profile_history(*, duckdb_connection, clickhouse, log=None) -> SnapshotPublishResult` (was `-> SwedenProfilePublishResult`). `SwedenProfilePublishResult` no longer exists. `tables` no longer exposes any `*REGISTRY*` name.

- [ ] **Step 1: Write the failing tests**

Rewrite `test_publish_sweden_company_profile_history_tracks_changes_and_removals` in `tests/test_sweden_company_clickhouse.py`:

```python
def test_publish_sweden_company_profile_history_publishes_proceedings_only(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """The registry half retired with the 2026-09-03 basic-info design: the two register
    sources each have their own source table now. Proceedings keep the observation +
    _current shape until the proceedings entity's own slice."""
    client = FakeClickHouseClient()
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(self: ClickhouseResource) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    with _sweden_company_duckdb(tmp_path) as connection:
        result = publish_sweden_company_profile_history(
            duckdb_connection=connection,
            clickhouse=resource,
        )

    assert result.candidates == 1
    assert result.observations_inserted == 2
    assert result.first_observations == 1
    assert result.changes == 0
    assert result.removals == 1
    assert client.table_checks == [
        (
            tables.COMPANY_PROCEEDING_OBSERVATIONS_TABLE_CH,
            tables.COMPANY_PROCEEDINGS_CURRENT_TABLE_CH,
        )
    ]
    removal_inserts = [
        statement
        for statement in client.statements
        if statement.lstrip().startswith("INSERT INTO") and "toUInt8(0)" in statement
    ]
    assert len(removal_inserts) == 1
    assert "candidate.has_observation = 0" in removal_inserts[0]
    assert not [
        statement for statement in client.statements if "registry" in statement
    ]
```

Rewrite `tests/test_sweden_company_profile_history.py` entirely:

```python
from pathlib import Path

from dagster_v3.defs.sweden_company import tables


def test_sweden_profile_history_migration_owns_history_and_current_snapshots() -> None:
    """000257's file is unchanged: it still declares the registry pair, which stays on the
    server until migration 000375 retires it. What changed is that nothing in Dagster
    writes that pair any more (2026-09-03 SE basic-info design, section 3.1)."""
    migration = (
        Path(__file__).resolve().parents[3]
        / "clickhouse"
        / "migrations"
        / "000257_corpscout_se_company_profile_history.up.sql"
    ).read_text(encoding="utf-8")

    for table_name in (
        "se_company_registry_observations",
        "se_company_registry_current",
        "se_company_proceeding_observations",
        "se_company_proceedings_current",
        "se_company_industry_observations",
        "se_company_industry_current",
    ):
        assert f"CREATE TABLE IF NOT EXISTS corpscout.{table_name}" in migration

    assert migration.count("PARTITION BY toYear(observed_at)") == 3
    assert migration.count("has_observation UInt8 DEFAULT 1") == 3
    for columns in (
        tables.SE_COMPANY_PROCEEDING_OBSERVATION_COLUMNS,
        tables.SE_COMPANY_INDUSTRY_OBSERVATION_COLUMNS,
    ):
        assert all(f"\n    {column} " in migration for column in columns)


def test_the_sweden_module_no_longer_names_the_retired_registry_tables() -> None:
    """The register sources are published by se_scb_companies / se_bolagsverket_companies.
    Leaving the old constants behind would invite a reader to believe something still
    writes them."""
    # COMPANY_REGISTRY is the stem every retired constant shared; the unrelated
    # WIKIDATA_REGISTRY_SEED_SPEC stays.
    assert [name for name in dir(tables) if "COMPANY_REGISTRY" in name] == []
    assert tables.SCB_COMPANIES_TABLE_CH == "se_scb_companies"
    assert tables.BOLAGSVERKET_COMPANIES_TABLE_CH == "se_bolagsverket_companies"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd corpscout/services/dagster_v3 && uv run --frozen --no-sync pytest tests/test_sweden_company_clickhouse.py tests/test_sweden_company_profile_history.py -q -p no:warnings`
Expected: FAIL — `AttributeError: 'SwedenProfilePublishResult' object has no attribute 'candidates'` and `assert ['COMPANY_REGISTRY_CURRENT_TABLE_CH', ...] == []`.

- [ ] **Step 3: Rewrite the publish function**

In `src/dagster_v3/defs/sweden_company/clickhouse.py`, delete the `SwedenProfilePublishResult` dataclass (lines 71-75) and replace `publish_sweden_company_profile_history` (lines 77-121) with:

```python
def publish_sweden_company_profile_history(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> SnapshotPublishResult:
    """Publish typed Bolagsverket proceedings when they change.

    The source registry states this used to publish beside them
    (se_company_registry_observations / se_company_registry_current) retired with the
    2026-09-03 SE basic-info design: each register source now has its own source table,
    se_scb_companies and se_bolagsverket_companies. Proceedings keep the observation +
    physical-current shape until the proceedings entity's own slice.
    """
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.SWEDEN_DATABASE,
        tables=(
            tables.COMPANY_PROCEEDING_OBSERVATIONS_TABLE_CH,
            tables.COMPANY_PROCEEDINGS_CURRENT_TABLE_CH,
        ),
    )
    with clickhouse.get_connection() as client:
        return _publish_changed_snapshot(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_table="company_proceedings",
            history_table=tables.QUALIFIED_COMPANY_PROCEEDING_OBSERVATIONS_TABLE,
            current_table=tables.QUALIFIED_COMPANY_PROCEEDINGS_CURRENT_TABLE,
            current_table_name=tables.COMPANY_PROCEEDINGS_CURRENT_TABLE_CH,
            columns=tables.SE_COMPANY_PROCEEDING_OBSERVATION_COLUMNS,
            key_columns=("company_id", "source", "proceeding_identity"),
            state_fingerprint_column="proceeding_fingerprint",
            presence_column="has_proceeding",
            log=log,
        )
```

- [ ] **Step 4: Remove the registry constants from `tables.py`**

Delete these three blocks:
- lines 12-13, `COMPANY_REGISTRY_OBSERVATIONS_TABLE_CH` and `COMPANY_REGISTRY_CURRENT_TABLE_CH`
- lines 24-29, `QUALIFIED_COMPANY_REGISTRY_OBSERVATIONS_TABLE` and `QUALIFIED_COMPANY_REGISTRY_CURRENT_TABLE`
- lines 163-190, the whole `SE_COMPANY_REGISTRY_OBSERVATION_COLUMNS` tuple

- [ ] **Step 5: Retarget the asset**

In `src/dagster_v3/defs/sweden_company/assets.py`, replace the `sweden_company_profile_history_clickhouse` decorator metadata/description and its result block:

```python
@dg.asset(
    deps=["sweden_company_normalized_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "bolagsverket"},
    pool=SWEDEN_COMPANY_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_COMPANY_PROCEEDING_OBSERVATIONS_TABLE},
    description=(
        "Appends changed typed Bolagsverket proceeding observations to ClickHouse. The "
        "source registry states it also published retired with the 2026-09-03 basic-info "
        "design's source layer (se_scb_companies, se_bolagsverket_companies)."
    ),
)
def sweden_company_profile_history_clickhouse(
    context: dg.AssetExecutionContext,
    sweden_company_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    with read_only_duckdb_connection(sweden_company_duckdb) as connection:
        result = publish_sweden_company_profile_history(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            "proceeding_table": (
                tables.QUALIFIED_COMPANY_PROCEEDING_OBSERVATIONS_TABLE
            ),
            "proceeding_candidates": result.candidates,
            "proceeding_observations_inserted": result.observations_inserted,
            "proceeding_first_observations": result.first_observations,
            "proceeding_changes": result.changes,
            "proceeding_removals": result.removals,
        }
    )
```

- [ ] **Step 6: Update the module docs**

In `src/dagster_v3/defs/sweden_company/docs/sweden_company-design.md`:

Add a row to the normalized-DuckDB table listing, after the `company_registry_states` row:

```markdown
| `scb_companies` | one row per company with the whole SCB register record, in SCB's own organisation (source layer of the 2026-09-03 basic-info design) |
| `bolagsverket_companies` | one row per company with the whole Bolagsverket register record, in Bolagsverket's own organisation |
```

Replace the `sweden_company_profile_history_clickhouse` row of the asset table with these three rows:

```markdown
| `sweden_company_scb_companies_clickhouse` | `sweden_company.scb_companies` | `corpscout.se_scb_companies` | the whole SCB record per company, replaced only when its payload hash changes |
| `sweden_company_bolagsverket_companies_clickhouse` | `sweden_company.bolagsverket_companies` | `corpscout.se_bolagsverket_companies` | the whole Bolagsverket record per company, replaced only when its payload hash changes |
| `sweden_company_profile_history_clickhouse` | `sweden_company.company_proceedings` | `corpscout.se_company_proceeding_observations` | append-only proceeding history |
```

Replace the first bullet of the "The same split applies to company profiles and classifications" list — the `se_company_registry_observations` / `se_company_registry_current` bullet — with:

```markdown
- the register profile pair (`se_company_registry_observations` /
  `se_company_registry_current`) retired with the 2026-09-03 SE basic-info
  design: each register source now has one table of its own,
  `se_scb_companies` and `se_bolagsverket_companies`, both
  `ReplacingMergeTree(observed_at) ORDER BY company_id`, with no `_current`
  twin and no history table — the S3 snapshots are the archive;
```

In `docs/sweden-data-sources.md`, replace the `se_company_registry_observations` bullet (lines 60-63) with:

```markdown
- **`se_scb_companies`** — one row per company with the whole SCB register
  record in SCB's own organisation: raw `FtgStat` / `JEStat`, legal form, both
  name fields, registration date, the five SNI codes, the address columns and
  the marketing block. No derived status: interpreting SCB's codes belongs to
  the basic-info suggestion extractors.
- **`se_bolagsverket_companies`** — one row per company with the whole
  Bolagsverket register record: identity, name-protection sequence,
  registration country, both name forms, legal form, registration and
  deregistration dates with the deregistration reason, the pending-proceedings
  field, activity description and the packed postal address.
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `cd corpscout/services/dagster_v3 && WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_sweden_company_clickhouse.py tests/test_sweden_company_profile_history.py tests/test_sweden_company_assets.py tests/test_clickhouse_migrations.py -q -p no:warnings`
Expected: PASS, no failures. (`test_clickhouse_migrations.py` is included because `test_sweden_company_registry_migration_covers_exported_columns` imports `sweden_company_tables` and must not reference a constant this task deleted — it uses `SE_COMPANIES_EXPORT_COLUMNS`, `SE_COMPANY_ADDRESS_BASE_COLUMNS` and `SE_INDUSTRIES_EXPORT_COLUMNS` only.)

- [ ] **Step 8: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/clickhouse.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/tables.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/assets.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/docs/sweden_company-design.md \
        corpscout/services/dagster_v3/docs/sweden-data-sources.md \
        corpscout/services/dagster_v3/tests/test_sweden_company_clickhouse.py \
        corpscout/services/dagster_v3/tests/test_sweden_company_profile_history.py
git commit -m "$(cat <<'EOF'
refactor(dagster): stop writing the SE company registry observation tables

sweden_company_profile_history_clickhouse now publishes proceedings only; the
register profile pair it also wrote is superseded by the per-source tables.
The tables.py constants naming the retired pair are gone so nothing can quietly
start writing them again. The tables themselves stay on the server until the
gated retirement migration.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
EOF
)"
```

---

### Task 6: The domain-suggestions dbt model reads the union of the two source tables

**Files:**
- Modify: `src/dagster_v3/defs/company_domain_suggestions/dbt/models/sources.yml:8`
- Modify: `src/dagster_v3/defs/company_domain_suggestions/dbt/models/staging/stg_se_company_match_features.sql:16-48`
- Test: `tests/test_company_domain_suggestions_dbt.py:73-109`

**Interfaces:**
- Consumes: ClickHouse tables `corpscout.se_scb_companies` and `corpscout.se_bolagsverket_companies` (Task 1's DDL, Tasks 3-4's publishes).
- Produces: `stg_se_company_match_features` with an unchanged output contract — `country_iso2`, `company_id`, `company_name`, `feature_type`, `feature_subtype`, `normalized_value`, `raw_value`, `source_field` — and unchanged `source_field` values `scb.legal_name`, `scb.alternate_name`, `bolagsverket.legal_name`.

**How the dbt model is tested here:** `tests/test_company_domain_suggestions_dbt.py::test_company_domain_suggestion_dbt_project_parses` runs `dbtRunner().invoke(["parse", ...])` against the real project — a model referencing a source that `sources.yml` does not declare fails that parse. The sibling test in the same file pins the model's sources and feature vocabulary as text. There is no ClickHouse-executing test for this model; the union is validated on production in Task 7 by comparing row sets against the retiring table.

- [ ] **Step 1: Write the failing test**

Replace the source list and add two assertions in `test_sweden_company_match_features_are_normalized_and_technology_independent` (`tests/test_company_domain_suggestions_dbt.py:86-95`):

```python
    for source in (
        "se_companies",
        "se_scb_companies",
        "se_bolagsverket_companies",
        "se_company_addresses_current",
        "se_industries",
        "gleif_lei_records",
    ):
        assert f"source('corpscout', '{source}')" in model_sql

    # The retired registry projection is gone, and the two source_field values it produced
    # are still produced -- by a literal per union branch instead of by a `source` column.
    assert "se_company_registry_current" not in model_sql
    assert "'scb' AS source" in model_sql
    assert "'bolagsverket' AS source" in model_sql
    assert "concat(registry.source, '.legal_name')" in model_sql
    assert "concat(registry.source, '.alternate_name')" in model_sql
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd corpscout/services/dagster_v3 && uv run --frozen --no-sync pytest tests/test_company_domain_suggestions_dbt.py -q -p no:warnings`
Expected: FAIL — `assert "source('corpscout', 'se_scb_companies')" in model_sql`.

- [ ] **Step 3: Update `sources.yml`**

Replace line 8 (`      - name: se_company_registry_current`) with:

```yaml
      - name: se_scb_companies
      - name: se_bolagsverket_companies
```

- [ ] **Step 4: Rewrite the name-feature CTEs**

In `stg_se_company_match_features.sql`, replace the whole `company_name_values AS (...)` block (lines 16-48) with a `register_names` CTE followed by the rewritten `company_name_values`:

```sql
-- The two register source tables of the 2026-09-03 SE basic-info design replace the
-- retired registry projection. Each is ReplacingMergeTree(observed_at) ORDER BY
-- company_id, so FINAL gives the one current row per company; the retired table had a
-- has_company tombstone flag these do not need -- a row exists only while the source
-- delivers the company. Bolagsverket has no alternate name, exactly as before.
register_names AS (
    SELECT
        company_id,
        'scb' AS source,
        legal_name,
        alternate_name
    FROM {{ source('corpscout', 'se_scb_companies') }} FINAL

    UNION ALL

    SELECT
        company_id,
        'bolagsverket' AS source,
        legal_name,
        CAST(NULL AS Nullable(String)) AS alternate_name
    FROM {{ source('corpscout', 'se_bolagsverket_companies') }} FINAL
),

company_name_values AS (
    SELECT
        companies.company_id,
        companies.company_name,
        companies.company_name AS raw_value,
        'se_companies.legal_name' AS source_field
    FROM companies
    WHERE companies.company_name != ''

    UNION ALL

    SELECT
        companies.company_id,
        companies.company_name,
        ifNull(registry.legal_name, '') AS raw_value,
        concat(registry.source, '.legal_name') AS source_field
    FROM register_names AS registry
    INNER JOIN companies USING (company_id)
    WHERE ifNull(registry.legal_name, '') != ''

    UNION ALL

    SELECT
        companies.company_id,
        companies.company_name,
        ifNull(registry.alternate_name, '') AS raw_value,
        concat(registry.source, '.alternate_name') AS source_field
    FROM register_names AS registry
    INNER JOIN companies USING (company_id)
    WHERE ifNull(registry.alternate_name, '') != ''
),
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd corpscout/services/dagster_v3 && uv run --frozen --no-sync pytest tests/test_company_domain_suggestions_dbt.py -q -p no:warnings`
Expected: PASS, no failures — including `test_company_domain_suggestion_dbt_project_parses`, which is the `dbt parse` step for this change.

- [ ] **Step 6: Refresh the generated dbt state**

The dbt component reads a generated project from `src/dagster_v3/defs/.local_defs_state/`; runtime definition loads never regenerate it, and a deploy asserts it is present (`ansible/roles/dagster_dev/tasks/sync.yml:14-19`).

Run:
```bash
cd corpscout/services/dagster_v3
uv run --frozen --no-sync dg utils refresh-defs-state
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync dg check defs
```
Expected: the refresh completes without error and `dg check defs` exits 0. The regenerated state under `.local_defs_state/` is not committed (it is not tracked by git); it only has to exist before the Task 7 deploy.

- [ ] **Step 7: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/company_domain_suggestions/dbt/models/sources.yml \
        corpscout/services/dagster_v3/src/dagster_v3/defs/company_domain_suggestions/dbt/models/staging/stg_se_company_match_features.sql \
        corpscout/services/dagster_v3/tests/test_company_domain_suggestions_dbt.py
git commit -m "$(cat <<'EOF'
refactor(dagster): read SE name features from the register source tables

stg_se_company_match_features unions se_scb_companies and
se_bolagsverket_companies in place of se_company_registry_current, keeping the
same output columns and the same scb.legal_name / scb.alternate_name /
bolagsverket.legal_name source_field values. This is the last reader of the
retiring registry projection.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
EOF
)"
```

---

### Task 7: Production cutover and the gated retirement migration 000375

This task runs against the production ClickHouse and the production Dagster host. It is owner-gated: do not start it until Tasks 1-6 are merged and the whole suite is green.

**Why 000375 is written here and not earlier:** a `DROP TABLE` that has to wait for a deploy must never sit in the sequential ledger — a routine `make clickhouse-migrate-up` for unrelated work would apply it before the code that stopped reading the table is live. That is the 2026-08-25 ruling, pinned by `test_se_company_info_field_value_replaces_the_correction_ledger` and followed by the 000371 → 000372 pair, whose up file records the gate it verified immediately before applying. 000375 is authored at its apply step, with the measured gate in its comment.

**Files:**
- Create: `corpscout/clickhouse/migrations/000375_corpscout_retire_se_company_registry.up.sql`
- Create: `corpscout/clickhouse/migrations/000375_corpscout_retire_se_company_registry.down.sql`
- Modify: `tests/test_clickhouse_migrations.py` (one line in `EXPECTED_MIGRATIONS`, one test appended at the end)

**Interfaces:**
- Consumes: everything from Tasks 1-6, deployed.
- Produces: `corpscout.se_company_registry_observations` and `corpscout.se_company_registry_current` no longer exist on production.

- [ ] **Step 1: Apply migration 000373 alone**

The Makefile mounts `$(CURDIR)/clickhouse/migrations`, so run `make` against the checkout that holds the committed migrations (a worktree needs `corpscout/.env` copied in for `CLICKHOUSE_MIGRATE_URL`). Before applying, confirm the ledger and the directory agree: `ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT version, dirty FROM corpscout.schema_migrations ORDER BY sequence DESC LIMIT 1\""` must answer `372	0`, and `ls corpscout/clickhouse/migrations | tail -6` must list only the 000373/000374 pairs above 000372 (000375 does not exist yet).

```bash
make -C corpscout clickhouse-migrate-version
make -C corpscout clickhouse-migrate-up-one
make -C corpscout clickhouse-migrate-version
```
Expected: the first `version` prints `372` (clean), and the second prints `373` (clean). If it prints `dirty`, stop and repair forward — never rewind the shared ledger.

- [ ] **Step 2: Apply migration 000374 alone**

```bash
make -C corpscout clickhouse-migrate-up-one
make -C corpscout clickhouse-migrate-version
```
Expected: `374` (clean).

- [ ] **Step 3: Confirm both tables exist and are empty**

```sql
SELECT name, total_rows
FROM system.tables
WHERE database = 'corpscout'
  AND name IN ('se_scb_companies', 'se_bolagsverket_companies')
ORDER BY name;
```
Expected: two rows, `total_rows` 0 for both.

- [ ] **Step 4: Deploy the code**

Deploy from a pristine worktree of the merged commit (the `light_sync` playbook rsyncs the working tree with `--delete-after`, so a dirty tree would ship WIP). Migration ordering on deploy is **migrate → deploy code → materialize** (`docs/deployment-runbook.md:123`), which Steps 1-2 already satisfied. Every line must exit 0 before the next:

```bash
S=/private/tmp/claude-501/-Users-graovic-pulsarpoint-ppoint-companycollect/62b23c62-a06d-4dca-84a0-a4a0f3f72968/scratchpad/basic-info-0; mkdir -p $S
cd /Users/graovic/pulsarpoint/ppoint/companycollect && git worktree add $S/deploy-worktree <merged commit>
cp corpscout/services/dagster_v3/.env $S/deploy-worktree/corpscout/services/dagster_v3/.env
cd $S/deploy-worktree/corpscout/services/dagster_v3
uv sync --frozen
uv run --frozen --no-sync dbt parse --project-dir src/dagster_v3/defs/finland_ytj/dbt --profiles-dir src/dagster_v3/defs/finland_ytj/dbt
uv run --frozen --no-sync dbt parse --project-dir src/dagster_v3/defs/exchange_rates_v2/dbt --profiles-dir src/dagster_v3/defs/exchange_rates_v2/dbt
uv run --frozen --no-sync dg utils refresh-defs-state
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync dg check defs
cd ansible && ANSIBLE_BECOME_TIMEOUT=60 ansible-playbook -i inventory.ini light_sync.yml > $S/light_sync.log 2>&1; RC=$?; echo "RC=$RC"; tail -15 $S/light_sync.log
```
Expected: `dg check defs` exits 0 (the refresh regenerates `src/dagster_v3/defs/.local_defs_state/…/manifest.json`, which the playbook asserts exists); `RC=0` with a `PLAY RECAP` for `dagster` showing `failed=0`. Never judge the playbook by `| tail` alone — `tail` masks the exit code. Remove the deploy worktree afterwards (`git worktree remove $S/deploy-worktree && git worktree prune`). The Sweden register schedule keeps running throughout; nothing is stopped for this deploy.

- [ ] **Step 5: Materialize the two new exports on the production Dagster host**

Launch from the Dagster UI, never `dagster job backfill` (a CLI launch tags runs with a mismatched code-location name and orphans them). Select these three asset keys explicitly and materialize them together — `dg launch --assets +leaf` resolves only one hop in this build, so an implicit upstream selection would silently skip the rebuild:

- `sweden_company_normalized_duckdb`
- `sweden_company_scb_companies_clickhouse`
- `sweden_company_bolagsverket_companies_clickhouse`

Expected: all three succeed. The two export assets report `candidates` equal to their source's distinct company count and `inserted` equal to `candidates` (first publish, nothing to anti-join against).

- [ ] **Step 6: Verify the row counts against the retiring table (gate 1 of 3)**

```sql
SELECT
    (SELECT uniqExact(company_id) FROM corpscout.se_company_registry_current
     WHERE source = 'scb' AND has_company = 1)          AS registry_scb,
    (SELECT uniqExact(company_id) FROM corpscout.se_scb_companies)
                                                        AS new_scb,
    (SELECT uniqExact(company_id) FROM corpscout.se_company_registry_current
     WHERE source = 'bolagsverket' AND has_company = 1)  AS registry_bolagsverket,
    (SELECT uniqExact(company_id) FROM corpscout.se_bolagsverket_companies)
                                                        AS new_bolagsverket;
```
Gate: `new_scb = registry_scb` and `new_bolagsverket = registry_bolagsverket`. Record the four numbers — they go into 000375's comment. If they differ, stop: the DuckDB builders and `company_registry_states` use the same identity normalisation and the same rank-1 dedup, so a difference means one of them changed and must be explained before anything is dropped.

- [ ] **Step 7: Verify the name features are identical (gate 2 of 3)**

```sql
SELECT
    (SELECT count() FROM (
        SELECT company_id, legal_name FROM corpscout.se_company_registry_current
        WHERE source = 'scb' AND has_company = 1 AND ifNull(legal_name, '') != ''
        EXCEPT
        SELECT company_id, legal_name FROM corpscout.se_scb_companies FINAL
        WHERE ifNull(legal_name, '') != ''
    )) AS scb_legal_name_lost,
    (SELECT count() FROM (
        SELECT company_id, alternate_name FROM corpscout.se_company_registry_current
        WHERE source = 'scb' AND has_company = 1 AND ifNull(alternate_name, '') != ''
        EXCEPT
        SELECT company_id, alternate_name FROM corpscout.se_scb_companies FINAL
        WHERE ifNull(alternate_name, '') != ''
    )) AS scb_alternate_name_lost,
    (SELECT count() FROM (
        SELECT company_id, legal_name FROM corpscout.se_company_registry_current
        WHERE source = 'bolagsverket' AND has_company = 1
          AND ifNull(legal_name, '') != ''
        EXCEPT
        SELECT company_id, legal_name FROM corpscout.se_bolagsverket_companies FINAL
        WHERE ifNull(legal_name, '') != ''
    )) AS bolagsverket_legal_name_lost;
```
Gate: all three are 0. These are exactly the three feature streams `stg_se_company_match_features` builds, so a zero here means the dbt model's output cannot shrink.

- [ ] **Step 8: Verify the Date32 columns hold pre-1970 values**

```sql
SELECT
    min(registration_date),
    countIf(registration_date < toDate32('1970-01-01')) AS before_1970
FROM corpscout.se_bolagsverket_companies;
```
Expected: `min` is a real early-20th-century date (1900-01-01 or later, see below) and `before_1970` is greater than 0. If `min` is `1970-01-01` and `before_1970` is 0, the column was clamped — stop and investigate the type before dropping anything.

Then confirm no date was fabricated by the driver. `Date32` cannot hold dates before 1900-01-01, and the native driver writes such a value as `1970-01-01` (not NULL, not an error), which is why the DuckDB builders NULL any date outside Date32's range (final-review finding I1). Compare the DuckDB file the export read with the published table:

```bash
ssh dagster "cd /opt/companycollect/corpscout/dagster_v3 && sudo -n .venv/bin/python -c \"import duckdb; c = duckdb.connect('data/sweden_company_source.duckdb', read_only=True); print(c.sql(\\\"select count(*) filter (where registration_date is null) as null_dates, count(*) filter (where registration_date = date '1970-01-01') as epoch_dates from sweden_company.bolagsverket_companies\\\").fetchall())\""
```
```sql
SELECT countIf(registration_date IS NULL) AS null_dates,
       countIf(registration_date = toDate32('1970-01-01')) AS epoch_dates
FROM corpscout.se_bolagsverket_companies;
```
Gate: both pairs are equal. A ClickHouse `epoch_dates` above DuckDB's means the driver fabricated dates — stop before dropping anything.

- [ ] **Step 9: Rebuild the domain-suggestions model (gate 3 of 3)**

From the Dagster UI, materialize the `stg_se_company_match_features` asset for partition `SE` (group `company_domain_suggestions_dbt`). It is a view, so dbt recreates it against the new sources.

Then confirm the view actually reads the new tables and returns the same feature volume:
```sql
SELECT count(), uniqExact(company_id)
FROM corpscout.stg_se_company_match_features
WHERE feature_type = 'name';
```
Gate: the run succeeds and the count is at least the value the same query returned before the deploy (record it before Step 4 if you want an exact comparison). Also confirm nothing else reads the retiring pair:
```sql
SELECT count()
FROM system.query_log
WHERE event_time > now() - INTERVAL 7 DAY
  AND type = 'QueryFinish'
  AND (hasAny(tables, ['corpscout.se_company_registry_current'])
       OR hasAny(tables, ['corpscout.se_company_registry_observations']));
```
Expected: 0 once the deploy and the rebuild are done. A non-zero count from before the deploy is expected; re-run the query with `event_time > <deploy timestamp>` and require 0.

- [ ] **Step 10: Write `000375_corpscout_retire_se_company_registry.up.sql`**

Substitute the four numbers measured in Step 6 into the comment.

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- Retires the SE company registry profile pair, replaced by the per-source tables
-- se_scb_companies (000373) and se_bolagsverket_companies (000374) of the 2026-09-03 SE
-- basic-info design. Written at the apply step, after ClickHouse and Dagster were already
-- running the code that no longer reads this pair, per the 2026-08-25 ruling that a DROP
-- which has to wait for a deploy never sits in the ledger.
--
-- Gate verified immediately before apply, on production:
--   se_scb_companies holds <NEW_SCB> companies against se_company_registry_current's
--   <REGISTRY_SCB> for source scb, and se_bolagsverket_companies <NEW_BOLAGSVERKET>
--   against <REGISTRY_BOLAGSVERKET>
--   the three EXCEPT checks on legal_name and alternate_name each returned 0 rows lost
--   stg_se_company_match_features was rebuilt on the union of the two new tables
--   query_log showed no read of either retired table after the deploy
-- UNDROP window is roughly 480 s after apply.
DROP TABLE IF EXISTS corpscout.se_company_registry_observations;

DROP TABLE IF EXISTS corpscout.se_company_registry_current;
```

- [ ] **Step 11: Write `000375_corpscout_retire_se_company_registry.down.sql`**

The down file restores 000257's shape exactly — schema only, never the rows.

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- Recreates the retired pair empty, in its 000257 shape. This restores the schema only,
-- not the observations -- the S3 snapshots and the per-source tables are the archive.
CREATE TABLE IF NOT EXISTS corpscout.se_company_registry_observations
(
    company_id String,
    source LowCardinality(String),
    company_id_raw Nullable(String),
    legal_name Nullable(String),
    legal_name_raw Nullable(String),
    alternate_name Nullable(String),
    legal_form_code LowCardinality(Nullable(String)),
    source_status_code LowCardinality(Nullable(String)),
    source_secondary_status_code LowCardinality(Nullable(String)),
    derived_status LowCardinality(Nullable(String)),
    status_reason LowCardinality(Nullable(String)),
    incorporation_date Nullable(Date),
    dissolution_date Nullable(Date),
    activity_description Nullable(String),
    name_protection_sequence Nullable(String),
    registration_country_code LowCardinality(Nullable(String)),
    marketing_block_code LowCardinality(Nullable(String)),
    proceedings_raw Nullable(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash String,
    source_record_uid String DEFAULT lower(hex(SHA256(concat(
        'company-source-record-v1\nstructured\n',
        if(source = 'bolagsverket', 'sweden_bolagsverket', 'sweden_scb'),
        '\nregistry_company\n', source_record_id, '\n', lowerUTF8(source_payload_hash)
    )))),
    updated_from_raw_at DateTime64(3, 'UTC'),
    has_company UInt8,
    state_fingerprint FixedString(64),
    observation_fingerprint FixedString(64),
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
PARTITION BY toYear(observed_at)
ORDER BY (company_id, source, observed_at, observation_fingerprint);

CREATE TABLE IF NOT EXISTS corpscout.se_company_registry_current
(
    company_id String,
    source LowCardinality(String),
    company_id_raw Nullable(String),
    legal_name Nullable(String),
    legal_name_raw Nullable(String),
    alternate_name Nullable(String),
    legal_form_code LowCardinality(Nullable(String)),
    source_status_code LowCardinality(Nullable(String)),
    source_secondary_status_code LowCardinality(Nullable(String)),
    derived_status LowCardinality(Nullable(String)),
    status_reason LowCardinality(Nullable(String)),
    incorporation_date Nullable(Date),
    dissolution_date Nullable(Date),
    activity_description Nullable(String),
    name_protection_sequence Nullable(String),
    registration_country_code LowCardinality(Nullable(String)),
    marketing_block_code LowCardinality(Nullable(String)),
    proceedings_raw Nullable(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash String,
    source_record_uid String DEFAULT lower(hex(SHA256(concat(
        'company-source-record-v1\nstructured\n',
        if(source = 'bolagsverket', 'sweden_bolagsverket', 'sweden_scb'),
        '\nregistry_company\n', source_record_id, '\n', lowerUTF8(source_payload_hash)
    )))),
    updated_from_raw_at DateTime64(3, 'UTC'),
    has_company UInt8,
    state_fingerprint FixedString(64),
    observation_fingerprint FixedString(64),
    observed_at DateTime64(3, 'UTC'),
    has_observation UInt8 DEFAULT 1
)
ENGINE = MergeTree
ORDER BY (company_id, source);
```

- [ ] **Step 12: Write the ledger test**

Add `"000375_corpscout_retire_se_company_registry",` to `EXPECTED_MIGRATIONS` after the 000374 entry, and append this test to `tests/test_clickhouse_migrations.py`:

```python
def test_the_se_registry_pair_is_retired_by_000375_and_by_nothing_else() -> None:
    """The DROP lives in its own entry, written at the apply step: 000373 and 000374 only
    create the replacements. The down file restores 000257's shape -- schema only, since
    the per-source tables and the S3 snapshots are the archive."""
    up = _migration_sql("000375_corpscout_retire_se_company_registry.up.sql")
    down = _migration_sql("000375_corpscout_retire_se_company_registry.down.sql")

    assert "DROP TABLE IF EXISTS corpscout.se_company_registry_observations" in up
    assert "DROP TABLE IF EXISTS corpscout.se_company_registry_current" in up
    assert up.count("DROP TABLE") == 2
    # Narrow: the proceeding and industry pairs 000257 also created stay, until their own
    # entities' slices retire them.
    assert "se_company_proceeding" not in up
    assert "se_company_industry" not in up
    assert "se_scb_companies" not in up.split("DROP TABLE")[1]
    assert "se_bolagsverket_companies" not in up.split("DROP TABLE")[1]

    # The two creating migrations of this slice drop nothing at all.
    for name in (
        "000373_corpscout_se_scb_companies",
        "000374_corpscout_se_bolagsverket_companies",
    ):
        assert "DROP TABLE" not in _migration_sql(f"{name}.up.sql"), name

    for table in (
        "se_company_registry_observations",
        "se_company_registry_current",
    ):
        assert f"CREATE TABLE IF NOT EXISTS corpscout.{table}\n" in down
    assert "ENGINE = ReplacingMergeTree(observed_at)" in down
    assert "ORDER BY (company_id, source, observed_at, observation_fingerprint)" in down
    assert "ORDER BY (company_id, source)" in down
```

- [ ] **Step 13: Run the ledger tests**

Run: `cd corpscout/services/dagster_v3 && uv run --frozen --no-sync pytest tests/test_clickhouse_migrations.py tests/test_sweden_company_source_tables.py -q -p no:warnings`
Expected: PASS, no failures.

- [ ] **Step 14: Apply migration 000375**

Commit Step 15 first, or run `make` from the checkout where the 000375 files exist, because the Makefile mounts the working tree's migrations directory.

```bash
make -C corpscout clickhouse-migrate-up-one
make -C corpscout clickhouse-migrate-version
```
Expected: `375` (clean). Confirm the pair is gone:
```sql
SELECT count() FROM system.tables
WHERE database = 'corpscout'
  AND name IN ('se_company_registry_observations', 'se_company_registry_current');
```
Expected: 0. The UNDROP window is roughly 480 s — if anything looks wrong, `UNDROP TABLE corpscout.se_company_registry_current` within it.

- [ ] **Step 15: Commit**

```bash
git add corpscout/clickhouse/migrations/000375_corpscout_retire_se_company_registry.up.sql \
        corpscout/clickhouse/migrations/000375_corpscout_retire_se_company_registry.down.sql \
        corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py
git commit -m "$(cat <<'EOF'
chore(dagster): retire se_company_registry_observations and _current

Migration 000375, written at its apply step with the verified gate in its
comment: both per-source tables hold the same company sets the registry
projection did, the three name-feature EXCEPT checks lost nothing, and
stg_se_company_match_features was rebuilt on their union before the drop.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
EOF
)"
```

---

### Task 8: Whole-suite verification and handoff to slice 1

**Files:**
- Modify: `docs/superpowers/specs/2026-09-03-se-company-basic-info-design.md` (a short status line under section 10's slice 0)

**Interfaces:**
- Consumes: everything above.
- Produces: the handoff contract slice 1's extractors read.

- [ ] **Step 1: Run the full unit suite**

Run: `cd corpscout/services/dagster_v3 && WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests -q -p no:warnings -m "not integration"`
Expected: PASS, no failures, no errors. Any failure outside the files this plan touched is a pre-existing condition — record it, do not paper over it.

- [ ] **Step 2: Run the integration harness**

Run: `cd corpscout/services/dagster_v3 && uv run --frozen --no-sync pytest tests/test_sweden_company_source_tables_clickhouse_local.py -q -p no:warnings -m integration`
Expected: PASS (2 passed), or a skip if Docker is unavailable on this machine.

- [ ] **Step 3: Check the definitions load**

Run: `cd corpscout/services/dagster_v3 && WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync dg check defs`
Expected: exit code 0.

- [ ] **Step 4: Confirm nothing in the tree still names the retired tables**

Run: `rg -n "se_company_registry_observations|se_company_registry_current" --glob '!**/node_modules/**' corpscout/services/dagster_v3/src corpscout/services/dagster_v3/tests corpscout/services/backoffice 2>/dev/null`
Expected: exactly two hits, both in `tests/test_sweden_company_profile_history.py`, which pins that migration 000257's *file* still declares them (that file is unchanged by design; 000375 removes the tables from the server, not the historical migration text). No hit in `src/`, none in the backoffice.

- [ ] **Step 5: Record the slice-0 handoff in the spec**

In `docs/superpowers/specs/2026-09-03-se-company-basic-info-design.md`, under section 10's slice 0 entry, append this line (adjusting the date to the day Task 7 was applied):

```markdown
   Done <YYYY-MM-DD>: migrations 000373/000374 applied, both assets materialized on prod,
   the domain-suggestions model rebuilt on their union, and the registry pair dropped by
   000375. Slice 1's `se_basic_info_suggestions_scb` reads
   `corpscout.se_scb_companies` and `se_basic_info_suggestions_bolagsverket` reads
   `corpscout.se_bolagsverket_companies`, both one row per company, both
   `ReplacingMergeTree(observed_at) ORDER BY company_id` — read them with `FINAL` or
   `argMax(..., observed_at)`. Neither carries a `source_record_uid`: the extractor
   computes the suggestion's `source_record_uid` itself, from `source_record_id` and
   `source_payload_hash`, the way migration 000257's DEFAULT expression did
   (`lower(hex(SHA256(concat('company-source-record-v1\nstructured\n', <'sweden_scb'|'sweden_bolagsverket'>, '\nregistry_company\n', source_record_id, '\n', lowerUTF8(source_payload_hash)))))`).
   The extractors do the interpreting: SCB's `source_status_code` (`FtgStat`) becomes
   `status`, Bolagsverket's `deregistration_date` implies its own, and
   `registration_date` becomes `incorporation_date`. Neither source table has, or will
   get, a `derived_status`.
```

- [ ] **Step 6: Commit**

```bash
git add corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-03-se-company-basic-info-design.md
git commit -m "$(cat <<'EOF'
docs(dagster): record the SE basic-info slice-0 handoff to the extractors

Names the exact contract slice 1 reads: the two per-source tables, how to take
their current row, how to derive the source_record_uid they deliberately do not
store, and which interpretations belong to the extractors rather than to the
source layer.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
EOF
)"
```

---

## Self-review

### 1. Spec coverage — section 3.1, section 6 (slice 0), section 10 (slice 0), section 11

| Spec requirement (verbatim or paraphrased) | Where it is implemented |
| --- | --- |
| `se_scb_companies`: one row per company, whole SCB record | Task 1 (DDL), Task 2 (`_replace_scb_companies_table`), Task 3 (publish) |
| SCB identifier, company name, legal-form code, the two SCB status codes, registration date, five SNI columns, address columns, marketing-block flag | Task 1 Step 4 column list; Task 2 Step 3 SELECT; Task 1 Step 2 pins the SNI and address columns explicitly |
| English column names where the name is a plain rename | Design decision 2, applied in Task 1's DDL and Task 2's SELECTs |
| SCB's own codes untouched, no derived status | Task 1 Step 1 (`assert "derived_status" not in up`), Task 1 Step 2 (`source_status_code`/`source_secondary_status_code` pins) |
| The `m*` previous-value twins are dropped | Task 1 Step 1 (`for twin in (...)` assertions); Task 2 never selects them |
| `se_bolagsverket_companies`: identity, name-protection sequence, registration country, name, legal form, deregistration date and reason, pending-proceedings field, registration date, activity description, postal address | Task 1 Step 6 column list (all ten present); Task 2 Step 3 second builder |
| Both `ReplacingMergeTree(observed_at) ORDER BY company_id` | Task 1 Steps 4 and 6; pinned by Task 1 Steps 1 and 2 |
| Replaced only when the source record changes | Task 3 Step 3 `publish_sweden_company_source_table` + `sweden_company_source_anti_join_sql`; proven on a real engine in Task 3 Step 7 |
| Provenance `source_run_id`, `source_record_id`, `source_payload_hash` | Task 1 DDL; pinned by `test_neither_source_table_carries_provenance_it_does_not_need` |
| `CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')` | Task 1 Steps 4 and 6; pinned in Task 1 Step 1 |
| No history table — S3 is the archive | No `*_current` twin and no observation table is created anywhere; Task 3's docstring records why `_publish_changed_snapshot` was not reused |
| Written by the existing register loads from the normalised DuckDB layer | Task 2 builds inside `replace_sweden_company_normalized_tables`; Tasks 3-4 add assets to the existing `sweden_company_refresh_job` on its existing weekly schedule |
| Section 6: `sweden_company_scb_companies_clickhouse`, `sweden_company_bolagsverket_companies_clickhouse` on the register job's existing cadence | Task 3 Step 4, Task 4 Step 3 (both added to `sweden_company_refresh_job`, which `sweden_company_refresh_weekly` drives) |
| Section 6: `sweden_company_profile_history_clickhouse` stops writing the registry observation tables and keeps writing proceedings | Task 5 |
| Section 10 slice 0: the domain-suggestions dbt model moved to their union | Task 6 |
| Section 10 slice 0: registry observation tables retired by gated drop migrations after the new tables are filled on prod | Task 7 (gate measured in Steps 6-9, migration written in Steps 10-11, applied in Step 14) |
| Section 11: the four names `se_scb_companies`, `se_bolagsverket_companies`, `sweden_company_scb_companies_clickhouse`, `sweden_company_bolagsverket_companies_clickhouse` | Task 1 (tables), Tasks 3-4 (assets) — used verbatim, never abbreviated |

Gaps found and closed while reviewing: the spec's Bolagsverket list says "name", which the code splits into `legal_name` and `legal_name_raw`; both are published (Task 1 Step 6), because the domain-suggestions model and the slice-1 name extractor need the parsed one and the packed one is the audit trail. The spec's SCB list says "company name" singular; both SCB name fields are published, because dropping `Foretagsnamn` would silently delete the `scb.alternate_name` feature stream `stg_se_company_match_features` produces today (design decision 3).

Deliberately out of scope, as the brief requires: `se_companies` and `companies_current_asset.py` are untouched; addresses, industries, proceedings, the backoffice and the basic-info entity tables are untouched.

### 2. Placeholder scan

Searched the plan for `TBD`, `TODO`, `implement later`, `fill in`, `add appropriate`, `handle edge cases`, `similar to Task`, and for code steps without code blocks. None found. Two intentional non-placeholders:

- Task 1's **Verify at execution** line gives the exact command and the exact expected output that decides whether the migration numbers still hold — the one fact that can change between writing and executing this plan.
- Task 7 Step 10's `<NEW_SCB>` / `<REGISTRY_SCB>` / `<NEW_BOLAGSVERKET>` / `<REGISTRY_BOLAGSVERKET>` and Task 8 Step 5's `<YYYY-MM-DD>` are values *measured on production* two steps earlier, in the same task, by a query the plan spells out. They are recordings, not decisions deferred to the reader. The 000371/000372 pair records its gate the same way.

### 3. Type consistency

- `tables.SCB_COMPANIES_TABLE_CH` / `BOLAGSVERKET_COMPANIES_TABLE_CH` / `QUALIFIED_SCB_COMPANIES_TABLE` / `QUALIFIED_BOLAGSVERKET_COMPANIES_TABLE` / `SE_SCB_COMPANIES_EXPORT_COLUMNS` / `SE_BOLAGSVERKET_COMPANIES_EXPORT_COLUMNS`: defined in Task 1 Steps 8-9, spelled identically in Tasks 2, 3, 4, 6, 7 and the tests.
- `publish_sweden_company_source_table(*, duckdb_connection, clickhouse, duckdb_table, clickhouse_table, columns, log=None)`: signature defined in Task 3 Step 3, called with exactly those keywords in Task 3 Step 4, Task 4 Step 3 and both unit tests.
- `sweden_company_source_anti_join_sql(*, qualified_stage, qualified_target)`: defined in Task 3 Step 3, called with those keywords in Task 3 Step 3 (inside the helper), Task 3 Step 7 and Task 4 Step 1's harness helpers.
- `SwedenSourceTablePublishResult(candidates, inserted)`: the two field names are used in the assets' metadata dicts and in both unit tests; nothing reads a third field.
- `publish_sweden_company_profile_history` returns `SnapshotPublishResult` after Task 5; the asset in Task 5 Step 5 reads `result.candidates` / `observations_inserted` / `first_observations` / `changes` / `removals`, which are that dataclass's five fields (`clickhouse.py:62-68`). `SwedenProfilePublishResult` is deleted and referenced nowhere afterwards.
- DuckDB table names `scb_companies` / `bolagsverket_companies` are string literals in three places each (the builder SQL, the counts dict, the asset call) and match across Tasks 2, 3 and 4.
- Column tuple length and order: 22 SCB names and 17 Bolagsverket names, in one order, asserted three ways — against the migration DDL (`declared_columns`, Task 1), against the DuckDB tables (`information_schema.columns`, Task 2), and against the generated INSERT column list (Tasks 3-4).
- Date types: `Nullable(Date32)` in both migrations, `::date` in DuckDB (which the Arrow/row insert path already handles for the existing `Nullable(Date)` registry columns), `toDate32(...)` literals in the clickhouse-local harness. Consistent.
