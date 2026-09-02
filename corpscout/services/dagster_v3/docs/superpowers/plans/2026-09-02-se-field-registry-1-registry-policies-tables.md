# SE Field Registry, Part 1: Registry, Policies and Tables Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the `info` field registry package (`dagster_v3.defs.se_company.fields`), its `source_precedence` policy, the SQL renderers that turn the registry into one resolve statement per field plus one wide-projection statement, the registry export asset, and the three migrations (long tables, new wide columns, widened decision CHECKs).

**Architecture:** The registry is code (frozen dataclasses validated at import). `sql.py` renders it into ClickHouse statements with server-side `{name:Type}` parameters; `export.py` writes the registry and every rendered statement into `corpscout.se_company_field_registry` (ReplacingMergeTree(version), read with `argMax(..., version)` like `se_code_labels`). Reviewer decisions in `se_company_info_field_value` win by construction inside the generated SQL; the policy only orders candidates. Nothing in this plan extracts candidates, runs a bulk resolve, or touches the backoffice -- those are sibling plans that import the exact names fixed here.

**Tech Stack:** Python 3.14 / uv, Dagster 1.13.9, clickhouse-driver 0.2.10, ClickHouse 26.5 (golang-migrate ledger; clickhouse-local via `docker run clickhouse/clickhouse-server:26.5` for the harness), pytest.

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-02-se-company-field-registry-design.md` -- sections 1-4, 5.1, 6, 7, 8.1, 8.3, 12, 14 are this plan's; binding authority for every name and value below.

## Global Constraints

- Python 3.14 project managed by uv. Every command runs from `corpscout/services/dagster_v3` as `uv run --frozen --no-sync <cmd>`. Tests: `uv run --frozen --no-sync pytest <file> -q -p no:warnings`.
- Dagster assets are autoloaded from `src/dagster_v3/defs/` by `dg.load_from_defs_folder` (`src/dagster_v3/definitions.py:13`). A module registers its asset with a module-level `defs = dg.Definitions(assets=[...])` exactly as `defs/se_company/info.py:979-980` does. No `from __future__ import annotations` in a module that defines an asset.
- Definitions-loading tests and `dg check defs` need `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix` in the environment.
- ClickHouse 26.5: plain MergeTree does not support `FINAL`; ReplacingMergeTree does. `ORDER BY` cannot contain Nullable columns. `argMax(arg, key)` SKIPS rows whose `arg` is NULL (verified on 26.5: `argMax(a, k)` over `('old', 1), (NULL, 2)` returns `old`); wrap the arg in a tuple to keep NULL rows.
- Migrations (`corpscout/clickhouse/migrations/`): first line `CREATE DATABASE IF NOT EXISTS corpscout;`; the file's last non-blank line ends with `;` (a statement, never a comment); no `;` inside `--` comments; no `TRUNCATE TABLE`, no `DROP TABLE` in an up file; every up has a down; names added to `EXPECTED_MIGRATIONS` in `tests/test_clickhouse_migrations.py`. Numbers are `000373`, `000374`, `000375` -- before Task 1 run `ls corpscout/clickhouse/migrations | tail -2` and confirm the last entry is `000372_corpscout_retire_se_company_info_correction.up.sql`; if the ledger moved, renumber all three consistently everywhere in this plan.
- Fixed interfaces other plans import -- never rename: package `dagster_v3.defs.se_company.fields` with modules `__init__`, `tables`, `registry`, `policies`, `sql`, `export`; every constant and function named in a task's **Interfaces / Produces** block; registry version `se-info-v1`; policy `source_precedence` / `source_precedence-v1`; asset `se_company_field_registry_clickhouse` in group `se_company_fields`.
- Commit by explicit path (the tree carries unrelated WIP: a playwright log and `codex-sd-examples/` -- leave them alone, never `git add -A`). Conventional Commits with these trailers, each on its own line, after a blank line:
  `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`
  `Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5`

## File map

| Path (relative to `corpscout/services/dagster_v3` unless noted) | Responsibility |
| --- | --- |
| `../../clickhouse/migrations/000373_corpscout_se_company_field_tables.{up,down}.sql` | registry, candidate, resolved tables + INSERT grants |
| `../../clickhouse/migrations/000374_corpscout_se_company_info_field_columns.{up,down}.sql` | eight new wide columns on `se_company_info` |
| `../../clickhouse/migrations/000375_corpscout_se_company_info_field_value_registry_checks.{up,down}.sql` | widened `known_field` / `known_source` CHECKs |
| `src/dagster_v3/defs/se_company/fields/__init__.py` | empty package marker |
| `src/dagster_v3/defs/se_company/fields/tables.py` | qualified table names + positional column tuples |
| `src/dagster_v3/defs/se_company/fields/registry.py` | `FieldSpec`, `DatatypeRegistry`, vocabularies, `INFO_REGISTRY`, validation, fingerprint |
| `src/dagster_v3/defs/se_company/fields/policies.py` | `FieldPolicy` protocol, `SourcePrecedence`, `POLICIES`, `policy_for` |
| `src/dagster_v3/defs/se_company/fields/sql.py` | `array_literal`, `sql_string`, `rank_sql`, `render_resolve_sql`, `render_projection_select_sql`, `render_projection_sql` |
| `src/dagster_v3/defs/se_company/fields/export.py` | `registry_rows`, asset `se_company_field_registry_clickhouse` |
| `tests/test_se_company_field_registry.py`, `..._policies.py`, `..._sql.py`, `..._export.py` | new tests |
| `tests/test_clickhouse_migrations.py`, `tests/test_se_company_layout.py`, `tests/test_se_company_info.py`, `tests/test_se_company_info_clickhouse_local.py` | existing tests extended / retargeted |

---

### Task 1: Migration 000373 and `fields/tables.py`

**Files:**
- Create: `corpscout/clickhouse/migrations/000373_corpscout_se_company_field_tables.up.sql`
- Create: `corpscout/clickhouse/migrations/000373_corpscout_se_company_field_tables.down.sql`
- Create: `src/dagster_v3/defs/se_company/fields/__init__.py` (empty file)
- Create: `src/dagster_v3/defs/se_company/fields/tables.py`
- Modify: `tests/test_clickhouse_migrations.py:386-388` (append two lines to `EXPECTED_MIGRATIONS`) and append one test after `test_the_correction_ledger_is_retired_by_000372_reversibly` (ends near line 4068)
- Modify: `tests/test_se_company_layout.py` (append two tests at the end of the file)

**Interfaces:**
- Consumes: `tests/se_company_ddl.py` helpers `declared_columns(table)`, `table_block(table)`, `MIGRATIONS_DIR`; the `_statements(sql)` helper already defined in `tests/test_se_company_layout.py`.
- Produces (fixed names):
  - `SE_COMPANY_FIELD_REGISTRY = "corpscout.se_company_field_registry"`, `SE_COMPANY_FIELD_CANDIDATE = "corpscout.se_company_field_candidate"`, `SE_COMPANY_FIELD = "corpscout.se_company_field"`, `SE_COMPANY_INFO = "corpscout.se_company_info"`, `SE_COMPANY_INFO_FIELD_VALUE = "corpscout.se_company_info_field_value"`
  - `SE_COMPANY_FIELD_CANDIDATE_COLUMNS: tuple[str, ...]` (10 names, DDL order, MATERIALIZED `evidence_hash` omitted), `SE_COMPANY_FIELD_COLUMNS` (15 names), `SE_COMPANY_FIELD_REGISTRY_COLUMNS` (13 names).
  - Tables: registry (spec 4.3, `sources Array(String)`, no ranks column), candidate (spec 5.1 verbatim), resolved (spec 8.1 plus `has_company` and `has_value` CHECKs), `GRANT INSERT` on candidate, resolved and `corpscout.se_company_info` to `corpscout_person_correction_writer`.

- [ ] **Step 1: Write the failing migration test**

Append to `tests/test_clickhouse_migrations.py` (after `test_the_correction_ledger_is_retired_by_000372_reversibly`):

```python
def test_se_company_field_tables_are_created_by_000373() -> None:
    """2026-09-02 field registry design (spec 4.3, 5.1, 8.1): the exported registry, the
    long append-only candidates and the long resolved table. Additive only -- nothing is
    removed -- and the writer role gains INSERT on the two long tables the backoffice
    resolve writes plus the wide projection it re-pivots afterwards."""
    up = _migration_sql("000373_corpscout_se_company_field_tables.up.sql")
    down = _migration_sql("000373_corpscout_se_company_field_tables.down.sql")

    for table in ("se_company_field_registry", "se_company_field_candidate", "se_company_field"):
        assert f"CREATE TABLE IF NOT EXISTS corpscout.{table}\n" in up
        assert f"DROP TABLE IF EXISTS corpscout.{table}" in down
    assert "ENGINE = ReplacingMergeTree(version)\nORDER BY (datatype, country, field)" in up
    assert "ENGINE = ReplacingMergeTree(extracted_at)\nORDER BY (company_id, field, source, source_record_uid)" in up
    assert "ENGINE = ReplacingMergeTree(resolved_at)\nORDER BY (company_id, field)" in up
    assert "sources Array(String)" in up and "ranks" not in up
    assert "DROP TABLE" not in up and "TRUNCATE" not in up

    for table in ("se_company_field_candidate", "se_company_field", "se_company_info"):
        assert f"GRANT INSERT ON corpscout.{table}\nTO corpscout_person_correction_writer" in up
        assert f"REVOKE INSERT ON corpscout.{table}\nFROM corpscout_person_correction_writer" in down
    assert "GRANT SELECT" not in up and "GRANT ALL" not in up
```

And extend `EXPECTED_MIGRATIONS` (after `"000372_corpscout_retire_se_company_info_correction",`):

```python
    "000373_corpscout_se_company_field_tables",
```

- [ ] **Step 2: Write the failing layout tests**

Append to `tests/test_se_company_layout.py`:

```python
def test_field_tables_from_000373_match_the_positional_column_tuples() -> None:
    """2026-09-02 registry design: one long candidates table, one long resolved table, one
    registry export table. The Python tuples are the positional insert lists (MATERIALIZED
    evidence_hash omitted), pinned to the migration the way INSERT_COLUMNS is pinned."""
    from dagster_v3.defs.se_company.fields.tables import (
        SE_COMPANY_FIELD_CANDIDATE_COLUMNS,
        SE_COMPANY_FIELD_COLUMNS,
        SE_COMPANY_FIELD_REGISTRY_COLUMNS,
    )

    candidate = table_block("se_company_field_candidate")
    assert [c for c in declared_columns("se_company_field_candidate") if c != "evidence_hash"] == list(
        SE_COMPANY_FIELD_CANDIDATE_COLUMNS
    )
    assert "evidence_hash FixedString(64) MATERIALIZED lower(hex(SHA256(concat(" in candidate
    assert "field, '\\n', source, '\\n', source_record_uid, '\\n', value, '\\n', value_json" in candidate
    assert "ENGINE = ReplacingMergeTree(extracted_at)" in candidate
    assert "ORDER BY (company_id, field, source, source_record_uid)" in candidate
    assert "CONSTRAINT has_company CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')" in candidate
    assert "CONSTRAINT has_value CHECK trim(value) != ''" in candidate

    resolved = table_block("se_company_field")
    assert declared_columns("se_company_field") == list(SE_COMPANY_FIELD_COLUMNS)
    assert "decision_id Nullable(UUID)" in resolved
    assert "ENGINE = ReplacingMergeTree(resolved_at)" in resolved and "ORDER BY (company_id, field)" in resolved
    assert "CONSTRAINT has_company CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')" in resolved
    assert "CONSTRAINT has_value CHECK trim(value) != ''" in resolved

    registry = table_block("se_company_field_registry")
    assert declared_columns("se_company_field_registry") == list(SE_COMPANY_FIELD_REGISTRY_COLUMNS)
    assert "sources Array(String)" in registry and "resolve_sql String" in registry
    assert "ENGINE = ReplacingMergeTree(version)" in registry and "ORDER BY (datatype, country, field)" in registry


def test_field_table_writer_grants_are_insert_only() -> None:
    up = (MIGRATIONS_DIR / "000373_corpscout_se_company_field_tables.up.sql").read_text()
    down = (MIGRATIONS_DIR / "000373_corpscout_se_company_field_tables.down.sql").read_text()
    for table in ("se_company_field_candidate", "se_company_field", "se_company_info"):
        assert f"GRANT INSERT ON corpscout.{table}\nTO corpscout_person_correction_writer" in up
        assert f"REVOKE INSERT ON corpscout.{table}\nFROM corpscout_person_correction_writer" in down
    assert "GRANT SELECT" not in up and "GRANT ALL" not in up
    # The registry table is read by both runners and written by Dagster alone.
    assert "GRANT INSERT ON corpscout.se_company_field_registry" not in up
    assert "DROP" not in _statements(up) and "TRUNCATE" not in _statements(up)
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `uv run --frozen --no-sync pytest tests/test_clickhouse_migrations.py::test_se_company_field_tables_are_created_by_000373 tests/test_clickhouse_migrations.py::test_clickhouse_migration_files_are_explicit tests/test_se_company_layout.py -q -p no:warnings`
Expected: FAIL -- `FileNotFoundError` for the 000373 file and `ModuleNotFoundError: dagster_v3.defs.se_company.fields`.

- [ ] **Step 4: Write the up migration**

`corpscout/clickhouse/migrations/000373_corpscout_se_company_field_tables.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- 2026-09-02 field registry design. The hand-written publisher of se_company_info gives
-- way to a registry-driven model in three long tables:
--   se_company_field_registry  -- the registry exported from dagster_v3 (one row per
--                                 field plus a field = '*' row carrying the wide
--                                 projection statement), ReplacingMergeTree(version)
--                                 read with argMax(..., version) like se_code_labels
--   se_company_field_candidate -- every source's value for every field, append-only,
--                                 one row per (company, field, source, source record)
--   se_company_field           -- one resolved row per (company, field)
-- Reviewer decisions stay in se_company_info_field_value (000371). Additive only.

CREATE TABLE IF NOT EXISTS corpscout.se_company_field_registry
(
    datatype LowCardinality(String),
    country LowCardinality(String),
    field String,
    value_type LowCardinality(String),
    display_group LowCardinality(String),
    structured Bool,
    python_only Bool,
    sources Array(String),
    policy_name LowCardinality(String),
    policy_version String,
    resolve_sql String,
    registry_version String,
    version DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (datatype, country, field);

-- value is the display form and is never empty: an absent value is no row. value_json
-- carries compare_key (the normalised form agreement is counted on) and the field's
-- structured members. A re-extraction of the same source record replaces its row
-- (ReplacingMergeTree on extracted_at), a new source record adds one, nothing is deleted.
CREATE TABLE IF NOT EXISTS corpscout.se_company_field_candidate
(
    company_id String,
    field LowCardinality(String),
    source LowCardinality(String),
    source_record_uid String,
    value String,
    value_json String DEFAULT '',
    observed_at DateTime64(3, 'UTC'),
    extracted_at DateTime64(3, 'UTC'),
    extractor_version LowCardinality(String),
    source_run_id String,
    evidence_hash FixedString(64) MATERIALIZED lower(hex(SHA256(concat(
        field, '\n', source, '\n', source_record_uid, '\n', value, '\n', value_json)))),

    CONSTRAINT has_company CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT has_value CHECK trim(value) != ''
)
ENGINE = ReplacingMergeTree(extracted_at)
ORDER BY (company_id, field, source, source_record_uid);

-- decision_id is set when a reviewer decision supplied the value. candidate_count and
-- agreeing_sources describe the eligible candidates the policy saw for that company.
CREATE TABLE IF NOT EXISTS corpscout.se_company_field
(
    company_id String,
    field LowCardinality(String),
    value String,
    value_json String DEFAULT '',
    source LowCardinality(String),
    source_record_uid String,
    observed_at DateTime64(3, 'UTC'),
    decision_id Nullable(UUID),
    policy_name LowCardinality(String),
    policy_version String,
    candidate_count UInt16,
    agreeing_sources Array(String),
    registry_version String,
    source_run_id String,
    resolved_at DateTime64(3, 'UTC'),

    CONSTRAINT has_company CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT has_value CHECK trim(value) != ''
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (company_id, field);

-- The backoffice resolves one company right after a decision: it runs the field's
-- statement (writes se_company_field) and the projection statement (writes
-- se_company_info). INSERT only, like every other grant to this role.
GRANT INSERT ON corpscout.se_company_field_candidate
TO corpscout_person_correction_writer;

GRANT INSERT ON corpscout.se_company_field
TO corpscout_person_correction_writer;

GRANT INSERT ON corpscout.se_company_info
TO corpscout_person_correction_writer;
```

- [ ] **Step 5: Write the down migration**

`corpscout/clickhouse/migrations/000373_corpscout_se_company_field_tables.down.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

REVOKE INSERT ON corpscout.se_company_info
FROM corpscout_person_correction_writer;

REVOKE INSERT ON corpscout.se_company_field
FROM corpscout_person_correction_writer;

REVOKE INSERT ON corpscout.se_company_field_candidate
FROM corpscout_person_correction_writer;

DROP TABLE IF EXISTS corpscout.se_company_field;

DROP TABLE IF EXISTS corpscout.se_company_field_candidate;

DROP TABLE IF EXISTS corpscout.se_company_field_registry;
```

- [ ] **Step 6: Write the package marker and `tables.py`**

Create `src/dagster_v3/defs/se_company/fields/__init__.py` as an empty file (the sibling `defs/se_company/__init__.py` is empty too).

`src/dagster_v3/defs/se_company/fields/tables.py`:

```python
"""Table names and positional column lists for the SE field registry layer.

The migration owns the schema (000373 creates the three long tables, 000374 widens the
projection). These tuples are the insert lists code binds to, pinned to the DDL by
tests/test_se_company_layout.py so a column added anywhere else fails loudly instead of
silently shifting values.
"""

SE_COMPANY_FIELD_REGISTRY = "corpscout.se_company_field_registry"
SE_COMPANY_FIELD_CANDIDATE = "corpscout.se_company_field_candidate"
SE_COMPANY_FIELD = "corpscout.se_company_field"
SE_COMPANY_INFO = "corpscout.se_company_info"
SE_COMPANY_INFO_FIELD_VALUE = "corpscout.se_company_info_field_value"

# corpscout.se_company_field_candidate in DDL order; evidence_hash is MATERIALIZED and omitted.
SE_COMPANY_FIELD_CANDIDATE_COLUMNS = (
    "company_id", "field", "source", "source_record_uid", "value", "value_json",
    "observed_at", "extracted_at", "extractor_version", "source_run_id",
)
# corpscout.se_company_field in DDL order.
SE_COMPANY_FIELD_COLUMNS = (
    "company_id", "field", "value", "value_json", "source", "source_record_uid", "observed_at",
    "decision_id", "policy_name", "policy_version", "candidate_count", "agreeing_sources",
    "registry_version", "source_run_id", "resolved_at",
)
# corpscout.se_company_field_registry in DDL order.
SE_COMPANY_FIELD_REGISTRY_COLUMNS = (
    "datatype", "country", "field", "value_type", "display_group", "structured", "python_only",
    "sources", "policy_name", "policy_version", "resolve_sql", "registry_version", "version",
)
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `uv run --frozen --no-sync pytest tests/test_clickhouse_migrations.py tests/test_se_company_layout.py -q -p no:warnings`
Expected: PASS (the whole migration ledger suite, including `test_every_migration_ends_with_a_statement_not_a_comment` and `test_clickhouse_migration_line_comments_do_not_contain_semicolons`).

- [ ] **Step 8: Commit**

```bash
git add corpscout/clickhouse/migrations/000373_corpscout_se_company_field_tables.up.sql \
        corpscout/clickhouse/migrations/000373_corpscout_se_company_field_tables.down.sql \
        corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/fields/__init__.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/fields/tables.py \
        corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py \
        corpscout/services/dagster_v3/tests/test_se_company_layout.py
git commit -m "feat(clickhouse): se_company_field registry, candidate and resolved tables (000373)

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---

### Task 2: Migration 000374 -- the registry scalars on `se_company_info`

**Files:**
- Create: `corpscout/clickhouse/migrations/000374_corpscout_se_company_info_field_columns.up.sql`
- Create: `corpscout/clickhouse/migrations/000374_corpscout_se_company_info_field_columns.down.sql`
- Modify: `src/dagster_v3/defs/se_company/fields/tables.py` (append two tuples)
- Modify: `tests/test_clickhouse_migrations.py` (`EXPECTED_MIGRATIONS` + one test)
- Modify: `tests/test_se_company_layout.py` (append one test)
- Modify: `tests/test_se_company_info.py:803-809` (`test_insert_columns_match_the_migration_in_order`)
- Modify: `tests/test_se_company_info_clickhouse_local.py:118` (`MIGRATIONS` tuple gains 000374 right after the 000371 entry)

**Interfaces:**
- Consumes: `declared_columns`, `FINAL_PROVENANCE`, `_statements` from the layout test module; `tests/se_company_ddl.py:_column_changes` replays `ADD COLUMN ... AFTER x` clauses one per line (see its docstring) -- the migration below is written in that format on purpose.
- Produces: `SE_COMPANY_INFO_REGISTRY_COLUMNS` (the 8 new columns, DDL order) and `SE_COMPANY_INFO_COLUMNS` (every insertable `se_company_info` column in DDL order, `evidence_set_hash` omitted) in `tables.py`. `render_projection_sql` (Task 6) inserts in `SE_COMPANY_INFO_COLUMNS` order.
- Column placement: all eight land between `primary_sni_code` and `wikidata_id`, so `FINAL_PROVENANCE` stays the tail (`tests/se_company_ddl.py:9-10`) and the old publisher's `INSERT_COLUMNS` (an explicit list) keeps working -- the new columns take their defaults there.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_clickhouse_migrations.py` and add `"000374_corpscout_se_company_info_field_columns",` after the 000373 entry in `EXPECTED_MIGRATIONS`:

```python
def test_se_company_info_gains_the_registry_scalars_in_000374() -> None:
    """Spec 8.3: the wide projection keeps every existing column and gains the scalars
    the registry resolves beyond the pilot's set, positioned before wikidata_id so the
    provenance tail stays last. One ALTER, each ADD positioned AFTER the one before it."""
    up = _migration_sql("000374_corpscout_se_company_info_field_columns.up.sql")
    down = _migration_sql("000374_corpscout_se_company_info_field_columns.down.sql")

    assert up.count("ALTER TABLE corpscout.se_company_info\n") == 1
    for clause in (
        "industry_label_en String DEFAULT '' AFTER primary_sni_code",
        "website Nullable(String) AFTER industry_label_en",
        "employee_count Nullable(UInt64) AFTER website",
        "employee_count_as_of Nullable(Date32) AFTER employee_count",
        "latest_revenue_amount Nullable(Decimal128(2)) AFTER employee_count_as_of",
        "latest_revenue_currency LowCardinality(String) DEFAULT '' AFTER latest_revenue_amount",
        "latest_revenue_amount_usd Nullable(Decimal128(2)) AFTER latest_revenue_currency",
        "latest_revenue_fiscal_year Nullable(UInt16) AFTER latest_revenue_amount_usd",
    ):
        assert f"ADD COLUMN IF NOT EXISTS {clause}" in up
        assert f"DROP COLUMN IF EXISTS {clause.split(' ')[0]}" in down
    assert "DROP TABLE" not in up and "TRUNCATE" not in up
```

Append to `tests/test_se_company_layout.py`:

```python
def test_the_projection_gains_the_registry_scalars_from_000374() -> None:
    """Spec 8.3: eight new wide columns between primary_sni_code and wikidata_id. The
    provenance tail is untouched, and SE_COMPANY_INFO_COLUMNS -- the list the registry
    projection inserts by -- is the deployed column list minus the MATERIALIZED hash."""
    from dagster_v3.defs.se_company.fields.tables import (
        SE_COMPANY_INFO_COLUMNS,
        SE_COMPANY_INFO_REGISTRY_COLUMNS,
    )

    up = (MIGRATIONS_DIR / "000374_corpscout_se_company_info_field_columns.up.sql").read_text()
    down = (MIGRATIONS_DIR / "000374_corpscout_se_company_info_field_columns.down.sql").read_text()

    columns = declared_columns("se_company_info")
    at = columns.index("primary_sni_code") + 1
    assert tuple(columns[at : at + 8]) == SE_COMPANY_INFO_REGISTRY_COLUMNS
    assert columns[at + 8] == "wikidata_id"
    assert tuple(columns[-len(FINAL_PROVENANCE):]) == FINAL_PROVENANCE
    assert [c for c in columns if c != "evidence_set_hash"] == list(SE_COMPANY_INFO_COLUMNS)

    assert up.count("ADD COLUMN IF NOT EXISTS") == 8 and down.count("DROP COLUMN IF EXISTS") == 8
    # Only the projection moves: the artifacts, the decisions and the long tables keep theirs.
    statements, down_statements = (_statements(text) for text in (up, down))
    assert statements.count("ALTER TABLE corpscout.se_company_info\n") == 1
    for other in ("se_company_info_scb", "se_company_info_field_value", "se_company_field"):
        assert other not in statements and other not in down_statements
```

Retarget `tests/test_se_company_info.py::test_insert_columns_match_the_migration_in_order` (replace the whole function):

```python
def test_insert_columns_match_the_migration_in_order() -> None:
    from dagster_v3.defs.se_company.fields.tables import SE_COMPANY_INFO_REGISTRY_COLUMNS
    from dagster_v3.defs.se_company.info import INSERT_COLUMNS
    from tests.se_company_ddl import declared_columns

    # 000374's registry-resolved scalars are filled by the registry projection only; the
    # retiring publisher inserts by this explicit list and leaves them to their defaults,
    # so they are the one gap allowed between the two lists.
    assert list(INSERT_COLUMNS) == [
        c for c in declared_columns("se_company_info")
        if c != "evidence_set_hash" and c not in SE_COMPANY_INFO_REGISTRY_COLUMNS
    ]
```

In `tests/test_se_company_info_clickhouse_local.py`, add to `MIGRATIONS` right after the `"000371_corpscout_se_company_info_field_value.up.sql",` entry:

```python
    # 000374 adds the registry-resolved scalars to se_company_info (industry label,
    # website, head count, latest revenue). INSERT_COLUMNS does not name them -- the
    # retiring publisher leaves them to their defaults -- but the `final_columns` section
    # compares system.columns with declared_columns(), which replays every up migration,
    # so the ALTER has to land here too.
    "000374_corpscout_se_company_info_field_columns.up.sql",
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --frozen --no-sync pytest tests/test_clickhouse_migrations.py::test_se_company_info_gains_the_registry_scalars_in_000374 tests/test_se_company_layout.py::test_the_projection_gains_the_registry_scalars_from_000374 tests/test_se_company_info.py::test_insert_columns_match_the_migration_in_order -q -p no:warnings`
Expected: FAIL -- `FileNotFoundError` / `ImportError: cannot import name 'SE_COMPANY_INFO_REGISTRY_COLUMNS'`.

- [ ] **Step 3: Write the migration**

`corpscout/clickhouse/migrations/000374_corpscout_se_company_info_field_columns.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- 2026-09-02 field registry design (spec 8.3): the wide projection keeps its name,
-- engine and every existing column so se_companies_serving and the backoffice loaders
-- survive the cutover, and gains the scalars the registry resolves beyond the pilot's
-- set -- the industry label and the derived scale figures (website, head count, latest
-- revenue with its fiscal year). They sit between primary_sni_code and wikidata_id so the
-- provenance tail the layout tests pin stays last, and each ADD COLUMN positions itself
-- AFTER the one before it in the same statement (000306 precedent -- ClickHouse applies
-- an ALTER's commands in order).
--
-- Nullable where "no value" is a fact (website, counts, amounts, the year), DEFAULT ''
-- for the two strings every consumer reads through ifNull. Rows written by the retiring
-- publisher read as NULL / '' until the registry-driven resolve rebuilds them. That
-- publisher inserts by its explicit INSERT_COLUMNS list, so the new columns take their
-- defaults there and nothing about it breaks.

ALTER TABLE corpscout.se_company_info
    ADD COLUMN IF NOT EXISTS industry_label_en String DEFAULT '' AFTER primary_sni_code,
    ADD COLUMN IF NOT EXISTS website Nullable(String) AFTER industry_label_en,
    ADD COLUMN IF NOT EXISTS employee_count Nullable(UInt64) AFTER website,
    ADD COLUMN IF NOT EXISTS employee_count_as_of Nullable(Date32) AFTER employee_count,
    ADD COLUMN IF NOT EXISTS latest_revenue_amount Nullable(Decimal128(2)) AFTER employee_count_as_of,
    ADD COLUMN IF NOT EXISTS latest_revenue_currency LowCardinality(String) DEFAULT '' AFTER latest_revenue_amount,
    ADD COLUMN IF NOT EXISTS latest_revenue_amount_usd Nullable(Decimal128(2)) AFTER latest_revenue_currency,
    ADD COLUMN IF NOT EXISTS latest_revenue_fiscal_year Nullable(UInt16) AFTER latest_revenue_amount_usd;
```

`corpscout/clickhouse/migrations/000374_corpscout_se_company_info_field_columns.down.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- No MATERIALIZED expression reads any of these (evidence_set_hash covers
-- evidence_hashes only), so they drop cleanly, newest first.

ALTER TABLE corpscout.se_company_info
    DROP COLUMN IF EXISTS latest_revenue_fiscal_year,
    DROP COLUMN IF EXISTS latest_revenue_amount_usd,
    DROP COLUMN IF EXISTS latest_revenue_currency,
    DROP COLUMN IF EXISTS latest_revenue_amount,
    DROP COLUMN IF EXISTS employee_count_as_of,
    DROP COLUMN IF EXISTS employee_count,
    DROP COLUMN IF EXISTS website,
    DROP COLUMN IF EXISTS industry_label_en;
```

- [ ] **Step 4: Append the column tuples to `tables.py`**

```python
# The eight columns 000374 adds to corpscout.se_company_info. Filled by the registry
# projection only; the retiring publisher leaves them to their defaults.
SE_COMPANY_INFO_REGISTRY_COLUMNS = (
    "industry_label_en", "website", "employee_count", "employee_count_as_of",
    "latest_revenue_amount", "latest_revenue_currency", "latest_revenue_amount_usd",
    "latest_revenue_fiscal_year",
)
# corpscout.se_company_info in DDL order after 000374; evidence_set_hash is MATERIALIZED
# and omitted. The projection statement (sql.py) inserts in exactly this order.
SE_COMPANY_INFO_COLUMNS = (
    "company_id", "legal_name", "legal_form_code", "legal_form_label_en", "legal_form_label_sv",
    "status", "incorporation_date", "description", "description_sv", "description_language",
    "llm_enhanced", "description_sources", "description_source_record_uids",
    "description_source_count", "primary_nace_code", "primary_sni_code",
    *SE_COMPANY_INFO_REGISTRY_COLUMNS,
    "wikidata_id", "lei", "source_record_uids", "evidence_hashes", "correction_ids",
    "suggestion_id", "model_provider", "model_name", "prompt_version", "source_run_id",
    "resolved_at",
)
```

- [ ] **Step 5: Run the unit tests to verify they pass**

Run: `uv run --frozen --no-sync pytest tests/test_clickhouse_migrations.py tests/test_se_company_layout.py tests/test_se_company_info.py -q -p no:warnings`
Expected: PASS.

- [ ] **Step 6: Run the info harness (docker clickhouse-local, several minutes)**

Run: `uv run --frozen --no-sync pytest tests/test_se_company_info_clickhouse_local.py -q -p no:warnings`
Expected: PASS -- `test_the_deployed_final_columns_are_what_the_ddl_replay_says` now sees the eight columns from `system.columns` in the same positions `declared_columns` replays. (If the docker image is missing the module skips; pull `clickhouse/clickhouse-server:26.5` and rerun -- a skip is not a pass here.)

- [ ] **Step 7: Commit**

```bash
git add corpscout/clickhouse/migrations/000374_corpscout_se_company_info_field_columns.up.sql \
        corpscout/clickhouse/migrations/000374_corpscout_se_company_info_field_columns.down.sql \
        corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/fields/tables.py \
        corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py \
        corpscout/services/dagster_v3/tests/test_se_company_layout.py \
        corpscout/services/dagster_v3/tests/test_se_company_info.py \
        corpscout/services/dagster_v3/tests/test_se_company_info_clickhouse_local.py
git commit -m "feat(clickhouse): registry scalars on se_company_info (000374)

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---

### Task 3: `policies.py` and the registry types

**Files:**
- Create: `src/dagster_v3/defs/se_company/fields/registry.py` (types and vocabularies only -- the instance, validation and fingerprint come in Task 4)
- Create: `src/dagster_v3/defs/se_company/fields/policies.py`
- Test: `tests/test_se_company_field_policies.py`

**Interfaces:**
- Produces (fixed names):
  - `registry.FieldSpec(name, value_type, display_group, structured, sources, policy="source_precedence", python_only=False)` and `registry.DatatypeRegistry(datatype, country, key_columns, fields, version)` -- frozen dataclasses.
  - `registry.KNOWN_SOURCES = ("scb", "bolagsverket", "esef", "wikidata", "ratsit", "domains", "llm")`, `registry.VALUE_TYPES = ("text", "code", "date", "integer", "decimal", "url", "json")`, `registry.DISPLAY_GROUPS = ("identity", "activity", "scale")`, `registry.REVIEWER = "reviewer"`.
  - `policies.FieldPolicy` (Protocol: `name: str`, `version: str`, `candidate_filter_sql(field) -> str`, `winner_order_sql(field) -> str`, `compare_key_sql(field) -> str`), `policies.SourcePrecedence` (name `source_precedence`, version `source_precedence-v1`), `policies.POLICIES: dict[str, FieldPolicy]`, `policies.policy_for(field) -> FieldPolicy` (raises `KeyError` for an unbound name).
- Fragment contract every policy writes against: the candidate alias `c` (columns of `corpscout.se_company_field_candidate`) plus `rank`, the 1-based position of `c.source` in the field's precedence tuple, which `sql.py` (Task 6) projects beside them as `indexOf([...sources...], c.source) AS rank`.
- Import direction: `registry.py` imports `policies.POLICIES` (Task 4); `policies.py` imports `FieldSpec` under `TYPE_CHECKING` only, so there is no runtime cycle.

- [ ] **Step 1: Write the failing tests**

`tests/test_se_company_field_policies.py`:

```python
"""The default resolve policy, pinned as text (spec 7.2). The fragments are executed
against real candidates in tests/test_se_company_field_sql.py; here they are frozen so
an edit shows up as a diff, and so the exported resolve_sql of every field diffs cleanly."""

from dataclasses import FrozenInstanceError

import pytest

from dagster_v3.defs.se_company.fields.policies import POLICIES, FieldPolicy, SourcePrecedence, policy_for
from dagster_v3.defs.se_company.fields.registry import (
    DISPLAY_GROUPS,
    KNOWN_SOURCES,
    REVIEWER,
    VALUE_TYPES,
    DatatypeRegistry,
    FieldSpec,
)

LEGAL_NAME = FieldSpec("legal_name", "text", "identity", False, ("scb", "bolagsverket", "wikidata"))
WEBSITE = FieldSpec("website", "url", "scale", False, ("domains", "wikidata"))


def test_the_registry_types_are_frozen_with_the_spec_defaults() -> None:
    assert LEGAL_NAME.policy == "source_precedence" and LEGAL_NAME.python_only is False
    with pytest.raises(FrozenInstanceError):
        LEGAL_NAME.name = "renamed"  # type: ignore[misc]
    registry = DatatypeRegistry(datatype="info", country="SE", key_columns=("company_id",),
                                fields=(LEGAL_NAME, WEBSITE), version="test")
    assert registry.fields[1] is WEBSITE
    with pytest.raises(FrozenInstanceError):
        registry.version = "test-2"  # type: ignore[misc]


def test_the_vocabularies() -> None:
    assert KNOWN_SOURCES == ("scb", "bolagsverket", "esef", "wikidata", "ratsit", "domains", "llm")
    assert VALUE_TYPES == ("text", "code", "date", "integer", "decimal", "url", "json")
    assert DISPLAY_GROUPS == ("identity", "activity", "scale")
    assert REVIEWER == "reviewer" and REVIEWER not in KNOWN_SOURCES


def test_source_precedence_is_the_registered_default() -> None:
    assert set(POLICIES) == {"source_precedence"}
    policy = policy_for(LEGAL_NAME)
    assert isinstance(policy, SourcePrecedence)
    assert (policy.name, policy.version) == ("source_precedence", "source_precedence-v1")


def test_source_precedence_fragments_are_pinned_as_text() -> None:
    policy = SourcePrecedence()
    assert policy.candidate_filter_sql(LEGAL_NAME) == "c.value IS NOT NULL AND trim(c.value) != ''"
    assert policy.winner_order_sql(LEGAL_NAME) == "rank ASC, c.observed_at DESC, c.source_record_uid DESC"
    assert policy.compare_key_sql(LEGAL_NAME) == (
        "if(JSONHas(c.value_json, 'compare_key'), "
        "JSONExtractString(c.value_json, 'compare_key'), lowerUTF8(trim(c.value)))"
    )


def test_the_fragments_do_not_depend_on_the_field() -> None:
    """The precedence tuple enters through sql.py's rank column, so every field gets the
    same three fragments."""
    policy = SourcePrecedence()
    for method in ("candidate_filter_sql", "winner_order_sql", "compare_key_sql"):
        assert getattr(policy, method)(LEGAL_NAME) == getattr(policy, method)(WEBSITE)


def test_policy_for_rejects_an_unbound_name() -> None:
    with pytest.raises(KeyError):
        policy_for(FieldSpec("x", "text", "identity", False, ("scb",), policy="tolerance"))


def test_source_precedence_satisfies_the_protocol() -> None:
    policy: FieldPolicy = SourcePrecedence()
    assert callable(policy.candidate_filter_sql) and callable(policy.winner_order_sql)
    assert callable(policy.compare_key_sql)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --frozen --no-sync pytest tests/test_se_company_field_policies.py -q -p no:warnings`
Expected: FAIL -- `ModuleNotFoundError: No module named 'dagster_v3.defs.se_company.fields.policies'`.

- [ ] **Step 3: Write `registry.py` (types and vocabularies)**

`src/dagster_v3/defs/se_company/fields/registry.py`:

```python
"""The SE company field registry: framework types and the ``info`` instance.

A registry declares every scalar attribute of one datatype (``info`` today), the sources
that may contribute it in precedence order, and the policy that picks a value. It is
owned by code, validated at import, and exported to ClickHouse by export.py; the
backoffice reads the export and never edits it (spec 2026-09-02, section 4).
"""

from dataclasses import dataclass

KNOWN_SOURCES = ("scb", "bolagsverket", "esef", "wikidata", "ratsit", "domains", "llm")
VALUE_TYPES = ("text", "code", "date", "integer", "decimal", "url", "json")
DISPLAY_GROUPS = ("identity", "activity", "scale")
# Reviewer decisions are not a source: they win by construction in the generated SQL
# (sql.py) and the backoffice renders them above the candidates list. Listing it in a
# field's sources is a registry error.
REVIEWER = "reviewer"


@dataclass(frozen=True)
class FieldSpec:
    name: str                          # snake_case, unique within the datatype
    value_type: str                    # one of VALUE_TYPES
    display_group: str                 # one of DISPLAY_GROUPS
    structured: bool                   # compare value_json instead of value
    sources: tuple[str, ...]           # precedence order, first wins; position is the rank
    policy: str = "source_precedence"  # name in policies.POLICIES
    python_only: bool = False          # resolved by Dagster alone; backoffice shows "next run"


@dataclass(frozen=True)
class DatatypeRegistry:
    datatype: str                  # "info"
    country: str                   # "SE"
    key_columns: tuple[str, ...]   # ("company_id",) for info; composite for financial/jobs later
    fields: tuple[FieldSpec, ...]
    version: str                   # se-info-vN; bumped on any field, source, rank or policy change
```

- [ ] **Step 4: Write `policies.py`**

`src/dagster_v3/defs/se_company/fields/policies.py`:

```python
"""Resolve policies: the ``FieldPolicy`` interface and the default ``source_precedence``.

A policy contributes three SQL fragments to the statement sql.py renders for a field
(spec section 7). Every fragment is written against the candidate alias ``c`` -- the
columns of corpscout.se_company_field_candidate -- plus ``rank``, the 1-based position
of ``c.source`` in the field's precedence tuple, which sql.py projects beside them
(``indexOf([...], c.source) AS rank``; a source absent from the tuple is filtered out
before any policy sees it). Reviewer decisions never pass through a policy.

Zero fields override the default today (spec 7.3). Adding one is a registry edit plus one
class here, registered in POLICIES, with its own tests; the registry version bumps.
"""

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from dagster_v3.defs.se_company.fields.registry import FieldSpec


class FieldPolicy(Protocol):
    name: str
    version: str

    def candidate_filter_sql(self, field: "FieldSpec") -> str:
        """WHERE fragment over c.* -- which candidates are eligible at all."""

    def winner_order_sql(self, field: "FieldSpec") -> str:
        """ORDER BY fragment over c.* and rank -- the first row per company wins."""

    def compare_key_sql(self, field: "FieldSpec") -> str:
        """Expression over c.* -- candidates with equal keys agree with each other."""


class SourcePrecedence:
    """First source in the tuple wins; within a source the newest observation wins, and a
    tie on observed_at is broken by the source record uid so the pick is deterministic."""

    name = "source_precedence"
    version = "source_precedence-v1"

    def candidate_filter_sql(self, field: "FieldSpec") -> str:
        return "c.value IS NOT NULL AND trim(c.value) != ''"

    def winner_order_sql(self, field: "FieldSpec") -> str:
        return "rank ASC, c.observed_at DESC, c.source_record_uid DESC"

    def compare_key_sql(self, field: "FieldSpec") -> str:
        # JSONHas returns 0 for '' (value_json's default) rather than raising.
        return (
            "if(JSONHas(c.value_json, 'compare_key'), "
            "JSONExtractString(c.value_json, 'compare_key'), lowerUTF8(trim(c.value)))"
        )


POLICIES: dict[str, FieldPolicy] = {SourcePrecedence.name: SourcePrecedence()}


def policy_for(field: "FieldSpec") -> FieldPolicy:
    """The policy bound to ``field``; a name outside POLICIES is a registry error that
    validate_registry catches at import, so the KeyError here is for ad-hoc specs."""
    return POLICIES[field.policy]
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run --frozen --no-sync pytest tests/test_se_company_field_policies.py -q -p no:warnings`
Expected: PASS (7 tests).

- [ ] **Step 6: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/fields/registry.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/fields/policies.py \
        corpscout/services/dagster_v3/tests/test_se_company_field_policies.py
git commit -m "feat(se): field policy interface with the source_precedence default

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---

### Task 4: `INFO_REGISTRY`, import-time validation, version fingerprint

**Files:**
- Modify: `src/dagster_v3/defs/se_company/fields/registry.py` (append below `DatatypeRegistry`)
- Test: `tests/test_se_company_field_registry.py`

**Interfaces:**
- Consumes: `policies.POLICIES`, `policies.policy_for`.
- Produces (fixed names): `INFO_REGISTRY: DatatypeRegistry` (datatype `info`, country `SE`, key_columns `("company_id",)`, version `se-info-v1`, the 12 fields of spec 4.2 in table order); `validate_registry(registry) -> None` (raises `ValueError`; called at import on `INFO_REGISTRY`); `field_by_name(registry, name) -> FieldSpec` (raises `KeyError`); `field_names(registry) -> tuple[str, ...]`; `registry_fingerprint(registry) -> str` (sha256 hex of the canonical JSON below).
- The fingerprint for `se-info-v1` is `5ddd00ddef6b722aeca8dec9fe360a720e6c00d4c4c5bc0704828671c80a88ac` (computed with the exact payload construction in Step 3; the test pins it -- a registry edit without a version bump and a new pin fails).

- [ ] **Step 1: Write the failing tests**

`tests/test_se_company_field_registry.py`:

```python
"""The info registry against spec 4.2, its import-time rules (spec 4.1) and the pin that
makes the version string track the content (spec 12)."""

import dataclasses

import pytest

from dagster_v3.defs.se_company.fields.registry import (
    INFO_REGISTRY,
    DatatypeRegistry,
    FieldSpec,
    field_by_name,
    field_names,
    registry_fingerprint,
    validate_registry,
)

# Spec section 4.2, transcribed row by row: (group, field, type, sources in precedence order).
SPEC_4_2 = (
    ("identity", "legal_name", "text", ("scb", "bolagsverket", "wikidata")),
    ("identity", "legal_form_code", "code", ("scb", "bolagsverket")),
    ("identity", "status", "code", ("scb", "bolagsverket")),
    ("identity", "incorporation_date", "date", ("scb", "bolagsverket", "wikidata")),
    ("activity", "description", "text", ("llm", "esef", "wikidata", "scb")),
    ("activity", "description_sv", "text", ("llm", "scb")),
    ("activity", "primary_sni_code", "code", ("scb", "ratsit")),
    ("activity", "primary_nace_code", "code", ("scb", "ratsit")),
    ("activity", "industry_label_en", "text", ("scb", "ratsit", "wikidata")),
    ("scale", "website", "url", ("domains", "wikidata")),
    ("scale", "employee_count", "json", ("esef", "bolagsverket", "ratsit", "wikidata")),
    ("scale", "latest_revenue", "json", ("esef", "bolagsverket", "ratsit")),
)


def _registry(*fields: FieldSpec) -> DatatypeRegistry:
    return DatatypeRegistry(datatype="info", country="SE", key_columns=("company_id",),
                            fields=fields, version="test")


def test_info_registry_matches_spec_4_2_exactly() -> None:
    assert (INFO_REGISTRY.datatype, INFO_REGISTRY.country, INFO_REGISTRY.key_columns,
            INFO_REGISTRY.version) == ("info", "SE", ("company_id",), "se-info-v1")
    assert [(f.display_group, f.name, f.value_type, f.sources) for f in INFO_REGISTRY.fields] == list(SPEC_4_2)
    for field in INFO_REGISTRY.fields:
        # json-typed fields compare value_json (structured); everything else compares value.
        assert field.structured is (field.value_type == "json"), field.name
        # No override and no python_only field today (spec 7.3).
        assert field.policy == "source_precedence" and field.python_only is False, field.name


def test_lookups() -> None:
    assert field_names(INFO_REGISTRY) == tuple(row[1] for row in SPEC_4_2)
    assert field_by_name(INFO_REGISTRY, "website").sources == ("domains", "wikidata")
    with pytest.raises(KeyError):
        field_by_name(INFO_REGISTRY, "revenue")


@pytest.mark.parametrize(
    ("fields", "message"),
    [
        ((FieldSpec("a", "text", "identity", False, ("scb",)),
          FieldSpec("a", "text", "identity", False, ("esef",))), "duplicate field 'a'"),
        ((FieldSpec("a", "text", "identity", False, ("scb", "scb")),), "duplicate source"),
        ((FieldSpec("a", "text", "identity", False, ("scb", "reviewer")),), "'reviewer' is not a source"),
        ((FieldSpec("a", "text", "identity", False, ("brreg",)),), "unknown source 'brreg'"),
        ((FieldSpec("a", "text", "identity", False, ("scb",), policy="tolerance"),), "unknown policy 'tolerance'"),
        ((FieldSpec("a", "money", "identity", False, ("scb",)),), "unknown value_type 'money'"),
        ((FieldSpec("a", "text", "finance", False, ("scb",)),), "unknown display_group 'finance'"),
        ((FieldSpec("a", "text", "identity", False, ()),), "no sources"),
    ],
    ids=["duplicate-field", "duplicate-source", "reviewer", "unknown-source", "unknown-policy",
         "unknown-value-type", "unknown-display-group", "no-sources"],
)
def test_validate_registry_rejects(fields: tuple[FieldSpec, ...], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        validate_registry(_registry(*fields))


def test_validate_registry_rejects_a_registry_without_key_columns() -> None:
    with pytest.raises(ValueError, match="no key columns"):
        validate_registry(dataclasses.replace(INFO_REGISTRY, key_columns=()))


def test_validate_registry_accepts_the_info_registry() -> None:
    validate_registry(INFO_REGISTRY)  # also ran at import; a failure there breaks Definitions


def test_the_version_is_pinned_to_the_registry_content() -> None:
    """Spec 12: the version string changes when any field, source order, policy binding or
    policy version changes. The fingerprint hashes exactly those, so editing the registry
    without bumping the version AND this pin fails here -- that is the intended friction."""
    assert registry_fingerprint(INFO_REGISTRY) == (
        "5ddd00ddef6b722aeca8dec9fe360a720e6c00d4c4c5bc0704828671c80a88ac"
    )
    reordered = dataclasses.replace(INFO_REGISTRY, fields=tuple(
        dataclasses.replace(f, sources=("bolagsverket", "scb", "wikidata")) if f.name == "legal_name" else f
        for f in INFO_REGISTRY.fields
    ))
    assert registry_fingerprint(reordered) != registry_fingerprint(INFO_REGISTRY)
    renamed_version = dataclasses.replace(INFO_REGISTRY, version="se-info-v2")
    assert registry_fingerprint(renamed_version) == registry_fingerprint(INFO_REGISTRY)  # content, not label
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --frozen --no-sync pytest tests/test_se_company_field_registry.py -q -p no:warnings`
Expected: FAIL -- `ImportError: cannot import name 'INFO_REGISTRY'`.

- [ ] **Step 3: Append the instance, validation and fingerprint to `registry.py`**

Make the import block at the top of `registry.py` read (policies.py imports `FieldSpec` under `TYPE_CHECKING` only, so this is not a cycle):

```python
import hashlib
import json
from dataclasses import dataclass

from dagster_v3.defs.se_company.fields.policies import POLICIES, policy_for
```

then append after `DatatypeRegistry`:

```python
def validate_registry(registry: DatatypeRegistry) -> None:
    """Import-time rules (spec 4.1): unique field names; at least one source per field
    and no duplicate within it; every source in KNOWN_SOURCES; ``reviewer`` never listed;
    every policy in POLICIES; value types and display groups from the fixed vocabularies."""
    if not registry.key_columns:
        raise ValueError(f"{registry.datatype}: no key columns")
    seen: set[str] = set()
    for field in registry.fields:
        if field.name in seen:
            raise ValueError(f"{registry.datatype}: duplicate field {field.name!r}")
        seen.add(field.name)
        if field.value_type not in VALUE_TYPES:
            raise ValueError(f"{field.name}: unknown value_type {field.value_type!r}")
        if field.display_group not in DISPLAY_GROUPS:
            raise ValueError(f"{field.name}: unknown display_group {field.display_group!r}")
        if not field.sources:
            raise ValueError(f"{field.name}: no sources")
        if len(set(field.sources)) != len(field.sources):
            raise ValueError(f"{field.name}: duplicate source in {field.sources}")
        for source in field.sources:
            if source == REVIEWER:
                raise ValueError(f"{field.name}: {REVIEWER!r} is not a source")
            if source not in KNOWN_SOURCES:
                raise ValueError(f"{field.name}: unknown source {source!r}")
        if field.policy not in POLICIES:
            raise ValueError(f"{field.name}: unknown policy {field.policy!r}")


def field_by_name(registry: DatatypeRegistry, name: str) -> FieldSpec:
    for field in registry.fields:
        if field.name == name:
            return field
    raise KeyError(name)


def field_names(registry: DatatypeRegistry) -> tuple[str, ...]:
    return tuple(field.name for field in registry.fields)


def registry_fingerprint(registry: DatatypeRegistry) -> str:
    """sha256 of everything a version bump must track -- fields, their sources in order,
    the policy binding and the bound policy's version -- and nothing else (not the
    version label itself). tests/test_se_company_field_registry.py pins it per version."""
    payload = {
        "datatype": registry.datatype,
        "country": registry.country,
        "key_columns": list(registry.key_columns),
        "fields": [
            {
                "name": field.name,
                "value_type": field.value_type,
                "display_group": field.display_group,
                "structured": field.structured,
                "sources": list(field.sources),
                "policy": field.policy,
                "policy_version": policy_for(field).version,
                "python_only": field.python_only,
            }
            for field in registry.fields
        ],
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# Spec section 4.2. Precedence order first wins; reviewer decisions always come before
# every source and are therefore not listed.
INFO_REGISTRY = DatatypeRegistry(
    datatype="info",
    country="SE",
    key_columns=("company_id",),
    version="se-info-v1",
    fields=(
        FieldSpec("legal_name", "text", "identity", False, ("scb", "bolagsverket", "wikidata")),
        FieldSpec("legal_form_code", "code", "identity", False, ("scb", "bolagsverket")),
        FieldSpec("status", "code", "identity", False, ("scb", "bolagsverket")),
        FieldSpec("incorporation_date", "date", "identity", False, ("scb", "bolagsverket", "wikidata")),
        FieldSpec("description", "text", "activity", False, ("llm", "esef", "wikidata", "scb")),
        FieldSpec("description_sv", "text", "activity", False, ("llm", "scb")),
        FieldSpec("primary_sni_code", "code", "activity", False, ("scb", "ratsit")),
        FieldSpec("primary_nace_code", "code", "activity", False, ("scb", "ratsit")),
        FieldSpec("industry_label_en", "text", "activity", False, ("scb", "ratsit", "wikidata")),
        FieldSpec("website", "url", "scale", False, ("domains", "wikidata")),
        # value_json members: count, as_of, period (spec 4.2)
        FieldSpec("employee_count", "json", "scale", True, ("esef", "bolagsverket", "ratsit", "wikidata")),
        # value_json members: amount, currency, amount_usd, fiscal_year, period_end (spec 4.2)
        FieldSpec("latest_revenue", "json", "scale", True, ("esef", "bolagsverket", "ratsit")),
    ),
)
validate_registry(INFO_REGISTRY)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run --frozen --no-sync pytest tests/test_se_company_field_registry.py tests/test_se_company_field_policies.py -q -p no:warnings`
Expected: PASS. If `test_the_version_is_pinned_to_the_registry_content` fails on the hash, diff the payload construction against Step 3 character by character (key names, list vs tuple, `separators=(",", ":")`, `sort_keys=True`); do not paste the new hash until the payload matches.

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/fields/registry.py \
        corpscout/services/dagster_v3/tests/test_se_company_field_registry.py
git commit -m "feat(se): the SE info field registry, validated at import

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---

### Task 5: Migration 000375 -- decision CHECKs widened to the registry

**Files:**
- Create: `corpscout/clickhouse/migrations/000375_corpscout_se_company_info_field_value_registry_checks.up.sql`
- Create: `corpscout/clickhouse/migrations/000375_corpscout_se_company_info_field_value_registry_checks.down.sql`
- Modify: `tests/test_clickhouse_migrations.py` (`EXPECTED_MIGRATIONS` + one test)

**Interfaces:**
- Consumes: `registry.INFO_REGISTRY`, `registry.KNOWN_SOURCES`, `registry.field_names`.
- Produces: `known_field` = the 12 registry field names in registry order; `known_source` = `KNOWN_SOURCES + ('reviewer',)`. ClickHouse syntax `ALTER TABLE ... DROP CONSTRAINT x, ADD CONSTRAINT x CHECK ...` (000299 precedent). A CHECK is metadata; no row is rewritten.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_clickhouse_migrations.py`, and add `"000375_corpscout_se_company_info_field_value_registry_checks",` after the 000374 entry in `EXPECTED_MIGRATIONS`:

```python
def test_field_value_checks_are_widened_to_the_registry_in_000375() -> None:
    """Spec 4.3 / 6: the decisions table keeps its shape but its two CHECKs follow the
    registry -- pinned here to registry.py so the lists cannot drift. Adding a field or
    a source to the registry fails this test until a new migration widens the CHECK."""
    from dagster_v3.defs.se_company.fields.registry import INFO_REGISTRY, KNOWN_SOURCES, field_names

    up = _migration_sql("000375_corpscout_se_company_info_field_value_registry_checks.up.sql")
    down = _migration_sql("000375_corpscout_se_company_info_field_value_registry_checks.down.sql")

    def in_list(values: tuple[str, ...]) -> str:
        return "(" + ", ".join(f"'{value}'" for value in values) + ")"

    assert up.count("ALTER TABLE corpscout.se_company_info_field_value\n") == 2
    assert (
        "    DROP CONSTRAINT known_field,\n"
        "    ADD CONSTRAINT known_field CHECK field IN " + in_list(field_names(INFO_REGISTRY)) + ";"
    ) in up
    assert (
        "    DROP CONSTRAINT known_source,\n"
        "    ADD CONSTRAINT known_source CHECK source IN " + in_list((*KNOWN_SOURCES, "reviewer")) + ";"
    ) in up
    # The down restores 000371's pilot lists verbatim.
    assert "ADD CONSTRAINT known_field CHECK field IN ('description', 'description_sv');" in down
    assert "ADD CONSTRAINT known_source CHECK source IN ('scb', 'esef', 'wikidata', 'llm', 'reviewer');" in down
    assert "DROP TABLE" not in up and "TRUNCATE" not in up
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run --frozen --no-sync pytest tests/test_clickhouse_migrations.py::test_field_value_checks_are_widened_to_the_registry_in_000375 -q -p no:warnings`
Expected: FAIL -- `FileNotFoundError`.

- [ ] **Step 3: Write the migration**

`corpscout/clickhouse/migrations/000375_corpscout_se_company_info_field_value_registry_checks.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- The decisions table is unchanged in shape (spec section 6), but its two CHECKs were
-- written for the pilot's two text fields. Widened to the info registry's field list and
-- to every known candidate source plus 'reviewer' (a typed value). Both lists are pinned
-- to dagster_v3's registry.py by tests/test_clickhouse_migrations.py, so a registry edit
-- that adds a field or a source fails there until a new migration widens the CHECK again.
-- DROP CONSTRAINT + ADD CONSTRAINT in one ALTER (000299 precedent) -- a CHECK is
-- metadata, no row is rewritten, and every existing row satisfies the wider list.

ALTER TABLE corpscout.se_company_info_field_value
    DROP CONSTRAINT known_field,
    ADD CONSTRAINT known_field CHECK field IN ('legal_name', 'legal_form_code', 'status', 'incorporation_date', 'description', 'description_sv', 'primary_sni_code', 'primary_nace_code', 'industry_label_en', 'website', 'employee_count', 'latest_revenue');

ALTER TABLE corpscout.se_company_info_field_value
    DROP CONSTRAINT known_source,
    ADD CONSTRAINT known_source CHECK source IN ('scb', 'bolagsverket', 'esef', 'wikidata', 'ratsit', 'domains', 'llm', 'reviewer');
```

`corpscout/clickhouse/migrations/000375_corpscout_se_company_info_field_value_registry_checks.down.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- Restores 000371's pilot lists. Rows written for the wider lists would fail these
-- CHECKs only on a later INSERT, never retroactively.

ALTER TABLE corpscout.se_company_info_field_value
    DROP CONSTRAINT known_field,
    ADD CONSTRAINT known_field CHECK field IN ('description', 'description_sv');

ALTER TABLE corpscout.se_company_info_field_value
    DROP CONSTRAINT known_source,
    ADD CONSTRAINT known_source CHECK source IN ('scb', 'esef', 'wikidata', 'llm', 'reviewer');
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run --frozen --no-sync pytest tests/test_clickhouse_migrations.py -q -p no:warnings`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add corpscout/clickhouse/migrations/000375_corpscout_se_company_info_field_value_registry_checks.up.sql \
        corpscout/clickhouse/migrations/000375_corpscout_se_company_info_field_value_registry_checks.down.sql \
        corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py
git commit -m "feat(clickhouse): widen the field-value CHECKs to the registry (000375)

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---

### Task 6: `sql.py` -- the resolve and projection renderers, pinned as text

**Files:**
- Create: `src/dagster_v3/defs/se_company/fields/sql.py`
- Test: `tests/test_se_company_field_sql.py` (unit half; Task 7 appends the harness half to the same file)

**Interfaces:**
- Consumes: `tables.*`, `registry.DatatypeRegistry/FieldSpec/field_names`, `policies.policy_for`, `tests/se_company_ddl.projection_aliases`.
- Produces (fixed names): `array_literal(values: tuple[str, ...]) -> str` (`['a', 'b']`, quotes backslash-escaped), `sql_string(value) -> str`, `rank_sql(field) -> str` (`indexOf(<array literal of field.sources>, c.source)`), `render_resolve_sql(registry, field) -> str`, `render_projection_select_sql(registry) -> str`, `render_projection_sql(registry) -> str`.
- Parameters are ClickHouse server-side placeholders: resolve uses `{field:String}`, `{company_ids:Array(String)}`, `{source_run_id:String}`, `{resolved_at:DateTime64(3)}`; the projection uses `{company_ids:Array(String)}` only. From clickhouse-driver they render server-side only when the `Client` is built with `settings={"server_side_params": True}` (driver 0.2.7+; `ClickhouseResource.settings` reaches `Client(**kwargs)` in `dagster_clickhouse/resource.py:75-87`) -- the resolve plan's concern; the backoffice passes `query_params`; the harness (Task 7) inlines literals.
- Semantics (spec 7.4, executed in Task 7): the live decision (newest `(created_at, toString(value_id))`) wins when its `value IS NOT NULL`; otherwise the policy's winner among eligible candidates (source in the field's tuple AND the policy filter); no row when neither. The decision CTE aggregates a TUPLE because `argMax(value, ...)` would skip the NULL (release) row and return the older decision instead. `render_projection_sql` inserts in `SE_COMPANY_INFO_COLUMNS` order and publishes only companies with a `legal_name` candidate from `scb` or `bolagsverket` (spec 8.3's SCB-row rule) and at least one winning candidate uid (`has_evidence` CHECK).

- [ ] **Step 1: Write the failing unit tests**

`tests/test_se_company_field_sql.py`:

```python
"""The registry's generated SQL, pinned as text here and executed against real DDL in the
clickhouse-local harness further down (spec 7.4, 8.3)."""

from dagster_v3.defs.se_company.fields.registry import INFO_REGISTRY, field_by_name, field_names
from dagster_v3.defs.se_company.fields.sql import (
    array_literal,
    rank_sql,
    render_projection_select_sql,
    render_projection_sql,
    render_resolve_sql,
    sql_string,
)
from dagster_v3.defs.se_company.fields.tables import SE_COMPANY_INFO_COLUMNS
from tests.se_company_ddl import projection_aliases

LEGAL_NAME_RESOLVE_SQL = """INSERT INTO corpscout.se_company_field
    (company_id, field, value, value_json, source, source_record_uid, observed_at, decision_id, policy_name, policy_version, candidate_count, agreeing_sources, registry_version, source_run_id, resolved_at)
WITH
    candidates AS (
        SELECT c.company_id AS company_id, c.source AS source, c.source_record_uid AS source_record_uid,
               c.value AS value, c.value_json AS value_json, c.observed_at AS observed_at
        FROM corpscout.se_company_field_candidate AS c FINAL
        WHERE c.field = {field:String} AND c.company_id IN {company_ids:Array(String)}
    ),
    eligible AS (
        SELECT c.company_id AS company_id, c.source AS source, c.source_record_uid AS source_record_uid,
               c.value AS value, c.value_json AS value_json, c.observed_at AS observed_at,
               indexOf(['scb', 'bolagsverket', 'wikidata'], c.source) AS rank,
               if(JSONHas(c.value_json, 'compare_key'), JSONExtractString(c.value_json, 'compare_key'), lowerUTF8(trim(c.value))) AS compare_key
        FROM candidates AS c
        WHERE has(['scb', 'bolagsverket', 'wikidata'], c.source) AND (c.value IS NOT NULL AND trim(c.value) != '')
    ),
    agreement AS (
        SELECT company_id, compare_key, arraySort(groupUniqArray(source)) AS agreeing_sources
        FROM eligible GROUP BY company_id, compare_key
    ),
    counted AS (
        SELECT company_id, toUInt16(count()) AS candidate_count FROM eligible GROUP BY company_id
    ),
    winner AS (
        SELECT c.* FROM eligible AS c
        ORDER BY c.company_id, rank ASC, c.observed_at DESC, c.source_record_uid DESC
        LIMIT 1 BY c.company_id
    ),
    decision AS (
        SELECT company_id,
               argMax((value, source, source_ref, source_at, created_at, value_id),
                      (created_at, toString(value_id))) AS latest
        FROM corpscout.se_company_info_field_value
        WHERE field = {field:String} AND company_id IN {company_ids:Array(String)}
        GROUP BY company_id
    ),
    live AS (
        SELECT company_id, assumeNotNull(tupleElement(latest, 1)) AS value,
               tupleElement(latest, 2) AS source, tupleElement(latest, 3) AS source_ref,
               ifNull(tupleElement(latest, 4), tupleElement(latest, 5)) AS observed_at,
               tupleElement(latest, 6) AS decision_id
        FROM decision WHERE tupleElement(latest, 1) IS NOT NULL
    ),
    decided AS (
        SELECT c.company_id AS company_id, c.value AS value, c.value_json AS value_json,
               c.source AS source, c.source_record_uid AS source_record_uid,
               c.observed_at AS observed_at, c.decision_id AS decision_id,
               if(JSONHas(c.value_json, 'compare_key'), JSONExtractString(c.value_json, 'compare_key'), lowerUTF8(trim(c.value))) AS compare_key
        FROM (
            SELECT l.company_id AS company_id, l.value AS value, ifNull(k.value_json, '') AS value_json,
                   l.source AS source, l.source_ref AS source_record_uid,
                   l.observed_at AS observed_at, l.decision_id AS decision_id
            FROM live AS l
            LEFT JOIN candidates AS k
                ON k.company_id = l.company_id AND k.source = l.source AND k.source_record_uid = l.source_ref
        ) AS c
    )
SELECT
    d.company_id AS company_id, {field:String} AS field, d.value AS value, d.value_json AS value_json,
    d.source AS source, d.source_record_uid AS source_record_uid, d.observed_at AS observed_at,
    toNullable(d.decision_id) AS decision_id,
    'source_precedence' AS policy_name, 'source_precedence-v1' AS policy_version,
    ifNull(n.candidate_count, toUInt16(0)) AS candidate_count, a.agreeing_sources AS agreeing_sources,
    'se-info-v1' AS registry_version, {source_run_id:String} AS source_run_id,
    {resolved_at:DateTime64(3)} AS resolved_at
FROM decided AS d
LEFT JOIN counted AS n ON n.company_id = d.company_id
LEFT JOIN agreement AS a ON a.company_id = d.company_id AND a.compare_key = d.compare_key
UNION ALL
SELECT
    w.company_id AS company_id, {field:String} AS field, w.value AS value, w.value_json AS value_json,
    w.source AS source, w.source_record_uid AS source_record_uid, w.observed_at AS observed_at,
    CAST(NULL AS Nullable(UUID)) AS decision_id,
    'source_precedence' AS policy_name, 'source_precedence-v1' AS policy_version,
    ifNull(n.candidate_count, toUInt16(0)) AS candidate_count, a.agreeing_sources AS agreeing_sources,
    'se-info-v1' AS registry_version, {source_run_id:String} AS source_run_id,
    {resolved_at:DateTime64(3)} AS resolved_at
FROM winner AS w
LEFT JOIN counted AS n ON n.company_id = w.company_id
LEFT JOIN agreement AS a ON a.company_id = w.company_id AND a.compare_key = w.compare_key
WHERE w.company_id NOT IN (SELECT company_id FROM decided)"""


def test_literals() -> None:
    assert sql_string("it's") == "'it\\'s'"
    assert array_literal(("scb", "bolagsverket")) == "['scb', 'bolagsverket']"
    assert array_literal(()) == "[]"
    assert rank_sql(field_by_name(INFO_REGISTRY, "website")) == "indexOf(['domains', 'wikidata'], c.source)"


def test_the_legal_name_resolve_statement_is_pinned_as_text() -> None:
    assert render_resolve_sql(INFO_REGISTRY, field_by_name(INFO_REGISTRY, "legal_name")) == LEGAL_NAME_RESOLVE_SQL


def test_every_field_renders_its_own_precedence_and_the_same_parameters() -> None:
    for field in INFO_REGISTRY.fields:
        sql = render_resolve_sql(INFO_REGISTRY, field)
        assert f"indexOf({array_literal(field.sources)}, c.source) AS rank" in sql
        assert f"WHERE has({array_literal(field.sources)}, c.source) AND (" in sql
        for parameter in ("{field:String}", "{company_ids:Array(String)}", "{source_run_id:String}",
                          "{resolved_at:DateTime64(3)}"):
            assert parameter in sql
        assert "%(" not in sql  # server-side parameters only, never driver-side ones
        assert "'se-info-v1' AS registry_version" in sql
    # The release path: the decision CTE aggregates a tuple, never the bare Nullable value.
    assert "argMax(value," not in LEGAL_NAME_RESOLVE_SQL


def test_the_projection_inserts_every_wide_column_in_ddl_order() -> None:
    select = render_projection_select_sql(INFO_REGISTRY)
    assert projection_aliases(select) == list(SE_COMPANY_INFO_COLUMNS)
    statement = render_projection_sql(INFO_REGISTRY)
    assert statement.startswith(
        "INSERT INTO corpscout.se_company_info\n    (" + ", ".join(SE_COMPANY_INFO_COLUMNS) + ")\nWITH\n"
    )
    assert statement.endswith(select)
    assert statement.count("{company_ids:Array(String)}") == 7 and "{field:String}" not in statement


def test_the_projection_reads_every_registry_field_by_name() -> None:
    """The pivot is hand-written per wide column, so a field added to the registry must be
    wired into it (or deliberately left out of the wide row) -- this makes that a decision
    rather than an omission. json fields are read through their value_json members."""
    select = render_projection_select_sql(INFO_REGISTRY)
    for name in field_names(INFO_REGISTRY):
        assert f"field = '{name}'" in select, name
    assert f"has({array_literal(field_names(INFO_REGISTRY))}, f.field)" in select
    for member in ("'count'", "'as_of'", "'amount'", "'currency'", "'amount_usd'", "'fiscal_year'", "'language'"):
        assert member in select
    # spec 8.3: the SCB/Bolagsverket legal-name gate, the label lookup, the kept id columns.
    assert "WHERE field = 'legal_name' AND source IN ('scb', 'bolagsverket')" in select
    assert "FROM corpscout.se_code_labels WHERE code_type = 'legal_form'" in select
    assert "FROM corpscout.se_company_info_wikidata" in select and "FROM corpscout.se_company_info_esef" in select
    assert "FROM corpscout.se_company_info_enrichment_observation" in select
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --frozen --no-sync pytest tests/test_se_company_field_sql.py -q -p no:warnings`
Expected: FAIL -- `ModuleNotFoundError: No module named 'dagster_v3.defs.se_company.fields.sql'`.

- [ ] **Step 3: Write `sql.py`**

`src/dagster_v3/defs/se_company/fields/sql.py`:

```python
"""Renders the registry into the ClickHouse statements both runners execute verbatim.

``render_resolve_sql`` -- one INSERT per field into corpscout.se_company_field (spec 7.4):
the live decision from se_company_info_field_value wins when its value is not NULL,
otherwise the policy's winner among the eligible candidates; no row when neither.
``render_projection_sql`` -- the wide corpscout.se_company_info row re-pivoted from the
resolved rows (spec 8.3), inserting in SE_COMPANY_INFO_COLUMNS order.

Parameters are ClickHouse server-side ``{name:Type}`` placeholders -- ``field``,
``company_ids``, ``source_run_id`` and ``resolved_at`` for resolve, ``company_ids`` alone
for the projection. clickhouse-driver renders them server-side only when the Client is
built with settings={'server_side_params': True} (0.2.7+); the backoffice passes them as
query_params. Never mix in driver-side ``%(name)s`` placeholders.

Verified on ClickHouse 26.5 (tests/test_se_company_field_sql.py harness):
- ``argMax(arg, key)`` SKIPS rows whose ``arg`` is NULL, so the decision CTE aggregates a
  tuple -- a release row (value IS NULL) must be the latest, not be skipped in favour of
  the older value it released.
- A ``WITH`` clause is visible in both branches of a UNION ALL; an alias may precede FINAL.
"""

from dagster_v3.defs.se_company.fields.policies import policy_for
from dagster_v3.defs.se_company.fields.registry import DatatypeRegistry, FieldSpec, field_names
from dagster_v3.defs.se_company.fields.tables import (
    SE_COMPANY_FIELD,
    SE_COMPANY_FIELD_CANDIDATE,
    SE_COMPANY_FIELD_COLUMNS,
    SE_COMPANY_INFO,
    SE_COMPANY_INFO_COLUMNS,
    SE_COMPANY_INFO_FIELD_VALUE,
)


def sql_string(value: str) -> str:
    """``value`` as a ClickHouse string literal (backslash-escaped quotes)."""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def array_literal(values: tuple[str, ...]) -> str:
    """``values`` as an inline ClickHouse Array(String) literal, e.g. ['scb', 'esef']."""
    return "[" + ", ".join(sql_string(value) for value in values) + "]"


def rank_sql(field: FieldSpec) -> str:
    """1-based position of the candidate's source in the field's precedence tuple (0 = not listed)."""
    return f"indexOf({array_literal(field.sources)}, c.source)"


# The candidate columns every CTE carries, aliased explicitly so a renamed DDL column
# fails here rather than binding by position.
CANDIDATE_PROJECTION = (
    "c.company_id AS company_id, c.source AS source, c.source_record_uid AS source_record_uid,\n"
    "               c.value AS value, c.value_json AS value_json, c.observed_at AS observed_at"
)


def render_resolve_sql(registry: DatatypeRegistry, field: FieldSpec) -> str:
    policy = policy_for(field)
    sources = array_literal(field.sources)
    columns = ", ".join(SE_COMPANY_FIELD_COLUMNS)
    return f"""INSERT INTO {SE_COMPANY_FIELD}
    ({columns})
WITH
    candidates AS (
        SELECT {CANDIDATE_PROJECTION}
        FROM {SE_COMPANY_FIELD_CANDIDATE} AS c FINAL
        WHERE c.field = {{field:String}} AND c.company_id IN {{company_ids:Array(String)}}
    ),
    eligible AS (
        SELECT {CANDIDATE_PROJECTION},
               {rank_sql(field)} AS rank,
               {policy.compare_key_sql(field)} AS compare_key
        FROM candidates AS c
        WHERE has({sources}, c.source) AND ({policy.candidate_filter_sql(field)})
    ),
    agreement AS (
        SELECT company_id, compare_key, arraySort(groupUniqArray(source)) AS agreeing_sources
        FROM eligible GROUP BY company_id, compare_key
    ),
    counted AS (
        SELECT company_id, toUInt16(count()) AS candidate_count FROM eligible GROUP BY company_id
    ),
    winner AS (
        SELECT c.* FROM eligible AS c
        ORDER BY c.company_id, {policy.winner_order_sql(field)}
        LIMIT 1 BY c.company_id
    ),
    decision AS (
        SELECT company_id,
               argMax((value, source, source_ref, source_at, created_at, value_id),
                      (created_at, toString(value_id))) AS latest
        FROM {SE_COMPANY_INFO_FIELD_VALUE}
        WHERE field = {{field:String}} AND company_id IN {{company_ids:Array(String)}}
        GROUP BY company_id
    ),
    live AS (
        SELECT company_id, assumeNotNull(tupleElement(latest, 1)) AS value,
               tupleElement(latest, 2) AS source, tupleElement(latest, 3) AS source_ref,
               ifNull(tupleElement(latest, 4), tupleElement(latest, 5)) AS observed_at,
               tupleElement(latest, 6) AS decision_id
        FROM decision WHERE tupleElement(latest, 1) IS NOT NULL
    ),
    decided AS (
        SELECT c.company_id AS company_id, c.value AS value, c.value_json AS value_json,
               c.source AS source, c.source_record_uid AS source_record_uid,
               c.observed_at AS observed_at, c.decision_id AS decision_id,
               {policy.compare_key_sql(field)} AS compare_key
        FROM (
            SELECT l.company_id AS company_id, l.value AS value, ifNull(k.value_json, '') AS value_json,
                   l.source AS source, l.source_ref AS source_record_uid,
                   l.observed_at AS observed_at, l.decision_id AS decision_id
            FROM live AS l
            LEFT JOIN candidates AS k
                ON k.company_id = l.company_id AND k.source = l.source AND k.source_record_uid = l.source_ref
        ) AS c
    )
SELECT
    d.company_id AS company_id, {{field:String}} AS field, d.value AS value, d.value_json AS value_json,
    d.source AS source, d.source_record_uid AS source_record_uid, d.observed_at AS observed_at,
    toNullable(d.decision_id) AS decision_id,
    {sql_string(policy.name)} AS policy_name, {sql_string(policy.version)} AS policy_version,
    ifNull(n.candidate_count, toUInt16(0)) AS candidate_count, a.agreeing_sources AS agreeing_sources,
    {sql_string(registry.version)} AS registry_version, {{source_run_id:String}} AS source_run_id,
    {{resolved_at:DateTime64(3)}} AS resolved_at
FROM decided AS d
LEFT JOIN counted AS n ON n.company_id = d.company_id
LEFT JOIN agreement AS a ON a.company_id = d.company_id AND a.compare_key = d.compare_key
UNION ALL
SELECT
    w.company_id AS company_id, {{field:String}} AS field, w.value AS value, w.value_json AS value_json,
    w.source AS source, w.source_record_uid AS source_record_uid, w.observed_at AS observed_at,
    CAST(NULL AS Nullable(UUID)) AS decision_id,
    {sql_string(policy.name)} AS policy_name, {sql_string(policy.version)} AS policy_version,
    ifNull(n.candidate_count, toUInt16(0)) AS candidate_count, a.agreeing_sources AS agreeing_sources,
    {sql_string(registry.version)} AS registry_version, {{source_run_id:String}} AS source_run_id,
    {{resolved_at:DateTime64(3)}} AS resolved_at
FROM winner AS w
LEFT JOIN counted AS n ON n.company_id = w.company_id
LEFT JOIN agreement AS a ON a.company_id = w.company_id AND a.compare_key = w.compare_key
WHERE w.company_id NOT IN (SELECT company_id FROM decided)"""


def render_projection_select_sql(registry: DatatypeRegistry) -> str:
    """The wide row per company from its resolved rows (spec 8.3), as a bare SELECT so a
    caller can stage it (publish_with_stage) or insert it directly (render_projection_sql).

    Column sources: registry fields by name (json members extracted from value_json);
    legal-form labels from se_code_labels on the resolved code; wikidata_id / lei from the
    artifacts as today; llm provenance from the observation when the description came from
    the llm source, the pilot's deterministic constants otherwise; correction_ids = every
    decision id applied; source_record_uids / evidence_hashes = the winning candidates'.
    A company is published only with a legal_name candidate from scb or bolagsverket and
    at least one winning candidate uid (the table's has_evidence CHECK).
    """
    fields = array_literal(field_names(registry))
    return f"""WITH
    resolved AS (
        SELECT f.company_id AS company_id, f.field AS field, f.value AS value, f.value_json AS value_json,
               f.source AS source, f.source_record_uid AS source_record_uid, f.decision_id AS decision_id,
               f.source_run_id AS source_run_id, f.resolved_at AS resolved_at,
               ifNull(c.evidence_hash, '') AS evidence_hash
        FROM {SE_COMPANY_FIELD} AS f FINAL
        LEFT JOIN (
            SELECT company_id, field, source, source_record_uid, toString(evidence_hash) AS evidence_hash
            FROM {SE_COMPANY_FIELD_CANDIDATE} FINAL
            WHERE company_id IN {{company_ids:Array(String)}}
        ) AS c ON c.company_id = f.company_id AND c.field = f.field AND c.source = f.source
              AND c.source_record_uid = f.source_record_uid
        WHERE f.company_id IN {{company_ids:Array(String)}} AND has({fields}, f.field)
    ),
    pivot AS (
        SELECT company_id,
               anyIf(value, field = 'legal_name') AS legal_name,
               anyIf(value, field = 'legal_form_code') AS legal_form_code,
               anyIf(value, field = 'status') AS status,
               anyIf(value, field = 'incorporation_date') AS incorporation_date_text,
               anyIf(value, field = 'description') AS description,
               anyIf(JSONExtractString(value_json, 'language'), field = 'description') AS description_language,
               anyIf(source, field = 'description') AS description_source,
               anyIf(source_record_uid, field = 'description') AS description_source_record_uid,
               anyIf(value, field = 'description_sv') AS description_sv,
               anyIf(value, field = 'primary_nace_code') AS primary_nace_code,
               anyIf(value, field = 'primary_sni_code') AS primary_sni_code,
               anyIf(value, field = 'industry_label_en') AS industry_label_en,
               anyIf(value, field = 'website') AS website,
               anyIf(JSONExtract(value_json, 'count', 'Nullable(UInt64)'), field = 'employee_count') AS employee_count,
               anyIf(toDate32OrNull(JSONExtractString(value_json, 'as_of')), field = 'employee_count') AS employee_count_as_of,
               anyIf(toDecimal128OrNull(JSONExtractRaw(value_json, 'amount'), 2), field = 'latest_revenue') AS latest_revenue_amount,
               anyIf(JSONExtractString(value_json, 'currency'), field = 'latest_revenue') AS latest_revenue_currency,
               anyIf(toDecimal128OrNull(JSONExtractRaw(value_json, 'amount_usd'), 2), field = 'latest_revenue') AS latest_revenue_amount_usd,
               anyIf(JSONExtract(value_json, 'fiscal_year', 'Nullable(UInt16)'), field = 'latest_revenue') AS latest_revenue_fiscal_year,
               arraySort(groupUniqArrayIf(source_record_uid, source_record_uid != '')) AS source_record_uids,
               arraySort(groupUniqArrayIf(evidence_hash, evidence_hash != '')) AS evidence_hashes,
               arraySort(groupArrayIf(assumeNotNull(decision_id), decision_id IS NOT NULL)) AS correction_ids,
               max(resolved_at) AS last_resolved_at,
               argMax(source_run_id, resolved_at) AS last_source_run_id
        FROM resolved
        GROUP BY company_id
    ),
    description_candidates AS (
        SELECT company_id,
               arrayMap(t -> tupleElement(t, 1), arraySort(groupArray((source, source_record_uid)))) AS description_sources,
               arrayMap(t -> tupleElement(t, 2), arraySort(groupArray((source, source_record_uid)))) AS description_source_record_uids,
               toUInt8(count()) AS description_source_count
        FROM {SE_COMPANY_FIELD_CANDIDATE} FINAL
        WHERE field = 'description' AND company_id IN {{company_ids:Array(String)}}
        GROUP BY company_id
    ),
    publishable AS (
        SELECT DISTINCT company_id FROM {SE_COMPANY_FIELD_CANDIDATE} FINAL
        WHERE field = 'legal_name' AND source IN ('scb', 'bolagsverket')
          AND company_id IN {{company_ids:Array(String)}}
    ),
    legal_form_labels AS (
        SELECT code, argMax(label_en, version) AS label_en, argMax(label_sv, version) AS label_sv
        FROM corpscout.se_code_labels WHERE code_type = 'legal_form' GROUP BY code
    ),
    wikidata_ids AS (
        SELECT company_id, argMax(wikidata_id, observed_at) AS wikidata_id
        FROM corpscout.se_company_info_wikidata
        WHERE company_id IN {{company_ids:Array(String)}} GROUP BY company_id
    ),
    leis AS (
        SELECT company_id, argMax(lei, observed_at) AS lei
        FROM corpscout.se_company_info_esef
        WHERE company_id IN {{company_ids:Array(String)}} GROUP BY company_id
    ),
    observations AS (
        SELECT toString(suggestion_id) AS suggestion_ref,
               argMax(model_provider, created_at) AS model_provider,
               argMax(model_name, created_at) AS model_name,
               argMax(prompt_version, created_at) AS prompt_version
        FROM corpscout.se_company_info_enrichment_observation
        WHERE company_id IN {{company_ids:Array(String)}} GROUP BY suggestion_id
    )
SELECT
    p.company_id AS company_id,
    p.legal_name AS legal_name,
    nullIf(p.legal_form_code, '') AS legal_form_code,
    ifNull(lf.label_en, '') AS legal_form_label_en,
    ifNull(lf.label_sv, '') AS legal_form_label_sv,
    p.status AS status,
    toDate32OrNull(p.incorporation_date_text) AS incorporation_date,
    nullIf(p.description, '') AS description,
    nullIf(p.description_sv, '') AS description_sv,
    p.description_language AS description_language,
    p.description_source = 'llm' AS llm_enhanced,
    dc.description_sources AS description_sources,
    dc.description_source_record_uids AS description_source_record_uids,
    ifNull(dc.description_source_count, toUInt8(0)) AS description_source_count,
    p.primary_nace_code AS primary_nace_code,
    p.primary_sni_code AS primary_sni_code,
    p.industry_label_en AS industry_label_en,
    nullIf(p.website, '') AS website,
    p.employee_count AS employee_count,
    p.employee_count_as_of AS employee_count_as_of,
    p.latest_revenue_amount AS latest_revenue_amount,
    p.latest_revenue_currency AS latest_revenue_currency,
    p.latest_revenue_amount_usd AS latest_revenue_amount_usd,
    p.latest_revenue_fiscal_year AS latest_revenue_fiscal_year,
    nullIf(ifNull(w.wikidata_id, ''), '') AS wikidata_id,
    nullIf(ifNull(e.lei, ''), '') AS lei,
    p.source_record_uids AS source_record_uids,
    p.evidence_hashes AS evidence_hashes,
    p.correction_ids AS correction_ids,
    if(p.description_source = 'llm', toUUIDOrNull(p.description_source_record_uid), NULL) AS suggestion_id,
    if(p.description_source = 'llm', ifNull(o.model_provider, ''), 'deterministic') AS model_provider,
    if(p.description_source = 'llm', ifNull(o.model_name, ''), 'se-company-info-rules') AS model_name,
    if(p.description_source = 'llm', ifNull(o.prompt_version, ''), 'se-company-info-rules-v1') AS prompt_version,
    p.last_source_run_id AS source_run_id,
    p.last_resolved_at AS resolved_at
FROM pivot AS p
INNER JOIN publishable AS pub ON pub.company_id = p.company_id
LEFT JOIN legal_form_labels AS lf ON lf.code = p.legal_form_code
LEFT JOIN description_candidates AS dc ON dc.company_id = p.company_id
LEFT JOIN wikidata_ids AS w ON w.company_id = p.company_id
LEFT JOIN leis AS e ON e.company_id = p.company_id
LEFT JOIN observations AS o ON o.suggestion_ref = p.description_source_record_uid
WHERE p.legal_name != '' AND notEmpty(p.source_record_uids)"""


def render_projection_sql(registry: DatatypeRegistry) -> str:
    """The projection as one INSERT -- what the registry's field = '*' row carries."""
    columns = ", ".join(SE_COMPANY_INFO_COLUMNS)
    return f"INSERT INTO {SE_COMPANY_INFO}\n    ({columns})\n{render_projection_select_sql(registry)}"
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run --frozen --no-sync pytest tests/test_se_company_field_sql.py -q -p no:warnings`
Expected: PASS (5 tests). If the pinned-text test fails, diff the two texts (`pytest -vv`) and fix the RENDERER's whitespace to the pin, not the other way round -- the pin is the text executed in Task 7's prototype.

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/fields/sql.py \
        corpscout/services/dagster_v3/tests/test_se_company_field_sql.py
git commit -m "feat(se): render the resolve and projection statements from the registry

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---

### Task 7: Execute the generated SQL in clickhouse-local

**Files:**
- Modify: `tests/test_se_company_field_sql.py` (append the harness below the unit tests)

**Interfaces:**
- Consumes: `render_resolve_sql`, `render_projection_sql`, `INFO_REGISTRY`, `field_by_name`; `tests/se_company_ddl.MIGRATIONS_DIR`; `_clickhouse_local_command` and `_literal` from `tests/test_se_company_person_clickhouse_local.py` (docker fallback `clickhouse/clickhouse-server:26.5`, skips without docker).
- Covers every path of spec 7.4: decision beats winner (ALPHA legal_name), released decision falls back (BETA: an older value row then a newer NULL row), precedence beats recency (EPSILON: scb older than bolagsverket still wins), same-source recency (BETA: two scb records), unlisted source filtered (EPSILON ratsit; GAMMA status from ratsit only -> no row), agreement counting on the compare key (ALPHA description: esef + wikidata agree; employee_count keys), no row when nothing (NOBODY), a decision that copied a candidate carries that candidate's value_json (ALPHA employee_count), and the backoffice sequence (new decision -> one-field resolve -> re-pivot). The projection produces the expected wide rows and withholds GAMMA (no scb/bolagsverket legal name).
- "Empty candidates filtered": the candidate table's `has_value` CHECK rejects an empty or whitespace-only value at INSERT, so the policy filter cannot be exercised through the table; it is pinned as text (Task 3) and the eligibility path it shares -- unlisted sources -- is executed here.

- [ ] **Step 1: Append the harness to `tests/test_se_company_field_sql.py`**

Append the block below; move its `import` / `from ... import` lines up into the module's existing import block (the four `import re` ... `from tests.test_se_company_person_clickhouse_local import ...` lines), keeping everything else in place.

```python
# --------------------------------------------------------------------------------------
# clickhouse-local harness: the statements above, executed against the migrations' DDL
# with seeded candidates, decisions, labels, artifacts and one observation. Runs twice,
# with and without join_use_nulls, like the info harness: every LEFT JOIN miss must read
# the same under both.

import re
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from tests.se_company_ddl import MIGRATIONS_DIR
from tests.test_se_company_person_clickhouse_local import _clickhouse_local_command, _literal

# Every migration that creates or alters one of NEEDED_TABLES, in ledger order. GRANT
# statements and statements aimed at other tables are filtered out per statement.
NEEDED_MIGRATIONS = (
    "000150_corpscout_se_translations.up.sql",
    "000297_corpscout_se_company_info.up.sql",
    "000299_corpscout_se_company_info_sole_traders.up.sql",
    "000301_corpscout_se_company_info_description_sv.up.sql",
    "000304_corpscout_se_company_info_llm_enhanced.up.sql",
    "000305_corpscout_se_code_labels_swedish.up.sql",
    "000306_corpscout_se_company_info_legal_form_label.up.sql",
    "000365_corpscout_se_company_info_esef_enrichment.up.sql",
    "000371_corpscout_se_company_info_field_value.up.sql",
    "000373_corpscout_se_company_field_tables.up.sql",
    "000374_corpscout_se_company_info_field_columns.up.sql",
    "000375_corpscout_se_company_info_field_value_registry_checks.up.sql",
)
NEEDED_TABLES = frozenset({
    "se_code_labels", "se_company_info", "se_company_info_esef", "se_company_info_wikidata",
    "se_company_info_enrichment_observation", "se_company_info_field_value",
    "se_company_field_registry", "se_company_field_candidate", "se_company_field",
})
_TABLE_RE = re.compile(r"^(?:CREATE TABLE(?: IF NOT EXISTS)?|ALTER TABLE)\s+corpscout\.(\w+)", re.IGNORECASE)
_PARAMETER_RE = re.compile(r"\{([a-z_]+):[A-Za-z0-9(), ]+\}")


def _schema_statements(migrations: tuple[str, ...]) -> list[str]:
    statements: list[str] = []
    for name in migrations:
        text = (MIGRATIONS_DIR / name).read_text(encoding="utf-8")
        for raw in text.split(";"):
            statement = "\n".join(line for line in raw.splitlines() if not line.strip().startswith("--")).strip()
            if not statement:
                continue
            if statement.upper().startswith("CREATE DATABASE"):
                statements.append(statement)
                continue
            match = _TABLE_RE.match(statement)
            if match and match.group(1) in NEEDED_TABLES:
                statements.append(statement)
    return statements


def render_named(sql: str, parameters: dict[str, Any]) -> str:
    """Inline the server-side ``{name:Type}`` placeholders for the CLI. clickhouse-local
    takes --param_name flags, but one script here runs a statement with several parameter
    sets, so the literals are substituted in the text instead."""

    def literal(match: re.Match[str]) -> str:
        value = parameters[match.group(1)]
        if isinstance(value, tuple | list):
            return "[" + ", ".join(_literal(item) for item in value) + "]"
        return _literal(value)

    rendered = _PARAMETER_RE.sub(literal, sql)
    assert not _PARAMETER_RE.search(rendered), rendered
    return rendered


ALPHA = "5565200028"    # the full row: every field, two decisions, all artifacts
BETA = "5560125220"     # release: an older decision then a NULL row; two scb records
EPSILON = "5567654321"  # precedence over recency; an unlisted source; an llm description
GAMMA = "5569999991"    # legal name from wikidata only: resolved, never published
NOBODY = "5560000010"   # in the company set, no candidates, no decisions
COMPANIES = (ALPHA, BETA, EPSILON, GAMMA, NOBODY)
RUN_ID = "fixture-run-1"
T_RESOLVED = datetime(2026, 9, 2, 12, 0, tzinfo=UTC)
T_RESOLVED_2 = datetime(2026, 9, 2, 12, 5, tzinfo=UTC)
D_ALPHA_NAME = "11111111-1111-1111-1111-111111111111"
D_ALPHA_EMPLOYEES = "22222222-2222-2222-2222-222222222222"
D_BETA_OLD = "33333333-3333-3333-3333-333333333333"
D_BETA_RELEASE = "44444444-4444-4444-4444-444444444444"
SUGGESTION = "55555555-5555-5555-5555-555555555555"
D_EPSILON_NAME = "66666666-6666-6666-6666-666666666666"

FIXTURE = f"""
INSERT INTO corpscout.se_company_field_candidate
    (company_id, field, source, source_record_uid, value, value_json, observed_at, extracted_at, extractor_version, source_run_id)
VALUES
    ('{ALPHA}', 'legal_name', 'scb', 'scb-a', 'Alpha AB', '', '2026-08-01 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{ALPHA}', 'legal_name', 'wikidata', 'wikidata:Q1', 'Alpha', '', '2026-08-02 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{ALPHA}', 'legal_name', 'bolagsverket', 'bv-a', 'Alpha Aktiebolag', '', '2026-07-01 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{ALPHA}', 'legal_form_code', 'bolagsverket', 'bv-a', 'AB-ORGFO', '', '2026-07-01 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{ALPHA}', 'status', 'bolagsverket', 'bv-a', 'active', '', '2026-07-01 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{ALPHA}', 'incorporation_date', 'bolagsverket', 'bv-a', '2001-02-03', '', '2026-07-01 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{ALPHA}', 'description', 'esef', 'doc-1', 'Alpha builds payment software.', '{{"language":"en"}}', '2025-04-02 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{ALPHA}', 'description', 'wikidata', 'wikidata:Q1', 'alpha builds payment software.', '{{"language":"en"}}', '2026-08-02 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{ALPHA}', 'description', 'scb', 'scb-a', 'IT consultants.', '{{"language":"en"}}', '2026-08-01 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{ALPHA}', 'description_sv', 'scb', 'scb-a', 'IT-konsulter.', '{{"language":"sv"}}', '2026-08-01 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{ALPHA}', 'primary_sni_code', 'scb', 'scb-a', '62010', '', '2026-08-01 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{ALPHA}', 'primary_nace_code', 'scb', 'scb-a', '62.01', '', '2026-08-01 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{ALPHA}', 'industry_label_en', 'scb', 'scb-a', 'Computer programming activities', '', '2026-08-01 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{ALPHA}', 'website', 'domains', 'fp-1', 'https://alpha.example', '', '2026-08-03 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{ALPHA}', 'website', 'wikidata', 'wikidata:Q1', 'http://alpha.example/', '', '2026-08-02 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{ALPHA}', 'employee_count', 'esef', 'doc-1', '1 200', '{{"compare_key":"1200","count":1200,"as_of":"2024-12-31","period":"FY2024"}}', '2024-12-31 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{ALPHA}', 'employee_count', 'wikidata', 'wikidata:Q1', '1150', '{{"compare_key":"1150","count":1150}}', '2026-08-02 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{ALPHA}', 'latest_revenue', 'esef', 'doc-1', 'SEK 123,456,789.50', '{{"compare_key":"SEK:2024:123456789.50","amount":123456789.5,"currency":"SEK","amount_usd":11500000.25,"fiscal_year":2024,"period_end":"2024-12-31"}}', '2024-12-31 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{BETA}', 'legal_name', 'scb', 'scb-b1', 'Beta AB', '', '2026-08-01 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{BETA}', 'legal_name', 'scb', 'scb-b2', 'Beta Holding AB', '', '2026-08-05 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{EPSILON}', 'legal_name', 'bolagsverket', 'bv-e', 'Epsilon AB', '', '2026-08-01 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{EPSILON}', 'legal_name', 'scb', 'scb-e', 'Epsilon Aktiebolag', '', '2026-07-01 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{EPSILON}', 'legal_name', 'ratsit', 'ratsit-e', 'Epsilon (ratsit)', '', '2026-08-30 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{EPSILON}', 'description', 'llm', '{SUGGESTION}', 'Epsilon makes widgets.', '{{"language":"en"}}', '2026-08-10 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{EPSILON}', 'description', 'scb', 'scb-e', 'Widget maker.', '{{"language":"en"}}', '2026-08-01 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{GAMMA}', 'legal_name', 'wikidata', 'wikidata:Q9', 'Gamma', '', '2026-08-02 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{GAMMA}', 'status', 'ratsit', 'ratsit-g', 'active', '', '2026-08-02 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}');

INSERT INTO corpscout.se_company_info_field_value
    (value_id, company_id, field, value, source, source_ref, source_at, decided_by, note, created_at)
VALUES
    ('{D_ALPHA_NAME}', '{ALPHA}', 'legal_name', 'Alpha Group AB', 'reviewer', '', NULL, 'backoffice', '', '2026-08-25 10:00:00'),
    ('{D_ALPHA_EMPLOYEES}', '{ALPHA}', 'employee_count', '1150', 'wikidata', 'wikidata:Q1', '2026-08-02 00:00:00', 'backoffice', '', '2026-08-25 10:00:00'),
    ('{D_BETA_OLD}', '{BETA}', 'legal_name', 'Beta Corp', 'reviewer', '', NULL, 'backoffice', '', '2026-08-10 10:00:00'),
    ('{D_BETA_RELEASE}', '{BETA}', 'legal_name', NULL, 'reviewer', '', NULL, 'backoffice', '', '2026-08-11 10:00:00');

INSERT INTO corpscout.se_code_labels (code_type, code, label_en, label_sv, version)
VALUES ('legal_form', 'AB-ORGFO', 'Limited company (aktiebolag)', 'Aktiebolag', toDateTime('2026-08-02 00:00:00'));

INSERT INTO corpscout.se_company_info_wikidata
    (company_id, source_record_uid, observed_at, source_run_id, wikidata_id, wikidata_url, name)
VALUES ('{ALPHA}', 'wikidata:Q1', '2026-08-02 00:00:00', '{RUN_ID}', 'Q1', 'https://www.wikidata.org/wiki/Q1', 'Alpha');

INSERT INTO corpscout.se_company_info_esef
    (company_id, source_record_uid, observed_at, source_run_id, source_document_id, lei, fiscal_year,
     company_description, description_language, description_confidence)
VALUES ('{ALPHA}', 'doc-1', '2025-04-02 00:00:00', '{RUN_ID}', 'doc-1', '5493001KJTIIGC8Y1R12', 2024,
        'Alpha builds payment software.', 'en', 0.9);

INSERT INTO corpscout.se_company_info_enrichment_observation
    (suggestion_id, company_id, input_hash, suggestion, raw_response, model_provider, model_name, prompt_version,
     prompt_tokens, completion_tokens, source_run_id, created_at)
VALUES ('{SUGGESTION}', '{EPSILON}', repeat('0', 64), '{{}}', '', 'deepseek', 'deepseek-v4-flash',
        'se-company-info-description-v3', 1, 1, '{RUN_ID}', '2026-08-10 00:00:00');
""".strip()

# The backoffice sequence: a new decision, the one-field resolve for that company alone,
# then the projection for that company alone.
EPSILON_DECISION_SQL = f"""
INSERT INTO corpscout.se_company_info_field_value
    (value_id, company_id, field, value, source, source_ref, source_at, decided_by, note, created_at)
VALUES ('{D_EPSILON_NAME}', '{EPSILON}', 'legal_name', 'Epsilon Group AB', 'reviewer', '', NULL, 'backoffice', '', '2026-08-26 10:00:00');
""".strip()

RESOLVED_COLUMNS = (
    "company_id", "field", "value", "source", "source_record_uid", "observed_at", "decision_id",
    "candidate_count", "agreeing_sources", "value_json", "policy_name", "policy_version",
    "registry_version", "source_run_id", "resolved_at",
)
RESOLVED_SQL = (
    "SELECT company_id, field, value, source, source_record_uid, toString(observed_at), "
    "ifNull(toString(decision_id), ''), candidate_count, agreeing_sources, value_json, policy_name, "
    "policy_version, registry_version, source_run_id, toString(resolved_at) "
    "FROM corpscout.se_company_field FINAL ORDER BY company_id, field"
)
# Arrays are selected bare (TSV renders them as ['a','b']); Nullables through ifNull(toString()).
WIDE_COLUMNS = (
    "company_id", "legal_name", "legal_form_code", "legal_form_label_en", "legal_form_label_sv", "status",
    "incorporation_date", "description", "description_sv", "description_language", "llm_enhanced",
    "description_sources", "description_source_record_uids", "description_source_count",
    "primary_nace_code", "primary_sni_code", "industry_label_en", "website", "employee_count",
    "employee_count_as_of", "latest_revenue_amount", "latest_revenue_currency", "latest_revenue_amount_usd",
    "latest_revenue_fiscal_year", "wikidata_id", "lei", "source_record_uids", "evidence_hash_count",
    "correction_ids", "suggestion_id", "model_provider", "model_name", "prompt_version", "source_run_id",
    "resolved_at",
)
WIDE_SQL = (
    "SELECT company_id, legal_name, ifNull(legal_form_code, ''), legal_form_label_en, legal_form_label_sv, status, "
    "ifNull(toString(incorporation_date), ''), ifNull(description, ''), ifNull(description_sv, ''), "
    "description_language, toUInt8(llm_enhanced), description_sources, description_source_record_uids, "
    "description_source_count, primary_nace_code, primary_sni_code, industry_label_en, ifNull(website, ''), "
    "ifNull(toString(employee_count), ''), ifNull(toString(employee_count_as_of), ''), "
    "ifNull(toString(toFloat64(latest_revenue_amount)), ''), latest_revenue_currency, "
    "ifNull(toString(toFloat64(latest_revenue_amount_usd)), ''), ifNull(toString(latest_revenue_fiscal_year), ''), "
    "ifNull(wikidata_id, ''), ifNull(lei, ''), source_record_uids, length(evidence_hashes), correction_ids, "
    "ifNull(toString(suggestion_id), ''), model_provider, model_name, prompt_version, source_run_id, "
    "toString(resolved_at) FROM corpscout.se_company_info FINAL ORDER BY company_id"
)


def _marked(label: str, query: str) -> str:
    return f"SELECT '@@{label}';\n{query} FORMAT TSV;\n"


def _script(*, join_use_nulls: int) -> str:
    parts: list[str] = []
    if join_use_nulls:
        parts.append("SET join_use_nulls = 1;")
    parts.append(";\n".join(_schema_statements(NEEDED_MIGRATIONS)) + ";")
    parts.append(FIXTURE)
    params = {"company_ids": COMPANIES, "source_run_id": RUN_ID, "resolved_at": T_RESOLVED}
    for field in INFO_REGISTRY.fields:
        parts.append(render_named(render_resolve_sql(INFO_REGISTRY, field), {**params, "field": field.name}) + ";")
    parts.append(_marked("resolved", RESOLVED_SQL))
    parts.append(render_named(render_projection_sql(INFO_REGISTRY), {"company_ids": COMPANIES}) + ";")
    parts.append(_marked("wide", WIDE_SQL))
    parts.append(_marked(
        "wide_columns",
        "SELECT name FROM system.columns WHERE database = 'corpscout' AND table = 'se_company_info' ORDER BY position",
    ))
    parts.append(EPSILON_DECISION_SQL)
    legal_name = field_by_name(INFO_REGISTRY, "legal_name")
    parts.append(render_named(render_resolve_sql(INFO_REGISTRY, legal_name), {
        "company_ids": (EPSILON,), "source_run_id": "backoffice-1", "resolved_at": T_RESOLVED_2,
        "field": "legal_name"}) + ";")
    parts.append(_marked("resolved_after_decision", RESOLVED_SQL))
    parts.append(render_named(render_projection_sql(INFO_REGISTRY), {"company_ids": (EPSILON,)}) + ";")
    parts.append(_marked("wide_after_decision", WIDE_SQL))
    return "\n".join(parts) + "\n"


@pytest.fixture(scope="module", params=(0, 1), ids=("join_use_nulls_off", "join_use_nulls_on"))
def sections(request: pytest.FixtureRequest) -> dict[str, list[list[str]]]:
    command = _clickhouse_local_command()
    try:
        completed = subprocess.run(
            command, input=_script(join_use_nulls=request.param), capture_output=True, text=True, timeout=900
        )
    except (OSError, subprocess.TimeoutExpired) as exc:  # pragma: no cover - env
        pytest.skip(f"clickhouse-local is unusable here: {exc}")
    assert completed.returncode == 0, completed.stderr or completed.stdout
    result: dict[str, list[list[str]]] = {}
    current = ""
    for line in completed.stdout.splitlines():
        if line.startswith("@@"):
            current = line[2:]
            result[current] = []
        elif current and line.strip():
            result[current].append(line.split("\t"))
    return result


def _resolved(rows: list[list[str]]) -> dict[tuple[str, str], dict[str, str]]:
    return {(row[0], row[1]): dict(zip(RESOLVED_COLUMNS, row, strict=True)) for row in rows}


def _wide(rows: list[list[str]]) -> dict[str, dict[str, str]]:
    return {row[0]: dict(zip(WIDE_COLUMNS, row, strict=True)) for row in rows}


@pytest.mark.integration
def test_a_decision_beats_the_winner(sections: dict[str, list[list[str]]]) -> None:
    row = _resolved(sections["resolved"])[(ALPHA, "legal_name")]
    assert (row["value"], row["source"], row["source_record_uid"]) == ("Alpha Group AB", "reviewer", "")
    assert row["decision_id"] == D_ALPHA_NAME
    assert row["observed_at"] == "2026-08-25 10:00:00.000"  # source_at NULL -> the decision's created_at
    # The three eligible candidates are still counted; a typed value agrees with none of them.
    assert (row["candidate_count"], row["agreeing_sources"], row["value_json"]) == ("3", "[]", "")


@pytest.mark.integration
def test_a_released_decision_falls_back_to_the_winner(sections: dict[str, list[list[str]]]) -> None:
    """BETA's newest decision row is a release (value NULL) written after a value row. A
    bare argMax(value, ...) would skip the NULL row and resurrect 'Beta Corp'; the tuple
    aggregation keeps it, so the winner is used -- and among BETA's two scb records the
    newer observation wins (same-source recency)."""
    row = _resolved(sections["resolved"])[(BETA, "legal_name")]
    assert (row["value"], row["source"], row["source_record_uid"]) == ("Beta Holding AB", "scb", "scb-b2")
    assert row["decision_id"] == "" and row["observed_at"] == "2026-08-05 00:00:00.000"
    assert (row["candidate_count"], row["agreeing_sources"]) == ("2", "['scb']")


@pytest.mark.integration
def test_precedence_beats_recency_and_unlisted_sources_are_ignored(sections: dict[str, list[list[str]]]) -> None:
    row = _resolved(sections["resolved"])[(EPSILON, "legal_name")]
    # scb is rank 1 although bolagsverket observed a month later; ratsit is not in the tuple.
    assert (row["value"], row["source"], row["source_record_uid"]) == ("Epsilon Aktiebolag", "scb", "scb-e")
    assert (row["candidate_count"], row["agreeing_sources"]) == ("2", "['scb']")
    # GAMMA's only status candidate is from ratsit, which status does not list: no row.
    assert (GAMMA, "status") not in _resolved(sections["resolved"])


@pytest.mark.integration
def test_agreement_is_counted_on_the_compare_key(sections: dict[str, list[list[str]]]) -> None:
    resolved = _resolved(sections["resolved"])
    description = resolved[(ALPHA, "description")]
    # llm is absent, so esef (rank 2) wins; wikidata's text differs only in case -> agrees.
    assert (description["value"], description["source"]) == ("Alpha builds payment software.", "esef")
    assert (description["candidate_count"], description["agreeing_sources"]) == ("3", "['esef','wikidata']")
    assert description["value_json"] == '{"language":"en"}'
    employees = resolved[(ALPHA, "employee_count")]
    # A decision that copied the wikidata candidate carries that candidate's value_json, and
    # its compare_key ('1150') agrees with wikidata alone.
    assert (employees["value"], employees["source"], employees["decision_id"]) == ("1150", "wikidata", D_ALPHA_EMPLOYEES)
    assert employees["value_json"] == '{"compare_key":"1150","count":1150}'
    assert (employees["candidate_count"], employees["agreeing_sources"]) == ("2", "['wikidata']")


@pytest.mark.integration
def test_no_row_when_nothing_and_every_row_carries_provenance(sections: dict[str, list[list[str]]]) -> None:
    resolved = _resolved(sections["resolved"])
    assert not [key for key in resolved if key[0] == NOBODY]
    assert len(resolved) == 16  # ALPHA 12 fields + BETA 1 + EPSILON 2 + GAMMA 1
    for row in resolved.values():
        assert (row["policy_name"], row["policy_version"], row["registry_version"]) == (
            "source_precedence", "source_precedence-v1", "se-info-v1")
        assert (row["source_run_id"], row["resolved_at"]) == (RUN_ID, "2026-09-02 12:00:00.000")


@pytest.mark.integration
def test_the_projection_builds_the_expected_wide_rows(sections: dict[str, list[list[str]]]) -> None:
    wide = _wide(sections["wide"])
    assert set(wide) == {ALPHA, BETA, EPSILON}  # GAMMA: no scb/bolagsverket legal name; NOBODY: nothing
    assert wide[ALPHA] == {
        "company_id": ALPHA, "legal_name": "Alpha Group AB", "legal_form_code": "AB-ORGFO",
        "legal_form_label_en": "Limited company (aktiebolag)", "legal_form_label_sv": "Aktiebolag",
        "status": "active", "incorporation_date": "2001-02-03",
        "description": "Alpha builds payment software.", "description_sv": "IT-konsulter.",
        "description_language": "en", "llm_enhanced": "0",
        "description_sources": "['esef','scb','wikidata']",
        "description_source_record_uids": "['doc-1','scb-a','wikidata:Q1']", "description_source_count": "3",
        "primary_nace_code": "62.01", "primary_sni_code": "62010",
        "industry_label_en": "Computer programming activities", "website": "https://alpha.example",
        "employee_count": "1150", "employee_count_as_of": "",  # the decided wikidata value has no as_of
        "latest_revenue_amount": "123456789.5", "latest_revenue_currency": "SEK",
        "latest_revenue_amount_usd": "11500000.25", "latest_revenue_fiscal_year": "2024",
        "wikidata_id": "Q1", "lei": "5493001KJTIIGC8Y1R12",
        "source_record_uids": "['bv-a','doc-1','fp-1','scb-a','wikidata:Q1']",  # unique, no reviewer ''
        "evidence_hash_count": "11",  # one per winning candidate row: bv-a x3, doc-1 x2, fp-1, scb-a x4, Q1
        "correction_ids": f"['{D_ALPHA_NAME}','{D_ALPHA_EMPLOYEES}']",
        "suggestion_id": "", "model_provider": "deterministic", "model_name": "se-company-info-rules",
        "prompt_version": "se-company-info-rules-v1", "source_run_id": RUN_ID,
        "resolved_at": "2026-09-02 12:00:00.000",
    }
    epsilon = wide[EPSILON]
    assert (epsilon["legal_name"], epsilon["description"], epsilon["llm_enhanced"]) == ("Epsilon Aktiebolag", "Epsilon makes widgets.", "1")
    assert (epsilon["suggestion_id"], epsilon["model_provider"], epsilon["model_name"], epsilon["prompt_version"]) == (
        SUGGESTION, "deepseek", "deepseek-v4-flash", "se-company-info-description-v3")
    assert (epsilon["description_sources"], epsilon["description_source_count"]) == ("['llm','scb']", "2")
    assert (epsilon["source_record_uids"], epsilon["evidence_hash_count"]) == (f"['{SUGGESTION}','scb-e']", "2")
    beta = wide[BETA]
    assert (beta["legal_name"], beta["legal_form_code"], beta["status"], beta["description"]) == ("Beta Holding AB", "", "", "")
    assert (beta["description_sources"], beta["description_source_count"], beta["correction_ids"]) == ("[]", "0", "[]")
    assert (beta["employee_count"], beta["latest_revenue_amount"], beta["website"]) == ("", "", "")
    assert (beta["source_record_uids"], beta["evidence_hash_count"]) == ("['scb-b2']", "1")


@pytest.mark.integration
def test_the_deployed_wide_columns_are_what_the_ddl_replay_says(sections: dict[str, list[list[str]]]) -> None:
    from tests.se_company_ddl import declared_columns

    assert [row[0] for row in sections["wide_columns"]] == declared_columns("se_company_info")


@pytest.mark.integration
def test_a_decision_re_resolves_one_field_and_re_pivots_one_company(sections: dict[str, list[list[str]]]) -> None:
    """Spec 9's sequence, executed: after a decision the backoffice runs the field's
    statement for that company, then the projection. Only that field's row moves
    (ReplacingMergeTree(resolved_at), newer version wins); the wide row picks it up with
    the decision id, the new run id and the new resolved_at."""
    resolved = _resolved(sections["resolved_after_decision"])
    assert (resolved[(EPSILON, "legal_name")]["value"], resolved[(EPSILON, "legal_name")]["decision_id"]) == (
        "Epsilon Group AB", D_EPSILON_NAME)
    assert (resolved[(EPSILON, "legal_name")]["source_run_id"], resolved[(EPSILON, "legal_name")]["resolved_at"]) == (
        "backoffice-1", "2026-09-02 12:05:00.000")
    assert (resolved[(EPSILON, "description")]["source_run_id"], resolved[(EPSILON, "description")]["resolved_at"]) == (
        RUN_ID, "2026-09-02 12:00:00.000")
    assert len(resolved) == 16  # one version per (company, field) under FINAL
    wide = _wide(sections["wide_after_decision"])
    epsilon = wide[EPSILON]
    assert (epsilon["legal_name"], epsilon["correction_ids"]) == ("Epsilon Group AB", f"['{D_EPSILON_NAME}']")
    assert (epsilon["source_run_id"], epsilon["resolved_at"]) == ("backoffice-1", "2026-09-02 12:05:00.000")
    assert epsilon["source_record_uids"] == f"['{SUGGESTION}']"  # scb-e no longer wins anything
    assert wide[ALPHA]["resolved_at"] == "2026-09-02 12:00:00.000"  # untouched by EPSILON's re-pivot
```

- [ ] **Step 2: Run the harness**

Run: `uv run --frozen --no-sync pytest tests/test_se_company_field_sql.py -q -p no:warnings`
Expected: PASS -- 5 unit tests + 8 harness tests x 2 join settings (docker pulls `clickhouse/clickhouse-server:26.5` on first use; a `skip` means docker is not running, not that the SQL works). The statements and fixture values above were executed on 26.5 while this plan was written and produced exactly these rows; a mismatch is a transcription slip on one side -- read `completed.stderr` for a ClickHouse error (the script aborts on the first one) before touching either.

- [ ] **Step 3: Commit**

```bash
git add corpscout/services/dagster_v3/tests/test_se_company_field_sql.py
git commit -m "test(se): execute the registry resolve and projection SQL in clickhouse-local

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---

### Task 8: `export.py` -- registry rows and the export asset

**Files:**
- Create: `src/dagster_v3/defs/se_company/fields/export.py`
- Test: `tests/test_se_company_field_export.py`

**Interfaces:**
- Consumes: `INFO_REGISTRY`, `policy_for`, `render_resolve_sql`, `render_projection_sql`, `SE_COMPANY_FIELD_REGISTRY`, `SE_COMPANY_FIELD_REGISTRY_COLUMNS`; `assert_clickhouse_tables_exist(clickhouse, *, database, tables)` from `dagster_v3.defs.clickhouse.resolved`.
- Produces (fixed names): `registry_rows(registry, *, rendered_at: datetime) -> list[dict[str, object]]` -- one dict per field (keys = `SE_COMPANY_FIELD_REGISTRY_COLUMNS`, in that order) plus the projection row (`field = '*'`, `value_type = 'projection'`, `display_group = ''`, `structured = False`, `python_only = False`, `sources = []`, `policy_name = ''`, `policy_version = ''`, `resolve_sql = render_projection_sql(registry)`); `GROUP_NAME = "se_company_fields"`; asset `se_company_field_registry_clickhouse` (no deps; one `INSERT ... VALUES` of every row with `version = rendered_at`, so a re-export is a new version and `argMax(..., version)` readers switch without deletes -- the `se_code_labels_clickhouse` pattern, `defs/sweden_company/translation.py:226-273`).
- Readers (the resolve plan, the backoffice) select with `argMax(resolve_sql, version)` grouped by `(datatype, country, field)`.

- [ ] **Step 1: Write the failing tests**

`tests/test_se_company_field_export.py`:

```python
"""The registry export: rows match the module, the asset seeds them in one insert."""

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

import dagster as dg

from dagster_v3.defs.se_company.fields.export import (
    GROUP_NAME,
    registry_rows,
    se_company_field_registry_clickhouse,
)
from dagster_v3.defs.se_company.fields.registry import INFO_REGISTRY, field_by_name, field_names
from dagster_v3.defs.se_company.fields.sql import render_projection_sql, render_resolve_sql
from dagster_v3.defs.se_company.fields.tables import SE_COMPANY_FIELD_REGISTRY_COLUMNS

RENDERED_AT = datetime(2026, 9, 2, 12, 0, tzinfo=UTC)


def test_one_row_per_field_plus_the_projection_row() -> None:
    rows = registry_rows(INFO_REGISTRY, rendered_at=RENDERED_AT)
    assert [row["field"] for row in rows] == [*field_names(INFO_REGISTRY), "*"]
    for row in rows:
        assert tuple(row) == SE_COMPANY_FIELD_REGISTRY_COLUMNS  # dict order == positional insert list
        assert (row["datatype"], row["country"], row["registry_version"], row["version"]) == (
            "info", "SE", "se-info-v1", RENDERED_AT)


def test_field_rows_carry_the_registry_and_the_generated_statement() -> None:
    rows = {row["field"]: row for row in registry_rows(INFO_REGISTRY, rendered_at=RENDERED_AT)}
    website = rows["website"]
    assert website["sources"] == ["domains", "wikidata"]
    assert (website["value_type"], website["display_group"], website["structured"], website["python_only"]) == (
        "url", "scale", False, False)
    assert (website["policy_name"], website["policy_version"]) == ("source_precedence", "source_precedence-v1")
    assert website["resolve_sql"] == render_resolve_sql(INFO_REGISTRY, field_by_name(INFO_REGISTRY, "website"))
    assert rows["employee_count"]["structured"] is True


def test_the_projection_row_carries_the_wide_statement() -> None:
    row = registry_rows(INFO_REGISTRY, rendered_at=RENDERED_AT)[-1]
    assert (row["field"], row["value_type"], row["display_group"], row["structured"], row["python_only"]) == (
        "*", "projection", "", False, False)
    assert (row["sources"], row["policy_name"], row["policy_version"]) == ([], "", "")
    assert row["resolve_sql"] == render_projection_sql(INFO_REGISTRY)


class _FakeClient:
    def __init__(self) -> None:
        self.inserts: list[tuple[str, list[tuple[Any, ...]]]] = []

    def execute(self, sql: str, params: Any = None) -> list[tuple[Any, ...]]:
        if "system.tables" in sql:
            return [("se_company_field_registry",)]
        self.inserts.append((sql, list(params)))
        return []


class _FakeClickhouse:
    def __init__(self, client: _FakeClient) -> None:
        self._client = client

    @contextmanager
    def get_connection(self) -> Iterator[_FakeClient]:
        yield self._client


def test_the_asset_seeds_every_row_through_one_insert() -> None:
    client = _FakeClient()
    result = se_company_field_registry_clickhouse.node_def.compute_fn.decorated_fn(
        dg.build_asset_context(), _FakeClickhouse(client)
    )
    (sql, rows), = client.inserts
    assert sql == (
        "INSERT INTO corpscout.se_company_field_registry ("
        + ", ".join(SE_COMPANY_FIELD_REGISTRY_COLUMNS) + ") VALUES"
    )
    assert len(rows) == len(INFO_REGISTRY.fields) + 1
    assert rows[0][:3] == ("info", "SE", "legal_name") and rows[-1][2:4] == ("*", "projection")
    versions = {row[-1] for row in rows}
    assert len(versions) == 1 and next(iter(versions)).tzinfo is UTC  # one version per export
    assert result.metadata["rows"] == len(rows) and result.metadata["registry_version"] == "se-info-v1"
    assert result.metadata["fields"] == len(INFO_REGISTRY.fields)


def test_the_asset_is_wired_into_the_definitions() -> None:
    from dagster_v3.definitions import defs as load_defs

    repository = load_defs().get_repository_def()
    node = repository.asset_graph.get(dg.AssetKey("se_company_field_registry_clickhouse"))
    assert node.group_name == GROUP_NAME == "se_company_fields"
    assert node.parent_keys == set()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_field_export.py -q -p no:warnings`
Expected: FAIL -- `ModuleNotFoundError: No module named 'dagster_v3.defs.se_company.fields.export'`.

- [ ] **Step 3: Write `export.py`**

`src/dagster_v3/defs/se_company/fields/export.py`:

```python
"""Exports the registry and its generated SQL to corpscout.se_company_field_registry.

One row per field plus one ``field = '*'`` row carrying the wide projection statement, so
both runners -- the Dagster resolve asset and the backoffice's per-company resolve after a
decision -- read every statement they need from this one table with
``argMax(resolve_sql, version)``. Every export is a NEW version of each row
(ReplacingMergeTree(version)), so a registry change becomes effective without deletes,
exactly like se_code_labels_clickhouse re-seeds the label dictionary.

Assets
  se_company_field_registry_clickhouse -> corpscout.se_company_field_registry
"""

from datetime import UTC, datetime

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.se_company.fields.policies import policy_for
from dagster_v3.defs.se_company.fields.registry import INFO_REGISTRY, DatatypeRegistry
from dagster_v3.defs.se_company.fields.sql import render_projection_sql, render_resolve_sql
from dagster_v3.defs.se_company.fields.tables import (
    SE_COMPANY_FIELD_REGISTRY,
    SE_COMPANY_FIELD_REGISTRY_COLUMNS,
)

GROUP_NAME = "se_company_fields"
DATABASE = "corpscout"
PROJECTION_FIELD = "*"
PROJECTION_VALUE_TYPE = "projection"


def registry_rows(registry: DatatypeRegistry, *, rendered_at: datetime) -> list[dict[str, object]]:
    """One dict per field in SE_COMPANY_FIELD_REGISTRY_COLUMNS key order, then the
    projection row. ``rendered_at`` is the row version shared by the whole export."""
    rows: list[dict[str, object]] = []
    for field in registry.fields:
        policy = policy_for(field)
        rows.append({
            "datatype": registry.datatype,
            "country": registry.country,
            "field": field.name,
            "value_type": field.value_type,
            "display_group": field.display_group,
            "structured": field.structured,
            "python_only": field.python_only,
            "sources": list(field.sources),
            "policy_name": policy.name,
            "policy_version": policy.version,
            "resolve_sql": render_resolve_sql(registry, field),
            "registry_version": registry.version,
            "version": rendered_at,
        })
    rows.append({
        "datatype": registry.datatype,
        "country": registry.country,
        "field": PROJECTION_FIELD,
        "value_type": PROJECTION_VALUE_TYPE,
        "display_group": "",
        "structured": False,
        "python_only": False,
        "sources": [],
        "policy_name": "",
        "policy_version": "",
        "resolve_sql": render_projection_sql(registry),
        "registry_version": registry.version,
        "version": rendered_at,
    })
    return rows


@dg.asset(
    name="se_company_field_registry_clickhouse",
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse"},
    metadata={"table": SE_COMPANY_FIELD_REGISTRY},
    description=(
        "Exports the SE info field registry (fields, sources in precedence order, policy "
        "bindings) and every generated resolve statement plus the wide projection statement "
        "to corpscout.se_company_field_registry. ReplacingMergeTree(version) + argMax in the "
        "readers make a re-export effective without deletes."
    ),
)
def se_company_field_registry_clickhouse(
    context: dg.AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(clickhouse, database=DATABASE, tables=("se_company_field_registry",))
    rows = registry_rows(INFO_REGISTRY, rendered_at=datetime.now(UTC))
    values = [tuple(row[column] for column in SE_COMPANY_FIELD_REGISTRY_COLUMNS) for row in rows]
    columns = ", ".join(SE_COMPANY_FIELD_REGISTRY_COLUMNS)
    with clickhouse.get_connection() as client:
        client.execute(f"INSERT INTO {SE_COMPANY_FIELD_REGISTRY} ({columns}) VALUES", values)
    context.log.info("exported %d registry rows for %s", len(rows), INFO_REGISTRY.version)
    return dg.MaterializeResult(
        metadata={
            "rows": len(rows),
            "fields": len(INFO_REGISTRY.fields),
            "registry_version": INFO_REGISTRY.version,
            "table": SE_COMPANY_FIELD_REGISTRY,
        }
    )


defs = dg.Definitions(assets=[se_company_field_registry_clickhouse])
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_field_export.py -q -p no:warnings`
Expected: PASS (5 tests).

- [ ] **Step 5: Check the definitions load and the whole SE suite is green**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync dg check defs`
Expected: the two closing lines `All component YAML validated successfully.` and `All definitions loaded successfully.` (BetaWarning lines about `backfill_policy` above them are pre-existing noise; an import error names the failing module instead).

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_field_registry.py tests/test_se_company_field_policies.py tests/test_se_company_field_sql.py tests/test_se_company_field_export.py tests/test_se_company_layout.py tests/test_clickhouse_migrations.py tests/test_se_company_info.py tests/test_clickhouse_leaf_checks.py -q -p no:warnings`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/fields/export.py \
        corpscout/services/dagster_v3/tests/test_se_company_field_export.py
git commit -m "feat(se): export the field registry and its generated SQL to ClickHouse

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---

## Handoff to the sibling plans

- Extractors (spec 5.2, 5.3) insert into `SE_COMPANY_FIELD_CANDIDATE` in `SE_COMPANY_FIELD_CANDIDATE_COLUMNS` order; `value_json` of json fields must carry the members the projection reads: `count`, `as_of` (ISO date) for `employee_count`; `count`, `amount`, `amount_usd`, `fiscal_year` are JSON numbers (never quoted -- the projection uses `toDecimal128OrNull(JSONExtractRaw(...))` / `JSONExtract(..., 'Nullable(UInt64)')` which return NULL for quoted numbers); `compare_key`, `as_of` (ISO date), `currency`, `language`, `period`, `period_end` are JSON strings; `language` for `description`; `compare_key` everywhere agreement should not fall back to `lowerUTF8(trim(value))`.
- The resolve asset (spec 8.2) executes `render_resolve_sql` per field with `server_side_params=True` (or the registry table's `resolve_sql`) and publishes the projection through `publish_with_stage(select_sql=render_projection_select_sql(INFO_REGISTRY), insert_columns=SE_COMPANY_INFO_COLUMNS, select_parameters=...)` -- note `publish_with_stage` binds `%(name)s` driver-side parameters, so it needs the same `server_side_params` client setting or a rendered statement.
- The backoffice (spec 9) reads `resolve_sql` by `(datatype, country, field)` with `argMax(resolve_sql, version)`, and the `field = '*'` row for the projection.

## Self-review

- Spec coverage for this plan's sections: 4.1 types + validation (Tasks 3, 4); 4.2 instance (Task 4); 4.3 export table, projection row, CHECK widening pinned to the registry (Tasks 1, 5, 8); 5.1 table verbatim (Task 1); 6 decision semantics in SQL (Task 6, executed Task 7); 7.1-7.4 policy interface, default policy, generated statement, every listed harness path (Tasks 3, 6, 7); 8.1 resolved table (Task 1); 8.3 wide projection columns and the new-column migration (Tasks 2, 6, 7); 12 migrations 1-3 and the registry/policy/pivot tests (all tasks); 14 names (throughout).
- Spec deviations, each deliberate: `argMax` over a tuple in the decision CTE (26.5 skips NULL args, which would break releases); `rank` is projected by `sql.py` for every policy rather than computed inside `SourcePrecedence` (same expression, one place); the resolved table carries `has_company` and `has_value` CHECKs beyond spec 8.1 (the "absent = no row, never empty" invariant, mirrored from the candidate table); the "empty candidates filtered" harness case cannot be seeded through the table because the `has_value` CHECK rejects it, so the unlisted-source path is executed and the filter fragment pinned as text; `source_record_uids` / `evidence_hashes` are unique and exclude the reviewer's empty uid, with a `notEmpty` guard so the `has_evidence` CHECK can never abort a batch.
- Placeholder scan: no TBD/TODO; every step has its code; no reference to a name defined outside these tasks.
- Type consistency: `render_resolve_sql(registry, field)`, `render_projection_select_sql(registry)`, `render_projection_sql(registry)`, `registry_rows(registry, *, rendered_at)`, `policy_for(field)`, `field_by_name(registry, name)`, `field_names(registry)`, `registry_fingerprint(registry)`, `validate_registry(registry)` are called with these exact signatures in every task that uses them.
