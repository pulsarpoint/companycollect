# Resolved Finland ClickHouse Tables Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create the first resolved Finland ClickHouse table contract and export path from Dagster/DuckDB into migration-owned ClickHouse tables.

**Architecture:** ClickHouse stores only resolved analytical tables, not raw source copies. Migrations own durable ClickHouse DDL under `corpscout/clickhouse/migrations`; Dagster verifies tables exist, builds resolved DuckDB tables, then exports rows into ClickHouse. This first slice implements the reusable schema/export foundation and loads Finland YTJ companies, websites, and industries; XBRL financial exports use the same contract in a separate financial-export plan after metrics and exchange-rate tables are approved.

**Tech Stack:** Dagster, DuckDB, ClickHouse native driver, `dagster_clickhouse.ClickhouseResource`, `golang-migrate` SQL migrations, pytest, Go migration-shape tests.

---

## File Structure

- Create `corpscout/clickhouse/migrations/000012_create_resolved_finland_tables.up.sql`: creates `corpscout_resolved` database and resolved `fi_*` tables.
- Create `corpscout/clickhouse/migrations/000012_create_resolved_finland_tables.down.sql`: drops resolved `fi_*` tables and database.
- Create `corpscout/scheduler/internal/db/clickhouse_resolved_finland_migration_test.go`: tests migration SQL shape.
- Create `corpscout/dagster_v3/src/dagster_v3/defs/clickhouse/__init__.py`: package marker for shared Dagster ClickHouse helpers.
- Create `corpscout/dagster_v3/src/dagster_v3/defs/clickhouse/resolved.py`: reusable schema checks, table contracts, and native insert helpers.
- Create `corpscout/dagster_v3/src/dagster_v3/defs/finland_resolved/__init__.py`: package marker.
- Create `corpscout/dagster_v3/src/dagster_v3/defs/finland_resolved/tables.py`: DuckDB/ClickHouse resolved table names and column contracts.
- Create `corpscout/dagster_v3/src/dagster_v3/defs/finland_resolved/assets.py`: DuckDB resolved table asset and ClickHouse export asset for YTJ-derived tables.
- Create `corpscout/dagster_v3/tests/test_clickhouse_resolved.py`: unit tests for ClickHouse helpers.
- Create `corpscout/dagster_v3/tests/test_finland_resolved_assets.py`: unit tests for resolved DuckDB SQL and asset graph.

## Resolved ClickHouse Database

Use a new database:

```text
corpscout_resolved
```

Reason: `corpscout_sources` currently contains source-copy and spike tables. A separate database makes the resolved analytical contract obvious and avoids mixing raw/source-shaped tables with product-ready tables.

## Initial Resolved Tables

Create these tables in the first migration:

- `corpscout_resolved.fi_companies`
- `corpscout_resolved.fi_websites`
- `corpscout_resolved.fi_industries`
- `corpscout_resolved.fi_addresses`
- `corpscout_resolved.fi_registered_entries`
- `corpscout_resolved.fi_legal_forms`
- `corpscout_resolved.fi_financial_statements`
- `corpscout_resolved.fi_financial_metrics`

Only the first three are loaded in this implementation slice. The remaining tables are created so the contract is visible and future export assets can target migrated tables.

## Column Patterns

Every resolved table includes:

```sql
source_system LowCardinality(String),
source_run_id String,
source_record_id String,
source_payload_hash FixedString(64),
resolved_at DateTime64(3, 'UTC')
```

Translation fields use:

```sql
description_original Nullable(String),
description_language Nullable(String),
description_en Nullable(String),
description_translated_at Nullable(DateTime64(3, 'UTC')),
description_translation_provider Nullable(String),
description_translation_model Nullable(String)
```

Financial fields use:

```sql
amount_original Nullable(Decimal(38, 6)),
currency_original Nullable(String),
amount_usd Nullable(Decimal(38, 6)),
fx_rate_to_usd Nullable(Decimal(18, 8)),
fx_rate_date Nullable(Date),
fx_converted_at Nullable(DateTime64(3, 'UTC'))
```

---

## Task 1: Add Resolved Finland ClickHouse Migration Contract

**Files:**
- Create: `corpscout/clickhouse/migrations/000012_create_resolved_finland_tables.up.sql`
- Create: `corpscout/clickhouse/migrations/000012_create_resolved_finland_tables.down.sql`
- Create: `corpscout/scheduler/internal/db/clickhouse_resolved_finland_migration_test.go`

- [ ] **Step 1: Write the failing migration-shape test**

Create `corpscout/scheduler/internal/db/clickhouse_resolved_finland_migration_test.go`:

```go
package db

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClickHouseResolvedFinlandMigrationCreatesResolvedTables(t *testing.T) {
	upPath := filepath.Join("..", "..", "..", "clickhouse", "migrations", "000012_create_resolved_finland_tables.up.sql")
	downPath := filepath.Join("..", "..", "..", "clickhouse", "migrations", "000012_create_resolved_finland_tables.down.sql")

	up, err := os.ReadFile(upPath)
	require.NoError(t, err)
	down, err := os.ReadFile(downPath)
	require.NoError(t, err)

	upSQL := string(up)
	downSQL := string(down)

	require.Contains(t, upSQL, "CREATE DATABASE IF NOT EXISTS `corpscout_resolved`")

	for _, table := range []string{
		"fi_companies",
		"fi_websites",
		"fi_industries",
		"fi_addresses",
		"fi_registered_entries",
		"fi_legal_forms",
		"fi_financial_statements",
		"fi_financial_metrics",
	} {
		require.Contains(t, upSQL, "CREATE TABLE IF NOT EXISTS `corpscout_resolved`.`"+table+"`")
		require.Contains(t, downSQL, "DROP TABLE IF EXISTS `corpscout_resolved`.`"+table+"`")
	}

	for _, column := range []string{
		"`source_system` LowCardinality(String)",
		"`source_run_id` String",
		"`source_record_id` String",
		"`source_payload_hash` FixedString(64)",
		"`resolved_at` DateTime64(3, 'UTC')",
	} {
		require.GreaterOrEqual(t, strings.Count(upSQL, column), 8, "audit column must appear on every resolved table")
	}

	require.Contains(t, upSQL, "`description_original` Nullable(String)")
	require.Contains(t, upSQL, "`description_language` Nullable(String)")
	require.Contains(t, upSQL, "`description_en` Nullable(String)")
	require.Contains(t, upSQL, "`description_translated_at` Nullable(DateTime64(3, 'UTC'))")
	require.Contains(t, upSQL, "`amount_original` Nullable(Decimal(38, 6))")
	require.Contains(t, upSQL, "`currency_original` Nullable(String)")
	require.Contains(t, upSQL, "`amount_usd` Nullable(Decimal(38, 6))")
	require.Contains(t, upSQL, "`fx_rate_date` Nullable(Date)")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db -run TestClickHouseResolvedFinlandMigrationCreatesResolvedTables -v
```

Expected: FAIL with `open ../../../clickhouse/migrations/000012_create_resolved_finland_tables.up.sql: no such file or directory`.

- [ ] **Step 3: Create the up migration**

Create `corpscout/clickhouse/migrations/000012_create_resolved_finland_tables.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS `corpscout_resolved`;

CREATE TABLE IF NOT EXISTS `corpscout_resolved`.`fi_companies` (
  `business_id` String,
  `country_iso2` FixedString(2),
  `name` String,
  `name_normalized` String,
  `registration_date` Nullable(Date),
  `end_date` Nullable(Date),
  `lifecycle_status` LowCardinality(String),
  `is_active` Bool,
  `legal_form_code` Nullable(String),
  `legal_form_description_original` Nullable(String),
  `legal_form_description_language` Nullable(String),
  `legal_form_description_en` Nullable(String),
  `legal_form_description_translated_at` Nullable(DateTime64(3, 'UTC')),
  `legal_form_description_translation_provider` Nullable(String),
  `legal_form_description_translation_model` Nullable(String),
  `primary_website_url` Nullable(String),
  `primary_website_host` Nullable(String),
  `source_system` LowCardinality(String),
  `source_run_id` String,
  `source_record_id` String,
  `source_payload_hash` FixedString(64),
  `resolved_at` DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(`resolved_at`)
ORDER BY (`country_iso2`, `business_id`);

CREATE TABLE IF NOT EXISTS `corpscout_resolved`.`fi_websites` (
  `business_id` String,
  `website_url` String,
  `website_normalized_url` String,
  `website_host` String,
  `website_path` String,
  `registered_on` Nullable(Date),
  `ended_on` Nullable(Date),
  `is_current` Bool,
  `is_primary` Bool,
  `source_system` LowCardinality(String),
  `source_run_id` String,
  `source_record_id` String,
  `source_payload_hash` FixedString(64),
  `resolved_at` DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(`resolved_at`)
ORDER BY (`website_host`, `business_id`, `website_normalized_url`);

CREATE TABLE IF NOT EXISTS `corpscout_resolved`.`fi_industries` (
  `business_id` String,
  `source_industry_code` Nullable(String),
  `source_industry_code_set` Nullable(String),
  `description_original` Nullable(String),
  `description_language` Nullable(String),
  `description_en` Nullable(String),
  `description_translated_at` Nullable(DateTime64(3, 'UTC')),
  `description_translation_provider` Nullable(String),
  `description_translation_model` Nullable(String),
  `nace_revision` Nullable(String),
  `nace_code` Nullable(String),
  `nace_normalized_code` Nullable(String),
  `nace_mapping_method` LowCardinality(String),
  `nace_mapping_status` LowCardinality(String),
  `is_primary` Bool,
  `source_system` LowCardinality(String),
  `source_run_id` String,
  `source_record_id` String,
  `source_payload_hash` FixedString(64),
  `resolved_at` DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(`resolved_at`)
ORDER BY (`business_id`, `source_industry_code`, `nace_revision`, `nace_normalized_code`)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS `corpscout_resolved`.`fi_addresses` (
  `business_id` String,
  `address_type_code` Nullable(String),
  `street` Nullable(String),
  `building_number` Nullable(String),
  `entrance` Nullable(String),
  `apartment_number` Nullable(String),
  `post_office_box` Nullable(String),
  `post_code` Nullable(String),
  `city_original` Nullable(String),
  `city_language` Nullable(String),
  `city_en` Nullable(String),
  `city_translated_at` Nullable(DateTime64(3, 'UTC')),
  `city_translation_provider` Nullable(String),
  `city_translation_model` Nullable(String),
  `municipality_code` Nullable(String),
  `country` Nullable(String),
  `registered_on` Nullable(Date),
  `source_system` LowCardinality(String),
  `source_run_id` String,
  `source_record_id` String,
  `source_payload_hash` FixedString(64),
  `resolved_at` DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(`resolved_at`)
ORDER BY (`business_id`, `address_type_code`, `post_code`)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS `corpscout_resolved`.`fi_registered_entries` (
  `business_id` String,
  `register_code` Nullable(String),
  `entry_type_code` Nullable(String),
  `authority` Nullable(String),
  `description_original` Nullable(String),
  `description_language` Nullable(String),
  `description_en` Nullable(String),
  `description_translated_at` Nullable(DateTime64(3, 'UTC')),
  `description_translation_provider` Nullable(String),
  `description_translation_model` Nullable(String),
  `registered_on` Nullable(Date),
  `ended_on` Nullable(Date),
  `is_current` Bool,
  `source_system` LowCardinality(String),
  `source_run_id` String,
  `source_record_id` String,
  `source_payload_hash` FixedString(64),
  `resolved_at` DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(`resolved_at`)
ORDER BY (`business_id`, `register_code`, `entry_type_code`, `registered_on`)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS `corpscout_resolved`.`fi_legal_forms` (
  `business_id` String,
  `legal_form_code` Nullable(String),
  `description_original` Nullable(String),
  `description_language` Nullable(String),
  `description_en` Nullable(String),
  `description_translated_at` Nullable(DateTime64(3, 'UTC')),
  `description_translation_provider` Nullable(String),
  `description_translation_model` Nullable(String),
  `registered_on` Nullable(Date),
  `ended_on` Nullable(Date),
  `is_current` Bool,
  `source_system` LowCardinality(String),
  `source_run_id` String,
  `source_record_id` String,
  `source_payload_hash` FixedString(64),
  `resolved_at` DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(`resolved_at`)
ORDER BY (`business_id`, `legal_form_code`, `registered_on`)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS `corpscout_resolved`.`fi_financial_statements` (
  `statement_key` String,
  `business_id` String,
  `financial_date` Date,
  `period_start` Nullable(Date),
  `period_end` Nullable(Date),
  `reported_company_name` Nullable(String),
  `source_url` String,
  `xml_object_key` String,
  `xml_sha256` FixedString(64),
  `source_system` LowCardinality(String),
  `source_run_id` String,
  `source_record_id` String,
  `source_payload_hash` FixedString(64),
  `resolved_at` DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(`resolved_at`)
PARTITION BY toYYYYMM(`financial_date`)
ORDER BY (`business_id`, `financial_date`, `statement_key`);

CREATE TABLE IF NOT EXISTS `corpscout_resolved`.`fi_financial_metrics` (
  `statement_key` String,
  `business_id` String,
  `financial_date` Date,
  `period_start` Nullable(Date),
  `period_end` Nullable(Date),
  `metric_key` LowCardinality(String),
  `metric_label` String,
  `amount_original` Nullable(Decimal(38, 6)),
  `currency_original` Nullable(String),
  `amount_usd` Nullable(Decimal(38, 6)),
  `fx_rate_to_usd` Nullable(Decimal(18, 8)),
  `fx_rate_date` Nullable(Date),
  `fx_converted_at` Nullable(DateTime64(3, 'UTC')),
  `source_concept_qname` Nullable(String),
  `mapping_version` String,
  `source_system` LowCardinality(String),
  `source_run_id` String,
  `source_record_id` String,
  `source_payload_hash` FixedString(64),
  `resolved_at` DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(`resolved_at`)
PARTITION BY toYYYYMM(`financial_date`)
ORDER BY (`business_id`, `financial_date`, `metric_key`, `statement_key`);
```

- [ ] **Step 4: Create the down migration**

Create `corpscout/clickhouse/migrations/000012_create_resolved_finland_tables.down.sql`:

```sql
DROP TABLE IF EXISTS `corpscout_resolved`.`fi_financial_metrics`;
DROP TABLE IF EXISTS `corpscout_resolved`.`fi_financial_statements`;
DROP TABLE IF EXISTS `corpscout_resolved`.`fi_legal_forms`;
DROP TABLE IF EXISTS `corpscout_resolved`.`fi_registered_entries`;
DROP TABLE IF EXISTS `corpscout_resolved`.`fi_addresses`;
DROP TABLE IF EXISTS `corpscout_resolved`.`fi_industries`;
DROP TABLE IF EXISTS `corpscout_resolved`.`fi_websites`;
DROP TABLE IF EXISTS `corpscout_resolved`.`fi_companies`;
DROP DATABASE IF EXISTS `corpscout_resolved`;
```

- [ ] **Step 5: Run migration-shape test**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db -run TestClickHouseResolvedFinlandMigrationCreatesResolvedTables -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/clickhouse/migrations/000012_create_resolved_finland_tables.up.sql \
  corpscout/clickhouse/migrations/000012_create_resolved_finland_tables.down.sql \
  corpscout/scheduler/internal/db/clickhouse_resolved_finland_migration_test.go
git commit -m "Add resolved Finland ClickHouse tables"
```

## Task 2: Add Shared Dagster ClickHouse Table Verification

**Files:**
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/clickhouse/__init__.py`
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/clickhouse/resolved.py`
- Create: `corpscout/dagster_v3/tests/test_clickhouse_resolved.py`

- [ ] **Step 1: Write failing tests for table verification**

Create `corpscout/dagster_v3/tests/test_clickhouse_resolved.py`:

```python
from collections.abc import Iterator
from contextlib import contextmanager

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    REQUIRED_FINLAND_RESOLVED_TABLES,
    assert_clickhouse_tables_exist,
)


class FakeClickHouseClient:
    def __init__(self, existing_tables: set[str]) -> None:
        self.existing_tables = existing_tables
        self.statements: list[str] = []

    def execute(self, sql: str, params: dict[str, object] | None = None) -> list[tuple[str]]:
        self.statements.append(sql)
        if "system.tables" not in sql:
            raise AssertionError(sql)
        database = str(params["database"]) if params else ""
        requested = set(params["tables"]) if params else set()
        return [
            (table.removeprefix(f"{database}."),)
            for table in sorted(self.existing_tables)
            if table.startswith(f"{database}.") and table.removeprefix(f"{database}.") in requested
        ]


def test_required_finland_resolved_tables_are_explicit() -> None:
    assert REQUIRED_FINLAND_RESOLVED_TABLES == (
        "fi_companies",
        "fi_websites",
        "fi_industries",
        "fi_addresses",
        "fi_registered_entries",
        "fi_legal_forms",
        "fi_financial_statements",
        "fi_financial_metrics",
    )


def test_assert_clickhouse_tables_exist_uses_official_resource(monkeypatch) -> None:
    resource = ClickhouseResource(host="localhost")
    client = FakeClickHouseClient(
        {"corpscout_resolved.fi_companies", "corpscout_resolved.fi_websites"}
    )

    @contextmanager
    def fake_get_connection(self: ClickhouseResource) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    assert_clickhouse_tables_exist(
        resource,
        database="corpscout_resolved",
        tables=("fi_companies", "fi_websites"),
    )

    assert "system.tables" in client.statements[0]


def test_assert_clickhouse_tables_exist_reports_missing_tables(monkeypatch) -> None:
    resource = ClickhouseResource(host="localhost")
    client = FakeClickHouseClient({"corpscout_resolved.fi_companies"})

    @contextmanager
    def fake_get_connection(self: ClickhouseResource) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    try:
        assert_clickhouse_tables_exist(
            resource,
            database="corpscout_resolved",
            tables=("fi_companies", "fi_websites"),
        )
    except ValueError as exc:
        assert str(exc) == "Missing ClickHouse tables in corpscout_resolved: fi_websites"
    else:
        raise AssertionError("expected missing table error")
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_clickhouse_resolved.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'dagster_v3.defs.clickhouse'`.

- [ ] **Step 3: Implement table verification helper**

Create empty `corpscout/dagster_v3/src/dagster_v3/defs/clickhouse/__init__.py`.

Create `corpscout/dagster_v3/src/dagster_v3/defs/clickhouse/resolved.py`:

```python
from __future__ import annotations

from collections.abc import Sequence

from dagster_clickhouse import ClickhouseResource

RESOLVED_DATABASE = "corpscout_resolved"

REQUIRED_FINLAND_RESOLVED_TABLES = (
    "fi_companies",
    "fi_websites",
    "fi_industries",
    "fi_addresses",
    "fi_registered_entries",
    "fi_legal_forms",
    "fi_financial_statements",
    "fi_financial_metrics",
)


def assert_clickhouse_tables_exist(
    clickhouse: ClickhouseResource,
    *,
    database: str,
    tables: Sequence[str],
) -> None:
    requested_tables = tuple(tables)
    with clickhouse.get_connection() as client:
        rows = client.execute(
            """
            SELECT name
            FROM system.tables
            WHERE database = %(database)s
              AND name IN %(tables)s
            """,
            {"database": database, "tables": requested_tables},
        )

    existing = {str(row[0]) for row in rows}
    missing = [table for table in requested_tables if table not in existing]
    if missing:
        raise ValueError(f"Missing ClickHouse tables in {database}: {', '.join(missing)}")
```

- [ ] **Step 4: Run tests**

Run:

```bash
uv run pytest tests/test_clickhouse_resolved.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/clickhouse/__init__.py \
  src/dagster_v3/defs/clickhouse/resolved.py \
  tests/test_clickhouse_resolved.py
git commit -m "Add ClickHouse resolved table checks"
```

## Task 3: Add Resolved Finland Table Contracts In Dagster

**Files:**
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/finland_resolved/__init__.py`
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/finland_resolved/tables.py`
- Create: `corpscout/dagster_v3/tests/test_finland_resolved_assets.py`

- [ ] **Step 1: Write failing tests for resolved table contracts**

Create `corpscout/dagster_v3/tests/test_finland_resolved_assets.py`:

```python
from dagster_v3.defs.clickhouse.resolved import RESOLVED_DATABASE
from dagster_v3.defs.finland_resolved import tables


def test_finland_resolved_table_names_match_clickhouse_contract() -> None:
    assert RESOLVED_DATABASE == "corpscout_resolved"
    assert tables.FI_COMPANIES_TABLE == "fi_companies"
    assert tables.FI_WEBSITES_TABLE == "fi_websites"
    assert tables.FI_INDUSTRIES_TABLE == "fi_industries"
    assert tables.FINLAND_YTJ_RESOLVED_TABLES == (
        "fi_companies",
        "fi_websites",
        "fi_industries",
    )


def test_finland_resolved_columns_include_audit_metadata() -> None:
    for table_name in tables.FINLAND_YTJ_RESOLVED_TABLES:
        assert tables.AUDIT_COLUMNS <= set(tables.RESOLVED_TABLE_COLUMNS[table_name])


def test_finland_industries_keeps_nace_keys_without_labels() -> None:
    industry_columns = set(tables.RESOLVED_TABLE_COLUMNS[tables.FI_INDUSTRIES_TABLE])

    assert {
        "nace_revision",
        "nace_code",
        "nace_normalized_code",
        "nace_mapping_method",
        "nace_mapping_status",
    } <= industry_columns
    assert "nace_title_en" not in industry_columns
    assert "nace_description_en" not in industry_columns
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/test_finland_resolved_assets.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'dagster_v3.defs.finland_resolved'`.

- [ ] **Step 3: Implement table constants**

Create empty `corpscout/dagster_v3/src/dagster_v3/defs/finland_resolved/__init__.py`.

Create `corpscout/dagster_v3/src/dagster_v3/defs/finland_resolved/tables.py`:

```python
FI_COMPANIES_TABLE = "fi_companies"
FI_WEBSITES_TABLE = "fi_websites"
FI_INDUSTRIES_TABLE = "fi_industries"

FINLAND_YTJ_RESOLVED_TABLES = (
    FI_COMPANIES_TABLE,
    FI_WEBSITES_TABLE,
    FI_INDUSTRIES_TABLE,
)

AUDIT_COLUMNS = {
    "source_system",
    "source_run_id",
    "source_record_id",
    "source_payload_hash",
    "resolved_at",
}

RESOLVED_TABLE_COLUMNS = {
    FI_COMPANIES_TABLE: (
        "business_id",
        "country_iso2",
        "name",
        "name_normalized",
        "registration_date",
        "end_date",
        "lifecycle_status",
        "is_active",
        "legal_form_code",
        "legal_form_description_original",
        "legal_form_description_language",
        "legal_form_description_en",
        "legal_form_description_translated_at",
        "legal_form_description_translation_provider",
        "legal_form_description_translation_model",
        "primary_website_url",
        "primary_website_host",
        "source_system",
        "source_run_id",
        "source_record_id",
        "source_payload_hash",
        "resolved_at",
    ),
    FI_WEBSITES_TABLE: (
        "business_id",
        "website_url",
        "website_normalized_url",
        "website_host",
        "website_path",
        "registered_on",
        "ended_on",
        "is_current",
        "is_primary",
        "source_system",
        "source_run_id",
        "source_record_id",
        "source_payload_hash",
        "resolved_at",
    ),
    FI_INDUSTRIES_TABLE: (
        "business_id",
        "source_industry_code",
        "source_industry_code_set",
        "description_original",
        "description_language",
        "description_en",
        "description_translated_at",
        "description_translation_provider",
        "description_translation_model",
        "nace_revision",
        "nace_code",
        "nace_normalized_code",
        "nace_mapping_method",
        "nace_mapping_status",
        "is_primary",
        "source_system",
        "source_run_id",
        "source_record_id",
        "source_payload_hash",
        "resolved_at",
    ),
}
```

- [ ] **Step 4: Run tests**

Run:

```bash
uv run pytest tests/test_finland_resolved_assets.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/finland_resolved/__init__.py \
  src/dagster_v3/defs/finland_resolved/tables.py \
  tests/test_finland_resolved_assets.py
git commit -m "Add Finland resolved table contracts"
```

## Task 4: Build Resolved YTJ DuckDB Tables

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/finland_resolved/assets.py`
- Modify: `corpscout/dagster_v3/tests/test_finland_resolved_assets.py`

- [ ] **Step 1: Write failing DuckDB transform test**

Append to `tests/test_finland_resolved_assets.py`:

```python
import duckdb

from dagster_v3.defs.finland_resolved.assets import build_finland_ytj_resolved_tables
from dagster_v3.defs.finland_ytj.resources import LocalDuckDBResource


def test_build_finland_ytj_resolved_tables_creates_company_website_and_industry_tables(
    tmp_path,
) -> None:
    database_path = tmp_path / "source.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("create schema finland_prhytj")
        connection.execute(
            """
            create table finland_prhytj.all_companies (
                country_iso2 varchar,
                source_slug varchar,
                source_run_id varchar,
                source_record_id varchar,
                source_payload_hash varchar,
                business_id varchar,
                registration_date varchar,
                end_date varchar,
                lifecycle_status varchar,
                is_active boolean,
                primary_name varchar,
                website_url varchar,
                website_normalized_url varchar,
                website_host varchar,
                website_path varchar,
                website_registered_on varchar,
                website_ended_on varchar,
                raw_company varchar
            )
            """
        )
        connection.execute(
            """
            insert into finland_prhytj.all_companies values (
                'FI',
                'finland_prhytj',
                'run-1',
                '1234567-8',
                repeat('a', 64),
                '1234567-8',
                '2024-01-02',
                '',
                'active',
                true,
                'Example Oy',
                'https://example.fi',
                'https://example.fi',
                'example.fi',
                '',
                '2024-01-03',
                '',
                '{"businessLine":{"code":"0111","codeSet":"NACE_REV_2_1","description":"Viljely"}}'
            )
            """
        )

    row_counts = build_finland_ytj_resolved_tables(LocalDuckDBResource(database_path=str(database_path)))

    assert row_counts == {
        "fi_companies": 1,
        "fi_websites": 1,
        "fi_industries": 1,
    }

    with duckdb.connect(str(database_path), read_only=True) as connection:
        company = connection.execute(
            "select business_id, name, primary_website_host from finland_resolved.fi_companies"
        ).fetchone()
        website = connection.execute(
            "select business_id, website_host, is_primary from finland_resolved.fi_websites"
        ).fetchone()
        industry = connection.execute(
            """
            select business_id, nace_revision, nace_normalized_code, description_original
            from finland_resolved.fi_industries
            """
        ).fetchone()

    assert company == ("1234567-8", "Example Oy", "example.fi")
    assert website == ("1234567-8", "example.fi", True)
    assert industry == ("1234567-8", "NACE_REV_2_1", "0111", "Viljely")
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
uv run pytest tests/test_finland_resolved_assets.py::test_build_finland_ytj_resolved_tables_creates_company_website_and_industry_tables -v
```

Expected: FAIL with `ModuleNotFoundError` or missing `build_finland_ytj_resolved_tables`.

- [ ] **Step 3: Implement DuckDB transform**

Create `corpscout/dagster_v3/src/dagster_v3/defs/finland_resolved/assets.py`:

```python
from __future__ import annotations

import json

import dagster as dg

from dagster_v3.defs.finland_resolved import tables
from dagster_v3.defs.finland_ytj.resources import LocalDuckDBResource

RESOLVED_DUCKDB_SCHEMA = "finland_resolved"
YTJ_DUCKDB_SCHEMA = "finland_prhytj"
YTJ_ALL_COMPANIES_TABLE = "all_companies"


def build_finland_ytj_resolved_tables(source_duckdb: LocalDuckDBResource) -> dict[str, int]:
    with source_duckdb.connect() as connection:
        connection.execute(f"create schema if not exists {RESOLVED_DUCKDB_SCHEMA}")
        connection.create_function(
            "fi_primary_industry_json",
            _primary_industry_json,
            ["VARCHAR"],
            "VARCHAR",
            null_handling="special",
        )
        connection.execute(_fi_companies_sql())
        connection.execute(_fi_websites_sql())
        connection.execute(_fi_industries_sql())
        return {
            table: int(
                connection.execute(
                    f"select count(*) from {RESOLVED_DUCKDB_SCHEMA}.{table}"
                ).fetchone()[0]
            )
            for table in tables.FINLAND_YTJ_RESOLVED_TABLES
        }


@dg.asset(
    deps=["finland_ytj_all_companies_duckdb"],
    group_name="finland_resolved",
    kinds={"python", "duckdb", "sql"},
    description="Resolved Finland company, website, and industry DuckDB tables derived from YTJ.",
)
def finland_ytj_resolved_duckdb(
    ytj_duckdb: LocalDuckDBResource,
) -> dg.MaterializeResult:
    row_counts = build_finland_ytj_resolved_tables(ytj_duckdb)
    return dg.MaterializeResult(metadata={f"{table}_row_count": count for table, count in row_counts.items()})


defs = dg.Definitions(assets=[finland_ytj_resolved_duckdb])


def _fi_companies_sql() -> str:
    return f"""
    create or replace table {RESOLVED_DUCKDB_SCHEMA}.{tables.FI_COMPANIES_TABLE} as
    select
      business_id,
      country_iso2,
      primary_name as name,
      lower(primary_name) as name_normalized,
      try_cast(nullif(registration_date, '') as date) as registration_date,
      try_cast(nullif(end_date, '') as date) as end_date,
      lifecycle_status,
      coalesce(is_active, false) as is_active,
      cast(null as varchar) as legal_form_code,
      cast(null as varchar) as legal_form_description_original,
      cast(null as varchar) as legal_form_description_language,
      cast(null as varchar) as legal_form_description_en,
      cast(null as timestamp) as legal_form_description_translated_at,
      cast(null as varchar) as legal_form_description_translation_provider,
      cast(null as varchar) as legal_form_description_translation_model,
      nullif(website_normalized_url, '') as primary_website_url,
      nullif(website_host, '') as primary_website_host,
      source_slug as source_system,
      source_run_id,
      source_record_id,
      source_payload_hash,
      now() as resolved_at
    from {YTJ_DUCKDB_SCHEMA}.{YTJ_ALL_COMPANIES_TABLE}
    where business_id is not null and business_id != ''
    """


def _fi_websites_sql() -> str:
    return f"""
    create or replace table {RESOLVED_DUCKDB_SCHEMA}.{tables.FI_WEBSITES_TABLE} as
    select
      business_id,
      website_url,
      website_normalized_url,
      website_host,
      website_path,
      try_cast(nullif(website_registered_on, '') as date) as registered_on,
      try_cast(nullif(website_ended_on, '') as date) as ended_on,
      website_ended_on is null or website_ended_on = '' as is_current,
      true as is_primary,
      source_slug as source_system,
      source_run_id,
      source_record_id,
      source_payload_hash,
      now() as resolved_at
    from {YTJ_DUCKDB_SCHEMA}.{YTJ_ALL_COMPANIES_TABLE}
    where business_id is not null
      and business_id != ''
      and website_normalized_url is not null
      and website_normalized_url != ''
    """


def _fi_industries_sql() -> str:
    return f"""
    create or replace table {RESOLVED_DUCKDB_SCHEMA}.{tables.FI_INDUSTRIES_TABLE} as
    with extracted as (
      select
        *,
        fi_primary_industry_json(raw_company) as industry_json
      from {YTJ_DUCKDB_SCHEMA}.{YTJ_ALL_COMPANIES_TABLE}
    )
    select
      business_id,
      json_extract_string(industry_json, '$.code') as source_industry_code,
      json_extract_string(industry_json, '$.codeSet') as source_industry_code_set,
      json_extract_string(industry_json, '$.description') as description_original,
      'fi' as description_language,
      cast(null as varchar) as description_en,
      cast(null as timestamp) as description_translated_at,
      cast(null as varchar) as description_translation_provider,
      cast(null as varchar) as description_translation_model,
      coalesce(json_extract_string(industry_json, '$.codeSet'), 'NACE_REV_2_1') as nace_revision,
      json_extract_string(industry_json, '$.code') as nace_code,
      regexp_replace(json_extract_string(industry_json, '$.code'), '[^0-9A-Za-z]', '', 'g') as nace_normalized_code,
      'direct_code' as nace_mapping_method,
      case when json_extract_string(industry_json, '$.code') is null then 'missing_source_code' else 'mapped' end as nace_mapping_status,
      true as is_primary,
      source_slug as source_system,
      source_run_id,
      source_record_id,
      source_payload_hash,
      now() as resolved_at
    from extracted
    where business_id is not null and business_id != ''
    """


def _primary_industry_json(raw_company: str | None) -> str | None:
    if not raw_company:
        return None
    try:
        payload = json.loads(raw_company)
    except json.JSONDecodeError:
        return None
    for key in ("businessLine", "mainBusinessLine"):
        business_line = payload.get(key)
        if isinstance(business_line, dict):
            return json.dumps(business_line)
    business_lines = payload.get("businessLines")
    if isinstance(business_lines, list):
        for business_line in business_lines:
            if isinstance(business_line, dict):
                return json.dumps(business_line)
    return None
```

- [ ] **Step 4: Run focused test**

Run:

```bash
uv run pytest tests/test_finland_resolved_assets.py::test_build_finland_ytj_resolved_tables_creates_company_website_and_industry_tables -v
```

Expected: PASS.

- [ ] **Step 5: Run related tests**

Run:

```bash
uv run pytest tests/test_finland_resolved_assets.py tests/test_finland_ytj_assets.py -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/dagster_v3/defs/finland_resolved/assets.py tests/test_finland_resolved_assets.py
git commit -m "Build resolved Finland YTJ DuckDB tables"
```

## Task 5: Export Resolved DuckDB Tables To ClickHouse

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/clickhouse/resolved.py`
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/finland_resolved/assets.py`
- Modify: `corpscout/dagster_v3/tests/test_clickhouse_resolved.py`
- Modify: `corpscout/dagster_v3/tests/test_finland_resolved_assets.py`

- [ ] **Step 1: Write failing tests for export helper**

Append to `tests/test_clickhouse_resolved.py`:

```python
import duckdb

from dagster_v3.defs.clickhouse.resolved import export_duckdb_table_to_clickhouse


def test_export_duckdb_table_to_clickhouse_inserts_rows_in_column_order(tmp_path) -> None:
    database_path = tmp_path / "source.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("create schema finland_resolved")
        connection.execute(
            """
            create table finland_resolved.fi_companies (
                business_id varchar,
                country_iso2 varchar,
                source_system varchar
            )
            """
        )
        connection.execute("insert into finland_resolved.fi_companies values ('1234567-8', 'FI', 'finland_prhytj')")

    client = FakeInsertClickHouseClient()

    row_count = export_duckdb_table_to_clickhouse(
        duckdb_path=database_path,
        clickhouse_client=client,
        duckdb_schema="finland_resolved",
        duckdb_table="fi_companies",
        clickhouse_database="corpscout_resolved",
        clickhouse_table="fi_companies",
        columns=("business_id", "country_iso2", "source_system"),
        truncate=True,
    )

    assert row_count == 1
    assert client.statements == ["TRUNCATE TABLE `corpscout_resolved`.`fi_companies`"]
    assert client.insert_calls == [
        (
            "INSERT INTO `corpscout_resolved`.`fi_companies` (`business_id`, `country_iso2`, `source_system`) VALUES",
            [("1234567-8", "FI", "finland_prhytj")],
        )
    ]


class FakeInsertClickHouseClient(FakeClickHouseClient):
    def __init__(self) -> None:
        super().__init__(set())
        self.insert_calls: list[tuple[str, list[tuple[object, ...]]]] = []

    def execute(self, sql: str, params: object | None = None) -> list[tuple[str]]:
        if sql.startswith("INSERT INTO"):
            if not isinstance(params, list):
                raise AssertionError("insert params must be a list of row tuples")
            self.insert_calls.append((sql, params))
            return []
        self.statements.append(sql)
        return []
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
uv run pytest tests/test_clickhouse_resolved.py::test_export_duckdb_table_to_clickhouse_inserts_rows_in_column_order -v
```

Expected: FAIL with missing `export_duckdb_table_to_clickhouse`.

- [ ] **Step 3: Implement export helper**

Modify `src/dagster_v3/defs/clickhouse/resolved.py`:

```python
from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

import duckdb

from dagster_clickhouse import ClickhouseResource

RESOLVED_DATABASE = "corpscout_resolved"

REQUIRED_FINLAND_RESOLVED_TABLES = (
    "fi_companies",
    "fi_websites",
    "fi_industries",
    "fi_addresses",
    "fi_registered_entries",
    "fi_legal_forms",
    "fi_financial_statements",
    "fi_financial_metrics",
)


def assert_clickhouse_tables_exist(
    clickhouse: ClickhouseResource,
    *,
    database: str,
    tables: Sequence[str],
) -> None:
    requested_tables = tuple(tables)
    with clickhouse.get_connection() as client:
        rows = client.execute(
            """
            SELECT name
            FROM system.tables
            WHERE database = %(database)s
              AND name IN %(tables)s
            """,
            {"database": database, "tables": requested_tables},
        )

    existing = {str(row[0]) for row in rows}
    missing = [table for table in requested_tables if table not in existing]
    if missing:
        raise ValueError(f"Missing ClickHouse tables in {database}: {', '.join(missing)}")


def export_duckdb_table_to_clickhouse(
    *,
    duckdb_path: str | Path,
    clickhouse_client: Any,
    duckdb_schema: str,
    duckdb_table: str,
    clickhouse_database: str,
    clickhouse_table: str,
    columns: Sequence[str],
    truncate: bool,
) -> int:
    if truncate:
        clickhouse_client.execute(
            f"TRUNCATE TABLE {_quote_clickhouse_identifier(clickhouse_database)}.{_quote_clickhouse_identifier(clickhouse_table)}"
        )

    duckdb_columns = ", ".join(_quote_duckdb_identifier(column) for column in columns)
    duckdb_qualified_table = (
        f"{_quote_duckdb_identifier(duckdb_schema)}.{_quote_duckdb_identifier(duckdb_table)}"
    )
    with duckdb.connect(str(duckdb_path), read_only=True) as connection:
        rows = connection.execute(
            f"select {duckdb_columns} from {duckdb_qualified_table}"
        ).fetchall()

    if not rows:
        return 0

    clickhouse_columns = ", ".join(_quote_clickhouse_identifier(column) for column in columns)
    clickhouse_client.execute(
        (
            f"INSERT INTO {_quote_clickhouse_identifier(clickhouse_database)}."
            f"{_quote_clickhouse_identifier(clickhouse_table)} ({clickhouse_columns}) VALUES"
        ),
        rows,
    )
    return len(rows)


def _quote_duckdb_identifier(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def _quote_clickhouse_identifier(identifier: str) -> str:
    escaped = identifier.replace("`", "``")
    return f"`{escaped}`"
```

- [ ] **Step 4: Run export helper test**

Run:

```bash
uv run pytest tests/test_clickhouse_resolved.py::test_export_duckdb_table_to_clickhouse_inserts_rows_in_column_order -v
```

Expected: PASS.

- [ ] **Step 5: Add ClickHouse export asset**

Append to `src/dagster_v3/defs/finland_resolved/assets.py`:

```python
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    RESOLVED_DATABASE,
    assert_clickhouse_tables_exist,
    export_duckdb_table_to_clickhouse,
)


@dg.asset(
    deps=[finland_ytj_resolved_duckdb],
    group_name="finland_resolved",
    kinds={"python", "duckdb", "clickhouse"},
    description="Exports resolved Finland YTJ DuckDB tables to migrated ClickHouse tables.",
)
def finland_ytj_resolved_clickhouse(
    clickhouse: ClickhouseResource,
    ytj_duckdb: LocalDuckDBResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RESOLVED_DATABASE,
        tables=tables.FINLAND_YTJ_RESOLVED_TABLES,
    )
    with clickhouse.get_connection() as client:
        row_counts = {
            table: export_duckdb_table_to_clickhouse(
                duckdb_path=ytj_duckdb.path(),
                clickhouse_client=client,
                duckdb_schema=RESOLVED_DUCKDB_SCHEMA,
                duckdb_table=table,
                clickhouse_database=RESOLVED_DATABASE,
                clickhouse_table=table,
                columns=tables.RESOLVED_TABLE_COLUMNS[table],
                truncate=True,
            )
            for table in tables.FINLAND_YTJ_RESOLVED_TABLES
        }
    return dg.MaterializeResult(metadata={f"{table}_row_count": count for table, count in row_counts.items()})
```

Update the `defs` block in the same file:

```python
defs = dg.Definitions(
    assets=[
        finland_ytj_resolved_duckdb,
        finland_ytj_resolved_clickhouse,
    ]
)
```

- [ ] **Step 6: Add asset graph test**

Append to `tests/test_finland_resolved_assets.py`:

```python
from dagster_v3.definitions import defs as load_project_defs


def test_finland_resolved_clickhouse_asset_is_registered() -> None:
    repository = load_project_defs().get_repository_def()
    asset_keys = {key.path[-1] for key in repository.asset_graph.get_all_asset_keys()}

    assert "finland_ytj_resolved_duckdb" in asset_keys
    assert "finland_ytj_resolved_clickhouse" in asset_keys
```

- [ ] **Step 7: Run focused tests**

Run:

```bash
uv run pytest tests/test_clickhouse_resolved.py tests/test_finland_resolved_assets.py -v
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add src/dagster_v3/defs/clickhouse/resolved.py \
  src/dagster_v3/defs/finland_resolved/assets.py \
  tests/test_clickhouse_resolved.py \
  tests/test_finland_resolved_assets.py
git commit -m "Export resolved Finland YTJ tables to ClickHouse"
```

## Task 6: Validate And Materialize

**Files:**
- No code changes unless validation finds an issue.

- [ ] **Step 1: Run Dagster tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest -v
```

Expected: all tests pass.

- [ ] **Step 2: Run Dagster definition check**

Run:

```bash
uv run dg check defs
```

Expected: all definitions load.

- [ ] **Step 3: Run Go migration tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db -run 'ClickHouse.*Migration' -v
```

Expected: all ClickHouse migration-shape tests pass.

- [ ] **Step 4: Apply ClickHouse migrations locally**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make clickhouse-migrate-up
```

Expected: migrations apply successfully. If migration version `000012` is already dirty or partially applied, stop and inspect ClickHouse migration state before retrying.

- [ ] **Step 5: Materialize source and resolved assets**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run dg launch --assets finland_ytj_all_companies_duckdb
uv run dg launch --assets finland_ytj_resolved_duckdb
uv run dg launch --assets finland_ytj_resolved_clickhouse
```

Expected: all three runs succeed. The ClickHouse materialization metadata includes row counts for `fi_companies`, `fi_websites`, and `fi_industries`.

- [ ] **Step 6: Verify ClickHouse row counts**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
set -a; source .env; set +a
uv run python - <<'PY'
from clickhouse_driver import Client
import os

client = Client(
    host=os.environ["CLICKHOUSE_HOST"],
    port=int(os.environ.get("CLICKHOUSE_NATIVE_PORT", "9002")),
    user=os.environ["CLICKHOUSE_USER"],
    password=os.environ.get("CLICKHOUSE_PASSWORD") or "",
    database=os.environ["CLICKHOUSE_DATABASE"],
)
for table in ("fi_companies", "fi_websites", "fi_industries"):
    print(table, client.execute(f"SELECT count() FROM corpscout_resolved.{table}")[0][0])
PY
```

Expected: `fi_companies` has rows. `fi_websites` and `fi_industries` may have fewer rows than companies, but should be non-zero if source data contains websites and business-line codes.

- [ ] **Step 7: Commit validation fixes if any**

If validation required code changes:

```bash
git add corpscout/clickhouse/migrations \
  corpscout/scheduler/internal/db/clickhouse_resolved_finland_migration_test.go \
  corpscout/dagster_v3/src/dagster_v3/defs \
  corpscout/dagster_v3/tests
git commit -m "Validate resolved Finland ClickHouse export"
```

If no code changes were needed, do not create an empty commit.

## Rollout Notes

- Existing `corpscout_sources.fi_prhytj_*` migrations remain in place for now. This plan does not delete or rewrite historical source-copy tables.
- `corpscout_resolved.fi_*` is the new target contract for product-ready Finland data.
- Financial tables are created in migration `000012`, but populated by a separate financial-export implementation after XBRL metrics and exchange-rate tables are finalized.
- If `CLICKHOUSE_NATIVE_PORT` is not set, Dagster helpers should default to `9002`, matching `companycollect/clickhouse_docker/docker-compose.yml`.
