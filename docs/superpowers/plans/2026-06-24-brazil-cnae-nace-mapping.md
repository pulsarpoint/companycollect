# Brazil CNAE to NACE Mapping Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Brazil-specific many-to-many CNAE-to-NACE mapping reference table that Dagster can materialize directly into ClickHouse from curated fixture rows.

**Architecture:** Add a source-owned `dagster_v3.defs.brazil_cnae` package with constants, fixture parsing, validation, direct ClickHouse stage-table replacement, and one Dagster asset. The source does not use DuckDB or dlt because the mapping is static curated reference data. ClickHouse DDL remains migration-owned, and tests cover fixture parsing, many-to-many mapping edges, stage-table exchange, migration registration, and Dagster definition loading.

**Tech Stack:** Python 3.14, Dagster assets, `dagster-clickhouse`, ClickHouse SQL migrations, pytest, CSV fixtures.

---

## Scope

This plan implements the `br_cnae_to_nace` reference table and the Dagster materialization that publishes it.

This plan includes a small committed seed fixture so the asset is runnable and testable. Expanding the fixture to the complete curated CNAE-to-NACE mapping is a data curation task before Brazil CNPJ ingestion uses the table in production. The code must reject empty fixtures, duplicate mapping edges, rows with missing required values, and fixture rows whose NACE target is not present in `corpscout.nace_categories`.

## File Structure

- Create `corpscout/dagster_v3/src/dagster_v3/defs/brazil_cnae/__init__.py`: package marker for auto-loading.
- Create `corpscout/dagster_v3/src/dagster_v3/defs/brazil_cnae/tables.py`: table name and column order constants for Python and tests.
- Create `corpscout/dagster_v3/src/dagster_v3/defs/brazil_cnae/mapping.py`: CSV fixture parser, row validation, NACE target validation, and direct ClickHouse stage-table replacement.
- Create `corpscout/dagster_v3/src/dagster_v3/defs/brazil_cnae/assets.py`: one Dagster asset that calls `replace_br_cnae_to_nace_clickhouse(...)`.
- Create `corpscout/dagster_v3/src/dagster_v3/defs/brazil_cnae/fixtures/br_cnae_to_nace.csv`: small seed fixture with many-to-one and one-to-many examples.
- Create `corpscout/clickhouse/migrations/000050_corpscout_br_cnae_to_nace.up.sql`: migration-owned DDL.
- Create `corpscout/clickhouse/migrations/000050_corpscout_br_cnae_to_nace.down.sql`: migration rollback.
- Modify `corpscout/dagster_v3/tests/test_clickhouse_migrations.py`: register migration `000050` and assert DDL covers exported columns.
- Create `corpscout/dagster_v3/tests/test_brazil_cnae.py`: behavior tests for parsing, validation, direct ClickHouse replacement, and Dagster definition registration.

## Task 1: Register the ClickHouse Table Contract

**Files:**
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_cnae/__init__.py`
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_cnae/tables.py`
- Create: `corpscout/clickhouse/migrations/000050_corpscout_br_cnae_to_nace.up.sql`
- Create: `corpscout/clickhouse/migrations/000050_corpscout_br_cnae_to_nace.down.sql`
- Modify: `corpscout/dagster_v3/tests/test_clickhouse_migrations.py`

- [ ] **Step 1: Write the failing migration test**

In `corpscout/dagster_v3/tests/test_clickhouse_migrations.py`, add this import near the existing table imports:

```python
from dagster_v3.defs.brazil_cnae import tables as brazil_cnae_tables
```

Append the new migration name to `EXPECTED_MIGRATIONS` after `000049_corpscout_commoncrawl_domains_nace_confidence`:

```python
    "000050_corpscout_br_cnae_to_nace",
```

Add this test near the other ClickHouse table-specific migration tests:

```python
def test_brazil_cnae_mapping_migration_covers_exported_columns() -> None:
    sql = _migration_sql("000050_corpscout_br_cnae_to_nace.up.sql")
    down_sql = _migration_sql("000050_corpscout_br_cnae_to_nace.down.sql")

    assert "CREATE TABLE IF NOT EXISTS corpscout.br_cnae_to_nace" in sql
    for column_name in brazil_cnae_tables.BR_CNAE_TO_NACE_COLUMNS:
        assert f"    {column_name} " in sql

    assert "ENGINE = ReplacingMergeTree(pulled_at)" in sql
    assert (
        "ORDER BY (cnae_version, cnae_normalized_code, nace_revision, nace_normalized_code)"
        in sql
    )
    assert "DROP TABLE IF EXISTS corpscout.br_cnae_to_nace" in down_sql
```

- [ ] **Step 2: Run the focused test to verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_clickhouse_migrations.py::test_brazil_cnae_mapping_migration_covers_exported_columns -q
```

Expected: FAIL with `ModuleNotFoundError: No module named 'dagster_v3.defs.brazil_cnae'` or missing migration file.

- [ ] **Step 3: Add the table constants**

Create `corpscout/dagster_v3/src/dagster_v3/defs/brazil_cnae/__init__.py`:

```python
"""Brazil CNAE to NACE reference mapping assets."""
```

Create `corpscout/dagster_v3/src/dagster_v3/defs/brazil_cnae/tables.py`:

```python
BRAZIL_CNAE_DATABASE = "corpscout"
BR_CNAE_TO_NACE_TABLE = "br_cnae_to_nace"
QUALIFIED_BR_CNAE_TO_NACE_TABLE = (
    f"{BRAZIL_CNAE_DATABASE}.{BR_CNAE_TO_NACE_TABLE}"
)

BR_CNAE_TO_NACE_COLUMNS = (
    "cnae_version",
    "cnae_code",
    "cnae_normalized_code",
    "cnae_description_pt",
    "cnae_description_en",
    "nace_revision",
    "nace_code",
    "nace_normalized_code",
    "nace_description_en",
    "mapping_source",
    "source_url",
    "source_payload_hash",
    "source_run_id",
    "pulled_at",
)
```

- [ ] **Step 4: Add the ClickHouse migrations**

Create `corpscout/clickhouse/migrations/000050_corpscout_br_cnae_to_nace.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.br_cnae_to_nace
(
    cnae_version LowCardinality(String),
    cnae_code String,
    cnae_normalized_code String,
    cnae_description_pt String,
    cnae_description_en String,
    nace_revision LowCardinality(String),
    nace_code String,
    nace_normalized_code String,
    nace_description_en String,
    mapping_source String,
    source_url String,
    source_payload_hash FixedString(64),
    source_run_id String,
    pulled_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(pulled_at)
ORDER BY (cnae_version, cnae_normalized_code, nace_revision, nace_normalized_code);
```

Create `corpscout/clickhouse/migrations/000050_corpscout_br_cnae_to_nace.down.sql`:

```sql
DROP TABLE IF EXISTS corpscout.br_cnae_to_nace;
```

- [ ] **Step 5: Run the focused migration tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_clickhouse_migrations.py::test_brazil_cnae_mapping_migration_covers_exported_columns tests/test_clickhouse_migrations.py::test_clickhouse_migration_files_are_explicit -q
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git -C /Users/graovic/pulsarpoint/ppoint/companycollect add \
  corpscout/dagster_v3/src/dagster_v3/defs/brazil_cnae/__init__.py \
  corpscout/dagster_v3/src/dagster_v3/defs/brazil_cnae/tables.py \
  corpscout/clickhouse/migrations/000050_corpscout_br_cnae_to_nace.up.sql \
  corpscout/clickhouse/migrations/000050_corpscout_br_cnae_to_nace.down.sql \
  corpscout/dagster_v3/tests/test_clickhouse_migrations.py
git -C /Users/graovic/pulsarpoint/ppoint/companycollect commit -m "feat: add Brazil CNAE NACE mapping table"
```

## Task 2: Parse and Validate Mapping Fixture Rows

**Files:**
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_cnae/mapping.py`
- Create: `corpscout/dagster_v3/tests/test_brazil_cnae.py`

- [ ] **Step 1: Write failing parser and validation tests**

Create `corpscout/dagster_v3/tests/test_brazil_cnae.py` with this content:

```python
from datetime import datetime, timezone
from pathlib import Path

import pytest

from dagster_v3.defs.brazil_cnae import tables
from dagster_v3.defs.brazil_cnae.mapping import (
    build_br_cnae_to_nace_rows,
    normalize_cnae_code,
    normalize_nace_code,
)


PULLED_AT = datetime(2026, 6, 24, 12, 0, tzinfo=timezone.utc)


def _write_fixture(tmp_path: Path, rows: list[str]) -> Path:
    path = tmp_path / "br_cnae_to_nace.csv"
    path.write_text(
        "\n".join(
            [
                "cnae_version,cnae_code,cnae_description_pt,cnae_description_en,"
                "nace_revision,nace_code,nace_description_en,mapping_source,source_url",
                *rows,
            ]
        )
        + "\n"
    )
    return path


def test_code_normalizers_keep_stable_lookup_keys() -> None:
    assert normalize_cnae_code("6201-5/01") == "6201501"
    assert normalize_cnae_code(" 62.01-5/01 ") == "6201501"
    assert normalize_nace_code("62.01") == "6201"
    assert normalize_nace_code(" 62.01 ") == "6201"


def test_build_rows_supports_many_to_many_edges(tmp_path: Path) -> None:
    fixture = _write_fixture(
        tmp_path,
        [
            "CNAE_2_0,6201-5/01,Desenvolvimento de programas sob encomenda,"
            "Custom software development,NACE_REV_2,62.01,"
            "Computer programming activities,ibge_concla_isic_bridge,"
            "https://example.test/cnae",
            "CNAE_2_0,6201-5/01,Desenvolvimento de programas sob encomenda,"
            "Custom software development,NACE_REV_2,62.02,"
            "Computer consultancy activities,ibge_concla_isic_bridge,"
            "https://example.test/cnae",
            "CNAE_2_0,6311-9/00,Tratamento de dados e hospedagem,"
            "Data processing and hosting,NACE_REV_2,63.11,"
            "Data processing, hosting and related activities,ibge_concla_isic_bridge,"
            "https://example.test/cnae",
        ],
    )

    rows = build_br_cnae_to_nace_rows(
        fixture_path=fixture,
        source_run_id="run-1",
        pulled_at=PULLED_AT,
        valid_nace_targets={
            ("NACE_REV_2", "6201"),
            ("NACE_REV_2", "6202"),
            ("NACE_REV_2", "6311"),
        },
    )

    assert len(rows) == 3
    assert rows[0][:4] == (
        "CNAE_2_0",
        "6201-5/01",
        "6201501",
        "Desenvolvimento de programas sob encomenda",
    )
    assert rows[0][5:8] == ("NACE_REV_2", "62.01", "6201")
    assert rows[1][5:8] == ("NACE_REV_2", "62.02", "6202")
    assert rows[2][5:8] == ("NACE_REV_2", "63.11", "6311")
    assert rows[0][11] == rows[1][11] == rows[2][11]
    assert len(rows[0][11]) == 64
    assert rows[0][12] == "run-1"
    assert rows[0][13] == PULLED_AT
    assert len({row[2] for row in rows}) == 2
    assert len({row[7] for row in rows}) == 3
    assert len(rows[0]) == len(tables.BR_CNAE_TO_NACE_COLUMNS)


def test_build_rows_rejects_duplicate_edges(tmp_path: Path) -> None:
    fixture = _write_fixture(
        tmp_path,
        [
            "CNAE_2_0,6201-5/01,Desenvolvimento,Software,NACE_REV_2,62.01,"
            "Computer programming activities,ibge_concla_isic_bridge,"
            "https://example.test/cnae",
            "CNAE_2_0,6201-5/01,Desenvolvimento,Software,NACE_REV_2,62.01,"
            "Computer programming activities,ibge_concla_isic_bridge,"
            "https://example.test/cnae",
        ],
    )

    with pytest.raises(ValueError, match="Duplicate Brazil CNAE to NACE mapping edge"):
        build_br_cnae_to_nace_rows(
            fixture_path=fixture,
            source_run_id="run-1",
            pulled_at=PULLED_AT,
            valid_nace_targets={("NACE_REV_2", "6201")},
        )


def test_build_rows_rejects_missing_required_values(tmp_path: Path) -> None:
    fixture = _write_fixture(
        tmp_path,
        [
            "CNAE_2_0,,Desenvolvimento,Software,NACE_REV_2,62.01,"
            "Computer programming activities,ibge_concla_isic_bridge,"
            "https://example.test/cnae",
        ],
    )

    with pytest.raises(ValueError, match="Missing required fixture value: cnae_code"):
        build_br_cnae_to_nace_rows(
            fixture_path=fixture,
            source_run_id="run-1",
            pulled_at=PULLED_AT,
            valid_nace_targets={("NACE_REV_2", "6201")},
        )


def test_build_rows_rejects_unknown_nace_targets(tmp_path: Path) -> None:
    fixture = _write_fixture(
        tmp_path,
        [
            "CNAE_2_0,6201-5/01,Desenvolvimento,Software,NACE_REV_2,62.01,"
            "Computer programming activities,ibge_concla_isic_bridge,"
            "https://example.test/cnae",
        ],
    )

    with pytest.raises(ValueError, match="Unknown NACE target"):
        build_br_cnae_to_nace_rows(
            fixture_path=fixture,
            source_run_id="run-1",
            pulled_at=PULLED_AT,
            valid_nace_targets={("NACE_REV_2", "9999")},
        )
```

- [ ] **Step 2: Run the parser tests to verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_brazil_cnae.py::test_code_normalizers_keep_stable_lookup_keys tests/test_brazil_cnae.py::test_build_rows_supports_many_to_many_edges -q
```

Expected: FAIL with `ModuleNotFoundError: No module named 'dagster_v3.defs.brazil_cnae.mapping'`.

- [ ] **Step 3: Implement parser and row validation**

Create `corpscout/dagster_v3/src/dagster_v3/defs/brazil_cnae/mapping.py` with this initial content:

```python
from __future__ import annotations

import csv
import hashlib
import re
from collections.abc import Iterable, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dagster_v3.defs.brazil_cnae import tables

REQUIRED_FIXTURE_COLUMNS = (
    "cnae_version",
    "cnae_code",
    "cnae_description_pt",
    "cnae_description_en",
    "nace_revision",
    "nace_code",
    "nace_description_en",
    "mapping_source",
    "source_url",
)

NormalizedNaceTarget = tuple[str, str]
MappingRow = tuple[Any, ...]


def normalize_cnae_code(value: str) -> str:
    return re.sub(r"[^0-9]", "", value.strip())


def normalize_nace_code(value: str) -> str:
    return re.sub(r"[^0-9A-Za-z]", "", value.strip())


def build_br_cnae_to_nace_rows(
    *,
    fixture_path: str | Path,
    source_run_id: str,
    valid_nace_targets: set[NormalizedNaceTarget],
    pulled_at: datetime | None = None,
) -> list[MappingRow]:
    fixture = Path(fixture_path)
    payload = fixture.read_bytes()
    source_payload_hash = hashlib.sha256(payload).hexdigest()
    resolved_pulled_at = pulled_at or datetime.now(timezone.utc)
    rows: list[MappingRow] = []
    seen_edges: set[tuple[str, str, str, str]] = set()

    with fixture.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        _validate_fixture_header(reader.fieldnames)
        for line_number, raw_row in enumerate(reader, start=2):
            row = _normalized_fixture_row(raw_row, line_number=line_number)
            cnae_normalized_code = normalize_cnae_code(row["cnae_code"])
            nace_normalized_code = normalize_nace_code(row["nace_code"])
            if not cnae_normalized_code:
                raise ValueError(f"Empty normalized CNAE code at line {line_number}")
            if not nace_normalized_code:
                raise ValueError(f"Empty normalized NACE code at line {line_number}")

            edge = (
                row["cnae_version"],
                cnae_normalized_code,
                row["nace_revision"],
                nace_normalized_code,
            )
            if edge in seen_edges:
                raise ValueError(
                    "Duplicate Brazil CNAE to NACE mapping edge: "
                    f"{row['cnae_version']} {row['cnae_code']} -> "
                    f"{row['nace_revision']} {row['nace_code']}"
                )
            seen_edges.add(edge)

            if (row["nace_revision"], nace_normalized_code) not in valid_nace_targets:
                raise ValueError(
                    "Unknown NACE target: "
                    f"{row['nace_revision']} {row['nace_code']} "
                    f"(normalized {nace_normalized_code})"
                )

            rows.append(
                (
                    row["cnae_version"],
                    row["cnae_code"],
                    cnae_normalized_code,
                    row["cnae_description_pt"],
                    row["cnae_description_en"],
                    row["nace_revision"],
                    row["nace_code"],
                    nace_normalized_code,
                    row["nace_description_en"],
                    row["mapping_source"],
                    row["source_url"],
                    source_payload_hash,
                    source_run_id,
                    resolved_pulled_at,
                )
            )

    if not rows:
        raise ValueError("Brazil CNAE to NACE fixture produced no rows")
    return rows


def _validate_fixture_header(fieldnames: Sequence[str] | None) -> None:
    if fieldnames is None:
        raise ValueError("Brazil CNAE to NACE fixture is missing a header row")
    missing_columns = [
        column for column in REQUIRED_FIXTURE_COLUMNS if column not in fieldnames
    ]
    if missing_columns:
        raise ValueError(
            "Brazil CNAE to NACE fixture missing columns: "
            + ", ".join(missing_columns)
        )


def _normalized_fixture_row(
    raw_row: dict[str, str | None],
    *,
    line_number: int,
) -> dict[str, str]:
    row: dict[str, str] = {}
    for column in REQUIRED_FIXTURE_COLUMNS:
        value = (raw_row.get(column) or "").strip()
        if not value:
            raise ValueError(
                f"Missing required fixture value: {column} at line {line_number}"
            )
        row[column] = value
    return row
```

- [ ] **Step 4: Run the parser tests to verify they pass**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_brazil_cnae.py::test_code_normalizers_keep_stable_lookup_keys tests/test_brazil_cnae.py::test_build_rows_supports_many_to_many_edges tests/test_brazil_cnae.py::test_build_rows_rejects_duplicate_edges tests/test_brazil_cnae.py::test_build_rows_rejects_missing_required_values tests/test_brazil_cnae.py::test_build_rows_rejects_unknown_nace_targets -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git -C /Users/graovic/pulsarpoint/ppoint/companycollect add \
  corpscout/dagster_v3/src/dagster_v3/defs/brazil_cnae/mapping.py \
  corpscout/dagster_v3/tests/test_brazil_cnae.py
git -C /Users/graovic/pulsarpoint/ppoint/companycollect commit -m "feat: parse Brazil CNAE NACE mapping fixture"
```

## Task 3: Publish Mapping Rows Directly to ClickHouse

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_cnae/mapping.py`
- Modify: `corpscout/dagster_v3/tests/test_brazil_cnae.py`

- [ ] **Step 1: Add failing tests for ClickHouse replacement**

Append these helpers and tests to `corpscout/dagster_v3/tests/test_brazil_cnae.py`:

```python
class FakeClickHouseClient:
    def __init__(self) -> None:
        self.statements: list[tuple[str, object | None]] = []
        self.nace_targets = [
            ("NACE_REV_2", "6201", "Computer programming activities"),
            ("NACE_REV_2", "6202", "Computer consultancy activities"),
            ("NACE_REV_2", "6311", "Data processing, hosting and related activities"),
        ]

    def execute(
        self,
        sql: str,
        params: object | None = None,
    ) -> list[tuple[object, ...]]:
        self.statements.append((sql, params))
        if "FROM `corpscout`.`nace_categories`" in sql:
            return self.nace_targets
        return []


def test_replace_clickhouse_uses_stage_insert_exchange_and_drop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dagster_v3.defs.brazil_cnae import mapping

    fixture = _write_fixture(
        tmp_path,
        [
            "CNAE_2_0,6201-5/01,Desenvolvimento de programas sob encomenda,"
            "Custom software development,NACE_REV_2,62.01,"
            "Computer programming activities,ibge_concla_isic_bridge,"
            "https://example.test/cnae",
            "CNAE_2_0,6201-5/01,Desenvolvimento de programas sob encomenda,"
            "Custom software development,NACE_REV_2,62.02,"
            "Computer consultancy activities,ibge_concla_isic_bridge,"
            "https://example.test/cnae",
        ],
    )
    monkeypatch.setattr(
        mapping.uuid,
        "uuid4",
        lambda: type("U", (), {"hex": "abc123"})(),
    )
    client = FakeClickHouseClient()

    counts = mapping.replace_br_cnae_to_nace_clickhouse(
        clickhouse_client=client,
        fixture_path=fixture,
        source_run_id="run-1",
        pulled_at=PULLED_AT,
    )

    assert counts == {"rows": 2, "cnae_codes": 1, "nace_targets": 2}
    assert client.statements[0][0] == (
        "SELECT classification_version, normalized_code, description_en "
        "FROM `corpscout`.`nace_categories`"
    )
    assert client.statements[1][0] == (
        "CREATE TABLE `corpscout`.`_tmp_br_cnae_to_nace_abc123` AS "
        "`corpscout`.`br_cnae_to_nace`"
    )
    insert_sql, insert_params = client.statements[2]
    assert insert_sql.startswith(
        "INSERT INTO `corpscout`.`_tmp_br_cnae_to_nace_abc123` "
        "(cnae_version, cnae_code, cnae_normalized_code"
    )
    assert isinstance(insert_params, list)
    assert len(insert_params) == 2
    assert client.statements[3][0] == (
        "EXCHANGE TABLES `corpscout`.`_tmp_br_cnae_to_nace_abc123` "
        "AND `corpscout`.`br_cnae_to_nace`"
    )
    assert client.statements[-1][0] == (
        "DROP TABLE IF EXISTS `corpscout`.`_tmp_br_cnae_to_nace_abc123`"
    )


def test_replace_clickhouse_drops_stage_after_insert_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dagster_v3.defs.brazil_cnae import mapping

    class FailingInsertClient(FakeClickHouseClient):
        def execute(
            self,
            sql: str,
            params: object | None = None,
        ) -> list[tuple[object, ...]]:
            self.statements.append((sql, params))
            if "FROM `corpscout`.`nace_categories`" in sql:
                return self.nace_targets
            if sql.startswith("INSERT INTO"):
                raise RuntimeError("insert failed")
            return []

    fixture = _write_fixture(
        tmp_path,
        [
            "CNAE_2_0,6201-5/01,Desenvolvimento,Software,NACE_REV_2,62.01,"
            "Computer programming activities,ibge_concla_isic_bridge,"
            "https://example.test/cnae",
        ],
    )
    monkeypatch.setattr(
        mapping.uuid,
        "uuid4",
        lambda: type("U", (), {"hex": "failed"})(),
    )
    client = FailingInsertClient()

    with pytest.raises(RuntimeError, match="insert failed"):
        mapping.replace_br_cnae_to_nace_clickhouse(
            clickhouse_client=client,
            fixture_path=fixture,
            source_run_id="run-1",
            pulled_at=PULLED_AT,
        )

    assert client.statements[-1][0] == (
        "DROP TABLE IF EXISTS `corpscout`.`_tmp_br_cnae_to_nace_failed`"
    )
```

- [ ] **Step 2: Run the new replacement test to verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_brazil_cnae.py::test_replace_clickhouse_uses_stage_insert_exchange_and_drop -q
```

Expected: FAIL with `AttributeError: module 'dagster_v3.defs.brazil_cnae.mapping' has no attribute 'uuid'` or missing `replace_br_cnae_to_nace_clickhouse`.

- [ ] **Step 3: Implement direct ClickHouse replacement**

Update `corpscout/dagster_v3/src/dagster_v3/defs/brazil_cnae/mapping.py` so it includes these imports and functions. Keep the parser functions from Task 2.

Add imports:

```python
import uuid
```

Add these functions below `build_br_cnae_to_nace_rows(...)`:

```python
def replace_br_cnae_to_nace_clickhouse(
    *,
    clickhouse_client: Any,
    fixture_path: str | Path,
    source_run_id: str,
    pulled_at: datetime | None = None,
) -> dict[str, int]:
    valid_nace_targets = _load_valid_nace_targets(clickhouse_client)
    rows = build_br_cnae_to_nace_rows(
        fixture_path=fixture_path,
        source_run_id=source_run_id,
        valid_nace_targets=valid_nace_targets,
        pulled_at=pulled_at,
    )
    stage_table = f"_tmp_{tables.BR_CNAE_TO_NACE_TABLE}_{uuid.uuid4().hex}"
    qualified_stage_table = _qualified_clickhouse_table(stage_table)
    qualified_target_table = _qualified_clickhouse_table(tables.BR_CNAE_TO_NACE_TABLE)
    primary_error: Exception | None = None

    try:
        clickhouse_client.execute(
            f"CREATE TABLE {qualified_stage_table} AS {qualified_target_table}"
        )
        columns = ", ".join(tables.BR_CNAE_TO_NACE_COLUMNS)
        clickhouse_client.execute(
            f"INSERT INTO {qualified_stage_table} ({columns}) VALUES",
            rows,
        )
        clickhouse_client.execute(
            f"EXCHANGE TABLES {qualified_stage_table} AND {qualified_target_table}"
        )
    except Exception as exc:
        primary_error = exc
        raise
    finally:
        try:
            clickhouse_client.execute(f"DROP TABLE IF EXISTS {qualified_stage_table}")
        except Exception:
            if primary_error is None:
                raise

    return {
        "rows": len(rows),
        "cnae_codes": len({str(row[2]) for row in rows}),
        "nace_targets": len({(str(row[5]), str(row[7])) for row in rows}),
    }


def _load_valid_nace_targets(clickhouse_client: Any) -> set[NormalizedNaceTarget]:
    rows = clickhouse_client.execute(
        "SELECT classification_version, normalized_code, description_en "
        f"FROM {_qualified_clickhouse_table('nace_categories')}"
    )
    targets = {(str(row[0]), str(row[1])) for row in rows}
    if not targets:
        raise ValueError("No NACE categories available for Brazil CNAE mapping validation")
    return targets


def _qualified_clickhouse_table(table: str) -> str:
    return (
        f"{_quote_clickhouse_identifier(tables.BRAZIL_CNAE_DATABASE)}."
        f"{_quote_clickhouse_identifier(table)}"
    )


def _quote_clickhouse_identifier(identifier: str) -> str:
    escaped = identifier.replace("`", "``")
    return f"`{escaped}`"
```

- [ ] **Step 4: Run all Brazil CNAE behavior tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_brazil_cnae.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git -C /Users/graovic/pulsarpoint/ppoint/companycollect add \
  corpscout/dagster_v3/src/dagster_v3/defs/brazil_cnae/mapping.py \
  corpscout/dagster_v3/tests/test_brazil_cnae.py
git -C /Users/graovic/pulsarpoint/ppoint/companycollect commit -m "feat: publish Brazil CNAE NACE mapping to ClickHouse"
```

## Task 4: Add the Dagster Asset and Seed Fixture

**Files:**
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_cnae/assets.py`
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_cnae/fixtures/br_cnae_to_nace.csv`
- Modify: `corpscout/dagster_v3/tests/test_brazil_cnae.py`

- [ ] **Step 1: Add failing asset registration tests**

Append these tests to `corpscout/dagster_v3/tests/test_brazil_cnae.py`:

```python
def test_seed_fixture_builds_non_empty_rows() -> None:
    from dagster_v3.defs.brazil_cnae.assets import BR_CNAE_TO_NACE_FIXTURE

    rows = build_br_cnae_to_nace_rows(
        fixture_path=BR_CNAE_TO_NACE_FIXTURE,
        source_run_id="seed-test",
        pulled_at=PULLED_AT,
        valid_nace_targets={
            ("NACE_REV_2", "6201"),
            ("NACE_REV_2", "6202"),
            ("NACE_REV_2", "6311"),
        },
    )

    assert len(rows) == 3


def test_brazil_cnae_asset_is_registered() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    keys = {key.to_user_string() for key in repo.asset_graph.get_all_asset_keys()}

    assert "brazil_cnae_to_nace_clickhouse" in keys
```

- [ ] **Step 2: Run the asset registration test to verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_brazil_cnae.py::test_brazil_cnae_asset_is_registered -q
```

Expected: FAIL because `dagster_v3.defs.brazil_cnae.assets` does not exist or the asset key is absent.

- [ ] **Step 3: Add the seed fixture**

Create directory `corpscout/dagster_v3/src/dagster_v3/defs/brazil_cnae/fixtures`.

Create `corpscout/dagster_v3/src/dagster_v3/defs/brazil_cnae/fixtures/br_cnae_to_nace.csv`:

```csv
cnae_version,cnae_code,cnae_description_pt,cnae_description_en,nace_revision,nace_code,nace_description_en,mapping_source,source_url
CNAE_2_0,6201-5/01,Desenvolvimento de programas de computador sob encomenda,Custom software development,NACE_REV_2,62.01,Computer programming activities,seed_fixture_isic_bridge,https://concla.ibge.gov.br/
CNAE_2_0,6201-5/01,Desenvolvimento de programas de computador sob encomenda,Custom software development,NACE_REV_2,62.02,Computer consultancy activities,seed_fixture_isic_bridge,https://concla.ibge.gov.br/
CNAE_2_0,6311-9/00,Tratamento de dados provedores de serviços de aplicação e serviços de hospedagem na internet,Data processing hosting and application service providers,NACE_REV_2,63.11,Data processing, hosting and related activities,seed_fixture_isic_bridge,https://concla.ibge.gov.br/
```

- [ ] **Step 4: Add the Dagster asset**

Create `corpscout/dagster_v3/src/dagster_v3/defs/brazil_cnae/assets.py`:

```python
from pathlib import Path

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.brazil_cnae.mapping import (
    replace_br_cnae_to_nace_clickhouse,
)

GROUP_NAME = "brazil_cnae"
BR_CNAE_TO_NACE_FIXTURE = Path(__file__).parent / "fixtures" / "br_cnae_to_nace.csv"


@dg.asset(
    deps=[dg.AssetKey("nace_categories_clickhouse")],
    group_name=GROUP_NAME,
    kinds={"clickhouse", "reference"},
    description=(
        "Brazil CNAE to NACE many-to-many mapping edges from curated fixture data."
    ),
)
def brazil_cnae_to_nace_clickhouse(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    with clickhouse.get_connection() as client:
        counts = replace_br_cnae_to_nace_clickhouse(
            clickhouse_client=client,
            fixture_path=BR_CNAE_TO_NACE_FIXTURE,
            source_run_id=context.run_id,
        )

    context.log.info("Published Brazil CNAE to NACE mapping: counts=%s", counts)
    return dg.MaterializeResult(metadata=counts)
```

- [ ] **Step 5: Run the asset tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_brazil_cnae.py::test_seed_fixture_builds_non_empty_rows tests/test_brazil_cnae.py::test_brazil_cnae_asset_is_registered -q
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git -C /Users/graovic/pulsarpoint/ppoint/companycollect add \
  corpscout/dagster_v3/src/dagster_v3/defs/brazil_cnae/assets.py \
  corpscout/dagster_v3/src/dagster_v3/defs/brazil_cnae/fixtures/br_cnae_to_nace.csv \
  corpscout/dagster_v3/tests/test_brazil_cnae.py
git -C /Users/graovic/pulsarpoint/ppoint/companycollect commit -m "feat: add Brazil CNAE mapping Dagster asset"
```

## Task 5: Final Verification and Notes

**Files:**
- Verify: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_cnae/*`
- Verify: `corpscout/dagster_v3/tests/test_brazil_cnae.py`
- Verify: `corpscout/dagster_v3/tests/test_clickhouse_migrations.py`
- Verify: `corpscout/clickhouse/migrations/000050_corpscout_br_cnae_to_nace.*.sql`

- [ ] **Step 1: Run focused tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_brazil_cnae.py tests/test_clickhouse_migrations.py -q
```

Expected: PASS.

- [ ] **Step 2: Run definitions load smoke test**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run python - <<'PY'
from dagster_v3.definitions import defs
repo = defs().get_repository_def()
keys = {key.to_user_string() for key in repo.asset_graph.get_all_asset_keys()}
assert "brazil_cnae_to_nace_clickhouse" in keys
print("ok")
PY
```

Expected: prints `ok`.

- [ ] **Step 3: Run full Dagster test suite**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest -q
```

Expected: PASS.

- [ ] **Step 4: Run diff whitespace check**

Run:

```bash
git -C /Users/graovic/pulsarpoint/ppoint/companycollect diff --check
```

Expected: no output.

- [ ] **Step 5: Commit any final test-only fixes**

If Step 1 through Step 4 required small test/import corrections, commit them:

```bash
git -C /Users/graovic/pulsarpoint/ppoint/companycollect status --short
git -C /Users/graovic/pulsarpoint/ppoint/companycollect add \
  corpscout/dagster_v3/src/dagster_v3/defs/brazil_cnae \
  corpscout/dagster_v3/tests/test_brazil_cnae.py \
  corpscout/dagster_v3/tests/test_clickhouse_migrations.py \
  corpscout/clickhouse/migrations/000050_corpscout_br_cnae_to_nace.up.sql \
  corpscout/clickhouse/migrations/000050_corpscout_br_cnae_to_nace.down.sql
git -C /Users/graovic/pulsarpoint/ppoint/companycollect commit -m "test: verify Brazil CNAE mapping reference"
```

Expected: either a new commit is created for real final fixes or `git status --short` shows no changes and no commit is needed.

## Self-Review

Spec coverage:

- Brazil-specific table: Task 1.
- Many-to-many edge modeling: Task 2 tests and parser.
- No generic mapping table: table and package are `brazil_cnae` only.
- No DuckDB or dlt: Task 4 asset calls a direct ClickHouse replace function.
- Migration-owned DDL: Task 1.
- Stage-table ClickHouse replacement: Task 3.
- NACE target validation: Task 2 and Task 3.
- Dagster materialization: Task 4.
- Tests and verification: Tasks 1 through 5.

Red-flag scan:

- The seed fixture is intentionally small and runnable. The complete curated fixture expansion is a separate data input before Brazil company ingestion, not an incomplete code step in this implementation plan.
- No steps require unlisted files or undefined functions after the tasks that create them.

Type consistency:

- `replace_br_cnae_to_nace_clickhouse(...)` signature is consistent between tests, implementation, and asset.
- Table columns are defined once in `tables.BR_CNAE_TO_NACE_COLUMNS` and used by tests and ClickHouse insert code.
- Normalized NACE target tuple shape is consistently `(nace_revision, nace_normalized_code)`.
