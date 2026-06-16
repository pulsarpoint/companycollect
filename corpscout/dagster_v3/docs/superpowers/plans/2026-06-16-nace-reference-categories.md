# NACE Reference Categories Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Dagster `nace_categories` asset that pulls official NACE Rev. 2 and Rev. 2.1 categories and loads the canonical reference table directly into ClickHouse.

**Architecture:** Use a typed Python parser and dlt source for the EU Publications Office SPARQL CSV output. The Dagster asset ensures the ClickHouse database/table exists, truncates the table, then runs dlt with a ClickHouse destination and `write_disposition="append"`. No DuckDB staging asset is created for NACE because this is small reference data with no pre-warehouse join/filter step.

**Tech Stack:** Dagster, dagster-dlt, dlt ClickHouse destination, ClickHouse, requests, CSV, pytest.

---

## File Structure

- Create `src/dagster_v3/defs/nace/__init__.py`: package marker.
- Create `src/dagster_v3/defs/nace/tables.py`: ClickHouse table name, column list, DDL, and version constants.
- Create `src/dagster_v3/defs/nace/resources.py`: ClickHouse resource copied/adapted from older Dagster code with test-injected client support.
- Create `src/dagster_v3/defs/nace/source.py`: SPARQL query definitions, row parser, dlt source/resource, and dlt pipeline factory.
- Create `src/dagster_v3/defs/nace/assets.py`: Dagster dlt asset, translator, schema creation call, and definitions object.
- Create `tests/test_nace_categories.py`: parser, dlt source, asset graph, DDL, and resource tests.
- Modify `pyproject.toml`: install the ClickHouse Python driver used by dlt's ClickHouse destination.

## Source Constants

Use these official EU Publications Office SPARQL details:

```python
SPARQL_ENDPOINT = "https://publications.europa.eu/webapi/rdf/sparql"
NACE_SCHEMES = {
    "NACE_REV_2": {
        "scheme_uri": "http://data.europa.eu/ux2/nace2/nace2",
        "valid_from": "2008-01-01",
        "valid_to": "2024-12-31",
        "is_current": 0,
    },
    "NACE_REV_2_1": {
        "scheme_uri": "http://data.europa.eu/ux2/nace2.1/nace2.1",
        "valid_from": "2025-01-01",
        "valid_to": None,
        "is_current": 1,
    },
}
```

Use this SPARQL query shape per scheme:

```sparql
prefix skos: <http://www.w3.org/2004/02/skos/core#>
select ?concept ?notation ?label ?broader where {
  ?concept skos:inScheme <SCHEME_URI> ;
           skos:notation ?notation ;
           skos:prefLabel ?label .
  filter(lang(?label) = "en")
  optional { ?concept skos:broader ?broader . }
}
order by ?notation
```

## Task 1: Add ClickHouse Dependency

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Write the failing dependency check**

Add this test to `tests/test_nace_categories.py`:

```python
def test_dlt_clickhouse_destination_dependencies_are_available() -> None:
    import clickhouse_connect
    import dlt

    assert clickhouse_connect
    assert hasattr(dlt.destinations, "clickhouse")
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
uv run pytest tests/test_nace_categories.py::test_dlt_clickhouse_destination_dependencies_are_available -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'clickhouse_connect'`.

- [ ] **Step 3: Update dependency**

In `pyproject.toml`, keep:

```toml
"dlt[duckdb]>=1.27.2",
```

and add:

```toml
"clickhouse-connect>=0.10.0",
```

Do not use `dlt[clickhouse]`: in this project it conflicts with `boto3>=1.43.29` through `s3fs` / `aiobotocore` / `botocore` constraints. The installed `dlt` package already exposes `dlt.destinations.clickhouse`; the missing runtime dependency is the ClickHouse driver.

- [ ] **Step 4: Sync and verify**

Run:

```bash
uv sync
uv run pytest tests/test_nace_categories.py::test_dlt_clickhouse_destination_dependencies_are_available -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml uv.lock tests/test_nace_categories.py
git commit -m "Add ClickHouse dlt dependency for NACE"
```

## Task 2: Define ClickHouse Table Contract

**Files:**
- Create: `src/dagster_v3/defs/nace/__init__.py`
- Create: `src/dagster_v3/defs/nace/tables.py`
- Test: `tests/test_nace_categories.py`

- [ ] **Step 1: Write the failing table contract test**

Append:

```python
from dagster_v3.defs.nace import tables


def test_nace_categories_clickhouse_schema_contract() -> None:
    assert tables.NACE_DATABASE == "reference"
    assert tables.NACE_CATEGORIES_TABLE == "nace_categories"
    assert tables.QUALIFIED_NACE_CATEGORIES_TABLE == "reference.nace_categories"
    assert tables.NACE_CATEGORIES_COLUMNS == (
        "classification_version",
        "code",
        "normalized_code",
        "parent_code",
        "level",
        "section_code",
        "description_en",
        "concept_uri",
        "parent_concept_uri",
        "source_scheme_uri",
        "source_url",
        "source_payload_hash",
        "valid_from",
        "valid_to",
        "is_current",
        "source_run_id",
        "pulled_at",
    )
    assert "CREATE TABLE IF NOT EXISTS reference.nace_categories" in tables.NACE_CATEGORIES_DDL
    assert "ORDER BY (classification_version, normalized_code)" in tables.NACE_CATEGORIES_DDL
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
uv run pytest tests/test_nace_categories.py::test_nace_categories_clickhouse_schema_contract -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'dagster_v3.defs.nace'`.

- [ ] **Step 3: Create package and table constants**

Create `src/dagster_v3/defs/nace/__init__.py` as an empty file.

Create `src/dagster_v3/defs/nace/tables.py`:

```python
NACE_DATABASE = "reference"
NACE_CATEGORIES_TABLE = "nace_categories"
QUALIFIED_NACE_CATEGORIES_TABLE = f"{NACE_DATABASE}.{NACE_CATEGORIES_TABLE}"

NACE_CATEGORIES_COLUMNS = (
    "classification_version",
    "code",
    "normalized_code",
    "parent_code",
    "level",
    "section_code",
    "description_en",
    "concept_uri",
    "parent_concept_uri",
    "source_scheme_uri",
    "source_url",
    "source_payload_hash",
    "valid_from",
    "valid_to",
    "is_current",
    "source_run_id",
    "pulled_at",
)

NACE_CATEGORIES_DDL = f"""
CREATE TABLE IF NOT EXISTS {QUALIFIED_NACE_CATEGORIES_TABLE}
(
    classification_version LowCardinality(String),
    code String,
    normalized_code String,
    parent_code Nullable(String),
    level LowCardinality(String),
    section_code Nullable(String),
    description_en String,
    concept_uri String,
    parent_concept_uri Nullable(String),
    source_scheme_uri String,
    source_url String,
    source_payload_hash FixedString(64),
    valid_from Date,
    valid_to Nullable(Date),
    is_current UInt8,
    source_run_id String,
    pulled_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (classification_version, normalized_code)
"""
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
uv run pytest tests/test_nace_categories.py::test_nace_categories_clickhouse_schema_contract -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/nace/__init__.py src/dagster_v3/defs/nace/tables.py tests/test_nace_categories.py
git commit -m "Define NACE ClickHouse table contract"
```

## Task 3: Add ClickHouse Resource And DDL Helper

**Files:**
- Create: `src/dagster_v3/defs/nace/resources.py`
- Test: `tests/test_nace_categories.py`

- [ ] **Step 1: Write failing resource tests**

Append:

```python
from dagster_v3.defs.nace.resources import ClickHouseResource, prepare_nace_categories_table


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.commands: list[str] = []
        self.inserts: list[tuple[str, list[list[object]], list[str]]] = []

    def command(self, sql: str) -> None:
        self.commands.append(sql)

    def insert(self, table: str, data: list[list[object]], column_names: list[str]) -> None:
        self.inserts.append((table, data, column_names))


def test_clickhouse_resource_reuses_supplied_client_for_table_setup() -> None:
    client = FakeClickHouseClient()
    resource = ClickHouseResource(
        host="localhost",
        password="secret",
        clickhouse_client=client,
    )

    prepare_nace_categories_table(resource)

    assert client.commands[0] == "CREATE DATABASE IF NOT EXISTS reference"
    assert client.commands[1].startswith("CREATE TABLE IF NOT EXISTS reference.nace_categories")
    assert client.commands[2] == "TRUNCATE TABLE reference.nace_categories"
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
uv run pytest tests/test_nace_categories.py::test_clickhouse_resource_reuses_supplied_client_for_table_setup -v
```

Expected: FAIL with `ModuleNotFoundError` or missing resource symbol.

- [ ] **Step 3: Implement resource**

Create `src/dagster_v3/defs/nace/resources.py`:

```python
from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol

import clickhouse_connect
import dagster as dg
from pydantic import PrivateAttr

from dagster_v3.defs.nace import tables


class ClickHouseClient(Protocol):
    def command(self, sql: str) -> Any:
        ...

    def insert(self, table: str, data: Sequence[Sequence[Any]], column_names: Sequence[str]) -> Any:
        ...


class ClickHouseResource(dg.ConfigurableResource):
    host: str
    port: str = "8123"
    username: str = "default"
    password: str
    database: str = "default"
    secure: str = "false"

    _client: ClickHouseClient | None = PrivateAttr(default=None)

    def __init__(self, clickhouse_client: ClickHouseClient | None = None, **data: Any) -> None:
        super().__init__(**data)
        self._client = clickhouse_client

    def client(self) -> ClickHouseClient:
        if self._client is None:
            self._client = clickhouse_connect.get_client(
                host=self.host,
                port=int(self.port),
                username=self.username,
                password=self.password,
                database=self.database,
                secure=self.secure.lower() == "true",
            )
        return self._client


def prepare_nace_categories_table(clickhouse: ClickHouseResource) -> None:
    client = clickhouse.client()
    client.command(f"CREATE DATABASE IF NOT EXISTS {tables.NACE_DATABASE}")
    client.command(tables.NACE_CATEGORIES_DDL)
    client.command(f"TRUNCATE TABLE {tables.QUALIFIED_NACE_CATEGORIES_TABLE}")
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
uv run pytest tests/test_nace_categories.py::test_clickhouse_resource_reuses_supplied_client_for_table_setup -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/nace/resources.py tests/test_nace_categories.py
git commit -m "Add ClickHouse resource for NACE schema setup"
```

## Task 4: Implement NACE Row Parser

**Files:**
- Create: `src/dagster_v3/defs/nace/source.py`
- Test: `tests/test_nace_categories.py`

- [ ] **Step 1: Write failing parser tests**

Append:

```python
from dagster_v3.defs.nace.source import NaceScheme, build_nace_rows, normalize_nace_code


def test_normalize_nace_code_preserves_sections_and_strips_numeric_punctuation() -> None:
    assert normalize_nace_code("A") == "A"
    assert normalize_nace_code("01") == "01"
    assert normalize_nace_code("01.1") == "011"
    assert normalize_nace_code("01.11") == "0111"


def test_build_nace_rows_preserves_hierarchy_and_versions() -> None:
    scheme = NaceScheme(
        classification_version="NACE_REV_2_1",
        scheme_uri="http://data.europa.eu/ux2/nace2.1/nace2.1",
        valid_from="2025-01-01",
        valid_to=None,
        is_current=1,
    )
    source_rows = [
        {
            "concept": "http://data.europa.eu/ux2/nace2.1/A",
            "notation": "A",
            "label": "A AGRICULTURE, FORESTRY AND FISHING",
            "broader": "",
        },
        {
            "concept": "http://data.europa.eu/ux2/nace2.1/01",
            "notation": "01",
            "label": "01 Crop and animal production, hunting and related service activities",
            "broader": "http://data.europa.eu/ux2/nace2.1/A",
        },
        {
            "concept": "http://data.europa.eu/ux2/nace2.1/011",
            "notation": "01.1",
            "label": "01.1 Growing of non-perennial crops",
            "broader": "http://data.europa.eu/ux2/nace2.1/01",
        },
        {
            "concept": "http://data.europa.eu/ux2/nace2.1/0111",
            "notation": "01.11",
            "label": "01.11 Growing of cereals, other than rice, leguminous crops and oil seeds",
            "broader": "http://data.europa.eu/ux2/nace2.1/011",
        },
    ]

    rows = build_nace_rows(
        scheme=scheme,
        source_rows=source_rows,
        source_url="https://publications.europa.eu/webapi/rdf/sparql",
        source_payload_hash="a" * 64,
        source_run_id="run-1",
        pulled_at="2026-06-16T00:00:00.000Z",
    )

    assert [row["level"] for row in rows] == ["section", "division", "group", "class"]
    assert rows[0]["section_code"] == "A"
    assert rows[1]["parent_code"] == "A"
    assert rows[2]["parent_code"] == "01"
    assert rows[3]["parent_code"] == "01.1"
    assert rows[3]["normalized_code"] == "0111"
    assert rows[3]["classification_version"] == "NACE_REV_2_1"
    assert rows[3]["valid_from"] == "2025-01-01"
    assert rows[3]["valid_to"] is None
    assert rows[3]["is_current"] == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/test_nace_categories.py::test_normalize_nace_code_preserves_sections_and_strips_numeric_punctuation tests/test_nace_categories.py::test_build_nace_rows_preserves_hierarchy_and_versions -v
```

Expected: FAIL with missing `dagster_v3.defs.nace.source`.

- [ ] **Step 3: Implement parser**

Create `src/dagster_v3/defs/nace/source.py` with parser-only content first:

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class NaceScheme:
    classification_version: str
    scheme_uri: str
    valid_from: str
    valid_to: str | None
    is_current: int


def normalize_nace_code(code: str) -> str:
    stripped = code.strip()
    return stripped if stripped.isalpha() else stripped.replace(".", "")


def nace_level(code: str) -> str:
    normalized = normalize_nace_code(code)
    if normalized.isalpha():
        return "section"
    if len(normalized) == 2:
        return "division"
    if len(normalized) == 3:
        return "group"
    if len(normalized) == 4:
        return "class"
    raise ValueError(f"Unsupported NACE code level: {code}")


def concept_code(concept_uri: str) -> str | None:
    value = concept_uri.rstrip("/").rsplit("/", maxsplit=1)[-1]
    if not value:
        return None
    if value.isalpha():
        return value
    if len(value) == 2:
        return value
    if len(value) == 3:
        return f"{value[:2]}.{value[2:]}"
    if len(value) == 4:
        return f"{value[:2]}.{value[2:]}"
    return value


def build_nace_rows(
    *,
    scheme: NaceScheme,
    source_rows: list[dict[str, str]],
    source_url: str,
    source_payload_hash: str,
    source_run_id: str,
    pulled_at: str,
) -> list[dict[str, Any]]:
    if not source_rows:
        raise ValueError(f"{scheme.classification_version} returned no NACE rows")

    section_by_prefix = {
        row["notation"].strip()[:1]: row["notation"].strip()
        for row in source_rows
        if row.get("notation", "").strip().isalpha()
    }
    rows: list[dict[str, Any]] = []
    for source_row in source_rows:
        code = source_row["notation"].strip()
        normalized_code = normalize_nace_code(code)
        level = nace_level(code)
        parent_concept_uri = source_row.get("broader", "").strip() or None
        parent_code = concept_code(parent_concept_uri) if parent_concept_uri else None
        section_code = code if level == "section" else section_by_prefix.get(normalized_code[:1])
        rows.append(
            {
                "classification_version": scheme.classification_version,
                "code": code,
                "normalized_code": normalized_code,
                "parent_code": parent_code,
                "level": level,
                "section_code": section_code,
                "description_en": source_row["label"].strip(),
                "concept_uri": source_row["concept"].strip(),
                "parent_concept_uri": parent_concept_uri,
                "source_scheme_uri": scheme.scheme_uri,
                "source_url": source_url,
                "source_payload_hash": source_payload_hash,
                "valid_from": scheme.valid_from,
                "valid_to": scheme.valid_to,
                "is_current": scheme.is_current,
                "source_run_id": source_run_id,
                "pulled_at": pulled_at,
            }
        )
    return rows
```

- [ ] **Step 4: Run parser tests**

Run:

```bash
uv run pytest tests/test_nace_categories.py::test_normalize_nace_code_preserves_sections_and_strips_numeric_punctuation tests/test_nace_categories.py::test_build_nace_rows_preserves_hierarchy_and_versions -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/nace/source.py tests/test_nace_categories.py
git commit -m "Add NACE parser"
```

## Task 5: Implement SPARQL CSV Fetch And dlt Source

**Files:**
- Modify: `src/dagster_v3/defs/nace/source.py`
- Test: `tests/test_nace_categories.py`

- [ ] **Step 1: Write failing dlt source test**

Append:

```python
import csv
from io import StringIO

from dagster_v3.defs.nace.source import NACE_CATEGORIES_DLT_TABLE, nace_categories_source


class FakeSparqlResponse:
    def __init__(self, text: str) -> None:
        self.text = text
        self.content = text.encode("utf-8")

    def raise_for_status(self) -> None:
        pass


class FakeSparqlSession:
    def __init__(self, csv_by_scheme_uri: dict[str, str]) -> None:
        self.csv_by_scheme_uri = csv_by_scheme_uri
        self.calls: list[tuple[str, dict[str, str], int]] = []

    def get(self, url: str, params: dict[str, str], timeout: int = 120) -> FakeSparqlResponse:
        self.calls.append((url, params, timeout))
        query = params["query"]
        for scheme_uri, body in self.csv_by_scheme_uri.items():
            if scheme_uri in query:
                return FakeSparqlResponse(body)
        raise AssertionError("query did not contain expected scheme URI")


def _nace_csv(rows: list[dict[str, str]]) -> str:
    buffer = StringIO()
    writer = csv.DictWriter(buffer, fieldnames=["concept", "notation", "label", "broader"])
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def test_nace_dlt_source_yields_both_versions() -> None:
    session = FakeSparqlSession(
        {
            "http://data.europa.eu/ux2/nace2/nace2": _nace_csv(
                [
                    {
                        "concept": "http://data.europa.eu/ux2/nace2/A",
                        "notation": "A",
                        "label": "A AGRICULTURE, FORESTRY AND FISHING",
                        "broader": "",
                    }
                ]
            ),
            "http://data.europa.eu/ux2/nace2.1/nace2.1": _nace_csv(
                [
                    {
                        "concept": "http://data.europa.eu/ux2/nace2.1/B",
                        "notation": "B",
                        "label": "B MINING AND QUARRYING",
                        "broader": "",
                    }
                ]
            ),
        }
    )

    source = nace_categories_source(session=session, source_run_id="run-1", pulled_at="2026-06-16T00:00:00.000Z")
    rows = list(source.resources[NACE_CATEGORIES_DLT_TABLE])

    assert [row["classification_version"] for row in rows] == ["NACE_REV_2", "NACE_REV_2_1"]
    assert [row["code"] for row in rows] == ["A", "B"]
    assert len(session.calls) == 2
    assert session.calls[0][1]["format"] == "text/csv"
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
uv run pytest tests/test_nace_categories.py::test_nace_dlt_source_yields_both_versions -v
```

Expected: FAIL with missing `nace_categories_source` or `NACE_CATEGORIES_DLT_TABLE`.

- [ ] **Step 3: Implement fetch and dlt source**

Append to `source.py`:

```python
import csv
import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from typing import Protocol

import dlt
import requests
from dlt.extract.resource import DltResource
from dlt.pipeline.pipeline import Pipeline

SPARQL_ENDPOINT = "https://publications.europa.eu/webapi/rdf/sparql"
NACE_CATEGORIES_DLT_TABLE = "nace_categories"
NACE_DLT_DATASET_NAME = "reference"

NACE_SCHEMES = (
    NaceScheme("NACE_REV_2", "http://data.europa.eu/ux2/nace2/nace2", "2008-01-01", "2024-12-31", 0),
    NaceScheme("NACE_REV_2_1", "http://data.europa.eu/ux2/nace2.1/nace2.1", "2025-01-01", None, 1),
)


class HttpSession(Protocol):
    def get(self, url: str, params: dict[str, str], timeout: int = 120) -> Any:
        ...


def nace_sparql_query(scheme_uri: str) -> str:
    return f"""
prefix skos: <http://www.w3.org/2004/02/skos/core#>
select ?concept ?notation ?label ?broader where {{
  ?concept skos:inScheme <{scheme_uri}> ;
           skos:notation ?notation ;
           skos:prefLabel ?label .
  filter(lang(?label) = "en")
  optional {{ ?concept skos:broader ?broader . }}
}}
order by ?notation
"""


def parse_sparql_csv(body: str) -> list[dict[str, str]]:
    return [dict(row) for row in csv.DictReader(StringIO(body))]


def fetch_nace_scheme_rows(
    *,
    scheme: NaceScheme,
    session: HttpSession | None = None,
    timeout_seconds: int = 120,
) -> tuple[list[dict[str, str]], str, str]:
    http_session = session or requests.Session()
    response = http_session.get(
        SPARQL_ENDPOINT,
        params={"query": nace_sparql_query(scheme.scheme_uri), "format": "text/csv"},
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    payload_hash = hashlib.sha256(response.content).hexdigest()
    return parse_sparql_csv(response.text), SPARQL_ENDPOINT, payload_hash


@dlt.source(name="nace")
def nace_categories_source(
    *,
    session: HttpSession | None = None,
    source_run_id: str = "",
    pulled_at: str | None = None,
) -> DltResource:
    return _nace_categories_resource(
        session=session,
        source_run_id=source_run_id,
        pulled_at=pulled_at or datetime.now(UTC).isoformat(timespec="milliseconds"),
    )


@dlt.resource(
    name=NACE_CATEGORIES_DLT_TABLE,
    write_disposition="append",
    primary_key=("classification_version", "normalized_code"),
)
def _nace_categories_resource(
    *,
    session: HttpSession | None,
    source_run_id: str,
    pulled_at: str,
) -> Iterator[dict[str, Any]]:
    for scheme in NACE_SCHEMES:
        source_rows, source_url, payload_hash = fetch_nace_scheme_rows(
            scheme=scheme,
            session=session,
        )
        yield from build_nace_rows(
            scheme=scheme,
            source_rows=source_rows,
            source_url=source_url,
            source_payload_hash=payload_hash,
            source_run_id=source_run_id,
            pulled_at=pulled_at,
        )


def nace_clickhouse_pipeline() -> Pipeline:
    return dlt.pipeline(
        pipeline_name="nace_categories",
        destination=dlt.destinations.clickhouse(),
        dataset_name=NACE_DLT_DATASET_NAME,
        dev_mode=False,
    )
```

- [ ] **Step 4: Run dlt source test**

Run:

```bash
uv run pytest tests/test_nace_categories.py::test_nace_dlt_source_yields_both_versions -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/nace/source.py tests/test_nace_categories.py
git commit -m "Add NACE dlt source"
```

## Task 6: Add Dagster Asset

**Files:**
- Create: `src/dagster_v3/defs/nace/assets.py`
- Test: `tests/test_nace_categories.py`

- [ ] **Step 1: Write failing asset graph test**

Append:

```python
import dagster as dg

from dagster_v3.definitions import defs as load_project_defs


def test_nace_categories_asset_is_registered_as_dlt_clickhouse_asset() -> None:
    repository = load_project_defs().get_repository_def()
    asset_keys = {key.path[-1] for key in repository.asset_graph.get_all_asset_keys()}
    resource_keys = repository.get_top_level_resources().keys()

    assert "nace_categories" in asset_keys
    assert "dlt" in resource_keys
    assert "clickhouse" in resource_keys
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
uv run pytest tests/test_nace_categories.py::test_nace_categories_asset_is_registered_as_dlt_clickhouse_asset -v
```

Expected: FAIL because the NACE asset and/or `clickhouse` resource is not registered.

- [ ] **Step 3: Implement asset**

Create `src/dagster_v3/defs/nace/assets.py`:

```python
from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import dagster as dg
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData

from dagster_v3.defs.nace.resources import ClickHouseResource, prepare_nace_categories_table
from dagster_v3.defs.nace.source import (
    NACE_CATEGORIES_DLT_TABLE,
    nace_categories_source,
    nace_clickhouse_pipeline,
)
from dagster_v3.defs.nace import tables


class NaceDltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data: DltResourceTranslatorData) -> dg.AssetSpec:
        spec = super().get_asset_spec(data)
        if data.resource.name != NACE_CATEGORIES_DLT_TABLE:
            return spec
        return spec.replace_attributes(
            key="nace_categories",
            deps=[],
            group_name="nace",
            description="Official NACE Rev. 2 and Rev. 2.1 category reference table loaded to ClickHouse.",
            kinds={"python", "dlt", "clickhouse", "reference"},
        )


@dlt_assets(
    dlt_source=nace_categories_source(),
    dlt_pipeline=nace_clickhouse_pipeline(),
    name="nace_categories",
    dagster_dlt_translator=NaceDltTranslator(),
)
def nace_categories_asset(
    context: dg.AssetExecutionContext,
    dlt: DagsterDltResource,
    clickhouse: ClickHouseResource,
) -> Iterator[Any]:
    prepare_nace_categories_table(clickhouse)
    yield from dlt.run(
        context=context,
        dlt_source=nace_categories_source(source_run_id=context.run_id),
        dlt_pipeline=nace_clickhouse_pipeline(),
    )


defs = dg.Definitions(
    assets=[nace_categories_asset],
    resources={
        "clickhouse": ClickHouseResource(
            host=dg.EnvVar("CLICKHOUSE_HOST"),
            port=dg.EnvVar("CLICKHOUSE_PORT"),
            username=dg.EnvVar("CLICKHOUSE_USER"),
            password=dg.EnvVar("CLICKHOUSE_PASSWORD"),
            database=dg.EnvVar("CLICKHOUSE_DATABASE"),
            secure=dg.EnvVar("CLICKHOUSE_SECURE"),
        ),
    },
)
```

- [ ] **Step 4: Run asset graph test**

Run:

```bash
uv run pytest tests/test_nace_categories.py::test_nace_categories_asset_is_registered_as_dlt_clickhouse_asset -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/nace/assets.py tests/test_nace_categories.py
git commit -m "Add NACE Dagster asset"
```

## Task 7: Add Metadata And Row Count Expectations

**Files:**
- Modify: `src/dagster_v3/defs/nace/source.py`
- Modify: `src/dagster_v3/defs/nace/assets.py`
- Test: `tests/test_nace_categories.py`

- [ ] **Step 1: Write parser metadata test**

Append:

```python
from collections import Counter


def test_nace_rows_support_materialization_metadata_counts() -> None:
    rows = [
        {"classification_version": "NACE_REV_2", "level": "section"},
        {"classification_version": "NACE_REV_2", "level": "division"},
        {"classification_version": "NACE_REV_2_1", "level": "section"},
    ]

    by_version = Counter(row["classification_version"] for row in rows)
    by_level = Counter(row["level"] for row in rows)

    assert dict(by_version) == {"NACE_REV_2": 2, "NACE_REV_2_1": 1}
    assert dict(by_level) == {"section": 2, "division": 1}
```

- [ ] **Step 2: Run focused test**

Run:

```bash
uv run pytest tests/test_nace_categories.py::test_nace_rows_support_materialization_metadata_counts -v
```

Expected: PASS. This is a low-risk test anchoring the metadata shape before adding richer metadata in the asset.

- [ ] **Step 3: Add asset logging**

In `nace_categories_asset`, add logs before schema setup and dlt run:

```python
context.log.info("Preparing ClickHouse table reference.nace_categories")
prepare_nace_categories_table(clickhouse)
context.log.info("Loading NACE reference categories into ClickHouse with dlt")
```

Do not manually query ClickHouse row counts in this task; dlt load info is enough for first implementation. Add row-count queries only after the first successful materialization if users need richer metadata.

- [ ] **Step 4: Run NACE tests**

Run:

```bash
uv run pytest tests/test_nace_categories.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/nace/assets.py tests/test_nace_categories.py
git commit -m "Add NACE asset logging"
```

## Task 8: Validate Definitions

**Files:**
- No code changes unless validation finds an issue.

- [ ] **Step 1: Run full tests**

Run:

```bash
uv run pytest -v
```

Expected: all tests pass.

- [ ] **Step 2: Run Dagster definitions check**

Run:

```bash
uv run dg check defs
```

Expected: definitions load successfully.

- [ ] **Step 3: Inspect definitions list**

Run:

```bash
uv run dg list defs --json
```

Expected: output includes asset key `nace_categories` with group `nace`.

- [ ] **Step 4: Commit validation fixes if any**

If validation required code changes:

```bash
git add src/dagster_v3/defs/nace tests/test_nace_categories.py pyproject.toml uv.lock
git commit -m "Validate NACE Dagster definitions"
```

If no changes were required, do not create an empty commit.

## Task 9: Manual Materialization Check

**Files:**
- No code changes expected.

- [ ] **Step 1: Confirm required environment variables**

The run requires:

```bash
CLICKHOUSE_HOST
CLICKHOUSE_PORT
CLICKHOUSE_USER
CLICKHOUSE_PASSWORD
CLICKHOUSE_DATABASE
CLICKHOUSE_SECURE
```

- [ ] **Step 2: Materialize the asset**

Run:

```bash
uv run dg launch --assets nace_categories
```

Expected: run succeeds after creating `reference.nace_categories` and loading both NACE versions.

- [ ] **Step 3: Verify ClickHouse data**

Use the available ClickHouse client or Python client to run:

```sql
SELECT classification_version, count()
FROM reference.nace_categories
GROUP BY classification_version
ORDER BY classification_version;
```

Expected approximate counts from the official endpoint:

- `NACE_REV_2`: around `996`
- `NACE_REV_2_1`: around `1047`

- [ ] **Step 4: Verify hierarchy levels**

Run:

```sql
SELECT classification_version, level, count()
FROM reference.nace_categories
GROUP BY classification_version, level
ORDER BY classification_version, level;
```

Expected: both versions include `section`, `division`, `group`, and `class`.

## Self-Review Notes

- Spec coverage: source, direct ClickHouse load, table DDL, versioned schema, observability, tests, and no DuckDB staging are covered.
- Placeholder scan: no placeholder implementation steps remain.
- Type consistency: `ClickHouseResource`, `prepare_nace_categories_table`, `NaceScheme`, `nace_categories_source`, `nace_clickhouse_pipeline`, and table constants are named consistently across tasks.
