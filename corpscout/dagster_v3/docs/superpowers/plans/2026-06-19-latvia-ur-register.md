# Latvia UR Register Spine — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Load the Latvia UR Register of Enterprises (`register.csv`, ~485k entities, CC0) into a local DuckDB file via a dlt streaming-download asset, normalized to a stable company-spine schema keyed on `regcode`.

**Architecture:** Mirror the existing `finland_ytj` module exactly — a `@dlt.resource` streams the bulk CSV download (progress logging, temp-file staging), yields normalized flat row dicts, and `@dlt_assets` loads them to `data/latvia_ur.duckdb` (`latvia_ur.entities`) with `write_disposition="replace"`, `primary_key="regcode"`, behind a single-writer concurrency pool. No ClickHouse and no `definitions.py` in this module (assets are auto-discovered via `dg.load_from_defs_folder`, like `finland_ytj`); ClickHouse export arrives later with the `latvia_resolved` layer.

**Tech Stack:** Python 3, Dagster 1.13.9, dagster-dlt, dlt (duckdb destination), duckdb, Python stdlib `csv`. Run all commands with `GOWORK` irrelevant (Python); use `uv run` for `dg`/`pytest`. Source dossier: `companycollect/companies/analysis/latvia/`. Analysis: `docs/superpowers/analysis/2026-06-19-latvia-ur-sources.md`.

**Reference module (read these to match style):**
- `src/dagster_v3/defs/finland_ytj/assets.py` (dlt source + pipeline + `pipelines_dir`, temp-file download, empty-download guard)
- `src/dagster_v3/defs/norway_brreg/resources.py` (`@dlt.source`/`@dlt.resource`, streaming `_download_bytes`, row-mapper helpers)
- `src/dagster_v3/defs/norway_brreg/tables.py` (`*_COLUMNS` dlt schema dict + `copy_dlt_columns`)
- `src/dagster_v3/defs/norway_brreg/assets.py` (`@dlt_assets`, `DagsterDltTranslator`, `pool=`, `_duckdb_table_count`)

**Live-verified facts (2026-06-19):**
- `register.csv` is `;`-delimited UTF-8. Header (exact order):
  `regcode;sepa;name;name_before_quotes;name_in_quotes;name_after_quotes;without_quotes;regtype;regtype_text;type;type_text;registered;terminated;closed;address;index;addressid;region;city;atvk;reregistration_term`
- Quoting is real (RFC-style): e.g. `"IK ""KRASTNIEKI A I"""` and addresses containing `;` inside quotes → must use a CSV parser, never `split(';')` (Risk R4).
- Download URL: `https://data.gov.lv/dati/dataset/4de9697f-850b-45ec-8bba-61fa09ce932f/resource/25e80bf3-f107-4ab4-89ef-251b5b9374e9/download/register.csv`
- Status derivation: registered/active unless `terminated` is set or `closed` is flagged (Risk: keep both `terminated_date` and `closed_flag`).
- `vat_id = "LV" + regcode` (not present in the CSV).

---

### Task 1: Module skeleton + column schema (`tables.py`)

**Files:**
- Create: `src/dagster_v3/defs/latvia_ur/__init__.py`
- Create: `src/dagster_v3/defs/latvia_ur/tables.py`
- Test: `tests/test_latvia_ur_tables.py`

- [ ] **Step 1: Create the package marker**

`src/dagster_v3/defs/latvia_ur/__init__.py`:

```python
"""Latvia UR (Uzņēmumu reģistrs) Register of Enterprises company data assets."""
```

- [ ] **Step 2: Write the failing test for the column schema**

`tests/test_latvia_ur_tables.py`:

```python
from dagster_v3.defs.latvia_ur import tables


def test_entities_columns_has_regcode_primary_key():
    columns = tables.LATVIA_UR_ENTITIES_COLUMNS
    assert columns["regcode"] == {"data_type": "text", "nullable": False}


def test_entities_columns_cover_expected_fields():
    expected = {
        "country_iso2",
        "source_slug",
        "source_run_id",
        "source_line_number",
        "source_record_id",
        "source_payload_hash",
        "regcode",
        "vat_id",
        "sepa",
        "legal_name",
        "name_in_quotes",
        "legal_form_code",
        "legal_form_text",
        "legal_form_description_en",
        "regtype_code",
        "regtype_text",
        "registered_date",
        "terminated_date",
        "closed_flag",
        "status",
        "is_active",
        "address",
        "postal_code",
        "address_id",
        "region_code",
        "city_code",
        "atvk_code",
        "reregistration_term",
        "source_url",
        "raw_entity",
    }
    assert set(tables.LATVIA_UR_ENTITIES_COLUMNS) == expected


def test_copy_dlt_columns_is_a_deep_copy():
    copied = tables.copy_dlt_columns(tables.LATVIA_UR_ENTITIES_COLUMNS)
    copied["regcode"]["nullable"] = True
    assert tables.LATVIA_UR_ENTITIES_COLUMNS["regcode"]["nullable"] is False
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `uv run pytest tests/test_latvia_ur_tables.py -v`
Expected: FAIL with `ModuleNotFoundError: dagster_v3.defs.latvia_ur.tables` (or `AttributeError`).

- [ ] **Step 4: Implement `tables.py`**

`src/dagster_v3/defs/latvia_ur/tables.py`:

```python
from typing import Any

DLT_DATASET_NAME = "latvia_ur"
ENTITIES_TABLE = "entities"

LATVIA_UR_ENTITIES_COLUMNS: dict[str, dict[str, Any]] = {
    "country_iso2": {"data_type": "text"},
    "source_slug": {"data_type": "text"},
    "source_run_id": {"data_type": "text"},
    "source_line_number": {"data_type": "bigint"},
    "source_record_id": {"data_type": "text"},
    "source_payload_hash": {"data_type": "text"},
    "regcode": {"data_type": "text", "nullable": False},
    "vat_id": {"data_type": "text"},
    "sepa": {"data_type": "text"},
    "legal_name": {"data_type": "text"},
    "name_in_quotes": {"data_type": "text"},
    "legal_form_code": {"data_type": "text"},
    "legal_form_text": {"data_type": "text"},
    "legal_form_description_en": {"data_type": "text"},
    "regtype_code": {"data_type": "text"},
    "regtype_text": {"data_type": "text"},
    "registered_date": {"data_type": "text"},
    "terminated_date": {"data_type": "text"},
    "closed_flag": {"data_type": "text"},
    "status": {"data_type": "text"},
    "is_active": {"data_type": "bool"},
    "address": {"data_type": "text"},
    "postal_code": {"data_type": "text"},
    "address_id": {"data_type": "text"},
    "region_code": {"data_type": "text"},
    "city_code": {"data_type": "text"},
    "atvk_code": {"data_type": "text"},
    "reregistration_term": {"data_type": "text"},
    "source_url": {"data_type": "text"},
    "raw_entity": {"data_type": "text"},
}


def copy_dlt_columns(columns: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {name: dict(spec) for name, spec in columns.items()}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `uv run pytest tests/test_latvia_ur_tables.py -v`
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
git add src/dagster_v3/defs/latvia_ur/__init__.py src/dagster_v3/defs/latvia_ur/tables.py tests/test_latvia_ur_tables.py
git commit -m "feat(latvia_ur): add register entity column schema"
```

---

### Task 2: CSV normalization + dlt source (`resources.py`)

**Files:**
- Create: `src/dagster_v3/defs/latvia_ur/resources.py`
- Test: `tests/test_latvia_ur_resources.py`

This task contains all parsing logic and is the heart of the module. It uses Python's `csv` module (Risk R4) and a temp-file download (Risk R1/large-file safety, mirrors `finland_ytj`). It raises on an empty parse to protect the `replace` disposition (Risk R3).

- [ ] **Step 1: Write the failing tests**

`tests/test_latvia_ur_resources.py`:

```python
import json

from dagster_v3.defs.latvia_ur import resources

SAMPLE_CSV = (
    "regcode;sepa;name;name_before_quotes;name_in_quotes;name_after_quotes;"
    "without_quotes;regtype;regtype_text;type;type_text;registered;terminated;"
    "closed;address;index;addressid;region;city;atvk;reregistration_term\n"
    # active SIA, address contains a quoted semicolon and doubled quotes
    "40103550818;LV95ZZZ40103550818;\"SIA \"\"Psihologs; Tava kabata\"\"\";SIA;"
    "Psihologs; Tava kabata;\"\";0;K;Komercreģistrs;SIA;"
    "Sabiedrība ar ierobežotu atbildību;2012-05-31;;;\"Valmiera, \"\"Centrs\"\"\";"
    "4201;103045133;100015821;0;0885162;\n"
    # terminated + closed IK
    "41202013815;LV53ZZZ41202013815;\"IK \"\"KRASTNIEKI A I\"\"\";IK;KRASTNIEKI A I;"
    "\"\";0;K;Komercreģistrs;IK;Individuālais komersants;1998-02-26;2014-04-10;L;"
    "\"Dundagas nov., Kolka\";3275;103045134;100015821;0;0885162;\n"
)


def _rows(csv_text: str) -> list[dict]:
    return list(
        resources.iter_latvia_ur_entity_rows_from_text(csv_text, run_id="run-1")
    )


def test_parses_quoted_semicolons_and_doubled_quotes():
    rows = _rows(SAMPLE_CSV)
    assert len(rows) == 2
    first = rows[0]
    assert first["regcode"] == "40103550818"
    assert first["legal_name"] == 'SIA "Psihologs; Tava kabata"'
    assert first["name_in_quotes"] == "Psihologs; Tava kabata"
    assert first["address"] == 'Valmiera, "Centrs"'


def test_active_entity_status_and_vat():
    first = _rows(SAMPLE_CSV)[0]
    assert first["status"] == "active"
    assert first["is_active"] is True
    assert first["terminated_date"] == ""
    assert first["closed_flag"] == ""
    assert first["vat_id"] == "LV40103550818"
    assert first["legal_form_code"] == "SIA"
    assert first["legal_form_description_en"] == "Private limited company"
    assert first["postal_code"] == "4201"
    assert first["atvk_code"] == "0885162"
    assert first["country_iso2"] == "LV"
    assert first["source_slug"] == "latvia_ur_register"
    assert first["source_run_id"] == "run-1"
    assert first["source_record_id"] == "40103550818"
    assert first["source_line_number"] == 1


def test_terminated_entity_status():
    second = _rows(SAMPLE_CSV)[1]
    assert second["regcode"] == "41202013815"
    assert second["status"] == "terminated"
    assert second["is_active"] is False
    assert second["terminated_date"] == "2014-04-10"
    assert second["closed_flag"] == "L"
    assert second["legal_form_code"] == "IK"
    assert second["legal_form_description_en"] == "Individual merchant (sole trader)"


def test_closed_without_termination_is_inactive():
    csv_text = (
        "regcode;sepa;name;name_before_quotes;name_in_quotes;name_after_quotes;"
        "without_quotes;regtype;regtype_text;type;type_text;registered;terminated;"
        "closed;address;index;addressid;region;city;atvk;reregistration_term\n"
        "40000000001;;ZS Test;ZS;Test;;0;K;Komercreģistrs;ZS;Zemnieka saimniecība;"
        "2000-01-01;;L;Some address;1001;1;1;1;0010000;\n"
    )
    row = _rows(csv_text)[0]
    assert row["status"] == "closed"
    assert row["is_active"] is False


def test_raw_entity_is_json_of_source_columns():
    first = _rows(SAMPLE_CSV)[0]
    raw = json.loads(first["raw_entity"])
    assert raw["regcode"] == "40103550818"
    assert raw["type"] == "SIA"


def test_blank_regcode_rows_are_skipped():
    csv_text = (
        "regcode;sepa;name;name_before_quotes;name_in_quotes;name_after_quotes;"
        "without_quotes;regtype;regtype_text;type;type_text;registered;terminated;"
        "closed;address;index;addressid;region;city;atvk;reregistration_term\n"
        ";;No regcode;;;;0;K;K;SIA;SIA;2000-01-01;;;addr;1001;1;1;1;0010000;\n"
        "40000000002;;Good;;Good;;0;K;K;SIA;SIA;2000-01-01;;;addr;1001;1;1;1;0010000;\n"
    )
    rows = _rows(csv_text)
    assert [r["regcode"] for r in rows] == ["40000000002"]


def test_empty_csv_yields_no_rows():
    header_only = (
        "regcode;sepa;name;name_before_quotes;name_in_quotes;name_after_quotes;"
        "without_quotes;regtype;regtype_text;type;type_text;registered;terminated;"
        "closed;address;index;addressid;region;city;atvk;reregistration_term\n"
    )
    assert _rows(header_only) == []
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_latvia_ur_resources.py -v`
Expected: FAIL with `AttributeError`/`ModuleNotFoundError` (functions not yet defined).

- [ ] **Step 3: Implement `resources.py`**

`src/dagster_v3/defs/latvia_ur/resources.py`:

```python
from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import tempfile
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any, Protocol

import dlt
import requests
from dlt.extract.resource import DltResource

from dagster_v3.defs.latvia_ur import tables

COUNTRY = "LV"
SOURCE_SLUG = "latvia_ur_register"
DLT_DATASET_NAME = tables.DLT_DATASET_NAME
ENTITIES_TABLE = tables.ENTITIES_TABLE
REGISTER_DOWNLOAD_URL = (
    "https://data.gov.lv/dati/dataset/4de9697f-850b-45ec-8bba-61fa09ce932f/"
    "resource/25e80bf3-f107-4ab4-89ef-251b5b9374e9/download/register.csv"
)
DEFAULT_TIMEOUT_SECONDS = 300
DEFAULT_USER_AGENT = "corpscout-dagster-v3-dev/0.1"
DOWNLOAD_CHUNK_BYTES = 1024 * 1024
PROGRESS_LOG_EVERY_ROWS = 50000
CSV_DELIMITER = ";"
LOGGER = logging.getLogger(__name__)

# Common Latvian legal forms (code -> English description). Unknown -> "".
LATVIA_LEGAL_FORM_DESCRIPTION_EN_BY_CODE = {
    "SIA": "Private limited company",
    "AS": "Public limited company",
    "IK": "Individual merchant (sole trader)",
    "IU": "Individual undertaking",
    "ZS": "Farm holding",
    "ZemnSaimn": "Farm holding",
    "KS": "Limited partnership",
    "PS": "General partnership",
    "BO": "Association or foundation",
    "NO": "Association or foundation",
    "VAS": "State joint-stock company",
    "PAS": "Municipal joint-stock company",
}


class HttpSession(Protocol):
    def get(self, url: str, *, timeout: int, stream: bool = False) -> Any: ...


@dlt.source(name="latvia_ur_register")
def latvia_ur_source(
    *,
    download_url: str = REGISTER_DOWNLOAD_URL,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    user_agent: str = DEFAULT_USER_AGENT,
    run_id: str = "",
    session: HttpSession | None = None,
) -> DltResource:
    return _entities_resource(
        download_url=download_url,
        timeout_seconds=timeout_seconds,
        user_agent=user_agent,
        run_id=run_id,
        session=session,
    )


@dlt.resource(
    name=ENTITIES_TABLE,
    write_disposition="replace",
    primary_key="regcode",
    columns=tables.copy_dlt_columns(tables.LATVIA_UR_ENTITIES_COLUMNS),
)
def _entities_resource(
    *,
    download_url: str,
    timeout_seconds: int,
    user_agent: str,
    run_id: str,
    session: HttpSession | None,
) -> Iterator[dict[str, Any]]:
    with tempfile.TemporaryDirectory(prefix="latvia_ur_") as tmpdir:
        csv_path = Path(tmpdir) / "register.csv"
        _download_to_path(
            url=download_url,
            dest=csv_path,
            timeout_seconds=timeout_seconds,
            user_agent=user_agent,
            session=session,
        )
        seen = False
        with csv_path.open("r", encoding="utf-8", newline="") as handle:
            for row in iter_latvia_ur_entity_rows(handle, run_id=run_id):
                seen = True
                yield row
        if not seen:
            raise ValueError(
                "Latvia UR register.csv produced no entity rows; refusing to replace the table"
            )


def iter_latvia_ur_entity_rows_from_text(
    csv_text: str,
    *,
    run_id: str = "",
) -> Iterator[dict[str, Any]]:
    yield from iter_latvia_ur_entity_rows(io.StringIO(csv_text), run_id=run_id)


def iter_latvia_ur_entity_rows(
    handle: Any,
    *,
    run_id: str = "",
    log: Callable[..., None] | None = None,
) -> Iterator[dict[str, Any]]:
    progress_log = log or LOGGER.info
    reader = csv.DictReader(handle, delimiter=CSV_DELIMITER)
    emitted = 0
    for source_row in reader:
        regcode = _clean(source_row.get("regcode"))
        if regcode == "":
            continue
        emitted += 1
        if PROGRESS_LOG_EVERY_ROWS > 0 and emitted % PROGRESS_LOG_EVERY_ROWS == 0:
            progress_log("Processed Latvia UR register rows: rows=%s", emitted)
        yield _entity_row(source_row, line_number=emitted, run_id=run_id)


def _entity_row(
    source_row: dict[str, Any],
    *,
    line_number: int,
    run_id: str,
) -> dict[str, Any]:
    regcode = _clean(source_row.get("regcode"))
    terminated = _clean(source_row.get("terminated"))
    closed = _clean(source_row.get("closed"))
    legal_form_code = _clean(source_row.get("type"))
    status = _status(terminated=terminated, closed=closed)
    return {
        "country_iso2": COUNTRY,
        "source_slug": SOURCE_SLUG,
        "source_run_id": run_id,
        "source_line_number": line_number,
        "source_record_id": regcode,
        "source_payload_hash": _payload_hash(source_row),
        "regcode": regcode,
        "vat_id": f"LV{regcode}" if regcode else "",
        "sepa": _clean(source_row.get("sepa")),
        "legal_name": _clean(source_row.get("name")),
        "name_in_quotes": _clean(source_row.get("name_in_quotes")),
        "legal_form_code": legal_form_code,
        "legal_form_text": _clean(source_row.get("type_text")),
        "legal_form_description_en": LATVIA_LEGAL_FORM_DESCRIPTION_EN_BY_CODE.get(
            legal_form_code, ""
        ),
        "regtype_code": _clean(source_row.get("regtype")),
        "regtype_text": _clean(source_row.get("regtype_text")),
        "registered_date": _clean(source_row.get("registered")),
        "terminated_date": terminated,
        "closed_flag": closed,
        "status": status,
        "is_active": status == "active",
        "address": _clean(source_row.get("address")),
        "postal_code": _clean(source_row.get("index")),
        "address_id": _clean(source_row.get("addressid")),
        "region_code": _clean(source_row.get("region")),
        "city_code": _clean(source_row.get("city")),
        "atvk_code": _clean(source_row.get("atvk")),
        "reregistration_term": _clean(source_row.get("reregistration_term")),
        "source_url": REGISTER_DOWNLOAD_URL,
        "raw_entity": _json_dumps(source_row),
    }


def _status(*, terminated: str, closed: str) -> str:
    if terminated:
        return "terminated"
    if closed:
        return "closed"
    return "active"


def _download_to_path(
    *,
    url: str,
    dest: Path,
    timeout_seconds: int,
    user_agent: str,
    session: HttpSession | None,
    log: Callable[..., None] | None = None,
) -> None:
    http_session = session or requests.Session()
    response = http_session.get(url, timeout=timeout_seconds, stream=True)
    response.raise_for_status()
    iter_content = getattr(response, "iter_content", None)
    with dest.open("wb") as out:
        if callable(iter_content):
            for chunk in iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
                if chunk:
                    out.write(chunk)
        else:
            out.write(response.content)


def _payload_hash(source_row: dict[str, Any]) -> str:
    body = _json_dumps(source_row, sort_keys=True)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _json_dumps(payload: dict[str, Any], *, sort_keys: bool = False) -> str:
    return json.dumps(
        {key: _clean(value) for key, value in payload.items()},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=sort_keys,
    )


def _clean(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()
```

Notes for the implementer:
- `_clean` strips whitespace; `csv.DictReader` already removes the surrounding quotes and un-doubles `""`.
- The `user_agent` is accepted for parity with sibling modules even though `requests.Session().headers` is not mutated here; keep it (a later task may attach it). Do **not** add `headers=` to the `.get()` call — the `HttpSession` protocol and the test stub (Task 3) do not pass headers.
- `is_active` must be a real `bool` (the tests assert `is True` / `is False`).

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_latvia_ur_resources.py -v`
Expected: PASS (7 tests).

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/latvia_ur/resources.py tests/test_latvia_ur_resources.py
git commit -m "feat(latvia_ur): parse and normalize register.csv into entity rows"
```

---

### Task 3: dlt asset + DuckDB load (`assets.py`)

**Files:**
- Create: `src/dagster_v3/defs/latvia_ur/assets.py`
- Test: `tests/test_latvia_ur_assets.py`

- [ ] **Step 1: Write the failing test (loads a stubbed CSV into a temp DuckDB)**

`tests/test_latvia_ur_assets.py`:

```python
from pathlib import Path

import duckdb

from dagster_v3.defs.latvia_ur import assets, resources

SAMPLE_CSV = (
    "regcode;sepa;name;name_before_quotes;name_in_quotes;name_after_quotes;"
    "without_quotes;regtype;regtype_text;type;type_text;registered;terminated;"
    "closed;address;index;addressid;region;city;atvk;reregistration_term\n"
    "40103550818;LV95ZZZ40103550818;\"SIA \"\"Psihologs\"\"\";SIA;Psihologs;\"\";0;"
    "K;Komercreģistrs;SIA;Sabiedrība ar ierobežotu atbildību;2012-05-31;;;"
    "\"Valmiera\";4201;103045133;100015821;0;0885162;\n"
    "41202013815;LV53ZZZ41202013815;\"IK \"\"KRASTNIEKI\"\"\";IK;KRASTNIEKI;\"\";0;"
    "K;Komercreģistrs;IK;Individuālais komersants;1998-02-26;2014-04-10;L;"
    "\"Dundaga\";3275;103045134;100015821;0;0885162;\n"
)


class _FakeResponse:
    def __init__(self, body: bytes) -> None:
        self._body = body
        self.content = body

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int = 0):
        yield self._body


class _FakeSession:
    def __init__(self, body: bytes) -> None:
        self._body = body

    def get(self, url: str, *, timeout: int, stream: bool = False) -> _FakeResponse:
        return _FakeResponse(self._body)


def test_pipeline_loads_register_rows_into_duckdb(tmp_path: Path):
    db_path = tmp_path / "latvia_ur.duckdb"
    pipelines_dir = tmp_path / "dlt"

    load_info = assets.run_latvia_ur_dlt_pipeline(
        database_path=db_path,
        run_id="run-1",
        session=_FakeSession(SAMPLE_CSV.encode("utf-8")),
        pipelines_dir=pipelines_dir,
    )
    assert load_info is not None

    qualified = f"{resources.DLT_DATASET_NAME}.{resources.ENTITIES_TABLE}"
    with duckdb.connect(str(db_path), read_only=True) as conn:
        count = conn.execute(f"select count(*) from {qualified}").fetchone()[0]
        active = conn.execute(
            f"select count(*) from {qualified} where is_active"
        ).fetchone()[0]
        sample = conn.execute(
            f"select vat_id, status, legal_form_description_en from {qualified} "
            "where regcode = '40103550818'"
        ).fetchone()
    assert count == 2
    assert active == 1
    assert sample == ("LV40103550818", "active", "Private limited company")


def test_duckdb_table_count_helper(tmp_path: Path):
    db_path = tmp_path / "latvia_ur.duckdb"
    assets.run_latvia_ur_dlt_pipeline(
        database_path=db_path,
        run_id="run-1",
        session=_FakeSession(SAMPLE_CSV.encode("utf-8")),
        pipelines_dir=tmp_path / "dlt",
    )
    qualified = f"{resources.DLT_DATASET_NAME}.{resources.ENTITIES_TABLE}"
    assert assets._duckdb_table_count(database_path=db_path, table_name=qualified) == 2
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_latvia_ur_assets.py -v`
Expected: FAIL (`AttributeError: run_latvia_ur_dlt_pipeline`).

- [ ] **Step 3: Implement `assets.py`**

`src/dagster_v3/defs/latvia_ur/assets.py`:

```python
from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import dagster as dg
import dlt
import duckdb
from dagster import AssetExecutionContext
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData

from dagster_v3.defs.latvia_ur import resources, tables

GROUP_NAME = "latvia_ur"
LATVIA_UR_DUCKDB_POOL = "latvia_ur_duckdb"
LATVIA_UR_DUCKDB_PATH = Path("data/latvia_ur.duckdb")
DLT_DATASET_NAME = tables.DLT_DATASET_NAME
ENTITIES_TABLE = tables.ENTITIES_TABLE
ENTITIES_ASSET_KEY = "latvia_ur_entities_duckdb"


class LatviaUrDltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data: DltResourceTranslatorData) -> dg.AssetSpec:
        spec = super().get_asset_spec(data)
        if data.resource.name != ENTITIES_TABLE:
            return spec
        return spec.replace_attributes(
            key=dg.AssetKey(ENTITIES_ASSET_KEY),
            deps=[],
            group_name=GROUP_NAME,
            description="Latvia UR register entity bulk data loaded to local DuckDB with dlt.",
            kinds={"python", "dlt", "duckdb"},
        )


def latvia_ur_pipeline(
    database_path: str | Path,
    *,
    pipelines_dir: str | Path | None = None,
) -> Any:
    return dlt.pipeline(
        pipeline_name="latvia_ur_register",
        destination=dlt.destinations.duckdb(str(database_path)),
        dataset_name=DLT_DATASET_NAME,
        dev_mode=False,
        pipelines_dir=str(pipelines_dir) if pipelines_dir is not None else None,
    )


def run_latvia_ur_dlt_pipeline(
    *,
    database_path: str | Path,
    run_id: str,
    session: resources.HttpSession | None = None,
    pipelines_dir: str | Path | None = None,
) -> Any:
    Path(database_path).parent.mkdir(parents=True, exist_ok=True)
    return latvia_ur_pipeline(database_path, pipelines_dir=pipelines_dir).run(
        resources.latvia_ur_source(run_id=run_id, session=session)
    )


@dlt_assets(
    dlt_source=resources.latvia_ur_source(),
    dlt_pipeline=latvia_ur_pipeline(LATVIA_UR_DUCKDB_PATH),
    name=ENTITIES_ASSET_KEY,
    dagster_dlt_translator=LatviaUrDltTranslator(),
    pool=LATVIA_UR_DUCKDB_POOL,
)
def latvia_ur_entities_duckdb_asset(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
) -> Iterator[Any]:
    """Load Latvia UR register entity bulk data to local DuckDB with dlt."""
    LATVIA_UR_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    context.log.info(
        "Starting Latvia UR register dlt load: source_url=%s, duckdb_path=%s, "
        "dataset=%s, table=%s",
        resources.REGISTER_DOWNLOAD_URL,
        LATVIA_UR_DUCKDB_PATH,
        DLT_DATASET_NAME,
        ENTITIES_TABLE,
    )
    yield from dlt.run(context=context)
    row_count = _duckdb_table_count(
        database_path=LATVIA_UR_DUCKDB_PATH,
        table_name=f"{DLT_DATASET_NAME}.{ENTITIES_TABLE}",
    )
    context.log.info(
        "Completed Latvia UR register dlt load: duckdb_path=%s, dataset=%s, table=%s, rows=%s",
        LATVIA_UR_DUCKDB_PATH,
        DLT_DATASET_NAME,
        ENTITIES_TABLE,
        row_count,
    )


def _duckdb_table_count(*, database_path: str | Path, table_name: str) -> int:
    with duckdb.connect(str(database_path), read_only=True) as connection:
        value = connection.execute(f"select count(*) from {table_name}").fetchone()[0]
    return int(value)
```

Notes for the implementer:
- If `dlt.pipeline(..., pipelines_dir=None)` rejects `None`, branch instead: build kwargs and only include `pipelines_dir` when not `None`. Verify against the installed dlt version before assuming.
- The `@dlt_assets` decorator instantiates `latvia_ur_pipeline(LATVIA_UR_DUCKDB_PATH)` at import time with the default (global) `pipelines_dir`; this matches `norway_brreg`. The per-checkout `pipelines_dir` override (Risk R8) is exercised through `run_latvia_ur_dlt_pipeline` and is available for tests/CLI; do not change the decorator form.

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest tests/test_latvia_ur_assets.py -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/latvia_ur/assets.py tests/test_latvia_ur_assets.py
git commit -m "feat(latvia_ur): load register entities to DuckDB via dlt asset"
```

---

### Task 4: Definitions discovery + concurrency pool + full verification

**Files:**
- No new source files (assets auto-discovered, like `finland_ytj`).

- [ ] **Step 1: Confirm the asset is discovered exactly once**

Run: `uv run dg list defs --json` and confirm `latvia_ur_entities_duckdb` appears **exactly once** (grep the output).
Run: `uv run dg check defs`
Expected: `dg check` passes; the asset is listed once under group `latvia_ur`.

If the asset is missing: ensure `tests`/imports don't shadow it and that `src/dagster_v3/defs/latvia_ur/assets.py` is importable.
If it appears **twice** (auto-discovery + an accidental `definitions.py`): delete the `definitions.py` — this module relies on auto-discovery (Architecture section). This is the one structural risk to verify.

- [ ] **Step 2: Register the single-writer concurrency pool (Risk R2)**

The DuckDB file is single-writer. Set the pool limit on the Dagster instance (one-time, like `finland_ytj_duckdb` / `norway_brreg_duckdb`):

Run: `uv run dagster instance concurrency set latvia_ur_duckdb 1`
Expected: confirms the limit is set to 1.

(Document this in the module: the `pool="latvia_ur_duckdb"` on the asset enforces it at runtime once the instance limit exists.)

- [ ] **Step 3: Run the whole module test suite + a project-wide check**

Run: `uv run pytest tests/test_latvia_ur_tables.py tests/test_latvia_ur_resources.py tests/test_latvia_ur_assets.py -v`
Expected: all green (12 tests).

Run: `uv run dg check defs`
Expected: PASS.

- [ ] **Step 4: (Optional, env-gated) live smoke**

Only if explicitly running a live check: materialize the asset and confirm ~485k rows.

Run: `uv run dg launch --assets latvia_ur_entities_duckdb`
Expected: completes; `data/latvia_ur.duckdb` has `latvia_ur.entities` with ~485,000 rows. Do **not** run this in CI / unattended; it downloads the full register.

- [ ] **Step 5: Commit any verification doc updates**

```bash
git add -A
git commit -m "chore(latvia_ur): verify discovery, set duckdb concurrency pool" --allow-empty
```

---

## Self-Review

- **Spec coverage:** register spine load (Task 2/3) ✓; CSV quoting R4 (Task 2 tests) ✓; empty-download guard R3 (`_entities_resource` raises; covered indirectly) ✓; single-writer pool R2 (Task 3 `pool=` + Task 4 instance limit) ✓; `pipelines_dir` R8 (`run_latvia_ur_dlt_pipeline`) ✓; status/vat derivation ✓. Financials (Module 2), resolved/ClickHouse (Module 3), and PII (deferred) are intentionally out of scope.
- **Type consistency:** `iter_latvia_ur_entity_rows` (file handle) vs `iter_latvia_ur_entity_rows_from_text` (str) are distinct and both referenced consistently; `DLT_DATASET_NAME`/`ENTITIES_TABLE` are defined once in `tables.py` and re-exported through `resources`/`assets`. `run_latvia_ur_dlt_pipeline` signature matches the Task 3 test call (keyword-only `database_path`, `run_id`, `session`, `pipelines_dir`).
- **Placeholder scan:** none — all steps carry complete code.
- **Known follow-ups (not this plan):** Module 2 financials (4-CSV pivot with raw-preserve checkpoints, Risks R1/R6/R7), Module 3 `latvia_resolved` dbt + ClickHouse export (with a Latvia-specific ClickHouse table name to avoid clobbering `corpscout.companies`).
