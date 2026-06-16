# Norway Brreg Company Data Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Norway company data assets to `dagster_v3` using Brreg entities as the company spine, Brreg sub-entities as establishments, English-translated descriptive fields, and Regnskapsregisteret financial statement enrichment with original-currency and USD amounts.

**Architecture:** Follow the current Finland v3 pattern: source-owned package under `src/dagster_v3/defs`, dlt assets loading into a local DuckDB file, pure row-builder functions with focused tests, and Dagster graph assertions through `defs()`. Use Brreg bulk JSON gzip for entities and sub-entities, then add a derived text-translation table that preserves original Norwegian source text and stores English text with translation provenance. Store exchange rates once in a separate shared `exchange_rates` Dagster section backed by ClickHouse table `reference.exchange_rates`, then let Norway and future country sources depend on that asset for USD conversion. Load Regnskapsregisteret original financial statements first, then build a derived Norway financial metrics table with each amount in original currency and USD. Keep Brreg roles out of this implementation because the Norway analysis marks the endpoint as PII-bearing and requiring a retention/redaction decision.

**Tech Stack:** Dagster 1.13, dagster-dlt, dlt DuckDB destination, DuckDB, `ijson` streaming JSON parser, Norges Bank open-data exchange rates, Python `decimal.Decimal`, pytest, dg CLI.

---

## Source Context

- Norway analysis: `companycollect/companies/analysis/norway/`
- Base entities handoff: `companycollect/companies/analysis/norway/data_model/sources/brregenhet/countrydata_implementation_handoff.json`
- Sub-entities handoff: `companycollect/companies/analysis/norway/data_model/sources/brregunderenhet/countrydata_implementation_handoff.json`
- Financials handoff: `companycollect/companies/analysis/norway/data_model/sources/brregregnskap/countrydata_implementation_handoff.json`
- License: NLOD 2.0, attribution required: `Kilde: Bronnoysundregistrene (NLOD 2.0)`
- FX source: Norges Bank open data API / exchange rates, `https://data.norges-bank.no/api/data/EXR`
- Existing implementation model: `src/dagster_v3/defs/finland_ytj/assets.py`, `src/dagster_v3/defs/finland_xbrl/assets.py`

## File Structure

- Create `src/dagster_v3/defs/norway_brreg/__init__.py`: package marker.
- Prerequisite plan: `docs/superpowers/plans/2026-06-16-exchange-rates-clickhouse-reference.md` creates the shared `exchange_rates` Dagster section and ClickHouse table `reference.exchange_rates`.
- Create `src/dagster_v3/defs/norway_brreg/resources.py`: `NorwayDuckDBResource` and reusable HTTP protocol.
- Create `src/dagster_v3/defs/norway_brreg/assets.py`: dlt sources, Dagster assets, row builders, API download helpers, translation-table builder, shared exchange-rate lookup, USD conversion builder, and `defs`.
- Create `tests/test_norway_brreg_assets.py`: unit tests for dependency presence, row building, gzip streaming, original/translated text modeling, dlt loading, financial API paging/caching behavior, FX conversion, and asset graph shape.
- Modify `pyproject.toml`: add `ijson` for streaming Brreg top-level JSON arrays without loading the full bulk file into memory.
- Modify `README.md`: document Norway assets, source URLs, scope, attribution, and roles exclusion.

## Asset Graph

```text
norway_brreg_entities_duckdb
  -> norway_brreg_company_text_translations
  -> norway_brreg_financial_candidates
  -> norway_brreg_financial_statements_duckdb
  -> norway_brreg_financial_statement_metrics

norway_brreg_sub_entities_duckdb
  -> norway_brreg_company_text_translations

exchange_rates
  -> norway_brreg_financial_statement_metrics
```

Expected groups:

```text
norway_brreg
```

Expected DuckDB database file:

```text
data/norway_brreg.duckdb
```

Expected dlt dataset:

```text
norway_brreg
```

Expected dlt tables:

```text
entities
sub_entities
financial_statements
company_text_translations
financial_statement_metrics
```

---

## Task 0: Implement Shared Exchange Rate ClickHouse Section First

**Files:**
- Reference plan: `docs/superpowers/plans/2026-06-16-exchange-rates-clickhouse-reference.md`

- [ ] **Step 1: Complete the exchange-rate reference plan**

Run the separate exchange-rate implementation plan before implementing Norway USD metrics:

```bash
uv run pytest tests/test_exchange_rates_assets.py -v
uv run dg list defs --json
```

Expected: tests pass and `dg list defs --json` includes the `exchange_rates` asset in group `exchange_rates`. The asset stores online exchange rates in ClickHouse table `reference.exchange_rates`.

- [ ] **Step 2: Confirm Norway can depend on the shared asset**

The Norway metric asset added later in this plan must declare:

```python
deps=["norway_brreg_financial_statements_duckdb", "exchange_rates"]
```

and read ClickHouse table:

```text
reference.exchange_rates
```

instead of creating a Norway-specific FX table.

---

## Task 1: Add Norway Package, Resource, and Dependency

**Files:**
- Create: `src/dagster_v3/defs/norway_brreg/__init__.py`
- Create: `src/dagster_v3/defs/norway_brreg/resources.py`
- Modify: `pyproject.toml`
- Test: `tests/test_norway_brreg_assets.py`

- [ ] **Step 1: Write the failing dependency and resource tests**

Create `tests/test_norway_brreg_assets.py` with:

```python
from pathlib import Path

import duckdb

from dagster_v3.defs.norway_brreg.resources import NorwayDuckDBResource


def test_streaming_json_dependency_is_available() -> None:
    import ijson

    assert ijson


def test_norway_duckdb_resource_defaults_to_country_database() -> None:
    resource = NorwayDuckDBResource()

    assert resource.path() == Path("data/norway_brreg.duckdb")


def test_norway_duckdb_resource_connects_to_configured_path(tmp_path: Path) -> None:
    resource = NorwayDuckDBResource(database_path=str(tmp_path / "norway.duckdb"))

    with resource.connect() as connection:
        connection.execute("create table smoke(id integer)")
        connection.execute("insert into smoke values (1)")

    with duckdb.connect(str(tmp_path / "norway.duckdb"), read_only=True) as connection:
        assert connection.execute("select id from smoke").fetchone() == (1,)
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'dagster_v3.defs.norway_brreg'` or `ModuleNotFoundError: No module named 'ijson'`.

- [ ] **Step 3: Add dependency**

In `pyproject.toml`, add this dependency next to the existing data parsing dependencies:

```toml
"ijson>=3.4.0",
```

Run:

```bash
uv sync
```

- [ ] **Step 4: Create package and resource**

Create `src/dagster_v3/defs/norway_brreg/__init__.py`:

```python
"""Norway Brreg company data assets."""
```

Create `src/dagster_v3/defs/norway_brreg/resources.py`:

```python
from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Protocol

import dagster as dg
import duckdb


class HttpSession(Protocol):
    headers: dict[str, str]

    def get(self, url: str, params: dict[str, Any] | None = None, timeout: int = 120) -> Any:
        ...


class NorwayDuckDBResource(dg.ConfigurableResource):
    database_path: str = "data/norway_brreg.duckdb"

    def path(self) -> Path:
        return Path(self.database_path)

    @contextmanager
    def connect(self, *, read_only: bool = False) -> Iterator[Any]:
        path = self.path()
        path.parent.mkdir(parents=True, exist_ok=True)
        connection = duckdb.connect(str(path), read_only=read_only)
        try:
            yield connection
        finally:
            connection.close()
```

- [ ] **Step 5: Run focused tests**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml uv.lock src/dagster_v3/defs/norway_brreg tests/test_norway_brreg_assets.py
git commit -m "Add Norway Brreg package resource"
```

---

## Task 2: Implement Brreg Entity Bulk Loader

**Files:**
- Modify: `src/dagster_v3/defs/norway_brreg/assets.py`
- Modify: `tests/test_norway_brreg_assets.py`

- [ ] **Step 1: Add failing entity row, gzip, dlt, and graph tests**

Append to `tests/test_norway_brreg_assets.py`:

```python
import gzip
import json
from pathlib import Path
from typing import Any

from dagster_v3.definitions import defs as load_project_defs
import dagster_v3.defs.norway_brreg.assets as brreg_assets


class FakeResponse:
    def __init__(self, content: bytes = b"", payload: Any | None = None, status_code: int = 200) -> None:
        self.content = content
        self._payload = payload
        self.status_code = status_code

    def json(self) -> Any:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeHttpSession:
    def __init__(self, content: bytes = b"", payloads: dict[str, Any] | None = None) -> None:
        self.content = content
        self.payloads = payloads or {}
        self.calls: list[tuple[str, dict | None, int]] = []
        self.headers: dict[str, str] = {}

    def get(self, url: str, params: dict | None = None, timeout: int = 120) -> FakeResponse:
        self.calls.append((url, params, timeout))
        if url in self.payloads:
            return FakeResponse(payload=self.payloads[url])
        return FakeResponse(content=self.content)


def _gzip_json_array(records: list[dict[str, Any]]) -> bytes:
    return gzip.compress(json.dumps(records).encode("utf-8"))


def _entity_record(**overrides: Any) -> dict[str, Any]:
    record = {
        "organisasjonsnummer": "923609016",
        "navn": "EQUINOR ASA",
        "organisasjonsform": {"kode": "ASA", "beskrivelse": "Allmennaksjeselskap"},
        "hjemmeside": "www.equinor.com",
        "registreringsdatoEnhetsregisteret": "1995-03-12",
        "registrertIMvaregisteret": True,
        "naeringskode1": {"kode": "06.100", "beskrivelse": "Utvinning av raolje"},
        "naeringskode2": {"kode": "06.200", "beskrivelse": "Utvinning av naturgass"},
        "antallAnsatte": 21467,
        "forretningsadresse": {
            "adresse": ["Forusbeen 50"],
            "postnummer": "4035",
            "poststed": "STAVANGER",
            "kommune": "STAVANGER",
            "kommunenummer": "1103",
            "landkode": "NO",
        },
        "stiftelsesdato": "1972-09-18",
        "registrertIForetaksregisteret": True,
        "sisteInnsendteAarsregnskap": "2024",
        "konkurs": False,
        "underAvvikling": False,
        "underTvangsavviklingEllerTvangsopplosning": False,
        "erIKonsern": True,
        "overordnetEnhet": "000000000",
        "_links": {"self": {"href": "https://data.brreg.no/enhetsregisteret/api/enheter/923609016"}},
    }
    record.update(overrides)
    return record


def test_entity_rows_extract_company_spine_fields() -> None:
    rows = brreg_assets.build_entity_rows([_entity_record()], run_id="test-run")

    assert rows[0]["country_iso2"] == "NO"
    assert rows[0]["source_slug"] == "norway_brregenhet"
    assert rows[0]["source_run_id"] == "test-run"
    assert rows[0]["source_line_number"] == 1
    assert rows[0]["source_record_id"] == "923609016"
    assert rows[0]["org_number"] == "923609016"
    assert rows[0]["vat_id"] == "NO923609016MVA"
    assert rows[0]["legal_name"] == "EQUINOR ASA"
    assert rows[0]["legal_form_code"] == "ASA"
    assert rows[0]["nace1_code"] == "06.100"
    assert rows[0]["employee_count"] == 21467
    assert rows[0]["status"] == "active"
    assert rows[0]["is_active"] is True
    assert rows[0]["last_submitted_accounts_year"] == "2024"
    assert json.loads(rows[0]["raw_entity"])["organisasjonsnummer"] == "923609016"


def test_entity_status_derivation_handles_liquidation_and_bankruptcy() -> None:
    rows = brreg_assets.build_entity_rows(
        [
            _entity_record(organisasjonsnummer="1", konkurs=True),
            _entity_record(organisasjonsnummer="2", underAvvikling=True),
            _entity_record(organisasjonsnummer="3", underTvangsavviklingEllerTvangsopplosning=True),
        ],
        run_id="test-run",
    )

    assert [row["status"] for row in rows] == [
        "bankrupt",
        "liquidation",
        "compulsory_liquidation",
    ]
    assert [row["is_active"] for row in rows] == [False, False, False]


def test_brreg_entity_source_downloads_gzip_and_yields_rows() -> None:
    session = FakeHttpSession(_gzip_json_array([_entity_record(), _entity_record(organisasjonsnummer="999999999")]))

    source = brreg_assets.norway_brreg_entities_source(session=session, run_id="test-run")
    rows = list(source.resources[brreg_assets.ENTITIES_TABLE])

    assert [row["org_number"] for row in rows] == ["923609016", "999999999"]
    assert session.calls == [
        ("https://data.brreg.no/enhetsregisteret/api/enheter/lastned", None, 120)
    ]
    assert session.headers["User-Agent"] == brreg_assets.DEFAULT_USER_AGENT


def test_entity_dlt_pipeline_loads_entities_table(tmp_path: Path) -> None:
    session = FakeHttpSession(_gzip_json_array([_entity_record()]))

    load_info = brreg_assets.run_norway_brreg_entities_dlt_pipeline(
        database_path=tmp_path / "norway.duckdb",
        run_id="test-run",
        session=session,
    )

    assert load_info
    with duckdb.connect(str(tmp_path / "norway.duckdb"), read_only=True) as connection:
        rows = connection.execute(
            "select org_number, legal_name, status from norway_brreg.entities"
        ).fetchall()

    assert rows == [("923609016", "EQUINOR ASA", "active")]


def test_norway_entity_asset_is_registered() -> None:
    asset_graph = load_project_defs().resolve_asset_graph()

    assert "norway_brreg_entities_duckdb" in {key.path[-1] for key in asset_graph.get_all_asset_keys()}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py -v
```

Expected: FAIL because `assets.py`, dlt source functions, and Dagster asset registration do not exist.

- [ ] **Step 3: Implement entity loader**

Create `src/dagster_v3/defs/norway_brreg/assets.py` with this initial implementation:

```python
import gzip
import hashlib
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import dagster as dg
import dlt
import ijson
import requests
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData
from dlt.extract.resource import DltResource
from dlt.pipeline.pipeline import Pipeline

from dagster_v3.defs.norway_brreg.resources import HttpSession, NorwayDuckDBResource

COUNTRY = "NO"
GROUP_NAME = "norway_brreg"
DLT_DATASET_NAME = "norway_brreg"
ENTITIES_TABLE = "entities"
SUB_ENTITIES_TABLE = "sub_entities"
FINANCIAL_STATEMENTS_TABLE = "financial_statements"
ENTITY_SOURCE_SLUG = "norway_brregenhet"
SUB_ENTITY_SOURCE_SLUG = "norway_brregunderenhet"
FINANCIAL_SOURCE_SLUG = "norway_brregregnskap"
BRREG_BASE_URL = "https://data.brreg.no/enhetsregisteret/api"
REGNSKAP_BASE_URL = "https://data.brreg.no/regnskapsregisteret/regnskap"
DEFAULT_TIMEOUT_SECONDS = 120
DEFAULT_USER_AGENT = "corpscout-dagster-v3-dev/0.1"


class NorwayBrregDltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data: DltResourceTranslatorData) -> dg.AssetSpec:
        spec = super().get_asset_spec(data)
        asset_names = {
            ENTITIES_TABLE: "norway_brreg_entities_duckdb",
            SUB_ENTITIES_TABLE: "norway_brreg_sub_entities_duckdb",
            FINANCIAL_STATEMENTS_TABLE: "norway_brreg_financial_statements_duckdb",
        }
        if data.resource.name not in asset_names:
            return spec
        return spec.replace_attributes(
            key=asset_names[data.resource.name],
            deps=[],
            group_name=GROUP_NAME,
            kinds={"python", "dlt", "duckdb"},
        )


@dlt.source(name="norway_brreg_entities")
def norway_brreg_entities_source(
    *,
    base_url: str = BRREG_BASE_URL,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    user_agent: str = DEFAULT_USER_AGENT,
    run_id: str = "",
    session: HttpSession | None = None,
) -> DltResource:
    return _entities_resource(
        base_url=base_url,
        timeout_seconds=timeout_seconds,
        user_agent=user_agent,
        run_id=run_id,
        session=session,
    )


@dlt.resource(name=ENTITIES_TABLE, write_disposition="replace", primary_key="org_number")
def _entities_resource(
    *,
    base_url: str,
    timeout_seconds: int,
    user_agent: str,
    run_id: str,
    session: HttpSession | None,
) -> Iterator[dict[str, Any]]:
    response_body = _download_bytes(
        url=f"{base_url}/enheter/lastned",
        timeout_seconds=timeout_seconds,
        user_agent=user_agent,
        session=session,
    )
    for line_number, entity in enumerate(_stream_gzip_json_array(response_body), start=1):
        if isinstance(entity, dict):
            yield _entity_row(entity, line_number=line_number, run_id=run_id)


def run_norway_brreg_entities_dlt_pipeline(
    *,
    database_path: str | Path,
    run_id: str,
    session: HttpSession | None = None,
) -> Any:
    return norway_brreg_pipeline(database_path, pipeline_name="norway_brreg_entities").run(
        norway_brreg_entities_source(run_id=run_id, session=session)
    )


def norway_brreg_pipeline(database_path: str | Path, *, pipeline_name: str) -> Pipeline:
    database_file = Path(database_path)
    database_file.parent.mkdir(parents=True, exist_ok=True)
    return dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=dlt.destinations.duckdb(str(database_file)),
        dataset_name=DLT_DATASET_NAME,
        dev_mode=False,
    )


@dlt_assets(
    dlt_source=norway_brreg_entities_source(),
    dlt_pipeline=norway_brreg_pipeline(
        NorwayDuckDBResource().path(),
        pipeline_name="norway_brreg_entities",
    ),
    name="norway_brreg_entities_duckdb",
    dagster_dlt_translator=NorwayBrregDltTranslator(),
)
def norway_brreg_entities_duckdb_asset(
    context: dg.AssetExecutionContext,
    dlt: DagsterDltResource,
    norway_duckdb: NorwayDuckDBResource,
) -> Iterator[Any]:
    """Load Brreg entity bulk data to local DuckDB with dlt."""
    yield from dlt.run(
        context=context,
        dlt_source=norway_brreg_entities_source(run_id=context.run_id),
        dlt_pipeline=norway_brreg_pipeline(
            norway_duckdb.path(),
            pipeline_name="norway_brreg_entities",
        ),
    )


defs = dg.Definitions(
    assets=[norway_brreg_entities_duckdb_asset],
    resources={"norway_duckdb": NorwayDuckDBResource()},
)
```

Continue in the same file with helpers:

```python
def build_entity_rows(entities: list[dict[str, Any]], *, run_id: str) -> list[dict[str, Any]]:
    return [
        _entity_row(entity, line_number=index, run_id=run_id)
        for index, entity in enumerate(entities, start=1)
    ]


def _entity_row(entity: dict[str, Any], *, line_number: int, run_id: str) -> dict[str, Any]:
    org_number = _string(entity.get("organisasjonsnummer"))
    vat_registered = _bool(entity.get("registrertIMvaregisteret"))
    business_address = _dict(entity.get("forretningsadresse"))
    legal_form = _dict(entity.get("organisasjonsform"))
    nace1 = _dict(entity.get("naeringskode1"))
    nace2 = _dict(entity.get("naeringskode2"))
    nace3 = _dict(entity.get("naeringskode3"))
    status = _entity_status(entity)
    return {
        "country_iso2": COUNTRY,
        "source_slug": ENTITY_SOURCE_SLUG,
        "source_run_id": run_id,
        "source_line_number": line_number,
        "source_record_id": org_number,
        "source_payload_hash": source_payload_hash(entity),
        "org_number": org_number,
        "vat_id": f"NO{org_number}MVA" if vat_registered and org_number else "",
        "legal_name": _string(entity.get("navn")),
        "legal_form_code": _string(legal_form.get("kode")),
        "legal_form_description_original": _string(legal_form.get("beskrivelse")),
        "registration_date": _string(entity.get("registreringsdatoEnhetsregisteret")),
        "incorporation_date": _string(entity.get("stiftelsesdato")),
        "website": _string(entity.get("hjemmeside")),
        "phone": _string(entity.get("telefon")),
        "nace1_code": _string(nace1.get("kode")),
        "nace1_description_original": _string(nace1.get("beskrivelse")),
        "nace2_code": _string(nace2.get("kode")),
        "nace2_description_original": _string(nace2.get("beskrivelse")),
        "nace3_code": _string(nace3.get("kode")),
        "nace3_description_original": _string(nace3.get("beskrivelse")),
        "articles_purpose_original": _joined_text_lines(entity.get("vedtektsfestetFormaal")),
        "activity_text_original": _joined_text_lines(entity.get("aktivitet")),
        "employee_count": _int_or_none(entity.get("antallAnsatte")),
        "has_registered_employee_count": _bool(entity.get("harRegistrertAntallAnsatte")),
        "business_address_lines": _address_lines(business_address),
        "business_postal_code": _string(business_address.get("postnummer")),
        "business_city": _string(business_address.get("poststed")),
        "business_municipality": _string(business_address.get("kommune")),
        "business_municipality_code": _string(business_address.get("kommunenummer")),
        "business_country_code": _string(business_address.get("landkode")),
        "is_vat_registered": vat_registered,
        "is_enterprise_register_registered": _bool(entity.get("registrertIForetaksregisteret")),
        "is_group_member": _bool(entity.get("erIKonsern")),
        "parent_org_number": _string(entity.get("overordnetEnhet")),
        "last_submitted_accounts_year": _string(entity.get("sisteInnsendteAarsregnskap")),
        "status": status,
        "is_active": status == "active",
        "source_url": _source_url(entity),
        "raw_entity": _raw_json(entity),
    }


def _entity_status(entity: dict[str, Any]) -> str:
    if _bool(entity.get("konkurs")):
        return "bankrupt"
    if _bool(entity.get("underTvangsavviklingEllerTvangsopplosning")):
        return "compulsory_liquidation"
    if _bool(entity.get("underAvvikling")):
        return "liquidation"
    return "active"
```

Add shared helper functions:

```python
def _download_bytes(
    *,
    url: str,
    timeout_seconds: int,
    user_agent: str,
    session: HttpSession | None,
) -> bytes:
    http_session = session or requests.Session()
    http_session.headers["User-Agent"] = user_agent
    response = http_session.get(url, timeout=timeout_seconds)
    response.raise_for_status()
    return response.content


def _stream_gzip_json_array(body: bytes) -> Iterator[Any]:
    with gzip.GzipFile(fileobj=BytesIO(body)) as stream:
        yield from ijson.items(stream, "item")


def source_payload_hash(payload: dict[str, Any]) -> str:
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _source_url(record: dict[str, Any]) -> str:
    return _string(_dict(_dict(_dict(record.get("_links")).get("self")).get("href")))


def _address_lines(address: dict[str, Any]) -> str:
    return "\n".join(_string(line) for line in _list(address.get("adresse")) if _string(line))


def _raw_json(value: dict[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _string(value: Any) -> str:
    return "" if value is None else str(value)


def _bool(value: Any) -> bool:
    return bool(value) if isinstance(value, bool) else False


def _int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)
```

Also add this import at the top:

```python
from io import BytesIO
```

- [ ] **Step 4: Run focused tests**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py -v
uv run dg list defs --json
```

Expected: tests pass and JSON output includes `norway_brreg_entities_duckdb` in group `norway_brreg`.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/norway_brreg tests/test_norway_brreg_assets.py
git commit -m "Add Norway Brreg entity loader"
```

---

## Task 3: Implement Brreg Sub-Entity Bulk Loader

**Files:**
- Modify: `src/dagster_v3/defs/norway_brreg/assets.py`
- Modify: `tests/test_norway_brreg_assets.py`

- [ ] **Step 1: Add failing sub-entity tests**

Append:

```python
def _sub_entity_record(**overrides: Any) -> dict[str, Any]:
    record = {
        "organisasjonsnummer": "933489272",
        "navn": "- ZOTKO .NO",
        "organisasjonsform": {
            "kode": "BEDR",
            "beskrivelse": "Underenhet til naeringsdrivende og offentlig forvaltning",
        },
        "registreringsdatoEnhetsregisteret": "2024-05-23",
        "registrertIMvaregisteret": False,
        "naeringskode1": {"kode": "47.250", "beskrivelse": "Detaljhandel med drikkevarer"},
        "harRegistrertAntallAnsatte": False,
        "overordnetEnhet": "933365573",
        "oppstartsdato": "2024-06-01",
        "beliggenhetsadresse": {
            "adresse": ["Follerovegen 20"],
            "postnummer": "6652",
            "poststed": "SURNADAL",
            "kommune": "SURNADAL",
            "kommunenummer": "1566",
            "landkode": "NO",
        },
    }
    record.update(overrides)
    return record


def test_sub_entity_rows_extract_establishment_fields() -> None:
    rows = brreg_assets.build_sub_entity_rows([_sub_entity_record()], run_id="test-run")

    assert rows[0]["source_slug"] == "norway_brregunderenhet"
    assert rows[0]["establishment_org_number"] == "933489272"
    assert rows[0]["parent_org_number"] == "933365573"
    assert rows[0]["legal_name"] == "- ZOTKO .NO"
    assert rows[0]["nace1_code"] == "47.250"
    assert rows[0]["site_address_lines"] == "Follerovegen 20"
    assert rows[0]["site_municipality_code"] == "1566"
    assert json.loads(rows[0]["raw_sub_entity"])["organisasjonsnummer"] == "933489272"


def test_brreg_sub_entity_source_downloads_gzip_and_yields_rows() -> None:
    session = FakeHttpSession(_gzip_json_array([_sub_entity_record()]))

    source = brreg_assets.norway_brreg_sub_entities_source(session=session, run_id="test-run")
    rows = list(source.resources[brreg_assets.SUB_ENTITIES_TABLE])

    assert [row["establishment_org_number"] for row in rows] == ["933489272"]
    assert session.calls == [
        ("https://data.brreg.no/enhetsregisteret/api/underenheter/lastned", None, 120)
    ]


def test_sub_entity_dlt_pipeline_loads_sub_entities_table(tmp_path: Path) -> None:
    session = FakeHttpSession(_gzip_json_array([_sub_entity_record()]))

    load_info = brreg_assets.run_norway_brreg_sub_entities_dlt_pipeline(
        database_path=tmp_path / "norway.duckdb",
        run_id="test-run",
        session=session,
    )

    assert load_info
    with duckdb.connect(str(tmp_path / "norway.duckdb"), read_only=True) as connection:
        rows = connection.execute(
            "select establishment_org_number, parent_org_number from norway_brreg.sub_entities"
        ).fetchall()

    assert rows == [("933489272", "933365573")]


def test_norway_sub_entity_asset_is_registered() -> None:
    asset_graph = load_project_defs().resolve_asset_graph()

    assert "norway_brreg_sub_entities_duckdb" in {
        key.path[-1] for key in asset_graph.get_all_asset_keys()
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py -v
```

Expected: FAIL because sub-entity builders and assets are absent.

- [ ] **Step 3: Add sub-entity source, pipeline runner, and asset**

Append to `assets.py`:

```python
@dlt.source(name="norway_brreg_sub_entities")
def norway_brreg_sub_entities_source(
    *,
    base_url: str = BRREG_BASE_URL,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    user_agent: str = DEFAULT_USER_AGENT,
    run_id: str = "",
    session: HttpSession | None = None,
) -> DltResource:
    return _sub_entities_resource(
        base_url=base_url,
        timeout_seconds=timeout_seconds,
        user_agent=user_agent,
        run_id=run_id,
        session=session,
    )


@dlt.resource(
    name=SUB_ENTITIES_TABLE,
    write_disposition="replace",
    primary_key="establishment_org_number",
)
def _sub_entities_resource(
    *,
    base_url: str,
    timeout_seconds: int,
    user_agent: str,
    run_id: str,
    session: HttpSession | None,
) -> Iterator[dict[str, Any]]:
    response_body = _download_bytes(
        url=f"{base_url}/underenheter/lastned",
        timeout_seconds=timeout_seconds,
        user_agent=user_agent,
        session=session,
    )
    for line_number, sub_entity in enumerate(_stream_gzip_json_array(response_body), start=1):
        if isinstance(sub_entity, dict):
            yield _sub_entity_row(sub_entity, line_number=line_number, run_id=run_id)


def run_norway_brreg_sub_entities_dlt_pipeline(
    *,
    database_path: str | Path,
    run_id: str,
    session: HttpSession | None = None,
) -> Any:
    return norway_brreg_pipeline(database_path, pipeline_name="norway_brreg_sub_entities").run(
        norway_brreg_sub_entities_source(run_id=run_id, session=session)
    )


@dlt_assets(
    dlt_source=norway_brreg_sub_entities_source(),
    dlt_pipeline=norway_brreg_pipeline(
        NorwayDuckDBResource().path(),
        pipeline_name="norway_brreg_sub_entities",
    ),
    name="norway_brreg_sub_entities_duckdb",
    dagster_dlt_translator=NorwayBrregDltTranslator(),
)
def norway_brreg_sub_entities_duckdb_asset(
    context: dg.AssetExecutionContext,
    dlt: DagsterDltResource,
    norway_duckdb: NorwayDuckDBResource,
) -> Iterator[Any]:
    """Load Brreg sub-entity bulk data to local DuckDB with dlt."""
    yield from dlt.run(
        context=context,
        dlt_source=norway_brreg_sub_entities_source(run_id=context.run_id),
        dlt_pipeline=norway_brreg_pipeline(
            norway_duckdb.path(),
            pipeline_name="norway_brreg_sub_entities",
        ),
    )
```

Add row builders:

```python
def build_sub_entity_rows(sub_entities: list[dict[str, Any]], *, run_id: str) -> list[dict[str, Any]]:
    return [
        _sub_entity_row(sub_entity, line_number=index, run_id=run_id)
        for index, sub_entity in enumerate(sub_entities, start=1)
    ]


def _sub_entity_row(
    sub_entity: dict[str, Any],
    *,
    line_number: int,
    run_id: str,
) -> dict[str, Any]:
    establishment_org_number = _string(sub_entity.get("organisasjonsnummer"))
    site_address = _dict(sub_entity.get("beliggenhetsadresse"))
    legal_form = _dict(sub_entity.get("organisasjonsform"))
    nace1 = _dict(sub_entity.get("naeringskode1"))
    return {
        "country_iso2": COUNTRY,
        "source_slug": SUB_ENTITY_SOURCE_SLUG,
        "source_run_id": run_id,
        "source_line_number": line_number,
        "source_record_id": establishment_org_number,
        "source_payload_hash": source_payload_hash(sub_entity),
        "establishment_org_number": establishment_org_number,
        "parent_org_number": _string(sub_entity.get("overordnetEnhet")),
        "legal_name": _string(sub_entity.get("navn")),
        "legal_form_code": _string(legal_form.get("kode")),
        "legal_form_description_original": _string(legal_form.get("beskrivelse")),
        "registration_date": _string(sub_entity.get("registreringsdatoEnhetsregisteret")),
        "startup_date": _string(sub_entity.get("oppstartsdato")),
        "nace1_code": _string(nace1.get("kode")),
        "nace1_description_original": _string(nace1.get("beskrivelse")),
        "employee_count": _int_or_none(sub_entity.get("antallAnsatte")),
        "has_registered_employee_count": _bool(sub_entity.get("harRegistrertAntallAnsatte")),
        "site_address_lines": _address_lines(site_address),
        "site_postal_code": _string(site_address.get("postnummer")),
        "site_city": _string(site_address.get("poststed")),
        "site_municipality": _string(site_address.get("kommune")),
        "site_municipality_code": _string(site_address.get("kommunenummer")),
        "site_country_code": _string(site_address.get("landkode")),
        "raw_sub_entity": _raw_json(sub_entity),
    }
```

Update `defs` assets list:

```python
defs = dg.Definitions(
    assets=[
        norway_brreg_entities_duckdb_asset,
        norway_brreg_sub_entities_duckdb_asset,
    ],
    resources={"norway_duckdb": NorwayDuckDBResource()},
)
```

- [ ] **Step 4: Run focused tests**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py -v
uv run dg list defs --json
```

Expected: tests pass and `dg` output includes `norway_brreg_sub_entities_duckdb`.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/norway_brreg/assets.py tests/test_norway_brreg_assets.py
git commit -m "Add Norway Brreg sub-entity loader"
```

---

## Task 4: Add Financial Candidate Asset

**Files:**
- Modify: `src/dagster_v3/defs/norway_brreg/assets.py`
- Modify: `tests/test_norway_brreg_assets.py`

- [ ] **Step 1: Add failing candidate test**

Append:

```python
def test_financial_candidate_asset_selects_entities_with_accounts_signal(tmp_path: Path) -> None:
    resource = NorwayDuckDBResource(database_path=str(tmp_path / "norway.duckdb"))
    with resource.connect() as connection:
        connection.execute("create schema norway_brreg")
        connection.execute(
            """
            create table norway_brreg.entities(
                org_number varchar,
                legal_name varchar,
                legal_form_code varchar,
                last_submitted_accounts_year varchar,
                is_active boolean
            )
            """
        )
        connection.execute(
            """
            insert into norway_brreg.entities values
            ('923609016', 'EQUINOR ASA', 'ASA', '2024', true),
            ('111111111', 'NO ACCOUNTS', 'ENK', '', true),
            ('222222222', 'DISSOLVED AS', 'AS', '2023', false)
            """
        )

    result = brreg_assets.build_financial_candidates(resource)

    assert result.metadata["candidate_count"] == 1
    with resource.connect(read_only=True) as connection:
        rows = connection.execute(
            "select org_number, accounts_year from norway_brreg.financial_candidates"
        ).fetchall()

    assert rows == [("923609016", "2024")]


def test_financial_candidate_asset_depends_on_entities() -> None:
    asset_graph = load_project_defs().resolve_asset_graph()

    assert asset_graph.get(
        brreg_assets.dg.AssetKey("norway_brreg_financial_candidates")
    ).parent_keys == {brreg_assets.dg.AssetKey("norway_brreg_entities_duckdb")}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py::test_financial_candidate_asset_selects_entities_with_accounts_signal tests/test_norway_brreg_assets.py::test_financial_candidate_asset_depends_on_entities -v
```

Expected: FAIL because candidate asset is absent.

- [ ] **Step 3: Add candidate builder and asset**

Append to `assets.py`:

```python
@dg.asset(
    name="norway_brreg_financial_candidates",
    group_name=GROUP_NAME,
    deps=["norway_brreg_entities_duckdb"],
    kinds={"python", "duckdb", "sql"},
)
def norway_brreg_financial_candidates(
    norway_duckdb: NorwayDuckDBResource,
) -> dg.MaterializeResult:
    """Entities whose Brreg record signals filed annual accounts."""
    return build_financial_candidates(norway_duckdb)


def build_financial_candidates(norway_duckdb: NorwayDuckDBResource) -> dg.MaterializeResult:
    with norway_duckdb.connect() as connection:
        connection.execute(
            """
            create table if not exists norway_brreg.financial_candidates as
            select
                org_number,
                legal_name,
                legal_form_code,
                last_submitted_accounts_year as accounts_year
            from norway_brreg.entities
            where false
            """
        )
        connection.execute("delete from norway_brreg.financial_candidates")
        connection.execute(
            """
            insert into norway_brreg.financial_candidates
            select
                org_number,
                legal_name,
                legal_form_code,
                last_submitted_accounts_year as accounts_year
            from norway_brreg.entities
            where is_active = true
              and coalesce(last_submitted_accounts_year, '') <> ''
              and legal_form_code in ('AS', 'ASA', 'NUF')
            order by org_number
            """
        )
        candidate_count = connection.execute(
            "select count(*) from norway_brreg.financial_candidates"
        ).fetchone()[0]
    return dg.MaterializeResult(metadata={"candidate_count": candidate_count})
```

Update `defs` assets list:

```python
defs = dg.Definitions(
    assets=[
        norway_brreg_entities_duckdb_asset,
        norway_brreg_sub_entities_duckdb_asset,
        norway_brreg_financial_candidates,
    ],
    resources={"norway_duckdb": NorwayDuckDBResource()},
)
```

- [ ] **Step 4: Run focused tests**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py -v
uv run dg list defs --json
```

Expected: tests pass and `norway_brreg_financial_candidates` depends only on `norway_brreg_entities_duckdb`.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/norway_brreg/assets.py tests/test_norway_brreg_assets.py
git commit -m "Add Norway financial candidate asset"
```

---

## Task 4A: Add Original Norwegian Text Plus English Translation Model

**Files:**
- Modify: `src/dagster_v3/defs/norway_brreg/assets.py`
- Modify: `tests/test_norway_brreg_assets.py`

- [ ] **Step 1: Add failing text translation tests**

Append:

```python
def test_entity_rows_preserve_original_norwegian_description_fields() -> None:
    rows = brreg_assets.build_entity_rows(
        [
            _entity_record(
                vedtektsfestetFormaal=["Aa utvikle energi", "og tjenester."],
                aktivitet=["Produksjon og markedsforing av energi."],
            )
        ],
        run_id="test-run",
    )

    assert rows[0]["legal_form_description_original"] == "Allmennaksjeselskap"
    assert rows[0]["nace1_description_original"] == "Utvinning av raolje"
    assert rows[0]["articles_purpose_original"] == "Aa utvikle energi\nog tjenester."
    assert rows[0]["activity_text_original"] == "Produksjon og markedsforing av energi."


def test_company_text_translation_rows_model_original_and_english_text() -> None:
    rows = brreg_assets.build_company_text_translation_rows(
        [
            {
                "org_number": "923609016",
                "source_payload_hash": "a" * 64,
                "legal_form_description_original": "Allmennaksjeselskap",
                "nace1_description_original": "Utvinning av raolje",
                "articles_purpose_original": "Aa utvikle energi\nog tjenester.",
                "activity_text_original": "Produksjon og markedsforing av energi.",
            }
        ],
        translations={
            "Allmennaksjeselskap": "Public limited company",
            "Utvinning av raolje": "Extraction of crude petroleum",
            "Aa utvikle energi\nog tjenester.": "To develop energy and services.",
            "Produksjon og markedsforing av energi.": "Production and marketing of energy.",
        },
        run_id="translation-run",
    )

    assert rows == [
        {
            "country_iso2": "NO",
            "org_number": "923609016",
            "field_name": "legal_form_description",
            "source_language": "no",
            "original_text": "Allmennaksjeselskap",
            "translated_text_en": "Public limited company",
            "translation_provider": "translation_memory",
            "translation_run_id": "translation-run",
            "source_payload_hash": "a" * 64,
        },
        {
            "country_iso2": "NO",
            "org_number": "923609016",
            "field_name": "nace1_description",
            "source_language": "no",
            "original_text": "Utvinning av raolje",
            "translated_text_en": "Extraction of crude petroleum",
            "translation_provider": "translation_memory",
            "translation_run_id": "translation-run",
            "source_payload_hash": "a" * 64,
        },
        {
            "country_iso2": "NO",
            "org_number": "923609016",
            "field_name": "articles_purpose",
            "source_language": "no",
            "original_text": "Aa utvikle energi\nog tjenester.",
            "translated_text_en": "To develop energy and services.",
            "translation_provider": "translation_memory",
            "translation_run_id": "translation-run",
            "source_payload_hash": "a" * 64,
        },
        {
            "country_iso2": "NO",
            "org_number": "923609016",
            "field_name": "activity_text",
            "source_language": "no",
            "original_text": "Produksjon og markedsforing av energi.",
            "translated_text_en": "Production and marketing of energy.",
            "translation_provider": "translation_memory",
            "translation_run_id": "translation-run",
            "source_payload_hash": "a" * 64,
        },
    ]


def test_company_text_translation_asset_depends_on_entities_and_sub_entities() -> None:
    asset_graph = load_project_defs().resolve_asset_graph()

    assert asset_graph.get(
        brreg_assets.dg.AssetKey("norway_brreg_company_text_translations")
    ).parent_keys == {
        brreg_assets.dg.AssetKey("norway_brreg_entities_duckdb"),
        brreg_assets.dg.AssetKey("norway_brreg_sub_entities_duckdb"),
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py::test_entity_rows_preserve_original_norwegian_description_fields tests/test_norway_brreg_assets.py::test_company_text_translation_rows_model_original_and_english_text tests/test_norway_brreg_assets.py::test_company_text_translation_asset_depends_on_entities_and_sub_entities -v
```

Expected: FAIL because original text columns, translation row builder, and translation asset are absent.

- [ ] **Step 3: Add original text columns to entity rows**

In `_entity_row`, replace the current description keys:

```python
"legal_form_description": _string(legal_form.get("beskrivelse")),
"nace1_description": _string(nace1.get("beskrivelse")),
"nace2_description": _string(nace2.get("beskrivelse")),
"nace3_description": _string(nace3.get("beskrivelse")),
```

with:

```python
"legal_form_description_original": _string(legal_form.get("beskrivelse")),
"nace1_description_original": _string(nace1.get("beskrivelse")),
"nace2_description_original": _string(nace2.get("beskrivelse")),
"nace3_description_original": _string(nace3.get("beskrivelse")),
"articles_purpose_original": _joined_text_lines(entity.get("vedtektsfestetFormaal")),
"activity_text_original": _joined_text_lines(entity.get("aktivitet")),
```

In `_sub_entity_row`, replace:

```python
"legal_form_description": _string(legal_form.get("beskrivelse")),
"nace1_description": _string(nace1.get("beskrivelse")),
```

with:

```python
"legal_form_description_original": _string(legal_form.get("beskrivelse")),
"nace1_description_original": _string(nace1.get("beskrivelse")),
```

Add:

```python
def _joined_text_lines(value: Any) -> str:
    return "\n".join(_string(line) for line in _list(value) if _string(line))
```

- [ ] **Step 4: Add translation row builder**

Append:

```python
TRANSLATABLE_ENTITY_FIELDS = (
    "legal_form_description",
    "nace1_description",
    "nace2_description",
    "nace3_description",
    "articles_purpose",
    "activity_text",
)


def build_company_text_translation_rows(
    entity_rows: list[dict[str, Any]],
    *,
    translations: dict[str, str],
    run_id: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for entity in entity_rows:
        org_number = _string(entity.get("org_number"))
        source_payload_hash = _string(entity.get("source_payload_hash"))
        for field_name in TRANSLATABLE_ENTITY_FIELDS:
            original_text = _string(entity.get(f"{field_name}_original")).strip()
            if not original_text:
                continue
            translated_text = translations.get(original_text, "")
            rows.append(
                {
                    "country_iso2": COUNTRY,
                    "org_number": org_number,
                    "field_name": field_name,
                    "source_language": "no",
                    "original_text": original_text,
                    "translated_text_en": translated_text,
                    "translation_provider": "translation_memory",
                    "translation_run_id": run_id,
                    "source_payload_hash": source_payload_hash,
                }
            )
    return rows
```

- [ ] **Step 5: Add derived DuckDB translation asset**

Append:

```python
@dg.asset(
    name="norway_brreg_company_text_translations",
    group_name=GROUP_NAME,
    deps=["norway_brreg_entities_duckdb", "norway_brreg_sub_entities_duckdb"],
    kinds={"python", "duckdb", "translation"},
)
def norway_brreg_company_text_translations(
    context: dg.AssetExecutionContext,
    norway_duckdb: NorwayDuckDBResource,
) -> dg.MaterializeResult:
    """Original Norwegian descriptive text plus reviewed English translations."""
    return build_company_text_translations_table(
        norway_duckdb=norway_duckdb,
        run_id=context.run_id,
    )


def build_company_text_translations_table(
    *,
    norway_duckdb: NorwayDuckDBResource,
    run_id: str,
) -> dg.MaterializeResult:
    with norway_duckdb.connect() as connection:
        connection.execute(
            """
            create table if not exists norway_brreg.company_text_translations(
                country_iso2 varchar,
                org_number varchar,
                field_name varchar,
                source_language varchar,
                original_text varchar,
                translated_text_en varchar,
                translation_provider varchar,
                translation_run_id varchar,
                source_payload_hash varchar
            )
            """
        )
        entity_rows = connection.execute(
            """
            select
                org_number,
                source_payload_hash,
                legal_form_description_original,
                nace1_description_original,
                nace2_description_original,
                nace3_description_original,
                articles_purpose_original,
                activity_text_original
            from norway_brreg.entities
            """
        ).fetchall()
        columns = [
            "org_number",
            "source_payload_hash",
            "legal_form_description_original",
            "nace1_description_original",
            "nace2_description_original",
            "nace3_description_original",
            "articles_purpose_original",
            "activity_text_original",
        ]
        source_rows = [dict(zip(columns, row, strict=True)) for row in entity_rows]
        translation_memory = load_translation_memory(connection)
        rows = build_company_text_translation_rows(
            source_rows,
            translations=translation_memory,
            run_id=run_id,
        )
        connection.execute("delete from norway_brreg.company_text_translations")
        if rows:
            connection.executemany(
                """
                insert into norway_brreg.company_text_translations values
                (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        row["country_iso2"],
                        row["org_number"],
                        row["field_name"],
                        row["source_language"],
                        row["original_text"],
                        row["translated_text_en"],
                        row["translation_provider"],
                        row["translation_run_id"],
                        row["source_payload_hash"],
                    )
                    for row in rows
                ],
            )
    return dg.MaterializeResult(
        metadata={
            "translation_rows": len(rows),
            "missing_translation_rows": sum(1 for row in rows if not row["translated_text_en"]),
        }
    )


def load_translation_memory(connection: Any) -> dict[str, str]:
    connection.execute(
        """
        create table if not exists norway_brreg.translation_memory(
            source_language varchar,
            target_language varchar,
            original_text varchar,
            translated_text varchar,
            reviewed_at varchar
        )
        """
    )
    return {
        original_text: translated_text
        for original_text, translated_text in connection.execute(
            """
            select original_text, translated_text
            from norway_brreg.translation_memory
            where source_language = 'no'
              and target_language = 'en'
            """
        ).fetchall()
    }
```

Update `defs` assets list to include `norway_brreg_company_text_translations`.

- [ ] **Step 6: Run focused tests**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py -v
uv run dg list defs --json
```

Expected: tests pass and `norway_brreg_company_text_translations` depends on both source bulk assets.

- [ ] **Step 7: Commit**

```bash
git add src/dagster_v3/defs/norway_brreg/assets.py tests/test_norway_brreg_assets.py
git commit -m "Model Norway company text translations"
```

---

## Task 5: Implement Regnskapsregisteret Financial Statement Loader

**Files:**
- Modify: `src/dagster_v3/defs/norway_brreg/assets.py`
- Modify: `tests/test_norway_brreg_assets.py`

- [ ] **Step 1: Add failing financial statement tests**

Append:

```python
def _financial_record(**overrides: Any) -> dict[str, Any]:
    record = {
        "id": 5667197,
        "journalnr": "2025428073",
        "regnskapstype": "SELSKAP",
        "virksomhet": {"organisasjonsnummer": "923609016", "organisasjonsform": "ASA"},
        "regnskapsperiode": {"fraDato": "2024-01-01", "tilDato": "2024-12-31"},
        "valuta": "USD",
        "oppstillingsplan": "store",
        "revisjon": {"ikkeRevidertAarsregnskap": False, "fravalgRevisjon": False},
        "regnkapsprinsipper": {"smaaForetak": False, "regnskapsregler": "forenkletAnvendelseIFRS"},
        "egenkapitalGjeld": {
            "egenkapital": {"sumEgenkapital": 41090000000.00},
            "gjeldOversikt": {
                "sumGjeld": 68060000000.00,
                "kortsiktigGjeld": {"sumKortsiktigGjeld": 42024000000.00},
                "langsiktigGjeld": {"sumLangsiktigGjeld": 26036000000.00},
            },
        },
        "eiendeler": {
            "sumEiendeler": 109150000000.00,
            "omloepsmidler": {"sumOmloepsmidler": 45079000000.00},
            "anleggsmidler": {"sumAnleggsmidler": 64071000000.00},
        },
        "resultatregnskapResultat": {
            "ordinaertResultatFoerSkattekostnad": 8168000000.00,
            "aarsresultat": 8141000000.00,
            "finansresultat": {"nettoFinans": -2179000000.00},
            "driftsresultat": {
                "driftsresultat": 10347000000.00,
                "driftsinntekter": {"sumDriftsinntekter": 72543000000.00},
            },
        },
    }
    record.update(overrides)
    return record


def test_financial_statement_rows_extract_metrics_and_preserve_currency() -> None:
    rows = brreg_assets.build_financial_statement_rows(
        [_financial_record()],
        source_org_number="923609016",
        run_id="test-run",
    )

    assert rows[0]["org_number"] == "923609016"
    assert rows[0]["accounts_type"] == "SELSKAP"
    assert rows[0]["period_start"] == "2024-01-01"
    assert rows[0]["period_end"] == "2024-12-31"
    assert rows[0]["original_currency"] == "USD"
    assert rows[0]["revenue_original"] == "72543000000.0"
    assert rows[0]["operating_result_original"] == "10347000000.0"
    assert rows[0]["net_result_original"] == "8141000000.0"
    assert rows[0]["total_assets_original"] == "109150000000.0"
    assert rows[0]["equity_original"] == "41090000000.0"
    assert rows[0]["total_debt_original"] == "68060000000.0"
    assert json.loads(rows[0]["raw_financial_statement"])["id"] == 5667197


def test_financial_source_fetches_each_candidate_org_number() -> None:
    payloads = {
        "https://data.brreg.no/regnskapsregisteret/regnskap/923609016": [_financial_record()],
        "https://data.brreg.no/regnskapsregisteret/regnskap/111111111": [],
    }
    session = FakeHttpSession(payloads=payloads)

    source = brreg_assets.norway_brreg_financial_statements_source(
        org_numbers=["923609016", "111111111"],
        session=session,
        run_id="test-run",
        request_delay_seconds=0,
    )
    rows = list(source.resources[brreg_assets.FINANCIAL_STATEMENTS_TABLE])

    assert [row["org_number"] for row in rows] == ["923609016"]
    assert [call[0] for call in session.calls] == list(payloads.keys())


def test_financial_statement_dlt_pipeline_loads_table(tmp_path: Path) -> None:
    payloads = {
        "https://data.brreg.no/regnskapsregisteret/regnskap/923609016": [_financial_record()],
    }
    session = FakeHttpSession(payloads=payloads)

    load_info = brreg_assets.run_norway_brreg_financial_statements_dlt_pipeline(
        database_path=tmp_path / "norway.duckdb",
        org_numbers=["923609016"],
        run_id="test-run",
        session=session,
        request_delay_seconds=0,
    )

    assert load_info
    with duckdb.connect(str(tmp_path / "norway.duckdb"), read_only=True) as connection:
        rows = connection.execute(
            """
            select org_number, period_end, accounts_type, original_currency, revenue_original
            from norway_brreg.financial_statements
            """
        ).fetchall()

    assert rows == [("923609016", "2024-12-31", "SELSKAP", "USD", "72543000000.0")]
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py -v
```

Expected: FAIL because financial source, row builder, and dlt runner are absent.

- [ ] **Step 3: Add financial source and dlt runner**

Append:

```python
@dlt.source(name="norway_brreg_financial_statements")
def norway_brreg_financial_statements_source(
    *,
    org_numbers: list[str],
    base_url: str = REGNSKAP_BASE_URL,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    user_agent: str = DEFAULT_USER_AGENT,
    run_id: str = "",
    request_delay_seconds: float = 1.0,
    session: HttpSession | None = None,
    sleep: Any = time.sleep,
) -> DltResource:
    return _financial_statements_resource(
        org_numbers=org_numbers,
        base_url=base_url,
        timeout_seconds=timeout_seconds,
        user_agent=user_agent,
        run_id=run_id,
        request_delay_seconds=request_delay_seconds,
        session=session,
        sleep=sleep,
    )


@dlt.resource(
    name=FINANCIAL_STATEMENTS_TABLE,
    write_disposition="replace",
    primary_key=("org_number", "period_end", "accounts_type"),
)
def _financial_statements_resource(
    *,
    org_numbers: list[str],
    base_url: str,
    timeout_seconds: int,
    user_agent: str,
    run_id: str,
    request_delay_seconds: float,
    session: HttpSession | None,
    sleep: Any,
) -> Iterator[dict[str, Any]]:
    for index, org_number in enumerate(org_numbers):
        payload = _download_json(
            url=f"{base_url}/{org_number}",
            timeout_seconds=timeout_seconds,
            user_agent=user_agent,
            session=session,
        )
        if isinstance(payload, list):
            yield from build_financial_statement_rows(
                payload,
                source_org_number=org_number,
                run_id=run_id,
            )
        if request_delay_seconds > 0 and index < len(org_numbers) - 1:
            sleep(request_delay_seconds)


def run_norway_brreg_financial_statements_dlt_pipeline(
    *,
    database_path: str | Path,
    org_numbers: list[str],
    run_id: str,
    session: HttpSession | None = None,
    request_delay_seconds: float = 1.0,
) -> Any:
    return norway_brreg_pipeline(
        database_path,
        pipeline_name="norway_brreg_financial_statements",
    ).run(
        norway_brreg_financial_statements_source(
            org_numbers=org_numbers,
            run_id=run_id,
            session=session,
            request_delay_seconds=request_delay_seconds,
        )
    )
```

Add `import time` at the top.

Add row builder:

```python
def build_financial_statement_rows(
    records: list[dict[str, Any]],
    *,
    source_org_number: str,
    run_id: str,
) -> list[dict[str, Any]]:
    return [
        _financial_statement_row(record, source_org_number=source_org_number, run_id=run_id)
        for record in records
        if isinstance(record, dict)
    ]


def _financial_statement_row(
    record: dict[str, Any],
    *,
    source_org_number: str,
    run_id: str,
) -> dict[str, Any]:
    company = _dict(record.get("virksomhet"))
    period = _dict(record.get("regnskapsperiode"))
    income = _dict(record.get("resultatregnskapResultat"))
    operating = _dict(income.get("driftsresultat"))
    financial = _dict(income.get("finansresultat"))
    assets = _dict(record.get("eiendeler"))
    current_assets = _dict(assets.get("omloepsmidler"))
    fixed_assets = _dict(assets.get("anleggsmidler"))
    equity_debt = _dict(record.get("egenkapitalGjeld"))
    equity = _dict(equity_debt.get("egenkapital"))
    debt = _dict(equity_debt.get("gjeldOversikt"))
    current_debt = _dict(debt.get("kortsiktigGjeld"))
    long_debt = _dict(debt.get("langsiktigGjeld"))
    accounts_type = _string(record.get("regnskapstype"))
    period_end = _string(period.get("tilDato"))
    org_number = _string(company.get("organisasjonsnummer")) or source_org_number
    return {
        "country_iso2": COUNTRY,
        "source_slug": FINANCIAL_SOURCE_SLUG,
        "source_run_id": run_id,
        "source_record_id": f"{org_number}:{period_end}:{accounts_type}",
        "source_payload_hash": source_payload_hash(record),
        "org_number": org_number,
        "journal_number": _string(record.get("journalnr")),
        "accounts_type": accounts_type,
        "period_start": _string(period.get("fraDato")),
        "period_end": period_end,
        "original_currency": _string(record.get("valuta")),
        "layout": _string(record.get("oppstillingsplan")),
        "revenue_original": _decimal_string(_dict(operating.get("driftsinntekter")).get("sumDriftsinntekter")),
        "operating_result_original": _decimal_string(operating.get("driftsresultat")),
        "net_financial_items_original": _decimal_string(financial.get("nettoFinans")),
        "pre_tax_result_original": _decimal_string(income.get("ordinaertResultatFoerSkattekostnad")),
        "net_result_original": _decimal_string(income.get("aarsresultat")),
        "total_assets_original": _decimal_string(assets.get("sumEiendeler")),
        "current_assets_original": _decimal_string(current_assets.get("sumOmloepsmidler")),
        "fixed_assets_original": _decimal_string(fixed_assets.get("sumAnleggsmidler")),
        "equity_original": _decimal_string(equity.get("sumEgenkapital")),
        "total_debt_original": _decimal_string(debt.get("sumGjeld")),
        "current_debt_original": _decimal_string(current_debt.get("sumKortsiktigGjeld")),
        "long_term_debt_original": _decimal_string(long_debt.get("sumLangsiktigGjeld")),
        "raw_financial_statement": _raw_json(record),
    }


def _decimal_string(value: Any) -> str:
    return "" if value is None else str(value)
```

Add JSON downloader:

```python
def _download_json(
    *,
    url: str,
    timeout_seconds: int,
    user_agent: str,
    session: HttpSession | None,
) -> Any:
    http_session = session or requests.Session()
    http_session.headers["User-Agent"] = user_agent
    response = http_session.get(url, timeout=timeout_seconds)
    response.raise_for_status()
    return response.json()
```

- [ ] **Step 4: Run focused tests**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/norway_brreg/assets.py tests/test_norway_brreg_assets.py
git commit -m "Add Norway financial statement loader"
```

---

## Task 6A: Add Norway USD Metrics Using Shared Exchange Rates

Execution order: complete Task 6 before this task, because the metrics asset depends on `norway_brreg_financial_statements_duckdb` and the shared ClickHouse-backed `exchange_rates` asset from Task 0.

**Files:**
- Modify: `src/dagster_v3/defs/norway_brreg/assets.py`
- Modify: `tests/test_norway_brreg_assets.py`

- [ ] **Step 1: Add failing FX conversion tests**

Append:

```python
from decimal import Decimal


def test_build_financial_metric_rows_keeps_original_and_usd_amounts() -> None:
    rows = brreg_assets.build_financial_metric_rows(
        [
            {
                "org_number": "923609016",
                "period_end": "2024-12-31",
                "accounts_type": "SELSKAP",
                "original_currency": "NOK",
                "revenue_original": "113573",
                "operating_result_original": "1000",
                "net_result_original": "",
                "total_assets_original": "227146",
                "equity_original": "500",
                "total_debt_original": "100",
            }
        ],
        nok_per_currency={
            ("2024-12-31", "USD"): Decimal("11.3573"),
            ("2024-12-31", "NOK"): Decimal("1"),
        },
    )

    assert rows[0]["original_currency"] == "NOK"
    assert rows[0]["fx_rate_date"] == "2024-12-31"
    assert rows[0]["fx_source"] == "Norges Bank EXR"
    assert rows[0]["revenue_original"] == "113573"
    assert rows[0]["revenue_usd"] == "10000.00"
    assert rows[0]["operating_result_usd"] == "88.05"
    assert rows[0]["net_result_usd"] == ""
    assert rows[0]["total_assets_usd"] == "20000.00"


def test_metric_asset_depends_on_financial_statements_and_shared_exchange_rates() -> None:
    asset_graph = load_project_defs().resolve_asset_graph()

    assert asset_graph.get(
        brreg_assets.dg.AssetKey("norway_brreg_financial_statement_metrics")
    ).parent_keys == {
        brreg_assets.dg.AssetKey("norway_brreg_financial_statements_duckdb"),
        brreg_assets.dg.AssetKey("exchange_rates"),
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py::test_build_financial_metric_rows_keeps_original_and_usd_amounts tests/test_norway_brreg_assets.py::test_metric_asset_depends_on_financial_statements_and_shared_exchange_rates -v
```

Expected: FAIL because Norway metric helpers and the derived metrics asset are absent.

- [ ] **Step 3: Add imports and metrics constants**

Add near the top of `assets.py`:

```python
from decimal import Decimal, ROUND_HALF_UP

import polars as pl
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.exchange_rates.clickhouse import load_nok_per_currency
from dagster_v3.defs.exchange_rates.source import usd_rate_for_currency
```

Add constants:

```python
FX_SOURCE_NAME = "Norges Bank EXR"
FINANCIAL_METRICS_TABLE = "financial_statement_metrics"
FINANCIAL_AMOUNT_FIELDS = (
    "revenue",
    "operating_result",
    "net_financial_items",
    "pre_tax_result",
    "net_result",
    "total_assets",
    "current_assets",
    "fixed_assets",
    "equity",
    "total_debt",
    "current_debt",
    "long_term_debt",
)
```

- [ ] **Step 4: Add Norway metric row helpers**

Append:

```python
def build_financial_metric_rows(
    financial_rows: list[dict[str, Any]],
    *,
    nok_per_currency: dict[tuple[str, str], Decimal],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for statement in financial_rows:
        currency = _string(statement.get("original_currency")).upper()
        rate_date = _string(statement.get("period_end"))
        usd_rate = usd_rate_for_currency(
            currency=currency,
            rate_date=rate_date,
            nok_per_currency=nok_per_currency,
        )
        row = {
            "org_number": _string(statement.get("org_number")),
            "period_end": rate_date,
            "accounts_type": _string(statement.get("accounts_type")),
            "original_currency": currency,
            "fx_rate_date": rate_date,
            "fx_source": FX_SOURCE_NAME,
            "fx_rate_to_usd": str(usd_rate),
        }
        for field_name in FINANCIAL_AMOUNT_FIELDS:
            original_value = _string(statement.get(f"{field_name}_original"))
            row[f"{field_name}_original"] = original_value
            row[f"{field_name}_usd"] = _amount_to_usd(original_value, usd_rate)
        rows.append(row)
    return rows


def _amount_to_usd(value: str, usd_rate: Decimal) -> str:
    if not value:
        return ""
    amount = Decimal(value) * usd_rate
    return str(amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
```

- [ ] **Step 5: Add metrics asset and shared-rate lookup**

Append:

```python
@dg.asset(
    name="norway_brreg_financial_statement_metrics",
    group_name=GROUP_NAME,
    deps=["norway_brreg_financial_statements_duckdb", "exchange_rates"],
    kinds={"python", "duckdb", "fx"},
)
def norway_brreg_financial_statement_metrics(
    norway_duckdb: NorwayDuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    """Financial statement metrics with original-currency amounts and USD conversions."""
    return build_financial_statement_metrics_table(
        norway_duckdb=norway_duckdb,
        clickhouse=clickhouse,
    )


def build_financial_statement_metrics_table(
    *,
    norway_duckdb: NorwayDuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    with norway_duckdb.connect() as connection:
        financial_rows = connection.execute(
            "select * from norway_brreg.financial_statements"
        ).fetchall()
        financial_columns = [column[0] for column in connection.description]
        financial_dicts = [dict(zip(financial_columns, row, strict=True)) for row in financial_rows]
        rate_dates = sorted({_string(row.get("period_end")) for row in financial_dicts})
        currencies = sorted({_string(row.get("original_currency")).upper() for row in financial_dicts} | {"USD", "NOK"})
        nok_per_currency = load_nok_per_currency(
            clickhouse,
            rate_dates=rate_dates,
            currencies=currencies,
        )
        metric_rows = build_financial_metric_rows(
            financial_dicts,
            nok_per_currency=nok_per_currency,
        )
        connection.register("metric_rows", pl.DataFrame(metric_rows))
        connection.execute("drop table if exists norway_brreg.financial_statement_metrics")
        connection.execute(
            "create table norway_brreg.financial_statement_metrics as select * from metric_rows"
        )
        connection.unregister("metric_rows")
    return dg.MaterializeResult(metadata={"financial_metric_rows": len(metric_rows)})
```

Update `defs` assets list to include:

```python
norway_brreg_financial_statement_metrics,
```

- [ ] **Step 6: Keep Norway definitions resource-local**

Update Norway `defs` assets list, but do not add another `clickhouse` resource here. The separate exchange-rate section provides the shared top-level `clickhouse` resource when project definitions are merged.

```python
defs = dg.Definitions(
    assets=[
        norway_brreg_entities_duckdb_asset,
        norway_brreg_sub_entities_duckdb_asset,
        norway_brreg_company_text_translations,
        norway_brreg_financial_candidates,
        norway_brreg_financial_statements_duckdb,
        norway_brreg_financial_statement_metrics,
    ],
    resources={
        "norway_duckdb": NorwayDuckDBResource(),
    },
)
```

- [ ] **Step 7: Run focused tests**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py -v
uv run dg list defs --json
```

Expected: tests pass and `dg` output includes `exchange_rates` and `norway_brreg_financial_statement_metrics`.

- [ ] **Step 8: Commit**

```bash
git add src/dagster_v3/defs/norway_brreg/assets.py tests/test_norway_brreg_assets.py
git commit -m "Add Norway USD financial metrics"
```

---

## Task 6: Wire Financial Statement Dagster Asset

**Files:**
- Modify: `src/dagster_v3/defs/norway_brreg/assets.py`
- Modify: `tests/test_norway_brreg_assets.py`

- [ ] **Step 1: Add failing asset wiring tests**

Append:

```python
def test_financial_statement_asset_is_registered_downstream_of_candidates() -> None:
    asset_graph = load_project_defs().resolve_asset_graph()

    assert "norway_brreg_financial_statements_duckdb" in {
        key.path[-1] for key in asset_graph.get_all_asset_keys()
    }
    assert asset_graph.get(
        brreg_assets.dg.AssetKey("norway_brreg_financial_statements_duckdb")
    ).parent_keys == {brreg_assets.dg.AssetKey("norway_brreg_financial_candidates")}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py::test_financial_statement_asset_is_registered_downstream_of_candidates -v
```

Expected: FAIL because the financial statement Dagster asset is not registered.

- [ ] **Step 3: Add config, org-number loader, and asset**

Append:

```python
class NorwayFinancialStatementsConfig(dg.Config):
    max_orgs: int | None = 100
    request_delay_seconds: float = 1.0


@dg.asset(
    name="norway_brreg_financial_statements_duckdb",
    group_name=GROUP_NAME,
    deps=["norway_brreg_financial_candidates"],
    kinds={"python", "dlt", "duckdb"},
)
def norway_brreg_financial_statements_duckdb(
    context: dg.AssetExecutionContext,
    config: NorwayFinancialStatementsConfig,
    norway_duckdb: NorwayDuckDBResource,
) -> dg.MaterializeResult:
    """Load Regnskapsregisteret financial statements for selected Brreg entities."""
    org_numbers = load_financial_candidate_org_numbers(
        norway_duckdb,
        max_orgs=config.max_orgs,
    )
    run_norway_brreg_financial_statements_dlt_pipeline(
        database_path=norway_duckdb.path(),
        org_numbers=org_numbers,
        run_id=context.run_id,
        request_delay_seconds=config.request_delay_seconds,
    )
    return dg.MaterializeResult(metadata={"org_numbers_count": len(org_numbers)})


def load_financial_candidate_org_numbers(
    norway_duckdb: NorwayDuckDBResource,
    *,
    max_orgs: int | None,
) -> list[str]:
    limit_clause = "" if max_orgs is None else f"limit {int(max_orgs)}"
    with norway_duckdb.connect(read_only=True) as connection:
        return [
            row[0]
            for row in connection.execute(
                f"""
                select org_number
                from norway_brreg.financial_candidates
                order by org_number
                {limit_clause}
                """
            ).fetchall()
        ]
```

Update `defs` assets list:

```python
defs = dg.Definitions(
    assets=[
        norway_brreg_entities_duckdb_asset,
        norway_brreg_sub_entities_duckdb_asset,
        norway_brreg_financial_candidates,
        norway_brreg_financial_statements_duckdb,
    ],
    resources={"norway_duckdb": NorwayDuckDBResource()},
)
```

- [ ] **Step 4: Run focused tests and definitions listing**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py -v
uv run dg list defs --json
```

Expected: tests pass and `dg` output includes `norway_brreg_financial_statements_duckdb` in group `norway_brreg`.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/norway_brreg/assets.py tests/test_norway_brreg_assets.py
git commit -m "Wire Norway financial statement asset"
```

---

## Task 7: Update Documentation and Verify Full Project

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update README**

Add a Norway section:

```markdown
## Norway Brreg Assets

`dagster_v3.defs.norway_brreg` loads official Norway company data from
Bronnoysundregistrene under NLOD 2.0.

Assets:

- `norway_brreg_entities_duckdb`: Brreg Enhetsregisteret `/enheter/lastned` JSON gzip bulk file loaded to `norway_brreg.entities`.
- `norway_brreg_sub_entities_duckdb`: Brreg Enhetsregisteret `/underenheter/lastned` JSON gzip bulk file loaded to `norway_brreg.sub_entities`.
- `norway_brreg_company_text_translations`: original Norwegian descriptive fields and reviewed English translations in `norway_brreg.company_text_translations`.
- `norway_brreg_financial_candidates`: active AS/ASA/NUF entities with `sisteInnsendteAarsregnskap`.
- `norway_brreg_financial_statements_duckdb`: Regnskapsregisteret `/regnskap/{orgnr}` JSON data loaded to `norway_brreg.financial_statements` with original-currency amount columns.
- `exchange_rates`: shared ClickHouse exchange-rate reference asset, stored in `reference.exchange_rates`, used by Norway and future sources.
- `norway_brreg_financial_statement_metrics`: original-currency amounts plus USD conversions in `norway_brreg.financial_statement_metrics`, using `reference.exchange_rates`.

Attribution: `Kilde: Bronnoysundregistrene (NLOD 2.0)`.

The Brreg roles endpoint is intentionally excluded from this first implementation because it
contains person names and birth dates. Add it only after a retention and minimization policy is
defined.
```

- [ ] **Step 2: Run full verification**

Run:

```bash
uv run pytest -v
uv run dg list defs --json
git -C /Users/graovic/pulsarpoint/ppoint/companycollect diff --check
```

Expected:

- pytest passes.
- `dg list defs --json` includes:
  - `norway_brreg_entities_duckdb`
  - `norway_brreg_sub_entities_duckdb`
  - `norway_brreg_company_text_translations`
  - `norway_brreg_financial_candidates`
  - `norway_brreg_financial_statements_duckdb`
  - `exchange_rates`
  - `norway_brreg_financial_statement_metrics`
- diff check reports no whitespace errors.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "Document Norway Brreg Dagster assets"
```

---

## Execution Notes

- Do not add schedules in this pass. Materialize manually until the bulk loads and per-org financial fetch behavior are proven.
- Do not ingest `brregroller` in this pass. It includes personal data and should be handled in a separate plan with explicit minimization and retention rules.
- Do not add ClickHouse normalized country-company tables in this pass. The current v3 Finland source loads source-shaped DuckDB tables first; keep Norway consistent.
- Keep all source errors wrapped at the boundary of helper functions where useful, but let Dagster asset boundaries log failures once through Dagster. Avoid logging payload bodies.
