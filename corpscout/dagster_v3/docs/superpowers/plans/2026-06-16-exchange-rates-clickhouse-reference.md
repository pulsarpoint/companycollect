# Exchange Rates ClickHouse Reference Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a separate Dagster exchange-rate section that fetches online exchange rates and stores them in ClickHouse.

**Architecture:** Create `src/dagster_v3/defs/exchange_rates` as a shared reference package, parallel to the existing `nace` reference package. The asset uses dlt's REST API source to fetch ECB euro foreign exchange reference rates, normalizes each response into a stable row shape, and loads those rows into `reference.exchange_rates` in ClickHouse. Downstream query APIs and USD conversion helpers are intentionally deferred until this sync path is working and verifiable.

**Tech Stack:** Dagster 1.13, dagster-dlt, dlt REST API source, dlt ClickHouse destination, `dagster_clickhouse.ClickhouseResource`, ClickHouse `MergeTree`, ECB Data Portal EXR API, Python `decimal.Decimal`, pytest, dg CLI.

---

## File Structure

- Create `src/dagster_v3/defs/exchange_rates/__init__.py`: package marker.
- Create `src/dagster_v3/defs/exchange_rates/tables.py`: database/table names, columns, and ClickHouse DDL.
- Create `src/dagster_v3/defs/exchange_rates/clickhouse.py`: schema preparation helper using `dagster_clickhouse.ClickhouseResource`.
- Create `src/dagster_v3/defs/exchange_rates/source.py`: dlt REST API configuration, ECB SDMX JSON payload parser, EUR identity resource, and ClickHouse pipeline factory.
- Create `src/dagster_v3/defs/exchange_rates/assets.py`: Dagster dlt asset, translator, ClickHouse resource wiring, and `defs`.
- Create `tests/test_exchange_rates_assets.py`: payload parser, REST API config, ClickHouse DDL, dlt source, asset graph, and resource tests.
- Modify `README.md`: document the exchange-rate reference sync section.

## Table Contract

ClickHouse table:

```text
reference.exchange_rates
```

Columns:

```text
rate_date
base_currency
quote_currency
rate
source
source_url
source_payload_hash
source_run_id
pulled_at
_dlt_load_id
_dlt_id
```

Canonical shape:

- Store rates as `base_currency -> quote_currency`, where `rate` means one unit of `base_currency` equals `rate` units of `quote_currency`.
- Use `base_currency = "EUR"` for ECB rows because ECB series such as `D.USD.EUR.SP00.A` represent the USD value of one EUR.
- Insert an identity row for `EUR -> EUR`.
- Do not expose a downstream conversion API in this milestone. The only deliverable is online source sync into ClickHouse.

## Deferred Work

After `reference.exchange_rates` is materialized reliably, country pipelines can use
the plain `dagster_v3.exchange_rates` client package for lookup and conversion.
Create a separate plan for country-specific financial metric wiring:

- Financial metric schemas that retain original currency amount, converted USD amount, rate date, source, and rate used.

## Task 1: Define ClickHouse Table Contract

**Files:**
- Create: `src/dagster_v3/defs/exchange_rates/__init__.py`
- Create: `src/dagster_v3/defs/exchange_rates/tables.py`
- Test: `tests/test_exchange_rates_assets.py`

- [ ] **Step 1: Write failing table contract tests**

Create `tests/test_exchange_rates_assets.py`:

```python
import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.definitions import defs as load_project_defs
from dagster_v3.defs.exchange_rates import tables


def test_exchange_rates_clickhouse_schema_contract() -> None:
    assert tables.EXCHANGE_RATES_DATABASE == "reference"
    assert tables.EXCHANGE_RATES_TABLE == "exchange_rates"
    assert tables.QUALIFIED_EXCHANGE_RATES_TABLE == "reference.exchange_rates"
    assert tables.EXCHANGE_RATES_COLUMNS == (
        "rate_date",
        "base_currency",
        "quote_currency",
        "rate",
        "source",
        "source_url",
        "source_payload_hash",
        "source_run_id",
        "pulled_at",
        "_dlt_load_id",
        "_dlt_id",
    )
    assert "CREATE TABLE IF NOT EXISTS reference.exchange_rates" in tables.EXCHANGE_RATES_DDL
    assert "ORDER BY (quote_currency, base_currency, rate_date, source)" in tables.EXCHANGE_RATES_DDL
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
uv run pytest tests/test_exchange_rates_assets.py::test_exchange_rates_clickhouse_schema_contract -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'dagster_v3.defs.exchange_rates'`.

- [ ] **Step 3: Create package and table constants**

Create `src/dagster_v3/defs/exchange_rates/__init__.py`:

```python
"""Shared exchange-rate reference assets."""
```

Create `src/dagster_v3/defs/exchange_rates/tables.py`:

```python
EXCHANGE_RATES_DATABASE = "reference"
EXCHANGE_RATES_TABLE = "exchange_rates"
QUALIFIED_EXCHANGE_RATES_TABLE = f"{EXCHANGE_RATES_DATABASE}.{EXCHANGE_RATES_TABLE}"

EXCHANGE_RATES_COLUMNS = (
    "rate_date",
    "base_currency",
    "quote_currency",
    "rate",
    "source",
    "source_url",
    "source_payload_hash",
    "source_run_id",
    "pulled_at",
    "_dlt_load_id",
    "_dlt_id",
)

EXCHANGE_RATES_DDL = f"""
CREATE TABLE IF NOT EXISTS {QUALIFIED_EXCHANGE_RATES_TABLE}
(
    rate_date Date,
    base_currency LowCardinality(String),
    quote_currency LowCardinality(String),
    rate Decimal(38, 12),
    source LowCardinality(String),
    source_url String,
    source_payload_hash FixedString(64),
    source_run_id String,
    pulled_at DateTime64(3, 'UTC'),
    _dlt_load_id String,
    _dlt_id String
)
ENGINE = MergeTree
ORDER BY (quote_currency, base_currency, rate_date, source)
"""
```

- [ ] **Step 4: Run focused test**

Run:

```bash
uv run pytest tests/test_exchange_rates_assets.py::test_exchange_rates_clickhouse_schema_contract -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/exchange_rates tests/test_exchange_rates_assets.py
git commit -m "Add exchange rate ClickHouse table contract"
```

## Task 2: Add ClickHouse Preparation Helper

**Files:**
- Create: `src/dagster_v3/defs/exchange_rates/clickhouse.py`
- Modify: `tests/test_exchange_rates_assets.py`

- [ ] **Step 1: Add failing ClickHouse helper tests**

Append:

```python
from collections.abc import Iterator
from contextlib import contextmanager
from typing import get_type_hints

from dagster_v3.defs.exchange_rates.clickhouse import prepare_exchange_rates_table


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, sql: str) -> None:
        self.statements.append(sql)


def test_prepare_exchange_rates_table_is_typed_for_official_resource() -> None:
    annotations = get_type_hints(prepare_exchange_rates_table)

    assert annotations["clickhouse"] is ClickhouseResource


def test_prepare_exchange_rates_table_uses_reference_database(monkeypatch) -> None:
    resource = ClickhouseResource(host="localhost")
    client = FakeClickHouseClient()

    @contextmanager
    def fake_get_connection(self: ClickhouseResource) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    prepare_exchange_rates_table(resource)

    assert client.statements == [
        "CREATE DATABASE IF NOT EXISTS reference",
        tables.EXCHANGE_RATES_DDL.strip(),
        "TRUNCATE TABLE reference.exchange_rates",
    ]

```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/test_exchange_rates_assets.py::test_prepare_exchange_rates_table_is_typed_for_official_resource tests/test_exchange_rates_assets.py::test_prepare_exchange_rates_table_uses_reference_database -v
```

Expected: FAIL because `clickhouse.py` does not exist.

- [ ] **Step 3: Create helper**

Create `src/dagster_v3/defs/exchange_rates/clickhouse.py`:

```python
from __future__ import annotations

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.exchange_rates import tables


def prepare_exchange_rates_table(clickhouse: ClickhouseResource) -> None:
    with clickhouse.get_connection() as client:
        client.execute(f"CREATE DATABASE IF NOT EXISTS {tables.EXCHANGE_RATES_DATABASE}")
        client.execute(tables.EXCHANGE_RATES_DDL.strip())
        client.execute(f"TRUNCATE TABLE {tables.QUALIFIED_EXCHANGE_RATES_TABLE}")
```

- [ ] **Step 4: Run focused tests**

Run:

```bash
uv run pytest tests/test_exchange_rates_assets.py::test_prepare_exchange_rates_table_is_typed_for_official_resource tests/test_exchange_rates_assets.py::test_prepare_exchange_rates_table_uses_reference_database -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/exchange_rates/clickhouse.py tests/test_exchange_rates_assets.py
git commit -m "Add exchange rate ClickHouse preparation"
```

## Task 3: Add Online Exchange Rate Source with dlt REST API

**Files:**
- Create: `src/dagster_v3/defs/exchange_rates/source.py`
- Modify: `tests/test_exchange_rates_assets.py`

- [ ] **Step 1: Add failing source sync tests**

Append:

```python
from dagster_v3.defs.exchange_rates import source as fx_source


def _ecb_payload(value: float = 1.0389) -> dict:
    return {
        "dataSets": [
            {
                "series": {
                    "0:0:0:0:0": {
                        "observations": {
                            "0": [value, 0, 0, None, None],
                        }
                    }
                }
            }
        ]
    }


def test_exchange_rate_rest_api_config_models_ecb_endpoint() -> None:
    config = fx_source.exchange_rate_rest_api_config(
        rate_dates=["2024-12-31"],
        currencies=["USD"],
        source_run_id="run-1",
        pulled_at="2026-06-16T00:00:00.000Z",
    )

    resources = {resource["name"]: resource for resource in config["resources"]}
    usd_resource = resources["exchange_rates_usd_2024_12_31"]

    assert config["client"]["base_url"] == "https://data-api.ecb.europa.eu/service/data/EXR/"
    assert config["client"]["headers"] == {"User-Agent": "corpscout-dagster-v3-dev/0.1"}
    assert usd_resource["table_name"] == "exchange_rates"
    assert usd_resource["endpoint"] == {
        "path": "D.USD.EUR.SP00.A",
        "params": {
            "format": "jsondata",
            "startPeriod": "2024-12-31",
            "endPeriod": "2024-12-31",
        },
        "paginator": "single_page",
    }
    assert usd_resource["processing_steps"]


def test_ecb_rate_row_from_payload_returns_reference_row() -> None:
    row = fx_source.ecb_rate_row_from_payload(
        _ecb_payload(),
        quote_currency="USD",
        rate_date="2024-12-31",
        source_url="https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A",
        source_run_id="run-1",
        pulled_at="2026-06-16T00:00:00.000Z",
    )

    assert row["rate_date"] == "2024-12-31"
    assert row["base_currency"] == "EUR"
    assert row["quote_currency"] == "USD"
    assert row["rate"] == "1.0389"
    assert row["source"] == "ECB EXR"
    assert len(row["source_payload_hash"]) == 64


def test_identity_exchange_rates_resource_yields_eur_rows() -> None:
    resource = fx_source.identity_exchange_rates_resource(
        rate_dates=["2024-12-31"],
        source_run_id="run-1",
        pulled_at="2026-06-16T00:00:00.000Z",
    )

    assert list(resource) == [
        {
            "rate_date": "2024-12-31",
            "base_currency": "EUR",
            "quote_currency": "EUR",
            "rate": "1",
            "source": "identity",
            "source_url": "",
            "source_payload_hash": "0" * 64,
            "source_run_id": "run-1",
            "pulled_at": "2026-06-16T00:00:00.000Z",
        }
    ]
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/test_exchange_rates_assets.py::test_exchange_rate_rest_api_config_models_ecb_endpoint tests/test_exchange_rates_assets.py::test_ecb_rate_row_from_payload_returns_reference_row tests/test_exchange_rates_assets.py::test_identity_exchange_rates_resource_yields_eur_rows -v
```

Expected: FAIL because `source.py` does not exist.

- [ ] **Step 3: Create source module**

Create `src/dagster_v3/defs/exchange_rates/source.py`:

```python
from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import dlt
from dlt.extract.resource import DltResource
from dlt.pipeline.pipeline import Pipeline
from dlt.sources.rest_api import rest_api_source
from dlt.sources.rest_api.typing import RESTAPIConfig

from dagster_v3.defs.exchange_rates import tables

ECB_EXR_BASE_URL = "https://data-api.ecb.europa.eu/service/data/EXR"
EXCHANGE_RATES_DLT_PIPELINE_NAME = "reference_exchange_rates"
EXCHANGE_RATES_DLT_TABLE = "exchange_rates"
EXCHANGE_RATES_DLT_DATASET_NAME: str | None = None
FX_SOURCE_NAME = "ECB EXR"
DEFAULT_CLICKHOUSE_NATIVE_PORT = 9002
DEFAULT_CLICKHOUSE_HTTP_PORT = 8123
DEFAULT_USER_AGENT = "corpscout-dagster-v3-dev/0.1"
```

Append dlt REST API source configuration:

```python
@dlt.source(name="exchange_rates")
def exchange_rates_source(
    *,
    rate_dates: list[str],
    currencies: list[str],
    source_run_id: str = "",
    pulled_at: str | None = None,
) -> list[DltResource]:
    effective_pulled_at = pulled_at or _utc_now_iso()
    ecb_source = rest_api_source(
        config=exchange_rate_rest_api_config(
            rate_dates=rate_dates,
            currencies=currencies,
            source_run_id=source_run_id,
            pulled_at=effective_pulled_at,
        ),
        name="exchange_rates_ecb",
    )
    return [
        *ecb_source.resources.values(),
        identity_exchange_rates_resource(
            rate_dates=rate_dates,
            source_run_id=source_run_id,
            pulled_at=effective_pulled_at,
        ),
    ]


def exchange_rate_rest_api_config(
    *,
    rate_dates: list[str],
    currencies: list[str],
    source_run_id: str,
    pulled_at: str,
    user_agent: str = DEFAULT_USER_AGENT,
) -> RESTAPIConfig:
    resources: list[Any] = []
    for rate_date in sorted(set(rate_dates)):
        for currency in sorted({currency.upper() for currency in currencies} | {"USD", "NOK"}):
            if currency == "EUR":
                continue
            resource_name = f"exchange_rates_{currency.lower()}_{rate_date.replace('-', '_')}"
            source_url = f"{ECB_EXR_BASE_URL}/D.{currency}.EUR.SP00.A"
            resources.append(
                {
                    "name": resource_name,
                    "table_name": EXCHANGE_RATES_DLT_TABLE,
                    "write_disposition": "append",
                    "primary_key": ["rate_date", "base_currency", "quote_currency", "source"],
                    "endpoint": {
                        "path": f"D.{currency}.EUR.SP00.A",
                        "params": {
                            "format": "jsondata",
                            "startPeriod": rate_date,
                            "endPeriod": rate_date,
                        },
                        "paginator": "single_page",
                    },
                    "processing_steps": [
                        {
                            "yield_map": _ecb_mapper(
                                quote_currency=currency,
                                rate_date=rate_date,
                                source_url=source_url,
                                source_run_id=source_run_id,
                                pulled_at=pulled_at,
                            )
                        }
                    ],
                }
            )
    return {
        "client": {
            "base_url": f"{ECB_EXR_BASE_URL}/",
            "headers": {"User-Agent": user_agent},
        },
        "resources": resources,
    }
```

Append row builders:

```python
def identity_exchange_rates_resource(
    *,
    rate_dates: list[str],
    source_run_id: str,
    pulled_at: str,
) -> DltResource:
    return dlt.resource(
        [
            identity_eur_row(
                rate_date=rate_date,
                source_run_id=source_run_id,
                pulled_at=pulled_at,
            )
            for rate_date in sorted(set(rate_dates))
        ],
        name="exchange_rates_identity",
        table_name=EXCHANGE_RATES_DLT_TABLE,
        write_disposition="append",
        primary_key=["rate_date", "base_currency", "quote_currency", "source"],
    )


def _ecb_mapper(
    *,
    quote_currency: str,
    rate_date: str,
    source_url: str,
    source_run_id: str,
    pulled_at: str,
) -> Any:
    def mapper(payload: dict[str, Any]) -> Iterator[dict[str, Any]]:
        yield ecb_rate_row_from_payload(
            payload,
            quote_currency=quote_currency,
            rate_date=rate_date,
            source_url=source_url,
            source_run_id=source_run_id,
            pulled_at=pulled_at,
        )

    return mapper


def ecb_rate_row_from_payload(
    payload: dict[str, Any],
    *,
    quote_currency: str,
    rate_date: str,
    source_url: str,
    source_run_id: str,
    pulled_at: str,
) -> dict[str, Any]:
    values = _extract_observation_values(payload)
    if not values:
        raise ValueError(f"ECB returned no EUR/{quote_currency} rate for {rate_date}")
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return {
        "rate_date": rate_date,
        "base_currency": "EUR",
        "quote_currency": quote_currency.upper(),
        "rate": str(values[-1]),
        "source": FX_SOURCE_NAME,
        "source_url": source_url,
        "source_payload_hash": hashlib.sha256(body.encode("utf-8")).hexdigest(),
        "source_run_id": source_run_id,
        "pulled_at": pulled_at,
    }


def identity_eur_row(*, rate_date: str, source_run_id: str, pulled_at: str) -> dict[str, Any]:
    return {
        "rate_date": rate_date,
        "base_currency": "EUR",
        "quote_currency": "EUR",
        "rate": "1",
        "source": "identity",
        "source_url": "",
        "source_payload_hash": "0" * 64,
        "source_run_id": source_run_id,
        "pulled_at": pulled_at,
    }


```

Append pipeline and utility helpers:

```python
def exchange_rates_clickhouse_pipeline(credentials: dict[str, Any] | None = None) -> Pipeline:
    return dlt.pipeline(
        pipeline_name=EXCHANGE_RATES_DLT_PIPELINE_NAME,
        destination=dlt.destinations.clickhouse(
            credentials=credentials or clickhouse_destination_credentials_from_env()
        ),
        dataset_name=EXCHANGE_RATES_DLT_DATASET_NAME,
        dev_mode=False,
    )


def clickhouse_destination_credentials_from_env() -> dict[str, Any]:
    return {
        "host": os.getenv("CLICKHOUSE_HOST", "localhost"),
        "port": _int_env("CLICKHOUSE_NATIVE_PORT", DEFAULT_CLICKHOUSE_NATIVE_PORT),
        "http_port": _int_env(
            "CLICKHOUSE_HTTP_PORT",
            _int_env("CLICKHOUSE_PORT", DEFAULT_CLICKHOUSE_HTTP_PORT),
        ),
        "username": os.getenv("CLICKHOUSE_USER", "default"),
        "password": os.getenv("CLICKHOUSE_PASSWORD") or None,
        "database": tables.EXCHANGE_RATES_DATABASE,
        "secure": 1 if _bool_env("CLICKHOUSE_SECURE", False) else 0,
    }


def _extract_observation_values(payload: dict[str, Any]) -> list[Decimal]:
    if "series" in payload:
        series = payload["series"]
    else:
        data_sets = payload.get("dataSets", [])
        if not data_sets:
            return []
        series = data_sets[0].get("series", {})
    values: list[Decimal] = []
    for series_payload in series.values():
        observations = series_payload.get("observations", {})
        for observation in observations.values():
            if observation:
                values.append(Decimal(str(observation[0])))
    return values


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    return default if value is None or value.strip() == "" else int(value)


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
```

- [ ] **Step 4: Run focused tests**

Run:

```bash
uv run pytest tests/test_exchange_rates_assets.py -v
```

Expected: PASS for the current exchange-rate source tests.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/exchange_rates/source.py tests/test_exchange_rates_assets.py
git commit -m "Add dlt REST API exchange rate source"
```

## Task 4: Register Exchange Rate Dagster Asset

**Files:**
- Create: `src/dagster_v3/defs/exchange_rates/assets.py`
- Modify: `tests/test_exchange_rates_assets.py`

- [ ] **Step 1: Add failing asset registration tests**

Append:

```python
from dagster_v3.defs.exchange_rates import assets as fx_assets
from dagster_v3.defs.exchange_rates.source import EXCHANGE_RATES_DLT_TABLE


def test_exchange_rates_asset_is_registered_as_dlt_clickhouse_asset() -> None:
    repository = load_project_defs().get_repository_def()
    asset_keys = {key.path[-1] for key in repository.asset_graph.get_all_asset_keys()}
    resource_keys = repository.get_top_level_resources().keys()

    assert "exchange_rates" in asset_keys
    assert "dlt" in resource_keys
    assert "clickhouse" in resource_keys
    assert (
        repository.get_top_level_resources()["clickhouse"].configurable_resource_cls
        is ClickhouseResource
    )


def test_exchange_rate_dlt_translator_maps_asset_contract() -> None:
    source = fx_source.exchange_rates_source(rate_dates=["2024-12-31"], currencies=["USD"])
    resource = source.resources["exchange_rates_usd_2024_12_31"]
    data = type(
        "TranslatorData",
        (),
        {
            "resource": resource,
            "destination": fx_source.exchange_rates_clickhouse_pipeline().destination,
        },
    )()

    spec = fx_assets.ExchangeRatesDltTranslator().get_asset_spec(data)

    assert spec.key == dg.AssetKey("exchange_rates")
    assert spec.group_name == "exchange_rates"
    assert spec.deps == []
    assert {"python", "dlt", "clickhouse", "reference", "fx"}.issubset(spec.kinds)
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/test_exchange_rates_assets.py::test_exchange_rates_asset_is_registered_as_dlt_clickhouse_asset tests/test_exchange_rates_assets.py::test_exchange_rate_dlt_translator_maps_asset_contract -v
```

Expected: FAIL because `assets.py` does not exist.

- [ ] **Step 3: Create asset module**

Create `src/dagster_v3/defs/exchange_rates/assets.py`:

```python
from collections.abc import Iterator
import os
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData

from dagster_v3.defs.exchange_rates.clickhouse import prepare_exchange_rates_table
from dagster_v3.defs.exchange_rates.source import (
    DEFAULT_CLICKHOUSE_NATIVE_PORT,
    EXCHANGE_RATES_DLT_TABLE,
    exchange_rates_clickhouse_pipeline,
    exchange_rates_source,
)
```

Append:

```python
class ExchangeRatesDltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data: DltResourceTranslatorData) -> dg.AssetSpec:
        spec = super().get_asset_spec(data)
        if not data.resource.name.startswith(EXCHANGE_RATES_DLT_TABLE):
            return spec
        return spec.replace_attributes(
            key="exchange_rates",
            deps=[],
            group_name="exchange_rates",
            description="Shared exchange rates loaded to ClickHouse from online reference sources.",
            kinds={"python", "dlt", "clickhouse", "reference", "fx"},
        )


class ExchangeRatesConfig(dg.Config):
    rate_dates: list[str] = []
    currencies: list[str] = ["NOK", "USD", "EUR", "GBP", "SEK", "DKK"]


@dlt_assets(
    dlt_source=exchange_rates_source(rate_dates=[], currencies=[]),
    dlt_pipeline=exchange_rates_clickhouse_pipeline(),
    name="exchange_rates",
    dagster_dlt_translator=ExchangeRatesDltTranslator(),
)
def exchange_rates_asset(
    context: AssetExecutionContext,
    config: ExchangeRatesConfig,
    dlt: DagsterDltResource,
    clickhouse: ClickhouseResource,
) -> Iterator[Any]:
    context.log.info("Preparing ClickHouse table reference.exchange_rates")
    prepare_exchange_rates_table(clickhouse)
    context.log.info("Loading exchange rates into ClickHouse with dlt")
    yield from dlt.run(
        context=context,
        dlt_source=exchange_rates_source(
            rate_dates=config.rate_dates,
            currencies=config.currencies,
            source_run_id=context.run_id,
        ),
        dlt_pipeline=exchange_rates_clickhouse_pipeline(),
    )
```

Append the optional ClickHouse resource helper and asset definitions. Do not register a
`clickhouse` resource in this module because the existing project definitions already
provide the shared top-level `clickhouse` resource key.

```python
def clickhouse_resource_from_env() -> ClickhouseResource:
    return ClickhouseResource(
        host=dg.EnvVar("CLICKHOUSE_HOST"),
        port=_int_env("CLICKHOUSE_NATIVE_PORT", DEFAULT_CLICKHOUSE_NATIVE_PORT),
        user=dg.EnvVar("CLICKHOUSE_USER"),
        password=dg.EnvVar("CLICKHOUSE_PASSWORD"),
        database=dg.EnvVar("CLICKHOUSE_DATABASE"),
        secure=_bool_env("CLICKHOUSE_SECURE", False),
    )


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    return default if value is None or value.strip() == "" else int(value)


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


defs = dg.Definitions(
    assets=[exchange_rates_asset],
)
```

- [ ] **Step 4: Run focused tests and definitions listing**

Run:

```bash
uv run pytest tests/test_exchange_rates_assets.py -v
uv run dg list defs --json
```

Expected: tests pass and `exchange_rates` is registered in group `exchange_rates`.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/exchange_rates/assets.py tests/test_exchange_rates_assets.py
git commit -m "Register exchange rate ClickHouse asset"
```

## Task 5: Document Exchange Rate Section

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update README**

Add:

```markdown
## Exchange Rate Reference Asset

`dagster_v3.defs.exchange_rates` loads online exchange rates into ClickHouse table
`reference.exchange_rates`. This section only owns online sync and storage of exchange-rate
reference rows.

The asset key is `exchange_rates`, group `exchange_rates`.

Source:

- ECB Data Portal EXR API for EUR-to-currency reference rates.
- Identity rows for EUR-to-EUR.

Deferred:

- A downstream query API for other source packages.
- USD conversion helpers and financial metric contracts.
- Rules for retaining original currency values alongside converted USD values.
```

- [ ] **Step 2: Run full verification**

Run:

```bash
uv run pytest tests/test_exchange_rates_assets.py -v
uv run dg list defs --json
git -C /Users/graovic/pulsarpoint/ppoint/companycollect diff --check
```

Expected:

- Exchange-rate tests pass.
- `dg list defs --json` includes `exchange_rates`.
- diff check reports no whitespace errors.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "Document exchange rate reference asset"
```
