from collections.abc import Iterator
from contextlib import contextmanager
from typing import get_type_hints

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.definitions import defs as load_project_defs
from dagster_v3.defs.exchange_rates import source as fx_source
from dagster_v3.defs.exchange_rates.clickhouse import prepare_exchange_rates_table
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
    assert "ORDER BY (quote_currency, base_currency, rate_date, source)" in (
        tables.EXCHANGE_RATES_DDL
    )


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


def _ecb_payload(value: float = 1.0389) -> dict:
    return {
        "dataSets": [
            _ecb_dataset_payload(value),
        ]
    }


def _ecb_dataset_payload(value: float = 1.0389) -> dict:
    return {
        "series": {
            "0:0:0:0:0": {
                "observations": {
                    "0": [value, 0, 0, None, None],
                }
            }
        }
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
    nok_resource = resources["exchange_rates_nok_2024_12_31"]

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
    assert nok_resource["endpoint"]["path"] == "D.NOK.EUR.SP00.A"


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


def test_ecb_rate_row_from_dlt_selected_dataset_returns_reference_row() -> None:
    row = fx_source.ecb_rate_row_from_payload(
        _ecb_dataset_payload(),
        quote_currency="USD",
        rate_date="2024-12-31",
        source_url="https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A",
        source_run_id="run-1",
        pulled_at="2026-06-16T00:00:00.000Z",
    )

    assert row["base_currency"] == "EUR"
    assert row["quote_currency"] == "USD"
    assert row["rate"] == "1.0389"


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


def test_exchange_rates_clickhouse_destination_credentials_use_reference_database(
    monkeypatch,
) -> None:
    monkeypatch.setenv("CLICKHOUSE_HOST", "companycollect")
    monkeypatch.setenv("CLICKHOUSE_PORT", "8123")
    monkeypatch.setenv("CLICKHOUSE_NATIVE_PORT", "9002")
    monkeypatch.setenv("CLICKHOUSE_USER", "default")
    monkeypatch.setenv("CLICKHOUSE_PASSWORD", "password123")
    monkeypatch.setenv("CLICKHOUSE_SECURE", "false")

    credentials = fx_source.clickhouse_destination_credentials_from_env()

    assert credentials["host"] == "companycollect"
    assert credentials["port"] == 9002
    assert credentials["http_port"] == 8123
    assert credentials["username"] == "default"
    assert credentials["password"] == "password123"
    assert credentials["database"] == "reference"
    assert credentials["secure"] == 0


def test_exchange_rates_clickhouse_pipeline_targets_reference_database_without_dataset_prefix() -> None:
    pipeline = fx_source.exchange_rates_clickhouse_pipeline()

    assert pipeline.pipeline_name == "reference_exchange_rates"
    assert pipeline.dataset_name is None
    assert pipeline.dev_mode is False
    assert pipeline.destination.destination_name == "clickhouse"


def test_exchange_rates_source_exposes_ecb_and_identity_resources() -> None:
    source = fx_source.exchange_rates_source(
        rate_dates=["2024-12-31"],
        currencies=["USD"],
        source_run_id="run-1",
        pulled_at="2026-06-16T00:00:00.000Z",
    )

    assert {
        "exchange_rates_usd_2024_12_31",
        "exchange_rates_nok_2024_12_31",
        "exchange_rates_identity",
    }.issubset(source.resources.keys())


def test_exchange_rate_dlt_translator_maps_asset_contract() -> None:
    from dagster_v3.defs.exchange_rates import assets as fx_assets

    source = fx_source.exchange_rates_source(
        rate_dates=["2024-12-31"],
        currencies=["USD"],
        source_run_id="run-1",
        pulled_at="2026-06-16T00:00:00.000Z",
    )
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


def test_exchange_rates_clickhouse_resource_uses_native_driver_port(monkeypatch) -> None:
    from dagster_v3.defs.exchange_rates import assets as fx_assets

    monkeypatch.setenv("CLICKHOUSE_NATIVE_PORT", "9002")
    monkeypatch.setenv("CLICKHOUSE_SECURE", "false")

    resource = fx_assets.clickhouse_resource_from_env()

    assert resource.port == 9002
    assert resource.secure is False
