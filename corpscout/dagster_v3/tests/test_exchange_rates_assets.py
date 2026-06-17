from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import dagster as dg
import duckdb
from dagster_clickhouse import ClickhouseResource

from dagster_v3.definitions import defs as load_project_defs
from dagster_v3.defs.exchange_rates import source as fx_source
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
    assert not hasattr(tables, "EXCHANGE_RATES_DDL")


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.inserted_rows: list[tuple] = []

    def execute(self, sql: str, params: list[tuple] | None = None) -> None:
        self.statements.append(sql)
        if params is not None:
            self.inserted_rows.extend(params)


class FakeClickHouseResource:
    def __init__(self, client: FakeClickHouseClient) -> None:
        self.client = client

    @contextmanager
    def get_connection(self) -> Iterator[FakeClickHouseClient]:
        yield self.client


class DuckDbBackedDltResource:
    def __init__(self, duckdb_path: Path, clickhouse: FakeClickHouseResource) -> None:
        self.duckdb_path = duckdb_path
        self.clickhouse = clickhouse

    def run(self, **_: object) -> Iterator[dg.MaterializeResult]:
        with duckdb.connect(str(self.duckdb_path), read_only=True) as connection:
            rows = connection.execute(
                """
                select
                  rate_date,
                  base_currency,
                  quote_currency,
                  rate,
                  source,
                  source_url,
                  source_payload_hash,
                  source_run_id,
                  pulled_at,
                  _dlt_load_id,
                  _dlt_id
                from reference.exchange_rates
                order by rate_date, quote_currency
                """
            ).fetchall()
        with self.clickhouse.get_connection() as client:
            client.execute(
                "INSERT INTO reference.exchange_rates VALUES",
                rows,
            )
        yield dg.MaterializeResult(metadata={"inserted_rows": len(rows)})


def _create_exchange_rates_duckdb_fixture(duckdb_path: Path) -> None:
    with duckdb.connect(str(duckdb_path)) as connection:
        connection.execute("create schema reference")
        connection.execute(
            """
            create table reference.exchange_rates (
              rate_date date,
              base_currency varchar,
              quote_currency varchar,
              rate decimal(18, 8),
              source varchar,
              source_url varchar,
              source_payload_hash varchar,
              source_run_id varchar,
              pulled_at timestamp,
              _dlt_load_id varchar,
              _dlt_id varchar
            )
            """
        )
        connection.execute(
            """
            insert into reference.exchange_rates values
              ('2026-05-01', 'EUR', 'USD', 1.15800000, 'ECB EXR',
               'https://ecb.example/USD', repeat('a', 64), 'run-test',
               '2026-05-01 12:00:00', 'load-1', 'row-1'),
              ('2026-05-01', 'EUR', 'EUR', 1.00000000, 'identity',
               '', repeat('0', 64), 'run-test', '2026-05-01 12:00:00',
               'load-1', 'row-2')
            """
        )


def test_exchange_rates_backfill_materialization_loads_duckdb_rows_without_runtime_ddl(
    tmp_path: Path,
) -> None:
    from dagster_v3.defs.exchange_rates import assets as fx_assets

    duckdb_path = tmp_path / "exchange_rates.duckdb"
    _create_exchange_rates_duckdb_fixture(duckdb_path)
    client = FakeClickHouseClient()
    clickhouse = FakeClickHouseResource(client)
    dlt_resource = DuckDbBackedDltResource(duckdb_path, clickhouse)

    result = dg.materialize(
        [fx_assets.exchange_rates_backfill_asset],
        partition_key="2026-05-01",
        resources={
            "clickhouse": clickhouse,
            "dlt": dlt_resource,
        },
        run_config={
            "ops": {
                "exchange_rates_backfill": {
                    "config": {
                        "currencies": ["USD"],
                    }
                }
            }
        },
    )

    assert result.success
    assert not any(statement.upper().startswith("CREATE ") for statement in client.statements)
    assert client.statements[0].startswith("ALTER TABLE reference.exchange_rates DELETE WHERE")
    assert client.statements[1] == "INSERT INTO reference.exchange_rates VALUES"
    assert [(row[0].isoformat(), row[1], row[2], str(row[3])) for row in client.inserted_rows] == [
        ("2026-05-01", "EUR", "EUR", "1.00000000"),
        ("2026-05-01", "EUR", "USD", "1.15800000"),
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


def _ecb_range_payload() -> dict:
    return {
        "structure": {
            "dimensions": {
                "series": [
                    {"id": "FREQ", "values": [{"id": "D"}]},
                    {"id": "CURRENCY", "values": [{"id": "NOK"}, {"id": "USD"}]},
                    {"id": "CURRENCY_DENOM", "values": [{"id": "EUR"}]},
                    {"id": "EXR_TYPE", "values": [{"id": "SP00"}]},
                    {"id": "EXR_SUFFIX", "values": [{"id": "A"}]},
                ],
                "observation": [
                    {
                        "values": [
                            {"id": "2024-12-30"},
                            {"id": "2024-12-31"},
                        ]
                    }
                ],
            }
        },
        "dataSets": [
            {
                "series": {
                    "0:0:0:0:0": {"observations": {"0": [11.8], "1": [11.79]}},
                    "0:1:0:0:0": {"observations": {"0": [1.04], "1": [1.0389]}},
                }
            }
        ],
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
        "data_selector": "$",
    }
    assert usd_resource["processing_steps"]
    assert nok_resource["endpoint"]["path"] == "D.NOK.EUR.SP00.A"


def test_exchange_rate_range_rest_api_config_models_bulk_ecb_endpoint() -> None:
    config = fx_source.exchange_rate_range_rest_api_config(
        start_date="2024-12-01",
        end_date="2024-12-31",
        currencies=["USD", "NOK"],
        source_run_id="run-1",
        pulled_at="2026-06-16T00:00:00.000Z",
    )

    resources = {resource["name"]: resource for resource in config["resources"]}
    resource = resources["exchange_rates_ecb_2024_12_01_2024_12_31"]

    assert config["client"]["base_url"] == "https://data-api.ecb.europa.eu/service/data/EXR/"
    assert config["client"]["headers"] == {"User-Agent": "corpscout-dagster-v3-dev/0.1"}
    assert resource["table_name"] == "exchange_rates"
    assert resource["endpoint"] == {
        "path": "D.NOK+USD.EUR.SP00.A",
        "params": {
            "format": "jsondata",
            "startPeriod": "2024-12-01",
            "endPeriod": "2024-12-31",
        },
        "paginator": "single_page",
        "data_selector": "$",
    }
    assert resource["processing_steps"]


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


def test_ecb_rate_rows_from_range_payload_returns_dated_rows() -> None:
    rows = fx_source.ecb_rate_rows_from_range_payload(
        _ecb_range_payload(),
        quote_currencies=["NOK", "USD"],
        source_url="https://data-api.ecb.europa.eu/service/data/EXR/D.NOK+USD.EUR.SP00.A",
        source_run_id="run-1",
        pulled_at="2026-06-16T00:00:00.000Z",
    )

    assert [
        (row["rate_date"], row["quote_currency"], row["rate"])
        for row in rows
    ] == [
        ("2024-12-30", "NOK", "11.8"),
        ("2024-12-31", "NOK", "11.79"),
        ("2024-12-30", "USD", "1.04"),
        ("2024-12-31", "USD", "1.0389"),
    ]
    assert {row["base_currency"] for row in rows} == {"EUR"}
    assert {row["source"] for row in rows} == {"ECB EXR"}
    assert all(len(row["source_payload_hash"]) == 64 for row in rows)


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


def test_identity_exchange_rates_for_dates_resource_yields_actual_rate_dates() -> None:
    resource = fx_source.identity_exchange_rates_for_dates_resource(
        rate_dates=["2024-12-30", "2024-12-31"],
        source_run_id="run-1",
        pulled_at="2026-06-16T00:00:00.000Z",
    )

    rows = list(resource)

    assert [row["rate_date"] for row in rows] == ["2024-12-30", "2024-12-31"]
    assert {row["quote_currency"] for row in rows} == {"EUR"}


def test_exchange_rates_range_source_exposes_bulk_resource_and_identity_resource() -> None:
    source = fx_source.exchange_rates_range_source(
        start_date="2024-12-01",
        end_date="2024-12-31",
        currencies=["USD"],
        source_run_id="run-1",
        pulled_at="2026-06-16T00:00:00.000Z",
    )

    assert "exchange_rates_ecb_2024_12_01_2024_12_31" in source.resources.keys()
    assert "exchange_rates_identity" in source.resources.keys()


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


def test_exchange_rate_month_partition_window() -> None:
    from datetime import date

    from dagster_v3.defs.exchange_rates import assets as fx_assets

    assert fx_assets.month_partition_window(
        "2024-02-01",
        today=date(2024, 6, 16),
    ) == ("2024-02-01", "2024-02-29")
    assert fx_assets.month_partition_window(
        "2024-06-01",
        today=date(2024, 6, 16),
    ) == ("2024-06-01", "2024-06-16")
    assert fx_assets.month_partition_range_window(
        start_partition_key="2024-02-01",
        end_partition_key="2024-04-01",
        today=date(2024, 6, 16),
    ) == ("2024-02-01", "2024-04-30")


def test_exchange_rate_day_partition_window() -> None:
    from dagster_v3.defs.exchange_rates import assets as fx_assets

    assert fx_assets.day_partition_window("2024-12-31") == (
        "2024-12-31",
        "2024-12-31",
    )
    assert fx_assets.day_partition_range_window(
        start_partition_key="2024-12-30",
        end_partition_key="2024-12-31",
    ) == ("2024-12-30", "2024-12-31")


def test_delete_exchange_rates_window_targets_source_dates_and_currencies() -> None:
    from dagster_v3.defs.exchange_rates import assets as fx_assets

    client = FakeClickHouseClient()

    fx_assets.delete_exchange_rates_window(
        client,
        start_date="2024-12-01",
        end_date="2024-12-31",
        currencies=["NOK", "USD", "EUR"],
    )

    assert client.statements == [
        (
            "ALTER TABLE reference.exchange_rates DELETE WHERE "
            "source IN ('ECB EXR', 'identity') "
            "AND rate_date >= '2024-12-01' "
            "AND rate_date <= '2024-12-31' "
            "AND quote_currency IN ('EUR', 'NOK', 'USD')"
        )
    ]


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


def test_exchange_rate_dlt_translator_maps_backfill_asset_contract() -> None:
    from dagster_v3.defs.exchange_rates import assets as fx_assets

    source = fx_source.exchange_rates_range_source(
        start_date="2024-12-01",
        end_date="2024-12-31",
        currencies=["USD"],
        source_run_id="run-1",
        pulled_at="2026-06-16T00:00:00.000Z",
    )
    resource = source.resources["exchange_rates_ecb_2024_12_01_2024_12_31"]
    data = type(
        "TranslatorData",
        (),
        {
            "resource": resource,
            "destination": fx_source.exchange_rates_clickhouse_pipeline().destination,
        },
    )()

    spec = fx_assets.ExchangeRatesDltTranslator(
        asset_key="exchange_rates_backfill",
        description="Backfill shared exchange rates.",
    ).get_asset_spec(data)

    assert spec.key == dg.AssetKey("exchange_rates_backfill")
    assert spec.group_name == "exchange_rates"
    assert spec.deps == []
    assert {"python", "dlt", "clickhouse", "reference", "fx"}.issubset(spec.kinds)


def test_exchange_rates_assets_and_daily_schedule_are_registered() -> None:
    repository = load_project_defs().get_repository_def()
    asset_keys = {key.path[-1] for key in repository.asset_graph.get_all_asset_keys()}
    schedule_names = {schedule.name for schedule in repository.schedule_defs}
    resource_keys = repository.get_top_level_resources().keys()

    assert "exchange_rates_backfill" in asset_keys
    assert "exchange_rates_daily" in asset_keys
    assert "exchange_rates" not in asset_keys
    assert "exchange_rates_daily_schedule" in schedule_names
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
