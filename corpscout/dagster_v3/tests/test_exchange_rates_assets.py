from collections.abc import Iterator
from contextlib import contextmanager
import json
from pathlib import Path

import dagster as dg
import dlt
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
        self.insert_calls: list[tuple[str, list[tuple]]] = []

    def execute(self, sql: str, params: list[tuple] | None = None) -> list[tuple]:
        if params is None:
            self.statements.append(sql)
        else:
            self.insert_calls.append((sql, params))
        return []


class FakeClickHouseResource:
    def __init__(self, client: FakeClickHouseClient) -> None:
        self.client = client

    @contextmanager
    def get_connection(self) -> Iterator[FakeClickHouseClient]:
        yield self.client


class FakeHttpResponse:
    def __init__(self, payload: dict) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        pass

    def json(self) -> dict:
        return self.payload


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


def test_ecb_exchange_rate_raw_resource_fetches_endpoint_and_yields_raw_payload(
    monkeypatch,
) -> None:
    calls: list[dict] = []

    def fake_get(url: str, *, params: dict, headers: dict, timeout: int) -> FakeHttpResponse:
        calls.append({"url": url, "params": params, "headers": headers, "timeout": timeout})
        return FakeHttpResponse(_ecb_range_payload())

    monkeypatch.setattr(fx_source.requests, "get", fake_get)

    rows = list(
        fx_source.ecb_exchange_rate_raw_resource(
            start_date="2024-12-01",
            end_date="2024-12-31",
            currencies=["USD", "NOK"],
            source_run_id="run-1",
            pulled_at="2026-06-16T00:00:00.000Z",
        )
    )

    assert calls == [
        {
            "url": "https://data-api.ecb.europa.eu/service/data/EXR/D.NOK+USD.EUR.SP00.A",
            "params": {
                "format": "jsondata",
                "startPeriod": "2024-12-01",
                "endPeriod": "2024-12-31",
            },
            "headers": {"User-Agent": "corpscout-dagster-v3-dev/0.1"},
            "timeout": 30,
        }
    ]
    assert rows[0]["start_date"] == "2024-12-01"
    assert rows[0]["end_date"] == "2024-12-31"
    assert rows[0]["quote_currencies_json"] == '["NOK","USD"]'
    assert json.loads(rows[0]["source_payload_json"]) == _ecb_range_payload()
    assert len(rows[0]["source_payload_hash"]) == 64


def test_exchange_rates_raw_range_source_exposes_raw_resource() -> None:
    source = fx_source.exchange_rates_raw_range_source(
        start_date="2024-12-01",
        end_date="2024-12-31",
        currencies=["USD"],
        source_run_id="run-1",
        pulled_at="2026-06-16T00:00:00.000Z",
    )

    assert list(source.resources.keys()) == ["ecb_raw_payloads"]
    assert source.resources["ecb_raw_payloads"].table_name == "ecb_raw_payloads"


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


def test_normalize_exchange_rates_ecb_duckdb_reads_raw_payloads(tmp_path: Path) -> None:
    from dagster_v3.defs.exchange_rates import assets as fx_assets

    duckdb_path = tmp_path / "exchange_rates.duckdb"
    _create_raw_payload_table(duckdb_path)

    counts = fx_assets.normalize_exchange_rates_ecb_duckdb(
        duckdb_path=duckdb_path,
        start_date="2024-12-01",
        end_date="2024-12-31",
    )

    with duckdb.connect(str(duckdb_path), read_only=True) as connection:
        rows = connection.execute(
            """
            select rate_date, quote_currency, rate, source
            from exchange_rates_stage.ecb_rates
            order by rate_date, quote_currency
            """
        ).fetchall()

    assert counts == {"raw_payloads": 1, "ecb_rates": 4}
    assert rows == [
        ("2024-12-30", "NOK", "11.8", "ECB EXR"),
        ("2024-12-30", "USD", "1.04", "ECB EXR"),
        ("2024-12-31", "NOK", "11.79", "ECB EXR"),
        ("2024-12-31", "USD", "1.0389", "ECB EXR"),
    ]


def test_generate_exchange_rates_identity_duckdb_uses_ecb_rate_dates(tmp_path: Path) -> None:
    from dagster_v3.defs.exchange_rates import assets as fx_assets

    duckdb_path = tmp_path / "exchange_rates.duckdb"
    _create_ecb_rates_table(duckdb_path)

    counts = fx_assets.generate_exchange_rates_identity_duckdb(
        duckdb_path=duckdb_path,
        start_date="2024-12-30",
        end_date="2024-12-31",
    )

    with duckdb.connect(str(duckdb_path), read_only=True) as connection:
        rows = connection.execute(
            """
            select rate_date, base_currency, quote_currency, rate, source
            from exchange_rates_stage.identity_rates
            order by rate_date
            """
        ).fetchall()

    assert counts == {"identity_rates": 2}
    assert rows == [
        ("2024-12-30", "EUR", "EUR", "1", "identity"),
        ("2024-12-31", "EUR", "EUR", "1", "identity"),
    ]


def test_export_exchange_rates_clickhouse_reads_duckdb_and_inserts_union(
    tmp_path: Path,
) -> None:
    from dagster_v3.defs.exchange_rates import assets as fx_assets

    duckdb_path = tmp_path / "exchange_rates.duckdb"
    _create_ecb_rates_table(duckdb_path)
    fx_assets.generate_exchange_rates_identity_duckdb(
        duckdb_path=duckdb_path,
        start_date="2024-12-30",
        end_date="2024-12-31",
    )
    client = FakeClickHouseClient()

    counts = fx_assets.export_exchange_rates_clickhouse(
        duckdb_path=duckdb_path,
        clickhouse=FakeClickHouseResource(client),
        start_date="2024-12-30",
        end_date="2024-12-31",
    )

    assert counts["rows"] == 4
    assert client.statements == [
        (
            "ALTER TABLE reference.exchange_rates DELETE WHERE "
            "source IN ('ECB EXR', 'identity') "
            "AND rate_date >= '2024-12-30' "
            "AND rate_date <= '2024-12-31'"
        )
    ]
    assert client.insert_calls[0][0] == (
        "INSERT INTO `reference`.`exchange_rates` (`rate_date`, `base_currency`, "
        "`quote_currency`, `rate`, `source`, `source_url`, `source_payload_hash`, "
        "`source_run_id`, `pulled_at`, `_dlt_load_id`, `_dlt_id`) VALUES"
    )
    assert [(row[0], row[2], row[3], row[4]) for row in client.insert_calls[0][1]] == [
        ("2024-12-30", "NOK", "11.8", "ECB EXR"),
        ("2024-12-31", "USD", "1.0389", "ECB EXR"),
        ("2024-12-30", "EUR", "1", "identity"),
        ("2024-12-31", "EUR", "1", "identity"),
    ]


def test_exchange_rate_month_partition_window() -> None:
    from datetime import date

    from dagster_v3.defs.exchange_rates import assets as fx_assets

    assert fx_assets.month_partition_window(
        "2024-02-01",
        today=date(2024, 6, 16),
    ) == ("2024-02-01", "2024-02-29")
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


def test_delete_exchange_rates_window_targets_source_dates() -> None:
    from dagster_v3.defs.exchange_rates import assets as fx_assets

    client = FakeClickHouseClient()

    fx_assets.delete_exchange_rates_window(
        client,
        start_date="2024-12-01",
        end_date="2024-12-31",
    )

    assert client.statements == [
        (
            "ALTER TABLE reference.exchange_rates DELETE WHERE "
            "source IN ('ECB EXR', 'identity') "
            "AND rate_date >= '2024-12-01' "
            "AND rate_date <= '2024-12-31'"
        )
    ]


def test_exchange_rate_dlt_translator_maps_raw_asset_contract() -> None:
    from dagster_v3.defs.exchange_rates import assets as fx_assets

    source = fx_source.exchange_rates_raw_range_source(
        start_date="2024-12-01",
        end_date="2024-12-31",
        currencies=["USD"],
        source_run_id="run-1",
        pulled_at="2026-06-16T00:00:00.000Z",
    )
    resource = source.resources["ecb_raw_payloads"]
    data = type(
        "TranslatorData",
        (),
        {
            "resource": resource,
            "destination": dlt.destinations.duckdb("data/test.duckdb"),
        },
    )()

    spec = fx_assets.ExchangeRatesDltTranslator(
        asset_key="exchange_rates_raw_duckdb",
        description="Raw shared exchange rates.",
    ).get_asset_spec(data)

    assert spec.key == dg.AssetKey("exchange_rates_raw_duckdb")
    assert spec.group_name == "exchange_rates"
    assert spec.deps == []
    assert {"python", "dlt", "duckdb", "reference", "fx"}.issubset(spec.kinds)


def test_exchange_rates_assets_and_daily_schedule_are_registered() -> None:
    repository = load_project_defs().get_repository_def()
    asset_keys = {key.path[-1] for key in repository.asset_graph.get_all_asset_keys()}
    schedule_names = {schedule.name for schedule in repository.schedule_defs}
    resource_keys = repository.get_top_level_resources().keys()

    assert "exchange_rates_raw_duckdb" in asset_keys
    assert "exchange_rates_ecb_rates_duckdb" in asset_keys
    assert "exchange_rates_identity_rates_duckdb" in asset_keys
    assert "exchange_rates_clickhouse" in asset_keys
    assert "exchange_rates_backfill" not in asset_keys
    assert "exchange_rates_daily" not in asset_keys
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


def _create_raw_payload_table(duckdb_path: Path) -> None:
    with duckdb.connect(str(duckdb_path)) as connection:
        connection.execute("create schema exchange_rates_stage")
        connection.execute(
            """
            create table exchange_rates_stage.ecb_raw_payloads (
              start_date varchar,
              end_date varchar,
              quote_currencies_json varchar,
              source_url varchar,
              request_params_json varchar,
              source_payload_json varchar,
              source_payload_hash varchar,
              source_run_id varchar,
              pulled_at varchar
            )
            """
        )
        payload_json = json.dumps(_ecb_range_payload(), sort_keys=True, separators=(",", ":"))
        connection.execute(
            """
            insert into exchange_rates_stage.ecb_raw_payloads values (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "2024-12-01",
                "2024-12-31",
                '["NOK","USD"]',
                "https://data-api.ecb.europa.eu/service/data/EXR/D.NOK+USD.EUR.SP00.A",
                '{"endPeriod":"2024-12-31","format":"jsondata","startPeriod":"2024-12-01"}',
                payload_json,
                "a" * 64,
                "run-1",
                "2026-06-16T00:00:00.000Z",
            ],
        )


def _create_ecb_rates_table(duckdb_path: Path) -> None:
    with duckdb.connect(str(duckdb_path)) as connection:
        connection.execute("create schema exchange_rates_stage")
        columns = ", ".join(f"{column} varchar" for column in tables.EXCHANGE_RATES_COLUMNS)
        connection.execute(f"create table exchange_rates_stage.ecb_rates ({columns})")
        connection.execute(
            f"""
            insert into exchange_rates_stage.ecb_rates
            ({", ".join(tables.EXCHANGE_RATES_COLUMNS)})
            values
              ('2024-12-30', 'EUR', 'NOK', '11.8', 'ECB EXR',
               'https://ecb.example', repeat('a', 64), 'run-1',
               '2026-06-16T00:00:00.000Z', '', 'row-nok'),
              ('2024-12-31', 'EUR', 'USD', '1.0389', 'ECB EXR',
               'https://ecb.example', repeat('a', 64), 'run-1',
               '2026-06-16T00:00:00.000Z', '', 'row-usd')
            """
        )
