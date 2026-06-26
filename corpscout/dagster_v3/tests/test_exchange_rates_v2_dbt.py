from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
import json
import os
from pathlib import Path
import subprocess
import sys

import dagster as dg
import duckdb
from dagster_clickhouse import ClickhouseResource

from dagster_v3.definitions import defs as load_project_defs
from dagster_v3.defs.exchange_rates_v2 import assets as fx_v2_assets
from dagster_v3.defs.exchange_rates_v2 import source as fx_v2_source
from dagster_v3.defs.exchange_rates_v2 import tables as fx_v2_tables

DBT_PROJECT_DIR = (
    Path(__file__).parents[1]
    / "src"
    / "dagster_v3"
    / "defs"
    / "exchange_rates_v2"
    / "dbt"
)


class FakeHttpResponse:
    def __init__(self, payload: dict) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        pass

    def json(self) -> dict:
        return self.payload


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


def test_exchange_rates_v2_dependencies_are_available() -> None:
    import dagster_dbt
    import dbt

    assert dagster_dbt
    assert dbt


def test_exchange_rates_v2_dbt_uses_absolute_default_duckdb_path() -> None:
    from dagster_v3.defs.exchange_rates_v2 import assets as fx_v2_assets

    assert fx_v2_assets.EXCHANGE_RATES_V2_DUCKDB_PATH.is_absolute()
    assert Path(os.environ["EXCHANGE_RATES_V2_DUCKDB_PATH"]).is_absolute()


def test_exchange_rates_v2_is_not_partitioned() -> None:
    # No partitions: a single run pulls the whole window in one ECB request.
    assert not hasattr(fx_v2_assets, "EXCHANGE_RATES_V2_PARTITIONS")
    start, end = fx_v2_assets._full_range()
    assert start == fx_v2_assets.EXCHANGE_RATES_V2_START_DATE == "2023-01-01"
    assert end >= start  # [START_DATE, today]


def test_config_defaults_to_full_ecb_reference_set() -> None:
    cfg = fx_v2_assets.ExchangeRatesV2Config()
    assert set(cfg.currencies) == set(fx_v2_source.ECB_REFERENCE_CURRENCIES)
    # covers the currencies of the target countries (Poland, Czechia, Hungary,
    # Switzerland, UK, Nordics, ...) so adding a country needs no rate re-fetch.
    assert {"USD", "PLN", "CZK", "HUF", "RON", "CHF", "GBP", "NOK", "SEK", "DKK"} <= set(
        cfg.currencies
    )


def test_quote_currencies_force_usd_only_not_nok() -> None:
    # USD is always required (canonical conversion target); no other currency is
    # forced. EUR (the base) is never a quote currency.
    assert fx_v2_source._quote_currencies(["JPY"]) == ["JPY", "USD"]
    assert fx_v2_source._quote_currencies(["EUR"]) == ["USD"]
    assert fx_v2_source._quote_currencies(["usd", "nok"]) == ["NOK", "USD"]


def test_exchange_rates_v2_raw_resource_fetches_endpoint_and_yields_raw_payload(
    monkeypatch,
) -> None:
    calls: list[dict] = []

    def fake_get(url: str, *, params: dict, headers: dict, timeout: int) -> FakeHttpResponse:
        calls.append({"url": url, "params": params, "headers": headers, "timeout": timeout})
        return FakeHttpResponse(_ecb_range_payload())

    monkeypatch.setattr(fx_v2_source.requests, "get", fake_get)

    rows = list(
        fx_v2_source.ecb_exchange_rate_v2_raw_resource(
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


def test_exchange_rates_v2_dbt_models_build_expected_duckdb_tables(tmp_path: Path) -> None:
    duckdb_path = tmp_path / "exchange_rates_v2.duckdb"
    _create_raw_payload_table(duckdb_path)

    _run_dbt_build(duckdb_path)

    with duckdb.connect(str(duckdb_path), read_only=True) as connection:
        ecb_rows = connection.execute(
            """
            select rate_date, base_currency, quote_currency, rate, source, pulled_at
            from exchange_rates_v2_stage.ecb_rates
            order by rate_date, quote_currency
            """
        ).fetchall()
        identity_rows = connection.execute(
            """
            select rate_date, base_currency, quote_currency, rate, source, pulled_at
            from exchange_rates_v2_stage.identity_rates
            order by rate_date
            """
        ).fetchall()

    assert ecb_rows == [
        (date(2024, 12, 30), "EUR", "NOK", Decimal("11.800000000000"), "ECB EXR", datetime(2026, 6, 16)),
        (date(2024, 12, 30), "EUR", "USD", Decimal("1.040000000000"), "ECB EXR", datetime(2026, 6, 16)),
        (date(2024, 12, 31), "EUR", "NOK", Decimal("11.790000000000"), "ECB EXR", datetime(2026, 6, 16)),
        (date(2024, 12, 31), "EUR", "USD", Decimal("1.038900000000"), "ECB EXR", datetime(2026, 6, 16)),
    ]
    assert identity_rows == [
        (date(2024, 12, 30), "EUR", "EUR", Decimal("1.000000000000"), "identity", datetime(2026, 6, 16)),
        (date(2024, 12, 31), "EUR", "EUR", Decimal("1.000000000000"), "identity", datetime(2026, 6, 16)),
    ]


def test_exchange_rates_v2_clickhouse_export_reads_dbt_tables(tmp_path: Path) -> None:
    from dagster_v3.defs.exchange_rates_v2 import assets as fx_v2_assets

    duckdb_path = tmp_path / "exchange_rates_v2.duckdb"
    _create_raw_payload_table(duckdb_path)
    _run_dbt_build(duckdb_path)
    client = FakeClickHouseClient()

    with duckdb.connect(str(duckdb_path)) as connection:
        counts = fx_v2_assets.export_exchange_rates_v2_clickhouse(
            duckdb_connection=connection,
            clickhouse=FakeClickHouseResource(client),
            start_date="2024-12-30",
            end_date="2024-12-31",
        )

    assert counts["rows"] == 6
    assert client.statements == [
        (
            "ALTER TABLE corpscout.exchange_rates DELETE WHERE "
            "source IN ('ECB EXR', 'identity') "
            "AND rate_date >= '2024-12-30' "
            "AND rate_date <= '2024-12-31'"
        )
    ]
    assert client.insert_calls[0][0] == (
        "INSERT INTO `corpscout`.`exchange_rates` (`rate_date`, `base_currency`, "
        "`quote_currency`, `rate`, `source`, `source_url`, `source_payload_hash`, "
        "`source_run_id`, `pulled_at`, `_dlt_load_id`, `_dlt_id`) VALUES"
    )
    assert len(client.insert_calls[0][1]) == 6


def test_exchange_rates_v2_assets_and_job_are_registered() -> None:
    repository = load_project_defs().get_repository_def()
    asset_keys = {key.path[-1] for key in repository.asset_graph.get_all_asset_keys()}
    job_names = set(repository.job_names)
    resource_keys = repository.get_top_level_resources().keys()
    exchange_rates_v2_keys = {
        key
        for key in repository.asset_graph.get_all_asset_keys()
        if key.path[-1].startswith("exchange_rates_v2")
    }
    partitions_defs = {
        repository.asset_graph.get(key).partitions_def
        for key in exchange_rates_v2_keys
    }

    assert "exchange_rates_v2_raw_duckdb" in asset_keys
    assert "exchange_rates_v2_ecb_rates" in asset_keys
    assert "exchange_rates_v2_identity_rates" in asset_keys
    assert "exchange_rates_v2_clickhouse" in asset_keys
    assert "exchange_rates_v2_job" in job_names
    assert "dbt" in resource_keys
    assert "dlt" in resource_keys
    assert "clickhouse" in resource_keys
    assert (
        repository.get_top_level_resources()["clickhouse"].configurable_resource_cls
        is ClickhouseResource
    )
    # de-partitioned: every v2 asset is non-partitioned now.
    assert partitions_defs == {None}
    # all DuckDB-touching assets still share one pool so overlapping runs
    # serialize against the single-writer DuckDB file.
    pools = {
        asset_def.op.pool
        for asset_def in (
            fx_v2_assets.exchange_rates_v2_raw_duckdb_asset,
            fx_v2_assets.exchange_rates_v2_dbt_assets,
            fx_v2_assets.exchange_rates_v2_clickhouse,
        )
    }
    assert pools == {fx_v2_assets.EXCHANGE_RATES_V2_DUCKDB_POOL}


def test_exchange_rates_v2_daily_schedule_registered() -> None:
    repository = load_project_defs().get_repository_def()
    sched = repository.get_schedule_def("exchange_rates_v2_daily_schedule")
    assert sched.cron_schedule == "30 18 * * 1-5"
    assert sched.job.name == "exchange_rates_v2_job"


def _create_raw_payload_table(duckdb_path: Path) -> None:
    with duckdb.connect(str(duckdb_path)) as connection:
        connection.execute("create schema exchange_rates_v2_stage")
        connection.execute(
            """
            create table exchange_rates_v2_stage.ecb_raw_payloads (
              start_date varchar,
              end_date varchar,
              quote_currencies_json varchar,
              source_url varchar,
              request_params_json varchar,
              source_payload_json varchar,
              source_payload_hash varchar,
              source_run_id varchar,
              pulled_at timestamp with time zone
            )
            """
        )
        payload_json = json.dumps(_ecb_range_payload(), sort_keys=True, separators=(",", ":"))
        connection.execute(
            """
            insert into exchange_rates_v2_stage.ecb_raw_payloads values (?, ?, ?, ?, ?, ?, ?, ?, ?)
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


def _run_dbt_build(duckdb_path: Path) -> None:
    env = {
        **os.environ,
        "EXCHANGE_RATES_V2_DUCKDB_PATH": str(duckdb_path),
    }
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "dbt.cli.main",
            "build",
            "--project-dir",
            str(DBT_PROJECT_DIR),
            "--profiles-dir",
            str(DBT_PROJECT_DIR),
            "--vars",
            json.dumps({"start_date": "2024-12-01", "end_date": "2024-12-31"}),
        ],
        cwd=Path(__file__).parents[1],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
