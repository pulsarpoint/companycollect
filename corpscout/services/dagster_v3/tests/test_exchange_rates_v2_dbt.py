from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
import json
import os
from pathlib import Path
import subprocess
import sys

import duckdb
from dagster_clickhouse import ClickhouseResource

from dagster_v3.definitions import defs as load_project_defs
from dagster_v3.defs.exchange_rates_v2 import assets as fx_v2_assets
from dagster_v3.defs.exchange_rates_v2 import source as fx_v2_source

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


def _ecb_range_payload_with_null_rate() -> dict:
    payload = _ecb_range_payload()
    payload["structure"]["dimensions"]["observation"][0]["values"].insert(
        0,
        {"id": "2024-12-29"},
    )
    for series in payload["dataSets"][0]["series"].values():
        series["observations"] = {
            "0": [None],
            **{str(int(key) + 1): value for key, value in series["observations"].items()},
        }
    return payload


def _ecb_single_day_payload() -> dict:
    payload = _ecb_range_payload()
    payload["structure"]["dimensions"]["observation"][0]["values"] = [
        {"id": "2024-12-31"}
    ]
    for series in payload["dataSets"][0]["series"].values():
        latest_value = series["observations"]["1"]
        series["observations"] = {"0": latest_value}
    return payload


def test_exchange_rates_v2_dependencies_are_available() -> None:
    import dagster_dbt
    import dbt

    assert dagster_dbt
    assert dbt


def test_exchange_rates_v2_dbt_uses_absolute_default_duckdb_path() -> None:
    from dagster_v3.defs.exchange_rates_v2 import assets as fx_v2_assets

    assert fx_v2_assets.EXCHANGE_RATES_V2_DUCKDB_PATH.is_absolute()
    assert Path(os.environ["EXCHANGE_RATES_V2_DUCKDB_PATH"]).is_absolute()


def test_exchange_rates_v2_uses_yearly_partitions() -> None:
    partition_keys = fx_v2_assets.EXCHANGE_RATES_V2_PARTITIONS.get_partition_keys()

    assert partition_keys[0] == "2006"
    assert partition_keys[-1] == str(date.today().year)
    assert fx_v2_assets._partition_range("2006") == ("2006-01-01", "2006-12-31")
    assert fx_v2_assets._partition_range(str(date.today().year))[0] == (
        f"{date.today().year}-01-01"
    )
    assert fx_v2_assets._partition_range(str(date.today().year))[1] == date.today().isoformat()


def test_config_defaults_to_full_ecb_reference_set() -> None:
    cfg = fx_v2_assets.ExchangeRatesV2Config()
    assert set(cfg.currencies) == set(fx_v2_source.ECB_REFERENCE_CURRENCIES)
    # covers the currencies of the target countries (Poland, Czechia, Hungary,
    # Switzerland, UK, Nordics, ...) so adding a country needs no rate re-fetch.
    assert {
        "USD",
        "LVL",
        "PLN",
        "CZK",
        "HUF",
        "RON",
        "CHF",
        "GBP",
        "NOK",
        "SEK",
        "DKK",
    } <= set(cfg.currencies)


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


def test_exchange_rates_v2_raw_resource_fetches_year_batches(monkeypatch) -> None:
    calls: list[dict] = []

    def fake_get(url: str, *, params: dict, headers: dict, timeout: int) -> FakeHttpResponse:
        calls.append({"url": url, "params": params, "headers": headers, "timeout": timeout})
        return FakeHttpResponse(_ecb_range_payload())

    monkeypatch.setattr(fx_v2_source.requests, "get", fake_get)

    rows = list(
        fx_v2_source.ecb_exchange_rate_v2_raw_resource(
            start_date="2006-06-15",
            end_date="2008-02-10",
            currencies=["USD", "LVL"],
            source_run_id="run-1",
            pulled_at="2026-06-16T00:00:00.000Z",
        )
    )

    assert [call["params"] for call in calls] == [
        {
            "format": "jsondata",
            "startPeriod": "2006-06-15",
            "endPeriod": "2006-12-31",
        },
        {
            "format": "jsondata",
            "startPeriod": "2007-01-01",
            "endPeriod": "2007-12-31",
        },
        {
            "format": "jsondata",
            "startPeriod": "2008-01-01",
            "endPeriod": "2008-02-10",
        },
    ]
    assert [(row["start_date"], row["end_date"]) for row in rows] == [
        ("2006-06-15", "2006-12-31"),
        ("2007-01-01", "2007-12-31"),
        ("2008-01-01", "2008-02-10"),
    ]


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


def test_exchange_rates_v2_dbt_models_skip_null_observation_values(
    tmp_path: Path,
) -> None:
    duckdb_path = tmp_path / "exchange_rates_v2.duckdb"
    _create_raw_payload_table(duckdb_path, payload=_ecb_range_payload_with_null_rate())

    _run_dbt_build(duckdb_path)

    with duckdb.connect(str(duckdb_path), read_only=True) as connection:
        null_rates = connection.execute(
            """
            select count(*)
            from exchange_rates_v2_stage.ecb_rates
            where rate is null
            """
        ).fetchone()[0]
        rate_dates = connection.execute(
            """
            select distinct rate_date
            from exchange_rates_v2_stage.ecb_rates
            order by rate_date
            """
        ).fetchall()

    assert null_rates == 0
    assert rate_dates == [(date(2024, 12, 30),), (date(2024, 12, 31),)]


def test_exchange_rates_v2_dbt_rebuild_removes_stale_rates(tmp_path: Path) -> None:
    duckdb_path = tmp_path / "exchange_rates_v2.duckdb"
    _create_raw_payload_table(duckdb_path)
    _run_dbt_build(duckdb_path)

    with duckdb.connect(str(duckdb_path)) as connection:
        connection.execute("delete from exchange_rates_v2_stage.ecb_raw_payloads")
    _insert_raw_payload(
        duckdb_path,
        start_date="2024-12-01",
        end_date="2024-12-31",
        source_run_id="run-2",
        pulled_at="2026-06-17T00:00:00.000Z",
        source_payload_hash="d" * 64,
        payload=_ecb_single_day_payload(),
    )
    _run_dbt_build(duckdb_path)

    with duckdb.connect(str(duckdb_path), read_only=True) as connection:
        rate_dates = connection.execute(
            """
            select distinct rate_date
            from exchange_rates_v2_stage.ecb_rates
            order by rate_date
            """
        ).fetchall()

    assert rate_dates == [(date(2024, 12, 31),)]


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


def test_clear_exchange_rates_v2_raw_window_removes_overlapping_payloads(
    tmp_path: Path,
) -> None:
    duckdb_path = tmp_path / "exchange_rates_v2.duckdb"
    _create_raw_payload_table(duckdb_path)
    _insert_raw_payload(
        duckdb_path,
        start_date="2024-01-01",
        end_date="2024-12-31",
        source_run_id="older-run",
        pulled_at="2026-06-15T00:00:00.000Z",
        source_payload_hash="b" * 64,
    )
    _insert_raw_payload(
        duckdb_path,
        start_date="2025-01-01",
        end_date="2025-12-31",
        source_run_id="outside-run",
        pulled_at="2026-06-15T00:00:00.000Z",
        source_payload_hash="c" * 64,
    )

    with duckdb.connect(str(duckdb_path)) as connection:
        fx_v2_assets._clear_exchange_rates_v2_raw_window(
            connection,
            start_date="2024-01-01",
            end_date="2024-12-31",
        )
        ranges = connection.execute(
            """
            select start_date, end_date
            from exchange_rates_v2_stage.ecb_raw_payloads
            order by start_date, end_date
            """
        ).fetchall()

    assert ranges == [("2025-01-01", "2025-12-31")]


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
    assert partitions_defs == {fx_v2_assets.EXCHANGE_RATES_V2_PARTITIONS}
    assert repository.get_job("exchange_rates_v2_job").partitions_def is (
        fx_v2_assets.EXCHANGE_RATES_V2_PARTITIONS
    )
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
    assert sched.execution_timezone == fx_v2_assets.EXCHANGE_RATES_V2_TIMEZONE


def _create_raw_payload_table(
    duckdb_path: Path,
    *,
    payload: dict | None = None,
) -> None:
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
    _insert_raw_payload(
        duckdb_path,
        start_date="2024-12-01",
        end_date="2024-12-31",
        source_run_id="run-1",
        pulled_at="2026-06-16T00:00:00.000Z",
        source_payload_hash="a" * 64,
        payload=payload or _ecb_range_payload(),
    )


def _insert_raw_payload(
    duckdb_path: Path,
    *,
    start_date: str,
    end_date: str,
    source_run_id: str,
    pulled_at: str,
    source_payload_hash: str,
    payload: dict | None = None,
) -> None:
    payload_json = json.dumps(
        payload or _ecb_range_payload(),
        sort_keys=True,
        separators=(",", ":"),
    )
    with duckdb.connect(str(duckdb_path)) as connection:
        connection.execute(
            """
            insert into exchange_rates_v2_stage.ecb_raw_payloads values (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                start_date,
                end_date,
                '["NOK","USD"]',
                "https://data-api.ecb.europa.eu/service/data/EXR/D.NOK+USD.EUR.SP00.A",
                (
                    f'{{"endPeriod":"{end_date}","format":"jsondata",'
                    f'"startPeriod":"{start_date}"}}'
                ),
                payload_json,
                source_payload_hash,
                source_run_id,
                pulled_at,
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
