from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path

import duckdb
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.estonia_ar import tables
from dagster_v3.defs.estonia_financial import clickhouse as ee_clickhouse
from dagster_v3.defs.estonia_financial import financials, metrics
from tests.test_estonia_ar_financials import _seed_raw


class _StubRate:
    def __init__(self, rate: Decimal, rate_date: str) -> None:
        self.rate = rate
        self.rate_date = rate_date
        self.source = "TEST"


class _StubExchangeRates:
    def usd_rates(self, requests):
        return {
            (r.currency, r.rate_date): _StubRate(Decimal("1.10"), r.rate_date)
            for r in requests
        }


class _FakeClickHouse:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.inserts: list[tuple[str, list]] = []

    def execute(self, sql, params=None):
        if "system.tables" in sql:
            return [(tables.EE_FINANCIAL_METRICS_TABLE,)]
        if isinstance(params, (list, tuple)):
            self.inserts.append((sql, list(params)))
            return None
        self.statements.append(sql)
        return None


def _build_native(db_path: Path) -> None:
    _seed_raw(db_path)
    with duckdb.connect(str(db_path)) as conn:
        financials.build_estonia_ar_financial_statements(
            duckdb_connection=conn, source_run_id="run-1"
        )
        metrics.build_estonia_ar_financial_metrics(
            duckdb_connection=conn, source_run_id="run-1"
        )


def test_build_native_metrics_eur_no_usd(tmp_path: Path):
    db_path = tmp_path / "estonia_ar_source.duckdb"
    _build_native(db_path)
    wide = f"{tables.DLT_DATASET_NAME}.{tables.FINANCIAL_METRICS_WIDE_TABLE}"
    with duckdb.connect(str(db_path), read_only=True) as conn:
        row = conn.execute(
            "select currency, revenue_amount_original, revenue_amount_usd, "
            "net_result_amount_original, fx_source "
            f"from {wide} where report_id = '1703729'"
        ).fetchone()
        cols = [r[0] for r in conn.execute(f"describe {wide}").fetchall()]
    assert row[0] == "EUR"
    assert row[1] == Decimal("100000.00")  # native, no rounding factor
    assert row[2] is None  # usd not filled yet
    assert row[3] == Decimal("5000.00")
    assert row[4] == ""  # fx_source empty
    assert set(cols) == set(tables.EE_FINANCIAL_METRICS_COLUMNS)


def test_usd_conversion_fills_usd(tmp_path: Path):
    db_path = tmp_path / "estonia_ar_source.duckdb"
    _build_native(db_path)
    with duckdb.connect(str(db_path)) as conn:
        counts = metrics.apply_estonia_ar_usd_conversion(
            duckdb_connection=conn, exchange_rates=_StubExchangeRates()
        )
        rerun_counts = metrics.apply_estonia_ar_usd_conversion(
            duckdb_connection=conn, exchange_rates=_StubExchangeRates()
        )
    assert rerun_counts == counts
    assert counts["rate_pairs"] == 1  # EUR / 2019-12-31
    assert counts["rows_converted"] == 1
    wide = f"{tables.DLT_DATASET_NAME}.{tables.FINANCIAL_METRICS_WIDE_TABLE}"
    with duckdb.connect(str(db_path), read_only=True) as conn:
        row = conn.execute(
            "select revenue_amount_usd, net_result_amount_usd, fx_rate_to_usd, fx_source "
            f"from {wide} where report_id = '1703729'"
        ).fetchone()
    assert row[0] == Decimal("110000.00")  # 100000 * 1.10
    assert row[1] == Decimal("5500.00")  # 5000 * 1.10
    assert row[2] == Decimal("1.100000000000")
    assert row[3] == "TEST"


def test_usd_conversion_without_rates_resets_usd_columns(tmp_path: Path):
    class _EmptyExchangeRates:
        def usd_rates(self, requests):
            return {}

    db_path = tmp_path / "estonia_ar_source.duckdb"
    _build_native(db_path)
    with duckdb.connect(str(db_path)) as conn:
        metrics.apply_estonia_ar_usd_conversion(
            duckdb_connection=conn,
            exchange_rates=_StubExchangeRates(),
        )
        counts = metrics.apply_estonia_ar_usd_conversion(
            duckdb_connection=conn,
            exchange_rates=_EmptyExchangeRates(),
        )
        wide = f"{tables.DLT_DATASET_NAME}.{tables.FINANCIAL_METRICS_WIDE_TABLE}"
        row = conn.execute(
            "select revenue_amount_usd, fx_rate_to_usd, fx_rate_date, fx_source "
            f"from {wide} where report_id = '1703729'"
        ).fetchone()

    assert counts == {"rate_pairs": 1, "rates_found": 0, "rows_converted": 0}
    assert row == (None, None, None, "")


def test_load_rates_batches_requests():
    requests = [
        metrics._request("EUR", f"20{10 + i // 12:02d}-{i % 12 + 1:02d}-28")
        for i in range(130)
    ]

    class _Spy:
        def __init__(self) -> None:
            self.sizes: list[int] = []

        def usd_rates(self, reqs):
            self.sizes.append(len(reqs))
            return {(r.currency, r.rate_date): _StubRate(Decimal("1.1"), r.rate_date) for r in reqs}

    spy = _Spy()
    out = metrics._load_rates(spy, requests)
    assert max(spy.sizes) <= metrics._RATE_REQUEST_BATCH
    assert len(spy.sizes) >= 3
    assert len(out) == 130


def test_export_metrics_replaces_clickhouse_table(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "estonia_ar_source.duckdb"
    _build_native(db_path)
    with duckdb.connect(str(db_path)) as conn:
        metrics.apply_estonia_ar_usd_conversion(
            duckdb_connection=conn, exchange_rates=_StubExchangeRates()
        )

    fake = _FakeClickHouse()

    @contextmanager
    def fake_get_connection(self):
        yield fake

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    with duckdb.connect(str(db_path), read_only=True) as conn:
        rows = ee_clickhouse.export_estonia_financial_metrics_clickhouse(
            duckdb_connection=conn,
            clickhouse=ClickhouseResource(host="localhost"),
        )
    assert rows == 1
    assert any("EXCHANGE TABLES" in stmt for stmt in fake.statements)
    _, inserted_rows = fake.inserts[0]
    assert len(inserted_rows[0]) == len(tables.EE_FINANCIAL_METRICS_EXPORT_COLUMNS)
