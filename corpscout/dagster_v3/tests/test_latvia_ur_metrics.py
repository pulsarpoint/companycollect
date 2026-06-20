from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path

import duckdb
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.latvia_ur import clickhouse as latvia_ur_clickhouse
from dagster_v3.defs.latvia_ur import metrics, tables
from dagster_v3.defs.latvia_ur.financials import build_latvia_ur_financial_statements
from tests.test_latvia_ur_financials import _seed_raw  # reuse the raw seed helper


class _FakeClickHouse:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.inserts: list[tuple[str, list]] = []

    def execute(self, sql, params=None):
        if "system.tables" in sql:
            return [(tables.LV_FINANCIAL_METRICS_TABLE,)]
        if isinstance(params, (list, tuple)):
            self.inserts.append((sql, list(params)))
            return None
        self.statements.append(sql)
        return None


class _StubRate:
    def __init__(self, rate: Decimal, rate_date: str) -> None:
        self.rate = rate
        self.rate_date = rate_date
        self.source = "TEST"

    def convert(self, amount):
        return None if amount is None else (amount * self.rate)


class _StubExchangeRates:
    def __init__(self) -> None:
        self.requested: list[tuple[str, str]] = []

    def usd_rates(self, requests):
        for request in requests:
            self.requested.append((request.currency, request.rate_date))
        return {
            (r.currency, r.rate_date): _StubRate(Decimal("1.10"), r.rate_date)
            for r in requests
        }


def _build_native_metrics(db_path: Path) -> None:
    _seed_raw(db_path)
    build_latvia_ur_financial_statements(database_path=db_path, source_run_id="run-1")
    metrics.build_latvia_ur_financial_metrics(database_path=db_path, source_run_id="run-1")


def test_build_native_metrics_no_usd(tmp_path: Path):
    db_path = tmp_path / "latvia_ur_source.duckdb"
    _build_native_metrics(db_path)

    wide = f"{tables.DLT_DATASET_NAME}.{tables.FINANCIAL_METRICS_WIDE_TABLE}"
    with duckdb.connect(str(db_path), read_only=True) as conn:
        row = conn.execute(
            "select revenue_amount_original, revenue_amount_usd, "
            "net_result_amount_original, net_result_amount_usd, fx_rate_to_usd, fx_source, "
            "currency, total_assets_amount_original, equity_amount_original "
            f"from {wide} where statement_id = '709390'"
        ).fetchone()
        cols = [r[0] for r in conn.execute(f"describe {wide}").fetchall()]
    # native scaled originals present; USD/fx left empty by the build step
    assert row[0] == Decimal("135.00")  # revenue (ONES factor)
    assert row[1] is None  # revenue_usd not filled yet
    assert row[2] == Decimal("-3860.00")  # net_result, signed
    assert row[3] is None  # net_result_usd not filled yet
    assert row[4] is None  # fx_rate_to_usd
    assert row[5] == ""  # fx_source empty
    assert row[6] == "EUR"
    assert row[7] == Decimal("5031.00")
    assert row[8] == Decimal("-15283.00")
    assert set(cols) == set(tables.LV_FINANCIAL_METRICS_COLUMNS)


def test_usd_conversion_step_fills_usd(tmp_path: Path):
    db_path = tmp_path / "latvia_ur_source.duckdb"
    _build_native_metrics(db_path)

    stub = _StubExchangeRates()
    counts = metrics.apply_latvia_ur_usd_conversion(
        database_path=db_path,
        exchange_rates=stub,
    )
    assert counts["rate_pairs"] == 1  # both rows are EUR / 2016-12-31
    assert counts["rates_found"] == 1
    assert counts["rows_converted"] == 2
    assert ("EUR", "2016-12-31") in stub.requested

    wide = f"{tables.DLT_DATASET_NAME}.{tables.FINANCIAL_METRICS_WIDE_TABLE}"
    with duckdb.connect(str(db_path), read_only=True) as conn:
        row = conn.execute(
            "select revenue_amount_original, revenue_amount_usd, "
            "net_result_amount_usd, fx_rate_to_usd, fx_source "
            f"from {wide} where statement_id = '709390'"
        ).fetchone()
    assert row[0] == Decimal("135.00")  # original unchanged
    assert row[1] == Decimal("148.50")  # 135 * 1.10
    assert row[2] == Decimal("-4246.00")  # -3860 * 1.10, signed
    assert row[3] == Decimal("1.100000000000")
    assert row[4] == "TEST"


def test_usd_conversion_no_rates_leaves_usd_null(tmp_path: Path):
    db_path = tmp_path / "latvia_ur_source.duckdb"
    _build_native_metrics(db_path)

    class _EmptyRates:
        def usd_rates(self, requests):
            return {}

    counts = metrics.apply_latvia_ur_usd_conversion(
        database_path=db_path, exchange_rates=_EmptyRates()
    )
    assert counts["rows_converted"] == 0
    wide = f"{tables.DLT_DATASET_NAME}.{tables.FINANCIAL_METRICS_WIDE_TABLE}"
    with duckdb.connect(str(db_path), read_only=True) as conn:
        usd = conn.execute(
            f"select revenue_amount_usd from {wide} where statement_id = '709390'"
        ).fetchone()[0]
    assert usd is None


def test_unknown_rounding_factor_defaults_to_one(tmp_path: Path):
    db_path = tmp_path / "latvia_ur_source.duckdb"
    _seed_raw(db_path)
    build_latvia_ur_financial_statements(database_path=db_path, source_run_id="run-1")
    # rewrite rounded_to_nearest to an unknown unit on the wide table
    wide = f"{tables.DLT_DATASET_NAME}.{tables.FINANCIAL_STATEMENTS_WIDE_TABLE}"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(f"update {wide} set rounded_to_nearest = 'WEIRD'")

    metrics.build_latvia_ur_financial_metrics(
        database_path=db_path,
        source_run_id="run-1",
    )
    metrics_table = f"{tables.DLT_DATASET_NAME}.{tables.FINANCIAL_METRICS_WIDE_TABLE}"
    with duckdb.connect(str(db_path), read_only=True) as conn:
        value = conn.execute(
            f"select revenue_amount_original from {metrics_table} where statement_id = '709390'"
        ).fetchone()[0]
    assert value == Decimal("135.00")  # unknown factor -> 1, value unchanged


def test_export_financial_metrics_replaces_clickhouse_table(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "latvia_ur_source.duckdb"
    _build_native_metrics(db_path)
    metrics.apply_latvia_ur_usd_conversion(
        database_path=db_path, exchange_rates=_StubExchangeRates()
    )

    fake = _FakeClickHouse()

    @contextmanager
    def fake_get_connection(self):
        yield fake

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    rows = latvia_ur_clickhouse.export_latvia_ur_clickhouse_financial_metrics(
        database_path=db_path,
        clickhouse=ClickhouseResource(host="localhost"),
    )

    assert rows == 2
    assert any("EXCHANGE TABLES" in stmt for stmt in fake.statements)
    assert fake.inserts, "expected a batched INSERT of the metrics"
    _, inserted_rows = fake.inserts[0]
    assert len(inserted_rows) == 2
    assert len(inserted_rows[0]) == len(tables.LV_FINANCIAL_METRICS_COLUMNS)
