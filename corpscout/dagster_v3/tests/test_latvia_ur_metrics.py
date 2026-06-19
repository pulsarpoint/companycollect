from decimal import Decimal
from pathlib import Path

import duckdb

from dagster_v3.defs.latvia_ur import metrics, tables
from dagster_v3.defs.latvia_ur.financials import build_latvia_ur_financial_statements
from tests.test_latvia_ur_financials import _seed_raw  # reuse the raw seed helper


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


def test_metrics_scale_and_convert(tmp_path: Path):
    db_path = tmp_path / "latvia_ur_source.duckdb"
    _seed_raw(db_path)
    build_latvia_ur_financial_statements(database_path=db_path, source_run_id="run-1")

    stub = _StubExchangeRates()
    counts = metrics.build_latvia_ur_financial_metrics(
        database_path=db_path,
        source_run_id="run-1",
        exchange_rates=stub,
    )
    assert counts["metrics"] == 2
    # one EUR rate request per distinct period_end_date (both rows are 2016-12-31)
    assert ("EUR", "2016-12-31") in stub.requested

    wide = f"{tables.DLT_DATASET_NAME}.{tables.FINANCIAL_METRICS_WIDE_TABLE}"
    with duckdb.connect(str(db_path), read_only=True) as conn:
        row = conn.execute(
            "select revenue_amount_original, revenue_amount_usd, "
            "net_result_amount_original, net_result_amount_usd, fx_rate_to_usd, fx_source, "
            "total_assets_amount_original, equity_amount_original "
            f"from {wide} where statement_id = '709390'"
        ).fetchone()
        cols = [r[0] for r in conn.execute(f"describe {wide}").fetchall()]
    # ONES factor -> revenue 135 EUR; USD = 135 * 1.10 = 148.5
    assert row[0] == Decimal("135.00")
    assert row[1] == Decimal("148.50")
    assert row[2] == Decimal("-3860.00")  # net_income, signed
    assert row[3] == Decimal("-4246.00")  # -3860 * 1.10
    assert row[4] == Decimal("1.100000000000")
    assert row[5] == "TEST"
    assert row[6] == Decimal("5031.00")  # total_assets
    assert row[7] == Decimal("-15283.00")  # equity, signed
    assert set(cols) == set(tables.LV_FINANCIAL_METRICS_COLUMNS)


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
        exchange_rates=_StubExchangeRates(),
    )
    metrics_table = f"{tables.DLT_DATASET_NAME}.{tables.FINANCIAL_METRICS_WIDE_TABLE}"
    with duckdb.connect(str(db_path), read_only=True) as conn:
        value = conn.execute(
            f"select revenue_amount_original from {metrics_table} where statement_id = '709390'"
        ).fetchone()[0]
    assert value == Decimal("135.00")  # unknown factor -> 1, value unchanged
