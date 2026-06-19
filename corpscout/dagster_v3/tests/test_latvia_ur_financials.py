from contextlib import contextmanager
from pathlib import Path

import duckdb
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.latvia_ur import clickhouse as latvia_ur_clickhouse
from dagster_v3.defs.latvia_ur import financials, tables


class _FakeClickHouse:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.inserts: list[tuple[str, list]] = []

    def execute(self, sql, params=None):
        if "system.tables" in sql:
            return [(tables.LV_FINANCIAL_STATEMENTS_TABLE,)]
        if isinstance(params, (list, tuple)):
            self.inserts.append((sql, list(params)))
            return None
        self.statements.append(sql)
        return None


class _FakeResponse:
    def __init__(self, body: bytes) -> None:
        self._body = body
        self.content = body

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int = 0):
        yield self._body


class _FakeSession:
    def __init__(self, body: bytes) -> None:
        self._body = body

    def get(self, url: str, *, timeout: int, stream: bool = False) -> _FakeResponse:
        return _FakeResponse(self._body)


BALANCE_CSV = (
    "statement_id;file_id;cash;marketable_securities;accounts_receivable;inventories;"
    "total_current_assets;investments;fixed_assets;intangible_assets;"
    "total_non_current_assets;total_assets;future_housing_repairs_payments;"
    "current_liabilities;non_current_liabilities;provisions;equity;total_equities\n"
    "709390;16544390;100;0;0;0;5031;0;0;0;0;5031;0;0;0;0;-15283;5031\n"
    "709391;16544392;0;0;0;0;0;0;0;0;0;0;0;0;0;0;-608;0\n"
)


def test_load_financial_csv_into_duckdb(tmp_path: Path):
    db_path = tmp_path / "latvia_ur_source.duckdb"
    rows = financials.load_latvia_ur_financial_csv(
        database_path=db_path,
        download_url="https://data.gov.lv/example/balance_sheets.csv",
        raw_table=tables.BALANCE_SHEETS_RAW_TABLE,
        session=_FakeSession(BALANCE_CSV.encode("utf-8")),
    )
    assert rows == 2
    qualified = f"{tables.DLT_DATASET_NAME}.{tables.BALANCE_SHEETS_RAW_TABLE}"
    with duckdb.connect(str(db_path), read_only=True) as conn:
        # all_varchar keeps values as text (incl. the negative equity); pivot casts later.
        equity = conn.execute(
            f"select equity from {qualified} where statement_id = '709390'"
        ).fetchone()
        cols = [
            row[0]
            for row in conn.execute(f"describe {qualified}").fetchall()
        ]
    assert equity == ("-15283",)
    assert cols[:2] == ["statement_id", "file_id"]


def test_load_is_idempotent_replace(tmp_path: Path):
    db_path = tmp_path / "latvia_ur_source.duckdb"
    for _ in range(2):
        rows = financials.load_latvia_ur_financial_csv(
            database_path=db_path,
            download_url="https://data.gov.lv/example/balance_sheets.csv",
            raw_table=tables.BALANCE_SHEETS_RAW_TABLE,
            session=_FakeSession(BALANCE_CSV.encode("utf-8")),
        )
    assert rows == 2
    qualified = f"{tables.DLT_DATASET_NAME}.{tables.BALANCE_SHEETS_RAW_TABLE}"
    with duckdb.connect(str(db_path), read_only=True) as conn:
        assert conn.execute(f"select count(*) from {qualified}").fetchone() == (2,)


FS_CSV = (
    "id;file_id;legal_entity_registration_number;source_schema;source_type;year;"
    "year_started_on;year_ended_on;employees;rounded_to_nearest;currency;created_at\n"
    "709390;16544390;40103504912;DokGPUIENv1;UGP;2016;2016-01-01;2016-12-31;3;ONES;EUR;"
    "2020-10-30 23:33:27.773\n"
    "709391;16544392;40103466358;DokGPUIENv1;UGP;2016;2016-01-01;2016-12-31;0;ONES;EUR;"
    "2020-10-30 23:33:28.000\n"
)
INCOME_CSV = (
    "statement_id;file_id;net_turnover;by_nature_inventory_change;"
    "by_nature_long_term_investment_expenses;by_nature_other_operating_revenues;"
    "by_nature_material_expenses;by_nature_labour_expenses;"
    "by_nature_depreciation_expenses;by_function_cost_of_goods_sold;"
    "by_function_gross_profit;by_function_selling_expenses;"
    "by_function_administrative_expenses;by_function_other_operating_revenues;"
    "other_operating_expenses;equity_investment_earnings;"
    "other_long_term_investment_earnings;other_interest_revenues;"
    "investment_fair_value_adjustments;interest_expenses;extra_revenues;extra_expenses;"
    "income_before_income_taxes;provision_for_income_taxes;income_after_income_taxes;"
    "other_taxes;extra_dividends;net_income\n"
    "709390;16544390;135;0;0;0;0;0;0;0;0;0;0;0;0;0;0;0;0;0;0;0;-3860;0;-3860;0;0;-3860\n"
)
# cash flow only for statement 709391, plus an ORPHAN (statement 999) with no spine row.
CASHFLOW_CSV = (
    "statement_id;file_id;cfo_dm_cash_received_from_customers;"
    "cfo_dm_cash_paid_to_suppliers_employees;cfo_dm_other_cash_received_paid;"
    "cfo_dm_operating_cash_flow;cfo_dm_interest_paid;cfo_dm_income_taxes_paid;"
    "cfo_dm_extra_items_cash_flow;cfo_dm_net_operating_cash_flow;"
    "cfo_im_income_before_income_taxes;cfo_im_income_before_changes_in_working_capital;"
    "cfo_im_operating_cash_flow;cfo_im_interest_paid;cfo_im_income_taxes_paid;"
    "cfo_im_extra_items_cash_flow;cfo_im_net_operating_cash_flow;"
    "cfi_acquisition_of_stocks_shares;cfi_sale_proceeds_from_stocks_shares;"
    "cfi_acquisition_of_fixed_assets_intangible_assets;"
    "cfi_sale_proceeds_from_fixed_assets_intangible_assets;cfi_loans_made;"
    "cfi_repayments_of_loans_received;cfi_interest_received;cfi_dividends_received;"
    "cfi_net_investing_cash_flow;"
    "cff_proceeds_from_stocks_bonds_issuance_or_contributed_capital;cff_loans_received;"
    "cff_subsidies_grants_donations_received;cff_repayments_of_loans_made;"
    "cff_repayments_of_lease_obligations;cff_dividends_paid;cff_net_financing_cash_flow;"
    "effect_of_exchange_rate_change;net_increase;at_beginning_of_year;at_end_of_year\n"
    "709391;16544392;1;2;3;4;5;6;7;8;9;10;11;12;13;14;15;16;17;18;19;20;21;22;23;24;25;26;"
    "27;28;29;"
    "30;31;32;33;34;35\n"
    "999;99;1;2;3;4;5;6;7;8;9;10;11;12;13;14;15;16;17;18;19;20;21;22;23;24;25;26;27;28;29;"
    "30;31;32;33;34;35\n"
)


def _seed_raw(db_path: Path) -> None:
    for raw_table, csv in (
        (tables.FINANCIAL_STATEMENTS_RAW_TABLE, FS_CSV),
        (tables.BALANCE_SHEETS_RAW_TABLE, BALANCE_CSV),
        (tables.INCOME_STATEMENTS_RAW_TABLE, INCOME_CSV),
        (tables.CASH_FLOW_STATEMENTS_RAW_TABLE, CASHFLOW_CSV),
    ):
        financials.load_latvia_ur_financial_csv(
            database_path=db_path,
            download_url="https://data.gov.lv/example.csv",
            raw_table=raw_table,
            session=_FakeSession(csv.encode("utf-8")),
        )


def test_pivot_builds_wide_table_and_counts_orphans(tmp_path: Path):
    db_path = tmp_path / "latvia_ur_source.duckdb"
    _seed_raw(db_path)

    counts = financials.build_latvia_ur_financial_statements(
        database_path=db_path,
        source_run_id="run-1",
    )

    assert counts["financial_statements"] == 2
    assert counts["cash_flow_orphans"] == 1  # statement 999 has no spine row
    assert counts["balance_orphans"] == 0
    assert counts["income_orphans"] == 0

    wide = f"{tables.DLT_DATASET_NAME}.{tables.FINANCIAL_STATEMENTS_WIDE_TABLE}"
    with duckdb.connect(str(db_path), read_only=True) as conn:
        row = conn.execute(
            f"select regcode, fiscal_year, employees, total_assets, equity, net_income, "
            f"net_increase from {wide} where statement_id = '709390'"
        ).fetchone()
        # statement 709390 has balance + income but no cash flow -> cash flow is NULL
        cols = [r[0] for r in conn.execute(f"describe {wide}").fetchall()]
    assert row[0] == "40103504912"  # regcode
    assert row[1] == 2016  # fiscal_year cast to int
    assert row[2] == 3  # employees cast to bigint
    assert row[3] == 5031  # total_assets
    assert row[4] == -15283  # equity (signed)
    assert row[5] == -3860  # net_income
    assert row[6] is None  # net_increase (no cash flow for this statement)
    assert set(cols) == set(tables.LV_FINANCIAL_STATEMENTS_COLUMNS)


def test_export_financial_statements_replaces_clickhouse_table(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "latvia_ur_source.duckdb"
    _seed_raw(db_path)
    financials.build_latvia_ur_financial_statements(
        database_path=db_path, source_run_id="run-1"
    )

    fake = _FakeClickHouse()

    @contextmanager
    def fake_get_connection(self):
        yield fake

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    rows = latvia_ur_clickhouse.export_latvia_ur_clickhouse_financial_statements(
        database_path=db_path,
        clickhouse=ClickhouseResource(host="localhost"),
    )

    assert rows == 2
    assert any("EXCHANGE TABLES" in stmt for stmt in fake.statements)
    assert fake.inserts, "expected a batched INSERT of the wide statements"
    _, inserted_rows = fake.inserts[0]
    assert len(inserted_rows) == 2
    assert len(inserted_rows[0]) == len(tables.LV_FINANCIAL_STATEMENTS_COLUMNS)
