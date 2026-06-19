from pathlib import Path

import duckdb

from dagster_v3.defs.latvia_ur import financials, tables


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
