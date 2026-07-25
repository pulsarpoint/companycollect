from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any

import duckdb
import pytest

from dagster_v3.defs.finland_hilma import tables
from dagster_v3.defs.finland_hilma.parsing import (
    apply_finland_hilma_usd_conversion,
    build_finland_hilma_notices,
    load_export_bytes_into_raw_table,
    validate_export_header,
)


def _header() -> str:
    # Real portal header quirk: some titles contain quoted embedded newlines.
    titles = list(tables.EXPECTED_HEADER_TITLES)
    titles[33] = "In preparation,\n need and opportunities for innovation was considered."
    return ";".join(f'"{t}"' for t in titles)


def _row(overrides: dict[int, str]) -> str:
    cells = [""] * len(tables.RAW_COLUMNS)
    for index, value in overrides.items():
        cells[index] = value
    return ";".join(f'"{c}"' if ";" in c or '"' not in c else c for c in cells)


def _fixture_bytes() -> bytes:
    award = _row(
        {
            0: "2026-053663",
            2: "LOT-1",
            3: "2026-07-18T06:00:00Z",
            6: "Contract award notice %u2013 general directive, standard regime (TED eF29)",
            7: "Sähköenergian hankinta",
            15: "09310000",
            17: "TeeSe Botnia Oy Ab",
            22: "2859365-3",
            51: "Onninen Oy (1071207-9)//Ahlsell Oy (1819153-8)//No Id Winner Ky",
            52: "6700000",
            53: "EUR",
        }
    )
    older_duplicate = _row(
        {
            0: "2026-053663",
            2: "LOT-1",
            3: "2026-07-01T06:00:00Z",
            6: "Contract award notice %u2013 general directive, standard regime (TED eF29)",
            51: "Stale Winner Oy (1111111-1)",
            52: "1",
            53: "EUR",
        }
    )
    contract_notice = _row(
        {
            0: "2026-000001",
            3: "2026-07-18T07:00:00Z",
            6: "National contract notice (E3)",
            7: "Pieni hankinta – ääkköset öäå",
            22: "0000000-0",
            29: "50000",
            30: "EUR",
        }
    )
    text = "\r\n".join([_header(), award, contract_notice]) + "\r\n"
    older = "\r\n".join([_header(), older_duplicate]) + "\r\n"
    return text.encode("cp1252"), older.encode("cp1252")


@pytest.fixture
def connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(":memory:")


def test_validate_header_refuses_partial_exports() -> None:
    partial = ";".join(f'"{t}"' for t in tables.EXPECTED_HEADER_TITLES[:30])
    with pytest.raises(ValueError, match="30 columns"):
        validate_export_header(partial + "\r\n")


def test_load_build_dedup_and_winners(connection: duckdb.DuckDBPyConnection) -> None:
    newer, older = _fixture_bytes()
    # Older export uploaded first, newer second — dedup must keep the newer row.
    rows_old = load_export_bytes_into_raw_table(
        duckdb_connection=connection,
        csv_bytes=older,
        source_key="exports/20260701T000000Z_a.csv",
        replace=True,
    )
    rows_new = load_export_bytes_into_raw_table(
        duckdb_connection=connection,
        csv_bytes=newer,
        source_key="exports/20260718T000000Z_b.csv",
        replace=False,
    )
    assert (rows_old, rows_new) == (1, 2)

    counts = build_finland_hilma_notices(
        duckdb_connection=connection, source_run_id="run"
    )
    assert counts == {"notices": 2, "winners": 3, "winners_with_business_id": 2}

    notices = f"{tables.DLT_DATASET_NAME}.{tables.NOTICES_TABLE}"
    award = connection.execute(
        f"""
        select notice_type, is_award, buyer_business_id,
               procurement_value_amount_original, procurement_value_currency,
               winners_raw
        from {notices} where notice_number = '2026-053663'
        """
    ).fetchone()
    assert award[0] == "Contract award notice – general directive, standard regime (TED eF29)"
    assert award[1] == 1
    assert award[2] == "2859365-3"
    assert award[3] == Decimal("6700000.00")  # newer export won the dedup
    assert award[4] == "EUR"
    assert "Onninen" in award[5]

    plain = connection.execute(
        f"select is_award, notice_name_fi, notice_estimated_value_amount_original "
        f"from {notices} where notice_number = '2026-000001'"
    ).fetchone()
    assert plain[0] == 0
    assert plain[1] == "Pieni hankinta – ääkköset öäå"  # cp1252 round trip
    assert plain[2] == Decimal("50000.00")

    winners = connection.execute(
        f"""
        select winner_ordinal, winner_name, winner_business_id
        from {tables.DLT_DATASET_NAME}.{tables.NOTICE_WINNERS_TABLE}
        where notice_number = '2026-053663' order by winner_ordinal
        """
    ).fetchall()
    assert winners == [
        (1, "Onninen Oy", "1071207-9"),
        (2, "Ahlsell Oy", "1819153-8"),
        (3, "No Id Winner Ky", ""),
    ]


@dataclass
class _FakeRate:
    rate: Decimal
    rate_date: date
    source: str


class _FakeExchangeRates:
    def __init__(self, rates: dict[tuple[str, str], _FakeRate]) -> None:
        self._rates = rates

    def usd_rates(self, requests: list[Any]) -> dict[tuple[str, str], _FakeRate]:
        result = {}
        missing = []
        for request in requests:
            key = (request.currency, str(request.rate_date))
            if key in self._rates:
                result[key] = self._rates[key]
            else:
                missing.append(key)
        if missing:
            raise LookupError(f"missing: {missing}")
        return result


def test_usd_conversion_per_amount(connection: duckdb.DuckDBPyConnection) -> None:
    newer, _ = _fixture_bytes()
    load_export_bytes_into_raw_table(
        duckdb_connection=connection,
        csv_bytes=newer,
        source_key="exports/x.csv",
        replace=True,
    )
    build_finland_hilma_notices(duckdb_connection=connection, source_run_id="run")
    exchange_rates = _FakeExchangeRates(
        {
            ("EUR", "2026-07-18"): _FakeRate(
                rate=Decimal("1.05"), rate_date=date(2026, 7, 18), source="ecb"
            )
        }
    )
    counts = apply_finland_hilma_usd_conversion(
        duckdb_connection=connection,
        exchange_rates=exchange_rates,
    )
    rerun_counts = apply_finland_hilma_usd_conversion(
        duckdb_connection=connection,
        exchange_rates=exchange_rates,
    )
    assert rerun_counts == counts
    assert counts["procurement_values_converted"] == 1

    notices = f"{tables.DLT_DATASET_NAME}.{tables.NOTICES_TABLE}"
    award = connection.execute(
        f"select procurement_value_amount_usd, fx_rate_to_usd, fx_source "
        f"from {notices} where notice_number = '2026-053663'"
    ).fetchone()
    assert award == (Decimal("7035000.00"), Decimal("1.05"), "ecb")

    estimate = connection.execute(
        f"select notice_estimated_value_amount_usd, fx_rate_to_usd "
        f"from {notices} where notice_number = '2026-000001'"
    ).fetchone()
    # Estimated value converted with its own currency; the row-level fx trio
    # reflects the (empty) procurement-value currency -> stays NULL.
    assert estimate == (Decimal("52500.00"), None)


def test_usd_conversion_without_rates_resets_usd_values(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    newer, _ = _fixture_bytes()
    load_export_bytes_into_raw_table(
        duckdb_connection=connection,
        csv_bytes=newer,
        source_key="exports/x.csv",
        replace=True,
    )
    build_finland_hilma_notices(duckdb_connection=connection, source_run_id="run")
    apply_finland_hilma_usd_conversion(
        duckdb_connection=connection,
        exchange_rates=_FakeExchangeRates(
            {
                ("EUR", "2026-07-18"): _FakeRate(
                    rate=Decimal("1.05"),
                    rate_date=date(2026, 7, 18),
                    source="ecb",
                )
            }
        ),
    )

    counts = apply_finland_hilma_usd_conversion(
        duckdb_connection=connection,
        exchange_rates=_FakeExchangeRates({}),
    )
    notices = f"{tables.DLT_DATASET_NAME}.{tables.NOTICES_TABLE}"
    row = connection.execute(
        "select procurement_value_amount_usd, fx_rate_to_usd, "
        f"fx_rate_date, fx_source from {notices} "
        "where notice_number = '2026-053663'"
    ).fetchone()

    assert counts == {
        "rate_pairs": 1,
        "rates_found": 0,
        "procurement_values_converted": 0,
    }
    assert row == (None, None, None, "")


def test_notices_columns_match_contract(connection: duckdb.DuckDBPyConnection) -> None:
    newer, _ = _fixture_bytes()
    load_export_bytes_into_raw_table(
        duckdb_connection=connection,
        csv_bytes=newer,
        source_key="exports/x.csv",
        replace=True,
    )
    build_finland_hilma_notices(duckdb_connection=connection, source_run_id="run")
    for duckdb_table, contract in (
        (tables.NOTICES_TABLE, tables.FI_HILMA_NOTICES_COLUMNS),
        (tables.NOTICE_WINNERS_TABLE, tables.FI_HILMA_NOTICE_WINNERS_COLUMNS),
    ):
        columns = tuple(
            row[0]
            for row in connection.execute(
                """
                select column_name from information_schema.columns
                where table_schema = ? and table_name = ?
                order by ordinal_position
                """,
                [tables.DLT_DATASET_NAME, duckdb_table],
            ).fetchall()
        )
        assert columns == contract
