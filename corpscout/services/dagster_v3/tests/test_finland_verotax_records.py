from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb
import pytest

from dagster_v3.defs.finland_verotax import tables
from dagster_v3.defs.finland_verotax.metrics import apply_finland_verotax_usd_conversion
from dagster_v3.defs.finland_verotax.records import (
    build_finland_verotax_tax_records,
    load_csv_path_into_raw_table,
)

HEADER_8 = (
    "Verovuosi | Skatteår;Y-tunnus | FO-nummer;"
    "Verovelvollisen nimi | Den skattskyldiges namn;"
    "Verotuskunta | Beskattningskommun;"
    "Verotettava tulo | Beskattningsbar inkomst;"
    "Maksuunpannut verot yhteensä | Debiterade skatter ;"
    "Veronpalautus | Skatteåterbäring;Jäännösvero | Kvarskatt"
)
HEADER_9 = (
    "Verovuosi | Skatteår;Y-tunnus | FO-nummer;"
    "Verovelvollisen nimi | Den skattskyldiges namn;"
    "Verotuskunta | Beskattningskommun;"
    "Verotettava tulo | Beskattningsbar inkomst;"
    "Maksuunpannut verot yhteensä | Debiterade skatter ;"
    "Ennakot yhteensä | Förskott sammanlagt;"
    "Veronpalautus | Skatteåterbäring;Jäännösvero | Kvarskatt"
)


def _write_latin1_csv(path: Path, lines: list[str]) -> Path:
    path.write_text("\n".join(lines) + "\n", encoding="latin-1")
    return path


def _load_year(
    connection: duckdb.DuckDBPyConnection, tmp_path: Path, year: int, lines: list[str]
) -> int:
    csv_path = _write_latin1_csv(tmp_path / f"{year}.csv", lines)
    return load_csv_path_into_raw_table(
        duckdb_connection=connection,
        csv_path=csv_path,
        raw_table=tables.raw_table_for_year(year),
        source_url=f"https://www.vero.fi/test/{year}.csv",
    )


@pytest.fixture
def connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(":memory:")


def test_load_rejects_unexpected_column_count(
    connection: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    csv_path = _write_latin1_csv(tmp_path / "bad.csv", ["a;b;c", "1;2;3"])
    with pytest.raises(ValueError, match="unexpected column count"):
        load_csv_path_into_raw_table(
            duckdb_connection=connection,
            csv_path=csv_path,
            raw_table="raw_bad",
            source_url="https://example.test/bad.csv",
        )


def test_load_rejects_empty_file(
    connection: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    csv_path = _write_latin1_csv(tmp_path / "empty.csv", [HEADER_8])
    with pytest.raises(ValueError, match="zero rows"):
        load_csv_path_into_raw_table(
            duckdb_connection=connection,
            csv_path=csv_path,
            raw_table="raw_empty",
            source_url="https://example.test/empty.csv",
        )


def test_load_and_build_across_8_and_9_column_years(
    connection: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    rows_2024 = _load_year(
        connection,
        tmp_path,
        2024,
        [
            HEADER_8,
            "2024;0104539-0;Testi Öy Åland;091 Helsinki;12345,67;2469,13;0,00;100,50",
            "2024;3134710-3;No Muni Oy;Ei kotikuntaa;0,00;0,00;0,00;0,00",
            '2024;3126326-8;UAB "Tokajus";200 Ulkomaat;125000,00;31652,50;0,00;31652,50',
        ],
    )
    rows_2021 = _load_year(
        connection,
        tmp_path,
        2021,
        [
            HEADER_9,
            "2021;0104539-0;Testi Öy Åland;837 Tampere;1000,00;200,00;150,00;0,00;50,00",
        ],
    )
    assert rows_2024 == 3
    assert rows_2021 == 1

    counts = build_finland_verotax_tax_records(
        duckdb_connection=connection,
        source_run_id="test-run",
        years=(2021, 2024),
    )
    assert counts == {"tax_records": 4, "duplicate_business_year_keys": 0}

    qualified = f"{tables.DLT_DATASET_NAME}.{tables.TAX_RECORDS_TABLE}"
    row = connection.execute(
        f"""
        select taxpayer_name, municipality_code, municipality_name, period_end_date,
               taxable_income_amount_original, taxes_total_amount_original,
               prepayments_total_amount_original, residual_tax_amount_original,
               currency, source_record_id
        from {qualified}
        where business_id = '0104539-0' and tax_year = 2024
        """
    ).fetchone()
    assert row == (
        "Testi Öy Åland",  # latin-1 characters survive the round trip
        "091",
        "Helsinki",
        date(2024, 12, 31),
        Decimal("12345.67"),
        Decimal("2469.13"),
        None,  # 8-column year: prepayments absent
        Decimal("100.50"),
        "EUR",
        "0104539-0:2024",
    )

    nine_col = connection.execute(
        f"""
        select prepayments_total_amount_original, municipality_code
        from {qualified}
        where business_id = '0104539-0' and tax_year = 2021
        """
    ).fetchone()
    assert nine_col == (Decimal("150.00"), "837")

    no_muni = connection.execute(
        f"""
        select municipality_code, municipality_name
        from {qualified}
        where business_id = '3134710-3'
        """
    ).fetchone()
    assert no_muni == ("", "Ei kotikuntaa")

    # Unquoted literal '"' in a company name survives verbatim (quote handling
    # is disabled — the source never CSV-quotes fields).
    quoted_name = connection.execute(
        f"select taxpayer_name from {qualified} where business_id = '3126326-8'"
    ).fetchone()
    assert quoted_name == ('UAB "Tokajus"',)


@dataclass
class _FakeRate:
    rate: Decimal
    rate_date: date
    source: str


class _FakeExchangeRates:
    def __init__(self, rates: dict[tuple[str, str], _FakeRate]) -> None:
        self._rates = rates

    def usd_rates(self, requests: list[Any]) -> dict[tuple[str, str], _FakeRate]:
        result: dict[tuple[str, str], _FakeRate] = {}
        missing = []
        for request in requests:
            key = (request.currency, str(request.rate_date))
            if key in self._rates:
                result[key] = self._rates[key]
            else:
                missing.append(key)
        if missing:
            raise LookupError(f"missing rates: {missing}")
        return result


def test_usd_conversion_fills_pairs(
    connection: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    _load_year(
        connection,
        tmp_path,
        2024,
        [HEADER_8, "2024;0104539-0;Testi Oy;091 Helsinki;1000,00;200,00;10,00;0,00"],
    )
    _load_year(
        connection,
        tmp_path,
        2023,
        [HEADER_8, "2023;0104539-0;Testi Oy;091 Helsinki;500,00;100,00;0,00;5,00"],
    )
    build_finland_verotax_tax_records(
        duckdb_connection=connection, source_run_id="run", years=(2023, 2024)
    )
    # 2023 rate is missing -> that row keeps native-only values.
    exchange_rates = _FakeExchangeRates(
        {
            ("EUR", "2024-12-31"): _FakeRate(
                rate=Decimal("1.10"), rate_date=date(2024, 12, 31), source="ecb"
            )
        }
    )
    counts = apply_finland_verotax_usd_conversion(
        duckdb_connection=connection, exchange_rates=exchange_rates
    )
    rerun_counts = apply_finland_verotax_usd_conversion(
        duckdb_connection=connection, exchange_rates=exchange_rates
    )
    assert rerun_counts == counts
    assert counts == {"rate_pairs": 2, "rates_found": 1, "rows_converted": 1}

    qualified = f"{tables.DLT_DATASET_NAME}.{tables.TAX_RECORDS_TABLE}"
    converted = connection.execute(
        f"""
        select taxable_income_amount_usd, fx_rate_to_usd, fx_source
        from {qualified} where tax_year = 2024
        """
    ).fetchone()
    assert converted == (Decimal("1100.00"), Decimal("1.10"), "ecb")

    unconverted = connection.execute(
        f"""
        select taxable_income_amount_usd, fx_rate_to_usd, fx_source
        from {qualified} where tax_year = 2023
        """
    ).fetchone()
    assert unconverted == (None, None, "")


def test_usd_conversion_without_rates_resets_usd_values(
    connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    _load_year(
        connection,
        tmp_path,
        2024,
        [HEADER_8, "2024;0104539-0;Testi Oy;091 Helsinki;1000,00;200,00;10,00;0,00"],
    )
    build_finland_verotax_tax_records(
        duckdb_connection=connection,
        source_run_id="run",
        years=(2024,),
    )
    apply_finland_verotax_usd_conversion(
        duckdb_connection=connection,
        exchange_rates=_FakeExchangeRates(
            {
                ("EUR", "2024-12-31"): _FakeRate(
                    rate=Decimal("1.10"),
                    rate_date=date(2024, 12, 31),
                    source="ecb",
                )
            }
        ),
    )

    counts = apply_finland_verotax_usd_conversion(
        duckdb_connection=connection,
        exchange_rates=_FakeExchangeRates({}),
    )
    qualified = f"{tables.DLT_DATASET_NAME}.{tables.TAX_RECORDS_TABLE}"
    row = connection.execute(
        "select taxable_income_amount_usd, fx_rate_to_usd, "
        f"fx_rate_date, fx_source from {qualified}"
    ).fetchone()

    assert counts == {"rate_pairs": 1, "rates_found": 0, "rows_converted": 0}
    assert row == (None, None, None, "")


def test_tax_records_columns_match_contract(
    connection: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    _load_year(
        connection,
        tmp_path,
        2024,
        [HEADER_8, "2024;0104539-0;Testi Oy;091 Helsinki;1,00;0,00;0,00;0,00"],
    )
    build_finland_verotax_tax_records(
        duckdb_connection=connection, source_run_id="run", years=(2024,)
    )
    columns = tuple(
        row[0]
        for row in connection.execute(
            """
            select column_name from information_schema.columns
            where table_schema = ? and table_name = ?
            order by ordinal_position
            """,
            [tables.DLT_DATASET_NAME, tables.TAX_RECORDS_TABLE],
        ).fetchall()
    )
    assert columns == tables.FI_TAX_RECORDS_COLUMNS
