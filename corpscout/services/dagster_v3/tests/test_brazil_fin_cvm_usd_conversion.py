from datetime import date
from decimal import Decimal
from pathlib import Path

import duckdb

from dagster_v3.defs.brazil_financial.cvm.parsing import (
    BRAZIL_CVM_DUCKDB_SCHEMA,
    DFP_STATEMENT_ROWS_TABLE,
)


class _StubRate:
    def __init__(self, rate: Decimal, rate_date: str) -> None:
        self.rate = rate
        self.rate_date = rate_date
        self.source = "TEST"


class _StubExchangeRates:
    def __init__(self) -> None:
        self.requested: list[tuple[str, str]] = []

    def usd_rates(self, requests):
        for request in requests:
            self.requested.append((request.currency, request.rate_date))
        return {
            (request.currency, request.rate_date): _StubRate(
                Decimal("0.20"),
                request.rate_date,
            )
            for request in requests
        }


def test_statement_rows_usd_conversion_maps_real_to_brl_and_applies_scale(
    tmp_path: Path,
) -> None:
    from dagster_v3.defs.brazil_financial.cvm.usd_conversion import (
        apply_brazil_cvm_statement_rows_usd_conversion,
    )

    db_path = tmp_path / "source.duckdb"
    with duckdb.connect(str(db_path)) as connection:
        _seed_statement_rows(connection)
        exchange_rates = _StubExchangeRates()
        counts = apply_brazil_cvm_statement_rows_usd_conversion(
            duckdb_connection=connection,
            exchange_rates=exchange_rates,
        )

        rows = connection.execute(
            f"""
            select source_record_id, amount_original, amount_usd, fx_rate_to_usd,
                   fx_rate_date, fx_source
            from {BRAZIL_CVM_DUCKDB_SCHEMA}.{DFP_STATEMENT_ROWS_TABLE}
            order by source_record_id
            """
        ).fetchall()

    assert counts == {"rate_pairs": 1, "rates_found": 1, "rows_converted": 2}
    assert exchange_rates.requested == [("BRL", "2026-03-31")]
    assert rows == [
        (
            "row-brl",
            Decimal("100.0000000000"),
            Decimal("20.000000"),
            Decimal("0.200000000000"),
            date(2026, 3, 31),
            "TEST",
        ),
        (
            "row-real-mil",
            Decimal("2148915.0000000000"),
            Decimal("429783000.000000"),
            Decimal("0.200000000000"),
            date(2026, 3, 31),
            "TEST",
        ),
    ]


def test_statement_rows_usd_conversion_without_rates_leaves_usd_null(
    tmp_path: Path,
) -> None:
    from dagster_v3.defs.brazil_financial.cvm.usd_conversion import (
        apply_brazil_cvm_statement_rows_usd_conversion,
    )

    class _EmptyRates:
        def usd_rates(self, requests):
            return {}

    db_path = tmp_path / "source.duckdb"
    with duckdb.connect(str(db_path)) as connection:
        _seed_statement_rows(connection)
        counts = apply_brazil_cvm_statement_rows_usd_conversion(
            duckdb_connection=connection,
            exchange_rates=_EmptyRates(),
        )
        amount_usd = connection.execute(
            f"""
            select amount_usd
            from {BRAZIL_CVM_DUCKDB_SCHEMA}.{DFP_STATEMENT_ROWS_TABLE}
            where source_record_id = 'row-real-mil'
            """
        ).fetchone()[0]

    assert counts == {"rate_pairs": 1, "rates_found": 0, "rows_converted": 0}
    assert amount_usd is None


def test_statement_rows_usd_conversion_handles_large_scaled_amounts(
    tmp_path: Path,
) -> None:
    from dagster_v3.defs.brazil_financial.cvm.usd_conversion import (
        apply_brazil_cvm_statement_rows_usd_conversion,
    )

    class _LargeAmountRates:
        def usd_rates(self, requests):
            return {
                (request.currency, request.rate_date): _StubRate(
                    Decimal("0.187263357173"),
                    request.rate_date,
                )
                for request in requests
            }

    db_path = tmp_path / "source.duckdb"
    with duckdb.connect(str(db_path)) as connection:
        _seed_statement_rows(connection)
        connection.execute(
            f"""
            insert into {BRAZIL_CVM_DUCKDB_SCHEMA}.{DFP_STATEMENT_ROWS_TABLE}
            values (
                'row-large-negative',
                'REAL',
                'MIL',
                date '2026-03-31',
                cast('-165335000000000010.0000000000' as decimal(38, 10)),
                NULL,
                NULL,
                NULL,
                ''
            )
            """
        )

        counts = apply_brazil_cvm_statement_rows_usd_conversion(
            duckdb_connection=connection,
            exchange_rates=_LargeAmountRates(),
        )
        amount_usd = connection.execute(
            f"""
            select amount_usd
            from {BRAZIL_CVM_DUCKDB_SCHEMA}.{DFP_STATEMENT_ROWS_TABLE}
            where source_record_id = 'row-large-negative'
            """
        ).fetchone()[0]

    assert counts == {"rate_pairs": 1, "rates_found": 1, "rows_converted": 3}
    assert amount_usd == Decimal("-30961187158197952674.004992")


def test_statement_rows_usd_conversion_requires_source_table(
    tmp_path: Path,
) -> None:
    import pytest

    from dagster_v3.defs.brazil_financial.cvm.usd_conversion import (
        apply_brazil_cvm_statement_rows_usd_conversion,
    )

    db_path = tmp_path / "source.duckdb"
    with duckdb.connect(str(db_path)) as connection:
        with pytest.raises(RuntimeError, match="brazil_cvm.dfp_statement_rows"):
            apply_brazil_cvm_statement_rows_usd_conversion(
                duckdb_connection=connection,
                exchange_rates=_StubExchangeRates(),
            )


def _seed_statement_rows(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(f"create schema if not exists {BRAZIL_CVM_DUCKDB_SCHEMA}")
    connection.execute(
        f"""
        create table {BRAZIL_CVM_DUCKDB_SCHEMA}.{DFP_STATEMENT_ROWS_TABLE} (
            source_record_id varchar,
            currency varchar,
            scale varchar,
            period_end_date date,
            amount_original decimal(38, 10),
            amount_usd decimal(38, 6),
            fx_rate_to_usd decimal(38, 12),
            fx_rate_date date,
            fx_source varchar
        )
        """
    )
    connection.executemany(
        f"""
        insert into {BRAZIL_CVM_DUCKDB_SCHEMA}.{DFP_STATEMENT_ROWS_TABLE}
        values (?, ?, ?, cast(? as date), cast(? as decimal(38, 10)),
                NULL, NULL, NULL, '')
        """,
        [
            ("row-real-mil", "REAL", "MIL", "2026-03-31", "2148915.0000000000"),
            ("row-brl", "BRL", "UNIDADE", "2026-03-31", "100.0000000000"),
        ],
    )
