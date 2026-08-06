from datetime import date
from decimal import Decimal
from pathlib import Path

import duckdb


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
                Decimal("0.10"),
                request.rate_date,
            )
            for request in requests
        }


def test_facts_usd_conversion_converts_monetary_rows_and_is_idempotent(
    tmp_path: Path,
) -> None:
    from dagster_v3.defs.sweden_financial.usd_conversion import (
        apply_sweden_financial_facts_usd_conversion,
    )

    with duckdb.connect(str(tmp_path / "source.duckdb")) as connection:
        _seed_facts(connection)
        exchange_rates = _StubExchangeRates()

        first_counts = apply_sweden_financial_facts_usd_conversion(
            duckdb_connection=connection,
            exchange_rates=exchange_rates,
        )
        second_counts = apply_sweden_financial_facts_usd_conversion(
            duckdb_connection=connection,
            exchange_rates=exchange_rates,
        )
        rows = connection.execute(
            """
            select source_record_id, amount_usd, fx_rate_to_usd,
                   fx_rate_date, fx_source
            from sweden_financial.facts
            order by source_record_id
            """
        ).fetchall()

    assert (
        first_counts
        == second_counts
        == {
            "rate_pairs": 1,
            "rates_found": 1,
            "monetary_rows": 2,
            "rows_converted": 2,
            "rows_without_fx": 0,
        }
    )
    assert exchange_rates.requested == [
        ("SEK", "2025-12-31"),
        ("SEK", "2025-12-31"),
    ]
    assert rows == [
        (
            "sek-assets",
            Decimal("1787636.8000000000"),
            Decimal("0.100000000000"),
            date(2025, 12, 31),
            "TEST",
        ),
        (
            "sek-revenue",
            Decimal("50966.8000000000"),
            Decimal("0.100000000000"),
            date(2025, 12, 31),
            "TEST",
        ),
        ("unitless-solvency", None, None, None, ""),
    ]


def test_facts_usd_conversion_clears_stale_values_when_rate_is_unavailable(
    tmp_path: Path,
) -> None:
    from dagster_v3.defs.sweden_financial.usd_conversion import (
        apply_sweden_financial_facts_usd_conversion,
    )

    class _MissingExchangeRates:
        def usd_rates(self, requests):
            raise LookupError("missing test rate")

    with duckdb.connect(str(tmp_path / "source.duckdb")) as connection:
        _seed_facts(connection)
        connection.execute(
            """
            update sweden_financial.facts
            set amount_usd = 1,
                fx_rate_to_usd = 1,
                fx_rate_date = date '2020-01-01',
                fx_source = 'STALE'
            where currency = 'SEK'
            """
        )

        counts = apply_sweden_financial_facts_usd_conversion(
            duckdb_connection=connection,
            exchange_rates=_MissingExchangeRates(),
        )
        rows = connection.execute(
            """
            select amount_usd, fx_rate_to_usd, fx_rate_date, fx_source
            from sweden_financial.facts
            where currency = 'SEK'
            order by source_record_id
            """
        ).fetchall()

    assert counts == {
        "rate_pairs": 1,
        "rates_found": 0,
        "monetary_rows": 2,
        "rows_converted": 0,
        "rows_without_fx": 2,
    }
    assert rows == [(None, None, None, ""), (None, None, None, "")]


def _seed_facts(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute("create schema sweden_financial")
    connection.execute(
        """
        create table sweden_financial.facts (
            source_record_id varchar,
            report_period_end date,
            amount_original decimal(38, 10),
            amount_usd decimal(38, 10),
            currency varchar,
            fx_rate_to_usd decimal(38, 12),
            fx_rate_date date,
            fx_source varchar
        )
        """
    )
    connection.execute(
        """
        insert into sweden_financial.facts values
            ('sek-revenue', date '2025-12-31', 509668, NULL, 'SEK', NULL, NULL, ''),
            ('sek-assets', date '2025-12-31', 17876368, NULL, 'SEK', NULL, NULL, ''),
            ('unitless-solvency', date '2025-12-31', 42.5, NULL, NULL, NULL, NULL, '')
        """
    )
