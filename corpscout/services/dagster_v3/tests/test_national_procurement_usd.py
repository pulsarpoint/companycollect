from dataclasses import dataclass
from datetime import date
from decimal import Decimal

import duckdb

from dagster_v3.defs.common.eur_usd import apply_eur_usd_conversion


@dataclass(frozen=True)
class _Rate:
    rate: Decimal
    rate_date: date
    source: str = "fixture"


class _ExchangeRates:
    def usd_rates(self, requests):
        return {
            (request.currency, request.rate_date): _Rate(
                rate=Decimal("1.20"),
                rate_date=date.fromisoformat(request.rate_date),
            )
            for request in requests
        }


def test_eur_values_are_converted_on_the_first_available_source_date() -> None:
    connection = duckdb.connect(":memory:")
    connection.execute(
        """
        CREATE TABLE awards
        (
            notification_date DATE,
            publication_date DATE,
            amount_eur DECIMAL(38, 2),
            amount_usd DECIMAL(38, 2)
        )
        """
    )
    connection.execute(
        """
        INSERT INTO awards VALUES
            (DATE '2026-01-05', DATE '2026-01-07', 100.00, NULL),
            (NULL, DATE '2026-01-08', 50.00, 999.00)
        """
    )

    counts = apply_eur_usd_conversion(
        duckdb_connection=connection,
        exchange_rates=_ExchangeRates(),
        qualified_table="awards",
        rate_date_columns=("notification_date", "publication_date"),
        amount_columns=(("amount_eur", "amount_usd"),),
    )

    assert connection.execute(
        "SELECT amount_usd FROM awards ORDER BY publication_date"
    ).fetchall() == [(Decimal("120.00"),), (Decimal("60.00"),)]
    assert counts == {
        "rate_dates": 2,
        "rates_found": 2,
        "converted_amount_usd": 2,
    }
