from __future__ import annotations

import os
import re
from collections.abc import Iterable
from decimal import Decimal
from typing import Any

import clickhouse_connect

from exchange_rates.models import (
    ExchangeRateComponent,
    ExchangeRateRequest,
    UsdExchangeRate,
)

DEFAULT_EXCHANGE_RATES_TABLE = "reference.exchange_rates"
DEFAULT_CLICKHOUSE_HTTP_PORT = 8123


class ExchangeRateClient:
    def __init__(self, clickhouse_client: Any) -> None:
        self._clickhouse_client = clickhouse_client

    @classmethod
    def from_env(cls) -> ExchangeRateClient:
        return cls(
            clickhouse_connect.get_client(
                host=os.getenv("CLICKHOUSE_HOST", "localhost"),
                port=_int_env(
                    "CLICKHOUSE_HTTP_PORT",
                    _int_env("CLICKHOUSE_PORT", DEFAULT_CLICKHOUSE_HTTP_PORT),
                ),
                username=os.getenv("CLICKHOUSE_USER", "default"),
                password=os.getenv("CLICKHOUSE_PASSWORD", ""),
                database=os.getenv("CLICKHOUSE_DATABASE", "reference"),
                secure=_bool_env("CLICKHOUSE_SECURE", False),
            )
        )

    def usd_rate(self, *, currency: str, rate_date: str) -> UsdExchangeRate:
        rates = self.usd_rates([ExchangeRateRequest(currency=currency, rate_date=rate_date)])
        return rates[(currency.upper(), rate_date)]

    def usd_rates(
        self,
        requests: Iterable[ExchangeRateRequest],
    ) -> dict[tuple[str, str], UsdExchangeRate]:
        normalized_requests = [
            ExchangeRateRequest(currency=request.currency.upper(), rate_date=request.rate_date)
            for request in requests
        ]
        if not normalized_requests:
            return {}

        unique_requests = list(dict.fromkeys(normalized_requests))
        components = self._load_components_for_requests(requests=unique_requests)

        return {
            (request.currency, request.rate_date): self._resolve_usd_rate(
                request=request,
                components=components,
            )
            for request in normalized_requests
        }

    def convert_to_usd(
        self,
        *,
        amount: Decimal,
        currency: str,
        rate_date: str,
    ) -> Decimal:
        return self.usd_rate(currency=currency, rate_date=rate_date).convert(amount)

    def _load_components_for_requests(
        self,
        *,
        requests: list[ExchangeRateRequest],
    ) -> dict[tuple[str, str, str], ExchangeRateComponent]:
        requested_components_sql = _requested_components_sql(requests)
        result = self._clickhouse_client.query(
            f"""
            WITH requested_rates AS (
                {requested_components_sql}
            ),
            requested_counts AS (
                SELECT
                    request_currency,
                    requested_rate_date,
                    countDistinct(quote_currency) AS required_currency_count
                FROM requested_rates
                GROUP BY request_currency, requested_rate_date
            ),
            available_dates AS (
                SELECT
                    requested_rates.request_currency AS request_currency,
                    requested_rates.requested_rate_date AS requested_rate_date,
                    exchange_rates.rate_date AS rate_date,
                    if(exchange_rates.rate_date <= requested_rates.requested_rate_date, 0, 1) AS priority
                FROM requested_rates
                INNER JOIN {DEFAULT_EXCHANGE_RATES_TABLE} AS exchange_rates
                    ON exchange_rates.base_currency = 'EUR'
                   AND exchange_rates.quote_currency = requested_rates.quote_currency
                INNER JOIN requested_counts
                    ON requested_counts.request_currency = requested_rates.request_currency
                   AND requested_counts.requested_rate_date = requested_rates.requested_rate_date
                GROUP BY
                    requested_rates.request_currency,
                    requested_rates.requested_rate_date,
                    exchange_rates.rate_date,
                    priority,
                    requested_counts.required_currency_count
                HAVING countDistinct(exchange_rates.quote_currency) = requested_counts.required_currency_count
            ),
            selected_dates AS (
                SELECT
                    request_currency,
                    requested_rate_date,
                    if(
                        countIf(priority = 0) > 0,
                        maxIf(rate_date, priority = 0),
                        minIf(rate_date, priority = 1)
                    ) AS selected_rate_date
                FROM available_dates
                GROUP BY request_currency, requested_rate_date
            )
            SELECT
                selected_dates.request_currency,
                toString(selected_dates.requested_rate_date),
                toString(exchange_rates.rate_date),
                exchange_rates.quote_currency,
                toString(exchange_rates.rate),
                exchange_rates.source,
                exchange_rates.source_url,
                exchange_rates.source_payload_hash,
                toString(exchange_rates.pulled_at)
            FROM selected_dates
            INNER JOIN requested_rates
                ON requested_rates.request_currency = selected_dates.request_currency
               AND requested_rates.requested_rate_date = selected_dates.requested_rate_date
            INNER JOIN {DEFAULT_EXCHANGE_RATES_TABLE} AS exchange_rates
                ON exchange_rates.base_currency = 'EUR'
               AND exchange_rates.quote_currency = requested_rates.quote_currency
               AND exchange_rates.rate_date = selected_dates.selected_rate_date
            ORDER BY
                selected_dates.request_currency,
                selected_dates.requested_rate_date,
                exchange_rates.quote_currency
            """,
            parameters={},
        )
        rows = getattr(result, "result_rows", result)
        return {
            (str(row[0]).upper(), str(row[1]), str(row[3]).upper()): ExchangeRateComponent(
                rate_date=str(row[2]),
                base_currency="EUR",
                quote_currency=str(row[3]).upper(),
                rate=Decimal(str(row[4])),
                source=str(row[5]),
                source_url=str(row[6]),
                source_payload_hash=str(row[7]),
                pulled_at=str(row[8]),
            )
            for row in rows
        }

    def _resolve_usd_rate(
        self,
        *,
        request: ExchangeRateRequest,
        components: dict[tuple[str, str, str], ExchangeRateComponent],
    ) -> UsdExchangeRate:
        required_currencies = _required_currencies(request.currency)
        if not all(
            (request.currency, request.rate_date, currency) in components
            for currency in required_currencies
        ):
            raise LookupError(
                f"No USD exchange rate for {request.currency} on, before, or after {request.rate_date}"
            )

        usd_component = components[(request.currency, request.rate_date, "USD")]
        rate_date = usd_component.rate_date
        if request.currency == "USD":
            return UsdExchangeRate(
                currency=request.currency,
                requested_rate_date=request.rate_date,
                rate_date=rate_date,
                rate=Decimal("1"),
                eur_to_usd=usd_component.rate,
                eur_to_currency=usd_component.rate,
                source=usd_component.source,
                components=(usd_component,),
            )

        if request.currency == "EUR":
            eur_component = components[(request.currency, request.rate_date, "EUR")]
            return UsdExchangeRate(
                currency=request.currency,
                requested_rate_date=request.rate_date,
                rate_date=rate_date,
                rate=usd_component.rate,
                eur_to_usd=usd_component.rate,
                eur_to_currency=Decimal("1"),
                source=usd_component.source,
                components=(usd_component, eur_component),
            )

        currency_component = components[(request.currency, request.rate_date, request.currency)]
        return UsdExchangeRate(
            currency=request.currency,
            requested_rate_date=request.rate_date,
            rate_date=rate_date,
            rate=usd_component.rate / currency_component.rate,
            eur_to_usd=usd_component.rate,
            eur_to_currency=currency_component.rate,
            source=currency_component.source,
            components=(usd_component, currency_component),
        )


def _requested_components_sql(requests: list[ExchangeRateRequest]) -> str:
    rows: list[str] = []
    seen: set[tuple[str, str, str]] = set()
    for request in requests:
        request_currency = _sql_currency(request.currency)
        requested_rate_date = _sql_date(request.rate_date)
        for quote_currency in sorted(_required_currencies(request_currency)):
            key = (request_currency, requested_rate_date, quote_currency)
            if key in seen:
                continue
            rows.append(
                "SELECT "
                f"'{request_currency}' AS request_currency, "
                f"toDate('{requested_rate_date}') AS requested_rate_date, "
                f"'{quote_currency}' AS quote_currency"
            )
            seen.add(key)
    return "\nUNION ALL\n".join(rows)


def _sql_currency(currency: str) -> str:
    value = currency.upper()
    if not value.isalpha() or len(value) != 3:
        raise ValueError(f"Invalid currency code: {currency}")
    return value


def _sql_date(rate_date: str) -> str:
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", rate_date):
        raise ValueError(f"Invalid rate date: {rate_date}")
    return rate_date


def _required_currencies(currency: str) -> set[str]:
    if currency == "USD":
        return {"USD"}
    if currency == "EUR":
        return {"USD", "EUR"}
    return {"USD", currency}


def _latest_common_rate_date(
    *,
    requested_rate_date: str,
    currencies: set[str],
    components: dict[tuple[str, str], ExchangeRateComponent],
) -> str | None:
    candidate_dates = sorted(
        {
            rate_date
            for quote_currency, rate_date in components
            if quote_currency in currencies and rate_date <= requested_rate_date
        },
        reverse=True,
    )
    for rate_date in candidate_dates:
        if all((currency, rate_date) in components for currency in currencies):
            return rate_date
    fallback_dates = sorted(
        {
            rate_date
            for quote_currency, rate_date in components
            if quote_currency in currencies and rate_date > requested_rate_date
        }
    )
    for rate_date in fallback_dates:
        if all((currency, rate_date) in components for currency in currencies):
            return rate_date
    return None


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    return default if value is None or value.strip() == "" else int(value)


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}
