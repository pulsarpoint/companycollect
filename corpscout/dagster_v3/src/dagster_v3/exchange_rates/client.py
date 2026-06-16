from __future__ import annotations

import os
from collections.abc import Iterable
from decimal import Decimal
from typing import Any

import clickhouse_connect

from dagster_v3.exchange_rates.models import (
    ExchangeRateComponent,
    ExchangeRateRequest,
    UsdExchangeRate,
)

DEFAULT_EXCHANGE_RATES_TABLE = "reference.exchange_rates"
DEFAULT_CLICKHOUSE_HTTP_PORT = 8123


class ExchangeRateClient:
    def __init__(
        self,
        clickhouse_client: Any,
        *,
        table: str = DEFAULT_EXCHANGE_RATES_TABLE,
    ) -> None:
        self._clickhouse_client = clickhouse_client
        self._table = table

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

        quote_currencies = sorted(
            {request.currency for request in normalized_requests} | {"USD"}
        )
        max_rate_date = max(request.rate_date for request in normalized_requests)
        components = self._load_components(
            quote_currencies=quote_currencies,
            max_rate_date=max_rate_date,
        )

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

    def _load_components(
        self,
        *,
        quote_currencies: list[str],
        max_rate_date: str,
    ) -> dict[tuple[str, str], ExchangeRateComponent]:
        result = self._clickhouse_client.query(
            f"""
            SELECT
                toString(rate_date),
                quote_currency,
                toString(rate),
                source,
                source_url,
                source_payload_hash,
                toString(pulled_at)
            FROM {self._table}
            WHERE base_currency = 'EUR'
              AND quote_currency IN %(quote_currencies)s
              AND rate_date <= %(max_rate_date)s
            ORDER BY quote_currency, rate_date DESC
            """,
            parameters={
                "quote_currencies": quote_currencies,
                "max_rate_date": max_rate_date,
            },
        )
        rows = getattr(result, "result_rows", result)
        return {
            (str(row[1]).upper(), str(row[0])): ExchangeRateComponent(
                rate_date=str(row[0]),
                base_currency="EUR",
                quote_currency=str(row[1]).upper(),
                rate=Decimal(str(row[2])),
                source=str(row[3]),
                source_url=str(row[4]),
                source_payload_hash=str(row[5]),
                pulled_at=str(row[6]),
            )
            for row in rows
        }

    def _resolve_usd_rate(
        self,
        *,
        request: ExchangeRateRequest,
        components: dict[tuple[str, str], ExchangeRateComponent],
    ) -> UsdExchangeRate:
        rate_date = _latest_common_rate_date(
            requested_rate_date=request.rate_date,
            currencies=_required_currencies(request.currency),
            components=components,
        )
        if rate_date is None:
            raise LookupError(
                f"No USD exchange rate for {request.currency} on or before {request.rate_date}"
            )

        usd_component = components[("USD", rate_date)]
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
            eur_component = components[("EUR", rate_date)]
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

        currency_component = components[(request.currency, rate_date)]
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
    return None


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    return default if value is None or value.strip() == "" else int(value)


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}
