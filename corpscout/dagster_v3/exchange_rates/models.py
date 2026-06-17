from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class ExchangeRateRequest:
    currency: str
    rate_date: str


@dataclass(frozen=True)
class ExchangeRateComponent:
    rate_date: str
    base_currency: str
    quote_currency: str
    rate: Decimal
    source: str
    source_url: str
    source_payload_hash: str
    pulled_at: str


@dataclass(frozen=True)
class UsdExchangeRate:
    currency: str
    requested_rate_date: str
    rate_date: str
    rate: Decimal
    eur_to_usd: Decimal
    eur_to_currency: Decimal
    source: str
    components: tuple[ExchangeRateComponent, ...]

    def convert(self, amount: Decimal) -> Decimal:
        return amount * self.rate
