from decimal import Decimal
from typing import Any

from exchange_rates import ExchangeRateClient, ExchangeRateRequest


def test_exchange_rate_client_resolves_latest_common_usd_cross_rate() -> None:
    clickhouse = FakeNativeClickHouseClient(
        rows=[
            _component_row("NOK", "2024-12-31", "2024-12-30", "USD", "1.0400"),
            _component_row("NOK", "2024-12-31", "2024-12-30", "NOK", "11.8000"),
        ]
    )
    client = ExchangeRateClient(clickhouse)

    rate = client.usd_rate(currency="NOK", rate_date="2024-12-31")

    assert rate.currency == "NOK"
    assert rate.requested_rate_date == "2024-12-31"
    assert rate.rate_date == "2024-12-30"
    assert rate.rate == Decimal("0.08813559322033898305084745763")
    assert rate.eur_to_usd == Decimal("1.0400")
    assert rate.eur_to_currency == Decimal("11.8000")
    assert rate.source == "ECB EXR"
    assert [component.quote_currency for component in rate.components] == ["USD", "NOK"]
    assert clickhouse.queries[0].parameters == {}
    assert "requested_rates" in clickhouse.queries[0].sql
    assert "selected_dates" in clickhouse.queries[0].sql
    assert "toDate('2024-12-31')" in clickhouse.queries[0].sql


def test_exchange_rate_client_aliases_available_date_columns_for_clickhouse() -> None:
    clickhouse = FakeNativeClickHouseClient(rows=[])
    client = ExchangeRateClient(clickhouse)

    try:
        client.usd_rate(currency="NOK", rate_date="2024-12-31")
    except LookupError:
        pass
    else:
        raise AssertionError("Expected missing rate to raise LookupError")

    sql = clickhouse.queries[0].sql
    assert "requested_rates.request_currency AS request_currency" in sql
    assert "requested_rates.requested_rate_date AS requested_rate_date" in sql
    assert "exchange_rates.rate_date AS rate_date" in sql


def test_exchange_rate_client_handles_usd_and_eur_without_extra_cross_rate() -> None:
    clickhouse = FakeNativeClickHouseClient(
        rows=[
            _component_row("USD", "2024-12-31", "2024-12-31", "USD", "1.0389"),
            _component_row("EUR", "2024-12-31", "2024-12-31", "USD", "1.0389"),
            _component_row("EUR", "2024-12-31", "2024-12-31", "EUR", "1"),
        ]
    )
    client = ExchangeRateClient(clickhouse)

    usd = client.usd_rate(currency="USD", rate_date="2024-12-31")
    eur = client.usd_rate(currency="EUR", rate_date="2024-12-31")

    assert usd.rate == Decimal("1")
    assert usd.rate_date == "2024-12-31"
    assert eur.rate == Decimal("1.0389")
    assert eur.eur_to_currency == Decimal("1")
    assert eur.rate_date == "2024-12-31"


def test_exchange_rate_client_converts_amount_to_usd() -> None:
    client = ExchangeRateClient(
        FakeNativeClickHouseClient(
            rows=[
                (
                    "NOK",
                    "2024-12-31",
                    "2024-12-31",
                    "USD",
                    "1.0389",
                    "ECB EXR",
                    "usd-1231",
                    "hash-usd-1231",
                    "2026-06-16",
                ),
                (
                    "NOK",
                    "2024-12-31",
                    "2024-12-31",
                    "NOK",
                    "11.7950",
                    "ECB EXR",
                    "nok-1231",
                    "hash-nok-1231",
                    "2026-06-16",
                ),
            ]
        )
    )

    amount_usd = client.convert_to_usd(
        amount=Decimal("1250000"),
        currency="NOK",
        rate_date="2024-12-31",
    )

    assert amount_usd == Decimal("110099.6184824077999152183128")


def test_exchange_rate_client_loads_batch_rates() -> None:
    client = ExchangeRateClient(
        FakeNativeClickHouseClient(
            rows=[
                _component_row("NOK", "2024-12-31", "2024-12-31", "USD", "1.0389"),
                _component_row("NOK", "2024-12-31", "2024-12-31", "NOK", "11.7950"),
                _component_row("USD", "2023-12-31", "2023-12-29", "USD", "1.1050"),
            ]
        )
    )

    rates = client.usd_rates(
        [
            ExchangeRateRequest(currency="NOK", rate_date="2024-12-31"),
            ExchangeRateRequest(currency="USD", rate_date="2023-12-31"),
        ]
    )

    assert rates[("NOK", "2024-12-31")].rate == Decimal("0.08807969478592623993217465028")
    assert rates[("USD", "2023-12-31")].rate == Decimal("1")


def test_exchange_rate_client_uses_earliest_common_rate_when_request_predates_data() -> None:
    client = ExchangeRateClient(
        FakeNativeClickHouseClient(
            rows=[
                (
                    "NOK",
                    "2022-12-31",
                    "2023-01-02",
                    "USD",
                    "1.0666",
                    "ECB EXR",
                    "usd-2023",
                    "hash-usd-2023",
                    "2026-06-16",
                ),
                (
                    "NOK",
                    "2022-12-31",
                    "2023-01-02",
                    "NOK",
                    "10.5138",
                    "ECB EXR",
                    "nok-2023",
                    "hash-nok-2023",
                    "2026-06-16",
                ),
            ]
        )
    )

    rate = client.usd_rate(currency="NOK", rate_date="2022-12-31")

    assert rate.requested_rate_date == "2022-12-31"
    assert rate.rate_date == "2023-01-02"
    assert rate.rate == Decimal("0.1014476212216325210675493161")
    assert client._clickhouse_client.queries[0].parameters == {}
    assert "toDate('2022-12-31')" in client._clickhouse_client.queries[0].sql


def test_exchange_rate_client_batches_unique_currency_date_requests() -> None:
    clickhouse = FakeNativeClickHouseClient(
        rows=[
            (
                "NOK",
                "2024-12-31",
                "2024-12-31",
                "USD",
                "1.0389",
                "ECB EXR",
                "usd-1231",
                "hash-usd-1231",
                "2026-06-16",
            ),
            (
                "NOK",
                "2024-12-31",
                "2024-12-31",
                "NOK",
                "11.7950",
                "ECB EXR",
                "nok-1231",
                "hash-nok-1231",
                "2026-06-16",
            ),
            (
                "NOK",
                "2022-12-31",
                "2023-01-02",
                "USD",
                "1.0666",
                "ECB EXR",
                "usd-2023",
                "hash-usd-2023",
                "2026-06-16",
            ),
            (
                "NOK",
                "2022-12-31",
                "2023-01-02",
                "NOK",
                "10.5138",
                "ECB EXR",
                "nok-2023",
                "hash-nok-2023",
                "2026-06-16",
            ),
        ]
    )
    client = ExchangeRateClient(clickhouse)

    rates = client.usd_rates(
        [
            ExchangeRateRequest(currency="NOK", rate_date="2024-12-31"),
            ExchangeRateRequest(currency="NOK", rate_date="2024-12-31"),
            ExchangeRateRequest(currency="NOK", rate_date="2022-12-31"),
        ]
    )

    assert rates[("NOK", "2024-12-31")].rate_date == "2024-12-31"
    assert rates[("NOK", "2022-12-31")].rate_date == "2023-01-02"
    assert len(clickhouse.queries) == 1
    assert "requested_rates" in clickhouse.queries[0].sql
    assert "selected_dates" in clickhouse.queries[0].sql


def test_exchange_rate_client_raises_when_rate_is_missing() -> None:
    client = ExchangeRateClient(FakeNativeClickHouseClient(rows=[]))

    try:
        client.usd_rate(currency="NOK", rate_date="2024-12-31")
    except LookupError as error:
        assert "No USD exchange rate for NOK on, before, or after 2024-12-31" in str(error)
    else:
        raise AssertionError("Expected missing rate to raise LookupError")


def _component_row(
    request_currency: str,
    requested_rate_date: str,
    selected_rate_date: str,
    quote_currency: str,
    rate: str,
) -> tuple[str, str, str, str, str, str, str, str, str]:
    return (
        request_currency,
        requested_rate_date,
        selected_rate_date,
        quote_currency,
        rate,
        "ECB EXR",
        f"{quote_currency.lower()}-{selected_rate_date}",
        f"hash-{quote_currency.lower()}-{selected_rate_date}",
        "2026-06-16",
    )


class FakeQueryResult:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.result_rows = rows


class FakeNativeClickHouseClient:
    def __init__(self, *, rows: list[tuple[Any, ...]]) -> None:
        self.rows = rows
        self.queries: list[FakeQuery] = []

    def query(self, sql: str, parameters: dict[str, object]) -> FakeQueryResult:
        self.queries.append(FakeQuery(sql=sql, parameters=parameters))
        return FakeQueryResult(self.rows)


class FakeQuery:
    def __init__(self, *, sql: str, parameters: dict[str, object]) -> None:
        self.sql = sql
        self.parameters = parameters
