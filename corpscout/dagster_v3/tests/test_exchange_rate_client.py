from decimal import Decimal

from dagster_v3.exchange_rates import ExchangeRateClient, ExchangeRateRequest


def test_exchange_rate_client_resolves_latest_common_usd_cross_rate() -> None:
    clickhouse = FakeNativeClickHouseClient(
        rows=[
            ("2024-12-31", "USD", "1.0389", "ECB EXR", "usd-1231", "hash-usd-1231", "2026-06-16"),
            ("2024-12-30", "USD", "1.0400", "ECB EXR", "usd-1230", "hash-usd-1230", "2026-06-16"),
            ("2024-12-30", "NOK", "11.8000", "ECB EXR", "nok-1230", "hash-nok-1230", "2026-06-16"),
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
    assert clickhouse.queries[0].parameters == {
        "quote_currencies": ["NOK", "USD"],
        "max_rate_date": "2024-12-31",
    }


def test_exchange_rate_client_handles_usd_and_eur_without_extra_cross_rate() -> None:
    clickhouse = FakeNativeClickHouseClient(
        rows=[
            ("2024-12-31", "USD", "1.0389", "ECB EXR", "usd-1231", "hash-usd-1231", "2026-06-16"),
            ("2024-12-31", "EUR", "1", "identity", "", "0" * 64, "2026-06-16"),
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
                    "2024-12-31",
                    "USD",
                    "1.0389",
                    "ECB EXR",
                    "usd-1231",
                    "hash-usd-1231",
                    "2026-06-16",
                ),
                (
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
                (
                    "2024-12-31",
                    "USD",
                    "1.0389",
                    "ECB EXR",
                    "usd-1231",
                    "hash-usd-1231",
                    "2026-06-16",
                ),
                (
                    "2024-12-31",
                    "NOK",
                    "11.7950",
                    "ECB EXR",
                    "nok-1231",
                    "hash-nok-1231",
                    "2026-06-16",
                ),
                (
                    "2023-12-29",
                    "USD",
                    "1.1050",
                    "ECB EXR",
                    "usd-2023",
                    "hash-usd-2023",
                    "2026-06-16",
                ),
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


def test_exchange_rate_client_raises_when_rate_is_missing() -> None:
    client = ExchangeRateClient(FakeNativeClickHouseClient(rows=[]))

    try:
        client.usd_rate(currency="NOK", rate_date="2024-12-31")
    except LookupError as error:
        assert "No USD exchange rate for NOK on or before 2024-12-31" in str(error)
    else:
        raise AssertionError("Expected missing rate to raise LookupError")


class FakeQueryResult:
    def __init__(self, rows: list[tuple[str, str, str, str, str, str, str]]) -> None:
        self.result_rows = rows


class FakeNativeClickHouseClient:
    def __init__(self, *, rows: list[tuple[str, str, str, str, str, str, str]]) -> None:
        self.rows = rows
        self.queries: list[FakeQuery] = []

    def query(self, sql: str, parameters: dict[str, object]) -> FakeQueryResult:
        self.queries.append(FakeQuery(sql=sql, parameters=parameters))
        return FakeQueryResult(self.rows)


class FakeQuery:
    def __init__(self, *, sql: str, parameters: dict[str, object]) -> None:
        self.sql = sql
        self.parameters = parameters
