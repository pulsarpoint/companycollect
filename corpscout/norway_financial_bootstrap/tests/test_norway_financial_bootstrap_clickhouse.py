from norway_financial_bootstrap.candidates import FinancialCandidate
from norway_financial_bootstrap.clickhouse import (
    NO_COMPANIES_QUERY,
    financial_candidates_from_clickhouse,
)


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def query(self, sql: str):
        self.queries.append(sql)
        return type(
            "QueryResult",
            (),
            {
                "result_rows": [
                    ("200", "B AS", "https://b.example", "2024"),
                    ("100", "A AS", "", "2023"),
                ]
            },
        )()


def test_financial_candidates_from_clickhouse_reads_fixed_no_companies_table() -> None:
    client = FakeClickHouseClient()

    candidates = financial_candidates_from_clickhouse(client)

    assert client.queries == [NO_COMPANIES_QUERY]
    assert "from corpscout.no_companies" in NO_COMPANIES_QUERY.lower()
    assert "is_active = true" in NO_COMPANIES_QUERY.lower()
    assert "last_submitted_accounts_year is not null" in NO_COMPANIES_QUERY.lower()
    assert candidates == [
        FinancialCandidate("200", "B AS", "https://b.example", "2024"),
        FinancialCandidate("100", "A AS", "", "2023"),
    ]
