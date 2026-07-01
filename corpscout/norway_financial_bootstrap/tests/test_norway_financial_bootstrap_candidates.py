import polars as pl

from norway_financial_bootstrap.candidates import (
    FinancialCandidate,
    build_financial_candidates,
)


def test_build_financial_candidates_filters_active_companies_with_accounts_year() -> None:
    frame = pl.DataFrame(
        [
            {
                "org_number": "200",
                "name": "B AS",
                "primary_website_url": "https://b.example",
                "is_active": True,
                "last_submitted_accounts_year": "2024",
            },
            {
                "org_number": "100",
                "name": "A AS",
                "primary_website_url": None,
                "is_active": True,
                "last_submitted_accounts_year": "2023",
            },
            {
                "org_number": "300",
                "name": "INACTIVE AS",
                "primary_website_url": "",
                "is_active": False,
                "last_submitted_accounts_year": "2024",
            },
        ]
    )

    assert build_financial_candidates(frame) == [
        FinancialCandidate("100", "A AS", "", "2023"),
        FinancialCandidate("200", "B AS", "https://b.example", "2024"),
    ]
