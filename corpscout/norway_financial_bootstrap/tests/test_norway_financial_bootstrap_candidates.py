import polars as pl

from norway_financial_bootstrap.candidates import (
    FinancialCandidate,
    build_financial_candidates,
)


def test_build_financial_candidates_filters_active_companies_with_accounts_year() -> None:
    frame = pl.DataFrame(
        {
            "org_number": ["300", "100", "200", "400"],
            "name": ["ignored", "ignored", "ignored", "ignored"],
            "primary_website_url": ["https://x.test", None, "", ""],
            "is_active": [True, True, False, True],
            "last_submitted_accounts_year": ["2024", "2023", "2024", None],
        }
    )

    assert build_financial_candidates(frame) == [
        FinancialCandidate("100"),
        FinancialCandidate("300"),
    ]


def test_financial_candidate_mapping_contains_only_org_number() -> None:
    assert FinancialCandidate("923609016").as_org_mapping() == {
        "org_number": "923609016"
    }
