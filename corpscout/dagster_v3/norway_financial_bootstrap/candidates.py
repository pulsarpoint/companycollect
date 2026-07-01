from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import polars as pl


@dataclass(frozen=True, order=True)
class FinancialCandidate:
    org_number: str
    legal_name: str
    website: str
    last_submitted_accounts_year: str

    def as_org_mapping(self) -> dict[str, str]:
        return {
            "org_number": self.org_number,
            "legal_name": self.legal_name,
            "website": self.website,
            "last_submitted_accounts_year": self.last_submitted_accounts_year,
        }


def build_financial_candidates(frame: pl.DataFrame) -> list[FinancialCandidate]:
    rows = (
        frame.filter(
            pl.col("is_active").eq(True)
            & pl.col("last_submitted_accounts_year").is_not_null()
        )
        .select(
            pl.col("org_number").cast(pl.String),
            pl.col("name").cast(pl.String).alias("legal_name"),
            pl.col("primary_website_url")
            .fill_null("")
            .cast(pl.String)
            .alias("website"),
            pl.col("last_submitted_accounts_year").cast(pl.String),
        )
        .sort("org_number")
        .to_dicts()
    )

    return [
        FinancialCandidate(
            row["org_number"],
            row["legal_name"],
            row["website"],
            row["last_submitted_accounts_year"],
        )
        for row in rows
    ]


def missing_candidates(
    candidates: Iterable[FinancialCandidate],
    completed_org_years: set[tuple[str, str]],
) -> list[FinancialCandidate]:
    return [
        candidate
        for candidate in candidates
        if (
            candidate.org_number,
            candidate.last_submitted_accounts_year,
        )
        not in completed_org_years
    ]
