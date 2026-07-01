from dataclasses import dataclass

import polars as pl


@dataclass(frozen=True, order=True)
class FinancialCandidate:
    org_number: str

    def as_org_mapping(self) -> dict[str, str]:
        return {"org_number": self.org_number}


def build_financial_candidates(frame: pl.DataFrame) -> list[FinancialCandidate]:
    rows = (
        frame.filter(
            pl.col("is_active").eq(True)
            & pl.col("last_submitted_accounts_year").is_not_null()
        )
        .select(pl.col("org_number").cast(pl.String))
        .unique()
        .sort("org_number")
        .to_dicts()
    )
    return [FinancialCandidate(row["org_number"]) for row in rows]
