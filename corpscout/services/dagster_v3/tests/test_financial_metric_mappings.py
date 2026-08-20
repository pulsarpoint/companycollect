from dagster_v3.defs.common.financial_metric_mappings import (
    FINANCIAL_METRIC_MAPPINGS,
    concept_metric_codes_for_source,
    metric_concepts_for_source,
)


def test_mapping_has_canonical_presentation_keys() -> None:
    assert tuple(FINANCIAL_METRIC_MAPPINGS) == (
        "revenue",
        "operating_result",
        "net_result",
        "total_assets",
        "equity",
        "liabilities",
        "cash_and_bank",
        "current_assets",
        "current_liabilities",
        "personnel_expenses",
        "wages_and_salaries",
        "employees",
    )


def test_source_projections_use_physical_metric_names() -> None:
    assert metric_concepts_for_source("bolagsverket")["operating_profit_loss"] == (
        "Rorelseresultat",
    )
    assert metric_concepts_for_source("esef")["cash"] == (
        "ifrs-full:CashAndCashEquivalents",
    )
    assert concept_metric_codes_for_source("bolagsverket")["AretsResultat"] == (
        "profit_loss"
    )


def test_esef_revenue_fallback_includes_investment_property_rental_income() -> None:
    revenue_concepts = FINANCIAL_METRIC_MAPPINGS["revenue"]["esef"]

    assert revenue_concepts[-1] == "ifrs-full:RentalIncomeFromInvestmentProperty"
    assert "ifrs-full:RevenueFromInterest" not in revenue_concepts
    assert all(
        "ProfitFromPropertyManagement" not in concept for concept in revenue_concepts
    )
