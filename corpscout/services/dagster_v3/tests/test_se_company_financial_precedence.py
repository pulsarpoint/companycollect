"""Spec section 5: Ratsit first, the registers at 900 (they never share a scope), the
restated column last; the reviewer on top of everything including company rules."""

from dagster_v3.defs.se_company.financial import tables
from dagster_v3.defs.se_company.financial.precedence import (
    FINANCIAL_PRECEDENCE,
    REVIEWER_PRECEDENCE,
    RULE_PRECEDENCE,
    precedence_for,
    precedence_rows,
)

FULL = {"reviewer": 20000, "ratsit": 1000, "bolagsverket": 900, "esef": 900, "bolagsverket_comparative": 800}
THREE = {"reviewer": 20000, "ratsit": 1000, "bolagsverket": 900, "esef": 900}


def test_every_folded_field_has_a_map_and_the_reviewer_tops_each() -> None:
    assert set(FINANCIAL_PRECEDENCE) == set(tables.FOLDED_FIELDS)
    for field, by_source in FINANCIAL_PRECEDENCE.items():
        assert by_source["reviewer"] == REVIEWER_PRECEDENCE == 20000, field
        assert max(by_source.values()) == 20000, field
        assert set(by_source) <= set(tables.SOURCES), field
        assert "reviewer_draft" not in by_source, field
        assert precedence_for(field, "reviewer_draft") is None, field
    assert REVIEWER_PRECEDENCE > RULE_PRECEDENCE == 10000


def test_ratsit_leads_every_field_it_supplies() -> None:
    for field, by_source in FINANCIAL_PRECEDENCE.items():
        if "ratsit" in by_source:
            assert by_source["ratsit"] == 1000, field
            for source, precedence in by_source.items():
                if source not in ("reviewer", "ratsit"):
                    assert precedence < 1000, (field, source)


def test_bolagsverket_and_esef_tie_because_they_never_share_a_scope() -> None:
    both = [f for f, m in FINANCIAL_PRECEDENCE.items() if "bolagsverket" in m and "esef" in m]
    assert len(both) == 13
    for field in both:
        assert FINANCIAL_PRECEDENCE[field]["bolagsverket"] == FINANCIAL_PRECEDENCE[field]["esef"] == 900


def test_the_numbers_of_the_spec() -> None:
    for field in ("period_start", "fiscal_year", "period_months", "currency", "revenue", "total_assets"):
        assert FINANCIAL_PRECEDENCE[field] == FULL, field
    for field in ("operating_result", "net_result", "equity", "liabilities", "employees"):
        assert FINANCIAL_PRECEDENCE[field] == THREE, field
    for field in ("cash_and_bank", "personnel_expenses"):
        assert FINANCIAL_PRECEDENCE[field] == {"reviewer": 20000, "bolagsverket": 900, "esef": 900}, field
    for field in ("current_assets", "current_liabilities"):
        assert FINANCIAL_PRECEDENCE[field] == {"reviewer": 20000, "ratsit": 1000, "bolagsverket": 900}, field
    assert FINANCIAL_PRECEDENCE["wages_and_salaries"] == {"reviewer": 20000, "bolagsverket": 900}
    for field in (
        "operating_costs", "result_after_financial_items", "ebitda", "fixed_assets",
        "share_capital", "untaxed_reserves", "provisions", "long_term_liabilities", "dividend",
    ):
        assert FINANCIAL_PRECEDENCE[field] == {"reviewer": 20000, "ratsit": 1000}, field


def test_the_comparative_source_appears_only_where_bolagsverket_restates() -> None:
    restated = {f for f, m in FINANCIAL_PRECEDENCE.items() if "bolagsverket_comparative" in m}
    assert restated == {"period_start", "fiscal_year", "period_months", "currency", "revenue", "total_assets"}
    assert precedence_for("revenue", "bolagsverket_comparative") == 800
    assert precedence_for("net_result", "bolagsverket_comparative") is None
    assert precedence_for("no_such_field", "ratsit") is None


def test_rows_are_fields_in_fold_order_highest_first_ties_by_source_name() -> None:
    rows = precedence_rows()
    assert len(rows) == 82
    assert rows[:5] == [
        ("period_start", "reviewer", 20000),
        ("period_start", "ratsit", 1000),
        ("period_start", "bolagsverket", 900),
        ("period_start", "esef", 900),
        ("period_start", "bolagsverket_comparative", 800),
    ]
    assert rows[-4:] == [
        ("employees", "reviewer", 20000), ("employees", "ratsit", 1000),
        ("employees", "bolagsverket", 900), ("employees", "esef", 900),
    ]
    assert tuple(dict.fromkeys(field for field, _, _ in rows)) == tables.FOLDED_FIELDS
