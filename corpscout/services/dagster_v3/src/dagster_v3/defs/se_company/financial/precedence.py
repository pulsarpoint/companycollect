"""Per-field, per-source precedence of the financial fold (spec 2026-09-11 section 5).

One map per field, scope-agnostic: Bolagsverket and ESEF never meet, because Bolagsverket
only ever supplies the standalone scope and ESEF only the consolidated one, so both can sit
at 900. RATSIT FIRST (owner decision 2026-09-11, "I think ratsit information is pretty
accurate"): the measured disagreement with Bolagsverket is rounding to a tenth of a million,
not error, and the figures come from the same filed reports. Precedence only decides when
several sources have an opinion on the same cell; for the 1.09M company-years only Ratsit
covers and the nine figures only Ratsit carries, the order never matters. The comparative
source (a later filing's restated prior-year column) appears only where Bolagsverket
restates a figure, revenue and total assets. The reviewer is a source like the others,
ranked above every automated one and above company rules (RULE_PRECEDENCE); `reviewer_draft`
appears in no map and can never win a fold.

A source absent from a field's map cannot supply that field; a company rule (slice 4) can
rank it in for one company or one period. A change to these numbers is a full re-fold
(`changed_only: false` over all 64 buckets) after the export.
"""

from dagster_v3.defs.se_company.financial import tables

# The number a reviewer's "Use this" rule carries, and the reviewer source's own rank
# (above it, so an activated typed value outranks any preference rule).
RULE_PRECEDENCE = 10000
REVIEWER_PRECEDENCE = 20000

_FULL = {"reviewer": 20000, "ratsit": 1000, "bolagsverket": 900, "esef": 900, "bolagsverket_comparative": 800}
_THREE_SOURCES = {"reviewer": 20000, "ratsit": 1000, "bolagsverket": 900, "esef": 900}
_REGISTERS_ONLY = {"reviewer": 20000, "bolagsverket": 900, "esef": 900}
_RATSIT_AND_BOLAGSVERKET = {"reviewer": 20000, "ratsit": 1000, "bolagsverket": 900}
_BOLAGSVERKET_ONLY = {"reviewer": 20000, "bolagsverket": 900}
_RATSIT_ONLY = {"reviewer": 20000, "ratsit": 1000}

# Grouped by source set for readability, NOT in fold order: iterate tables.FOLDED_FIELDS or
# precedence_rows(), never this dict, when order matters.
FINANCIAL_PRECEDENCE: dict[str, dict[str, int]] = {
    # Period attributes and the currency: every source that delivers a period has them.
    "period_start": dict(_FULL),
    "fiscal_year": dict(_FULL),
    "period_months": dict(_FULL),
    "currency": dict(_FULL),
    # The two figures a later Bolagsverket filing restates.
    "revenue": dict(_FULL),
    "total_assets": dict(_FULL),
    # Carried by Ratsit and both registers.
    "operating_result": dict(_THREE_SOURCES),
    "net_result": dict(_THREE_SOURCES),
    "equity": dict(_THREE_SOURCES),
    "liabilities": dict(_THREE_SOURCES),
    "employees": dict(_THREE_SOURCES),
    # Registers only: Ratsit publishes neither.
    "cash_and_bank": dict(_REGISTERS_ONLY),
    "personnel_expenses": dict(_REGISTERS_ONLY),
    # Ratsit and Bolagsverket (ESEF's mapping has no current-asset split).
    "current_assets": dict(_RATSIT_AND_BOLAGSVERKET),
    "current_liabilities": dict(_RATSIT_AND_BOLAGSVERKET),
    # Bolagsverket alone.
    "wages_and_salaries": dict(_BOLAGSVERKET_ONLY),
    # Ratsit alone: the nine lines only its reports carry.
    "operating_costs": dict(_RATSIT_ONLY),
    "result_after_financial_items": dict(_RATSIT_ONLY),
    "ebitda": dict(_RATSIT_ONLY),
    "fixed_assets": dict(_RATSIT_ONLY),
    "share_capital": dict(_RATSIT_ONLY),
    "untaxed_reserves": dict(_RATSIT_ONLY),
    "provisions": dict(_RATSIT_ONLY),
    "long_term_liabilities": dict(_RATSIT_ONLY),
    "dividend": dict(_RATSIT_ONLY),
}

# Not an assert: this runs at import time under load_from_defs_folder, and `python -O`
# would strip it. A raise names the offending tuples in the code location's error.
if set(FINANCIAL_PRECEDENCE) != set(tables.FOLDED_FIELDS):
    raise ValueError(
        f"FINANCIAL_PRECEDENCE keys {sorted(FINANCIAL_PRECEDENCE)} "
        f"must equal FOLDED_FIELDS {sorted(tables.FOLDED_FIELDS)}"
    )

_UNKNOWN_SOURCES = {
    (field, source)
    for field, by_source in FINANCIAL_PRECEDENCE.items()
    for source in by_source
    if source not in tables.SOURCES
}
if _UNKNOWN_SOURCES:
    raise ValueError(f"FINANCIAL_PRECEDENCE names sources outside tables.SOURCES: {sorted(_UNKNOWN_SOURCES)}")


def precedence_for(field: str, source: str) -> int | None:
    """The global precedence of `source` for `field`, or None when it cannot supply it."""
    return FINANCIAL_PRECEDENCE.get(field, {}).get(source)


def precedence_rows() -> list[tuple[str, str, int]]:
    """Every (field, source, precedence) pair, fields in fold order, highest first, ties by
    source name -- the rows the export asset writes as company_id '' / period_key ''."""
    rows: list[tuple[str, str, int]] = []
    for field in tables.FOLDED_FIELDS:
        by_source = FINANCIAL_PRECEDENCE[field]
        for source, precedence in sorted(by_source.items(), key=lambda item: (-item[1], item[0])):
            rows.append((field, source, precedence))
    return rows
