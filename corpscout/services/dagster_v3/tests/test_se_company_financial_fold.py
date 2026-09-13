"""The pure per-period financial fold (spec 2026-09-11 section 6): currency first, money
gated by currency with the USD twin travelling with its figure, ungated employees and period
fields, rules most specific first, hide, the publish rule, withdrawal and reactivation, the
history shapes."""

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from dagster_v3.defs.se_company.financial import tables
from dagster_v3.defs.se_company.financial.fold import (
    CREATED,
    EMPTY_CELL,
    FOLD_VERSION,
    HIDDEN,
    REACTIVATED,
    UPDATED,
    WITHDRAWN,
    FinancialRow,
    Money,
    MoneyCell,
    Suggestion,
    change_kind_for,
    fold_company_periods,
    fold_financial,
    resolve_rules,
    withdrawn_row,
)

COMPANY = "5567081699"
PERIOD = "standalone:2023-12-31"
END = date(2023, 12, 31)
AT = datetime(2026, 9, 12, 20, 0, tzinfo=UTC)
FOLDED_AT = datetime(2026, 9, 13, 8, 0, tzinfo=UTC)
RUN = "run-1"


def money(**figures: tuple[str, str | None]) -> dict[str, Money]:
    """Every monetary field, the named ones with (original, usd) as decimal strings."""
    cells = {field: Money(None, None) for field in tables.MONETARY_FIELDS}
    for field, (original, usd) in figures.items():
        cells[field] = Money(Decimal(original), None if usd is None else Decimal(usd))
    return cells


def suggestion(source: str, uid: str = "", *, currency: str | None = "SEK", period_key: str = PERIOD,
               period_end: date = END, scope: str = "standalone", employees: int | None = None,
               period_start: date | None = date(2023, 1, 1), fiscal_year: int | None = 2023,
               period_months: int | None = 12, **figures) -> Suggestion:
    return Suggestion(
        company_id=COMPANY, source=source, period_key=period_key, scope=scope, period_end=period_end,
        source_record_uid=uid or f"{source}-uid", suggested_at=AT, period_start=period_start,
        fiscal_year=fiscal_year, period_months=period_months, currency=currency,
        money=money(**figures), employees=employees,
    )


def cell(row: FinancialRow, field: str) -> MoneyCell:
    return row.money[field]


def test_currency_is_decided_first_and_gates_the_money() -> None:
    """Ratsit (1000) beats Bolagsverket (900) on revenue -- but a company rule that ranks
    Bolagsverket's currency first makes the row EUR, and then only EUR rows may supply money:
    Bolagsverket's revenue wins with ITS USD twin, Ratsit's revenue is out although it ranks
    higher, and Ratsit still supplies the ungated employee count."""
    rows = [
        suggestion("ratsit", employees=21, revenue=("60300000", "6005001.802437")),
        suggestion("bolagsverket", currency="EUR", revenue=("5500000", "5900000")),
    ]
    row = fold_financial(COMPANY, PERIOD, rows, {"currency": {"bolagsverket": 5000}}, source_run_id=RUN)
    assert row is not None
    assert (row.currency, row.currency_source) == ("EUR", "bolagsverket")
    assert cell(row, "revenue") == MoneyCell(Decimal("5500000"), Decimal("5900000"), "bolagsverket")
    assert (row.employees, row.employees_source) == (21, "ratsit")
    assert row.sources == ("bolagsverket", "ratsit")
    assert (row.active, row.inactive_reason, row.fold_version, row.source_run_id) == (1, "", FOLD_VERSION, RUN)


def test_without_a_rule_ratsit_first_and_the_twin_travels_with_the_figure() -> None:
    rows = [
        suggestion("bolagsverket", employees=2100, revenue=("59016040", "5877138.085783"), equity=("3379581", "336000")),
        suggestion("ratsit", revenue=("60300000", "6005001.802437")),
    ]
    row = fold_financial(COMPANY, PERIOD, rows, source_run_id=RUN)
    assert row is not None
    assert (row.currency, row.currency_source) == ("SEK", "ratsit")   # 1000 over 900, the same value
    assert cell(row, "revenue") == MoneyCell(Decimal("60300000"), Decimal("6005001.802437"), "ratsit")
    assert cell(row, "equity") == MoneyCell(Decimal("3379581"), Decimal("336000"), "bolagsverket")
    assert cell(row, "dividend") == EMPTY_CELL
    assert (row.employees, row.employees_source) == (2100, "bolagsverket")
    assert (row.period_start, row.period_start_source) == (date(2023, 1, 1), "ratsit")
    assert row.sources == ("bolagsverket", "ratsit")


def test_a_row_without_a_currency_supplies_employees_and_dates_but_never_money() -> None:
    """The ESEF rows with a blank currency (spec 6 step 2)."""
    only = suggestion("esef", currency=None, scope="consolidated", period_key="consolidated:2023-12-31",
                      employees=4200, revenue=("1300000000", "129400000"))
    row = fold_financial(COMPANY, "consolidated:2023-12-31", [only], source_run_id=RUN)
    assert row is not None
    assert (row.currency, row.currency_source) == ("", "")
    assert cell(row, "revenue") == EMPTY_CELL
    assert (row.employees, row.employees_source, row.sources) == (4200, "esef", ("esef",))
    assert row.scope == "consolidated"


def test_a_source_with_no_map_entry_cannot_supply_a_field_unless_a_rule_ranks_it_in() -> None:
    comparative = suggestion("bolagsverket_comparative", revenue=("54900000", "5499000"), equity=("1", "1"))
    without_rule = fold_financial(COMPANY, PERIOD, [comparative], source_run_id=RUN)
    assert without_rule is not None
    assert cell(without_rule, "revenue").source == "bolagsverket_comparative"
    assert cell(without_rule, "equity") == EMPTY_CELL
    with_rule = fold_financial(COMPANY, PERIOD, [comparative], {"equity": {"bolagsverket_comparative": 950}}, source_run_id=RUN)
    assert with_rule is not None
    assert cell(with_rule, "equity") == MoneyCell(Decimal("1"), Decimal("1"), "bolagsverket_comparative")


def test_a_low_rule_demotes_a_source_for_one_period() -> None:
    rows = [suggestion("ratsit", revenue=("60300000", "6005001")), suggestion("bolagsverket", revenue=("59016040", "5877138"))]
    row = fold_financial(COMPANY, PERIOD, rows, {"revenue": {"ratsit": 100}}, source_run_id=RUN)
    assert row is not None
    assert cell(row, "revenue").source == "bolagsverket"
    assert row.currency_source == "ratsit"   # the rule names one field only


def test_ties_go_to_the_smaller_source_then_the_smaller_uid_never_to_recency() -> None:
    rows = [
        suggestion("ratsit", "ratsit:x:0:1", revenue=("1", "1")),
        suggestion("ratsit", "ratsit:x:0:0", revenue=("2", "2")),
        suggestion("bolagsverket", "b", revenue=("3", "3")),
    ]
    row = fold_financial(COMPANY, PERIOD, rows, {"revenue": {"ratsit": 900}, "currency": {"ratsit": 900}}, source_run_id=RUN)
    assert row is not None
    assert cell(row, "revenue") == MoneyCell(Decimal("3"), Decimal("3"), "bolagsverket")   # 'bolagsverket' < 'ratsit' at 900
    only_ratsit = fold_financial(COMPANY, PERIOD, rows[:2], source_run_id=RUN)
    assert only_ratsit is not None
    assert cell(only_ratsit, "revenue") == MoneyCell(Decimal("2"), Decimal("2"), "ratsit")  # the smaller uid


def test_resolve_rules_overlays_the_period_on_the_company_wide_on_the_global() -> None:
    by_period = {
        "": {"revenue": {"bolagsverket": 2000, "esef": 50}},
        PERIOD: {"revenue": {"bolagsverket": 500}},
    }
    assert resolve_rules(by_period, PERIOD) == {"revenue": {"bolagsverket": 500, "esef": 50}}
    assert resolve_rules(by_period, "standalone:2022-12-31") == {"revenue": {"bolagsverket": 2000, "esef": 50}}
    assert resolve_rules(None, PERIOD) == {} and resolve_rules({}, PERIOD) == {}
    rows = [suggestion("ratsit", revenue=("1", "1")), suggestion("bolagsverket", revenue=("2", "2"))]
    this_period = fold_financial(COMPANY, PERIOD, rows, resolve_rules(by_period, PERIOD), source_run_id=RUN)
    assert this_period is not None and cell(this_period, "revenue").source == "ratsit"          # 1000 > 500
    other = [suggestion("ratsit", period_key="standalone:2022-12-31", period_end=date(2022, 12, 31), revenue=("1", "1")),
             suggestion("bolagsverket", period_key="standalone:2022-12-31", period_end=date(2022, 12, 31), revenue=("2", "2"))]
    other_period = fold_financial(COMPANY, "standalone:2022-12-31", other, resolve_rules(by_period, "standalone:2022-12-31"), source_run_id=RUN)
    assert other_period is not None and cell(other_period, "revenue").source == "bolagsverket"   # 2000 > 1000


def test_a_reviewer_row_outranks_everything_and_a_draft_never_folds() -> None:
    rows = [suggestion("ratsit", revenue=("1", "1")), suggestion("reviewer", revenue=("9", "9")), suggestion("reviewer_draft", revenue=("8", "8"))]
    row = fold_financial(COMPANY, PERIOD, rows, source_run_id=RUN)
    assert row is not None and cell(row, "revenue").source == "reviewer"
    assert fold_financial(COMPANY, PERIOD, [suggestion("reviewer_draft", revenue=("8", "8"))], source_run_id=RUN) is None


def test_hidden_folds_normally_and_lands_inactive() -> None:
    row = fold_financial(COMPANY, PERIOD, [suggestion("ratsit", revenue=("1", "1"))], hidden=True, source_run_id=RUN)
    assert row is not None
    assert (row.active, row.inactive_reason, cell(row, "revenue").source) == (0, HIDDEN, "ratsit")


def test_none_when_nothing_wins_or_nothing_is_live() -> None:
    assert fold_financial(COMPANY, PERIOD, [], source_run_id=RUN) is None
    # A comparative row carrying only equity: no map names it for equity, the row has no
    # employees, but its currency and dates still win -- the row publishes (spec 6 step 5).
    dates_only = fold_financial(COMPANY, PERIOD, [suggestion("bolagsverket_comparative", equity=("1", "1"))], source_run_id=RUN)
    assert dates_only is not None and dates_only.sources == ("bolagsverket_comparative",) and cell(dates_only, "equity") == EMPTY_CELL
    bare = suggestion("bolagsverket_comparative", currency=None, period_start=None, fiscal_year=None, period_months=None, equity=("1", "1"))
    assert fold_financial(COMPANY, PERIOD, [bare], source_run_id=RUN) is None


def test_mismatched_company_or_period_is_refused() -> None:
    with pytest.raises(ValueError, match="company_id"):
        fold_financial("5560000002", PERIOD, [suggestion("ratsit", revenue=("1", "1"))], source_run_id=RUN)
    with pytest.raises(ValueError, match="period_key"):
        fold_financial(COMPANY, "standalone:2022-12-31", [suggestion("ratsit", revenue=("1", "1"))], source_run_id=RUN)


def test_the_tuples_follow_the_ddl_column_order() -> None:
    row = fold_financial(COMPANY, PERIOD, [suggestion("ratsit", employees=21, revenue=("60300000", "6005001.802437"))], source_run_id=RUN)
    assert row is not None
    values = row.as_tuple(FOLDED_AT)
    assert len(values) == len(tables.MAIN_COLUMNS) == 80
    named = dict(zip(tables.MAIN_COLUMNS, values, strict=True))
    assert named["company_id"] == COMPANY and named["period_key"] == PERIOD and named["period_end"] == END
    assert named["revenue_amount_original"] == Decimal("60300000") and named["revenue_source"] == "ratsit"
    assert named["dividend_source"] == "" and named["dividend_amount_usd"] is None
    assert named["sources"] == ["ratsit"] and named["folded_at"] == FOLDED_AT and named["fold_version"] == FOLD_VERSION
    history = row.history_tuple(changed_fields=["revenue", "employees"], changed_at=FOLDED_AT, change_kind=CREATED, fold_run_id=RUN)
    assert len(history) == len(tables.HISTORY_COLUMNS) == 84
    assert history[:80] == values and history[80:] == (["revenue", "employees"], FOLDED_AT, CREATED, RUN)


def test_changed_fields_lists_every_valued_field_on_first_publish_and_only_differences_after() -> None:
    first = fold_financial(COMPANY, PERIOD, [suggestion("ratsit", employees=21, revenue=("60300000", "6005001"), equity=("3400000", "338000"))], source_run_id=RUN)
    assert first is not None
    assert first.changed_fields_against(None) == ["period_start", "fiscal_year", "period_months", "currency", "revenue", "equity", "employees"]
    same = fold_financial(COMPANY, PERIOD, [suggestion("ratsit", employees=21, revenue=("60300000.000000", "6005001"), equity=("3400000", "338000"))], source_run_id="run-2")
    assert same is not None and same.changed_fields_against(first) == []       # Decimal equality, not text
    usd_moved = fold_financial(COMPANY, PERIOD, [suggestion("ratsit", employees=21, revenue=("60300000", "6100000"), equity=("3400000", "338000"))], source_run_id=RUN)
    assert usd_moved is not None and usd_moved.changed_fields_against(first) == ["revenue"]   # the twin is part of the field
    source_moved = fold_financial(COMPANY, PERIOD, [suggestion("ratsit", employees=21, revenue=("60300000", "6005001"), equity=("3400000", "338000")), suggestion("reviewer", currency=None, period_start=None, fiscal_year=None, period_months=None, employees=21)], source_run_id=RUN)
    assert source_moved is not None and source_moved.changed_fields_against(first) == ["employees"]  # same value, new source
    assert not same.activity_changed_against(first)
    hidden = fold_financial(COMPANY, PERIOD, [suggestion("ratsit", employees=21, revenue=("60300000", "6005001"), equity=("3400000", "338000"))], hidden=True, source_run_id=RUN)
    assert hidden is not None and hidden.changed_fields_against(first) == [] and hidden.activity_changed_against(first)


def test_change_kinds_and_withdrawal() -> None:
    live = fold_financial(COMPANY, PERIOD, [suggestion("ratsit", revenue=("1", "1"))], source_run_id=RUN)
    hidden = fold_financial(COMPANY, PERIOD, [suggestion("ratsit", revenue=("1", "1"))], hidden=True, source_run_id=RUN)
    assert live is not None and hidden is not None
    gone = withdrawn_row(live, source_run_id="run-9")
    assert (gone.active, gone.inactive_reason, gone.source_run_id, gone.fold_version) == (0, WITHDRAWN, "run-9", FOLD_VERSION)
    assert cell(gone, "revenue") == cell(live, "revenue")                # keeps the last values
    assert change_kind_for(live, gone) == WITHDRAWN
    assert change_kind_for(live, hidden) == HIDDEN
    assert change_kind_for(hidden, live) == REACTIVATED
    assert change_kind_for(gone, live) == REACTIVATED
    assert change_kind_for(gone, hidden) == HIDDEN
    assert change_kind_for(live, live) == UPDATED


def test_fold_company_periods_creates_withdraws_hides_reactivates_and_rewrites_the_unchanged() -> None:
    p22 = "standalone:2022-12-31"
    live_23 = [suggestion("ratsit", revenue=("60300000", "6005001"))]
    live_22 = [suggestion("ratsit", period_key=p22, period_end=date(2022, 12, 31), revenue=("57100000", "5475989"))]
    first = fold_company_periods(COMPANY, [*live_23, *live_22], [], None, set(), source_run_id=RUN)
    assert [row.period_key for row in first.rows] == [p22, PERIOD]
    assert (first.published, first.created, first.unchanged, first.unpublished) == (2, 2, 0, 0)
    assert [(entry.row.period_key, entry.change_kind) for entry in first.history] == [(p22, CREATED), (PERIOD, CREATED)]
    assert first.history[0].changed_fields == ("period_start", "fiscal_year", "period_months", "currency", "revenue")

    # Round 2: 2022 gone from the sources, 2023 hidden by a rule -- the withdrawn row keeps its values.
    second = fold_company_periods(COMPANY, live_23, list(first.rows), None, {PERIOD}, source_run_id="run-2")
    by_key = {row.period_key: row for row in second.rows}
    assert (by_key[p22].active, by_key[p22].inactive_reason, cell(by_key[p22], "revenue").original) == (0, WITHDRAWN, Decimal("57100000"))
    assert (by_key[PERIOD].active, by_key[PERIOD].inactive_reason) == (0, HIDDEN)
    assert (second.published, second.withdrawn, second.hidden, second.unchanged) == (0, 1, 1, 0)
    assert sorted((entry.row.period_key, entry.change_kind, entry.changed_fields) for entry in second.history) == [(p22, WITHDRAWN, ()), (PERIOD, HIDDEN, ())]

    # Round 3: nothing changed since round 2 -- both rows are returned again (folded_at will
    # advance) with no history; the withdrawn one stays withdrawn.
    third = fold_company_periods(COMPANY, live_23, list(second.rows), None, {PERIOD}, source_run_id="run-3")
    assert (len(third.rows), len(third.history), third.unchanged) == (2, 0, 2)

    # Round 4: the hide is released and 2022 is delivered again with a new figure.
    live_22_again = [suggestion("ratsit", period_key=p22, period_end=date(2022, 12, 31), revenue=("57200000", "5480000"))]
    fourth = fold_company_periods(COMPANY, [*live_23, *live_22_again], list(third.rows), None, set(), source_run_id="run-4")
    kinds = {entry.row.period_key: (entry.change_kind, entry.changed_fields) for entry in fourth.history}
    assert kinds == {p22: (REACTIVATED, ("revenue",)), PERIOD: (REACTIVATED, ())}
    assert (fourth.published, fourth.reactivated) == (2, 2)

    # A period with live rows but no winner and no main row writes nothing (unpublished).
    bare = suggestion("bolagsverket_comparative", currency=None, period_start=None, fiscal_year=None, period_months=None, equity=("1", "1"))
    fifth = fold_company_periods(COMPANY, [bare], [], None, set(), source_run_id=RUN)
    assert (fifth.rows, fifth.history, fifth.unpublished) == ((), (), 1)


def test_fold_company_periods_refuses_foreign_rows() -> None:
    other = suggestion("ratsit", revenue=("1", "1"))
    foreign = Suggestion(**{**{f: getattr(other, f) for f in other.__slots__}, "company_id": "5560000002"})
    with pytest.raises(ValueError, match="company_id"):
        fold_company_periods(COMPANY, [foreign], [], None, set(), source_run_id=RUN)
