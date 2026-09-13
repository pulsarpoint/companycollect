"""The per-period fold of financial suggestion rows into one published row (spec 2026-09-11
section 6).

Pure: no I/O, no clock. The batch layer reads and writes; this module decides. One call folds
ONE period of one company -- its current live suggestion rows, which share a period_key -- into
one row of se_company_financial, or None when no field has a winner.

Currency first: among the rows that name a currency, the highest effective precedence sets the
row's currency. Money is gated by it: a figure competes only among rows in that currency, and
the winner supplies its native figure and its own USD conversion together, never mixed. The
period attributes and the employee count compete without the gate. Rules (spec 5) replace the
global precedence number for one (field, source) pair, most specific first: the period rule,
else the company-wide rule, else the global map; a rule may rank in a source the global map
does not name. reviewer_draft is never a candidate. Ties, which only a company rule can create,
go to the smaller source name, then the smaller source_record_uid -- never to recency, so a
re-extracted row with the same content cannot flip a winner.
"""

from collections.abc import Mapping, Sequence, Set
from dataclasses import dataclass, fields, replace
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from dagster_v3.defs.se_company.financial import tables
from dagster_v3.defs.se_company.financial.precedence import precedence_for

FOLD_VERSION = "financial-fold-v1"
EXCLUDED_SOURCES: tuple[str, ...] = ("reviewer_draft",)
HIDDEN, WITHDRAWN = "hidden", "withdrawn"
CREATED, UPDATED, REACTIVATED = "created", "updated", "reactivated"
# The company-wide rule scope: a precedence row whose period_key is empty (spec 4.4).
COMPANY_WIDE = ""
UNGATED_FIELDS: tuple[str, ...] = (*tables.PERIOD_FIELDS, "employees")

# field -> source -> precedence: the effective rules of ONE period (resolve_rules).
Rules = Mapping[str, Mapping[str, int]]
EMPTY_RULES: Rules = {}
# period_key ('' = company-wide) -> field -> source -> precedence, as the batch reads them.
RulesByPeriod = Mapping[str, Rules]


@dataclass(frozen=True, slots=True)
class Money:
    """One monetary cell of a suggestion row: the native figure and the source's own USD
    conversion. They travel together or not at all."""

    original: Decimal | None
    usd: Decimal | None


@dataclass(frozen=True, slots=True)
class Suggestion:
    """One current live suggestion row of one period. None in a value means no opinion."""

    company_id: str
    source: str
    period_key: str
    scope: str
    period_end: date
    source_record_uid: str
    suggested_at: datetime
    period_start: date | None
    fiscal_year: int | None
    period_months: int | None
    currency: str | None
    money: Mapping[str, Money]  # one entry per field of tables.MONETARY_FIELDS
    employees: int | None


@dataclass(frozen=True, slots=True)
class MoneyCell:
    """One monetary field of a main row: the winner's figure, its USD twin and its source
    ('' with two Nones when no source supplied the field)."""

    original: Decimal | None
    usd: Decimal | None
    source: str


EMPTY_CELL = MoneyCell(None, None, "")


@dataclass(frozen=True, slots=True)
class FinancialRow:
    """One row of se_company_financial minus `folded_at`, which `as_tuple` takes. A _source
    is '' when the field has no value; `currency` is '' when no source names one."""

    company_id: str
    scope: str
    period_end: date
    period_key: str
    period_start: date | None
    period_start_source: str
    fiscal_year: int | None
    fiscal_year_source: str
    period_months: int | None
    period_months_source: str
    currency: str
    currency_source: str
    money: Mapping[str, MoneyCell]  # one entry per field of tables.MONETARY_FIELDS
    employees: int | None
    employees_source: str
    sources: tuple[str, ...]
    active: int
    inactive_reason: str
    fold_version: str
    source_run_id: str

    def field_value(self, field: str) -> tuple[Any, str]:
        """(value, source) of one folded field. A monetary value is the (original, usd) pair
        -- the twin travels with its figure, so a changed conversion is a changed field -- and
        the empty currency reads as None."""
        if field in self.money:
            cell = self.money[field]
            return (cell.original, cell.usd), cell.source
        if field == "currency":
            return (self.currency or None), self.currency_source
        return getattr(self, field), getattr(self, f"{field}_source")

    def as_values(self) -> dict[str, Any]:
        values = {f.name: getattr(self, f.name) for f in fields(self) if f.name != "money"}
        values["sources"] = list(self.sources)
        for field, cell in self.money.items():
            values[tables.original_column(field)] = cell.original
            values[tables.usd_column(field)] = cell.usd
            values[tables.source_column(field)] = cell.source
        return values

    def as_tuple(self, folded_at: datetime) -> tuple[Any, ...]:
        """The row in tables.MAIN_COLUMNS order, ready for an INSERT ... VALUES."""
        values = self.as_values()
        values["folded_at"] = folded_at
        return tuple(values[column] for column in tables.MAIN_COLUMNS)

    def history_tuple(
        self, *, changed_fields: Sequence[str], changed_at: datetime, change_kind: str, fold_run_id: str
    ) -> tuple[Any, ...]:
        """The row in tables.HISTORY_COLUMNS order: the NEW image (spec 4.3, the basic-info
        shape) with what changed and how. `changed_at` is the fold's `folded_at`."""
        return (*self.as_tuple(changed_at), list(changed_fields), changed_at, change_kind, fold_run_id)

    def changed_fields_against(self, other: "FinancialRow | None") -> list[str]:
        """The folded fields whose value or source differ from `other`; every field with a
        value when there is no other row. `fold_version`, `folded_at`, `source_run_id` and the
        activity columns are not compared (activity has `activity_changed_against`)."""
        changed: list[str] = []
        for field in tables.FOLDED_FIELDS:
            value, source = self.field_value(field)
            if other is None:
                if _has_value(field, value):
                    changed.append(field)
                continue
            other_value, other_source = other.field_value(field)
            if value != other_value or source != other_source:
                changed.append(field)
        return changed

    def activity_changed_against(self, other: "FinancialRow | None") -> bool:
        return other is not None and (self.active, self.inactive_reason) != (other.active, other.inactive_reason)


@dataclass(frozen=True, slots=True)
class HistoryEntry:
    """One row of se_company_financial_history: the new image of a period and what happened."""

    row: FinancialRow
    changed_fields: tuple[str, ...]
    change_kind: str


@dataclass(frozen=True, slots=True)
class FoldResult:
    """One company's folded periods: every row to write (active, hidden and withdrawn alike)
    and the history entries for the ones that changed."""

    rows: tuple[FinancialRow, ...]
    history: tuple[HistoryEntry, ...]
    published: int    # active rows in `rows`
    created: int      # the five change kinds count history entries
    updated: int
    hidden: int
    withdrawn: int
    reactivated: int
    unchanged: int    # rows rewritten with no history entry
    unpublished: int  # periods with live rows but no winner and no main row: nothing written


def _has_value(field: str, value: Any) -> bool:
    if field in tables.MONETARY_FIELDS:
        return value[0] is not None
    return value is not None


def resolve_rules(rules_by_period: RulesByPeriod | None, period_key: str) -> Rules:
    """The effective rules of one period: the company-wide rules (period_key '') overlaid by
    the period's own, per (field, source) -- most specific first (spec 5)."""
    if not rules_by_period:
        return EMPTY_RULES
    company_wide = rules_by_period.get(COMPANY_WIDE, {})
    period = rules_by_period.get(period_key, {})
    return {
        field: {**company_wide.get(field, {}), **period.get(field, {})}
        for field in sorted(set(company_wide) | set(period))
    }


def _effective_precedence(field: str, source: str, rules: Rules) -> int | None:
    ruled = rules.get(field, {}).get(source)
    if ruled is not None:
        return ruled
    return precedence_for(field, source)


def _winner(field: str, candidates: Sequence[Suggestion], rules: Rules) -> Suggestion | None:
    """The highest effective precedence among `candidates` (rows that have an opinion on
    `field`), ties to the smaller source name then the smaller source_record_uid; None when
    no candidate's source may supply the field."""
    ranked: list[tuple[int, str, str, Suggestion]] = []
    for suggestion in candidates:
        precedence = _effective_precedence(field, suggestion.source, rules)
        if precedence is None:
            continue
        ranked.append((-precedence, suggestion.source, suggestion.source_record_uid, suggestion))
    if not ranked:
        return None
    ranked.sort(key=lambda item: item[:3])
    return ranked[0][3]


def fold_financial(
    company_id: str,
    period_key: str,
    suggestions: Sequence[Suggestion],
    rules: Rules | None = None,
    hidden: bool = False,
    *,
    source_run_id: str,
) -> FinancialRow | None:
    """Fold one period's current live suggestion rows (spec 6 steps 1 to 6), or None when no
    field has a winner. `rules` are the period's effective rules (resolve_rules); `hidden`
    folds the row normally and lands it with active 0, inactive_reason 'hidden'."""
    rows = [s for s in suggestions if s.source not in EXCLUDED_SOURCES]
    for suggestion in rows:
        if suggestion.company_id != company_id:
            raise ValueError(f"suggestion company_id {suggestion.company_id!r} is not {company_id!r}")
        if suggestion.period_key != period_key:
            raise ValueError(f"suggestion period_key {suggestion.period_key!r} is not {period_key!r}")
    if not rows:
        return None
    active_rules = rules if rules is not None else EMPTY_RULES
    winning_sources: set[str] = set()

    currency_winner = _winner("currency", [s for s in rows if s.currency is not None], active_rules)
    currency = currency_winner.currency if currency_winner is not None else ""
    currency_source = currency_winner.source if currency_winner is not None else ""
    if currency_winner is not None:
        winning_sources.add(currency_winner.source)

    money: dict[str, MoneyCell] = {}
    for field in tables.MONETARY_FIELDS:
        winner = None
        if currency_winner is not None:
            winner = _winner(
                field,
                [s for s in rows if s.currency == currency and s.money[field].original is not None],
                active_rules,
            )
        if winner is None:
            money[field] = EMPTY_CELL
        else:
            money[field] = MoneyCell(winner.money[field].original, winner.money[field].usd, winner.source)
            winning_sources.add(winner.source)

    ungated: dict[str, Any] = {}
    for field in UNGATED_FIELDS:
        winner = _winner(field, [s for s in rows if getattr(s, field) is not None], active_rules)
        if winner is None:
            ungated[field] = None
            ungated[f"{field}_source"] = ""
        else:
            ungated[field] = getattr(winner, field)
            ungated[f"{field}_source"] = winner.source
            winning_sources.add(winner.source)

    if not winning_sources:
        return None
    return FinancialRow(
        company_id=company_id, scope=rows[0].scope, period_end=rows[0].period_end, period_key=period_key,
        currency=currency, currency_source=currency_source, money=money,
        sources=tuple(sorted(winning_sources)),
        active=0 if hidden else 1, inactive_reason=HIDDEN if hidden else "",
        fold_version=FOLD_VERSION, source_run_id=source_run_id,
        **ungated,
    )


def withdrawn_row(previous: FinancialRow, *, source_run_id: str) -> FinancialRow:
    """A period with a main row and no live suggestion keeps its last values with active 0
    (spec 6, batch step 3)."""
    return replace(previous, active=0, inactive_reason=WITHDRAWN, fold_version=FOLD_VERSION, source_run_id=source_run_id)


def change_kind_for(previous: FinancialRow, new: FinancialRow) -> str:
    """What happened to a period whose values, sources or activity changed (spec 4.3's five
    kinds). Reactivation covers a withdrawn period coming back and a hide rule released."""
    if new.inactive_reason == WITHDRAWN and previous.inactive_reason != WITHDRAWN:
        return WITHDRAWN
    if new.inactive_reason == HIDDEN and previous.inactive_reason != HIDDEN:
        return HIDDEN
    if new.active == 1 and previous.active == 0:
        return REACTIVATED
    return UPDATED


def fold_company_periods(
    company_id: str,
    suggestions: Sequence[Suggestion],
    previous: Sequence[FinancialRow],
    rules_by_period: RulesByPeriod | None,
    hidden_periods: Set[str],
    *,
    source_run_id: str,
) -> FoldResult:
    """Every period of one company: the live suggestion rows grouped by period_key, folded
    against the company's current main rows. A period with a main row and no live row is
    withdrawn; a period whose fold changed nothing is still returned (the batch rewrites it so
    folded_at advances); a period with live rows, no winner and no main row writes nothing."""
    for suggestion in suggestions:
        if suggestion.company_id != company_id:
            raise ValueError(f"suggestion company_id {suggestion.company_id!r} is not {company_id!r}")
    for row in previous:
        if row.company_id != company_id:
            raise ValueError(f"main row company_id {row.company_id!r} is not {company_id!r}")
    by_period: dict[str, list[Suggestion]] = {}
    for suggestion in suggestions:
        by_period.setdefault(suggestion.period_key, []).append(suggestion)
    previous_by_period = {row.period_key: row for row in previous}

    rows: list[FinancialRow] = []
    history: list[HistoryEntry] = []
    counts = {CREATED: 0, UPDATED: 0, HIDDEN: 0, WITHDRAWN: 0, REACTIVATED: 0}
    unchanged = unpublished = 0
    for period_key in sorted(set(by_period) | set(previous_by_period)):
        before = previous_by_period.get(period_key)
        folded = None
        if period_key in by_period:
            folded = fold_financial(
                company_id, period_key, by_period[period_key], resolve_rules(rules_by_period, period_key),
                period_key in hidden_periods, source_run_id=source_run_id,
            )
        if folded is None:
            if before is None:
                unpublished += 1
                continue
            folded = withdrawn_row(before, source_run_id=source_run_id)
        changed_fields = folded.changed_fields_against(before)
        if before is None:
            history.append(HistoryEntry(folded, tuple(changed_fields), CREATED))
            counts[CREATED] += 1
        elif changed_fields or folded.activity_changed_against(before):
            kind = change_kind_for(before, folded)
            history.append(HistoryEntry(folded, tuple(changed_fields), kind))
            counts[kind] += 1
        else:
            unchanged += 1
        rows.append(folded)
    return FoldResult(
        rows=tuple(rows), history=tuple(history),
        published=sum(1 for row in rows if row.active == 1),
        created=counts[CREATED], updated=counts[UPDATED], hidden=counts[HIDDEN],
        withdrawn=counts[WITHDRAWN], reactivated=counts[REACTIVATED],
        unchanged=unchanged, unpublished=unpublished,
    )
