# SE company financial entity, slice 3: the fold — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the 7.58M rows of `se_company_financial_suggestion` into one published row per company, scope and period end in `se_company_financial`, with history, through a pure per-period fold and a paged batch layer, exposed as a 64-bucket partitioned asset and a targeted asset, proven on a real ClickHouse, and backfilled on prod.

**Architecture:** `financial/fold.py` is pure: `fold_financial` decides one period (currency first, money gated by the currency with the USD twin travelling with its figure, employees and period fields ungated, rules most specific first, hide) and `fold_company_periods` walks a company's periods against its current main rows (created / updated / hidden / withdrawn / reactivated, unchanged rows still returned). `financial/batch.py` reads a page of companies (five FINAL reads), folds in memory, writes history then main; `changed_only` selects a company when its newest suggestion, precedence decision or hide decision is newer than its newest `folded_at`, or it was never folded and has a live row. `financial/assets.py` gains the two fold assets. Every module text below was written, linted and run on 2026-09-13 (76 unit tests; the clickhouse-local fold test under both `join_use_nulls` settings on ClickHouse 26.5) before this plan was written; the tasks transcribe them.

**Tech Stack:** Python dataclasses + `decimal.Decimal`; ClickHouse 26.5 (`ReplacingMergeTree FINAL`, `Decimal(38, 6)`, `Date32`, `Array(LowCardinality(String))`); Dagster (`StaticPartitionsDefinition`, `BackfillPolicy.multi_run(1)`, op pools, `dg.Config`); pytest with `tests/clickhouse_local.py` (Docker `clickhouse/clickhouse-server:26.5`).

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-11-se-company-financial-entity-design.md` — sections 4.2 to 4.5, 5, 6, 8, 11, 12 item 3, 13; the package note `src/dagster_v3/defs/se_company/financial/docs/financial-design.md`.

## Global Constraints

- Branch `se-financial-entity`, worktree `/Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity` (at 9f75d7462, equal to main; `.env` and `.venv` in place). Paths below are relative to `corpscout/services/dagster_v3` inside that worktree unless they start with `corpscout/`. Never touch the main checkout `/Users/graovic/pulsarpoint/ppoint/companycollect`. Do not use `git stash`.
- No migration in this slice. The main table's CHECKs bind every row the fold writes: `period_key = concat(scope, ':', toString(period_end))`, `scope IN ('standalone', 'consolidated')`, `company_id` ten or twelve digits. A `_source` column is `''` when the field has no value; `currency` is `''` when no source names one; `sources` is the sorted set of sources that won at least one field.
- Spec 6, exactly: currency first (highest effective precedence among rows naming a currency; ties to the smaller source name, then the smaller `source_record_uid`); each of the 20 figures competes only among rows in the row's currency and the winner supplies `_amount_original` and `_amount_usd` together; employees and `period_start`, `fiscal_year`, `period_months` compete ungated; a row without a currency never supplies money; `hidden` folds normally and lands `active = 0, inactive_reason = 'hidden'`; a row is returned when any field has a winner; no live suggestion → None, and the batch withdraws a main row that exists (`active = 0, inactive_reason = 'withdrawn'`, last values kept); a hidden period whose rule was released comes back `reactivated`. `fold_version` is the module constant `financial-fold-v1`.
- Spec 5: rules resolve most specific first — the period rule `(company_id, period_key, field, source)`, else the company-wide rule (`period_key = ''`), else the global map (`precedence.precedence_for`). A rule may rank in a source the global map does not name. `reviewer_draft` never folds. The global export (`company_id = ''`) is never a selection watermark: a precedence change is a manual full re-fold (`changed_only: false` over all 64 buckets).
- Spec 4.3: a history row is the NEW image plus `changed_fields` (the folded fields whose value or source changed; every valued field on first publish), `changed_at` (= the fold's `folded_at`), `change_kind` in created / updated / hidden / withdrawn / reactivated, `fold_run_id`. Written only when values, sources or activity changed.
- Spec 8: `se_company_financial_fold` on 64 static partitions `bucket_00..bucket_63` over `modulo(cityHash64(company_id), 64)`, `BackfillPolicy.multi_run(max_partitions_per_run=1)`, pool `se_company_financial_fold`, config `changed_only` (default true) and `page_size` (default 5,000); `se_company_financial_fold_companies` unpartitioned, `company_ids` required, `changed_only` default false. Both in group `se_company_financial`.
- Pages of 5,000 companies; id-bound statements run with `{"max_query_size": 1_048_576, "max_execution_time": 1800}`.
- Tests run with `uv run --env-file .env pytest ... -q` from `corpscout/services/dagster_v3`; integration tests add `-m integration` (Docker image present locally); `uv run ruff check <files>` clean; `uv run dg check defs` clean where a task touches definitions.
- Commit messages follow Conventional Commits and end with EXACTLY these two lines, pasted verbatim:
  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_013oGzirJgExBzVuy9GQBYHz
- Task 7 (prod) runs after the branch's final review and merge, on the owner's "do it" for this slice.

---

## File structure

| File | Responsibility |
|---|---|
| `src/dagster_v3/defs/se_company/financial/fold.py` (new) | Pure fold: `Suggestion`, `Money`, `MoneyCell`, `FinancialRow`, `HistoryEntry`, `FoldResult`, `resolve_rules`, `fold_financial`, `withdrawn_row`, `change_kind_for`, `fold_company_periods` |
| `src/dagster_v3/defs/se_company/financial/tables.py` (modify) | Gains `LIVE_ROW_PREDICATE` (moved from `suggestions.py`, which re-exports it) so `batch.py` can import it without an import cycle through `assets.py` |
| `src/dagster_v3/defs/se_company/financial/suggestions.py` (modify) | `LIVE_ROW_PREDICATE = tables.LIVE_ROW_PREDICATE` |
| `src/dagster_v3/defs/se_company/financial/batch.py` (new) | The page layer: SQL texts, row converters, the watermark selection, `fold_companies`, `fold_bucket`, `FoldCounts` |
| `src/dagster_v3/defs/se_company/financial/assets.py` (modify) | `FOLD_POOL`, `FINANCIAL_FOLD_PARTITIONS`, `financial_bucket_index`, the two configs, the two fold assets |
| `tests/test_se_company_financial_fold.py` (new) | The pure fold |
| `tests/test_se_company_financial_batch.py` (new) | The batch with a fake client, SQL texts, rendered-size guard |
| `tests/test_se_company_financial_assets.py` (modify) | Registration of the fold assets and their configs |
| `tests/test_se_company_financial_fold_clickhouse_local.py` (new) | Four fold rounds on the real engine |
| `tests/test_se_company_financial_extractors_sql.py`, `tests/test_se_company_financial_extractors_clickhouse_local.py` (modify) | The slice-2 parked nits |
| `src/dagster_v3/defs/se_company/financial/docs/financial-design.md`, the spec (modify) | Fold section; slice record |

---

### Task 1: The pure fold

**Files:**
- Create: `src/dagster_v3/defs/se_company/financial/fold.py`
- Test: `tests/test_se_company_financial_fold.py`

**Interfaces:**
- Consumes: `tables.MONETARY_FIELDS`, `tables.PERIOD_FIELDS`, `tables.FOLDED_FIELDS`, `tables.MAIN_COLUMNS`, `tables.HISTORY_COLUMNS`, `tables.original_column/usd_column/source_column`; `precedence.precedence_for(field, source) -> int | None`.
- Produces: `FOLD_VERSION = "financial-fold-v1"`, `EXCLUDED_SOURCES`, `HIDDEN`, `WITHDRAWN`, `CREATED`, `UPDATED`, `REACTIVATED`, `COMPANY_WIDE = ""`, `UNGATED_FIELDS`, types `Rules`, `RulesByPeriod`, dataclasses `Money(original, usd)`, `Suggestion(company_id, source, period_key, scope, period_end, source_record_uid, suggested_at, period_start, fiscal_year, period_months, currency, money, employees)`, `MoneyCell(original, usd, source)`, `EMPTY_CELL`, `FinancialRow(...)` with `field_value`, `as_values`, `as_tuple(folded_at)`, `history_tuple(*, changed_fields, changed_at, change_kind, fold_run_id)`, `changed_fields_against(other)`, `activity_changed_against(other)`; `HistoryEntry(row, changed_fields, change_kind)`; `FoldResult(rows, history, published, created, updated, hidden, withdrawn, reactivated, unchanged, unpublished)`; functions `resolve_rules(rules_by_period, period_key) -> Rules`, `fold_financial(company_id, period_key, suggestions, rules=None, hidden=False, *, source_run_id) -> FinancialRow | None`, `withdrawn_row(previous, *, source_run_id)`, `change_kind_for(previous, new) -> str`, `fold_company_periods(company_id, suggestions, previous, rules_by_period, hidden_periods, *, source_run_id) -> FoldResult`.

- [ ] **Step 1: Write the failing tests**

`tests/test_se_company_financial_fold.py`:

```python
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
```

- [ ] **Step 2: Run them to verify they fail**

```bash
uv run --env-file .env pytest tests/test_se_company_financial_fold.py -q
```

Expected: FAIL at import, `cannot import name 'fold'` / `ModuleNotFoundError`.

- [ ] **Step 3: Write the module**

`src/dagster_v3/defs/se_company/financial/fold.py`, exactly:

```python
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
```

- [ ] **Step 4: Run the tests and ruff**

```bash
uv run --env-file .env pytest tests/test_se_company_financial_fold.py -q
uv run ruff check src/dagster_v3/defs/se_company/financial/fold.py tests/test_se_company_financial_fold.py
```

Expected: 16 passed; ruff clean.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/financial/fold.py corpscout/services/dagster_v3/tests/test_se_company_financial_fold.py
git commit -m "feat(se-financial): the pure per-period fold, currency first, money gated, rules most specific first"
```

---

### Task 2: The batch layer

**Files:**
- Modify: `src/dagster_v3/defs/se_company/financial/tables.py` (one constant after `SUGGESTION_VALUE_COLUMNS`)
- Modify: `src/dagster_v3/defs/se_company/financial/suggestions.py` (one line)
- Create: `src/dagster_v3/defs/se_company/financial/batch.py`
- Test: `tests/test_se_company_financial_batch.py`; the existing `tests/test_se_company_financial_suggestions.py`, `tests/test_se_company_financial_extractors_sql.py`, `tests/test_se_company_financial_tables.py` must stay green.

**Interfaces:**
- Consumes: Task 1's module; `tables.LIVE_ROW_PREDICATE` (added here); `common.normalized_se_company_ids`.
- Produces: `BUCKET_COUNT = 64`, `PAGE_SIZE = 5_000`, `FOLD_ID_BOUND_QUERY_SETTINGS`, `SUGGESTION_SELECT_COLUMNS` (52), `MAIN_COMPARE_COLUMNS` (77), `RULE_SELECT_COLUMNS`, `FoldCounts` with `as_metadata()`, SQL text functions `bucket_company_ids_sql`, `suggestion_watermarks_sql`, `main_watermarks_sql`, `rule_watermarks_sql`, `hide_watermarks_sql`, `current_suggestions_sql`, `current_main_rows_sql`, `company_rules_sql`, `hidden_periods_sql`, `main_insert_sql`, `history_insert_sql`; converters `suggestion_from_row`, `main_row_from_row`, `rules_by_company`; `fold_companies(client, company_ids, *, changed_only, source_run_id, folded_at, page_size=PAGE_SIZE, log=None) -> FoldCounts`; `fold_bucket(client, bucket, *, changed_only, source_run_id, folded_at, page_size=PAGE_SIZE, log=None) -> FoldCounts`.

- [ ] **Step 1: Move the live-row predicate to `tables.py` (breaks the import cycle assets → batch → suggestions → assets)**

In `src/dagster_v3/defs/se_company/financial/tables.py`, replace exactly

```python
# The value columns a live suggestion row may carry; a tombstone has every one NULL.
SUGGESTION_VALUE_COLUMNS: tuple[str, ...] = (*MONETARY_SUGGESTION_COLUMNS, "employees")
```

with

```python
# The value columns a live suggestion row may carry; a tombstone has every one NULL.
SUGGESTION_VALUE_COLUMNS: tuple[str, ...] = (*MONETARY_SUGGESTION_COLUMNS, "employees")
# A live suggestion row carries at least one of them. Applied on both sides of the extractors'
# state hash (suggestions.py) and to the rows the fold reads (batch.py); defined here, below
# every other module of the package, so both can import it without a cycle.
LIVE_ROW_PREDICATE = "(" + " OR ".join(f"{column} IS NOT NULL" for column in SUGGESTION_VALUE_COLUMNS) + ")"
```

and in `src/dagster_v3/defs/se_company/financial/suggestions.py` replace the line

```python
LIVE_ROW_PREDICATE = "(" + " OR ".join(f"{column} IS NOT NULL" for column in tables.SUGGESTION_VALUE_COLUMNS) + ")"
```

with

```python
LIVE_ROW_PREDICATE = tables.LIVE_ROW_PREDICATE
```

Run `uv run --env-file .env pytest tests/test_se_company_financial_suggestions.py tests/test_se_company_financial_extractors_sql.py tests/test_se_company_financial_tables.py -q` — expected: all passed (the text is identical).

- [ ] **Step 2: Write the failing tests**

`tests/test_se_company_financial_batch.py`:

```python
"""The batch around the pure financial fold (spec 2026-09-11 section 6, batch layer): the
selection watermarks, paging, history before main, withdrawal, the SQL texts and the rendered
size of the id-bound statements. A fake client answers each SELECT by its SQL-text function
name and records every statement."""

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest

from dagster_v3.defs.se_company.financial import batch, tables
from dagster_v3.defs.se_company.financial.fold import FOLD_VERSION
from dagster_v3.defs.se_company.financial.tables import LIVE_ROW_PREDICATE

T0 = datetime(2026, 9, 1, 8, 0, tzinfo=UTC)
T1 = datetime(2026, 9, 2, 8, 0, tzinfo=UTC)
T2 = datetime(2026, 9, 3, 8, 0, tzinfo=UTC)
FOLDED_AT = datetime(2026, 9, 13, 9, 0, tzinfo=UTC)
A, B, C = "5560000001", "5560000002", "5560000003"
P23, P22 = "standalone:2023-12-31", "standalone:2022-12-31"


def suggestion_row(company_id: str, source: str, period_key: str = P23, *, currency: str | None = "SEK",
                   employees: int | None = None, revenue: tuple[str, str] | None = ("100", "10"),
                   equity: tuple[str, str] | None = None) -> tuple:
    """One row in SUGGESTION_SELECT_COLUMNS order, the shape current_suggestions_sql returns."""
    scope, end = period_key.split(":")
    values: dict[str, Any] = {
        "company_id": company_id, "source": source, "period_key": period_key, "scope": scope,
        "period_end": date.fromisoformat(end), "source_record_uid": f"{source}-{period_key}",
        "suggested_at": T0, "period_start": date(int(end[:4]), 1, 1), "fiscal_year": int(end[:4]),
        "period_months": 12, "currency": currency, "employees": employees,
    }
    for column in tables.MONETARY_SUGGESTION_COLUMNS:
        values[column] = None
    for field, pair in (("revenue", revenue), ("equity", equity)):
        if pair is not None:
            values[tables.original_column(field)] = Decimal(pair[0])
            values[tables.usd_column(field)] = Decimal(pair[1])
    return tuple(values[column] for column in batch.SUGGESTION_SELECT_COLUMNS)


def main_row(company_id: str, period_key: str = P23, *, revenue: tuple[str, str, str] = ("100", "10", "ratsit"),
             active: int = 1, inactive_reason: str = "", sources: tuple[str, ...] = ("ratsit",)) -> tuple:
    """One row in MAIN_COMPARE_COLUMNS order, the shape current_main_rows_sql returns."""
    scope, end = period_key.split(":")
    values: dict[str, Any] = {
        "company_id": company_id, "scope": scope, "period_end": date.fromisoformat(end), "period_key": period_key,
        "period_start": date(int(end[:4]), 1, 1), "period_start_source": "ratsit",
        "fiscal_year": int(end[:4]), "fiscal_year_source": "ratsit", "period_months": 12, "period_months_source": "ratsit",
        "currency": "SEK", "currency_source": "ratsit", "employees": None, "employees_source": "",
        "sources": list(sources), "active": active, "inactive_reason": inactive_reason,
    }
    for field in tables.MONETARY_FIELDS:
        values[tables.original_column(field)] = None
        values[tables.usd_column(field)] = None
        values[tables.source_column(field)] = ""
    values["revenue_amount_original"], values["revenue_amount_usd"], values["revenue_source"] = Decimal(revenue[0]), Decimal(revenue[1]), revenue[2]
    return tuple(values[column] for column in batch.MAIN_COMPARE_COLUMNS)


class FakeClient:
    def __init__(self, answers: dict[str, list]) -> None:
        self.answers = answers
        self.calls: list[tuple[str, Any, Any]] = []
        self.inserts: list[tuple[str, list]] = []

    def execute(self, sql, params=None, settings=None):
        self.calls.append((sql, params, settings))
        if sql.startswith("INSERT"):
            self.inserts.append((sql, list(params)))
            return []
        for name, rows in self.answers.items():
            if sql == getattr(batch, name)():
                return rows
        raise AssertionError(f"unexpected SQL: {sql[:90]}")


def answers(**extra) -> dict[str, list]:
    base = {
        "suggestion_watermarks_sql": [], "main_watermarks_sql": [], "rule_watermarks_sql": [],
        "hide_watermarks_sql": [], "current_suggestions_sql": [], "current_main_rows_sql": [],
        "company_rules_sql": [], "hidden_periods_sql": [],
    }
    base.update(extra)
    return base


def run(client, ids, *, changed_only=True, page_size=batch.PAGE_SIZE):
    return batch.fold_companies(client, ids, changed_only=changed_only, source_run_id="run-1", folded_at=FOLDED_AT, page_size=page_size)


def inserted(client, sql_name: str) -> list[dict]:
    columns = tables.MAIN_COLUMNS if sql_name == "main_insert_sql" else tables.HISTORY_COLUMNS
    statement = getattr(batch, sql_name)()
    return [dict(zip(columns, values, strict=True)) for sql, rows in client.inserts if sql == statement for values in rows]


def test_the_sql_texts_pin_final_the_live_predicate_the_draft_exclusion_and_the_hash() -> None:
    assert batch.PAGE_SIZE == 5_000 and batch.BUCKET_COUNT == 64
    assert batch.FOLD_ID_BOUND_QUERY_SETTINGS == {"max_query_size": 1_048_576, "max_execution_time": 1800}
    assert batch.bucket_company_ids_sql() == (
        f"SELECT DISTINCT company_id\nFROM {tables.QUALIFIED_SUGGESTION_TABLE}\n"
        "WHERE modulo(cityHash64(company_id), 64) = %(bucket)s\nORDER BY company_id"
    )
    live = batch.current_suggestions_sql()
    assert live.startswith(f"SELECT {', '.join(batch.SUGGESTION_SELECT_COLUMNS)}\nFROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL\n")
    assert "source != 'reviewer_draft'" in live and f"AND {LIVE_ROW_PREDICATE}\n" in live
    assert live.endswith("ORDER BY company_id, period_key, source")
    marks = batch.suggestion_watermarks_sql()
    assert f"countIf({LIVE_ROW_PREDICATE}) AS live" in marks and " FINAL\n" in marks and "source != 'reviewer_draft'" in marks
    assert batch.main_watermarks_sql() == (
        f"SELECT company_id, max(folded_at) AS folded_at\nFROM {tables.QUALIFIED_MAIN_TABLE}\n"
        "WHERE company_id IN %(company_ids)s\nGROUP BY company_id"
    )
    assert "FINAL" not in batch.rule_watermarks_sql() and "FINAL" not in batch.hide_watermarks_sql()
    assert batch.company_rules_sql().endswith("WHERE company_id IN %(company_ids)s AND removed = 0")
    assert batch.hidden_periods_sql().endswith("WHERE company_id IN %(company_ids)s AND action = 'hide' AND removed = 0")
    assert batch.current_main_rows_sql() == (
        f"SELECT {', '.join(batch.MAIN_COMPARE_COLUMNS)}\nFROM {tables.QUALIFIED_MAIN_TABLE} FINAL\nWHERE company_id IN %(company_ids)s"
    )
    assert batch.main_insert_sql() == f"INSERT INTO {tables.QUALIFIED_MAIN_TABLE} ({', '.join(tables.MAIN_COLUMNS)}) VALUES"
    assert batch.history_insert_sql() == f"INSERT INTO {tables.QUALIFIED_HISTORY_TABLE} ({', '.join(tables.HISTORY_COLUMNS)}) VALUES"
    assert len(batch.SUGGESTION_SELECT_COLUMNS) == 52 and len(batch.MAIN_COMPARE_COLUMNS) == 77
    for sql_name in ("suggestion_watermarks_sql", "main_watermarks_sql", "rule_watermarks_sql", "hide_watermarks_sql",
                     "current_suggestions_sql", "current_main_rows_sql", "company_rules_sql", "hidden_periods_sql"):
        assert getattr(batch, sql_name)().count("%(company_ids)s") == 1, sql_name


def test_a_full_page_of_twelve_digit_ids_renders_well_under_max_query_size() -> None:
    """clickhouse-driver inlines the id list into the statement text; the widest statement of
    the page at PAGE_SIZE must stay under the 1 MiB the settings raise max_query_size to."""
    ids = [f"{199001010000 + index:012d}" for index in range(batch.PAGE_SIZE)]
    rendered_list = "(" + ", ".join(f"'{company_id}'" for company_id in ids) + ")"
    widest = max(
        len(getattr(batch, sql_name)().replace("%(company_ids)s", rendered_list))
        for sql_name in ("suggestion_watermarks_sql", "current_suggestions_sql", "current_main_rows_sql", "company_rules_sql")
    )
    assert 60_000 < widest < batch.FOLD_ID_BOUND_QUERY_SETTINGS["max_query_size"] // 8


def test_selection_by_watermarks() -> None:
    """A: never folded with a live row -> in. B: folded after its newest suggestion -> out.
    C: folded, but a hide decision is newer -> in. D: only tombstones and never folded -> out.
    E: folded, then a precedence decision -> in. F: no suggestion at all -> out."""
    d, e, f = "5560000004", "5560000005", "5560000006"
    client = FakeClient(answers(
        suggestion_watermarks_sql=[(A, T0, 2), (B, T0, 1), (C, T0, 1), (d, T1, 0), (e, T0, 1)],
        main_watermarks_sql=[(B, T1), (C, T1), (e, T1)],
        hide_watermarks_sql=[(C, T2)],
        rule_watermarks_sql=[(e, T2), (B, T0)],
    ))
    assert batch._changed_company_ids(client, [A, B, C, d, e, f]) == [A, C, e]
    assert all(settings == batch.FOLD_ID_BOUND_QUERY_SETTINGS for _, _, settings in client.calls)


def test_a_first_fold_writes_history_then_main_and_rewrites_unchanged_rows() -> None:
    client = FakeClient(answers(
        suggestion_watermarks_sql=[(A, T0, 2), (B, T0, 1)],
        current_suggestions_sql=[
            suggestion_row(A, "bolagsverket", employees=2100, revenue=("59016040", "5877138")),
            suggestion_row(A, "ratsit", revenue=("60300000", "6005001")),
            suggestion_row(A, "ratsit", P22, revenue=("57100000", "5475989")),
            suggestion_row(B, "ratsit", revenue=("1", "1")),
        ],
        current_main_rows_sql=[main_row(B, revenue=("1", "1", "ratsit"))],
    ))
    counts = run(client, [B, A])
    assert (counts.companies, counts.considered, counts.pages) == (2, 2, 1)
    assert (counts.periods, counts.published, counts.created, counts.unchanged, counts.unpublished) == (3, 3, 2, 1, 0)
    statements = [sql.split(" ")[0] + " " + sql.split(" ")[2] for sql, _ in client.inserts]
    assert statements == [f"INSERT {tables.QUALIFIED_HISTORY_TABLE}", f"INSERT {tables.QUALIFIED_MAIN_TABLE}"]
    main = {(row["company_id"], row["period_key"]): row for row in inserted(client, "main_insert_sql")}
    assert set(main) == {(A, P22), (A, P23), (B, P23)}
    a23 = main[(A, P23)]
    assert (a23["revenue_amount_original"], a23["revenue_amount_usd"], a23["revenue_source"]) == (Decimal("60300000"), Decimal("6005001"), "ratsit")
    assert (a23["employees"], a23["employees_source"], a23["sources"]) == (2100, "bolagsverket", ["bolagsverket", "ratsit"])
    assert (a23["folded_at"], a23["fold_version"], a23["source_run_id"], a23["active"]) == (FOLDED_AT, FOLD_VERSION, "run-1", 1)
    assert main[(B, P23)]["folded_at"] == FOLDED_AT     # unchanged, still rewritten
    history = inserted(client, "history_insert_sql")
    assert [(row["company_id"], row["period_key"], row["change_kind"]) for row in history] == [(A, P22, "created"), (A, P23, "created")]
    assert history[1]["changed_fields"] == ["period_start", "fiscal_year", "period_months", "currency", "revenue", "employees"]
    assert history[1]["changed_at"] == FOLDED_AT and history[1]["fold_run_id"] == "run-1"


def test_a_period_the_sources_dropped_is_withdrawn_and_a_hidden_one_lands_inactive() -> None:
    client = FakeClient(answers(
        suggestion_watermarks_sql=[(A, T2, 1)],
        main_watermarks_sql=[(A, T1)],
        current_suggestions_sql=[suggestion_row(A, "ratsit", revenue=("100", "10"))],
        current_main_rows_sql=[main_row(A, P23), main_row(A, P22, revenue=("50", "5", "ratsit"))],
        hidden_periods_sql=[(A, P23)],
    ))
    counts = run(client, [A])
    assert (counts.periods, counts.published, counts.withdrawn, counts.hidden, counts.unchanged) == (2, 0, 1, 1, 0)
    main = {row["period_key"]: row for row in inserted(client, "main_insert_sql")}
    assert (main[P22]["active"], main[P22]["inactive_reason"], main[P22]["revenue_amount_original"]) == (0, "withdrawn", Decimal("50"))
    assert (main[P22]["fold_version"], main[P22]["source_run_id"]) == (FOLD_VERSION, "run-1")
    assert (main[P23]["active"], main[P23]["inactive_reason"]) == (0, "hidden")
    history = {row["period_key"]: row for row in inserted(client, "history_insert_sql")}
    assert (history[P22]["change_kind"], history[P22]["changed_fields"]) == ("withdrawn", [])
    assert (history[P23]["change_kind"], history[P23]["changed_fields"]) == ("hidden", [])


def test_company_rules_reach_the_fold_per_period() -> None:
    """A company-wide rule demotes Ratsit for revenue; a period rule for 2022 restores it."""
    client = FakeClient(answers(
        suggestion_watermarks_sql=[(A, T0, 4)],
        current_suggestions_sql=[
            suggestion_row(A, "ratsit", revenue=("1", "1")), suggestion_row(A, "bolagsverket", revenue=("2", "2")),
            suggestion_row(A, "ratsit", P22, revenue=("3", "3")), suggestion_row(A, "bolagsverket", P22, revenue=("4", "4")),
        ],
        company_rules_sql=[(A, "", "revenue", "ratsit", 100), (A, P22, "revenue", "ratsit", 5000)],
    ))
    run(client, [A])
    main = {row["period_key"]: row for row in inserted(client, "main_insert_sql")}
    assert main[P23]["revenue_source"] == "bolagsverket" and main[P22]["revenue_source"] == "ratsit"
    assert batch.rules_by_company([(A, "", "revenue", "ratsit", 100), (A, P22, "revenue", "ratsit", 5000)]) == {
        A: {"": {"revenue": {"ratsit": 100}}, P22: {"revenue": {"ratsit": 5000}}}
    }


def test_changed_only_false_takes_every_page_and_pages_are_cut_at_page_size() -> None:
    client = FakeClient(answers())
    counts = run(client, [A, B, C], changed_only=False, page_size=2)
    assert (counts.companies, counts.considered, counts.pages, counts.periods) == (3, 3, 2, 0)
    watermark_calls = [sql for sql, _, _ in client.calls if sql == batch.suggestion_watermarks_sql()]
    assert watermark_calls == []
    pages = [params["company_ids"] for sql, params, _ in client.calls if sql == batch.current_suggestions_sql()]
    assert pages == [[A, B], [C]]
    assert client.inserts == []


def test_bad_ids_and_buckets_are_refused_before_any_query() -> None:
    client = FakeClient(answers())
    with pytest.raises(ValueError, match="10 or 12 digits"):
        run(client, ["abc"])
    with pytest.raises(ValueError, match="bucket out of range"):
        batch.fold_bucket(client, 64, changed_only=True, source_run_id="run-1", folded_at=FOLDED_AT)
    assert client.calls == []


def test_fold_bucket_reads_the_bucket_ids_then_folds_them() -> None:
    client = FakeClient(answers(bucket_company_ids_sql=[(A,), (B,)], suggestion_watermarks_sql=[(A, T0, 1)],
                                current_suggestions_sql=[suggestion_row(A, "ratsit")]))
    counts = batch.fold_bucket(client, 7, changed_only=True, source_run_id="run-1", folded_at=FOLDED_AT)
    assert client.calls[0][1] == {"bucket": 7} and (counts.companies, counts.considered, counts.periods) == (2, 1, 1)
```

- [ ] **Step 3: Run them to verify they fail**

```bash
uv run --env-file .env pytest tests/test_se_company_financial_batch.py -q
```

Expected: FAIL at import (`cannot import name 'batch'`).

- [ ] **Step 4: Write the module**

`src/dagster_v3/defs/se_company/financial/batch.py`, exactly:

```python
"""Read a page of companies' current suggestion rows, fold every period in memory, write
history then main (spec 2026-09-11 section 6, batch layer).

Every SELECT is a function returning its exact text, so the clickhouse-local harness runs the
same SQL the asset runs. Parameters bind client-side through clickhouse-driver's %(name)s
syntax, which is why the partition filter says modulo(...) rather than the % operator. The page
is five reads (watermarks aside), a pure fold per company and two inserts.
"""

from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from dagster_v3.defs.se_company.common import normalized_se_company_ids
from dagster_v3.defs.se_company.financial import tables
from dagster_v3.defs.se_company.financial.tables import LIVE_ROW_PREDICATE
from dagster_v3.defs.se_company.financial.fold import (
    EXCLUDED_SOURCES,
    FOLD_VERSION,
    FinancialRow,
    Money,
    MoneyCell,
    Suggestion,
    fold_company_periods,
)

BUCKET_COUNT = 64
# Pages of 5,000 companies, not the siblings' 20,000 (spec 6): a company carries about
# thirteen suggestion rows across its sources and periods and each row is 65 values wide, so a
# page is about 65k rows in memory; the 64-bucket backfill is about 12k companies per bucket,
# three pages each.
PAGE_SIZE = 5_000

# clickhouse-driver renders %(company_ids)s into the statement text; a 5,000-id page is about
# 65 KB per binding and every statement here binds the list once, so 1 MiB is >15x the worst
# case (guard test in tests/test_se_company_financial_batch.py). max_execution_time makes a
# pathological page fail visibly instead of holding the pool slot forever.
FOLD_ID_BOUND_QUERY_SETTINGS = {"max_query_size": 1_048_576, "max_execution_time": 1800}

# What the fold needs of a live suggestion row; amount_scale, the fx columns and the stamps
# stay behind (the backoffice shows them from the suggestion row).
SUGGESTION_SELECT_COLUMNS: tuple[str, ...] = (
    "company_id", "source", "period_key", "scope", "period_end", "source_record_uid", "suggested_at",
    "period_start", "fiscal_year", "period_months", "currency",
    *tables.MONETARY_SUGGESTION_COLUMNS,
    "employees",
)
# The main read compares values, sources and activity; folded_at, fold_version and
# source_run_id are the writer's, never compared, and a withdrawal replaces them.
MAIN_COMPARE_COLUMNS: tuple[str, ...] = tuple(
    column for column in tables.MAIN_COLUMNS if column not in ("folded_at", "fold_version", "source_run_id")
)
RULE_SELECT_COLUMNS: tuple[str, ...] = ("company_id", "period_key", "field", "source", "precedence")
_EXCLUDED_SQL = " AND ".join(f"source != '{source}'" for source in EXCLUDED_SOURCES)


@dataclass(frozen=True, slots=True)
class FoldCounts:
    companies: int      # ids handed in
    considered: int     # ids the selection picked (== companies when changed_only=False)
    pages: int
    periods: int        # main rows written (active, hidden and withdrawn alike)
    published: int      # active rows written
    created: int        # the five change kinds are history rows, not row states
    updated: int
    hidden: int
    withdrawn: int
    reactivated: int
    unchanged: int      # rows rewritten with no history row
    unpublished: int    # periods with live rows, no winner and no main row: nothing written

    def as_metadata(self) -> dict[str, Any]:
        return {
            "companies": self.companies, "considered": self.considered, "pages": self.pages,
            "periods": self.periods, "published": self.published, "created": self.created,
            "updated": self.updated, "hidden": self.hidden, "withdrawn": self.withdrawn,
            "reactivated": self.reactivated, "unchanged": self.unchanged,
            "unpublished": self.unpublished, "fold_version": FOLD_VERSION,
        }


def bucket_company_ids_sql() -> str:
    """The same hash and modulus as the sibling folds -- never a second hash function."""
    return (
        "SELECT DISTINCT company_id\n"
        f"FROM {tables.QUALIFIED_SUGGESTION_TABLE}\n"
        f"WHERE modulo(cityHash64(company_id), {BUCKET_COUNT}) = %(bucket)s\n"
        "ORDER BY company_id"
    )


def suggestion_watermarks_sql() -> str:
    """Newest non-draft suggestion per company and how many of its current rows are live.
    FINAL, so a period whose current version is a tombstone does not count as live through
    an older version -- and the max is over EVERY row, because a tombstone arriving changes
    the published set as much as a figure does. reviewer_draft is excluded: saving an
    unactivated draft must not wake the company up (the basic-info ruling of slice 3c)."""
    return (
        "SELECT company_id, max(suggested_at) AS suggested_at, "
        f"countIf({LIVE_ROW_PREDICATE}) AS live\n"
        f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL\n"
        f"WHERE company_id IN %(company_ids)s AND {_EXCLUDED_SQL}\n"
        "GROUP BY company_id"
    )


def main_watermarks_sql() -> str:
    return (
        "SELECT company_id, max(folded_at) AS folded_at\n"
        f"FROM {tables.QUALIFIED_MAIN_TABLE}\n"
        "WHERE company_id IN %(company_ids)s\n"
        "GROUP BY company_id"
    )


def rule_watermarks_sql() -> str:
    """Every precedence decision of the company, released versions included: a release is a
    new version with removed = 1 and a newer decided_at, and a company that could not see it
    would apply the rule for ever. No FINAL: max() over the versions is the newest anyway. The
    global export (company_id '') is never in the id list, so it never selects a company on
    its own (spec 6, batch step 2)."""
    return (
        "SELECT company_id, max(decided_at) AS decided_at\n"
        f"FROM {tables.QUALIFIED_PRECEDENCE_TABLE}\n"
        "WHERE company_id IN %(company_ids)s\n"
        "GROUP BY company_id"
    )


def hide_watermarks_sql() -> str:
    return (
        "SELECT company_id, max(decided_at) AS decided_at\n"
        f"FROM {tables.QUALIFIED_RULE_TABLE}\n"
        "WHERE company_id IN %(company_ids)s\n"
        "GROUP BY company_id"
    )


def current_suggestions_sql() -> str:
    """The page's current LIVE suggestion rows: tombstones (every value NULL) and drafts never
    reach the fold."""
    return (
        f"SELECT {', '.join(SUGGESTION_SELECT_COLUMNS)}\n"
        f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL\n"
        f"WHERE company_id IN %(company_ids)s AND {_EXCLUDED_SQL}\n"
        f"    AND {LIVE_ROW_PREDICATE}\n"
        "ORDER BY company_id, period_key, source"
    )


def current_main_rows_sql() -> str:
    return (
        f"SELECT {', '.join(MAIN_COMPARE_COLUMNS)}\n"
        f"FROM {tables.QUALIFIED_MAIN_TABLE} FINAL\n"
        "WHERE company_id IN %(company_ids)s"
    )


def company_rules_sql() -> str:
    """The page's active company rules, newest version per key via FINAL; period_key '' is a
    company-wide rule, anything else one period (spec 4.4)."""
    return (
        f"SELECT {', '.join(RULE_SELECT_COLUMNS)}\n"
        f"FROM {tables.QUALIFIED_PRECEDENCE_TABLE} FINAL\n"
        "WHERE company_id IN %(company_ids)s AND removed = 0"
    )


def hidden_periods_sql() -> str:
    return (
        "SELECT company_id, period_key\n"
        f"FROM {tables.QUALIFIED_RULE_TABLE} FINAL\n"
        "WHERE company_id IN %(company_ids)s AND action = 'hide' AND removed = 0"
    )


def main_insert_sql() -> str:
    return f"INSERT INTO {tables.QUALIFIED_MAIN_TABLE} ({', '.join(tables.MAIN_COLUMNS)}) VALUES"


def history_insert_sql() -> str:
    return f"INSERT INTO {tables.QUALIFIED_HISTORY_TABLE} ({', '.join(tables.HISTORY_COLUMNS)}) VALUES"


def suggestion_from_row(row: Sequence[Any]) -> Suggestion:
    values = dict(zip(SUGGESTION_SELECT_COLUMNS, row, strict=True))
    money = {
        field: Money(values.pop(tables.original_column(field)), values.pop(tables.usd_column(field)))
        for field in tables.MONETARY_FIELDS
    }
    return Suggestion(money=money, **values)


# Comparison and withdrawal only: fold_version and source_run_id are filled with "" and
# replaced by the fold before anything is written.
def main_row_from_row(row: Sequence[Any]) -> FinancialRow:
    values = dict(zip(MAIN_COMPARE_COLUMNS, row, strict=True))
    money = {
        field: MoneyCell(
            values.pop(tables.original_column(field)), values.pop(tables.usd_column(field)),
            values.pop(tables.source_column(field)),
        )
        for field in tables.MONETARY_FIELDS
    }
    values["sources"] = tuple(values["sources"])
    return FinancialRow(money=money, fold_version="", source_run_id="", **values)


def rules_by_company(rows: Sequence[Sequence[Any]]) -> dict[str, dict[str, dict[str, dict[str, int]]]]:
    """(company_id, period_key, field, source, precedence) rows, as read by company_rules_sql,
    -> company -> period_key -> field -> source -> precedence (fold.resolve_rules's input)."""
    out: dict[str, dict[str, dict[str, dict[str, int]]]] = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
    for company_id, period_key, field, source, precedence in rows:
        out[company_id][period_key][field][source] = int(precedence)
    return {
        company: {period: {field: dict(sources) for field, sources in fields.items()} for period, fields in periods.items()}
        for company, periods in out.items()
    }


def _pages(items: Sequence[str], size: int) -> list[list[str]]:
    return [list(items[index : index + size]) for index in range(0, len(items), size)]


def _changed_company_ids(client: Any, company_ids: list[str]) -> list[str]:
    """The companies to re-fold: those whose newest suggestion, precedence decision or hide
    decision is newer than their newest folded_at, and those never folded that have a live
    row. A company with only tombstones and no main row stays out: there is nothing to
    publish and nothing to withdraw."""
    params = {"company_ids": company_ids}

    def read(sql: str) -> list:
        return client.execute(sql, params, settings=FOLD_ID_BOUND_QUERY_SETTINGS)

    suggested = {row[0]: (row[1], int(row[2])) for row in read(suggestion_watermarks_sql())}
    folded = dict(read(main_watermarks_sql()))
    ruled = dict(read(rule_watermarks_sql()))
    hidden = dict(read(hide_watermarks_sql()))
    changed: list[str] = []
    for company_id in company_ids:
        if company_id not in suggested:
            continue
        newest, live = suggested[company_id]
        for stamp in (ruled.get(company_id), hidden.get(company_id)):
            if stamp is not None and stamp > newest:
                newest = stamp
        if company_id not in folded:
            if live > 0:
                changed.append(company_id)
        elif newest > folded[company_id]:
            changed.append(company_id)
    return changed


def fold_companies(
    client: Any,
    company_ids: Sequence[str],
    *,
    changed_only: bool,
    source_run_id: str,
    folded_at: datetime,
    page_size: int = PAGE_SIZE,
    log: Callable[..., object] | None = None,
) -> FoldCounts:
    """Fold the given companies in pages of `page_size`. Every folded company's every period
    is rewritten with this `folded_at` -- active, hidden and withdrawn rows alike -- so the
    changed_only selection converges (the basic-info decision of 2026-09-04); history rows are
    written only where values, sources or activity changed, and always BEFORE the main
    insert."""
    ids = list(normalized_se_company_ids(company_ids))
    considered = pages = periods = published = unchanged = unpublished = 0
    kinds = {"created": 0, "updated": 0, "hidden": 0, "withdrawn": 0, "reactivated": 0}
    for page in _pages(ids, page_size):
        pages += 1
        scope = _changed_company_ids(client, page) if changed_only else page
        considered += len(scope)
        if not scope:
            continue
        params = {"company_ids": scope}

        def read(sql: str) -> list:
            return client.execute(sql, params, settings=FOLD_ID_BOUND_QUERY_SETTINGS)

        by_company: dict[str, list[Suggestion]] = defaultdict(list)
        for row in read(current_suggestions_sql()):
            suggestion = suggestion_from_row(row)
            by_company[suggestion.company_id].append(suggestion)
        current: dict[str, list[FinancialRow]] = defaultdict(list)
        for row in read(current_main_rows_sql()):
            main_row = main_row_from_row(row)
            current[main_row.company_id].append(main_row)
        rules = rules_by_company(read(company_rules_sql()))
        hidden_periods: dict[str, set[str]] = defaultdict(set)
        for company_id, period_key in read(hidden_periods_sql()):
            hidden_periods[company_id].add(period_key)

        main_rows: list[tuple[Any, ...]] = []
        history_rows: list[tuple[Any, ...]] = []
        page_published = 0
        for company_id in scope:
            suggestions = by_company.get(company_id, [])
            previous = current.get(company_id, [])
            if not suggestions and not previous:
                continue
            result = fold_company_periods(
                company_id, suggestions, previous, rules.get(company_id), hidden_periods.get(company_id, set()),
                source_run_id=source_run_id,
            )
            page_published += result.published
            unchanged += result.unchanged
            unpublished += result.unpublished
            for kind in kinds:
                kinds[kind] += getattr(result, kind)
            main_rows.extend(row.as_tuple(folded_at) for row in result.rows)
            history_rows.extend(
                entry.row.history_tuple(
                    changed_fields=entry.changed_fields, changed_at=folded_at,
                    change_kind=entry.change_kind, fold_run_id=source_run_id,
                )
                for entry in result.history
            )
        published += page_published
        periods += len(main_rows)
        if history_rows:
            # History first: a crash between the two statements costs at most a page's
            # duplicate history rows on retry -- each attempt with its own changed_at, so the
            # plain MergeTree keeps both -- which is cheaper than the other order's failure
            # mode, a published row whose first history row is missing.
            client.execute(history_insert_sql(), history_rows)
        if main_rows:
            client.execute(main_insert_sql(), main_rows)
        if log is not None:
            log(
                "Folded financial page %d: companies=%d considered=%d periods=%d published=%d history=%d",
                pages, len(page), len(scope), len(main_rows), page_published, len(history_rows),
            )
    return FoldCounts(
        companies=len(ids), considered=considered, pages=pages, periods=periods, published=published,
        created=kinds["created"], updated=kinds["updated"], hidden=kinds["hidden"],
        withdrawn=kinds["withdrawn"], reactivated=kinds["reactivated"], unchanged=unchanged,
        unpublished=unpublished,
    )


def fold_bucket(
    client: Any,
    bucket: int,
    *,
    changed_only: bool,
    source_run_id: str,
    folded_at: datetime,
    page_size: int = PAGE_SIZE,
    log: Callable[..., object] | None = None,
) -> FoldCounts:
    """Fold every company whose id hashes into `bucket` (0..63)."""
    if not 0 <= bucket < BUCKET_COUNT:
        raise ValueError(f"bucket out of range: {bucket}")
    company_ids = [
        row[0]
        for row in client.execute(bucket_company_ids_sql(), {"bucket": bucket}, settings=FOLD_ID_BOUND_QUERY_SETTINGS)
    ]
    return fold_companies(
        client, company_ids, changed_only=changed_only, source_run_id=source_run_id,
        folded_at=folded_at, page_size=page_size, log=log,
    )
```

- [ ] **Step 5: Run the tests and ruff**

```bash
uv run --env-file .env pytest tests/test_se_company_financial_batch.py tests/test_se_company_financial_suggestions.py tests/test_se_company_financial_extractors_sql.py tests/test_se_company_financial_tables.py -q
uv run ruff check src/dagster_v3/defs/se_company/financial tests/test_se_company_financial_batch.py
```

Expected: 9 batch tests passed plus the three existing files green; ruff clean.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/financial/tables.py corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/financial/suggestions.py corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/financial/batch.py corpscout/services/dagster_v3/tests/test_se_company_financial_batch.py
git commit -m "feat(se-financial): the fold's batch layer, five FINAL reads per page, history before main"
```

---

### Task 3: The two fold assets

**Files:**
- Modify: `src/dagster_v3/defs/se_company/financial/assets.py` (replace the whole file with the text below; the precedence export inside it is unchanged)
- Modify: `tests/test_se_company_financial_assets.py` (replace the whole file with the text below; the four export tests inside it are unchanged)

**Interfaces:**
- Consumes: Task 2's `BUCKET_COUNT`, `PAGE_SIZE`, `FoldCounts`, `fold_bucket`, `fold_companies`; `common.normalized_se_company_ids`.
- Produces: `FOLD_POOL = "se_company_financial_fold"`, `FINANCIAL_FOLD_PARTITIONS`, `financial_bucket_index(partition_key) -> int`, `FinancialFoldConfig(changed_only=True, page_size=5000)`, `FinancialFoldCompaniesConfig(company_ids, changed_only=False, page_size=5000)`, assets `se_company_financial_fold` (partitioned, pooled, deps on the four extractor asset keys) and `se_company_financial_fold_companies`.

- [ ] **Step 1: Write the test file (the whole file)**

`tests/test_se_company_financial_assets.py`, exactly:

```python
"""The precedence export (spec section 8): global rows only, idempotent, and registered
without dependencies; the two fold assets (slice 3): 64 static buckets, one partition per run,
the serial pool, the extractor deps, the configs."""

from datetime import UTC, datetime

import dagster as dg
import pytest

from dagster_v3.defs.se_company.financial import tables
from dagster_v3.defs.se_company.financial.assets import (
    EXTRACTOR_ASSET_NAMES,
    FOLD_POOL,
    GROUP_NAME,
    FinancialFoldCompaniesConfig,
    FinancialFoldConfig,
    export_precedence,
    financial_bucket_index,
    se_company_financial_fold,
    se_company_financial_fold_companies,
)
from dagster_v3.defs.se_company.financial.precedence import precedence_rows
from dagster_v3.definitions import defs as load_project_defs

_SELECT_STORED_SQL = (
    f"SELECT field, source, precedence FROM {tables.QUALIFIED_PRECEDENCE_TABLE} "
    "FINAL WHERE company_id = '' AND period_key = '' AND removed = 0"
)
_INSERT_SQL = (
    f"INSERT INTO {tables.QUALIFIED_PRECEDENCE_TABLE} "
    f"({', '.join(tables.PRECEDENCE_COLUMNS)}) VALUES"
)


class FakeClient:
    """Dispatches on the whole statement text against the table's own qualified name."""

    def __init__(self, stored: list[tuple] = ()) -> None:
        self.calls: list[tuple[str, object]] = []
        self.stored = list(stored)

    def execute(self, sql, params=None, settings=None):
        self.calls.append((sql, params))
        if sql == _SELECT_STORED_SQL:
            return list(self.stored)
        if sql.startswith(f"INSERT INTO {tables.QUALIFIED_PRECEDENCE_TABLE}"):
            return []
        raise AssertionError(f"unexpected statement: {sql!r}")


def test_export_inserts_the_82_global_rows_and_counts_stale_pairs() -> None:
    client = FakeClient(
        stored=[("period_start", "reviewer", 20000), ("legacy_field", "ratsit", 1000)]
    )
    exported_at = datetime(2026, 9, 12, 8, 0, 0, 123000, tzinfo=UTC)
    pairs, stale = export_precedence(client, exported_at)
    assert (pairs, stale) == (82, 1)
    select_sql, _ = client.calls[0]
    assert select_sql == _SELECT_STORED_SQL
    insert_sql, rows = client.calls[1]
    assert insert_sql == _INSERT_SQL
    assert rows[0] == ("", "", "period_start", "reviewer", 20000, 0, "code", "", exported_at)
    assert all(row[0] == "" and row[1] == "" for row in rows)
    assert [row[2:5] for row in rows] == precedence_rows()
    assert [call[0].split(" ", 1)[0] for call in client.calls] == ["SELECT", "INSERT"]


def test_export_against_a_changed_dictionary_inserts_every_row() -> None:
    stored = list(precedence_rows())
    stored[-1] = (stored[-1][0], stored[-1][1], 999)  # one number differs from the dictionary
    client = FakeClient(stored=stored)
    pairs, stale = export_precedence(client, datetime(2026, 9, 12, 9, 0, tzinfo=UTC))
    assert (pairs, stale) == (82, 0)
    assert [call[0].split(" ", 1)[0] for call in client.calls] == ["SELECT", "INSERT"]


def test_export_against_matching_stored_rows_inserts_nothing() -> None:
    client = FakeClient(stored=list(precedence_rows()))
    pairs, stale = export_precedence(client, datetime(2026, 9, 12, 10, 0, tzinfo=UTC))
    assert (pairs, stale) == (0, 0)
    assert [call[0].split(" ", 1)[0] for call in client.calls] == ["SELECT"]


def test_a_pair_the_dictionary_no_longer_names_is_stale_but_never_deleted() -> None:
    stored = [*precedence_rows(), ("revenue", "wikidata", 200)]
    client = FakeClient(stored=stored)
    pairs, stale = export_precedence(client, datetime(2026, 9, 12, 11, 0, tzinfo=UTC))
    assert (pairs, stale) == (82, 1)
    assert [call[0].split(" ", 1)[0] for call in client.calls] == ["SELECT", "INSERT"]


def test_the_export_asset_is_registered_without_dependencies() -> None:
    repository = load_project_defs().get_repository_def()
    node = repository.asset_graph.get(dg.AssetKey("se_company_financial_precedence_clickhouse"))
    assert node.parent_keys == set()
    assert node.partitions_def is None
    assert node.group_name == GROUP_NAME == "se_company_financial"


def test_the_bucket_fold_is_partitioned_pooled_and_downstream_of_the_four_extractors() -> None:
    asset = se_company_financial_fold
    assert asset.key == dg.AssetKey("se_company_financial_fold")
    keys = asset.partitions_def.get_partition_keys()
    assert len(keys) == 64 and keys[0] == "bucket_00" and keys[-1] == "bucket_63"
    assert asset.backfill_policy.max_partitions_per_run == 1
    assert asset.op.pool == FOLD_POOL == "se_company_financial_fold"
    assert asset.dependency_keys == {dg.AssetKey(name) for name in EXTRACTOR_ASSET_NAMES}
    spec = next(iter(asset.specs))
    assert spec.group_name == GROUP_NAME
    assert spec.metadata["table"] == tables.QUALIFIED_MAIN_TABLE
    keys = load_project_defs().get_repository_def().asset_graph.get_all_asset_keys()
    assert dg.AssetKey("se_company_financial_fold") in keys
    assert dg.AssetKey("se_company_financial_fold_companies") in keys


def test_the_targeted_fold_is_unpartitioned_and_unpooled() -> None:
    asset = se_company_financial_fold_companies
    assert asset.partitions_def is None and asset.op.pool is None
    assert asset.dependency_keys == set()
    assert next(iter(asset.specs)).group_name == GROUP_NAME


def test_the_fold_configs_default_to_the_spec_and_validate_ids() -> None:
    assert (FinancialFoldConfig().changed_only, FinancialFoldConfig().page_size) == (True, 5000)
    targeted = FinancialFoldCompaniesConfig(company_ids=[" 5567081699 ", "5567081699"])
    assert (targeted.company_ids, targeted.changed_only, targeted.page_size) == (["5567081699"], False, 5000)
    with pytest.raises(ValueError):
        FinancialFoldCompaniesConfig(company_ids=[])
    with pytest.raises(ValueError, match="10 or 12 digits"):
        FinancialFoldCompaniesConfig(company_ids=["abc"])


def test_bucket_keys_parse_and_bad_ones_are_refused() -> None:
    assert financial_bucket_index("bucket_00") == 0 and financial_bucket_index("bucket_63") == 63
    with pytest.raises(ValueError, match="invalid financial fold partition key"):
        financial_bucket_index("bucket_7")
    with pytest.raises(ValueError, match="out of range"):
        financial_bucket_index("bucket_64")
```

- [ ] **Step 2: Run it to verify the new tests fail**

```bash
uv run --env-file .env pytest tests/test_se_company_financial_assets.py -q
```

Expected: FAIL at import (`cannot import name 'FOLD_POOL'`).

- [ ] **Step 3: Write the assets module (the whole file)**

`src/dagster_v3/defs/se_company/financial/assets.py`, exactly:

```python
"""Dagster assets of the financial entity: the precedence export (slice 1) and the two fold
assets (slice 3). The extractors and the extract job live in their own modules (slice 2)."""

import re
from datetime import UTC, datetime
from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field, field_validator

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.se_company.common import normalized_se_company_ids
from dagster_v3.defs.se_company.financial import tables
from dagster_v3.defs.se_company.financial.batch import (
    BUCKET_COUNT,
    PAGE_SIZE as FOLD_PAGE_SIZE,
    FoldCounts,
    fold_bucket,
    fold_companies,
)
from dagster_v3.defs.se_company.financial.precedence import precedence_rows

GROUP_NAME = "se_company_financial"
# The bucket fold's page reads are primary-key seeks scattered over the whole suggestion table
# (the bucket hash spreads a page's ids across every granule); the instance defaults every pool
# to limit 1 (dagster.yaml), so this pool runs the 64 buckets one at a time and a backfill
# can never put sixty-four FINAL reads on the server at once (spec section 8). The targeted
# fold below (a few ids) stays unpooled.
FOLD_POOL = "se_company_financial_fold"
EXTRACTOR_SOURCES: tuple[str, ...] = ("bolagsverket", "bolagsverket_comparative", "esef", "ratsit")
EXTRACTOR_ASSET_NAMES: tuple[str, ...] = tuple(
    f"se_company_financial_suggestions_{source}" for source in EXTRACTOR_SOURCES
)


def export_precedence(client: Any, exported_at: datetime) -> tuple[int, int]:
    """Insert every (field, source, precedence) pair as a global rule (company_id '',
    period_key '', decided_by 'code'). Returns (pairs inserted, stale pairs): stale is the
    count of global (field, source) pairs present in ClickHouse that the dictionary no
    longer names at all -- they stay until removed by hand (the export never deletes). A
    pair whose number merely changed is not stale; the insert corrects it. Never touches a
    company-scoped row.

    Read the stored global rows first: when they already equal `precedence_rows()`, insert
    nothing and report 0 pairs (the caller reads that as `unchanged`); otherwise insert every
    pair."""
    wanted = precedence_rows()
    stored = {
        tuple(row)
        for row in client.execute(
            f"SELECT field, source, precedence FROM {tables.QUALIFIED_PRECEDENCE_TABLE} "
            "FINAL WHERE company_id = '' AND period_key = '' AND removed = 0"
        )
    }
    pairs = 0
    if stored != set(wanted):
        rows = [
            ("", "", field, source, precedence, 0, "code", "", exported_at)
            for field, source, precedence in wanted
        ]
        client.execute(
            f"INSERT INTO {tables.QUALIFIED_PRECEDENCE_TABLE} "
            f"({', '.join(tables.PRECEDENCE_COLUMNS)}) VALUES",
            rows,
        )
        pairs = len(rows)
    stale = len(
        {(field, source) for field, source, _ in stored}
        - {(field, source) for field, source, _ in wanted}
    )
    return pairs, stale


@dg.asset(
    name="se_company_financial_precedence_clickhouse",
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={"table": tables.QUALIFIED_PRECEDENCE_TABLE},
    description=(
        "Exports FINANCIAL_PRECEDENCE to se_company_financial_precedence as global rules "
        "(company_id '', period_key '') for the fold and the backoffice to read. The Python "
        "dictionary is the only source for these rows; re-run after changing it, then "
        "re-fold every bucket with changed_only false (a precedence change is not a "
        "per-company change). Idle re-materialisation is a no-op: when the stored rows "
        "already match the dictionary, nothing is written and the watermark does not move. "
        "Never touches a company-scoped row."
    ),
)
def se_company_financial_precedence_clickhouse(
    context: dg.AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse, database=tables.DATABASE, tables=(tables.PRECEDENCE_TABLE,)
    )
    exported_at = datetime.now(UTC)
    with clickhouse.get_connection() as client:
        pairs, stale = export_precedence(client, exported_at)
    if stale:
        context.log.warning(
            "%d precedence pairs exist in ClickHouse that the dictionary no longer names; "
            "they stay until removed by hand",
            stale,
        )
    return dg.MaterializeResult(
        metadata={
            "pairs": pairs, "stale_pairs": stale, "unchanged": pairs == 0,
            "table": tables.QUALIFIED_PRECEDENCE_TABLE,
        }
    )


FINANCIAL_FOLD_PARTITIONS = dg.StaticPartitionsDefinition(
    [f"bucket_{bucket:02d}" for bucket in range(BUCKET_COUNT)]
)
_FOLD_TABLES = (
    tables.SUGGESTION_TABLE, tables.MAIN_TABLE, tables.HISTORY_TABLE,
    tables.PRECEDENCE_TABLE, tables.RULE_TABLE,
)


def financial_bucket_index(partition_key: str) -> int:
    match = re.fullmatch(r"bucket_(\d{2})", partition_key)
    if match is None:
        raise ValueError(f"invalid financial fold partition key: {partition_key!r}")
    bucket = int(match.group(1))
    if not 0 <= bucket < BUCKET_COUNT:
        raise ValueError(f"financial fold bucket out of range: {bucket}")
    return bucket


class FinancialFoldConfig(dg.Config):
    # True: only companies whose newest suggestion, precedence decision or hide decision is
    # newer than their last fold, plus companies never folded that have a live row. False
    # re-folds the whole bucket (what a precedence change needs, spec 5); history rows are
    # written either way only where values, sources or activity changed.
    changed_only: bool = True
    # Companies per page (spec 6: 5,000, about 65k suggestion rows in memory).
    page_size: int = Field(default=FOLD_PAGE_SIZE, ge=1, le=20_000)


class FinancialFoldCompaniesConfig(dg.Config):
    company_ids: list[str] = Field(min_length=1)
    changed_only: bool = False
    page_size: int = Field(default=FOLD_PAGE_SIZE, ge=1, le=20_000)

    @field_validator("company_ids")
    @classmethod
    def _valid_ids(cls, value: list[str]) -> list[str]:
        return list(normalized_se_company_ids(value))


def _fold_metadata(counts: FoldCounts, config: dg.Config, **extra: Any) -> dict[str, Any]:
    return {
        **counts.as_metadata(),
        "changed_only": config.changed_only,
        "page_size": config.page_size,
        "table": tables.QUALIFIED_MAIN_TABLE,
        "history_table": tables.QUALIFIED_HISTORY_TABLE,
        **extra,
    }


@dg.asset(
    name="se_company_financial_fold",
    partitions_def=FINANCIAL_FOLD_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    group_name=GROUP_NAME,
    pool=FOLD_POOL,
    deps=[dg.AssetKey(name) for name in EXTRACTOR_ASSET_NAMES],
    kinds={"clickhouse", "python"},
    metadata={"table": tables.QUALIFIED_MAIN_TABLE, "history_table": tables.QUALIFIED_HISTORY_TABLE},
    description=(
        "Folds the current financial suggestion rows of the companies in one of 64 hash buckets "
        "into se_company_financial, one row per company, scope and period end: the currency is "
        "decided first by precedence (Ratsit, then the registers), each figure competes only "
        "among rows in that currency and brings its own USD twin, employees and the period "
        "attributes compete ungated, reviewer rules re-rank and hide rules deactivate, a period "
        "the sources stopped delivering is withdrawn, and every change is appended to "
        "se_company_financial_history first. changed_only=false re-folds the whole bucket (run "
        "over all 64 after a precedence change). Pooled at FOLD_POOL (instance default limit 1), "
        "so a backfill runs one bucket at a time. Manual: launch a partition or a backfill."
    ),
)
def se_company_financial_fold(
    context: dg.AssetExecutionContext, config: FinancialFoldConfig, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(clickhouse, database=tables.DATABASE, tables=_FOLD_TABLES)
    bucket = financial_bucket_index(context.partition_key)
    with clickhouse.get_connection() as client:
        counts = fold_bucket(
            client, bucket, changed_only=config.changed_only, source_run_id=context.run_id,
            folded_at=datetime.now(UTC), page_size=config.page_size, log=context.log.info,
        )
    return dg.MaterializeResult(metadata=_fold_metadata(counts, config, bucket=bucket))


@dg.asset(
    name="se_company_financial_fold_companies",
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={"table": tables.QUALIFIED_MAIN_TABLE, "history_table": tables.QUALIFIED_HISTORY_TABLE},
    description=(
        "The targeted financial fold: the companies named in config.company_ids, whatever their "
        "bucket, changed_only false by default. The backoffice's Fold now button (slice 4) "
        "launches this asset for one company."
    ),
)
def se_company_financial_fold_companies(
    context: dg.AssetExecutionContext, config: FinancialFoldCompaniesConfig, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(clickhouse, database=tables.DATABASE, tables=_FOLD_TABLES)
    with clickhouse.get_connection() as client:
        counts = fold_companies(
            client, config.company_ids, changed_only=config.changed_only, source_run_id=context.run_id,
            folded_at=datetime.now(UTC), page_size=config.page_size, log=context.log.info,
        )
    return dg.MaterializeResult(metadata=_fold_metadata(counts, config))
```

- [ ] **Step 4: Run the tests, the definitions check and ruff**

```bash
uv run --env-file .env pytest tests/test_se_company_financial_assets.py tests/test_se_company_financial_jobs.py -q
uv run dg check defs
uv run ruff check src/dagster_v3/defs/se_company/financial/assets.py tests/test_se_company_financial_assets.py
```

Expected: 9 + 3 passed; `All definitions loaded successfully.`; ruff clean.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/financial/assets.py corpscout/services/dagster_v3/tests/test_se_company_financial_assets.py
git commit -m "feat(se-financial): the 64-bucket fold asset and the targeted fold asset"
```

---

### Task 4: The fold on a real ClickHouse

**Files:**
- Create: `tests/test_se_company_financial_fold_clickhouse_local.py`

**Interfaces:**
- Consumes: Tasks 1 to 3; migration `000401`; `tests/clickhouse_local.py`'s `clickhouse_local_command`.

- [ ] **Step 1: Write the test**

`tests/test_se_company_financial_fold_clickhouse_local.py`, exactly:

```python
"""The financial fold end to end against a real ClickHouse (clickhouse-local).

Claims a fake client cannot settle (spec 2026-09-11 sections 6 and 11):

1. Migration 000401's tables accept every shape the fold writes: an 80-value main tuple
   (Nullable(Decimal(38, 6)), Date32, Nullable(UInt16), Nullable(UInt64),
   Array(LowCardinality(String))), an 84-value history tuple, a company precedence rule and a
   hide rule -- and every main row passes the table's CHECKs.
2. A company folds end to end through the REAL batch SQL -- selection, the five page reads
   under FINAL, history then main -- and the row reads back as the fold built it: Ratsit's
   rounded revenue over Bolagsverket's exact one with Ratsit's own USD twin, Bolagsverket's
   employees, SEK from Ratsit, both sources; the consolidated ESEF period stands on its own.
3. Re-running the same fold selects NOTHING: the rewrite advanced max(folded_at) past every
   watermark.
4. A hide rule deactivates its period, a company-wide precedence rule flips revenue to
   Bolagsverket for the period where both compete, and a tombstone (every value NULL, newer
   than the last fold) withdraws its period with the last values kept.
5. A changed_only=False refold reproduces every row the driver wrote -- Decimal(38, 6) money,
   Date32 dates, the UInt16/UInt64 counts and the sources array round-trip EQUAL -- so nothing
   turns `updated` and no history row is written.

`_LocalClient` is a clickhouse-driver-shaped client over `clickhouse-local`: the process is
stateless, so the session keeps every statement it has run and replays the whole script for
each SELECT. Decimals come back quoted (output_format_json_quote_decimals) and parse exactly.
Both `join_use_nulls` settings run: none of these statements joins, so the parametrization
guards a future one, not a live risk.
"""

import json
import subprocess
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from dagster_v3.defs.se_company.financial import batch, tables
from dagster_v3.defs.se_company.financial.assets import export_precedence
from dagster_v3.defs.se_company.financial.fold import FOLD_VERSION
from tests.clickhouse_local import clickhouse_local_command

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATION_FILE = "000401_corpscout_se_company_financial_entity.up.sql"

A, B = "5567081699", "5560000002"     # A: Bolagsverket + Ratsit + ESEF; B: Ratsit only
P23, P22, C23 = "standalone:2023-12-31", "standalone:2022-12-31", "consolidated:2023-12-31"
SUGGESTED_AT = datetime(2026, 9, 12, 20, 0, tzinfo=UTC)
EXPORTED_AT = datetime(2026, 9, 12, 21, 0, tzinfo=UTC)
FIRST_FOLD_AT = datetime(2026, 9, 13, 8, 0, tzinfo=UTC)
SECOND_FOLD_AT = datetime(2026, 9, 13, 9, 0, tzinfo=UTC)
RULE_AT = datetime(2026, 9, 13, 9, 30, tzinfo=UTC)          # after FIRST_FOLD_AT, so round 3 selects A
TOMBSTONED_AT = datetime(2026, 9, 13, 9, 40, tzinfo=UTC)
THIRD_FOLD_AT = datetime(2026, 9, 13, 10, 0, tzinfo=UTC)
FOURTH_FOLD_AT = datetime(2026, 9, 13, 11, 0, tzinfo=UTC)
COMPANY_IDS = [A, B]


def _literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, datetime):
        return f"toDateTime64('{value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}', 3, 'UTC')"
    if isinstance(value, date):
        return f"toDate32('{value.isoformat()}')"
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, list):
        return "[" + ", ".join(_literal(item) for item in value) + "]"
    if isinstance(value, tuple):
        return "(" + ", ".join(_literal(item) for item in value) + ")"
    if isinstance(value, (int, float)):
        return repr(value)
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _schema_statements() -> list[str]:
    statements: list[str] = []
    text = (MIGRATIONS_DIR / MIGRATION_FILE).read_text(encoding="utf-8")
    for raw in text.split(";"):
        statement = "\n".join(line for line in raw.splitlines() if not line.strip().startswith("--")).strip()
        if statement.upper().startswith("CREATE DATABASE") or statement.startswith("CREATE TABLE"):
            statements.append(statement)
    return statements


_QUERY_COLUMNS: dict[str, tuple[str, ...]] = {
    batch.bucket_company_ids_sql(): ("company_id",),
    batch.suggestion_watermarks_sql(): ("company_id", "suggested_at", "live"),
    batch.main_watermarks_sql(): ("company_id", "folded_at"),
    batch.rule_watermarks_sql(): ("company_id", "decided_at"),
    batch.hide_watermarks_sql(): ("company_id", "decided_at"),
    batch.current_suggestions_sql(): batch.SUGGESTION_SELECT_COLUMNS,
    batch.current_main_rows_sql(): batch.MAIN_COMPARE_COLUMNS,
    batch.company_rules_sql(): batch.RULE_SELECT_COLUMNS,
    batch.hidden_periods_sql(): ("company_id", "period_key"),
}
_DATETIME_COLUMNS = frozenset({"suggested_at", "folded_at", "decided_at"})
_DATE_COLUMNS = frozenset({"period_end", "period_start"})
_DECIMAL_COLUMNS = frozenset(tables.MONETARY_SUGGESTION_COLUMNS)
_INT_COLUMNS = frozenset({"employees", "live", "precedence", "fiscal_year", "period_months", "active"})


def _value(column: str, value: Any) -> Any:
    if value is None:
        return None
    if column in _DATETIME_COLUMNS:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=UTC)
    if column in _DATE_COLUMNS:
        return date.fromisoformat(value)
    if column in _DECIMAL_COLUMNS:
        return Decimal(value)
    if column in _INT_COLUMNS:
        return int(value)
    return value


class _LocalClient:
    """A clickhouse-driver-shaped client over clickhouse-local. Writes are remembered and
    replayed before every read, because each invocation starts with an empty server."""

    def __init__(self, setting: int) -> None:
        self.statements: list[str] = [f"SET join_use_nulls = {setting}", *_schema_statements()]

    def add(self, statement: str) -> None:
        self.statements.append(statement)

    def _run(self, query: str) -> list[str]:
        script = ";\n".join([*self.statements, query]) + ";\n"
        try:
            completed = subprocess.run(clickhouse_local_command(), input=script, capture_output=True, text=True, timeout=900)
        except (OSError, subprocess.TimeoutExpired) as exc:  # pragma: no cover - env
            pytest.skip(f"clickhouse-local is unusable here: {exc}")
        assert completed.returncode == 0, completed.stderr or completed.stdout
        return [line for line in completed.stdout.splitlines() if line.strip()]

    def execute(self, sql, params=None, settings=None):
        if sql.startswith("INSERT"):
            values = ", ".join(_literal(tuple(row)) for row in params)
            self.add(f"{sql} {values}")
            return []
        columns = _QUERY_COLUMNS.get(sql)
        rendered = sql
        for name, value in (params or {}).items():
            rendered = rendered.replace(f"%({name})s", _literal(tuple(value) if isinstance(value, list) else value))
        assert "%(" not in rendered, rendered
        lines = self._run(
            f"SELECT * FROM ({rendered}) AS q FORMAT JSONCompactEachRow SETTINGS output_format_json_quote_decimals = 1"
        )
        if columns is None:   # a one-off read (export_precedence's stored rows): raw
            return [tuple(json.loads(line)) for line in lines]
        return [tuple(_value(column, item) for column, item in zip(columns, json.loads(line), strict=True)) for line in lines]

    def read(self, query: str) -> list[list[str]]:
        """A read the batch does not make: the test's own assertions, TSV."""
        return [line.split("\t") for line in self._run(query)]


def _suggestion(company_id: str, source: str, period_key: str, *, suggested_at: datetime = SUGGESTED_AT,
                currency: str | None = "SEK", employees: int | None = None, amount_scale: int = 1,
                money: dict[str, tuple[str, str]] | None = None) -> tuple:
    """One row in tables.SUGGESTION_COLUMNS order. `money` maps a field to (original, usd) as
    decimal strings; an omitted `money` with no employees is a tombstone."""
    scope, end = period_key.split(":")
    values: dict[str, Any] = {
        "company_id": company_id, "source": source, "period_key": period_key, "suggestion_id": "f" * 64,
        "suggested_at": suggested_at, "source_record_uid": f"{source}:{period_key}", "scope": scope,
        "period_end": date.fromisoformat(end), "period_end_derived": 0,
        "period_start": None if money is None and employees is None else date(int(end[:4]), 1, 1),
        "fiscal_year": None if money is None and employees is None else int(end[:4]),
        "period_months": None if money is None and employees is None else 12,
        "filing_fiscal_year": None, "currency": currency, "amount_scale": amount_scale,
        "employees": employees, "fx_rate_to_usd": None, "fx_rate_date": None, "fx_source": "",
        "decided_by": "", "note": "", "source_run_id": "r", "extractor_version": "v",
    }
    for column in tables.MONETARY_SUGGESTION_COLUMNS:
        values[column] = None
    for field, (original, usd) in (money or {}).items():
        values[tables.original_column(field)] = Decimal(original)
        values[tables.usd_column(field)] = Decimal(usd)
    return tuple(values[column] for column in tables.SUGGESTION_COLUMNS)


def _insert(table: str, columns: tuple[str, ...], rows: list[tuple]) -> str:
    return f"INSERT INTO {table} ({', '.join(columns)}) VALUES " + ", ".join(_literal(row) for row in rows)


SUGGESTIONS = [
    _suggestion(A, "bolagsverket", P23, employees=2100, money={"revenue": ("59016040", "5877138.085783"), "equity": ("3379581", "336000")}),
    _suggestion(A, "ratsit", P23, amount_scale=1000000, money={"revenue": ("60300000", "6005001.802437"), "equity": ("3400000", "338000")}),
    _suggestion(A, "ratsit", P22, amount_scale=1000000, money={"revenue": ("57100000", "5475989.498063")}),
    _suggestion(A, "esef", C23, money={"revenue": ("1296506000", "129113115.53"), "equity": ("2030344000", "202000000")}),
    _suggestion(B, "ratsit", P23, amount_scale=1000000, employees=5, money={"revenue": ("100000000", "10000000")}),
]
TOMBSTONE = _suggestion(A, "ratsit", P22, suggested_at=TOMBSTONED_AT, currency=None)


@pytest.fixture(scope="module", params=(0, 1), ids=("join_use_nulls_off", "join_use_nulls_on"))
def folded(request: pytest.FixtureRequest) -> dict[str, Any]:
    """Four rounds against one clickhouse-local session: the first fold, the re-run, a fold
    under a hide rule, a company-wide precedence rule and a tombstone, and a changed_only=False
    refold that proves the round trip through real ClickHouse reproduces every row exactly."""
    client = _LocalClient(request.param)
    client.add(_insert(tables.QUALIFIED_SUGGESTION_TABLE, tables.SUGGESTION_COLUMNS, SUGGESTIONS))
    export_precedence(client, EXPORTED_AT)              # the 82 global rows, through the real code

    first = batch.fold_companies(client, COMPANY_IDS, changed_only=True, source_run_id="run-1", folded_at=FIRST_FOLD_AT)
    # Captured HERE: round 3 changes A's 2023 row (hidden, revenue flipped), so a later read
    # against the live table would not show what round 1 published.
    first_rows = client.read(
        "SELECT period_key, currency, currency_source, toString(revenue_amount_original), toString(revenue_amount_usd), "
        "revenue_source, toString(equity_amount_original), equity_source, ifNull(toString(employees), 'NULL'), employees_source, "
        "arrayStringConcat(sources, ','), toString(active), inactive_reason, toString(period_start), toString(fiscal_year) "
        f"FROM {tables.QUALIFIED_MAIN_TABLE} FINAL WHERE company_id = '{A}' ORDER BY period_key"
    )
    rerun = batch.fold_companies(client, COMPANY_IDS, changed_only=True, source_run_id="run-2", folded_at=SECOND_FOLD_AT)

    client.add(_insert(tables.QUALIFIED_RULE_TABLE, tables.RULE_COLUMNS, [(A, P23, "hide", 0, "backoffice", "", RULE_AT)]))
    client.add(_insert(tables.QUALIFIED_PRECEDENCE_TABLE, tables.PRECEDENCE_COLUMNS, [(A, "", "revenue", "bolagsverket", 5000, 0, "backoffice", "", RULE_AT)]))
    client.add(_insert(tables.QUALIFIED_SUGGESTION_TABLE, tables.SUGGESTION_COLUMNS, [TOMBSTONE]))
    third = batch.fold_companies(client, COMPANY_IDS, changed_only=True, source_run_id="run-3", folded_at=THIRD_FOLD_AT)
    # Captured HERE too: round 4 rewrites every row with its own folded_at and run id.
    third_rows = client.read(
        "SELECT period_key, toString(active), inactive_reason, toString(revenue_amount_original), toString(revenue_amount_usd), revenue_source, "
        "toString(equity_amount_original), equity_source, fold_version, source_run_id, toString(folded_at) "
        f"FROM {tables.QUALIFIED_MAIN_TABLE} FINAL WHERE company_id = '{A}' ORDER BY period_key"
    )
    fourth = batch.fold_companies(client, COMPANY_IDS, changed_only=False, source_run_id="run-4", folded_at=FOURTH_FOLD_AT)
    return {"client": client, "first": first, "first_rows": first_rows, "rerun": rerun, "third": third, "third_rows": third_rows, "fourth": fourth}


def test_the_first_fold_publishes_every_period_ratsit_first_with_its_own_twin(folded) -> None:
    counts = folded["first"]
    assert (counts.companies, counts.considered, counts.pages) == (2, 2, 1)
    assert (counts.periods, counts.published, counts.created, counts.unchanged, counts.unpublished) == (4, 4, 4, 0, 0)
    rows = {row[0]: row for row in folded["first_rows"]}
    assert set(rows) == {C23, P22, P23}
    standalone = rows[P23]
    assert standalone[1:6] == ["SEK", "ratsit", "60300000", "6005001.802437", "ratsit"]   # 1000 over 900, twin travels
    assert standalone[6:8] == ["3400000", "ratsit"]
    assert standalone[8:10] == ["2100", "bolagsverket"]                                    # ungated, Ratsit has none
    assert standalone[10:15] == ["bolagsverket,ratsit", "1", "", "2023-01-01", "2023"]
    consolidated = rows[C23]
    assert consolidated[1:6] == ["SEK", "esef", "1296506000", "129113115.53", "esef"] and consolidated[10] == "esef"
    assert rows[P22][3:6] == ["57100000", "5475989.498063", "ratsit"]
    b_rows = folded["client"].read(
        f"SELECT toString(revenue_amount_original), toString(employees), fold_version FROM {tables.QUALIFIED_MAIN_TABLE} FINAL WHERE company_id = '{B}'"
    )
    assert b_rows == [["100000000", "5", FOLD_VERSION]]


def test_the_history_of_the_first_fold_is_one_created_row_per_period_with_its_valued_fields(folded) -> None:
    rows = folded["client"].read(
        f"SELECT period_key, change_kind, arrayStringConcat(changed_fields, ','), toString(changed_at) FROM {tables.QUALIFIED_HISTORY_TABLE} "
        f"WHERE fold_run_id = 'run-1' AND company_id = '{A}' ORDER BY period_key"
    )
    stamp = FIRST_FOLD_AT.strftime("%Y-%m-%d %H:%M:%S.000")
    assert rows == [
        [C23, "created", "period_start,fiscal_year,period_months,currency,revenue,equity", stamp],
        [P22, "created", "period_start,fiscal_year,period_months,currency,revenue", stamp],
        [P23, "created", "period_start,fiscal_year,period_months,currency,revenue,equity,employees", stamp],
    ]


def test_re_running_the_fold_selects_nothing(folded) -> None:
    counts = folded["rerun"]
    assert counts.considered == 0 and counts.periods == 0 and counts.created == 0
    assert folded["client"].read(f"SELECT count() FROM {tables.QUALIFIED_HISTORY_TABLE} WHERE fold_run_id = 'run-2'") == [["0"]]


def test_a_hide_a_company_rule_and_a_tombstone_all_move_their_periods(folded) -> None:
    counts = folded["third"]
    assert counts.considered == 1                                   # A only: B has nothing newer than its fold
    assert (counts.periods, counts.published, counts.hidden, counts.withdrawn, counts.unchanged) == (3, 1, 1, 1, 1)
    client = folded["client"]
    rows = {row[0]: row for row in folded["third_rows"]}
    # Hidden, and the company-wide rule (5000) flips revenue to Bolagsverket WITH Bolagsverket's USD; equity stays Ratsit's.
    assert rows[P23][1:8] == ["0", "hidden", "59016040", "5877138.085783", "bolagsverket", "3400000", "ratsit"]
    # Withdrawn keeps the last values; the writer's stamps are this run's.
    assert rows[P22][1:6] == ["0", "withdrawn", "57100000", "5475989.498063", "ratsit"] and rows[P22][8:10] == [FOLD_VERSION, "run-3"]
    assert rows[C23][1:3] == ["1", ""]                              # the ESEF-only period: unchanged, still rewritten
    kinds = client.read(
        f"SELECT period_key, change_kind, arrayStringConcat(changed_fields, ',') FROM {tables.QUALIFIED_HISTORY_TABLE} "
        "WHERE fold_run_id = 'run-3' ORDER BY period_key"
    )
    assert kinds == [[P22, "withdrawn", ""], [P23, "hidden", "revenue"]]
    assert {row[10] for row in rows.values()} == {THIRD_FOLD_AT.strftime("%Y-%m-%d %H:%M:%S.000")}   # every period of A carries run-3's folded_at


def test_a_stable_refold_reproduces_every_row_the_driver_wrote(folded) -> None:
    """A changed_only=False refold with nothing different since round 3 forces every row
    through the real read-compare-write cycle: Decimal(38, 6), Date32, Nullable(UInt16),
    Nullable(UInt64) and Array(LowCardinality(String)) must come back EQUAL, or a row turns
    `updated` and writes a spurious history entry."""
    counts = folded["fourth"]
    assert counts.considered == 2
    for kind in ("created", "updated", "hidden", "withdrawn", "reactivated"):
        assert getattr(counts, kind) == 0, kind
    client = folded["client"]
    rows = client.read(f"SELECT count() FROM {tables.QUALIFIED_MAIN_TABLE} FINAL")
    assert counts.unchanged == counts.periods == int(rows[0][0]) == 4
    assert client.read(f"SELECT count() FROM {tables.QUALIFIED_HISTORY_TABLE} WHERE fold_run_id = 'run-4'") == [["0"]]
    stamps = client.read(f"SELECT count(DISTINCT folded_at) FROM {tables.QUALIFIED_MAIN_TABLE} FINAL")
    assert stamps == [["1"]]                                        # the rewrite stamped FOURTH_FOLD_AT on every row


def test_the_precedence_export_wrote_the_global_rows(folded) -> None:
    rows = folded["client"].read(
        f"SELECT count(), uniqExact(field) FROM {tables.QUALIFIED_PRECEDENCE_TABLE} FINAL WHERE company_id = '' AND period_key = ''"
    )
    assert rows == [["82", "25"]]                                   # 3 period fields + currency + 20 money + employees
```

- [ ] **Step 2: Run it on the engine and ruff**

```bash
uv run --env-file .env pytest tests/test_se_company_financial_fold_clickhouse_local.py -q -m integration
uv run ruff check tests/test_se_company_financial_fold_clickhouse_local.py
```

Expected: 12 passed (six tests under each `join_use_nulls` setting), about 30 s. This exact scenario produced exactly these rows on 2026-09-13 with the module texts of Tasks 1 to 3; a difference is a transcription slip, not an expectation to adjust — report it.

- [ ] **Step 3: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity
git add corpscout/services/dagster_v3/tests/test_se_company_financial_fold_clickhouse_local.py
git commit -m "test(se-financial): the fold end to end on clickhouse-local, four rounds under both join_use_nulls settings"
```

---

### Task 5: The slice-2 parked nits

**Files:**
- Modify: `tests/test_se_company_financial_extractors_sql.py` (replace the whole file with the text below: one import and one test added)
- Modify: `tests/test_se_company_financial_extractors_clickhouse_local.py` (replace the whole file with the text below: the normalizer version is imported instead of hardcoded, and a v1 report under a different hash pins the report CTE's version filter behaviourally)

- [ ] **Step 1: Write both files**

`tests/test_se_company_financial_extractors_sql.py`, exactly:

```python
"""The four financial extractors' SQL (spec section 7): the contracts a fake client cannot
settle are in the clickhouse-local test; these pin the text each module renders."""

from dagster_v3.defs.se_company.financial import bolagsverket, esef, ratsit
from dagster_v3.defs.sweden_ratsit.normalization import RATSIT_NORMALIZER_VERSION
from dagster_v3.defs.se_company.financial.suggestions import FINANCIAL_SELECT_COLUMNS, FINANCIAL_TARGET
from dagster_v3.defs.se_company.basic_info.extract import insert_page_sql


def _projection(sql: str) -> list[str]:
    """The aliases of the outer SELECT (the last bare `SELECT` line up to the next unindented line)."""
    lines = sql.splitlines()
    start = max(i for i, line in enumerate(lines) if line == "SELECT") + 1
    aliases = []
    for line in lines[start:]:
        if not line.startswith("    "):
            break
        aliases.append(line.rsplit(" AS ", 1)[1].rstrip(","))
    return aliases


def test_bolagsverket_reported_picks_the_fuller_statement_per_period() -> None:
    live = bolagsverket.reported_live_sql()
    assert "PARTITION BY m.company_id, m.report_period_end ORDER BY " in live
    assert live.index("toUInt8(m.revenue_amount_original IS NOT NULL) + ") < live.index("DESC, m.statement_key ASC")
    assert "WHERE m.observation_kind = 'reported'" in live
    assert "WHERE m.rn = 1\n    AND (revenue_amount_original IS NOT NULL OR " in live
    assert live.endswith(" OR employees IS NOT NULL)")
    assert "FROM corpscout.se_bolagsverket_financial_metrics AS m FINAL" in live
    assert "ON universe.company_id = m.company_id" in live
    assert "    concat('standalone:', toString(m.report_period_end)) AS period_key" in live
    assert "    'standalone' AS scope" in live and "    m.statement_key AS source_record_uid" in live
    assert "    m.operating_profit_loss_amount_original AS operating_result_amount_original" in live
    assert "    m.profit_loss_amount_usd AS net_result_amount_usd" in live
    assert "    CAST(NULL AS Nullable(Decimal(38, 6))) AS ebitda_amount_original" in live
    assert "    nullIf(toString(m.currency), '') AS currency" in live and "    toUInt32(1) AS amount_scale" in live
    assert "    m.employees AS employees" in live and "    m.source_fiscal_year AS filing_fiscal_year" in live
    assert "%(company_ids)s" not in live and "AND m.company_id IN %(company_ids)s" in bolagsverket.reported_live_sql(scoped=True)


def test_bolagsverket_comparative_takes_the_newest_restating_filing_and_two_figures() -> None:
    live = bolagsverket.comparative_live_sql()
    assert "WHERE m.observation_kind = 'comparative'" in live
    assert "ORDER BY ifNull(m.source_fiscal_year, 0) DESC, m.statement_key ASC" in live
    assert "    m.revenue_amount_original AS revenue_amount_original" in live
    assert "    m.total_assets_amount_usd AS total_assets_amount_usd" in live
    assert "    CAST(NULL AS Nullable(Decimal(38, 6))) AS equity_amount_original" in live
    assert "    CAST(NULL AS Nullable(UInt64)) AS employees" in live
    assert "    'bolagsverket_comparative' AS source" in live


def test_esef_composes_versions_newest_first_and_maps_scope_and_types() -> None:
    live = esef.esef_live_sql()
    assert "toUInt32OrZero(extract(m.fxo_id, '-([0-9]+)$')) AS version" in live
    assert "INNER JOIN corpscout.se_esef_filings AS f\n        ON f.lei = m.lei AND f.period_end = m.period_end AND f.fxo_id = m.fxo_id" in live
    assert "WHERE m.scope = 'consolidated_ifrs'" in live and "GROUP BY m.company_id, m.period_end" in live
    assert "argMaxIf(m.revenue_amount_original, m.version, m.revenue_amount_original IS NOT NULL) AS revenue_amount_original" in live
    assert "argMaxIf(m.currency, m.version, m.currency != '') AS currency" in live
    assert "    concat('consolidated:', toString(e.period_end)) AS period_key" in live
    assert "    CAST(e.cash_and_bank_amount_original AS Nullable(Decimal(38, 6))) AS cash_and_bank_amount_original" in live
    assert "    CAST(if(e.employees < 0, NULL, e.employees) AS Nullable(UInt64)) AS employees" in live
    assert "    CAST(e.fx_rate_to_usd AS Nullable(Decimal(38, 12))) AS fx_rate_to_usd" in live
    assert "    nullIf(toString(e.currency), '') AS currency" in live
    assert "AND f.company_id IN %(company_ids)s" in esef.esef_live_sql(scoped=True)


def test_ratsit_scales_by_unit_derives_the_end_date_and_ranks_duplicates() -> None:
    live = ratsit.ratsit_live_sql()
    assert "argMax(r.result_sha256, (r.normalized_at, r.result_sha256)) AS result_sha256" in live
    assert "AND r.normalizer_version = %(normalizer_version)s" in live
    assert "AND report.normalizer_version = p.normalizer_version" in live
    assert "multiIf(p.scope = 'company', 'standalone', p.scope = 'consolidated', 'consolidated', '') AS entity_scope" in live
    assert "ifNull(p.period_end, makeDate32(p.fiscal_year, 12, 31)) AS effective_end" in live
    assert "PARTITION BY p.company_id, entity_scope, effective_end\n            ORDER BY ifNull(p.period_months, 0) DESC, p.financial_report_index DESC, p.period_index DESC" in live
    assert "WHERE p.monetary_unit IS NOT NULL\n        AND p.scope IN ('company', 'consolidated')\n        AND (p.period_end IS NOT NULL OR p.fiscal_year BETWEEN 1900 AND 2299)" in live
    assert "    p.revenue_amount * multiIf(p.monetary_unit = 'MSEK', 1000000, p.monetary_unit = 'TSEK', 1000, 1) AS revenue_amount_original" in live
    assert "    p.revenue_amount_usd AS revenue_amount_usd" in live
    assert "    toUInt8(p.period_end IS NULL) AS period_end_derived" in live
    assert "    'SEK' AS currency" in live and "    CAST(p.employee_count AS Nullable(UInt64)) AS employees" in live
    assert "    CAST(NULL AS Nullable(Decimal(38, 6))) AS cash_and_bank_amount_original" in live
    scoped = ratsit.ratsit_live_sql(scoped=True)
    assert scoped.count("%(company_ids)s") == 2  # the report CTE and the periods scan


def test_every_extractor_projects_the_select_columns_in_order_and_inserts_the_target() -> None:
    for live in (bolagsverket.reported_live_sql(), bolagsverket.comparative_live_sql(), esef.esef_live_sql(), ratsit.ratsit_live_sql()):
        assert _projection(live) == list(FINANCIAL_SELECT_COLUMNS)
        assert live.count("\n    AND (revenue_amount_original IS NOT NULL OR ") == 1
    insert = insert_page_sql(select_sql=bolagsverket.reported_select_sql(), target=FINANCIAL_TARGET)
    assert insert.startswith(f"INSERT INTO corpscout.se_company_financial_suggestion ({', '.join(FINANCIAL_TARGET.insert_columns)})\nWITH (SELECT now64(3, 'UTC')) AS stamp\n")
    assert insert.count("live_period_keys") == 3


def test_the_four_assets_carry_their_sources_and_deps() -> None:
    import dagster as dg
    assets = {
        bolagsverket.se_company_financial_suggestions_bolagsverket: ("bolagsverket", "se_bolagsverket_financial_metrics_clickhouse"),
        bolagsverket.se_company_financial_suggestions_bolagsverket_comparative: ("bolagsverket_comparative", "se_bolagsverket_financial_metrics_clickhouse"),
        esef.se_company_financial_suggestions_esef: ("esef", "esef_financial_metrics_clickhouse"),
        ratsit.se_company_financial_suggestions_ratsit: ("ratsit", "se_ratsit_financial_periods_usd"),
    }
    for asset, (source, dep) in assets.items():
        assert asset.key == dg.AssetKey(f"se_company_financial_suggestions_{source}")
        assert dg.AssetKey(dep) in asset.dependency_keys and dg.AssetKey("se_company_basic_info_fold") in asset.dependency_keys
        spec = next(iter(asset.specs))
        assert spec.metadata["source"] == source and spec.group_name == "se_company_financial"


def test_the_ratsit_asset_binds_the_running_normalizer_version() -> None:
    """The report CTE and the periods join both read %(normalizer_version)s; the asset must
    bind it, or the first prod page fails on an unbound placeholder (the address entity pins
    its params the same way)."""
    assert ratsit.RATSIT_SELECT_PARAMS == {"normalizer_version": RATSIT_NORMALIZER_VERSION}
    assert RATSIT_NORMALIZER_VERSION == "ratsit-normalizer-v2"
    assert ratsit.ratsit_live_sql().count("%(normalizer_version)s") == 2   # selected as the report's version, and the CTE filter
    assert "AND report.normalizer_version = p.normalizer_version" in ratsit.ratsit_live_sql()
```

`tests/test_se_company_financial_extractors_clickhouse_local.py`, exactly:

```python
"""The financial extractors' SQL on a real ClickHouse (spec 2026-09-11 section 7). Claims a
fake client cannot settle:
1. Each page select produces the fifty-nine supplied columns in a UNION ALL whose live and
   tombstone branches agree on every type, and every inserted row passes the table's CHECKs.
2. suggestion_id equals sha256(company_id, source, period_key, suggested_at) on every row.
3. The state-hash scope selects a company before anything is suggested, nothing after the
   page is written (it converges), selects it again when the rebuilt source drops a period,
   and converges once the tombstone is written -- under join_use_nulls 0 and 1.
4. Bolagsverket: the fuller of two statements wins a period; the comparative source takes the
   newest restating filing and carries revenue and total assets only.
5. ESEF: the newest version wins field by field with the older version filling its gaps; a
   blank currency becomes NULL; a negative employee count becomes NULL; the EUR filer keeps EUR.
6. Ratsit: only the latest report of the running normalizer version (a superseded
   normalizer generation's report and periods, sharing the same result_sha256, never leak
   in); MSEK scaled to full units with amount_scale 1000000; the USD twin copied; an undated
   period keyed on Dec 31 of its fiscal year and flagged; the longer of two periods with one
   end wins; an employment-only period publishes employees alone; a row without a unit, and
   a row with neither a date nor a fiscal year in range, are skipped; a consolidated report
   maps to the consolidated scope.
7. A company outside se_company_basic_info never reaches the suggestion table.
8. A source row with no figure and no employee count is skipped, not written, and the scan
   still converges (C1): a zero-metric Bolagsverket statement, an all-NULL ESEF filing whose
   only non-NULL source field is a negative employee count, and an all-NULL Ratsit period.
"""

import subprocess
from pathlib import Path

import pytest

from dagster_v3.defs.esef_filings import tables as esef_tables
from dagster_v3.defs.esef_filings.country_views import build_se_esef_view_sql
from dagster_v3.defs.se_company.basic_info.extract import insert_page_sql
from dagster_v3.defs.se_company.financial import bolagsverket, esef, ratsit
from dagster_v3.defs.se_company.financial.suggestions import FINANCIAL_TARGET
from dagster_v3.defs.sweden_ratsit.normalization import RATSIT_NORMALIZER_VERSION
from tests.clickhouse_local import clickhouse_local_command, render

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
FIXTURE = Path(__file__).resolve().parent / "fixtures" / "se_company_financial_source_tables.sql"
TABLE = FINANCIAL_TARGET.qualified_table
A, B, OUT = "5567081699", "5560000002", "5569999999"   # A: every source; B: ESEF (EUR) + Ratsit consolidated; OUT: not in the universe
LEI_A, LEI_B = "AAAAAAAAAAAAAAAAAAAA", "BBBBBBBBBBBBBBBBBBBB"


def _statements(text: str) -> list[str]:
    out = []
    for raw in text.split(";"):
        statement = "\n".join(line for line in raw.splitlines() if not line.strip().startswith("--")).strip()
        if statement:
            out.append(statement)
    return out


def _schema() -> list[str]:
    schema = ["CREATE DATABASE IF NOT EXISTS corpscout"]
    schema += [s for s in _statements((MIGRATIONS_DIR / "000401_corpscout_se_company_financial_entity.up.sql").read_text(encoding="utf-8")) if s.startswith("CREATE TABLE")]
    schema += [s for s in _statements((MIGRATIONS_DIR / "000377_corpscout_se_company_basic_info.up.sql").read_text(encoding="utf-8")) if s.startswith("CREATE TABLE IF NOT EXISTS corpscout.se_company_basic_info\n")]
    schema += _statements(FIXTURE.read_text(encoding="utf-8"))
    schema.append(build_se_esef_view_sql(esef_tables.SE_ESEF_VIEWS[0]).rstrip(";"))
    return schema


ROWS = [
    f"INSERT INTO corpscout.se_company_basic_info (company_id, legal_name, legal_name_source, status, status_source, folded_at, fold_version, source_run_id) VALUES ('{A}', 'A AB', 'scb', 'active', 'scb', now64(3), 'v1', 'r'), ('{B}', 'B AB', 'scb', 'active', 'scb', now64(3), 'v1', 'r')",
    # Bolagsverket: A reported 2023; 2022 twice (s22a fuller than s22b); 2022 restated by the 2023 and 2024 filings (2024 newest); OUT is not in the universe.
    # s21 (C1): a reported 2021 statement with every metric and employees NULL -- must be skipped, not written as a pseudo-tombstone.
    f"""INSERT INTO corpscout.se_bolagsverket_financial_metrics (country_iso2, source_slug, source_run_id, source_record_id, statement_key, source_record_uid, company_id, report_period_start, report_period_end, fiscal_year, observation_kind, source_fiscal_year, currency, revenue_amount_original, revenue_amount_usd, operating_profit_loss_amount_original, operating_profit_loss_amount_usd, profit_loss_amount_original, profit_loss_amount_usd, total_assets_amount_original, total_assets_amount_usd, equity_amount_original, equity_amount_usd, employees, fx_rate_to_usd, fx_rate_date, fx_source, mapping_version, resolved_at) VALUES
    ('SE','sweden_financial','r','s23:1','s23','u23','{A}','2023-01-01','2023-12-31',2023,'reported',2023,'SEK',59016040,5876000,946563,94000,946563,94000,54302472,5400000,3379581,336000,2100,0.0996,'2023-12-29','ecb','m1',now64(3)),
    ('SE','sweden_financial','r','s22a:1','s22a','u22a','{A}','2022-01-01','2022-12-31',2022,'reported',2022,'SEK',54910071,5500000,NULL,NULL,64959,6500,39228252,3900000,2433018,243000,1800,0.1,'2022-12-30','ecb','m1',now64(3)),
    ('SE','sweden_financial','r','s22b:1','s22b','u22b','{A}','2022-01-01','2022-12-31',2022,'reported',2022,'SEK',54910071,5500000,NULL,NULL,NULL,NULL,39228252,3900000,NULL,NULL,NULL,0.1,'2022-12-30','ecb','m1',now64(3)),
    ('SE','sweden_financial','r','s23:2','s23','u23','{A}','2022-01-01','2022-12-31',2022,'comparative',2023,'SEK',54910071,5500000,NULL,NULL,NULL,NULL,39228252,3900000,NULL,NULL,NULL,0.1,'2022-12-30','ecb','m1',now64(3)),
    ('SE','sweden_financial','r','s24:2','s24','u24','{A}','2022-01-01','2022-12-31',2022,'comparative',2024,'SEK',54900000,5499000,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,0.1,'2022-12-30','ecb','m1',now64(3)),
    ('SE','sweden_financial','r','sX:1','sX','uX','{OUT}','2023-01-01','2023-12-31',2023,'reported',2023,'SEK',1,1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,0.1,'2023-12-29','ecb','m1',now64(3)),
    ('SE','sweden_financial','r','s21:1','s21','u21','{A}','2021-01-01','2021-12-31',2021,'reported',2021,'SEK',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,0.1,'2021-12-30','ecb','m1',now64(3))""",
    # ESEF: A has two versions of 2023 (v0 revenue + equity in SEK; v1 revenue + employees, blank currency); B files in EUR with a negative employee count.
    # B 2022 (C1): every money column NULL and employees -1 -- entirely NULL only after the
    # negative-employees rule, pinning that the WHERE reads the projection alias (employees),
    # not the raw source column (e.employees = -1, not NULL).
    f"INSERT INTO corpscout.esef_entity_registry_map (lei, country_iso2, registry_id_raw, registry_id, match_source, link_status, source_run_id, resolved_at) VALUES ('{LEI_A}','SE','{A}','{A}','register','register_verified','r',now64(3)), ('{LEI_B}','SE','{B}','{B}','register','register_verified','r',now64(3))",
    f"INSERT INTO corpscout.esef_filings (lei, entity_name, fxo_id, country, period_end, date_added, processed_at, json_url, package_url, report_url, viewer_url, package_sha256, error_count, warning_count, inconsistency_count, has_json_facts, source_url, source_run_id, resolved_at) VALUES ('{LEI_A}','A AB','{LEI_A}-2023-12-31-ESEF-SE-0','SE','2023-12-31','2024-04-01',now64(3),'','','','','',0,0,0,1,'','r',now64(3)), ('{LEI_A}','A AB','{LEI_A}-2023-12-31-ESEF-SE-1','SE','2023-12-31','2024-05-01',now64(3),'','','','','',0,0,0,1,'','r',now64(3)), ('{LEI_B}','B AB','{LEI_B}-2023-12-31-ESEF-SE-0','SE','2023-12-31','2024-04-01',now64(3),'','','','','',0,0,0,1,'','r',now64(3)), ('{LEI_B}','B AB','{LEI_B}-2022-12-31-ESEF-SE-0','SE','2022-12-31','2023-04-01',now64(3),'','','','','',0,0,0,1,'','r',now64(3))",
    f"""INSERT INTO corpscout.esef_financial_metrics (lei, entity_name, fxo_id, country, scope, fiscal_year, period_start, period_end, currency, revenue_amount_original, revenue_amount_usd, equity_amount_original, equity_amount_usd, employees, mapped_fact_count, source_fact_count, mapping_version, fx_rate_to_usd, fx_rate_date, fx_source, viewer_url, source_run_id, resolved_at) VALUES
    ('{LEI_A}','A AB','{LEI_A}-2023-12-31-ESEF-SE-0','SE','consolidated_ifrs',2023,'2023-01-01','2023-12-31','SEK',1296506000,129000000,2030344000,202000000,NULL,5,9,'m',0.0996,'2023-12-29','ecb','','r',now64(3)),
    ('{LEI_A}','A AB','{LEI_A}-2023-12-31-ESEF-SE-1','SE','consolidated_ifrs',2023,'2023-01-01','2023-12-31','',1300000000,129400000,NULL,NULL,4200,5,9,'m',0.0996,'2023-12-29','ecb','','r',now64(3)),
    ('{LEI_B}','B AB','{LEI_B}-2023-12-31-ESEF-SE-0','SE','consolidated_ifrs',2023,'2023-01-01','2023-12-31','EUR',5000000,5400000,NULL,NULL,-1,5,9,'m',1.08,'2023-12-29','ecb','','r',now64(3)),
    ('{LEI_B}','B AB','{LEI_B}-2022-12-31-ESEF-SE-0','SE','consolidated_ifrs',2022,'2022-01-01','2022-12-31','SEK',NULL,NULL,NULL,NULL,-1,5,9,'m',1.08,'2022-12-29','ecb','','r',now64(3))""",
    # Ratsit: A's latest report is 'a'*64 (the older 'e'*64 must not leak); B's consolidated report.
    # C2: a v1 report and a v1 period duplicate A's 2023 period under the SAME result_sha256
    # ('a'*64) but the superseded normalizer_version -- the pinned join must exclude them, so
    # the expected 2023 row stays the v2 values (60300000 / 6005001.802437), never the v1
    # revenue (99 / 9900000).
    # C1: period_index 7 (fiscal_year 2016) has every amount and employee_count NULL -- must
    # be skipped, not written as a pseudo-tombstone.
    # the 'c' v1 report is newer than every v2 report under a different hash; without the CTE's
    # version filter argMax picks it, the version-pinned join finds no v2 period under it, and A
    # vanishes from the Ratsit live rows, which the scope assertion catches -- so this row pins
    # the CTE filter behaviourally, where the 'a'*64 v1 duplicate pins only the join.
    f"INSERT INTO corpscout.se_ratsit_financial_reports (company_id, result_sha256, normalizer_version, financial_report_index, scope, monetary_unit, period_count, normalized_at) VALUES ('{A}', repeat('e',64), 'ratsit-normalizer-v2', 0, 'company', 'MSEK', 1, '2026-01-01 00:00:00'), ('{A}', repeat('a',64), 'ratsit-normalizer-v2', 0, 'company', 'MSEK', 7, '2026-09-01 00:00:00'), ('{A}', repeat('a',64), 'ratsit-normalizer-v1', 0, 'company', 'MSEK', 1, '2026-09-02 00:00:00'), ('{A}', repeat('c',64), 'ratsit-normalizer-v1', 0, 'company', 'MSEK', 1, '2026-09-03 00:00:00'), ('{B}', repeat('b',64), 'ratsit-normalizer-v2', 0, 'consolidated', 'MSEK', 1, '2026-09-01 00:00:00')",
    f"""INSERT INTO corpscout.se_ratsit_financial_periods (company_id, result_sha256, normalizer_version, financial_report_index, period_index, period_kind, scope, monetary_unit, fiscal_year, period_start, period_end, period_months, revenue_amount, revenue_amount_usd, equity_amount, equity_amount_usd, employee_count, fx_rate_to_usd, fx_rate_date, fx_source, normalized_at) VALUES
    ('{A}', repeat('e',64), 'ratsit-normalizer-v2', 0, 0, 'financial_only', 'company', 'MSEK', 2017, '2017-01-01', '2017-12-31', 12, 1, 100000, NULL, NULL, NULL, 0.1, '2017-12-29', 'ecb', '2026-01-01 00:00:00'),
    ('{A}', repeat('a',64), 'ratsit-normalizer-v2', 0, 0, 'financial_and_employment', 'company', 'MSEK', 2023, '2023-01-01', '2023-12-31', 12, 60.3, 6005001.802437, 3.4, 338000, 21, 0.099585436193, '2023-12-29', 'ECB EXR', '2026-09-01 00:00:00'),
    ('{A}', repeat('a',64), 'ratsit-normalizer-v1', 0, 0, 'financial_and_employment', 'company', 'MSEK', 2023, '2023-01-01', '2023-12-31', 12, 99, 9900000, 3.4, 338000, 21, 0.099585436193, '2023-12-29', 'ECB EXR', '2026-09-02 00:00:00'),
    ('{A}', repeat('c',64), 'ratsit-normalizer-v1', 0, 0, 'financial_and_employment', 'company', 'MSEK', 2023, '2023-01-01', '2023-12-31', 12, 77, 7700000, 3.4, 338000, 21, 0.099585436193, '2023-12-29', 'ECB EXR', '2026-09-03 00:00:00'),
    ('{A}', repeat('a',64), 'ratsit-normalizer-v2', 0, 1, 'financial_only', 'company', 'MSEK', 2021, NULL, NULL, NULL, 50, 5000000, NULL, NULL, NULL, 0.1, '2021-12-30', 'ECB EXR', '2026-09-01 00:00:00'),
    ('{A}', repeat('a',64), 'ratsit-normalizer-v2', 0, 2, 'financial_only', 'company', 'MSEK', 2020, '2020-07-01', '2020-12-31', 6, 20, 2000000, NULL, NULL, NULL, 0.1, '2020-12-30', 'ECB EXR', '2026-09-01 00:00:00'),
    ('{A}', repeat('a',64), 'ratsit-normalizer-v2', 0, 3, 'financial_only', 'company', 'MSEK', 2020, '2020-01-01', '2020-12-31', 12, 40, 4000000, NULL, NULL, NULL, 0.1, '2020-12-30', 'ECB EXR', '2026-09-01 00:00:00'),
    ('{A}', repeat('a',64), 'ratsit-normalizer-v2', 0, 4, 'employment_only', 'company', 'MSEK', 2019, '2019-01-01', '2019-12-31', 12, NULL, NULL, NULL, NULL, 15, NULL, NULL, '', '2026-09-01 00:00:00'),
    ('{A}', repeat('a',64), 'ratsit-normalizer-v2', 0, 5, 'financial_only', 'company', NULL, 2018, '2018-01-01', '2018-12-31', 12, 9, NULL, NULL, NULL, NULL, NULL, NULL, '', '2026-09-01 00:00:00'),
    ('{A}', repeat('a',64), 'ratsit-normalizer-v2', 0, 6, 'financial_only', 'company', 'MSEK', 1850, NULL, NULL, NULL, 1, NULL, NULL, NULL, NULL, NULL, NULL, '', '2026-09-01 00:00:00'),
    ('{A}', repeat('a',64), 'ratsit-normalizer-v2', 0, 7, 'financial_only', 'company', 'MSEK', 2016, '2016-01-01', '2016-12-31', 12, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '', '2026-09-01 00:00:00'),
    ('{B}', repeat('b',64), 'ratsit-normalizer-v2', 0, 0, 'financial_only', 'consolidated', 'MSEK', 2023, '2023-01-01', '2023-12-31', 12, 100, 10000000, NULL, NULL, NULL, 0.1, '2023-12-29', 'ECB EXR', '2026-09-01 00:00:00')""",
]

READ_SQL = (
    "SELECT source, company_id, period_key, toString(period_end_derived), ifNull(currency, 'NULL'), toString(amount_scale), "
    "ifNull(toString(revenue_amount_original), 'NULL'), ifNull(toString(revenue_amount_usd), 'NULL'), "
    "ifNull(toString(equity_amount_original), 'NULL'), ifNull(toString(employees), 'NULL'), "
    "ifNull(toString(filing_fiscal_year), 'NULL'), source_record_uid, ifNull(toString(period_months), 'NULL'), ifNull(toString(fx_rate_date), 'NULL') "
    f"FROM {TABLE} FINAL ORDER BY source, company_id, period_key FORMAT TSV"
)
IDENTITY_SQL = (
    f"SELECT count() FROM {TABLE} FINAL WHERE suggestion_id != lower(hex(SHA256(concat(company_id, '\\n', "
    "toString(source), '\\n', period_key, '\\n', toString(suggested_at)))))"
)
TOMBSTONE_SQL = (
    "SELECT source, company_id, period_key, scope, toString(period_end), toString(amount_scale), "
    "toString(revenue_amount_original IS NULL), toString(employees IS NULL), ifNull(currency, 'NULL') "
    f"FROM {TABLE} FINAL WHERE source = 'bolagsverket' AND period_key = 'standalone:2023-12-31' FORMAT TSV"
)
SOURCES = (
    ("bolagsverket", bolagsverket.reported_changed_scope_sql(), bolagsverket.reported_select_sql(), bolagsverket.BOLAGSVERKET_EXTRACTOR_VERSION),
    ("bolagsverket_comparative", bolagsverket.comparative_changed_scope_sql(), bolagsverket.comparative_select_sql(), bolagsverket.BOLAGSVERKET_COMPARATIVE_EXTRACTOR_VERSION),
    ("esef", esef.esef_changed_scope_sql(), esef.esef_select_sql(), esef.ESEF_EXTRACTOR_VERSION),
    ("ratsit", ratsit.ratsit_changed_scope_sql(), ratsit.ratsit_select_sql(), ratsit.RATSIT_EXTRACTOR_VERSION),
)



def _scope(scope_sql: str, source: str) -> str:
    # normalizer_version is unused text for the other three sources; render() only
    # substitutes placeholders that are actually present, so binding it unconditionally here
    # is harmless for them and required for ratsit's report CTE and periods join.
    return render(scope_sql, {"source": source, "normalizer_version": RATSIT_NORMALIZER_VERSION}) + "\nORDER BY company_id"


def _insert(select_sql: str, ids: list[str], version: str) -> str:
    return render(
        insert_page_sql(select_sql=select_sql, target=FINANCIAL_TARGET),
        {"company_ids": ids, "source_run_id": "run-1", "extractor_version": version, "normalizer_version": RATSIT_NORMALIZER_VERSION},
    )


def _run(join_use_nulls: int) -> dict[str, list[str]]:
    script = [f"SET join_use_nulls = {join_use_nulls}"] + _schema() + ROWS
    for name, scope_sql, select_sql, version in SOURCES:
        script += [f"SELECT '## scope-before {name}'", _scope(scope_sql, name), _insert(select_sql, [A, B, OUT], version), f"SELECT '## scope-after {name}'", _scope(scope_sql, name)]
    script += ["SELECT '## rows'", READ_SQL, "SELECT '## identity'", IDENTITY_SQL]
    bolagsverket_scope, bolagsverket_select, bolagsverket_version = SOURCES[0][1], SOURCES[0][2], SOURCES[0][3]
    script += [
        "ALTER TABLE corpscout.se_bolagsverket_financial_metrics DELETE WHERE statement_key = 's23' AND observation_kind = 'reported' SETTINGS mutations_sync = 2",
        "SELECT '## scope-after-delete'", _scope(bolagsverket_scope, "bolagsverket"),
        _insert(bolagsverket_select, [A], bolagsverket_version),
        "SELECT '## scope-after-tombstone'", _scope(bolagsverket_scope, "bolagsverket"),
        "SELECT '## tombstone'", TOMBSTONE_SQL,
    ]
    completed = subprocess.run(clickhouse_local_command(), input=";\n".join(script) + ";\n", capture_output=True, text=True, timeout=900)
    assert completed.returncode == 0, completed.stderr or completed.stdout
    sections: dict[str, list[str]] = {}
    current = ""
    for line in completed.stdout.splitlines():
        if line.startswith("## "):
            current = line[3:]
            sections[current] = []
        elif line.strip():
            sections[current].append(line)
    return sections


@pytest.mark.parametrize("join_use_nulls", [0, 1])
def test_the_four_extractors_write_the_expected_periods_and_converge(join_use_nulls: int) -> None:
    s = _run(join_use_nulls)
    assert s["scope-before bolagsverket"] == [A] and s["scope-after bolagsverket"] == []
    assert s["scope-before bolagsverket_comparative"] == [A] and s["scope-after bolagsverket_comparative"] == []
    assert s["scope-before esef"] == [B, A] and s["scope-after esef"] == []
    assert s["scope-before ratsit"] == [B, A] and s["scope-after ratsit"] == []
    rows = [line.split("\t") for line in s["rows"]]
    assert rows == [
        ["bolagsverket", A, "standalone:2022-12-31", "0", "SEK", "1", "54910071", "5500000", "2433018", "1800", "2022", "s22a", "12", "2022-12-30"],
        ["bolagsverket", A, "standalone:2023-12-31", "0", "SEK", "1", "59016040", "5876000", "3379581", "2100", "2023", "s23", "12", "2023-12-29"],
        ["bolagsverket_comparative", A, "standalone:2022-12-31", "0", "SEK", "1", "54900000", "5499000", "NULL", "NULL", "2024", "s24", "12", "2022-12-30"],
        ["esef", B, "consolidated:2023-12-31", "0", "EUR", "1", "5000000", "5400000", "NULL", "NULL", "NULL", f"{LEI_B}-2023-12-31-ESEF-SE-0", "12", "2023-12-29"],
        ["esef", A, "consolidated:2023-12-31", "0", "SEK", "1", "1300000000", "129400000", "2030344000", "4200", "NULL", f"{LEI_A}-2023-12-31-ESEF-SE-1", "12", "2023-12-29"],
        ["ratsit", B, "consolidated:2023-12-31", "0", "SEK", "1000000", "100000000", "10000000", "NULL", "NULL", "NULL", f"ratsit:{B}:0:0", "12", "2023-12-29"],
        ["ratsit", A, "standalone:2019-12-31", "0", "SEK", "1000000", "NULL", "NULL", "NULL", "15", "NULL", f"ratsit:{A}:0:4", "12", "NULL"],
        ["ratsit", A, "standalone:2020-12-31", "0", "SEK", "1000000", "40000000", "4000000", "NULL", "NULL", "NULL", f"ratsit:{A}:0:3", "12", "2020-12-30"],
        ["ratsit", A, "standalone:2021-12-31", "1", "SEK", "1000000", "50000000", "5000000", "NULL", "NULL", "NULL", f"ratsit:{A}:0:1", "NULL", "2021-12-30"],
        ["ratsit", A, "standalone:2023-12-31", "0", "SEK", "1000000", "60300000", "6005001.802437", "3400000", "21", "NULL", f"ratsit:{A}:0:0", "12", "2023-12-29"],
    ]
    assert s["identity"] == ["0"]
    assert s["scope-after-delete"] == [A] and s["scope-after-tombstone"] == []
    assert [line.split("\t") for line in s["tombstone"]] == [["bolagsverket", A, "standalone:2023-12-31", "standalone", "2023-12-31", "1", "1", "1", "NULL"]]
```

- [ ] **Step 2: Run them, then prove the new fixture row is load-bearing**

```bash
uv run --env-file .env pytest tests/test_se_company_financial_extractors_sql.py -q
uv run --env-file .env pytest tests/test_se_company_financial_extractors_clickhouse_local.py -q -m integration
```

Expected: 7 passed; 2 passed. Then TEMPORARILY delete the text `AND r.normalizer_version = %(normalizer_version)s` from the report CTE in `src/dagster_v3/defs/se_company/financial/ratsit.py` (it occurs once), re-run the clickhouse-local test — expected: 2 FAILED (company A drops out of the Ratsit live rows, so the scope assertion fails) — then restore the file (`git checkout -- src/dagster_v3/defs/se_company/financial/ratsit.py`) and re-run green. Paste both outcomes in the report. `uv run ruff check` on both test files must be clean.

- [ ] **Step 3: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity
git add corpscout/services/dagster_v3/tests/test_se_company_financial_extractors_sql.py corpscout/services/dagster_v3/tests/test_se_company_financial_extractors_clickhouse_local.py
git commit -m "test(se-financial): pin the Ratsit asset's select_params and the report CTE's version filter"
```

---

### Task 6: Docs and whole-slice check

**Files:**
- Modify: `src/dagster_v3/defs/se_company/financial/docs/financial-design.md`
- Modify: `docs/superpowers/specs/2026-09-11-se-company-financial-entity-design.md` (section 12 item 3)

- [ ] **Step 1: Append the fold section to the design note**

Append at the end of `financial-design.md`:

```markdown
## Fold (slice 3)

`fold.py` is pure and decides ONE period: `fold_financial` picks the currency first (highest
effective precedence among the rows naming one; ties to the smaller source name, then uid),
lets each of the twenty figures compete only among rows in that currency (the winner brings
its own USD twin), lets employees and the period attributes compete ungated, and returns None
when no field has a winner. `resolve_rules` overlays a period's rules on the company-wide ones
on the global map. `fold_company_periods` walks a company's periods against its current main
rows: `created`, `updated`, `hidden`, `withdrawn` (no live row left; the last values stay with
`active 0`) and `reactivated`; an unchanged period is still returned so the batch can advance
its `folded_at`.

`batch.py` folds pages of 5,000 companies: five FINAL reads (live suggestions, main rows,
company rules, hide rules; the watermarks aside), the pure fold, then history BEFORE main.
`changed_only` selects a company when its newest suggestion, precedence decision or hide
decision (released versions included) is newer than its newest `folded_at`, or it was never
folded and has a live row. The global precedence export is NOT a watermark: after changing
the dictionary, export it and re-fold every bucket with `changed_only: false`.

Assets: `se_company_financial_fold` (64 hash buckets, one partition per run, pool
`se_company_financial_fold`, downstream of the four extractors) and
`se_company_financial_fold_companies` (`company_ids`, the backoffice's Fold now target).
Runbook for a full backfill while the run queue is held: run the 64 partitions in-process on
the dagster host, one after the other (the plan's Task 7 script), then re-run three buckets
with `changed_only: true` and expect `considered 0`.
```

- [ ] **Step 2: Record code completion in the spec**

Section 12 item 3 (`3. Fold: ...`): append `Code complete 2026-09-13 on branch se-financial-entity (plan 2026-09-13-se-company-financial-3-fold.md); prod backfill pending.` and re-wrap the item at the surrounding width (three-space continuation indent).

- [ ] **Step 3: Run the slice's suites and the wider suite once**

```bash
uv run --env-file .env pytest tests/test_se_company_financial_fold.py tests/test_se_company_financial_batch.py tests/test_se_company_financial_assets.py tests/test_se_company_financial_jobs.py tests/test_se_company_financial_suggestions.py tests/test_se_company_financial_tables.py tests/test_se_company_financial_extractors_sql.py tests/test_se_company_state_scan.py tests/test_se_company_person_extractors_sql.py -q
uv run --env-file .env pytest tests/test_se_company_financial_fold_clickhouse_local.py tests/test_se_company_financial_extractors_clickhouse_local.py tests/test_se_company_person_extractors_clickhouse_local.py -q -m integration
set -a; source .env; set +a; uv run pytest tests -q -p no:cacheprovider --ignore=tests/test_schedule_cron_contracts.py --deselect tests/test_backfill_policy_contracts.py::test_every_partitioned_asset_uses_multi_run_backfill_policy --deselect tests/test_duckdb_bulk_loading_contract.py::test_production_has_only_the_explicit_ted_executemany_debt --deselect tests/test_nace_categories.py::test_nace_assets_are_registered_as_staged_flow --deselect tests/test_sweden_address_geocoding.py::test_lantmateriet_credentials_are_documented_without_values --deselect tests/test_technology_aliases_clickhouse.py::test_catalog_asset_publishes_aliases_clears_them_and_rejects_bad_input 2>&1 | tail -3
```

Expected: all green; the wider run `N passed, 3 skipped, 5 deselected`, no failures (about 8 minutes).

- [ ] **Step 4: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/financial/docs/financial-design.md corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-11-se-company-financial-entity-design.md
git commit -m "docs(se-financial): the fold in the package note; spec records slice 3 code complete"
```

---

### Task 7: Prod backfill (after the final review and the merge)

**Files:** none. Runs against the dagster and companycollect hosts.

**Preconditions:** the whole-branch review is clean and the branch is merged into main (`git merge --no-ff` from the main checkout, having checked the owner's dirty files do not overlap the branch's and re-checked overlap if main moved); the worktree fast-forwarded to main; prod ledger 403 (the address track's 000403 is applied; this slice has no migration); the run queue state noted (if still held by the ESEF refresh runs, everything below runs in-process).

- [ ] **Step 1: Deploy dagster_v3 from the worktree**

```bash
D=/Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity/corpscout/services/dagster_v3
cd "$D" && test -z "$(git diff main --stat)" && test -f "$D/.env" && test -z "$(git status --porcelain src)"
uv sync --frozen
uv run --frozen --no-sync dbt parse --project-dir "$D/src/dagster_v3/defs/finland_ytj/dbt" --profiles-dir "$D/src/dagster_v3/defs/finland_ytj/dbt"
uv run --frozen --no-sync dbt parse --project-dir "$D/src/dagster_v3/defs/exchange_rates_v2/dbt" --profiles-dir "$D/src/dagster_v3/defs/exchange_rates_v2/dbt"
uv run --frozen --no-sync dg utils refresh-defs-state
uv run --frozen --no-sync dg check defs
cd "$D/ansible" && ANSIBLE_BECOME_TIMEOUT=60 ansible-playbook -i inventory.ini light_sync.yml > /tmp/light_sync_slice3.log 2>&1; echo "RC=$?"; tail -2 /tmp/light_sync_slice3.log
```

Expected: `RC=0`, `failed=0`. Confirm through GraphQL `assetNodes` that `se_company_financial_fold` is live with 64 partition keys and `se_company_financial_fold_companies` exists. Confirm the 82 global precedence rows are still there (`SELECT count() FROM corpscout.se_company_financial_precedence FINAL WHERE company_id = ''`).

- [ ] **Step 2: Smoke on one company with the targeted fold**

In-process on the host (the runner script `/tmp/fin_host_run.sh` from slice 2 selects by asset name; write a sibling `/tmp/fin_fold_one.sh` or call the CLI directly):

```bash
ssh dagster 'cd /opt/companycollect/corpscout/dagster_v3 && sudo -n bash -c "set -a; source .env; set +a; export HOME=/root DAGSTER_HOME=/opt/companycollect/corpscout/dagster_v3 DAGSTER_DISABLE_TELEMETRY=1 PYTHONDONTWRITEBYTECODE=1 VIRTUAL_ENV=/opt/companycollect/corpscout/dagster_v3/.venv PATH=/root/.local/bin:/opt/companycollect/corpscout/dagster_v3/.venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin TERM=dumb; timeout 1800 .venv/bin/dagster asset materialize -m dagster_v3.definitions -a defs --select se_company_financial_fold_companies --config-json \"{\\\"ops\\\": {\\\"se_company_financial_fold_companies\\\": {\\\"config\\\": {\\\"company_ids\\\": [\\\"5567081699\\\"]}}}}\" > /tmp/fin_fold_smoke.log 2>&1; echo exit=\$?"; grep -E "RUN_SUCCESS|RUN_FAILURE|Traceback" /tmp/fin_fold_smoke.log | cut -c1-160'
```

(Quoting across ssh, sudo and bash is brittle: prefer writing the JSON to `/tmp/fin_fold_smoke.json` with scp and passing `--config-json "$(cat /tmp/fin_fold_smoke.json)"` inside a host-side script, as slice 2's `fin_host_run.sh` did.) Then read:

```sql
SELECT period_key, currency, currency_source, revenue_amount_original, revenue_amount_usd, revenue_source, total_assets_source, equity_source, employees, employees_source, arrayStringConcat(sources, ',') AS sources, active FROM corpscout.se_company_financial FINAL WHERE company_id = '5567081699' ORDER BY period_key;
SELECT change_kind, count() FROM corpscout.se_company_financial_history WHERE company_id = '5567081699' GROUP BY change_kind;
```

Derive the expectation from the source rows first — the fold's answer must follow the precedence map applied to what the suggestion table actually holds, not this prose:

```sql
SELECT source, period_key, currency, employees, revenue_amount_original, total_assets_amount_original FROM corpscout.se_company_financial_suggestion FINAL WHERE company_id = '5567081699' ORDER BY period_key, source;
```

Expected: 8 standalone periods (2018-08-31 to 2025-12-31) and 3 consolidated (2022 to 2024); standalone 2023: currency SEK from ratsit, revenue 60,300,000 / 6,005,001.80 from ratsit, employees 2,100 from bolagsverket (Ratsit outranks Bolagsverket for employees at 1000 over 900, but Ratsit's 2023 row carries no employee count on prod; where it does, Ratsit's count wins), sources `bolagsverket,ratsit`; 2018-08-31 has sources `bolagsverket_comparative` alone; consolidated 2023: revenue 1,296,506,000 from esef; history: 11 `created`.

- [ ] **Step 3: The 64-bucket backfill, in-process, one bucket at a time**

Upload and launch this host script (as root, detached), then poll `/tmp/fin_fold_all.status` with a remote sleep loop inside one ssh call (a local background poller gets killed for memory on the Mac):

```bash
#!/bin/bash
# /tmp/fin_fold_all.sh -- the 64 fold buckets, one after the other, in-process on the dagster host.
set -u
cd /opt/companycollect/corpscout/dagster_v3 || exit 2
set -a; source .env; set +a
export HOME=/root DAGSTER_HOME=/opt/companycollect/corpscout/dagster_v3 DAGSTER_DISABLE_TELEMETRY=1 PYTHONDONTWRITEBYTECODE=1 VIRTUAL_ENV=/opt/companycollect/corpscout/dagster_v3/.venv PATH=/root/.local/bin:/opt/companycollect/corpscout/dagster_v3/.venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin TERM=dumb
cfg='{"ops": {"se_company_financial_fold": {"config": {"changed_only": true, "page_size": 5000}}}}'
first="${1:-0}"
[ "$first" = "0" ] && : > /tmp/fin_fold_all.status
for n in $(seq -f "%02g" "$first" 63); do
  start=$(date +%s)
  timeout 7200 .venv/bin/dagster asset materialize -m dagster_v3.definitions -a defs --select se_company_financial_fold --partition "bucket_$n" --config-json "$cfg" > "/tmp/fin_fold_bucket_$n.log" 2>&1
  rc=$?; end=$(date +%s)
  echo "bucket_$n exit=$rc wall=$((end-start))s pages=$(grep -c 'Folded financial page' "/tmp/fin_fold_bucket_$n.log")" >> /tmp/fin_fold_all.status
  if [ $rc -ne 0 ]; then echo "STOP at bucket_$n" >> /tmp/fin_fold_all.status; exit $rc; fi
done
echo "ALL DONE" >> /tmp/fin_fold_all.status
```

Launch: `ssh dagster 'sudo -n nohup bash /tmp/fin_fold_all.sh 0 > /tmp/fin_fold_all.launch 2>&1 < /dev/null &'`. Time bucket_00 alone first: at tens of seconds to a few minutes per bucket, proceed; above about five minutes per bucket, stop and raise page_size in the script (the config allows up to 20,000, which turns five full FINAL scans per bucket into two). Expected: about 22k companies per bucket (1.4M / 64), five pages, tens of seconds to a few minutes per bucket; record the total wall time. A bucket that fails stops the loop: read its log, fix or rule, and relaunch with the bucket number as the argument (`bash /tmp/fin_fold_all.sh 17`), which resumes from that bucket and appends to the status file. A non-zero exit can leave a claimed slot on the pool se_company_financial_fold (the run was SIGTERMed by timeout); before relaunching, run scripts/dagster-health-check.py --fix on the host (uv run python scripts/dagster-health-check.py --fix from the project directory with the .env sourced) so the leaked slot cannot block the next bucket. Every bucket's metadata comes back through GraphQL `assetMaterializations(limit: 64)` on `se_company_financial_fold`: sum `periods`, `published`, `created`, `considered`, `unpublished` over the 64 partitions for the record.

- [ ] **Step 4: Convergence check**

Re-run buckets 00, 31 and 63 the same way (`changed_only: true`). Expected: `considered = 0`, `periods = 0` on each.

- [ ] **Step 5: Readouts (spec section 11)**

```sql
SELECT scope, count() AS rows, uniqExact(company_id) AS companies, countIf(active = 1) AS active_rows, countIf(inactive_reason = 'hidden') AS hidden, countIf(inactive_reason = 'withdrawn') AS withdrawn FROM corpscout.se_company_financial FINAL GROUP BY scope ORDER BY scope;
SELECT uniqExact(company_id) AS companies_with_a_standalone_period FROM corpscout.se_company_financial FINAL WHERE scope = 'standalone' AND active = 1;
SELECT count() AS latest_rows FROM corpscout.se_company_financials_latest;
SELECT currency_source, count() FROM corpscout.se_company_financial FINAL GROUP BY currency_source ORDER BY count() DESC;
SELECT revenue_source, count() FROM corpscout.se_company_financial FINAL GROUP BY revenue_source ORDER BY count() DESC;
SELECT total_assets_source, count() FROM corpscout.se_company_financial FINAL GROUP BY total_assets_source ORDER BY count() DESC;
SELECT equity_source, count() FROM corpscout.se_company_financial FINAL GROUP BY equity_source ORDER BY count() DESC;
SELECT employees_source, count() FROM corpscout.se_company_financial FINAL GROUP BY employees_source ORDER BY count() DESC;
SELECT currency, count() FROM corpscout.se_company_financial FINAL GROUP BY currency ORDER BY count() DESC LIMIT 8;
SELECT count() AS derived_only_rows FROM corpscout.se_company_financial AS m FINAL WHERE sources = ['ratsit'] AND (company_id, period_key) IN (SELECT company_id, period_key FROM corpscout.se_company_financial_suggestion FINAL WHERE source = 'ratsit' AND period_end_derived = 1);
SELECT count(DISTINCT a.company_id) AS companies_with_two_standalone_ends_within_7_days FROM corpscout.se_company_financial AS a FINAL INNER JOIN corpscout.se_company_financial AS b FINAL ON a.company_id = b.company_id AND a.scope = b.scope WHERE a.scope = 'standalone' AND a.active = 1 AND b.active = 1 AND a.period_end < b.period_end AND dateDiff('day', a.period_end, b.period_end) <= 7;
SELECT change_kind, count() FROM corpscout.se_company_financial_history GROUP BY change_kind ORDER BY change_kind;
SELECT length(sources) AS source_count, count() FROM corpscout.se_company_financial FINAL GROUP BY source_count ORDER BY source_count;
SELECT count() AS money_rows_discarded_by_the_currency_gate FROM corpscout.se_company_financial_suggestion AS s FINAL INNER JOIN corpscout.se_company_financial AS m FINAL ON m.company_id = s.company_id AND m.period_key = s.period_key WHERE s.currency IS NOT NULL AND s.currency != m.currency AND s.revenue_amount_original IS NOT NULL;
-- unpublished: sum the 'unpublished' metadata over the 64 partitions of se_company_financial_fold (GraphQL assetMaterializations limit 64); expected 0.
```

Expected shape: history only `created` on the first backfill; the standalone company count in the same range as the 579,766 rows `se_company_financials_latest` holds (Bolagsverket companies) plus the Ratsit-only ones (about 1.09M company-years are Ratsit-only, spec 5) — record the numbers; the seven-day readout decides whether a merge rule is worth designing later (spec 6).

- [ ] **Step 6: Record**

Append to spec section 12 item 3 the prod record: date, deploy, the smoke, the 64 buckets' totals and wall time, the convergence check, every readout number, and the seven-day count with the ruling it implies. Commit on the branch as `docs(se-financial): slice 3 shipped, prod record; plan ticked`, merge into main (`--no-ff`, after re-checking overlap), fast-forward the worktree, update the memory file, then write slice 4's plan (the cutover; its migration is 000404 (the spec's 000402 went to the person-roles track and 000403 to the address track)).

---

## Self-review

**Spec coverage.** Section 6's fold steps 1 to 6 → Task 1 (`fold_financial`), tested one by one; its batch layer steps 1 to 4 → Task 2 (`fold_companies`, `_changed_company_ids`, `fold_company_periods`); section 5's rule resolution → `resolve_rules` (Task 1) with the period-over-company-wide-over-global test; section 4.3's history shape → `history_tuple` and `changed_fields_against`; section 8's two assets, partitions, pool, backfill policy and configs → Task 3; section 11's pure fold tests and clickhouse-local batch test under both `join_use_nulls` settings → Tasks 1 and 4; section 11's prod readouts → Task 7 Step 5; section 12 item 3 → Tasks 6 and 7. The slice-2 parked nits → Task 5. Not in this slice: readers, the backoffice, `company_financials_latest`'s Sweden leg (slice 4).

**Deviations, stated.** `fold_financial` takes `source_run_id` as a keyword beyond the spec's five positional parameters. The fold asset declares the four extractors as deps (lineage the spec leaves implicit). `LIVE_ROW_PREDICATE` is defined in `tables.py` and re-exported by `suggestions.py` (an import cycle otherwise). A history row is written for an activity change with no value change, with an empty `changed_fields` (spec 4.3 says "values, sources or activity changed"). `page_size` is capped at 20,000 by the config.

**Placeholder scan.** None: every module, test and script is given in full; the prod steps carry their expected results.

**Type consistency.** `Suggestion.money: Mapping[str, Money]` is what `batch.suggestion_from_row` builds and `fold_financial` reads (`s.money[field].original`); `FinancialRow.money: Mapping[str, MoneyCell]` is what `main_row_from_row` builds and `as_values` flattens into `MAIN_COLUMNS`; `fold_company_periods(company_id, suggestions, previous, rules_by_period, hidden_periods, *, source_run_id)` is called with `rules.get(company_id)` (a `RulesByPeriod` or None) and `hidden_periods.get(company_id, set())`; `FoldCounts.as_metadata()` feeds `_fold_metadata` in `assets.py`; `financial_bucket_index` parses the keys `FINANCIAL_FOLD_PARTITIONS` defines.
