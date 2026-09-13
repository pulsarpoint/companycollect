"""Read a page of companies' current suggestion rows, fold every period in memory, write
history then main (spec 2026-09-11 section 6, batch layer).

Every SELECT is a function returning its exact text, so the clickhouse-local harness runs the
same SQL the asset runs. Parameters bind client-side through clickhouse-driver's %(name)s
syntax, which is why the partition filter says modulo(...) rather than the % operator. The page
is four reads under FINAL (live suggestions, main rows, company rules, hide rules) after four
watermark reads (one of them FINAL), a pure fold per company and two inserts.
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
# five suggestion rows across its sources and periods (7.58M rows over 1.4M companies on
# 2026-09-12) and each row is 52 values wide, so a page is about 27k rows, roughly 300 MB in
# memory with the driver tuples and the fold's objects; the 64-bucket backfill is about 12k
# companies per bucket, three pages each.
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
