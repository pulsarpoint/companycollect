"""Read current suggestion rows, fold in memory, rewrite every folded company so
`folded_at` advances and `changed_only` converges; write a history row only when a value
or source changed (spec section 5).

Every SELECT is a function returning its exact text so the clickhouse-local harness runs
the same SQL. Parameters bind client-side through clickhouse-driver's %(name)s syntax,
which is why the partition filter says modulo(...) rather than the % operator.
"""

from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from dagster_v3.defs.se_company.basic_info import tables
from dagster_v3.defs.se_company.basic_info.fold import BasicInfoRow, Suggestion, fold_basic_info
from dagster_v3.defs.se_company.common import normalized_se_company_ids

BUCKET_COUNT = 64
PAGE_SIZE = 20_000

# clickhouse-driver substitutes %(company_ids)s client-side (clickhouse_driver/util/escape.py
# renders the list into the query TEXT), so every id in a page lands in the statement the
# server has to parse. A full PAGE_SIZE page renders current_suggestions_sql to 320,286
# bytes with 12-digit ids (280,286 with 10-digit ones) -- past ClickHouse's 262,144-byte
# default max_query_size, which rejects the query with Code: 62 "Max query size exceeded"
# before reading anything. Raising the setting per query is the precedent this repo already
# set for the same failure in se_company/address.py (SCAN_MAX_QUERY_SIZE); PAGE_SIZE stays
# 20,000 and every SELECT that binds company_ids passes these settings to client.execute.
# See tests/test_se_company_basic_info_batch.py for the measured render size.
ID_BOUND_QUERY_SETTINGS = {"max_query_size": 1_048_576}  # 1 MiB: >3x the measured worst case

_SUGGESTION_SELECT_COLUMNS = (
    "company_id",
    "source",
    "source_record_uid",
    "observed_at",
    *tables.VALUE_COLUMNS,
)
_MAIN_COMPARE_COLUMNS = tuple(
    c for c in tables.MAIN_COLUMNS if c not in ("folded_at", "fold_version", "source_run_id")
)


@dataclass(frozen=True, slots=True)
class FoldCounts:
    companies: int
    considered: int
    folded: int
    changed: int
    unchanged: int
    unpublished: int

    def as_metadata(self) -> dict[str, int]:
        return {
            "companies": self.companies,
            "considered": self.considered,
            "folded": self.folded,
            "changed": self.changed,
            "unchanged": self.unchanged,
            "unpublished": self.unpublished,
        }


def bucket_company_ids_sql() -> str:
    return (
        "SELECT DISTINCT company_id\n"
        f"FROM {tables.QUALIFIED_SUGGESTION_TABLE}\n"
        f"WHERE modulo(cityHash64(company_id), {BUCKET_COUNT}) = %(bucket)s\n"
        "ORDER BY company_id"
    )


def suggestion_watermarks_sql() -> str:
    return (
        "SELECT company_id, max(suggested_at) AS suggested_at\n"
        f"FROM {tables.QUALIFIED_SUGGESTION_TABLE}\n"
        "WHERE company_id IN %(company_ids)s\n"
        "GROUP BY company_id"
    )


def main_watermarks_sql() -> str:
    return (
        "SELECT company_id, max(folded_at) AS folded_at\n"
        f"FROM {tables.QUALIFIED_MAIN_TABLE}\n"
        "WHERE company_id IN %(company_ids)s\n"
        "GROUP BY company_id"
    )


def current_suggestions_sql() -> str:
    return (
        f"SELECT {', '.join(_SUGGESTION_SELECT_COLUMNS)}\n"
        f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL\n"
        "WHERE company_id IN %(company_ids)s\n"
        "ORDER BY company_id, source"
    )


def current_main_rows_sql() -> str:
    return (
        f"SELECT {', '.join(_MAIN_COMPARE_COLUMNS)}\n"
        f"FROM {tables.QUALIFIED_MAIN_TABLE} FINAL\n"
        "WHERE company_id IN %(company_ids)s"
    )


def main_insert_sql() -> str:
    return f"INSERT INTO {tables.QUALIFIED_MAIN_TABLE} ({', '.join(tables.MAIN_COLUMNS)}) VALUES"


def history_insert_sql() -> str:
    return (
        f"INSERT INTO {tables.QUALIFIED_HISTORY_TABLE} "
        f"({', '.join(tables.HISTORY_COLUMNS)}) VALUES"
    )


def suggestion_from_row(row: Sequence[Any]) -> Suggestion:
    return Suggestion(**dict(zip(_SUGGESTION_SELECT_COLUMNS, row)))


# Comparison only: the row is read from _MAIN_COMPARE_COLUMNS, which excludes folded_at,
# fold_version and source_run_id, so those two are filled with "" purely to satisfy the
# dataclass. Never pass the result to as_tuple -- writing it would put an empty
# fold_version and source_run_id into the main table. Only the row that fold_basic_info
# produces is ever written.
def main_row_from_row(row: Sequence[Any]) -> BasicInfoRow:
    values = dict(zip(_MAIN_COMPARE_COLUMNS, row))
    return BasicInfoRow(fold_version="", source_run_id="", **values)


def _pages(items: Sequence[str], size: int) -> list[list[str]]:
    return [list(items[i : i + size]) for i in range(0, len(items), size)]


def _changed_company_ids(client: Any, company_ids: list[str]) -> list[str]:
    params = {"company_ids": company_ids}
    suggested = dict(
        client.execute(suggestion_watermarks_sql(), params, settings=ID_BOUND_QUERY_SETTINGS)
    )
    folded = dict(
        client.execute(main_watermarks_sql(), params, settings=ID_BOUND_QUERY_SETTINGS)
    )
    return [
        company_id
        for company_id in company_ids
        if company_id in suggested
        and (company_id not in folded or suggested[company_id] > folded[company_id])
    ]


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
    """Fold the given companies in pages. Every folded company gets a new main row, so
    `folded_at` advances and the `changed_only` selection converges (owner decision
    2026-09-04); a history row is added only when a value or source changed against the
    current main row. A full re-fold with `changed_only=False` rewrites every published
    row, which is fine for a manual backfill."""
    # Sorted, de-duplicated, validated: the helper raises "Sweden company ids must be 10
    # or 12 digits" on a bad id, before any query.
    ids = list(normalized_se_company_ids(company_ids))
    considered = folded = changed = unchanged = unpublished = 0
    for page in _pages(ids, page_size):
        page_unchanged, page_unpublished = unchanged, unpublished
        scope = _changed_company_ids(client, page) if changed_only else page
        considered += len(scope)
        if not scope:
            continue
        params = {"company_ids": scope}
        by_company: dict[str, list[Suggestion]] = defaultdict(list)
        for row in client.execute(
            current_suggestions_sql(), params, settings=ID_BOUND_QUERY_SETTINGS
        ):
            suggestion = suggestion_from_row(row)
            by_company[suggestion.company_id].append(suggestion)
        current = {
            row[0]: main_row_from_row(row)
            for row in client.execute(
                current_main_rows_sql(), params, settings=ID_BOUND_QUERY_SETTINGS
            )
        }
        main_rows: list[tuple[Any, ...]] = []
        history_rows: list[tuple[Any, ...]] = []
        for company_id in scope:
            folded_row = fold_basic_info(
                company_id, by_company.get(company_id, []), source_run_id=source_run_id
            )
            if folded_row is None:
                unpublished += 1
                continue
            folded += 1
            values = folded_row.as_tuple(folded_at)
            main_rows.append(values)
            changed_fields = folded_row.changed_fields_against(current.get(company_id))
            if changed_fields:
                changed += 1
                history_rows.append((*values, changed_fields))
            else:
                unchanged += 1
        if history_rows:
            # History first: the two statements are not one transaction, so a failure
            # between them costs a duplicate history row on the retry (a distinct
            # folded_at, visible in the timeline) rather than a published main row whose
            # first-publish history is never written because the retry sees no change.
            client.execute(history_insert_sql(), history_rows)
        if main_rows:
            # Every folded company lands here, changed or not: folded_at must advance on
            # every fold for changed_only's `max(suggested_at) > max(folded_at)` selection
            # to converge (owner decision 2026-09-04) instead of re-selecting an unchanged
            # company forever. history_rows holds only the rows whose value or source
            # changed against the current main row, so a page can have main rows with no
            # history rows -- hence the separate guards.
            client.execute(main_insert_sql(), main_rows)
        if log is not None:
            log(
                "Folded basic info page: companies=%d considered=%d changed=%d "
                "unchanged=%d unpublished=%d",
                len(page),
                len(scope),
                len(history_rows),
                unchanged - page_unchanged,
                unpublished - page_unpublished,
            )
    return FoldCounts(
        companies=len(ids),
        considered=considered,
        folded=folded,
        changed=changed,
        unchanged=unchanged,
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
        row[0] for row in client.execute(bucket_company_ids_sql(), {"bucket": bucket})
    ]
    return fold_companies(
        client,
        company_ids,
        changed_only=changed_only,
        source_run_id=source_run_id,
        folded_at=folded_at,
        page_size=page_size,
        log=log,
    )
