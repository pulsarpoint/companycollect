"""Read normalized rows, fold in memory, geocode the page's distinct location keys, write
history then main (spec sections 5 and 6, amended 2026-09-06).

Every SELECT is a function returning its exact text so the clickhouse-local harness runs
the same SQL. Parameters bind client-side through clickhouse-driver's %(name)s syntax.
"""

from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from dagster_v3.defs.se_company.address import tables
from dagster_v3.defs.se_company.address.fold import (
    EXCLUDED_SOURCES,
    PUBLISHABLE_STATUSES,
    NormalizedRow,
    PublishedAddress,
    fold_company_addresses,
)
from dagster_v3.defs.se_company.address.geocode import geocode_addresses
from dagster_v3.defs.se_company.address.normalize_se import NORMALIZER_VERSION
from dagster_v3.defs.se_company.address.precedence import FIELD as PRECEDENCE_FIELD
from dagster_v3.defs.se_company.common import normalized_se_company_ids
from dagster_v3.defs.sweden_company.address_resolution_policy import SWEDEN_ADDRESS_RESOLUTION_POLICY
from dagster_v3.defs.sweden_company.address_resolution_shadow import ensure_reference_documents
from dagster_v3.defs.sweden_company.geocode_store import LEGACY_ADOPTED_POLICY_VERSION

BUCKET_COUNT = 64
PAGE_SIZE = 20_000

# clickhouse-driver renders %(company_ids)s into the statement text; a 20,000-id page is
# about 300 KB, past ClickHouse's 262,144-byte default max_query_size. Each statement here
# binds the id list once, so 1 MiB is >3x the measured worst case (guard test in
# tests/test_se_company_address_batch.py). max_execution_time makes a pathological page
# fail visibly instead of holding the pool slot forever.
FOLD_ID_BOUND_QUERY_SETTINGS = {"max_query_size": 1_048_576, "max_execution_time": 1800}

NORMALIZED_SELECT_COLUMNS: tuple[str, ...] = (
    "company_id", "source", "slot", "normalized_id", "kind", *tables.COMPONENT_COLUMNS, "country_code",
    "normalized_address", "address_key", "parse_status", "normalizer_version", "suggested_at",
)
MAIN_COMPARE_COLUMNS: tuple[str, ...] = tuple(
    c for c in tables.MAIN_COLUMNS if c not in ("folded_at", "fold_version", "source_run_id")
)
_PUBLISHABLE_SQL = ", ".join(f"'{status}'" for status in PUBLISHABLE_STATUSES)
_EXCLUDED_SQL = " AND ".join(f"source != '{source}'" for source in EXCLUDED_SOURCES)


@dataclass(frozen=True, slots=True)
class FoldCounts:
    companies: int
    considered: int
    folded: int        # companies with at least one candidate
    published: int     # active rows written
    hidden: int
    # withdrawn rows written this run (every previously published key without a candidate,
    # already-withdrawn ones included); the newly withdrawn ones are inside `changed`
    withdrawn: int
    changed: int       # history rows
    unchanged: int
    unpublished: int   # considered companies with no candidate and no main row
    # geocoded/cache_hits/matched all accumulate per page: a location key shared by
    # companies in different pages counts once per page, so these are not distinct-key
    # counts across the whole run.
    geocoded: int      # distinct location keys handed to geocode_addresses, per page
    cache_hits: int
    matched: int       # keys the matcher resolved this run (misses), per page

    def as_metadata(self) -> dict[str, int]:
        return {
            "companies": self.companies, "considered": self.considered, "folded": self.folded,
            "published": self.published, "hidden": self.hidden, "withdrawn": self.withdrawn,
            "changed": self.changed, "unchanged": self.unchanged, "unpublished": self.unpublished,
            "geocoded": self.geocoded, "cache_hits": self.cache_hits, "matched": self.matched,
        }


def bucket_company_ids_sql() -> str:
    return (
        "SELECT DISTINCT company_id\n"
        f"FROM {tables.QUALIFIED_NORMALIZED_TABLE}\n"
        f"WHERE modulo(cityHash64(company_id), {BUCKET_COUNT}) = %(bucket)s\n"
        "ORDER BY company_id"
    )


def normalized_watermarks_sql() -> str:
    """Newest non-draft normalized version per company and how many current rows are
    publishable. FINAL, so a row whose current version is no_address does not count as
    publishable through an older version."""
    return (
        f"SELECT company_id, max(normalized_at) AS normalized_at, countIf(parse_status IN ({_PUBLISHABLE_SQL})) AS publishable\n"
        f"FROM {tables.QUALIFIED_NORMALIZED_TABLE} FINAL\n"
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
    return (
        "SELECT company_id, max(decided_at) AS decided_at\n"
        f"FROM {tables.QUALIFIED_RULE_TABLE}\n"
        "WHERE company_id IN %(company_ids)s\n"
        "GROUP BY company_id"
    )


def stale_companies_sql() -> str:
    """Companies with a live (not withdrawn, not foreign) row geocoded under another
    policy or OSM extract, or folded from another normalizer version. The imported
    `legacy_adopted_v1` family is an unconditional cache hit, so it is never stale."""
    return (
        "SELECT DISTINCT company_id\n"
        f"FROM {tables.QUALIFIED_MAIN_TABLE} FINAL\n"
        "WHERE company_id IN %(company_ids)s AND inactive_reason != 'withdrawn' AND geocode_status != 'foreign'\n"
        f"  AND geocode_policy != '{LEGACY_ADOPTED_POLICY_VERSION}'\n"
        "  AND (geocode_policy != %(policy)s OR geocode_reference != %(reference)s OR normalizer_version != %(normalizer)s)"
    )


def current_normalized_sql() -> str:
    return (
        f"SELECT {', '.join(NORMALIZED_SELECT_COLUMNS)}\n"
        f"FROM {tables.QUALIFIED_NORMALIZED_TABLE} FINAL\n"
        f"WHERE company_id IN %(company_ids)s AND {_EXCLUDED_SQL} AND parse_status IN ({_PUBLISHABLE_SQL})\n"
        "ORDER BY company_id, source, slot"
    )


def current_main_rows_sql() -> str:
    return (
        f"SELECT {', '.join(MAIN_COMPARE_COLUMNS)}\n"
        f"FROM {tables.QUALIFIED_MAIN_TABLE} FINAL\n"
        "WHERE company_id IN %(company_ids)s"
    )


def hidden_keys_sql() -> str:
    return (
        "SELECT company_id, address_key\n"
        f"FROM {tables.QUALIFIED_RULE_TABLE} FINAL\n"
        "WHERE company_id IN %(company_ids)s AND action = 'hide' AND removed = 0"
    )


def company_precedence_sql() -> str:
    return (
        "SELECT company_id, source, precedence\n"
        f"FROM {tables.QUALIFIED_PRECEDENCE_TABLE} FINAL\n"
        f"WHERE company_id IN %(company_ids)s AND field = '{PRECEDENCE_FIELD}' AND removed = 0"
    )


def main_insert_sql() -> str:
    return f"INSERT INTO {tables.QUALIFIED_MAIN_TABLE} ({', '.join(tables.MAIN_COLUMNS)}) VALUES"


def history_insert_sql() -> str:
    return f"INSERT INTO {tables.QUALIFIED_HISTORY_TABLE} ({', '.join(tables.HISTORY_COLUMNS)}) VALUES"


def normalized_row_from_row(row: Sequence[Any]) -> NormalizedRow:
    return NormalizedRow(**dict(zip(NORMALIZED_SELECT_COLUMNS, row, strict=True)))


# Comparison and withdrawal only: fold_version and source_run_id are filled with "" and
# replaced by the fold before anything is written.
def main_row_from_row(row: Sequence[Any]) -> PublishedAddress:
    values = dict(zip(MAIN_COMPARE_COLUMNS, row, strict=True))
    for name in ("kinds", "sources", "slots", "normalized_ids"):
        values[name] = tuple(values[name])
    return PublishedAddress(fold_version="", source_run_id="", **values)


def _pages(items: Sequence[str], size: int) -> list[list[str]]:
    return [list(items[i : i + size]) for i in range(0, len(items), size)]


def _changed_company_ids(client: Any, company_ids: list[str], *, policy: str, reference: str) -> list[str]:
    params = {"company_ids": company_ids}
    normalized = {
        row[0]: (row[1], int(row[2]))
        for row in client.execute(normalized_watermarks_sql(), params, settings=FOLD_ID_BOUND_QUERY_SETTINGS)
    }
    folded = dict(client.execute(main_watermarks_sql(), params, settings=FOLD_ID_BOUND_QUERY_SETTINGS))
    ruled = dict(client.execute(rule_watermarks_sql(), params, settings=FOLD_ID_BOUND_QUERY_SETTINGS))
    stale = {
        row[0]
        for row in client.execute(
            stale_companies_sql(),
            {**params, "policy": policy, "reference": reference, "normalizer": NORMALIZER_VERSION},
            settings=FOLD_ID_BOUND_QUERY_SETTINGS,
        )
    }
    changed: list[str] = []
    for company_id in company_ids:
        if company_id not in normalized:
            continue
        newest, publishable = normalized[company_id]
        rule_mark = ruled.get(company_id)
        if rule_mark is not None and rule_mark > newest:
            newest = rule_mark
        if company_id not in folded:
            if publishable > 0:
                changed.append(company_id)
        elif newest > folded[company_id] or company_id in stale:
            changed.append(company_id)
    return changed


def fold_companies(
    client: Any,
    duckdb: Any,
    company_ids: Sequence[str],
    *,
    changed_only: bool,
    source_run_id: str,
    folded_at: datetime,
    page_size: int = PAGE_SIZE,
    log: Callable[..., object] | None = None,
) -> FoldCounts:
    """Fold the given companies in pages. Every folded company's whole set is rewritten
    with this `folded_at` (active, hidden and withdrawn rows alike) so the changed_only
    selection converges; history rows only where the compared fields changed. The page's
    distinct location keys go through geocode_addresses once, before the writes; the
    function inserts its own fresh outcomes into the cache, so a crash between it and the
    main insert costs nothing on retry."""
    ids = list(normalized_se_company_ids(company_ids))
    policy = SWEDEN_ADDRESS_RESOLUTION_POLICY.version
    reference = ensure_reference_documents(duckdb, log=log)
    considered = folded = published = hidden = withdrawn = changed = unchanged = unpublished = 0
    geocoded = cache_hits = matched = 0
    for page in _pages(ids, page_size):
        scope = _changed_company_ids(client, page, policy=policy, reference=reference) if changed_only else page
        considered += len(scope)
        if not scope:
            continue
        params = {"company_ids": scope}
        by_company: dict[str, list[NormalizedRow]] = defaultdict(list)
        for row in client.execute(current_normalized_sql(), params, settings=FOLD_ID_BOUND_QUERY_SETTINGS):
            normalized = normalized_row_from_row(row)
            by_company[normalized.company_id].append(normalized)
        current: dict[str, list[PublishedAddress]] = defaultdict(list)
        for row in client.execute(current_main_rows_sql(), params, settings=FOLD_ID_BOUND_QUERY_SETTINGS):
            published_row = main_row_from_row(row)
            current[published_row.company_id].append(published_row)
        hidden_keys: dict[str, set[str]] = defaultdict(set)
        for company_id, key in client.execute(hidden_keys_sql(), params, settings=FOLD_ID_BOUND_QUERY_SETTINGS):
            hidden_keys[company_id].add(key)
        precedence: dict[str, dict[str, int]] = defaultdict(dict)
        for company_id, source, number in client.execute(company_precedence_sql(), params, settings=FOLD_ID_BOUND_QUERY_SETTINGS):
            precedence[company_id][source] = int(number)

        pending: list[tuple[str, PublishedAddress]] = []
        # Snapshot the running totals so the page's own log line can report page-local
        # deltas below, rather than the totals accumulated since fold_companies started.
        page_published_start, page_hidden_start, page_withdrawn_start = published, hidden, withdrawn
        for company_id in scope:
            rows = by_company.get(company_id, [])
            previous = current.get(company_id, [])
            if not rows and not previous:
                unpublished += 1
                continue
            result = fold_company_addresses(
                company_id, rows, previous, hidden_keys.get(company_id, set()), precedence.get(company_id),
                source_run_id=source_run_id,
            )
            if result.published or result.hidden:
                folded += 1
            published += result.published
            hidden += result.hidden
            withdrawn += result.withdrawn
            pending.extend((company_id, row) for row in result.rows)

        to_geocode = {row.location_key(): row.as_normalized_address() for _, row in pending if row.needs_geocode()}
        outcomes = geocode_addresses(
            to_geocode, clickhouse=client, duckdb=duckdb, run_id=source_run_id, matched_at=folded_at, log=log,
        ) if to_geocode else {}
        geocoded += len(to_geocode)
        cache_hits += sum(1 for outcome in outcomes.values() if outcome.from_cache)
        matched += sum(1 for outcome in outcomes.values() if not outcome.from_cache)

        main_rows: list[tuple[Any, ...]] = []
        history_rows: list[tuple[Any, ...]] = []
        for company_id, row in pending:
            if row.needs_geocode():
                outcome = outcomes.get(row.location_key())
                if outcome is None:
                    raise RuntimeError(f"no geocode outcome for {company_id} {row.location_key()}")
                row = row.with_geocode(outcome)
            previous_row = next((p for p in current.get(company_id, []) if p.address_key == row.address_key), None)
            values = row.as_tuple(folded_at)
            main_rows.append(values)
            if row.changed_against(previous_row):
                changed += 1
                history_rows.append(values)
            else:
                unchanged += 1
        if history_rows:
            # History first: a crash between the two statements costs up to a page's worth
            # of duplicate history rows on retry -- one per changed key in the page, each
            # attempt's own `folded_at` so the plain MergeTree never collapses them -- which
            # is cheaper than the alternative order's failure mode, a published row whose
            # first history row is missing.
            client.execute(history_insert_sql(), history_rows)
        if main_rows:
            client.execute(main_insert_sql(), main_rows)
        if log is not None:
            log(
                "Folded address page: companies=%d considered=%d published=%d hidden=%d withdrawn=%d "
                "history=%d geocoded=%d hits=%d matched=%d",
                len(page), len(scope), published - page_published_start, hidden - page_hidden_start,
                withdrawn - page_withdrawn_start, len(history_rows), len(to_geocode),
                sum(1 for o in outcomes.values() if o.from_cache), sum(1 for o in outcomes.values() if not o.from_cache),
            )
    return FoldCounts(
        companies=len(ids), considered=considered, folded=folded, published=published, hidden=hidden,
        withdrawn=withdrawn, changed=changed, unchanged=unchanged, unpublished=unpublished,
        geocoded=geocoded, cache_hits=cache_hits, matched=matched,
    )


def fold_bucket(
    client: Any,
    duckdb: Any,
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
        client, duckdb, company_ids, changed_only=changed_only, source_run_id=source_run_id,
        folded_at=folded_at, page_size=page_size, log=log,
    )
