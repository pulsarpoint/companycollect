# src/dagster_v3/defs/se_company/person/batch.py
"""Read normalized rows, fold in memory, write history then main (spec 2026-09-09 section
5.6).

Every SELECT is a function returning its exact text, so the clickhouse-local harness runs the
same SQL the asset runs. Parameters bind client-side through clickhouse-driver's %(name)s
syntax. Unlike the address fold there is no geocoder here: no DuckDB, no pool, no cache --
the page is four reads, a pure fold and two inserts.
"""

from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from dagster_v3.defs.se_company.common import normalized_se_company_ids
from dagster_v3.defs.se_company.person import tables
from dagster_v3.defs.se_company.person.fold import (
    EXCLUDED_SOURCES,
    FOLD_VERSION,
    FOLDABLE_STATUS,
    MATCH_THRESHOLD,
    MatchPair,
    NormalizedRow,
    PersonRule,
    PublishedPerson,
    fold_company_persons,
)
from dagster_v3.defs.se_company.person.precedence import FIELD as PRECEDENCE_FIELD

BUCKET_COUNT = 64
PAGE_SIZE = 20_000

# clickhouse-driver renders %(company_ids)s into the statement text; one bound 20,000-id
# page is about 320 KB, past ClickHouse's 262,144-byte default max_query_size. Every
# statement here binds the list once (~320 KB rendered, 3.3x headroom) EXCEPT
# `match_pairs_sql`, which binds it twice -- the outer IN prunes the pair table before the
# join, which the join alone would not -- and renders to about 640 KB, 1.64x headroom under
# this setting and the widest statement of the page (measured by the guard test in
# tests/test_se_company_person_batch.py). max_execution_time makes a pathological page fail
# visibly instead of holding a connection forever.
FOLD_ID_BOUND_QUERY_SETTINGS = {"max_query_size": 1_048_576, "max_execution_time": 1800}

NORMALIZED_SELECT_COLUMNS: tuple[str, ...] = (
    "company_id", "source", "slot", "normalized_id", "parse_status",
    "first_tokens", "middle_tokens", "last_tokens",
    "display_first", "display_last", "display_name",
    "birth_year", "wikidata_id", "role_code", "role_year", "role_from", "role_to", "data",
)
# The main read takes every column: the fold compares 26 of them, and a history row is the
# PREVIOUS image, which needs that row's own folded_at, fold_version and source_run_id too.
MAIN_SELECT_COLUMNS: tuple[str, ...] = tables.MAIN_COLUMNS
RULE_SELECT_COLUMNS: tuple[str, ...] = ("company_id", "rule_id", "kind", "person_keys", "slots")
MATCH_PAIR_SELECT_COLUMNS: tuple[str, ...] = (
    "company_id", "members_a", "members_b", "confidence", "reason",
    "name_a", "name_b", "model", "prompt_version",
)
_EXCLUDED_SQL = " AND ".join(f"source != '{source}'" for source in EXCLUDED_SOURCES)


@dataclass(frozen=True, slots=True)
class FoldCounts:
    companies: int          # ids handed in
    considered: int         # ids the selection picked (== companies when changed_only=False)
    pages: int
    persons: int            # active rows written
    created: int            # the five change kinds are history rows, not row states
    updated: int
    hidden: int
    withdrawn: int
    reactivated: int
    unchanged: int          # rows rewritten with no history row
    stale_rules: int        # active rules with a key or slot that resolved to nothing
    sets_split_by_birth_year: int

    def as_metadata(self) -> dict[str, Any]:
        return {
            "companies": self.companies, "considered": self.considered, "pages": self.pages,
            "persons": self.persons, "created": self.created, "updated": self.updated,
            "hidden": self.hidden, "withdrawn": self.withdrawn,
            "reactivated": self.reactivated, "unchanged": self.unchanged,
            "stale_rules": self.stale_rules,
            "sets_split_by_birth_year": self.sets_split_by_birth_year,
            "fold_version": FOLD_VERSION,
        }


def bucket_company_ids_sql() -> str:
    """The same hash and modulus as the address fold -- never a second hash function."""
    return (
        "SELECT DISTINCT company_id\n"
        f"FROM {tables.QUALIFIED_NORMALIZED_TABLE}\n"
        f"WHERE modulo(cityHash64(company_id), {BUCKET_COUNT}) = %(bucket)s\n"
        "ORDER BY company_id"
    )


def normalized_watermarks_sql() -> str:
    """Newest normalized version per company and how many of its current rows fold. FINAL,
    so a row whose current version is no_person does not count as foldable through an older
    ok version -- and the max is over EVERY status, because a row leaving `ok` changes the
    published set as much as a new one arriving."""
    return (
        "SELECT company_id, max(normalized_at) AS normalized_at, "
        f"countIf(parse_status = '{FOLDABLE_STATUS}') AS foldable\n"
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
    """Every rule version, whatever its `active` value: a Reset is a NEW version of the same
    rule row with active = 0 and a newer created_at, and a company that could not see it
    would apply the rule for ever. No FINAL: max() over the versions is the newest anyway."""
    return (
        "SELECT company_id, max(created_at) AS created_at\n"
        f"FROM {tables.QUALIFIED_RULE_TABLE}\n"
        "WHERE company_id IN %(company_ids)s\n"
        "GROUP BY company_id"
    )


def company_precedence_watermarks_sql() -> str:
    return (
        "SELECT company_id, max(decided_at) AS decided_at\n"
        f"FROM {tables.QUALIFIED_PRECEDENCE_TABLE}\n"
        "WHERE company_id IN %(company_ids)s\n"
        "GROUP BY company_id"
    )


def match_watermarks_sql() -> str:
    """Newest match stamp per company (spec section 4). No FINAL: max() over the versions is
    the newest anyway, and an errored attempt is a real input change too -- it can turn a
    company's pairs from something into nothing."""
    return (
        "SELECT company_id, max(matched_at) AS matched_at\n"
        f"FROM {tables.QUALIFIED_MATCH_STATE_TABLE}\n"
        "WHERE company_id IN %(company_ids)s\n"
        "GROUP BY company_id"
    )


def match_pairs_sql() -> str:
    """The page's scored pairs at or above the threshold, for the company's CURRENT input.

    The state table is one row per company, so the join on `input_hash` is what supersedes a
    previous input's pairs without deleting them; `error = ''` keeps a company whose last
    attempt failed from folding on a hash nothing certified. An INNER JOIN, so the result
    cannot depend on `join_use_nulls`.

    The id list is bound TWICE on purpose: the inner select needs it, and the outer
    `p.company_id IN` prunes the pair table's granules before the join instead of after it.
    That doubles the rendered statement to about 640 KB at PAGE_SIZE, which is why
    FOLD_ID_BOUND_QUERY_SETTINGS raises max_query_size to 1 MiB."""
    return (
        f"SELECT {', '.join(f'p.{column}' for column in MATCH_PAIR_SELECT_COLUMNS)}\n"
        f"FROM {tables.QUALIFIED_MATCH_TABLE} AS p FINAL\n"
        "INNER JOIN (\n"
        "    SELECT company_id, input_hash, matched_at\n"
        f"    FROM {tables.QUALIFIED_MATCH_STATE_TABLE} FINAL\n"
        "    WHERE company_id IN %(company_ids)s AND error = ''\n"
        ") AS s ON s.company_id = p.company_id AND s.input_hash = p.input_hash\n"
        "    AND s.matched_at = p.matched_at\n"
        f"WHERE p.company_id IN %(company_ids)s AND p.confidence >= {MATCH_THRESHOLD}\n"
        "ORDER BY p.company_id, p.candidate_a, p.candidate_b"
    )


def global_precedence_watermark_sql() -> str:
    """One scalar for the whole run: the newest export of the global order. A dictionary
    change therefore re-folds every company once, which is exactly what it should do -- the
    order decides every published spelling. An empty table answers with the epoch."""
    return (
        "SELECT max(decided_at) AS decided_at\n"
        f"FROM {tables.QUALIFIED_PRECEDENCE_TABLE}\n"
        "WHERE company_id = ''"
    )


def current_normalized_sql() -> str:
    return (
        f"SELECT {', '.join(NORMALIZED_SELECT_COLUMNS)}\n"
        f"FROM {tables.QUALIFIED_NORMALIZED_TABLE} FINAL\n"
        f"WHERE company_id IN %(company_ids)s AND {_EXCLUDED_SQL} "
        f"AND parse_status = '{FOLDABLE_STATUS}'\n"
        "ORDER BY company_id, source, slot"
    )


def current_main_rows_sql() -> str:
    return (
        f"SELECT {', '.join(MAIN_SELECT_COLUMNS)}\n"
        f"FROM {tables.QUALIFIED_MAIN_TABLE} FINAL\n"
        "WHERE company_id IN %(company_ids)s"
    )


def active_rules_sql() -> str:
    return (
        f"SELECT {', '.join(RULE_SELECT_COLUMNS)}\n"
        f"FROM {tables.QUALIFIED_RULE_TABLE} FINAL\n"
        "WHERE company_id IN %(company_ids)s AND active = 1\n"
        "ORDER BY company_id, rule_id"
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
    return (
        f"INSERT INTO {tables.QUALIFIED_HISTORY_TABLE} "
        f"({', '.join(tables.HISTORY_COLUMNS)}) VALUES"
    )


def normalized_row_from_row(row: Sequence[Any]) -> NormalizedRow:
    values = dict(zip(NORMALIZED_SELECT_COLUMNS, row, strict=True))
    for name in ("first_tokens", "middle_tokens", "last_tokens"):
        values[name] = tuple(values[name])
    return NormalizedRow(**values)


def main_row_from_row(row: Sequence[Any]) -> PublishedPerson:
    values = dict(zip(MAIN_SELECT_COLUMNS, row, strict=True))
    for name in ("sources", "slots", "normalized_ids", *tables.MEMBER_COLUMNS,
                 "role_codes", "role_years", "current_roles"):
        values[name] = tuple(values[name])
    values["role_sources"] = tuple(tuple(sources) for sources in values["role_sources"])
    return PublishedPerson(**values)


def rule_from_row(row: Sequence[Any]) -> PersonRule:
    company_id, rule_id, kind, person_keys, slots = row
    return PersonRule(company_id, str(rule_id), kind, tuple(person_keys), tuple(slots))


def match_pair_from_row(row: Sequence[Any]) -> MatchPair:
    values = dict(zip(MATCH_PAIR_SELECT_COLUMNS, row, strict=True))
    return MatchPair(
        members_a=tuple(values["members_a"]), members_b=tuple(values["members_b"]),
        confidence=float(values["confidence"]), reason=str(values["reason"]),
        name_a=str(values["name_a"]), name_b=str(values["name_b"]),
        model=str(values["model"]), prompt_version=str(values["prompt_version"]),
    )


def _pages(items: Sequence[str], size: int) -> list[list[str]]:
    return [list(items[index : index + size]) for index in range(0, len(items), size)]


def _changed_company_ids(
    client: Any, company_ids: list[str], *, global_precedence_at: datetime | None
) -> list[str]:
    params = {"company_ids": company_ids}

    def read(sql: str) -> list:
        return client.execute(sql, params, settings=FOLD_ID_BOUND_QUERY_SETTINGS)

    normalized = {row[0]: (row[1], int(row[2])) for row in read(normalized_watermarks_sql())}
    folded = dict(read(main_watermarks_sql()))
    ruled = dict(read(rule_watermarks_sql()))
    decided = dict(read(company_precedence_watermarks_sql()))
    matched = dict(read(match_watermarks_sql()))
    changed: list[str] = []
    for company_id in company_ids:
        if company_id not in normalized:
            continue
        newest, foldable = normalized[company_id]
        for stamp in (
            ruled.get(company_id), decided.get(company_id), matched.get(company_id),
            global_precedence_at,
        ):
            if stamp is not None and stamp > newest:
                newest = stamp
        if company_id not in folded:
            if foldable > 0:
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
    """Fold the given companies in pages of `page_size`.

    Every folded company's whole set is rewritten with this `folded_at` -- active, hidden and
    withdrawn rows alike -- so the changed_only selection converges; history rows are written
    only where a compared column changed, and always BEFORE the main insert."""
    ids = list(normalized_se_company_ids(company_ids))
    current_year = (folded_at.astimezone(UTC) if folded_at.tzinfo else folded_at).year
    global_precedence_at = None
    if changed_only and ids:
        rows = client.execute(global_precedence_watermark_sql())
        global_precedence_at = rows[0][0] if rows else None
    considered = pages = persons = unchanged = stale_rules = sets_split = 0
    kinds = {"created": 0, "updated": 0, "hidden": 0, "withdrawn": 0, "reactivated": 0}
    for page in _pages(ids, page_size):
        pages += 1
        scope = (
            _changed_company_ids(client, page, global_precedence_at=global_precedence_at)
            if changed_only
            else page
        )
        considered += len(scope)
        if not scope:
            continue
        params = {"company_ids": scope}

        def read(sql: str) -> list:
            return client.execute(sql, params, settings=FOLD_ID_BOUND_QUERY_SETTINGS)

        by_company: dict[str, list[NormalizedRow]] = defaultdict(list)
        for row in read(current_normalized_sql()):
            normalized = normalized_row_from_row(row)
            by_company[normalized.company_id].append(normalized)
        current: dict[str, list[PublishedPerson]] = defaultdict(list)
        for row in read(current_main_rows_sql()):
            published = main_row_from_row(row)
            current[published.company_id].append(published)
        rules: dict[str, list[PersonRule]] = defaultdict(list)
        for row in read(active_rules_sql()):
            rule = rule_from_row(row)
            rules[rule.company_id].append(rule)
        precedence: dict[str, dict[str, int]] = defaultdict(dict)
        for company_id, source, number in read(company_precedence_sql()):
            precedence[company_id][source] = int(number)
        matches: dict[str, list[MatchPair]] = defaultdict(list)
        for row in read(match_pairs_sql()):
            matches[str(row[0])].append(match_pair_from_row(row))

        main_rows: list[tuple[Any, ...]] = []
        history_rows: list[tuple[Any, ...]] = []
        page_persons = 0
        for company_id in scope:
            rows = by_company.get(company_id, [])
            previous = current.get(company_id, [])
            if not rows and not previous:
                continue
            result = fold_company_persons(
                company_id, rows, previous, rules.get(company_id, []),
                precedence.get(company_id), source_run_id=source_run_id,
                current_year=current_year, matches=matches.get(company_id, []),
            )
            page_persons += result.persons
            unchanged += result.unchanged
            stale_rules += result.stale_rules
            sets_split += result.sets_split_by_birth_year
            for kind in kinds:
                kinds[kind] += getattr(result, kind)
            main_rows.extend(row.as_tuple(folded_at) for row in result.rows)
            history_rows.extend(
                entry.row.history_tuple(
                    changed_at=folded_at, change_kind=entry.change_kind, fold_run_id=source_run_id
                )
                for entry in result.history
            )
        persons += page_persons
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
                "Folded person page %d: companies=%d considered=%d persons=%d rows=%d history=%d",
                pages, len(page), len(scope), page_persons, len(main_rows), len(history_rows),
            )
    return FoldCounts(
        companies=len(ids), considered=considered, pages=pages, persons=persons,
        created=kinds["created"], updated=kinds["updated"], hidden=kinds["hidden"],
        withdrawn=kinds["withdrawn"], reactivated=kinds["reactivated"], unchanged=unchanged,
        stale_rules=stale_rules, sets_split_by_birth_year=sets_split,
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
        for row in client.execute(
            bucket_company_ids_sql(), {"bucket": bucket}, settings=FOLD_ID_BOUND_QUERY_SETTINGS
        )
    ]
    return fold_companies(
        client, company_ids, changed_only=changed_only, source_run_id=source_run_id,
        folded_at=folded_at, page_size=page_size, log=log,
    )
