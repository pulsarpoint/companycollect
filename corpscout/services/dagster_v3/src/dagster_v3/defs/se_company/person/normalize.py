"""The normalize step (spec 2026-09-09 section 4): raw suggestion rows whose normalized row
is missing, computed from another raw version, or computed by an older normalizer are
normalized in Python and written as new versions of se_company_person_normalized.

The shape is the address entity's, module for module. Two details are this entity's own:

1. THE CHANGE SCAN COMPARES suggestion_id, not a timestamp. se_company_person_normalized
   carries no suggested_at, and suggestion_id is already sha256(company_id, source, slot,
   suggested_at), so any changed observation changes it.
2. `data` IS A String HOLDING A JSON OBJECT, constrained to one by the tables that carry it
   (CONSTRAINT valid_data). The read coerces anything else to the empty object, so a
   malformed extractor row degrades to `{}` instead of failing a whole 20,000-company page
   on the insert. Nothing here needs the native JSON type, and nothing here writes text SQL.
"""

import hashlib
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from dagster_v3.defs.se_company.basic_info.extract import SCAN_QUERY_SETTINGS, scope_pages
from dagster_v3.defs.se_company.person import tables
from dagster_v3.defs.se_company.person.normalize_se import (
    NORMALIZER_VERSION,
    NormalizedPerson,
    RawPerson,
    normalize_se_person,
)

PAGE_SIZE = 20_000
# changed_rows_sql() binds %(company_ids)s four times; see the test of the same name.
NORMALIZE_ID_BOUND_QUERY_SETTINGS = {"max_query_size": 4_194_304, "max_execution_time": 1800}
# This module's own scratch-table prefix (basic_info/extract.py:scope_pages), so a person
# scan's scratch table can never collide with a basic-info or an address one.
SCRATCH_SCOPE_PREFIX = "corpscout._tmp_person_scope_"

RAW_ROW_COLUMNS: tuple[str, ...] = (
    "company_id", "source", "slot", "suggestion_id", "full_name", "first_name", "last_name",
    "birth_year", "wikidata_id", "role_original", "role_key", "fiscal_year", "role_from",
    "role_to", "data",
)


def normalize_person(raw: RawPerson) -> NormalizedPerson:
    """The per-country dispatcher of spec section 4. Only Sweden exists."""
    return normalize_se_person(raw)


def _raw_select(alias: str) -> str:
    """`data` is coerced to a JSON object here so the normalized table's valid_data
    constraint can only ever fire on a row somebody wrote by hand."""
    return ", ".join(
        f"if(JSONType({alias}.data) = 'Object', {alias}.data, '{{}}') AS data"
        if column == "data"
        else f"{alias}.{column} AS {column}"
        for column in RAW_ROW_COLUMNS
    )


def _normalized_keys_sql() -> str:
    return (
        "SELECT company_id, source, slot, suggestion_id, "
        "toString(normalizer_version) AS normalizer_version\n"
        f"FROM {tables.QUALIFIED_NORMALIZED_TABLE} FINAL\n"
        "WHERE company_id IN %(company_ids)s"
    )


def changed_rows_sql() -> str:
    """The page's raw rows that need (re)normalizing: never normalized, computed from an
    older raw version, or computed by another normalizer version. Two branches instead of a
    LEFT JOIN so the result does not depend on join_use_nulls."""
    return (
        f"SELECT {_raw_select('r')}\n"
        f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} AS r FINAL\n"
        f"LEFT ANTI JOIN ({_normalized_keys_sql()}) AS n\n"
        "    ON n.company_id = r.company_id AND n.source = r.source AND n.slot = r.slot\n"
        "WHERE r.company_id IN %(company_ids)s\n"
        "UNION ALL\n"
        f"SELECT {_raw_select('r')}\n"
        f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} AS r FINAL\n"
        f"INNER JOIN ({_normalized_keys_sql()}) AS n\n"
        "    ON n.company_id = r.company_id AND n.source = r.source AND n.slot = r.slot\n"
        "WHERE r.company_id IN %(company_ids)s\n"
        "    AND (n.suggestion_id != r.suggestion_id "
        "OR n.normalizer_version != %(normalizer_version)s)"
    )


def all_rows_sql() -> str:
    return (
        f"SELECT {_raw_select('r')}\n"
        f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} AS r FINAL\n"
        "WHERE r.company_id IN %(company_ids)s"
    )


def changed_scope_sql() -> str:
    """Company ids with at least one raw row that needs normalizing (the whole table)."""
    keys = (
        "SELECT company_id, source, slot, suggestion_id, "
        "toString(normalizer_version) AS normalizer_version\n"
        f"FROM {tables.QUALIFIED_NORMALIZED_TABLE} FINAL"
    )
    return (
        "SELECT DISTINCT company_id FROM (\n"
        f"SELECT r.company_id AS company_id FROM {tables.QUALIFIED_SUGGESTION_TABLE} AS r FINAL\n"
        f"LEFT ANTI JOIN ({keys}) AS n\n"
        "    ON n.company_id = r.company_id AND n.source = r.source AND n.slot = r.slot\n"
        "UNION ALL\n"
        f"SELECT r.company_id AS company_id FROM {tables.QUALIFIED_SUGGESTION_TABLE} AS r FINAL\n"
        f"INNER JOIN ({keys}) AS n\n"
        "    ON n.company_id = r.company_id AND n.source = r.source AND n.slot = r.slot\n"
        "WHERE n.suggestion_id != r.suggestion_id "
        "OR n.normalizer_version != %(normalizer_version)s\n"
        ") AS changed"
    )


def all_scope_sql() -> str:
    return f"SELECT DISTINCT company_id FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL"


def normalized_insert_sql() -> str:
    return (
        f"INSERT INTO {tables.QUALIFIED_NORMALIZED_TABLE} "
        f"({', '.join(tables.NORMALIZED_COLUMNS)}) VALUES"
    )


def normalized_row(raw_row: Sequence[Any], normalized_at: datetime) -> tuple[Any, ...]:
    """One insert tuple in tables.NORMALIZED_COLUMNS order from one raw row in RAW_ROW_COLUMNS
    order. `data` and `role_key` pass through untouched -- the read already coerced `data` to
    a JSON object, and `role_key` is the source's own code, which the maps are keyed on."""
    row = dict(zip(RAW_ROW_COLUMNS, raw_row, strict=True))
    normalized = normalize_person(
        RawPerson(
            source=row["source"],
            full_name=row["full_name"],
            first_name=row["first_name"],
            last_name=row["last_name"],
            birth_year=row["birth_year"],
            wikidata_id=row["wikidata_id"],
            role_original=row["role_original"],
            role_key=row["role_key"],
        )
    )
    values = {
        "company_id": row["company_id"],
        "source": row["source"],
        "slot": row["slot"],
        "suggestion_id": row["suggestion_id"],
        "normalized_id": hashlib.sha256(
            f"{row['suggestion_id']}\n{NORMALIZER_VERSION}".encode()
        ).hexdigest(),
        "normalizer_version": NORMALIZER_VERSION,
        "parse_status": normalized.parse_status,
        "parse_notes": list(normalized.parse_notes),
        "first_tokens": list(normalized.first_tokens),
        "middle_tokens": list(normalized.middle_tokens),
        "last_tokens": list(normalized.last_tokens),
        "display_first": normalized.display_first,
        "display_last": normalized.display_last,
        "display_name": normalized.display_name,
        "birth_year": row["birth_year"],
        "wikidata_id": row["wikidata_id"],
        "role_key": row["role_key"],
        "role_code": normalized.role_code,
        "role_year": row["fiscal_year"],
        "role_from": row["role_from"],
        "role_to": row["role_to"],
        "data": row["data"] or "{}",
        "normalized_at": normalized_at,
    }
    return tuple(values[column] for column in tables.NORMALIZED_COLUMNS)


@dataclass(frozen=True, slots=True)
class NormalizeCounts:
    companies: int
    pages: int
    rows: int
    ok: int
    partial: int
    no_person: int

    def as_metadata(self) -> dict[str, Any]:
        return {
            "companies": self.companies,
            "pages": self.pages,
            "rows": self.rows,
            "ok": self.ok,
            "partial": self.partial,
            "no_person": self.no_person,
            "normalizer_version": NORMALIZER_VERSION,
        }


def _normalize_page(
    client: Any, company_ids: Sequence[str], *, changed_only: bool, normalized_at: datetime
) -> dict[str, int]:
    params = {"company_ids": sorted(company_ids), "normalizer_version": NORMALIZER_VERSION}
    raw_rows = client.execute(
        changed_rows_sql() if changed_only else all_rows_sql(),
        params,
        settings=NORMALIZE_ID_BOUND_QUERY_SETTINGS,
    )
    rows = [normalized_row(raw_row, normalized_at) for raw_row in raw_rows]
    status_index = tables.NORMALIZED_COLUMNS.index("parse_status")
    counts = {status: 0 for status in tables.PARSE_STATUSES}
    for row in rows:
        counts[row[status_index]] += 1
    if rows:
        client.execute(normalized_insert_sql(), rows)
    counts["rows"] = len(rows)
    return counts


def _accumulate(pages: list[dict[str, int]], companies: int) -> NormalizeCounts:
    total = {key: sum(page[key] for page in pages) for key in ("rows", *tables.PARSE_STATUSES)}
    return NormalizeCounts(
        companies=companies, pages=len(pages), rows=total["rows"], ok=total["ok"],
        partial=total["partial"], no_person=total["no_person"],
    )


def normalize_companies(
    client: Any, company_ids: Sequence[str], *, changed_only: bool, normalized_at: datetime,
    page_size: int = PAGE_SIZE, log: Any = None,
) -> NormalizeCounts:
    """Normalize the named companies' raw rows, paged in memory (no scan)."""
    ids = sorted(set(company_ids))
    pages: list[dict[str, int]] = []
    for start in range(0, len(ids), page_size):
        pages.append(
            _normalize_page(
                client, ids[start : start + page_size],
                changed_only=changed_only, normalized_at=normalized_at,
            )
        )
        if log is not None:
            log.info("normalized page %d: %d rows", len(pages), pages[-1]["rows"])
    return _accumulate(pages, len(ids))


def normalize_all(
    client: Any, *, changed_only: bool, normalized_at: datetime,
    page_size: int = PAGE_SIZE, log: Any = None,
) -> NormalizeCounts:
    """Scan the raw table once for the companies that need normalizing, then page them."""
    scope_sql = changed_scope_sql() if changed_only else all_scope_sql()
    pages: list[dict[str, int]] = []
    companies = 0
    for page in scope_pages(
        client, scope_sql=scope_sql, params={"normalizer_version": NORMALIZER_VERSION},
        page_size=page_size, settings=SCAN_QUERY_SETTINGS, prefix=SCRATCH_SCOPE_PREFIX,
    ):
        companies += len(page)
        pages.append(
            _normalize_page(client, page, changed_only=changed_only, normalized_at=normalized_at)
        )
        if log is not None:
            log.info("normalized page %d: %d companies, %d rows", len(pages), len(page), pages[-1]["rows"])
    return _accumulate(pages, companies)
