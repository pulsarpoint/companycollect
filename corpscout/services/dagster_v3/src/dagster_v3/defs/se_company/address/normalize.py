"""The normalize step (spec section 4): raw suggestion rows whose normalized row is missing,
older than the raw version, or on an older normalizer are normalized in Python and written
as new versions of se_company_address_normalized. One function per country; only Sweden."""

import hashlib
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from dagster_v3.defs.se_company.address import tables
from dagster_v3.defs.se_company.address.normalize_se import (
    NORMALIZER_VERSION,
    NormalizedAddress,
    RawAddress,
    address_key,
    normalize_se_address,
)
from dagster_v3.defs.se_company.basic_info.extract import SCAN_QUERY_SETTINGS, scope_pages

PAGE_SIZE = 20_000

# changed_rows_sql() binds %(company_ids)s four times (two UNION ALL branches, each with the
# outer WHERE plus the nested _normalized_keys_sql()), so a full PAGE_SIZE page of 12-digit
# ids renders far larger than basic-info's per-page queries -- past even basic-info's raised
# ID_BOUND_QUERY_SETTINGS (1,048,576 bytes: ~1.28 MB at PAGE_SIZE=20,000), which is why this
# module has its own, wider setting instead of importing that one. See
# tests/test_se_company_address_normalize.py for the measured render size.
NORMALIZE_ID_BOUND_QUERY_SETTINGS = {"max_query_size": 4_194_304, "max_execution_time": 1800}

# This module's own scratch-table prefix (see basic_info/extract.py:scope_pages), so an
# address scan's scratch table can never collide with a basic-info one.
SCRATCH_SCOPE_PREFIX = "corpscout._tmp_address_scope_"

RAW_ROW_COLUMNS: tuple[str, ...] = (
    "company_id", "source", "slot", "suggestion_id", "suggested_at", "kind", *tables.RAW_ADDRESS_COLUMNS,
)


def normalize_address(raw: RawAddress) -> NormalizedAddress:
    """The per-country dispatcher of spec section 4. Only Sweden exists; every SE company's
    addresses go through the Swedish rules, and the rules themselves decide `foreign`."""
    return normalize_se_address(raw)


def _raw_select(alias: str) -> str:
    return ", ".join(f"{alias}.{column} AS {column}" for column in RAW_ROW_COLUMNS)


def _normalized_keys_sql() -> str:
    return (
        "SELECT company_id, source, slot, suggested_at, toString(normalizer_version) AS normalizer_version\n"
        f"FROM {tables.QUALIFIED_NORMALIZED_TABLE} FINAL\n"
        "WHERE company_id IN %(company_ids)s"
    )


def changed_rows_sql() -> str:
    """The page's raw rows that need (re)normalizing: never normalized, raw newer than the
    normalized row, or normalized on another normalizer version. Two branches instead of a
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
        "    AND (r.suggested_at > n.suggested_at OR n.normalizer_version != %(normalizer_version)s)"
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
        "SELECT company_id, source, slot, suggested_at, toString(normalizer_version) AS normalizer_version\n"
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
        "WHERE r.suggested_at > n.suggested_at OR n.normalizer_version != %(normalizer_version)s\n"
        ") AS changed"
    )


def all_scope_sql() -> str:
    return f"SELECT DISTINCT company_id FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL"


def normalized_insert_sql() -> str:
    return f"INSERT INTO {tables.QUALIFIED_NORMALIZED_TABLE} ({', '.join(tables.NORMALIZED_COLUMNS)}) VALUES"


def clickhouse_stamp(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%d %H:%M:%S.") + f"{moment.microsecond // 1000:03d}"


def normalized_row(raw_row: Sequence[Any], normalized_at: datetime) -> tuple[Any, ...]:
    """One insert tuple in tables.NORMALIZED_COLUMNS order from one raw row in RAW_ROW_COLUMNS order."""
    row = dict(zip(RAW_ROW_COLUMNS, raw_row, strict=True))
    raw = RawAddress(**{column: row[column] for column in tables.RAW_ADDRESS_COLUMNS})
    normalized = normalize_address(raw)
    normalized_id = hashlib.sha256(
        f"{row['company_id']}\n{row['source']}\n{row['slot']}\n{clickhouse_stamp(normalized_at)}".encode()
    ).hexdigest()
    values = {
        "company_id": row["company_id"],
        "source": row["source"],
        "slot": row["slot"],
        "normalized_id": normalized_id,
        "suggestion_id": row["suggestion_id"],
        "suggested_at": row["suggested_at"],
        "kind": row["kind"],
        "care_of": normalized.care_of,
        "box": normalized.box,
        "street_name": normalized.street_name,
        "house_number": normalized.house_number,
        "unit": normalized.unit,
        "postal_code": normalized.postal_code,
        "city": normalized.city,
        "country_code": normalized.country_code,
        "normalized_address": normalized.normalized_address,
        "address_key": address_key(normalized),
        "parse_status": normalized.parse_status,
        "parse_notes": normalized.parse_notes,
        "normalizer_version": NORMALIZER_VERSION,
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
    no_address: int
    foreign: int

    def as_metadata(self) -> dict[str, Any]:
        return {
            "companies": self.companies,
            "pages": self.pages,
            "rows": self.rows,
            "ok": self.ok,
            "partial": self.partial,
            "no_address": self.no_address,
            "foreign": self.foreign,
            "normalizer_version": NORMALIZER_VERSION,
        }


def _normalize_page(client: Any, company_ids: Sequence[str], *, changed_only: bool, normalized_at: datetime) -> dict[str, int]:
    params = {"company_ids": sorted(company_ids), "normalizer_version": NORMALIZER_VERSION}
    raw_rows = client.execute(
        changed_rows_sql() if changed_only else all_rows_sql(), params, settings=NORMALIZE_ID_BOUND_QUERY_SETTINGS
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
        partial=total["partial"], no_address=total["no_address"], foreign=total["foreign"],
    )


def normalize_companies(
    client: Any, company_ids: Sequence[str], *, changed_only: bool, normalized_at: datetime,
    page_size: int = PAGE_SIZE, log: Any = None,
) -> NormalizeCounts:
    """Normalize the named companies' raw rows, paged in memory (no scan)."""
    ids = sorted(set(company_ids))
    pages = []
    for start in range(0, len(ids), page_size):
        page = ids[start : start + page_size]
        pages.append(_normalize_page(client, page, changed_only=changed_only, normalized_at=normalized_at))
        if log is not None:
            log.info("normalized page %d: %d rows", len(pages), pages[-1]["rows"])
    return _accumulate(pages, len(ids))


def normalize_all(
    client: Any, *, changed_only: bool, normalized_at: datetime, page_size: int = PAGE_SIZE, log: Any = None,
) -> NormalizeCounts:
    """Scan the raw table once for the companies that need normalizing, then page them."""
    scope_sql = changed_scope_sql() if changed_only else all_scope_sql()
    pages = []
    companies = 0
    for page in scope_pages(client, scope_sql=scope_sql, params={"normalizer_version": NORMALIZER_VERSION},
                            page_size=page_size, settings=SCAN_QUERY_SETTINGS, prefix=SCRATCH_SCOPE_PREFIX):
        companies += len(page)
        pages.append(_normalize_page(client, page, changed_only=changed_only, normalized_at=normalized_at))
        if log is not None:
            log.info("normalized page %d: %d companies, %d rows", len(pages), len(page), pages[-1]["rows"])
    return _accumulate(pages, companies)
