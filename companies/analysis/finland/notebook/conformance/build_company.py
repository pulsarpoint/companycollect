"""structured prh_ytj tables -> canonical `registrations` and `company`.

Pure: structured DataFrames in, canonical DataFrame out. Future Dagster assets.
Finland is single-key (business_id); company_uid is the surrogate
"c:" + sha1("FI:" + business_id) per the contract (LEI absent in YTJ open data).
"""

from __future__ import annotations

import datetime as dt
import hashlib

import polars as pl

COUNTRY = "FI"
RESOLUTION_VERSION = "finland-v1"


def _registration_uid(business_id: str) -> str:
    return f"{COUNTRY}:{business_id}"


def _company_uid(business_id: str) -> str:
    return "c:" + hashlib.sha1(f"{COUNTRY}:{business_id}".encode()).hexdigest()


def build_registrations(structured: dict[str, pl.DataFrame], *, run_id: str, now: dt.datetime) -> pl.DataFrame:
    statuses = structured["fi_prhytj_statuses"]
    names = structured["fi_prhytj_names"]
    websites = structured.get("fi_prhytj_websites", pl.DataFrame())
    addresses = structured.get("fi_prhytj_addresses", pl.DataFrame())
    lines = structured.get("fi_prhytj_business_lines", pl.DataFrame())

    rows = []
    for s in statuses.iter_rows(named=True):
        bid = s["business_id"]
        primary_name = _current_primary(names, bid, "name", "name_type_code")
        website = _first(websites, bid, "normalized_url")
        addr = _primary_address(addresses, bid)
        line = _first_row(lines, bid)
        rows.append({
            "registration_uid": _registration_uid(bid),
            "company_uid": _company_uid(bid),
            "country": COUNTRY,
            "registration_number": bid,
            "registry_source": "finland_prhytj",
            "is_primary": 1,
            "entity_role": "domestic",
            "legal_name": primary_name,
            "legal_form_code": None,
            "lifecycle_status": s.get("lifecycle_status") or "unknown",
            "is_active": 1 if s.get("is_active") else 0,
            "incorporation_date": _date(s.get("registration_date")),
            "dissolution_date": _date(s.get("end_date")),
            "addr_street": addr.get("street"),
            "addr_post_code": addr.get("post_code"),
            "addr_city": addr.get("city"),
            "addr_municipality_code": addr.get("municipality_code"),
            "addr_country": addr.get("country") or COUNTRY,
            "activity_code": (line or {}).get("business_line_type"),
            "activity_scheme": (line or {}).get("business_line_code_set"),
            "vat_number": None,
            "eu_id": None,
            "lei": None,
            "primary_website": website,
            "source_run_id": run_id,
            "ingested_at": now,
            "updated_at": now,
        })
    from conformance.schemas import REGISTRATIONS
    return pl.DataFrame(rows, schema=REGISTRATIONS)


def build_company(registrations: pl.DataFrame, *, now: dt.datetime) -> pl.DataFrame:
    rows = []
    for company_uid, group in _group_by(registrations, "company_uid"):
        primary = group[0]
        rows.append({
            "company_uid": company_uid,
            "uid_scheme": "surrogate",
            "lei": None,
            "primary_name": primary["legal_name"],
            "status": "active" if any(r["is_active"] for r in group) else "inactive",
            "legal_form_code": primary["legal_form_code"],
            "home_country": COUNTRY,
            "incorporation_date": primary["incorporation_date"],
            "dissolution_date": primary["dissolution_date"],
            "registration_count": len(group),
            "operating_countries": sorted({r["country"] for r in group}),
            "primary_website": primary["primary_website"],
            "sources": ["finland_prhytj"],
            "resolution_version": RESOLUTION_VERSION,
            "first_seen_at": now,
            "updated_at": now,
        })
    from conformance.schemas import COMPANY
    return pl.DataFrame(rows, schema=COMPANY)


# --- helpers ---------------------------------------------------------------
def _date(value) -> dt.date | None:
    if not value:
        return None
    try:
        return dt.date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _current_primary(df: pl.DataFrame, bid: str, value_col: str, type_col: str) -> str | None:
    if df.is_empty():
        return None
    sub = df.filter(pl.col("business_id") == bid)
    if sub.is_empty():
        return None
    primary = sub.filter(pl.col(type_col) == "1") if type_col in sub.columns else sub
    chosen = primary if not primary.is_empty() else sub
    return chosen[value_col].to_list()[0]


def _first(df: pl.DataFrame, bid: str, col: str):
    if df.is_empty():
        return None
    sub = df.filter(pl.col("business_id") == bid)
    return sub[col].to_list()[0] if not sub.is_empty() else None


def _first_row(df: pl.DataFrame, bid: str) -> dict | None:
    if df.is_empty():
        return None
    sub = df.filter(pl.col("business_id") == bid)
    return sub.row(0, named=True) if not sub.is_empty() else None


def _primary_address(df: pl.DataFrame, bid: str) -> dict:
    row = _first_row(df, bid)
    return row or {}


def _group_by(df: pl.DataFrame, key: str):
    for value in df[key].unique().to_list():
        yield value, list(df.filter(pl.col(key) == value).iter_rows(named=True))
