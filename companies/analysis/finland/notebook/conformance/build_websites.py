"""structured prh_ytj websites -> canonical `company_websites`.

Registry-provided sites are scope='registration', source_kind='registry',
confidence 1.0. corpscout-discovered sites are out of scope here.
"""

from __future__ import annotations

import datetime as dt
import hashlib

import polars as pl

COUNTRY = "FI"


def _company_uid(business_id: str) -> str:
    return "c:" + hashlib.sha1(f"{COUNTRY}:{business_id}".encode()).hexdigest()


def build_websites(websites: pl.DataFrame, *, run_id: str, now: dt.datetime) -> pl.DataFrame:
    rows = []
    for w in websites.iter_rows(named=True):
        bid = w["business_id"]
        reg_uid = f"{COUNTRY}:{bid}"
        normalized = w.get("normalized_url") or w.get("url") or ""
        if not normalized:
            continue
        rows.append({
            "website_uid": hashlib.sha1(f"registration:{reg_uid}:{normalized}".encode()).hexdigest(),
            "company_uid": _company_uid(bid),
            "registration_uid": reg_uid,
            "country": COUNTRY,
            "scope": "registration",
            "url": w.get("url") or normalized,
            "normalized_url": normalized,
            "host": w.get("host") or "",
            "is_primary": 1 if w.get("is_primary") else 0,
            "source_kind": "registry",
            "discovery_method": "registry_field",
            "registry_source": "finland_prhytj",
            "confidence": 1.0,
            "is_live": 0,
            "first_seen_at": now,
            "last_seen_at": now,
            "updated_at": now,
        })
    from conformance.schemas import COMPANY_WEBSITES
    return pl.DataFrame(rows, schema=COMPANY_WEBSITES)
