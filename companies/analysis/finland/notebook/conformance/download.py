"""Download Finland raw source files directly to S3 (first asset).

URL constants live here (no separate urls module). Download functions are
added in Phase 3. Mirrors the existing source clients; copied, not imported.
"""

from __future__ import annotations

import requests

# --- Source URLs (Phase 2) ---------------------------------------------------
PRH_YTJ_COMPANIES_URL = "https://avoindata.prh.fi/opendata-ytj-api/v3/companies"
PRH_YTJ_DESCRIPTION_URL = "https://avoindata.prh.fi/opendata-ytj-api/v3/description"
PRH_XBRL_BASE_URL = "https://avoindata.prh.fi/opendata-xbrl-api/v3"
USER_AGENT = "corpscout-conformance/0.1 (finland)"

# prh_ytj code lists to fetch (code, lang), order per the source catalog.
CODE_LISTS: list[tuple[str, str]] = [
    ("REK", "en"), ("REK_KDI", "en"), ("VIRANOM", "en"), ("TLAJI", "en"),
    ("YRMU", "en"), ("STATUS3", "en"), ("KIELI", "en"),
]


def probe() -> dict[str, int]:
    """Return HTTP status for one probe request per source URL. Confirms the
    endpoints resolve before any bulk download is wired up (Phase 3)."""
    headers = {"User-Agent": USER_AGENT}
    statuses: dict[str, int] = {}
    r = requests.get(PRH_YTJ_COMPANIES_URL, params={"page": 1}, headers=headers, timeout=60)
    statuses["prh_ytj_companies"] = r.status_code
    r = requests.get(
        PRH_YTJ_DESCRIPTION_URL, params={"code": "STATUS3", "lang": "en"},
        headers=headers, timeout=60,
    )
    statuses["prh_ytj_description"] = r.status_code
    r = requests.get(
        f"{PRH_XBRL_BASE_URL}/all_financial_statements",
        params={"registeredDateStart": "2025-01-01", "registeredDateEnd": "2025-01-02", "page": 1},
        headers=headers, timeout=60,
    )
    statuses["prh_xbrl_discovery"] = r.status_code
    return statuses


# --- Phase 3: Download functions (URLs -> raw files in S3) -------------------

import json
import os
import time
from collections.abc import Iterator
from urllib.parse import urlencode

import boto3
from botocore.config import Config

BUCKET = "conformance-finland"
_TIMEOUT = 300
_RETRY_DELAYS = [1, 2, 4, 8]


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["CORPSCOUT_S3_ENDPOINT"],
        aws_access_key_id=os.environ["CORPSCOUT_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["CORPSCOUT_S3_SECRET_KEY"],
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def ensure_bucket(s3) -> None:
    existing = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
    if BUCKET not in existing:
        s3.create_bucket(Bucket=BUCKET)


def _get(session: requests.Session, url: str, params: dict) -> requests.Response:
    for attempt in range(len(_RETRY_DELAYS) + 1):
        try:
            r = session.get(url, params=params, headers={"User-Agent": USER_AGENT}, timeout=_TIMEOUT)
            if r.status_code == 429 or r.status_code >= 500:
                if attempt < len(_RETRY_DELAYS):
                    time.sleep(_RETRY_DELAYS[attempt])
                    continue
            r.raise_for_status()
            return r
        except (requests.ConnectionError, requests.Timeout):
            if attempt == len(_RETRY_DELAYS):
                raise
            time.sleep(_RETRY_DELAYS[attempt])
    raise RuntimeError("unreachable")


def _iter_companies(session: requests.Session) -> Iterator[dict]:
    page, total, seen = 1, None, 0
    while True:
        payload = _get(session, PRH_YTJ_COMPANIES_URL, {"page": page}).json()
        if payload.get("totalResults") is not None:
            total = int(payload["totalResults"])
        companies = payload.get("companies") or []
        for c in companies:
            seen += 1
            yield c
        if (total is not None and seen >= total) or not companies or len(companies) < 100:
            return
        page += 1


def download_prh_ytj(run_id: str, max_companies: int | None = None) -> dict:
    """Download the YTJ company snapshot (NDJSON) + code lists to S3.
    max_companies bounds the reference run; None = full snapshot.
    First asset: URLs -> raw files in S3."""
    s3 = s3_client()
    ensure_bucket(s3)
    session = requests.Session()
    lines: list[bytes] = []
    count = 0
    for company in _iter_companies(session):
        lines.append(json.dumps(company, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
        count += 1
        if max_companies and count >= max_companies:
            break
    snapshot_key = f"runs/{run_id}/source.ndjson"
    s3.put_object(Bucket=BUCKET, Key=snapshot_key, Body=b"\n".join(lines) + b"\n")

    code_list_keys = []
    for code, lang in CODE_LISTS:
        body = _get(session, PRH_YTJ_DESCRIPTION_URL, {"code": code, "lang": lang}).content
        key = f"runs/{run_id}/codelists/{code}.{lang}.tsv"
        s3.put_object(Bucket=BUCKET, Key=key, Body=body)
        code_list_keys.append(key)
    return {"snapshot_key": snapshot_key, "companies": count, "code_list_keys": code_list_keys}


def _xbrl_url(business_id: str, financial_date: str) -> str:
    return f"{PRH_XBRL_BASE_URL}/financial?" + urlencode(
        {"businessId": business_id, "financialDate": financial_date}
    )


def download_prh_xbrl(run_id: str, registered_start: str, registered_end: str) -> dict:
    """Download one registration-month window of XBRL statements to S3, with a
    listing.json mirroring the production raw layer. Bounded sample for the
    reference (one month)."""
    s3 = s3_client()
    ensure_bucket(s3)
    session = requests.Session()
    documents = []
    page = 1
    while True:
        payload = _get(
            session, f"{PRH_XBRL_BASE_URL}/all_financial_statements",
            {"registeredDateStart": registered_start, "registeredDateEnd": registered_end, "page": page},
        ).json()
        items = payload.get("financials", [])
        if not items:
            break
        for item in items:
            bid = str(item.get("businessId") or "").strip()
            fdate = str(item.get("financialDate") or "").strip()
            if not bid or not fdate:
                continue
            body = _get(session, f"{PRH_XBRL_BASE_URL}/financial",
                        {"businessId": bid, "financialDate": fdate}).content
            object_key = f"companies/{bid}/{fdate}.xml"
            s3.put_object(Bucket=BUCKET, Key=object_key, Body=body)
            documents.append({
                "business_id": bid, "financial_date": fdate,
                "registration_date": item.get("registrationDate"),
                "object_key": object_key, "source_url": _xbrl_url(bid, fdate),
            })
        total = int(payload.get("totalResults") or 0)
        if total and page * 100 >= total:
            break
        page += 1
    listing_key = f"windows/{registered_start}/listing.json"
    s3.put_object(
        Bucket=BUCKET, Key=listing_key,
        Body=json.dumps({"documents": documents, "skipped": []}, indent=2).encode("utf-8"),
    )
    return {"listing_key": listing_key, "documents": len(documents)}
