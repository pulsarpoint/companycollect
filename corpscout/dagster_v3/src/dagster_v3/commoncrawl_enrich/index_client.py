import logging

import requests

from dagster_v3.commoncrawl_enrich.models import IndexRecord

LOGGER = logging.getLogger(__name__)

DEFAULT_CRAWL_ID = "CC-MAIN-2025-21"
COLUMNAR_INDEX = "s3://commoncrawl/cc-index/table/cc-main/warc/crawl={crawl}/subset=warc"
CDX_URL = "https://index.commoncrawl.org/{crawl}-index"
USER_AGENT = "corpscout-commoncrawl-enrich/0.1 (goran.raovic@gmail.com)"

# Row shape: (host, path, status, mime, timestamp, filename, offset, length, url)
IndexRow = tuple


def select_best_record(domain: str, rows: list[IndexRow], *, crawl_id: str) -> IndexRecord | None:
    """Pick the latest HTTP-200 HTML homepage ('/') capture for the domain."""
    candidates = [
        r for r in rows
        if str(r[2]) == "200" and "html" in str(r[3]).lower() and str(r[1]) in ("/", "")
    ]
    if not candidates:
        return None
    best = max(candidates, key=lambda r: str(r[4]))  # newest timestamp wins
    return IndexRecord(
        root_domain=domain, warc_filename=str(best[5]), offset=int(best[6]),
        length=int(best[7]), url=str(best[8]), http_status=200, crawl_id=crawl_id,
    )


def resolve_via_duckdb(domains: list[str], *, crawl_id: str, duckdb_con) -> dict[str, IndexRecord]:
    """Resolve homepage records for a batch of domains via the CC columnar Parquet index.

    `duckdb_con` is a duckdb connection with httpfs installed + anonymous S3 access.
    """
    path = COLUMNAR_INDEX.format(crawl=crawl_id)
    placeholders = ", ".join("?" for _ in domains)
    rows = duckdb_con.execute(
        f"""
        select url_host_name, url_path, fetch_status, content_mime_type,
               fetch_time, warc_filename, warc_record_offset, warc_record_length, url
        from read_parquet('{path}/*.parquet', hive_partitioning=1)
        where url_host_name in ({placeholders}) and url_path in ('/', '')
        """,
        domains,
    ).fetchall()
    grouped: dict[str, list[IndexRow]] = {}
    for row in rows:
        host = str(row[0]).removeprefix("www.")
        grouped.setdefault(host, []).append(row)
    out: dict[str, IndexRecord] = {}
    for domain in domains:
        rec = select_best_record(domain, grouped.get(domain, []), crawl_id=crawl_id)
        if rec is not None:
            out[domain] = rec
    return out


def resolve_via_cdx(domain: str, *, crawl_id: str, session: requests.Session | None = None) -> IndexRecord | None:
    """Per-domain fallback using the public CDX API (no AWS)."""
    http = session or requests.Session()
    response = http.get(
        CDX_URL.format(crawl=crawl_id),
        params={"url": f"{domain}/", "output": "json", "filter": "status:200", "limit": 20},
        headers={"User-Agent": USER_AGENT}, timeout=60,
    )
    if response.status_code != 200 or not response.text.strip():
        return None
    rows: list[IndexRow] = []
    for line in response.text.splitlines():
        import json
        rec = json.loads(line)
        host = (rec.get("url", "").split("/")[2] if "://" in rec.get("url", "") else domain).removeprefix("www.")
        rows.append((host, "/", rec.get("status", ""), rec.get("mime", ""), rec.get("timestamp", ""),
                     rec.get("filename", ""), rec.get("offset", 0), rec.get("length", 0), rec.get("url", "")))
    return select_best_record(domain, rows, crawl_id=crawl_id)
