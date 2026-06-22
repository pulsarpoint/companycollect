"""Naive direct processing of one CommonCrawl file into ClickHouse.

Two functions, one per file type, each: stream the file, extract per the file's role,
and append rows to the corpscout output tables (ReplacingMergeTree, deduped on
root_domain/url/crawl_id). `ch_client` is anything with `.execute(sql, rows)`
(e.g. a clickhouse_driver Client / dagster_clickhouse connection).

- WET  -> commoncrawl_domains          (one row per homepage: emails + page_type + NACE + top-3 audit)
- WARC -> commoncrawl_technologies     (one row per page x technology, page->website->domain keys)
        + commoncrawl_page_signals     (one row per page: emails + social platforms)
"""
from datetime import datetime, timezone

from warcio.archiveiterator import ArchiveIterator

from commoncrawl_enrich import extract, segment, tech
from commoncrawl_enrich.classifier import PageClassifier

DOMAINS_TABLE = "corpscout.commoncrawl_domains"
TECHNOLOGIES_TABLE = "corpscout.commoncrawl_technologies"
PAGE_SIGNALS_TABLE = "corpscout.commoncrawl_page_signals"

DOMAINS_COLUMNS = (
    "crawl_id", "url", "root_domain", "subdomain", "emails", "email_count",
    "page_type", "page_type_score", "nace_code", "nace_label", "nace_division",
    "nace_confident", "nace_margin", "nace_score", "nace_method",
    "nace_top3_codes", "nace_top3_labels", "nace_top3_scores",
    "source_url", "source_run_id", "resolved_at",
)
TECHNOLOGIES_COLUMNS = (
    "crawl_id", "url", "root_domain", "subdomain", "technology", "category",
    "version", "confidence", "source_url", "source_run_id", "resolved_at",
)
PAGE_SIGNALS_COLUMNS = (
    "crawl_id", "url", "root_domain", "subdomain", "emails", "social_platforms",
    "source_url", "source_run_id", "resolved_at",
)


def _insert(ch_client, table: str, columns, rows: list, batch_size: int) -> int:
    if not rows:
        return 0
    col_sql = ", ".join(columns)
    for start in range(0, len(rows), batch_size):
        ch_client.execute(f"INSERT INTO {table} ({col_sql}) VALUES", rows[start:start + batch_size])
    return len(rows)


def process_wet_to_clickhouse(
    source: str, *, classifier: PageClassifier, ch_client, crawl_id: str,
    source_url: str | None = None, source_run_id: str = "", resolved_at: datetime | None = None,
    limit: int | None = None, session=None, homepages_only: bool = True, batch_size: int = 2000,
) -> dict:
    """Process one WET file -> append homepage rows (industry + emails) to commoncrawl_domains."""
    resolved_at = resolved_at or datetime.now(timezone.utc)
    source_url = source_url if source_url is not None else (
        source if str(source).startswith(("http://", "https://")) else "")
    stream = segment._open_stream(source, session)
    recs: list[tuple] = []
    try:
        for record in ArchiveIterator(stream):
            if record.rec_type != "conversion":
                continue
            uri = record.rec_headers.get_header("WARC-Target-URI") or ""
            if homepages_only and not segment._is_homepage(uri):
                continue
            text = record.content_stream().read().decode("utf-8", "replace")
            root, sub = segment._host(uri)
            emails = [e.email for e in extract.extract_emails(text)]
            recs.append((uri, root, sub, emails, text))
            if limit and len(recs) >= limit:
                break
    finally:
        stream.close()

    results = classifier.classify([r[4] for r in recs]) if recs else []
    rows = [
        (crawl_id, uri, root, sub, emails, len(emails),
         res.page_type, float(res.page_type_score),
         res.nace_code, res.nace_label, res.nace_division,
         int(res.nace_confident), float(res.nace_margin), float(res.nace_score), res.method,
         res.nace_top3, res.nace_top3_labels, [float(s) for s in res.nace_top3_scores],
         source_url, source_run_id, resolved_at)
        for (uri, root, sub, emails, _text), res in zip(recs, results)
    ]
    _insert(ch_client, DOMAINS_TABLE, DOMAINS_COLUMNS, rows, batch_size)
    return {
        "records": len(rows),
        "with_email": sum(1 for r in rows if r[4]),
        "page_types": sum(1 for r in rows if r[6]),
        "industries": sum(1 for r in rows if r[8]),
    }


def process_warc_to_clickhouse(
    source: str, *, ch_client, crawl_id: str, source_url: str | None = None,
    source_run_id: str = "", resolved_at: datetime | None = None, limit: int | None = None,
    session=None, batch_size: int = 2000,
) -> dict:
    """Process one WARC file -> append per-page technologies + page signals (emails/socials)."""
    resolved_at = resolved_at or datetime.now(timezone.utc)
    source_url = source_url if source_url is not None else (
        source if str(source).startswith(("http://", "https://")) else "")
    stream = segment._open_stream(source, session)
    signal_rows: list[tuple] = []
    tech_rows: list[tuple] = []
    pages = 0
    try:
        for record in ArchiveIterator(stream):
            if record.rec_type != "response":
                continue
            http = record.http_headers
            content_type = (http.get_header("Content-Type") or "").lower() if http else ""
            if "html" not in content_type:
                continue
            html = record.content_stream().read().decode("utf-8", "replace")
            uri = record.rec_headers.get_header("WARC-Target-URI") or ""
            root, sub = segment._host(uri)
            parsed = extract.parse_html(html)
            headers = {k: v for k, v in (http.headers if http else [])}
            emails = [e.email for e in extract.extract_emails(html)]
            socials = sorted({s.platform for s in extract.extract_socials(parsed.links)})
            signal_rows.append((crawl_id, uri, root, sub, emails, socials,
                                source_url, source_run_id, resolved_at))
            for t in tech.detect_technologies(html, headers):
                tech_rows.append((crawl_id, uri, root, sub, t.technology, t.category,
                                  t.version, int(t.confidence), source_url, source_run_id, resolved_at))
            pages += 1
            if limit and pages >= limit:
                break
    finally:
        stream.close()

    _insert(ch_client, TECHNOLOGIES_TABLE, TECHNOLOGIES_COLUMNS, tech_rows, batch_size)
    _insert(ch_client, PAGE_SIGNALS_TABLE, PAGE_SIGNALS_COLUMNS, signal_rows, batch_size)
    return {
        "pages": pages,
        "technologies": len(tech_rows),
        "with_social": sum(1 for r in signal_rows if r[5]),
        "with_email": sum(1 for r in signal_rows if r[4]),
    }
