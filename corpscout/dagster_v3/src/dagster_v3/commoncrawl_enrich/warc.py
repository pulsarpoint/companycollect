import io
import logging

import requests
from warcio.archiveiterator import ArchiveIterator

from dagster_v3.commoncrawl_enrich.models import FetchedPage, IndexRecord

LOGGER = logging.getLogger(__name__)

DATA_HOST = "https://data.commoncrawl.org"
USER_AGENT = "corpscout-commoncrawl-enrich/0.1 (goran.raovic@gmail.com)"


def parse_warc_record(raw_gzip_bytes: bytes, record: IndexRecord) -> FetchedPage | None:
    """Parse a single (gzipped) WARC response record into a FetchedPage; None if not HTML."""
    try:
        for warc_record in ArchiveIterator(io.BytesIO(raw_gzip_bytes)):
            if warc_record.rec_type != "response":
                continue
            http = warc_record.http_headers
            content_type = (http.get_header("Content-Type") or "").lower() if http else ""
            if "html" not in content_type:
                return None
            status = int((http.get_statuscode() or "0")) if http else 0
            headers = {k: v for k, v in (http.headers if http else [])}
            body = warc_record.content_stream().read()
            html = body.decode("utf-8", "replace")
            capture = warc_record.rec_headers.get_header("WARC-Date") or ""
            return FetchedPage(
                root_domain=record.root_domain, final_url=record.url, http_status=status,
                headers=headers, html=html, capture_date=capture[:10], crawl_id=record.crawl_id,
            )
    except Exception as exc:  # noqa: BLE001 - malformed record -> treat as miss
        LOGGER.debug("WARC parse failed for %s: %s", record.root_domain, exc)
        return None
    return None


def fetch_page(record: IndexRecord, *, session: requests.Session | None = None,
               timeout_seconds: int = 60) -> FetchedPage | None:
    """Byte-range GET the single WARC record from the free CloudFront mirror, then parse it."""
    http = session or requests.Session()
    end = record.offset + record.length - 1
    response = http.get(
        f"{DATA_HOST}/{record.warc_filename}",
        headers={"User-Agent": USER_AGENT, "Range": f"bytes={record.offset}-{end}"},
        timeout=timeout_seconds,
    )
    if response.status_code not in (200, 206) or not response.content:
        return None
    return parse_warc_record(response.content, record)
