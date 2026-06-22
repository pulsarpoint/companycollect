import gzip
import io

from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter

from commoncrawl_enrich import warc
from commoncrawl_enrich.models import IndexRecord


def _make_warc_record_bytes() -> bytes:
    buf = io.BytesIO()
    writer = WARCWriter(buf, gzip=True)
    payload = b"<html lang='sk'><title>Firma</title></html>"
    http_headers = StatusAndHeaders(
        "200 OK", [("Content-Type", "text/html"), ("Server", "nginx")], protocol="HTTP/1.1")
    record = writer.create_warc_record(
        "https://firma.sk/", "response",
        payload=io.BytesIO(payload), length=len(payload), http_headers=http_headers)
    writer.write_record(record)
    return buf.getvalue()


def test_parse_warc_record_extracts_html_status_headers():
    rec = IndexRecord(root_domain="firma.sk", warc_filename="f.warc.gz", offset=0, length=0,
                      url="https://firma.sk/", http_status=200, crawl_id="CC-MAIN-2025-21")
    page = warc.parse_warc_record(_make_warc_record_bytes(), rec)
    assert page is not None
    assert page.http_status == 200 and "Firma" in page.html
    assert page.headers.get("Server", "").lower() == "nginx"
    assert page.root_domain == "firma.sk"


def test_parse_warc_record_non_html_returns_none():
    rec = IndexRecord(root_domain="x.sk", warc_filename="f", offset=0, length=0,
                      url="https://x.sk/", http_status=200, crawl_id="c")
    # gzip of empty/garbage -> not a WARC record -> None
    assert warc.parse_warc_record(gzip.compress(b"not a warc"), rec) is None
