from dagster_v3.commoncrawl_enrich import index_client
from dagster_v3.commoncrawl_enrich.models import IndexRecord


def test_select_best_record_prefers_latest_200_html_homepage():
    rows = [
        # (host, path, status, mime, timestamp, filename, offset, length, url)
        ("firma.sk", "/", "200", "text/html", "20240101000000", "f1.warc.gz", 10, 100, "https://firma.sk/"),
        ("firma.sk", "/", "200", "text/html", "20250501000000", "f2.warc.gz", 20, 200, "https://firma.sk/"),
        ("firma.sk", "/kontakt", "200", "text/html", "20250601000000", "f3.warc.gz", 0, 50, "https://firma.sk/kontakt"),
        ("firma.sk", "/", "301", "text/html", "20250701000000", "f4.warc.gz", 0, 50, "https://firma.sk/"),
    ]
    rec = index_client.select_best_record("firma.sk", rows, crawl_id="CC-MAIN-2025-21")
    assert isinstance(rec, IndexRecord)
    assert rec.warc_filename == "f2.warc.gz" and rec.offset == 20 and rec.length == 200


def test_select_best_record_none_when_no_usable_capture():
    rows = [("x.sk", "/", "404", "text/html", "20250101000000", "f.warc.gz", 0, 1, "https://x.sk/")]
    assert index_client.select_best_record("x.sk", rows, crawl_id="CC-MAIN-2025-21") is None
