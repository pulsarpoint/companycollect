import json

from commoncrawl_enrich import index_client
from commoncrawl_enrich.models import IndexRecord


class _FakeResp:
    def __init__(self, status, text=""):
        self.status_code = status
        self.text = text


class _FakeCdxSession:
    """Returns the given status sequence; only the 200 carries the CDX body."""

    def __init__(self, statuses, body):
        self.statuses = list(statuses)
        self.body = body
        self.calls = 0

    def get(self, url, **kwargs):
        status = self.statuses[min(self.calls, len(self.statuses) - 1)]
        self.calls += 1
        return _FakeResp(status, self.body if status == 200 else "")


def test_resolve_via_cdx_retries_503_then_succeeds():
    body = json.dumps({"url": "https://firma.sk/", "status": "200", "mime": "text/html",
                       "timestamp": "20250501", "filename": "f.warc.gz", "offset": 10, "length": 20})
    sess = _FakeCdxSession([503, 503, 200], body)
    rec = index_client.resolve_via_cdx("firma.sk", crawl_id="c", session=sess, backoff_seconds=0)
    assert rec is not None and rec.warc_filename == "f.warc.gz" and rec.offset == 10
    assert sess.calls == 3


def test_resolve_via_cdx_404_is_a_clean_miss():
    sess = _FakeCdxSession([404], "")
    assert index_client.resolve_via_cdx("x.sk", crawl_id="c", session=sess, backoff_seconds=0) is None
    assert sess.calls == 1


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
