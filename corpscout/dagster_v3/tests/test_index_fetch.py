from io import BytesIO

from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter

from index_enrich import fetch


def _one_record_bytes(uri, html):
    buf = BytesIO()
    w = WARCWriter(buf, gzip=True)
    headers = StatusAndHeaders("200 OK", [("Content-Type", "text/html"), ("Server", "nginx")],
                               protocol="HTTP/1.1")
    w.write_record(w.create_warc_record(uri, "response", payload=BytesIO(html.encode()),
                                        http_headers=headers))
    return buf.getvalue()


class FakeS3:
    def __init__(self, data):
        self._data = data
        self.ranges = []

    def get_object(self, Bucket, Key, Range):
        self.ranges.append((Bucket, Key, Range))
        start, end = Range.removeprefix("bytes=").split("-")
        return {"Body": BytesIO(self._data[int(start):int(end) + 1])}


def test_fetch_warc_record_parses_html_and_headers():
    blob = _one_record_bytes("http://acme.com/en/", "<html><body>hi wp-content</body></html>")
    s3 = FakeS3(blob)
    html, headers = fetch.fetch_warc_record(s3, "crawl-data/x.warc.gz", 0, len(blob))
    assert "wp-content" in html and headers.get("Server") == "nginx"
    assert s3.ranges[0] == ("commoncrawl", "crawl-data/x.warc.gz", f"bytes=0-{len(blob)-1}")
