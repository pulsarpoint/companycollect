"""Offline tests for direct WET/WARC -> ClickHouse ingestion (warcio fixtures, fake CH)."""
from datetime import datetime, timezone
from io import BytesIO

from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter

from commoncrawl_enrich import ingest
from commoncrawl_enrich.classifier import IndustryResult
from commoncrawl_enrich.models import Technology

RESOLVED = datetime(2026, 6, 23, tzinfo=timezone.utc)


class FakeCH:
    """Captures (table, columns, rows) per INSERT."""

    def __init__(self):
        self.inserts: list[tuple[str, list]] = []

    def execute(self, sql, rows=None):
        self.inserts.append((sql, rows))

    def rows_for(self, table: str) -> list:
        out: list = []
        for sql, rows in self.inserts:
            if table in sql:
                out.extend(rows or [])
        return out


class FakeWappalyzer:
    """Returns WordPress for any page whose body contains 'wp-content'."""

    def analyze_batch(self, items):
        out = {}
        for key, _headers, body in items:
            out[key] = ([Technology(technology="WordPress", category="CMS", version="6.1",
                                     confidence=100)] if "wp-content" in body else [])
        return out


class FakeClassifier:
    def classify(self, texts):
        return [IndustryResult(
            nace_code="62.01", nace_label="Programming", nace_division="62",
            nace_confident=True, nace_score=0.8, nace_margin=0.2,
            nace_top3=["62.01", "62.09", "47.11"], nace_top3_labels=["a", "b", "c"],
            nace_top3_scores=[0.8, 0.5, 0.3], method="embedding",
        ) for _ in texts]


def _write_wet(path, pages):
    with open(path, "wb") as fh:
        writer = WARCWriter(fh, gzip=True)
        for uri, text in pages:
            rec = writer.create_warc_record(
                uri, "conversion", payload=BytesIO(text.encode()),
                warc_content_type="text/plain")
            writer.write_record(rec)


def _write_warc(path, pages):
    with open(path, "wb") as fh:
        writer = WARCWriter(fh, gzip=True)
        for uri, html in pages:
            headers = StatusAndHeaders("200 OK", [("Content-Type", "text/html")],
                                       protocol="HTTP/1.0")
            rec = writer.create_warc_record(
                uri, "response", payload=BytesIO(html.encode()), http_headers=headers)
            writer.write_record(rec)


def test_process_wet_to_clickhouse(tmp_path):
    wet = tmp_path / "x.warc.wet.gz"
    _write_wet(wet, [
        ("http://acme.com/", "ACME makes software. Contact info@acme.com"),
        ("http://acme.com/about", "deep page, not a homepage"),  # filtered (homepages_only)
        ("http://shop.example.org/", "online shop selling things"),
    ])
    ch = FakeCH()
    stats = ingest.process_wet_to_clickhouse(
        str(wet), classifier=FakeClassifier(), ch_client=ch, crawl_id="CC-MAIN-2026-25",
        source_url="http://data/x.wet.gz", source_run_id="run1", resolved_at=RESOLVED)
    rows = ch.rows_for(ingest.DOMAINS_TABLE)
    assert stats["records"] == 2  # only the two homepages
    assert all(len(r) == len(ingest.DOMAINS_COLUMNS) for r in rows)
    by_domain = {r[2]: r for r in rows}
    acme = by_domain["acme.com"]
    assert acme[1] == "http://acme.com/" and acme[4] == ["info@acme.com"] and acme[5] == 1
    assert acme[8] == "62.01" and acme[14] == "embedding"  # nace_code, nace_method
    assert acme[15] == ["62.01", "62.09", "47.11"]          # top3 codes audit log
    assert acme[20] == RESOLVED                              # resolved_at


def test_process_warc_to_clickhouse_writes_tech_and_signals(tmp_path):
    warc = tmp_path / "x.warc.gz"
    _write_warc(warc, [
        ("http://acme.com/", "<html><head><link href='wp-content/style.css'></head>"
                             "<body><a href='https://facebook.com/acme'>fb</a> "
                             "mailto:hi@acme.com</body></html>"),
        ("http://acme.com/contact", "<html><body>plain page, no tech</body></html>"),
    ])
    ch = FakeCH()
    stats = ingest.process_warc_to_clickhouse(
        str(warc), ch_client=ch, crawl_id="CC-MAIN-2026-25", wappalyzer=FakeWappalyzer(),
        source_url="http://data/x.warc.gz", source_run_id="run1", resolved_at=RESOLVED)
    tech_rows = ch.rows_for(ingest.TECHNOLOGIES_TABLE)
    signal_rows = ch.rows_for(ingest.PAGE_SIGNALS_TABLE)
    assert stats["pages"] == 2
    assert all(len(r) == len(ingest.TECHNOLOGIES_COLUMNS) for r in tech_rows)
    assert all(len(r) == len(ingest.PAGE_SIGNALS_COLUMNS) for r in signal_rows)
    # WordPress detected on the homepage, normalized one row per page x tech
    wp = [r for r in tech_rows if r[4] == "WordPress"]
    assert len(wp) == 1 and wp[0][1] == "http://acme.com/" and wp[0][2] == "acme.com"
    assert len(signal_rows) == 2  # one per page
    home = next(r for r in signal_rows if r[1] == "http://acme.com/")
    assert "facebook" in home[5]  # social_platforms


def test_empty_file_inserts_nothing(tmp_path):
    wet = tmp_path / "empty.warc.wet.gz"
    _write_wet(wet, [])
    ch = FakeCH()
    stats = ingest.process_wet_to_clickhouse(
        str(wet), classifier=FakeClassifier(), ch_client=ch, crawl_id="c", resolved_at=RESOLVED)
    assert stats["records"] == 0 and ch.inserts == []
