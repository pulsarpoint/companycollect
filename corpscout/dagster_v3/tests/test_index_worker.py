from datetime import datetime, timezone
from io import BytesIO

import pyarrow.parquet as pq
from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter

from commoncrawl_enrich.classifier import IndustryResult
from index_enrich import worker

RESOLVED = datetime(2026, 6, 23, tzinfo=timezone.utc)


def _rec(uri, html):
    buf = BytesIO()
    w = WARCWriter(buf, gzip=True)
    h = StatusAndHeaders("200 OK", [("Content-Type", "text/html")], protocol="HTTP/1.1")
    w.write_record(w.create_warc_record(uri, "response", payload=BytesIO(html.encode()), http_headers=h))
    return buf.getvalue()


class FakeS3:
    def __init__(self, blobs):  # key -> bytes
        self._blobs = blobs

    def get_object(self, Bucket, Key, Range):
        s, e = Range.removeprefix("bytes=").split("-")
        return {"Body": BytesIO(self._blobs[Key][int(s):int(e) + 1])}


class FakeClassifier:
    def classify(self, texts):
        return [IndustryResult(nace_code="62.01", nace_label="x", nace_division="62",
                               nace_confident=True, method="embedding") for _ in texts]


def test_worker_processes_worklist_to_parquet(tmp_path):
    blob = _rec("http://acme.com/en/", "ACME info@acme.com")
    worklist = [{"root_domain": "acme.com", "url": "http://acme.com/en/",
                 "warc_filename": "w.warc.gz", "warc_record_offset": 0,
                 "warc_record_length": len(blob)}]
    out = tmp_path / "out.parquet"
    stats = worker.run_shard(worklist, s3=FakeS3({"w.warc.gz": blob}),
                             classifier=FakeClassifier(), crawl_id="CC-MAIN-2026-25",
                             out_path=out, resolved_at=RESOLVED)
    assert stats["domains"] == 1 and stats["errors"] == 0 and out.exists()
    assert pq.read_table(out).to_pylist()[0]["nace_code"] == "62.01"


def test_worker_skips_bad_records(tmp_path):
    good = _rec("http://ok.com/", "ok info@ok.com")
    worklist = [
        {"root_domain": "ok.com", "url": "http://ok.com/", "warc_filename": "g.gz",
         "warc_record_offset": 0, "warc_record_length": len(good)},
        {"root_domain": "bad.com", "url": "http://bad.com/", "warc_filename": "missing.gz",
         "warc_record_offset": 0, "warc_record_length": 10},  # KeyError in FakeS3 -> skipped
    ]
    out = tmp_path / "out.parquet"
    stats = worker.run_shard(worklist, s3=FakeS3({"g.gz": good}), classifier=FakeClassifier(),
                             crawl_id="C", out_path=out, resolved_at=RESOLVED)
    assert stats["domains"] == 1 and stats["errors"] == 1
