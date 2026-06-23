# CommonCrawl Index-Driven Domain Enrichment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enrich every CommonCrawl domain with industry (+ homepage emails + technologies) by querying the columnar URL index for one representative page per domain, byte-range-fetching just those records, and classifying them — no WET/WARC whole-file processing.

**Architecture:** A worklist query (DuckDB over the index parquet parts off-AWS, or Athena on AWS) emits one row per domain `(root_domain, url, warc_filename, offset, length)`. Workers byte-range-GET each WARC record, parse it with `warcio`, and reuse the existing `classifier`/`tech`/`extract` to produce `commoncrawl_domains` (+ `commoncrawl_technologies`) rows → Parquet. Validate on 10k domains, then scale on AWS `us-east-1`.

**Tech Stack:** Python 3.14, `uv`, DuckDB (+ httpfs), boto3, warcio, pyarrow, openai (embeddings via the DGX over Tailscale). Reuses `commoncrawl_enrich.{extract,classifier,nace_embed,tech,wappalyzer_client}` (current locations; if the package-split plan runs, update imports).

---

## File Structure
- **Create** `index_enrich/__init__.py` — new service package.
- **Create** `index_enrich/worklist.py` — DuckDB window query → worklist Parquet (+ documented Athena SQL).
- **Create** `index_enrich/fetch.py` — `fetch_warc_record(...)` byte-range read → `(html, headers)`.
- **Create** `index_enrich/schema.py` — `commoncrawl_domains` Parquet schema + row builder (column order == migration 046).
- **Create** `index_enrich/classify.py` — `enrich_domain(...)` → `(domain_row, tech_rows)`.
- **Create** `index_enrich/worker.py` — read a worklist shard → fetch+enrich → write Parquet; one-shot CLI.
- **Create** `scripts/index_enrich_validate.py` — 10k/one-TLD validation + measurement.
- **Modify** `pyproject.toml` — add `"index_enrich"` to hatch packages.
- **Tests:** `tests/test_index_worklist.py`, `tests/test_index_fetch.py`, `tests/test_index_classify.py`, `tests/test_index_worker.py`.

Commands run from `corpscout/dagster_v3/` with `uv run`.

---

## Task 1: Worklist query (shallowest page per domain)

**Files:** Create `index_enrich/__init__.py`, `index_enrich/worklist.py`; Modify `pyproject.toml`; Test `tests/test_index_worklist.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_index_worklist.py`:

```python
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from index_enrich import worklist


def _write_index(path, rows):
    # rows: (domain, url, url_path, status, mime, warc_filename, offset, length)
    cols = ["url_host_registered_domain", "url", "url_path", "fetch_status",
            "content_mime_detected", "warc_filename", "warc_record_offset",
            "warc_record_length", "content_languages"]
    data = {c: [] for c in cols}
    for d, u, p, s, m, wf, off, ln in rows:
        for c, v in zip(cols, [d, u, p, s, m, wf, off, ln, "eng"]):
            data[c].append(v)
    pq.write_table(pa.table(data), path)


def test_worklist_picks_shallowest_200_html_per_domain(tmp_path):
    idx = tmp_path / "idx.parquet"
    _write_index(idx, [
        ("acme.com", "http://acme.com/en/about", "/en/about", 200, "text/html", "w1.gz", 10, 5),
        ("acme.com", "http://acme.com/en/", "/en/", 200, "text/html", "w2.gz", 20, 6),    # shallowest
        ("acme.com", "http://acme.com/en/", "/en/", 301, "text/html", "w3.gz", 0, 1),      # not 200
        ("shop.org", "http://shop.org/p/1.html", "/p/1.html", 200, "text/html", "w4.gz", 30, 7),
        ("img.org", "http://img.org/logo.png", "/logo.png", 200, "image/png", "w5.gz", 0, 1),  # not html
    ])
    con = duckdb.connect()
    rows = worklist.run_worklist(con, f"read_parquet('{idx}')", crawl="CC-MAIN-2026-25").fetchall()
    by_dom = {r[0]: r for r in rows}
    # columns: root_domain(0), url(1), warc_filename(2), offset(3), length(4), languages(5)
    assert set(by_dom) == {"acme.com", "shop.org"}            # img.org dropped (no html), no dupes
    assert by_dom["acme.com"][1] == "http://acme.com/en/"     # shallowest 200-html
    assert by_dom["acme.com"][2] == "w2.gz" and by_dom["acme.com"][3] == 20
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_index_worklist.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'index_enrich'`.

- [ ] **Step 3: Implement the worklist query**

Create `index_enrich/__init__.py`:
```python
"""Index-driven domain enrichment: query the CommonCrawl URL index for one page per domain."""
```

Create `index_enrich/worklist.py`:
```python
"""Build the per-domain worklist from the CommonCrawl columnar URL index.

`source` is a DuckDB table expression over the index parquet:
- test/off-AWS small: read_parquet('idx.parquet')
- off-AWS full: read_parquet(['https://data.commoncrawl.org/<part>', ...], hive_partitioning=true)
- on-AWS: read_parquet('s3://commoncrawl/cc-index/table/cc-main/warc/crawl=.../subset=warc/*.parquet')

One row per domain: the shallowest fetch_status=200 HTML page + its WARC location.
"""

ATHENA_SQL = """
-- AWS path: run against the `ccindex` Athena table, UNLOAD result to your S3 bucket.
SELECT root_domain, url, warc_filename, warc_record_offset, warc_record_length, content_languages
FROM (
  SELECT url_host_registered_domain AS root_domain, url, warc_filename,
         warc_record_offset, warc_record_length, content_languages,
         ROW_NUMBER() OVER (
           PARTITION BY url_host_registered_domain
           ORDER BY length(url_path) - length(replace(url_path,'/','')) ASC,
                    length(url_path) ASC
         ) rn
  FROM ccindex
  WHERE crawl = ? AND subset = 'warc'
    AND fetch_status = 200
    AND content_mime_detected IN ('text/html','application/xhtml+xml')
) WHERE rn = 1
"""

_HTML_MIME = ("text/html", "application/xhtml+xml")


def worklist_query(source: str, *, where: str = "") -> str:
    extra = f" AND ({where})" if where else ""
    mime = ", ".join(f"'{m}'" for m in _HTML_MIME)
    return f"""
        SELECT root_domain, url, warc_filename, warc_record_offset, warc_record_length, content_languages
        FROM (
          SELECT url_host_registered_domain AS root_domain, url, warc_filename,
                 warc_record_offset, warc_record_length, content_languages,
                 row_number() OVER (
                   PARTITION BY url_host_registered_domain
                   ORDER BY length(url_path) - length(replace(url_path,'/','')) ASC,
                            length(url_path) ASC
                 ) AS rn
          FROM {source}
          WHERE fetch_status = 200
            AND content_mime_detected IN ({mime}){extra}
        ) WHERE rn = 1
    """


def run_worklist(con, source: str, *, crawl: str = "", where: str = ""):
    """Execute the worklist query; returns a DuckDB result (use .fetchall() or .arrow())."""
    return con.execute(worklist_query(source, where=where))


def build_worklist(con, source: str, out_path, *, where: str = "") -> int:
    """Write the worklist to a Parquet file; returns row count."""
    table = run_worklist(con, source, where=where).arrow()
    import pyarrow.parquet as pq
    pq.write_table(table, out_path)
    return table.num_rows
```

(`crawl` is accepted for symmetry with the Athena path; for DuckDB the crawl is already scoped by which parquet parts `source` points at.)

- [ ] **Step 4: Register the package + run tests**

Add `"index_enrich"` to `[tool.hatch.build.targets.wheel] packages` in `pyproject.toml`.
Run: `uv run pytest tests/test_index_worklist.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add index_enrich/__init__.py index_enrich/worklist.py tests/test_index_worklist.py pyproject.toml
git commit -m "feat: index_enrich worklist query (shallowest page per domain)"
```

---

## Task 2: Fetch one WARC record by byte range

**Files:** Create `index_enrich/fetch.py`; Test `tests/test_index_fetch.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_index_fetch.py`:
```python
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
    html, headers = fetch.fetch_warc_record(s3, "crawl-data/.../x.warc.gz", 0, len(blob))
    assert "wp-content" in html and headers.get("Server") == "nginx"
    assert s3.ranges[0][2] == f"bytes=0-{len(blob)-1}"
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_index_fetch.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'index_enrich.fetch'`.

- [ ] **Step 3: Implement the fetch**

Create `index_enrich/fetch.py`:
```python
"""Fetch a single WARC record by byte range and parse it to (html, headers)."""
from io import BytesIO

from warcio.archiveiterator import ArchiveIterator

CC_BUCKET = "commoncrawl"


def fetch_warc_record(s3, warc_filename: str, offset: int, length: int, *,
                      bucket: str = CC_BUCKET) -> tuple[str, dict]:
    """Byte-range GET one WARC record -> (html, response-headers dict). `s3` has get_object."""
    resp = s3.get_object(Bucket=bucket, Key=warc_filename,
                         Range=f"bytes={offset}-{offset + length - 1}")
    data = resp["Body"].read()
    record = next(ArchiveIterator(BytesIO(data)))
    http = record.http_headers
    headers = {k: v for k, v in (http.headers if http else [])}
    html = record.content_stream().read().decode("utf-8", "replace")
    return html, headers
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_index_fetch.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add index_enrich/fetch.py tests/test_index_fetch.py
git commit -m "feat: index_enrich fetch_warc_record (byte-range -> html+headers)"
```

---

## Task 3: Per-domain enrichment row builder

**Files:** Create `index_enrich/schema.py`, `index_enrich/classify.py`; Test `tests/test_index_classify.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_index_classify.py`:
```python
from datetime import datetime, timezone

from commoncrawl_enrich.classifier import IndustryResult
from commoncrawl_enrich.models import Technology
from index_enrich import classify, schema

RESOLVED = datetime(2026, 6, 23, tzinfo=timezone.utc)


class FakeClassifier:
    def classify(self, texts):
        return [IndustryResult(nace_code="62.01", nace_label="Programming", nace_division="62",
                               nace_confident=True, nace_score=0.8, method="embedding") for _ in texts]


class FakeWappalyzer:
    def analyze_batch(self, items):
        return {k: [Technology(technology="WordPress", category="CMS", version="6.1", confidence=100)]
                for k, _, _ in items}


def test_enrich_domain_builds_domain_and_tech_rows():
    html = "<html><body>ACME software, info@acme.com</body></html>"
    headers = {"Server": "nginx"}
    domain_row, tech_rows = classify.enrich_domain(
        html, headers, root_domain="acme.com", url="http://acme.com/en/",
        crawl_id="CC-MAIN-2026-25", classifier=FakeClassifier(), wappalyzer=FakeWappalyzer(),
        resolved_at=RESOLVED)
    assert len(domain_row) == len(schema.DOMAINS_COLUMNS)
    assert domain_row[2] == "acme.com" and domain_row[8] == "62.01"       # root_domain, nace_code
    assert "info@acme.com" in domain_row[4]                                # emails
    assert tech_rows and tech_rows[0][4] == "WordPress"                    # technology
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_index_classify.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'index_enrich.classify'`.

- [ ] **Step 3: Implement schema + enrich_domain**

Create `index_enrich/schema.py` (column order == migration 046 `commoncrawl_domains`; tech == 047):
```python
import pyarrow as pa

DOMAINS_COLUMNS = (
    "crawl_id", "url", "root_domain", "subdomain", "emails", "email_count",
    "page_type", "page_type_score", "nace_code", "nace_label", "nace_division",
    "nace_confident", "nace_margin", "nace_score", "nace_method",
    "nace_top3_codes", "nace_top3_labels", "nace_top3_scores",
    "source_url", "source_run_id", "resolved_at",
)
DOMAINS_PARQUET_SCHEMA = pa.schema([
    ("crawl_id", pa.string()), ("url", pa.string()), ("root_domain", pa.string()),
    ("subdomain", pa.string()), ("emails", pa.list_(pa.string())), ("email_count", pa.uint32()),
    ("page_type", pa.string()), ("page_type_score", pa.float32()),
    ("nace_code", pa.string()), ("nace_label", pa.string()), ("nace_division", pa.string()),
    ("nace_confident", pa.uint8()), ("nace_margin", pa.float32()), ("nace_score", pa.float32()),
    ("nace_method", pa.string()), ("nace_top3_codes", pa.list_(pa.string())),
    ("nace_top3_labels", pa.list_(pa.string())), ("nace_top3_scores", pa.list_(pa.float32())),
    ("source_url", pa.string()), ("source_run_id", pa.string()),
    ("resolved_at", pa.timestamp("us", tz="UTC")),
])
TECHNOLOGIES_COLUMNS = (
    "crawl_id", "url", "root_domain", "subdomain", "technology", "category",
    "version", "confidence", "source_url", "source_run_id", "resolved_at",
)


def write_domain_rows_parquet(rows: list, out_path) -> int:
    import pyarrow.parquet as pq
    columns = list(zip(*rows)) if rows else [() for _ in DOMAINS_COLUMNS]
    arrays = [pa.array(list(col), type=DOMAINS_PARQUET_SCHEMA.field(i).type)
              for i, col in enumerate(columns)]
    pq.write_table(pa.Table.from_arrays(arrays, schema=DOMAINS_PARQUET_SCHEMA), out_path)
    return len(rows)
```

Create `index_enrich/classify.py`:
```python
"""Turn one fetched record into a per-domain industry row (+ homepage tech rows)."""
from datetime import datetime, timezone
from urllib.parse import urlparse

import tldextract

from commoncrawl_enrich import extract
from index_enrich.schema import DOMAINS_COLUMNS  # noqa: F401 (documents column order)

_TE = tldextract.TLDExtract(suffix_list_urls=(), cache_dir=None)


def _subdomain(url: str) -> str:
    try:
        return _TE(url).subdomain
    except Exception:  # noqa: BLE001
        return ""


def enrich_domain(html: str, headers: dict, *, root_domain: str, url: str, crawl_id: str,
                  classifier, wappalyzer=None, source_run_id: str = "",
                  resolved_at: datetime | None = None) -> tuple[tuple, list[tuple]]:
    resolved_at = resolved_at or datetime.now(timezone.utc)
    sub = _subdomain(url)
    parsed = extract.parse_html(html)
    emails = [e.email for e in extract.extract_emails(html)]
    res = classifier.classify([parsed.text or html])[0]
    domain_row = (
        crawl_id, url, root_domain, sub, emails, len(emails),
        res.page_type, float(res.page_type_score),
        res.nace_code, res.nace_label, res.nace_division,
        int(res.nace_confident), float(res.nace_margin), float(res.nace_score), res.method,
        res.nace_top3, res.nace_top3_labels, [float(s) for s in res.nace_top3_scores],
        url, source_run_id, resolved_at,
    )
    tech_rows: list[tuple] = []
    if wappalyzer is not None:
        hmap = {k: [v] for k, v in (headers or {}).items()}
        for techs in wappalyzer.analyze_batch([(url, hmap, html)]).values():
            for t in techs:
                tech_rows.append((crawl_id, url, root_domain, sub, t.technology, t.category,
                                  t.version, int(t.confidence), url, source_run_id, resolved_at))
    return domain_row, tech_rows
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_index_classify.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add index_enrich/schema.py index_enrich/classify.py tests/test_index_classify.py
git commit -m "feat: index_enrich enrich_domain (industry row + homepage tech rows)"
```

---

## Task 4: The worker (worklist shard -> fetch+enrich -> Parquet)

**Files:** Create `index_enrich/worker.py`; Test `tests/test_index_worker.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_index_worker.py`:
```python
from datetime import datetime, timezone
from io import BytesIO

import pyarrow.parquet as pq
from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter

from commoncrawl_enrich.classifier import IndustryResult
from index_enrich import worker

RESOLVED = datetime(2026, 6, 23, tzinfo=timezone.utc)


def _rec(uri, html):
    buf = BytesIO(); w = WARCWriter(buf, gzip=True)
    h = StatusAndHeaders("200 OK", [("Content-Type", "text/html")], protocol="HTTP/1.1")
    w.write_record(w.create_warc_record(uri, "response", payload=BytesIO(html.encode()), http_headers=h))
    return buf.getvalue()


class FakeS3:
    def __init__(self, blobs):  # key -> bytes
        self._blobs = blobs
    def get_object(self, Bucket, Key, Range):
        s, e = Range.removeprefix("bytes=").split("-")
        return {"Body": BytesIO(self._blobs[Key][int(s):int(e)+1])}


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
    assert stats["domains"] == 1 and out.exists()
    assert pq.read_table(out).to_pylist()[0]["nace_code"] == "62.01"
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_index_worker.py -v`
Expected: FAIL with `AttributeError: module 'index_enrich.worker' has no attribute 'run_shard'`.

- [ ] **Step 3: Implement the worker**

Create `index_enrich/worker.py`:
```python
"""Process a worklist shard: fetch each record, enrich, write one Parquet of domain rows."""
import argparse
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from index_enrich import classify, fetch, schema

LOGGER = logging.getLogger(__name__)


def run_shard(worklist: list[dict], *, s3, classifier, crawl_id: str, out_path,
              wappalyzer=None, source_run_id: str = "", resolved_at: datetime | None = None) -> dict:
    resolved_at = resolved_at or datetime.now(timezone.utc)
    domain_rows: list[tuple] = []
    errors = 0
    for item in worklist:
        try:
            html, headers = fetch.fetch_warc_record(
                s3, item["warc_filename"], int(item["warc_record_offset"]),
                int(item["warc_record_length"]))
            row, _tech = classify.enrich_domain(
                html, headers, root_domain=item["root_domain"], url=item["url"],
                crawl_id=crawl_id, classifier=classifier, wappalyzer=wappalyzer,
                source_run_id=source_run_id, resolved_at=resolved_at)
            domain_rows.append(row)
        except Exception as exc:  # noqa: BLE001 - skip a bad record, keep the shard going
            errors += 1
            LOGGER.warning("enrich failed for %s: %s", item.get("root_domain"), exc)
    schema.write_domain_rows_parquet(domain_rows, out_path)
    return {"domains": len(domain_rows), "errors": errors, "out": str(out_path)}


def _load_classifier():
    from commoncrawl_enrich import nace_embed
    from commoncrawl_enrich.classifier import PageClassifier
    refs = Path(os.environ.get("COMMONCRAWL_REFS_DIR", "data"))
    ref = nace_embed.NaceReference.load(str(refs / "nace_reference.npz"))
    protos = nace_embed.PrototypeSet.load(str(refs / "page_type_prototypes.npz"))
    return PageClassifier(ref, protos, nace_embed.EmbeddingClient.from_env())


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="Enrich a worklist shard of domains.")
    ap.add_argument("--worklist", required=True, help="Parquet shard of worklist rows")
    ap.add_argument("--out", required=True)
    ap.add_argument("--crawl-id", required=True)
    args = ap.parse_args(argv)

    import boto3
    import pyarrow.parquet as pq
    rows = pq.read_table(args.worklist).to_pylist()
    stats = run_shard(rows, s3=boto3.client("s3"), classifier=_load_classifier(),
                      crawl_id=args.crawl_id, out_path=args.out)
    print(json.dumps(stats, default=str))


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_index_worker.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add index_enrich/worker.py tests/test_index_worker.py
git commit -m "feat: index_enrich worker (worklist shard -> fetch+enrich -> parquet)"
```

---

## Task 5: 10k-domain validation script (live, on AWS or off-AWS)

**Files:** Create `scripts/index_enrich_validate.py`

- [ ] **Step 1: Write the script**

Create `scripts/index_enrich_validate.py`:
```python
#!/usr/bin/env python
"""Validate index-driven enrichment on a small subset and measure throughput.

Off-AWS (DuckDB over the index parts over HTTP; fetch from data.commoncrawl.org):
    set -a; . ./.env; set +a
    uv run python scripts/index_enrich_validate.py --crawl CC-MAIN-2026-25 --limit 10000

The full off-AWS index scan is heavy; pass --index-glob to point at a small local/HTTP subset
for a first smoke test, or run the worklist step on AWS (Athena) and load its Parquet here.
"""
import argparse
import time

import boto3
import duckdb

from index_enrich import worker, worklist


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--crawl", default="CC-MAIN-2026-25")
    ap.add_argument("--index-glob", required=True,
                    help="DuckDB read_parquet source for the index parts (HTTP list / s3 glob / local)")
    ap.add_argument("--where", default="", help="extra SQL filter, e.g. url_host_tld='sk'")
    ap.add_argument("--limit", type=int, default=10000)
    ap.add_argument("--out", default="data/commoncrawl/index_validate.parquet")
    args = ap.parse_args()

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    t = time.monotonic()
    src = f"read_parquet([{args.index_glob}], hive_partitioning=true)"
    rows = worklist.run_worklist(con, src, where=args.where).fetchmany(args.limit)
    cols = ["root_domain", "url", "warc_filename", "warc_record_offset", "warc_record_length"]
    items = [dict(zip(cols, r)) for r in rows]
    print(f"worklist: {len(items)} domains in {time.monotonic()-t:.0f}s")

    t = time.monotonic()
    stats = worker.run_shard(items, s3=boto3.client("s3"),
                             classifier=worker._load_classifier(), crawl_id=args.crawl,
                             out_path=args.out)
    dt = time.monotonic() - t
    print(f"enriched {stats['domains']} domains ({stats['errors']} errors) in {dt:.0f}s "
          f"-> {stats['domains']/max(dt,1e-9):.1f} domains/s")
    print("projection to 40M domains: "
          f"{40_000_000/max(stats['domains']/max(dt,1e-9),1e-9)/86400:.1f} worker-days single-stream")
```

- [ ] **Step 2: Verify it parses**

Run: `uv run python -c "import ast; ast.parse(open('scripts/index_enrich_validate.py').read()); print('ok')"`
Expected: `ok`.

- [ ] **Step 3: Commit**

```bash
git add scripts/index_enrich_validate.py
git commit -m "feat: index-driven enrichment 10k validation + projection"
```

- [ ] **Step 4: (manual, AWS us-east-1 recommended) run it**

On the EC2 instance (Tailscale → DGX for embeddings), run the worklist on Athena (or a small
DuckDB subset) and the validation script. Record: worklist query time/cost, fetch+enrich
domains/sec, embed rate to the DGX, Parquet bytes/domain, and eyeball classification quality.
Project to ~40M. No commit (measurement).

---

## Task 6: Full verification

- [ ] **Step 1: Whole index_enrich suite + Dagster defs**

Run: `uv run pytest tests/test_index_*.py -q && uv run dg check defs`
Expected: all pass; `All definitions loaded successfully.`

- [ ] **Step 2: No commit** (verification only).

---

## Operational notes (AWS, not code)
- **Region `us-east-1`** is mandatory (CommonCrawl bucket locality → free/fast S3 + range GETs).
- **Worklist on AWS:** register the `ccindex` Athena table + the crawl partition; run `ATHENA_SQL`; `UNLOAD` to your S3 bucket as Parquet. Shard it by row-range for the workers.
- **Embeddings:** install Tailscale on the EC2 instance so workers reach the DGX (`COMMONCRAWL_EMBED_BASE_URL`); or stand up an AWS GPU embedder later.
- **Scale:** N workers over worklist shards; embedding (~40M, ~4 days, DGX-bound) is the gate. Add NATS only when one box isn't enough.
- **ClickHouse load:** the per-domain Parquets → `commoncrawl_domains` (migration 046) + the homepage tech → `commoncrawl_technologies` (047).
- If the package-split plan later runs, update `index_enrich` imports from `commoncrawl_enrich.*` to `wet_processing.*`/`warc_processing.*` accordingly.

---

## Notes for the implementer
- Tests are offline (synthetic index Parquet, synthetic gzipped WARC records via warcio, fake S3/classifier/wappalyzer). Only Task 5 step 4 needs live endpoints (index, S3, DGX).
- Commit by explicit path.
- `data/*.npz` (reference matrices) are produced by the `commoncrawl_classify` assets / the calibrate script; the worker loads them via `COMMONCRAWL_REFS_DIR`.
