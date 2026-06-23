# CommonCrawl WET Worker (Phase 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A stateless, dockerized worker that takes one CommonCrawl WET file (S3 path or CDN url) and writes one Parquet of `commoncrawl_domains` rows (industry + page-type + emails + top-3 audit) to a local path or S3 bucket.

**Architecture:** Reuse the tested `commoncrawl_enrich` classification stack. Extract a shared `build_wet_domain_rows` from `process_wet_to_clickhouse`, add a Parquet schema + writer, wrap them in a `worker.run_wet_task(WetTask)` core with a one-shot CLI, then bake the package + reference `.npz` into a Docker image built in GitHub CI. The NATS `serve` mode + orchestrator + ledger are Phase 2 (not in this plan).

**Tech Stack:** Python 3.14, `uv`, pyarrow, warcio, boto3, openai (embeddings), numpy, lxml, tldextract; Docker; GitHub Actions.

---

## File Structure

- **Modify** `commoncrawl_enrich/ingest.py` — lazy-import `WappalyzerClient` (decouple WET from dlt); extract `build_wet_domain_rows`; add `DOMAINS_PARQUET_SCHEMA` + `write_domain_rows_parquet`.
- **Create** `commoncrawl_enrich/worker.py` — `WetTask`, `run_wet_task`, `load_classifier`, `_make_s3`, `main` (one-shot CLI).
- **Create** `corpscout/commoncrawl-worker/Dockerfile` — image (deps + package + baked `.npz`).
- **Create** `corpscout/commoncrawl-worker/run.sh` — pull image from registry + run with a task JSON.
- **Create** `corpscout/commoncrawl-worker/refs/.gitkeep` — image build copies the `.npz` here.
- **Create** `.github/workflows/commoncrawl-worker.yml` — CI build + push to GHCR.
- **Create** `scripts/benchmark_wet.py` — one-file benchmark + ×100k projection.
- **Modify** `tests/test_ingest.py` — tests for `build_wet_domain_rows` + Parquet writer.
- **Create** `tests/test_worker.py` — tests for `WetTask`, `run_wet_task` (local + S3 output, fake classifier/S3).

Commands run from `corpscout/dagster_v3/` with `uv run` unless noted.

---

## Task 1: Decouple WET from dlt + extract `build_wet_domain_rows`

**Files:**
- Modify: `commoncrawl_enrich/ingest.py`
- Test: `tests/test_ingest.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_ingest.py`:

```python
def test_build_wet_domain_rows_matches_clickhouse_rows(tmp_path):
    from commoncrawl_enrich import ingest
    wet = tmp_path / "x.warc.wet.gz"
    _write_wet(wet, [
        ("http://acme.com/", "ACME makes software. Contact info@acme.com"),
        ("http://acme.com/about", "deep page"),               # filtered (homepages_only)
        ("http://shop.example.org/", "online shop selling things"),
    ])
    rows = ingest.build_wet_domain_rows(
        str(wet), classifier=FakeClassifier(), crawl_id="CC-MAIN-2026-25",
        source_url="http://data/x.wet.gz", source_run_id="run1", resolved_at=RESOLVED)
    assert len(rows) == 2                                      # homepages only
    assert all(len(r) == len(ingest.DOMAINS_COLUMNS) for r in rows)
    acme = next(r for r in rows if r[2] == "acme.com")
    assert acme[1] == "http://acme.com/" and acme[4] == ["info@acme.com"] and acme[8] == "62.01"
    assert acme[20] == RESOLVED
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_ingest.py::test_build_wet_domain_rows_matches_clickhouse_rows -v`
Expected: FAIL with `AttributeError: module 'commoncrawl_enrich.ingest' has no attribute 'build_wet_domain_rows'`.

- [ ] **Step 3: Implement the refactor**

In `commoncrawl_enrich/ingest.py`, change the top imports (remove the module-level `WappalyzerClient` import so the WET path doesn't pull `dlt`):

```python
from datetime import datetime, timezone

from warcio.archiveiterator import ArchiveIterator

from commoncrawl_enrich import extract, segment
from commoncrawl_enrich.classifier import PageClassifier
```

In `process_warc_to_clickhouse`, lazy-import the client at the top of the function body (first line inside it), and drop the `WappalyzerClient | None` annotation on the parameter (use `wappalyzer=None`):

```python
def process_warc_to_clickhouse(
    source: str, *, ch_client, crawl_id: str, wappalyzer=None,
    source_url: str | None = None, source_run_id: str = "",
    resolved_at: datetime | None = None, limit: int | None = None,
    session=None, batch_size: int = 2000,
) -> dict:
    """Process one WARC file -> append per-page technologies + page signals (emails/socials)."""
    from commoncrawl_enrich.wappalyzer_client import WappalyzerClient
    resolved_at = resolved_at or datetime.now(timezone.utc)
    ...
    wappalyzer = wappalyzer or WappalyzerClient.from_env()
```

Replace `process_wet_to_clickhouse` with the extracted builder + a thin sink. The `_domain_stats` helper already exists from earlier; keep it. New code:

```python
def build_wet_domain_rows(
    source: str, *, classifier: PageClassifier, crawl_id: str,
    source_url: str | None = None, source_run_id: str = "", resolved_at: datetime | None = None,
    limit: int | None = None, session=None, homepages_only: bool = True,
) -> list[tuple]:
    """Stream one WET file -> commoncrawl_domains rows (emails + page_type + NACE + top-3 audit).
    Shared by the ClickHouse sink and the Parquet sink."""
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
    return [
        (crawl_id, uri, root, sub, emails, len(emails),
         res.page_type, float(res.page_type_score),
         res.nace_code, res.nace_label, res.nace_division,
         int(res.nace_confident), float(res.nace_margin), float(res.nace_score), res.method,
         res.nace_top3, res.nace_top3_labels, [float(s) for s in res.nace_top3_scores],
         source_url, source_run_id, resolved_at)
        for (uri, root, sub, emails, _text), res in zip(recs, results)
    ]


def process_wet_to_clickhouse(
    source: str, *, classifier: PageClassifier, ch_client, crawl_id: str,
    source_url: str | None = None, source_run_id: str = "", resolved_at: datetime | None = None,
    limit: int | None = None, session=None, homepages_only: bool = True, batch_size: int = 2000,
) -> dict:
    """Process one WET file -> append homepage rows to commoncrawl_domains."""
    rows = build_wet_domain_rows(
        source, classifier=classifier, crawl_id=crawl_id, source_url=source_url,
        source_run_id=source_run_id, resolved_at=resolved_at, limit=limit,
        session=session, homepages_only=homepages_only)
    _insert(ch_client, DOMAINS_TABLE, DOMAINS_COLUMNS, rows, batch_size)
    return _domain_stats(rows)
```

If `_domain_stats` does not yet exist in the file, add it next to `_insert`:

```python
def _domain_stats(rows: list) -> dict:
    return {
        "records": len(rows),
        "with_email": sum(1 for r in rows if r[4]),
        "page_types": sum(1 for r in rows if r[6]),
        "industries": sum(1 for r in rows if r[8]),
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_ingest.py -v`
Expected: PASS (the new test + the existing WET/WARC ingest tests still pass).

- [ ] **Step 5: Commit**

```bash
git add commoncrawl_enrich/ingest.py tests/test_ingest.py
git commit -m "refactor: extract build_wet_domain_rows; lazy-import wappalyzer in WET path"
```

---

## Task 2: Parquet schema + writer

**Files:**
- Modify: `commoncrawl_enrich/ingest.py`
- Test: `tests/test_ingest.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_ingest.py`:

```python
def test_write_domain_rows_parquet_roundtrip(tmp_path):
    import pyarrow.parquet as pq
    from commoncrawl_enrich import ingest
    wet = tmp_path / "x.warc.wet.gz"
    _write_wet(wet, [("http://acme.com/", "ACME software, info@acme.com")])
    rows = ingest.build_wet_domain_rows(
        str(wet), classifier=FakeClassifier(), crawl_id="CC-MAIN-2026-25", resolved_at=RESOLVED)
    out = tmp_path / "acme.parquet"
    n = ingest.write_domain_rows_parquet(rows, out)
    assert n == 1
    table = pq.read_table(out)
    assert table.schema.equals(ingest.DOMAINS_PARQUET_SCHEMA)
    assert table.column_names == list(ingest.DOMAINS_COLUMNS)
    rec = table.to_pylist()[0]
    assert rec["root_domain"] == "acme.com" and rec["nace_code"] == "62.01"
    assert rec["nace_top3_codes"] == ["62.01", "62.09", "47.11"]


def test_write_domain_rows_parquet_empty(tmp_path):
    import pyarrow.parquet as pq
    from commoncrawl_enrich import ingest
    out = tmp_path / "empty.parquet"
    assert ingest.write_domain_rows_parquet([], out) == 0
    assert pq.read_table(out).schema.equals(ingest.DOMAINS_PARQUET_SCHEMA)  # typed, still loadable
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_ingest.py::test_write_domain_rows_parquet_roundtrip -v`
Expected: FAIL with `AttributeError: module 'commoncrawl_enrich.ingest' has no attribute 'DOMAINS_PARQUET_SCHEMA'`.

- [ ] **Step 3: Implement schema + writer**

In `commoncrawl_enrich/ingest.py`, add the pyarrow import at the top (after the datetime import):

```python
import pyarrow as pa
import pyarrow.parquet as pq
```

Add, right after the `DOMAINS_COLUMNS`/`TECHNOLOGIES_COLUMNS`/`PAGE_SIGNALS_COLUMNS` definitions:

```python
# Column order == DOMAINS_COLUMNS so the file is directly CH-loadable:
#   INSERT INTO corpscout.commoncrawl_domains SELECT * FROM file('x.parquet')
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


def write_domain_rows_parquet(rows: list, out_path) -> int:
    """Write commoncrawl_domains rows to one Parquet file (one file per WET file)."""
    columns = list(zip(*rows)) if rows else [() for _ in DOMAINS_COLUMNS]
    arrays = [pa.array(list(col), type=DOMAINS_PARQUET_SCHEMA.field(i).type)
              for i, col in enumerate(columns)]
    pq.write_table(pa.Table.from_arrays(arrays, schema=DOMAINS_PARQUET_SCHEMA), out_path)
    return len(rows)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_ingest.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add commoncrawl_enrich/ingest.py tests/test_ingest.py
git commit -m "feat: DOMAINS_PARQUET_SCHEMA + write_domain_rows_parquet"
```

---

## Task 3: `WetTask` config object

**Files:**
- Create: `commoncrawl_enrich/worker.py`
- Test: `tests/test_worker.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_worker.py`:

```python
import pytest

from commoncrawl_enrich.worker import WetTask


def test_wettask_from_json_local():
    task = WetTask.from_json({
        "crawl_id": "CC-MAIN-2026-25", "file_index": 42,
        "wet_path": "s3://crawls/wet/CC-MAIN-...-00042.warc.wet.gz",
        "output": {"kind": "local", "path": "data/out"},
    })
    assert task.crawl_id == "CC-MAIN-2026-25" and task.file_index == 42
    assert task.output["kind"] == "local" and task.limit is None


def test_wettask_from_json_s3_with_limit():
    task = WetTask.from_json({
        "crawl_id": "C", "file_index": "7", "wet_path": "https://data.commoncrawl.org/x.gz",
        "output": {"kind": "s3", "bucket": "results", "prefix": "wet"}, "limit": 100,
    })
    assert task.file_index == 7 and task.limit == 100  # file_index coerced to int


def test_wettask_missing_field_raises():
    with pytest.raises(KeyError):
        WetTask.from_json({"crawl_id": "C", "file_index": 1})  # no wet_path/output
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_worker.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'commoncrawl_enrich.worker'`.

- [ ] **Step 3: Implement `WetTask`**

Create `commoncrawl_enrich/worker.py`:

```python
"""Stateless WET worker: one WET file -> one Parquet of commoncrawl_domains rows.

Phase 1: a one-shot CLI (`python -m commoncrawl_enrich.worker --task task.json`). The NATS
`serve` mode and the orchestrator are Phase 2 and reuse `run_wet_task` unchanged.
"""
import argparse
import json
import os
import shutil
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from commoncrawl_enrich import ingest, nace_embed, segment
from commoncrawl_enrich.classifier import PageClassifier


@dataclass(frozen=True)
class WetTask:
    crawl_id: str
    file_index: int
    wet_path: str          # s3://bucket/key or https://data.commoncrawl.org/... or local path
    output: dict           # {"kind":"local","path":...} | {"kind":"s3","bucket":...,"prefix":...}
    limit: int | None = None

    @classmethod
    def from_json(cls, data: dict) -> "WetTask":
        return cls(
            crawl_id=data["crawl_id"], file_index=int(data["file_index"]),
            wet_path=data["wet_path"], output=data["output"], limit=data.get("limit"),
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_worker.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add commoncrawl_enrich/worker.py tests/test_worker.py
git commit -m "feat: WetTask config object for the WET worker"
```

---

## Task 4: `run_wet_task` — local source, local output

**Files:**
- Modify: `commoncrawl_enrich/worker.py`
- Test: `tests/test_worker.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_worker.py` (top-level imports + fixtures, then the test):

```python
from datetime import datetime, timezone
from io import BytesIO

import pyarrow.parquet as pq
from warcio.warcwriter import WARCWriter

from commoncrawl_enrich import worker
from commoncrawl_enrich.classifier import IndustryResult

RESOLVED = datetime(2026, 6, 23, tzinfo=timezone.utc)


class FakeClassifier:
    def classify(self, texts):
        return [IndustryResult(
            nace_code="62.01", nace_label="Programming", nace_division="62",
            nace_confident=True, nace_score=0.8, nace_margin=0.2,
            nace_top3=["62.01", "62.09", "47.11"], nace_top3_labels=["a", "b", "c"],
            nace_top3_scores=[0.8, 0.5, 0.3], method="embedding") for _ in texts]


def _write_wet(path, pages):
    with open(path, "wb") as fh:
        w = WARCWriter(fh, gzip=True)
        for uri, text in pages:
            w.write_record(w.create_warc_record(
                uri, "conversion", payload=BytesIO(text.encode()),
                warc_content_type="text/plain"))


def test_run_wet_task_local_to_local(tmp_path):
    wet = tmp_path / "CC-MAIN-...-00042.warc.wet.gz"
    _write_wet(wet, [("http://acme.com/", "ACME software, info@acme.com")])
    out_dir = tmp_path / "out"
    task = worker.WetTask(
        crawl_id="CC-MAIN-2026-25", file_index=42, wet_path=str(wet),
        output={"kind": "local", "path": str(out_dir)})
    stats = worker.run_wet_task(task, classifier=FakeClassifier())
    expected = out_dir / "CC-MAIN-2026-25" / "42.parquet"
    assert expected.exists()
    assert stats["records"] == 1 and stats["industries"] == 1
    assert stats["parquet_bytes"] > 0 and stats["output"].endswith("42.parquet")
    assert pq.read_table(expected).to_pylist()[0]["nace_code"] == "62.01"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_worker.py::test_run_wet_task_local_to_local -v`
Expected: FAIL with `AttributeError: module 'commoncrawl_enrich.worker' has no attribute 'run_wet_task'`.

- [ ] **Step 3: Implement `run_wet_task` (+ source/output helpers)**

Append to `commoncrawl_enrich/worker.py`:

```python
def _resolve_source(wet_path: str, *, s3, session, dest: str) -> tuple[str, bool]:
    """Return (local_path, downloaded). Downloads s3://… and http(s)://… to `dest`."""
    if wet_path.startswith("s3://"):
        parsed = urlparse(wet_path)
        s3.download_file(parsed.netloc, parsed.path.lstrip("/"), dest)
        return dest, True
    if wet_path.startswith(("http://", "https://")):
        with session.get(wet_path, stream=True,
                         headers={"User-Agent": segment.USER_AGENT}, timeout=600) as resp:
            resp.raise_for_status()
            with open(dest, "wb") as out:
                for chunk in resp.iter_content(1 << 20):
                    out.write(chunk)
        return dest, True
    return wet_path, False  # local path: use in place


def _emit_output(parquet_tmp: str, task: "WetTask", *, s3) -> str:
    """Copy/upload the Parquet to the task output; return its uri."""
    name = f"{task.crawl_id}/{task.file_index}.parquet"
    out = task.output
    if out["kind"] == "local":
        dest = Path(out["path"]) / name
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(parquet_tmp, dest)
        return f"file://{dest}"
    key = "/".join(p for p in (out.get("prefix", "").strip("/"), name) if p)
    with open(parquet_tmp, "rb") as body:
        s3.put_object(Bucket=out["bucket"], Key=key, Body=body)
    return f"s3://{out['bucket']}/{key}"


def run_wet_task(task: "WetTask", *, classifier: PageClassifier, s3=None, session=None) -> dict:
    """Download one WET file, classify homepages, write one Parquet to the task output."""
    session = session or segment.requests.Session()
    fd, wet_tmp = tempfile.mkstemp(suffix=".warc.wet.gz"); os.close(fd)
    fd, pq_tmp = tempfile.mkstemp(suffix=".parquet"); os.close(fd)
    timings: dict[str, float] = {}
    downloaded = False
    try:
        t = time.monotonic()
        src, downloaded = _resolve_source(task.wet_path, s3=s3, session=session, dest=wet_tmp)
        timings["download_s"] = round(time.monotonic() - t, 2)

        t = time.monotonic()
        rows = ingest.build_wet_domain_rows(
            src, classifier=classifier, crawl_id=task.crawl_id,
            source_url=task.wet_path, limit=task.limit)
        ingest.write_domain_rows_parquet(rows, pq_tmp)
        timings["process_s"] = round(time.monotonic() - t, 2)

        location = _emit_output(pq_tmp, task, s3=s3)
        stats = ingest._domain_stats(rows)
        stats.update(parquet_bytes=Path(pq_tmp).stat().st_size, output=location, **timings)
        return stats
    finally:
        if downloaded and Path(wet_tmp).exists():
            os.unlink(wet_tmp)
        if Path(pq_tmp).exists():
            os.unlink(pq_tmp)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_worker.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add commoncrawl_enrich/worker.py tests/test_worker.py
git commit -m "feat: run_wet_task (local source + local output)"
```

---

## Task 5: `run_wet_task` — S3 output + S3 source (fakes)

**Files:**
- Test: `tests/test_worker.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_worker.py`:

```python
class FakeS3:
    def __init__(self, files=None):
        self._files = files or {}       # (bucket,key) -> local path to copy from
        self.put_calls = []             # (bucket, key, bytes)

    def download_file(self, bucket, key, dest):
        import shutil
        shutil.copyfile(self._files[(bucket, key)], dest)

    def put_object(self, Bucket, Key, Body):
        self.put_calls.append((Bucket, Key, Body.read()))


def test_run_wet_task_s3_output(tmp_path):
    wet = tmp_path / "in.warc.wet.gz"
    _write_wet(wet, [("http://acme.com/", "ACME software")])
    s3 = FakeS3()
    task = worker.WetTask(
        crawl_id="CC-MAIN-2026-25", file_index=7, wet_path=str(wet),
        output={"kind": "s3", "bucket": "results", "prefix": "wet/"})
    stats = worker.run_wet_task(task, classifier=FakeClassifier(), s3=s3)
    assert len(s3.put_calls) == 1
    bucket, key, body = s3.put_calls[0]
    assert bucket == "results" and key == "wet/CC-MAIN-2026-25/7.parquet"
    assert body[:4] == b"PAR1"          # parquet magic
    assert stats["output"] == "s3://results/wet/CC-MAIN-2026-25/7.parquet"


def test_run_wet_task_s3_source(tmp_path):
    wet = tmp_path / "remote.warc.wet.gz"
    _write_wet(wet, [("http://acme.com/", "ACME software")])
    s3 = FakeS3(files={("crawls", "wet/00009.warc.wet.gz"): str(wet)})
    out_dir = tmp_path / "out"
    task = worker.WetTask(
        crawl_id="C", file_index=9, wet_path="s3://crawls/wet/00009.warc.wet.gz",
        output={"kind": "local", "path": str(out_dir)})
    stats = worker.run_wet_task(task, classifier=FakeClassifier(), s3=s3)
    assert (out_dir / "C" / "9.parquet").exists() and stats["records"] == 1
```

- [ ] **Step 2: Run test to verify it fails — or passes**

Run: `uv run pytest tests/test_worker.py -k s3 -v`
Expected: PASS (these exercise already-implemented code paths). If either fails, fix `_emit_output`/`_resolve_source` until green. This task is a behaviour lock, not new code.

- [ ] **Step 3: (only if a test failed) fix the helper**

No new code expected. If the S3 key assertion failed, ensure `_emit_output` joins `prefix` and `name` with `"/"` and strips a trailing slash on `prefix` (already in the Task 4 code).

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_worker.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/test_worker.py
git commit -m "test: run_wet_task S3 output + S3 source via fakes"
```

---

## Task 6: `load_classifier` + `_make_s3` + one-shot CLI

**Files:**
- Modify: `commoncrawl_enrich/worker.py`
- Test: `tests/test_worker.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_worker.py`:

```python
def test_make_s3_requires_endpoint(monkeypatch):
    monkeypatch.delenv("CORPSCOUT_S3_ENDPOINT", raising=False)
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    client = worker._make_s3()           # falls back to default session (no crash on construction)
    assert client is not None


def test_main_one_shot_local(tmp_path, monkeypatch):
    wet = tmp_path / "in.warc.wet.gz"
    _write_wet(wet, [("http://acme.com/", "ACME software")])
    task_json = tmp_path / "task.json"
    task_json.write_text(__import__("json").dumps({
        "crawl_id": "CC-MAIN-2026-25", "file_index": 1, "wet_path": str(wet),
        "output": {"kind": "local", "path": str(tmp_path / "out")}}))
    # inject a fake classifier so main() doesn't need a live embedding endpoint
    monkeypatch.setattr(worker, "load_classifier", lambda: FakeClassifier())
    worker.main(["--task", str(task_json)])
    assert (tmp_path / "out" / "CC-MAIN-2026-25" / "1.parquet").exists()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_worker.py::test_main_one_shot_local -v`
Expected: FAIL with `AttributeError: module 'commoncrawl_enrich.worker' has no attribute 'main'`.

- [ ] **Step 3: Implement `_make_s3`, `load_classifier`, `main`**

Append to `commoncrawl_enrich/worker.py`:

```python
def _make_s3():
    import boto3
    from botocore.config import Config
    profile = os.environ.get("AWS_PROFILE") or os.environ.get("CORPSCOUT_S3_PROFILE")
    session = boto3.session.Session(profile_name=profile) if profile else boto3.session.Session()
    return session.client(
        "s3", endpoint_url=os.environ.get("CORPSCOUT_S3_ENDPOINT"),
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}))


def load_classifier(refs_dir: str | None = None) -> PageClassifier:
    """Build the PageClassifier from baked reference .npz + env-driven embedding (+ optional LLM)."""
    refs = Path(refs_dir or os.environ.get("COMMONCRAWL_REFS_DIR", "data"))
    ref = nace_embed.NaceReference.load(str(refs / "nace_reference.npz"))
    protos = nace_embed.PrototypeSet.load(str(refs / "page_type_prototypes.npz"))
    embedder = nace_embed.EmbeddingClient.from_env()
    llm = None
    base = os.environ.get("COMMONCRAWL_LLM_BASE_URL")
    if base:
        from commoncrawl_enrich.llm import from_openai
        llm = from_openai(base_url=base, model=os.environ.get("COMMONCRAWL_LLM_BASE_MODEL") or "x",
                          api_key="x", enable_thinking=False)
    return PageClassifier(ref, protos, embedder, llm=llm)


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="Process one CommonCrawl WET file -> Parquet.")
    ap.add_argument("--task", required=True, help="path to a WetTask JSON file")
    args = ap.parse_args(argv)

    task = WetTask.from_json(json.loads(Path(args.task).read_text()))
    classifier = load_classifier()
    s3 = None
    if task.output.get("kind") == "s3" or task.wet_path.startswith("s3://"):
        s3 = _make_s3()
    stats = run_wet_task(task, classifier=classifier, s3=s3)
    print(json.dumps(stats, default=str))


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_worker.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add commoncrawl_enrich/worker.py tests/test_worker.py
git commit -m "feat: load_classifier + _make_s3 + one-shot worker CLI"
```

---

## Task 7: Benchmark script

**Files:**
- Create: `scripts/benchmark_wet.py`

- [ ] **Step 1: Write the script**

Create `scripts/benchmark_wet.py`:

```python
#!/usr/bin/env python
"""Benchmark one WET file end to end (worker.run_wet_task) and project to a full crawl.

Run (embed endpoint + reference npz required):
    set -a; . ./.env; set +a
    uv run python scripts/benchmark_wet.py --segment-index 0
"""
import argparse
import time

from commoncrawl_enrich import segment, worker


def _human(n: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TiB"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--segment-index", type=int, default=0)
    ap.add_argument("--out-dir", default="data/commoncrawl/benchmark")
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()

    crawl = segment.latest_crawl()
    wet_url = segment.first_wet_url(crawl=crawl, segment_index=args.segment_index)
    print(f"crawl={crawl}  wet={wet_url.rsplit('/', 1)[-1]}")

    t = time.monotonic()
    classifier = worker.load_classifier()
    print(f"loaded classifier in {time.monotonic()-t:.1f}s")

    task = worker.WetTask(
        crawl_id=crawl, file_index=args.segment_index, wet_path=wet_url,
        output={"kind": "local", "path": args.out_dir}, limit=args.limit)
    stats = worker.run_wet_task(task, classifier=classifier)

    rows, pbytes = stats["records"], stats["parquet_bytes"]
    print(f"download {stats['download_s']}s  process {stats['process_s']}s  "
          f"homepages={rows}  industries={stats['industries']}  "
          f"page_types={stats['page_types']}  with_email={stats['with_email']}")
    print(f"parquet {_human(pbytes)}  ({pbytes/max(rows,1):.0f} B/row)  -> {stats['output']}")
    print("\n=== projection: full crawl = 100,000 WET files ===")
    print(f"   process: {stats['process_s']*100000/86400:.1f} core-days single-stream")
    print(f"   parquet storage: {_human(pbytes*100000)}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify it imports cleanly**

Run: `uv run python -c "import ast; ast.parse(open('scripts/benchmark_wet.py').read()); print('ok')"`
Expected: `ok`.

- [ ] **Step 3: Commit**

```bash
git add scripts/benchmark_wet.py
git commit -m "feat: one-WET-file benchmark with full-crawl projection"
```

- [ ] **Step 4: (manual, requires live endpoints) run the benchmark**

Run: `set -a; . ./.env; set +a; uv run python scripts/benchmark_wet.py --segment-index 0`
Expected: prints download/process seconds, homepage count, Parquet size/row, and the ×100,000 projection. Record these numbers — they size Phase 2's worker count and storage budget. (No commit; this is a measurement.)

---

## Task 8: Dockerfile + run.sh

**Files:**
- Create: `corpscout/commoncrawl-worker/Dockerfile`
- Create: `corpscout/commoncrawl-worker/run.sh`
- Create: `corpscout/commoncrawl-worker/refs/.gitkeep`

- [ ] **Step 1: Create the refs placeholder**

```bash
mkdir -p corpscout/commoncrawl-worker/refs
touch corpscout/commoncrawl-worker/refs/.gitkeep
```

- [ ] **Step 2: Write the Dockerfile**

Create `corpscout/commoncrawl-worker/Dockerfile`. **Build context = `companycollect/corpscout`** (so the Dockerfile can reach both `dagster_v3/commoncrawl_enrich` and `commoncrawl-worker/refs/`). CI stages the `.npz` into `commoncrawl-worker/refs/` first (Task 9):

```dockerfile
FROM python:3.14-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 COMMONCRAWL_REFS_DIR=/refs
WORKDIR /app

# Runtime deps for the WET path only (no dlt/dagster/duckdb/clickhouse).
RUN pip install --no-cache-dir \
      "numpy>=2" "pyarrow>=16" "warcio>=1.7" "openai>=1.40" \
      "boto3>=1.34" "tldextract>=5" "lxml>=6" "requests>=2.31"

# The package (only commoncrawl_enrich is needed at runtime).
COPY dagster_v3/commoncrawl_enrich /app/commoncrawl_enrich
# Baked reference matrices (staged into commoncrawl-worker/refs/ by CI).
COPY commoncrawl-worker/refs/nace_reference.npz /refs/nace_reference.npz
COPY commoncrawl-worker/refs/page_type_prototypes.npz /refs/page_type_prototypes.npz

ENTRYPOINT ["python", "-m", "commoncrawl_enrich.worker"]
```

- [ ] **Step 3: Write run.sh**

Create `corpscout/commoncrawl-worker/run.sh`:

```bash
#!/usr/bin/env bash
# Pull the worker image and process one WET file described by a task JSON.
#   ./run.sh path/to/task.json
# Env: COMMONCRAWL_WORKER_IMAGE, COMMONCRAWL_EMBED_BASE_URL, CORPSCOUT_S3_ENDPOINT,
#      AWS_PROFILE / AWS creds, optional COMMONCRAWL_LLM_BASE_URL.
set -euo pipefail

TASK_FILE="${1:?usage: run.sh <task.json>}"
IMAGE="${COMMONCRAWL_WORKER_IMAGE:?set COMMONCRAWL_WORKER_IMAGE to the registry image}"

docker pull "$IMAGE"
docker run --rm \
  -e COMMONCRAWL_EMBED_BASE_URL \
  -e COMMONCRAWL_EMBED_MODEL \
  -e COMMONCRAWL_LLM_BASE_URL \
  -e COMMONCRAWL_LLM_BASE_MODEL \
  -e CORPSCOUT_S3_ENDPOINT \
  -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_PROFILE \
  -v "$(cd "$(dirname "$TASK_FILE")" && pwd):/work" \
  -v "${COMMONCRAWL_OUT_DIR:-$PWD/out}:/out" \
  "$IMAGE" --task "/work/$(basename "$TASK_FILE")"
```

- [ ] **Step 4: Make run.sh executable + verify Dockerfile parses**

```bash
chmod +x corpscout/commoncrawl-worker/run.sh
docker build --help >/dev/null && echo "docker present"
```
Expected: `docker present` (a full build needs the `.npz`; that happens in CI — Task 9).

- [ ] **Step 5: Commit**

```bash
git add corpscout/commoncrawl-worker/Dockerfile corpscout/commoncrawl-worker/run.sh \
        corpscout/commoncrawl-worker/refs/.gitkeep
git commit -m "feat: commoncrawl-worker Dockerfile + run.sh"
```

---

## Task 9: GitHub CI — build + push the image

**Files:**
- Create: `.github/workflows/commoncrawl-worker.yml`

- [ ] **Step 1: Write the workflow**

Create `commoncrawl-worker.yml` under the **git repo's** `.github/workflows/` (find it with `git rev-parse --show-toplevel`). It stages the baked `.npz` into the build context, then builds + pushes to GHCR:

```yaml
name: commoncrawl-worker

on:
  push:
    branches: [main]
    paths:
      - "companycollect/corpscout/dagster_v3/commoncrawl_enrich/**"
      - "companycollect/corpscout/commoncrawl-worker/**"
      - ".github/workflows/commoncrawl-worker.yml"
  workflow_dispatch: {}

env:
  IMAGE: ghcr.io/${{ github.repository }}/commoncrawl-worker

jobs:
  build:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      packages: write
    steps:
      - uses: actions/checkout@v4
        with:
          lfs: true
      - name: Stage reference matrices into the build context
        working-directory: companycollect/corpscout
        run: |
          cp dagster_v3/data/nace_reference.npz commoncrawl-worker/refs/nace_reference.npz
          cp dagster_v3/data/commoncrawl/page_type_prototypes.npz commoncrawl-worker/refs/page_type_prototypes.npz
      - uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}
      - uses: docker/build-push-action@v6
        with:
          context: companycollect/corpscout
          file: companycollect/corpscout/commoncrawl-worker/Dockerfile
          push: true
          tags: ${{ env.IMAGE }}:latest,${{ env.IMAGE }}:${{ github.sha }}
```

The build `context: companycollect/corpscout` matches the Dockerfile COPY paths from Task 8 (`dagster_v3/commoncrawl_enrich`, `commoncrawl-worker/refs/*.npz`). The `paths:`/context assume the git repo root is the monorepo root (`pulsarpoint/ppoint`); if `git rev-parse --show-toplevel` is `companycollect`, drop the `companycollect/` prefix from the workflow paths/context.

- [ ] **Step 2: Validate YAML**

Run: `ROOT=$(git rev-parse --show-toplevel); uv run python -c "import yaml; yaml.safe_load(open('$ROOT/.github/workflows/commoncrawl-worker.yml')); print('ok')"`
Expected: `ok`.

- [ ] **Step 3: Commit**

```bash
ROOT=$(git rev-parse --show-toplevel)
git add "$ROOT/.github/workflows/commoncrawl-worker.yml"
git commit -m "ci: build + push commoncrawl-worker image to GHCR"
```

---

## Task 10: Full verification

- [ ] **Step 1: Run the whole worker + ingest suite**

Run: `uv run pytest tests/test_worker.py tests/test_ingest.py tests/test_classifier.py tests/test_nace_embed.py -q`
Expected: all pass.

- [ ] **Step 2: Verify Dagster defs still load (package import sanity)**

Run: `uv run dg check defs`
Expected: `All definitions loaded successfully.`

- [ ] **Step 3: (manual) end-to-end one-shot against a real file**

```bash
set -a; . ./.env; set +a
cat > /tmp/task.json <<'JSON'
{"crawl_id":"CC-MAIN-2026-25","file_index":0,
 "wet_path":"https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-25/segments/.../wet/....warc.wet.gz",
 "output":{"kind":"local","path":"data/commoncrawl/benchmark"},"limit":200}
JSON
uv run python -m commoncrawl_enrich.worker --task /tmp/task.json
```
(Resolve a real `wet_path` with `uv run python -c "from commoncrawl_enrich import segment; print(segment.first_wet_url())"`.)
Expected: prints a JSON stats line; the Parquet exists at `data/commoncrawl/benchmark/CC-MAIN-2026-25/0.parquet`.

- [ ] **Step 4: No commit** (verification only).

---

## Notes for the implementer

- All unit tests are offline (warcio fixtures + fake classifier/S3); only the benchmark (Task 7 step 4) and Task 10 step 3 need live endpoints (`COMMONCRAWL_EMBED_BASE_URL`, optionally `COMMONCRAWL_LLM_BASE_URL`, `CORPSCOUT_S3_ENDPOINT`).
- Commit by explicit path only — the tree carries unrelated WIP.
- The `.npz` files live under `data/` (gitignored). They are **not** committed; CI copies them into the image build context. If CI runs where `data/` is absent, generate them first (the `commoncrawl_classify` assets / the calibrate script produce them) or pull from S3 — out of scope for this plan; assume they exist in the CI checkout for now.
