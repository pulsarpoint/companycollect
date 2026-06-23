# CommonCrawl Package Split + WET Worker (Phase 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split the `commoncrawl_enrich` spike into a shared core + per-service packages, then build a stateless, dockerized **WET worker** (WET S3 path in → one Parquet of `commoncrawl_domains` rows out, local or S3).

**Architecture:** Retire the dead Phase-0 `enrich`/`run` island, then carve the live code into `commoncrawl_enrich` (shared primitives: `segment`, `extract`, `models`, `ico`), `wet_processing` (industry stack: `nace_embed`, `page_types`, `classifier`, `llm` + `domains.py` + `worker.py`), and `warc_processing` (`tech`, `wappalyzer_client` + `pages.py`). Then add the WET worker on `wet_processing`. NATS `serve` mode + orchestrator + ledger are Phase 2 (separate plan).

**Tech Stack:** Python 3.14, `uv`, pyarrow, warcio, boto3, openai, numpy, lxml, tldextract; Docker; GitHub Actions; hatchling packaging.

---

## Part A — Restructure (mechanical; validated by the existing test suite staying green)

The blast radius (measured): legacy island imported only by itself + its 6 tests; live importers of the moved clusters are `commoncrawl_classify/{build,assets}.py`, `scripts/{spike_nace_embed,mine_parked,calibrate_page_types}.py`, and the listed tests. After each task run the **full** suite; nothing new should fail.

### Task A1: Retire the dead Phase-0 island

**Files (delete):** `commoncrawl_enrich/{enrich,run,warc,index_client,parquet_out,metrics}.py`, `tests/test_commoncrawl_enrich_{enrich,run,warc,index,parquet,metrics}.py`

- [ ] **Step 1: Confirm nothing live imports them**

Run: `grep -rlE "commoncrawl_enrich\.(enrich|run|warc|index_client|parquet_out|metrics)|from commoncrawl_enrich import (enrich|run|warc|index_client|parquet_out|metrics)" src scripts commoncrawl_enrich | grep -vE "/(run|enrich|warc|index_client|parquet_out|metrics)\.py$"`
Expected: no output (only the island's own files reference each other).

- [ ] **Step 2: Delete the modules + their tests**

```bash
git rm commoncrawl_enrich/enrich.py commoncrawl_enrich/run.py commoncrawl_enrich/warc.py \
       commoncrawl_enrich/index_client.py commoncrawl_enrich/parquet_out.py commoncrawl_enrich/metrics.py \
       tests/test_commoncrawl_enrich_enrich.py tests/test_commoncrawl_enrich_run.py \
       tests/test_commoncrawl_enrich_warc.py tests/test_commoncrawl_enrich_index.py \
       tests/test_commoncrawl_enrich_parquet.py tests/test_commoncrawl_enrich_metrics.py
```

- [ ] **Step 3: Run the full suite**

Run: `uv run pytest tests/ -q`
Expected: PASS (fewer tests; no import errors). If a `tests/conftest.py` or another file imports a deleted module, remove that reference.

- [ ] **Step 4: Commit**

```bash
git add -A commoncrawl_enrich/ tests/
git commit -m "chore: retire dead Phase-0 enrich/run island (superseded by WET/WARC processing)"
```

### Task A2: Clean `segment.py` to a pure primitive

**Files:** Modify `commoncrawl_enrich/segment.py`

- [ ] **Step 1: Remove the legacy higher-level functions + their imports**

In `commoncrawl_enrich/segment.py`, delete `process_wet_file`, `process_warc_file`, the `_WET_SCHEMA` / `_WARC_SCHEMA` pyarrow schema constants, and the now-unused imports `from concurrent.futures import ThreadPoolExecutor`, `import pyarrow as pa`, `import pyarrow.parquet as pq`, `from commoncrawl_enrich import extract, tech`, `from commoncrawl_enrich.llm import LLMArm`. Keep: `logging`, `urllib.parse.urlparse`, `import requests`, `import tldextract`, `from warcio.archiveiterator import ArchiveIterator`, and the helpers `wet_url_to_warc`, `latest_crawl`, `first_wet_url`, `_open_stream`, `_host`, `_is_homepage`, plus `USER_AGENT`, `DATA_HOST`, `COLLINFO_URL`, `_TE`.

- [ ] **Step 2: Verify segment imports nothing service-specific**

Run: `grep -nE "import" commoncrawl_enrich/segment.py | grep -E "tech|llm|extract|pyarrow|ThreadPool"`
Expected: no output.

- [ ] **Step 3: Run the full suite**

Run: `uv run pytest tests/ -q`
Expected: PASS. (The only callers of the removed functions were the deleted island + `scripts/commoncrawl_process_one.py`, deleted in Task A6.)

- [ ] **Step 4: Commit**

```bash
git add commoncrawl_enrich/segment.py
git commit -m "refactor: reduce segment.py to pure stream/host/crawl primitives"
```

### Task A3: Create `warc_processing` (move `tech`, `wappalyzer_client`; split WARC IO)

**Files:**
- Create dir `warc_processing/` with `__init__.py`
- Move: `commoncrawl_enrich/tech.py` → `warc_processing/tech.py`; `commoncrawl_enrich/wappalyzer_client.py` → `warc_processing/wappalyzer_client.py`
- Create: `warc_processing/pages.py` (WARC half of `ingest.py`)
- Modify: `pyproject.toml`, `tests/test_commoncrawl_enrich_tech.py`, `tests/test_wappalyzer_client.py`

- [ ] **Step 1: Move the modules + add the package**

```bash
mkdir -p warc_processing
printf '"""WARC-file processing service: per-page technologies + page signals."""\n' > warc_processing/__init__.py
git mv commoncrawl_enrich/tech.py warc_processing/tech.py
git mv commoncrawl_enrich/wappalyzer_client.py warc_processing/wappalyzer_client.py
```

- [ ] **Step 2: Fix internal import in `warc_processing/tech.py`**

In `warc_processing/tech.py`, the lazy import inside `_wappalyzer_client` changes package:

```python
from warc_processing.wappalyzer_client import WappalyzerClient
```

(`from commoncrawl_enrich.models import Technology` stays — `models` is core.)

- [ ] **Step 3: Create `warc_processing/pages.py` from the WARC half of `ingest.py`**

Move `process_warc_to_clickhouse`, `_headers_multidict`, `TECHNOLOGIES_TABLE`, `PAGE_SIGNALS_TABLE`, `TECHNOLOGIES_COLUMNS`, `PAGE_SIGNALS_COLUMNS`, and a private `_insert` into `warc_processing/pages.py`. Header + imports:

```python
"""Process one WARC file -> per-page technologies + page signals, appended to ClickHouse."""
from datetime import datetime, timezone

from warcio.archiveiterator import ArchiveIterator

from commoncrawl_enrich import extract, segment

TECHNOLOGIES_TABLE = "corpscout.commoncrawl_technologies"
PAGE_SIGNALS_TABLE = "corpscout.commoncrawl_page_signals"
TECHNOLOGIES_COLUMNS = (
    "crawl_id", "url", "root_domain", "subdomain", "technology", "category",
    "version", "confidence", "source_url", "source_run_id", "resolved_at",
)
PAGE_SIGNALS_COLUMNS = (
    "crawl_id", "url", "root_domain", "subdomain", "emails", "social_platforms",
    "source_url", "source_run_id", "resolved_at",
)


def _insert(ch_client, table, columns, rows, batch_size):
    if not rows:
        return 0
    col_sql = ", ".join(columns)
    for start in range(0, len(rows), batch_size):
        ch_client.execute(f"INSERT INTO {table} ({col_sql}) VALUES", rows[start:start + batch_size])
    return len(rows)
```

Then paste the existing `_headers_multidict` and `process_warc_to_clickhouse` bodies verbatim from `ingest.py`, changing only the lazy wappalyzer import line inside `process_warc_to_clickhouse` to:

```python
    from warc_processing.wappalyzer_client import WappalyzerClient
```

- [ ] **Step 4: Repoint the tech/wappalyzer tests**

In `tests/test_commoncrawl_enrich_tech.py`: replace `commoncrawl_enrich.tech` / `from commoncrawl_enrich import tech` with `warc_processing.tech` / `from warc_processing import tech` (and any `from commoncrawl_enrich.tech import …`). In `tests/test_wappalyzer_client.py`: replace `from commoncrawl_enrich.wappalyzer_client import WappalyzerClient` with `from warc_processing.wappalyzer_client import WappalyzerClient`.

```bash
git mv tests/test_commoncrawl_enrich_tech.py tests/test_warc_tech.py
git mv tests/test_wappalyzer_client.py tests/test_warc_wappalyzer_client.py
```
Then apply the import edits above to the renamed files.

- [ ] **Step 5: Register the package**

In `pyproject.toml`, add `"warc_processing"` to `[tool.hatch.build.targets.wheel] packages`:

```toml
packages = ["src/dagster_v3", "translations", "temporal", "exchange_rates", "commoncrawl_enrich", "warc_processing", "wet_processing"]
```
(Adding `wet_processing` now too — created in A4.)

- [ ] **Step 6: Run the suite**

Run: `uv run pytest tests/test_warc_tech.py tests/test_warc_wappalyzer_client.py -q`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add -A warc_processing/ tests/test_warc_tech.py tests/test_warc_wappalyzer_client.py pyproject.toml commoncrawl_enrich/
git commit -m "refactor: move tech + wappalyzer + WARC IO into warc_processing"
```

### Task A4: Create `wet_processing` (move industry stack; split WET IO)

**Files:**
- Create dir `wet_processing/` with `__init__.py`
- Move: `nace_embed.py`, `page_types.py`, `classifier.py`, `llm.py` → `wet_processing/`
- Create: `wet_processing/domains.py` (WET half of `ingest.py`); delete `commoncrawl_enrich/ingest.py`
- Modify importers: `src/dagster_v3/defs/commoncrawl_classify/{build,assets}.py`, tests, scripts

- [ ] **Step 1: Move the modules + add the package**

```bash
mkdir -p wet_processing
printf '"""WET-file processing service: industry classification + emails per homepage."""\n' > wet_processing/__init__.py
git mv commoncrawl_enrich/nace_embed.py wet_processing/nace_embed.py
git mv commoncrawl_enrich/page_types.py wet_processing/page_types.py
git mv commoncrawl_enrich/classifier.py wet_processing/classifier.py
git mv commoncrawl_enrich/llm.py wet_processing/llm.py
```

- [ ] **Step 2: Fix internal imports in the moved modules**

- `wet_processing/classifier.py`: `from commoncrawl_enrich import nace_embed, page_types` → `from wet_processing import nace_embed, page_types`; `from commoncrawl_enrich.llm import LLMArm` → `from wet_processing.llm import LLMArm`.
- `wet_processing/page_types.py`: keep `from commoncrawl_enrich import segment` (segment is core).
- `wet_processing/llm.py`: keep `from commoncrawl_enrich.extract import _to_e164` and `from commoncrawl_enrich.models import …` (core).
- `wet_processing/nace_embed.py`: no internal imports — no change.

- [ ] **Step 3: Create `wet_processing/domains.py` from the WET half of `ingest.py`**

```python
"""Process one WET file -> commoncrawl_domains rows (industry + page_type + emails + top-3 audit)."""
from datetime import datetime, timezone

from commoncrawl_enrich import extract, segment
from wet_processing.classifier import PageClassifier

DOMAINS_TABLE = "corpscout.commoncrawl_domains"
DOMAINS_COLUMNS = (
    "crawl_id", "url", "root_domain", "subdomain", "emails", "email_count",
    "page_type", "page_type_score", "nace_code", "nace_label", "nace_division",
    "nace_confident", "nace_margin", "nace_score", "nace_method",
    "nace_top3_codes", "nace_top3_labels", "nace_top3_scores",
    "source_url", "source_run_id", "resolved_at",
)


def _insert(ch_client, table, columns, rows, batch_size):
    if not rows:
        return 0
    col_sql = ", ".join(columns)
    for start in range(0, len(rows), batch_size):
        ch_client.execute(f"INSERT INTO {table} ({col_sql}) VALUES", rows[start:start + batch_size])
    return len(rows)


def _domain_stats(rows: list) -> dict:
    return {
        "records": len(rows),
        "with_email": sum(1 for r in rows if r[4]),
        "page_types": sum(1 for r in rows if r[6]),
        "industries": sum(1 for r in rows if r[8]),
    }
```

Then paste the existing `process_wet_to_clickhouse` body verbatim from `ingest.py` into `domains.py` (it already references `extract`, `segment`, `PageClassifier`, `DOMAINS_*`, `_insert`, `_domain_stats`). Delete `ingest.py`:

```bash
git rm commoncrawl_enrich/ingest.py
```

- [ ] **Step 4: Repoint live importers**

- `src/dagster_v3/defs/commoncrawl_classify/build.py`: `from commoncrawl_enrich import nace_embed` → `from wet_processing import nace_embed`.
- `src/dagster_v3/defs/commoncrawl_classify/assets.py`: `from commoncrawl_enrich import nace_embed` → `from wet_processing import nace_embed`.
- `scripts/spike_nace_embed.py`, `scripts/mine_parked.py`, `scripts/calibrate_page_types.py`: change `nace_embed`/`page_types`/`classifier` imports to `wet_processing`; keep `segment` as `commoncrawl_enrich`.

- [ ] **Step 5: Repoint + split the tests**

```bash
git mv tests/test_nace_embed.py tests/test_wet_nace_embed.py
git mv tests/test_classifier.py tests/test_wet_classifier.py
git mv tests/test_page_types.py tests/test_wet_page_types.py
git mv tests/test_commoncrawl_enrich_llm.py tests/test_wet_llm.py
git mv tests/test_ingest.py tests/test_wet_domains.py
```
In each renamed WET test, replace `commoncrawl_enrich.{nace_embed,page_types,classifier,llm}` and `from commoncrawl_enrich import {nace_embed,page_types}` with the `wet_processing` equivalents; keep `from commoncrawl_enrich.models import …` and `from commoncrawl_enrich import segment`. In `tests/test_wet_domains.py`, change `from commoncrawl_enrich import ingest` → `from wet_processing import domains as ingest` (the alias keeps the WET test bodies unchanged), and **cut** the WARC test (`test_process_warc_to_clickhouse_writes_tech_and_signals`) — it moves to a new file. Create `tests/test_warc_pages.py` holding that cut WARC test plus the warcio `_write_wet`/`_write_warc` helpers and the `FakeWappalyzer`/`FakeCH` classes (copy them from the old `test_ingest.py`), with `from warc_processing import pages` and calling `pages.process_warc_to_clickhouse`.

- [ ] **Step 6: Run the full suite + Dagster defs**

Run: `uv run pytest tests/ -q && uv run dg check defs`
Expected: PASS and `All definitions loaded successfully.`

- [ ] **Step 7: Commit**

```bash
git add -A wet_processing/ commoncrawl_enrich/ src/dagster_v3/defs/commoncrawl_classify/ scripts/ tests/
git commit -m "refactor: move industry stack + WET IO into wet_processing; split tests"
```

### Task A5: Delete the superseded one-file script + refresh README

**Files:** delete `scripts/commoncrawl_process_one.py`; modify `commoncrawl_enrich/README.md`

- [ ] **Step 1: Delete the superseded script**

```bash
git rm scripts/commoncrawl_process_one.py
```

- [ ] **Step 2: Update the README to describe the three packages**

Replace the top of `commoncrawl_enrich/README.md` so it states: `commoncrawl_enrich` is the shared core (`segment`, `extract`, `models`, `ico`); `wet_processing` is the WET service (industry classification + `domains`/`worker`); `warc_processing` is the WARC service (`tech`/`wappalyzer_client`/`pages`).

- [ ] **Step 3: Run the suite**

Run: `uv run pytest tests/ -q`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add scripts/ commoncrawl_enrich/README.md
git commit -m "chore: drop superseded one-file script; document the 3-package split"
```

---

## Part B — WET worker (TDD on `wet_processing`)

### Task B1: `build_wet_domain_rows` (extract the shared row-builder)

**Files:** Modify `wet_processing/domains.py`; Test `tests/test_wet_domains.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_wet_domains.py` (the file uses `from wet_processing import domains as ingest` and the warcio `_write_wet`/`FakeClassifier`/`RESOLVED` helpers from the split):

```python
def test_build_wet_domain_rows(tmp_path):
    wet = tmp_path / "x.warc.wet.gz"
    _write_wet(wet, [
        ("http://acme.com/", "ACME makes software. Contact info@acme.com"),
        ("http://acme.com/about", "deep page"),                 # filtered (homepages_only)
        ("http://shop.example.org/", "online shop"),
    ])
    rows = ingest.build_wet_domain_rows(
        str(wet), classifier=FakeClassifier(), crawl_id="CC-MAIN-2026-25", resolved_at=RESOLVED)
    assert len(rows) == 2 and all(len(r) == len(ingest.DOMAINS_COLUMNS) for r in rows)
    acme = next(r for r in rows if r[2] == "acme.com")
    assert acme[1] == "http://acme.com/" and acme[4] == ["info@acme.com"] and acme[8] == "62.01"
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_wet_domains.py::test_build_wet_domain_rows -v`
Expected: FAIL with `AttributeError: … has no attribute 'build_wet_domain_rows'`.

- [ ] **Step 3: Extract the builder in `wet_processing/domains.py`**

Replace `process_wet_to_clickhouse` with the builder + a thin sink:

```python
def build_wet_domain_rows(
    source: str, *, classifier: PageClassifier, crawl_id: str,
    source_url: str | None = None, source_run_id: str = "", resolved_at: datetime | None = None,
    limit: int | None = None, session=None, homepages_only: bool = True,
) -> list[tuple]:
    """Stream one WET file -> commoncrawl_domains rows. Shared by the CH and Parquet sinks."""
    from warcio.archiveiterator import ArchiveIterator
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
    rows = build_wet_domain_rows(
        source, classifier=classifier, crawl_id=crawl_id, source_url=source_url,
        source_run_id=source_run_id, resolved_at=resolved_at, limit=limit,
        session=session, homepages_only=homepages_only)
    _insert(ch_client, DOMAINS_TABLE, DOMAINS_COLUMNS, rows, batch_size)
    return _domain_stats(rows)
```

- [ ] **Step 4: Run the WET domain tests**

Run: `uv run pytest tests/test_wet_domains.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add wet_processing/domains.py tests/test_wet_domains.py
git commit -m "refactor: extract build_wet_domain_rows in wet_processing.domains"
```

### Task B2: Parquet schema + writer

**Files:** Modify `wet_processing/domains.py`; Test `tests/test_wet_domains.py`

- [ ] **Step 1: Write the failing test**

```python
def test_write_domain_rows_parquet_roundtrip(tmp_path):
    import pyarrow.parquet as pq
    wet = tmp_path / "x.warc.wet.gz"
    _write_wet(wet, [("http://acme.com/", "ACME software, info@acme.com")])
    rows = ingest.build_wet_domain_rows(
        str(wet), classifier=FakeClassifier(), crawl_id="CC-MAIN-2026-25", resolved_at=RESOLVED)
    out = tmp_path / "acme.parquet"
    assert ingest.write_domain_rows_parquet(rows, out) == 1
    table = pq.read_table(out)
    assert table.schema.equals(ingest.DOMAINS_PARQUET_SCHEMA)
    assert table.column_names == list(ingest.DOMAINS_COLUMNS)
    assert table.to_pylist()[0]["nace_top3_codes"] == ["62.01", "62.09", "47.11"]


def test_write_domain_rows_parquet_empty(tmp_path):
    import pyarrow.parquet as pq
    out = tmp_path / "empty.parquet"
    assert ingest.write_domain_rows_parquet([], out) == 0
    assert pq.read_table(out).schema.equals(ingest.DOMAINS_PARQUET_SCHEMA)
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_wet_domains.py::test_write_domain_rows_parquet_roundtrip -v`
Expected: FAIL with `AttributeError: … 'DOMAINS_PARQUET_SCHEMA'`.

- [ ] **Step 3: Add schema + writer to `wet_processing/domains.py`**

Add the pyarrow imports at the top (`import pyarrow as pa` / `import pyarrow.parquet as pq`), then:

```python
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

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/test_wet_domains.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add wet_processing/domains.py tests/test_wet_domains.py
git commit -m "feat: DOMAINS_PARQUET_SCHEMA + write_domain_rows_parquet"
```

### Task B3: `WetTask` config object

**Files:** Create `wet_processing/worker.py`; Test `tests/test_wet_worker.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_wet_worker.py`:

```python
import pytest

from wet_processing.worker import WetTask


def test_wettask_from_json_local():
    t = WetTask.from_json({"crawl_id": "CC-MAIN-2026-25", "file_index": 42,
                           "wet_path": "s3://crawls/wet/x.warc.wet.gz",
                           "output": {"kind": "local", "path": "data/out"}})
    assert t.crawl_id == "CC-MAIN-2026-25" and t.file_index == 42 and t.limit is None


def test_wettask_from_json_coerces_index_and_limit():
    t = WetTask.from_json({"crawl_id": "C", "file_index": "7",
                           "wet_path": "https://data.commoncrawl.org/x.gz",
                           "output": {"kind": "s3", "bucket": "r", "prefix": "wet"}, "limit": 100})
    assert t.file_index == 7 and t.limit == 100


def test_wettask_missing_field_raises():
    with pytest.raises(KeyError):
        WetTask.from_json({"crawl_id": "C", "file_index": 1})
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_wet_worker.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'wet_processing.worker'`.

- [ ] **Step 3: Implement `WetTask`**

Create `wet_processing/worker.py`:

```python
"""Stateless WET worker: one WET file -> one Parquet of commoncrawl_domains rows.

Phase 1: a one-shot CLI (`python -m wet_processing.worker --task task.json`). Phase 2 adds a
NATS `serve` mode that reuses `run_wet_task` unchanged.
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

from commoncrawl_enrich import segment
from wet_processing import domains, nace_embed
from wet_processing.classifier import PageClassifier


@dataclass(frozen=True)
class WetTask:
    crawl_id: str
    file_index: int
    wet_path: str          # s3://bucket/key | https://data.commoncrawl.org/... | local path
    output: dict           # {"kind":"local","path":...} | {"kind":"s3","bucket":...,"prefix":...}
    limit: int | None = None

    @classmethod
    def from_json(cls, data: dict) -> "WetTask":
        return cls(crawl_id=data["crawl_id"], file_index=int(data["file_index"]),
                   wet_path=data["wet_path"], output=data["output"], limit=data.get("limit"))
```

- [ ] **Step 4: Run the test**

Run: `uv run pytest tests/test_wet_worker.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add wet_processing/worker.py tests/test_wet_worker.py
git commit -m "feat: WetTask config object for the WET worker"
```

### Task B4: `run_wet_task` (local + S3 source/output)

**Files:** Modify `wet_processing/worker.py`; Test `tests/test_wet_worker.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_wet_worker.py`:

```python
from datetime import datetime, timezone
from io import BytesIO

import pyarrow.parquet as pq
from warcio.warcwriter import WARCWriter

from wet_processing import worker
from wet_processing.classifier import IndustryResult

RESOLVED = datetime(2026, 6, 23, tzinfo=timezone.utc)


class FakeClassifier:
    def classify(self, texts):
        return [IndustryResult(
            nace_code="62.01", nace_label="Programming", nace_division="62",
            nace_confident=True, nace_score=0.8, nace_margin=0.2,
            nace_top3=["62.01", "62.09", "47.11"], nace_top3_labels=["a", "b", "c"],
            nace_top3_scores=[0.8, 0.5, 0.3], method="embedding") for _ in texts]


class FakeS3:
    def __init__(self, files=None):
        self._files = files or {}
        self.put_calls = []

    def download_file(self, bucket, key, dest):
        import shutil
        shutil.copyfile(self._files[(bucket, key)], dest)

    def put_object(self, Bucket, Key, Body):
        self.put_calls.append((Bucket, Key, Body.read()))


def _write_wet(path, pages):
    with open(path, "wb") as fh:
        w = WARCWriter(fh, gzip=True)
        for uri, text in pages:
            w.write_record(w.create_warc_record(
                uri, "conversion", payload=BytesIO(text.encode()), warc_content_type="text/plain"))


def test_run_wet_task_local_to_local(tmp_path):
    wet = tmp_path / "in.warc.wet.gz"
    _write_wet(wet, [("http://acme.com/", "ACME software, info@acme.com")])
    task = worker.WetTask(crawl_id="CC-MAIN-2026-25", file_index=42, wet_path=str(wet),
                          output={"kind": "local", "path": str(tmp_path / "out")})
    stats = worker.run_wet_task(task, classifier=FakeClassifier())
    out = tmp_path / "out" / "CC-MAIN-2026-25" / "42.parquet"
    assert out.exists() and stats["records"] == 1 and stats["parquet_bytes"] > 0
    assert pq.read_table(out).to_pylist()[0]["nace_code"] == "62.01"


def test_run_wet_task_s3_output(tmp_path):
    wet = tmp_path / "in.warc.wet.gz"
    _write_wet(wet, [("http://acme.com/", "ACME software")])
    s3 = FakeS3()
    task = worker.WetTask(crawl_id="CC-MAIN-2026-25", file_index=7, wet_path=str(wet),
                          output={"kind": "s3", "bucket": "results", "prefix": "wet/"})
    stats = worker.run_wet_task(task, classifier=FakeClassifier(), s3=s3)
    bucket, key, body = s3.put_calls[0]
    assert key == "wet/CC-MAIN-2026-25/7.parquet" and body[:4] == b"PAR1"
    assert stats["output"] == "s3://results/wet/CC-MAIN-2026-25/7.parquet"


def test_run_wet_task_s3_source(tmp_path):
    wet = tmp_path / "remote.warc.wet.gz"
    _write_wet(wet, [("http://acme.com/", "ACME software")])
    s3 = FakeS3(files={("crawls", "wet/9.warc.wet.gz"): str(wet)})
    task = worker.WetTask(crawl_id="C", file_index=9, wet_path="s3://crawls/wet/9.warc.wet.gz",
                          output={"kind": "local", "path": str(tmp_path / "out")})
    stats = worker.run_wet_task(task, classifier=FakeClassifier(), s3=s3)
    assert (tmp_path / "out" / "C" / "9.parquet").exists() and stats["records"] == 1
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_wet_worker.py::test_run_wet_task_local_to_local -v`
Expected: FAIL with `AttributeError: … 'run_wet_task'`.

- [ ] **Step 3: Implement `run_wet_task` + helpers**

Append to `wet_processing/worker.py`:

```python
def _resolve_source(wet_path: str, *, s3, session, dest: str) -> tuple[str, bool]:
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
    return wet_path, False


def _emit_output(parquet_tmp: str, task: "WetTask", *, s3) -> str:
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
        rows = domains.build_wet_domain_rows(
            src, classifier=classifier, crawl_id=task.crawl_id,
            source_url=task.wet_path, limit=task.limit)
        domains.write_domain_rows_parquet(rows, pq_tmp)
        timings["process_s"] = round(time.monotonic() - t, 2)
        location = _emit_output(pq_tmp, task, s3=s3)
        stats = domains._domain_stats(rows)
        stats.update(parquet_bytes=Path(pq_tmp).stat().st_size, output=location, **timings)
        return stats
    finally:
        if downloaded and Path(wet_tmp).exists():
            os.unlink(wet_tmp)
        if Path(pq_tmp).exists():
            os.unlink(pq_tmp)
```

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/test_wet_worker.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add wet_processing/worker.py tests/test_wet_worker.py
git commit -m "feat: run_wet_task (local + S3 source/output)"
```

### Task B5: `load_classifier` + `_make_s3` + one-shot CLI

**Files:** Modify `wet_processing/worker.py`; Test `tests/test_wet_worker.py`

- [ ] **Step 1: Write the failing test**

```python
def test_main_one_shot_local(tmp_path, monkeypatch):
    import json
    wet = tmp_path / "in.warc.wet.gz"
    _write_wet(wet, [("http://acme.com/", "ACME software")])
    task_json = tmp_path / "task.json"
    task_json.write_text(json.dumps({
        "crawl_id": "CC-MAIN-2026-25", "file_index": 1, "wet_path": str(wet),
        "output": {"kind": "local", "path": str(tmp_path / "out")}}))
    monkeypatch.setattr(worker, "load_classifier", lambda: FakeClassifier())
    worker.main(["--task", str(task_json)])
    assert (tmp_path / "out" / "CC-MAIN-2026-25" / "1.parquet").exists()
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_wet_worker.py::test_main_one_shot_local -v`
Expected: FAIL with `AttributeError: … 'main'`.

- [ ] **Step 3: Implement `_make_s3`, `load_classifier`, `main`**

Append to `wet_processing/worker.py`:

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
    refs = Path(refs_dir or os.environ.get("COMMONCRAWL_REFS_DIR", "data"))
    ref = nace_embed.NaceReference.load(str(refs / "nace_reference.npz"))
    protos = nace_embed.PrototypeSet.load(str(refs / "page_type_prototypes.npz"))
    embedder = nace_embed.EmbeddingClient.from_env()
    llm = None
    base = os.environ.get("COMMONCRAWL_LLM_BASE_URL")
    if base:
        from wet_processing.llm import from_openai
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

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/test_wet_worker.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add wet_processing/worker.py tests/test_wet_worker.py
git commit -m "feat: load_classifier + _make_s3 + one-shot worker CLI"
```

### Task B6: Benchmark script

**Files:** Create `scripts/benchmark_wet.py`

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

from commoncrawl_enrich import segment
from wet_processing import worker


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

    task = worker.WetTask(crawl_id=crawl, file_index=args.segment_index, wet_path=wet_url,
                          output={"kind": "local", "path": args.out_dir}, limit=args.limit)
    stats = worker.run_wet_task(task, classifier=classifier)
    rows, pbytes = stats["records"], stats["parquet_bytes"]
    print(f"download {stats['download_s']}s  process {stats['process_s']}s  homepages={rows}  "
          f"industries={stats['industries']}  page_types={stats['page_types']}  "
          f"with_email={stats['with_email']}")
    print(f"parquet {_human(pbytes)}  ({pbytes/max(rows,1):.0f} B/row)  -> {stats['output']}")
    print("\n=== projection: full crawl = 100,000 WET files ===")
    print(f"   process: {stats['process_s']*100000/86400:.1f} core-days single-stream")
    print(f"   parquet storage: {_human(pbytes*100000)}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify it parses**

Run: `uv run python -c "import ast; ast.parse(open('scripts/benchmark_wet.py').read()); print('ok')"`
Expected: `ok`.

- [ ] **Step 3: Commit**

```bash
git add scripts/benchmark_wet.py
git commit -m "feat: one-WET-file benchmark with full-crawl projection"
```

- [ ] **Step 4: (manual, live endpoints) run the benchmark**

Run: `set -a; . ./.env; set +a; uv run python scripts/benchmark_wet.py --segment-index 0`
Expected: prints download/process seconds, homepage count, Parquet size/row, ×100,000 projection. Record these — they size Phase 2. No commit.

### Task B7: Dockerfile + run.sh

**Files:** Create `corpscout/commoncrawl-worker/{Dockerfile,run.sh,refs/.gitkeep}`

- [ ] **Step 1: Create the refs placeholder**

```bash
mkdir -p corpscout/commoncrawl-worker/refs
touch corpscout/commoncrawl-worker/refs/.gitkeep
```

- [ ] **Step 2: Write the Dockerfile** (build context = `companycollect/corpscout`)

Create `corpscout/commoncrawl-worker/Dockerfile`:

```dockerfile
FROM python:3.14-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 COMMONCRAWL_REFS_DIR=/refs PYTHONPATH=/app
WORKDIR /app

# WET-path runtime deps only (no dlt/dagster/duckdb/clickhouse).
RUN pip install --no-cache-dir \
      "numpy>=2" "pyarrow>=16" "warcio>=1.7" "openai>=1.40" \
      "boto3>=1.34" "tldextract>=5" "lxml>=6" "requests>=2.31"

# Shared core + the WET service package.
COPY dagster_v3/commoncrawl_enrich /app/commoncrawl_enrich
COPY dagster_v3/wet_processing /app/wet_processing
# Baked reference matrices (staged into commoncrawl-worker/refs/ by CI).
COPY commoncrawl-worker/refs/nace_reference.npz /refs/nace_reference.npz
COPY commoncrawl-worker/refs/page_type_prototypes.npz /refs/page_type_prototypes.npz

ENTRYPOINT ["python", "-m", "wet_processing.worker"]
```

- [ ] **Step 3: Write run.sh**

Create `corpscout/commoncrawl-worker/run.sh`:

```bash
#!/usr/bin/env bash
# Pull the worker image and process one WET file described by a task JSON.
#   ./run.sh path/to/task.json
set -euo pipefail
TASK_FILE="${1:?usage: run.sh <task.json>}"
IMAGE="${COMMONCRAWL_WORKER_IMAGE:?set COMMONCRAWL_WORKER_IMAGE to the registry image}"
docker pull "$IMAGE"
docker run --rm \
  -e COMMONCRAWL_EMBED_BASE_URL -e COMMONCRAWL_EMBED_MODEL \
  -e COMMONCRAWL_LLM_BASE_URL -e COMMONCRAWL_LLM_BASE_MODEL \
  -e CORPSCOUT_S3_ENDPOINT \
  -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_PROFILE \
  -v "$(cd "$(dirname "$TASK_FILE")" && pwd):/work" \
  -v "${COMMONCRAWL_OUT_DIR:-$PWD/out}:/out" \
  "$IMAGE" --task "/work/$(basename "$TASK_FILE")"
```

- [ ] **Step 4: Make executable + sanity-check**

```bash
chmod +x corpscout/commoncrawl-worker/run.sh
docker build --help >/dev/null && echo "docker present"
```
Expected: `docker present`.

- [ ] **Step 5: Commit**

```bash
git add corpscout/commoncrawl-worker/Dockerfile corpscout/commoncrawl-worker/run.sh \
        corpscout/commoncrawl-worker/refs/.gitkeep
git commit -m "feat: commoncrawl-worker Dockerfile + run.sh"
```

### Task B8: GitHub CI — build + push the image

**Files:** Create `<repo-root>/.github/workflows/commoncrawl-worker.yml`

- [ ] **Step 1: Write the workflow** (locate `.github/` with `git rev-parse --show-toplevel`)

```yaml
name: commoncrawl-worker
on:
  push:
    branches: [main]
    paths:
      - "companycollect/corpscout/dagster_v3/commoncrawl_enrich/**"
      - "companycollect/corpscout/dagster_v3/wet_processing/**"
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
        with: { lfs: true }
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

`context: companycollect/corpscout` matches the Dockerfile COPY paths. The `paths:`/context assume the git repo root is the monorepo root; if `git rev-parse --show-toplevel` is `companycollect`, drop the `companycollect/` prefix everywhere.

- [ ] **Step 2: Validate YAML**

Run: `ROOT=$(git rev-parse --show-toplevel); uv run python -c "import yaml; yaml.safe_load(open('$ROOT/.github/workflows/commoncrawl-worker.yml')); print('ok')"`
Expected: `ok`.

- [ ] **Step 3: Commit**

```bash
ROOT=$(git rev-parse --show-toplevel)
git add "$ROOT/.github/workflows/commoncrawl-worker.yml"
git commit -m "ci: build + push commoncrawl-worker image to GHCR"
```

### Task B9: Full verification

- [ ] **Step 1: Whole suite + Dagster defs**

Run: `uv run pytest tests/ -q && uv run dg check defs`
Expected: all pass; `All definitions loaded successfully.`

- [ ] **Step 2: (manual, live) end-to-end one-shot**

```bash
set -a; . ./.env; set +a
WET=$(uv run python -c "from commoncrawl_enrich import segment; print(segment.first_wet_url())")
cat > /tmp/task.json <<JSON
{"crawl_id":"$(uv run python -c 'from commoncrawl_enrich import segment; print(segment.latest_crawl())')",
 "file_index":0,"wet_path":"$WET",
 "output":{"kind":"local","path":"data/commoncrawl/benchmark"},"limit":200}
JSON
uv run python -m wet_processing.worker --task /tmp/task.json
```
Expected: a JSON stats line; Parquet at `data/commoncrawl/benchmark/<crawl>/0.parquet`. No commit.

---

## Notes for the implementer
- Unit tests are offline (warcio fixtures + fake classifier/S3); only B6 step 4 and B9 step 2 need live endpoints.
- Commit by explicit path (the tree carries unrelated WIP) — except the bulk restructure commits (A1–A4) which touch many files; there `git add -A <listed dirs>` is acceptable since those dirs are wholly part of the move.
- `data/*.npz` are gitignored (not committed); CI copies them into the image context. Assume they exist in the CI checkout (produced by the `commoncrawl_classify` assets / calibrate script).
- After A3/A4, `commoncrawl_enrich` contains only `segment`, `extract`, `models`, `ico` (+ `__init__`, `README.md`).
