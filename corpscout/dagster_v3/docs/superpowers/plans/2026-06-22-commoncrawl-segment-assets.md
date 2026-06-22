# CommonCrawl Segment Processing (WET+WARC) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Process ONE CommonCrawl segment (a 1:1 WET+WARC file pair) end-to-end via Dagster assets:
download both files from `data.commoncrawl.org` to local disk, run the standalone per-file
processors into one Parquet each, then remove the raw downloads.

**Architecture:** Two Dagster assets in `src/dagster_v3/defs/commoncrawl/` — a **download** asset
(WET+WARC → local) and a **process** asset (local files → two Parquet, then delete raw). The actual
extraction is the already-built standalone package `commoncrawl_enrich/segment.py`. The two passes
are deliberately asymmetric by data granularity:
- `process_wet_file` = **homepage records only** (`url` path `/`) → homepage emails + **LLM industry
  (per domain)**. The only LLM work; small.
- `process_warc_file` = **all pages** → **per-page technologies** (the priority; Wappalyzer/regex is
  CPU-cheap, so this pass is download-bound, not compute-bound) + socials + emails on every page.

Scoped to one segment with a dev `limit`; a later wrapper loops all segments. Per-page tech means the
full WARC is processed (no homepage shortcut) — the ~90 TB WARC download is the accepted cost at scale.

**Tech Stack:** Dagster, `commoncrawl_enrich.segment` (warcio + lxml + pyarrow), `tldextract`,
`commoncrawl_enrich.llm` (OpenAI-compatible vLLM, think-off), DuckDB (test assertions).

**Working dir for all commands:** `corpscout/dagster_v3/`. Run via `uv run`. Commit to `main` by
**explicit path** (the tree carries unrelated WIP; never `git add -A`).

**Already in the working tree (uncommitted):** `commoncrawl_enrich/segment.py` (the two processors),
and `tldextract` added to `pyproject.toml`/`uv.lock`. Task 1 tests + commits them.

**Measured context (for sanity-checking live runs):** one WET file ≈ 24,365 records / ~76 MB / ~83 s
to parse+email at ~2,700 rec/s; industry think-off ≈ 1.4 s/call (batchable on the free vLLM);
think-off matches think-on for industry. WARC file ≈ 1 GB.

**LLM config (from `dagster_v3/.env`):** `COMMONCRAWL_LLM_BASE_URL=http://100.77.62.33:8888/v1`,
`COMMONCRAWL_LLM_BASE_MODEL=qwen3:6b` (aliases the served `RedHatAI/Qwen3.6-35B-A3B-NVFP4`).

---

## File structure

- `commoncrawl_enrich/segment.py` — **exists**; the two per-file processors. (Task 1: test + commit.)
- `tests/test_commoncrawl_enrich_segment.py` — **new**; tests for the processors. (Task 1.)
- `src/dagster_v3/defs/commoncrawl/__init__.py` — **new**; package docstring. (Task 2.)
- `src/dagster_v3/defs/commoncrawl/segment_source.py` — **new**; config + URL resolution +
  local-path + download helpers. (Task 2.)
- `src/dagster_v3/defs/commoncrawl/assets.py` — **new**; download asset + process asset + job. (Tasks 3–5.)
- `tests/test_commoncrawl_segment_assets.py` — **new**; helper + registration tests. (Tasks 2, 5.)

> **Dagster gotcha (must follow):** do **NOT** put `from __future__ import annotations` in
> `assets.py` — it breaks Dagster's op context-type validation for `@dg.asset`.

---

## Task 1: Test + commit the standalone `segment.py` processors

**Files:**
- Test: `tests/test_commoncrawl_enrich_segment.py` (create)
- Modify (commit existing): `commoncrawl_enrich/segment.py`, `pyproject.toml`, `uv.lock`

- [ ] **Step 1: Write the test**

```python
# tests/test_commoncrawl_enrich_segment.py
import io

import duckdb
from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter

from commoncrawl_enrich import segment
from commoncrawl_enrich.llm import LLMArm


def _write_wet(tmp_path):
    buf = io.BytesIO()
    writer = WARCWriter(buf, gzip=True)
    payload = "Knihy s.r.o. — Predávame knihy. Kontakt: info@knihy.sk".encode()
    rec = writer.create_warc_record(
        "https://www.knihy.sk/", "conversion",
        payload=io.BytesIO(payload), length=len(payload), warc_content_type="text/plain")
    writer.write_record(rec)
    path = tmp_path / "seg.warc.wet.gz"
    path.write_bytes(buf.getvalue())
    return path


def _write_warc(tmp_path):
    buf = io.BytesIO()
    writer = WARCWriter(buf, gzip=True)
    html = (b"<html lang='sk'><head><title>Knihy</title></head><body>"
            b"<a href='mailto:info@knihy.sk'>mail</a>"
            b"<a href='https://www.facebook.com/knihy'>fb</a>"
            b"<link href='/wp-content/x.css'></body></html>")
    headers = StatusAndHeaders("200 OK",
        [("Content-Type", "text/html"), ("Server", "nginx")], protocol="HTTP/1.1")
    rec = writer.create_warc_record(
        "https://www.knihy.sk/", "response",
        payload=io.BytesIO(html), length=len(html), http_headers=headers)
    writer.write_record(rec)
    path = tmp_path / "seg.warc.gz"
    path.write_bytes(buf.getvalue())
    return path


def test_wet_url_to_warc():
    assert segment.wet_url_to_warc(
        "https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-21/segments/1/wet/X.warc.wet.gz"
    ) == "https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-21/segments/1/warc/X.warc.gz"


def test_process_wet_file_emails_and_domain(tmp_path):
    out = tmp_path / "wet.parquet"
    stats = segment.process_wet_file(str(_write_wet(tmp_path)), out, llm=None)
    assert stats["records"] == 1 and stats["with_email"] == 1
    row = duckdb.connect().execute(
        f"select url, root_domain, subdomain, email_count, emails from read_parquet('{out}')"
    ).fetchone()
    assert row[0] == "https://www.knihy.sk/" and row[1] == "knihy.sk" and row[2] == "www"
    assert row[3] == 1 and row[4] == ["info@knihy.sk"]


def test_process_wet_file_with_llm_industry(tmp_path):
    out = tmp_path / "w.parquet"
    arm = LLMArm(chat=lambda s, u: '{"label":"Bookstore","nace_hint":"47.61","confidence":90}')
    stats = segment.process_wet_file(str(_write_wet(tmp_path)), out, llm=arm)
    assert stats["with_industry"] == 1
    row = duckdb.connect().execute(
        f"select industry_label, industry_nace_hint, industry_confidence from read_parquet('{out}')"
    ).fetchone()
    assert row == ("Bookstore", "47.61", 90)


def test_process_warc_file_socials_tech_mailto(tmp_path):
    out = tmp_path / "warc.parquet"
    stats = segment.process_warc_file(str(_write_warc(tmp_path)), out)
    assert stats["records"] == 1 and stats["with_tech"] == 1 and stats["with_social"] == 1
    row = duckdb.connect().execute(
        f"select emails, social_platforms, technologies from read_parquet('{out}')"
    ).fetchone()
    assert "info@knihy.sk" in row[0] and row[1] == ["facebook"] and "WordPress" in row[2]


def test_process_wet_file_respects_limit(tmp_path):
    # two records, limit=1 -> only one row
    buf = io.BytesIO()
    writer = WARCWriter(buf, gzip=True)
    for host in ("a.sk", "b.sk"):
        p = f"x@{host}".encode()
        writer.write_record(writer.create_warc_record(
            f"https://{host}/", "conversion", payload=io.BytesIO(p), length=len(p),
            warc_content_type="text/plain"))
    wet = tmp_path / "two.warc.wet.gz"
    wet.write_bytes(buf.getvalue())
    stats = segment.process_wet_file(str(wet), tmp_path / "o.parquet", llm=None, limit=1)
    assert stats["records"] == 1


def test_process_wet_file_homepages_only_skips_subpages(tmp_path):
    buf = io.BytesIO()
    writer = WARCWriter(buf, gzip=True)
    for url in ("https://shop.sk/", "https://shop.sk/products/123"):  # homepage + subpage
        payload = f"Contact us at hi@shop.sk ({url})".encode()
        writer.write_record(writer.create_warc_record(
            url, "conversion", payload=io.BytesIO(payload), length=len(payload),
            warc_content_type="text/plain"))
    wet = tmp_path / "mix.warc.wet.gz"
    wet.write_bytes(buf.getvalue())
    out = tmp_path / "o.parquet"
    stats = segment.process_wet_file(str(wet), out, llm=None)  # homepages_only=True by default
    assert stats["records"] == 1  # only the homepage kept
    url = duckdb.connect().execute(f"select url from read_parquet('{out}')").fetchone()[0]
    assert url == "https://shop.sk/"
```

Note: `process_wet_file` defaults to `homepages_only=True` — only `/` pages are kept (industry is a
domain property). `process_warc_file` processes every page (per-page tech). The other WET tests above
use homepage URLs, so they still pass under the homepage filter.

- [ ] **Step 2: Run it (should PASS against the existing `segment.py`)**

Run: `uv run pytest tests/test_commoncrawl_enrich_segment.py -q`
Expected: PASS (6 tests). The behaviour assertions are authoritative — if one FAILS, fix
`segment.py`. (If the failure is in building the fixture, the installed `warcio`'s
`create_warc_record` API differs slightly — adjust the fixture's record construction, keeping the
assertions identical. `conversion`/`response` record types + `warc_content_type` are standard warcio.)

- [ ] **Step 3: Commit the processors, their test, and the `tldextract` dependency**

```bash
git add commoncrawl_enrich/segment.py tests/test_commoncrawl_enrich_segment.py pyproject.toml uv.lock
git commit -m "feat(commoncrawl_enrich): per-file WET/WARC segment processors + tldextract"
```

---

## Task 2: Dagster `commoncrawl` module — config + source helpers

**Files:**
- Create: `src/dagster_v3/defs/commoncrawl/__init__.py`
- Create: `src/dagster_v3/defs/commoncrawl/segment_source.py`
- Test: `tests/test_commoncrawl_segment_assets.py`

- [ ] **Step 1: Write the failing test (pure helper `local_paths`)**

```python
# tests/test_commoncrawl_segment_assets.py
from pathlib import Path

from dagster_v3.defs.commoncrawl import segment_source as src


def test_local_paths_maps_wet_and_warc():
    wet_url = ("https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-21/segments/"
               "1746990412205.50/wet/CC-MAIN-20250512011722-20250512041722-00000.warc.wet.gz")
    wet_path, warc_path = src.local_paths(wet_url, "data/cc")
    assert wet_path == Path("data/cc/CC-MAIN-20250512011722-20250512041722-00000.warc.wet.gz")
    assert warc_path == Path("data/cc/CC-MAIN-20250512011722-20250512041722-00000.warc.gz")
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/test_commoncrawl_segment_assets.py -q`
Expected: FAIL (`ModuleNotFoundError: dagster_v3.defs.commoncrawl`).

- [ ] **Step 3: Create the module + helpers**

```python
# src/dagster_v3/defs/commoncrawl/__init__.py
"""CommonCrawl WET+WARC per-segment processing assets (download → process → remove)."""
```

```python
# src/dagster_v3/defs/commoncrawl/segment_source.py
import gzip
import logging
from pathlib import Path

import dagster as dg
import requests

from commoncrawl_enrich.segment import wet_url_to_warc

LOGGER = logging.getLogger(__name__)
USER_AGENT = "corpscout-commoncrawl-enrich/0.1 (goran.raovic@gmail.com)"
DATA_HOST = "https://data.commoncrawl.org"
COLLINFO_URL = "https://index.commoncrawl.org/collinfo.json"  # all crawls, newest first


class CommonCrawlSegmentConfig(dg.Config):
    crawl: str = ""                 # "" -> resolve the latest available crawl (collinfo.json)
    segment_index: int = 0          # which WET path from wet.paths.gz
    local_dir: str = "data/commoncrawl/raw"
    out_dir: str = "data/commoncrawl/parquet"
    limit: int | None = None        # dev: cap records processed per file
    run_industry: bool = True


def latest_crawl(session: requests.Session | None = None) -> str:
    """Id of the newest CommonCrawl crawl (first entry of collinfo.json), e.g. 'CC-MAIN-2026-25'."""
    http = session or requests.Session()
    resp = http.get(COLLINFO_URL, headers={"User-Agent": USER_AGENT}, timeout=60)
    resp.raise_for_status()
    return resp.json()[0]["id"]


def resolve_wet_url(crawl: str, segment_index: int, session: requests.Session | None = None) -> str:
    """The Nth WET file URL for a crawl (latest crawl when `crawl` is empty), from wet.paths.gz."""
    http = session or requests.Session()
    if not crawl:
        crawl = latest_crawl(http)
    resp = http.get(f"{DATA_HOST}/crawl-data/{crawl}/wet.paths.gz",
                    headers={"User-Agent": USER_AGENT}, timeout=60)
    resp.raise_for_status()
    paths = gzip.decompress(resp.content).decode().splitlines()
    return f"{DATA_HOST}/{paths[segment_index]}"


def local_paths(wet_url: str, local_dir: str) -> tuple[Path, Path]:
    """Deterministic local destinations for a WET url and its paired WARC url."""
    warc_url = wet_url_to_warc(wet_url)
    base = Path(local_dir)
    return base / Path(wet_url).name, base / Path(warc_url).name


def download_file(url: str, dest: Path, session: requests.Session | None = None) -> int:
    """Stream a file to `dest`; returns bytes written."""
    http = session or requests.Session()
    dest.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with http.get(url, stream=True, headers={"User-Agent": USER_AGENT}, timeout=600) as resp:
        resp.raise_for_status()
        with open(dest, "wb") as out:
            for chunk in resp.iter_content(1 << 20):
                if chunk:
                    out.write(chunk)
                    written += len(chunk)
    return written
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest tests/test_commoncrawl_segment_assets.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/commoncrawl/__init__.py src/dagster_v3/defs/commoncrawl/segment_source.py tests/test_commoncrawl_segment_assets.py
git commit -m "feat(commoncrawl): segment config + URL/local-path/download helpers"
```

---

## Task 3: Download asset (WET+WARC pair → local)

**Files:**
- Create: `src/dagster_v3/defs/commoncrawl/assets.py`

- [ ] **Step 1: Create the download asset**

```python
# src/dagster_v3/defs/commoncrawl/assets.py
from pathlib import Path

import dagster as dg
import requests
from dagster import AssetExecutionContext

from commoncrawl_enrich.segment import wet_url_to_warc
from dagster_v3.defs.commoncrawl.segment_source import (
    CommonCrawlSegmentConfig,
    download_file,
    local_paths,
    resolve_wet_url,
)

GROUP_NAME = "commoncrawl"


@dg.asset(
    name="commoncrawl_segment_download",
    group_name=GROUP_NAME,
    kinds={"python", "s3"},
    description="Download one CommonCrawl (WET, WARC) pair from data.commoncrawl.org to local disk.",
)
def commoncrawl_segment_download(
    context: AssetExecutionContext, config: CommonCrawlSegmentConfig
) -> dg.MaterializeResult:
    session = requests.Session()
    wet_url = resolve_wet_url(config.crawl, config.segment_index, session)
    warc_url = wet_url_to_warc(wet_url)
    wet_path, warc_path = local_paths(wet_url, config.local_dir)
    context.log.info("Downloading WET %s -> %s", wet_url, wet_path)
    wet_bytes = download_file(wet_url, wet_path, session)
    context.log.info("Downloading WARC %s -> %s", warc_url, warc_path)
    warc_bytes = download_file(warc_url, warc_path, session)
    return dg.MaterializeResult(metadata={
        "wet_url": wet_url, "warc_url": warc_url,
        "wet_path": str(wet_path), "warc_path": str(warc_path),
        "wet_mb": round(wet_bytes / 1e6, 1), "warc_mb": round(warc_bytes / 1e6, 1),
    })
```

- [ ] **Step 2: Verify it loads**

Run: `uv run dg check defs`
Expected: ends with "All definitions loaded successfully."

- [ ] **Step 3: Commit**

```bash
git add src/dagster_v3/defs/commoncrawl/assets.py
git commit -m "feat(commoncrawl): download asset for one WET+WARC pair"
```

---

## Task 4: Process asset (local files → Parquet, then remove raw)

**Files:**
- Modify: `src/dagster_v3/defs/commoncrawl/assets.py`

- [ ] **Step 1: Append the process asset**

Add these imports at the top of `assets.py` (after the existing imports):

```python
import os

from commoncrawl_enrich.llm import from_openai
from commoncrawl_enrich.segment import process_warc_file, process_wet_file
```

Append the asset:

```python
def _build_llm():
    """OpenAI-compatible vLLM arm (think-off) from env, or None if unconfigured."""
    base = os.environ.get("COMMONCRAWL_LLM_BASE_URL")
    if not base:
        return None
    model = os.environ.get("COMMONCRAWL_LLM_BASE_MODEL") or os.environ.get("COMMONCRAWL_LLM_MODEL")
    if not model:
        return None
    return from_openai(
        base_url=base, model=model,
        api_key=os.environ.get("COMMONCRAWL_LLM_API_KEY", "not-needed"),
        enable_thinking=False,  # industry: think-off matches think-on at ~15x speed
    )


@dg.asset(
    name="commoncrawl_segment_parquet",
    deps=[dg.AssetKey("commoncrawl_segment_download")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    description="Process the local WET+WARC pair into one Parquet each, then delete the raw files.",
)
def commoncrawl_segment_parquet(
    context: AssetExecutionContext, config: CommonCrawlSegmentConfig
) -> dg.MaterializeResult:
    wet_url = resolve_wet_url(config.crawl, config.segment_index)
    wet_path, warc_path = local_paths(wet_url, config.local_dir)
    out_dir = Path(config.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    wet_parquet = out_dir / (wet_path.name.removesuffix(".gz") + ".parquet")
    warc_parquet = out_dir / (warc_path.name.removesuffix(".gz") + ".parquet")

    llm = _build_llm() if config.run_industry else None
    if config.run_industry and llm is None:
        context.log.warning("No COMMONCRAWL_LLM_BASE_URL — WET pass runs without industry.")

    context.log.info("Processing WET %s (limit=%s, industry=%s)", wet_path, config.limit, llm is not None)
    wet_stats = process_wet_file(str(wet_path), wet_parquet, llm=llm, limit=config.limit)
    context.log.info("Processing WARC %s (limit=%s)", warc_path, config.limit)
    warc_stats = process_warc_file(str(warc_path), warc_parquet, limit=config.limit)

    for raw in (wet_path, warc_path):  # download -> process -> remove
        try:
            raw.unlink()
        except FileNotFoundError:
            pass

    return dg.MaterializeResult(metadata={
        "wet_records": wet_stats["records"], "wet_with_email": wet_stats["with_email"],
        "wet_with_industry": wet_stats["with_industry"], "wet_parquet": str(wet_parquet),
        "warc_records": warc_stats["records"], "warc_with_social": warc_stats["with_social"],
        "warc_with_tech": warc_stats["with_tech"], "warc_parquet": str(warc_parquet),
    })
```

- [ ] **Step 2: Verify it loads**

Run: `uv run dg check defs`
Expected: "All definitions loaded successfully."

- [ ] **Step 3: Commit**

```bash
git add src/dagster_v3/defs/commoncrawl/assets.py
git commit -m "feat(commoncrawl): process asset (WET+WARC -> Parquet, remove raw)"
```

---

## Task 5: Job, registration test, and one-segment live run

**Files:**
- Modify: `src/dagster_v3/defs/commoncrawl/assets.py`
- Modify: `tests/test_commoncrawl_segment_assets.py`

- [ ] **Step 1: Append the job to `assets.py`**

```python
commoncrawl_segment_job = dg.define_asset_job(
    "commoncrawl_segment_job",
    selection=dg.AssetSelection.assets("commoncrawl_segment_parquet").upstream(),
)
```

- [ ] **Step 2: Add the registration test**

Append to `tests/test_commoncrawl_segment_assets.py`:

```python
def test_segment_job_and_assets_registered():
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    keys = {
        k.path[-1]
        for k in repo.get_job("commoncrawl_segment_job").asset_layer.executable_asset_keys
    }
    assert keys == {"commoncrawl_segment_download", "commoncrawl_segment_parquet"}
```

- [ ] **Step 3: Run the registration test + defs check**

Run: `uv run pytest tests/test_commoncrawl_segment_assets.py -q`
Expected: PASS.
Run: `uv run dg check defs`
Expected: "All definitions loaded successfully."

- [ ] **Step 4: Commit**

```bash
git add src/dagster_v3/defs/commoncrawl/assets.py tests/test_commoncrawl_segment_assets.py
git commit -m "feat(commoncrawl): segment job + registration test"
```

- [ ] **Step 5: One-segment live run (manual verification — see the output Parquet + flow)**

Launch the chain with a small `limit` so the first run is fast to inspect (downloads the full
pair ~1.15 GB, but processes only `limit` records). Export the `.env` LLM config first:

```bash
export $(grep -E '^COMMONCRAWL_LLM_(BASE_URL|BASE_MODEL)=' .env | xargs)
export COMMONCRAWL_LLM_MODEL="$COMMONCRAWL_LLM_BASE_MODEL"   # segment_source reads BASE_MODEL too
uv run dg launch --assets 'commoncrawl_segment_download,commoncrawl_segment_parquet' \
  --config-json '{"ops": {"commoncrawl_segment_download": {"config": {"segment_index": 0, "limit": 500}}, "commoncrawl_segment_parquet": {"config": {"segment_index": 0, "limit": 500}}}}'
```

Then inspect the output:

```bash
uv run python -c "
import duckdb, glob
con = duckdb.connect()
wet = sorted(glob.glob('data/commoncrawl/parquet/*.warc.wet.parquet'))[-1]
warc = sorted(glob.glob('data/commoncrawl/parquet/*.warc.parquet'))[-1]
print('WET sample:'); 
print(con.execute(f\"select root_domain, subdomain, email_count, industry_label, industry_nace_hint from read_parquet('{wet}') where industry_label<>'' limit 8\").fetchall())
print('WARC sample:')
print(con.execute(f\"select root_domain, list_distinct(technologies) t, social_platforms from read_parquet('{warc}') where length(technologies)>0 limit 8\").fetchall())
"
```

Expected: the WET Parquet has `(root_domain, subdomain, emails, industry_label, industry_nace_hint)`
rows; the WARC Parquet has `(root_domain, technologies, social_platforms, emails)` rows; the raw
`.gz` files under `data/commoncrawl/raw/` are gone (removed after processing). This is the
output + full flow to tune against — re-run with a different `segment_index`/`limit` to iterate.

---

## Done — what this delivers

One Dagster segment chain: **download** the WET+WARC pair → **process** into two Parquet
(`*.warc.wet.parquet` with emails + LLM industry; `*.warc.parquet` with emails + socials +
technologies) → **remove** the raw downloads. Reuses the standalone `commoncrawl_enrich/segment.py`
so the same processors run unchanged in the future all-segments wrapper / AWS run.

**Out of scope (later):** the wrapper loop over all 90k segments (download-process-remove in a
loop, two parallel apps for WET vs WARC), input-token trimming + embeddings for the industry tier,
and the AWS/us-east-1 deployment for the full crawl.
