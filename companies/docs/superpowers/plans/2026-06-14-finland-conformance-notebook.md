# Finland Conformance Notebook Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a self-contained marimo notebook under `companies/analysis/finland/notebook/` that downloads raw Finland source files to S3, parses them into structured Parquet, and builds canonical Parquet matching the 8-table contract — with pure transform functions shaped like future Dagster asset bodies.

**Architecture:** Three pure-function layers (download → parse → build), all future Dagster assets. Parse logic is COPIED from `dagster_corpscout` (not imported) and its package imports rewritten local. ClickHouse is bypassed entirely; canonical Parquet is the only output. A marimo notebook orchestrates and validates.

**Tech Stack:** Python 3.12, uv, marimo, polars, pyarrow, duckdb, boto3, requests, lxml.

**Spec:** `companies/docs/superpowers/specs/2026-06-14-country-conformance-notebooks-design.md`
**Contract:** `companies/analysis/_canonical/canonical_schema.md`

---

## CRITICAL: Git rules for this repo

A **concurrent process is actively committing** to this same `main` branch.

- **Plain `git commit` only. NEVER `git commit --amend`** (it clobbers the concurrent writer's commits).
- **`git add` only the exact files named in the task.** NEVER `git add -A`, `git add .`, or `git add -u`.
- Each phase is exactly one commit. Confirm the phase works before starting the next.
- All work lives under `companies/analysis/finland/notebook/` — never touch other paths.

All commands run from the repo root `/Users/graovic/pulsarpoint/ppoint/companycollect` unless stated. The notebook dir is `companies/analysis/finland/notebook/` (abbreviated `NB/` below).

---

## File structure

```
companies/analysis/finland/notebook/
  pyproject.toml              Phase 1  uv env
  conformance/
    __init__.py
    download.py               Phase 2-3  URL constants + download_* → S3 (first asset)
    _vendor/                  Phase 5  COPIED XBRL parser only (imports rewritten local)
      __init__.py
      prh_xbrl_parser.py        copy of prh_xbrl/parser.py
      prh_xbrl_tables.py        copy of prh_xbrl/tables.py
      prh_xbrl_spec.py          PARSER_VERSION + object-key helpers
    structured.py             Phase 5  prh_ytj via native Polars; prh_xbrl via copied parser
    schemas.py                Phase 6  canonical table Polars schemas (single source of truth)
    validate.py               Phase 6  assert a DataFrame matches a canonical schema
    build_company.py          Phase 6  structured → canonical company + registrations
    build_financials.py       Phase 6  structured → canonical financials
    build_websites.py         Phase 6  structured → canonical company_websites
  analysis_method.md          Phase 4  pedagogical Polars→Parquet method
  partitioning.md             (later)  partition-strategy doc
  finland_conformance.py      Phase 6  marimo notebook orchestrator
  tests/
    __init__.py
    test_validate.py          Phase 6
    test_build_company.py     Phase 6
    test_build_financials.py  Phase 6
    test_build_websites.py    Phase 6
    test_structured.py        Phase 5
  output/                     (gitignored) raw/ structured/ canonical/ artifacts
```

Reference (read-only, the copy sources):
- `corpscout/dagster_v2/dagster_corpscout/sources/finland/prh_ytj/{client,parser,normalizer,tables,spec}.py`
- `corpscout/dagster_v2/dagster_corpscout/sources/finland/prh_xbrl/{client,parser,tables,spec}.py`

---

## Phase 1 — Environment

**Files:**
- Create: `NB/pyproject.toml`
- Create: `NB/.gitignore`
- Create: `NB/conformance/__init__.py`, `NB/tests/__init__.py`

- [ ] **Step 1: Create the uv project file**

Create `companies/analysis/finland/notebook/pyproject.toml`:

```toml
[project]
name = "finland-conformance"
version = "0.1.0"
description = "Finland raw -> canonical Parquet conformance notebook"
requires-python = ">=3.12"
dependencies = [
    "marimo>=0.9",
    "polars>=1.0",
    "pyarrow>=17",
    "duckdb>=1.1",
    "boto3>=1.34",
    "requests>=2.32",
    "lxml>=5.2",
]

[dependency-groups]
dev = [
    "pytest>=8",
]

[tool.pytest.ini_options]
testpaths = ["tests"]
```

- [ ] **Step 2: Create `.gitignore` so heavy/artifact files never get committed**

Create `companies/analysis/finland/notebook/.gitignore`:

```
.venv/
output/
__pycache__/
.pytest_cache/
*.egg-info/
uv.lock
```

- [ ] **Step 3: Create empty package markers**

Create `companies/analysis/finland/notebook/conformance/__init__.py` with content:

```python
"""Finland conformance: download -> parse -> build canonical Parquet."""
```

Create `companies/analysis/finland/notebook/tests/__init__.py` (empty file).

- [ ] **Step 4: Create the env and confirm it works**

Run (from repo root):
```bash
cd companies/analysis/finland/notebook && uv sync --extra dev 2>&1 | tail -5
```
Expected: resolves and installs; `.venv/` created. Then confirm imports:
```bash
cd companies/analysis/finland/notebook && uv run python -c "import marimo, polars, pyarrow, duckdb, boto3, lxml; print('ok')"
```
Expected: prints `ok`.

- [ ] **Step 5: Commit (plain, named files only)**

```bash
git add companies/analysis/finland/notebook/pyproject.toml \
        companies/analysis/finland/notebook/.gitignore \
        companies/analysis/finland/notebook/conformance/__init__.py \
        companies/analysis/finland/notebook/tests/__init__.py
git commit -m "Phase 1: Finland conformance notebook uv environment"
```

---

## Phase 2 — Source URL constants

**Files:**
- Create: `NB/conformance/download.py` (constants only this phase)

URLs come verbatim from the existing specs:
- prh_ytj companies: `https://avoindata.prh.fi/opendata-ytj-api/v3/companies`
- prh_ytj code-list description path: `https://avoindata.prh.fi/opendata-ytj-api/v3/description`
- prh_xbrl base: `https://avoindata.prh.fi/opendata-xbrl-api/v3`

- [ ] **Step 1: Write `download.py` with only the URL constants and a probe helper**

Create `companies/analysis/finland/notebook/conformance/download.py`:

```python
"""Download Finland raw source files directly to S3 (first asset).

URL constants live here (no separate urls module). Download functions are
added in Phase 3. Mirrors the existing source clients; copied, not imported.
"""

from __future__ import annotations

import requests

# --- Source URLs (Phase 2) ---------------------------------------------------
PRH_YTJ_COMPANIES_URL = "https://avoindata.prh.fi/opendata-ytj-api/v3/companies"
PRH_YTJ_DESCRIPTION_URL = "https://avoindata.prh.fi/opendata-ytj-api/v3/description"
PRH_XBRL_BASE_URL = "https://avoindata.prh.fi/opendata-xbrl-api/v3"
USER_AGENT = "corpscout-conformance/0.1 (finland)"

# prh_ytj code lists to fetch (code, lang), order per the source catalog.
CODE_LISTS: list[tuple[str, str]] = [
    ("REK", "en"), ("REK_KDI", "en"), ("VIRANOM", "en"), ("TLAJI", "en"),
    ("YRMU", "en"), ("STATUS3", "en"), ("KIELI", "en"),
]


def probe() -> dict[str, int]:
    """Return HTTP status for one probe request per source URL. Confirms the
    endpoints resolve before any bulk download is wired up (Phase 3)."""
    headers = {"User-Agent": USER_AGENT}
    statuses: dict[str, int] = {}
    r = requests.get(PRH_YTJ_COMPANIES_URL, params={"page": 1}, headers=headers, timeout=60)
    statuses["prh_ytj_companies"] = r.status_code
    r = requests.get(
        PRH_YTJ_DESCRIPTION_URL, params={"code": "STATUS3", "lang": "en"},
        headers=headers, timeout=60,
    )
    statuses["prh_ytj_description"] = r.status_code
    r = requests.get(
        f"{PRH_XBRL_BASE_URL}/all_financial_statements",
        params={"registeredDateStart": "2025-01-01", "registeredDateEnd": "2025-01-02", "page": 1},
        headers=headers, timeout=60,
    )
    statuses["prh_xbrl_discovery"] = r.status_code
    return statuses
```

- [ ] **Step 2: Confirm the URLs resolve**

Run (from repo root):
```bash
cd companies/analysis/finland/notebook && uv run python -c "from conformance.download import probe; print(probe())"
```
Expected: a dict with all three statuses `200` (e.g. `{'prh_ytj_companies': 200, 'prh_ytj_description': 200, 'prh_xbrl_discovery': 200}`). If any is not 200, stop and investigate before proceeding.

- [ ] **Step 3: Commit**

```bash
git add companies/analysis/finland/notebook/conformance/download.py
git commit -m "Phase 2: Finland source URL constants + endpoint probe"
```

---

## Phase 3 — Download to S3 (first asset)

The download functions mirror the existing clients (`prh_ytj/client.py iter_companies`/`fetch_code_list`, `prh_xbrl/client.py PRHXBRLClient`) but are self-contained here. They write raw bytes directly to S3 and return a small manifest dict (the future asset's `MaterializeResult` metadata).

**S3 config** comes from env vars (same names as corpscout): `CORPSCOUT_S3_ENDPOINT`, `CORPSCOUT_S3_ACCESS_KEY`, `CORPSCOUT_S3_SECRET_KEY`. Bucket: `conformance-finland` (kept separate from production buckets).

**Files:**
- Modify: `NB/conformance/download.py`

- [ ] **Step 1: Add the S3 client + download functions to `download.py`**

Append to `companies/analysis/finland/notebook/conformance/download.py`:

```python
import json
import os
import time
from collections.abc import Iterator
from urllib.parse import urlencode

import boto3
from botocore.config import Config

BUCKET = "conformance-finland"
_TIMEOUT = 300
_RETRY_DELAYS = [1, 2, 4, 8]


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["CORPSCOUT_S3_ENDPOINT"],
        aws_access_key_id=os.environ["CORPSCOUT_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["CORPSCOUT_S3_SECRET_KEY"],
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def ensure_bucket(s3) -> None:
    existing = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
    if BUCKET not in existing:
        s3.create_bucket(Bucket=BUCKET)


def _get(session: requests.Session, url: str, params: dict) -> requests.Response:
    for attempt in range(len(_RETRY_DELAYS) + 1):
        try:
            r = session.get(url, params=params, headers={"User-Agent": USER_AGENT}, timeout=_TIMEOUT)
            if r.status_code == 429 or r.status_code >= 500:
                if attempt < len(_RETRY_DELAYS):
                    time.sleep(_RETRY_DELAYS[attempt])
                    continue
            r.raise_for_status()
            return r
        except (requests.ConnectionError, requests.Timeout):
            if attempt == len(_RETRY_DELAYS):
                raise
            time.sleep(_RETRY_DELAYS[attempt])
    raise RuntimeError("unreachable")


def _iter_companies(session: requests.Session) -> Iterator[dict]:
    page, total, seen = 1, None, 0
    while True:
        payload = _get(session, PRH_YTJ_COMPANIES_URL, {"page": page}).json()
        if payload.get("totalResults") is not None:
            total = int(payload["totalResults"])
        companies = payload.get("companies") or []
        for c in companies:
            seen += 1
            yield c
        if (total is not None and seen >= total) or not companies or len(companies) < 100:
            return
        page += 1


def download_prh_ytj(run_id: str, max_companies: int | None = None) -> dict:
    """Download the YTJ company snapshot (NDJSON) + code lists to S3.
    max_companies bounds the reference run; None = full snapshot.
    First asset: URLs -> raw files in S3."""
    s3 = s3_client()
    ensure_bucket(s3)
    session = requests.Session()
    lines: list[bytes] = []
    count = 0
    for company in _iter_companies(session):
        lines.append(json.dumps(company, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
        count += 1
        if max_companies and count >= max_companies:
            break
    snapshot_key = f"runs/{run_id}/source.ndjson"
    s3.put_object(Bucket=BUCKET, Key=snapshot_key, Body=b"\n".join(lines) + b"\n")

    code_list_keys = []
    for code, lang in CODE_LISTS:
        body = _get(session, PRH_YTJ_DESCRIPTION_URL, {"code": code, "lang": lang}).content
        key = f"runs/{run_id}/codelists/{code}.{lang}.tsv"
        s3.put_object(Bucket=BUCKET, Key=key, Body=body)
        code_list_keys.append(key)
    return {"snapshot_key": snapshot_key, "companies": count, "code_list_keys": code_list_keys}


def _xbrl_url(business_id: str, financial_date: str) -> str:
    return f"{PRH_XBRL_BASE_URL}/financial?" + urlencode(
        {"businessId": business_id, "financialDate": financial_date}
    )


def download_prh_xbrl(run_id: str, registered_start: str, registered_end: str) -> dict:
    """Download one registration-month window of XBRL statements to S3, with a
    listing.json mirroring the production raw layer. Bounded sample for the
    reference (one month)."""
    s3 = s3_client()
    ensure_bucket(s3)
    session = requests.Session()
    documents = []
    page = 1
    while True:
        payload = _get(
            session, f"{PRH_XBRL_BASE_URL}/all_financial_statements",
            {"registeredDateStart": registered_start, "registeredDateEnd": registered_end, "page": page},
        ).json()
        items = payload.get("financials", [])
        if not items:
            break
        for item in items:
            bid = str(item.get("businessId") or "").strip()
            fdate = str(item.get("financialDate") or "").strip()
            if not bid or not fdate:
                continue
            body = _get(session, f"{PRH_XBRL_BASE_URL}/financial",
                        {"businessId": bid, "financialDate": fdate}).content
            object_key = f"companies/{bid}/{fdate}.xml"
            s3.put_object(Bucket=BUCKET, Key=object_key, Body=body)
            documents.append({
                "business_id": bid, "financial_date": fdate,
                "registration_date": item.get("registrationDate"),
                "object_key": object_key, "source_url": _xbrl_url(bid, fdate),
            })
        total = int(payload.get("totalResults") or 0)
        if total and page * 100 >= total:
            break
        page += 1
    listing_key = f"windows/{registered_start}/listing.json"
    s3.put_object(
        Bucket=BUCKET, Key=listing_key,
        Body=json.dumps({"documents": documents, "skipped": []}, indent=2).encode("utf-8"),
    )
    return {"listing_key": listing_key, "documents": len(documents)}
```

- [ ] **Step 2: Confirm download lands raw objects in S3 (bounded smoke run)**

Requires `CORPSCOUT_S3_*` env vars set. Run (from repo root):
```bash
cd companies/analysis/finland/notebook && uv run python -c "
from conformance.download import download_prh_ytj, download_prh_xbrl, s3_client, BUCKET
m1 = download_prh_ytj('reftest', max_companies=200); print('ytj', m1['companies'], m1['snapshot_key'])
m2 = download_prh_xbrl('reftest', '2025-01-01', '2025-01-08'); print('xbrl', m2['documents'], m2['listing_key'])
s3 = s3_client(); print('keys', len(s3.list_objects_v2(Bucket=BUCKET).get('Contents', [])))
"
```
Expected: prints `ytj 200 runs/reftest/source.ndjson`, `xbrl <N>` with N≥1, `listing` key, and a key count > 0. Re-running must overwrite the same keys without error.

- [ ] **Step 3: Commit**

```bash
git add companies/analysis/finland/notebook/conformance/download.py
git commit -m "Phase 3: Finland download functions write raw source files to S3"
```

---

## Phase 4 — Analysis method (pedagogical doc)

Documentation gate before transforms. No code; the doc is the deliverable.

**Files:**
- Create: `NB/analysis_method.md`

- [ ] **Step 1: Write `analysis_method.md`**

Create `companies/analysis/finland/notebook/analysis_method.md` covering, in order:

1. **Goal & layers** — raw (S3) → structured Parquet (parser output) → canonical Parquet (8-table contract). Why structured Parquet is persisted (it's a future asset).
2. **Per-source raw shape:**
   - prh_ytj: one JSON object per company per NDJSON line (`businessId`, `names[]`, `tradeRegisterStatus`, `companyForms[]`, `mainBusinessLine`, `website`, `addresses[]`, `registeredEntries[]`). Cite `country_company_profile_mapping.md`.
   - prh_xbrl: per-statement XBRL XML; facts are `fi_met:*` keyed by `fi_dim:MCY` dimension members, not element names. Cite `prh_xbrl_schema_spike/schema_analysis.md`.
3. **How to convert JSONL with Polars (prh_ytj) — the idiomatic path:** `pl.read_ndjson` (or `scan_ndjson` for the full snapshot) reads JSONL into nested struct/list columns; reshape with vectorized expressions (`.struct.field()`, `.explode()`, `.unnest()`, `.list.eval()`) — NOT a Python row loop. Encode the domain rules here: the `status='2'` liveness pitfall → derive from `tradeRegisterStatus`; current-primary name = `type==1 & endDate null`; website URL normalization. Profile with `df.null_count()` / DuckDB `SUMMARIZE`; reference `_templates/profile_source.py` and `country_company_profile_mapping.md`.
4. **When to reuse a parser instead (prh_xbrl):** XML is not tabular — Polars can't parse XBRL, so the `lxml` parser is COPIED and reused, and its row output wrapped into Polars. The rule: **native Polars for JSONL/CSV; reuse the parser only where parsing is non-trivial (XML).** Structured Parquet, one dataset per table: prh_ytj → statuses/names/websites/addresses/business_lines (native Polars); prh_xbrl → documents/contexts/units/facts (parser).
5. **Mapping to canonical:** how structured rows map to `registrations`/`company`/`company_websites`/`financials` per the dossier §6 and the metric map in the schema spike. Which 4 of 8 tables Finland fills; which 4 are known-absent.
6. **Validation:** every canonical DataFrame is checked against `schemas.py` before writing.

- [ ] **Step 2: Confirm the doc covers every source entity and the target schema**

Run (from repo root):
```bash
grep -E "prh_ytj|prh_xbrl|structured|canonical|registrations|financials|company_websites|known-absent|Polars" companies/analysis/finland/notebook/analysis_method.md | wc -l
```
Expected: a non-trivial count (≥ 8 matching lines), confirming all sources, layers, and tables are mentioned. Manually re-read to confirm each of the 4 populated tables has a mapping paragraph.

- [ ] **Step 3: Commit**

```bash
git add companies/analysis/finland/notebook/analysis_method.md
git commit -m "Phase 4: Finland source analysis method (Polars -> Parquet)"
```

---

## Phase 5 — Parse to structured Parquet (copy parsers + serialize)

Copy the parsers into `conformance/_vendor/`, rewriting `dagster_corpscout` imports to local, then add `structured.py` that runs them over the S3 raw files and writes structured Parquet.

**Files:**
- Create: `NB/conformance/_vendor/__init__.py` and the copied modules
- Create: `NB/conformance/structured.py`
- Create: `NB/tests/test_structured.py`

- [ ] **Step 1: Copy ONLY the XBRL parser into `_vendor/`, rewriting imports**

prh_ytj is JSONL — no parser is copied for it; Polars reads JSONL natively in Step 5. Only the XBRL XML parser is reused.

Create `companies/analysis/finland/notebook/conformance/_vendor/__init__.py` (empty).

Copy these files verbatim, then rewrite imports:
- `prh_xbrl/tables.py` → `_vendor/prh_xbrl_tables.py` (column-name constants; no imports to rewrite).
- `prh_xbrl/spec.py` → `_vendor/prh_xbrl_spec.py` (keep `PARSER_VERSION` and the object-key helpers).
- `prh_xbrl/parser.py` → `_vendor/prh_xbrl_parser.py`. Rewrite `from dagster_corpscout.sources.finland.prh_xbrl import spec, tables` → `from conformance._vendor import prh_xbrl_spec as spec, prh_xbrl_tables as tables`.

- [ ] **Step 2: Verify the copied XBRL parser imports cleanly**

Run (from repo root):
```bash
cd companies/analysis/finland/notebook && uv run python -c "
from conformance._vendor.prh_xbrl_parser import parse_statement_xml
print('vendored xbrl parser import ok')
"
```
Expected: prints `vendored xbrl parser import ok`.

- [ ] **Step 3: Write the failing test for `structured.py`**

Create `companies/analysis/finland/notebook/tests/test_structured.py`:

```python
import datetime as dt
import json

import polars as pl

from conformance.structured import ytj_structured_from_ndjson, xbrl_structured_from_statements


def test_ytj_structured_produces_statuses_and_names():
    record = {
        "businessId": {"value": "0104539-0"},
        "tradeRegisterStatus": "1", "status": "2",
        "registrationDate": "2001-01-01", "endDate": None,
        "names": [{"name": "Acme Oy", "type": "1", "registrationDate": "2001-01-01", "endDate": None}],
        "website": {"url": "acme.fi", "registrationDate": "2010-01-01", "endDate": None},
        "addresses": [{"type": 1, "street": "Main 1", "postCode": "00100", "country": "FI",
                       "postOffices": [{"languageCode": "1", "city": "Helsinki", "municipalityCode": "091"}]}],
        "mainBusinessLine": {"type": "62010", "typeCodeSet": "TOL2008", "descriptions": []},
    }
    ndjson = (json.dumps(record) + "\n").encode("utf-8")
    tables = ytj_structured_from_ndjson(ndjson)
    assert tables["fi_prhytj_statuses"]["business_id"].to_list() == ["0104539-0"]
    assert tables["fi_prhytj_statuses"]["is_active"].to_list() == [True]
    assert tables["fi_prhytj_names"]["name"].to_list() == ["Acme Oy"]
    assert tables["fi_prhytj_websites"]["normalized_url"].to_list() == ["https://acme.fi"]
    assert tables["fi_prhytj_websites"]["host"].to_list() == ["acme.fi"]
    assert tables["fi_prhytj_addresses"]["city"].to_list() == ["Helsinki"]
    assert tables["fi_prhytj_business_lines"]["business_line_type"].to_list() == ["62010"]


def test_xbrl_structured_extracts_facts():
    xml = b'''<xbrl xmlns="http://www.xbrl.org/2003/instance"
        xmlns:fi_met="http://www.suomi.fi/xbrl/crr/dict/met">
      <context id="c1"><entity><identifier scheme="s">0104539-0</identifier></entity>
        <period><instant>2024-12-31</instant></period></context>
      <unit id="u1"><measure>iso4217:EUR</measure></unit>
      <fi_met:mi53 contextRef="c1" unitRef="u1">1000</fi_met:mi53>
    </xbrl>'''
    stmt = {"business_id": "0104539-0", "financial_date": "2024-12-31",
            "registration_date": "2025-01-10", "object_key": "k", "source_url": "u", "body": xml}
    tables = xbrl_structured_from_statements([stmt], run_id="t", parsed_at=dt.datetime(2026, 1, 1))
    assert tables["fi_prh_xbrl_facts"].height >= 1
    assert "0104539-0" in tables["fi_prh_xbrl_statement_documents"]["business_id"].to_list()
```

- [ ] **Step 4: Run the test to verify it fails**

Run (from repo root):
```bash
cd companies/analysis/finland/notebook && uv run pytest tests/test_structured.py -v
```
Expected: FAIL — `ModuleNotFoundError: conformance.structured` / function not defined.

- [ ] **Step 5: Implement `structured.py`**

Create `companies/analysis/finland/notebook/conformance/structured.py`:

```python
"""Raw -> structured Parquet.

prh_ytj (JSONL): native Polars — read NDJSON into nested structs/lists and
reshape with vectorized expressions. No Python row loop, no copied parser.
prh_xbrl (XML): reuse the copied lxml parser (XML is not tabular), then wrap
its rows into Polars.

Pure: bytes/statements in, dict[table_name -> polars.DataFrame] out. The
notebook handles S3 read and Parquet write at its edges. These functions are
the future structured-layer Dagster assets.
"""

from __future__ import annotations

import datetime as dt

import polars as pl

from conformance._vendor.prh_xbrl_parser import parse_statement_xml
from conformance._vendor import prh_xbrl_tables as xbrl_tables


def ytj_structured_from_ndjson(ndjson: bytes) -> dict[str, pl.DataFrame]:
    """prh_ytj JSONL -> structured frames, idiomatic Polars. Domain rules
    (the status='2' liveness pitfall, current-primary name, URL normalization)
    are expressed as Polars expressions."""
    df = pl.read_ndjson(ndjson).with_columns(
        pl.col("businessId").struct.field("value").alias("business_id")
    )

    statuses = (
        df.select(
            "business_id",
            pl.col("tradeRegisterStatus").alias("trade_register_status"),
            pl.col("registrationDate").alias("registration_date"),
            pl.col("endDate").fill_null("").alias("end_date"),
        )
        # Liveness from tradeRegisterStatus/endDate — never the constant `status` field.
        .with_columns(
            pl.when((pl.col("end_date") != "") | (pl.col("trade_register_status") == "3"))
            .then(pl.lit("ceased")).otherwise(pl.lit("active")).alias("lifecycle_status")
        )
        .with_columns((pl.col("lifecycle_status") == "active").alias("is_active"))
    )

    names = (
        df.select("business_id", "names")
        .explode("names").drop_nulls("names").unnest("names")
        .select(
            "business_id", "name",
            pl.col("type").alias("name_type_code"),
            pl.col("endDate").is_null().alias("is_current"),
            (pl.col("type") == "1").alias("is_primary"),
        )
    )

    websites = (
        df.select("business_id", "website")
        .with_columns(pl.col("website").struct.field("url").alias("url"))
        .filter(pl.col("url").is_not_null() & (pl.col("url") != ""))
        .with_columns(
            pl.when(pl.col("url").str.contains("://")).then(pl.col("url"))
            .otherwise(pl.concat_str([pl.lit("https://"), pl.col("url")])).alias("normalized_url")
        )
        .with_columns(
            pl.col("normalized_url").str.replace(r"^https?://", "").str.split("/").list.first().alias("host"),
            pl.lit(True).alias("is_current"),
            pl.lit(True).alias("is_primary"),
        )
        .select("business_id", "url", "normalized_url", "host", "is_current", "is_primary")
    )

    addresses = (
        df.select("business_id", "addresses")
        .explode("addresses").drop_nulls("addresses").unnest("addresses")
        .with_columns(
            pl.col("postOffices").list.eval(pl.element().struct.field("city")).list.first().alias("city"),
            pl.col("postOffices").list.eval(pl.element().struct.field("municipalityCode")).list.first().alias("municipality_code"),
        )
        .select(
            "business_id",
            pl.col("type").alias("address_type_code"),
            "street",
            pl.col("postCode").alias("post_code"),
            "city", "municipality_code", "country",
        )
    )

    business_lines = (
        df.select("business_id", "mainBusinessLine")
        .with_columns(
            pl.col("mainBusinessLine").struct.field("type").alias("business_line_type"),
            pl.col("mainBusinessLine").struct.field("typeCodeSet").alias("business_line_code_set"),
        )
        .filter(pl.col("business_line_type").is_not_null())
        .select("business_id", "business_line_type", "business_line_code_set")
    )

    return {
        "fi_prhytj_statuses": statuses,
        "fi_prhytj_names": names,
        "fi_prhytj_websites": websites,
        "fi_prhytj_addresses": addresses,
        "fi_prhytj_business_lines": business_lines,
    }


def xbrl_structured_from_statements(
    statements: list[dict], *, run_id: str, parsed_at: dt.datetime
) -> dict[str, pl.DataFrame]:
    by_table: dict[str, list[dict]] = {
        xbrl_tables.STATEMENT_DOCUMENTS_TABLE: [], xbrl_tables.CONTEXTS_TABLE: [],
        xbrl_tables.UNITS_TABLE: [], xbrl_tables.FACTS_TABLE: [],
    }
    for s in statements:
        parsed = parse_statement_xml(
            business_id=s["business_id"], financial_date=s["financial_date"],
            registration_date=s.get("registration_date"), source_url=s["source_url"],
            xml_object_key=s["object_key"], source_run_id=run_id,
            body=s["body"], parsed_at=parsed_at,
        )
        for table, rows in parsed.rows_by_table.items():
            by_table[table].extend(rows)
    # Drop nested fact/context columns Polars can't infer flatly; not consumed downstream.
    drop = {"dimensions", "measures", "schema_refs", "validation_warnings"}
    out = {}
    for table, rows in by_table.items():
        frame = pl.DataFrame(rows, infer_schema_length=None) if rows else pl.DataFrame()
        out[table] = frame.drop([c for c in drop if c in frame.columns]) if rows else frame
    return out
```

- [ ] **Step 6: Run the test to verify it passes**

Run (from repo root):
```bash
cd companies/analysis/finland/notebook && uv run pytest tests/test_structured.py -v
```
Expected: 2 passed.

- [ ] **Step 7: Commit**

```bash
git add companies/analysis/finland/notebook/conformance/_vendor \
        companies/analysis/finland/notebook/conformance/structured.py \
        companies/analysis/finland/notebook/tests/test_structured.py
git commit -m "Phase 5: structured Parquet - native Polars for JSONL, copied parser for XBRL"
```

---

## Phase 6 — Build canonical tables (the next assets)

Define the canonical Polars schemas, a validator, and the four `build_*` functions, then the marimo notebook that runs everything end-to-end. These build functions are the next Dagster assets.

**Files:**
- Create: `NB/conformance/schemas.py`, `validate.py`, `build_company.py`, `build_financials.py`, `build_websites.py`
- Create: `NB/tests/test_validate.py`, `test_build_company.py`, `test_build_financials.py`, `test_build_websites.py`
- Create: `NB/finland_conformance.py`

- [ ] **Step 1: Write `schemas.py` (canonical column sets, single source of truth)**

Create `companies/analysis/finland/notebook/conformance/schemas.py`:

```python
"""Canonical table column sets (subset Finland populates) — the contract in
`companies/analysis/_canonical/canonical_schema.md`, as Polars-checkable schemas.
Only required columns and their dtypes are asserted by validate.py.
"""

import polars as pl

REGISTRATIONS: dict[str, pl.DataType] = {
    "registration_uid": pl.Utf8, "company_uid": pl.Utf8, "country": pl.Utf8,
    "registration_number": pl.Utf8, "registry_source": pl.Utf8, "is_primary": pl.UInt8,
    "entity_role": pl.Utf8, "legal_name": pl.Utf8, "legal_form_code": pl.Utf8,
    "lifecycle_status": pl.Utf8, "is_active": pl.UInt8,
    "incorporation_date": pl.Date, "dissolution_date": pl.Date,
    "addr_street": pl.Utf8, "addr_post_code": pl.Utf8, "addr_city": pl.Utf8,
    "addr_municipality_code": pl.Utf8, "addr_country": pl.Utf8,
    "activity_code": pl.Utf8, "activity_scheme": pl.Utf8,
    "vat_number": pl.Utf8, "eu_id": pl.Utf8, "lei": pl.Utf8, "primary_website": pl.Utf8,
    "source_run_id": pl.Utf8, "ingested_at": pl.Datetime, "updated_at": pl.Datetime,
}

COMPANY: dict[str, pl.DataType] = {
    "company_uid": pl.Utf8, "uid_scheme": pl.Utf8, "lei": pl.Utf8,
    "primary_name": pl.Utf8, "status": pl.Utf8, "legal_form_code": pl.Utf8,
    "home_country": pl.Utf8, "incorporation_date": pl.Date, "dissolution_date": pl.Date,
    "registration_count": pl.UInt16, "operating_countries": pl.List(pl.Utf8),
    "primary_website": pl.Utf8, "sources": pl.List(pl.Utf8),
    "resolution_version": pl.Utf8, "first_seen_at": pl.Datetime, "updated_at": pl.Datetime,
}

FINANCIALS: dict[str, pl.DataType] = {
    "company_uid": pl.Utf8, "registration_uid": pl.Utf8, "country": pl.Utf8,
    "statement_id": pl.Utf8, "period_start": pl.Date, "period_end": pl.Date,
    "period_type": pl.Utf8, "period_reference": pl.Utf8, "basis": pl.Utf8,
    "currency": pl.Utf8, "metric_code": pl.Utf8, "value": pl.Float64,
    "source_metric_id": pl.Utf8, "registry_source": pl.Utf8, "mapping_version": pl.Utf8,
    "source_run_id": pl.Utf8, "ingested_at": pl.Datetime, "updated_at": pl.Datetime,
}

COMPANY_WEBSITES: dict[str, pl.DataType] = {
    "website_uid": pl.Utf8, "company_uid": pl.Utf8, "registration_uid": pl.Utf8,
    "country": pl.Utf8, "scope": pl.Utf8, "url": pl.Utf8, "normalized_url": pl.Utf8,
    "host": pl.Utf8, "is_primary": pl.UInt8, "source_kind": pl.Utf8,
    "discovery_method": pl.Utf8, "registry_source": pl.Utf8, "confidence": pl.Float32,
    "is_live": pl.UInt8, "first_seen_at": pl.Datetime, "last_seen_at": pl.Datetime,
    "updated_at": pl.Datetime,
}

# Finland fills these 4. The other 4 contract tables (persons, company_people,
# company_contacts, company_relationships) are KNOWN-ABSENT in Finland open data.
POPULATED = {"registrations": REGISTRATIONS, "company": COMPANY,
             "financials": FINANCIALS, "company_websites": COMPANY_WEBSITES}
```

- [ ] **Step 2: Write the failing test for `validate.py`**

Create `companies/analysis/finland/notebook/tests/test_validate.py`:

```python
import polars as pl
import pytest

from conformance.schemas import REGISTRATIONS
from conformance.validate import validate_table


def _minimal_registration_row() -> dict:
    return {name: None for name in REGISTRATIONS}


def test_validate_passes_for_correct_columns():
    df = pl.DataFrame([_minimal_registration_row()], schema=REGISTRATIONS)
    validate_table(df, REGISTRATIONS, unique_key="registration_uid")  # no raise


def test_validate_rejects_missing_column():
    df = pl.DataFrame([_minimal_registration_row()], schema=REGISTRATIONS).drop("country")
    with pytest.raises(ValueError, match="missing columns"):
        validate_table(df, REGISTRATIONS)


def test_validate_rejects_duplicate_key():
    row = _minimal_registration_row() | {"registration_uid": "FI:1"}
    df = pl.DataFrame([row, row], schema=REGISTRATIONS)
    with pytest.raises(ValueError, match="duplicate"):
        validate_table(df, REGISTRATIONS, unique_key="registration_uid")
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cd companies/analysis/finland/notebook && uv run pytest tests/test_validate.py -v`
Expected: FAIL — `conformance.validate` not found.

- [ ] **Step 4: Implement `validate.py`**

Create `companies/analysis/finland/notebook/conformance/validate.py`:

```python
"""Assert a canonical DataFrame matches a schema from schemas.py."""

from __future__ import annotations

import polars as pl


def validate_table(df: pl.DataFrame, schema: dict, *, unique_key: str | None = None) -> None:
    required = set(schema)
    present = set(df.columns)
    missing = required - present
    if missing:
        raise ValueError(f"missing columns: {sorted(missing)}")
    for name, dtype in schema.items():
        if df.schema[name] != dtype:
            raise ValueError(f"column {name!r} has dtype {df.schema[name]}, expected {dtype}")
    if unique_key is not None:
        non_null = df.filter(pl.col(unique_key).is_not_null())
        if non_null.height != non_null.select(unique_key).n_unique():
            raise ValueError(f"duplicate values in key column {unique_key!r}")
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd companies/analysis/finland/notebook && uv run pytest tests/test_validate.py -v`
Expected: 3 passed.

- [ ] **Step 6: Commit the schema + validator**

```bash
git add companies/analysis/finland/notebook/conformance/schemas.py \
        companies/analysis/finland/notebook/conformance/validate.py \
        companies/analysis/finland/notebook/tests/test_validate.py
git commit -m "Phase 6a: canonical schemas + validator"
```

- [ ] **Step 7: Write the failing test for `build_company.py`**

Create `companies/analysis/finland/notebook/tests/test_build_company.py`:

```python
import datetime as dt

import polars as pl

from conformance.build_company import build_registrations, build_company
from conformance.schemas import REGISTRATIONS, COMPANY
from conformance.validate import validate_table


def _structured() -> dict[str, pl.DataFrame]:
    now = dt.datetime(2026, 1, 1)
    statuses = pl.DataFrame([{
        "business_id": "0104539-0", "trade_register_status": "1",
        "lifecycle_status": "active", "is_active": True,
        "registration_date": "2001-01-01", "end_date": "",
        "source_run_id": "t", "ingested_at": now,
    }])
    names = pl.DataFrame([{"business_id": "0104539-0", "name": "Acme Oy",
                           "name_type_code": "1", "is_current": True, "is_primary": True}])
    websites = pl.DataFrame([{"business_id": "0104539-0", "normalized_url": "https://acme.fi",
                              "is_current": True}])
    addresses = pl.DataFrame([{"business_id": "0104539-0", "address_type_code": 1,
                               "street": "Main 1", "post_code": "00100", "country": "FI"}])
    business_lines = pl.DataFrame([{"business_id": "0104539-0",
                                    "business_line_type": "62010", "business_line_code_set": "TOL2008"}])
    return {"fi_prhytj_statuses": statuses, "fi_prhytj_names": names,
            "fi_prhytj_websites": websites, "fi_prhytj_addresses": addresses,
            "fi_prhytj_business_lines": business_lines}


def test_build_registrations_matches_schema():
    df = build_registrations(_structured(), run_id="t", now=dt.datetime(2026, 1, 1))
    validate_table(df, REGISTRATIONS, unique_key="registration_uid")
    assert df["registration_uid"].to_list() == ["FI:0104539-0"]
    assert df["company_uid"].to_list()[0].startswith("c:")
    assert df["is_active"].to_list() == [1]


def test_build_company_rolls_up_registrations():
    regs = build_registrations(_structured(), run_id="t", now=dt.datetime(2026, 1, 1))
    df = build_company(regs, now=dt.datetime(2026, 1, 1))
    validate_table(df, COMPANY, unique_key="company_uid")
    assert df.height == 1
    assert df["registration_count"].to_list() == [1]
    assert df["primary_name"].to_list() == ["Acme Oy"]
```

- [ ] **Step 8: Run to verify it fails**

Run: `cd companies/analysis/finland/notebook && uv run pytest tests/test_build_company.py -v`
Expected: FAIL — module not found.

- [ ] **Step 9: Implement `build_company.py`**

Create `companies/analysis/finland/notebook/conformance/build_company.py`:

```python
"""structured prh_ytj tables -> canonical `registrations` and `company`.

Pure: structured DataFrames in, canonical DataFrame out. Future Dagster assets.
Finland is single-key (business_id); company_uid is the surrogate
"c:" + sha1("FI:" + business_id) per the contract (LEI absent in YTJ open data).
"""

from __future__ import annotations

import datetime as dt
import hashlib

import polars as pl

COUNTRY = "FI"
RESOLUTION_VERSION = "finland-v1"


def _registration_uid(business_id: str) -> str:
    return f"{COUNTRY}:{business_id}"


def _company_uid(business_id: str) -> str:
    return "c:" + hashlib.sha1(f"{COUNTRY}:{business_id}".encode()).hexdigest()


def build_registrations(structured: dict[str, pl.DataFrame], *, run_id: str, now: dt.datetime) -> pl.DataFrame:
    statuses = structured["fi_prhytj_statuses"]
    names = structured["fi_prhytj_names"]
    websites = structured.get("fi_prhytj_websites", pl.DataFrame())
    addresses = structured.get("fi_prhytj_addresses", pl.DataFrame())
    lines = structured.get("fi_prhytj_business_lines", pl.DataFrame())

    rows = []
    for s in statuses.iter_rows(named=True):
        bid = s["business_id"]
        primary_name = _current_primary(names, bid, "name", "name_type_code")
        website = _first(websites, bid, "normalized_url")
        addr = _primary_address(addresses, bid)
        line = _first_row(lines, bid)
        rows.append({
            "registration_uid": _registration_uid(bid),
            "company_uid": _company_uid(bid),
            "country": COUNTRY,
            "registration_number": bid,
            "registry_source": "finland_prhytj",
            "is_primary": 1,
            "entity_role": "domestic",
            "legal_name": primary_name,
            "legal_form_code": None,
            "lifecycle_status": s.get("lifecycle_status") or "unknown",
            "is_active": 1 if s.get("is_active") else 0,
            "incorporation_date": _date(s.get("registration_date")),
            "dissolution_date": _date(s.get("end_date")),
            "addr_street": addr.get("street"),
            "addr_post_code": addr.get("post_code"),
            "addr_city": addr.get("city"),
            "addr_municipality_code": addr.get("municipality_code"),
            "addr_country": addr.get("country") or COUNTRY,
            "activity_code": (line or {}).get("business_line_type"),
            "activity_scheme": (line or {}).get("business_line_code_set"),
            "vat_number": None,
            "eu_id": None,
            "lei": None,
            "primary_website": website,
            "source_run_id": run_id,
            "ingested_at": now,
            "updated_at": now,
        })
    from conformance.schemas import REGISTRATIONS
    return pl.DataFrame(rows, schema=REGISTRATIONS)


def build_company(registrations: pl.DataFrame, *, now: dt.datetime) -> pl.DataFrame:
    rows = []
    for company_uid, group in _group_by(registrations, "company_uid"):
        primary = group[0]
        rows.append({
            "company_uid": company_uid,
            "uid_scheme": "surrogate",
            "lei": None,
            "primary_name": primary["legal_name"],
            "status": "active" if any(r["is_active"] for r in group) else "inactive",
            "legal_form_code": primary["legal_form_code"],
            "home_country": COUNTRY,
            "incorporation_date": primary["incorporation_date"],
            "dissolution_date": primary["dissolution_date"],
            "registration_count": len(group),
            "operating_countries": sorted({r["country"] for r in group}),
            "primary_website": primary["primary_website"],
            "sources": ["finland_prhytj"],
            "resolution_version": RESOLUTION_VERSION,
            "first_seen_at": now,
            "updated_at": now,
        })
    from conformance.schemas import COMPANY
    return pl.DataFrame(rows, schema=COMPANY)


# --- helpers ---------------------------------------------------------------
def _date(value) -> dt.date | None:
    if not value:
        return None
    try:
        return dt.date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _current_primary(df: pl.DataFrame, bid: str, value_col: str, type_col: str) -> str | None:
    if df.is_empty():
        return None
    sub = df.filter(pl.col("business_id") == bid)
    if sub.is_empty():
        return None
    primary = sub.filter(pl.col(type_col) == "1") if type_col in sub.columns else sub
    chosen = primary if not primary.is_empty() else sub
    return chosen[value_col].to_list()[0]


def _first(df: pl.DataFrame, bid: str, col: str):
    if df.is_empty():
        return None
    sub = df.filter(pl.col("business_id") == bid)
    return sub[col].to_list()[0] if not sub.is_empty() else None


def _first_row(df: pl.DataFrame, bid: str) -> dict | None:
    if df.is_empty():
        return None
    sub = df.filter(pl.col("business_id") == bid)
    return sub.row(0, named=True) if not sub.is_empty() else None


def _primary_address(df: pl.DataFrame, bid: str) -> dict:
    row = _first_row(df, bid)
    return row or {}


def _group_by(df: pl.DataFrame, key: str):
    for value in df[key].unique().to_list():
        yield value, list(df.filter(pl.col(key) == value).iter_rows(named=True))
```

- [ ] **Step 10: Run to verify it passes**

Run: `cd companies/analysis/finland/notebook && uv run pytest tests/test_build_company.py -v`
Expected: 2 passed. If `_group_by` errors, simplify it as noted.

- [ ] **Step 11: Commit company/registrations build**

```bash
git add companies/analysis/finland/notebook/conformance/build_company.py \
        companies/analysis/finland/notebook/tests/test_build_company.py
git commit -m "Phase 6b: build canonical registrations + company"
```

- [ ] **Step 12: Write the failing test for `build_financials.py`**

Create `companies/analysis/finland/notebook/tests/test_build_financials.py`:

```python
import datetime as dt

import polars as pl

from conformance.build_financials import build_financials, METRIC_MAP
from conformance.schemas import FINANCIALS
from conformance.validate import validate_table


def test_metric_map_has_known_metrics():
    assert ("fi_met:md103", "fi_MC:x673") in METRIC_MAP  # revenue
    assert METRIC_MAP[("fi_met:mi53", "fi_MC:x360")] == "total_assets"


def test_build_financials_maps_facts():
    facts = pl.DataFrame([{
        "statement_key": "s1", "business_id": "0104539-0",
        "financial_date": dt.date(2024, 12, 31),
        "concept_qname": "fi_met:md103", "mcy_member_code": "fi_MC:x673",
        "ref_member_code": None, "numeric_value": 1000.0, "value_kind": "numeric",
    }])
    documents = pl.DataFrame([{"statement_key": "s1", "business_id": "0104539-0",
                               "financial_date": dt.date(2024, 12, 31),
                               "reported_period_start": dt.date(2024, 1, 1),
                               "reported_period_end": dt.date(2024, 12, 31)}])
    df = build_financials(facts, documents, run_id="t", now=dt.datetime(2026, 1, 1))
    validate_table(df, FINANCIALS)
    assert "revenue" in df["metric_code"].to_list()
    assert df.filter(pl.col("metric_code") == "revenue")["value"].to_list() == [1000.0]
    assert df["currency"].unique().to_list() == ["EUR"]
```

- [ ] **Step 13: Run to verify it fails**

Run: `cd companies/analysis/finland/notebook && uv run pytest tests/test_build_financials.py -v`
Expected: FAIL — module not found.

- [ ] **Step 14: Implement `build_financials.py`**

Create `companies/analysis/finland/notebook/conformance/build_financials.py`:

```python
"""structured prh_xbrl facts -> canonical tall `financials`.

Metric map from companies/analysis/finland/prh_xbrl_schema_spike/schema_analysis.md:
(concept_qname, fi_dim:MCY member) -> canonical metric_code. period_reference from
fi_dim:REF: absent = current, present = prior. All observed values are EUR.
"""

from __future__ import annotations

import datetime as dt
import hashlib

import polars as pl

COUNTRY = "FI"
MAPPING_VERSION = "finland-fin-v1"

METRIC_MAP: dict[tuple[str, str], str] = {
    ("fi_met:md103", "fi_MC:x673"): "revenue",
    ("fi_met:md103", "fi_MC:x689"): "operating_profit_loss",
    ("fi_met:md103", "fi_MC:x740"): "profit_loss",
    ("fi_met:mi53", "fi_MC:x360"): "total_assets",
    ("fi_met:mi53", "fi_MC:x376"): "equity",
    ("fi_met:mi53", "fi_MC:x424"): "liabilities",
    ("fi_met:mi53", "fi_MC:x399"): "cash_and_bank",
    ("fi_met:mi53", "fi_MC:x435"): "current_assets",
    ("fi_met:mi53", "fi_MC:x1768"): "current_receivables",
    ("fi_met:mi53", "fi_MC:x1811"): "current_liabilities",
    ("fi_met:md103", "fi_MC:x5"): "personnel_expenses",
    ("fi_met:md103", "fi_MC:x6"): "wages_and_salaries",
}


def _registration_uid(business_id: str) -> str:
    return f"{COUNTRY}:{business_id}"


def _company_uid(business_id: str) -> str:
    return "c:" + hashlib.sha1(f"{COUNTRY}:{business_id}".encode()).hexdigest()


def build_financials(facts: pl.DataFrame, documents: pl.DataFrame, *, run_id: str, now: dt.datetime) -> pl.DataFrame:
    doc_periods = {
        d["statement_key"]: (d.get("reported_period_start"), d.get("reported_period_end"), d["financial_date"])
        for d in documents.iter_rows(named=True)
    }
    rows = []
    for f in facts.iter_rows(named=True):
        metric = METRIC_MAP.get((f.get("concept_qname"), f.get("mcy_member_code")))
        if metric is None or f.get("value_kind") != "numeric" or f.get("numeric_value") is None:
            continue
        bid = f["business_id"]
        start, end, fdate = doc_periods.get(f["statement_key"], (None, None, f["financial_date"]))
        rows.append({
            "company_uid": _company_uid(bid),
            "registration_uid": _registration_uid(bid),
            "country": COUNTRY,
            "statement_id": f["statement_key"],
            "period_start": start,
            "period_end": end or fdate,
            "period_type": "duration" if metric in {"revenue", "operating_profit_loss",
                "profit_loss", "personnel_expenses", "wages_and_salaries"} else "instant",
            "period_reference": "prior" if f.get("ref_member_code") else "current",
            "basis": "individual",
            "currency": "EUR",
            "metric_code": metric,
            "value": float(f["numeric_value"]),
            "source_metric_id": f"{f.get('concept_qname')}/{f.get('mcy_member_code')}",
            "registry_source": "finland_prh_xbrl",
            "mapping_version": MAPPING_VERSION,
            "source_run_id": run_id,
            "ingested_at": now,
            "updated_at": now,
        })
    from conformance.schemas import FINANCIALS
    return pl.DataFrame(rows, schema=FINANCIALS)
```

- [ ] **Step 15: Run to verify it passes**

Run: `cd companies/analysis/finland/notebook && uv run pytest tests/test_build_financials.py -v`
Expected: 2 passed.

- [ ] **Step 16: Commit financials build**

```bash
git add companies/analysis/finland/notebook/conformance/build_financials.py \
        companies/analysis/finland/notebook/tests/test_build_financials.py
git commit -m "Phase 6c: build canonical tall financials"
```

- [ ] **Step 17: Write the failing test for `build_websites.py`**

Create `companies/analysis/finland/notebook/tests/test_build_websites.py`:

```python
import datetime as dt

import polars as pl

from conformance.build_websites import build_websites
from conformance.schemas import COMPANY_WEBSITES
from conformance.validate import validate_table


def test_build_websites_from_registry():
    websites = pl.DataFrame([{"business_id": "0104539-0", "url": "http://acme.fi",
                              "normalized_url": "https://acme.fi", "host": "acme.fi",
                              "is_current": True, "is_primary": True}])
    df = build_websites(websites, run_id="t", now=dt.datetime(2026, 1, 1))
    validate_table(df, COMPANY_WEBSITES, unique_key="website_uid")
    assert df["scope"].to_list() == ["registration"]
    assert df["source_kind"].to_list() == ["registry"]
    assert df["host"].to_list() == ["acme.fi"]
```

- [ ] **Step 18: Run to verify it fails**

Run: `cd companies/analysis/finland/notebook && uv run pytest tests/test_build_websites.py -v`
Expected: FAIL — module not found.

- [ ] **Step 19: Implement `build_websites.py`**

Create `companies/analysis/finland/notebook/conformance/build_websites.py`:

```python
"""structured prh_ytj websites -> canonical `company_websites`.

Registry-provided sites are scope='registration', source_kind='registry',
confidence 1.0. corpscout-discovered sites are out of scope here.
"""

from __future__ import annotations

import datetime as dt
import hashlib

import polars as pl

COUNTRY = "FI"


def _company_uid(business_id: str) -> str:
    return "c:" + hashlib.sha1(f"{COUNTRY}:{business_id}".encode()).hexdigest()


def build_websites(websites: pl.DataFrame, *, run_id: str, now: dt.datetime) -> pl.DataFrame:
    rows = []
    for w in websites.iter_rows(named=True):
        bid = w["business_id"]
        reg_uid = f"{COUNTRY}:{bid}"
        normalized = w.get("normalized_url") or w.get("url") or ""
        if not normalized:
            continue
        rows.append({
            "website_uid": hashlib.sha1(f"registration:{reg_uid}:{normalized}".encode()).hexdigest(),
            "company_uid": _company_uid(bid),
            "registration_uid": reg_uid,
            "country": COUNTRY,
            "scope": "registration",
            "url": w.get("url") or normalized,
            "normalized_url": normalized,
            "host": w.get("host") or "",
            "is_primary": 1 if w.get("is_primary") else 0,
            "source_kind": "registry",
            "discovery_method": "registry_field",
            "registry_source": "finland_prhytj",
            "confidence": 1.0,
            "is_live": 0,
            "first_seen_at": now,
            "last_seen_at": now,
            "updated_at": now,
        })
    from conformance.schemas import COMPANY_WEBSITES
    return pl.DataFrame(rows, schema=COMPANY_WEBSITES)
```

- [ ] **Step 20: Run to verify it passes**

Run: `cd companies/analysis/finland/notebook && uv run pytest tests/test_build_websites.py -v`
Expected: 1 passed.

- [ ] **Step 21: Commit websites build**

```bash
git add companies/analysis/finland/notebook/conformance/build_websites.py \
        companies/analysis/finland/notebook/tests/test_build_websites.py
git commit -m "Phase 6d: build canonical company_websites"
```

- [ ] **Step 22: Write the marimo notebook orchestrator**

Create `companies/analysis/finland/notebook/finland_conformance.py` as a marimo notebook (plain `.py`). It must: read raw from S3, run `structured.py`, write structured Parquet to `output/structured/`, run the `build_*` functions, validate each against `schemas.py`, write canonical Parquet to `output/canonical/`, and print the cardinalities the partition doc needs. Use this structure:

```python
import marimo

app = marimo.App()


@app.cell
def _():
    import datetime as dt
    import json
    import pathlib

    import polars as pl

    from conformance import download
    from conformance import structured as st
    from conformance.build_company import build_registrations, build_company
    from conformance.build_financials import build_financials
    from conformance.build_websites import build_websites
    from conformance import schemas
    from conformance.validate import validate_table

    OUT = pathlib.Path("output")
    RUN_ID = "ref-2025-01"
    NOW = dt.datetime(2026, 6, 14)
    return (build_company, build_financials, build_registrations, build_websites,
            dt, download, json, pl, schemas, st, validate_table, OUT, RUN_ID, NOW)


@app.cell
def _(download, RUN_ID):
    # Phase 3 assets: download raw -> S3 (bounded reference sample).
    ytj_meta = download.download_prh_ytj(RUN_ID, max_companies=2000)
    xbrl_meta = download.download_prh_xbrl(RUN_ID, "2025-01-01", "2025-01-31")
    ytj_meta, xbrl_meta
    return ytj_meta, xbrl_meta


@app.cell
def _(download, json, st, dt, RUN_ID, pl, OUT, ytj_meta, xbrl_meta):
    # Load raw from S3 and run the copied parsers -> structured Parquet.
    s3 = download.s3_client()
    ndjson = s3.get_object(Bucket=download.BUCKET, Key=ytj_meta["snapshot_key"])["Body"].read()
    ytj_tables = st.ytj_structured_from_ndjson(ndjson)

    listing = json.loads(s3.get_object(Bucket=download.BUCKET, Key=xbrl_meta["listing_key"])["Body"].read())
    statements = []
    for d in listing["documents"]:
        body = s3.get_object(Bucket=download.BUCKET, Key=d["object_key"])["Body"].read()
        statements.append({**d, "body": body})
    xbrl_tables = st.xbrl_structured_from_statements(statements, run_id=RUN_ID, parsed_at=dt.datetime(2026, 6, 14))

    (OUT / "structured").mkdir(parents=True, exist_ok=True)
    for name, df in {**ytj_tables, **xbrl_tables}.items():
        if df.height:
            df.write_parquet(OUT / "structured" / f"{name}.parquet")
    return ytj_tables, xbrl_tables


@app.cell
def _(build_registrations, build_company, build_financials, build_websites,
      validate_table, schemas, ytj_tables, xbrl_tables, NOW, RUN_ID, OUT):
    # Build canonical tables and validate against the contract.
    regs = build_registrations(ytj_tables, run_id=RUN_ID, now=NOW)
    comp = build_company(regs, now=NOW)
    fin = build_financials(xbrl_tables["fi_prh_xbrl_facts"],
                           xbrl_tables["fi_prh_xbrl_statement_documents"], run_id=RUN_ID, now=NOW)
    sites = build_websites(ytj_tables["fi_prhytj_websites"], run_id=RUN_ID, now=NOW)

    validate_table(regs, schemas.REGISTRATIONS, unique_key="registration_uid")
    validate_table(comp, schemas.COMPANY, unique_key="company_uid")
    validate_table(fin, schemas.FINANCIALS)
    validate_table(sites, schemas.COMPANY_WEBSITES, unique_key="website_uid")

    (OUT / "canonical").mkdir(parents=True, exist_ok=True)
    for name, df in {"registrations": regs, "company": comp,
                     "financials": fin, "company_websites": sites}.items():
        df.write_parquet(OUT / "canonical" / f"{name}.parquet")
    return comp, fin, regs, sites


@app.cell
def _(comp, fin, regs, sites):
    # Cardinalities for the partition doc. The other 4 contract tables
    # (persons, company_people, company_contacts, company_relationships) are
    # KNOWN-ABSENT in Finland open data.
    {
        "company": comp.height, "registrations": regs.height,
        "financials": fin.height, "company_websites": sites.height,
        "distinct_companies": comp["company_uid"].n_unique(),
        "financial_periods": fin["period_end"].n_unique() if fin.height else 0,
    }
    return


if __name__ == "__main__":
    app.run()
```

- [ ] **Step 23: Run the full unit-test suite**

Run: `cd companies/analysis/finland/notebook && uv run pytest -v`
Expected: all tests pass (structured, validate, build_company, build_financials, build_websites).

- [ ] **Step 24: Run the notebook end-to-end against real S3 (bounded sample)**

Requires `CORPSCOUT_S3_*` env vars. Run:
```bash
cd companies/analysis/finland/notebook && uv run python finland_conformance.py
```
Expected: completes without validation errors; `output/canonical/{registrations,company,financials,company_websites}.parquet` exist; the final cell prints non-zero `company`, `registrations`, `company_websites` counts (financials may be small for a one-month window). Spot-check:
```bash
cd companies/analysis/finland/notebook && uv run python -c "
import polars as pl
print(pl.read_parquet('output/canonical/company.parquet').head())
print(pl.read_parquet('output/canonical/financials.parquet').head())
"
```

- [ ] **Step 25: Commit the notebook**

```bash
git add companies/analysis/finland/notebook/finland_conformance.py
git commit -m "Phase 6e: marimo notebook runs Finland end-to-end to canonical Parquet"
```

---

## After all phases

The partition doc (`partitioning.md`) is the next deliverable, written from the cardinalities the notebook prints — handled as a follow-up once real numbers are in hand.

## Notes for the implementer

- **Real-data fragility:** the `build_*` functions assume the structured column names produced in `structured.py` (e.g. `fi_prhytj_statuses.business_id` from the native Polars transform, `fi_prh_xbrl_facts.concept_qname` from the copied parser). If a column name differs at runtime, fix the reference in the build function and its test together — do not loosen the schema in `schemas.py`.
- **Native Polars schema inference (prh_ytj):** `pl.read_ndjson` infers struct/list fields from the data. If an optional field (e.g. `website`) is absent across the whole sample, `.struct.field(...)` raises — guard optional structs (`"website" in df.columns`) and validate against a real snapshot. `.list.eval(pl.element().struct.field(...))` needs a recent Polars; confirm the version resolved in Phase 1 supports it.
- **XBRL nested columns:** the XBRL parser rows carry nested fields (`dimensions`, `measures`, `schema_refs`, `validation_warnings`); `structured.py` drops them before building the DataFrame since no `build_*` consumes them.
- **S3 absence:** if `CORPSCOUT_S3_*` is unset, Phases 3/6 end-to-end steps can't run; the unit tests (which use synthetic data) still must pass. Do not mark a phase confirmed on unit tests alone where the step calls for an S3 smoke run — flag it for the user instead.
