# Slovakia Financials Split Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split the monolithic `slovakia_financials_incremental` asset into four independent, individually re-runnable assets: RÚZ raw download → S3, S3 → DuckDB metrics decode, EUR→USD conversion, and ClickHouse export.

**Architecture:** The download asset sweeps the RÚZ API after the existing DuckDB id-cursor and stores *raw, undecoded* JSON statement bundles as NDJSON batches in S3 (bucket `source-slovakia-financials`), with templates deduplicated under a shared prefix. The decode asset lists S3 batches, anti-joins a `processed_batches` DuckDB bookkeeping table, and appends decoded rows into an *accumulating* `slovakia_financials.financial_metrics` DuckDB table (per-batch delete+insert = idempotent reprocess). USD conversion stays a separate set-based SQL step. The ClickHouse export switches from append to the repo-standard atomic full replace (stage + `EXCHANGE TABLES`) of the accumulated table.

**Tech Stack:** Dagster (`dg.asset`, deps chains, op pools), DuckDB, boto3 via the shared `ObjectStoreResource`, `dlt.sources.helpers.requests` HTTP session (inside `RuzClient`), ClickHouse via `dagster_clickhouse`, pytest.

## Design decisions locked in (and why)

- **Raw fidelity to S3:** the download asset stores exactly what the API returned (statement, entity, all reports incl. non-public stubs, deleted statements) — decoding/filtering happens downstream. Re-decoding history never re-hits the API.
- **Cursor stays in DuckDB** (`incremental.py`, unchanged): preserves state continuity for the existing deployment, reuses tested code. *Rejected alternative:* deriving the cursor from S3 batch key names — clean but restarts the sweep from 0 on cutover unless seeded; not worth the migration ceremony.
- **Batch key encodes the id range** (`batch-{after:012d}-{last:012d}/statements.ndjson`) so listings are self-describing without GETs.
- **Templates stored once** under `slovakia_financials/templates/template-{id}.json` (exists-check skip), not per batch — they repeat across thousands of statements.
- **DuckDB metrics table becomes accumulating** (was: replaced with only the latest batch each run). Extra `source_batch_key` column (NOT exported — the export passes the explicit `SK_FINANCIAL_METRICS_COLUMNS` tuple) enables per-batch idempotent delete+insert.
- **ClickHouse export switches to `truncate=True`** (atomic full replace): now that DuckDB holds full history, this matches the repo golden path and stops accumulating duplicate row versions in the `ReplacingMergeTree`. No ClickHouse migration needed — migration 000043 is untouched.
- **Job and schedule keep their names** (`slovakia_financials_incremental_job`, `slovakia_financials_incremental_schedule`, cron `0 5 * * *`) so schedule state survives the cutover.
- **`object_store` resource** is already bound globally (via `Definitions.merge`; `finland_xbrl` provides `"object_store": ObjectStoreResource()`), same as `sweden_financial` relies on — no new resource binding.

## Global Constraints

- Work in `corpscout/dagster_v3` (all paths below are relative to it). Run everything with `uv run` (e.g. `uv run pytest`, `uv run dg check defs`).
- **No `from __future__ import annotations` in any module defining `@dg.asset`** (breaks Dagster context-type validation).
- Every asset touching `data/slovakia_financials_source.duckdb` carries `pool=SLOVAKIA_FINANCIALS_DUCKDB_POOL` (`"slovakia_financials_duckdb"`).
- ClickHouse: migration owns DDL; code only asserts existence + exchanges. Export the `tables.SK_FINANCIAL_METRICS_COLUMNS` tuple, nothing more. Refuse to replace on zero rows (`raise ValueError`).
- HTTP via `dlt.sources.helpers.requests` (already inside `RuzClient`) — never plain `requests`.
- Never transform with Python row loops in SQL-able steps; the per-statement decode loop is inherently per-record (nested JSON → row) and is the existing accepted shape.
- **Commit by explicit path only** — never `git add -A`.
- Validate before done: `uv run dg check defs` + `uv run pytest tests/test_slovakia_financials.py`.

## File structure

- Create: `src/dagster_v3/defs/slovakia_financials/raw_store.py` — S3 layout + batch/template IO (no HTTP, no DuckDB).
- Create: `src/dagster_v3/defs/slovakia_financials/download.py` — RÚZ sweep → S3 bundles (no DuckDB except via caller).
- Modify: `src/dagster_v3/defs/slovakia_financials/metrics.py` — pure `decode_statement_bundle` replaces client-coupled `process_statement`; `build_metrics_from_batches` (S3 → accumulating DuckDB) replaces `build_slovakia_financials`; `apply_slovakia_usd_conversion` kept.
- Modify: `src/dagster_v3/defs/slovakia_financials/clickhouse.py` — atomic replace + empty guard.
- Rewrite: `src/dagster_v3/defs/slovakia_financials/assets.py` — 4 assets + job + schedule.
- Rework: `tests/test_slovakia_financials.py` — same file, new fakes + tests per stage.
- Create: `src/dagster_v3/defs/slovakia_financials/docs/slovakia_financials-design.md` — required per-source design doc.
- Unchanged: `client.py`, `incremental.py`, `tables.py` (except no changes needed), `clickhouse/migrations/000043_*`.

---

### Task 0: Baseline

**Files:** none modified.

- [ ] **Step 1: Confirm clean baseline in the worktree**

Run (from `corpscout/dagster_v3`):
```bash
uv run pytest tests/test_slovakia_financials.py -q
```
Expected: all tests PASS (10 tests). If anything fails, STOP and report — do not proceed on a broken baseline.

- [ ] **Step 2: Confirm defs load**

Run: `uv run dg check defs`
Expected: exit 0, "All components validated successfully." (or equivalent success output).

---

### Task 1: `raw_store.py` — S3 batch layout and IO

**Files:**
- Create: `src/dagster_v3/defs/slovakia_financials/raw_store.py`
- Test: `tests/test_slovakia_financials.py` (append new section; keep existing tests untouched for now)

**Interfaces:**
- Consumes: `ObjectStoreResource` duck-typed methods `write_bytes(key, body, bucket=)`, `read_bytes(key, bucket=)`, `exists(key, bucket=)`, `list_keys(prefix, bucket=)`.
- Produces (used by Tasks 3, 4, 6):
  - `RAW_BUCKET: str = "source-slovakia-financials"`
  - `statement_batch_key(after_id: int, last_id: int) -> str`
  - `manifest_key_for(batch_key: str) -> str`
  - `parse_batch_id_range(batch_key: str) -> tuple[int, int] | None`
  - `write_statement_batch(object_store, *, after_id: int, last_id: int, bundles: list[dict], manifest: dict) -> str` (returns batch key)
  - `list_statement_batch_keys(object_store) -> list[str]` (sorted)
  - `read_statement_batch(object_store, batch_key: str) -> list[dict]`
  - `template_exists(object_store, template_id: int) -> bool`
  - `write_template(object_store, template_id: int, template: dict) -> None`
  - `read_template(object_store, template_id: int) -> dict`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_slovakia_financials.py` (below the existing imports; add `import json` to the imports and this fake + tests at the end of the file):

```python
from dagster_v3.defs.slovakia_financials import raw_store


class FakeObjectStore:
    """In-memory stand-in for ObjectStoreResource (mirrors the Sweden test fake)."""

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def ensure_bucket(self, bucket: str | None = None) -> None:
        pass

    def write_bytes(self, key: str, body: bytes, bucket: str | None = None) -> None:
        assert bucket is not None
        self.objects[(bucket, key)] = body

    def read_bytes(self, key: str, bucket: str | None = None) -> bytes:
        assert bucket is not None
        return self.objects[(bucket, key)]

    def exists(self, key: str, bucket: str | None = None) -> bool:
        assert bucket is not None
        return (bucket, key) in self.objects

    def list_keys(self, prefix: str, bucket: str | None = None) -> list[str]:
        assert bucket is not None
        return [k for (b, k) in self.objects if b == bucket and k.startswith(prefix)]


def test_batch_key_roundtrip():
    key = raw_store.statement_batch_key(0, 5002)
    assert key == (
        "slovakia_financials/raw_statements/batch-000000000000-000000005002/statements.ndjson"
    )
    assert raw_store.parse_batch_id_range(key) == (0, 5002)
    assert raw_store.parse_batch_id_range("slovakia_financials/raw_statements/junk") is None
    assert raw_store.manifest_key_for(key) == (
        "slovakia_financials/raw_statements/batch-000000000000-000000005002/manifest.json"
    )


def test_write_and_read_statement_batch():
    store = FakeObjectStore()
    bundles = [
        {"statement_id": 5001, "statement": {"id": 5001}, "entity": {"ico": "1"}, "reports": []},
        {"statement_id": 5002, "statement": {"stav": "ZMAZANÉ"}, "entity": None, "reports": []},
    ]
    key = raw_store.write_statement_batch(
        store, after_id=5000, last_id=5002, bundles=bundles,
        manifest={"source_run_id": "r1", "statement_count": 2},
    )
    assert raw_store.list_statement_batch_keys(store) == [key]
    assert raw_store.read_statement_batch(store, key) == bundles
    manifest = json.loads(
        store.read_bytes(raw_store.manifest_key_for(key), bucket=raw_store.RAW_BUCKET)
    )
    assert manifest["statement_count"] == 2


def test_template_store_roundtrip():
    store = FakeObjectStore()
    assert raw_store.template_exists(store, 699) is False
    raw_store.write_template(store, 699, {"nazov": "Úč POD"})
    assert raw_store.template_exists(store, 699) is True
    assert raw_store.read_template(store, 699) == {"nazov": "Úč POD"}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_slovakia_financials.py -k "batch_key or statement_batch or template_store" -v`
Expected: FAIL with `ImportError: cannot import name 'raw_store'` (collection error).

- [ ] **Step 3: Implement `raw_store.py`**

Create `src/dagster_v3/defs/slovakia_financials/raw_store.py`:

```python
"""S3 layout + IO for raw RÚZ statement batches.

One download run stores one batch: `statements.ndjson` (one JSON bundle per
statement: raw statement + raw entity + raw reports, exactly as the API
returned them) plus a small `manifest.json`. Statement templates repeat
across thousands of statements, so they are stored once under a shared
`templates/` prefix. The batch directory name encodes the swept id range,
so listings are self-describing without reading any object.
"""

import json
import re
from typing import Any

RAW_BUCKET = "source-slovakia-financials"
BATCH_PREFIX = "slovakia_financials/raw_statements/"
TEMPLATE_PREFIX = "slovakia_financials/templates/"

_BATCH_KEY_RE = re.compile(r"batch-(\d{12})-(\d{12})/statements\.ndjson$")


def statement_batch_key(after_id: int, last_id: int) -> str:
    return f"{BATCH_PREFIX}batch-{after_id:012d}-{last_id:012d}/statements.ndjson"


def manifest_key_for(batch_key: str) -> str:
    return batch_key.rsplit("/", 1)[0] + "/manifest.json"


def parse_batch_id_range(batch_key: str) -> tuple[int, int] | None:
    match = _BATCH_KEY_RE.search(batch_key)
    if match is None:
        return None
    return int(match.group(1)), int(match.group(2))


def write_statement_batch(
    object_store: Any,
    *,
    after_id: int,
    last_id: int,
    bundles: list[dict[str, Any]],
    manifest: dict[str, Any],
) -> str:
    batch_key = statement_batch_key(after_id, last_id)
    body = "\n".join(json.dumps(bundle, ensure_ascii=False) for bundle in bundles) + "\n"
    object_store.write_bytes(batch_key, body.encode("utf-8"), bucket=RAW_BUCKET)
    object_store.write_bytes(
        manifest_key_for(batch_key),
        json.dumps(manifest, ensure_ascii=False).encode("utf-8"),
        bucket=RAW_BUCKET,
    )
    return batch_key


def list_statement_batch_keys(object_store: Any) -> list[str]:
    return sorted(
        key
        for key in object_store.list_keys(BATCH_PREFIX, bucket=RAW_BUCKET)
        if key.endswith("/statements.ndjson")
    )


def read_statement_batch(object_store: Any, batch_key: str) -> list[dict[str, Any]]:
    body = object_store.read_bytes(batch_key, bucket=RAW_BUCKET).decode("utf-8")
    return [json.loads(line) for line in body.splitlines() if line.strip()]


def template_key(template_id: int) -> str:
    return f"{TEMPLATE_PREFIX}template-{int(template_id)}.json"


def template_exists(object_store: Any, template_id: int) -> bool:
    return object_store.exists(template_key(template_id), bucket=RAW_BUCKET)


def write_template(object_store: Any, template_id: int, template: dict[str, Any]) -> None:
    object_store.write_bytes(
        template_key(template_id),
        json.dumps(template, ensure_ascii=False).encode("utf-8"),
        bucket=RAW_BUCKET,
    )


def read_template(object_store: Any, template_id: int) -> dict[str, Any]:
    return json.loads(
        object_store.read_bytes(template_key(template_id), bucket=RAW_BUCKET).decode("utf-8")
    )
```

(`object_store: Any` keeps the module import-light and fake-friendly; the real
`ObjectStoreResource` satisfies it structurally, same approach as `client.py`'s
`HttpSession` protocol.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_slovakia_financials.py -v`
Expected: ALL tests PASS (existing 10 + new 3).

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/slovakia_financials/raw_store.py tests/test_slovakia_financials.py
git commit -m "feat(slovakia): add S3 raw-batch store for RÚZ statement bundles"
```

---

### Task 2: Pure decode — `decode_statement_bundle` in `metrics.py`

**Files:**
- Modify: `src/dagster_v3/defs/slovakia_financials/metrics.py` (add function; do NOT remove `process_statement`/`build_slovakia_financials` yet — Task 4 removes them)
- Test: `tests/test_slovakia_financials.py`

**Interfaces:**
- Consumes: existing `extract_report_metrics`, `_period_dates`, `METRIC_NAMES` (all already in `metrics.py`).
- Produces (used by Task 4):
  - `decode_statement_bundle(bundle: dict[str, Any], template_lookup: Callable[[int], dict[str, Any]]) -> dict[str, Any] | None` — same row-dict shape as `process_statement` returns today (keys: `statement_id`, `ruz_entity_id`, `ico`, `template_name`, `statement_type`, `fiscal_year`, `period_start`, `period_end`, `filed_date`, `approved_date`, `currency`, `metrics`, `mapped_metric_count`, `template_mapped`); `None` for deleted statements.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_slovakia_financials.py`:

```python
def _pod_bundle() -> dict:
    return {
        "statement_id": 5001,
        "statement": {
            "id": 5001, "idUJ": 9001, "idUctovnychVykazov": [7001],
            "obdobieOd": "2023-01", "obdobieDo": "2023-12",
            "datumZostaveniaK": "2023-12-31", "typ": "Riadna",
            "datumPodania": "2024-03-15", "datumSchvalenia": "2024-04-01",
        },
        "entity": {"id": 9001, "ico": "36355232"},
        "reports": [
            {"id": 7001, "idSablony": 699, "pristupnostDat": "Verejné", "obsah": POD_OBSAH}
        ],
    }


def test_decode_statement_bundle_resolves_entity_and_metrics():
    row = metrics.decode_statement_bundle(_pod_bundle(), lambda tid: {699: POD_TEMPLATE}[tid])
    assert row["ico"] == "36355232" and row["ruz_entity_id"] == "9001"
    assert row["template_name"] == "Úč POD" and row["template_mapped"] == 1
    assert row["fiscal_year"] == 2023 and row["period_end"] == "2023-12-31"
    assert row["metrics"]["net_result"] == 120.0 and row["mapped_metric_count"] == 6


def test_decode_statement_bundle_skips_deleted():
    bundle = {"statement_id": 5002, "statement": {"stav": "ZMAZANÉ"}, "entity": None, "reports": []}
    assert metrics.decode_statement_bundle(bundle, lambda tid: {}) is None


def test_decode_statement_bundle_ignores_non_public_reports():
    bundle = _pod_bundle()
    bundle["reports"][0]["pristupnostDat"] = "Neverejné"
    row = metrics.decode_statement_bundle(bundle, lambda tid: {699: POD_TEMPLATE}[tid])
    assert row["template_mapped"] == 0 and row["mapped_metric_count"] == 0
    assert row["metrics"]["net_result"] is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_slovakia_financials.py -k decode_statement_bundle -v`
Expected: FAIL with `AttributeError: module ... has no attribute 'decode_statement_bundle'`.

- [ ] **Step 3: Implement `decode_statement_bundle`**

Add to `src/dagster_v3/defs/slovakia_financials/metrics.py` (directly below `process_statement`):

```python
def decode_statement_bundle(
    bundle: dict[str, Any],
    template_lookup: Callable[[int], dict[str, Any]],
) -> dict[str, Any] | None:
    """Decode one raw S3 statement bundle into a flat metrics row.

    Pure counterpart of `process_statement`: same output shape, but reads the
    already-fetched raw JSON (statement + entity + reports) instead of calling
    the API. Returns None for deleted statements.
    """
    statement = bundle.get("statement") or {}
    if statement.get("stav") == "ZMAZANÉ":
        return None
    statement_id = int(bundle["statement_id"])
    entity = bundle.get("entity") or {}
    entity_id = statement.get("idUJ")
    ico = str(entity.get("ico") or "")

    merged: dict[str, float | None] = {metric: None for metric in METRIC_NAMES}
    template_name = ""
    mapped = False
    for report in bundle.get("reports") or []:
        if report.get("pristupnostDat") != "Verejné":
            continue
        obsah = report.get("obsah") or {}
        if "tabulky" not in obsah:
            continue
        template_id = report.get("idSablony")
        if template_id is None:
            continue
        template = template_lookup(int(template_id))
        extracted, family = extract_report_metrics(obsah, template)
        if extracted is None:
            continue
        mapped = True
        template_name = str(template.get("nazov") or family or "")
        for metric, value in extracted.items():
            if merged[metric] is None and value is not None:
                merged[metric] = value

    period_start, period_end, fiscal_year = _period_dates(statement)
    return {
        "statement_id": statement_id,
        "ruz_entity_id": str(entity_id) if entity_id is not None else "",
        "ico": ico,
        "template_name": template_name,
        "statement_type": str(statement.get("typ") or ""),
        "fiscal_year": fiscal_year,
        "period_start": period_start,
        "period_end": period_end,
        "filed_date": statement.get("datumPodania"),
        "approved_date": statement.get("datumSchvalenia"),
        "currency": "EUR",
        "metrics": merged,
        "mapped_metric_count": sum(1 for v in merged.values() if v is not None),
        "template_mapped": 1 if mapped else 0,
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_slovakia_financials.py -v`
Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/slovakia_financials/metrics.py tests/test_slovakia_financials.py
git commit -m "feat(slovakia): add pure decode_statement_bundle for raw S3 bundles"
```

---

### Task 3: `download.py` — RÚZ sweep to S3

**Files:**
- Create: `src/dagster_v3/defs/slovakia_financials/download.py`
- Test: `tests/test_slovakia_financials.py`

**Interfaces:**
- Consumes: `RuzClient` (`statement_ids`, `statement`, `entity`, `report`, `template` — existing), `raw_store` (Task 1).
- Produces (used by Task 6):
  - `sweep_statements_to_s3(*, object_store, source_run_id: str, after_id: int, max_statements: int, changed_since: str = tables.RUZ_CHANGED_SINCE, client: RuzClient | None = None, request_delay_seconds: float = 0.05, log=None) -> dict[str, Any]` — returns `{"fetched_ids": int, "stored_statements": int, "fetch_failed": int, "templates_stored": int, "last_id": int, "batch_key": str | None}`. Writes nothing when the sweep returns zero ids. Does NOT touch the cursor (the asset owns that).

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_slovakia_financials.py`. First extend `FakeRuzClient` with call tracking — add one line to its `__init__` (after `self.templates = ...`): `self.template_calls = 0`, and change its `template` method to:

```python
    def template(self, tid):
        self.template_calls += 1
        return self.templates[tid]
```

Then add the tests:

```python
from dagster_v3.defs.slovakia_financials import download


def test_sweep_statements_to_s3_stores_raw_batch_and_templates():
    store = FakeObjectStore()
    client = FakeRuzClient()
    counts = download.sweep_statements_to_s3(
        object_store=store, source_run_id="r1", after_id=5000,
        max_statements=10, client=client, request_delay_seconds=0,
    )
    assert counts["fetched_ids"] == 2 and counts["stored_statements"] == 2
    assert counts["fetch_failed"] == 0 and counts["last_id"] == 5002
    assert counts["batch_key"] == raw_store.statement_batch_key(5000, 5002)

    bundles = raw_store.read_statement_batch(store, counts["batch_key"])
    assert [b["statement_id"] for b in bundles] == [5001, 5002]
    assert bundles[0]["entity"]["ico"] == "36355232"
    assert bundles[0]["reports"][0]["obsah"] == POD_OBSAH
    assert bundles[1]["statement"]["stav"] == "ZMAZANÉ"  # deleted stored raw, decoded later

    assert raw_store.read_template(store, 699) == POD_TEMPLATE
    assert counts["templates_stored"] == 1
    manifest = json.loads(
        store.read_bytes(
            raw_store.manifest_key_for(counts["batch_key"]), bucket=raw_store.RAW_BUCKET
        )
    )
    assert manifest["source_run_id"] == "r1" and manifest["statement_count"] == 2


def test_sweep_statements_to_s3_skips_existing_templates():
    store = FakeObjectStore()
    raw_store.write_template(store, 699, POD_TEMPLATE)
    client = FakeRuzClient()
    counts = download.sweep_statements_to_s3(
        object_store=store, source_run_id="r1", after_id=5000,
        max_statements=10, client=client, request_delay_seconds=0,
    )
    assert client.template_calls == 0 and counts["templates_stored"] == 0


def test_sweep_statements_to_s3_empty_sweep_writes_nothing():
    store = FakeObjectStore()
    counts = download.sweep_statements_to_s3(
        object_store=store, source_run_id="r1", after_id=6000,
        max_statements=10, client=FakeRuzClient(), request_delay_seconds=0,
    )
    assert counts["batch_key"] is None and counts["last_id"] == 6000
    assert store.objects == {}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_slovakia_financials.py -k sweep_statements -v`
Expected: FAIL with `ImportError: cannot import name 'download'` (collection error).

- [ ] **Step 3: Implement `download.py`**

Create `src/dagster_v3/defs/slovakia_financials/download.py`:

```python
"""Forward sweep of the RÚZ API into raw S3 statement batches.

Fetch-only: stores statements/entities/reports exactly as returned (including
deleted statements and non-public report stubs); decoding and filtering happen
downstream in metrics.build_metrics_from_batches. A statement whose fetch
fails is counted and skipped — mirroring the previous inline behaviour, later
statements still advance last_id, so failed ids are not retried.
"""

import time
from collections.abc import Callable
from typing import Any

from dagster_v3.defs.slovakia_financials import raw_store, tables
from dagster_v3.defs.slovakia_financials.client import RuzClient


def fetch_statement_bundle(
    client: RuzClient,
    statement_id: int,
    *,
    entity_cache: dict[int, dict[str, Any]],
) -> dict[str, Any]:
    statement = client.statement(statement_id)
    if statement.get("stav") == "ZMAZANÉ":
        return {"statement_id": statement_id, "statement": statement, "entity": None, "reports": []}
    entity = None
    entity_id = statement.get("idUJ")
    if entity_id is not None:
        entity = entity_cache.get(entity_id)
        if entity is None:
            entity = client.entity(entity_id)
            entity_cache[entity_id] = entity
    reports = [client.report(report_id) for report_id in statement.get("idUctovnychVykazov") or []]
    return {"statement_id": statement_id, "statement": statement, "entity": entity, "reports": reports}


def sweep_statements_to_s3(
    *,
    object_store: Any,
    source_run_id: str,
    after_id: int,
    max_statements: int,
    changed_since: str = tables.RUZ_CHANGED_SINCE,
    client: RuzClient | None = None,
    request_delay_seconds: float = 0.05,
    log: Callable[..., object] | None = None,
) -> dict[str, Any]:
    """Fetch up to `max_statements` raw bundles after `after_id`, store one S3 batch."""
    ruz = client or RuzClient()
    statement_ids, _ = ruz.statement_ids(
        changed_since=changed_since, after_id=after_id, max_records=max_statements
    )
    entity_cache: dict[int, dict[str, Any]] = {}
    seen_template_ids: set[int] = set()
    bundles: list[dict[str, Any]] = []
    fetch_failed = 0
    templates_stored = 0
    last_id = after_id
    for index, statement_id in enumerate(statement_ids):
        if index and request_delay_seconds:
            time.sleep(request_delay_seconds)
        try:
            bundle = fetch_statement_bundle(ruz, statement_id, entity_cache=entity_cache)
            templates_stored += _store_missing_templates(
                ruz, object_store, bundle["reports"], seen_template_ids
            )
        except Exception as exc:  # noqa: BLE001 - count, don't hide
            fetch_failed += 1
            if log is not None:
                log("RÚZ statement %s fetch failed: %s", statement_id, exc)
            continue
        last_id = statement_id
        bundles.append(bundle)

    batch_key = None
    if bundles:
        batch_key = raw_store.write_statement_batch(
            object_store,
            after_id=after_id,
            last_id=last_id,
            bundles=bundles,
            manifest={
                "source_run_id": source_run_id,
                "after_id": after_id,
                "last_id": last_id,
                "statement_count": len(bundles),
                "fetch_failed": fetch_failed,
                "source_url": tables.RUZ_BASE_URL,
            },
        )
    counts: dict[str, Any] = {
        "fetched_ids": len(statement_ids),
        "stored_statements": len(bundles),
        "fetch_failed": fetch_failed,
        "templates_stored": templates_stored,
        "last_id": last_id,
        "batch_key": batch_key,
    }
    if log is not None:
        log(
            "Stored Slovak RÚZ raw batch: ids=%s stored=%s failed=%s templates=%s "
            "last_id=%s key=%s",
            len(statement_ids), len(bundles), fetch_failed, templates_stored,
            last_id, batch_key,
        )
    return counts


def _store_missing_templates(
    client: RuzClient,
    object_store: Any,
    reports: list[dict[str, Any]],
    seen_template_ids: set[int],
) -> int:
    stored = 0
    for report in reports:
        if report.get("pristupnostDat") != "Verejné":
            continue
        if "tabulky" not in (report.get("obsah") or {}):
            continue
        template_id = report.get("idSablony")
        if template_id is None or int(template_id) in seen_template_ids:
            continue
        template_id = int(template_id)
        if not raw_store.template_exists(object_store, template_id):
            raw_store.write_template(object_store, template_id, client.template(template_id))
            stored += 1
        seen_template_ids.add(template_id)
    return stored
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_slovakia_financials.py -v`
Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/slovakia_financials/download.py tests/test_slovakia_financials.py
git commit -m "feat(slovakia): add RÚZ raw sweep to S3 statement batches"
```

---

### Task 4: Accumulating DuckDB build — `build_metrics_from_batches`

**Files:**
- Modify: `src/dagster_v3/defs/slovakia_financials/metrics.py`
- Test: `tests/test_slovakia_financials.py`

**Interfaces:**
- Consumes: `raw_store.list_statement_batch_keys` / `read_statement_batch` / `read_template` (Task 1), `decode_statement_bundle` (Task 2).
- Produces (used by Tasks 5, 6):
  - `build_metrics_from_batches(*, connection: duckdb.DuckDBPyConnection, object_store, source_run_id: str, log=None) -> dict[str, int]` — returns `{"batches_processed", "statements", "skipped", "table_statements", "table_mapped"}`.
  - `PROCESSED_BATCHES_TABLE: str` (qualified name `slovakia_financials.processed_batches`).
  - DuckDB `slovakia_financials.financial_metrics` schema = `tables.SK_FINANCIAL_METRICS_COLUMNS` + trailing `source_batch_key varchar`.
  - `apply_slovakia_usd_conversion` unchanged signature, but now also ensures the table exists first (safe on empty DB).
- Removes: `process_statement`, `build_slovakia_financials`, `_write_metrics_table` (the old client-coupled path) and their tests.

- [ ] **Step 1: Rewrite the build/USD tests**

In `tests/test_slovakia_financials.py`:

1. DELETE these tests: `test_process_statement_skips_deleted`, `test_process_statement_resolves_entity_and_metrics`, `test_build_writes_metrics_table`, `test_usd_conversion_fills_amounts`.
2. ADD a seeding helper and the new tests:

```python
def _seed_raw_batch(store: FakeObjectStore) -> str:
    raw_store.write_template(store, 699, POD_TEMPLATE)
    deleted = {"statement_id": 5002, "statement": {"id": 5002, "stav": "ZMAZANÉ"},
               "entity": None, "reports": []}
    return raw_store.write_statement_batch(
        store, after_id=5000, last_id=5002, bundles=[_pod_bundle(), deleted],
        manifest={"source_run_id": "r0", "statement_count": 2},
    )


def test_build_metrics_from_batches_writes_rows(tmp_path):
    store = FakeObjectStore()
    _seed_raw_batch(store)
    db = tmp_path / "skfin.duckdb"
    with duckdb.connect(str(db)) as con:
        counts = metrics.build_metrics_from_batches(
            connection=con, object_store=store, source_run_id="r1"
        )
    assert counts["batches_processed"] == 1 and counts["statements"] == 1
    assert counts["skipped"] == 1 and counts["table_statements"] == 1
    with duckdb.connect(str(db), read_only=True) as con:
        cols = [r[0] for r in con.execute(
            f"describe {tables.DLT_DATASET_NAME}.{tables.METRICS_TABLE}"
        ).fetchall()]
        assert cols == [*tables.SK_FINANCIAL_METRICS_COLUMNS, "source_batch_key"]
        row = con.execute(
            f"select ico, statement_id, fiscal_year, period_end_date, "
            f"total_assets_amount_original, total_assets_amount_usd, "
            f"net_result_amount_original, currency_original, source_batch_key "
            f"from {tables.DLT_DATASET_NAME}.{tables.METRICS_TABLE}"
        ).fetchone()
    assert row[0] == "36355232" and row[1] == "5001" and row[2] == 2023
    assert row[3] == dt.date(2023, 12, 31)
    assert float(row[4]) == 880.0 and row[5] is None  # usd not yet filled
    assert float(row[6]) == 120.0 and row[7] == "EUR"
    assert row[8] == raw_store.statement_batch_key(5000, 5002)


def test_build_metrics_from_batches_is_incremental_and_idempotent(tmp_path):
    store = FakeObjectStore()
    _seed_raw_batch(store)
    db = tmp_path / "skfin.duckdb"
    with duckdb.connect(str(db)) as con:
        metrics.build_metrics_from_batches(connection=con, object_store=store, source_run_id="r1")
        # second run: batch already recorded as processed → no-op
        counts = metrics.build_metrics_from_batches(
            connection=con, object_store=store, source_run_id="r2"
        )
        assert counts["batches_processed"] == 0 and counts["table_statements"] == 1
        # a second batch appends without touching the first
        bundle = _pod_bundle()
        bundle["statement_id"] = 5003
        bundle["statement"]["id"] = 5003
        raw_store.write_statement_batch(
            store, after_id=5002, last_id=5003, bundles=[bundle],
            manifest={"source_run_id": "r3", "statement_count": 1},
        )
        counts = metrics.build_metrics_from_batches(
            connection=con, object_store=store, source_run_id="r3"
        )
        assert counts["batches_processed"] == 1 and counts["table_statements"] == 2


def test_usd_conversion_fills_amounts(tmp_path):
    store = FakeObjectStore()
    _seed_raw_batch(store)
    db = tmp_path / "skfin.duckdb"
    with duckdb.connect(str(db)) as con:
        metrics.build_metrics_from_batches(connection=con, object_store=store, source_run_id="r1")
        counts = metrics.apply_slovakia_usd_conversion(connection=con, exchange_rates=FakeRates())
    assert counts["rows_converted"] == 1
    with duckdb.connect(str(db), read_only=True) as con:
        usd, fx = con.execute(
            f"select total_assets_amount_usd, fx_rate_to_usd "
            f"from {tables.DLT_DATASET_NAME}.{tables.METRICS_TABLE}"
        ).fetchone()
    assert float(usd) == 968.0 and float(fx) == 1.1  # 880 * 1.1
```

3. The `FakeRuzClient` class stays (Task 3's download tests use it).

- [ ] **Step 2: Run tests to verify the new ones fail**

Run: `uv run pytest tests/test_slovakia_financials.py -k "build_metrics or usd_conversion" -v`
Expected: FAIL with `AttributeError: ... has no attribute 'build_metrics_from_batches'`.

- [ ] **Step 3: Implement the accumulating build in `metrics.py`**

In `src/dagster_v3/defs/slovakia_financials/metrics.py`:

1. Add to the imports: `from dagster_v3.defs.slovakia_financials import raw_store` and delete the now-unused `import time` and `from dagster_v3.defs.slovakia_financials.client import RuzClient` (after step 2 below removes their users).
2. DELETE `process_statement`, `build_slovakia_financials`, and `_write_metrics_table`.
3. ADD (keeping `_STAGE_COLUMNS` and `_stage_row` — they are reused):

```python
PROCESSED_BATCHES_TABLE = f"{DLT_DATASET_NAME}.processed_batches"


def _ensure_metrics_tables(connection: duckdb.DuckDBPyConnection) -> None:
    amount_cols = ", ".join(
        f"{metric}_amount_original decimal(38, 2), {metric}_amount_usd decimal(38, 2)"
        for metric in METRIC_NAMES
    )
    connection.execute(f"create schema if not exists {DLT_DATASET_NAME}")
    connection.execute(
        f"create table if not exists {DLT_DATASET_NAME}.{METRICS_TABLE} ("
        f"country_iso2 varchar, source_slug varchar, source_run_id varchar, "
        f"source_record_id varchar, ico varchar, ruz_entity_id varchar, "
        f"statement_id varchar, template_name varchar, statement_type varchar, "
        f"fiscal_year integer, period_start_date date, period_end_date date, "
        f"filed_date date, approved_date date, currency_original varchar, "
        f"{amount_cols}, mapped_metric_count integer, template_mapped integer, "
        f"mapping_version varchar, fx_rate_to_usd decimal(38, 12), "
        f"fx_rate_date date, fx_converted_at timestamp, source_url varchar, "
        f"resolved_at timestamp, source_batch_key varchar)"
    )
    # Pre-split deployments created the table without the batch column.
    connection.execute(
        f"alter table {DLT_DATASET_NAME}.{METRICS_TABLE} "
        "add column if not exists source_batch_key varchar"
    )
    connection.execute(
        f"create table if not exists {PROCESSED_BATCHES_TABLE} ("
        "batch_key varchar primary key, source_run_id varchar, "
        "statements integer, processed_at timestamp)"
    )


def build_metrics_from_batches(
    *,
    connection: duckdb.DuckDBPyConnection,
    object_store: Any,
    source_run_id: str,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Decode every not-yet-processed S3 raw batch into the metrics table.

    Appends per batch with delete-then-insert on source_batch_key, so
    reprocessing a batch (or wiping processed_batches to force a full
    re-decode) is idempotent.
    """
    _ensure_metrics_tables(connection)
    all_keys = raw_store.list_statement_batch_keys(object_store)
    processed = {
        row[0]
        for row in connection.execute(
            f"select batch_key from {PROCESSED_BATCHES_TABLE}"
        ).fetchall()
    }
    pending = [key for key in all_keys if key not in processed]

    template_cache: dict[int, dict[str, Any]] = {}

    def template_lookup(template_id: int) -> dict[str, Any]:
        template = template_cache.get(template_id)
        if template is None:
            template = raw_store.read_template(object_store, template_id)
            template_cache[template_id] = template
        return template

    statements = 0
    skipped = 0
    for batch_key in pending:
        rows: list[dict[str, Any]] = []
        for bundle in raw_store.read_statement_batch(object_store, batch_key):
            row = decode_statement_bundle(bundle, template_lookup)
            if row is None:
                skipped += 1
                continue
            rows.append(row)
        _append_metrics_batch(connection, rows, source_run_id, batch_key)
        statements += len(rows)

    qualified = f"{DLT_DATASET_NAME}.{METRICS_TABLE}"
    table_statements = int(
        connection.execute(f"select count(*) from {qualified}").fetchone()[0]
    )
    table_mapped = int(
        connection.execute(
            f"select count(*) from {qualified} where template_mapped = 1"
        ).fetchone()[0]
    )
    counts = {
        "batches_processed": len(pending),
        "statements": statements,
        "skipped": skipped,
        "table_statements": table_statements,
        "table_mapped": table_mapped,
    }
    if log is not None:
        log(
            "Built Slovak RÚZ metrics from raw batches: batches=%s rows=%s skipped=%s "
            "table_rows=%s table_mapped=%s",
            len(pending), statements, skipped, table_statements, table_mapped,
        )
    return counts


def _append_metrics_batch(
    connection: duckdb.DuckDBPyConnection,
    rows: list[dict[str, Any]],
    source_run_id: str,
    batch_key: str,
) -> None:
    qualified = f"{DLT_DATASET_NAME}.{METRICS_TABLE}"
    amount_cols = ", ".join(f"{m}_amount_original decimal(38, 2)" for m in METRIC_NAMES)
    placeholders = ", ".join(["?"] * len(_STAGE_COLUMNS))
    metric_cols_select = ", ".join(
        f"{m}_amount_original, cast(null as decimal(38, 2)) as {m}_amount_usd"
        for m in METRIC_NAMES
    )
    connection.execute(
        f"create or replace temp table _sk_stage ("
        f"statement_id varchar, ruz_entity_id varchar, ico varchar, "
        f"template_name varchar, statement_type varchar, fiscal_year integer, "
        f"period_start date, period_end date, filed_date date, approved_date date, "
        f"currency_original varchar, {amount_cols}, "
        f"mapped_metric_count integer, template_mapped integer)"
    )
    if rows:
        connection.executemany(
            f"insert into _sk_stage values ({placeholders})",
            [_stage_row(row) for row in rows],
        )
    connection.execute("begin transaction")
    try:
        connection.execute(f"delete from {qualified} where source_batch_key = ?", [batch_key])
        connection.execute(
            f"""
            insert into {qualified} by name
            select
                'SK' as country_iso2,
                '{tables.SOURCE_SLUG}' as source_slug,
                '{source_run_id}' as source_run_id,
                statement_id as source_record_id,
                ico,
                ruz_entity_id,
                statement_id,
                template_name,
                statement_type,
                fiscal_year,
                period_start as period_start_date,
                period_end as period_end_date,
                filed_date,
                approved_date,
                currency_original,
                {metric_cols_select},
                mapped_metric_count,
                template_mapped,
                '{tables.MAPPING_VERSION}' as mapping_version,
                cast(null as decimal(38, 12)) as fx_rate_to_usd,
                cast(null as date) as fx_rate_date,
                cast(null as timestamp) as fx_converted_at,
                '{tables.RUZ_BASE_URL}' as source_url,
                now() as resolved_at,
                ? as source_batch_key
            from _sk_stage
            """,
            [batch_key],
        )
        connection.execute(
            f"delete from {PROCESSED_BATCHES_TABLE} where batch_key = ?", [batch_key]
        )
        connection.execute(
            f"insert into {PROCESSED_BATCHES_TABLE} values (?, ?, ?, now())",
            [batch_key, source_run_id, len(rows)],
        )
        connection.execute("commit")
    except Exception:
        connection.execute("rollback")
        raise
```

4. In `apply_slovakia_usd_conversion`, add `_ensure_metrics_tables(connection)` as the first line of the function body (before `qualified = ...`), so the USD asset is safe to run on a fresh database.

- [ ] **Step 4: Run the full test file**

Run: `uv run pytest tests/test_slovakia_financials.py -v`
Expected: ALL PASS (note `test_incremental_job_and_schedule` still passes — assets untouched so far; the old asset still imports `metrics` but no longer references the deleted functions only after Task 6. **If the old `assets.py` fails to import because `build_slovakia_financials` is gone, apply the minimal bridge now:** in `assets.py` replace the body of `slovakia_financials_incremental` with `raise NotImplementedError("replaced in Task 6")` is NOT acceptable — instead do Task 6's assets.py rewrite ONLY if imports break. Check first: `assets.py` calls `metrics.build_slovakia_financials` and `metrics.apply_slovakia_usd_conversion` at *runtime*, not import time, so imports keep working — only `uv run dg check defs` and tests must pass, and neither executes the asset body.)

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/slovakia_financials/metrics.py tests/test_slovakia_financials.py
git commit -m "feat(slovakia): decode raw S3 batches into accumulating DuckDB metrics"
```

---

### Task 5: ClickHouse export — atomic replace + empty guard

**Files:**
- Modify: `src/dagster_v3/defs/slovakia_financials/clickhouse.py`
- Test: `tests/test_slovakia_financials.py`

**Interfaces:**
- Consumes: DuckDB metrics table from Task 4; `export_duckdb_connection_table_to_clickhouse` (existing shared helper — `truncate=True` stages + `EXCHANGE TABLES`).
- Produces (used by Task 6): `export_slovakia_financials_clickhouse_metrics(*, duckdb_connection, clickhouse, log=None) -> int` — the `truncate` parameter is REMOVED (always atomic full replace), raises `ValueError` when the DuckDB table is empty.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_slovakia_financials.py`:

```python
def test_clickhouse_export_refuses_empty_table(tmp_path):
    from dagster_v3.defs.slovakia_financials.clickhouse import (
        export_slovakia_financials_clickhouse_metrics,
    )
    import pytest

    db = tmp_path / "skfin.duckdb"
    with duckdb.connect(str(db)) as con:
        metrics._ensure_metrics_tables(con)  # empty table exists
        with pytest.raises(ValueError, match="0 rows"):
            export_slovakia_financials_clickhouse_metrics(
                duckdb_connection=con, clickhouse=None
            )
```

(The guard fires before any ClickHouse call, so `clickhouse=None` never gets touched.)

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_slovakia_financials.py::test_clickhouse_export_refuses_empty_table -v`
Expected: FAIL — `AttributeError: 'NoneType' object has no attribute ...` or `TypeError` (the current code calls `assert_clickhouse_tables_exist` first), NOT the expected `ValueError`.

- [ ] **Step 3: Implement the guard + atomic replace**

Replace the function in `src/dagster_v3/defs/slovakia_financials/clickhouse.py` with:

```python
def export_slovakia_financials_clickhouse_metrics(
    *,
    duckdb_connection: duckdb.DuckDBPyConnection,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Atomically replace corpscout.sk_financial_metrics from the DuckDB table.

    The DuckDB staging table accumulates the full decoded history (see
    metrics.build_metrics_from_batches), so the export is the repo-standard
    full-snapshot replace (stage + EXCHANGE TABLES). Refuses to blank a
    populated table when staging is empty.
    """
    row_count = int(
        duckdb_connection.execute(
            f"select count(*) from {DLT_DATASET_NAME}.{tables.METRICS_TABLE}"
        ).fetchone()[0]
    )
    if row_count == 0:
        raise ValueError(
            f"refusing to replace {tables.QUALIFIED_METRICS_TABLE} with 0 rows "
            "from DuckDB staging"
        )
    assert_clickhouse_tables_exist(
        clickhouse, database=tables.SLOVAKIA_DATABASE, tables=(tables.METRICS_TABLE_CH,)
    )
    if log is not None:
        log(
            "Exporting Slovak RÚZ metrics to ClickHouse: table=%s rows=%s",
            tables.QUALIFIED_METRICS_TABLE,
            row_count,
        )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=DLT_DATASET_NAME,
            duckdb_table=tables.METRICS_TABLE,
            clickhouse_database=tables.SLOVAKIA_DATABASE,
            clickhouse_table=tables.METRICS_TABLE_CH,
            columns=tables.SK_FINANCIAL_METRICS_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log("Finished Slovak RÚZ metrics ClickHouse export: rows=%s", rows)
    return rows
```

(Keep the module's existing imports; the explicit `columns=tables.SK_FINANCIAL_METRICS_COLUMNS` is what keeps `source_batch_key` out of ClickHouse.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_slovakia_financials.py -v`
Expected: ALL PASS (incl. the untouched migration contract test `test_metrics_export_columns_match_migration`).

- [ ] **Step 5: Add the export-exclusion contract test and run it**

Add to `tests/test_slovakia_financials.py`:

```python
def test_source_batch_key_not_exported():
    assert "source_batch_key" not in tables.SK_FINANCIAL_METRICS_COLUMNS
```

Run: `uv run pytest tests/test_slovakia_financials.py::test_source_batch_key_not_exported -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/dagster_v3/defs/slovakia_financials/clickhouse.py tests/test_slovakia_financials.py
git commit -m "feat(slovakia): atomic full-replace ClickHouse export with empty-input guard"
```

---

### Task 6: `assets.py` — four assets, job, schedule

**Files:**
- Rewrite: `src/dagster_v3/defs/slovakia_financials/assets.py`
- Test: `tests/test_slovakia_financials.py` (update `test_incremental_job_and_schedule`)

**Interfaces:**
- Consumes: `download.sweep_statements_to_s3` (Task 3), `metrics.build_metrics_from_batches` / `apply_slovakia_usd_conversion` (Task 4), `export_slovakia_financials_clickhouse_metrics` (Task 5), `incremental.read_cursor`/`write_cursor` (existing), `raw_store.RAW_BUCKET` (Task 1), `ObjectStoreResource` (existing, resource key `object_store` — bound globally, do NOT add it to this module's resources dict).
- Produces: asset keys `slovakia_financials_raw_statements_s3`, `slovakia_financials_metrics_duckdb`, `slovakia_financials_usd_duckdb`, `slovakia_financials_metrics_clickhouse`; job `slovakia_financials_incremental_job`; schedule `slovakia_financials_incremental_schedule` (names unchanged).

- [ ] **Step 1: Update the definitions test to the four-asset shape**

Replace `test_incremental_job_and_schedule` in `tests/test_slovakia_financials.py` with:

```python
def test_incremental_job_and_schedule():
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    sch = repo.get_schedule_def("slovakia_financials_incremental_schedule")
    assert sch.cron_schedule == "0 5 * * *"
    assert sch.job.name == "slovakia_financials_incremental_job"
    keys = {
        k.path[-1]
        for k in repo.get_job("slovakia_financials_incremental_job").asset_layer.executable_asset_keys
    }
    assert keys == {
        "slovakia_financials_raw_statements_s3",
        "slovakia_financials_metrics_duckdb",
        "slovakia_financials_usd_duckdb",
        "slovakia_financials_metrics_clickhouse",
    }
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/test_slovakia_financials.py::test_incremental_job_and_schedule -v`
Expected: FAIL — keys still `{"slovakia_financials_incremental"}`.

- [ ] **Step 3: Rewrite `assets.py`**

Replace the entire content of `src/dagster_v3/defs/slovakia_financials/assets.py` with:

```python
from pathlib import Path

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.duckdb_resources import duckdb_resource
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.slovakia_financials import download, incremental, metrics, raw_store, tables
from dagster_v3.defs.slovakia_financials.clickhouse import (
    export_slovakia_financials_clickhouse_metrics,
)

GROUP_NAME = "slovakia_financials"
SLOVAKIA_FINANCIALS_DUCKDB_POOL = "slovakia_financials_duckdb"
SLOVAKIA_FINANCIALS_DUCKDB_PATH = Path("data/slovakia_financials_source.duckdb")


class SlovakiaFinancialsConfig(dg.Config):
    # Statements downloaded per run (each is ~3-4 RÚZ API calls). The id cursor
    # makes runs resumable; raise this / run repeatedly to sweep history faster.
    max_statements: int = 2000


@dg.asset(
    name="slovakia_financials_raw_statements_s3",
    group_name=GROUP_NAME,
    kinds={"python", "s3"},
    pool=SLOVAKIA_FINANCIALS_DUCKDB_POOL,
    description=(
        "Forward sweep of RÚZ financial statements after the id cursor: fetch "
        "statement + entity + reports RAW and store one NDJSON batch in S3 "
        "(templates deduplicated under a shared prefix), then advance the "
        "cursor. Walks all history in bounded chunks, then picks up new filings."
    ),
)
def slovakia_financials_raw_statements_s3(
    context: AssetExecutionContext,
    config: SlovakiaFinancialsConfig,
    object_store: ObjectStoreResource,
    slovakia_financials_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    object_store.ensure_bucket(raw_store.RAW_BUCKET)
    with slovakia_financials_duckdb.get_connection() as connection:
        after_id = incremental.read_cursor(connection)
    context.log.info("Slovak RÚZ sweep starting after statement id=%s", after_id)
    counts = download.sweep_statements_to_s3(
        object_store=object_store,
        source_run_id=context.run_id,
        after_id=after_id,
        max_statements=config.max_statements,
        log=context.log.info,
    )
    last_id = counts["last_id"]
    if last_id > after_id:
        with slovakia_financials_duckdb.get_connection() as connection:
            incremental.write_cursor(connection, last_id)
    return dg.MaterializeResult(
        metadata={
            **{key: value for key, value in counts.items() if key != "batch_key"},
            "batch_key": counts["batch_key"] or "",
            "s3_bucket": raw_store.RAW_BUCKET,
            "after_id": after_id,
        }
    )


@dg.asset(
    name="slovakia_financials_metrics_duckdb",
    deps=["slovakia_financials_raw_statements_s3"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "s3"},
    pool=SLOVAKIA_FINANCIALS_DUCKDB_POOL,
    metadata={"table": f"{tables.DLT_DATASET_NAME}.{tables.METRICS_TABLE}"},
    description=(
        "Decodes not-yet-processed raw S3 statement batches (statutory tables "
        "→ canonical metrics) and APPENDS them to the accumulating DuckDB "
        "metrics table; per-batch delete+insert keeps reprocessing idempotent."
    ),
)
def slovakia_financials_metrics_duckdb(
    context: AssetExecutionContext,
    object_store: ObjectStoreResource,
    slovakia_financials_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with slovakia_financials_duckdb.get_connection() as connection:
        counts = metrics.build_metrics_from_batches(
            connection=connection,
            object_store=object_store,
            source_run_id=context.run_id,
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    name="slovakia_financials_usd_duckdb",
    deps=["slovakia_financials_metrics_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=SLOVAKIA_FINANCIALS_DUCKDB_POOL,
    description=(
        "Fills *_amount_usd + fx_* columns on the DuckDB metrics table via the "
        "shared ExchangeRateClient (EUR→USD at each statement's period_end). "
        "Set-based and re-runnable in isolation."
    ),
)
def slovakia_financials_usd_duckdb(
    context: AssetExecutionContext,
    slovakia_financials_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    from exchange_rates import ExchangeRateClient

    with slovakia_financials_duckdb.get_connection() as connection:
        counts = metrics.apply_slovakia_usd_conversion(
            connection=connection,
            exchange_rates=ExchangeRateClient.from_env(),
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    name="slovakia_financials_metrics_clickhouse",
    deps=["slovakia_financials_usd_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=SLOVAKIA_FINANCIALS_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_METRICS_TABLE},
    description=(
        "Atomically replaces corpscout.sk_financial_metrics from the full "
        "accumulated DuckDB metrics table (stage + EXCHANGE TABLES)."
    ),
)
def slovakia_financials_metrics_clickhouse(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
    slovakia_financials_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with slovakia_financials_duckdb.get_connection() as connection:
        rows = export_slovakia_financials_clickhouse_metrics(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={"row_count": rows, "clickhouse_table": tables.QUALIFIED_METRICS_TABLE}
    )


# --- Jobs & schedules --------------------------------------------------------
SLOVAKIA_FINANCIALS_ASSETS = [
    slovakia_financials_raw_statements_s3,
    slovakia_financials_metrics_duckdb,
    slovakia_financials_usd_duckdb,
    slovakia_financials_metrics_clickhouse,
]

slovakia_financials_incremental_job = dg.define_asset_job(
    "slovakia_financials_incremental_job",
    selection=dg.AssetSelection.assets(
        "slovakia_financials_raw_statements_s3",
        "slovakia_financials_metrics_duckdb",
        "slovakia_financials_usd_duckdb",
        "slovakia_financials_metrics_clickhouse",
    ),
)
slovakia_financials_incremental_schedule = dg.ScheduleDefinition(
    name="slovakia_financials_incremental_schedule",
    job=slovakia_financials_incremental_job,
    cron_schedule="0 5 * * *",  # daily 05:00
    execution_timezone="Europe/Belgrade",
)

defs = dg.Definitions(
    assets=SLOVAKIA_FINANCIALS_ASSETS,
    jobs=[slovakia_financials_incremental_job],
    schedules=[slovakia_financials_incremental_schedule],
    resources={
        "slovakia_financials_duckdb": duckdb_resource(SLOVAKIA_FINANCIALS_DUCKDB_PATH),
    },
)
```

Notes for the implementer:
- The raw asset keeps the DuckDB pool because it reads/writes the id cursor in the source's DuckDB file.
- `object_store` is consumed by resource-key name — provided globally by another module's `Definitions.merge` binding; adding a second `"object_store"` binding here would conflict.
- The chain is wired with `deps=[...]` (no data passing) — each asset re-derives its inputs from S3/DuckDB, which is exactly what makes them independently re-runnable.

- [ ] **Step 4: Run the full test file and defs check**

Run:
```bash
uv run pytest tests/test_slovakia_financials.py -v
uv run dg check defs
```
Expected: ALL tests PASS; `dg check defs` exits 0.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/slovakia_financials/assets.py tests/test_slovakia_financials.py
git commit -m "feat(slovakia): split incremental pipeline into raw-S3/decode/usd/clickhouse assets"
```

---

### Task 7: Design doc + full validation

**Files:**
- Create: `src/dagster_v3/defs/slovakia_financials/docs/slovakia_financials-design.md`

**Interfaces:** documentation only; content must match what Tasks 1–6 actually built.

- [ ] **Step 1: Write the design doc**

Create `src/dagster_v3/defs/slovakia_financials/docs/slovakia_financials-design.md` (follow `docs/source-design-doc-template.md` headings where they apply):

```markdown
# slovakia_financials — design

## Source
Slovak RÚZ (Register účtovných závierok) open REST API, `https://www.registeruz.sk/cruz-public/api`
(free, no key; browser UA required — F5 WAF rejects bare UAs). No bulk file exists, and the
statement feed pages by ascending id (`pokracovat-za-id`), so this is the §4 "API-only" case —
but instead of date partitions we use a **single id cursor** (DuckDB
`slovakia_financials.ingest_cursor`): one forward sweep walks all history in bounded chunks
(default 2000 statements/run, ~3-4 API calls each) and then naturally picks up new filings,
with zero partition bookkeeping. Deviation from §4 recorded here: id-cursor instead of
MonthlyPartitions because the feed's natural window IS the id sequence.

## Pipeline shape (4 independent assets, daily 05:00 job)
1. `slovakia_financials_raw_statements_s3` — sweep after the cursor; store RAW JSON bundles
   (statement + entity + reports, exactly as returned, including deleted statements and
   non-public stubs) as one NDJSON batch per run in S3 bucket `source-slovakia-financials`
   under `slovakia_financials/raw_statements/batch-{after:012d}-{last:012d}/statements.ndjson`
   (+ `manifest.json`). Templates dedup to `slovakia_financials/templates/template-{id}.json`.
   Advances the cursor only after a successful batch write.
2. `slovakia_financials_metrics_duckdb` — lists S3 batches, anti-joins
   `slovakia_financials.processed_batches`, decodes new ones (statutory tables → canonical
   metrics via `TEMPLATE_METRIC_ROWS`) and appends into the accumulating
   `slovakia_financials.financial_metrics` DuckDB table. Per-batch delete+insert on
   `source_batch_key` → reprocessing is idempotent; wiping `processed_batches` forces a full
   re-decode from S3 without any API traffic.
3. `slovakia_financials_usd_duckdb` — standard §7 separate USD step (shared
   `ExchangeRateClient`, EUR keyed on `period_end_date`), set-based UPDATE.
4. `slovakia_financials_metrics_clickhouse` — atomic full replace of
   `corpscout.sk_financial_metrics` (migration 000043 owns DDL; export tuple
   `SK_FINANCIAL_METRICS_COLUMNS`; `source_batch_key` stays DuckDB-only; refuses empty input).

## Decisions / deviations
- **Raw-to-S3 before decode**: the API is the bottleneck (rate-limited per-statement calls);
  raw batches make decode changes (new template families, bug fixes) replayable without
  re-fetching. This replaces the pre-2026-07 monolith asset that fetched+decoded+converted+
  exported in one step.
- **Cursor in DuckDB, not S3**: continuity with the deployed cursor state; the batch keys in
  S3 additionally encode the id ranges if the cursor ever needs manual reconstruction.
- **ClickHouse export is full-replace** (was append+ReplacingMergeTree dedup): DuckDB now
  holds full decoded history, so the standard atomic snapshot replace applies. The
  ReplacingMergeTree engine from migration 000043 is kept (harmless under EXCHANGE).
- **dlt not used for the load boundary** (pre-existing deviation): the per-statement JSON API
  doesn't fit a dlt resource; HTTP retries come from `dlt.sources.helpers.requests` inside
  `RuzClient`.
- Fetch failures are counted (`fetch_failed`) and skipped permanently (later ids advance the
  cursor) — same semantics as the monolith; acceptable because RÚZ re-lists statements under
  `zmenene-od` when they change.
- Contact data / translation / NACE (§8, §8b, §8c): this module covers financials only; the
  register/contacts source for SK is tracked separately (see
  `docs/slovakia_rpo-financials-research.md`).

## Ops notes
- All four assets share pool `slovakia_financials_duckdb` (limit 1) — the raw asset holds the
  cursor in the same DuckDB file.
- To re-decode everything: `delete from slovakia_financials.processed_batches;` then
  materialize `slovakia_financials_metrics_duckdb` (S3-only, no API traffic).
- To re-sweep from scratch: reset the cursor
  (`update slovakia_financials.ingest_cursor set last_id = 0`) — existing S3 batch objects
  for the same id ranges are overwritten in place (idempotent keys).
```

- [ ] **Step 2: Full validation**

Run:
```bash
uv run pytest tests/test_slovakia_financials.py -v
uv run dg check defs
```
Expected: ALL PASS, exit 0. Also run the repo-wide fast checks if time permits: `uv run pytest tests/ -q -x -k "slovakia or clickhouse_migrations"`.

- [ ] **Step 3: Commit**

```bash
git add src/dagster_v3/defs/slovakia_financials/docs/slovakia_financials-design.md
git commit -m "docs(slovakia): add source design doc for the split raw-S3 pipeline"
```

---

## Self-review checklist (done at plan time)

- **Spec coverage**: download→S3 (Task 1+3), process (Task 2+4), USD (Task 4, kept separate asset in Task 6), ClickHouse (Task 5), each an independent asset (Task 6), worktree from main (created before planning), design doc (Task 7). ✓
- **Type consistency**: `sweep_statements_to_s3` returns `batch_key: str | None` — asset metadata coalesces to `""` (Dagster metadata rejects `None`). `decode_statement_bundle` output keys match `_stage_row` field access exactly (verified against existing `process_statement`). `build_metrics_from_batches` count keys match the asset metadata usage. ✓
- **Placeholder scan**: none. ✓
- **Migration/ops continuity**: DuckDB `alter table add column if not exists source_batch_key` covers the pre-split table; job/schedule names unchanged; ClickHouse migration untouched. ✓
- **Known non-goal**: the first post-cutover ClickHouse export only contains what the (possibly fresh) DuckDB table holds. If the deployed DuckDB file was ever wiped while ClickHouse accumulated history, the full-replace would shrink the CH table — the empty guard catches the zero case; for a partial case, re-decode from S3 (`processed_batches` wipe) first. Called out in the design doc ops notes.
