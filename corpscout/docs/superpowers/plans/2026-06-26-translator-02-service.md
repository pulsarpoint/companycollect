# Translator Service — Plan 02: Self-Contained Python Worker

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a single self-contained `translator/` package — absorbing the existing DuckDB queue, LLM provider, and Temporal batch/summarize activities — that, on a `source_slug` trigger, scans ClickHouse for untranslated free-text terms, translates them, and flushes term-level results into `corpscout.text_translations`.

**Architecture:** One new top-level package `translator/` in the dagster_v3 tree. Plan 02 **copies** the reusable modules (`translations/*`, `temporal/translations/queue.py`) into `translator/` with mechanical import rewrites (`translations.` → `translator.`), so `translator/` has no external intra-repo deps. The originals are left untouched so Dagster keeps working; **Plan 03 deletes them** (and the Dagster glue). On top of the copied core, Plan 02 adds a static `registry`, a ClickHouse `scan`, a ClickHouse `flush` (Memory staging so ClickHouse computes `cityHash64` server-side), a new `TranslateSourceWorkflow`, and a `worker` entrypoint.

**Tech Stack:** Python 3.10+, `temporalio`, `duckdb`, `clickhouse-connect`, `openai`, `pytest`. All already declared in `dagster_v3/pyproject.toml` — the worker adds **zero** new dependencies.

## Global Constraints

- Depends on **Plan 01** (`corpscout.text_translations` + `norway_companies_translated` must exist).
- **Self-contained:** after this plan, `translator/` imports only `translator.*`, `temporalio`, `duckdb`, `clickhouse_connect`, `openai`, stdlib. It must NOT import `translations.*` or `temporal.translations.*`.
- The 3 free-text fields + original columns: `articles_purpose`→`articles_purpose_original`, `activity_text`→`activity_text_original`, `company_description`→`company_description_original`. Target language `en`; Norway source language `no`.
- v1 hashing is **raw-text** `cityHash64(<original_col>)`, computed **only** in ClickHouse.
- Task queue name stays `translation-local-llm` (constant `LOCAL_LLM_TRANSLATION_TASK_QUEUE`, carried into `translator/activities.py`).
- ClickHouse env: `CLICKHOUSE_HOST`, `CLICKHOUSE_HTTP_PORT` (default `8123` — `clickhouse-connect` uses HTTP, not the native 9002), `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_DATABASE`, `CLICKHOUSE_SECURE`.
- LLM env: `TRANSLATION_PROVIDER_LOCAL_BASE_URL`, `TRANSLATION_PROVIDER_LOCAL_MODEL`, `TRANSLATION_PROVIDER_LOCAL_API_KEY` (default `not-needed`).
- Heavy imports (`duckdb`, `clickhouse_connect`) stay **inside activity function bodies** (matching the absorbed `activities.py` style) so the workflow sandbox stays clean.
- Run tests with `uv run pytest` from `corpscout/dagster_v3/`.
- Commit by explicit path; never `git add -A`.

---

## File Structure

| File | Origin / Responsibility |
|------|--------------------------|
| `translator/__init__.py` (create) | Package marker |
| `translator/types.py` (copy of `translations/types.py`) | `SmokeTranslationInput`, `SmokeTranslationResult` |
| `translator/queue.py` (copy of `translations/queue.py` + new method) | DuckDB queue; adds `FlushTranslationRow` + `completed_results_for_flush()` |
| `translator/smoke.py` (copy of `translations/smoke.py`) | `LocalOpenAICompatibleTranslationProvider` |
| `translator/provider_smoke.py` (copy of `translations/provider_smoke.py`) | `_categorize_exception`, `_parse_extra_body`, `_load_env_file` |
| `translator/queue_smoke.py` (copy of `translations/queue_smoke.py`) | `_provider_inputs`, `_map_provider_results_to_queue_ids` |
| `translator/activities.py` (copy of `temporal/translations/queue.py`) | `LOCAL_LLM_TRANSLATION_TASK_QUEUE`, `ProcessTranslationBatchInput`, `process_translation_batch`, `summarize_translation_queue` |
| `translator/registry.py` (create) | `FieldConfig`/`SourceConfig` + `REGISTRY` + `get_source_config()` |
| `translator/clickhouse.py` (create) | `clickhouse_client_from_env`, `build_scan_sql`, `scan_untranslated_terms` |
| `translator/flush.py` (create) | `build_flush_select_sql`, `flush_translations` |
| `translator/workflow.py` (create) | `TranslateSourceWorkflow` + `scan_and_seed_activity` + `flush_activity` |
| `translator/worker.py` (create) | `build_worker`, `run_worker`, `worker_main` |
| `translator/Dockerfile` (create) | Worker container |
| `corpscout/dagster_v3/pyproject.toml` (modify) | Add `translator` package + `translator-worker` script |
| `corpscout/docker-compose.yml` (modify) | `translator` service |
| `corpscout/dagster_v3/tests/test_translator_*.py` (create) | Tests per task |

All paths below are under `corpscout/dagster_v3/` unless noted.

---

## Task 1: Absorb the core into a self-contained `translator/` package

Copy the reusable modules verbatim, then rewrite their intra-repo imports mechanically. The originals stay in place (Dagster still uses them until Plan 03).

**Files:**
- Create (copies): `translator/__init__.py`, `translator/types.py`, `translator/queue.py`, `translator/smoke.py`, `translator/provider_smoke.py`, `translator/queue_smoke.py`, `translator/activities.py`
- Modify: `corpscout/dagster_v3/pyproject.toml`
- Test: `tests/test_translator_imports.py`

**Interfaces:**
- Produces (importable from `translator.*`): `TranslationQueue`, `TranslationQueueItem`, `ClaimedTranslationItem` (`translator.queue`); `SmokeTranslationInput`, `SmokeTranslationResult` (`translator.types`); `LocalOpenAICompatibleTranslationProvider` (`translator.smoke`); `LOCAL_LLM_TRANSLATION_TASK_QUEUE`, `ProcessTranslationBatchInput`, `ProcessTranslationBatchResult`, `process_translation_batch`, `summarize_translation_queue`, `process_translation_batch_once` (`translator.activities`).

- [ ] **Step 1: Write the failing import test**

Create `tests/test_translator_imports.py`:

```python
def test_translator_core_is_self_contained_and_importable():
    from translator.activities import (
        LOCAL_LLM_TRANSLATION_TASK_QUEUE,
        ProcessTranslationBatchInput,
        process_translation_batch,
        summarize_translation_queue,
    )
    from translator.queue import TranslationQueue, TranslationQueueItem
    from translator.smoke import LocalOpenAICompatibleTranslationProvider
    from translator.types import SmokeTranslationInput, SmokeTranslationResult

    assert LOCAL_LLM_TRANSLATION_TASK_QUEUE == "translation-local-llm"
    assert ProcessTranslationBatchInput is not None
    assert callable(process_translation_batch)
    assert callable(summarize_translation_queue)
    assert TranslationQueue and TranslationQueueItem
    assert LocalOpenAICompatibleTranslationProvider
    assert SmokeTranslationInput and SmokeTranslationResult


def test_translator_modules_do_not_import_old_packages():
    import pathlib

    pkg = pathlib.Path(__file__).resolve().parents[1] / "translator"
    for py in pkg.glob("*.py"):
        text = py.read_text(encoding="utf-8")
        assert "translations." not in text.replace("translator.", ""), f"{py.name} still references old package"
        assert "temporal.translations" not in text, f"{py.name} still references temporal.translations"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_translator_imports.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'translator'`.

- [ ] **Step 3: Create the package marker and copy the six modules**

Run (from `corpscout/dagster_v3/`):

```bash
mkdir -p translator
: > translator/__init__.py
cp translations/types.py          translator/types.py
cp translations/queue.py          translator/queue.py
cp translations/smoke.py          translator/smoke.py
cp translations/provider_smoke.py translator/provider_smoke.py
cp translations/queue_smoke.py    translator/queue_smoke.py
cp temporal/translations/queue.py translator/activities.py
```

- [ ] **Step 4: Rewrite intra-repo imports mechanically**

Every copied file references the old package only as `translations.` (verified: no copied file references `temporal.translations.`). Rewrite all occurrences to `translator.`:

```bash
sed -i '' 's/\btranslations\./translator./g' \
    translator/types.py \
    translator/queue.py \
    translator/smoke.py \
    translator/provider_smoke.py \
    translator/queue_smoke.py \
    translator/activities.py
```

(On Linux use `sed -i` without the `''` argument.)

This turns, e.g., `from translations.types import SmokeTranslationResult` → `from translator.types import SmokeTranslationResult` in `queue.py`/`activities.py`, and the deferred in-function imports in `activities.py` (`translator.provider_smoke`, `translator.queue`, `translator.queue_smoke`, `translator.smoke`).

- [ ] **Step 5: Register the package in pyproject**

In `corpscout/dagster_v3/pyproject.toml`, add `"translator"` to the wheel packages:

```toml
[tool.hatch.build.targets.wheel]
packages = ["src/dagster_v3", "translations", "temporal", "exchange_rates", "translator"]
force-include = { "pyproject.toml" = "pyproject.toml" }
```

(The old `translations`/`temporal` entries stay until Plan 03.)

- [ ] **Step 6: Run the test to verify it passes**

Run: `uv run pytest tests/test_translator_imports.py -q`
Expected: PASS (2 passed). If the self-containment test fails, inspect the named file — a stray `translations.` slipped through the sed.

- [ ] **Step 7: Commit**

```bash
git add corpscout/dagster_v3/translator/__init__.py \
        corpscout/dagster_v3/translator/types.py \
        corpscout/dagster_v3/translator/queue.py \
        corpscout/dagster_v3/translator/smoke.py \
        corpscout/dagster_v3/translator/provider_smoke.py \
        corpscout/dagster_v3/translator/queue_smoke.py \
        corpscout/dagster_v3/translator/activities.py \
        corpscout/dagster_v3/pyproject.toml \
        corpscout/dagster_v3/tests/test_translator_imports.py
git commit -m "feat(translator): absorb queue/provider/activities into self-contained package"
```

---

## Task 2: Registry

**Files:**
- Create: `translator/registry.py`
- Test: `tests/test_translator_registry.py`

**Interfaces:**
- Produces: `FieldConfig(field, original_col)`; `SourceConfig(source_slug, source_lang, ch_table, fields)`; `REGISTRY: dict[str, SourceConfig]`; `get_source_config(source_slug) -> SourceConfig` (raises `KeyError`).

- [ ] **Step 1: Write the failing test**

Create `tests/test_translator_registry.py`:

```python
import pytest

from translator.registry import FieldConfig, get_source_config


def test_norway_brreg_config_has_three_free_text_fields():
    config = get_source_config("norway_brreg")
    assert config.source_lang == "no"
    assert config.ch_table == "corpscout.companies"
    assert config.fields == (
        FieldConfig(field="articles_purpose", original_col="articles_purpose_original"),
        FieldConfig(field="activity_text", original_col="activity_text_original"),
        FieldConfig(field="company_description", original_col="company_description_original"),
    )


def test_unknown_source_raises_key_error():
    with pytest.raises(KeyError):
        get_source_config("atlantis")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_translator_registry.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'translator.registry'`.

- [ ] **Step 3: Implement the registry**

Create `translator/registry.py`:

```python
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FieldConfig:
    field: str
    original_col: str


@dataclass(frozen=True)
class SourceConfig:
    source_slug: str
    source_lang: str
    ch_table: str
    fields: tuple[FieldConfig, ...]


REGISTRY: dict[str, SourceConfig] = {
    "norway_brreg": SourceConfig(
        source_slug="norway_brreg",
        source_lang="no",
        ch_table="corpscout.companies",
        fields=(
            FieldConfig(field="articles_purpose", original_col="articles_purpose_original"),
            FieldConfig(field="activity_text", original_col="activity_text_original"),
            FieldConfig(field="company_description", original_col="company_description_original"),
        ),
    ),
}


def get_source_config(source_slug: str) -> SourceConfig:
    return REGISTRY[source_slug]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_translator_registry.py -q`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add corpscout/dagster_v3/translator/registry.py \
        corpscout/dagster_v3/tests/test_translator_registry.py
git commit -m "feat(translator): Norway translation registry"
```

---

## Task 3: `completed_results_for_flush()` on the queue

`translator/queue.py`'s `completed_results()` returns `source_text_hash` (sha256) but not raw `source_text`. The flush needs `(field, source_text, translated_text)` so ClickHouse computes the hash. Add a focused method to the **copied** `translator/queue.py`.

**Files:**
- Modify: `translator/queue.py`
- Test: `tests/test_translator_queue_flush_results.py`

**Interfaces:**
- Produces: `FlushTranslationRow(field: str, source_text: str, translated_text: str)` (frozen) and `TranslationQueue.completed_results_for_flush() -> list[FlushTranslationRow]` (only `completed` items; `field` from `translation_locations.source_field`).

- [ ] **Step 1: Write the failing test**

Create `tests/test_translator_queue_flush_results.py`:

```python
from translator.queue import FlushTranslationRow, TranslationQueue, TranslationQueueItem
from translator.types import SmokeTranslationResult


def _item(field: str, text: str) -> TranslationQueueItem:
    return TranslationQueueItem(
        source_duckdb_path="clickhouse",
        source_table="corpscout.companies",
        source_pk="",
        source_field=field,
        source_text=text,
        target_language="en",
    )


def test_completed_results_for_flush_returns_field_text_translation(tmp_path):
    queue = TranslationQueue(tmp_path / "q.duckdb")
    queue.initialize()
    queue.enqueue_items([_item("company_description", "Holdingselskap")])

    claimed = queue.claim_batch(limit=10, worker_id="w1")
    queue.complete_batch(
        claimed,
        [SmokeTranslationResult(item_id=claimed[0].item_id, translated_text="Holding company")],
        provider="prov",
        model="model",
        duration_seconds=0.1,
    )

    assert queue.completed_results_for_flush() == [
        FlushTranslationRow("company_description", "Holdingselskap", "Holding company")
    ]


def test_completed_results_for_flush_excludes_pending(tmp_path):
    queue = TranslationQueue(tmp_path / "q.duckdb")
    queue.initialize()
    queue.enqueue_items([_item("activity_text", "Bygg og anlegg")])
    assert queue.completed_results_for_flush() == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_translator_queue_flush_results.py -q`
Expected: FAIL — `ImportError: cannot import name 'FlushTranslationRow'`.

- [ ] **Step 3: Add the dataclass and method to `translator/queue.py`**

Add the dataclass immediately after the `CompletedTranslationQueueResult` dataclass:

```python
@dataclass(frozen=True)
class FlushTranslationRow:
    field: str
    source_text: str
    translated_text: str
```

Add this method to `TranslationQueue`, immediately after `completed_results()`:

```python
    def completed_results_for_flush(self) -> list[FlushTranslationRow]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                select
                    l.source_field,
                    i.source_text,
                    r.translated_text
                from translation_items i
                join translation_locations l on l.item_id = i.item_id
                join translation_results r on r.item_id = i.item_id
                where i.status = ?
                order by l.source_field, i.source_text
                """,
                [QUEUE_STATUS_COMPLETED],
            ).fetchall()
        return [
            FlushTranslationRow(field=row[0], source_text=row[1], translated_text=row[2])
            for row in rows
        ]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_translator_queue_flush_results.py -q`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add corpscout/dagster_v3/translator/queue.py \
        corpscout/dagster_v3/tests/test_translator_queue_flush_results.py
git commit -m "feat(translator): add completed_results_for_flush() to the queue"
```

---

## Task 4: ClickHouse client + scan

**Files:**
- Create: `translator/clickhouse.py`
- Test: `tests/test_translator_scan.py`

**Interfaces:**
- Produces: `clickhouse_client_from_env()`; `build_scan_sql(source_config, field) -> str`; `scan_untranslated_terms(client, source_config) -> list[tuple[str, str]]` (`(field, source_text)`).

- [ ] **Step 1: Write the failing test**

Create `tests/test_translator_scan.py`:

```python
from translator.clickhouse import build_scan_sql
from translator.registry import get_source_config


def test_build_scan_sql_selects_distinct_untranslated_terms():
    config = get_source_config("norway_brreg")
    sql = build_scan_sql(config, config.fields[2])  # company_description
    assert "SELECT DISTINCT c.company_description_original AS source_text" in sql
    assert "FROM corpscout.companies AS c" in sql
    assert "corpscout.text_translations" in sql
    assert "field = {field:String}" in sql
    assert "source_slug = {slug:String}" in sql
    assert "cityHash64(c.company_description_original)" in sql
    assert "c.company_description_original <> ''" in sql
    assert "t.source_text_hash IS NULL" in sql


def test_build_scan_sql_per_field_uses_its_original_column():
    config = get_source_config("norway_brreg")
    sql = build_scan_sql(config, config.fields[0])  # articles_purpose
    assert "c.articles_purpose_original" in sql
    assert "company_description_original" not in sql
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_translator_scan.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'translator.clickhouse'`.

- [ ] **Step 3: Implement**

Create `translator/clickhouse.py`:

```python
from __future__ import annotations

import os
from typing import Any

from translator.registry import FieldConfig, SourceConfig


def clickhouse_client_from_env() -> Any:
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        database=os.environ["CLICKHOUSE_DATABASE"],
        secure=os.getenv("CLICKHOUSE_SECURE", "false").lower() in {"1", "true", "yes"},
    )


def build_scan_sql(source_config: SourceConfig, field: FieldConfig) -> str:
    original = field.original_col
    return (
        f"SELECT DISTINCT c.{original} AS source_text\n"
        f"FROM {source_config.ch_table} AS c\n"
        f"LEFT JOIN (\n"
        f"    SELECT source_text_hash\n"
        f"    FROM corpscout.text_translations\n"
        f"    WHERE source_slug = {{slug:String}} AND field = {{field:String}}\n"
        f"    GROUP BY source_text_hash\n"
        f") AS t ON t.source_text_hash = cityHash64(c.{original})\n"
        f"WHERE c.{original} <> '' AND t.source_text_hash IS NULL"
    )


def scan_untranslated_terms(client: Any, source_config: SourceConfig) -> list[tuple[str, str]]:
    terms: list[tuple[str, str]] = []
    for field in source_config.fields:
        result = client.query(
            build_scan_sql(source_config, field),
            parameters={"slug": source_config.source_slug, "field": field.field},
        )
        for row in result.result_rows:
            terms.append((field.field, row[0]))
    return terms
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_translator_scan.py -q`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add corpscout/dagster_v3/translator/clickhouse.py \
        corpscout/dagster_v3/tests/test_translator_scan.py
git commit -m "feat(translator): ClickHouse client + untranslated-term scan"
```

---

## Task 5: Flush to `text_translations`

**Files:**
- Create: `translator/flush.py`
- Test: `tests/test_translator_flush.py`

**Interfaces:**
- Produces: `build_flush_select_sql(staging_table) -> str`; `flush_translations(client, source_config, rows, *, provider, model, version, run_id) -> int`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_translator_flush.py`:

```python
from translator.flush import build_flush_select_sql, flush_translations
from translator.queue import FlushTranslationRow
from translator.registry import get_source_config


def test_build_flush_select_sql_computes_hash_in_clickhouse():
    sql = build_flush_select_sql("corpscout.stage_abc")
    assert "INSERT INTO corpscout.text_translations" in sql
    assert "cityHash64(source_text)" in sql
    assert "FROM corpscout.stage_abc" in sql
    assert "{slug:String}" in sql and "{lang:String}" in sql and "{version:UInt64}" in sql


class _FakeClient:
    def __init__(self):
        self.commands: list[str] = []
        self.inserts: list[tuple] = []

    def command(self, sql, parameters=None):
        self.commands.append(sql)

    def insert(self, table, data, column_names=None):
        self.inserts.append((table, data, tuple(column_names or ())))


def test_flush_skips_empty_and_writes_rows():
    client = _FakeClient()
    config = get_source_config("norway_brreg")
    rows = [
        FlushTranslationRow("company_description", "Holdingselskap", "Holding company"),
        FlushTranslationRow("activity_text", "Tomt", ""),  # empty -> skipped
    ]
    written = flush_translations(
        client, config, rows, provider="prov", model="model", version=123, run_id="run-1"
    )
    assert written == 1
    assert any("CREATE TABLE" in c and "ENGINE = Memory" in c for c in client.commands)
    assert client.inserts and client.inserts[0][1] == [["company_description", "Holdingselskap", "Holding company"]]
    assert any("INSERT INTO corpscout.text_translations" in c for c in client.commands)
    assert any(c.startswith("DROP TABLE") for c in client.commands)


def test_flush_no_rows_is_noop():
    client = _FakeClient()
    config = get_source_config("norway_brreg")
    assert flush_translations(client, config, [], provider="p", model="m", version=1, run_id="r") == 0
    assert client.commands == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_translator_flush.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'translator.flush'`.

- [ ] **Step 3: Implement**

Create `translator/flush.py`:

```python
from __future__ import annotations

from typing import Any

from translator.queue import FlushTranslationRow
from translator.registry import SourceConfig


def _staging_table_name(run_id: str) -> str:
    safe = "".join(ch if ch.isalnum() else "_" for ch in run_id)
    return f"corpscout.text_translations_stage_{safe}"


def build_flush_select_sql(staging_table: str) -> str:
    return (
        "INSERT INTO corpscout.text_translations\n"
        "    (source_slug, field, source_text_hash, source_lang, target_lang,\n"
        "     translated_text, provider, model, version)\n"
        "SELECT\n"
        "    {slug:String}, field, cityHash64(source_text), {lang:String}, 'en',\n"
        "    translated_text, {provider:String}, {model:String}, {version:UInt64}\n"
        f"FROM {staging_table}"
    )


def flush_translations(
    client: Any,
    source_config: SourceConfig,
    rows: list[FlushTranslationRow],
    *,
    provider: str,
    model: str,
    version: int,
    run_id: str,
) -> int:
    data = [
        [row.field, row.source_text, row.translated_text]
        for row in rows
        if row.translated_text != ""
    ]
    if not data:
        return 0

    staging = _staging_table_name(run_id)
    client.command(
        f"CREATE TABLE IF NOT EXISTS {staging} "
        "(field String, source_text String, translated_text String) ENGINE = Memory"
    )
    try:
        client.insert(staging, data, column_names=["field", "source_text", "translated_text"])
        client.command(
            build_flush_select_sql(staging),
            parameters={
                "slug": source_config.source_slug,
                "lang": source_config.source_lang,
                "provider": provider,
                "model": model,
                "version": version,
            },
        )
    finally:
        client.command(f"DROP TABLE IF EXISTS {staging}")
    return len(data)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_translator_flush.py -q`
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```bash
git add corpscout/dagster_v3/translator/flush.py \
        corpscout/dagster_v3/tests/test_translator_flush.py
git commit -m "feat(translator): flush results to text_translations via Memory staging"
```

---

## Task 6: `TranslateSourceWorkflow` + scan/flush activities

**Files:**
- Create: `translator/workflow.py`
- Test: `tests/test_translator_workflow.py`

**Interfaces:**
- Consumes: `scan_untranslated_terms`, `clickhouse_client_from_env` (Task 4); `flush_translations` (Task 5); `TranslationQueue`, `TranslationQueueItem`, `completed_results_for_flush` (Tasks 1,3); `ProcessTranslationBatchInput`, `process_translation_batch`, `summarize_translation_queue` (Task 1, `translator.activities`).
- Produces: `TranslateSourceWorkflowInput` (frozen dataclass, fields listed in code); `ScanAndSeedInput(source_slug, queue_duckdb_path)`; `FlushInput(source_slug, queue_duckdb_path, version)`; activities `scan_and_seed_activity` / `flush_activity`; `TranslateSourceWorkflowOutput(enqueued_items, completed_items, failed_retryable_items, flushed_rows)`; `TranslateSourceWorkflow`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_translator_workflow.py`:

```python
import uuid

import pytest
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from translator import activities as acts
from translator import workflow as wf


@pytest.mark.asyncio
async def test_translate_source_workflow_scans_translates_flushes(tmp_path, monkeypatch):
    monkeypatch.setattr(wf, "clickhouse_client_from_env", lambda: object())
    monkeypatch.setattr(
        wf,
        "scan_untranslated_terms",
        lambda client, cfg: [("company_description", "Holdingselskap"), ("activity_text", "Bygg")],
    )

    flushed = {}

    def fake_flush(client, cfg, rows, *, provider, model, version, run_id):
        flushed["rows"] = list(rows)
        return len(rows)

    monkeypatch.setattr(wf, "flush_translations", fake_flush)

    def fake_process_once(params, *, provider=None):
        from translator.queue import TranslationQueue
        from translator.types import SmokeTranslationResult

        q = TranslationQueue(params.duckdb_path)
        q.initialize()
        claimed = q.claim_batch(limit=params.batch_size, worker_id=params.worker_id)
        if not claimed:
            return acts.ProcessTranslationBatchResult(status="empty", item_count=0, duration_seconds=0.0)
        q.complete_batch(
            claimed,
            [SmokeTranslationResult(item_id=c.item_id, translated_text=c.source_text.upper()) for c in claimed],
            provider="fake", model="fake", duration_seconds=0.0,
        )
        return acts.ProcessTranslationBatchResult(status="success", item_count=len(claimed), duration_seconds=0.0)

    monkeypatch.setattr(acts, "process_translation_batch_once", fake_process_once)

    params = wf.TranslateSourceWorkflowInput(
        source_slug="norway_brreg", queue_dir=str(tmp_path), batch_size=10, timeout_seconds=5,
        max_batch_failures=0, worker_id="test-worker", max_tokens=64, extra_body_json="",
        initialize_timeout_seconds=10, batch_timeout_buffer_seconds=5, summarize_timeout_seconds=10,
        activity_maximum_attempts=1, lease_timeout_seconds=60, scan_timeout_seconds=10, flush_timeout_seconds=10,
    )

    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client, task_queue="test-translator",
            workflows=[wf.TranslateSourceWorkflow],
            activities=[wf.scan_and_seed_activity, wf.flush_activity,
                        acts.process_translation_batch, acts.summarize_translation_queue],
        ):
            result = await env.client.execute_workflow(
                wf.TranslateSourceWorkflow.run, params,
                id=f"test-{uuid.uuid4()}", task_queue="test-translator",
            )

    assert result.enqueued_items == 2
    assert result.completed_items == 2
    assert result.flushed_rows == 2
    assert sorted(r.translated_text for r in flushed["rows"]) == ["BYGG", "HOLDINGSELSKAP"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_translator_workflow.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'translator.workflow'`. (If pytest reports the async test as unsupported, confirm `pytest.ini`/`pyproject` has `asyncio_mode = auto` like the existing temporal tests; if a `@pytest.mark.asyncio` plugin is missing, copy the marker/config the existing `temporal` tests already use.)

- [ ] **Step 3: Implement**

Create `translator/workflow.py`:

```python
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.common import RetryPolicy

from translator.activities import (
    ProcessTranslationBatchInput,
    process_translation_batch,
    summarize_translation_queue,
)
from translator.clickhouse import clickhouse_client_from_env, scan_untranslated_terms
from translator.flush import flush_translations
from translator.registry import get_source_config


@dataclass(frozen=True)
class TranslateSourceWorkflowInput:
    source_slug: str
    queue_dir: str
    batch_size: int
    timeout_seconds: int
    max_batch_failures: int
    worker_id: str
    max_tokens: int
    extra_body_json: str
    initialize_timeout_seconds: int
    batch_timeout_buffer_seconds: int
    summarize_timeout_seconds: int
    activity_maximum_attempts: int
    lease_timeout_seconds: int
    scan_timeout_seconds: int
    flush_timeout_seconds: int


@dataclass(frozen=True)
class TranslateSourceWorkflowOutput:
    enqueued_items: int
    completed_items: int
    failed_retryable_items: int
    flushed_rows: int


@dataclass(frozen=True)
class ScanAndSeedInput:
    source_slug: str
    queue_duckdb_path: str


@dataclass(frozen=True)
class FlushInput:
    source_slug: str
    queue_duckdb_path: str
    version: int


def scan_and_seed_once(params: ScanAndSeedInput) -> int:
    from translator.queue import TranslationQueue, TranslationQueueItem

    source_config = get_source_config(params.source_slug)
    client = clickhouse_client_from_env()
    try:
        terms = scan_untranslated_terms(client, source_config)
    finally:
        close = getattr(client, "close", None)
        if callable(close):
            close()

    queue = TranslationQueue(params.queue_duckdb_path)
    queue.initialize()
    items = [
        TranslationQueueItem(
            source_duckdb_path="clickhouse",
            source_table=source_config.ch_table,
            source_pk="",
            source_field=field,
            source_text=source_text,
            target_language="en",
        )
        for field, source_text in terms
    ]
    return queue.enqueue_items(items)


def flush_once(params: FlushInput) -> int:
    import os

    from translator.queue import TranslationQueue

    source_config = get_source_config(params.source_slug)
    rows = TranslationQueue(params.queue_duckdb_path).completed_results_for_flush()
    if not rows:
        return 0
    client = clickhouse_client_from_env()
    try:
        return flush_translations(
            client,
            source_config,
            rows,
            provider="local-llm",
            model=os.environ.get("TRANSLATION_PROVIDER_LOCAL_MODEL", "local-llm"),
            version=params.version,
            run_id=params.queue_duckdb_path,
        )
    finally:
        close = getattr(client, "close", None)
        if callable(close):
            close()


@activity.defn
async def scan_and_seed_activity(params: ScanAndSeedInput) -> int:
    return await asyncio.to_thread(scan_and_seed_once, params)


@activity.defn
async def flush_activity(params: FlushInput) -> int:
    return await asyncio.to_thread(flush_once, params)


def _queue_path(queue_dir: str, source_slug: str) -> str:
    return f"{queue_dir.rstrip('/')}/{source_slug}.duckdb"


@workflow.defn
class TranslateSourceWorkflow:
    @workflow.run
    async def run(self, params: TranslateSourceWorkflowInput) -> TranslateSourceWorkflowOutput:
        queue_path = _queue_path(params.queue_dir, params.source_slug)

        enqueued = await workflow.execute_activity(
            scan_and_seed_activity,
            ScanAndSeedInput(source_slug=params.source_slug, queue_duckdb_path=queue_path),
            start_to_close_timeout=timedelta(seconds=params.scan_timeout_seconds),
            retry_policy=RetryPolicy(maximum_attempts=params.activity_maximum_attempts),
        )

        failure_count = 0
        while True:
            result = await workflow.execute_activity(
                process_translation_batch,
                ProcessTranslationBatchInput(
                    duckdb_path=queue_path,
                    batch_size=params.batch_size,
                    timeout_seconds=params.timeout_seconds,
                    worker_id=params.worker_id,
                    max_tokens=params.max_tokens,
                    extra_body_json=params.extra_body_json,
                    lease_timeout_seconds=params.lease_timeout_seconds,
                ),
                start_to_close_timeout=timedelta(
                    seconds=params.timeout_seconds + params.batch_timeout_buffer_seconds
                ),
                retry_policy=RetryPolicy(maximum_attempts=params.activity_maximum_attempts),
            )
            if result.status == "empty":
                break
            if result.status != "success":
                failure_count += 1
                if params.max_batch_failures > 0 and failure_count > params.max_batch_failures:
                    break

        version = int(workflow.now().timestamp())
        flushed = await workflow.execute_activity(
            flush_activity,
            FlushInput(source_slug=params.source_slug, queue_duckdb_path=queue_path, version=version),
            start_to_close_timeout=timedelta(seconds=params.flush_timeout_seconds),
            retry_policy=RetryPolicy(maximum_attempts=params.activity_maximum_attempts),
        )

        summary = await workflow.execute_activity(
            summarize_translation_queue,
            queue_path,
            start_to_close_timeout=timedelta(seconds=params.summarize_timeout_seconds),
            retry_policy=RetryPolicy(maximum_attempts=params.activity_maximum_attempts),
        )
        return TranslateSourceWorkflowOutput(
            enqueued_items=enqueued,
            completed_items=summary["completed_items"],
            failed_retryable_items=summary["failed_retryable_items"],
            flushed_rows=flushed,
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_translator_workflow.py -q`
Expected: PASS (1 passed).

- [ ] **Step 5: Commit**

```bash
git add corpscout/dagster_v3/translator/workflow.py \
        corpscout/dagster_v3/tests/test_translator_workflow.py
git commit -m "feat(translator): TranslateSourceWorkflow with scan/translate/flush"
```

---

## Task 7: Worker entrypoint

**Files:**
- Create: `translator/worker.py`
- Modify: `corpscout/dagster_v3/pyproject.toml` (add `translator-worker` script)
- Test: `tests/test_translator_worker.py`

**Interfaces:**
- Produces: `build_worker(client) -> Worker`; `run_worker(temporal_address=None)`; `worker_main() -> int`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_translator_worker.py`:

```python
from translator import worker as w
from translator.activities import LOCAL_LLM_TRANSLATION_TASK_QUEUE
from translator.workflow import TranslateSourceWorkflow


def test_build_worker_registers_workflow_and_activities(monkeypatch):
    captured = {}

    class _FakeWorker:
        def __init__(self, client, *, task_queue, workflows, activities):
            captured.update(task_queue=task_queue, workflows=workflows, activities=activities)

    monkeypatch.setattr(w, "Worker", _FakeWorker)
    w.build_worker(object())

    assert captured["task_queue"] == LOCAL_LLM_TRANSLATION_TASK_QUEUE
    assert TranslateSourceWorkflow in captured["workflows"]
    names = {getattr(a, "__name__", "") for a in captured["activities"]}
    assert {"scan_and_seed_activity", "flush_activity",
            "process_translation_batch", "summarize_translation_queue"} <= names
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_translator_worker.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'translator.worker'`.

- [ ] **Step 3: Implement**

Create `translator/worker.py`:

```python
from __future__ import annotations

import asyncio
import os

from temporalio.client import Client
from temporalio.worker import Worker

from translator.activities import (
    LOCAL_LLM_TRANSLATION_TASK_QUEUE,
    process_translation_batch,
    summarize_translation_queue,
)
from translator.workflow import (
    TranslateSourceWorkflow,
    flush_activity,
    scan_and_seed_activity,
)


def build_worker(client: object) -> Worker:
    return Worker(
        client,
        task_queue=LOCAL_LLM_TRANSLATION_TASK_QUEUE,
        workflows=[TranslateSourceWorkflow],
        activities=[
            scan_and_seed_activity,
            flush_activity,
            process_translation_batch,
            summarize_translation_queue,
        ],
    )


async def run_worker(temporal_address: str | None = None) -> None:
    address = temporal_address or os.environ.get("TEMPORAL_ADDRESS", "companycollect:7233")
    client = await Client.connect(address)
    await build_worker(client).run()


def worker_main() -> int:
    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        return 130
    return 0
```

- [ ] **Step 4: Add the console script**

In `corpscout/dagster_v3/pyproject.toml`, under `[project.scripts]` (after the existing `translation-temporal-*` entries), add:

```toml
translator-worker = "translator.worker:worker_main"
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/ -q -k translator`
Expected: PASS (all translator tests green).

- [ ] **Step 6: Commit**

```bash
git add corpscout/dagster_v3/translator/worker.py \
        corpscout/dagster_v3/pyproject.toml \
        corpscout/dagster_v3/tests/test_translator_worker.py
git commit -m "feat(translator): Temporal worker entrypoint"
```

---

## Task 8: Container + compose service

**Files:**
- Create: `translator/Dockerfile`
- Modify: `corpscout/docker-compose.yml`

- [ ] **Step 1: Create the Dockerfile**

Create `corpscout/dagster_v3/translator/Dockerfile`:

```dockerfile
FROM python:3.12-slim

RUN pip install --no-cache-dir uv
WORKDIR /app
COPY corpscout/dagster_v3 /app/dagster_v3
WORKDIR /app/dagster_v3
RUN uv sync --no-dev
CMD ["uv", "run", "translator-worker"]
```

- [ ] **Step 2: Add the compose service**

In `corpscout/docker-compose.yml`, copy the `scheduler` service's `extra_hosts` value and ClickHouse credential wiring verbatim, then add after the `scheduler` block:

```yaml
  translator:
    build:
      context: .
      dockerfile: dagster_v3/translator/Dockerfile
    extra_hosts:
      - "companycollect:100.85.212.113"
    environment:
      TEMPORAL_ADDRESS: companycollect:7233
      CLICKHOUSE_HOST: companycollect
      CLICKHOUSE_HTTP_PORT: "8123"
      CLICKHOUSE_USER: corpscout
      CLICKHOUSE_PASSWORD: ${CLICKHOUSE_PASSWORD}
      CLICKHOUSE_DATABASE: corpscout
      CLICKHOUSE_SECURE: "false"
      TRANSLATION_PROVIDER_LOCAL_BASE_URL: ${TRANSLATION_PROVIDER_LOCAL_BASE_URL}
      TRANSLATION_PROVIDER_LOCAL_MODEL: ${TRANSLATION_PROVIDER_LOCAL_MODEL}
      TRANSLATION_PROVIDER_LOCAL_API_KEY: ${TRANSLATION_PROVIDER_LOCAL_API_KEY:-not-needed}
    volumes:
      - ./data/translator-queues:/app/dagster_v3/data/translator
    restart: unless-stopped
```

- [ ] **Step 3: Validate compose syntax**

Run: `docker compose -f corpscout/docker-compose.yml config >/dev/null && echo OK`
Expected: `OK`.

- [ ] **Step 4: Commit**

```bash
git add corpscout/dagster_v3/translator/Dockerfile corpscout/docker-compose.yml
git commit -m "feat(translator): container image + compose service"
```

---

## Self-Review

**Spec coverage (this plan's slice):** self-contained Python worker reusing the existing queue/provider/activities (Decisions 1, 2, consolidation choice) ✓ Tasks 1,3,6,7; ClickHouse scan (build #1) ✓ Task 4; flush via ClickHouse-side hash (build #2, Decision 10) ✓ Task 5; registry (build #3, Decision 11) ✓ Task 2; task queue `translation-local-llm` ✓ Tasks 6,7; drain-and-complete error isolation ✓ Task 6 loop. Trigger (build #4) + removals + deleting the old three folders = **Plan 03**.

**Placeholder scan:** none — full code or exact `cp`/`sed`/commands per step. The two "verify against existing" callouts (async pytest config in Task 6; `scheduler`'s host/credentials in Task 8) name the exact existing reference to copy.

**Type consistency:** `FlushTranslationRow(field, source_text, translated_text)` defined in Task 3, imported identically in Tasks 5,6. `ProcessTranslationBatchInput`/`ProcessTranslationBatchResult`/`process_translation_batch`/`summarize_translation_queue`/`LOCAL_LLM_TRANSLATION_TASK_QUEUE` all sourced from `translator.activities` (post-rewrite) in Tasks 1,6,7 and the tests. `build_scan_sql`/`scan_untranslated_terms`/`flush_translations`/`build_flush_select_sql` signatures match impl ↔ tests. The workflow test monkeypatches `translator.activities.process_translation_batch_once`, which exists in the absorbed `activities.py`.

**Self-containment guard:** Task 1's `test_translator_modules_do_not_import_old_packages` fails the build if any `translator/*.py` still references `translations.`/`temporal.translations.`, enforcing the consolidation before Plan 03 deletes the originals.
