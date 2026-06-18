# Speed Up Norway Translation Queue Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `norway_brreg_translation_queue` rematerialization fast when the translation queue is already seeded.

**Architecture:** Keep the existing queue schema and Temporal workflow. Add a configurable fast path that skips the expensive full source DuckDB scan and idempotent multi-million-row insert when the queue already has locations, while preserving an explicit config switch to refresh/add missing queue items.

**Tech Stack:** Dagster assets/config, DuckDB, existing `TranslationQueue` package, pytest.

---

### Task 1: Add Fast-Path Queue Seeding

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets.py`
- Test: `corpscout/dagster_v3/tests/test_norway_brreg_assets.py`

- [x] **Step 1: Write a failing test for already-seeded queue rematerialization**

Add a test that creates a queue DuckDB with one location, passes a missing source DuckDB path, and asserts `seed_norway_brreg_translation_queue(..., refresh_existing_queue=False)` does not attach or scan the missing source.

- [x] **Step 2: Implement a direct fast path**

Add `refresh_existing_queue: bool = False` to `seed_norway_brreg_translation_queue`. If the queue exists and has `location_items > 0`, return queue summary metadata without attaching `source_db`.

- [x] **Step 3: Wire Dagster config**

Add `refresh_existing_queue: bool = False` to `NorwayBrregTranslationConfig` and pass it from `norway_brreg_translation_queue` to `seed_norway_brreg_translation_queue`.

- [x] **Step 4: Preserve full refresh behavior**

Keep existing source scan and insert behavior when the queue is empty or `refresh_existing_queue=True`.

- [x] **Step 5: Verify**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_norway_brreg_assets.py tests/test_translation_queue.py -q
uv run dg check defs
```

Expected: all tests pass and definitions load.

- [x] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add docs/superpowers/plans/2026-06-18-speed-up-norway-translation-queue.md corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets.py corpscout/dagster_v3/tests/test_norway_brreg_assets.py
git commit -m "fix: skip reseeding existing norway translation queue"
```

### Task 2: Make Fast Path Read-Only And Timed

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets.py`
- Test: `corpscout/dagster_v3/tests/test_norway_brreg_assets.py`

- [x] **Step 1: Write a regression test proving the skip path does not initialize the queue**

Create a seeded queue DuckDB, monkeypatch `TranslationQueue.initialize` to raise, and assert `seed_norway_brreg_translation_queue(...)` still returns `source_scan_skipped=True`.

- [x] **Step 2: Replace `TranslationQueue.initialize()` in the skip path**

Open the queue DuckDB with `duckdb.connect(str(queue_path), read_only=True)` and use `_translation_queue_summary_from_connection(connection, table_prefix="main")`.

- [x] **Step 3: Add timing logs around queue seed and Temporal workflow start**

Log before/after `seed_norway_brreg_translation_queue(...)` and before/after `start_translation_workflow(...)` in `norway_brreg_translation_queue`.

- [x] **Step 4: Verify**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_norway_brreg_assets.py::test_seed_norway_brreg_translation_queue_skips_seeded_queue_source_scan tests/test_norway_brreg_assets.py::test_seed_norway_brreg_translation_queue_does_not_initialize_seeded_queue tests/test_norway_brreg_assets.py::test_seed_norway_brreg_translation_queue_refreshes_seeded_queue_when_configured -q
uv run pytest tests/test_norway_brreg_assets.py tests/test_translation_queue.py -q
uv run dg check defs
```

Expected: tests pass and definitions load.

- [x] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add docs/superpowers/plans/2026-06-18-speed-up-norway-translation-queue.md corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets.py corpscout/dagster_v3/tests/test_norway_brreg_assets.py
git commit -m "fix: make norway translation queue skip path read only"
```
