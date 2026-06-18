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
