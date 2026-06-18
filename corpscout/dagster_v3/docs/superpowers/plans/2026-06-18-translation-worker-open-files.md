# Translation Worker Open Files Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop the Temporal translation worker from leaking local LLM HTTP sockets until DuckDB fails with `Too many open files`.

**Architecture:** The Temporal batch activity creates a local OpenAI-compatible provider when no test provider is injected. That provider owns an OpenAI HTTP client and must close it after the provider request finishes, before reopening DuckDB to complete or fail the batch. Injected providers remain caller-owned and are not closed by the activity.

**Tech Stack:** Python, Temporal activity functions, OpenAI Python client, DuckDB, pytest.

---

### Task 1: Close Owned Translation Provider Clients

**Files:**
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/translations/smoke.py`
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/temporal/translations/queue.py`
- Test: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_translation_provider_smoke.py`
- Test: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_translation_temporal_queue.py`

- [x] **Step 1: Add provider close tests**

Add tests that prove `LocalOpenAICompatibleTranslationProvider.close()` closes an owned client, and does not close an injected client.

- [x] **Step 2: Run provider close tests and verify failure**

Run:

```bash
uv run pytest tests/test_translation_provider_smoke.py::test_local_provider_closes_owned_client tests/test_translation_provider_smoke.py::test_local_provider_does_not_close_injected_client -q
```

Expected: fail because `close()` does not exist yet.

- [x] **Step 3: Add Temporal activity lifecycle tests**

Add tests that prove `process_translation_batch_once()` closes an internally-created provider on both success and provider failure, before updating DuckDB.

- [x] **Step 4: Run Temporal lifecycle tests and verify failure**

Run:

```bash
uv run pytest tests/test_translation_temporal_queue.py::test_process_translation_batch_once_closes_owned_provider_on_success tests/test_translation_temporal_queue.py::test_process_translation_batch_once_closes_owned_provider_on_failure -q
```

Expected: fail because the activity does not close the provider.

- [x] **Step 5: Implement minimal close behavior**

In `LocalOpenAICompatibleTranslationProvider`, track whether the provider created the OpenAI client internally. Add a `close()` method that closes only internally-owned clients.

In `process_translation_batch_once()`, close the internally-created provider immediately after the provider call completes or fails, then write batch success/failure state to DuckDB.

- [x] **Step 6: Run focused translation tests**

Run:

```bash
uv run pytest tests/test_translation_provider_smoke.py tests/test_translation_temporal_queue.py -q
```

Expected: all selected tests pass.

- [x] **Step 7: Verify current worker descriptor root cause**

Run:

```bash
lsof -nP -p $(pgrep -f translation-temporal-worker | tail -1) | wc -l
```

Expected before restarting worker: high descriptor count remains because the current process already leaked sockets. Restarting the worker is required to drop existing sockets.

- [x] **Step 8: Commit**

Run:

```bash
git add translations/smoke.py temporal/translations/queue.py tests/test_translation_provider_smoke.py tests/test_translation_temporal_queue.py docs/superpowers/plans/2026-06-18-translation-worker-open-files.md
git commit -m "fix: close translation provider clients"
```
