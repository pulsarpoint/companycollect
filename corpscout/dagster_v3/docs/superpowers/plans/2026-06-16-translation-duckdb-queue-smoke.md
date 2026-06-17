# Translation DuckDB Queue Smoke Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an isolated DuckDB-backed translation queue and a 2000-item local LLM smoke loop that measures repeatable 50-item batch processing.

**Architecture:** This stays outside Dagster and Temporal. A plain `dagster_v3.translations.queue` package owns DuckDB tables, idempotent enqueue/claim/complete/fail operations, and a command-line smoke runner that inserts synthetic rows and processes them through the existing OpenAI-compatible provider.

**Tech Stack:** Python, DuckDB, OpenAI-compatible provider, pytest.

---

## File Structure

- Create `src/dagster_v3/translations/queue.py` for DuckDB schema, queue dataclasses, claim/complete/fail API, and summary metrics.
- Create `src/dagster_v3/translations/queue_smoke.py` for synthetic item generation, worker loop, CLI, and provider wiring.
- Modify `pyproject.toml` to expose `translation-queue-smoke`.
- Create `tests/test_translation_queue.py` for queue schema/API behavior.
- Create `tests/test_translation_queue_smoke.py` for loop behavior with fake providers.

## Tasks

### Task 1: Queue API

- [ ] Write failing tests for schema initialization, 2000-item enqueue, 50-item claim, successful completion, retryable failure, and status counts.
- [ ] Implement `TranslationQueue`, `TranslationQueueItem`, `ClaimedTranslationItem`, and `TranslationQueueSummary`.
- [ ] Verify focused queue tests pass.

### Task 2: Smoke Worker Loop

- [ ] Write failing tests for a fake provider processing 120 synthetic entries in 50-item batches and retrying a failed batch.
- [ ] Implement `run_translation_queue_smoke`, synthetic item generation, batch metrics, and CLI.
- [ ] Verify focused smoke loop tests pass.

### Task 3: Real Provider Run And Verification

- [ ] Expose `translation-queue-smoke` script in `pyproject.toml`.
- [ ] Run focused tests for translation queue and provider smoke.
- [ ] Run full local pytest suite.
- [ ] Run a real 2000-item smoke loop against the configured local LLM with batch size 50 and report utilization metrics.
