# Temporal Serialized Translation Worker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run the existing DuckDB translation queue through Temporal while serializing all local LLM calls on one task queue.

**Architecture:** One workflow type, `TranslationQueueWorkflow`, schedules batch activities on the shared `translation-local-llm` task queue. Multiple workflows may be started for different sources, but the worker CLI runs with `max_concurrent_activities=1`, so local model calls are queued and executed one at a time.

**Tech Stack:** Python, Temporal Python SDK, DuckDB queue package, OpenAI-compatible local provider, pytest.

---

## Tasks

### Task 1: Temporal Queue Workflow

- [ ] Add tests for workflow input defaults, activity result shape, and queue initialization through activity.
- [ ] Implement `src/dagster_v3/temporal/translations/queue.py`.
- [ ] Verify focused tests pass.

### Task 2: Worker, Start, And Status CLIs

- [ ] Add tests for CLI argument parsing and worker concurrency config helpers.
- [ ] Implement `translation-temporal-worker`, `translation-temporal-start`, and `translation-temporal-status` entry points.
- [ ] Verify focused tests pass.

### Task 3: Real Temporal Smoke

- [ ] Run unit/focused tests.
- [ ] Start one real worker with `max_concurrent_activities=1`.
- [ ] Start two real workflows against separate DuckDB files.
- [ ] Confirm both workflows complete and queue summaries show all items translated.
- [ ] Run full local pytest suite.
