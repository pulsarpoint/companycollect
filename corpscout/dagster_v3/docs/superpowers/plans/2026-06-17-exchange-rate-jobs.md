# Exchange Rate Jobs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add separate exchange-rate jobs for manual backfills and scheduled latest-day refreshes.

**Architecture:** Keep one partitioned asset graph. Define one shared asset selection for `exchange_rates_clickhouse.upstream()`, then create an unscheduled backfill job and a scheduled daily job from that same selection.

**Tech Stack:** Dagster assets, partitioned asset jobs, partitioned schedules, pytest.

---

## Files

- Modify `src/dagster_v3/defs/exchange_rates/assets.py`
  - Add `exchange_rates_selection`.
  - Add `exchange_rates_backfill_job`.
  - Update `exchange_rates_daily_job` to use the shared upstream selection.
  - Build `exchange_rates_daily_schedule` from the partitioned job with weekday/timezone settings.
  - Register both jobs and the daily schedule in `defs`.
- Modify `tests/test_exchange_rates_assets.py`
  - Assert both jobs are registered.
  - Assert the daily schedule is registered and points to `exchange_rates_daily_job`.

## Tasks

- [x] Add a failing registration test for the backfill job and scheduled daily job.
- [x] Update exchange-rate asset definitions with shared selection and two jobs.
- [x] Run the new registration test.
- [x] Run focused exchange-rate tests.
- [x] Run `dg check defs`.
- [ ] Commit only this job/schedule change, preserving unrelated user edits.

## Verification Commands

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_exchange_rates_assets.py::test_exchange_rates_assets_and_jobs_are_registered -q
uv run pytest tests/test_exchange_rates_assets.py tests/test_clickhouse_migrations.py -q
uv run dg check defs
```
