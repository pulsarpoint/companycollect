# Finland Prefect YTJ Base Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first Prefect raw-ingest task: download the full PRH YTJ companies JSON to S3 once, filter companies registered in a date range, write `base.json`, and create `base.parquet`.

**Architecture:** Keep the task in `processor/finland_raw_ingest.py`. Use testable helper functions for S3 idempotency, full JSON download, registration-date filtering, base JSON writing, Parquet writing, and the Prefect flow wrapper.

**Tech Stack:** Python 3.12, Prefect 3, requests, boto3, pytest.

---

## Tasks

- [x] Replace previous broad raw-ingest tests with YTJ full-to-base tests.
- [x] Verify tests fail before implementation because `download_ytj_full_and_base_to_s3` is missing.
- [x] Implement full JSON download to `source-finland-prhytj/full/date=<today>/companies.json`.
- [x] Skip the full download and base rebuild when `base.json` already exists and `refresh=False`.
- [x] Filter companies with `registrationDate >= start_date` and `registrationDate < today`.
- [x] Write filtered companies to `source-finland-prhytj/base/start_date=<start_date>/end_date=<today>/base.json`.
- [x] Write `base.parquet` from `base.json`.
- [x] Write a manifest beside `base.json`.
- [x] Scope `finland_raw_ingest_flow` to this YTJ base task only.
- [x] Update README and design docs to remove XBRL from this phase.
- [x] Run focused tests and import checks.

## Verification

Commands run from `companycollect/processor`:

```bash
uv run pytest tests/test_finland_raw_ingest.py -v
uv run pytest tests -v
uv run python - <<'PY'
from finland_raw_ingest import finland_raw_ingest_flow, ytj_base_markdown, serve_finland_raw_ingest
print(finland_raw_ingest_flow.name)
print(ytj_base_markdown({'bucket':'b','full_key':'f','base_key':'base','start_date':'2024-01-01','end_date':'2026-06-15','full_count':10,'base_count':2,'full_skipped':False}).splitlines()[0])
print(callable(serve_finland_raw_ingest))
PY
```
