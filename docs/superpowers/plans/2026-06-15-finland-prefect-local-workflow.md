# Finland Prefect YTJ Base Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first Prefect raw-ingest task: download the PRH YTJ `/all_companies` JSON to S3 once as `base.json`, and create `base.parquet`.

**Architecture:** Keep the task in `processor/finland_raw_ingest.py`. Use testable helper functions for S3 idempotency, `/all_companies` download, base JSON writing, Parquet writing, and the Prefect flow wrapper.

**Tech Stack:** Python 3.12, Prefect 3, requests, boto3, pytest.

---

## Tasks

- [x] Replace previous broad raw-ingest tests with YTJ base tests.
- [x] Verify tests fail before implementation because `download_ytj_base_to_s3` is missing.
- [x] Implement `/all_companies` download to `source-finland-prhytj/base/base.json`.
- [x] Skip the full download and base rebuild when `base.json` already exists and `refresh=False`.
- [x] Write `base.parquet` from `base.json`.
- [x] Write a manifest beside `base.json`.
- [x] Scope `finland_raw_ingest_flow` to this YTJ base task only.
- [x] Update README and design docs to remove XBRL, date filters, and cron serving from this phase.
- [x] Run focused tests and import checks.

## Verification

Commands run from `companycollect/processor`:

```bash
uv run pytest tests/test_finland_raw_ingest.py -v
uv run pytest tests -v
uv run python - <<'PY'
from finland_raw_ingest import finland_raw_ingest_flow, ytj_base_markdown
print(finland_raw_ingest_flow.name)
print(ytj_base_markdown({'bucket':'b','base_key':'base/base.json','parquet_key':'base/base.parquet','company_count':4,'base_skipped':False,'parquet_skipped':False}).splitlines()[0])
PY
```
