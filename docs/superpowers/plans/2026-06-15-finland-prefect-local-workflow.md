# Finland Prefect Raw Ingest Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build one Prefect workflow that downloads PRH YTJ JSON/NDJSON and PRH XBRL listing/XML data to S3/RustFS.

**Architecture:** Implement the workflow in `processor/finland_raw_ingest.py` with testable helper functions for S3 idempotency and API download behavior. Use existing `CORPSCOUT_S3_*` env vars and Finland source buckets.

**Tech Stack:** Python 3.12, Prefect 3, requests, boto3, pytest.

---

## Tasks

- [x] Add tests for raw S3 ingest behavior using fake S3 and fake HTTP clients.
- [x] Verify tests fail before implementation because `finland_raw_ingest` does not exist.
- [x] Add runtime dependencies: `boto3` and `requests`.
- [x] Implement PRH YTJ snapshot download to `source-finland-prhytj`.
- [x] Implement PRH XBRL window listing and XML download to `source-finland-prh-xbrl`.
- [x] Implement idempotent S3 object checks and `refresh` behavior.
- [x] Implement `finland_raw_ingest_flow` and `serve_finland_raw_ingest`.
- [x] Add README usage instructions for one-off and cron execution.
- [x] Run focused tests and import checks.

## Verification

Commands run from `companycollect/processor`:

```bash
uv run pytest tests/test_finland_raw_ingest.py -v
uv run pytest tests -v
uv run python - <<'PY'
from finland_raw_ingest import finland_raw_ingest_flow, raw_ingest_markdown, serve_finland_raw_ingest
print(finland_raw_ingest_flow.name)
print(raw_ingest_markdown({'bucket':'b','source_key':'k','company_count':1,'skipped':False}, {'bucket':'x','listing_key':'l','document_count':2,'downloaded_count':1,'skipped_count':1}).splitlines()[0])
print(callable(serve_finland_raw_ingest))
PY
```
