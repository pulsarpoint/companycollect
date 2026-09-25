# Crawler draft queues

Backoffice **SE → Domains → Add to crawl queue** launches only `website_crawl_input_job`.
Choose full crawl, jobs, or basic site info. Each type has one open draft per `queue_scope`
(default `workspace`). Multiple table selections and explicit URLs append to that draft.
Duplicate domains are retained once; the first queued URL wins until processing finishes.
Adding inputs never checks freshness or starts a crawl. Since ClickHouse migration 448 the
draft follows the shared processing queue contract
([spec](../superpowers/specs/2026-09-24-shared-processing-queue-contract-design.md)), like Webtech.

## Storage

- ClickHouse `corpscout.website_crawl_task_domains`: the only stored copy of each entry
  (`task_id, crawl_type, domain, website_url, source_name, submission_id, created_at`),
  `MergeTree`, `PARTITION BY task_id`, `ORDER BY (task_id, domain)`. Read it without `FINAL`.
- Existing `website_*_requests`: recurring per-domain presets, seeded for missing domains on
  import and never overwritten.
- PostgreSQL `processing.tasks`: lifecycle, the frozen execution (`config.execution`) and the
  counters written at completion. `processing.input_submissions`: idempotent import receipts
  (selection fingerprint and count; `manifest_uri` stays NULL).
- Existing `website_*_results`: outcomes, one row per request and attempt. Draft executions
  write no `website_crawl_submissions` receipts; that table serves the refresh sweep only.
- No object storage: the `website-crawl-queues` bucket receives nothing.

## Import

Materialize `website_crawl_input` with a stable `submission_id` and `crawl_type` plus either:

```yaml
crawl_type: full
submission_id: <uuid>
targets: [example.com, https://shop.example.com/catalog]
```

or `source_relation`, `website_column`, and `ids`/`filters`/`select_all` (with optional
`source_final`, `excluded_ids`, `max_domains` and `se_domain_filters`). Selection, normalization
and deduplication run inside ClickHouse (`selected_domains_sql`): hostname identity, leading
`www` stripped, one URL per hostname, HTTPS preferred. The import is a count for the receipt
and two `INSERT … SELECT` statements from that query: presets for missing domains, then the
task's entries minus domains already in the task. A submission is capped at one million
domains, a draft at one million entries.

A failed import blocks Start. Retry it with the same `submission_id` and selection: the retry
kills its stable ClickHouse query (`crawl-queue-import:<submission_id>`), deletes that
submission's own rows (`DELETE … WHERE task_id AND submission_id`, synchronous lightweight
delete) and reselects from the source as it is now. There is no manifest freezing the first
attempt, so source changes can change a retried submission. Sibling submissions are untouched.
Imports and Start share the task's PostgreSQL advisory lock.

## Start and resume

**Queues → Crawler** selects the current draft of the chosen crawl type; **Configure
processing** launches only the matching `website_*_results` job with `task_id`. Under the task
lock, the results asset:

1. freezes the draft (`status=selected`, `frozen_at`) and saves the execution in
   `processing.tasks.config.execution`: `execution_id` (the original Dagster run id),
   `profile` (model/API/page/limit settings, `force_refresh`, `refresh_interval_days`, CAPTCHA
   settings), `started_at` and `freshness_cutoff = started_at − refresh_interval_days`.
   `max_in_flight`, `batch_size`, timeouts and poll intervals are transport settings and may
   change between resumes; everything in the profile is frozen. The default-draft slot is
   released at once, so new additions form the next draft;
2. loops until nothing remains. *Remaining* is a live query: the task's entries whose
   `request_id` (`dagster-crawl-` + SHA-256 of `<execution_id>:<crawl_type>:<domain>`,
   computed in SQL) has no row in the results table for this execution (`run_id =
   execution_id`), joined with the current preset. For each page of 500 remaining entries the
   asset builds the payload (`effective_payload`) and skips disabled presets and fresh
   entries. An entry is fresh when any result for its `(domain, work_key)` with `finished_at`
   in `[freshness_cutoff, started_at]` was successful — a later failure inside that window
   never hides that success; `force_refresh` disables only that skip. Skips are not stored;
   they are recomputed on every pass;
3. keeps at most `max_in_flight` requests outstanding. Dispatch always **POSTs** the request
   (`POST /v1/crawls`); there is no GET-first check. The crawler treats an identical re-POST of
   a `request_id` it already holds as a no-op that returns the existing job — this is how a
   resume reattaches to a request an earlier run of the same execution already sent. A payload
   that differs from the one the crawler already holds gets HTTP 409: the domain's preset was
   edited after its crawl was requested in this execution, and the run stops with "preset for
   `<domain>` changed after its crawl was requested in this execution; revert the preset or
   crawl it in a new draft". Status polls (`GET /v1/crawls/{request_id}` and its `/result`)
   retry transient crawler errors (429/500/502/503/504 and connection failures) for up to 120
   seconds before failing the poll; a crawler reporting unhealthy workers fails every request
   the same way once that 120-second retry is exhausted — resume the task after the crawler
   recovers. Once a request is terminal and its result is uploaded, the outcome is stored
   through a `ResultBuffer` (200 rows or 5 seconds, acknowledged `async_insert`). An outcome
   leaves the window only after ClickHouse acknowledged it; a store failure while polling keeps
   the rows for the next poll. No outcome for `wait_timeout_seconds` while requests are
   outstanding raises `TimeoutError`;
4. finishes when a whole pass dispatches nothing and nothing is outstanding: counts
   `succeeded`/`failed` by request id from the results table, `skipped = total − succeeded −
   failed`, refuses if any dispatchable entry remains, marks the task `completed`
   (`completed_with_errors` in the run tags when any request failed), then drops the task's
   partition (`ALTER TABLE … DROP PARTITION`) and records `inputs_purged_at`. Both timestamps
   are written from PostgreSQL under a clock-skew guard: neither is ever saved earlier than the
   Dagster-host-stamped `frozen_at`, even when the database clock runs behind.

Re-running the same task with the same profile resumes the saved execution (`execution_id`
may be omitted). A resume recomputes *remaining* from results, so stored outcomes are never
repeated; every remaining request is re-sent (see step 3), and the crawler's no-op/409 handling
decides whether that reattaches to work already in flight or stops the run for a changed
preset. A completed task re-run only retries pending cleanup. A changed profile is rejected;
add the domains to a new draft to crawl them again. Task history stays readable from Dagster
run tags (`crawler/execution_id`, `crawler/outcome`, `crawler/succeeded_pages`,
`crawler/failed_pages`, `crawler/skipped_pages`) after the partition is gone.

## Operational notes

- **A wedged crawl** — stuck at the crawler and never reaching a terminal state — holds its
  `max_in_flight` window slot until it finishes or an operator cancels it directly at the
  crawler. A cancelled crawl is a terminal state and is stored as a failed outcome. Without a
  cancel, a resume just polls the same stuck request again and times out again after
  `wait_timeout_seconds`.
- **A crawler restart** turns its own in-flight jobs into failed outcomes (error "Interrupted
  before completion"). The task completes with errors; queue the affected domains in a new
  draft to retry them.

The older "Send for crawl" workflow (`website_*_requests` assets, `*_workflow` jobs, non-draft
tasks) is retired; a `task_id` that is not a draft-scope task is rejected. The refresh sweep
(`website_*_results` without `task_id`: explicit `domains`, `batch_id`, `batch_size ×
max_batches`, priority order, `website_crawl_submissions` receipts) is unchanged.

## Validation

`tests/test_crawl_draft_queue.py` (disposable PostgreSQL + ClickHouse + the crawler HTTP
fixture): entry-table contract, SQL/Python request-id parity, remaining/fresh/disabled
semantics, force refresh, dedup and receipt replay, a retry replacing only its own rows, the
bounded window with batched inserts, completion with partition purge, a timeout that resumes
by re-sending and reattaching to existing work, a resume refused after its preset changed
mid-execution, a request the crawler forgot being re-sent, and a lost cleanup acknowledgement.
`tests/test_website_crawl_input_assets.py` covers the selection SQL. `tests/test_website_crawl_results.py` covers the refresh sweep.
