# Shared processing queue contract

Status: agreed with the owner on 2026-09-24. Not implemented yet.

## Problem

Four processors keep their own version of "select entries, freeze them, send them to
a service, store the outcomes":

| Processor | Entries | Freeze / metadata | Cleanup | Extra copies |
| --- | --- | --- | --- | --- |
| Brave search | `company_brave_search_input` | `processing.tasks` selection | none | none |
| Website crawl | `website_crawl_task_domains` | `processing.tasks` selection | none | none |
| IP enrichment | `ip_enrichment_input` | own input module | none | none |
| Webtech | `webtech_scan_input` | draft queue + receipts | lightweight `DELETE` | S3 submission manifest, S3 execution plan per bucket |

Webtech is the most complete (multi-source drafts, receipts, purge), but it also
persists batches: an execution plan with numbered buckets, per-bucket checkpoints,
results published only after a whole scan, and a rule that a bucket must be rebuilt
byte-for-byte on resume. That bookkeeping duplicates what the results table and the
remote service already know. Each entry ends up stored three to four times, and the
purge is a mutation that needed a special table setting (migration 444).

## Decisions

1. **ClickHouse holds the entry lists, PostgreSQL holds coordination.** Entries are
   append-only and dropped as a whole. PostgreSQL keeps small, frequently changing
   rows: task status, submission receipts, locks, frozen execution settings. This
   follows ClickHouse's guidance to avoid mutations and the pattern of keeping queue
   state in PostgreSQL and bulk data in ClickHouse.
2. **No persisted batches.** A batch is only a transport envelope: how many entries
   one request carries. Nothing about it is stored, numbered or checkpointed.
3. **Done means a result exists.** Completion and progress are derived from the
   append-only results table, never from per-entry status updates.
4. **One stored copy of each entry.** The ClickHouse entry row is the only copy until
   its result exists. No submission manifest, no execution plan.

## The contract

### Entry table (one per processor)

- Columns: `task_id`, `input_id`, the processor's payload columns, `source_name`,
  `source_record_id`, `submission_id`, `submitted_at`.
- `input_id` is a stable hash of the normalized payload.
- `ENGINE = MergeTree`, `ORDER BY (task_id, input_id)`, `PARTITION BY task_id`.
  Only a handful of tasks are live at a time, so the partition count stays small.
- Never `UPDATE` or `DELETE` individual rows.

### Lifecycle

1. **Draft.** Submissions from any source append to the open draft for a scope, as
   the webtech draft queue does today. Each submission has a PostgreSQL receipt keyed
   by `submission_id` and a selection fingerprint.
2. **Submission retry.** Kill the submission's stable ClickHouse query, delete only that
   submission's rows (`submission_id` column), and select from the source again with
   the saved filters. No manifest is replayed.
3. **Freeze (Start).** The draft becomes a frozen task; new submissions open the next
   draft. The execution's settings and start time are frozen in the PostgreSQL task
   record and the run's tags.
4. **Execute.** Loop until nothing remains:
   - `remaining` = frozen entries of the task minus results of this execution
     (a ClickHouse anti-join).
   - Freshness skips are decided by a query bounded by the execution's frozen start
     time, so re-evaluating on resume gives the same answer.
   - Send the next remaining entries in envelopes sized for the service.
   - Write results as they arrive, tagged with `task_id`, `execution_id` and
     `input_id`, in acknowledged micro-batches (see Result writes).
5. **Complete.** When `remaining` is empty, mark the task completed.
6. **Clean up.** `ALTER TABLE ... DROP PARTITION` for the task. No mutation.

### Result writes

Never one insert per result. ClickHouse creates a data part per insert, and
`async_insert` with `wait_for_async_insert=1` does not coalesce a single writer that
waits for each flush: on 2026-09-24 the Brave writer produced 592 parts for 592
results in one hour, one row each, cleaned up by 440 merges. That is harmless at
Brave's ~10 results a minute, but per-entry webtech results (hundreds to thousands a
minute) would mean 10-30 new parts a second. ClickHouse delays inserts at 1,000 active
parts in a partition and rejects them at 3,000.

- Buffer results in the writer and insert every few seconds or every few hundred
  rows, whichever comes first. Concurrent workers of one execution share the buffer.
- Wait for the insert's acknowledgement before counting those entries as done.
- A crash loses only the unflushed buffer. Those entries are still `remaining`, and
  their stable request IDs make the service return the stored outcome on resubmit.

Rejected alternatives:

- **Buffer engine in front of the results table.** Its data is in memory and is lost
  on an abnormal restart (ClickHouse docs), which would silently break "done means a
  result exists". ClickHouse itself recommends async inserts instead.
- **Staging MergeTree plus a mover job.** Durable, but stores every result twice, adds
  a job with its own crash window, delays visibility, and still creates one part per
  result in the staging table.
- **Relying on server-side async insert batching alone.** It only combines inserts
  that arrive together from concurrent clients; a writer that waits for each
  acknowledgement flushes one row at a time.

### Recovery

- Dagster retries and re-executions keep the original run ID, which is the execution
  ID. Settings come back from the run tags / task record. Dagster holds run identity,
  not per-entry progress.
- Every request carries a stable identity derived from `execution_id` and `input_id`,
  so a resubmitted in-flight entry is recognised by the service instead of repeated.
- After any crash the resume is the same loop: recompute `remaining`, continue.

## Per-processor changes

- **Webtech.**
  - Remove the S3 submission manifest (`queue-inputs/`) and the execution plan
    (`queue-executions/`), bucket numbering and per-bucket checkpoints in
    `work_config`, and the legacy non-draft path once old tasks are finished.
  - Normalize entries inside ClickHouse (`INSERT ... SELECT`) where possible, instead
    of the ClickHouse → Python → ClickHouse round trip. Keep only what truly needs the
    public-suffix logic in Python.
  - **Scanner change required:** accept a per-page identity and expose results as each
    page finishes, instead of deduplicating a whole scan by file content and publishing
    only from `final-manifest.json`. The scanner already stores per-domain results and
    emits progress every 20 results.
  - Replace migration 444's mutation setting with the partitioned layout.
- **Website crawl.** Already per-entry (request IDs from execution and domain) and
  derives done from results. Gains drafts/receipts, `submission_id`, and partition
  cleanup; `website_crawl_task_domains` moves to the contract layout.
- **Brave search.** Already per-entry (result IDs from execution and input). Gains
  partition cleanup; today finished tasks are never removed (two identical
  736,554-row Sweden tasks were still stored on 2026-09-23).
- **IP enrichment.** `ip_enrichment_input` is `ORDER BY (input_id, task_id)` with no
  partitioning; move to the contract layout and check its execution against the rules
  above.

## Shared code

`defs/common/draft_queue.py` already holds drafts and receipts. Extend `common` with
freeze, execution identity, the `remaining` loop, completion and partition cleanup, so
each processor supplies only:

- how to normalize a source row into an entry (preferably as SQL), and
- how to send an envelope of entries and read back their results.

The backoffice queue page (`app/lib/queues.server.ts`) then reads every processor the
same way, and progress is continuous because results arrive per entry.

## Out of scope

- Per-entry leasing by many competing workers. If a processor ever needs that, it
  belongs in PostgreSQL with `FOR UPDATE SKIP LOCKED` or a broker, not ClickHouse.
- ClickHouse's ingestion "queue" engines (Kafka, S3Queue): they load streams into
  ClickHouse and are not work queues.

## Open questions

1. Scanner API shape for per-page identities and incremental results.
2. Whether any webtech normalization genuinely requires Python.
3. Order of migration: webtech first (least data, most to remove), then crawl, Brave,
   IP enrichment.
