# Brave task progress logs

Service-owned batch runs (`brave/service_batches=1`) poll the browser service every
two seconds and report progress directly from the materialization. The sensor skips
these runs to avoid interpreting delayed ClickHouse publication as zero throughput.
Logs show processed/total for the current batch, successes/failures, running/pending,
published/total, full-task progress and entries/minute. During recovery the full-task
count says `reconciling` until ClickHouse is verified, avoiding double-counting a
partially published batch. Speeds count newly observed processing, not prior saved
work. The defaults remain a report every 30 seconds or 100 outcomes, plus completion.
Actual increases in local saved outcomes emit a separate activity event; unchanged
progress heartbeats do not reset the inactivity clock.

The following sensor behavior remains in place for older per-request runs:


`brave_progress_sensor` appends `Brave progress` events to each running Brave draft
run, associated with the `company_brave_search_results` step. It also works for
workers started before this code was deployed. It reads PostgreSQL task metadata
and ClickHouse outcomes; it never starts, stops, retries, or updates a task.

Each line contains:

- Completed / total and percentage for the full frozen task selection, not the
  100-row input read window. Completed includes processed and reused entries.
- Processed, successful, failed, reused recent successes, and remaining entries.
- Entries per minute since the previous report, including failures. Zero means
  no new durable results in that interval. The first sample has no recent speed.
- Average entries per minute for this Dagster run and its number of new results.
  Saved results from previous runs contribute to task completion, but never to
  this run's speed. Reused entries do not inflate speed either.
- Current Dagster run state and task ID.

The sensor checks at a minimum interval of 30 seconds. It reports when either
`progress_log_interval_seconds` or `progress_log_every` is reached, subject to
daemon scheduling. The defaults are 30 seconds and 100 entries. A report still
appears when no entries finish, so long browser requests cannot hide a stall.

The sensor cursor retains only active runs and their last reported measurements.
A previously observed run gets one final snapshot when it finishes, fails, or is
canceled, then leaves the cursor. Runs finishing entirely between sensor ticks
retain the asset's existing completion metadata. Completed input partitions may
be purged; their saved task totals and result history remain sufficient for logs.

Outcomes are deduplicated by input ID within the task execution. The initial
observation counts pending inputs to establish the frozen freshness skips;
subsequent observations only aggregate results for that execution. Queries have
a 15-second timeout and are read-only. A monitoring query failure affects the
sensor tick, not the search worker. No schema migration or worker restart is needed.
Observer progress events do not reset the existing worker-inactivity alert clock.
