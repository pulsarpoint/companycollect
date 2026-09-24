# Webtech: load a draft, then start processing

This is the current Webtech lifecycle: an open draft accepts submissions into `corpscout.webtech_scan_input` via `webtech_scan_input`, then `webtech_scan_results` freezes membership into an execution and works it in envelopes until nothing remains. ClickHouse migration 446 gives the input table its queue-contract layout: `PARTITION BY task_id`, `ORDER BY (task_id, input_id)`, a `submission_id` column so a retried import replaces only its own rows, and `SETTINGS number_of_free_entries_in_pool_to_execute_mutation = 1` so that retry's lightweight delete is not starved by the shared mutation pool. Other processors keep their existing lifecycle.

## 1. Add to the queue

Materialize `webtech_scan_input` with `webtech_scan_input_job`:

```yaml
ops:
  webtech_scan_input:
    config:
      queue_scope: workspace
      submission_id: "11111111-1111-4111-8111-111111111111"
      targets:
        - https://novelic.com/
        - https://novelic.com/about
      source_name: manual
```

The result contains `task_id`, `submission_id`, this import's distinct `input_count`, and the whole queue's deduplicated `total`. The task stays `draft`. Recent pages remain in the input table. Loading does not scan, freeze membership or perform freshness filtering.

Add another source to the same open draft:

```yaml
ops:
  webtech_scan_input:
    config:
      queue_scope: workspace
      submission_id: "22222222-2222-4222-8222-222222222222"
      source_relation: corpscout.domains
      target_column: root_domain
      filters:
        root_domain: [novelic.com, example.com]
      source_name: domains
```

`force_rescan` has moved from input configuration to results configuration. Update saved Dagster input templates that still include it.

There is one default open draft per `(queue_scope, processor)`. `task_id` can explicitly name that draft. A new addition after freeze resolves to the next draft when task_id is omitted; an explicitly frozen task_id is rejected. `queue_scope` must eventually come from the authenticated workspace in the backoffice; it is operator configuration in Dagster today.

Every deliberate addition has a new submission_id. Retry with the same ID and source config to finish the same import. If omitted, Dagster's initial/root run ID supplies it, preserving automatic retries. Repeated manual launches without an explicit submission_id are new imports. A completed receipt is an idempotent no-op, including after its task freezes. One million distinct pages per task is the current bound.

There is no separate input manifest object. Normalized pages are inserted directly into `corpscout.webtech_scan_input`, one partition per task; PostgreSQL receipts (`processing.input_submissions`) store only the selection fingerprint and count, not the manual URL list or a manifest URI. Shared ClickHouse inputs are deduplicated by normalized page identity within a task: whichever submission's insert lands first keeps that page's source fields, and later submissions selecting the same page are no-ops for it. Import counts can overlap and must not be summed for the queue total.

A retry fences the original per-submission ClickHouse query with `KILL QUERY` on a deterministic `query_id`, deletes that submission's own rows (`DELETE ... WHERE task_id=... AND submission_id=...`, synchronous lightweight delete), and re-runs the source query, inserting whatever it currently yields. Source changes after the first attempt can change retry membership. A failed/unfinished import blocks Start until it is successfully retried. Submission cancellation/reconciliation is not exposed in this backend slice.

**Caveat:** because a shared page is stored under only one submission, retrying a failed submission can lose pages that an overlapping sibling submission also wanted, if the failed submission's rows are deleted and its source no longer yields that page (filters changed, source row removed, etc.). Those pages are restored only if the failed source still yields them on retry. Retry every failed submission before Start, not after.

## 2. Start processing

Materialize **only** `webtech_scan_results` with `webtech_scan_results_job`:

```yaml
ops:
  webtech_scan_results:
    config:
      task_id: "the-task-UUID-returned-by-loading"
      execution_id: "33333333-3333-4333-8333-333333333333"
      force_rescan: false
      recent_days: 30
      batch_size: 5000
```

When this asset begins, it takes the same task lock as imports, verifies the frozen input selection is unchanged (a live re-`inspect()` of the ClickHouse queue must match the snapshot saved at freeze), saves an execution identity/configuration/started-at/freshness-cutoff, and frees the default-draft slot. There is no separate execution-planning step or precomputed plan object: remaining work and completion are both derived live from `webtech_domain_scan_results`, not stored. A future backoffice Start action must show the pending Dagster launch until freeze is acknowledged; merely queuing a Dagster run is not yet a membership freeze.

The asset then loops: pull up to `batch_size` remaining input rows, submit them to the scanner as one envelope, and publish results as they arrive, until no remaining rows are returned. "Remaining" is this execution's frozen inputs minus inputs that already have a result for this execution's `crawl_id` (`webtech-<execution_id>`), minus inputs whose freshest result within `[freshness_cutoff, started_at]` for the current detector version was a success (skipped as fresh; `force_rescan` disables this second filter). Bounding staleness checks to `<= started_at` means results this execution itself writes can never change what counts as fresh on a resume. Envelopes are a transport detail only: `envelope_partition_key` names each one `envelope-<sha256-of-sorted-input-ids>[:24]`, deterministic from its entries, and nothing about the envelope is persisted outside that manifest object — there is no per-envelope or per-bucket bookkeeping in Postgres or ClickHouse.

Because envelope submission is idempotent by content, a resumed run that recomputes a smaller remaining set submits a smaller envelope with a different scan ID. The scanner accepts this as long as it is the *same* execution (same `crawl_id`): it cancels the orphaned scan of the previous envelope, recovers any pages that scan already stored, and proceeds with the new one. A submit for a *different* execution while one is active gets `409 Conflict`.

Results are published in micro-batches, not one insert per result: a `ResultBuffer` flushes every 500 results or 5 seconds (ClickHouse creates one part per insert, so batching avoids part storms). A publish failure while an envelope is still polling keeps its rows buffered for the next attempt; only a failure at end-of-envelope flush fails the run. After each envelope, the run reconciles against the scanner's final manifest (which lists every result, including any whose progress event was missed) and re-checks for any input still without a result, which is idempotent to re-publish or, if still missing, fails the run.

The profile currently contains freshness mode, age, batch size and detector version. Browser concurrency/timeouts remain deployment settings of the existing Webtech scanner; this change does not claim to provide per-execution overrides for them. Webtech has no LLM processing option in this path.

## Progress, retry and rerun

- PostgreSQL `processing.tasks` retains lifecycle, total and published success/failure/skip counters. `processing.input_submissions` tracks additions. `processing.task_progress` exposes counters; pending remote work remains queued until its batch publication is acknowledged (the generic item-lease running counter is not used by this scanner adapter).
- `tasks.config.execution` and Dagster `webtech/execution` / `webtech/execution_id` tags identify the frozen orchestration profile (execution_id, profile, started_at, freshness_cutoff). There is no `work_config.plan_uri` or batch checkpoint state: remaining work, skip-recent decisions and completion are all recomputed from `webtech_domain_scan_results` on every poll and on every resume, so there is nothing separate to go stale or replay from.
- During a remote scan, existing scanner/Dagster logs report live page progress. The PostgreSQL published counters advance only at Finish, from a fresh count over results; intra-run progress is the envelope/results-published log line and the scanner's own `/healthz`.
- Retry the results job with the same task and profile (and the same execution_id if supplied). An omitted execution_id resumes the task's saved execution. Changed settings on the same execution are rejected. Because remaining work is `entries minus this execution's results`, an interrupted run resumes by simply recomputing that difference; already-published results are excluded automatically, so nothing is ever re-submitted to the scanner.
- A completed Start retry retries pending input cleanup without submitting scanner work. Tasks with a published outcome for every input clear their input rows; add the pages to a new draft for another scan. A previous unfinished execution must resume first; website errors are saved outcomes and do not fail the Dagster run. Add failed pages to a new queue to retry them. Skip-recent avoids repeating successful fresh pages, while failed latest observations remain eligible.
- The task becomes `completed` when every input has a published outcome (success or website error) or an intentional skip. Runs with website errors succeed with `webtech/outcome=completed_with_errors`, page counters in run tags/materialization metadata, and a warning in the logs. An incomplete publication, scanner service failure or other pipeline error still fails the run and retains its inputs. After completion, the results asset drops that task's whole ClickHouse input partition (`ALTER TABLE webtech_scan_input DROP PARTITION <task_id>`, cheap because migration 446 partitions the table by `task_id`) rather than a row-by-row delete, then confirms zero rows remain and records `inputs_purged_at`. A failure between the drop and recording the marker is safely retried. Results, submission receipts and task counters remain. Incomplete tasks retain their input rows. The original `terminal_failed_count` remains available after completion; no error results are relabeled as successful.

Legacy tasks with NULL queue_scope still use their original frozen-selection processing path. They are never adopted as open drafts. The Common Crawl Webtech partition workflow remains independent.

## Validation

Disposable PostgreSQL/ClickHouse tests cover draft uniqueness and scope, overlapping sources, recent-page retention, immutable imports after source changes, committed-write retries, import/Start locking, failed-import blocking, execution configuration reuse, frozen freshness decisions, bounded page batches, completion guards, and actual Dagster asset recovery after a simulated lost result-publication acknowledgement. Scanner HTTP/object-storage boundaries are simulated; these tests do not launch real browser scans.

## Deployment receipt — 2026-09-24

Dagster hot sync applied the draft backend and synchronous publication guard, with successful remote definition validation and code-location reload. No service restart or browser scan was triggered. Both `webtech_scan_input_job` and `webtech_scan_results_job` example configurations passed the live Dagster config-validation API. There were no active or queued runs of these two jobs before deployment. Validation: 88 focused tests passed, Ruff passed, and `dg check defs` passed. No new database migration was needed; migration 124 was already applied.

## Worker permission repair — 2026-09-24

PostgreSQL migration 125 grants the existing `processing_worker` role SELECT, INSERT and UPDATE on `processing.input_submissions`. The earlier provisioning grant on ALL TABLES predated migration 124 and therefore did not cover this new table. Version 125 was applied to the deployed database and the privileges and read access were verified through the actual worker connection. No broader default privileges were granted.

## Cleanup worker capacity

ClickHouse migration 444 first set `number_of_free_entries_in_pool_to_execute_mutation=1` on `webtech_scan_input`: the default reservation of 20 free slots stalled even a small queue deletion while the shared pool had 30 of 32 slots occupied by unrelated merges. Migration 446 recreated the table for the task-partitioned queue contract and carries the same setting forward on the new `CREATE TABLE`, since it is scoped to the table, not persisted separately. Completion cleanup no longer needs it (`DROP PARTITION` is metadata-only), but the submission-retry lightweight delete described under "Add to the queue" above still does.
