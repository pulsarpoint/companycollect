# Webtech: load a draft, then start processing

This adopts migration 124 for Webtech using the existing two assets. No new database migration is required. Other processors keep their existing lifecycle.

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

Import selections are normalized into immutable, content-addressed JSONL objects in the existing Webtech object-store bucket, under `queue-inputs/webtech/<task>/<submission>/`. PostgreSQL receipts store the URI, selection fingerprint and count, not the manual URL list. The manifest retains every source-record association; shared ClickHouse inputs are deduplicated by normalized page identity and keep the first contribution's source fields. Import counts can overlap and must not be summed for the queue total.

A retry fences the original per-submission ClickHouse query, verifies the saved manifest checksum, and inserts only missing identities. Source changes after manifest finalization cannot change retry membership. A failed/unfinished import blocks Start until it is successfully retried. Submission cancellation/reconciliation is not exposed in this backend slice.

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

When this asset begins, it takes the same task lock as imports, verifies all receipts, freezes membership, saves an execution identity/configuration/cutoff, and frees the default-draft slot. Scanner dispatch happens only after this save and durable execution-plan preparation. A future backoffice Start action must show the pending Dagster launch until freeze is acknowledged; merely queuing a Dagster run is not yet a membership freeze.

Execution preparation records one decision per input page: `scan` or `skip_recent`, with the prior scan reference for skips. Freshness requires the latest known result for that exact root/origin/page and detector to be successful and within the saved window. A newer failed scan does not become fresh because an older successful scan exists. Force rescan keeps every input eligible. Decisions and candidate manifests remain fixed on retry, even when result data changes. Preparation uses bounded batches ordered by input identity, including when one root has many pages.

The profile currently contains freshness mode, age, batch size and detector version. Browser concurrency/timeouts remain deployment settings of the existing Webtech scanner; this change does not claim to provide per-execution overrides for them. Webtech has no LLM processing option in this path.

## Progress, retry and rerun

- PostgreSQL `processing.tasks` retains lifecycle, total and published success/failure/skip counters. `processing.input_submissions` tracks additions. `processing.task_progress` exposes counters; pending remote work remains queued until its batch publication is acknowledged (the generic item-lease running counter is not used by this scanner adapter).
- `tasks.config.execution` and Dagster `webtech/execution` / `webtech/execution_id` tags identify the frozen orchestration profile. `tasks.work_config.plan_uri` points to the durable plan; batch checkpoints in work_config are saved only after synchronous ClickHouse result publication (the publisher explicitly disables asynchronous insert acknowledgement for these batched writes). Detailed page decisions and original source associations are in object storage.
- During a remote scan, existing scanner/Dagster logs report live page progress. The PostgreSQL published counters advance per batch, after ClickHouse acknowledgement.
- Retry the results job with the same task and profile (and the same execution_id if supplied). An omitted execution_id resumes the task's saved execution. Changed settings on the same execution are rejected. Remote manifest identities stay stable, so interrupted work reattaches/reuses durable scanner results. An already published batch is skipped on resume. A lost publication acknowledgement safely replays the same results.
- A completed Start retry retries pending input cleanup without submitting scanner work. Tasks with a published outcome for every input clear their input rows; add the pages to a new draft for another scan. A previous unfinished execution must resume first; website errors are saved outcomes and do not fail the Dagster run. Add failed pages to a new queue to retry them. Skip-recent avoids repeating successful fresh pages, while failed latest observations remain eligible.
- The task becomes `completed` when every input has a published outcome (success or website error) or an intentional skip. Runs with website errors succeed with `webtech/outcome=completed_with_errors`, page counters in run tags/materialization metadata, and a warning in the logs. An incomplete publication, scanner service failure or other pipeline error still fails the run and retains its inputs. After completion, the results asset removes that task’s ClickHouse input rows under the task selection lock using synchronous lightweight deletion. It then records `inputs_purged_at`. A failure between deletion and recording the marker is safely retried. Results, execution manifests, submission receipts and task counters remain. Incomplete tasks retain their input rows. The original `terminal_failed_count` remains available after completion; no error results are relabeled as successful.

Legacy tasks with NULL queue_scope still use their original frozen-selection processing path. They are never adopted as open drafts. The Common Crawl Webtech partition workflow remains independent.

## Validation

Disposable PostgreSQL/ClickHouse tests cover draft uniqueness and scope, overlapping sources, recent-page retention, immutable imports after source changes, committed-write retries, import/Start locking, failed-import blocking, execution configuration reuse, frozen freshness decisions, bounded page batches, completion guards, and actual Dagster asset recovery after a simulated lost result-publication acknowledgement. Scanner HTTP/object-storage boundaries are simulated; these tests do not launch real browser scans.

## Deployment receipt — 2026-09-24

Dagster hot sync applied the draft backend and synchronous publication guard, with successful remote definition validation and code-location reload. No service restart or browser scan was triggered. Both `webtech_scan_input_job` and `webtech_scan_results_job` example configurations passed the live Dagster config-validation API. There were no active or queued runs of these two jobs before deployment. Validation: 88 focused tests passed, Ruff passed, and `dg check defs` passed. No new database migration was needed; migration 124 was already applied.

## Worker permission repair — 2026-09-24

PostgreSQL migration 125 grants the existing `processing_worker` role SELECT, INSERT and UPDATE on `processing.input_submissions`. The earlier provisioning grant on ALL TABLES predated migration 124 and therefore did not cover this new table. Version 125 was applied to the deployed database and the privileges and read access were verified through the actual worker connection. No broader default privileges were granted.

## Cleanup worker capacity

ClickHouse migration 444 sets `number_of_free_entries_in_pool_to_execute_mutation=1` on `webtech_scan_input`. The default reservation of 20 free slots stalled even a 50-row queue deletion while the shared pool had 30 of 32 slots occupied by unrelated merges. The setting is scoped to the input table; synchronous deletion and the post-delete purge marker remain unchanged.
