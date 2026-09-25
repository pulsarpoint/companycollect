# Assemble inputs, freeze the task, then process

## Scope of this change

Migration 124 extends the existing PostgreSQL `processing.tasks` table and adds
an import receipt table. It does not switch any assets or backoffice actions to
the new lifecycle, alter ClickHouse inputs, launch processing, or delete data.
Migration 124 was applied to the deployed `corpscout` PostgreSQL database on
2026-09-24. The migration ledger is clean at version 124; all four task columns,
three indexes and lifecycle constraints were verified. The 15 existing tasks
(10 ready, 5 selected) retained their statuses and NULL queue metadata. The new
import receipt table was empty after deployment. The schema-only deployment did not adopt application workflows. Webtech backend adoption is now described in [webtech-draft-queue.md](webtech-draft-queue.md).

There is no new queue table, queue ID, execution framework, or per-input frozen
flag. `task_id` identifies the assembled queue. Existing typed ClickHouse inputs,
processor results and execution IDs remain in use.

## Task metadata

The opt-in marker is `processing.tasks.queue_scope`. Existing tasks retain NULL
and their current statuses/behavior. New callers provide the authorized workspace
scope; they must not auto-adopt an old `selected` or `ready` task as a draft.

| Field | Meaning |
| --- | --- |
| `task_id` | Existing identity shared by every input in the task |
| `processor` | Existing processor identity; incompatible processor versions cannot share a draft |
| `queue_scope` | Workspace/owner scope for finding the default open draft |
| `status` | Existing status with `draft` and `completed` added |
| `frozen_at` | One timestamp freezes all inputs belonging to the task |
| `completed_at` | All intended work succeeded or was deliberately skipped, and results were published |
| `inputs_purged_at` | Retained inputs were deleted and deletion verified |
| `source_info`, `total` | Existing frozen input identity and deduplicated total |
| `config`, `work_config` | Existing task configuration; execution-specific settings stay with the execution |

Only one `draft` is allowed per `(queue_scope, processor)`, enforced by a partial
unique index. Starting one task releases that slot for a new draft immediately.

```text
draft --Start/freeze--> selected --activate--> ready --verified completion--> completed
  |                        |                    |
  +----cancel--------------+-----cancel---------+--> cancelled
```

`selected` means frozen and waiting to be activated. `ready` retains its existing
processor meaning; Dagster/processor execution state distinguishes queued,
running, interrupted and failed attempts. A failed execution does not unfreeze
the task. For processors such as Webtech that currently remain `selected`, their
adapter must implement activation/completion before adopting this lifecycle.

## Imports and manual additions

`processing.input_submissions` stores one receipt per import, not one row per
domain. It contains `submission_id`, `task_id`, source name, selection config and
fingerprint, optional immutable manifest URI, status, count, timestamps and error.
The selection config describes source filters/mapping or references a manual
upload; credentials and bulk inputs do not belong in PostgreSQL.

Each new import/manual addition obtains its own `submission_id`. Repeating the
same request with that ID resumes that import; reusing it with another task or
different selection is rejected. A completed receipt is an idempotent no-op.
An intentional new import, even with the same filters, uses a new submission ID.

Imports stream the normalized source selection straight into the processor's
ClickHouse entry table; nothing is stored in object storage. Each row carries
its `submission_id`. Retrying a failed import stops its stable ClickHouse
query, deletes only that submission's rows and selects from the source again
with the saved filters, so a retry reflects the source as it is at retry time.
Overlapping submissions are deduplicated by processor identity; retry failed
imports before Start. Different payloads for the same identity must be reported
as conflicts rather than silently replacing the existing input (for example two
company names).

`input_count` counts distinct contributions of that submission, including inputs
already contributed by another submission. It must not be summed to obtain the
task total. Webtech deduplicates normalized page identities; Brave company keys
include country; crawler and IP inputs retain their own normalization rules.

Submission states are `preparing`, `completed`, `failed`, `cancelled`. Failed or
unfinished submissions block Start. Cancellation requires stopping/fencing the
writer and reconciling partial contributions before declaring it cancelled;
shared inputs contributed by other imports must be preserved.

## Concurrency and Start

The migration enforces metadata shape and default-draft uniqueness. It does not
provide a transaction across PostgreSQL and ClickHouse. The next adapter changes
must implement the following write protocol before enabling the feature:

1. Atomically find/create the scoped draft, handling a unique-index conflict by
   reading the winner. Existing submission IDs always resolve to their original
   task, never to whichever draft is currently open.
2. Imports and Start acquire the same task lock. Recheck status after acquiring
   it. A new addition that lost the race with Start goes to the next draft.
3. Imports keep that lock through their ClickHouse writes and receipt completion;
   do not hold an open PostgreSQL transaction while copying. Serialize imports
   per task initially. Large imports may run in the background while the UI shows
   their receipt, but Start must wait or report the outstanding import.
4. A resumed import first fences any surviving ClickHouse query, then reconciles
   its partial writes under the lock. PostgreSQL lock release alone does not
   prove that a ClickHouse insert has stopped. Reuse the existing query-ID fencing
   approach, with per-submission identities instead of deleting the whole task.
5. Start verifies every receipt and the deduplicated input snapshot. Store the
   snapshot identity/count, freeze time, effective processing configuration and
   execution ID before dispatch. Freeze once; never reopen the task or edit its
   membership/configuration underneath an execution.
6. A dispatch failure leaves a frozen task awaiting processing. Retry the same
   Start/execution request; recover an already-created Dagster run by the saved
   execution identity before launching again. Starting twice must not create two
   executions accidentally. Input additions can meanwhile create the next draft.

## Execution settings, freshness and retries

Reuse the processor's existing execution identity and saved configuration. Brave
and crawler currently save execution details in the originating Dagster run's
tags; their adapters can extend those snapshots. Webtech needs the same explicit
execution distinction when adopting draft tasks. No execution/profile table is
introduced in this migration.

A snapshot contains resolved profile values (not only a mutable template name),
processor/detector version, freshness cutoff, concurrency, timeout/retry limits,
and applicable LLM/page settings. Omit credentials. Resume uses the same snapshot
and durable item checkpoints. A different profile is an explicit new execution,
and is supported only while the original inputs remain available.

Inputs are retained regardless of previous processing. Source pages may filter
and display last attempt, last success and in-progress work; execution preparation
evaluates freshness at the appropriate page/company/IP identity using its saved
cutoff and settings. Skipped inputs remain part of the total and get a recorded
reason/result reference. Freshness skips are evaluated at execution time by a query bounded by the execution's frozen start time and are never stored; the remaining work is always the frozen entries minus this execution's results. This behavior belongs to the adapter, not migration 124.

## Completion and cleanup

`frozen_at` is not a cleanup signal. Mark `completed` only after all inputs have
durable success/intentional-skip outcomes and every required result publication
is verified. A successful Dagster materialization alone is insufficient. Running,
failed, interrupted, cancelled or unpublished tasks are not automatically purged.

Remove completed tasks from the active-queue UI immediately. Retain inputs for a
configured period, then clean them in batches by task ID. Cleanup and starting a
new execution must take the same task lock; cleanup rechecks there are no active
executions. A new execution clears completion eligibility before being launched.
Record `inputs_purged_at` only after deletion is verified, preserving task and
submission metadata, configuration snapshots and results. A cleanup retry checks
what remains and finishes the same deletion. Never reuse a purged task for a new
execution; reconstruct a new draft from retained manifests if needed.

Retention duration and cleanup scheduling are intentionally not enabled in this
schema-only step. No data is automatically deleted by migration 124.

## Adoption order and verification

Webtech import/start/resume implements this contract (see the linked backend guide). Its backoffice queue tab is the next adoption step. Adapt Brave, crawler and IP enrichment separately. Existing
callers stay on their old lifecycle until explicitly migrated.

Schema checks cover concurrent draft uniqueness, scope isolation, freeze metadata,
cleanup ordering, import receipts, legacy data preservation and guarded rollback.
Existing ProcessingStore tests run against the extended schema. End-to-end import
versus Start races, stale-writer fencing, retries, deduplication and publication
checks remain required for each adapter implementation.
