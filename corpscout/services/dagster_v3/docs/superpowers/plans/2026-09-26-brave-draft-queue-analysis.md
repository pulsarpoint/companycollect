# Brave input queue alignment

Status: implementation complete and validated, 2026-09-26. Rollout uses ClickHouse
migration 453 and preserves the legacy table. Operational details are in
[Brave processing](../../company-brave-processing.md).

## Current behavior

Brave already has the important processing pieces: stable request/result IDs,
acknowledged ClickHouse writes, completed-attempt history, a latest-success
projection, browser cancellation, and verified/encrypted browser-assistant LLM
configuration. Reuse these pieces.

The queue lifecycle differs from Webtech and crawler:

| Area | Brave today | Proposed draft behavior |
| --- | --- | --- |
| Add companies | Backoffice generates a new task for every selection | Append to one open draft for the supported country and workspace |
| Retry import | Task-wide selection fingerprint; deletes that task's unconfirmed rows | Submission receipt; replace only that submission's rows |
| Start | Activates a fixed selection | Freeze draft membership and settings under the task lock |
| Resume | Requires the original Dagster run and its `brave/execution` tag | Load execution from `processing.tasks.config.execution`; retain tags for diagnostics |
| Work selection | Reads the complete input and checks outcomes in Python | Bounded remaining-work query using saved outcomes and frozen freshness window |
| Completion | Any saved search error makes the Dagster run fail | Individual search errors produce `completed_with_errors`; pipeline failures retain unfinished inputs |
| Cleanup | Keeps all input rows | Preserve membership history, then drop the completed task partition |
| History | Dagster run list; no retained company-membership projection | Company preview, paginated original selection, counts and result links |

Read-only live check on 2026-09-26: `company_brave_search_input` has 1,474,753 rows
across eight task IDs. Its engine is MergeTree, sorting key `(input_id, task_id)`,
with no partition key. No active `company_brave_search_job` runs were returned at
the time of the check. This does not establish which old tasks are complete.

## Proposed operator flow

1. Select companies and choose **Add to Brave queue**. Importing makes no browser
   or model calls and performs no freshness filtering.
2. Add further selections to the same draft. Each `(country_code, company_id)`
   appears once. Retain the first queued company-name snapshot until the task ends;
   overlapping selections must not silently change a prepared query's inputs.
3. Open **Queues → Brave**. Show the current draft, company names and country/IDs,
   source labels, submission dates, and import progress/errors.
4. Choose the query and browser-assistant LLM, verify it, and start. Freeze the
   company membership and content settings. New additions form the next draft.
5. Resume an interrupted task with its stored execution. Keep all acknowledged
   outcomes and reattach to outstanding browser requests using stable IDs.
6. Complete when every input has an outcome or a valid freshness skip. Preserve
   original membership and results, then remove only that task's input partition.

The current processor accepts SE companies only. Make that explicit in import
validation and draft scope. A country column is not evidence of multi-country
execution support. Preserve composite company identity throughout the UI and SQL.

## Storage and migration

Reuse PostgreSQL `processing.tasks` and `processing.input_submissions`, their unique
open-draft constraint, and session advisory locks. Existing LLM run/dependency
tables also remain in use. No new per-company PostgreSQL work queue is needed.
No PostgreSQL schema change is currently identified as necessary.

The ClickHouse draft-input table needs:

- `task_id`, `input_id`, `country_code`, `company_id`, `company_name`;
- `source_name`, `source_record_id`, `source_run_id`, `submission_id`, `submitted_at`;
- `MergeTree PARTITION BY task_id ORDER BY (task_id, input_id)`;
- nonempty identity/name/source constraints and the synchronous retry-delete setting
  used by the other draft queues.

Retain `country:company_id` as Brave's input identity. Changing it to a hash does
not improve deduplication and would change existing deterministic request IDs.
The country/company pair, not the company name, determines draft membership.

Add a small company-membership history table, for example
`company_brave_task_sources`, recording the task, country/company ID, queued name,
source and submission provenance. Preserve membership, including freshness skips,
before purging inputs; make retries idempotent and verify coverage before dropping
the partition. The existing `queue_task_sources` is explicitly domain/URL-shaped
and constrained to Webtech/crawler; do not store company IDs in its domain column.

Keep `company_brave_search_results` and its latest/latest-success projections.
They already carry task, execution, input, query and result identities. Failed
rescans must continue to preserve older successful answers.

Do not rebuild the populated legacy input table blindly: existing executions pin
its table UUID and selection metadata. The safest initial rollout creates the new
partitioned draft table alongside it (for example `company_brave_queue_input`) and
switches new imports to that table. Keep an explicitly bounded legacy resume path
until the eight old tasks have been audited and handled. Preserve old input IDs,
execution IDs and rows for unfinished work; retire the legacy path/table only after
completion or an explicit migration has been validated. Do not automatically
convert old selections into a new draft, which would change their execution identity.

## Import and execution

Reuse `common/draft_queue.py` for draft resolution, receipts, fingerprint checks,
and finish/failure handling. Retain server-side `INSERT SELECT` for company
selection rather than loading every company into Backoffice or Python. Deduplicate
against the destination under the task lock. Match the other queues' bounded
submission/task limits and expose invalid-row counts; an all-invalid import must
not become a successful empty queue.

Retry fences a stable query ID, synchronously removes only that submission's rows,
and reselects current source data. Start is blocked while a submission is failed or
preparing. Imports and Start take the same lock; if Start wins, new additions resolve
to the next draft.

Reuse `common/queue_execution.py` for lifecycle coordination, with Brave-specific
remaining-work SQL and browser dispatch kept in the Brave package. Freeze query
type/template, LLM profile and encrypted envelope, force/freshness policy, start
time and cutoff in the task. Transport concurrency, page size and logging controls
may change on resume. Preserve the existing rule that a new encryption nonce does
not change the identity of a selected model or browser request.

Compute remaining from frozen input minus this execution's saved results and valid
freshness skips. Read in bounded pages. Use external ClickHouse tables or typed
parameters for ID sets, including `page_outcomes` and selection exclusions; avoid
large literal `IN` clauses. Keep read and write connections separate while streaming.

Preserve selected-LLM preflight in Backoffice and in the worker, encrypted transport,
run dependency tracking, profile-disable cancellation, and ownership of external
browser requests. Recovery must reconcile the selected credentials with the saved
execution rather than verifying a replacement configuration and executing an
unverified older one. No Brave Ask API change is required for the draft lifecycle.

## Freshness and error policy: deliberate behavior changes

Today `force=false,rescan_old=false` skips any previous completed outcome indefinitely,
including errors. `rescan_old=true` applies a fixed 30-day age rule to successes and
errors alike. `page_outcomes` reads the latest outcome without an upper bound at
execution start, so another run's newer outcome can affect resumed selection.

Recommended alignment for new drafts:

- use `force_rescan=false` and configurable `recent_days=30`;
- evaluate the latest attempt for `(country_code, company_id, query_type)` within
  the frozen freshness window ending at execution start;
- skip only if that latest attempt succeeded; a newer failed attempt makes the
  company eligible in a new task even if an older success still exists;
- treat an error saved by this execution as completed for this execution, avoiding
  an endless retry loop; a subsequent task can retry it;
- force bypasses freshness, never the exclusion of this execution's saved results.

Changing the query template currently requires force to bypass cached answers.
Retain and explain this rule in the first version; a versioned query fingerprint
would be a separate change to search identity. Do not change it implicitly during
the queue migration. Legacy resumes retain their original skip policy.

Individual failed searches should finish with visible error counts, not masquerade
as pipeline failures. Model verification/authentication, storage failures, and
systemic service outages must remain visible pipeline failures that leave work
resumable. Review the browser exception boundary when implementing this distinction.

## Result publication

Brave currently writes one acknowledged row per browser callback. Align publication
with the shared queue contract using bounded micro-batches and timed flushes.
The existing `ResultBuffer` is not thread-safe, while Brave callbacks run in route
workers: coordinate publication in one writer or serialize buffer access explicitly.
Advance stored-result counters only after ClickHouse acknowledgement. Preserve the
browser response until that acknowledgement and flush on completion/cancellation;
an abrupt crash must recover by stable request identity. Include a timer/poll path
so a partial buffer flushes even when no new response arrives. Repair the
latest-success projection before marking completion and cleaning up inputs.

## Backoffice

- Change `se-company-brave.server.ts` to submit a stable `submission_id` and scope,
  leaving atomic draft/task resolution to Dagster.
- Add Brave submission-status handling and reuse `QueueImportStatus`; a failed
  import should retry its original receipt rather than create another fixed task.
- Extend current-draft selection, Start/Resume wording and processing controls to
  Brave. Keep LLM selection prominent; move transport settings to advanced controls.
- Show source companies in history with company links and country/ID, including
  skipped companies after cleanup. Reuse the interaction pattern of
  `QueueHistorySources`, with company-specific data/rendering.
- Add Brave outcome/count tags to history parsing. The current history parser uses
  the Webtech prefix for every non-crawler queue; merely adding a Brave badge will
  not expose correct completion counts.
- Distinguish completed search errors from infrastructure failures and provide a
  follow-up selection for retrying failed companies in a new draft.

## Implementation tasks

1. **Database migrations:** partitioned draft-input table, company-membership history,
   grants and schema contracts. Audit old task/execution states and document cutover.
2. **Draft imports:** append/deduplicate selections, receipts, source metadata,
   retry fencing, limits and import-versus-Start locking.
3. **Execution lifecycle:** task-owned frozen settings, bounded remaining queries,
   freshness policy, stable browser requests, completion and safe cleanup. Preserve
   a clearly separated legacy resume route during cutover.
4. **Publication and recovery:** acknowledged micro-batching, callback coordination,
   cancellation/LLM-disable behavior and latest-success repair. Integrate with task 3
   before enabling cleanup in production.
5. **Backoffice parity:** submission receipts, current draft, Start/Resume, history
   companies, accurate counts, and retry-failed selection.
6. **Rollout:** schema first, guarded code/UI cutover, bounded end-to-end test, then
   transition old tasks. No deletion based solely on a Dagster terminal status.

Acceptance tests: overlapping selections; concurrent imports/Start; interrupted
import with sibling submissions; replay with no duplicates; multi-country ID
collision; invalid/all-invalid inputs; 10,000-ID queries; latest failure after an
older success; frozen freshness on resume; forced resume; changed profile rejection;
LLM disable mid-run; interrupted ClickHouse acknowledgement; partial timed buffer;
completed-with-errors; history for freshness-skipped companies; interrupted cleanup;
and legacy execution recovery without new searches for saved outcomes.

## Code references

- `src/dagster_v3/defs/company_domains/input.py`: fixed-selection import.
- `src/dagster_v3/defs/company_domains/assets.py`: run-tag execution and processing.
- `src/dagster_v3/defs/company_domains/results.py`: current freshness and outcome reads.
- `src/dagster_v3/defs/company_domains/browser.py`: request identity, route callbacks,
  LLM verification and cancellation.
- `src/dagster_v3/defs/common/draft_queue.py`, `queue_execution.py`, `result_buffer.py`.
- `../backoffice/app/lib/se-company-brave.server.ts`, `queues.server.ts`,
  `queue-history.server.ts`, and `../backoffice/app/routes/admin-queue.tsx`.
- Existing agreed contract: `docs/superpowers/specs/2026-09-24-shared-processing-queue-contract-design.md`.

## Cutover audit (2026-09-26)

The retained legacy table contains 1,474,753 rows: seven nonempty task IDs plus
eight older rows without a task ID. PostgreSQL has nine `brave-v2` manifests, all
`ready` with no draft scope. This legacy status does not prove completion. No Brave
processing run is active; recent large-task runs were cancelled.

| Legacy task | Retained inputs | Saved result rows (all executions) |
| --- | ---: | ---: |
| `a9b44804-acb0-4085-8d4b-997a4f9d0594` | 736,554 | 73,006 |
| `0602f7d2-6158-48f1-9b31-0550d8383d4e` | 736,554 | 8,542 |
| `247d9459-8ffb-4edb-a863-333360626d30` | 813 | 2,280 |
| `007757fd-e2a2-419b-bc36-3365c7de4cda` | 813 | 140 |
| `f8e663a4-8ea8-41d2-9e15-efc6da4f3a71` | 8 | 0 |
| `f2ec3e8c-9193-4e0c-a426-c0256c325e63` | 2 | 0 |
| `de31e589-316b-4441-9d4b-6664b6340850` | 1 | 0 |

Two other manifests (`2305a766-…`, `03e9aeae-…`) each retain eight saved results
but no task-scoped input rows. Counts span executions and cannot be compared as
completion percentages. No legacy rows, execution IDs or table identities are
changed by this rollout. Backoffice exposes retained task selections separately for
recovery; new imports always use the draft processor.
