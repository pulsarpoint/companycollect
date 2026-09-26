# Brave processing

New Backoffice selections use `company_brave_queue_input_job` and the
`company_brave_queue_input` asset in `brave_domain_search`. Companies append to one
open `workspace:SE` draft. `company_brave_search_job` processes that draft through
`company_brave_search_results`. Brave Ask supplies answers; the selected LLM controls
its browser assistant. Only Swedish company inputs are currently supported.

## Import and Start

Backoffice sends a stable `submission_id` with compact selection filters. ClickHouse
selects the rows server-side; large ID lists travel as external tables. Importing
makes no browser/model calls and applies no freshness filtering. Each submission and
draft is limited to one million distinct companies.

```yaml
ops:
  company_brave_queue_input:
    config:
      submission_id: "a-new-UUID-retained-for-retries"
      queue_scope: workspace:SE
      source_relation: corpscout.se_companies_serving
      company_name_column: legal_name
      country_code: SE
      filters:
        company_id: ["5560004615"]
```

The draft deduplicates `(country_code, company_id)` and retains the first queued
company-name snapshot. Input IDs remain `SE:<company_id>`. Each row records its
source, run, submission and timestamp in `corpscout.company_brave_queue_input`.
Rows with empty identity/name are reported and excluded; an entirely invalid source
fails. A replayed completed submission returns its existing receipt. Retrying a
failed import fences its original query and replaces only that submission's rows.

Imports and Start share a PostgreSQL task lock. Start waits for imports and refuses
failed/unconfirmed submissions. Once Start freezes the draft, further additions
resolve to the next draft. The membership fingerprint, input table UUID, settings,
execution ID and freshness window are stored in `processing.tasks.config.execution`.
`processing.input_submissions` retains import receipts. No PostgreSQL schema change
or per-company PostgreSQL work queue is introduced.

## Search settings and freshness

```yaml
ops:
  company_brave_search_results:
    config:
      task_id: "the-draft-task-UUID"
      query_type: official_website
      query_template: "Find the official website of {company_name}."
      force_rescan: false
      recent_days: 30
      llm: # Backoffice supplies the verified profile and encrypted API key
        ...
```

The latest attempt for `(country_code, company_id, query_type)` within the frozen
freshness window ending at execution start determines whether to skip:

| Previous outcome | New draft behavior |
| --- | --- |
| Recent success | Skip |
| Latest attempt failed, absent or older than `recent_days` | Search |
| `force_rescan: true` | Search regardless of earlier executions |
| Any result already saved by this execution | Skip, including saved errors |

A changed query template does not invalidate cached answers; use `force_rescan` to
refresh the same query type. Later executions cannot change a resumed task's frozen
freshness decision. Bounded keyset queries select remaining companies.

Individual search errors are saved and finish as `completed_with_errors`. Model
verification, browser service and storage failures fail the pipeline and leave its
unfinished inputs resumable. History displays the outcome and failed/skipped counts.
**Queue failed companies again** adds the saved failures to an open draft, where the
operator can select processing settings before starting.

## LLM verification and cancellation

Backoffice requires a saved browser-assistant LLM, verifies it and sends an encrypted
API key. The worker verifies the frozen configuration again before consuming input.
The shared `CRAWLER_LLM_ENCRYPTION_KEY` is configured on Backoffice and the browser
service. Brave forwards the encrypted envelope; it does not decrypt it.

Resume keeps the original profile revision, provider, endpoint, model and encrypted
credentials. A new encryption nonce for the same profile does not alter identity.
Changing content settings or the model revision is rejected. Existing LLM dependency
tracking, disable-triggered cancellation and external-request ownership still apply.
Dagster retries retain the same registered LLM request: admission verifies their
parent chain, job and task, while monitoring remains open until pending retries
finish. Unrelated runs cannot reuse that request ID.
Graceful cancellation stops admission and cancels only this run's browser requests.
Cancelled browser requests remain evidence and can receive deterministic retry IDs;
they do not become completed company outcomes.

## Publication, history and cleanup

`corpscout.company_brave_search_results` stores every completed attempt, including
answer text, errors, browser diagnostics, task/execution/run and company identity.
Deterministic result IDs survive retries. Use `FINAL` for exact counts before merges.
The latest-attempt view includes failures; the latest-success projection preserves
an older good answer when a newer search fails.

One writer batches concurrent callbacks (up to 200 results or five seconds). Each
browser worker waits until ClickHouse acknowledges its result before taking another
company. Timed flushes also handle partial batches. A failed write stops processing;
resume checks saved outcomes and reuses stable browser request IDs. There is no
PostgreSQL response outbox.

Completion repairs the successful-answer projection, verifies no work remains and
records counts. Before removing the completed task's input partition, the worker
copies and verifies every company and its provenance in
`corpscout.company_brave_task_sources`. History therefore includes freshness skips.
An interrupted cleanup can be resumed; history and result rows are never purged.

## Resume a draft

Start the same results job with `task_id`. The saved execution is recovered directly
from PostgreSQL; the original Dagster run is not required. An optional `execution_id`
must match that saved execution. Omit content settings to reuse the saved profile;
transport settings such as concurrency, page size and timeouts may change. Dagster
run tags retain task/execution IDs and final outcome counts for Backoffice history.

## Existing fixed selections

Legacy `company_brave_search_input`, `company_brave_search_input_job` and
`company_brave_search_workflow` remain available for old `brave-v2` tasks. The existing
input table is unchanged because old executions pin its table UUID. Backoffice lists
these separately as **Retained legacy inputs** and uses their old processing controls.
No old selection is automatically converted or deleted.

Legacy resumes still use their original `execution_id` and Dagster `brave/execution`
tag. Their `force`/`rescan_old` policy is unchanged: without either flag, any saved
outcome suppresses another search; `rescan_old` uses 30 days. Saved outcomes from the
same execution are always retained. Legacy search errors still fail the run.
`mode: publish` remains available for repairing old execution projections.

## Deployment

Apply ClickHouse migration **453** before deploying these definitions and Backoffice.
It adds the partitioned input and company-membership tables alongside legacy inputs.
Grant `processing_publisher` SELECT on `company_brave_queue_input`; existing result
SELECT/INSERT grants remain necessary. The storage provisioning script includes it.
The worker uses its normal queue-management connection for imports and cleanup.

The previous migration **428** and `scripts/migrate-brave-results.py` cover old
PostgreSQL/S3 history cutover. Those archives are retained for audit; new outcomes
are stored directly in ClickHouse. Do not rerun an old outbox importer as queue setup.
