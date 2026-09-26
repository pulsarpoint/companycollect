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
      input_batch_size: 500 # configurable, up to 500
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

## Browser reuse

Each route worker keeps one browser open across sequential requests. The browser
service counts actual searches and recycles the process after ten by default;
cached result replays do not consume this budget. Set the
`company_brave_browser` resource's `max_requests_per_browser` to 20 to use twenty.
The browser service owns the default; an omitted resource setting leaves it intact.
The saved profile and route remain the same across process restarts. Broken browsers
and cancellation close early, and workers release their browser on exit using the
current execution ID. Service idle expiry is the cleanup fallback.

The service's request result JSON and live status expose `browser_usage` with the
generation, request count and restart threshold. These values also appear in browser
service logs; the request counter resets with a new browser generation.

## Publication, history and cleanup

`corpscout.company_brave_search_results` stores every completed attempt, including
answer text, errors, browser diagnostics, task/execution/run and company identity.
Deterministic result IDs survive retries. Use `FINAL` for exact counts before merges.
The latest-attempt view includes failures; the latest-success projection preserves
an older good answer when a newer search fails.

Dagster submits up to 500 remaining companies to `POST /v1/brave/batches`. The
browser service commits them to `brave-queue.sqlite3` before acknowledging them.
Four route workers (one per route by default) save each outcome in SQLite and
immediately take another item. They retain the existing atomic request/result JSON
and deterministic request IDs, including CAPTCHA observations.

After every item has an outcome, the service bulk-publishes the batch directly to
ClickHouse, repairs the successful-answer projection, and verifies every result ID.
Publication retries reuse saved outcomes. Dagster polls the batch heartbeat every
two seconds, logs processed/total, running, pending, successful, failed, published
and entries/minute, then independently verifies the result IDs in its ClickHouse
connection before sending the next batch. There is no overlap/refill between batches.
Results become visible in company/history views when the batch is published; live
progress is available in Dagster while it runs.

The service checks PostgreSQL LLM admission before each request and every two
seconds while a batch is active. It registers individual requests in the existing
external-request ledger, so CAPTCHA collection and cancellation keep their IDs.
A 60-second controller lease stops active requests and queued admission when the
Dagster controller disappears. Cancellation and restarts retain completed SQLite
outcomes; resume reconciles the unfinished service batch before selecting more
ClickHouse inputs. A new Backoffice owner can resume only after the previous owner
is stopped/finished and the new owner has the matching task/model dependency.
Completed SQLite batches are retained for seven days and pruned on later submissions;
unpublished batches are retained. PostgreSQL and ClickHouse history remain durable.

Completion repairs the successful-answer projection, verifies no work remains and
records counts. Before removing the completed task's input partition, the worker
copies and verifies every company and its provenance in
`corpscout.company_brave_task_sources`. History therefore includes freshness skips.
An interrupted cleanup can be resumed; history and result rows are never purged.

## Resume a draft

Start the same results job with `task_id`. The saved execution is recovered directly
from PostgreSQL; the original Dagster run is not required. An optional `execution_id`
must match that saved execution. Omit content settings to reuse the saved profile;
transport settings such as concurrency, page size and timeouts apply to newly submitted batches. An unfinished service batch retains its accepted configuration. Dagster
run tags retain task/execution IDs and final outcome counts for Backoffice history.

## Retired fixed selections

The old `company_brave_search_input` asset, input job and combined workflow have
been removed. Migration **454** drops their input table. Old manifests are marked
cancelled, so a saved execution cannot resume searches against deleted inputs.
Submit companies through the current draft queue to search them again.

Saved outcomes and old execution IDs remain available. History for these tasks
shows companies found in saved outcomes, rather than claiming the complete original
input selection. `mode: publish` can still repair saved execution projections.

## Deployment

Service-owned batches require the browser service's `BRAVE_CLICKHOUSE_URL`,
`BRAVE_CLICKHOUSE_USER`, `BRAVE_CLICKHOUSE_PASSWORD`, and `LLM_CONTROL_PG_URL`.
The ClickHouse publisher needs SELECT/INSERT on `company_brave_search_results`
and `se_company_brave_search_results_latest_success`; PostgreSQL uses the existing
`processing_worker` grants from migration 128. The encrypted LLM master key remains
`BROWSER_LLM_ENCRYPTION_KEY`. No new central database migration is required.
Deploy/configure the browser service before launching the updated Dagster definition.
An existing per-request run must be stopped and resumed to switch it to batches.


Apply ClickHouse migration **453** before deploying these definitions and Backoffice.
It adds the partitioned input and company-membership tables.

For legacy retirement, deploy the updated Backoffice and Dagster code first, verify
that no Brave runs are active, then mark old `brave-v2` manifests cancelled before
applying migration **454**. This permanently removes legacy queued inputs, including
rows with no task ID. Rolling back 454 only recreates the empty schema.
Grant `processing_publisher` SELECT on `company_brave_queue_input`; existing result
SELECT/INSERT grants remain necessary. The storage provisioning script includes it.
The worker uses its normal queue-management connection for imports and cleanup.

The previous migration **428** and `scripts/migrate-brave-results.py` cover old
PostgreSQL/S3 history cutover. Those archives are retained for audit; new outcomes
are stored directly in ClickHouse. Do not rerun an old outbox importer as queue setup.
