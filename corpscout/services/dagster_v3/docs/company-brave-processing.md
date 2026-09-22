# Brave processing

`company_brave_search_input` freezes a company selection in ClickHouse.
`company_brave_search_results` searches that selection and writes each completed outcome
straight to `corpscout.company_brave_search_results`, including errors. The group is
`brave_domain_search`; job names remain `company_brave_search_input_job`,
`company_brave_search_job`, and `company_brave_search_workflow`.

Dagster owns run execution and the `company_domains_brave` concurrency pool (limit 1).
The browser service owns the browser requests. ClickHouse owns completed attempts,
skipping, age checks, and result counts. There are no new per-company PostgreSQL
claims, leases, retries, progress counters, or response/outbox records.

The initializer still uses one `processing.tasks` PostgreSQL record per fixed
selection to retain its fingerprint, table UUID, upper bound, total and query defaults.
This is selection metadata, not processing progress. Do not use the legacy
`processing.task_progress` view to monitor new executions. Dagster metadata reports
`succeeded`, `failed`, `skipped`, and the execution ID; exact attempt counts are also
available from ClickHouse.

## Prepare the fixed input selection

```yaml
ops:
  company_brave_search_input:
    config:
      source_relation: corpscout.se_company_basic_info
      company_name_column: legal_name
      country_code: SE
      source_final: true
      filters:
        status: [active]
```

Initialization runs `INSERT SELECT` inside ClickHouse. It returns `task_id` and
`selected_companies`. Rows are saved under that task in
`corpscout.company_brave_search_input`, with input IDs such as `SE:5560004615`.
A task's selection must remain unchanged while processing or recovery is unfinished.

The source and company ID/name columns are configurable. `filters` maps scalar
column names to allowed values; values within one column use `IN`, different columns
use `AND`. Values are bound parameters. `company_name_pattern`, `company_id_length`,
`excluded_company_ids`, and `max_companies` further restrict selection. `company_ids`
is intended for tests. An unrestricted selection requires explicit `select_all: true`.

Reinitializing a task with the same selection fingerprint reuses its rows. Changed
filters require a new task ID. Failed initialization recovers only that task's
unconfirmed rows; a zero-row selection is valid. Its separate `company_brave_input`
pool allows selection preparation while another task is searching.

Backoffice's **Send for Brave analysis** launches `company_brave_search_workflow`
against `corpscout.se_companies_serving`. Query-based selections retain filters and
exclusions without expanding all matching IDs in the backoffice. The initializer
passes its task ID to the search asset via the `processing/task_id` run tag.

## Search and rescan rules

```yaml
ops:
  company_brave_search_results:
    config:
      task_id: "the-initialization-task-UUID"
      query_type: official_website
      query_template: "Find the official website of {company_name}."
      force: false
      rescan_old: false
      requests_per_route: 1
      input_batch_size: 100
      answer_timeout_seconds: 60
      progress_log_every: 100
      progress_log_interval_seconds: 30
```

Run logs include `Brave progress` at startup, completion, and after 100 newly
accounted inputs or 30 seconds when a result or skip advances the input loop.
Both thresholds are configurable. `processed` is the number of saved success/error
outcomes across the whole execution, including before a resume; `new_results`
counts only this run's acknowledged writes. `skipped` counts other existing outcomes
that do not need a new search. `remaining = total - processed - skipped` includes
in-flight and unexamined inputs, so it can also decrease as cached inputs are found.
The percentage includes processed and skipped entries. Counters advance after
ClickHouse acknowledges a write; final totals are read back from ClickHouse.

Search identity is `(country_code, company_id, query_type)`. The latest completed
attempt is selected by `(completed_at, result_id)`, whether its status is `success`
or `error`.

| Condition | Action |
| --- | --- |
| No completed outcome | Search |
| `force: true` | Search regardless of previous outcomes |
| `force: false`, `rescan_old: false` | Skip completed searches regardless of age |
| `force: false`, `rescan_old: true` | Search when the latest outcome is strictly older than 30 days |

The 30-day cutoff uses UTC and is fixed when the execution starts. A changed company
name, prompt, or processor version does not implicitly bypass this identity: use
`force` to deliberately refresh an existing search type. Different query types do
not suppress each other. This asset accepts Swedish company inputs only.

Each selected company receives one completed search attempt per execution. A saved
error is a completed outcome, not an automatically requeued company. Transport
recovery at the browser-service boundary reuses the same request ID. A run with
search errors reports failure with `allow_retries=False` and includes saved counts;
a new forced execution can search those companies again.

`freshness_days`, `retry_failed`, `max_attempts`, `retry_seconds`, `lease_seconds`,
`max_answer_timeout_seconds`, `export_batch_size`, and `export_interval_seconds` no
longer apply. Use `force`/`rescan_old`, `answer_timeout_seconds`, and execution recovery.

Custom physical input tables remain supported through `input_relation`; they need
unique nonempty `input_id` values and `country_code`, `company_id`, `company_name`
columns. Query templates can use other named columns. The registered table UUID,
upper bound and row count protect recovery from replaced or missing inputs.

## Results and durable writes

`corpscout.company_brave_search_results` stores full answer text, status,
completion time, company/query attribution, route, error type/stage, timing,
CAPTCHA diagnostics, task/execution/run IDs, and attempt identity. Failed searches
are retained alongside successes. There is no TTL.

`result_id` is derived from execution ID and input ID before submitting to the
browser service. A retry reuses that identity. Separate executions get new IDs and
preserve history. `ReplacingMergeTree` removes duplicate writes of the same attempt;
use `FINAL` for exact history and counts before merges occur.

The writer sends `async_insert=1, wait_for_async_insert=1`. Each browser route waits
for storage acknowledgement before taking another input. Connections are separate
per concurrent writer. A failed insert stops processing, and restarting rechecks
ClickHouse before submitting requests. There is no fallback PostgreSQL outbox.

`company_brave_search_results_latest` exposes the full latest attempt, including failures,
for each `(country_code, company_id, query_type)`. The `se_company_brave_search_successes`
materialized view feeds successful answers to the existing
`corpscout.se_company_brave_search_results_latest_success` table. Read this successful-answer projection with `FINAL`.
A failed rescan never removes the previous successful answer. New writes use empty
legacy archive/export identifiers because their full history is now in ClickHouse.

```sql
SELECT * FROM corpscout.company_brave_search_results_latest
WHERE country_code = 'SE' AND company_id = '5560004615';

SELECT status, answer_text, error_type, error_stage, completed_at
FROM corpscout.company_brave_search_results FINAL
WHERE country_code = 'SE' AND company_id = '5560004615'
ORDER BY completed_at DESC, result_id DESC;

SELECT status, count()
FROM corpscout.company_brave_search_results FINAL
WHERE execution_id = 'the-original-Dagster-run-UUID'
GROUP BY status;
```

## Resume an execution

Dagster retries reuse the original run ID as the execution ID. For manual recovery:

```yaml
ops:
  company_brave_search_results:
    config:
      execution_id: "the-original-Dagster-run-UUID"
```

The original run retains the selection, query, rescan settings, and start time in
its `brave/execution` tag. Recovery requires that Dagster run to remain available.
Saved outcomes belonging to that execution are always skipped, including forced
executions. A new run with `task_id` and `force: true` instead starts another search
of the selection. Explicit changes to a resumed execution's query or skip policy
are rejected; omit those fields when resuming.

`mode: publish` with `execution_id` repairs the successful-answer projection from
ClickHouse without reading the input table or launching Brave. Normal execution
also repairs its saved successes on entry and completion, covering an interrupted
materialized-view write.

## Cutover from the PostgreSQL outbox

1. Finish or stop old Brave workers before importing; do not let old and new writers overlap.
2. Apply ClickHouse migration **428**. Update the existing `processing_publisher`
   grants with SELECT/INSERT on `company_brave_search_results` and SELECT on
   `company_brave_search_results_latest`, retaining the current-table permissions.
3. Run `uv run python scripts/migrate-brave-results.py --execute` using the existing
   processing PostgreSQL/ClickHouse environment variables.
4. The importer loads existing country latest-success tables and S3 archive views, then migrates every
   Postgres result, including unpublished answers and errors. Archived answer text
   is combined with the retained PostgreSQL diagnostics. Each batch is compared
   field-by-field after insertion. Missing answers or mismatched rows stop cutover.
5. Deploy the new definitions only after verification succeeds. Retain the old
   PostgreSQL data and S3 objects; the importer never deletes or acknowledges them.

The import is replayable and will not replace direct-write results with incomplete
legacy rows. Old S3 history views remain available for audit. New search history is
in ClickHouse. The legacy publication module remains only for old-outbox recovery
and cutover tests, not for new search executions.

## Result naming

`company_brave_search_results` is both the primary Dagster asset and its physical
ClickHouse history table. `company_brave_search_results_latest` is the latest-attempt
view. The Swedish successful-answer projection is
`se_company_brave_search_results_latest_success`; the older Parquet archive is
`se_company_brave_search_results_s3_archive`. New results are written directly to
ClickHouse, so this legacy S3 archive is not a mirror of new search outcomes.

The job names and `brave/execution` tags are unchanged. To resume an execution
created before the asset rename, launch `company_brave_search_job` with the new
`company_brave_search_results` config key and the original `execution_id`.
