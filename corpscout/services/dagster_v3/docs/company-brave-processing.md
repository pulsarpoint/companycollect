# Brave processing

`company_brave_search_results`, in the `company_domains` group, processes a **fixed
selection stored in a physical ClickHouse input table**. PostgreSQL stores results
and progress. Starting a three-million-company task creates one PostgreSQL task
record; it does not copy three million inputs into PostgreSQL.

Dagster tracks the asset run. PostgreSQL tracks the individual company IDs inside
that run, so a failed run can resume without repeating saved work. Progress counts
are logged and attached to the Dagster materialization.

## Initialize the input selection

Materialize `company_brave_search_input` independently, or launch
`company_brave_search_input_job`. It receives filter parameters and runs one
`INSERT SELECT` entirely inside ClickHouse:

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

`source_relation` can refer to another country's table or view. Column mappings
are configurable: `company_id_column` defaults to `company_id` and
`company_name_column` defaults to `company_name`. `country_code` is used for
attribution and prefixes input IDs, such as `SE:5560004615`; it is not automatically
included in the Brave prompt. `source_final: true` applies ClickHouse `FINAL` when
reading sources such as the Swedish ReplacingMergeTree registry.

`company_ids` is reserved for testing. Production selections use `filters`, which
maps source column names to allowed values (including `company_id` when selecting
specific companies): values within a column use `IN`; different columns
are combined with `AND`. Column names are validated and values are bound as query
parameters. These are scalar equality filters, not raw SQL expressions.
`company_name_pattern` adds a bound `ILIKE` pattern, `company_id_length` selects
an ID length, and `excluded_company_ids` removes unchecked rows from a query selection.
`max_companies` optionally limits the selection in ID order. To deliberately
select an entire source without filters or a limit, supply `select_all: true`.

In the backoffice, select rows on Sweden → Companies and click **Send for Brave
analysis**. The action launches `company_brave_search_workflow`: initialization
from `corpscout.se_companies_serving` with the selected filters, followed by
processing with the default official-website query. “Select all matching” sends
filters and exclusions, without expanding all matching IDs in the backoffice.
The saved selection reflects the serving table when initialization executes.

The asset creates a task ID (or accepts an explicit UUID), inserts the selected
company rows under that `task_id` in `corpscout.company_brave_search_input`, and
validates the fixed selection. PostgreSQL stores its identity, configuration
fingerprint and exact total, with status `selected`. It contains no input payloads
or per-company progress rows until processing begins.

Inspect the materialization's `task_id` and `selected_companies` metadata. You can
also inspect the selected rows before running Brave:

```sql
SELECT input_id, company_id, company_name, country_code
FROM corpscout.company_brave_search_input
WHERE task_id = 'the-task-UUID'
ORDER BY input_id;
```

Each task retains its own fixed selection in the same physical table. Initializing
another task does not clear or append to an existing task's selection. Do not
manually modify a task's rows while it has unfinished work. The original registry
can change without affecting an already prepared selection.

Rematerializing initialization with the same task ID and filters reuses its saved
selection, even after processing has begun. Changed filters require a new task ID.
If initialization fails before the selection is confirmed, retrying stops that
task's outstanding insert, removes only its unconfirmed rows and reruns selection.
Other tasks' rows and results remain intact. Initialization has its own
`company_brave_input` pool, so another selection can be prepared while a Brave
processing task is running. A PostgreSQL session lock excludes
competing initializers of the same task; no transaction stays open during the
ClickHouse query. A zero-match selection is valid and reports a total of zero.

## Process the prepared selection

Materialize `company_brave_search_results`, or launch `company_brave_search_job`,
with the task ID returned by initialization:

```yaml
ops:
  company_brave_search_results:
    config:
      task_id: "the-task-UUID"
      query_type: official_website
      query_template: "Find the official website of {company_name}."
      requests_per_route: 1
      input_batch_size: 100
      freshness_days: 0
      export_batch_size: 100
      export_interval_seconds: 30
```

The first processing run freezes the query configuration and changes the task from
`selected` to `ready`. Its source relation, selected rows and total already exist.
There is no need to pass `company_ids` or the input table again.

To initialize and process together, launch `company_brave_search_workflow` with
both `ops` configurations, omitting `task_id` from the processing configuration.
The initialization asset records the task ID in the run tag `processing/task_id`;
the downstream asset reads that tag. An explicit task ID on initialization is
passed through too. The asset dependency ensures initialization finishes first.
Dagster's launchpad YAML is run configuration, not a separate discovered YAML file.

The worker reads at most `input_batch_size` rows into memory. It atomically commits
their IDs and the admission cursor to PostgreSQL. The cursor means “admitted for
processing”, never “everything before this ID succeeded”. Claims, retry dates,
lease tokens and accepted result IDs are compact per-item progress records.
Unadmitted inputs have no PostgreSQL item record.

As slots open, the worker claims pending IDs or admits another bounded page.
On restart, unfinished IDs are recovered and their input values are read from
ClickHouse again. A replaced table UUID, missing retry input or early end of the
queue fails the run rather than silently skipping unfinished work. These checks
do not make a mutable table immutable: retaining the prepared selection unchanged
is part of the input contract.

For an existing independently prepared custom input table, the processing asset
still accepts `input_relation` when creating a task. Such a table must have a
unique nonempty String `input_id` and `ORDER BY input_id`; views are not queues.
The shared Brave input table is instead scoped by the initialized task ID and has
`ORDER BY (input_id, task_id)` plus a task ID skipping index.
A custom domain input table can expose `input_id` and `domain`, with:

```yaml
input_relation: corpscout.my_domain_selection
input_namespace: domain
query_type: domain_owner
query_template: "Which company owns {domain}?"
```

Placeholders are simple column names; missing/empty values, attribute access,
conversions and format expressions are rejected. Templates are rendered in memory
from the selected row and never become SQL. PostgreSQL saves the rendered request
with its response, not arbitrary input columns.

## Requests, results and recovery

Four routes (`direct`, `crawl_proxy1`, `crawl_proxy2`, `crawl_proxy3`) each allow one
request by default. Fast routes save their result and refill while slower routes
are still busy. `requests_per_route` supports 1–8; `input_batch_size` must cover all
configured request slots. The Dagster pool `company_domains_brave` limits concurrent
materializations to one, preserving the existing proxy traffic limit.

The browser opens Brave's **Ask** page, waits for completed answer actions and
captures the answer's Copy text. It distinguishes that button from the question's
Copy button. The copied answer is stored as `answer_text`, together with query,
company attribution, route, source URL, status and result identity. Domain extraction
is a separate concern; an answer can contain more than one website.

PostgreSQL saves each response and its progress transition in the same synchronous
transaction. Claims use `FOR UPDATE SKIP LOCKED` and renewable lease tokens. A stale
worker cannot overwrite a reclaimed attempt. Defaults are three attempts, a
60-second retry delay and a 300-second lease. Crashed attempts count toward the
budget; graceful exit releases unfinished claims.

Answer generation starts with `answer_timeout_seconds: 60`. Each saved answer-generation
`TimeoutError` adds another 60 seconds for that company, capped by
`max_answer_timeout_seconds: 180`: normally 60 → 120 → 180 seconds. The count is
read from PostgreSQL attempt history, so retries on another route or after a restart
retain their allowance. A new company starts at 60 seconds. Page-load and Copy
timeouts, and non-timeout failures, do not increase the answer wait. Legacy timeouts
without a recorded stage also qualify for an increase. Navigation and Copy operations
retain the browser resource's `page_timeout_ms` (60 seconds by default); these are
per-operation limits, not a single deadline for the whole request.

Every attempt records `error_stage` (`page_setup`, `page_load`, `answer_generation`
or `copy`), `error_type`, `answer_timeout_ms` and whole-request `elapsed_ms` in its
PostgreSQL payload and Dagster logs. Successful attempts have an empty error stage.
These compact diagnostics remain in PostgreSQL after the response text is archived;
the S3 response schema is unchanged. Raw browser exceptions are never stored because
they can include proxy credentials.

Resume with only the original task ID:

```yaml
ops:
  company_brave_search_results:
    config:
      task_id: "the-original-task-UUID"
```

Once an item reaches `terminal_failed`, an ordinary resume leaves it finished.
To retry those failures, explicitly set `retry_failed: true` and increase
`max_attempts` above their existing attempt count, for example to 6 after an initial
three-attempt run. This requeues only failed items with remaining budget, preserving
all attempt numbers and history. Successful, cached, cancelled and still-pending
items are not requeued. Reusing the same setting does not reset the retry budget.

After processing starts, the saved input relation, namespace, template, query type
and freshness policy remain fixed. Operational settings such as route concurrency and batch size can
change. A completed task can resume without access to its input table. Use
`mode: publish` with the task ID to publish saved responses without Brave requests.

`freshness_days` defaults to 30; zero forces fresh requests. Only published
successes are reused. The work fingerprint covers processor version, namespace,
query type, template, rendered query and input values, but not task ID or queue
name. Reuse is checked as each item is claimed and recorded as `skipped` progress.

## Progress and publication

Read live counts in PostgreSQL:

```sql
SELECT * FROM processing.task_progress WHERE task_id = 'task-uuid';
SELECT total, admitted_count, source_cursor
FROM processing.tasks WHERE task_id = 'task-uuid';
```

`total = queued + running + retry_wait + succeeded + terminal_failed + skipped + cancelled`.
`queued` includes unadmitted ClickHouse inputs. `remaining = queued + running + retry_wait`.
Terminal counts update transactionally; progress polling reads counters and bounded
open work instead of scanning millions of finished item records. `status = ready`
means the task can be processed; completion is represented by `remaining = 0`.

Saved outcomes are assigned to closed export batches. ClickHouse writes the full
batch directly from `processing.brave_export` to immutable Parquet files in the
`company-brave-history` S3 bucket, using the server-owned `brave_history` named
collection. File paths are stable by country and batch ID:
`v1/country=SE/batch_id=<uuid>/results.parquet`. Retries reuse and verify those
files instead of appending duplicates or overwriting history.

After verifying every archived field against PostgreSQL, ClickHouse bulk imports
successful responses with `INSERT SELECT FROM postgresql(...)` into the country's
current table. For Sweden this is **`corpscout.se_company_brave_domains`**. Its
replacement key is `(company_id, query_type)` and its version is `completed_at`.
Read with `FINAL` for the latest successful answer before background merges finish.
An older export cannot replace a newer answer, and an unsuccessful refresh leaves
the previous successful answer available. The response is the complete copied
Brave text, not an extracted or independently verified domain.

Only after both checks pass does one PostgreSQL transaction mark the batch
published/archived, save its paths, row counts and content digests in
`processing.export_batches.archive_manifest`, and remove `answer_text` from the
outbox payload. Result IDs, attribution, progress and freshness-cache references
remain in PostgreSQL. Any failure before acknowledgment leaves the response there
for retry. No Brave request is needed to retry publication.

The history view disables ClickHouse's query condition cache for its S3 reads.
During cutover on 26.5.1, a filtered read returned zero successful responses with
that cache enabled and all 308 with it disabled. The setting belongs to the view,
so callers do not need to remember it. Its definer is the provisioned
`processing_publisher` user: other readers need only `SELECT` on the country
history view, without direct S3 or named-collection access.

All attempts, including errors and superseded answers, remain queryable through
**`corpscout.se_company_brave_domains_history`**, a view over the S3 table function. It stores no
second physical copy of history in ClickHouse. For example:

```sql
SELECT company_id, query_type, answer_text, completed_at, archive_path
FROM corpscout.se_company_brave_domains FINAL
WHERE company_id = '5560004615';

SELECT result_id, task_id, status, answer_text, completed_at, _path
FROM corpscout.se_company_brave_domains_history
WHERE company_id = '5560004615'
ORDER BY completed_at DESC;
```

For a known batch, filter `_path` or query `s3(brave_history, filename='...')`
directly to avoid scanning the entire history. The current table can be rebuilt
using `INSERT INTO corpscout.se_company_brave_domains SELECT *, _path FROM
corpscout.se_company_brave_domains_history WHERE status='success'`. `FINAL`
then resolves multiple successful versions for the same company and query type.

Input remains shared across countries. Output routes by the captured `country_code`
to `<country>_company_brave_domains` and its `_history` table. Provision that
country's migration first; an absent destination keeps its responses in PostgreSQL
and fails publication instead of putting them into Sweden's table.

`unpublished` counts saved outcomes awaiting acknowledgment, including failed
attempts. A publication outage does not make successful searches pending again.
Flushes happen at the configured result count, on the first completion after the
time threshold, and at the end. There is no distributed transaction: a crash after
receiving an external answer but before saving it can repeat that request.

## Deployment and scope

PostgreSQL uses the existing server shared with Dagster, in the application's
`corpscout` database and `processing` schema. Dagster's internal metadata is in its
separate `dagster` database. The instances share server resources.

Apply PostgreSQL migration `000120_processing_clickhouse_input` after `000119` and
ClickHouse migration `000411` after `000410`. The PostgreSQL migration preserves
old result/export data and refuses to remove input payloads while legacy tasks
are unfinished. It is forward-only. Deploy the matching worker code after applying
it; old workers require columns that the migration removes.

The input table was renamed in place by ClickHouse migration `000412`, preserving
its data and UUID. Apply PostgreSQL migration `000121` afterward to update saved
task references. Apply this pair while Brave tasks are idle.

Initialization additionally requires ClickHouse migration `000413` and PostgreSQL
migration `000122`. They add task-scoped input rows and the `selected` task state.
The ClickHouse table UUID and existing rows are preserved. Earlier pilot rows use
an empty selection task ID; migration 122 binds old tasks to that legacy selection.
Deploy the matching reader and initialization asset after applying both migrations.

Current-table/S3 publication requires ClickHouse migrations `000414`–`000416` and PostgreSQL
migration `000123`. Finish or gracefully pause old Brave workers before applying
123; it changes existing batch destination identities. Run
`scripts/provision-processing-storage.py` first to provision the archive bucket,
named collection and roles, using the existing private credentials file. S3
credentials come from `CORPSCOUT_S3_*`, never materialization parameters. The
optional `--s3-endpoint-for-clickhouse` is for hosts where the ClickHouse server
uses a different network address from the provisioning process.

Publish each existing task once with the new worker. Previously published batches
without an archive receipt are backfilled as well. Verify complete history and
current-table coverage before dropping the retired `company_brave_info`,
`company_brave_info_deduplicated`, empty `company_brave_search_results`, and unused
`se_company_brave_input` view. Migrations 409/410 retain their version files but no
longer create the retired objects; migration 414 does not drop live data.

Existing `PROCESSING_PG_URL` and `PROCESSING_CLICKHOUSE_*` credentials remain valid.
Retain the private provisioning credentials file. Back up PostgreSQL progress,
ClickHouse inputs/current tables, access metadata, and **the authoritative
`company-brave-history` S3 bucket**. This bucket has no expiry policy. It is not a
rebuildable source-download cache: once an answer is pruned from PostgreSQL, its
historical content lives in S3.

This change applies to Brave only. Translation, Ratsit and webtech remain unchanged.
Tests use disposable PostgreSQL 17, ClickHouse 26.5 and RustFS servers, including a real
three-million-row ClickHouse queue with only 100 admitted PostgreSQL IDs, concurrent
claims, restart recovery, migration preservation and replayable publication.

The [live ClickHouse-input pilot](company-brave-clickhouse-input-pilot-2026-09-15.md)
verified bounded admission, eight real answers and a resume without new requests.

The [initialization asset pilot](company-brave-initialization-pilot-2026-09-15.md)
verified selection, idempotent rematerialization and handoff to processing.
