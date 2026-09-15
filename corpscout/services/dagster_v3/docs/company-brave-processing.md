# Brave processing

`company_brave_search_results`, in the `company_domains` group, processes a **fixed
selection stored in a physical ClickHouse input table**. PostgreSQL stores results
and progress. Starting a three-million-company task creates one PostgreSQL task
record; it does not copy three million inputs into PostgreSQL.

Dagster tracks the asset run. PostgreSQL tracks the individual company IDs inside
that run, so a failed run can resume without repeating saved work. Progress counts
are logged and attached to the Dagster materialization.

## Prepare and inspect the selection in ClickHouse

Migration `000411_corpscout_company_processing_input` defines an empty,
country-independent example queue, `corpscout.company_processing_input`.
Populate it before launching a task. Selection happens here, independently of the
Brave asset; neither `company_ids` nor raw selection SQL is sent to the asset.

For example, on an empty queue:

```sql
INSERT INTO corpscout.company_processing_input
    (input_id, company_id, company_name, country_code)
SELECT concat('SE:', company_id), company_id, trimBoth(ifNull(legal_name, '')), 'SE'
FROM corpscout.se_company_basic_info FINAL
WHERE status = 'active'
  AND trimBoth(ifNull(legal_name, '')) != ''
  AND company_id IN ('5560004615', '5560160680');

SELECT * FROM corpscout.company_processing_input ORDER BY input_id;
SELECT count(), uniqExact(input_id) FROM corpscout.company_processing_input;
```

This SQL is an example selection, not a Sweden restriction. The asset requires an
explicit `input_relation` and accepts another prepared table with different input
columns. It does not create a view or a table at materialization time.

**Keep the selected table unchanged until its task and retries finish.** It is the
retained input snapshot. Changes to the original registry do not affect rows
already copied into this queue. Do not truncate, update, refill or replace a queue
that an unfinished task still needs. For concurrent independent selections, use
distinct prepared tables. There is no automatic input cleanup.

The table must use `MergeTree`, `ReplicatedMergeTree` or `SharedMergeTree`, in a
database with table UUIDs, with `ORDER BY input_id`. Views and replacing/aggregating
engines are rejected. `input_id` must be a unique, nonempty, non-nullable `String`
without NUL. It is sorted lexically by ClickHouse; it need not be an incremental
number. Use a country/source prefix if company IDs can overlap across countries.

## Start a task

Paste this YAML into Dagster's materialization launchpad. It is run configuration,
not a separate file automatically discovered by Dagster:

```yaml
ops:
  company_brave_search_results:
    config:
      input_relation: corpscout.company_processing_input
      input_namespace: company
      query_type: official_website
      query_template: "Find the official website of {company_name}."
      requests_per_route: 1
      input_batch_size: 100
      freshness_days: 0
      export_batch_size: 100
      export_interval_seconds: 30
```

Supply a UUID `task_id` if you want to choose its identity beforehand; otherwise
the asset generates and logs one. Startup validates uniqueness and counts the
selection inside ClickHouse. Only the total, table identity and upper input ID
are stored in PostgreSQL. This validation scans IDs inside ClickHouse, so startup
is not a constant-time operation, but company rows do not cross to PostgreSQL.

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

A domain input table can instead expose `input_id` and `domain`, with:

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

Resume with only the original task ID:

```yaml
ops:
  company_brave_search_results:
    config:
      task_id: "the-original-task-UUID"
```

The saved input relation, namespace, template, query type and freshness policy
remain fixed. Operational settings such as route concurrency and batch size can
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

Saved outcomes are assigned to closed export batches. ClickHouse imports them with
`INSERT SELECT FROM postgresql(processing_postgres, table='brave_export', schema='processing')`
filtered by batch ID. The importer verifies the deduplicated count before
acknowledging the batch. Partial imports and lost acknowledgments replay the same
result IDs. Read `corpscout.company_brave_info_deduplicated` or the base table with
`FINAL`; filter successful outcomes and the appropriate query type for downstream use.

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

Existing `PROCESSING_PG_URL` and `PROCESSING_CLICKHOUSE_*` credentials remain valid.
`scripts/provision-processing-storage.py` owns the least-privilege worker, export
reader, publisher and named collection setup. Retain its private credentials file.
Credentials are never materialization parameters. Back up both the PostgreSQL
progress/results and retained ClickHouse inputs, including access metadata.

This change applies to Brave only. Translation, Ratsit and webtech remain unchanged.
Tests use disposable PostgreSQL 17 and ClickHouse 26.5 servers, including a real
three-million-row ClickHouse queue with only 100 admitted PostgreSQL IDs, concurrent
claims, restart recovery, migration preservation and replayable publication.

The [live ClickHouse-input pilot](company-brave-clickhouse-input-pilot-2026-09-15.md)
verified bounded admission, eight real answers and a resume without new requests.
