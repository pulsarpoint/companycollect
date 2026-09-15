# Brave processing pilot

The `company_brave_search_results` asset in `company_domains` uses the shared
PostgreSQL `processing` schema for frozen input, claims, responses and progress.
ClickHouse imports closed result batches through its PostgreSQL integration.
This pilot replaces Brave's per-response S3 write and ClickHouse insert. The old
`company_brave_search_results` ClickHouse table and its S3 objects remain readable;
new responses go to `company_brave_info.answer_text`.

## Start a task

Paste this YAML into the Dagster materialization launchpad (it is run config,
not a separate file that Dagster discovers):

```yaml
ops:
  company_brave_search_results:
    config:
      input_relation: corpscout.se_company_brave_input
      input_namespace: se_company
      query_type: official_website
      query_template: "Find the official website of {company_name}."
      max_companies: 100
      requests_per_route: 1
```

The default input view selects active Swedish companies with nonempty names.
`max_companies` is optional; leaving it out selects all rows in the relation.
`company_ids` can restrict selection by `input_id`. For a new task the asset
streams **one SELECT** and inserts the frozen rows into PostgreSQL in batches.
The snapshot commits atomically before any browser starts. A failed snapshot
creates no partially ready task. An empty selection produces a successful empty
task. A duplicate or empty `input_id` fails snapshot preparation.

You can test a selection independently and then provide its view name:

```sql
CREATE VIEW corpscout.my_company_selection AS
SELECT company_id AS input_id, company_id, legal_name AS company_name,
       'SE' AS country_code
FROM corpscout.se_company_basic_info FINAL
WHERE status = 'active' AND company_id IN ('5560004615', '5560160680');

SELECT * FROM corpscout.my_company_selection;
```

This must be a normal named view/table visible to Dagster's ClickHouse connection,
not a session-local temporary view. Once the snapshot has committed, changes to
that view or its source rows cannot change this task's input. The view may then
be removed independently. View creation from a backoffice selection is a later
integration; this pilot accepts an existing view.

Every input needs a stable nonempty **String** `input_id`. IDs need not be numeric,
incremental or sortable: PostgreSQL tracks each item separately. For a domain
selection, expose `domain AS input_id, domain` and use, for example:

```yaml
input_relation: corpscout.my_domain_selection
input_namespace: domain
query_type: domain_owner
query_template: "Which company owns {domain}?"
```

`input_namespace` identifies the kind of entity. Keep it stable across selection
views containing the same kind of input. Template placeholders are simple column
names; missing/empty values, attribute access, conversions and format expressions
are rejected. All input columns and the exact rendered query are frozen. Dates
and decimals in custom views are stored as text. Templates never become SQL.

## Resume and publish

`task_id` is logged and attached as output metadata before preparation starts.
You may supply a UUID when starting a task to make its identity known beforehand.
Resume an interrupted task with:

```yaml
ops:
  company_brave_search_results:
    config:
      task_id: "the-UUID-from-the-original-run"
```

The same task uses its stored selection and queries. Input relation, selection,
query type, template and freshness options apply only when creating a task;
changing them requires a new task ID. Operational settings such as route
concurrency and export batch size can change on resume. Dagster run IDs identify
execution attempts; they are never a completion cursor.

For publication recovery alone, set `mode: publish` with that task ID. This reads
saved responses and makes no Brave requests. A ClickHouse outage may fail the
materialization while all browser work is already saved. Publication recovery
succeeds independently of whether some input items exhausted their search retries.

## Concurrency and recovery

Each direct/proxy worker holds its own browser. Four routes (`direct`,
`crawl_proxy1`, `crawl_proxy2`, `crawl_proxy3`) run with one request each by default.
A worker saves its outcome in a short PostgreSQL transaction before taking another
item; fast routes refill while slow routes are still busy. `requests_per_route`
allows 1–8. The Dagster `company_domains_brave` pool retains its existing limit of
one materialization, so overlapping Dagster runs do not multiply proxy traffic.

Claims use `FOR UPDATE SKIP LOCKED` and a fresh lease token. Heartbeats renew live
claims every third of `lease_seconds` (default 300 seconds). A stale worker cannot
save over a reclaimed item. Graceful exit releases unfinished claims; after a
killed process, expired claims become eligible again. Searches retry up to
`max_attempts` (default 3), waiting `retry_seconds` (default 60) after errors.
Attempts abandoned by crashed workers count toward that budget too.

Task progress is available directly in PostgreSQL throughout processing:

```sql
SELECT * FROM processing.task_progress WHERE task_id = 'task-uuid';
```

`total = queued + running + retry_wait + succeeded + terminal_failed + skipped + cancelled`.
`remaining = queued + running + retry_wait`. `unpublished` counts saved outcomes
whose export batch has not been acknowledged, including error attempts. A task
can therefore have `remaining = 0` and `unpublished > 0`. Task `status = ready`
means its snapshot is usable, not that processing or publication has finished.
Dagster materialization metadata includes these counts and the task ID.

New tasks reuse only published successes within `freshness_days` (default 30).
The fingerprint covers processor version, input namespace, query type, template,
rendered query and frozen input values. Selection view names, row limits and task
IDs do not change that fingerprint. `freshness_days: 0` forces fresh work. The
legacy S3-only Brave index is not reused for the new query contract. Failed items
remain visible; start a new selection to retry terminal failures, reusing its
already published successes as appropriate.

## Publication contract

Responses are committed with item state in PostgreSQL before publication.
Each result has a stable ID and timestamp. Export assigns a fixed set of results
to a batch in a transaction, then ClickHouse executes:

```sql
INSERT INTO corpscout.company_brave_info (...)
SELECT ...
FROM postgresql(processing_postgres, table='brave_export', schema='processing')
WHERE export_batch_id = 'batch-uuid';
```

The equality predicate is pushed to PostgreSQL; an index supports batch lookup.
The importer checks the deduplicated row count before acknowledging the batch.
An interrupted insert or lost acknowledgment replays the same IDs. Consumers
must read `corpscout.company_brave_info_deduplicated`, or the base table with
`FINAL`, for correctness before background merges. Error responses are retained
alongside successes, so domain consumers should filter `status = 'success'` and
choose the desired `query_type`/version.

Batches flush at 100 saved outcomes or on the first completion after 30 seconds,
plus a final flush; both thresholds are configurable. PostgreSQL is the durable
outbox, so an outage does not discard responses or make successful items pending.
There is no distributed transaction or guarantee that an external search executes
exactly once: a crash before saving its response can repeat that search.

The pilot retains snapshots/results/batches; no automatic pruning is enabled.
Include the `corpscout` PostgreSQL database in backups. Do not apply the existing
"S3 is rebuildable cache" retention policy to this queue. PostgreSQL uses one
serialized connection per Dagster run and synchronous commits. Larger evidence
objects and other processors can adopt this contract separately; translation,
Ratsit and web technology flows are unchanged by this pilot.

## Deployment

1. Apply PostgreSQL migration `000119_processing_tasks` to the application
   `corpscout` database and ClickHouse migration `000410_corpscout_company_brave_info`.
2. Run `scripts/provision-processing-storage.py` with `PROCESSING_ADMIN_PG_URL`
   and the existing administrative `CLICKHOUSE_*` environment. The ClickHouse
   administrator needs named-collection control during provisioning. The script
   saves generated credentials in a required mode-0600 `--credentials-file`;
   retain that file for idempotent provisioning. Supply a PostgreSQL host reachable
   from both Dagster and ClickHouse with `--postgres-host-for-clients`.
3. Copy only `PROCESSING_PG_URL`, `PROCESSING_CLICKHOUSE_USER`, and
   `PROCESSING_CLICKHOUSE_PASSWORD` into the server-owned Dagster `.env`.
   PostgreSQL worker/reader credentials never enter Dagster materialization YAML.
4. Deploy using the full Ansible `sync.yml` path because environment/dependencies
   changed. Verify definitions and the external read before launching any task.

The PostgreSQL worker has access only to the processing schema; the export reader
can read only `processing.brave_export`. The persisted ClickHouse named collection
`processing_postgres` uses that reader and forbids connection overrides. The SQL
user `processing_publisher` can use the collection and read/insert the result
table. The normal ClickHouse resource selects input; this separate publisher
resource uses `PROCESSING_CLICKHOUSE_*`. Administrative privileges are not needed
at materialization time. Back up ClickHouse access metadata/named collections too.

Validation uses disposable PostgreSQL 17 and ClickHouse 26.5 Docker servers,
including concurrent claims, stale fencing, partial publication, lost acknowledgments,
a publication outage, credential scope, and actual Dagster materialization/resume.
The browser tests intercept requests with fixtures; they do not query Brave.
