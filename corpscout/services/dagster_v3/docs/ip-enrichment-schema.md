# Shared IP enrichment schema

Migration `000433_corpscout_ip_enrichment` introduces two ClickHouse tables and
one ordinary view in `corpscout`. The `ip_enrichment_input` Dagster asset prepares
input batches from a list or a source relation. `ip_enrichment_results` processes
a prepared task using the existing MaxMind mapping and RDAP network cache.
The Workspace IP addresses page submits both steps through `ip_enrichment_workflow`.
The legacy GeoIP table and writer were retired by migrations 434–435.

## Input

`ip_enrichment_input` is an immutable batch of submissions, compatible with
`ClickHouseInputQueue` and its `(input_id, task_id)` sorting key. A producer
prepares the batch before registering it with the processing store. ClickHouse
does not enforce unique keys: admission must reject duplicate `input_id` values
within the task and producers must not append after admission.

Each row carries `ip`, `task_id`, `input_id`, `source_name`, `source_record_id`,
`source_run_id`, optional `observed_at`, and `submitted_at`. A record containing
multiple IPs needs a distinct input ID per IP. Multiple sources can submit the
same IP without losing their individual associations. Worker-level caching and
deduplication by canonical IP will avoid repeating fresh lookups.

Both tables require canonical IP strings: normalize IPv4 with
`toString(toIPv4(value))` and IPv6 with `toString(toIPv6(value))`. Invalid and
noncanonical strings are rejected. `ip_version` and the 256-way hash `bucket`
are materialized columns, so callers cannot accidentally supply inconsistent
values. Private and loopback IPs are valid inputs and receive `not_global`
lookup outcomes instead of external requests.

## Results and history

`ip_enrichment_results` stores one immutable outcome per processing attempt,
including task/execution/input/run identity, processor version, attempt number,
completion time, IP scope, GeoIP location, ASN, and normalized RDAP details.
The RDAP country is `rdap_country_code`, distinct from GeoIP `country_iso_code`.
`rdap_network_key` links to the existing network cache, while the copied
registration fields retain the information used for this particular result.
Raw RDAP responses remain in the existing RDAP storage.

The city, ASN, and RDAP components each have a lookup status, actual check time,
error code, and retry-after time. Statuses are `not_attempted`, `found`,
`not_found`, `not_global`, `retryable_error`, and `terminal_error`. Pending input
is represented by the absence of a result. A worker must populate lineage and
completion/check times, clear payload fields for negative/error outcomes, and
preserve original check times and MaxMind build epochs when reusing cached data.

Retries of a database write reuse the **same result UUID and identical row**.
A fresh lookup gets a new UUID. `ReplacingMergeTree` removes duplicate writes
without collapsing different attempts. Read history with `FINAL` for logical
counts independent of merge timing. There is no retention TTL.

## Current view

`ip_enrichment_current` stores no rows. It groups history by IP and exposes:

- Metadata and lookup statuses/errors from the latest completed attempt.
- The most recent conclusive payload independently for city, ASN, and RDAP.
- Each payload's `*_data_status`, `*_data_at`, and `*_data_result_id`.

Conclusive means `found`, `not_found`, or `not_global`. A failed refresh leaves
previous conclusive data available, but its latest error remains visible. A
newer negative lookup replaces old data. With no conclusive history, the view
returns the latest attempt's payload/status. Tuple aggregation preserves null
fields within a single lookup instead of filling gaps from older responses.

Payload selection uses actual check time, then completion time and result UUID
for deterministic ties. A later task reusing an older cache cannot overwrite
newer lookup information. Missing check times fall back to completion time for
selection, but remain null in `*_data_at` rather than fabricating a lookup time.

For bounded application reads, filter on `(bucket, ip)` using
`toUInt16(cityHash64(ip) % 256)`. This ordinary view computes over matching
history at query time. Input-only IPs need a left join from the input table.

## Migration and validation

The up migration only creates the new tables and view. It does not backfill,
enqueue work, modify existing tables, or change readers. The down migration
drops the view first, then results and input, deleting their data on rollback.

The real-engine tests in `tests/test_ip_enrichment_clickhouse_local.py` cover
multiple sources, IPv4/IPv6, retry deduplication, preserved history, component
fallback, null clearing, negative results, cached result age, deterministic
ties, filtering, canonical IP validation, and repeated up/down execution.

## Materializing the input asset

Apply migration 000433 before using `ip_enrichment_input` (group `ip_enrichment`)
or `ip_enrichment_input_job`. The asset uses the existing `clickhouse` and
`processing` resources, including `PROCESSING_PG_URL`, just like Brave input
selection. It prepares inputs only and does not perform GeoIP/RDAP lookups.

Explicit list, as launchpad run configuration:

```yaml
ops:
  ip_enrichment_input:
    config:
      ips: ["116.203.39.236", "185.28.20.221", "2001:4860:4860::8888"]
      source_name: manual
```

Select from an existing inventory with bound filters:

```yaml
ops:
  ip_enrichment_input:
    config:
      source_relation: corpscout.commoncrawl_ip_addresses
      ip_column: ip
      source_final: true
      filters:
        bucket: ["49"]
        ip_version: ["4", "6"]
      max_rows: 10000
```

Use either `ips` or `source_relation`. Table filters use OR within each list and
AND between columns. Selecting a whole relation requires `select_all: true` or
an explicit `max_rows` limit. Set `source_final` only for tables supporting
`FINAL`, not ordinary views. Source columns must contain individual IPs, not arrays.

Optional `source_record_id_column` preserves separate records referring to the
same IP. Without it, source identity defaults to the canonical IP. Optional
`observed_at_column` supplies observation time, keeping the maximum when duplicate
record/IP pairs occur. `source_name` defaults to the source relation (or `manual`
for a list), and `source_run_id` defaults to the loading Dagster run ID.

Explicit IPs are validated before execution. Table rows with null, blank, or
invalid IPs are skipped. Valid IPv4/IPv6 are canonicalized, duplicate record/IP
pairs are combined, and non-public addresses are retained for explicit lookup
outcomes. `max_rows` applies after this grouping, in IP/record order, so it caps
submissions rather than necessarily distinct IPs.

The materialization reports `task_id`, `selected_inputs`, and `selected_ips`.
Pass that task UUID in `task_id` to resume or inspect the same selection.
Re-materializing a completed selection leaves its rows unchanged, even if the
source changed. A new selection requires a new task UUID (omitting `task_id`
uses the processing task tag, root run ID on retry, or current run ID).
Changing parameters under an existing task UUID is rejected. Interrupted
selections cancel only their own outstanding insert and replace only their
unconfirmed task rows before retrying. No input payloads are copied to PostgreSQL.

`tests/test_ip_enrichment_input.py` exercises both modes and crash recovery using
disposable ClickHouse and PostgreSQL servers.

## Materializing enrichment results

Use `ip_enrichment_results` or `ip_enrichment_results_job` with the task UUID from
the input materialization:

```yaml
ops:
  ip_enrichment_results:
    config:
      task_id: "<input-task-uuid>"
      batch_size: 250
      max_requests: 250
      request_delay_seconds: 1.0
```

The input batch must be fully prepared. The processor validates its table UUID,
row count, unique input IDs, and upper bound against the saved selection. It uses
the same task lock as input preparation and shares the legacy `commoncrawl_rdap`
concurrency pool. Data stays in ClickHouse; PostgreSQL stores the selection
manifest, while Dagster run tags pin the execution identity and lookup policy.

Prerequisites are migration 000433, the existing RDAP tables and working
`rdap_network_trie` dictionary, and City/ASN databases in
`MAXMIND_DATABASE_DIRECTORY`. Missing storage or MaxMind files fails the run
before processing. No runtime DDL is performed.

Result and RDAP cache inserts use `async_insert=1` and `wait_for_async_insert=1`,
matching Brave. Each write waits for ClickHouse to flush it before processing continues;
network and segment writes complete before the lookup/result completion markers.
ClickHouse can combine concurrent compatible inserts, but this sequential worker may
still produce one-row flushes. Bulk input `INSERT SELECT` remains synchronous.

Each input produces one result with task/input/execution identity and independent
City, ASN, and RDAP outcomes. Non-public IPs receive `not_global` without external
requests. An individual lookup failure does not discard the other components.
GeoIP uses the installed database versions. RDAP reuses the most-specific known
fresh registration, including registrations discovered earlier in this run.
The default cache age is 30 days (`rdap_cache_days`). Coverage is best-known, not
proof that a more-specific undiscovered registration does not exist.

Network information includes `city_network`, `asn_network`, RDAP's inclusive
`rdap_start_address`/`rdap_end_address`, and the exact `rdap_matched_cidr` containing
the IP. Non-aligned RDAP ranges are decomposed into exact CIDR segments. New
registrations, segments, and direct-IP lookup outcomes are also written to the
existing `rdap_networks`, `rdap_network_segments`, and `rdap_ip_lookup_results`
tables. Universal `/0` responses and ranges not containing the requested IP are
rejected. Optional parent lookup depth defaults to one (`parent_depth`); parent
failures are reported but do not discard a valid direct registration.

`max_requests` bounds RDAP calls, including parent lookups. On exhaustion the run
reports failure with saved progress and leaves remaining inputs unfinished. To
continue, supply `execution_id` equal to the original results run UUID, along
with the same `task_id` and lookup settings. Operational limits (`batch_size`,
`max_requests`, and delay) can change on resume. Completed outcomes are skipped,
including writes whose acknowledgement was lost. Result IDs are deterministic
within the execution; fresh executions retain history with higher attempt numbers.

Lookup errors are saved and reported as a failed run rather than a successful
complete enrichment. Resume skips those saved outcomes. To retry them, start a
new execution after `retry_after`; cached retryable RDAP errors observe that
backoff. `force_rdap: true` in a new execution bypasses earlier RDAP cache/backoff.
City/ASN errors can be retried in a new execution after correcting the database
problem. Starting a new execution also adds result history for successful inputs
in that task, with RDAP cache reuse unless forced.

`tests/test_ip_enrichment_results.py` uses real disposable ClickHouse/PostgreSQL
storage and controlled MaxMind/RDAP responses to verify task isolation, both IP
families, network coverage, cache reuse, partial failures, request limits, and
resume after an interrupted write.


## Legacy GeoIP retirement (migrations 434–435)

Migration 434 copies the current legacy GeoIP snapshot into immutable results. It keeps
all City/ASN payload fields, database versions, lookup statuses and `ip_scope`.
`completed_at`, `city_checked_at` and `asn_checked_at` retain the original `enriched_at`.
RDAP is `not_attempted`, with no RDAP payload. Existing newer results stay authoritative.
IPv6 text is canonicalized as required by the destination, while `input_id` preserves
the original spelling as `legacy-geoip:<original IP>`.

The import uses `processor_version=legacy-geoip-import-v1` and fixed task/execution UUID
`cd603d91-8a85-52f4-9b74-61f95d2f763a`. Each result UUID is a deterministic 128-bit digest
of its source identity, timestamp and payload, making an identical replay idempotent.
These are imported historical results, not queued work: no input task or ProcessingStore
manifest is created and the migration task ID must not be submitted to the worker.

Deployment order: apply 434, validate the copy, switch all consumers to
`ip_enrichment_current`, deploy the retirement of the old GeoIP asset, then apply 435.
The final migration refuses to drop the old table if any source row's result ID and
SHA-256 fingerprint of all migrated fields cannot be found in the destination. It then
removes the temporary import view, legacy current view and legacy table. Its down migration
can reconstruct the source snapshot from the preserved import history. The forward-only
production ledger should use a new repair migration if restoration is needed.


## Workspace IP selection

The admin IP addresses page can select individual addresses across pages, the visible
page, or all addresses matching the applied search/version filters. Changing filters
clears the selection. All-matching selection carries a compact filter plus explicit
exclusions, and does not download the inventory into the browser or web server.

`ip_enrichment_workflow` runs input preparation before results processing with one
shared task UUID. Both selection modes use `source_relation: corpscout.commoncrawl_ip_addresses`.
Individual selections use `filters.ip` with the checked addresses; all-matching
selections use the applied search/version filters and exclusions. The application
does not insert input rows itself. Table selection groups duplicate observations by canonical IP and
keeps the maximum `last_seen`. `ip_search` accepts an exact IP (canonicalized) or a
literal prefix matching the admin list. `excluded_ips` removes canonical IPs from
that selection. The snapshot is frozen when the input step runs.

The UI submits `max_requests: null` so the entire selected batch can be processed;
the standalone worker default remains 250 requests. RDAP throttling, cache reuse,
error reporting, history and interrupted-run resume behavior are unchanged. Large
selections run in the background and the UI links to their Dagster run. Submission
success means the run was accepted, not that enrichment has already completed.
