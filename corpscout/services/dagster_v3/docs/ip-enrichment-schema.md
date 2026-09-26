# Shared IP enrichment schema

Migration `000433_corpscout_ip_enrichment` introduces two ClickHouse tables and
one ordinary view in `corpscout`. Since migration `000456` the input table follows the
shared processing queue contract: `ip_enrichment_input` appends to an open draft and
`ip_enrichment_results` freezes and processes it (see
[ip-enrichment-draft-queue.md](operations/ip-enrichment-draft-queue.md)). The Workspace IP
addresses page adds to the draft; processing starts from the queue page.
The legacy GeoIP table and writer were retired by migrations 434–435. Their imported rows
were removed in the 2026-09 clean re-run; every row now comes from `ip-enrichment-v1`.

## Input

`ip_enrichment_input` is `MergeTree`, `PARTITION BY task_id`, `ORDER BY (task_id,
input_id)`, read without `FINAL`. `input_id` is the bucket-prefixed JSON tuple of
`source_name`, `source_record_id` and `ip`, computed and enforced in ClickHouse; the
draft keeps one row per identity and a completed task drops its partition.

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

Migrations `000450` and `000451` add the IP registry special segments (IANA blocks, RIR
available/reserved ranges) and make `rdap_network_trie` serve only registrations classified
`reusable`; `ip_enrichment_results` reports `registry_level_responses` (registrations that answered
only their queried address). See `docs/operations/ip-registry-reference-data.md`.

## Materializing the input asset

Apply migrations 000433 and 000456 before using `ip_enrichment_input` (group
`ip_enrichment`) or `ip_enrichment_input_job`. The asset appends to the open draft
named by `queue_scope` (default `workspace`) with a stable `submission_id`, using the
existing `clickhouse` and `processing` resources, including `PROCESSING_PG_URL`, just
like Brave input selection. It prepares inputs only and does not perform GeoIP/RDAP
lookups.

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

Retry the failed addresses of an earlier task:

```yaml
ops:
  ip_enrichment_input:
    config:
      retry_failed_task_id: "<task uuid>"
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

The materialization reports `task_id`, `submission_id`, `input_count` (rows this
submission added) and `total`. Repeating a `submission_id` with the same selection is a
no-op; a different selection under it is rejected; a failed import is retried by
reselecting the source. New submissions after Start go to the next draft.
`tests/test_ip_enrichment_input.py` exercises every mode and crash recovery using
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
      registry_daily_budgets: {}
      ripe_rest: true
      apnic_whois: true
```

The asset freezes the draft through the shared queue-contract lifecycle
(`queue_execution.start_execution`), then walks each bucket's live remaining query
(entries with no result of this execution) in pages, with a bounded number of
ClickHouse round trips per page and one registry-class context query per miss. Outcomes
are written through a `ResultBuffer`, flushed at the end of every pass. RDAP freshness is
judged against the frozen execution's cache window, not wall-clock time at lookup. RIPE
misses go to the RIPE Database REST search and APNIC misses to whois `-r`, both without
personal data; an NIR's own object or a catch-all falls back to RDAP. An optional
per-registry daily budget (`registry_daily_budgets`) defers a miss at its limit instead of
requesting or failing, and the run waits only when nothing else remains. See
[ip-enrichment-draft-queue.md](operations/ip-enrichment-draft-queue.md) for the full
resolver, budget and pause behavior.

`max_requests` bounds RDAP calls, including parent lookups; reaching it flushes what was
resolved and fails the run, leaving the task `selected` so re-running it resumes the same
execution. The job carries `dagster/max_retries: "0"`, so Dagster's run retries never
relaunch it; resuming is the operator's re-run. RDAP or GeoIP lookup errors are saved and
reported as a published outcome (`completed_with_errors`), not a pipeline failure; retry them
by adding the addresses to a new draft (`retry_failed_task_id`; terminal RDAP errors are
re-served from their markers within `rdap_cache_days` unless that draft is processed with
`force_rdap: true`). Completion drops the task's ClickHouse partition.
`attempt` is always 1 — a retry is a new execution in a new draft, not a higher attempt
number. Every run reports the installed GeoLite2 build dates in its metadata.

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

**2026-09-26 note (the 2026-09 clean re-run).** The imported rows (8.29M,
`processor_version=legacy-geoip-import-v1`, task `cd603d91-…`) were truncated together with
the rest of `ip_enrichment_results` (plan `2026-09-25-ip-enrichment-queue-contract.md`,
Task 8). The paragraphs above describe the import as it was: 435's down migration now
restores an **empty** `commoncrawl_ip_geoip`, since the preserved import history it
reconstructs from is gone, and "newer results stay authoritative" and the fixed task UUID
no longer apply. The only rollback of those rows is the ClickHouse B2 backup named in Task 8
Step 2 (`rollback-backup.txt`).


## Workspace IP selection

The admin IP addresses page can select individual addresses across pages, the visible
page, or all addresses matching the applied search/version filters. Changing filters
clears the selection. All-matching selection carries a compact filter plus explicit
exclusions, and does not download the inventory into the browser or web server.

**Add to enrichment queue** launches `ip_enrichment_input_job` with a stable
`submission_id` and `queue_scope: workspace`; processing is started from Queues → IP
enrichment. Both selection modes use `source_relation: corpscout.commoncrawl_ip_addresses`.
Individual selections use `filters.ip` with the checked addresses; all-matching
selections use the applied search/version filters and exclusions. The application
does not insert input rows itself. Table selection groups duplicate observations by canonical IP and
keeps the maximum `last_seen`. `ip_search` accepts an exact IP (canonicalized) or a
literal prefix matching the admin list. `excluded_ips` removes canonical IPs from
that selection. The snapshot is frozen when the input step runs.

The queue sheet's template sends `max_requests: null` so the whole task is processed;
the standalone asset default remains 250 requests. Large drafts run in the background
and the queue page links to their Dagster run.
