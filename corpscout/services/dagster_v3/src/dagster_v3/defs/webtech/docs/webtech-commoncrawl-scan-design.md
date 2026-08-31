# Common Crawl web-technology scan

**Status:** remote service deployed and end-to-end real-host smoke passed on
2026-08-30. Production candidate selection covers the harmonic top one million
through 128 static hash partitions.

## Boundary

Dagster selects domains and exposes durable materializations. The dedicated
scanner workstation owns CloakBrowser, the Wappalyzer extension, browser
capacity, timeouts, and direct RustFS persistence. The two processes communicate
through a small authenticated HTTP API and immutable S3-compatible objects.

There is no message broker, scanner database, ClickHouse job-status table,
per-batch checkpoint object, or browser code in Dagster. The canonical scanner
and extension live in:

```text
corpscout/services/webtech/
```

The Dagster integration is a reusable `WebtechScannerComponent` instantiated by
`defs/webtech/component/defs.yaml`. Its configuration contains the API URL and
shared `WEBTECH_S3_PATH`; the API resource keeps `WEBTECH_API_TOKEN` as a
Dagster environment-variable reference so multiprocess workers resolve it at
runtime without embedding the secret in component YAML.

## Partition-aligned assets

Four durable assets share 128 static partitions, `hash_000` through `hash_127`:

1. `commoncrawl_webtech_candidates_manifest` selects ordered candidates from
   `corpscout.commoncrawl_domain_graph_signals FINAL` and writes the input
   manifest to RustFS.
2. `commoncrawl_webtech_scan_submission` submits that content-addressed input
   and materializes immediately with the deterministic remote scan ID.
3. `commoncrawl_webtech_remote_scan` stays `STARTED` while it polls the scanner
   every two seconds with a zero-wait HTTP request. It writes each status sample
   directly to the Dagster run log and materializes only after the scanner
   completes and its RustFS final manifest is verified.
4. `commoncrawl_webtech_results_clickhouse` validates the final manifest plus
   every referenced object checksum, then inserts the queryable result index.

There is no separate status asset and no sensor. The native Dagster status for
the active `commoncrawl_webtech_remote_scan` partition is `STARTED`, and its run
log contains completed/total counts, outcomes, technologies, progress age,
elapsed time, and throughput. `commoncrawl_webtech_scan_submission` means only
that the asynchronous request was accepted; `commoncrawl_webtech_remote_scan`
means the batch is genuinely complete and verified in RustFS.

The candidate universe is fixed in code to `cc_harmonic_rank` 1 through
1,000,000. Partition ownership is stable and evenly distributed by
`cityHash64(lower(root_domain)) % 128`; for example, `hash_000` owns remainder
zero. Rows remain ordered by harmonic rank inside each manifest.

The operator-facing candidate config defaults `crawl_id` to
`CC-MAIN-2026-apr-may-jun`, so a partition can be materialized directly from the
asset page. Launchpad can still override the crawl explicitly when required.

Before writing a manifest, the asset queries
`corpscout.webtech_domain_scan_results FINAL`. A domain is excluded when the
current detector has any terminal result with `scanned_at` in the previous
month. Freshness is domain- and detector-based rather than crawl-based, so a new
Common Crawl snapshot does not immediately rescan the same site. Setting
`force_rescan=true` explicitly bypasses only this freshness predicate; it does
not change the top-million boundary or hash ownership.

The `commoncrawl_webtech_scan_job` selects all four assets. The scanner API and
Dagster concurrency pool enforce the one-active-scan limit, so operators launch
partitions one at a time. `commoncrawl_webtech_finalize_job` can attach the same
polling asset and ClickHouse indexer to a submission that already exists, which
is useful after an interrupted Dagster run.

The polling loop does not hold an HTTP connection open. Every two seconds it
makes one zero-wait status request, logs the returned snapshot in the active
Dagster step, closes the request, and sleeps locally. Transient API transport
errors are logged and retried on the next interval. A completed scan is accepted
only after the asset reads and validates its RustFS final manifest.

## API and lifecycle

The scanner exposes:

```text
GET  /healthz
POST /v1/scans
GET  /v1/scans/{scan_id}?after_event=<sequence>&wait_seconds=30
POST /v1/scans/{scan_id}/cancel
```

All `/v1` routes require a bearer token. A submit request includes `crawl_id`,
`partition_key`, detector version, candidate-manifest S3 URI, and its SHA-256.
The scanner verifies the manifest and derives a deterministic scan ID from the
request content plus its effective browser settings. Repeating the same submit
attaches to the same active or completed scan.

Only one scan runs at a time. The current tested workstation settings are owned
by its `.env`:

| setting | value |
|---|---:|
| headless | `true` |
| browser contexts | `20` |
| pages per context | `1` |
| domains per fresh context | `1` |
| per-domain hard timeout | `60 seconds` |
| context launch stagger | `250 ms` |

Each context therefore serves one domain and is torn down completely. The pool
keeps up to 20 domains active and starts the next context when one result has
been persisted. There is no synchronized 20-domain execution barrier.

The scanner logs one structured progress line after every 20 stored domain
outcomes, plus start, heartbeat, stalled, completion, failure, and cancellation
lines in its systemd journal. `/healthz` exposes the active scan ID,
completed/total, progress age, and throughput. No CPU polling is required to
distinguish active progress, stalled work, completed work, and an idle service.

Dagster intentionally keeps the lightweight `commoncrawl_webtech_remote_scan`
step active while browsers execute so the partition has native `STARTED` state
and its status is visible in one run log. If that Dagster run is interrupted,
rerunning `commoncrawl_webtech_finalize_job` for the same partition attaches to
the durable submission. If the scanner service restarts and forgets in-memory
state, the polling asset repeats the idempotent submit; the service reconstructs
stored domains from RustFS and scans only missing candidates.

Cross-run asset edges after submission are metadata-only `Nothing` dependencies.
The finalizer reconstructs the submission and final-manifest references from
partitioned Dagster materialization metadata and reads RustFS directly. It never
loads a Python object from Dagster's local filesystem IO manager, so retrying a
finalization run cannot fail because a submission pickle is absent.

## Durable object contract

The candidate manifest is stored below:

```text
<WEBTECH_S3_PATH>/candidates/
  detector_version=<version>/
  crawl_id=<crawl-id>/
  partition_key=<partition>/
  dagster_run_id=<run-id>/manifest.json
```

Remote output is stored below:

```text
<WEBTECH_S3_PATH>/scans/
  detector_version=<version>/
  crawl_id=<crawl-id>/
  partition_key=<partition>/
  scan_id=<scan-id>/
    results/root_domain=<domain>/report.json
    final-manifest.json
```

Every domain result is written before its browser worker accepts another domain.
`final-manifest.json` is written last and names every result key, SHA-256, byte
size, outcome, duration, and technology count. The final marker is the only
completion checkpoint. A completed extension report with zero technologies is a
valid success.

The ClickHouse index is
`corpscout.webtech_domain_scan_results`. Migration 352 creates the table and
migration 355 adds the stable remote `scan_id`; migration 356 adds queryable
`timeout_stage` and `extension_failure_stage` diagnostics. `run_id` remains the
Dagster run that validated and indexed the object. `error_message` holds the
bounded safe error text, while complete reports and detailed stage timings
remain in RustFS.

## Why the browser moved off Dagster

The completed batch-isolated top-1,000 Dagster-host pilot ran for 13,469 seconds
(4.45 domains/minute) and produced 472 successes, 466 extension report timeouts,
54 navigation errors, and 8 browser errors. It consumed 34,618 CPU-seconds and
peaked at 6.17 GiB. Later isolated workstation tests showed materially better
throughput with 20 fresh contexts, local DNS, and a 250 ms launch stagger.

The extension 1.4.x finalization work also moved many prior hard timeouts to
completed reports. Remaining timeouts were frequently inside CPU-heavy
Wappalyzer processing rather than page navigation. This is why the workstation,
not Dagster config, owns concurrency and why capacity can be increased by moving
or scaling the scanner service without changing asset semantics.

## Validation and rollout gate

Focused validation:

```bash
cd corpscout/services/webtech
uv sync --all-groups
uv run pytest -q
uv run ruff check .

cd ../dagster_v3
uv run pytest -q tests/test_webtech_pilot.py tests/test_clickhouse_migrations.py
uv run ruff check src/dagster_v3/components/webtech_scanner_component.py \
  src/dagster_v3/defs/webtech tests/test_webtech_pilot.py
uv run dg check defs
```

Before launching all 128 production partitions, the first partition
materialization must confirm:

- all four assets materialize and the final/index counts agree;
- the `commoncrawl_webtech_remote_scan` partition stays `STARTED` and writes a
  current status line to its Dagster run log every two seconds;
- restart and cancellation leave resumable per-domain objects;
- timeout stages, success rate, and empty-success rate are acceptable;
- workstation CPU and memory remain bounded;
- observed throughput supports the projected production duration.

## Real-host smoke results

Dagster run `21417131-86ce-44a6-8c54-9e4e13235878` first validated the complete
three-asset chain with ten domains. Remote scan
`53e2992266a4c9ee8d19e7782b5eb317` stored 10/10 domain objects, produced nine
successes and one Wappalyzer-stage hard timeout with 21 technologies in 64.086
seconds, wrote the final manifest, and indexed exactly ten ClickHouse rows.

That smoke exposed two deployment-contract details which were corrected before
the final validation:

- candidate manifests now include and are keyed by Dagster run ID, so retries
  inside one run are idempotent while a later retry run receives a new remote
  scan ID;
- the systemd unit executes the virtualenv `uvicorn` binary directly instead of
  snap-packaged `uv`, keeping Chromium inside `webtech.service` for reliable
  teardown and resource accounting.

The corrected Dagster run `7d3e43f1-a726-43b8-a6ad-4531e30b052a` scanned three
domains under remote scan `0c2d5a5555be9729fe7d0f136cf9531e`. All three
succeeded with two technologies in 14.2 seconds; the final manifest and exactly
three ClickHouse rows agreed. The service cgroup peaked at 1,258,070,016 bytes,
used 25.98 CPU-seconds, returned to about 74 MB after completion, and retained no
browser profile directories or Webtech Chromium processes.

After production partitioning replaced the pilot partition, candidate-only
Dagster run `356e2f79-8524-403a-b0f2-ad3878a0d7b8` materialized `hash_000`
without selecting the remote-scan asset. The live Common Crawl snapshot contained
exactly 1,000,000 ranked domains across all 128 hash remainders; partition sizes
ranged from 7,609 to 8,069. `hash_000` wrote a 438,382-byte schema-v2 JSON
manifest with 7,766 candidates, spanning harmonic ranks 70 through 999,784. At
that materialization time no `hash_000` domain had a current-detector result in
the prior month, so the freshness predicate skipped zero rows in that partition.

The first production `hash_003` scan demonstrated the recovery requirement.
Scanner job `a1c846872118b608f7e9a38e886b76c1` completed all 7,915 domains and
wrote its final manifest, but the Dagster supervisor was stopped during
execution and killed the run worker at 5,640/7,915. The remote work continued
correctly. The current design intentionally restores a lightweight active
Dagster polling step to expose native `STARTED` state, but it never holds an HTTP
connection open. If that worker is interrupted, the existing submission and
per-domain RustFS objects remain durable and `commoncrawl_webtech_finalize_job`
reattaches to the same scan.
