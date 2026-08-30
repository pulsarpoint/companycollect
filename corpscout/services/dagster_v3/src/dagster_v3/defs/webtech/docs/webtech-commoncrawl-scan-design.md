# Common Crawl web-technology scan

**Status:** batch-isolated 1,000-domain iteration implemented and validated on the real Dagster host
on 2026-08-29.

## Objective and first-iteration boundary

The `webtech` Dagster definition scans the harmonic top 1,000 domains from one explicit Common
Crawl ranking snapshot. It renders each site with CloakBrowser and the packaged custom Wappalyzer
extension, stores the complete JSON outcome in RustFS, and indexes the outcome in ClickHouse.

This iteration deliberately stops at 1,000. `WebtechScanConfig.max_harmonic_rank` has a hard
validation ceiling of 1,000, so an accidental million-domain launch cannot happen by changing UI
config. Raising that ceiling is a code change made only after reviewing this pilot.

The implementation is entirely under:

```text
src/dagster_v3/defs/webtech/
├── __init__.py
├── assets.py
├── definitions.py
├── models.py
├── scanner.py
├── storage.py
├── extension/
│   ├── manifest.json
│   ├── categories.json
│   ├── js/
│   └── technologies/
└── docs/
    └── webtech-commoncrawl-scan-design.md
```

There is no extra API service, dlt pipeline, DuckDB database, component wrapper, schedule, sensor,
or scanner interface. The old POC service remains only as a local reference until a separate cleanup.

## Dagster model

There is one asset and one manually launched job:

- asset: `commoncrawl_webtech_scan_results`;
- job: `commoncrawl_webtech_scan_job`;
- group: `webtech`;
- partitions: one static pilot partition, `harmonic_top_1000`;
- backfill policy: `multi_run(max_partitions_per_run=1)`;
- concurrency pool: `webtech_cloakbrowser_session`.

The single pilot partition selects all domains from
`corpscout.commoncrawl_domain_graph_signals FINAL`, pins an explicit `crawl_id`, restricts ranks to
`1..max_harmonic_rank`, and orders them by harmonic rank. A successful row for the same crawl
and detector is skipped unless `force_rescan=true`; failed domains remain eligible on a retry.

The live pilot is pinned to:

```text
crawl_id=CC-MAIN-2026-apr-may-jun
max_harmonic_rank=1000
```

The source contains exactly 1,000 rows in that rank range, so one materialization exercises 100
sequential browser batches of 10 concurrent pages.

## Browser concurrency

The Dagster partition run splits candidates into batches of `page_worker_count`, which defaults to
10. Every batch owns a fresh temporary profile and persistent CloakBrowser `BrowserContext`. The
batch opens one Page per domain, analyzes all pages concurrently, and closes the complete context
before starting the next batch. Pages and extension service-worker state are never reused across
batches.

The `webtech_cloakbrowser_session` pool remains set to 1 so an operator cannot overlap two pilot
runs. Contexts are also not shared across batches or Dagster runs; sharing them would reintroduce the
cross-domain browser state that invalidated the first sustained pilot.

A worker starts with `https://<root_domain>`. A navigation-level failure gets one
`http://<root_domain>` attempt on a fresh Page. Server redirects are allowed and the result retains
both candidate root domain and final URL/hostname. The fallback replaces only that candidate's Page
inside its batch context. Every candidate Page is closed after its terminal result.

The real-host 10-page smoke test showed why the default extension report timeout is 120 seconds:
10 simultaneous DOM/JavaScript analysis passes took up to 73 seconds even though the same extension
completed in 8–11 seconds in a two-page smoke test. The 120-second run completed 10/10 domains;
60-second runs produced nondeterministic false callback timeouts on the busy host.

## Extension callback

The source extension has no credentials and no fixed port. For every partition run the scanner:

1. binds a standard-library callback server to `127.0.0.1:0`;
2. creates a temporary extension copy;
3. writes its random-token callback URL to `runtime-config.json` in that copy;
4. takes the next batch of at most `page_worker_count` candidates;
5. creates a fresh temporary profile and context with only that extension copy;
6. opens one Page per candidate and analyzes the batch concurrently;
7. closes every Page, context, and profile before taking the next batch;
8. closes the callback server and extension copy after the final batch.

The content script assigns every top-level document a UUID. The background service worker keeps
detection state by Chrome tab ID and includes that UUID in the final callback. Python routes callbacks
by UUID rather than URL, so two candidate domains may redirect to the same final URL without crossing
reports. The router also buffers a valid report that arrives before navigation returns and Python
registers its UUID waiter; without that buffer, a slow `DOMContentLoaded` could turn an already
completed extension analysis into a false callback timeout.

Page-context JS/DOM injection has a five-second timeout and treats CSP/load failures as empty signal
sets. Header, cookie, URL, and any other completed detections are still retained, and the content
script always advances to `analysisComplete` instead of hanging forever on one blocked injected
script.

The accepted extension contract is:

```json
{
  "schema_version": 2,
  "analysis_complete": true,
  "extension_version": "1.3.0",
  "page_token": "f2a8b975-18df-465d-89ef-0f09ca1621ec",
  "url": "https://example.com/",
  "technologies": []
}
```

Only schema 2, extension 1.3.0, `analysis_complete=true`, and a registered UUID are accepted. An empty
technology array is a successful completed analysis, not an error. The delayed extension JavaScript
and DOM pass remains unchanged.

## Persistence

`WEBTECH_S3_PATH` is required in the Dagster environment. The local pilot uses:

```text
WEBTECH_S3_PATH=s3://webtech/wappalyzer
```

Each attempt is deterministically stored at:

```text
s3://webtech/wappalyzer/
  crawl_id=<crawl-id>/
  root_domain=<root-domain>/
  report.json
```

The envelope records schema/detector versions, crawl, partition, Dagster run, candidate domain and
rank, outcome, requested/final URL, fallback flag, timestamps, duration, safe error text, and the full
extension report. JSON is deterministic and its SHA-256 and byte size are indexed.

The migration-owned table is `corpscout.webtech_domain_scan_results` (migration 352). It is a
`ReplacingMergeTree(recorded_at)` ordered by `(crawl_id, root_domain, detector_version)`. It stores
selection, outcome, timing, technology count, and exact RustFS pointer/checksum fields. A forced
rescan replaces the previous logical row and the deterministic RustFS object.

For the pilot partition, all RustFS objects are written before the ClickHouse insert batch.
An object-store failure therefore produces no misleading index row. Moving beyond 1,000 domains must
first change this to streaming bounded persistence so a browser/context failure cannot lose a large
partition's completed in-memory results.

## Operator config

| setting | first-iteration value |
|---|---:|
| `crawl_id` | required explicit ID |
| `max_harmonic_rank` | default and hard maximum `1000` |
| `force_rescan` | `false` |
| `headless` | `true` |
| `navigation_timeout_seconds` | `60` per protocol attempt |
| `report_timeout_seconds` | `120` after the page UUID appears |
| `page_worker_count` | context batch size and concurrency: `10`, accepted range `1..20` |

The materialization records scan wall time, domains per minute, and end-to-end asset wall time. For
the real-host pilot, browser-inclusive CPU and peak-memory figures come from the isolated transient
systemd cgroup around the Dagster launch rather than instrumentation inside the asset.

The backfill is launched through the Dagster UI/daemon, not `dagster job backfill`. On the instance:

```bash
uv run dagster instance concurrency set webtech_cloakbrowser_session 1
```

The earlier 128-partition local pilot backfill `9eb352d23f3445cb898c0f5e62462e26`
was canceled when the pilot changed to one partition.

## Verification and observed results

The package-equivalence smoke test used two concurrent pages:

- `example.com`: success, Cloudflare, 8.0 seconds;
- `handelsbanken.se`: success, 10.6 seconds, with the same six technologies as the original POC:
  Adobe Client Data Layer, Adobe Experience Platform Identity Service, Adobe Experience Platform
  Launch, Bootstrap, Priority Hints, and React.

The real Dagster host then completed a forced top-10 smoke materialization with one context and 10
pages: 10 successful reports, no failures, 29 technology detections, and 75.5 seconds of scan wall
time. Its launch cgroup, including Dagster definition startup and Chromium, used 209.4 CPU-seconds
and peaked at 2.10 GiB RAM over roughly 184 seconds of wall time.

The first sustained top-1,000 attempt reused one context and its Pages across domains. It was stopped
as invalid after 294 terminal outcomes: 52 successes, 15 navigation errors, and 227 report timeouts.
The first 50 completions had 42 successes, but the final success was completion 90; every completed
analysis afterward either timed out or failed navigation. Chromium grew to 45 renderer processes and
the launch cgroup consumed 11,050.9 CPU-seconds with an 8.49 GiB peak. This proved that a successful
10-page smoke test did not justify long-lived context reuse. Because the asset was canceled before
its persistence boundary, this attempt did not publish partial RustFS or ClickHouse results.

The replacement real-host validation scanned ranks 1–30 as three sequential 10-domain contexts.
Every context produced successful callbacks. The Dagster run materialized all 30 outcomes to RustFS
and ClickHouse: 22 successes, 3 navigation errors, 5 report timeouts, and 127 technology detections
in 401.6 seconds of scan time. Its full launch cgroup used 717.1 CPU-seconds and peaked at 3.11 GiB
RAM, versus the abandoned reusable-context run's 8.49 GiB peak. The materialization records the
number of browser contexts so future 1,000-domain runs can verify the expected count of 100.

The completed batch-isolated top-1,000 pilot was Dagster run
`f986219f-d173-4747-9477-6b30d37b31ce`. It materialized successfully after selecting and storing all
1,000 domains in 100 fresh browser contexts. The final outcomes were 472 successful reports (47.2%),
466 extension report timeouts (46.6%), 54 navigation errors (5.4%), and 8 browser errors (0.8%). Ten
successful reports contained an empty technology array. Successful reports contained 6,329 total
technology detections. Median and p95 terminal durations were 103.9 and 130.7 seconds for successful
reports, 127.1 and 141.0 seconds for report timeouts, 0.8 and 120.3 seconds for navigation errors, and
65.1 and 181.8 seconds for browser errors.

Scan wall time was 13,469.18 seconds (3h44m29s), or 4.45 domains per minute. The isolated transient
systemd unit ran for 3h46m45s, consumed 34,617.8 CPU-seconds (9.62 CPU-hours), and peaked at
6,625,103,872 bytes (6.17 GiB) RAM. Memory repeatedly returned below the peak after contexts closed,
so the batch-isolated lifecycle bounded memory. Callback success was nevertheless strongly
wave-shaped: one batch could time out 10/10 and the immediately following batch succeed 10/10. This,
together with the host CPU-pressure samples and isolated lower-concurrency reproductions, identifies
CPU contention around the shared extension service worker as the dominant cause of false report
timeouts.

The pilot therefore does not pass the gate for a million-domain run or for increasing one context to
20 pages. The next iteration should isolate browser execution on a dedicated scanner host and scale
through multiple browser processes/contexts with low per-context page concurrency. Dagster should
submit partition work to that service, long-poll compact progress, and materialize only after the
service has stored all per-domain outcomes plus a final manifest in RustFS.

The first completed backfill partition (`bucket_000`) attempted 7 domains: 6 successful reports,
1 navigation failure, 77 total technology detections, and 13.3 seconds average duration among the
stored outcomes. The 17-domain concurrency/debug partition completed successfully as a Dagster asset
and proved RustFS plus ClickHouse persistence.

Required checks are:

```bash
uv run pytest tests/test_webtech_pilot.py tests/test_clickhouse_migrations.py -q
uv run ruff check src/dagster_v3/defs/webtech tests/test_webtech_pilot.py
node --check src/dagster_v3/defs/webtech/extension/js/background.js
node --check src/dagster_v3/defs/webtech/extension/js/content.js
uv run dg check defs
```

The wheel must contain the extension manifest, background script, and technology JSON files. The
verified wheel does.

## Gate before increasing scope

Do not raise the rank ceiling or make 20 pages the default until the completed 1,000-domain pilot is
reviewed for:

- success, navigation-error, browser-error, and callback-timeout rates;
- median and tail duration under 10 pages;
- empty-success rate and technology distribution;
- CloakBrowser CPU and memory behavior;
- RustFS and ClickHouse errors;
- retry results for unsuccessful domains.

Before a million-domain pass, implement streaming per-domain persistence, bounded retry policy,
whole-context crash recovery, progress-rate metrics, and a deliberate history/overwrite decision for
forced rescans.
