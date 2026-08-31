# Web technology scanner

This uv project is the canonical home of the CloakBrowser scanner and packaged
`mywappalyzer` extension. The embedded application-detection catalog is from
Wappalyzer 6.12.6; the custom scanner integration that wraps it is extension
version 1.4.1. It exposes a small authenticated API for Dagster and retains the
local benchmark CLI for isolated experiments.

## Remote scanner API

Copy `.env.example` to `.env`, provide the API token and RustFS settings, then
start the service:

```bash
uv sync --all-groups
uv run uvicorn service:create_app --factory --host 0.0.0.0 --port 8088
```

The API contract is intentionally small:

- `GET /healthz`
- `POST /v1/scans`
- `GET /v1/scans/{scan_id}?after_event=0&wait_seconds=30`
- `POST /v1/scans/{scan_id}/cancel`

All `/v1` routes require `Authorization: Bearer $WEBTECH_API_TOKEN`. Dagster
writes the candidate manifest to RustFS and submits its URI plus SHA-256. The
service derives an idempotent scan ID from that content and its scanner settings,
stores each terminal domain result before reusing its worker slot, emits one
progress event per 20 stored results, and writes `final-manifest.json` last.

The service writes lifecycle logs for scan acceptance, start, every 20-result
progress window, periodic heartbeat, stalled progress, completion, failure, and
cancellation. `/healthz` reports the active scan ID, completed/total counts,
seconds since the last stored result, and current throughput; service activity
does not need to be inferred from CPU utilization.

Only one scan can be active because one workstation owns the configured browser
capacity. Dagster submits and exits, then its sensor performs a zero-wait status
request every 30 seconds. If the process restarts, the sensor resubmits the same request; the
service reconstructs already completed domains from RustFS and scans the missing
ones. There is no service database, message queue, or intermediate batch
checkpoint.

The checked-in user systemd unit is `deploy/webtech.service`. It runs 20 fresh,
headless one-page contexts by default, with a 60-second per-domain deadline and
250 ms launch stagger. Those capacity values belong to the scanner `.env`, not
Dagster run configuration.

Production deployment is managed by `ansible/site.yml`. The playbook preserves
the server-owned `.env` and runtime directories, refuses to deploy during an
active scan, restarts only when deployment content changes, and verifies the
service health endpoint. See `ansible/README.md` for prerequisites and commands.

## Isolated benchmark CLI

The CLI can run one or more independent CloakBrowser contexts, with a configurable
number of pages in each context. Each domain has one hard timeout covering page
creation, navigation, extension analysis, and result collection. As soon as a
page succeeds, fails, or times out, its JSON result is written to
`output/<run>/reports/`, the page is closed, and the next domain is opened. There
is no batch barrier.

`output/<run>/summary.json` records success and failure counts, failure details,
technology counts, wall time, and throughput. An interrupted run also writes a
partial summary. Page and browser teardown are separately bounded so a stuck
Chromium renderer cannot consume a worker or prevent the benchmark from exiting.
Hard timeouts record whether they expired during page creation, HTTPS or HTTP
navigation, extension-token injection, or Wappalyzer report collection.

The packaged extension emits schema-version 3 terminal reports. A normal scan is
`complete`; a content-script exception is `failed`; and a fixed 30-second
extension watchdog produces `partial` if analysis stops making progress. Partial
and failed reports retain any technologies detected before the failure and are
stored as `extension_error` outcomes. Each report includes elapsed timestamps for
the metadata, initial DOM/JavaScript, heavy-signal, delayed-pass, finalization,
and callback stages. The watchdog is fixed rather than reset by later XHR events,
and each XHR hostname is analyzed only once per page.

Run the checked-in harmonic top-200 input:

```bash
uv sync
uv run python main.py --domains-file domains-harmonic-top-200.txt --headless
```

Run every domain sequentially in a completely new browser profile and context:

```bash
uv run python main.py \
  --domains-file domains-harmonic-top-200.txt \
  --browser-count 1 \
  --pages-per-browser 1 \
  --domain-timeout-seconds 30 \
  --domains-per-context 1 \
  --headed
```

Run the first 100 domains through ten reusable contexts, one page per context:

```bash
uv run python main.py \
  --domains-file domains-harmonic-top-200.txt \
  --limit 100 \
  --browser-count 10 \
  --pages-per-browser 1 \
  --domain-timeout-seconds 40 \
  --headed
```

Run the same domains through ten parallel contexts, replacing each context and
profile after every domain:

```bash
uv run python main.py \
  --domains-file domains-harmonic-top-200.txt \
  --limit 100 \
  --browser-count 10 \
  --pages-per-browser 1 \
  --domain-timeout-seconds 40 \
  --domains-per-context 1 \
  --headed
```

Run the first 100 domains as ten batches of ten concurrent pages, replacing the
browser profile and context after every batch:

```bash
uv run python main.py \
  --domains-file domains-harmonic-top-200.txt \
  --limit 100 \
  --browser-count 1 \
  --pages-per-browser 10 \
  --domain-timeout-seconds 20 \
  --domains-per-context 10 \
  --headed
```

The input format is one domain per line. Blank lines and lines beginning with
`#` are ignored. Positional domains can also be scanned directly:

```bash
uv run python main.py example.com github.com
```

Useful options:

```text
--browser-count 1..20
--pages-per-browser 1..30
--domain-timeout-seconds SECONDS
--domains-per-context DOMAINS
--context-launch-interval-seconds SECONDS
--limit DOMAINS
--output-dir ./output
--headless / --headed
```

## Top-1,000 extension lifecycle benchmark

The controlled 2026-08-30 comparison used the same 32-vCPU host, harmonic
top-1,000 input, 20 headless fresh contexts, one domain per context, 250 ms
launch stagger, local dnsmasq, and 40-second domain deadline.

| Metric | Extension 1.3.0 | Extension 1.4.0 |
| --- | ---: | ---: |
| Complete successes | 635 | 812 |
| Hard timeouts | 315 | 124 |
| Staged extension errors | 0 | 10 |
| Navigation errors | 49 | 52 |
| Browser errors | 1 | 2 |
| Successful technology detections | 6,438 | 8,825 |
| Wall time | 1,244.458 s | 1,157.780 s |
| Throughput | 48.21/min | 51.82/min |
| CPU time | 24,084.909 s | 26,409.732 s |
| Average cores | 19.35 | 22.81 |
| Peak memory | 11.66 GiB | 11.90 GiB |

The exact domain comparison moved 215 previous hard timeouts to success, while
33 previous successes timed out in the new run; 90 domains timed out in both.
This confirms that fixed finalization and stopping post-finalization request
analysis remove a large source of nondeterministic report loss. Complete reports
entered finalization at a median 10.740 seconds and p95 22.162 seconds.

Of the 124 remaining hard timeouts, 111 were waiting for a Wappalyzer report and
13 were in navigation. A forced Amazon smoke showed why a same-thread watchdog
cannot eliminate all report timeouts: a synchronous regular-expression analysis
can occupy the extension service worker beyond the outer deadline, preventing
both the callback and watchdog timer from running. The next optimization should
therefore reduce or isolate regex work, using conservative literal/Aho-Corasick
prefiltering with normal JavaScript regex validation, cooperative chunks, or a
terminable worker boundary.
