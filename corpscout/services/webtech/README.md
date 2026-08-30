# Web technology isolation benchmark

This uv project runs the packaged `mywappalyzer` extension entirely on the local
machine. It does not use Dagster, ClickHouse, RustFS, S3, or `.env` settings.

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
