# Crawl service

The service accepts work through REST and the CLI, the same transport model as the
[browser service](../browser_service/README.md). Both call
`crawler_service.crawl.crawl_company`. Results remain available locally, and service
requests can additionally deliver to S3. The NATS JetStream input and its
completion-event stream were removed on 2026-09-22. This service collects pages and optionally describes the site; the
later LLM fact-analysis module remains separate.

With `CRAWL_CHALLENGE_AGENT_ENABLED=true`, REST website CAPTCHA
failures invoke the browser service's selected agent once per requested URL, with
a default of three runs per crawl attempt (`CRAWL_CHALLENGE_AGENT_MAX_RUNS`),
with request overrides and doubled budgets on retries after exhaustion.
The crawler checks actual access afterward and continues automatically on success.
This is enabled in the server's Ansible configuration; see
[automatic CAPTCHA assistance](HUMAN_ASSISTANCE.md#automatic-captcha-assistance).

Default discovery collects contacts, jobs and their descriptions, company/about
information, and financial-information links. Technology inference, tracker/resource
inventory and job interpretation are deferred. Custom `instructions` replace the
default selection request; explicit page lists and `site_info` retain their behavior.
`crawler-service-crawl` calls this collection flow.
See [scope and preserved source data](CRAWL_AND_ANALYZE.md#current-collection-scope).

[ClickHouse storage and queries](CLICKHOUSE.md) provide website-keyed history,
separate JSON section columns, and direct SQL reads of uploaded S3 bundles.
Importing into the stored table is currently explicit.

For a native systemd installation on `192.168.88.132`, use the
[Ansible deployment](ansible/README.md). It preserves server-local results under
`/var/lib/company-research/results` across application releases.

## Install and configure

Run these commands from `companycollect/corpscout/services/crawler_service`:

```bash
uv sync --extra service
cp .env.example .env
```

Fill in the needed keys in `.env`, or set environment variables. The package has
its own `pyproject.toml` and `uv.lock`; no sibling experiment package or its virtual
environment is required. Python 3.12+ is required. Browsers run in the independent
[browser service](../browser_service/README.md); set `BROWSER_API_URL` and
`BROWSER_API_TOKEN`. The crawler requires no display or browser runtime.

`DEEPSEEK` is used by the default direct DeepSeek API. `OPENROUTER_API_KEY` is used
when a request specifies `"api": "openrouter"`. Exact page lists without
instructions or site-info need neither key. Credentials belong to the service
environment; request bodies cannot supply credentials or output paths.

## REST

```bash
uv run --extra service crawler-service \
  --transport rest --output-dir ./data/crawl-service --env-file .env
```

The default address is `http://127.0.0.1:8080`; interactive API documentation is at
`/docs`. Submit a request, then poll its status and retrieve its result:

```bash
curl -sS http://127.0.0.1:8080/v1/crawls \
  -H 'Content-Type: application/json' --data-binary @examples/pages-job.json
curl -sS http://127.0.0.1:8080/v1/crawls/novelic-pages-001
curl -sS http://127.0.0.1:8080/v1/crawls/novelic-pages-001/result
```

| Endpoint | Behavior |
| --- | --- |
| `POST /v1/crawls` | Persists the request and returns `202` with job status and a `Location` header. |
| `GET /v1/crawls/{request_id}` | Returns current job status. Unknown IDs return `404`. |
| `GET /v1/crawls/{request_id}/result` | Streams saved JSON; returns `409` while pending. |
| `GET /healthz` | Returns `200` when the workers are ready; otherwise `503`. |

Set `CRAWL_API_TOKEN` to require `Authorization: Bearer <token>` on submission,
status and result endpoints. The launcher requires a token when `--host` binds
beyond localhost. This prototype is for trusted callers: it fetches caller-supplied
URLs and does not provide network isolation or a public multi-tenant URL policy.
TLS termination can be provided by the deployment's reverse proxy.

The job state is `queued`, `running`, `blocked`, `captcha`, `awaiting_human`,
`completed`, `failed`, or `cancelled`. A failed crawl produces a `failed` job even
when it has a saved result. For completed jobs, inspect `crawl_status` for
`finished`, `partial`, `skip_crawling`, or `needs_review`. A service-level execution
exception produces a failed job and a local error JSON file. See
[human assistance](HUMAN_ASSISTANCE.md) for live status, filtering and manual retries.

## Request format

REST uses this JSON object:

For automatic discovery across contacts, jobs, company information and financial
links, submit `{"url": "https://www.novelic.com/", "crawl": "full"}`.
This defaults to 100 pages and 30 external pages; `config` can override the limits.
Full mode also enables Brave web search (three queries by default). Disable it
with `"config": {"web_search": false}`. Targeted instruction requests enable it
with `"config": {"web_search": true}`. Page lists and site-info-only requests never
search. Parent/filing navigation defaults to three source domains, four pages per
source and depth three; override `max_source_domains`,
`max_source_pages_per_domain`, `max_source_depth`, `max_search_queries` or
`search_results_per_query` in `config`. These limits apply identically to CLI
and REST. See [source discovery](CRAWL_AND_ANALYZE.md#source-discovery)
for evidence requirements, search failures and the retained JSON provenance.
Do not combine `"crawl": "full"` with `pages` or `instructions`. The CLI equivalent
is `crawler-service-crawl https://www.novelic.com/ --crawl full --output-dir runs/novelic`.
Results include links, cleaned HTML, deterministic observations, elapsed time and
LLM navigation usage. See [full-crawl behavior and limits](CRAWL_AND_ANALYZE.md#full-crawl).

```json
{
  "request_id": "novelic-jobs-001",
  "url": "https://www.novelic.com/",
  "instructions": "Collect current job listings and full job descriptions. Skip employee stories and other employers.",
  "site_info": true,
  "config": {
    "max_pages": 30,
    "max_external_pages": 20,
    "max_model_calls": 60
  }
}
```

- `url` is required. `pages` optionally restricts the crawl to a list; without
  instructions every supplied page is fetched.
- `instructions` controls page selection. Remote callers send the text itself;
  the CLI also supports `--instructions-file`.
- `save_artifacts` defaults to `true` during development. Set it to `false` to
  retain only the bundled `result.json` in the crawl attempt directory. CLI:
  `--no-save-artifacts` (or `--save-artifacts` to enable). Saved job/request state
  remains necessary for restart recovery and deduplication. Reusing an ID with a
  different retention setting is a request conflict.
- `site_info: true` alone describes the input page and stops. Combine it with
  `pages`, `instructions`, or `crawl: true` to continue crawling.
- `api` defaults to `deepseek`. Optional `config` fields override `ResearchConfig`
  limits/model settings. Omitted fields retain the selected API's crawl defaults.
  For example, OpenRouter GLM selection uses `"api": "openrouter"` and
  `"config": {"model": "z-ai/glm-5.3", "provider": "parasail/fp8", "reasoning_effort": "high"}`.
- `request_id` may contain letters, digits, `_` and `-` (maximum 128 characters).
  REST generates one if omitted. Explicit IDs are recommended for client retries.

Reuse an ID with the same normalized request to retrieve/reuse its existing job.
A different request under that ID is a REST `409`.
Use a new ID when intentionally requesting a new crawl, including after a completed
partial/failed crawl. Examples for all modes are in `examples/`.

## Existing command line

Existing flags and JSON stdout remain available:

```bash
uv run crawler-service-crawl https://www.novelic.com/ \
  --pages https://www.novelic.com/careers/ --max-pages 1 \
  --output-dir ./data/cli-novelic

uv run crawler-service-crawl https://www.novelic.com/ \
  --site-info --output-dir ./data/cli-novelic-info --env-file .env
```

The CLI writes directly to its specified empty output directory. The service uses
job subdirectories below the service's output directory. Each route now also
writes `result.json`; CLI stdout keeps its existing crawl-manifest shape.

## Submitting from Backoffice

Callers submit with authenticated `POST /v1/crawls`. `POST /v1/crawls/validate` uses
the same `CrawlRequest` validation and returns a normalized payload without
enqueueing work or allocating a browser. Excess requests beyond `--max-pending`
are rejected rather than buffered by a broker.

## S3 results and completion events

Set `CRAWL_S3_BUCKET` in the service environment, or pass `--s3-bucket`, to enable
S3 delivery for REST and manual service requests. The standalone crawl
CLI keeps local output. With no bucket configured, local-only service operation
remains available when human assistance is disabled. Human assistance requires S3.
Example for the existing RustFS installation:

```dotenv
CRAWL_S3_BUCKET=crawls
CRAWL_S3_ENDPOINT_URL=http://rustfs:9000
CRAWL_S3_PREFIX=company-crawls
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=replace-me
AWS_SECRET_ACCESS_KEY=replace-me
```

```bash
uv run --extra service crawler-service \
  --output-dir ./data/crawl-service --env-file .env
```

The bucket must already exist. Provision read/head and conditional put access to
the selected prefix. Use `--s3-endpoint-url`, `--s3-prefix`, and `--s3-region` to
override environment settings. Leave the endpoint empty for AWS S3. Credentials
come from the environment file or the normal AWS credential provider chain;
temporary credentials can include `AWS_SESSION_TOKEN`. No credentials or bucket
choices are accepted in crawl requests, and objects are not made public.

The delivery order is:

1. Save the crawl result locally.
2. Upload `<prefix>/<request_id>/attempts/0001/result.json.gz`. This is the same portable JSON,
   including the manifest, site description and collected HTML.
3. If `save_artifacts` is true and additional files exist, upload
   `<prefix>/<request_id>/attempts/0001/artifacts.tar.gz` with the attempt's diagnostics
   and separate captures. Failed and cancelled attempts retain available diagnostics
   regardless of `save_artifacts`. The attempt number changes on execution retries.
4. Record the upload state and completion event on the job. REST and manual attempts
   expose them through the status API and SQLite history.

A completion event has this shape (hashes shortened here):

```json
{
  "schema_version": "company-crawl-event/1.0",
  "event_id": "crawl-...",
  "request_id": "novelic-jobs-001",
  "state": "completed",
  "crawl_status": "finished",
  "finished_at": "2026-09-17T20:00:00+00:00",
  "page_count": 16,
  "error": null,
  "result": {
    "bucket": "crawls",
    "key": "company-crawls/novelic-jobs-001/attempts/0001/result.json.gz",
    "sha256": "...",
    "bytes": 12345,
    "content_type": "application/json",
    "content_encoding": "gzip"
  },
  "artifacts": null
}
```

Each object descriptor's `sha256` and `bytes` describe the **compressed object
bytes**. A `version_id` is included when the store returns one. Download with an
S3 client configured for the same endpoint, verify the hash, decompress with gzip,
and read the JSON. The next LLM module can use that downloaded JSON as its input.
An artifact descriptor, when present, uses `application/gzip` without HTTP content
encoding because its payload is a gzip archive.

Every valid terminal job emits an event, including partial, skipped, review-needed
and failed crawls. A service execution failure uploads its safe error JSON in the
attempt's `result.json.gz` slot and emits `state: failed`. Failed crawls also emit
`state: failed`; inspect `crawl_status` for the detailed crawl outcome. Invalid/conflicting
requests emit `state: rejected`, a safe error code, the input stream and message
sequence, and `result: null` before termination. Rejections omit the untrusted
request ID and raw input; use the publisher's stream/sequence receipt to correlate.

Uploads use deterministic gzip bytes and conditional creation
([S3 conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)).
An existing object is accepted only when its stored request/content hashes, size
and content headers match. A different object is never overwritten. The selected
S3-compatible server must support `If-None-Match: *`. Reported storage errors leave
delivery pending; the service never retries with unconditional writes.

Upload failure leaves delivery pending. A restart resumes delivery from the local
result without recrawling; a crash mid-delivery may repeat a conditional upload.
Consumers should deduplicate on the stable `event_id`. This is at-least-once
delivery, not an end-to-end exactly-once guarantee.

Keep local state and use stable request IDs across publisher retries. Local results
are retained even after upload during development. Do not change the destination
of an existing outbox; use a new output directory and new request IDs for a new
destination. `delivery.json` records whether publication completed; the ordinary
job state describes crawl completion, which may precede S3/event delivery. Remote
object deletion or result-stream expiry after acknowledgement is not automatically
repaired. Set storage and event retention to cover downstream processing delays.

## Local output and recovery

The layout below shows the development default, `save_artifacts: true`. With
`false`, each completed attempt retains just `result.json`; `request.json` and
`job.json` still track the service job. Temporary processing files are cleaned up
when the crawl exits. See [retention behavior](CRAWL_AND_ANALYZE.md#artifact-retention).

```text
data/crawl-service/
  jobs/novelic-jobs-001/
    request.json
    job.json
    delivery.json               # S3 outbox and completion publication receipt
    attempts/0001/
      result.json
      crawl-manifest.json
      site-info.json             # when requested or the site is skipped/uncertain
      pages/p0001/page.html
      pages/p0001/input.json
      ...                       # existing crawl diagnostics and captures
```

`result.json` is a portable document with schema `company-crawl-result/1.2`:

```json
{
  "schema_version": "company-crawl-result/1.2",
  "crawl": {"status": "finished", "artifacts_saved": false, "site_info": null, "pages": []},
  "documents": [
    {"page_id": "p0001", "url": "https://example.com/", "html": "<h1>Example</h1>", "html_sha256": "...", "input": {}, "rendered_html": null}
  ]
}
```

This shortened example omits most manifest fields and the document's input
metadata. Actual JSON includes the complete manifest, cleaned and optional rendered
HTML, page metadata, target URL, headings, observed links and
[deterministic observations](PAGE_OBSERVATIONS.md) in `documents[].input.observations`.
Both retention modes can be analyzed with
`crawler-service-pages --crawl <attempt-directory>/result.json`.
Separate HTML captures are also accepted when retained.
Older 1.1 bundles remain accepted for analysis; observations were not collected
in those runs. No new request option is needed for observation collection.

JSON publication uses atomic replacement. Restarting the service recovers queued
or interrupted jobs from disk. Interrupted work gets a new attempt directory;
completed captures are preserved. A result saved just before a process crash is
adopted without recrawling. Interrupted work before result publication may repeat
HTTP/model requests; this is not exactly-once execution.

The initial implementation runs **one process per local output directory**,
enforced by a filesystem lock (Linux/macOS). `--concurrency` defaults to 1; `--max-pending` defaults
to 100 and counts all nonterminal jobs in each queue. Interactive retries have a
separate queue with `CRAWL_MANUAL_CONCURRENCY` workers (default 2), so a paused manual attempt does not leave other profiles idle.
The browser API still limits all inputs to one global browser capacity; busy claims retry
without creating a browser-side queue. Normal REST execution remains bounded by
`--concurrency`. Multiple independent hosts with
separate local disks do not share deduplication state. Keep the output directory
when restarting; copying only application code does not preserve completed jobs.

Crawler worker concurrency and browser capacity are configured independently.
Manual attempts share the same browser capacity, even though they have a separate
crawler queue.

## Validation

The JetStream input was removed on 2026-09-22; the records below that mention it
are historical.

The [five-request parallel validation](JETSTREAM_PARALLEL_VALIDATION_20260919.md)
records a live Backoffice → JetStream → browser → S3 → ClickHouse test with two
workers, including browser release after CAPTCHA failures.

```bash
uv run --extra service python -m unittest discover -s tests
```

Version 0.25 includes S3 HTTP boundary tests with the actual boto3 client and
isolated JetStream integration tests. They cover conditional object writes,
conflicts, slow uploads with progress acknowledgements, upload failures, restart
between upload and publication, lost publication acknowledgements, failed local
publication receipts, safe rejection/error events and request redelivery.
The complete Python 3.14 suite passed: 226 tests, with seven existing/optional
skips. Ruff, changed-module type checks, Ansible syntax/lint and the locked
deployment wheel/dependency build passed.
All 33 service tests also passed on Python 3.12.12, the deployment's Python version.

The [live RustFS/NOVELIC receipt](data/s3-delivery-20260917/summary.json) records
two real Careers crawls with artifacts disabled/enabled. Both captured 79,294 HTML
characters with zero LLM calls. Downloaded JSON passed hash and replay validation;
conditional reuploads returned the original objects, duplicate requests reused
the results, and each request produced one completion event with zero pending
acknowledgements. An isolated local broker was used; test objects are retained
under the unique prefix recorded in that receipt.

The service tests cover REST authentication and validation, capacity, result
retrieval, request conflicts, restart recovery, partial API configuration, site-info
and custom instructions, CLI stdout compatibility, and local error results.
The JetStream tests launch isolated brokers and cover progress acknowledgements,
lost-ack redelivery, duplicate suppression, poison messages, failed disk writes,
stable generated IDs and durable restart.

Validation passed on Python 3.12 (206 package tests, six existing skips), and in a
fresh package-local Python 3.14 installation with the service extra (202 tests,
seven skips, including the uninstalled optional catalog MCP module). All five real
JetStream tests and the three selector-benchmark tests passed. Runtime type checks
passed in the full development environment; all changed modules also passed in
the service-only environment. Ruff and lockfile checks passed.

The [live NOVELIC smoke-test receipt](data/service-smoke-20260917/final/summary.json)
records actual CLI, REST and JetStream command processes, all using the real
browser. Each captured Careers into local JSON with 79,294 HTML characters and
zero model calls; the JetStream consumer ended with zero pending acknowledgements.
The final smoke run uses the fresh package-local environment. This checks the
three service inputs; the prior selector benchmarks cover model
selection quality. Test processes were stopped after verification.

After relocation to `corpscout/services/crawler_service`, all 10,578 saved data
files matched their original SHA-256 hashes. The package tests, three benchmark
tests, seven page-agent lab tests and 38 backoffice fixture tests passed from the
updated locations. A [fresh NOVELIC smoke run](data/service-relocation-20260917/summary.json)
again completed through CLI, REST and JetStream with local JSON results and no
pending acknowledgements. The virtual environment was rebuilt at the new path;
historical JSON receipts retain their original paths as provenance.
# Live status and interactive retries

See [HUMAN_ASSISTANCE.md](HUMAN_ASSISTANCE.md) for the SQLite attempt history, live
status API, company-page failure deadlines, durable Brave search pauses, S3 archives
and Backoffice noVNC controls. Brave verification resumes the same pending search
only after explicit activation and confirmation. Manual retries use a separate worker and preserve the original failure.

## Browser request API

All crawls use the [external browser API](BROWSER_API.md). Browser ownership,
management and remote desktops live in the independent browser service. Manual
worker concurrency is configured with `CRAWL_MANUAL_CONCURRENCY` (default 2).
The browser service alone controls capacity and expires idle leases after 120 seconds.

### CAPTCHA request options

Since 0.38.0, JSON requests accept `challenge_agent_max_runs` (3–1000) and
`challenge_agent_model` (`deepseek-flash` or `z-ai/glm-5.3-flash`). Request overrides
apply to REST requests. The service-wide enable switch still
controls whether automatic assistance is available. See [human assistance](HUMAN_ASSISTANCE.md#request-budgets-and-retries-0380)
for retry escalation and Backoffice controls. These are CAPTCHA model settings;
`api` and `config.model` continue to control page discovery.

## Browser capacity and mode

The browser service launches browsers on demand under `--max-browsers` (default 6).
Backoffice **Browsers → Settings** persists capacity, idle timeout and retention in
SQLite; these values override CLI/environment settings. Normal crawls are headless;
interactive retries request a headed browser with Xvfb/noVNC. Both share the limit.

`session_id` in a crawl request reuses an existing profile. Without it, the crawler
creates a saved session. Retries automatically preserve the failed attempt's ID.
Each active attempt also has a distinct `browser_execution_id`; delayed commands
cannot affect a newer execution. Completion closes Chromium, retains the profile,
and releases capacity. See [browser API](BROWSER_API.md) for the full contract.
