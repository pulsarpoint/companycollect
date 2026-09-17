# Local crawl service

Version 0.23.0 accepts work through REST, the existing CLI, and NATS JetStream.
All three call `company_research.crawl.crawl_company`. Results stay on the local
filesystem. This service collects pages and optionally describes the site; the
later LLM fact-analysis module remains separate.

For a native systemd installation on `192.168.88.132`, use the
[Ansible deployment](ansible/README.md). It preserves server-local results under
`/var/lib/company-research/results` across application releases.

## Install and configure

Run these commands from `companycollect/corpscout/services/company_research`:

```bash
uv sync --extra service
cp .env.example .env
```

Fill in the needed keys in `.env`, or set environment variables. The package has
its own `pyproject.toml` and `uv.lock`; no sibling experiment package or its virtual
environment is required. Python 3.12+ and the browser runtime used by CloakBrowser
are required. Initial browser startup may download its runtime.

`DEEPSEEK` is used by the default direct DeepSeek API. `OPENROUTER_API_KEY` is used
when a request specifies `"api": "openrouter"`. Exact page lists without
instructions or site-info need neither key. Credentials belong to the service
environment; request bodies cannot supply credentials or output paths.

## REST

```bash
uv run --extra service company-research-service \
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
| `GET /healthz` | Returns `200` when workers and any enabled NATS input are ready; otherwise `503`. |

Set `CRAWL_API_TOKEN` to require `Authorization: Bearer <token>` on submission,
status and result endpoints. The launcher requires a token when `--host` binds
beyond localhost. This prototype is for trusted callers: it fetches caller-supplied
URLs and does not provide network isolation or a public multi-tenant URL policy.
TLS termination can be provided by the deployment's reverse proxy.

The job state is `queued`, `running`, `completed`, or `failed`. `completed` means
the crawler published a result; inspect `crawl_status` for its actual outcome
(`finished`, `partial`, `skip_crawling`, `needs_review`, or `failed`). A service-level
execution exception instead produces a failed job and a local error JSON file.

## Request format

REST and JetStream use the same JSON object:

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
- `site_info: true` alone describes the input page and stops. Combine it with
  `pages`, `instructions`, or `crawl: true` to continue crawling.
- `api` defaults to `deepseek`. Optional `config` fields override `ResearchConfig`
  limits/model settings. Omitted fields retain the selected API's crawl defaults.
  For example, OpenRouter GLM selection uses `"api": "openrouter"` and
  `"config": {"model": "z-ai/glm-5.3", "provider": "parasail/fp8", "reasoning_effort": "high"}`.
- `request_id` may contain letters, digits, `_` and `-` (maximum 128 characters).
  REST generates one if omitted; JetStream derives one from the stream/message
  sequence and publication timestamp so redelivery is stable, including across
  stream recreation. Explicit IDs are recommended for publisher retries.

Reuse an ID with the same normalized request to retrieve/reuse its existing job.
A different request under that ID is a REST `409` or a rejected JetStream message.
Use a new ID when intentionally requesting a new crawl, including after a completed
partial/failed crawl. Examples for all modes are in `examples/`.

## Existing command line

Existing flags and JSON stdout remain available:

```bash
uv run company-research-crawl https://www.novelic.com/ \
  --pages https://www.novelic.com/careers/ --max-pages 1 \
  --output-dir ./data/cli-novelic

uv run company-research-crawl https://www.novelic.com/ \
  --site-info --output-dir ./data/cli-novelic-info --env-file .env
```

The CLI writes directly to its specified empty output directory. REST/NATS use
job subdirectories below the service's output directory. Each route now also
writes `result.json`; CLI stdout keeps its existing crawl-manifest shape.

## NATS JetStream

For a local test broker, run a `nats-server` with JetStream enabled:

```bash
nats-server -js -a 127.0.0.1 -sd ./data/nats
```

Run both network inputs in one service process:

```bash
uv run --extra service company-research-service \
  --transport both --output-dir ./data/crawl-service --env-file .env \
  --nats-url nats://127.0.0.1:4222 --create-stream
```

Use `--transport nats` for a worker without REST. Defaults are stream
`COMPANY_CRAWL`, subject `company.crawl.requests`, durable consumer
`company-crawl-local`, and a 60-second acknowledgement deadline. Override them
with `--nats-stream`, `--nats-subject`, `--nats-durable`, and `--nats-ack-wait`.
`--create-stream` creates a missing file-backed work-queue stream. Existing stream
settings are not changed; an incompatible existing consumer is rejected at startup.
Without that flag, provision the stream first. `--nats-credentials` accepts a NATS
credentials file; `NATS_URL` supplies the server URL when `--nats-url` is absent.

Publish with a JetStream client so the publisher receives a persistence ack:

```python
import asyncio
from pathlib import Path
import nats

async def submit():
    client = await nats.connect("nats://127.0.0.1:4222")
    try:
        receipt = await client.jetstream().publish(
            "company.crawl.requests", Path("examples/pages-job.json").read_bytes()
        )
        print(receipt.stream, receipt.seq)
    finally:
        await client.close()

asyncio.run(submit())
```

The durable pull consumer fetches one message at a time and sends progress acks
while its crawl is queued/running. It acknowledges completion only after a terminal
local JSON result exists and is readable. A lost ack/redelivery reuses that result.
Queue pressure or a failed local write leaves the message retryable. Invalid or
conflicting requests are terminated only after a rejection receipt is saved under
`rejected/`; those receipts exclude raw input. Crawler outcomes such as `partial`
or `needs_review` are saved results and are acknowledged, not retried indefinitely.
No result/event is published to another NATS subject in this version.

The implementation uses the documented [NATS Python pull/ack APIs](https://nats-io.github.io/nats.py/modules.html)
and [FastAPI lifespan handling](https://fastapi.tiangolo.com/advanced/events/).

## Local output and recovery

```text
data/crawl-service/
  jobs/novelic-jobs-001/
    request.json
    job.json
    attempts/0001/
      result.json
      crawl-manifest.json
      site-info.json             # when requested or the site is skipped/uncertain
      pages/p0001/page.html
      pages/p0001/input.json
      ...                       # existing crawl diagnostics and captures
  rejected/                     # invalid JetStream request receipts
```

`result.json` is a portable document with schema `company-crawl-result/1.0`:

```json
{
  "schema_version": "company-crawl-result/1.0",
  "crawl": {"status": "finished", "site_info": null, "pages": []},
  "documents": [
    {"page_id": "p0001", "url": "https://example.com/", "html": "<h1>Example</h1>", "html_sha256": "..."}
  ]
}
```

The shortened `crawl` example omits most manifest fields. Actual JSON includes
the complete manifest and cleaned HTML for each fetched page. HTML captures remain
available for `company-research-pages --crawl <attempt-directory>`.

JSON publication uses atomic replacement. Restarting the service recovers queued
or interrupted jobs from disk. Interrupted work gets a new attempt directory;
completed captures are preserved. A result saved just before a process crash is
adopted without recrawling. Interrupted work before result publication may repeat
HTTP/model requests; this is not exactly-once execution.

The initial implementation runs **one process per local output directory**,
enforced by a filesystem lock (Linux/macOS). Use `--transport both` to share that
store between REST and NATS. `--concurrency` defaults to 1; `--max-pending` defaults
to 100 and counts queued plus running jobs. Multiple independent hosts with
separate local disks do not share deduplication state. Keep the output directory
when restarting; copying only application code does not preserve completed jobs.

## Validation

```bash
uv run --extra service python -m unittest discover -s tests
# Set NATS_SERVER=/path/to/nats-server to include real JetStream integration tests.
```

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

After relocation to `corpscout/services/company_research`, all 10,578 saved data
files matched their original SHA-256 hashes. The package tests, three benchmark
tests, seven page-agent lab tests and 38 backoffice fixture tests passed from the
updated locations. A [fresh NOVELIC smoke run](data/service-relocation-20260917/summary.json)
again completed through CLI, REST and JetStream with local JSON results and no
pending acknowledgements. The virtual environment was rebuilt at the new path;
historical JSON receipts retain their original paths as provenance.
