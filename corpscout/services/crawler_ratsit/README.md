# Ratsit crawler

Python 3.14 service for crawling Swedish companies through durable Temporal
workflows. Temporal owns pending work and retries, S3/RustFS stores response
JSON, and ClickHouse records terminal crawl outcomes.

One `ratsit-process` launches every configured CloakBrowser persistent context
directly. It does not expose or connect to Chrome DevTools Protocol endpoints.
The same process runs:

- one Temporal workflow/result worker on `ratsit-crawler`;
- one HTTP activity worker on `ratsit-http` for each browser context.

Every HTTP worker is permanently bound to one browser, has concurrency `1`,
and polls the same task queue. Temporal gives the next activity to an available
poller, so no local round-robin or shared DuckDB scheduler is needed. Workflow
and activity names are unchanged, so existing Temporal histories remain
compatible.

## Browser pool config

Copy the examples and protect the TOML file because proxy URLs may contain
credentials:

```shell
cp .env.example .env
cp crawler_ratsit/ansible/process-config.toml.example process.toml
chmod 0600 process.toml
```

The pool config has process-wide browser defaults, explicit rate limits, and
one entry per persistent context:

```toml
[process]
state_directory = "/absolute/path/to/ratsit-process-state"
headless = false

[limits]
per_browser_activities_per_second = 0.2
task_queue_activities_per_second = 0.2

[[browsers]]
id = "direct"
enabled = true

[[browsers]]
id = "proxy1"
enabled = true
proxy_url = "http://user:password@proxy1.example:8080"
```

Omit `proxy_url` for direct access. `enabled = false` disables an entire
context. Each enabled ID gets its own profile directory under
`process.state_directory`; IDs must be unique and contain lowercase letters,
digits, hyphens, or underscores.

`per_browser_activities_per_second` protects each browser independently.
`task_queue_activities_per_second` is Temporal's server-side global rate for
`ratsit-http`. Keep the global value explicit and identical on every process
polling that queue. Start at `0.2`—one new request every five seconds across the
whole queue—and raise it only from observed 429 and proxy results. Do not
multiply it automatically by the number of browsers.

## Run locally

Install and run with Python 3.14:

```shell
uv sync
uv run --env-file .env ratsit-process --config process.toml
```

Headed mode requires `DISPLAY` and `XAUTHORITY` in the process environment.
The process stops all Temporal pollers and browser contexts on SIGTERM or
SIGINT. An unexpected browser disconnect fails the process so systemd can
restart the complete unit.

The process must reach Temporal, S3/RustFS, and ClickHouse. The S3 bucket must
exist and the ClickHouse migration for `se_company_ratsit_crawl_results` must
already be applied.

## Submit one company

```shell
uv run --env-file .env ratsit-crawl 5562434182
```

The command generates a batch UUID unless `--batch-id <uuid>` is supplied. The
stable workflow ID is `ratsit/company/<company-id>`. Regular organisations use
ten digits; natural-person and sole-proprietor records may use twelve. The
canonical ID remains unchanged in Temporal, S3, and ClickHouse, while the
Ratsit URL uses its final ten digits.

A crawl and S3 upload are one retryable HTTP activity. HTTP 429 responses ask
Temporal to wait ten minutes before retrying. ClickHouse result recording is a
separate activity, so a database retry never repeats the Ratsit request.

Raw responses use:

```text
raw/batch_id=<batch-uuid>/identity_sha256=<company-id-sha256>/response.json
```

The JSON includes the selected HTML, crawl metadata, and `browser_id` used for
the request. ClickHouse stores only outcome, S3 location, content size, timing,
attempt count, errors, and Temporal provenance.

## Deploy

The single Ansible deployment is under
[`crawler_ratsit/ansible`](crawler_ratsit/ansible/README.md). It owns
`ratsit-process.service`, installs CloakBrowser, and retires the former
`ratsit-worker.service` and `ratsit-cdp.service` units during cutover.

## Test

```shell
uv run pytest
```

The normal suite never opens a browser. Run the opt-in direct CloakBrowser
integration test with:

```shell
RATSIT_RUN_INTEGRATION_TESTS=1 \
uv run pytest -s tests/integration/test_direct_browser.py
```

Set `RATSIT_INTEGRATION_PROXY_URL` for a proxy run or
`RATSIT_INTEGRATION_COMPANY_ID` for a different company. The standalone speed
experiment remains in [`ratsit_speed_probe`](ratsit_speed_probe/README.md).
