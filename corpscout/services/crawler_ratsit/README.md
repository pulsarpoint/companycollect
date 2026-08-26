# Ratsit crawler

Python 3.14 service for crawling one Swedish company through a durable Temporal
workflow. Temporal owns execution and retries, S3 stores the response JSON, and
ClickHouse records the terminal crawl outcome.

Install dependencies:

```shell
uv sync
```

Copy `.env.example` into the deployment secret/configuration system and make
the variables available to the processes. The S3 bucket must exist and the
ClickHouse migration for `se_company_ratsit_crawl_results` must already be up.

## Run the browser

Start the persistent local Chromium instance:

```shell
uv run ratsit-server --profile-dir ./profile --headed
```

The launcher now lives in [`ratsit_server`](ratsit_server/README.md). Its
Ansible playbook owns only the UID 1000 `ratsit-cdp.service` user unit. The
browser runs normally on that user's active graphical session.

## Run the Temporal worker

The Temporal worker has a separate inventory, role, installation directory,
environment, and systemd user unit under `crawler_ratsit/ansible`. For the
current deployment, point its inventory at the same server and keep the CDP URL
on loopback:

```shell
cd crawler_ratsit/ansible
cp inventory.example.ini inventory.ini
cp worker-environment.example worker-environment
# Edit inventory.ini and worker-environment with the real service values.
# Keep RATSIT_CDP_URL=http://127.0.0.1:9222 while both services share a host.
ansible-playbook site.yml
```

The deployed worker runs as UID 1000, restarts automatically, and uses
the CDP URL from its own environment. It must be able to reach these
dependencies:

- Temporal at `TEMPORAL_ADDRESS`, using `TEMPORAL_NAMESPACE` and the task queue
  named by `RATSIT_TEMPORAL_TASK_QUEUE`.
- S3/RustFS at `CORPSCOUT_S3_ENDPOINT`.
- ClickHouse at `CLICKHOUSE_HOST:CLICKHOUSE_HTTP_PORT`.
- CloakBrowser at `RATSIT_CDP_URL`.

The browser and worker playbooks can be deployed or restarted independently.
When they move to different production hosts, change the worker inventory and
provide a protected reachable CDP URL; the browser deployment remains
unchanged.

For local development instead, keep CDP private and create an SSH tunnel:

```shell
ssh -N -L 19222:127.0.0.1:9222 graovic@192.168.88.149
```

Then set `RATSIT_CDP_URL=http://127.0.0.1:19222` in `.env` and start the
foreground worker with `uv run --env-file .env ratsit-worker`. The tunnel must
remain open for as long as the local worker is running.

The worker polls the `ratsit-crawler` task queue with one activity slot by
default. A crawl and its S3 upload are one retryable activity. Recording the
result in ClickHouse is a separate activity, so a ClickHouse retry cannot make
another request to Ratsit.

## Submit one company

With a Temporal server available, submit one workflow manually:

```shell
uv run ratsit-crawl 5562434182
```

The command generates a batch UUID unless `--batch-id <uuid>` is supplied and
returns as soon as Temporal accepts the workflow. The stable workflow ID is
`ratsit/company/<company-id>`: another submission reports `already_running`
while that company is open, and a new run can reuse the ID after completion.
The batch and company IDs are also stored in Temporal memo for inspection.

Raw responses use this S3 layout:

```text
raw/batch_id=<batch-uuid>/identity_sha256=<company-id-sha256>/response.json
```

The JSON stores the selected HTML and crawl metadata. ClickHouse stores only
the outcome, S3 location, content size, timings, and Temporal provenance.

## Test

```shell
uv run pytest
```

The live CDP integration test is disabled during the normal test run. Because
the remote service binds CDP to loopback, first forward it over SSH:

```shell
ssh -N -L 19222:127.0.0.1:9222 graovic@192.168.88.149
```

Then run the integration test in a second terminal:

```shell
RATSIT_RUN_INTEGRATION_TESTS=1 \
RATSIT_CDP_URL=http://127.0.0.1:19222 \
uv run pytest -s tests/integration/test_remote_cdp.py
```

Set `RATSIT_INTEGRATION_COMPANY_ID` to test a company other than
`5562434182`. The test opens the real Ratsit page through the remote browser,
selects `main .main-inner`, converts that HTML to Markdown, and verifies that
the requested organisation number is present.
