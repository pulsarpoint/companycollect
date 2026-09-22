# Website crawl processing

The two stages have distinct assets and jobs:

| Input asset/table | Result asset/table |
| --- | --- |
| `website_full_crawl_requests` | `website_full_crawl_results` |
| `website_jobs_crawl_requests` | `website_jobs_crawl_results` |
| `website_site_info_requests` | `website_site_info_results` |

All tables live in `corpscout`. Backoffice domain selection launches the corresponding
`*_input_job`; the asset runs `INSERT ... SELECT`, freezes the selection under a crawl
task and starts no crawling. Backoffice's crawler page launches a `*_results_job` for
up to 100 explicit saved domains.

## Crawl tasks (the Brave-style path)

Each type has a workflow job that runs both assets in one run, like
`company_brave_search_workflow`:

| Workflow | Assets |
| --- | --- |
| `website_full_crawl_workflow` | `website_full_crawl_requests` → `website_full_crawl_results` |
| `website_jobs_crawl_workflow` | `website_jobs_crawl_requests` → `website_jobs_crawl_results` |
| `website_site_info_workflow` | `website_site_info_requests` → `website_site_info_results` |

The input asset tags the run with `processing/task_id`, and the results asset then
processes every enabled domain of that frozen task in batches of `batch_size`, until
the task is done. `batch_size * max_batches` does not cap a task. `domains` cannot be
combined with a task. A results run can also name an earlier task with `task_id`.

An execution is identified by the original Dagster run ID. Its task, content settings
(`api`, `model`, `max_pages`, `max_model_calls`, `page_selection`, `instructions`,
`crawler_config`, `force_refresh`, `refresh_interval_days`) and freshness cutoff are
saved in the run's `website_crawl/execution` tag. Dagster retries reuse it
automatically. For manual recovery, pass `execution_id` with the same settings:

```yaml
ops:
  website_site_info_results:
    config:
      execution_id: "the-original-Dagster-run-UUID"
      # plus the same required settings as the original run
```

Changing a fixed setting on resume is rejected; start a new execution instead.
Operational settings (`batch_size`, `max_in_flight`, CAPTCHA budget, timeouts) may
change. The execution ID is the default `batch_id`, so request IDs stay stable and
already submitted domains are recovered, never submitted twice.

## Required execution settings

A result asset requires these explicit fields. There is no silent fallback for them:

```yaml
ops:
  website_site_info_results:
    config:
      domains: [almi.se]
      challenge_agent_model: deepseek-flash
      challenge_agent_max_runs: 3
      api: deepseek
      model: deepseek-flash
      max_pages: 1
      max_model_calls: 20
      page_selection: basic_info
      batch_size: 25
      max_batches: 1
      refresh_interval_days: 30
```

For full/jobs, choose `page_selection: saved` to use the row's explicit pages or
its discovery preset. Choose `page_selection: instructions` and supply nonempty
`instructions` for custom discovery. Basic info requires `basic_info` and a one-page
limit. Supported CAPTCHA models are `deepseek-flash` and `z-ai/glm-5.3-flash`, with
3–1000 runs. Model API is `deepseek` or `openrouter`; use that API's model identifier.
`crawler_config` accepts additional validated ResearchConfig fields, including provider,
reasoning effort, timeouts, source/search limits and output-token limit. Required model
and limit fields cannot be overridden inside it. Credentials remain on the service.

`domains` can be omitted to process due inputs across the table. Selection is bounded
by `batch_size * max_batches`, ordered by priority descending then domain. `bucket`
can restrict work to one of the 256 stored buckets. `max_in_flight` defaults to 3.
No schedule is enabled. Default refresh is 30 days, configured per asset execution.
`force_refresh: true` bypasses freshness for this execution only. Disabled inputs
are skipped; already submitted work retains its original request snapshot.

## Results and recovery

Migration `000430` creates physical result tables with `ReplacingMergeTree`, keyed
by `(domain, request_id, attempt)`. Read complete history with `FINAL` to remove
duplicate writes of the same attempt. `*_latest` selects the newest attempt, including
failures, per `(domain, work_key)`. `*_latest_success` selects the newest valid response
for that same domain/effective content configuration. Failed,
partial and needs-review outcomes remain in history but do not satisfy freshness.
An unsuccessful later response never hides an earlier fresh success. API/model,
page selection, content limits and artifact requirements affect the work key;
priority, browser mode and CAPTCHA agent settings do not.

Columns include `site_info` (the company/site description JSON), `page_observations`
(deterministic contacts, jobs/document links and other observations), `pages`,
`model_usage`, state/error, source/input revision, timestamps and `s3_path`.
HTML and full original JSON remain in S3 and are readable through the existing
`website_crawl_results_s3_archive` mapping and Backoffice result viewer. Jobs and technology
interpretation remain offline work. One stored unsuccessful response does not fail
an otherwise completed batch; asset metadata reports successful/unsuccessful counts.

Before submission, the asset validates the selected batch with the crawler API and
persists immutable request snapshots in `website_crawl_submissions`. This is an audit
and recovery ledger, not an atomic claim queue. The Dagster pool and a direct PostgreSQL
session advisory lock serialize processors by type. No PostgreSQL transaction is held
while crawling. The existing `PROCESSING_PG_URL` must connect directly or use session
pooling, not transaction pooling.

A new materialization resumes receipts without results before admitting new work.
It reuses the original request ID/payload even if the new run has different settings.
Timeout/network/storage failures leave recoverable receipts. Crawler requests must
remain retained until collection finishes. Preserve `batch_id` for a manual replay;
otherwise each independent run gets a new execution identity. The crawler's durable
HTTP queue owns browser retries, CAPTCHA recovery and cancellation. Dagster waits up
to 1800 seconds per in-flight group, including pending S3 upload; timeout does not
cancel the crawler. Terminal outputs are inserted with acknowledged async inserts.

Run `uv run pytest tests/test_website_crawl_results.py` for real disposable PostgreSQL
and ClickHouse plus a crawler HTTP fixture: priority, required settings, source disable,
freshness, partial results, idempotent replay, HTTP validation, interruption/S3 recovery
and competing-run fencing. `uv run dg check defs` validates the actual Dagster graph.

## Result naming

- `*_results` is a physical ClickHouse table retaining completed attempt history.
- `*_results_latest` selects the newest attempt, including failures.
- `*_results_latest_success` selects the newest successful attempt.
- `website_crawl_results_s3_archive` reads the original S3 JSON bundles directly;
  it is shared by all three crawl types. Match the result's `s3_path` to `_path`,
  or use `request_id` and `attempt` to identify the attempt.

The former result `_current` views have been removed from migration 430. Input
`*_requests_current` views remain: their replacement key really does select the
current request configuration per domain. The 30-day freshness policy belongs to
the processor, not the latest-success view.
