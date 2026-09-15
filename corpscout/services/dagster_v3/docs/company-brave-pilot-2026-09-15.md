# Brave PostgreSQL pilot — 2026-09-15

> This records the earlier PostgreSQL-input pilot. The current workflow keeps the
> input selection in ClickHouse; see [the operator guide](company-brave-processing.md).

Both production Dagster runs succeeded. Eight company queries used Brave Ask, saved their complete copied answers in PostgreSQL, and published matching rows to ClickHouse. Resuming the same task created no new processing attempts, result rows, or export batches.

## Identity and deployment

- Browser fix: `95177c150` (deployed and checked against the local file SHA-256).
- Task: `2305a766-dae3-4f33-a30c-c808ea958819`.
- Initial Dagster run: `486dfb61-7207-4d03-b929-28fdb5f88f89`.
- Resume Dagster run: `bc61b700-c672-4e18-b4c5-565e39161bcb`.
- Job: `company_brave_search_job`; asset: `company_brave_search_results`.
- PostgreSQL: `192.168.88.161:5432`, database `corpscout`, schema `processing`. This is the same PostgreSQL instance used by Dagster; its metadata is in the separate `dagster` database.
- ClickHouse: `corpscout.company_brave_info`.

## Observed results

| Check | Result |
|---|---|
| Frozen inputs / successes | 8 / 8 |
| Errors, retries, remaining work | 0 |
| Saved PostgreSQL results | 8 |
| ClickHouse physical rows / distinct result IDs | 8 / 8 |
| Unpublished results at completion | 0 |
| Export batch sizes | 2, 4, 2; all acknowledged |
| Route distribution | Direct and each of three proxies returned two answers |
| Total copied answer text | 1,440 UTF-8 bytes |
| Initial Dagster run duration | 48.6 seconds |
| Resume Dagster run duration | 21.7 seconds |

Compared every saved answer verbatim between PostgreSQL and ClickHouse, along with result ID, input ID, rendered query, status, source URL, route, attempt and company name. After the resume run there were still eight total attempts, eight results and three export batches. Every item remained on attempt 1.

## Continuous work refill

These are PostgreSQL observations taken approximately every five seconds. Running counts are claimed work items, not browser network instrumentation.

| Observed at (UTC) | Queued | Running | Succeeded | Unpublished |
|---|---:|---:|---:|---:|
| 14:38:15 | 4 | 4 | 0 | 0 |
| 14:38:25 | 3 | 4 | 1 | 1 |
| 14:38:30 | 0 | 4 | 4 | 2 |
| 14:38:35 | 0 | 3 | 5 | 3 |
| 14:38:40 | 0 | 1 | 7 | 1 |
| 14:38:45 | 0 | 0 | 8 | 0 |

The first completed company freed a slot that was immediately refilled: one result was saved while four items remained running and three remained queued.

## Selected companies

| Input ID | Company | Route | Answer bytes |
|---|---|---|---:|
| 5560003468 | Sandvik Aktiebolag | direct | 367 |
| 5560004615 | Skanska AB | crawl_proxy1 | 139 |
| 5560073495 | AKTIEBOLAGET SKF | crawl_proxy2 | 70 |
| 5560125790 | AKTIEBOLAGET VOLVO | crawl_proxy3 | 124 |
| 5560142720 | Atlas Copco Aktiebolag | crawl_proxy2 | 335 |
| 5560160680 | Telefonaktiebolaget LM Ericsson | crawl_proxy3 | 121 |
| 5560427220 | H & M Hennes & Mauritz AB | crawl_proxy1 | 162 |
| 5560593575 | ASSA ABLOY AB | direct | 122 |

## Materialization configuration

The initial run used this configuration. Reusing this task ID resumes the completed task. Use a new UUID to run a fresh pilot with `freshness_days: 0`.

```yaml
ops:
  company_brave_search_results:
    config:
      task_id: "2305a766-dae3-4f33-a30c-c808ea958819"
      input_relation: corpscout.se_company_brave_input
      input_namespace: se_company
      query_type: official_website
      query_template: "Find the official website of {company_name}."
      company_ids:
        - "5560003468"
        - "5560004615"
        - "5560073495"
        - "5560125790"
        - "5560142720"
        - "5560160680"
        - "5560427220"
        - "5560593575"
      max_companies: 8
      requests_per_route: 1
      freshness_days: 0
      retry_seconds: 10
      export_batch_size: 4
      export_interval_seconds: 10
```

Read durable progress with:

```sql
SELECT * FROM processing.task_progress
WHERE task_id = '2305a766-dae3-4f33-a30c-c808ea958819';
```

## Scope and limits

This is a small functional pilot, not a throughput or failure-recovery benchmark. It verifies live Brave Ask on all four routes, dynamic queries (including H&M’s ampersand), durable saved responses, work refill, batched publication and completed-task resume. It does not establish capacity for webtech, large response bodies or sustained load. No outages or worker crashes were injected into production.

The browser/publication checks passed: 17 focused tests, including a browser fixture that exposes both question and answer Copy buttons and an incomplete streamed response. `uv run dg check defs` and deployment health checks also passed. Existing processing recovery tests cover additional failures with disposable databases.

Raw answers remain AI-generated text; this pilot did not independently verify each suggested company domain. Result retention is still manual. Webtech remains on its existing storage flow.
