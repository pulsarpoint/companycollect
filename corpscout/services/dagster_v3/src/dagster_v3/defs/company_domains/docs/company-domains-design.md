# Company domains: Brave answer collection

> Historical initial design. The current assets are `company_brave_queue_input`
> and `company_brave_search_results`, in group `brave_domain_search`. See the
> [current processing guide](../../../../../docs/company-brave-processing.md)
> for configuration, PostgreSQL progress and S3 response history.

## Source and scope

`company_brave_search_results`, in group `company_domains`, collects the answer
produced by Brave's search → More → Copy flow at https://search.brave.com.
It ports `services/searcher/brave.py` into Dagster and uses the same CloakBrowser
launch and proxy configuration as Ratsit. Dagster does not allow the requested
hyphenated group spelling `company-domains`.

The first input source is `corpscout.se_company_basic_info FINAL`: active Swedish
companies with a nonempty legal name. IDs stay strings. The prompt is generated
for each row: `Can you give me more information about Sweden company {company_name}`.
Copied text is discovery evidence, not a verified company-to-domain association.
This step does not extract or publish domain claims, financial amounts, industries,
contacts, or translated fields. Those cross-cutting transformations do not apply
until there is a separate structured extraction step. Proper names are unchanged.

## Throughput and lifecycle

One Dagster run drains eligible companies. A shared lazy iterator supplies the next
company whenever a browser slot is free. There is no fixed assignment per route,
batch-completion barrier, per-company Dagster run, or artificial request interval.
Faster routes naturally handle more companies. Input selection uses keyset pages,
not a full registry download or a list of millions of queued futures.

Routes are `direct`, `crawl_proxy1`, `crawl_proxy2`, `crawl_proxy3`; the proxy values
come from the same environment variables as Ratsit. Each slot owns a persistent
headless CloakBrowser and context, and a fresh page per company. Default
`requests_per_route: 1` gives four active requests at most. Raising it to N creates
N independent slots per route, giving 4 × N total. Pages and browser objects stay
on their owning thread. Browser startup, a depleted queue, and slow persistence
can temporarily lower utilization; four is a ceiling, not a guaranteed rate.

The bounded result queue holds at most one result per configured slot. Workers
stop claiming work when the asset is cancelled or persistence fails. Their current
page operations finish or time out before their browser closes. The
`company_domains_brave` op pool, with the instance's default pool limit 1, prevents
overlapping asset runs from multiplying this concurrency. Leave that pool at 1.

The run finishes when no more eligible rows are found (or `max_companies` is reached).
It does not poll indefinitely after the queue becomes empty. New companies and
IDs inserted behind the current keyset cursor are picked up on the next run. No
schedule or sensor is enabled; request refilling happens inside the asset itself.

## Storage, checkpointing, and schema

This browser source is neither a bulk feed nor a REST listing; dlt/DuckDB/dbt add
no extraction or transformation value here. Like Ratsit, it saves raw evidence
to S3 and indexes outcomes directly in ClickHouse. This is an intentional departure
from the standard dlt → DuckDB → ClickHouse bulk-source shape.

Every successful answer is saved as exact UTF-8 text at
`s3://company-domains-brave/SE/{company_id}/{run_id}/answer.txt`.
The shared `ObjectStoreResource` uses the existing `CORPSCOUT_S3_*` environment.
Migration `000409_corpscout_company_brave_search_results` owns
`corpscout.company_brave_search_results`, one outcome per country/company/run.
`ReplacingMergeTree(fetched_at)` sorts by `(country_code, company_id, source_run_id)`.
`RESULT_COLUMNS` pins the insertion order. All String columns are non-nullable;
failed searches use empty object keys and zero answer bytes.

The index contains company ID/name, the generated query and prompt version, route,
status/error category, source URL, fetched time, object location/size, and run ID.
Credential-bearing proxy URLs and raw browser exceptions are never stored or logged.
The answer text stays in object storage rather than a wide raw ClickHouse column.

Each success marker is written **after** its complete object is saved. Each result
is persisted immediately instead of waiting for the whole run. Successful same-name,
same-prompt results less than `freshness_days` old (default 30) are skipped on rerun.
Renamed companies and new prompt versions remain eligible. An S3 success followed
by a database failure can leave an orphan object; the company remains eligible,
so this cannot silently lose an answer. No destructive replacement is performed.

Individual search failures are recorded and do not stop other slots. After all
selected companies finish, any such failure marks the asset failed. A rerun skips
saved successes and retries failed/missing results. Browser startup/lifecycle,
selection, and persistence failures stop the run rather than declaring success.
There is no automatic retry loop within a browser slot.

## Launch

Apply migration 000409 through the normal migration workflow before materializing.
Launch `company_brave_search_job` or the `company_brave_search_results` asset.
For a bounded initial run, use:

```yaml
ops:
  company_brave_search_results:
    config:
      requests_per_route: 1
      max_companies: 20
```

Omit `max_companies` to drain the backlog. Optional `company_ids` restricts selection
to a list of active company IDs; the names are still read dynamically. Set
`freshness_days: 0` for an intentional fresh scan. `page_size` bounds database
selection pages independently of browser concurrency. Each Playwright operation
uses the resource's `page_timeout_ms` (60 seconds by default).

## Issues and verification

The OS clipboard is shared even across browser contexts. Reading it after four
concurrent Copy clicks can assign another company's text to the current company.
The page init script captures Brave's `navigator.clipboard.writeText` argument in
a page-local variable. The Copy button is still clicked; no shared clipboard read
or write is required. A fresh page and nonempty-answer wait prevent stale reuse.

`tests/test_company_domains_brave.py` covers slot refill under an intentionally
slow route, default and increased concurrency, bounded lazy consumption, cleanup,
failure handling, run limits, and object-before-index persistence.
`tests/test_company_domains_brave_integration.py` executes the real migration and
pending-selection query in ClickHouse and tests the Copy flow with an installed
CloakBrowser against a controlled page. These tests do not send requests to Brave.
Run them with `uv run pytest`, and validate registration with `uv run dg check defs`.

A bounded live smoke check on 2026-09-15 copied answers through the direct route,
`crawl_proxy1`, and `crawl_proxy3`. `crawl_proxy2` returned Brave's homepage and
search page, but its answer's More button did not appear within 60 seconds.
This is recorded as a failed attempt rather than an empty successful answer.
The live probe did not persist results or apply the migration.
