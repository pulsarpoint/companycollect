# Website crawl input tables — proposal

Status: input tables/assets are implemented in migration `000429`. Backoffice launches the input jobs; the assets perform all inserts. Migration `000430` adds per-type results and immutable submission receipts. Bounded results assets implement priority ordering, 30-day success freshness, required execution settings, HTTP completion and S3/ClickHouse publication. See the [current processing guide](../src/dagster_v3/defs/website_crawl/docs/website-crawl-processing.md). The detailed admission/lease design below remains a future scalable scheduler proposal: the current manually launched processors serialize per crawl type with a direct PostgreSQL session lock and have no automatic schedules or retry loops.

Use three physical ClickHouse input tables, each owned by a corresponding Dagster input asset:

| Input table / asset | Result table / asset | Default scope |
| --- | --- | --- |
| `corpscout.website_full_crawl_requests` | `corpscout.website_full_crawl_results` | Discover useful company, contact, jobs, and financial-information pages; preserve links, simplified HTML, and deterministic observations. |
| `corpscout.website_jobs_crawl_requests` | `corpscout.website_jobs_crawl_results` | Discover careers pages and job listings, including relevant external job boards. |
| `corpscout.website_site_info_requests` | `corpscout.website_site_info_results` | Classify and briefly describe the website/company using the bounded site-info workflow. |

The shared domain inventory feeds these input assets. The three tables have the same columns; the table identity selects the crawl preset. Business interpretation and technology/job analysis remain separate downstream work.

## Row identity and lifecycle

Each table has one **current desired configuration per normalized domain**. A row means “keep this domain's results fresh for this crawl type”; it is not a single execution or an active browser session.

Normalize domains using the existing crawl identity rules: lowercase, IDNA, remove a trailing dot and initial `www.`, and preserve other subdomains. Keep redirects and final URLs in the result. Company-to-domain relationships remain separate so several companies can share the same crawl.

Actual executions receive their own `request_id` and attempt IDs in PostgreSQL. Each attempt snapshots the input revision and resolved configuration. A browser `session_id` identifies its reusable browser state independently.

The source asset creates new domain rows without overwriting operator changes during every refresh. Input generation and Backoffice edits must use one serialized writing path with monotonically increasing revisions for each `(crawl_type, domain)`. Changing a domain creates a new identity and disables the old row. Changing priority, instructions, or limits writes a new revision of the same identity.

## Common columns

| Column | ClickHouse type | Default / requirement | Meaning |
| --- | --- | --- | --- |
| `domain` | `String` | Required; immutable identity | Normalized target website. |
| `website_url` | `String` | Required; normally `https://<domain>/` | Initial HTTP(S) URL. Must normalize to the input domain. |
| `bucket` | `UInt16` | Computed: `cityHash64(domain) % 256` | Dagster partition, 0–255; generated consistently by ClickHouse. |
| `enabled` | `Bool` | `true` | Whether new scheduled work may be admitted. |
| `priority` | `UInt8` | `50`; valid range 0–100 | Higher priority is selected first. |
| `page_mode` | `Enum8('discover' = 1, 'explicit' = 2)` | `discover` | Automatic page discovery or an explicitly supplied page list. |
| `pages` | `Array(String)` | `[]` | In discover mode: additional starting candidates. In explicit mode: the requested pages to collect. |
| `instructions` | `String` | `''` | Additional discovery/collection instructions; empty means use the crawl preset. |
| `headless` | `Bool` | `true` | Browser execution mode. |
| `proxy_route` | `LowCardinality(String)` | `direct` | Named route resolved by browser service; never proxy credentials. |
| `save_artifacts` | `Bool` | `true` | Retain detailed HTML/screenshots/diagnostics, following current development defaults. |
| `preset_version` | `UInt16` | `1`; must be positive | Immutable version of this type's defaults and discovery instructions. |
| `config_json` | `String` containing a JSON object | `'{}'` | Validated overrides for limits, discovery models, and CAPTCHA-agent settings. |
| `source` | `LowCardinality(String)` | `domain_inventory` | Origin of the input: inventory, Backoffice, or import. |
| `created_at` | `DateTime64(6, 'UTC')` | Writer supplied | Original creation time; preserved across revisions. |
| `updated_at` | `DateTime64(6, 'UTC')` | Writer supplied | Time of the current edit. |
| `revision` | `UInt64` | Writer supplied; increasing | Replacement version for this domain's input row. |

Normal producers need to supply the domain and starting URL. The shared input writer supplies identity normalization, provenance, timestamps, and revision; operational defaults have one authoritative definition at this input boundary. Runtime code receives the resolved values.

Use the same shape for all three tables initially. A basic-info request must use `page_mode='discover'` with `pages=[]`; it runs the site-info workflow without a full crawl. Additional instructions must remain within basic description/classification scope. Defaults for full/jobs/basic-info are versioned independently by the selected preset.

`explicit` requires a nonempty list of normalized HTTP(S) URLs. It collects the requested list directly; instructions do not silently remove pages from that list. Missing required pages produce partial/failed coverage. In discover mode, instructions guide selection and supplied pages are candidates rather than an exclusive boundary. External candidate URLs still follow the crawler's source-validation and navigation bounds.

The current crawler rejects `crawl='full'` combined with pages or instructions. Adapting that validation and preserving these explicit semantics is required during migration; this document does not claim that the existing API already accepts this proposed schema.

## Configuration overrides

Keep the fields used for selection, filtering, or routine Backoffice controls in columns. Use `config_json` for less common overrides, for example:

```json
{
  "max_pages": 30,
  "page_timeout_seconds": 45,
  "web_search": true,
  "max_search_queries": 3
}
```

The writer must validate a type-specific allowlist and value ranges; reject unknown keys and offline-analysis settings. JSON text follows existing Corpscout conventions. It is configuration, not an executable expression. Secrets stay in service configuration.

Do not duplicate top-level fields such as `headless` or `priority` in this object. Refresh intervals are asset-level policy and are not accepted as per-entry overrides in `config_json`. Resolve preset defaults plus overrides once when admitting an execution and retain that snapshot with its result. Omitted CAPTCHA-agent budgets preserve the agreed automatic budget behavior; an explicit override remains identifiable.

## Asset-level refresh policy

Each crawl-type processing asset owns a positive `refresh_interval_days` configuration value. Initially, all three use 30 days, with full crawl, jobs, and basic info independently configurable. Thirty days is the initial interpretation of “one month”.

The setting applies to every domain processed by that asset. Changing it affects subsequent eligibility checks without updating input rows. Retain the effective policy in run metadata for audit, but exclude it from the content work key: changing frequency does not change the content being requested.

The initial input schema has no per-domain refresh interval or refresh-interval override. An immediate individual refresh uses the one-time `force_refresh` execution action described below.

## Storage migration

The authoritative DDL lives in [000429 up](../../../clickhouse/migrations/000429_corpscout_website_crawl_requests.up.sql), with the corresponding [down migration](../../../clickhouse/migrations/000429_corpscout_website_crawl_requests.down.sql). It creates all three tables using `ReplacingMergeTree(revision) ORDER BY (bucket, domain)` and these ordinary views:

- `corpscout.website_full_crawl_requests_current`
- `corpscout.website_jobs_crawl_requests_current`
- `corpscout.website_site_info_requests_current`

Each view reads its table with `FINAL`, includes the materialized `bucket`, and retains disabled rows for operator inspection. Consumers apply their enabled/priority filters to the views.

Database constraints reject empty or uppercase domains, starting URLs without an HTTP(S) scheme and host, priorities above 100, zero revision/preset versions, explicit mode without pages, and configuration that is not a JSON object. They also reject the `refresh_interval_days` key in `config_json`, including null values. The site-info table additionally rejects explicit mode and nonempty page lists.

The input assets normalize selected websites and insert only missing domains with the table defaults. A future operator-editing writer must validate supported routes, instruction length, page limits, and the configuration allowlist and value ranges before appending revised configurations.

The up migration can be replayed without replacing existing input data. The down migration removes these views and tables only; dropping the input tables discards their rows. It does not remove the database or existing crawl result storage.

Priority and enabled state are deliberately outside the sorting key, so changing them replaces the same logical row. No physical `PARTITION BY` is proposed initially; `bucket` is the Dagster work partition and a leading sort-key column. Changing the bucket count requires a deliberate migration.

Current-row readers use `FINAL` (or an equivalent maximum-revision query) before applying mutable filters such as enabled/priority. `ReplacingMergeTree` merges asynchronously and does not supply transactional uniqueness or a permanent revision audit log. Executed input snapshots remain in attempt history. See the official [ReplacingMergeTree documentation](https://clickhouse.com/docs/engines/table-engines/mergetree-family/replacingmergetree).

## Freshness and execution

1. Read the current enabled inputs for a bucket and resolve their presets.
2. Check the corresponding result table for a published, qualifying successful result matching `(domain, crawl_type, work_key)` within the processing asset's configured `refresh_interval_days`.
3. Admit a bounded set of eligible inputs into PostgreSQL, favoring higher priority; repeat the freshness check after obtaining the execution claim.
4. Execute using the immutable configuration snapshot and browser service over HTTP.
5. Persist the result and artifacts, publish the ClickHouse result, and finish the PostgreSQL item. Publication failures retry the saved output.

`work_key` hashes canonical, output-relevant configuration: crawl type, preset version, initial URL, page mode/list, instructions, relevant discovery/extraction limits and models, and artifact requirements. Exclude scheduling-only fields such as priority, enabled, timestamps, and refresh interval. Browser routing and headless mode alone do not invalidate successful content; a manual forced request handles operational retests.

A fresh error does not hide an earlier valid success. Explicit-page collection cannot satisfy a standard discovery request because its work key differs. Full-crawl results do not automatically satisfy jobs or basic-info requests in the first version. Valid empty jobs output can qualify; CAPTCHA-only pages and incomplete required coverage cannot.

The input-to-result asset dependencies stay acyclic: previous-result freshness is a runtime lookup, not a reverse asset dependency. Re-materializing an input partition must preserve existing operator overrides and pending execution history.

## Priority and execution state

Copy priority into queued PostgreSQL work. Select eligible items by `priority DESC, next_attempt_at ASC, input_id ASC`, with leases and a cross-run active claim for `(domain, crawl_type)`. Give high-priority buckets admission opportunities as well; sorting only within an already-admitted bucket does not provide global priority. Admission must remain bounded, with a fairness policy so ordinary work eventually runs.

The existing `ProcessingStore.admit()` assumes sorted input IDs and a fixed lexical cursor, while `claim()` orders by retry time and input ID. Priority-aware admission is an explicit implementation change, not just a new ClickHouse column. The existing task-scoped claims also need cross-run domain/type ownership.

Update waiting queue items when Backoffice changes priority. Disabling an input stops future admissions and invalidates pending scheduled work; cancelling an active execution is a separate action. Recheck the current input revision before starting work, and snapshot any updated semantic configuration. Running attempts continue with their original snapshot.

Keep `status`, attempt count, lease owner/expiration, `next_attempt_at`, terminal failures, and publication state in PostgreSQL. A due domain in ClickHouse must not repeatedly reset an exhausted retry budget; PostgreSQL retains that decision until an explicit retry or configured retry cooldown permits another cycle.

## Manual requests

A Backoffice forced refresh creates a **one-time execution** linked to the appropriate input/domain and crawl type, with its own request ID, priority, and optional interactive mode or configuration overrides. `force_refresh` bypasses freshness for that execution only. It does not bypass domain ownership, capacity limits, or retry timing.

Do not persist an always-on `force_refresh` flag in the recurring input row: it would request a fresh crawl on every scheduler pass. One-time requests may snapshot custom instructions/pages without changing the recurring configuration. A stable manual submission ID prevents repeated clicks or transport retries from creating duplicate execution requests.

## Example current inputs

| Table | Domain | Priority | Page mode | Instructions |
| --- | --- | ---: | --- | --- |
| `website_full_crawl_requests` | `novelic.com` | 50 | discover | Default full-crawl preset. |
| `website_jobs_crawl_requests` | `novelic.com` | 80 | discover | Find current vacancies and follow the official external recruitment board if present. |
| `website_site_info_requests` | `melexis.com` | 50 | discover | Default basic-info preset. |

For example, changing the jobs asset's refresh interval to seven days would apply to all jobs inputs, including Novelic, without editing their rows.

## Integration and validation scope

Implement the full-crawl input/result asset pair first, then reuse the same package and request schema for jobs and basic info. Preserve CLI execution and existing artifacts/Backoffice links during migration.

This is an operational crawl workflow, not a bulk country-registry snapshot. It deliberately uses PostgreSQL claims and per-attempt S3/ClickHouse publication instead of the usual full-refresh DuckDB/dlt path described in `docs/data-source-guidelines.md`. Input revisions and crawl history must not be erased by a whole-table replacement. Monetary interpretation and translation/LLM analysis are downstream concerns, outside these input tables.

The migration tests in `tests/test_website_crawl_requests_clickhouse_local.py` exercise defaults, current-row replacement and disable behavior before background merges, input constraints, discovery/explicit page storage, and replay/rollback against ClickHouse.

Before the runtime implementation is accepted, additionally validate priority-aware admission, cross-run claims, freshness after failed attempts, asset-level refresh-policy changes, input-writer validation, explicit-list coverage, configuration changes, one-time force refresh, retry-budget exhaustion, publication recovery, and cancellation.
