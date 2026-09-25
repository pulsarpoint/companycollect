# Website crawl input selection

`website_crawl_input` (job `website_crawl_input_job`) selects website domains from an existing ClickHouse table or view, or from explicit targets, inserts missing recurring presets into the tables created by migration `000429` and appends the domains to the open crawl draft of the chosen `crawl_type` (`corpscout.website_crawl_task_domains`, migration `000448`). Processing is a separate step: see [crawler-draft-queue.md](../../../../../docs/operations/crawler-draft-queue.md) and [the processing guide](website-crawl-processing.md).

## Selection configuration

`CrawlQueueInputConfig` adds `crawl_type`, `queue_scope` (default `workspace`) and `submission_id` to the shared `CrawlInputConfig`:

| Parameter | Behavior |
| --- | --- |
| `source_relation` | Required `corpscout.table_name`, including ordinary views. |
| `id_column` | Column matched by `ids`; defaults to `domain`. Set `company_id` for company-based selections. |
| `website_column` | Column containing a hostname or HTTP(S) URL; defaults to `domain`. |
| `ids` | Explicit source IDs as strings. Numeric source IDs are compared as strings. |
| `excluded_ids` | Source IDs to exclude, using the same `id_column`; applied after IDs and filters. |
| `filters` | Mapping of source column names to lists of allowed string values. Numeric flags can use `"1"`/`"0"`. Nulls do not match. |
| `se_domain_filters` | SE domain-list criteria: domain text, company ID, source, association, active status, confidence bounds and shared domains. Requires `corpscout.se_company_domain`, `source_final: true` and `root_domain` as both ID and website column. |
| `source_final` | Set `true` for a replacing source table when current revisions are required. Leave `false` for views that already resolve their current rows. |
| `select_all` | Explicitly permit the whole source when IDs and filters are omitted. |
| `max_domains` | Optional positive limit on distinct valid domains, sorted alphabetically. Applies before skipping existing requests, so rerunning the same limited selection remains stable. |
| `priority` | Optional 0–100 priority for new entries. Omitted values use the migration's default. |

IDs and filters can also be combined: the ID list and every column filter must match. Values within a single filter are alternatives. An empty ID list alone never selects everything; an empty filter-value list is an error. `select_all` does not discard supplied filters. Table and column names must be simple identifiers; all values are bound query parameters. More complex source logic can be exposed through a ClickHouse view.

Selecting from any of the three destination tables or their current views is rejected. Missing source columns or unapplied destination migrations fail before insertion.

## Backoffice SE domains

At `/admin/se/companies/domains`, filter the domain entity, select domains or **Select all matching domains**, then **Add to crawl queue → Full crawl / Jobs / Basic info**. Backoffice launches `website_crawl_input_job` with the list's predicates as `se_domain_filters`, a stable `submission_id` and `queue_scope: workspace`; it never writes ClickHouse itself and never starts a crawl. Criteria are evaluated against `se_company_domain FINAL` when the asset runs, so source updates after the list loads can change the matches. Shared domains are submitted once.

## Examples

Select active SE domains supported by Brave with confidence at least 80%, excluding one domain:

```yaml
ops:
  website_crawl_input:
    config:
      crawl_type: full
      source_relation: corpscout.se_company_domain
      source_final: true
      id_column: root_domain
      website_column: root_domain
      excluded_ids: ["example.se"]
      se_domain_filters:
        source: brave
        status: active
        min_confidence: 0.8
```

Other optional `se_domain_filters` fields are `domain` (substring, with the list's SQL `LIKE` wildcard behavior), `company` (numeric company ID as a string), `association` (`connected`, `uncertain`, `not_connected`), `max_confidence` (0–1), and `shared` (boolean). Source also accepts `wikidata`, `esef_filing` and `common_crawl_identity`; status also accepts `inactive`. An empty filter object requires IDs or explicit `select_all: true`.

Select specific company IDs and their active websites:

```yaml
ops:
  website_crawl_input:
    config:
      crawl_type: full
      source_relation: corpscout.company_domains_resolved
      id_column: company_id
      website_column: website_host
      ids: ["5560049529", "5560726605"]
      filters:
        country_code: ["SE"]
        is_active: ["1"]
      priority: 80
```

Select jobs inputs by filters alone:

```yaml
ops:
  website_crawl_input:
    config:
      crawl_type: jobs
      source_relation: corpscout.company_domains_resolved
      website_column: website_host
      filters:
        country_code: ["SE", "NO"]
        is_active: ["1"]
      max_domains: 1000
```

Select basic-info inputs from a domain inventory:

```yaml
ops:
  website_crawl_input:
    config:
      crawl_type: site_info
      source_relation: corpscout.domains
      ids: ["novelic.com", "melexis.com"]
```

Launch `website_crawl_input_job` in the Dagster Launchpad; each run appends to the open draft for its crawl type and queue scope. IDs must exist in the named source. These examples do not directly submit new domains absent from that source.

## Crawl drafts

Every import appends to the open draft of its crawl type and scope; `task_id` may name that draft explicitly. Membership lives in `corpscout.website_crawl_task_domains` (one partition per task) with the selected URL, source and `submission_id`; a `processing.tasks` record keeps lifecycle and totals and `processing.input_submissions` the receipts. Repeating a `submission_id` with the same selection is a no-op; a different selection under it is rejected; a failed import is retried by reselecting the source. Membership does not change presets: a disabled request stays disabled and is skipped when the draft is processed.

## Storage and identity

Selection, normalization, deduplication and insertion run inside ClickHouse with `INSERT ... SELECT`; the asset does not transfer the source inventory through Python or replace a table. This operational workflow uses the append-only input design rather than the full-refresh DuckDB/dlt country-source pattern.

Bare hostnames and protocol-relative URLs use HTTPS. Hostnames are lowercased, IDNA encoded and stripped of a terminal dot. The domain identity also drops an initial `www.` while retaining other subdomains. The starting URL keeps its host, path, port and query string, with the fragment removed. Empty values, malformed hosts, unsupported protocols, credentials, whitespace inside URLs and invalid ports are skipped. When several selected rows resolve to the same domain, prefer HTTPS, then the shortest URL, then lexical order. IDNA uses ClickHouse's [IDNA implementation](https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/idna.cpp).

Each new domain gets revision 1, equal creation/update timestamps, and the source relation as provenance. Crawl settings come from the table defaults, with only an explicitly supplied priority overridden. Existing domains are skipped through the destination's `_current` view, including disabled entries. Reruns preserve all existing operator settings and never create a new revision or force a new crawl. Unselected domains are left intact. Refresh frequency remains processing-asset policy.

The selection assets are deliberately unpartitioned: one ID/filter request can add domains across all 256 stored buckets. The processing assets accept an optional bucket filter (0–255); automated partition scheduling is not enabled. Input insertion has the `website_crawl_input` pool, whose repository instance default is one. Each submission's inserts run under a stable ClickHouse query ID (`crawl-queue-import:<submission_id>`) that a retry kills before replacing that submission's rows. Writes are synchronous. Direct writers outside this path must coordinate separately; ClickHouse is not a transactional uniqueness service.

Materialization metadata includes `task_id`, `submission_id`, the submission's distinct `input_count` and the draft's `total`. A zero-match selection succeeds with zero inserted rows. No schedules or browser requests are created by these assets.

## Validation

`tests/test_website_crawl_input_assets.py` imports selections through `load_crawl_draft` against a disposable ClickHouse server. It covers IDs, filters, combined selection, current source revisions, shared-domain deduplication, URL/IDN normalization, invalid values, deterministic limits, injection-shaped input, and preservation of disabled/operator-edited requests. The schema behavior is covered separately by `tests/test_website_crawl_requests_clickhouse_local.py`.

These are website input records, not company financial, translation, or NACE facts. Those interpretations belong to the downstream processing and analysis assets.


## Processing

Drafts are processed by the `website_*_results` assets with `task_id`, launched from the Backoffice queue page (see [crawler-draft-queue.md](../../../../../docs/operations/crawler-draft-queue.md)). Without `task_id` the same assets run the refresh sweep described in [the processing guide](website-crawl-processing.md).
