# Website crawl input assets

The three input assets select website domains from an existing ClickHouse table or view and insert new recurring requests into the tables created by migration `000429`:

| Asset / destination in `corpscout` | Selection job |
| --- | --- |
| `website_full_crawl_requests` | `website_full_crawl_input_job` |
| `website_jobs_crawl_requests` | `website_jobs_crawl_input_job` |
| `website_site_info_requests` | `website_site_info_input_job` |

Materializing an input asset prepares requests for processing. Browser execution and result publication will be separate assets. The broader [crawl proposal](../../../../../docs/website-crawl-input-proposal.md) describes that remaining work.

## Selection configuration

All three assets use the same `CrawlInputConfig`:

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

At `/admin/se/companies/domains`, filter the existing domain entity and select individual domains or **Select all matching domains**, then choose **Add to crawl inputs → Full crawl / Jobs / Basic info**. All current list filters can be combined. The all-matching selection spans every page; unchecking a domain excludes it from that selection. Explicit selections survive pagination, while changing filters clears an all-matching selection. Shared domains are counted and submitted once, regardless of how many companies claim them.

Backoffice launches the matching `*_input_job` with the domain list's predicates as `se_domain_filters`. The asset runs the `INSERT ... SELECT` inside ClickHouse and freezes the selection under a crawl task (see below). No browser session is started. Source rows stay inside ClickHouse; the web server receives only the Dagster run. Criteria are evaluated against `se_company_domain FINAL` when saving, so source updates after the list loads can change the matches. A domain is considered shared if more than one company claims it in the complete current entity, even when another filter narrows the company rows.

Saving creates recurring input records and preserves existing requests, including disabled entries and operator settings. The run metadata states the task ID, how many domains the task selected and how many new request rows were added. No crawl is started by the input job alone. The UI and Dagster input assets share the same per-destination query ID to reject concurrent inserts. The generic assets remain available for programmatic imports; browser-processing assets remain separate work and must be started independently.

## Examples

Select active SE domains supported by Brave with confidence at least 80%, excluding one domain:

```yaml
ops:
  website_full_crawl_requests:
    config:
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
  website_full_crawl_requests:
    config:
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
  website_jobs_crawl_requests:
    config:
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
  website_site_info_requests:
    config:
      source_relation: corpscout.domains
      ids: ["novelic.com", "melexis.com"]
```

Use the corresponding job in the Dagster Launchpad. IDs must exist in the named source. These examples do not directly submit new domains absent from that source.

## Crawl tasks

Every input materialization also freezes its selection, the way `company_brave_search_input` does for Brave. `task_id` comes from the config, then the run's `processing/task_id` tag, then the run ID, and the asset tags the run with it. The selected domains, existing or newly added, are written to `corpscout.website_crawl_task_domains` (migration `000431`). A `processing.tasks` PostgreSQL record keeps the selection fingerprint, crawl type (processor `website-crawl-<type>-v1`) and total.

Rerunning the same `task_id` with the same selection reuses the frozen membership, even if the source has changed since. A different selection under that `task_id` is rejected. An interrupted selection is recovered like Brave's: the task's still-running insert is killed and its unconfirmed rows are deleted before selecting again. Membership does not change request settings: a disabled request stays disabled and is skipped when the task is processed.

## Storage and identity

Selection, normalization, deduplication and insertion run inside ClickHouse with `INSERT ... SELECT`; the asset does not transfer the source inventory through Python or replace a table. This operational workflow uses the append-only input design rather than the full-refresh DuckDB/dlt country-source pattern.

Bare hostnames and protocol-relative URLs use HTTPS. Hostnames are lowercased, IDNA encoded and stripped of a terminal dot. The domain identity also drops an initial `www.` while retaining other subdomains. The starting URL keeps its host, path, port and query string, with the fragment removed. Empty values, malformed hosts, unsupported protocols, credentials, whitespace inside URLs and invalid ports are skipped. When several selected rows resolve to the same domain, prefer HTTPS, then the shortest URL, then lexical order. IDNA uses ClickHouse's [IDNA implementation](https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/idna.cpp).

Each new domain gets revision 1, equal creation/update timestamps, and the source relation as provenance. Crawl settings come from the table defaults, with only an explicitly supplied priority overridden. Existing domains are skipped through the destination's `_current` view, including disabled entries. Reruns preserve all existing operator settings and never create a new revision or force a new crawl. Unselected domains are left intact. Refresh frequency remains processing-asset policy.

The selection assets are deliberately unpartitioned: one ID/filter request can add domains across all 256 stored buckets. The processing assets accept an optional bucket filter (0–255); automated partition scheduling is not enabled. Input insertion has the `website_crawl_input` pool, whose repository instance default is one. A stable per-destination ClickHouse query ID also rejects overlapping insert queries, including one still running after a client disconnect. Writes are synchronous, and retries reselect only missing domains. Direct writers outside this path must coordinate separately; ClickHouse is not a transactional uniqueness service.

Materialization metadata includes the source relation, destination relation and inserted-domain count. A zero-match selection succeeds with zero inserted rows. No schedules or browser requests are created by these assets.

## Validation

`tests/test_website_crawl_input_assets.py` materializes all three assets against a disposable ClickHouse server. It covers IDs, filters, combined selection, current source revisions, shared-domain deduplication, URL/IDN normalization, invalid values, deterministic limits, injection-shaped input, and preservation of disabled/operator-edited requests. The schema behavior is covered separately by `tests/test_website_crawl_requests_clickhouse_local.py`.

These are website input records, not company financial, translation, or NACE facts. Those interpretations belong to the downstream processing and analysis assets.


## Processing saved inputs

Use the separate `website_*_results` assets described in
[the processing guide](website-crawl-processing.md). Backoffice always launches the
input job to populate this table; it does not write the table directly or synthesize
materialization events. It launches the results job only through an explicit start.
