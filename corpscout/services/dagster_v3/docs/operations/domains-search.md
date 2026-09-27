# Domain list serving snapshot

`corpscout.domains_search` (migration 443) supplies membership for the Workspace → Domains list. Its membership comes only from `corpscout.domains`. Other tables enrich existing roots and never add domains.

| Field | Source / meaning |
| --- | --- |
| root_domain, sources, first_seen_at, last_seen_at | Canonical domains snapshot |
| has_dns_records, dns_last_observed_at | Any recorded DNS evidence and maximum last_seen in commoncrawl_domain_dns_records; not a claim of current resolution |
| website_count, observed_website_count | Website inventory rows; observed evidence does not establish current reachability |
| has_website | website_count > 0, materialized column |
| company_count | Distinct (SE, company_id) in se_company_domain FINAL, active=1 and association='connected' |
| has_company | company_count > 0, materialized column |
| refreshed_at, source_run_id | Publication provenance |

Materialize `domains_search` with `domains_search_job` after publishing domains, websites or company associations, or when DNS freshness is required. No recurring schedule is installed. DNS ingestion continues independently, so the list reflects the displayed snapshot timestamp. Dependencies express lineage and do not automatically launch a refresh.

The publisher freezes domain membership using CREATE TABLE CLONE AS (shared immutable parts, no full data copy), aggregates companies once, joins narrow DNS and website aggregates in bounded root ranges (default 500,000), verifies row count, then atomically exchanges the complete staging table. Empty inventory or a failed batch leaves the previous published snapshot in place. Temporary tables are cleaned on normal success/failure; process termination may require removal of abandoned UUID-suffixed domains_search_* staging tables after checking no corresponding run is active. Pool domains_search_publish uses the configured default concurrency limit of 1.

MergeTree ORDER BY root_domain supports prefix/cursor reads. Source arrays support any/all matching. DNS and website presence use the snapshot fields. Company presence uses current active rows in `company_domains_resolved`, including reviewer decisions and country in company identity. Company matching presence uses any saved outcome in `website_company_lookup_results`; failed, cancelled and not-found attempts count as attempted. These two presence filters do not wait for an inventory refresh. Visible rows get current company counts and latest matching status from bounded queries for the page's domains.

All filters apply before pagination. The global inventory total comes from `system.tables.total_rows`; an exact filtered count uses the same predicates without the cursor. For example, `suffix=se&companies=without&companyMatching=without` selects unassociated `.se` domains with no published matching attempt. Earlier local-only tests and requests still in progress have no published outcome and are not counted as attempted.

Row selection and **Select all matching domains** retain explicit exclusions across pages. Changing applied filters clears an all-matching selection. The shared **Add to crawl queue** control sends either explicit IDs or the full filters/exclusions to `website_crawl_input_job`, using `workspace_domain_filters` against `corpscout.domains_search`. Dagster selects the full set in ClickHouse without a page limit and adds it to the ordinary crawl draft. Choose **Basic info**, then configure the processing model and enable **Company matching** from the queue. This does not automatically accept company-domain proposals. The existing one-million-domain draft limit applies.

The expandable websites section reads `corpscout.websites` directly. An empty inventory has an explicit UI notice; absence of a website entry must not be interpreted as evidence that the domain has no website. After website inventory materialization, refresh domains_search to update its stored counts.

Validation: disposable ClickHouse tests cover source membership, duplicate evidence, removal of associations on refresh, atomic failure handling, and projection selection. Backoffice tests cover bound filters, source any/all serialization and cursor preservation. Actual production query timings are checked after initial publication.

## Deployment — 2026-09-24

Migration 443 applied successfully (clean ledger). Dagster hot sync validated and reloaded successfully, with no service restart. Backoffice typecheck/build and five focused tests pass; publisher has four disposable ClickHouse tests and migration contracts pass.

Initial materialization: `d10a5a03-6843-448d-80f1-232ea02fbb84`. At the last check it was STARTED with 4,000,008 staged rows out of 123,142,485 frozen roots. Shared-server disk I/O was the dominant delay. Publication and full-dataset latency validation are still pending; do not treat this receipt as a successful materialization. The earlier run `96215c86-d870-4a1a-9c40-0ed3a3ed1ab1` was canceled before publication to replace the source copy with a clone; its temporary tables and queries were confirmed cleaned up.

`websites` and `pages` currently contain no rows. Their initial population is a separate pending materialization. The domain list explicitly displays this limitation.
