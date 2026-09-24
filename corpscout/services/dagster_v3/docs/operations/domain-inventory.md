# Combined root-domain inventory

The first inventory step publishes `corpscout.domains` in ClickHouse.
It creates no website/page tables, changes no scanner or queue, and does not yet
switch the Workspace Domains reader. Migration 441 owns the table schema and drops the empty `domain_inventory` table.

## Schema

| Column | Meaning |
| --- | --- |
| `root_domain` | Normalized source root, one row per domain |
| `sources` | Sorted, distinct source-family labels |
| `first_seen_at` | First inclusion in this inventory's current continuous membership |
| `last_seen_at` | Start timestamp of the inventory build that most recently included it |
| `source_run_id` | Dagster run that published the row |

These timestamps describe inventory publication, not registration, page crawl or
technology scan dates. Refresh preserves first-seen timestamps for retained roots.
If a root disappears from every source it leaves the current inventory; a later
reappearance starts a new membership period. Source history remains upstream.

The publisher lowercases and trims source roots, removes trailing DNS dots, and
uses the Workspace hostname validation to exclude malformed values, emails and
IPv4 literals. It consumes upstream `root_domain` identities rather than applying
a new public-suffix policy to the large source datasets. Upstream normalization
remains authoritative.

## Sources

Only these three tables supply inventory membership:

| Label | Source |
| --- | --- |
| `commoncrawl` | `corpscout.commoncrawl_domains` |
| `se_company_domain` | `corpscout.se_company_domain` |
| `commoncrawl_graph` | `corpscout.commoncrawl_domain_graph_nodes`, restricted to published releases in `commoncrawl_domain_graph_snapshots` |

Only completed graph releases contribute domains; loading releases are excluded.
Graph membership does not imply archived pages or technology detections.

No registry union, graph ranks, DNS,
Open PageRank, Webtech or website-crawler result tables are read. The former
`domains_clickhouse` asset and its registry aggregate tables were retired by
migration 440. Their creation/alter DDL has also been removed. Migration 441
reuses `domains` for this inventory with the schema above.
Sources describe membership, not ownership or current technology detections.

## Build and publication

Materialize `domains`, or launch `domains_job`. Its default
configuration is four ClickHouse query threads and a one-hour limit per source
query. The job selects only the inventory asset; it does not rerun upstream crawls.

The Dagster pool `domains_publish` serializes publishers. Queries run
inside ClickHouse because the source data already lives there; dlt, DuckDB,
currency conversion and translation are not applicable to this derived index.

Archive and Swedish sources contribute grouped candidate roots to a unique staging
table. Graph nodes stream directly, with cross-release deduplication deferred to
the final merge. Domain
validation runs on this compact table so query predicate pushdown cannot evaluate
it for every archived URL. Source contribution counts include rejected candidates.
The final merge uses ordered root-domain ranges of at most approximately one
million contribution rows (`merge_batch_rows`, default and maximum 1,000,000).
A boundary domain stays wholly in one range so overlapping sources cannot split
across batches. Each range filters invalid roots, merges source labels and retains
existing first-seen timestamps using a sorting merge join against the same range
of the published table. This bounds both aggregation and spilled-file merge work. In-order aggregation is
disabled for this merge because its state blocks exceeded the query memory limit
at graph scale. Queries cap memory at 4 GiB and spill grouping/sorting work
above 512 MiB. Staging tables inherit the migration-owned schema.

Only the complete, nonempty build is published using `EXCHANGE TABLES`. A missing
source or failed query fails the build while preserving the previously published
table. Empty individual sources are allowed and remove their contributions;
an entirely empty build is refused. Source reads occur sequentially, so this is
a completed build rather than a transactionally synchronized snapshot across all
source systems. Source adapters must continue publishing their own data safely.

Failed builds fence their query IDs before cleaning up their own staging tables.
Retries rebuild from current source data. Do not run the Python publisher directly
alongside Dagster publishers; the shared asset pool is the single-writer boundary.

Refresh is explicit in this first step. No frequent schedule is enabled: Common
Crawl currently includes over a billion URL records, so repeated full rebuilds
need a measured cadence or compact source-membership projections first.

## Validation and subsequent steps

Integration tests use disposable ClickHouse and cover the three-source union, published graph releases and overlapping releases,
exclusion of unrelated populated tables, malformed inputs, source removal,
first-seen preservation and failed/empty build isolation. Migration contracts
and Dagster definitions are also checked.

Next steps are separate: switch Workspace browsing to the inventory with source
filters, add the website/page inventory, and publish scan/company summaries for
fast processing-history and technology filters. Those summaries are intentionally
not guessed or filled with zero in this first table.

## Published graph extension (2026-09-24)

Run `31012c9f-b534-4afb-aedc-525934721012` succeeded with 169 bounded merge ranges
and published 123,142,485 unique domains, adding 74,677,415 roots to the previous
48,465,070-domain inventory. `commoncrawl_graph` marks published graph-node
membership. Rank/signals, DNS and other unrelated sources remain excluded.

Source membership after validation:

| Sources | Domains |
| --- | ---: |
| commoncrawl | 3,427,920 |
| commoncrawl + commoncrawl_graph | 45,011,396 |
| commoncrawl + commoncrawl_graph + se_company_domain | 19,690 |
| commoncrawl + se_company_domain | 173 |
| commoncrawl_graph | 74,677,415 |
| commoncrawl_graph + se_company_domain | 3,395 |
| se_company_domain | 2,496 |

Whole-inventory merges failed at the 4 GiB query cap, including an attempted
external aggregation whose spilled-file merge exceeded the cap. The deployed
publisher now bounds both grouping and joining by root-domain ranges. Integration
tests cover overlaps at range boundaries, spilling under a 256 MiB test cap,
first-seen preservation, and publication safety after a later-range failure.
