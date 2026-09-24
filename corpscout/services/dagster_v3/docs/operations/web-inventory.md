# Websites and pages inventory

Migration 442 owns `corpscout.websites` and `corpscout.pages`. The `websites` and
`pages` Dagster assets are produced together by `web_inventory` in group
`web_inventory`. The manually launched `web_inventory_job` selects both. It does
not run the source scanners, create queues, or change their result tables.

## Sources

The first integration reads only these sources:

| Source label | Table | URL | Evidence timestamp |
| --- | --- | --- | --- |
| `commoncrawl` | `commoncrawl_domains` | `url` | `resolved_at` |
| `commoncrawl` | `commoncrawl_page_technologies` | `page_url` | `resolved_at` |
| `commoncrawl` | `commoncrawl_page_jsonld` | `page_url` | `resolved_at` |
| `commoncrawl` | `commoncrawl_domain_page_meta` | `source_url` | `resolved_at` |
| `webtech` | `webtech_domain_scan_results FINAL` | `page_url` (fallback `requested_url`), plus `final_url` | `scanned_at` |

Only successful Webtech scans contribute, including zero-technology successes.
The technology result table alone cannot represent those successes. Requested
and final URLs remain separate page identities. Both are recorded when valid and
inside the source root domain. A cross-domain redirect target is rejected and
counted rather than assigned to the wrong root. Redirect relationships remain in
the source scan record.

Common Crawl `resolved_at` is processing/extraction evidence time. These source
tables do not contain the archive capture timestamp. It is not described as
capture time and never populates `last_successful_fetch_at`. Successful Webtech
results populate live fetch time from `scanned_at`. Inventory first/last seen
track imports and are separate from these source observation times.

`se_company_domain`, graph nodes, DNS hostnames, links inside JSON-LD, and other
sources are not read by this integration. Swedish assumed websites can be added
as a separate, explicitly scoped producer later. Existing assumed inventory rows
are preserved and are promoted to observed if these sources provide evidence.

## Materialization config

Migration 442 must be applied first, the selected source tables must exist, and
all roots must already exist in `corpscout.domains`. Missing roots fail the build
with sample roots. The asset does not change the domain inventory.

Default configuration reads both connectors:

```yaml
ops:
  web_inventory:
    config:
      sources: [commoncrawl, webtech]
      insert_batch_rows: 50000
      merge_batch_rows: 100000
      max_threads: 4
      max_execution_time: 3600
```

Set `sources: [commoncrawl]` or `sources: [webtech]` to import only that connector.
This preserves previously published evidence from all sources. Empty selections
and unrecognized source names are rejected. There is no schedule or sensor.

## Identity, scale, and publication

The producer reuses Webtech `page_identity`: lowercase/IDNA hostname, default port
removal, retained path/query, and no fragment. It never invents a homepage for an
observed deeper page. IDs are migration-defined SHA-256 hashes of the normalized
origin/page URL. Site/page results are deduplicated across all source records and
scans, with sorted distinct source arrays and min/max evidence timestamps.

Only narrow URL evidence crosses into Python, in a streaming reader, to reuse the
exact existing URL normalizer. Inserts are batches of at most 50,000 rows. Source
payloads, technologies and JSON are not loaded. Set aggregation happens in
ClickHouse in bounded root ranges with external aggregation/sort enabled and a
4 GiB per-query cap. A single unusually large root is kept in one range to avoid
splitting its identity groups. Root validation uses a sorted join rather than a
hash set of the entire domain inventory.

Every build retains existing pages and websites, including previous first-seen,
last-observed and successful-fetch timestamps. Later failures or a partial source
refresh cannot erase earlier evidence. Replaying a successful build does not
increase logical row counts. An inventory entry is not a present-day reachability
claim. Source statistics record input rows, accepted candidate URLs and rejected
URLs, while asset metadata records final unique site/page counts.

Build both staged snapshots completely before publication. `EXCHANGE TABLES` is
atomic per table, not a transaction across both tables. Publish the superset of
old/new websites first, then pages. If page publication fails, old pages retain
their parents and newly published websites may temporarily have no pages. A retry
converges. Never try to roll back an uncertain exchange. Stage cleanup removes
only this build's UUID-named tables, after cancelling its queries on failure.

The shared `web_inventory_publish` Dagster pool serializes builds. Direct calls to
the Python publisher must also be serialized. Domain membership is validated at
build time, not enforced by a ClickHouse foreign key.

## Verification

```sh
uv run pytest tests/test_web_inventory.py tests/test_websites_pages_schema.py tests/test_clickhouse_migrations.py -q
uv run dg check defs
```

Tests run against disposable ClickHouse instances. They cover source overlap,
normalization, zero-technology scans, exclusion of failed scans, archive/live
fetch timestamps, retained history, invalid URLs, missing roots, partial source
imports, migration round trips, and safe recovery after the second exchange fails.

## Deployment receipt — 2026-09-24

- Applied migration 442 from a verified clean version 441. Final ledger version:
  442, clean.
- Verified live `corpscout.websites` and `corpscout.pages` are MergeTree tables
  with the generated IDs and evidence-status columns. Both were empty.
- Deployed with `ansible/light_sync.yml`: `ok=33`, `changed=11`, `failed=0`.
  The playbook confirmed the running Dagster supervisor was preserved.
- Verified deployed GraphQL group `web_inventory` contains `websites` and
  `pages`, both associated with `web_inventory_job`.
- No materialization was launched as part of this deployment.
