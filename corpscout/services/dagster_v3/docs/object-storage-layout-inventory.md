# Object storage layout inventory

Status: Phase 1 baseline
Inventory date: 2026-08-16
Scope: Corpscout Dagster object-store access and the RustFS host

## Executive summary

RustFS is limited by object metadata I/O, not by capacity or memory. The host had about
45% free filesystem space and about 20 GiB of available memory during the inventory,
but its data disk was 76-86% busy with 33-53 ms read latency while transferring less
than 1 MiB/s. This is the characteristic shape of random metadata I/O rather than a
bandwidth-bound workload.

The cached RustFS usage snapshot accounts for at least 1,841,324 objects across 13
buckets. It is incomplete and was last updated on 2026-08-12, so this is a lower bound,
not a current whole-cluster total. Two prefixes dominate the known count:

- Norway BRREG financial: 885,940 objects; 885,809 (99.985%) are smaller than 64 KiB.
- Sweden financial: 884,990 objects; mostly individual report XHTML documents, with
  an average size of about 297 KiB.

`source-sweden-company` is not the high-object-count bucket. Its cached snapshot has
four objects, and a filesystem sample shows only bulk source ZIPs and manifests. Its
root-prefix manifest lookup should still be replaced, but it is not the source of the
current metadata pressure.

The static audit found 34 production calls to `ObjectStoreResource.list_keys()`. Seven
enumerate entity-per-object layouts that need compaction, 20 should read a known key or
catalog instead, five are bounded enough to retain temporarily with guardrails, and
two are maintenance-only retention scans.

No object was changed or deleted during this inventory.

## Method and limitations

The baseline combines four read-only sources:

1. A static scan of every production `list_keys()` call in `src/dagster_v3/defs`.
2. RustFS's cached `.usage.v2.json` snapshot, read from its internal metadata area.
3. A targeted S3 API sample of small buckets to measure real `LIST` latency.
4. Host, process, filesystem, and block-device telemetry from the RustFS machine.

A live enumeration of all 35 buckets was intentionally stopped. Even a bucket with
fewer than 250 objects took 4-10 seconds for one `LIST` page, and listing the Brazil
PNCP bucket did not return within two minutes. Continuing a blind cluster-wide scan
would have added the exact workload under investigation. Missing buckets must be
measured later from bounded catalogs or RustFS usage snapshots rather than a recurring
root listing.

RustFS's usage snapshot is marked `complete: false`. It omits Denmark and several newer
buckets, and it contains an old `source-wikidata-company-seed` bucket that is absent
from the current 35-bucket API result. Counts in this document therefore establish
scale and priority, but they are not suitable for billing or deletion decisions.

## Runtime baseline

| Measurement | Observed value | Interpretation |
| --- | ---: | --- |
| RustFS filesystem | 3 TiB total, 1.7 TiB used, 1.3 TiB free | Capacity is not exhausted |
| RustFS memory | 30 GiB total, about 20 GiB available | Memory is not exhausted |
| RustFS host CPU | 16 vCPUs, load about 3-5 | Host CPU is not saturated |
| Data disk utilization | 76-86% | Storage is near its useful I/O ceiling during metadata work |
| Data disk read latency | 33-53 ms | Expensive random reads dominate |
| Data disk read throughput | About 0.5 MiB/s | The workload is metadata-bound, not throughput-bound |
| Small-bucket `LIST` latency | 4-10 seconds for fewer than 250 keys | Per-list overhead is already operationally significant |
| Brazil PNCP root `LIST` | More than two minutes without a response | Broad enumeration is unsafe in a task hot path |
| Dagster RustFS endpoint | `http://rustfs:9000`, resolving to a Tailscale address | Tailscale adds a hop, but cannot explain local disk wait |

Dagster-host CPU can limit parsing and normalization work, but it is not the primary
cause of slow object enumeration. The S3 request waits on RustFS and its data disk.

RustFS represents an object as filesystem metadata (a directory containing `xl.meta`,
and extra part files for multipart objects). Millions of small logical objects thus
become millions of metadata operations even when their aggregate byte size is modest.

## Known bucket concentration

These are cached RustFS values from 2026-08-12 unless noted otherwise.

| Bucket | Objects | Bytes | Relevant distribution | Assessment |
| --- | ---: | ---: | --- | --- |
| `source-norway-brreg` | 885,940 | 3.52 GB | 99.985% below 64 KiB; average about 4 KiB | Highest-value compaction target |
| `source-sweden-financial` | 884,990 | 268.80 GB | Average about 297 KiB; mostly report XHTML | Eliminate listing first; preserve durable originals unless a measured compaction design proves safe |
| `source-finland-prh-xbrl` | 68,277 | 2.89 GB | About 89.5% below 64 KiB | Compaction/catalog candidate |
| `source-wikidata-company-seed` | 1,585 | 58.05 MB | Old bucket name, absent from current bucket list | Investigate as a possible retired layout; do not delete from this inventory |
| `conformance-finland` | 195 | 11.99 MB | Small | Low priority |
| `source-brazil-pgfn` | 150 | 24.27 GB | Large objects | Low object-count risk |
| `source-brazil-cvm` | 102 | 730.96 MB | Small count | Low priority |
| `source-finland-prhytj` | 39 | 8.47 GB | Large objects | Low object-count risk |
| `source-gleif-reference-data` | 29 | 2.87 GB | Large snapshots plus manifests | Replace broad manifest lookup, but not a storage-count priority |
| `source-brazil-cgu` | 8 | 3.69 MB | Small count | Low priority |
| `source-sweden-company` | 4 | 319.36 MB | Bulk ZIPs plus manifests | Not the million-file source |
| `source-open-page-rank-domains` | 4 | 237.22 MB | Bulk ZIPs plus manifests | Low object-count risk |
| `crawls` | 1 cached; 229 live | 62.94 MB cached; 73.29 GB live | Cached snapshot is stale | Inventory from its own catalog before any retention work |

Denmark is missing from the cached usage snapshot. Its local source database contains
822,756 companies and 553,371 person IDs. The current raw model can store original,
English-translated, and failure objects per entity, plus production-unit captures.
Consequently, its eventual footprint can be several million objects even though a
safe current count was unavailable. Denmark is a P0 growth-risk source.

## Listing classifications

The classifications used below are:

- **Catalog/direct key**: the caller should consume an explicit catalog, latest pointer,
  deterministic manifest key, or Dagster metadata instead of enumerating a prefix.
- **Bounded list**: the prefix is constrained by a date, family, or small curated dataset.
  It may remain temporarily, but needs page/count/time limits and telemetry.
- **Compaction**: the layout stores one or more small objects per entity. Introduce a
  catalog and write coarser immutable batches, normally Parquet or compressed NDJSON.
- **Maintenance**: listing is used only by retention. Keep it out of materialization hot
  paths and drive it from a catalog before the source grows.

Priority indicates migration urgency, not data deletion authority.

| Priority | Source and call site | Current listing scope | Classification | Phase 2+ action |
| --- | --- | --- | --- | --- |
| P2 | `brazil_companies/cgu/parsing.py:800` | All archives for one CGU dataset | Bounded list | Add count/time limits; later publish a dataset archive catalog |
| P2 | `brazil_companies/rfb/source.py:301` | One family and `YYYY-MM` snapshot | Bounded list | Retain temporarily with guardrails; include keys in the partition manifest |
| P1 | `brazil_pncp/assets.py:117` | All raw pages for one month | Catalog/direct key | Store page count and exact keys in a monthly completion catalog |
| P1 | `brazil_pncp/assets.py:223` | All raw pages for one month | Catalog/direct key | Normalize from the monthly completion catalog |
| P1 | `brazil_pncp/assets.py:471` | All raw pages for one day | Catalog/direct key | Normalize from a daily completion catalog |
| P0 | `denmark_cvr/company_details.py:903` | One of 128 company-detail hash buckets | Compaction | Replace entity key discovery with a partition catalog and immutable batches |
| P0 | `denmark_cvr/duckdb_asset.py:256` | Every Denmark search-result source prefix | Catalog/direct key | Persist result keys in the search-run catalog and DuckDB ingestion ledger |
| P0 | `denmark_cvr/person_details.py:700` | One person-detail hash bucket | Compaction | Use a partition catalog plus immutable entity batches |
| P0 | `denmark_cvr/person_details.py:1007` | Each of 128 company-detail buckets during validation | Compaction | Validate against catalog counts/checksums instead of enumerating every object |
| P0 | `denmark_cvr/production_units.py:299` | One full/update production-unit capture prefix | Compaction | Write partitioned batches and a completion catalog |
| P0 | `denmark_cvr/production_units.py:513` | All captures in one source prefix | Compaction | Parse the exact batch keys recorded by the catalog |
| P1 | `esma_firds/assets.py:424` | All FIRDS archive metadata under the raw root | Catalog/direct key | Create a source-file catalog partitioned by publication date and type |
| P2 | `estonia_rhr_procurement/resources.py:188` | Manifests for one monthly partition | Catalog/direct key | Write/read a deterministic latest pointer for the month |
| P2 | `finland_hilma/assets.py:64` | All manually uploaded export CSVs | Bounded list | Retain with limits; add an upload catalog if count grows |
| P1 | `france_decp_procurement/resources.py:118` | All snapshot manifests | Catalog/direct key | Maintain a deterministic latest-manifest pointer |
| P2 | `gleif/assets.py:278` | Entire GLEIF raw prefix for retention | Maintenance | Select retention candidates from the source catalog, never a root scan |
| P1 | `gleif/source.py:402` | All manifests in the GLEIF raw prefix | Catalog/direct key | Read the requested run key or a latest pointer directly |
| P2 | `latvia_iub_procurement/resources.py:142` | Manifests for one monthly partition | Catalog/direct key | Write/read a deterministic latest pointer for the month |
| P0 | `norway_brreg_financial/financial_storage.py:255` | Annual-account documents for one year/chunk | Compaction | Read a chunk catalog and move small documents into immutable batches |
| P1 | `norway_brreg_financial/financial_storage.py:273` | Checkpoints for one response partition | Bounded list | Record ordered checkpoint keys in the partition catalog |
| P0 | `norway_brreg_financial/financial_storage.py:302` | All per-entity responses in one partition | Compaction | Consume response indexes and compact response bodies into batches |
| P1 | `norway_brreg_financial/financial_storage.py:342` | Every response-index Parquet object | Catalog/direct key | Add a response-index catalog/latest pointer |
| P2 | `open_page_rank/assets.py:166` | Entire raw prefix for retention | Maintenance | Drive retention from the manifest/catalog |
| P1 | `open_page_rank/source.py:129` | Every raw manifest, even when run ID is known | Catalog/direct key | Construct the run manifest key directly; use one latest pointer only as fallback |
| P1 | `slovakia_financials/raw_store.py:59` | All compacted statement batches | Catalog/direct key | Preserve the batched layout and add an ordered batch catalog |
| P2 | `slovakia_uvo_procurement/resources.py:173` | Manifests for one monthly partition | Catalog/direct key | Write/read a deterministic latest pointer for the month |
| P1 | `sweden_address_osm/resources.py:212` | All OSM snapshot manifests | Catalog/direct key | Maintain a deterministic latest pointer and direct object-to-manifest reference |
| P1 | `sweden_company/resources.py:89` | Entire Sweden company raw prefix | Catalog/direct key | Construct the run manifest key directly and maintain a latest pointer |
| P1 | `sweden_financial/parsing.py:253` | Raw archives for one filing year | Bounded list | Keep temporarily with limits; pass archive keys from the yearly catalog |
| P1 | `sweden_uhm_procurement/resources.py:136` | All snapshot manifests | Catalog/direct key | Maintain a deterministic latest-manifest pointer |
| P2 | `uk_companies_house/raw_archives.py:251` | All archive metadata for one source kind | Catalog/direct key | Maintain an archive catalog by kind and publication date |
| P0 | `wikidata/assets.py:3114` | All weekly snapshot manifests across every date | Catalog/direct key | Maintain a latest-complete-snapshot pointer |
| P0 | `wikidata/assets.py:3522` | All seed catalogs across dates on a 60-second sensor | Catalog/direct key | Drive readiness from Dagster events/dynamic partitions, not bucket enumeration |
| P0 | `wikidata/assets.py:3592` | All seed catalogs across dates on a 60-second sensor | Catalog/direct key | Drive readiness from Dagster events/dynamic partitions, not bucket enumeration |

## Phase 1 conclusions

The migration order should be guided by both current pressure and implementation risk:

1. Use Sweden company as the low-risk catalog/direct-key pilot. It exercises the v2
   contract without moving a large object population.
2. Use Denmark as the compaction pilot before company/person detail capture grows to
   several million objects. New writes should adopt v2 before a historical backfill.
3. Apply the proven compaction pattern to Norway BRREG, the highest confirmed small-file
   concentration and therefore the largest likely performance win.
4. Add catalogs and remove recurring enumeration from Sweden financial before deciding
   whether to compact report XHTML. The original reports may have audit and replay value,
   so object preservation and hot-path discovery are separate decisions.
5. Address Finland PRH XBRL after the pilots; its small-object ratio makes it the next
   clear compaction candidate.
6. Replace the two 60-second Wikidata sensor scans early even though their cached object
   count is smaller; frequency makes them unnecessary steady-state load.

Phase 2 is defined in the [object storage v2 contract](object-storage-v2-contract.md).
Until a source-specific pilot implements and verifies that contract, v1 objects remain
the recovery source and no migration should delete them.
