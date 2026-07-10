# CommonCrawl IP GeoIP enrichment

## Purpose

Enrich the unique IP addresses found in CommonCrawl DNS A and AAAA records with the
locally installed MaxMind GeoLite2 City and ASN databases. The output is the durable
`corpscout.commoncrawl_ip_geoip` ClickHouse dataset used by downstream provider and
technology inference.

## Source boundaries

- `corpscout.commoncrawl_domain_dns_record_summary` is the retry-safe DNS record source.
- `corpscout.commoncrawl_ip_addresses` is a refreshable ClickHouse projection containing
  one canonical IP row with DNS first/last-seen timestamps.
- `MAXMIND_DATABASE_DIRECTORY` contains versioned `GeoLite2-City_*` and
  `GeoLite2-ASN_*` directories. The resource selects the newest matching database file and
  reads its embedded build epoch.

This pipeline intentionally does not use dlt or DuckDB. There is no HTTP/file extraction
or relational transformation: each input IP is a local MMDB point lookup and the rows are
inserted directly into the migration-owned ClickHouse table.

## Partitioning and execution

`commoncrawl_ip_geoip` has 256 static Dagster partitions named `bucket_000` through
`bucket_255`. ClickHouse assigns canonical IP text to a stable bucket with
`cityHash64(ip) % 256`; the same expression is a schema contract and must not change
without rebuilding both IP tables.

The asset is manual-only: it has no schedule, sensor, or automation condition. A UI
backfill runs one bucket per Dagster run and the `commoncrawl_geoip` pool serializes runs
initially. Inside a bucket, MMDB results are written in bounded batches. Reruns select only
IPs missing the current City or ASN build epoch, so a partial run is safely resumable.

## ClickHouse model

Migration `000115_corpscout_commoncrawl_ip_geoip` owns:

- `commoncrawl_ip_addresses` and its refreshable materialized view over the DNS summary;
- `commoncrawl_ip_geoip`, a `ReplacingMergeTree(enriched_at)` keyed by `(bucket, ip)`;
- `commoncrawl_ip_geoip_current`, a `FINAL` view for consumers that require one current
  row per IP.

The GeoIP row preserves country and registered-country identity, subdivision arrays,
city GeoName identity, nullable coordinates with accuracy radius, timezone, ASN and AS
organization, the matched City/ASN networks, lookup statuses, and both MMDB build epochs.
Coordinates are nullable because `(0, 0)` is valid and cannot represent “missing.”

## Operational notes

- Apply ClickHouse migrations through the existing migration workflow before launching.
- Set `MAXMIND_DATABASE_DIRECTORY` to the directory containing the versioned MMDB folders.
- Materialize selected buckets manually. Rematerialize all buckets after installing a new
  City or ASN database; already-current rows are skipped.
- Do not infer ISP/cloud/CDN identity from `asn_organization` in this asset. Provider
  classification remains a separate downstream dataset with its own provenance.
