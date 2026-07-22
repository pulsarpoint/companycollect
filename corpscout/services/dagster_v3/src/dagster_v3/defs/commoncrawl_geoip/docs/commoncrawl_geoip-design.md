# CommonCrawl IP GeoIP enrichment

## Purpose

Enrich the unique IP addresses found in CommonCrawl DNS A and AAAA records with the
locally installed MaxMind GeoLite2 City and ASN databases. The output is the durable
`corpscout.commoncrawl_ip_geoip` ClickHouse dataset used by downstream provider and
technology inference.

## Source boundaries

- `corpscout.commoncrawl_domain_dns_record_ingest` is the retry-safe DNS record write boundary.
- `corpscout.commoncrawl_domain_dns_records` stores canonical unique DNS resource records.
- `corpscout.commoncrawl_domain_dns_record_sightings` stores their narrow temporal evidence.
- `corpscout.commoncrawl_ip_addresses` is an incremental aggregate registry containing one
  logical canonical IP row with DNS first/last-seen timestamps.
- `MAXMIND_DATABASE_DIRECTORY` contains `GeoLite2-City.mmdb` and
  `GeoLite2-ASN.mmdb` directly. The resource resolves those fixed paths and reads each
  database's embedded build epoch.

This pipeline intentionally does not use dlt or DuckDB. There is no HTTP/file extraction
or relational transformation: each input IP is a local MMDB point lookup and the rows are
inserted directly into the migration-owned ClickHouse table.

## Partitioning and execution

`commoncrawl_ip_addresses` and `commoncrawl_ip_geoip` share 256 static Dagster partitions named
`bucket_000` through `bucket_255`. ClickHouse assigns canonical IP text to a stable bucket with
`cityHash64(ip) % 256`; the same expression is a schema contract and must not change without
rebuilding both IP tables. Adding an address therefore changes one bucket rather than shifting
addresses between buckets.

The asset is manual-only: it has no schedule, sensor, or automation condition. A UI
backfill runs one bucket per Dagster run and the `commoncrawl_geoip` pool serializes runs
initially. Inside a bucket, MMDB results are written in bounded batches. Reruns select only
IPs missing the current City or ASN build epoch, so a partial run is safely resumable.

## Manual source observation

`commoncrawl_ip_addresses` is an observable external asset: Dagster represents the ClickHouse
table but does not materialize it. The `observe_commoncrawl_ip_addresses` job reads one selected
bucket from `commoncrawl_ip_addresses FINAL` and records its logical unique-IP count as the bucket's
data version. It emits an observation only; it never launches GeoIP or RDAP enrichment.

The count intentionally excludes `last_seen`. The registry is additive, so its logical row count
changes when a canonical IP first enters the bucket, while repeated DNS sightings only update
`last_seen`. Observing the same count again records the same data version. Observing a changed count
makes only the matching downstream GeoIP and RDAP partitions stale because all three assets share
the same partition definition.

Observation is manual and partition-specific. Dagster cannot know that ClickHouse changed until an
operator observes the relevant bucket:

```bash
uv run dg launch --job observe_commoncrawl_ip_addresses --partition bucket_007
```

In the UI, launch `observe_commoncrawl_ip_addresses` and explicitly select the relevant
`bucket_NNN` partition. One observation run does not refresh all 256 buckets. A manually selected
partition backfill can establish observations for several buckets, with one bucket executed per
run.

Native staleness means source membership changed since the child partition was materialized. The
backlog check below answers the different operational question of whether actionable GeoIP work
exists. Historical child materializations do not have source-observation provenance. Activate
tracking for each GeoIP partition by observing its source bucket and then materializing that child
partition once. This can be a one-time manual baseline across every partition that should be
tracked, or it can happen lazily as partitions are next processed. Until a partition is baselined,
the backlog check remains the authoritative update signal. In Dagster 1.13 the matching child
partition can show stale or unsynced in partition status/details, but the whole-asset graph badge
does not aggregate stale partitions.

## Manual update-availability check

`commoncrawl_geoip_update_available` is an unpartitioned, read-only asset check attached to
`commoncrawl_ip_addresses`. An operator evaluates it manually from that asset's Checks view, or
manually launches the `commoncrawl_ip_enrichment_backlog_checks` job. It scans all 256 stable
buckets and reports only buckets containing source IPs that have no logical current
`commoncrawl_ip_geoip` row.

This check is intentionally missing-only. An older City or ASN MMDB build epoch does not make the
check fail because that staleness is not a change in `commoncrawl_ip_addresses`. The enrichment
asset still refreshes stale epochs when an operator materializes it.

A **Failed** check at WARN severity means manual enrichment work is available, not that a pipeline
run failed. Its metadata includes pending IP and bucket counts, the actionable `bucket_NNN`
partition keys, and `checked_at_utc`. The operator materializes only the reported
`commoncrawl_ip_geoip` partitions and then evaluates the check again:

```bash
uv run dg launch --job commoncrawl_ip_enrichment_backlog_checks
uv run dg launch --job observe_commoncrawl_ip_addresses --partition bucket_007
uv run dg launch --assets commoncrawl_ip_geoip --partition bucket_007
uv run dg launch --job commoncrawl_ip_enrichment_backlog_checks
```

Production operators normally use Dagster's UI; these commands are the local CLI equivalent. A
check result and a source observation are snapshots of their last manual runs. Neither refreshes
when ClickHouse changes. No sensor, schedule, automation condition, auto-observe interval, or
background process evaluates the check, observes the source, or launches a reported partition.

## ClickHouse model

Migrations `000115_corpscout_commoncrawl_ip_geoip` and
`000122_corpscout_commoncrawl_ip_addresses_incremental` own:

- `commoncrawl_ip_addresses` and its retry-safe incremental materialized view over DNS observations;
- `commoncrawl_ip_geoip`, a `ReplacingMergeTree(enriched_at)` keyed by `(bucket, ip)`;
- `commoncrawl_ip_geoip_current`, a `FINAL` view for consumers that require one current
  row per IP.

The GeoIP row preserves country and registered-country identity, subdivision arrays,
city GeoName identity, nullable coordinates with accuracy radius, timezone, ASN and AS
organization, the matched City/ASN networks, lookup statuses, and both MMDB build epochs.
Coordinates are nullable because `(0, 0)` is valid and cannot represent “missing.”

## Operational notes

- Apply ClickHouse migrations through the existing migration workflow before launching.
- Set `MAXMIND_DATABASE_DIRECTORY` to the directory containing the MMDB files.
- Materialize selected buckets manually. Rematerialize all buckets after installing a new
  City or ASN database; already-current rows are skipped.
- Do not infer ISP/cloud/CDN identity from `asn_organization` in this asset. Provider
  classification remains a separate downstream dataset with its own provenance.
