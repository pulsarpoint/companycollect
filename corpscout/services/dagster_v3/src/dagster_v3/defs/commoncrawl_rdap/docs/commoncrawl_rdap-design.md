# CommonCrawl RDAP network enrichment

`commoncrawl_ip_addresses`, `commoncrawl_ip_rdap_networks`, and the GeoIP enrichment asset share
256 static Dagster partitions named `bucket_000` through `bucket_255`. ClickHouse assigns each
canonical IP to `cityHash64(ip) % 256`, so adding an address changes one stable bucket rather than
shifting addresses between buckets. `commoncrawl_ip_rdap_networks` remains manual-only.

The asset checks the ClickHouse `rdap_network_trie` before making a remote request. Successful RDAP
start/end ranges are decomposed into exact CIDRs and inserted in this order:

1. `corpscout.rdap_networks`
2. `corpscout.rdap_network_segments`
3. `corpscout.rdap_ip_lookup_results`

This ordering keeps partial failures resumable. Parent registrations are stored with
`segment_role='parent'`, so they remain queryable but do not suppress direct lookups through the
trie.

The trie represents the most-specific registration discovered so far. It is not proof that a more
specific registration does not exist.

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
backlog check below answers the different operational question of whether actionable RDAP work
exists. A new IP can already be covered by the network trie, so a changed source version does not
necessarily mean another RDAP request is needed. Historical child materializations do not have
source-observation provenance. Activate tracking for each RDAP partition by observing its source
bucket and then materializing that child partition once. This can be a one-time manual baseline
across every partition that should be tracked, or it can happen lazily as partitions are next
processed. Until a partition is baselined, the backlog check remains the authoritative update
signal. In Dagster 1.13 the matching child partition can show stale or unsynced in partition
status/details, but the whole-asset graph badge does not aggregate stale partitions.

## Manual update-availability check

`commoncrawl_rdap_update_available` is an unpartitioned, read-only asset check attached to
`commoncrawl_ip_addresses`. An operator evaluates it manually from that asset's Checks view, or
manually launches the `commoncrawl_ip_enrichment_backlog_checks` job. It scans all 256 stable
buckets and reports only buckets containing actionable RDAP work.

Actionability follows the enrichment asset's lookup rules; it is not a comparison of table row
counts. One RDAP network can cover many source IPs. An IP requires work only when the
`rdap_network_trie` does not cover it and it either has no current exact lookup result or has a
`retryable_error` whose retry time is due. Terminal results and retries scheduled for the future do
not count as pending work.

A **Failed** check at WARN severity means manual enrichment work is available, not that a pipeline
run failed. Its metadata separates new uncovered IPs from due retries and includes pending counts,
the actionable `bucket_NNN` partition keys, and `checked_at_utc`. The operator materializes only
the reported `commoncrawl_ip_rdap_networks` partitions and then evaluates the check again:

```bash
uv run dg launch --job commoncrawl_ip_enrichment_backlog_checks
uv run dg launch --job observe_commoncrawl_ip_addresses --partition bucket_007
uv run dg launch --assets commoncrawl_ip_rdap_networks --partition bucket_007
uv run dg launch --job commoncrawl_ip_enrichment_backlog_checks
```

Production operators normally use Dagster's UI; these commands are the local CLI equivalent. A
bucket can remain actionable after one run because `max_requests=250` bounds each materialization,
so the operator may need to materialize it and re-evaluate the check repeatedly. A check result is
a snapshot of the last manual evaluation, and a source observation is a snapshot of its selected
bucket. Neither refreshes when ClickHouse changes. No sensor, schedule, automation condition,
auto-observe interval, or background process evaluates the check, observes the source, or launches
a reported partition.

Migration `000126_corpscout_rdap_dictionary_reader` must be applied after the storage migration.
It creates a passwordless reader restricted to local ClickHouse connections and grants only the
segment table/view reads required by the dictionary. The migration account therefore needs
ClickHouse access-management permission in addition to normal schema DDL permission.

## Safe smoke run

Start with one partition and at most five total RDAP requests, including parent requests:

```bash
uv run dg launch \
  --assets commoncrawl_ip_rdap_networks \
  --partition bucket_000 \
  --config config/commoncrawl_rdap_smoke.yaml
```

After the run, inspect the network, segment, and lookup-result tables plus
`system.dictionaries`. Rerunning the same partition should make no requests for IPs covered by the
newly loaded trie segments.

```sql
SELECT count() FROM corpscout.rdap_networks_current;
SELECT count() FROM corpscout.rdap_network_segments_current;
SELECT count() FROM corpscout.rdap_ip_lookup_results_current;

SELECT name, status, element_count, last_exception
FROM system.dictionaries
WHERE database = 'corpscout' AND name = 'rdap_network_trie';
```
