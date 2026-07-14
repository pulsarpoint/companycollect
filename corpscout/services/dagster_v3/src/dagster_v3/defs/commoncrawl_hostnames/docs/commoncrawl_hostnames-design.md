# CommonCrawl hostname source tracking

`ctlogs_hostnames`, `commoncrawl_domains`,
`commoncrawl_domain_dns_record_observations`, and
`commoncrawl_domain_hostnames` share 16 static Dagster partitions named
`shard_00` through `shard_15`. Each table row is assigned by the existing
`cityHash64(root_domain) % 16` boundary (using `registered_domain` for CT), so
new rows affect one stable shard rather than shifting rows between shards.

## Manual source observation

The three input tables are observable external assets. Dagster represents them
but does not materialize them. The `observe_commoncrawl_hostname_sources` job
queries one selected shard and records a separate data version for each source:

| Source asset | Version watermark | Observation scope |
| --- | --- | --- |
| `ctlogs_hostnames` | `max(last_ingested_at)` | All CT hostname updates in the shard |
| `commoncrawl_domains` | `max(resolved_at)` | Non-empty CommonCrawl root domains in the shard |
| `commoncrawl_domain_dns_record_observations` | `max(loaded_at)` | AXFR observations in the shard |

The DNS source deliberately excludes non-AXFR rows because
`commoncrawl_domain_hostnames` consumes only AXFR observations from that table.
The timestamp watermarks are stable across ClickHouse background merges, unlike
physical row counts for the underlying `AggregatingMergeTree` and
`ReplacingMergeTree` tables. Empty shards use the Unix epoch, so observing an
unchanged source produces the same data version.

Observation is manual and partition-specific:

```bash
uv run dg launch \
  --job observe_commoncrawl_hostname_sources \
  --partition shard_07
```

In the Dagster UI, launch `observe_commoncrawl_hostname_sources` and select the
relevant `shard_NN` partition. The run emits observations for the three source
assets and no materialization. A manual partition backfill can observe several
shards; the backfill policy executes one shard per run.

There is no sensor, schedule, automation condition, or automatic materialization
attached to this job. A changed source version only makes the matching
`commoncrawl_domain_hostnames` partition stale. An operator decides whether and
when to materialize it:

```bash
uv run dg launch \
  --assets commoncrawl_domain_hostnames \
  --partition shard_07
```

## Staleness semantics

Source staleness is an update hint, not an exact pending-row count. The child
asset uses the same three watermarks as independent incremental cursors, which
keeps observation cheap and aligned with materialization. However:

- CT ingestion can advance for a hostname whose registered domain is not in the
  current CommonCrawl domain set.
- `resolved_at` can advance when an existing CommonCrawl domain is processed
  again.
- retrying an existing AXFR observation can advance `loaded_at`.
- a source can advance between its observation and the child materialization;
  the next observation can then conservatively mark the child stale even if that
  materialization already consumed the newer cursor.

Those cases can mark a child shard stale even if its next materialization writes
no effective rows. Computing an exact actionable fingerprint would require
expensive scans or joins over the large source tables, so the operator-facing
status intentionally favors a cheap, merge-safe indication that a source cursor
advanced.

Historical child materializations do not contain source-observation provenance.
To establish a baseline for a shard, first observe all three source assets for
that shard and then materialize `commoncrawl_domain_hostnames` once. After that,
a later source observation with a changed watermark makes only the matching
child shard stale. In Dagster 1.13, inspect partition status/details for this
state; the whole-asset graph badge does not aggregate stale partitions.
