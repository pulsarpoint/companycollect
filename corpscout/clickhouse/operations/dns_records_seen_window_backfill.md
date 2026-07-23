# DNS records seen-window backfill

Migration 161 introduces `corpscout.commoncrawl_domain_dns_records_v2`, an AggregatingMergeTree
that keeps one row per canonical DNS record with `first_seen`/`last_seen` observation bounds. It
replaces the per-scan `commoncrawl_domain_dns_record_sightings` fact, which appended one row per
record per scan (≈1.9 B rows after seven scans) even when nothing changed. A record whose value
changes gets a new `record_id` and therefore a new row with its own disjoint window, so value
history is preserved without per-scan duplication.

## Preconditions

1. Apply ClickHouse migration 161. No writer redeploy is required: both `cc-dns-scan` and
   `cc-dns-axfr` keep inserting into `commoncrawl_domain_dns_record_ingest`, and the new
   `commoncrawl_domain_dns_records_ingest_v2_mv` dual-writes alongside the 000155 triggers.
2. Confirm `max(last_loaded_at)` on `corpscout.commoncrawl_domain_dns_records_v2` is advancing and
   both service logs show successful record flushes.
3. Confirm a recent ClickHouse backup completed successfully.

The dual-write and the backfill are retry-safe: every column of the v2 table merges idempotently
(`min`/`max`/`any`/`groupUniqArrayArray`), so live ingest overlapping the backfill, or a rerun of a
completed bucket, collapses to the same logical row. Do not add `sum`-style counters to this table;
the outbox retry model would silently inflate them.

## Run all buckets

Run from `companycollect/corpscout`:

```bash
make clickhouse-dns-seen-window-backfill
```

Do not run `source .env` first. This repository's `.env` is dotenv input rather than a shell script,
and URL values containing characters such as `&` are intentionally stored without shell quoting.
The Make target reads and decodes `CLICKHOUSE_NATIVE_URL` directly from `.env`.

The procedure processes one bucket at a time: backfill first, then validation. It stops immediately
if either query fails.

## Resume after a failure

If bucket 6 fails during backfill, restart at that bucket:

```bash
make clickhouse-dns-seen-window-backfill START_BUCKET=6
```

If bucket 6's backfill completed but validation failed (for example because a merge had not caught
up), re-run only the validation:

```bash
make clickhouse-dns-seen-window-backfill START_BUCKET=6 END_BUCKET=6 PHASE=validate
make clickhouse-dns-seen-window-backfill START_BUCKET=7
```

Valid phases are `both`, `backfill`, and `validate`.

## Monitor progress

Use `system.processes` for the active bucket:

```sql
SELECT
    elapsed,
    formatReadableQuantity(read_rows) AS read_rows,
    formatReadableSize(read_bytes) AS read_bytes,
    formatReadableQuantity(written_rows) AS written_rows,
    memory_usage,
    query
FROM system.processes
WHERE query LIKE '%dns_records_seen_window%' OR query LIKE '%commoncrawl_domain_dns_records_v2%';
```

Inspect physical target rows by bucket (physical rows exceed logical rows until background merges
collapse them):

```sql
SELECT
    partition AS bucket,
    formatReadableQuantity(sum(rows)) AS physical_rows
FROM system.parts
WHERE database = 'corpscout'
  AND table = 'commoncrawl_domain_dns_records_v2'
  AND active
GROUP BY partition
ORDER BY toUInt8(partition);
```

## Cutover

After all 16 buckets pass validation and the dual-write has been confirmed healthy, confirm the
migration ledger is at 161:

```bash
make clickhouse-migrate-version
```

Then apply the cutover migration:

```bash
make clickhouse-migrate-up-one
```

Migration 162 drops the 000155 record/sighting triggers, renames
`commoncrawl_domain_dns_records → commoncrawl_domain_dns_records_legacy` and
`commoncrawl_domain_dns_records_v2 → commoncrawl_domain_dns_records`, reattaches the v2 trigger to
the canonical name, and recreates `commoncrawl_domain_dns_records_current` with the new columns.
During the rename the v2 trigger's target is briefly missing, so ingest inserts fail for a few
seconds and the writers' durable SQLite outboxes retry them — watch both service logs to confirm
flushes resume. The legacy table and the sightings table are left untouched.

## Cleanup

Soak for at least one full scan cycle. Confirm a fresh ClickHouse backup, then apply migration 163
(`make clickhouse-migrate-up-one`), which permanently drops `commoncrawl_domain_dns_records_legacy`
(~80 GiB) and `commoncrawl_domain_dns_record_sightings` (~27 GiB). Both drops raise
`max_table_size_to_drop` to 150 GB for the reviewed statements only. The down migration recreates
empty schemas; the deleted rows are only recoverable from the backup.

## Query patterns after cutover

Per-scan sighting membership no longer exists; recency of `last_seen` is the liveness signal.

Value history of a domain (a changed value appears as a second row with a disjoint window):

```sql
SELECT name, record_type, value, first_seen, last_seen
FROM corpscout.commoncrawl_domain_dns_records_current
WHERE root_domain = 'example.com'
ORDER BY name, record_type_code, first_seen;
```

Records currently live (seen within the most recent scan window):

```sql
SELECT name, record_type, value
FROM corpscout.commoncrawl_domain_dns_records_current
WHERE root_domain = 'example.com'
  AND last_seen >= now() - INTERVAL 7 DAY;
```

The view uses `FINAL`, which fully merges the `SimpleAggregateFunction` columns. For heavy
analytical scans, the equivalent explicit aggregation over the storage table
(`GROUP BY` the sort key with `min(first_seen)`, `max(last_seen)`, `any(...)` and
`optimize_aggregation_in_order = 1`) avoids FINAL overhead.
