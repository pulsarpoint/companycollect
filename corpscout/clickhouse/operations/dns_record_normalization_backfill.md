# DNS record normalization backfill

Migration 155 introduces the canonical DNS record dimension and monthly sighting fact. The legacy
wide observation table remains the historical backfill source and is not removed by the migration.

## Preconditions

1. Apply ClickHouse migration 155.
2. Deploy the updated `cc-dns-scan` and `cc-dns-axfr` binaries.
3. Confirm both service logs show successful record flushes and `max(loaded_at)` on
   `corpscout.commoncrawl_domain_dns_record_sightings` is advancing. The ingest table uses the
   `Null` engine, so it cannot be checked by counting stored rows.
4. Confirm `max(loaded_at)` on `corpscout.commoncrawl_domain_dns_record_observations` has stopped
   advancing after both services were deployed. This makes the legacy source a stable snapshot.
5. Confirm a recent ClickHouse backup completed successfully.

The new writer path and both backfills are retry-safe. Keep the legacy table until every bucket has
completed and the normalized tables have been validated.

## Run all buckets

Run from `companycollect/corpscout`:

```bash
make clickhouse-dns-record-backfill
```

Do not run `source .env` first. This repository's `.env` is dotenv input rather than a shell script,
and URL values containing characters such as `&` are intentionally stored without shell quoting.
The Make target reads and decodes `CLICKHOUSE_NATIVE_URL` directly from `.env`.

The procedure processes one bucket at a time. For each bucket, it backfills canonical record
definitions first and sightings second. It stops immediately if either query fails.

## Resume after a failure

If bucket 6 fails while backfilling definitions, restart at that bucket:

```bash
make clickhouse-dns-record-backfill START_BUCKET=6
```

If definitions for bucket 6 completed but its sightings failed, avoid repeating the expensive
definition aggregation:

```bash
make clickhouse-dns-record-backfill START_BUCKET=6 END_BUCKET=6 PHASE=sightings
make clickhouse-dns-record-backfill START_BUCKET=7
```

Valid phases are `both`, `records`, and `sightings`. Every operation is logically idempotent through
the target `ReplacingMergeTree` keys, although rerunning a completed sightings bucket temporarily
creates physical retry copies until ClickHouse merges them.

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
WHERE query LIKE '%dns_record%';
```

Inspect physical target rows by source bucket:

```sql
SELECT
    cityHash64(root_domain) % 16 AS bucket,
    formatReadableQuantity(count()) AS physical_rows
FROM corpscout.commoncrawl_domain_dns_record_sightings
GROUP BY bucket
ORDER BY bucket;
```

For logical record counts, query `corpscout.commoncrawl_domain_dns_records_current`. For logical
sighting counts during validation, query `commoncrawl_domain_dns_record_sightings FINAL` because
background replacement is asynchronous.

Do not drop `commoncrawl_domain_dns_record_observations` or its original materialized views as part
of the backfill itself.

## Remove the legacy write path

After all buckets pass validation, both updated writers are confirmed to be using
`commoncrawl_domain_dns_record_ingest`, the legacy table has stopped advancing, and a recent backup
is available, confirm that ClickHouse is currently at migration 155:

```bash
make clickhouse-migrate-version
```

Then apply the dedicated cleanup migration:

```bash
make clickhouse-migrate-up-one
```

Migration 156 drops `commoncrawl_ip_addresses_mv` and `domain_hostnames_ingest_mv`, then permanently
deletes `commoncrawl_domain_dns_record_observations`. It does not remove the corresponding v2 ingest
views or any normalized DNS table. Its down migration can recreate the empty legacy schema and
triggers, but it cannot restore the deleted rows; recovery of those rows requires the backup.

The legacy table is expected to exceed ClickHouse's default 50 GB deletion guard. Migration 156
raises `max_table_size_to_drop` to 150 GB only for this reviewed `DROP TABLE` statement. It does not
change the server-wide protection.

If an earlier copy of migration 156 stopped at the 50 GB guard, the migration is left dirty after
the two legacy views were removed. Those views are no longer active writers, and the table remains
intact. After updating the migration file, reset only the migration metadata and retry:

```bash
make clickhouse-migrate-version
# Expected: 156 (dirty)
make clickhouse-migrate-force VERSION=155
make clickhouse-migrate-up-one
```
