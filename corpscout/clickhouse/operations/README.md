# ClickHouse data operations

These SQL files preserve explicit recovery operations that are too large or too operationally
sensitive for the non-transactional ClickHouse migration ledger. No service, Dagster job, Make
target, or schedule executes them. Schema remains owned by `clickhouse/migrations`.

## `domain_hostnames` state recovery

The production rollout completed on 2026-07-14:

1. Migration 130 created `domain_hostnames_state` and `domain_hostnames_ingest_mv` while the public
   view continued reading the observation table.
2. Source buckets 0 through 15 were backfilled and validated.
3. Migration 131 switched the public view to grouped state rows.
4. Migration 132 switched the public view to the more efficient ordered `FINAL` read.

The bucket SQL is retained for rebuilding state in another populated installation that is still at
migration 129. It is not needed during normal operation because `domain_hostnames_ingest_mv`
synchronously maintains state for new observation inserts.

### Manual replay

Apply migration 130 by itself before replaying historical data. The materialized view must already
exist so live inserts are captured while the backfill runs. Then execute the backfill and validation
for each bucket with an authenticated, configured ClickHouse client:

```bash
for bucket in {0..15}; do
  clickhouse-client --param_bucket="$bucket" \
    < clickhouse/operations/domain_hostnames_backfill_bucket.sql
  clickhouse-client --param_bucket="$bucket" \
    < clickhouse/operations/domain_hostnames_validate_bucket.sql
done
```

Every stored aggregate is idempotent `min` or `max`, so interrupted buckets are safe to rerun. A
validation can report a transient mismatch if an observation is committed between its source and
state snapshots. Rerun both files for only that bucket. Investigate a mismatch that persists after
the rerun.

After all buckets validate, apply and verify migration 131 before applying migration 132. Do not run
unrestricted `clickhouse-migrate-up` on a populated migration-129 installation because that would
cut readers over before the supervised backfill completes.

Rolling migration 132 down restores the grouped state view. Rolling migration 131 down restores the
source-backed public view while leaving the state table and materialized view active.
