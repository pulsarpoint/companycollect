# EODHD market-data source design

## Scope

The source maintains EODHD exchange and symbol reference tables plus end-of-day
OHLCV prices in `corpscout.eodhd_eod_prices`.

## Price partitions

Historical prices use one Dagster partition per calendar year:

- `2020`: 2020-07-01 through 2020-12-31
- `2021` through `2025`: full calendar years
- `2026`: 2026-01-01 through 2026-06-30

Daily prices use one Dagster partition per date beginning 2026-07-01. This is a
deliberate deviation from the project's normal monthly time-series guidance:
EODHD's bulk endpoint has a natural one-day window, and a daily partition gives
an exact, independently retryable ingestion unit.

Price S3 keys are deterministic and contain no Dagster run ID:

```text
prices/history/year=2021/symbols/CDR.WAR.json.gz
prices/history/year=2021/catalog.json.gz
prices/daily/date=2026-07-22/prices.json.gz
```

Every object records its covered date range or covered symbols. Historical and
daily extraction checks this metadata before making an HTTP request. A retry
therefore reuses complete S3 data and requests only missing coverage.

## Legacy S3 inspection

The old `prices/partition=bucket_*/run_id=*` prefix was inspected once on the
Dagster host and contained no objects. The one-time script was then deleted;
legacy migration and cleanup are intentionally not part of the Dagster graph.

## Normalized storage

Each history year or daily date is parsed into its own DuckDB file and appended
to the migration-owned ClickHouse `eodhd_eod_prices` table. Price lineage uses
deterministic values such as `history:2021` and `daily:2026-07-22` rather than a
Dagster execution ID.

The weekly reference snapshot remains a separate concern and is not deleted by
the price migration.

## Jobs and schedule

- `eodhd_price_history_backfill_job`: fills missing historical coverage and
  publishes it to ClickHouse.
- `eodhd_price_daily_job`: fetches one daily partition.
- `eodhd_price_daily_schedule`: runs at 06:15 UTC; its operational state is
  managed in Dagster.
