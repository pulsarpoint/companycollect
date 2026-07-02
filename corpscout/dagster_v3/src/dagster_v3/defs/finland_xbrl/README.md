# Finland PRH XBRL

Finland XBRL loads financial statements from the PRH open XBRL API into a
partitioned parquet/S3 pipeline, parses the XML with the local lxml parser, builds
mapped financial metrics, converts EUR amounts to USD, and publishes the final
metrics to ClickHouse.

This pipeline does not depend on Finland YTJ eligibility. It pulls report listings
directly from PRH by statement registration date and then downloads every listed
statement XML unless that XML object already exists in S3.

## Source API

Base URL:

```text
https://avoindata.prh.fi/opendata-xbrl-api/v3
```

Report listing endpoint:

```text
GET /all_financial_statements
  ?registeredDateStart=<YYYY-MM-DD>
  &registeredDateEnd=<YYYY-MM-DD>
  &page=<number>
```

Statement XML endpoint:

```text
GET /financial
  ?businessId=<business_id>
  &financialDate=<YYYY-MM-DD>
```

`registrationDate` is the date the statement was registered/submitted/visible in
PRH. `financialDate` is the financial period end date for the statement. The
pipeline partitions by `registrationDate`; it keeps `financialDate` as statement
metadata and uses it as the XML fetch parameter.

PRH only supports registration-date listing searches starting on or after
`2023-07-01`; the current historical backfill starts later than that.

## Download Flow

New fixed listing snapshot asset:

```text
data_snapshot
```

This asset is the first step in the replacement financial reporting design. It
pulls `/all_financial_statements` from `registeredDateStart=2023-07-01` through
the fixed `registeredDateEnd=2026-06-01`, writes only these source columns to
CSV, and skips the operation when the fixed S3 object already exists:

```text
businessId,financialDate,registrationDate
```

Snapshot S3 object:

```text
bucket: source-finland-prh-xbrl
key:    financial_data/snapshot/registeredDateStart=2023-07-01/registeredDateEnd=2026-06-01/financial_statements.csv
```

Daily listing asset:

```text
data_daily
```

This asset uses daily partitions starting at `2026-06-01`. Each partition calls
`/all_financial_statements` with `registeredDateStart` and `registeredDateEnd`
both set to the partition date and writes the same three-column CSV shape as the
fixed snapshot.

Daily S3 object:

```text
bucket: source-finland-prh-xbrl
key:    financial_data/daily/registeredDateStart=<YYYY-MM-DD>/registeredDateEnd=<YYYY-MM-DD>/financial_statements.csv
```

Daily metadata publish chain:

```text
data_daily
-> data_daily_duckdb
-> data_daily_duckdb_ch
-> data_daily_xml
-> data_daily_xml_duckdb
```

`data_daily_duckdb` stores each daily CSV partition in
`data/finland_xbrl/financial_data_daily.duckdb` table
`finland_prh_xbrl.financial_data_daily`. The table includes `partition_key` plus
the same three PRH source columns. Re-materializing a partition deletes and
rewrites only that partition in DuckDB.

`data_daily_duckdb_ch` inserts the selected daily DuckDB partition into
`corpscout.fi_xbrl_financial_statement_listings`. It appends into the shared
metadata table rather than replacing the fixed snapshot load.

`data_daily_xml` reads the same shared ClickHouse listing table for the daily
registration-date partition, downloads or reuses the corresponding XML files, and
writes the same `financial_data/xml_snapshot/registeredDateStart=<date>/registeredDateEnd=<date>/`
S3 layout as historical XML partitions. `data_daily_xml_duckdb` then parses that
daily XML folder into a partition DuckDB using the same parser and table
contracts as `data_snapshot_xml_duckdb`.

Historical XML snapshot asset:

```text
data_snapshot_xml
```

This asset is monthly partitioned from `2023-07-01` through `2026-05-31`. Each
partition reads report metadata from
`corpscout.fi_xbrl_financial_statement_listings` for that registration-date
window, downloads XML with `/financial?businessId=...&financialDate=...`, and
stores XML files in a partition-scoped S3 folder.

XML snapshot S3 layout:

```text
bucket: source-finland-prh-xbrl
key:    financial_data/xml_snapshot/registeredDateStart=<YYYY-MM-DD>/registeredDateEnd=<YYYY-MM-DD>/companies/<business_id>/<financial_date>.xml
key:    financial_data/xml_snapshot/registeredDateStart=<YYYY-MM-DD>/registeredDateEnd=<YYYY-MM-DD>/manifest.jsonl
key:    financial_data/xml_snapshot/registeredDateStart=<YYYY-MM-DD>/registeredDateEnd=<YYYY-MM-DD>/_SUCCESS.json
```

`_SUCCESS.json` is the only partition-complete marker. If it exists, the whole
partition is skipped without querying ClickHouse or PRH. If the marker is missing
but some XML files already exist, those XML files are reused and only missing
documents are downloaded. The marker is written only after the partition finishes
successfully.

Historical XML snapshot parse asset:

```text
data_snapshot_xml_duckdb
```

This asset runs on the same monthly partitions as `data_snapshot_xml`. It requires
the XML snapshot `_SUCCESS.json` marker, reads the partition `manifest.jsonl`,
parses each listed XML object with the shared lxml parser, and writes a local
partition DuckDB:

```text
data/finland_xbrl/duckdb/xml_snapshot_parse/
  partition_key=<YYYY-MM-01>/
    data.duckdb
```

The DuckDB contains:

```text
statement_documents
facts
```

Temporary parquet files are written while parsing and removed after the DuckDB
tables are created.

Existing partitioned flow:

1. `finland_xbrl_financial_reports_backfill` and
   `finland_xbrl_financial_reports_incremental` call
   `XbrlApiResource.iter_financial_report_rows`.
2. `XbrlApiResource` pages `/all_financial_statements` until PRH returns an empty
   `financials` list.
3. Each listing row is written to local parquet under
   `data/finland_xbrl/parquet/financial_reports_<mode>/partition_key=<key>/data.parquet`.
4. `finland_xbrl_raw_xml_documents_backfill` and
   `finland_xbrl_raw_xml_documents_incremental` read the listing parquet for the
   same partition and filter rows to that partition's registration window.
5. For each selected row, the asset builds the deterministic S3 key:

```text
companies/<business_id>/<financial_date>.xml
```

6. If the object already exists and `refresh_existing=false`, the asset reuses it
   and writes a manifest row with `downloaded=false`.
7. Otherwise it calls `/financial?businessId=...&financialDate=...`, stores the XML
   bytes in bucket `source-finland-prh-xbrl`, and writes a manifest row with
   `downloaded=true`, `xml_sha256`, and `xml_size_bytes`.
8. The raw XML manifest is written to local parquet under
   `data/finland_xbrl/parquet/raw_xml_documents_<mode>/partition_key=<key>/data.parquet`.

The PRH HTTP client is a `DltRequestsClient` configured for 6 total attempts,
initial retry delay 30 seconds, max retry delay 480 seconds, and
`Retry-After` support. Listing and XML download assets also have a configurable
1 second delay between PRH requests.

## Partitions

Historical backfill partitions:

```text
MonthlyPartitionsDefinition(start_date="2025-06-01", end_date="2026-06-01")
```

That produces monthly partitions from `2025-06-01` through `2026-05-01`.
`2026-06-01` is excluded from historical backfill.

Incremental partitions:

```text
DailyPartitionsDefinition(
  start_date="2026-06-01",
  end_offset=1,
  hour_offset=6,
  timezone="Europe/Belgrade",
)
```

Daily partitions start at `2026-06-01`.

## Assets

Historical partition chain:

```text
finland_xbrl_financial_reports_backfill
-> finland_xbrl_raw_xml_documents_backfill
-> finland_xbrl_parse_backfill
```

Daily partition chain:

```text
finland_xbrl_financial_reports_incremental
-> finland_xbrl_raw_xml_documents_incremental
-> finland_xbrl_parse_incremental
```

Unpartitioned publish chain:

```text
fi_prh_xbrl_financial_metrics
-> fi_prh_xbrl_financial_metrics_usd
-> finland_xbrl_financial_metrics_clickhouse
```

Marker/catalog assets:

```text
finland_xbrl_raw_xml_documents
fi_prh_xbrl_xml_documents
```

These marker assets only summarize partition manifests. They do not download or
parse XML.

## Jobs And Schedule

Historical backfill job:

```text
finland_xbrl_historical_backfill_job
```

Runs only the monthly historical partition chain. It is intended for the fixed
historical range and should not be used as the daily refresh path.

Daily incremental job:

```text
finland_xbrl_incremental_job
```

Runs the daily listing, raw XML, and parse assets.

Publish job:

```text
finland_xbrl_publish_job
```

Builds metrics from all parsed backfill + incremental parquet files, performs EUR
to USD conversion through the shared exchange-rate client, and replaces
`corpscout.fi_financial_metrics` in ClickHouse.

Schedule:

```text
finland_xbrl_incremental_schedule
```

Runs the daily incremental job at 06:00 according to the partition definition.

## Storage

Raw XML object store:

```text
bucket: source-finland-prh-xbrl
key:    companies/<business_id>/<financial_date>.xml
```

Local parquet resource:

```text
data/finland_xbrl/parquet/
  financial_reports_backfill/partition_key=<month>/data.parquet
  financial_reports_incremental/partition_key=<day>/data.parquet
  raw_xml_documents_backfill/partition_key=<month>/data.parquet
  raw_xml_documents_incremental/partition_key=<day>/data.parquet
  statement_documents_backfill/partition_key=<month>/data.parquet
  statement_documents_incremental/partition_key=<day>/data.parquet
  facts_backfill/partition_key=<month>/data.parquet
  facts_incremental/partition_key=<day>/data.parquet
  financial_metrics/data.parquet
  financial_metrics_usd/data.parquet
```

## Parser And Metrics

`finland_xbrl_parse_*` reads XML bytes from S3 and parses them with
`parse_statement_xml` in `parser.py`. The parser emits:

```text
fi_prh_xbrl_statement_documents
fi_prh_xbrl_facts
```

`fi_prh_xbrl_financial_metrics` joins parsed statement rows to numeric,
non-comparative facts and maps XBRL concepts through
`metric_mapping.xbrl_metric_mapping_rows()`. The mapping lives in Python code, not
in a CSV file.

`fi_prh_xbrl_financial_metrics_usd` uses EUR as the original currency and converts
money columns to USD using the shared `exchange_rates` client. Missing rates raise
and fail the asset.

`finland_xbrl_financial_metrics_clickhouse` replaces
`corpscout.fi_financial_metrics` from the USD parquet.

## Operational Notes

- Existing XML objects are reused by default; set `refresh_existing=true` only when
  the raw XML should be downloaded again.
- Financial report listing partitions overwrite their local parquet file when
  rematerialized.
- The XML S3 key uses only `business_id` and `financial_date`. If PRH ever exposes
  multiple distinct statements for the same company and financial date, this
  storage contract would need a report identifier in the key.
- Parse failures are logged and skipped for that run; failed documents are retried
  on the next parse materialization because no failure marker is persisted.
- XBRL downloads are intentionally not filtered by YTJ active status, website, or
  company seed data.
