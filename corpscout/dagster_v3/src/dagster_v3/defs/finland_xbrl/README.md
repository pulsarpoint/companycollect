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
contracts as `data_snapshot_xml_duckdb`, under its own daily parse path:

```text
data/finland_xbrl/duckdb/xml_daily_parse/
  partition_key=<YYYY-MM-DD>/
    data.duckdb
```

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

Both tables use the parser contracts from `tables.py`. Temporary parquet files
are written while parsing and removed after the DuckDB tables are created.

Current partitioned flow:

1. `data_snapshot` downloads a fixed historical statement-listing CSV to S3.
2. `data_snapshot_duckdb` parses that CSV into DuckDB.
3. `data_snapshot_duckdb_ch` loads the historical listing rows into
   `corpscout.fi_xbrl_financial_statement_listings`.
4. `data_snapshot_xml` uses that ClickHouse listing table to download historical
   XML files into monthly S3 folders with `manifest.jsonl` and `_SUCCESS.json`.
5. `data_snapshot_xml_duckdb` parses those monthly XML folders into local DuckDB
   files with `statement_documents` and `facts` tables.
6. `data_daily` through `data_daily_xml_duckdb` runs the same flow for daily
   statement-registration partitions starting at `2026-06-01`.

The PRH HTTP client is a `DltRequestsClient` configured for 6 total attempts,
initial retry delay 30 seconds, max retry delay 480 seconds, and
`Retry-After` support.

## Partitions

Historical XML snapshot partitions:

```text
MonthlyPartitionsDefinition(start_date="2023-07-01", end_date="2026-06-01")
```

That produces monthly XML partitions from `2023-07-01` through `2026-05-01`.
`2026-06-01` is excluded from historical XML snapshot processing.

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

Fixed historical listing chain:

```text
data_snapshot
-> data_snapshot_duckdb
-> data_snapshot_duckdb_ch
```

Historical XML partition chain:

```text
data_snapshot_xml
-> data_snapshot_xml_duckdb
```

Daily partition chain:

```text
data_daily
-> data_daily_duckdb
-> data_daily_duckdb_ch
-> data_daily_xml
-> data_daily_xml_duckdb
```

Unpartitioned publish chain:

```text
fi_financial_statements_ch
fi_financial_metrics_parquet
-> fi_financial_metrics_usd_parquet
-> fi_financial_metrics_ch
```

## Jobs And Schedule

Historical listing snapshot job:

```text
finland_xbrl_data_snapshot_job
```

Runs the fixed historical statement-listing CSV, DuckDB parse, and ClickHouse
load.

Historical XML snapshot job:

```text
finland_xbrl_xml_snapshot_job
```

Runs monthly historical XML download and parse partitions. It is intended for the
fixed historical range and should not be used as the daily refresh path.

Daily incremental job:

```text
finland_xbrl_incremental_job
```

Runs the daily listing, DuckDB, ClickHouse listing, XML download, and XML parse
assets.

Publish job:

```text
finland_xbrl_publish_job
```

Publishes from all parsed historical and daily XML DuckDB partitions:

```text
data_snapshot_xml_duckdb
data_daily_xml_duckdb
-> fi_financial_statements_ch
-> fi_financial_metrics_parquet
-> fi_financial_metrics_usd_parquet
-> fi_financial_metrics_ch
```

`fi_financial_statements_ch` replaces `corpscout.fi_financial_statements` from
the parsed `statement_documents` tables. `fi_financial_metrics_parquet` joins
parsed statements to parsed facts and writes original-currency metrics parquet.
`fi_financial_metrics_usd_parquet` performs EUR to USD conversion through the
shared exchange-rate client. `fi_financial_metrics_ch` replaces
`corpscout.fi_financial_metrics` from that USD parquet.

Schedule:

```text
finland_xbrl_incremental_schedule
```

Runs the daily incremental job at 06:00 according to the partition definition.

## Storage

Statement listing and XML object store:

```text
bucket: source-finland-prh-xbrl
key:    financial_data/snapshot/registeredDateStart=2023-07-01/registeredDateEnd=2026-06-01/financial_statements.csv
key:    financial_data/daily/registeredDateStart=<YYYY-MM-DD>/registeredDateEnd=<YYYY-MM-DD>/financial_statements.csv
key:    financial_data/xml_snapshot/registeredDateStart=<YYYY-MM-DD>/registeredDateEnd=<YYYY-MM-DD>/companies/<business_id>/<financial_date>.xml
key:    financial_data/xml_snapshot/registeredDateStart=<YYYY-MM-DD>/registeredDateEnd=<YYYY-MM-DD>/manifest.jsonl
key:    financial_data/xml_snapshot/registeredDateStart=<YYYY-MM-DD>/registeredDateEnd=<YYYY-MM-DD>/_SUCCESS.json
```

Local DuckDB and parquet outputs:

```text
data/finland_xbrl/financial_data_snapshot.duckdb
data/finland_xbrl/financial_data_daily.duckdb
data/finland_xbrl/duckdb/xml_snapshot_parse/partition_key=<month>/data.duckdb
data/finland_xbrl/duckdb/xml_daily_parse/partition_key=<day>/data.duckdb
data/finland_xbrl/parquet/
  financial_metrics/data.parquet
  financial_metrics_usd/data.parquet
```

## Parser And Metrics

`data_snapshot_xml_duckdb` and `data_daily_xml_duckdb` read XML bytes from S3 and
parse them with `parse_statement_xml` in `parser.py`. The parser emits:

```text
statement_documents
facts
```

`fi_financial_metrics_parquet` joins parsed statement rows to numeric,
non-comparative facts and maps XBRL concepts through
`metric_mapping.xbrl_metric_mapping_rows()`. The mapping lives in Python code, not
in a CSV file.

`fi_financial_metrics_usd_parquet` uses EUR as the original currency and converts
money columns to USD using the shared `exchange_rates` client. Missing rates raise
and fail the asset.

`fi_financial_metrics_ch` replaces `corpscout.fi_financial_metrics` from the USD
parquet.

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
