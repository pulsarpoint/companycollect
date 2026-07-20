# Finland Source Design

Finland is split into two Dagster groups because the company register and the
financial statements have different source APIs, storage contracts, partitioning,
and schedules.

```text
finland_ytj  - PRH YTJ company register, resolved company profile tables
finland_xbrl - PRH XBRL financial statement listings, XML, parsed facts, metrics
```

## YTJ Register

`finland_ytj` is a full-snapshot register pipeline.

Source endpoint:

```text
GET https://avoindata.prh.fi/opendata-ytj-api/v3/all_companies
```

The response can be JSON or a zip containing JSON. `YtjApiResource` streams the
response to a temporary file, extracts JSON when the response is zip, and iterates
records with `ijson`.

Primary asset:

```text
finland_ytj_all_companies_duckdb
```

Destination:

```text
data/finland_ytj.duckdb
schema.table = finland_prhytj.all_companies
```

dlt uses `write_disposition="replace"`, so this asset is a full refresh. It has a
non-empty asset check and refuses to replace the table with an empty source.

Resolved dbt models:

```text
finland_ytj_resolved_fi_companies
finland_ytj_resolved_fi_company_addresses
finland_ytj_resolved_fi_names
finland_ytj_resolved_fi_websites
finland_ytj_resolved_fi_industries
```

ClickHouse export:

```text
finland_ytj_resolved_clickhouse
```

Target ClickHouse tables:

```text
corpscout.fi_companies
corpscout.fi_company_addresses
corpscout.fi_names
corpscout.fi_websites
corpscout.fi_industries
```

`fi_company_addresses` contains one current row per `(business_id, address_type)`
from the YTJ bulk payload's `addresses[]` array. PRH address type `1` maps to
`street` and type `2` to `postal`. The model composes the structured street,
building, entrance, apartment, c/o, PO box, and free-format fields into
`address_lines`; it selects the Finnish post-office name when available and
retains postal code, municipality code, country code, and source provenance.
Address registration date and the PRH address source code are retained explicitly.

Job and schedule:

```text
finland_ytj_resolved_job
finland_ytj_resolved_schedule: 45 4 * * * Europe/Belgrade
```

The YTJ DuckDB assets use pool `finland_ytj_duckdb` because the dlt load, dbt
models, and ClickHouse export share one DuckDB file.

## XBRL Financial Statements

`finland_xbrl` is a partitioned financial statement pipeline.

Listing endpoint:

```text
GET https://avoindata.prh.fi/opendata-xbrl-api/v3/all_financial_statements
  ?registeredDateStart=<YYYY-MM-DD>
  &registeredDateEnd=<YYYY-MM-DD>
  &page=<number>
```

XML endpoint:

```text
GET https://avoindata.prh.fi/opendata-xbrl-api/v3/financial
  ?businessId=<business_id>
  &financialDate=<YYYY-MM-DD>
```

Partitioning is based on PRH statement `registrationDate`, not company
registration date and not financial period end. `financialDate` is the statement
period end and is used only as metadata and as the XML endpoint parameter.

Historical partitions:

```text
Metadata snapshot: 2023-07-01 through 2026-06-01
XML snapshot monthly: 2023-07-01 through 2026-05-01
```

Daily partitions:

```text
Daily: 2026-06-01 onward
```

Historical chain:

```text
data_snapshot
-> data_snapshot_duckdb
-> data_snapshot_duckdb_ch
-> data_snapshot_xml
-> data_snapshot_xml_duckdb
```

Daily chain:

```text
data_daily
-> data_daily_duckdb
-> data_daily_duckdb_ch
-> data_daily_xml
-> data_daily_xml_duckdb
```

Publish chain:

```text
fi_financial_statements_ch
-> fi_financial_metrics_parquet
-> fi_financial_metrics_usd_parquet
-> fi_financial_metrics_ch
```

Raw XML storage:

```text
bucket: source-finland-prh-xbrl
key:    financial_data/xml_snapshot/registeredDateStart=<YYYY-MM-DD>/registeredDateEnd=<YYYY-MM-DD>/companies/<business_id>/<financial_date>.xml
key:    financial_data/xml_daily/registeredDateStart=<YYYY-MM-DD>/registeredDateEnd=<YYYY-MM-DD>/companies/<business_id>/<financial_date>.xml
```

Local DuckDB/parquet storage:

```text
data/finland_xbrl/
```

ClickHouse target:

```text
corpscout.fi_xbrl_financial_statement_listings
corpscout.fi_financial_statements
corpscout.fi_financial_metrics
```

The XBRL pipeline does not use YTJ eligibility, website filters, active-company
filters, or company seed tables. It downloads every statement listed by PRH for
the partition registration window and reuses existing XML objects unless the raw
XML partition is incomplete. Completed XML partitions are marked by `_SUCCESS.json`.

## Operational Separation

YTJ and XBRL are intentionally independent:

- YTJ refreshes the company register and resolved company profile tables.
- XBRL refreshes financial statement data and final financial metrics.
- Running the YTJ resolved job does not download XBRL XML.
- Running the XBRL snapshot, incremental, or publish jobs does not refresh YTJ.

See also:

```text
src/dagster_v3/defs/finland_ytj/README.md
src/dagster_v3/defs/finland_xbrl/README.md
```
