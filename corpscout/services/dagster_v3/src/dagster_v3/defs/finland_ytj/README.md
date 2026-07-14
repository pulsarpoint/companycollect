# Finland PRH YTJ

Finland YTJ loads the PRH all-companies register into DuckDB with dlt, resolves
the raw register rows into normalized company tables with dbt, and exports those
resolved tables to ClickHouse.

YTJ is the company-register pipeline. It does not decide which Finland XBRL
financial statements are downloaded; the XBRL pipeline pulls PRH financial
statement listings directly by registration date.

## Source API

Base URL:

```text
https://avoindata.prh.fi/opendata-ytj-api/v3
```

Full company register endpoint:

```text
GET /all_companies
```

The API returns either a JSON payload or a zip archive containing a JSON file.
The implementation streams the response to a temporary file, extracts the JSON
when needed, and parses company records with `ijson` so the full register does
not have to be loaded into memory as one Python object.

## Ingest Flow

1. `YtjApiResource.iter_all_companies()` calls
   `YtjApiResource.download_all_companies()`.
2. `download_all_companies()` streams `/all_companies` to a temporary file.
3. `_json_path_from_download()` uses the downloaded file directly if it is JSON,
   or extracts the first `.json` member if it is a zip archive.
4. `_iter_companies()` detects whether the JSON root is an array or an object with
   a `companies` array.
5. `finland_ytj_source()` wraps those rows as a dlt source.
6. `finland_ytj_all_companies_duckdb_asset` runs dlt with `write_disposition="replace"`.
7. dlt writes `finland_prhytj.all_companies` into:

```text
data/finland_ytj.duckdb
```

The load refuses to replace the table if PRH returns zero companies.

## DuckDB Table

dlt table:

```text
schema: finland_prhytj
table:  all_companies
```

Important normalized columns produced during ingest:

```text
business_id
registration_date
end_date
last_modified
trade_register_status
status
lifecycle_status
is_active
primary_name
website_url
website_normalized_url
website_host
website_path
website_registered_on
website_ended_on
raw_company
source_run_id
source_record_id
source_payload_hash
```

`raw_company` preserves the original PRH company JSON for dbt models that need
nested arrays such as names and business lines.

## Asset And Pooling

Main ingest asset:

```text
finland_ytj_all_companies_duckdb
```

Asset check:

```text
all_companies_non_empty
```

The dlt load uses Dagster pool:

```text
finland_ytj_duckdb
```

That pool should stay at limit 1 because the YTJ dlt load, dbt models, and
ClickHouse export share the same DuckDB file.

## Resolve Flow

dbt assets read `finland_prhytj.all_companies` and materialize normalized tables
in DuckDB schema:

```text
finland_resolved
```

dbt models:

```text
finland_ytj_resolved_fi_companies
finland_ytj_resolved_fi_names
finland_ytj_resolved_fi_websites
finland_ytj_resolved_fi_industries
```

`finland_ytj_resolved_clickhouse` exports the resolved DuckDB tables to migrated
ClickHouse tables:

```text
corpscout.fi_companies
corpscout.fi_names
corpscout.fi_websites
corpscout.fi_industries
```

`source_payload_hash` remains in DuckDB/dbt staging but is excluded from
ClickHouse export because it is high-cardinality provenance that is not queried.

## Jobs And Schedule

Resolved refresh job:

```text
finland_ytj_resolved_job
```

The job selects `finland_ytj_resolved_clickhouse.upstream()`, so it includes:

```text
finland_ytj_all_companies_duckdb
finland_ytj_resolved_fi_companies
finland_ytj_resolved_fi_names
finland_ytj_resolved_fi_websites
finland_ytj_resolved_fi_industries
finland_ytj_resolved_clickhouse
```

Schedule:

```text
finland_ytj_resolved_schedule
cron: 45 4 * * *
timezone: Europe/Belgrade
```

## Environment

`resolved.py` sets `FINLAND_YTJ_DUCKDB_PATH` from the default DuckDB resource path
at import time. This keeps dbt's `profiles.yml` and the Dagster DuckDB resource
pointing at the same file and prevents stale shell environment variables from
silently sending dbt to another DuckDB file.

## Relationship To Finland XBRL

YTJ and XBRL are separate Dagster groups:

```text
finland_ytj
finland_xbrl
```

Current XBRL financial statement downloads do not use YTJ company eligibility,
active status, website data, or resolved company rows. YTJ produces company
identity/profile tables; XBRL produces financial metrics.
