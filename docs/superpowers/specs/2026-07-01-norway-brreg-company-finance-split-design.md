# Norway Brreg Company And Finance Split Design

## Goal

Reorganize the Norway Brreg Dagster implementation into two clear source
packages:

```text
dagster_v3.defs.norway_brreg_company
dagster_v3.defs.norway_brreg_finance
```

Company assets should own only Brreg company/entity data. Finance assets should
own only Brreg financial report discovery, raw report storage, parsing, FX
conversion, and ClickHouse publishing.

The redesign should remove the current mixed graph where company updates can
trigger broad financial fetches for every active updated company. Daily finance
processing must be driven only by financial/accounting update signals from Brreg
entity updates, specifically `/sisteInnsendteAarsregnskap`.

## Current Problems

The current `norway_brreg` package mixes company and financial processing in
one Dagster group and one source API resource.

`norway_brreg_financial_fetches_updates_parquet` reads the daily normalized
`no_companies` update parquet and fetches financial reports for every row where:

```text
is_active = true
last_submitted_accounts_year is not null
```

That is too broad. A company can appear in the daily update feed because its
address, name, phone, website, industry code, employee count, or other
non-financial field changed. Those changes should not trigger financial report
fetches.

The current financial fetch model is also too coarse. It stores one fetch row
per organization/year with `raw_response` as a full JSON response string. That
makes it hard to skip already-stored individual financial reports by report ID.

The current resources are also mixed. One `NorwayBrregApiResource` handles both
company/entity APIs and financial APIs. That obscures the source boundary and
makes it easier for assets to call the wrong endpoint.

## Source Facts

Brreg company current-state data is available as a full current snapshot:

```text
GET /enhetsregisteret/api/enheter/lastned
```

Brreg company updates are available as a date-windowed update stream:

```text
GET /enhetsregisteret/api/oppdateringer/enheter
```

Relevant query parameters:

```text
dato
updatedBefore
oppdateringsid
organisasjonsnummer
includeChanges
page
size
sort
```

With `includeChanges=true`, the response includes an `endringer` array. For
financial report publication, the useful change path is:

```text
/sisteInnsendteAarsregnskap
```

Example observed update:

```json
{
  "oppdateringsid": 24577000,
  "dato": "2026-06-11T18:00:23.412Z",
  "organisasjonsnummer": "980345106",
  "endringstype": "Endring",
  "endringer": [
    {
      "op": "replace",
      "path": "/sisteInnsendteAarsregnskap",
      "value": "2025"
    }
  ]
}
```

This update gives `org_number` and `accounts_year`, but not the financial report
ID.

Brreg financial reports are available by organization number and optional year:

```text
GET /regnskapsregisteret/regnskap/{orgNummer}
GET /regnskapsregisteret/regnskap/{orgNummer}?år={year}
GET /regnskapsregisteret/regnskap/{orgNummer}?år={year}&regnskapstype=SELSKAP
GET /regnskapsregisteret/regnskap/{orgNummer}?år={year}&regnskapstype=KONSERN
```

The response contains financial report IDs:

```json
{
  "id": 6697842,
  "journalnr": "2026428651",
  "regnskapstype": "SELSKAP",
  "regnskapsperiode": {
    "fraDato": "2025-01-01",
    "tilDato": "2025-12-31"
  }
}
```

An exact report can be fetched by ID:

```text
GET /regnskapsregisteret/regnskap/{orgNummer}/{id}
```

There is no known public endpoint that returns all financial reports submitted
in a date window. Daily finance must therefore use company update events as the
candidate signal and then discover report IDs through the financial endpoint.

There is also no known public endpoint for "all companies as of historical date
X". Company processing should start from one current full snapshot and process
daily updates only from the cutover date forward.

## Target Package Layout

```text
src/dagster_v3/defs/
  norway_brreg_company/
    __init__.py
    definitions.py
    resources.py
    storage.py
    records.py
    normalize.py
    assets/
      __init__.py
      snapshot.py
      updates.py
      normalized.py
      clickhouse.py
      jobs.py

  norway_brreg_finance/
    __init__.py
    definitions.py
    resources.py
    storage.py
    reports.py
    normalize.py
    assets/
      __init__.py
      report_updates.py
      statements.py
      fx.py
      clickhouse.py
      jobs.py
```

The existing `dagster_v3.defs.norway_brreg` package should be retired once the
new packages are wired. During migration, compatibility imports may exist only
as a temporary step and should not remain in the final design.

## Resource Boundaries

### Company API Resource

`NorwayBrregCompanyApiResource` should only talk to Brreg company/entity APIs.
It should not write to S3, DuckDB, or ClickHouse.

Methods:

```text
download_entities_snapshot() -> bytes
iter_entity_updates(start, end, include_changes=False) -> Iterator[dict]
get_entity(org_number) -> dict
```

The resource owns HTTP configuration:

```text
base_url
user_agent
timeout_seconds
update_page_size
retry/backoff configuration if needed
```

### Finance API Resource

`NorwayBrregFinanceApiResource` should only talk to Brreg financial APIs. It
should not write to S3, DuckDB, or ClickHouse.

Methods:

```text
get_reports_for_year(org_number, year) -> list[dict]
get_report_by_id(org_number, report_id) -> dict
```

The daily pipeline should normally call `get_reports_for_year`. It should call
`get_report_by_id` only when it already knows a report ID and needs an exact
refresh.

### Storage Resources

Storage resources should own S3 paths and object-store operations. API resources
should not know S3 paths.

```text
NorwayBrregCompanyStorageResource
NorwayBrregFinanceStorageResource
```

These resources may use the existing `ObjectStoreResource` internally. They
should expose domain methods that keep path construction consistent, such as:

```text
company_snapshot_exists()
write_company_snapshot(...)
read_company_snapshot()
write_company_update_partition(partition_date, ...)
read_company_update_partition(partition_date)

raw_report_exists(org_number, year, report_type, report_id)
write_raw_report(org_number, year, report_type, report_id, ...)
read_raw_report(...)
write_report_update_manifest(partition_date, ...)
read_report_update_manifest(partition_date)
```

Do not create generic service/facade layers around these resources. The asset
functions should call the API resource and storage resource directly.

## Company Pipeline

The company pipeline has three levels:

```text
source download -> normalized parquet -> ClickHouse tables
```

### Company Assets

```text
norway_brreg_company_snapshot_s3
norway_brreg_company_updates_s3

norway_brreg_company_snapshot_parquet
norway_brreg_company_updates_parquet

norway_brreg_company_no_companies_clickhouse
norway_brreg_company_no_websites_clickhouse
norway_brreg_company_no_industries_clickhouse
```

`norway_brreg_company_updates_s3` and
`norway_brreg_company_updates_parquet` are daily-partitioned.

`norway_brreg_company_snapshot_s3` and
`norway_brreg_company_snapshot_parquet` are not partitioned.

### Company Snapshot Behavior

The full company snapshot is current-state data from Brreg. It should be
downloaded once and stored under a stable S3 key.

Suggested key:

```text
norway_brreg/company/raw/snapshot/entities.json
```

If the object already exists, the asset must skip downloading and return
metadata indicating the snapshot already existed.

Example behavior:

```text
if company snapshot object exists:
    log "snapshot exists; skipping download"
    return MaterializeResult(existing=True, downloaded=False)
else:
    download /enheter/lastned
    write snapshot object
    return MaterializeResult(existing=False, downloaded=True)
```

The snapshot key should not include a date. `downloaded_at` belongs in metadata
or an optional manifest, not in the object key.

### Company Daily Update Behavior

Daily company updates are partitioned by date.

For partition `YYYY-MM-DD`, the source window is:

```text
YYYY-MM-DDT00:00:00.000Z
YYYY-MM-DDT23:59:59.999Z
```

The daily update asset downloads that day's company update feed and writes the
raw update object for that partition.

Suggested key:

```text
norway_brreg/company/raw/updates/date=YYYY-MM-DD/entities.parquet
```

Daily update partitions may be overwritten on rerun. Unlike the snapshot, daily
partitions do not need skip-if-exists behavior. Rerunning a daily partition
should refresh the deterministic date-window result.

### Company Normalization

The snapshot parquet asset reads the full snapshot raw object and produces the
normalized table parquets for all companies.

The updates parquet asset reads one daily update raw object and produces
normalized update table parquets for that partition.

Suggested normalized keys:

```text
norway_brreg/company/normalized/snapshot/no_companies.parquet
norway_brreg/company/normalized/snapshot/no_websites.parquet
norway_brreg/company/normalized/snapshot/no_industries.parquet

norway_brreg/company/normalized/updates/date=YYYY-MM-DD/no_companies.parquet
norway_brreg/company/normalized/updates/date=YYYY-MM-DD/no_websites.parquet
norway_brreg/company/normalized/updates/date=YYYY-MM-DD/no_industries.parquet
norway_brreg/company/normalized/updates/date=YYYY-MM-DD/affected_orgs.parquet
norway_brreg/company/normalized/updates/date=YYYY-MM-DD/removed_orgs.parquet
```

The normalized schema should keep the current `no_*` table contracts used by
ClickHouse.

### Company ClickHouse Publishing

Snapshot ClickHouse assets should replace full company tables.

Daily update ClickHouse assets should apply only affected orgs from the update
partition. They should remove old rows for affected orgs and insert replacement
rows from the normalized update parquets. Removed orgs should delete rows where
appropriate.

Company ClickHouse publishing should not call finance code.

## Finance Pipeline

The finance pipeline has four levels:

```text
daily report discovery/storage -> parsed parquet -> FX parquet -> ClickHouse
```

There is no Dagster financial historical backfill job in this design. Historical
financial download is performed by the external/bootstrap application, which
writes raw report JSON objects to S3. Dagster reads those raw report objects for
parsing and publishing where needed.

### Finance Assets

Daily assets:

```text
norway_brreg_finance_report_updates_s3
norway_brreg_finance_statements_parquet
norway_brreg_finance_statements_usd_parquet
norway_brreg_finance_statements_clickhouse
```

All daily finance assets are daily-partitioned.

There is no finance snapshot/backfill Dagster download job.

If a one-time historical publish is needed after the external bootstrap writes
raw report objects, it should be a separate parse/publish job that reads
existing S3 raw reports only. It must not call the Brreg financial API.

### Finance Daily Discovery

For partition `YYYY-MM-DD`, finance daily discovery reads Brreg entity updates
directly with:

```text
includeChanges=true
```

It keeps only updates where an `endringer` entry has:

```text
path == "/sisteInnsendteAarsregnskap"
```

For each matching update:

```text
org_number = update.organisasjonsnummer
year = update.endringer[].value
```

The asset then calls:

```text
GET /regnskapsregisteret/regnskap/{org_number}?år={year}
```

The response contains one or more reports with `id`, `journalnr`,
`regnskapstype`, and `regnskapsperiode`.

For each report, build the raw report key and skip storage if that exact object
already exists.

Suggested raw report key:

```text
norway_brreg/finance/raw_reports/org={org_number}/year={year}/type={regnskapstype}/id={id}.json
```

If the object is missing, write the raw JSON report.

If the object exists, do not rewrite it and mark the manifest row as skipped.

### Finance Manifest

The daily report update asset writes a manifest parquet for the partition.

Suggested key:

```text
norway_brreg/finance/manifests/updates/date=YYYY-MM-DD/reports.parquet
```

Columns:

```text
partition_date
update_id
update_published_at
org_number
accounts_year
report_id
journal_number
report_type
period_start_date
period_end_date
raw_report_key
payload_hash
payload_size_bytes
status
fetched_at
error_type
error_message
```

Suggested status values:

```text
fetched
skipped_existing
no_reports
not_found
retryable_error
invalid_payload
```

Retryable errors must fail the partition after writing diagnostics. They should
not create raw report objects and should not be treated as completed work.

### Finance Parsing

`norway_brreg_finance_statements_parquet` reads the partition manifest and
loads raw report JSON objects referenced by successful rows.

It parses report JSON into normalized financial statement rows.

Suggested key:

```text
norway_brreg/finance/statements/updates/date=YYYY-MM-DD/statements.parquet
```

The parsed statement schema should keep the current resolved financial table
contract, including:

```text
country_iso2
source_system
source_run_id
source_record_id
org_number
legal_name
last_submitted_accounts_year
filing_id
journal_number
accounts_type
legal_form_code
is_parent_company
period_start_date
period_end_date
fiscal_year
currency
financial metric amount columns
source_url
resolved_at
```

The parser should not call Brreg APIs. It reads only S3 raw report objects.

### Finance FX Conversion

`norway_brreg_finance_statements_usd_parquet` reads the native-currency
statement parquet for the partition and adds USD fields.

Suggested key:

```text
norway_brreg/finance/statements_usd/updates/date=YYYY-MM-DD/statements.parquet
```

FX conversion should use the existing exchange-rate system and should preserve
the current financial table contract:

```text
*_amount_original
*_amount_usd
fx_rate_to_usd
fx_rate_date
fx_source
```

### Finance ClickHouse Publishing

`norway_brreg_finance_statements_clickhouse` reads the USD parquet partition and
applies only affected financial statement rows.

The natural replacement key should include:

```text
org_number
filing_id
accounts_type
period_end_date
```

Before inserting the replacement rows, ClickHouse publishing should remove any
existing rows matching the same affected report identities. It should not
replace unrelated historical reports for the same organization.

Finance ClickHouse publishing should not depend on company snapshot/update
assets. It may rely on ClickHouse tables existing, but the job should not pull
company assets into the finance graph.

## Jobs And Schedules

### Company Jobs

```text
norway_brreg_company_snapshot_job
norway_brreg_company_updates_job
```

`norway_brreg_company_snapshot_job` includes:

```text
norway_brreg_company_snapshot_s3
norway_brreg_company_snapshot_parquet
norway_brreg_company_no_companies_clickhouse
norway_brreg_company_no_websites_clickhouse
norway_brreg_company_no_industries_clickhouse
```

`norway_brreg_company_updates_job` includes:

```text
norway_brreg_company_updates_s3
norway_brreg_company_updates_parquet
norway_brreg_company_no_companies_clickhouse
norway_brreg_company_no_websites_clickhouse
norway_brreg_company_no_industries_clickhouse
```

The update job should be scheduled daily with
`dg.build_schedule_from_partitioned_job`.

### Finance Jobs

```text
norway_brreg_finance_daily_job
```

The finance daily job includes:

```text
norway_brreg_finance_report_updates_s3
norway_brreg_finance_statements_parquet
norway_brreg_finance_statements_usd_parquet
norway_brreg_finance_statements_clickhouse
```

The finance job should be scheduled daily with
`dg.build_schedule_from_partitioned_job`.

The finance daily job should not include company assets. It makes its own
`includeChanges=true` call to the entity update API because financial report
publication is inferred from the entity update stream, not from normalized
company table processing.

## S3 Object Layout

Company:

```text
norway_brreg/company/raw/snapshot/entities.json
norway_brreg/company/raw/updates/date=YYYY-MM-DD/entities.parquet

norway_brreg/company/normalized/snapshot/no_companies.parquet
norway_brreg/company/normalized/snapshot/no_websites.parquet
norway_brreg/company/normalized/snapshot/no_industries.parquet

norway_brreg/company/normalized/updates/date=YYYY-MM-DD/no_companies.parquet
norway_brreg/company/normalized/updates/date=YYYY-MM-DD/no_websites.parquet
norway_brreg/company/normalized/updates/date=YYYY-MM-DD/no_industries.parquet
norway_brreg/company/normalized/updates/date=YYYY-MM-DD/affected_orgs.parquet
norway_brreg/company/normalized/updates/date=YYYY-MM-DD/removed_orgs.parquet
```

Finance:

```text
norway_brreg/finance/raw_reports/org={org_number}/year={year}/type={report_type}/id={report_id}.json

norway_brreg/finance/manifests/updates/date=YYYY-MM-DD/reports.parquet
norway_brreg/finance/statements/updates/date=YYYY-MM-DD/statements.parquet
norway_brreg/finance/statements_usd/updates/date=YYYY-MM-DD/statements.parquet
```

Historical finance bootstrap writes only raw report JSON objects under
`raw_reports`. Dagster finance assets can parse and publish those objects, but
the daily finance job does not perform historical backfill.

## Error Handling

Company snapshot:

```text
existing object -> skip and succeed
download failure -> fail asset
empty snapshot -> fail asset
invalid payload -> fail asset
```

Company update partition:

```text
empty update list -> valid empty parquet and succeed
download failure -> fail asset
invalid payload -> fail asset
```

Finance daily discovery:

```text
no /sisteInnsendteAarsregnskap changes -> valid empty manifest and succeed
org/year returns no reports -> manifest row status no_reports and succeed
report object already exists -> manifest row status skipped_existing and succeed
new report object stored -> manifest row status fetched and succeed
429 / timeout / 5xx after retry budget -> manifest diagnostics and fail
invalid JSON payload -> manifest diagnostics and fail
```

Retryable failures must never write canonical raw report objects. This prevents
temporary source failures from becoming permanent "completed" markers.

## Migration Plan

1. Create `norway_brreg_company` package and move company-only API/resource,
   storage, normalization, and ClickHouse code into it.
2. Create company snapshot and daily update assets with the stable snapshot key
   and daily partition keys described above.
3. Create company jobs and daily schedule.
4. Create `norway_brreg_finance` package and move finance-only API/resource,
   storage, parser, FX, and ClickHouse code into it.
5. Replace broad finance candidate selection with
   `/sisteInnsendteAarsregnskap` change detection from entity updates with
   `includeChanges=true`.
6. Change finance raw storage from coarse org/year fetch parquet to report-level
   JSON objects keyed by org/year/type/id.
7. Add daily finance manifest parquet and make parsing read report keys from
   the manifest.
8. Wire finance daily job and schedule.
9. Remove old mixed `norway_brreg` assets and jobs once new definitions pass
   tests and `dg check defs`.
10. Keep the external historical finance bootstrap app as the only historical
    financial downloader.

## Tests

Company tests:

```text
snapshot asset skips download when stable S3 snapshot object exists
snapshot asset downloads and writes when object is missing
daily update asset writes partition object and allows empty updates
daily update asset uses partition date as UTC day window
company normalization produces no_companies, no_websites, no_industries
company ClickHouse snapshot replaces full tables
company ClickHouse update applies affected_orgs and removed_orgs
company job membership excludes finance assets
```

Finance tests:

```text
finance daily discovery calls entity updates with includeChanges=true
finance daily discovery ignores non-financial change paths
finance daily discovery keeps /sisteInnsendteAarsregnskap changes
finance discovery calls /regnskap/{org}?år={year}, not /regnskap/{org}
finance discovery stores missing report IDs as raw JSON
finance discovery skips already-existing report IDs
finance discovery writes manifest rows for fetched and skipped reports
finance discovery fails on retryable source failures
finance parser reads raw report JSON from manifest keys
finance FX asset adds USD columns
finance ClickHouse asset replaces only affected report identities
finance daily job membership excludes company assets
```

Definition checks:

```text
uv run pytest tests/test_norway_brreg_company*.py tests/test_norway_brreg_finance*.py -q
DAGSTER_HOME=$(mktemp -d) uv run dg check defs
```

## Non-Goals

Do not reconstruct historical company state before the first full snapshot.
Brreg does not provide an as-of full company snapshot endpoint.

Do not add a Dagster financial historical download/backfill job. Historical
finance raw report download is owned by the external/bootstrap app.

Do not make finance depend on company ClickHouse tables or company Dagster
assets.

Do not store raw financial API responses as giant strings inside a parquet row.
Raw reports should be JSON objects on S3. Parquet should be used for manifests,
normalized statements, FX-enriched statements, and ClickHouse staging.

Do not add generic service layers or one-implementation interfaces around API
and storage resources. Keep assets direct and source-specific.

## Acceptance Criteria

The final Norway BRREG Dagster UI shows separate groups/packages for company
and finance.

Company jobs do not materialize finance assets.

Finance jobs do not materialize company assets.

Running the company snapshot job twice downloads from Brreg only the first time.

Running a company daily partition downloads only that partition's company update
window.

Running a finance daily partition fetches financial reports only for update
events where `/sisteInnsendteAarsregnskap` changed.

Finance daily discovery fetches by `org + year`, discovers report IDs, and
stores only missing report ID JSON objects.

Retryable finance source failures fail the partition and do not create
completed raw report markers.

Parsed and FX parquet assets read only S3 data produced by upstream storage
assets and do not call Brreg APIs.
