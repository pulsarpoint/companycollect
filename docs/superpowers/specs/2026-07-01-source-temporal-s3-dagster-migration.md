# Source Migration To Temporal, S3, Dagster, And ClickHouse

## Purpose

Move Corpscout source pipelines to a standard architecture where long-running
external communication is owned by Temporal, durable source data is stored in
S3, Dagster processes only completed S3 data, and ClickHouse is the serving
database.

This applies to initial downloads, daily updates, source API calls, file
downloads, web/API document fetching, and translation or LLM enrichment. Dagster
should not hold worker slots for multi-hour or multi-day external-service work.

## Target Architecture

```text
Temporal schedule or manual Temporal workflow
  -> external source API / file download / translation service
  -> write raw objects and manifests to S3
  -> write _SUCCESS.json or manifest status=complete last

Dagster sensor
  -> detects completed S3 partition/run manifest
  -> launches partitioned Dagster processing job

Dagster assets
  -> parse raw S3 data
  -> normalize to parquet
  -> enrich / FX / translation joins
  -> validate row counts and schema
  -> publish to ClickHouse

ClickHouse
  -> final queryable company, industry, financial, reference, and enrichment tables
```

## Ownership Rules

Temporal owns:

- External HTTP/API/file communication.
- Rate limits, retries, backoff, and `429` handling.
- Multi-day initial downloads and large daily updates.
- Translation, LLM, crawling, and other slow external enrichment.
- Resume/checkpoint behavior.
- Writing raw response objects, downloaded files, shard outputs, and completion
  manifests to S3.

S3 owns:

- Raw source objects.
- Downloaded archives.
- API response JSON/XML/PDF objects.
- Translation/enrichment result shards.
- Per-partition manifests.
- Completion markers.

Dagster owns:

- S3 manifest sensing.
- Partition materialization.
- Parsing, normalization, FX conversion, joins, and validation.
- ClickHouse publishing.
- Asset lineage and data quality metadata.

ClickHouse owns:

- Final current-state tables.
- Historical/partitioned financial tables.
- Reference tables used by applications and downstream jobs.

## Hard Rules

1. Dagster assets must not perform long-running external API/file/translation
   work.
2. Dagster schedules must not duplicate Temporal schedules for the same source
   fetch.
3. Temporal writes data first and writes the completion marker last.
4. Dagster only processes complete manifests.
5. Raw S3 object keys are part of the data contract and must be stable.
6. Rerunning Dagster must not call external services.
7. Rerunning Temporal must skip existing immutable raw objects where possible.
8. Daily ingestion must be partitioned by source event date or source publication
   date, not by Dagster run date unless the source only provides current-state
   snapshots.
9. Initial historical downloads should be one-time Temporal workflows, not
   Dagster backfills.
10. DuckDB may be used as local processing scratch inside Dagster, but it must
    not be the durable landing layer for externally fetched data.

## Standard S3 Layout

Use one source bucket per source family where practical. Keep path structure
predictable:

```text
{source}/raw/snapshot/{object_name}
{source}/raw/updates/date=YYYY-MM-DD/{object_name}
{source}/raw/history/{partition_key}/{object_name}

{source}/manifests/snapshot/run={run_id}/manifest.parquet
{source}/manifests/updates/date=YYYY-MM-DD/manifest.parquet
{source}/manifests/updates/date=YYYY-MM-DD/_SUCCESS.json

{source}/normalized/snapshot/{table}.parquet
{source}/normalized/updates/date=YYYY-MM-DD/{table}.parquet

{source}/enriched/updates/date=YYYY-MM-DD/{table}.parquet
```

For report/document sources, object identity should include the external report
or document id:

```text
{source}/raw_reports/org={org}/year={year}/type={type}/id={report_id}.json
{source}/raw_documents/company={company_id}/document={document_id}/file.pdf
```

## Standard Manifest Contract

Every Temporal workflow writes a manifest with these fields or equivalent:

```text
source
workflow_name
run_id
partition_key
partition_start
partition_end
status
started_at
completed_at
source_event_count
requested_count
fetched_count
skipped_existing_count
not_found_count
failed_count
raw_object_count
raw_object_prefix
manifest_schema_version
error_type
error_message
```

Partition row manifests should include:

```text
partition_key
source_record_id
source_event_id
source_updated_at
external_object_id
org_number or company_id
raw_object_key
payload_hash
payload_size_bytes
status
fetched_at
error_type
error_message
```

Accepted terminal statuses:

```text
complete
complete_with_not_found
failed
```

Dagster processes only `complete` and explicitly allowed
`complete_with_not_found` manifests.

## Dagster Automation Pattern

Dagster should use sensors for externally produced S3 manifests:

```text
sensor polls S3 manifest prefix
  -> finds unprocessed _SUCCESS.json
  -> emits RunRequest for exact partition key
  -> run_key = source + partition_key + manifest hash/version
```

Dagster jobs remain partitioned by the same partition key as the S3 manifest:

```text
{source}_manifest_asset
{source}_normalized_parquet
{source}_enriched_parquet
{source}_clickhouse
```

If the source is snapshot-only, the sensor should trigger one non-partitioned
snapshot job when a new snapshot manifest appears.

## Source Migration Matrix

### Norway BRREG Company

Current state:

- Dagster currently downloads company snapshot and daily updates.
- Daily updates are partitioned in Dagster.
- Company data is normalized to `no_companies`, `no_websites`, and
  `no_industries`.

Target:

- Temporal owns full company snapshot download from
  `/enhetsregisteret/api/enheter/lastned`.
- Temporal owns daily company update download from
  `/enhetsregisteret/api/oppdateringer/enheter`.
- Dagster reads completed S3 raw objects/manifests and produces normalized
  parquet and ClickHouse tables.

S3 contract:

```text
norway_brreg/company/raw/snapshot/entities.json
norway_brreg/company/raw/updates/date=YYYY-MM-DD/entities.parquet
norway_brreg/company/manifests/updates/date=YYYY-MM-DD/_SUCCESS.json
norway_brreg/company/normalized/snapshot/no_companies.parquet
norway_brreg/company/normalized/updates/date=YYYY-MM-DD/no_companies.parquet
```

Jobs:

- Temporal: `norway_brreg_company_snapshot`, manual one-time or explicit refresh.
- Temporal: `norway_brreg_company_daily_update`, scheduled daily.
- Dagster: `norway_brreg_company_snapshot_process_job`.
- Dagster: `norway_brreg_company_daily_process_job`, sensor-triggered.

### Norway BRREG Finance

Current state:

- Historical bootstrap is already a standalone Temporal package.
- Bootstrap reads candidates from ClickHouse and writes raw report JSON to S3.
- Old Dagster financial fetch assets should be retired.

Target:

- Temporal owns historical raw report bootstrap.
- Temporal owns daily finance discovery from company update events where
  `/sisteInnsendteAarsregnskap` changed.
- Dagster reads raw report manifests, parses statements, applies FX, and
  publishes ClickHouse.

S3 contract:

```text
norway_brreg/finance/raw_reports/org={org}/year={year}/type={type}/id={id}.json
norway_brreg/finance/manifests/updates/date=YYYY-MM-DD/reports.parquet
norway_brreg/finance/manifests/updates/date=YYYY-MM-DD/_SUCCESS.json
norway_brreg/finance/statements/updates/date=YYYY-MM-DD/statements.parquet
norway_brreg/finance/statements_usd/updates/date=YYYY-MM-DD/statements.parquet
```

Jobs:

- Temporal: `norway_brreg_finance_historical_bootstrap`, manual.
- Temporal: `norway_brreg_finance_daily_discovery`, scheduled daily.
- Dagster: `norway_brreg_finance_daily_process_job`, sensor-triggered.
- Dagster: optional historical parse/publish job that reads existing S3 raw
  reports only.

### Finland YTJ Company

Current state:

- Dagster uses DLT/DuckDB for YTJ company data.
- It shares resource/pool concerns with other Finland assets.

Target:

- Temporal owns YTJ source download/snapshot acquisition if it contacts the
  external source.
- Dagster normalizes snapshot/update S3 objects and publishes ClickHouse.
- Finland YTJ should be separate from Finland XBRL finance.

S3 contract:

```text
finland_ytj/company/raw/snapshot/{source_object}
finland_ytj/company/manifests/snapshot/run={run_id}/manifest.parquet
finland_ytj/company/normalized/snapshot/fi_companies.parquet
```

Jobs:

- Temporal: `finland_ytj_company_snapshot`, scheduled according to source update
  frequency.
- Dagster: `finland_ytj_company_process_job`, sensor-triggered.

### Finland XBRL Finance

Current state:

- Dagster has monthly historical and daily incremental partitions.
- It downloads PRH XBRL listings and XML, parses XML, creates metrics, applies
  FX, and publishes ClickHouse.

Target:

- Temporal owns PRH financial report listing discovery.
- Temporal owns XML download to S3.
- Dagster reads completed listing/XML manifests, parses XML, builds metrics,
  applies FX, and publishes ClickHouse.
- Historical listing backfill is a one-time Temporal workflow.
- Daily discovery is a Temporal schedule.

S3 contract:

```text
finland_xbrl/listings/history/month=YYYY-MM/reports.parquet
finland_xbrl/listings/updates/date=YYYY-MM-DD/reports.parquet
finland_xbrl/raw_xml/business_id={business_id}/financial_date={date}/statement.xml
finland_xbrl/manifests/updates/date=YYYY-MM-DD/_SUCCESS.json
finland_xbrl/facts/updates/date=YYYY-MM-DD/facts.parquet
finland_xbrl/metrics_usd/updates/date=YYYY-MM-DD/metrics.parquet
```

Jobs:

- Temporal: `finland_xbrl_historical_listing_and_xml_download`, manual.
- Temporal: `finland_xbrl_daily_listing_and_xml_download`, scheduled daily.
- Dagster: `finland_xbrl_process_daily_job`, sensor-triggered.
- Dagster: `finland_xbrl_publish_existing_raw_job`, manual parse/publish from
  existing S3 data.

### Brazil RFB

Current state:

- Dagster downloads/processes large RFB company data and resolves companies,
  contacts, industries, and partners.
- Large downloads and processing can be long-running.

Target:

- Temporal owns external RFB archive discovery/download and S3 raw storage.
- Dagster owns unzip/parse/normalize from S3, ClickHouse publishing, and
  validation.
- If source archives are immutable by month/version, raw S3 keys must include
  source publication/version.

S3 contract:

```text
brazil_rfb/raw/archive_date=YYYY-MM-DD/{archive_name}.zip
brazil_rfb/manifests/snapshot/run={run_id}/manifest.parquet
brazil_rfb/normalized/snapshot/br_companies.parquet
brazil_rfb/normalized/snapshot/br_contacts.parquet
brazil_rfb/normalized/snapshot/br_industries.parquet
```

Jobs:

- Temporal: `brazil_rfb_archive_download`, scheduled or manual by publication.
- Dagster: `brazil_rfb_process_snapshot_job`, sensor-triggered.

### Brazil CNAE

Current state:

- Reference classification data is a smaller source.

Target:

- If it requires external download, Temporal owns download to S3.
- Dagster owns parsing/reference ClickHouse publishing.
- If source is static fixture-only, keep it in Dagster as a reference asset.

S3 contract:

```text
brazil_cnae/raw/snapshot/{source_object}
brazil_cnae/normalized/snapshot/br_cnae.parquet
```

Jobs:

- Temporal only if external fetch is needed.
- Dagster reference process job.

### Czech ARES

Current state:

- Dagster pulls the whole ARES CSV/snapshot and processes to companies and
  industries.

Target:

- Temporal owns snapshot download to S3.
- Dagster reads raw snapshot from S3, normalizes, and publishes ClickHouse.
- Since it is a full snapshot source, daily Dagster processing should not call
  the external source.

S3 contract:

```text
czech_ares/raw/snapshot/ares.csv.gz
czech_ares/manifests/snapshot/run={run_id}/manifest.parquet
czech_ares/normalized/snapshot/cz_companies.parquet
czech_ares/normalized/snapshot/cz_industries.parquet
```

Jobs:

- Temporal: `czech_ares_snapshot_download`, scheduled by source freshness.
- Dagster: `czech_ares_snapshot_process_job`, sensor-triggered.

### France SIRENE

Current state:

- Dagster downloads large SIRENE stock files and processes them.

Target:

- Temporal owns stock archive download from the external source.
- Dagster processes the completed S3 archive into normalized company and
  industry parquet and ClickHouse.

S3 contract:

```text
france_sirene/raw/snapshot/legal_units.zip
france_sirene/raw/snapshot/establishments.zip
france_sirene/manifests/snapshot/run={run_id}/manifest.parquet
france_sirene/normalized/snapshot/fr_companies.parquet
france_sirene/normalized/snapshot/fr_industries.parquet
```

Jobs:

- Temporal: `france_sirene_snapshot_download`, monthly or source-published.
- Dagster: `france_sirene_snapshot_process_job`, sensor-triggered.

### UK Companies House

Current state:

- Dagster handles register snapshots, financial archive downloads, and API/PDF
  financial flows.

Target:

- Temporal owns register archive download.
- Temporal owns accounts archive discovery/download.
- Temporal owns Companies House API/PDF document fetching.
- Dagster parses raw S3 objects and publishes companies, industries, and
  financial metrics/statements.

S3 contract:

```text
uk_companies_house/register/raw/snapshot/{archive_name}.zip
uk_companies_house/accounts/raw/archive={archive_name}/{object}
uk_companies_house/documents/raw/company={company_number}/document={document_id}/{file}
uk_companies_house/manifests/register/run={run_id}/manifest.parquet
uk_companies_house/manifests/accounts/date=YYYY-MM-DD/_SUCCESS.json
```

Jobs:

- Temporal: `uk_companies_house_register_snapshot_download`.
- Temporal: `uk_companies_house_accounts_archive_download`.
- Temporal: `uk_companies_house_document_fetch`, scheduled or queue-driven.
- Dagster: register process job, accounts process job, document parse job.

### Estonia AR

Current state:

- Dagster runs register/general-data/financial jobs and schedules.

Target:

- Temporal owns external register, general data, and financial source downloads.
- Dagster normalizes and publishes ClickHouse from S3.

S3 contract:

```text
estonia_ar/register/raw/snapshot/{source_object}
estonia_ar/general/raw/snapshot/{source_object}
estonia_ar/financial/raw/snapshot/{source_object}
estonia_ar/manifests/{domain}/run={run_id}/manifest.parquet
estonia_ar/normalized/snapshot/ee_companies.parquet
estonia_ar/normalized/snapshot/ee_financials.parquet
```

Jobs:

- Temporal: one source workflow per external endpoint/domain.
- Dagster: one process job per domain, plus ClickHouse publish.

### Latvia UR

Current state:

- Dagster runs register and financial jobs/schedules.

Target:

- Temporal owns external register and financial downloads.
- Dagster owns normalization and ClickHouse publishing.

S3 contract:

```text
latvia_ur/register/raw/snapshot/{source_object}
latvia_ur/financial/raw/snapshot/{source_object}
latvia_ur/manifests/{domain}/run={run_id}/manifest.parquet
latvia_ur/normalized/snapshot/lv_companies.parquet
latvia_ur/normalized/snapshot/lv_financials.parquet
```

Jobs:

- Temporal: `latvia_ur_register_download`, `latvia_ur_financial_download`.
- Dagster: `latvia_ur_register_process_job`, `latvia_ur_financial_process_job`.

### Slovakia RPO

Current state:

- Dagster handles RPO register download/process/publish.

Target:

- Temporal owns external register file/API download.
- Dagster reads completed raw S3, normalizes, and publishes ClickHouse.

S3 contract:

```text
slovakia_rpo/raw/snapshot/{source_object}
slovakia_rpo/manifests/snapshot/run={run_id}/manifest.parquet
slovakia_rpo/normalized/snapshot/sk_companies.parquet
```

Jobs:

- Temporal: `slovakia_rpo_snapshot_download`.
- Dagster: `slovakia_rpo_snapshot_process_job`.

### Slovakia Financials

Current state:

- Dagster has an incremental financials job/schedule.

Target:

- Temporal owns external financial update discovery and raw download.
- Dagster parses completed S3 data and publishes financial ClickHouse tables.

S3 contract:

```text
slovakia_financials/raw/updates/date=YYYY-MM-DD/{object}
slovakia_financials/manifests/updates/date=YYYY-MM-DD/_SUCCESS.json
slovakia_financials/normalized/updates/date=YYYY-MM-DD/financials.parquet
```

Jobs:

- Temporal: `slovakia_financials_daily_download`.
- Dagster: `slovakia_financials_daily_process_job`.

### GLEIF

Current state:

- Dagster has bootstrap and daily delta jobs.
- It tracks state around full bootstrap/deltas.

Target:

- Temporal owns GLEIF full file and delta download.
- Dagster reads S3 manifests and updates reference ClickHouse tables.
- State should be manifest-based rather than hidden inside a Dagster/DuckDB
  fetch step.

S3 contract:

```text
gleif/raw/bootstrap/{lei_full_file}
gleif/raw/deltas/date=YYYY-MM-DD/{delta_file}
gleif/manifests/bootstrap/run={run_id}/manifest.parquet
gleif/manifests/deltas/date=YYYY-MM-DD/_SUCCESS.json
gleif/normalized/current/lei_records.parquet
```

Jobs:

- Temporal: `gleif_reference_bootstrap_download`, manual.
- Temporal: `gleif_reference_delta_download`, scheduled daily.
- Dagster: `gleif_reference_process_job`, sensor-triggered.

### Open Page Rank

Current state:

- Dagster downloads a large ZIP and processes domains.

Target:

- Temporal owns ZIP download to S3.
- Dagster reads existing raw ZIP/CSV from S3, transforms domains, and publishes
  ClickHouse.

S3 contract:

```text
open_page_rank/raw/snapshot/domains.zip
open_page_rank/manifests/snapshot/run={run_id}/manifest.parquet
open_page_rank/normalized/snapshot/domains.parquet
```

Jobs:

- Temporal: `open_page_rank_snapshot_download`, weekly.
- Dagster: `open_page_rank_domains_process_job`, sensor-triggered.

### Wikidata

Current state:

- Dagster builds a company seed/reference set.
- Source communication may involve SPARQL/API or dump-derived data depending on
  implementation.

Target:

- Temporal owns any external Wikidata query/dump download.
- Dagster normalizes the completed S3 output and publishes ClickHouse seed
  tables.

S3 contract:

```text
wikidata/raw/snapshot/{source_object}
wikidata/manifests/snapshot/run={run_id}/manifest.parquet
wikidata/normalized/snapshot/company_seed.parquet
```

Jobs:

- Temporal: `wikidata_company_seed_download`, scheduled weekly or manual.
- Dagster: `wikidata_company_seed_process_job`.

### NACE

Current state:

- Reference taxonomy data is mostly static/staged.

Target:

- Keep static fixtures in Dagster if they are repo-owned.
- If NACE taxonomy is externally refreshed, Temporal downloads the source to S3
  and Dagster publishes reference ClickHouse tables.

S3 contract:

```text
nace/raw/snapshot/{source_object}
nace/normalized/snapshot/nace_categories.parquet
```

Jobs:

- Temporal only if external fetch is required.
- Dagster reference process job.

### Exchange Rates V2

Current state:

- Dagster has a daily exchange-rate job/schedule.

Target:

- If exchange-rate API calls are short and reliable, this can remain in Dagster
  temporarily.
- Long-term standard should move external FX API calls to Temporal daily
  schedule and S3 raw/manifest storage.
- Dagster reads completed FX raw data, validates coverage, and writes
  ClickHouse.

S3 contract:

```text
exchange_rates/raw/date=YYYY-MM-DD/rates.json
exchange_rates/manifests/date=YYYY-MM-DD/_SUCCESS.json
exchange_rates/normalized/date=YYYY-MM-DD/rates.parquet
```

Jobs:

- Temporal: `exchange_rates_daily_download`, scheduled daily.
- Dagster: `exchange_rates_daily_process_job`.

### Domains

Current state:

- Domain table assets derive from already published company/source data.

Target:

- Keep in Dagster. This is internal transformation work, not external
  acquisition.
- If external domain enrichment is introduced, that enrichment should be a
  Temporal workflow writing S3 manifests.

Jobs:

- Dagster-only unless external enrichment is added.

### Translations

Current state:

- Translation can be long-running and uses external LLM/translation services.

Target:

- Temporal owns all translation calls and retry/backoff/rate limits.
- Translation inputs are produced by Dagster/ClickHouse or S3 manifests.
- Temporal writes translated shards and completion manifests to S3.
- Dagster reads completed translation manifests and applies translations to
  normalized/enriched parquet or ClickHouse tables.

S3 contract:

```text
translations/{domain}/requests/run={run_id}/requests.parquet
translations/{domain}/responses/run={run_id}/shard={n}.parquet
translations/{domain}/manifests/run={run_id}/_SUCCESS.json
```

Jobs:

- Temporal: `translation_batch_run`, queue-driven or scheduled.
- Dagster: `translation_apply_job`, sensor-triggered.

## Migration Phases

### Phase 1: Common Contracts

- Create shared S3 manifest schema helpers.
- Create Dagster S3 manifest sensor helper.
- Create Temporal source app template.
- Define naming conventions for source buckets, raw keys, manifests, and
  normalized parquet keys.

### Phase 2: Norway As Reference Implementation

- Finish Norway company split.
- Finish Norway finance daily Temporal workflow.
- Make Dagster process only completed Norway S3 manifests.
- Remove old Norway Dagster external fetch assets.

### Phase 3: Large Snapshot Sources

Migrate sources where external download is large but transformation is local:

- Czech ARES.
- France SIRENE.
- UK Companies House register snapshot.
- Open Page Rank.
- Brazil RFB.

These should be straightforward because Temporal only needs to download raw
archives and write manifests.

### Phase 4: Daily/Incremental API Sources

Migrate sources where daily external communication matters:

- Norway company updates.
- Norway finance updates.
- GLEIF deltas.
- Slovakia financials.
- Exchange rates.
- UK Companies House accounts/API/PDF flows.

### Phase 5: Translation And Enrichment

- Move translation/LLM calls to Temporal.
- Move any web crawling or external enrichment to Temporal.
- Keep Dagster as the apply/publish layer.

## Acceptance Criteria

For each migrated source:

- Dagster source package has no external download/API calls in assets.
- Temporal workflow writes raw S3 data and completion manifests.
- Dagster sensor triggers processing only after completion.
- Dagster reruns do not call external services.
- Raw S3 keys are stable and documented.
- ClickHouse publishing remains in Dagster.
- Initial bootstrap and daily update workflows are separate where the source
  requires both.
- Source README/design document states the Temporal workflows, S3 contracts,
  Dagster assets, and ClickHouse tables.

## Non-Goals

- Do not move local parsing, normalization, FX conversion, or ClickHouse writes
  into Temporal.
- Do not make Dagster a workflow starter for every Temporal job unless there is
  a specific operator need.
- Do not use DuckDB files as durable source-of-truth landing storage.
- Do not store only final ClickHouse rows and discard raw external source data.

