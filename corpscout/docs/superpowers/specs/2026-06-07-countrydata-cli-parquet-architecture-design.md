# Countrydata CLI Parquet Architecture Design

## Purpose

Countrydata modules need to scale to roughly 200 countries and multiple data
sources per country without turning Corpscout into one large source-specific
ingestion application.

The architecture will make each country module an isolated batch data product.
Source modules download and normalize their own input data. Country modules merge
one or more source exports into a final country export. Corpscout imports the
final country export into central PostgreSQL.

## Decision

Use a CLI-first execution contract for countrydata modules.

Each country module must provide command-line operations that can be executed
locally, in containers, and by Temporal activities. The stable data contract is
Parquet files plus a JSON manifest. HTTP or gRPC APIs are optional future wrappers
around the same operations, not the primary contract.

DuckDB may be used internally for local processing, staging, profiling, and
normalization. It is not the required external contract. Parquet is mandatory as
the output contract.

## Architecture

```text
countrydata country app
  sync-source     -> download and normalize one source
  status-source   -> inspect one source state
  export-source   -> produce source-normalized Parquet
  status          -> inspect all source and final export state
  build-export    -> merge source exports into final country Parquet
  sync            -> run source sync and optionally build final export

central Corpscout
  registers country/source modules
  starts module commands through Temporal/container execution
  reads manifests from disk or object storage
  imports final country Parquet into central PostgreSQL
  tracks source status, run status, and import status
```

Corpscout central must not know source-specific parsing rules. It may know:

- country code
- source slug
- command image/version
- command arguments
- manifest path
- export schema version
- run status and audit metadata

## Module Layout

Finland PRH YTJ should become the reference implementation for the pattern.

```text
corpscout/countrydata/
  finland/
    country.go
    status.go
    export.go
    merge_rules.go

    prhytj/
      source.go
      download.go
      normalize.go
      export.go
      manifest.go
      types.go
      testdata/

  cmd/finland-countrydata/
    main.go
```

The package-level API stays concrete and source-specific. Avoid generic service
or registry interfaces unless there is a real boundary with multiple
implementations.

## CLI Contract

The Finland country app should expose these operations:

```bash
finland-countrydata sync-source --source prhytj --data-dir /data
finland-countrydata status-source --source prhytj --data-dir /data
finland-countrydata export-source --source prhytj --data-dir /data
finland-countrydata status --data-dir /data
finland-countrydata build-export --data-dir /data
finland-countrydata sync --source prhytj --build-export --data-dir /data
```

All commands must:

- write structured logs with `log/slog`
- return a non-zero exit code on failure
- write a JSON result to stdout for automation
- avoid printing secrets
- write durable state and manifests under `--data-dir`

The JSON stdout result is for execution status. The manifest is the durable data
contract.

## Data Directory Contract

```text
/data/
  sources/
    prhytj/
      snapshots/
        prh_ytj_v3_companies_<timestamp>.ndjson
      work/
        prh_ytj_v3.duckdb
      exports/
        <run-id>/
          companies.parquet
          company_names.parquet
          legal_forms.parquet
          industries.parquet
          addresses.parquet
          registered_entries.parquet
          tax_registrations.parquet
          websites.parquet
          manifest.json
      state.json

  final/
    exports/
      <run-id>/
        companies.parquet
        company_names.parquet
        identifiers.parquet
        addresses.parquet
        industries.parquet
        websites.parquet
        source_evidence.parquet
        manifest.json
    state.json
```

Source exports are inputs to country-level merge logic. Central Corpscout imports
only final country exports by default.

## Source Export Contract

A source export is normalized enough to be mergeable but still source-specific.
It must preserve lineage.

Required source export files for PRH YTJ:

- `companies.parquet`
- `company_names.parquet`
- `legal_forms.parquet`
- `industries.parquet`
- `addresses.parquet`
- `registered_entries.parquet`
- `tax_registrations.parquet`
- `websites.parquet`
- `manifest.json`

Every row must include enough lineage to trace it back to the source data:

```text
country_iso2
source_slug
source_run_id
source_record_id
source_native_id
source_payload_hash
source_item_hash
source_updated_at
exported_at
schema_version
```

The source export can contain source-specific columns, but common fields should
use shared names wherever possible.

## Final Country Export Contract

The final country export is the country-level product contract. It combines all
source exports for a country and applies merge rules.

Required final export files:

- `companies.parquet`
- `company_names.parquet`
- `identifiers.parquet`
- `addresses.parquet`
- `industries.parquet`
- `websites.parquet`
- `source_evidence.parquet`
- `manifest.json`

Every final company row must have:

```text
country_company_id
country_iso2
primary_source_slug
primary_source_record_id
business_id
legal_name
legal_name_en
legal_name_normalized
lifecycle_status
is_active
vat_id
euid
legal_form_code
legal_form_label
legal_form_label_en
primary_industry_code
primary_nace_code
primary_nace_revision
website_normalized_url
source_payload_hash
profile_hash
merge_rule_version
is_translated
exported_at
```

The final export should not hide lineage. Field-level source evidence belongs in
`source_evidence.parquet`.

## Merge Rules

Country-level merge logic must be explicit and versioned.

For Finland v1, with PRH YTJ as the only source:

```text
legal name         -> PRH YTJ
active status      -> PRH YTJ trade register status
VAT status         -> PRH YTJ registered entries
industry/NACE      -> PRH YTJ main business line mapped to NACE when possible
website            -> PRH YTJ website
address            -> PRH YTJ current visiting/postal address
```

For countries with multiple sources, `merge_rules.go` owns source priority and
conflict resolution. Examples:

```text
official registry name       -> official registry source wins
tax registration status      -> tax authority source wins when present
employee count               -> statistics source wins when present
website                      -> verified website source wins over registry text
industry/NACE                -> highest-confidence mapped source wins
```

Each merged field should record:

- final table
- final column
- source slug
- source record id
- source item hash
- merge rule name
- confidence

## Manifest Contract

Each source and final export writes a `manifest.json`.

Required fields:

```json
{
  "manifest_version": "countrydata.export.v1",
  "country_iso2": "FI",
  "source_slug": "prhytj",
  "export_kind": "source",
  "run_id": "20260607T120000Z-prhytj",
  "schema_version": "finland.prhytj.source.v1",
  "merge_rule_version": "",
  "created_at": "2026-06-07T12:00:00Z",
  "inputs": [
    {
      "path": "snapshots/prh_ytj_v3_companies_20260607T110000Z.ndjson",
      "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    }
  ],
  "files": [
    {
      "name": "companies",
      "path": "companies.parquet",
      "row_count": 123,
      "sha256": "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
      "schema_hash": "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
    }
  ],
  "records_seen": 123,
  "records_exported": 123,
  "decode_errors": 0,
  "warnings": []
}
```

For final country exports:

```json
{
  "export_kind": "final",
  "source_slug": null,
  "schema_version": "finland.final.v1",
  "merge_rule_version": "finland.merge.v1",
  "source_exports_used": [
    {
      "source_slug": "prhytj",
      "run_id": "20260607T120000Z-prhytj",
      "manifest_path": "/data/sources/prhytj/exports/20260607T120000Z-prhytj/manifest.json"
    }
  ]
}
```

`source_slug` is set for source exports and `null` for final country exports.

## Status Contract

`status-source` reports one source:

```text
source_slug
last_downloaded_at
last_snapshot_path
last_snapshot_sha256
last_normalized_at
last_exported_at
last_export_manifest_path
records_seen
records_exported
decode_errors
warnings
status
```

`status` reports:

- all source statuses
- final export status
- stale source warnings
- missing source warnings
- latest final manifest path

## Central Corpscout Import

Central Corpscout imports final country exports only.

The central importer should:

1. Read `manifest.json`.
2. Validate manifest version and file hashes.
3. Load Parquet into central staging tables.
4. Upsert central product tables.
5. Store import audit metadata.

Central tables should be generic across countries. They should not duplicate each
country/source raw schema.

Source-specific exports may be imported into central audit/debug storage later,
but they are not required for product queries.

## Temporal Execution

Temporal should execute countrydata modules through containers or local command
activities.

Generic workflow operations:

```text
SyncCountrySource(country=finland, source=prhytj)
GetCountrySourceStatus(country=finland, source=prhytj)
BuildCountryExport(country=finland)
ImportCountryExport(country=finland, manifest_path=/data/final/exports/20260607T120000Z/manifest.json)
```

Temporal should treat the module CLI as the execution boundary. It should not
call source-specific Go methods.

## Error Handling

Source modules should keep the existing error semantics:

- timeout
- not found
- line decode
- state
- unknown

Lower layers wrap and return errors with `github.com/cockroachdb/errors`.
Boundary CLI commands log errors once with `log/slog`.

Bad source rows should not stop the whole run unless the source command cannot
produce a valid export. Decode errors are counted and recorded in the manifest.

## Testing

Each source module needs:

- fixture tests based on real source records
- malformed-record tests
- source export schema tests
- manifest tests
- source status tests
- optional live integration tests

Each country module needs:

- merge rule unit tests
- final export schema tests
- source lineage tests
- multi-source conflict tests when a country has more than one source

Central Corpscout needs:

- manifest validation tests
- Parquet import tests
- central staging upsert tests
- Temporal command activity tests

## Migration From Current Finland Implementation

Current Finland PRH YTJ has two responsibilities that should be split:

1. Source ingestion and source normalization.
2. Central PostgreSQL raw/profile persistence.

Target migration:

1. Keep `countrydata/finland/prhytj` download/process parsing behavior.
2. Add source export generation to Parquet.
3. Add `cmd/finland-countrydata` with source and country commands.
4. Move country-level final export logic into `countrydata/finland`.
5. Keep the scheduler DB store temporarily while central import is built.
6. Deprecate source-specific central PostgreSQL raw/profile schemas once Parquet
   import is ready.

This avoids breaking existing sync commands while moving toward the isolated
module contract.

## Open Constraints

Parquet generation must be implemented with a Go library that is reliable in the
project's deployment environment. DuckDB can be introduced if it improves local
processing, but the first implementation should not require DuckDB unless the
library and container setup are verified.

Source modules must remain runnable without central Corpscout.

Central Corpscout must remain runnable without direct access to source internals.
