# Countrydata Package Implementation Guide

## Purpose

Use this skill to create or extend a complete standalone Go country package for
company data collection and export.

The package must live under:

```text
companycollect/companies/{country_slug}
```

The package input comes from prior discovery and data-model analysis under:

```text
companycollect/companies/analysis/{country_slug}
```

The output is a country binary that can download source data, build source-level
parquet exports, build final country parquet exports, and report source status
without importing Corpscout scheduler code.

## Non-Negotiable Architecture

- Each country has its own Go module: `companies/{country_slug}/go.mod`.
- Do not create `companies/go.mod`; it would include investigation scripts under
  `companies/analysis/*/scripts`.
- Shared helpers belong in `companies/common`, not copied into every country.
- Country packages must not import `corpscout`, `scheduler`, sqlc, or DB types.
- Corpscout should run the country binary or container and consume manifests and
  parquet files. It should not import the country module for new work.
- Runtime data must live under `companies/data/{country_slug}/countrydata`, not
  inside the country Go module.
- Generated runtime data and binaries must be ignored by git.

The reference implementation is Finland:

```text
companies/common/countryimport/
companies/finland/
companies/finland/prhytj/
companies/finland/cmd/finland-countrydata/
companies/data/finland/countrydata/
```

Use Finland as a structural example. Do not copy Finland field names,
PRH-specific pagination, or Finland-specific mappings into another country.

## Required Preflight Gate

Before writing a plan or editing code, verify that the upstream skills have run.
If any required artifacts are missing, stop and tell the user to run the missing
skill first. Do not invent source fields or transport details to bypass this
gate.

Required discovery artifacts from `company-open-data-discovery`:

```text
companies/analysis/{country_slug}/README.md
companies/analysis/{country_slug}/investigation.md
companies/analysis/{country_slug}/search_attempts.md
companies/analysis/{country_slug}/source_inventory.json
companies/analysis/{country_slug}/source_inventory.md
companies/analysis/{country_slug}/schema_notes.md
companies/analysis/{country_slug}/license_notes.md
companies/analysis/{country_slug}/scripts/downloader.go
companies/analysis/{country_slug}/scripts/sources.example.json
```

Required data-model artifacts from `company-country-data-model-analysis`:

```text
companies/analysis/{country_slug}/data_model/company_data_analysis.md
companies/analysis/{country_slug}/data_model/country_company_profile.schema.json
companies/analysis/{country_slug}/data_model/country_company_profile.example.json
companies/analysis/{country_slug}/data_model/country_company_profile_mapping.md
companies/analysis/{country_slug}/data_model/common_field_mapping_suggestions.md
companies/analysis/{country_slug}/data_model/sources/{source_slug}/source_field_catalog.json
companies/analysis/{country_slug}/data_model/sources/{source_slug}/source_field_catalog.md
```

Useful optional data-model artifacts:

```text
companies/analysis/{country_slug}/data_model/sources/{source_slug}/sample_record.json
companies/analysis/{country_slug}/data_model/sources/{source_slug}/countrydata_implementation_handoff.json
companies/data/{country_slug}/raw/
companies/data/{country_slug}/normalized/
```

If discovery files are missing, stop with:

```text
The discovery output is incomplete for {country_slug}. Run the
company-open-data-discovery skill first, then retry country package
implementation. Missing files:
- ...
```

If data-model files are missing, stop with:

```text
The data-model analysis output is incomplete for {country_slug}/{source_slug}.
Run the company-country-data-model-analysis skill first, then retry country
package implementation. Missing files:
- ...
```

If no `source_slug` is specified, read `source_inventory.json` and
`data_model/company_data_analysis.md`, choose sources marked as recommended or
useful secondary, and ask the user which source to implement when the choice is
not obvious.

## Expected Repository Shape

For a country with one source:

```text
companies/
  common/
    go.mod
    countryimport/
      env.go
      errors.go
      export_manifest.go
      metadata.go
      metadata_store.go
      options.go

  {country_slug}/
    go.mod
    go.sum
    README.md
    paths.go
    status.go
    status_test.go
    types.go
    export.go
    export_test.go
    cmd/
      {country_slug}-countrydata/
        main.go
        main_test.go
    {source_package}/
      README.md
      config.go
      config_test.go
      source.go
      types.go
      mapping.go
      mapping_test.go
      download.go
      download_test.go
      process.go
      process_test.go
      store.go
      export_rows.go
      export_rows_test.go
      export.go
      export_test.go
      parquet_writer.go
      parquet_writer_test.go
      live_integration_test.go
      testdata/

  data/
    {country_slug}/
      countrydata/
        sources/
          {source_package}/
            snapshots/
            exports/
              {run_id}/
                manifest.json
                *.parquet
        final/
          exports/
            {run_id}/
              manifest.json
              *.parquet
```

For a country with multiple sources, add one package per source and keep one
country-level CLI:

```text
companies/{country_slug}/{source_package_a}/
companies/{country_slug}/{source_package_b}/
companies/{country_slug}/cmd/{country_slug}-countrydata/
```

The final country export combines the source exports. Source sync operations
remain source-specific.

## Runtime Data Layout

Country-level default data root:

```text
../data/{country_slug}/countrydata
```

This path is relative to `companies/{country_slug}`. For example:

```text
companies/finland      -> ../data/finland/countrydata
absolute repo path     -> companies/data/finland/countrydata
```

Source-level default data root:

```text
../data/{country_slug}/countrydata/sources/{source_package}
```

Do not default to `./data/...` inside the country module. We had to move Finland
runtime data out of `companies/finland/data/...` because module-local generated
data makes gitignore and code ownership messy.

Add or verify these root `.gitignore` rules:

```gitignore
companies/data/*/countrydata/
companies/*/bin/
```

The root `.gitignore` may already ignore broader paths such as `companies/data/`
and `**/bin/`; still add explicit rules when missing because they document the
countrydata convention.

## Public API Contract

Every source package should expose:

```go
func NewSource(cfg Config) *Source
func ConfigFromEnv() Config
func (s *Source) Download(ctx context.Context, opts countryimport.DownloadOptions) (countryimport.DownloadResult, error)
func (s *Source) Process(ctx context.Context, opts countryimport.ProcessOptions) (countryimport.ProcessResult, error)
func (s *Source) Store(ctx context.Context, records []SourceRecord) (countryimport.StoreResult, error)
func (s *Source) Export(ctx context.Context, opts ExportOptions) (ExportResult, error)
```

Country package should expose:

```go
func LayoutForDataDir(dataDir string) Layout
func SourceStatusFromLatestManifest(dataDir string, sourceSlug string) (SourceStatus, error)
func BuildFinalExport(ctx context.Context, opts BuildExportOptions) (BuildExportResult, error)
```

The CLI must be a country command, not a per-source binary:

```text
{country_slug}-countrydata sync-source --source {source_package}
{country_slug}-countrydata status-source --source {source_package}
{country_slug}-countrydata export-source --source {source_package}
{country_slug}-countrydata status
{country_slug}-countrydata build-export
{country_slug}-countrydata sync --source {source_package} --build-export
```

CLI options:

```text
--env <path>             optional .env file
--data-dir <path>        country data root; defaults to ../data/{country}/countrydata
--source <slug>          required for source commands
--snapshot-path <path>   optional explicit source snapshot for export-source
--run-id <id>            optional deterministic run id for tests
--max-pages <n>          bounded remote sync for paginated sources
--chunk-size <n>         processing chunk size when Process is used
--build-export           for sync, also build final export
```

CLI responses should be JSON maps with stable keys:

```text
command
country_iso2
source
status
run_id
snapshot_path
source_manifest_path
final_manifest_path
records_seen
records_exported
decode_errors
```

## Source Snapshots Versus Exports

Keep these concepts separate:

- `snapshots/`: raw downloaded source data. For paginated APIs, this is usually
  NDJSON with one source record per line.
- `sources/{source}/exports/{run_id}/`: source-normalized parquet tables plus a
  manifest derived from a snapshot.
- `final/exports/{run_id}/`: country-level parquet tables plus a manifest
  derived from one or more source exports.

Finland example:

```text
sources/prhytj/snapshots/prh_ytj_v3_companies_20260607T115831Z.ndjson
sources/prhytj/exports/20260607T115833Z-prhytj/manifest.json
final/exports/20260607T115855Z-finland-final/manifest.json
```

`Download` creates snapshots. `Export` reads snapshots and creates source
parquet. `BuildFinalExport` reads source export manifests and creates final
country parquet.

## Implementation Workflow

### 1. Verify Inputs And Pick Sources

Read:

```text
companies/analysis/{country_slug}/source_inventory.json
companies/analysis/{country_slug}/data_model/company_data_analysis.md
companies/analysis/{country_slug}/data_model/country_company_profile_mapping.md
companies/analysis/{country_slug}/data_model/sources/{source_slug}/source_field_catalog.json
```

Extract:

- official source name and organization
- license and attribution
- source URL and base URL
- public/authenticated/paid/restricted access
- transport: bulk file, paginated API, CSV, JSON, XML, ZIP, gzip, parquet
- pagination and rate-limit notes
- native primary identifier
- company join keys across sources
- language fields and English labels
- data freshness and total-record estimates
- sample record paths

Do not implement a source that lacks enough analyzed field data. Ask for the
data-model analysis to be completed first.

### 2. Ensure `companies/common`

If `companies/common` already exists, reuse it.

If it is missing, create:

```text
companies/common/go.mod
companies/common/countryimport/
```

`countryimport` should contain shared, source-agnostic helpers:

- `DownloadOptions`, `ProcessOptions`, `StoreResult`
- `DownloadMetadata`, `ProcessMetadata`, metadata store interface/no-op store
- `.env` loading
- classified source errors: not_found, timeout, http_status, line_decode,
  remote_decode, invalid_config, file_io, state, no_snapshot
- export manifest structs and `SaveExportManifest`, `LoadExportManifest`
- `HashFileSHA256`

Keep this module small. Do not put country mappings, source-specific parsers, DB
logic, or Corpscout dependencies here.

### 3. Create The Country Module

Create:

```text
companies/{country_slug}/go.mod
```

Module path:

```text
github.com/pulsarpoint/companycollect/companies/{country_slug}
```

Require common through a local replace:

```go
require github.com/pulsarpoint/companycollect/companies/common v0.0.0
replace github.com/pulsarpoint/companycollect/companies/common => ../common
```

Add source-specific third-party dependencies only when needed. For parquet
exports, Finland uses:

```go
require github.com/parquet-go/parquet-go v0.30.1
```

Run `GOWORK=off go mod tidy` from the country module.

### 4. Create Country Layout And Status

Create `paths.go`:

- `CountryISO2`
- source constants such as `SourcePRHYTJ = "prhytj"`
- default data dir `../data/{country_slug}/countrydata`
- `LayoutForDataDir`
- `SourceDir`
- `SourceExportsDir`
- `FinalExportsDir`

Create `status.go`:

- reads latest complete manifest under `sources/{source}/exports`
- returns `missing` when no manifest exists
- skips incomplete newer run directories that do not contain `manifest.json`

Finland lesson: the latest export directory may be created before the manifest is
written. Status must skip incomplete newer run directories instead of reporting
a false failure.

### 5. Implement Source Config And State

`Config` should include:

- `BaseURL` or `DownloadURL`
- `DataDir`
- `PageDelay`
- `RequestTimeout`
- `UserAgent`
- `HTTPClient`
- `MetadataStore`
- credentials only when analysis says authentication is required

Use env vars with a source prefix:

```text
{SOURCE_PREFIX}_BASE_URL
{SOURCE_PREFIX}_DATA_DIR
{SOURCE_PREFIX}_PAGE_DELAY_MS
{SOURCE_PREFIX}_REQUEST_TIMEOUT_SECONDS
{SOURCE_PREFIX}_USER_AGENT
{SOURCE_PREFIX}_API_KEY
```

Never log secrets. Never include tokens, cookies, API keys, or full sensitive
request bodies in errors or manifests.

`Source` should hold concrete state:

- config
- HTTP client
- metadata store
- latest download metadata
- optional store callback

Do not add a generic source registry unless the country already has multiple
real sources and the CLI needs source dispatch.

### 6. Define Source-Native Types

Build source-native structs from:

```text
source_field_catalog.json
sample_record.json
country_company_profile_mapping.md
```

Rules:

- Preserve source-native fields first.
- Use pointers where absence differs from zero/empty.
- Keep raw payload and payload hash when processing line records.
- Include fields for multilingual labels and code sets.
- Include source updated timestamps and source-native IDs.
- Do not force a source into a global schema too early.

Finland lesson: PRH returned timestamps like `2025-12-31T07:39:20` without a
timezone. Timestamp parsers must accept documented and observed real-world
formats, not only ideal RFC3339.

### 7. Implement Download To Snapshots

For paginated APIs:

- use an HTTP client with timeout
- retry transient failures with bounded attempts
- classify timeouts with `ErrorKindTimeout`
- support `--max-pages` for smoke tests and local debugging
- write one compact source record per NDJSON line
- compute full snapshot SHA-256 while writing
- write to a temp file in `snapshots/`, then rename after success
- delete temp files on failure
- save metadata through a private helper

For bulk files:

- preserve the raw file when possible
- preserve useful extension
- stream to disk; do not load large files fully in memory
- compute hash and size while writing
- process later from the snapshot path

Finland lesson: full remote pulls can timeout after many pages. Keep `--max-pages`
and retry support so smoke tests and partial syncs remain useful.

### 8. Implement Process And Store

`Process` should:

- use explicit `ProcessOptions.SnapshotPath` when present
- otherwise use the latest successful download metadata
- otherwise pick latest file in `snapshots/`
- fail with `ErrorKindNoSnapshot` when nothing is available
- stream records; do not read entire large snapshots into memory
- decode each line/row into source-native records
- log malformed individual records with `slog.WarnContext` and continue
- flush chunks to `Store`
- check context cancellation during long scans
- save process metadata through a private helper

`Store` should:

- accept typed `[]SourceRecord`
- count and validate records when no DB writer exists
- return `StoreResult`
- not import sqlc, Corpscout DB, or scheduler types

Finland lesson: lower layers wrap and return; CLI/worker boundaries log once.
Do not log the same decode error in every layer.

### 9. Implement Source Parquet Export

Create `export_rows.go` and source export row structs. The source export should
preserve the source’s richness, not only the final country projection.

Typical source export files:

```text
companies.parquet
company_names.parquet
legal_forms.parquet
industries.parquet
addresses.parquet
registered_entries.parquet
tax_registrations.parquet
websites.parquet
```

Adapt file names to the source. Use only files that make sense for the source,
but keep names stable once emitted.

`Export` should:

- resolve snapshot path from options or latest snapshot
- stream and decode snapshot lines
- continue after malformed lines and count decode errors
- project source-native records into source export rows
- write parquet files under `sources/{source}/exports/{run_id}/`
- compute file SHA-256
- compute schema hashes from struct field names/types/parquet tags
- write `manifest.json` after all parquet files are complete
- use a public source key in manifests, e.g. `prhytj`, not only an internal slug

Finland lesson: a temp parquet file must be unique and must be closed before
rename. If write or close fails, remove the temp file.

### 10. Implement Final Country Export

Create `types.go` and `export.go` at country root.

The final export combines source exports into a country-level model. For one
source, it maps the primary source export into final tables. For multiple
sources, it applies source precedence and join rules from the data-model
analysis.

Typical final export files:

```text
companies.parquet
company_names.parquet
identifiers.parquet
addresses.parquet
industries.parquet
websites.parquet
source_evidence.parquet
```

Rules:

- final export reads source manifests, not raw snapshots
- validate source manifest version, kind, country, source slug, schema version
- verify each input parquet SHA before reading
- constrain manifest file paths to local relative files
- include source lineage in `source_evidence`
- set country company ID as `{ISO2}:{primary_native_identifier}`
- include `is_translated` where final rows contain translation-sensitive text
- add `_en` fields where the source has non-English text and English is useful
- keep profile hash stable by excluding volatile fields like `ExportedAt`

Finland lesson: final export should fail if a source manifest’s parquet SHA does
not match the actual file. This catches stale or corrupted exports.

### 11. Implement Country CLI

Create:

```text
companies/{country_slug}/cmd/{country_slug}-countrydata/main.go
```

The CLI should:

- parse commands and flags with `flag`
- optionally load `.env`
- construct concrete sources
- map country-level `--data-dir` to source-level data dirs via `Layout`
- log errors once at the boundary with `slog`
- classify errors with `countryimport.Classify`
- emit JSON result maps to stdout
- exit non-zero on failure

Do not create a scheduler command like `corpscout/scheduler/cmd/{country}-{source}-sync`
for new country packages. That path was removed for Finland when the architecture
changed.

### 12. Write Tests First

Default tests must not call live remote services.

Required tests:

- country layout uses `../data/{country}/countrydata`
- source config uses `../data/{country}/countrydata/sources/{source}`
- source config honors env overrides
- source record decoding preserves real fields
- mapping handles documented source shape
- projection from a real `sample_record.json`
- download with `httptest.Server` or local file writes snapshot, hash, size,
  page/file count, and metadata
- failed download removes temp snapshot
- missing snapshot returns `ErrorKindNoSnapshot`
- process continues after malformed line/row
- process flushes chunks and final partial chunk
- source export writes all expected parquet files and manifest
- source export skips bad lines and counts decode errors
- source export uses latest snapshot when explicit path is blank
- source export manifest has expected public source slug
- final export validates source manifest identity
- final export rejects source SHA mismatch
- final export writes all expected final parquet files
- status skips incomplete newer export run directories
- CLI parse tests for every command
- CLI result shape tests for `export-source`, `status-source`, `build-export`

Use real fixtures legally captured from source data. Include messy records:

- missing optional fields
- nulls
- empty arrays
- historical rows
- alternate languages
- unusual dates and timestamps
- ended registrations
- malformed individual lines/rows
- extra fields

Finland lesson: best-case fixtures miss real bugs. The PRH no-timezone timestamp
bug only appeared with real remote data.

### 13. Add Gated Live Tests

Live tests are required for real-world shape validation but skipped by default.

Use build tag and env gates:

```sh
COUNTRYDATA_{SOURCE_PREFIX}_LIVE=1 \
GOWORK=off go test -tags=integration ./{source_package}/... -run TestLive -count=1 -v

COUNTRYDATA_{SOURCE_PREFIX}_LIVE_FULL=1 \
GOWORK=off go test -tags=integration ./{source_package}/... -run TestLive -count=1 -v
```

Smoke test should download a bounded subset, export source parquet, and
optionally build final export. Full test should download and process the complete
remote source when practical.

When a live test finds a bad source shape, capture a small legal fixture and add
a default regression test.

### 14. Documentation

Country README should include:

```sh
GOWORK=off go build -o ./bin/{country_slug}-countrydata ./cmd/{country_slug}-countrydata
GOWORK=off go run ./cmd/{country_slug}-countrydata sync-source --source {source_package} --data-dir ../data/{country_slug}/countrydata --max-pages 2
GOWORK=off go run ./cmd/{country_slug}-countrydata status-source --source {source_package} --data-dir ../data/{country_slug}/countrydata
GOWORK=off go run ./cmd/{country_slug}-countrydata build-export --data-dir ../data/{country_slug}/countrydata
```

Document default output paths:

```text
../data/{country_slug}/countrydata/sources/{source_package}/snapshots/
../data/{country_slug}/countrydata/sources/{source_package}/exports/{run_id}/
../data/{country_slug}/countrydata/final/exports/{run_id}/
```

Source README should include fixture test commands and live test commands.

## Verification Commands

Run after implementation:

```sh
cd companycollect/companies/common
GOWORK=off go test ./... -count=1

cd companycollect/companies/{country_slug}
GOWORK=off go test ./... -count=1
GOWORK=off go build -o ./bin/{country_slug}-countrydata ./cmd/{country_slug}-countrydata
rm -f ./bin/{country_slug}-countrydata
rmdir ./bin 2>/dev/null || true
GOWORK=off go run ./cmd/{country_slug}-countrydata status-source --source {source_package}
```

If legacy Corpscout modules were touched:

```sh
cd companycollect/corpscout/countrydata
GOWORK=off go test ./... -count=1

cd companycollect/corpscout/scheduler
GOWORK=off go test ./internal/countrydata ./internal/db/gen -count=1
```

Do not claim full scheduler `go test ./...` passed unless it actually passed.
If unrelated failures exist, name the exact failing test and still run affected
packages.

Before commit:

```sh
git status --short
git diff --check
```

Generated runtime files under `companies/data/{country}/countrydata` should be
ignored and should not be staged.

## Finland Execution Lessons To Apply

- **Package location:** Finland started under `corpscout/countrydata`; it was
  moved to `companies/finland` because country processors are standalone
  applications.
- **Module boundary:** per-country `go.mod` is better than `companies/go.mod`.
  The latter would include `companies/analysis/*/scripts`.
- **Shared code:** common helpers are useful, but keep them source-agnostic.
  Use `companies/common/countryimport`.
- **Runtime data:** default to `../data/{country}/countrydata`; do not write
  generated data under `companies/{country}/data`.
- **Source snapshots:** snapshots are raw downloaded inputs, not parquet outputs.
  Keep `snapshots/` and `exports/` separate.
- **Status:** skip incomplete newer run directories without `manifest.json`.
- **Remote reliability:** paginated APIs need retries, request timeouts, and
  `--max-pages` for partial test runs.
- **Real-world parsing:** support observed source date/time formats, not only
  ideal documented formats.
- **Decode resilience:** malformed individual records should warn and continue;
  missing entire snapshot should fail.
- **Parquet safety:** write to unique temp file, close, then rename; delete temp
  files on failure.
- **Manifest integrity:** hash input snapshots and output files; verify source
  file hashes before final export.
- **Source identity:** distinguish internal source slug from public source key
  when needed; manifests should use stable public source keys.
- **Final hash stability:** exclude volatile fields such as export timestamp
  from profile hashes.
- **No scheduler import:** do not add `corpscout/scheduler/cmd/{country}-{source}-sync`
  or scheduler DB stores for new country packages.
- **Helper placement:** do not hide generic helpers in source-specific files.
  When Finland scheduler adapter was removed, shared test/store helpers had to be
  moved to neutral files so remaining sources still compiled.
- **Verification honesty:** report unrelated test failures separately and show
  the focused commands that passed.

## Common Mistakes

- Creating `companies/go.mod`.
- Defaulting data paths to `./data/...` inside the country module.
- Staging generated parquet/snapshot files.
- Importing scheduler/sqlc types into a country module.
- Implementing only fixture-best-case parsing.
- Treating source export and final export as the same thing.
- Writing manifests before all parquet files are successfully written.
- Using source-specific names in shared common helpers.
- Logging raw records, API keys, request bodies, or full sensitive payloads.
- Omitting live tests because fixture tests pass.
- Claiming the full remote source works after only `--max-pages` smoke testing.
