---
name: company-countrydata-source-implementation
description: Use when implementing a Go countrydata source in a country-owned module under companycollect/companies/{country_slug} from completed company source discovery and country data-model analysis artifacts.
---

# Company Countrydata Source Implementation

## Purpose

Use this skill only after both upstream skills have already produced their
artifacts:

- `company-open-data-discovery`
- `company-country-data-model-analysis`

Those artifacts must exist under:

```text
companycollect/companies/analysis/{country_slug}
```

The goal is to implement one analyzed data source as a standalone Go source
package inside the country-owned Go module under:

```text
companycollect/companies/{country_slug}/{source_package}
```

Do not use this skill to discover new sources, research licenses, or generate
country data analysis. If the required upstream outputs are missing, stop before
implementation and tell the user to run the missing skill first.

## Required Input And Preflight Gate

The task should identify:

```text
country_slug: finland
source_slug: finland_prh_ytj_v3
```

Before choosing a source, writing a plan, or editing code, verify both upstream
artifact sets.

Discovery output from `company-open-data-discovery`:

```text
companycollect/companies/analysis/{country_slug}/README.md
companycollect/companies/analysis/{country_slug}/investigation.md
companycollect/companies/analysis/{country_slug}/search_attempts.md
companycollect/companies/analysis/{country_slug}/source_inventory.json
companycollect/companies/analysis/{country_slug}/source_inventory.md
companycollect/companies/analysis/{country_slug}/schema_notes.md
companycollect/companies/analysis/{country_slug}/license_notes.md
companycollect/companies/analysis/{country_slug}/scripts/downloader.go
companycollect/companies/analysis/{country_slug}/scripts/sources.example.json
```

Data-model output from `company-country-data-model-analysis`:

```text
companycollect/companies/analysis/{country_slug}/data_model/company_data_analysis.md
companycollect/companies/analysis/{country_slug}/data_model/sources/{source_slug}/source_field_catalog.json
companycollect/companies/analysis/{country_slug}/data_model/sources/{source_slug}/source_field_catalog.md
companycollect/companies/analysis/{country_slug}/data_model/country_company_profile.schema.json
companycollect/companies/analysis/{country_slug}/data_model/country_company_profile.example.json
companycollect/companies/analysis/{country_slug}/data_model/country_company_profile_mapping.md
companycollect/companies/analysis/{country_slug}/data_model/common_field_mapping_suggestions.md
```

Useful optional implementation inputs:

```text
companycollect/companies/analysis/{country_slug}/data_model/sources/{source_slug}/sample_record.json
companycollect/companies/analysis/{country_slug}/investigation.md
companycollect/companies/data/{country_slug}/raw/
companycollect/companies/data/{country_slug}/normalized/
```

If any discovery artifact is missing, stop and say:

```text
The discovery output is incomplete for {country_slug}. Run the
company-open-data-discovery skill first, then retry this implementation.
Missing files:
- ...
```

If any data-model artifact is missing for the selected source, stop and say:

```text
The data-model analysis output is incomplete for {country_slug}/{source_slug}.
Run the company-country-data-model-analysis skill first, then retry this
implementation. Missing files:
- ...
```

If `source_slug` is missing, read `source_inventory.json` only after the
discovery output exists. Choose exactly one source with status such as
`recommended` or `useful_secondary_source`. If more than one source is plausible,
ask which source to implement, then verify the source-specific data-model files.

Do not invent source fields, licenses, pagination, authentication, mapping
rules, or sample records to bypass this gate.

## Architecture Rules

- Keep source logic in `companies/{country_slug}`, not `corpscout` or
  `scheduler/internal`.
- Each country owns its own `go.mod` and builds its own country-level binary.
- Reuse `github.com/pulsarpoint/companycollect/companies/common/countryimport` for shared
  options, results, metadata, env loading, and classified errors.
- Implement a concrete source package. Do not add a source registry or local
  interface unless there are multiple real implementations that need it.
- Expose the same public method shape for every source:

```go
func NewSource(cfg Config) *Source
func ConfigFromEnv() Config
func (s *Source) Download(ctx context.Context, opts countryimport.DownloadOptions) (countryimport.DownloadResult, error)
func (s *Source) Process(ctx context.Context, opts countryimport.ProcessOptions) (countryimport.ProcessResult, error)
func (s *Source) Store(ctx context.Context, records []SourceRecord) (countryimport.StoreResult, error)
```

- Keep source-specific state in `Source`: config, HTTP client, metadata store,
  last download/process metadata, and optional store callback.
- Read source inputs from environment variables or explicit options. Never
  hard-code secrets. Never log API keys, tokens, cookies, or full request bodies.
- Metadata persistence is internal to `Download` and `Process` through private
  helpers such as `saveDownloadMetadata` and `saveProcessMetadata`.
- If no DB or metadata store is provided, use the shared no-op metadata store.
- If no DB store is provided, `Store` should validate/count records and return a
  useful `StoreResult` so parser validation can run without Corpscout.

## Source Package Layout

For a source like `finland_prh_ytj_v3`, follow the existing Finland package as
the reference pattern:

```text
companies/{country_slug}/{source_package}/
  README.md
  config.go
  source.go
  types.go
  mapping.go
  download.go
  process.go
  store.go
  *_test.go
  live_integration_test.go
  testdata/
```

Also add a standalone CLI when the source should run outside scheduler:

```text
companies/{country_slug}/cmd/{country_slug}-countrydata/main.go
```

The country CLI should construct concrete sources and call `Download`,
`Process`, source export, status, and final export methods. Corpscout should run
the country binary or container and consume produced manifests/parquet files; do
not import country modules into scheduler for new implementations.

Use this as a structural template, not as copy-paste source code:

```text
README.md                 source-specific test and live-run notes
config.go                 env parsing, defaults, source config
source.go                 Source state, NewSource, injected dependencies
types.go                  source-native record and envelope structs
mapping.go                source-native to derived profile mapping
download.go               remote fetch to snapshot plus download metadata
process.go                snapshot streaming, decode recovery, chunking
store.go                  typed Store boundary and no-DB counting behavior
*_test.go                 fixture, mapping, download, process, store tests
live_integration_test.go  gated real remote smoke/full tests
testdata/                 legal real captured fixtures and messy records
```

Do not create a separate generic code-template folder yet. Use the Finland PRH
YTJ implementation as the worked example and adapt behavior to the source shape.
After a second source exists, compare both sources before extracting any reusable
code template.

## Implementation Workflow

### 1. Read The Generated Analysis

Start with `source_inventory.json` and the selected source entry. Extract:

- source name, organization, URL, license, attribution
- access type: public, authenticated, paid, or restricted
- remote shape: bulk file, paginated API, single API endpoint, CSV, JSON, XML,
  ZIP, gzip, NDJSON, or other streamable format
- pagination and rate-limit notes
- expected identifiers and join keys
- downloaded sample files and field catalog paths
- data freshness and total-record hints

Use `data_model/sources/{source_slug}` when present to define Go structs and
mapping rules. Preserve source-native fields first; derived Corpscout-friendly
profiles can be added after raw parsing is correct.

### 2. Write Tests First

Use test-driven development for each source. Default tests must not call the
real remote service.

Required default tests:

- config/env parsing keeps explicit values and applies defaults
- source record decoding preserves real source fields
- mapping rules handle the documented source shape
- download against `httptest.Server` or local files produces a snapshot and
  download metadata with path, hash, byte count, page/file count, and record
  count
- process reads the snapshot in chunks, calls `Store`, counts processed records,
  and continues after bad individual records/lines
- missing snapshot returns `countryimport.ErrorKindNoSnapshot`
- local missing file returns `countryimport.ErrorKindNotFound`
- context cancellation stops processing and returns a classified/wrapped error
- metadata store failures return `countryimport.ErrorKindState`

Fixtures should be captured from real source data when legally allowed. Include
messy records: missing optional fields, nulls, empty arrays, historical rows,
unexpected extra fields, alternate languages, unusual date formats, ended
registrations, and malformed individual lines/rows.

### 3. Implement Config And Types

Create a source-specific `Config` with:

- `BaseURL` or `DownloadURL`
- `DataDir`
- request timeout, page delay, user agent, and source-specific paging controls
- HTTP client
- metadata store
- source credentials only when the analysis says authentication is required

Use env vars with a clear source prefix, for example:

```text
PRH_YTJ_BASE_URL
PRH_YTJ_DATA_DIR
PRH_YTJ_PAGE_DELAY_MS
PRH_YTJ_REQUEST_TIMEOUT_SECONDS
PRH_YTJ_USER_AGENT
```

For other sources, use the same pattern with a source-specific uppercase prefix.

Define Go types from real records and field catalogs. Use pointers for optional
numeric/date fields when absence is semantically different from zero.

### 4. Implement Download

`Download` should create a complete source snapshot and return
`countryimport.DownloadResult`.

For paginated JSON APIs:

- request pages with polite delay and timeout
- validate the envelope before writing records
- write one compact source record per NDJSON line
- stop on max pages, empty page, or known total page count

For remote bulk files:

- download the raw file into the source data directory
- preserve the extension when useful for processing
- compute SHA-256 and byte count while writing
- record source file size, hash, URL, and duration

For CSV/XML/ZIP/gzip sources:

- prefer preserving the raw downloaded file as the snapshot
- stream during processing instead of loading the whole file into memory

Always write to a temporary file first and rename only after success. Remove
temporary files on failure. Save metadata through a private helper before
returning. Use `github.com/cockroachdb/errors` for wrapping and
`countryimport.WrapSourceError` for classified failures.

### 5. Implement Process

`Process` should use `ProcessOptions.SnapshotPath` when provided, otherwise the
latest successful download metadata.

Processing rules:

- fail with `ErrorKindNoSnapshot` when no snapshot is available
- stream line by line, row by row, or archive entry by archive entry
- decode into the source-native Go record type
- log malformed individual records with `slog.WarnContext` and continue
- include source slug, line/row number, and bounded error text in warnings
- do not log full raw records or secrets
- flush chunks to `Store` at `ChunkSize`
- flush the final partial chunk
- check context cancellation during long scans
- save process metadata through a private helper

Lower-level helpers wrap and return errors. Boundary layers such as CLI commands
and scheduler activities log operation-level errors once.

### 6. Implement Store

Keep initial storage source-local and direct:

- accept typed `[]SourceRecord`
- reject nil chunks if that indicates caller misuse
- when no DB writer is configured, count records and return
  `StoreResult{RecordsReceived: n, RecordsStored: n}`
- when adding DB persistence later, inject it as a concrete dependency or store
  callback owned by the source package

Do not import scheduler sqlc types into `companies/{country_slug}`.

### 7. Add Country CLI

The CLI should support:

```text
{country_slug}-countrydata sync-source --source {source_package} --env .env
{country_slug}-countrydata status-source --source {source_package} --env .env
{country_slug}-countrydata export-source --source {source_package} --env .env
{country_slug}-countrydata status --env .env
{country_slug}-countrydata build-export --env .env
{country_slug}-countrydata sync --source {source_package} --build-export --env .env
```

The CLI loads `.env`, constructs concrete sources, calls the same public methods
used by tests and source packages, logs once with `slog`, writes JSON results to
stdout, and exits non-zero on classified source failures.

### 8. Add Gated Live Tests

Live tests are required for real-world source shape validation, but must be
skipped by default.

Use a build tag and env gates:

```sh
COUNTRYDATA_{SOURCE_PREFIX}_LIVE=1 \
GOWORK=off go test -tags=integration ./{source_package}/... -run TestLive -count=1 -v

COUNTRYDATA_{SOURCE_PREFIX}_LIVE_FULL=1 \
GOWORK=off go test -tags=integration ./{source_package}/... -run TestLive -count=1 -v
```

Live smoke tests should download a bounded subset and process it. Full live
tests should download and process the complete remote source when practical.
When a live test finds a bad real-world shape, capture a small legal fixture and
turn it into a default regression test.

## Verification

Run shared helper tests from `companycollect/companies/common`:

```sh
GOWORK=off go test ./... -count=1
```

Run source and country CLI tests from `companycollect/companies/{country_slug}`:

```sh
GOWORK=off go test ./... -count=1
GOWORK=off go build -o ./bin/{country_slug}-countrydata ./cmd/{country_slug}-countrydata
rm -f ./bin/{country_slug}-countrydata
GOWORK=off go test -tags=integration ./{source_package}/... -run TestLive -count=1 -v
```

Report when live tests are skipped because env gates are not set. Do not claim a
full live source run passed unless it actually ran.

## Reference Implementation

Use these files as the concrete reference implementation:

```text
companycollect/companies/common/countryimport/
companycollect/companies/finland/prhytj/
companycollect/companies/finland/cmd/finland-countrydata/
```

Finland PRH YTJ is real, tested, and complete enough to guide the next source:
it includes shared common helper use, source config, typed source records, mapping,
download, process, store, metadata persistence, fixture tests, gated live tests,
a country-level standalone CLI, source parquet export, and final country parquet
export. Copy structure and behavior, not Finland-specific field names or
PRH-specific pagination assumptions.
