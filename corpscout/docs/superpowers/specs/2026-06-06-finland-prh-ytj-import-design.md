# Finland PRH YTJ Import Design

## Goal

Create a standalone Go country-data import module for Finland's PRH Open Data
YTJ API v3 companies source.

The module must be usable in two ways:

- as an independent command-line application that can download, process, and
  store the source without Corpscout scheduler, Temporal, or Postgres wiring
- as a dependency imported by Corpscout scheduler through a thin adapter

The first implemented source is Finland PRH YTJ v3 only. The public method shape
should be source-independent so future country sources can expose the same
triggerable operations.

## Context

The existing Corpscout scheduler has source-specific country packages under
`scheduler/internal`, such as `brreg`, `ariregister`, `france`, and `se`.
That layout is appropriate for scheduler-owned workflows, but it is not the
right boundary for reusable country import modules:

- Go `internal` packages are intentionally private to their parent tree.
- A country source inside `scheduler/internal` would be coupled to scheduler
  runtime dependencies.
- The user wants each country module to be independently runnable by adding a
  `main.go` command.

Finland's source analysis already exists under:

```text
companycollect/companies/analysis/finland
```

Key facts from that analysis:

- source: PRH Open Data YTJ API v3 companies endpoint
- endpoint: `https://avoindata.prh.fi/opendata-ytj-api/v3/companies`
- access: public, no auth
- license: CC-BY-4.0
- freshness: daily
- response shape: `{ "totalResults": 819096, "companies": [ ... ] }`
- page size: fixed at 100 records per page
- pagination: `page=N`
- full crawl: roughly 8191 pages

The old single-file `full_prh_data.csv` path is unavailable and must not be used.
The paginated API is the canonical bulk-style source for this design.

## Non-Goals

- Do not implement every country under `companycollect/companies/analysis`.
- Do not build a generic source registry or plugin loader in the first version.
- Do not add database schema or sqlc queries in this phase.
- Do not write source-derived records into Corpscout resolved company tables.
- Do not hide source-specific parsing behind an untyped generic processor.
- Do not make live PRH tests part of the default test suite.
- Do not store API keys, secrets, request bodies, or verbose stack traces in
  metadata files.

## Package Location

Add a standalone Go module:

```text
companycollect/corpscout/countrydata/
  go.mod
  import/
    source.go
    options.go
    metadata.go
    errors.go
    local_state.go
    env.go
  finland/
    prhytj/
      config.go
      source.go
      download.go
      process.go
      store.go
      types.go
      mapping.go
      source_test.go
      testdata/
  cmd/
    prhytj-import/
      main.go
```

The directory `import/` is intentional because the requested package structure
uses that folder name. Its Go package name should be `countryimport`, because
`import` is a Go keyword.

The module path should be:

```text
github.com/pulsarpoint/corpscout/countrydata
```

Scheduler can later consume it from `scheduler/go.mod` with a local replace while
the modules live in the same repository:

```text
replace github.com/pulsarpoint/corpscout/countrydata => ../countrydata
```

## Public API

The shared `countrydata/import` package defines source-independent options,
results, metadata, and error types. It may define a generic source contract
because this is a stable boundary between standalone commands, scheduler
adapters, and future source modules.

```go
package countryimport

type BulkSource[T any] interface {
    Download(ctx context.Context, opts DownloadOptions) (DownloadResult, error)
    Process(ctx context.Context, opts ProcessOptions) (ProcessResult, error)
    Store(ctx context.Context, records []T) (StoreResult, error)
    Save(ctx context.Context) error
}
```

Finland implements this as:

```go
type Source struct {
    // PRH-specific state, config, HTTP client, local metadata store, and
    // optional future DB store live here.
}

func (s *Source) Download(ctx context.Context, opts countryimport.DownloadOptions) (countryimport.DownloadResult, error)
func (s *Source) Process(ctx context.Context, opts countryimport.ProcessOptions) (countryimport.ProcessResult, error)
func (s *Source) Store(ctx context.Context, records []CompanyRecord) (countryimport.StoreResult, error)
func (s *Source) Save(ctx context.Context) error
```

This keeps the triggerable method names and option/result shapes consistent while
preserving typed source-specific records. Scheduler code should construct the
concrete Finland source directly for now. A generic runtime registry can wait
until there are at least two countrydata sources that need shared dispatch.

## Shared Options And Results

The shared package should expose practical source-neutral structs:

```go
type DownloadOptions struct {
    DataDir         string
    MaxPages        int
    PageStart       int
    PageDelay       time.Duration
    RequestTimeout  time.Duration
    UserAgent       string
    Force           bool
}

type DownloadResult struct {
    SourceSlug      string
    SnapshotPath    string
    MetadataPath    string
    BytesDownloaded int64
    RecordsSeen     int64
    PagesDownloaded int
    SHA256          string
    StartedAt       time.Time
    FinishedAt      time.Time
    Duration        time.Duration
}

type ProcessOptions struct {
    DataDir      string
    SnapshotPath string
    ChunkSize    int
    Limit        int64
}

type ProcessResult struct {
    SourceSlug        string
    SnapshotPath      string
    RecordsSeen       int64
    RecordsProcessed  int64
    RecordsStored     int64
    DecodeErrors      int64
    ChunksProcessed   int64
    StartedAt         time.Time
    FinishedAt        time.Time
    Duration          time.Duration
}

type StoreResult struct {
    RecordsReceived int64
    RecordsStored   int64
}
```

Defaults:

- `PageStart = 1`
- `ChunkSize = 500`
- `RequestTimeout = 60s`
- `PageDelay = 500ms`
- `UserAgent = "corpscout-countrydata/1.0"`

## Finland Source Config

Finland source configuration is loaded from environment variables. The CLI may
load a `.env` file before reading env vars. The source package should not depend
on Corpscout scheduler config.

Supported variables:

```text
PRH_YTJ_BASE_URL=https://avoindata.prh.fi/opendata-ytj-api/v3/companies
PRH_YTJ_DATA_DIR=./data/countrydata/finland/prhytj
PRH_YTJ_PAGE_START=1
PRH_YTJ_MAX_PAGES=
PRH_YTJ_PAGE_DELAY_MS=500
PRH_YTJ_REQUEST_TIMEOUT_SECONDS=60
PRH_YTJ_USER_AGENT=corpscout-countrydata/1.0
PRH_YTJ_STATE_FILE=./data/countrydata/finland/prhytj/state.json
```

`PRH_YTJ_MAX_PAGES` empty means crawl until the API returns an empty page or the
computed total page count is reached.

PRH currently needs no API key. The source state struct should still be able to
hold future source-specific sensitive config in memory, but secrets must not be
written to metadata files or logs.

## Download Design

PRH YTJ is a paginated API, but this module treats one complete crawl as a bulk
snapshot.

Download flow:

1. Resolve config from explicit options, env, then defaults.
2. Create the data directory if missing.
3. Create a temporary NDJSON file in the data directory.
4. Request page `PageStart` with `totalResults=true`.
5. Decode the page envelope and validate that `companies` exists.
6. Write each company object as one compact JSON line.
7. Continue with `page=N` until one of these conditions is true:
   - `MaxPages` pages were downloaded
   - the API returns zero companies
   - the computed total page count is reached
8. Compute SHA-256 while writing.
9. Atomically rename the temporary file to a stable snapshot path.
10. Store download metadata in memory.
11. Call `Save` to persist metadata to a local state file when no DB store exists.

Snapshot path format:

```text
{data_dir}/snapshots/prh_ytj_v3_companies_{UTC_TIMESTAMP}.ndjson
```

Metadata sidecar path:

```text
{snapshot_path}.metadata.json
```

The metadata should include:

```json
{
  "source_slug": "finland_prh_ytj_v3",
  "source_name": "PRH Open Data YTJ API v3 companies",
  "base_url": "https://avoindata.prh.fi/opendata-ytj-api/v3/companies",
  "snapshot_path": "...",
  "metadata_path": "...",
  "state_path": "...",
  "started_at": "...",
  "finished_at": "...",
  "duration_ms": 0,
  "bytes_downloaded": 0,
  "records_seen": 0,
  "pages_downloaded": 0,
  "first_page": 1,
  "last_page": 1,
  "total_results_reported": 819096,
  "sha256": "...",
  "http_statuses": { "200": 8191 },
  "license": "CC-BY-4.0",
  "attribution": "Finnish Patent and Registration Office (PRH) and Finnish Tax Administration"
}
```

If download fails before the final rename, the temporary file is removed. If it
fails after some pages, the error should identify whether the failure was a
timeout, HTTP status, response decode error, or local file error.

## Process Design

`Process` reads the latest downloaded snapshot unless `ProcessOptions.SnapshotPath`
is set.

Process flow:

1. Find the latest successful snapshot from in-memory metadata or local state.
2. Fail with `ErrorKindNoSnapshot` when no file can be processed.
3. Open the NDJSON snapshot.
4. Read line by line.
5. Decode each line into `prhytj.CompanyRecord`.
6. If a single line fails to decode, log once with `slog.WarnContext` including
   source slug, line number, and bounded error text; continue.
7. Add successfully decoded records to a chunk.
8. When the chunk reaches `ChunkSize`, call `Store`.
9. Flush the final partial chunk.
10. Save process metadata to local state.

The parser must preserve source-native data that matters for later mapping:

- `businessId`
- `euId`
- `names`
- `mainBusinessLine`
- `website`
- `companyForms`
- `companySituations`
- `registeredEntries`
- `addresses`
- `tradeRegisterStatus`
- `status`
- `registrationDate`
- `endDate`
- `lastModified`
- raw payload hash

The first implementation may expose both raw PRH structs and a mapped
`CompanyProfile` struct. Tests should focus on preserving source-native fields
and on deterministic mapping rules already documented in the Finland analysis:

- legal name: current `names[]` with `type=1` and no `endDate`; if several,
  choose latest `registrationDate`
- active status: `tradeRegisterStatus == "1"` and no `endDate`
- VAT id: `FI` plus `businessId.value` with the dash removed
- VAT registration: active `registeredEntries[]` in register `6`
- employer registration: active `registeredEntries[]` in register `5`
- prepayment registration: active `registeredEntries[]` in register `7`
- website: `website.url`, normalized to include a scheme for derived profile use

## Store Design

`Store` receives typed `[]prhytj.CompanyRecord` chunks.

For now, no DB store is configured by default. In that case:

- validate the chunk is non-nil
- count received records
- update in-memory processing stats
- return `StoreResult{RecordsReceived: n, RecordsStored: n}`

This makes full processing useful for parser validation even before database
persistence exists.

A future DB store can be added as an optional dependency on `Source`. The source
package should not import Corpscout scheduler sqlc types. Corpscout scheduler can
provide an adapter later if needed.

## Save And Local State

`Save` persists local state only when no database metadata store is configured.
With no DB, it writes JSON to `PRH_YTJ_STATE_FILE` or the default state path under
the data directory.

State shape:

```json
{
  "source_slug": "finland_prh_ytj_v3",
  "latest_snapshot_path": "...",
  "latest_snapshot_sha256": "...",
  "latest_download": {},
  "latest_process": {},
  "snapshots": []
}
```

`Save` should be atomic:

1. write to `{state_path}.tmp`
2. fsync or close cleanly
3. rename to `{state_path}`

If a DB store is later configured, `Save` should write to the DB store and local
state can be disabled or kept as an optional sidecar. In this first version,
there is no DB store, so local state is the source of truth for latest snapshot
metadata.

## Error Handling

Use `github.com/cockroachdb/errors` for wrapping and stack traces. Lower-level
download, decode, file, and state helpers wrap and return errors. Boundary code,
such as the CLI or scheduler adapter, logs once.

The shared package defines classified errors:

```go
type ErrorKind string

const (
    ErrorKindNotFound       ErrorKind = "not_found"
    ErrorKindNoSnapshot     ErrorKind = "no_snapshot"
    ErrorKindTimeout        ErrorKind = "timeout"
    ErrorKindHTTPStatus     ErrorKind = "http_status"
    ErrorKindRemoteDecode   ErrorKind = "remote_decode"
    ErrorKindLineDecode     ErrorKind = "line_decode"
    ErrorKindInvalidConfig  ErrorKind = "invalid_config"
    ErrorKindFileIO         ErrorKind = "file_io"
    ErrorKindState          ErrorKind = "state"
)

type SourceError struct {
    Kind   ErrorKind
    Source string
    URL    string
    Path   string
    Status int
    Err    error
}
```

Add helpers:

```go
func IsKind(err error, kind ErrorKind) bool
func Classify(err error) ErrorKind
```

Behavior:

- missing process snapshot returns `ErrorKindNoSnapshot`
- missing local file returns `ErrorKindNotFound`
- context deadline and network timeout return `ErrorKindTimeout`
- non-2xx HTTP response returns `ErrorKindHTTPStatus`
- invalid PRH page envelope returns `ErrorKindRemoteDecode`
- bad NDJSON line increments decode stats, logs a warning, and continues
- local state write failure returns `ErrorKindState`

Logs must use `log/slog`. The CLI and scheduler adapter are the boundary layers
that log operation-level errors. `Process` may log bad individual data lines
because continuing is intentional and the line would otherwise disappear.

Never log secrets, tokens, cookies, or full request bodies. Error previews should
be bounded.

## Standalone CLI

Add:

```text
countrydata/cmd/prhytj-import/main.go
```

Commands:

```text
prhytj-import download --env .env
prhytj-import process --env .env
prhytj-import run --env .env
```

`run` performs:

```text
Download -> Process -> Save
```

CLI rules:

- load `.env` when `--env` is passed
- construct `prhytj.Source`
- call the same public methods the scheduler adapter will call
- log once at command boundary with `slog`
- exit non-zero on classified source-level failures

## Corpscout Scheduler Integration

Scheduler integration should be a thin adapter in `scheduler/internal`, not the
home of source logic.

The adapter should:

- import `github.com/pulsarpoint/corpscout/countrydata/finland/prhytj`
- construct the concrete `prhytj.Source`
- call `Download`, `Process`, and `Save`
- translate results into scheduler/Temporal result structs when needed
- log once at scheduler activity or worker boundary

Temporal registration should stay direct in app wiring with
`RegisterActivityWithOptions`, matching existing Corpscout architecture.

No generic registry interface is needed for the first Finland-only integration.

## Testing Strategy

The tests must catch real PRH formatting issues, not just best-case fixtures.
Use three layers.

### Layer 1: Real Captured Fixture Tests

Store captured PRH records and pages under:

```text
countrydata/finland/prhytj/testdata/
  prh_page_1.json
  prh_page_messy_records.json
  prh_record_dynava.json
  prh_snapshot_mixed.ndjson
```

Fixtures should be based on real PRH data and include:

- missing `euId`
- missing `website`
- empty `companySituations`
- ceased company with `endDate`
- multiple primary names with historical `endDate`
- multiple company forms
- address with Finnish and Swedish post offices
- address with missing optional address fields
- register entries with ended VAT/employer/prepayment rows
- active VAT/employer/prepayment rows
- unexpected but valid extra JSON fields

Default tests should assert:

- page envelope parsing reads `companies[]`
- record parsing preserves source-native fields
- profile mapping follows Finland rules
- badly formed NDJSON line increments decode error count and processing continues
- missing snapshot returns `ErrorKindNoSnapshot`
- local state `Save` writes and reloads latest snapshot metadata
- hash, byte count, page count, and record count are saved

### Layer 2: Local Full-Flow Tests

Use `httptest.Server` to serve captured PRH-style pages. Test:

```text
Download -> Save -> Process -> Store
```

The local full-flow test should verify:

- multiple pages are downloaded
- the snapshot is NDJSON with one source company per line
- SHA-256 matches the saved metadata
- process chunking calls `Store` with expected chunk sizes
- malformed source lines are logged and skipped
- no database is required

These tests are deterministic and should run in the default suite:

```bash
GOWORK=off go test ./...
```

from `companycollect/corpscout/countrydata`.

### Layer 3: Live PRH Integration Tests

Add integration tests skipped by default and enabled only with explicit flags:

```bash
COUNTRYDATA_PRH_YTJ_LIVE=1 \
go test -tags=integration ./finland/prhytj/... -run TestLivePRHYTJSmoke -count=1 -v

COUNTRYDATA_PRH_YTJ_LIVE_FULL=1 \
go test -tags=integration ./finland/prhytj/... -run TestLivePRHYTJFullDataset -count=1 -v
```

Live smoke test:

- downloads a small bounded page count from the real PRH API
- processes all downloaded lines
- verifies no source-level failure
- records any decode warnings for investigation

Live full dataset test:

- downloads the real PRH full dataset to a temp data directory
- processes every downloaded line in chunks
- logs malformed lines and continues
- fails on timeout, invalid page envelope, missing snapshot, local file errors, or
  page fetch failures that prevent a complete snapshot
- reports pages, records, bytes, SHA-256, decode errors, and duration

Live full tests should not run in normal CI. They are suitable for manual runs or
a later scheduled job because they depend on a public remote API and can take
time.

## Fixture Refresh Workflow

The first implementation does not need a separate fixture-refresh command. Use
the live smoke test or a bounded standalone download run to capture small PRH
pages and records manually, then commit those captures under `testdata`.

When a live test finds a bad real-world shape, the implementation workflow should
capture a small representative record/page and add it to `testdata` so the issue
becomes a deterministic regression test.

## Design Decisions

- The country source code lives outside `scheduler/internal`.
- Finland PRH YTJ v3 is the only source implemented now.
- The paginated API is treated as a bulk snapshot.
- Snapshot data is stored as NDJSON to make line-by-line processing and bad-line
  recovery straightforward.
- Local state is JSON on disk until a DB metadata store is added.
- Store is a no-op metadata/counting boundary without DB.
- Default tests use real captured fixtures and local HTTP servers.
- Full real remote tests are opt-in and explicit.
- Scheduler integration is a thin adapter over the standalone module.
