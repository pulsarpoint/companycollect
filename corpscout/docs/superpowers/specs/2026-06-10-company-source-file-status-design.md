# Company Source File Status Design

## Purpose

Corpscout needs to show the status of every file required by a company source.
Operators should see which files are expected, which files were downloaded,
when they were downloaded, whether a required file is missing, and the log for a
specific file. Operators should also be able to trigger download of one exact
file without downloading the whole source.

This feature sits under the existing source actions:

```text
source
  expected files
  per-file download runs
  whole-source download action
  ClickHouse import action
```

The source package owns the expected file list. The UI is read-only for source
metadata and file definitions.

## Current State

The current company source model is action-run oriented:

- `data_sources` stores source metadata.
- `data_source_actions` stores source actions such as `pull_source` and
  `import_clickhouse`.
- `data_source_action_runs` stores whole-action history.
- The download workflow returns one source file path through
  `DownloadSourceResult`.
- The UI action tab shows actions and action runs, but it cannot show expected
  file status or retry one file.

This is not enough for sources such as Finland PRH YTJ, where we need a company
snapshot plus code lists such as register codes and status codes.

## Goals

- Persist the list of expected downloadable files for each source.
- Show latest status for every expected file in the UI.
- Keep download history per file.
- Store short structured logs and safe error messages per file run.
- Detect missing files when no successful run exists or when the latest path is
  gone from disk.
- Trigger download of one exact file through Temporal.
- Keep whole-source download as an orchestration over file downloads.
- Let ClickHouse import choose an explicit set of successful file runs.
- Keep source metadata and file definitions read-only in the UI.

## Non-Goals

- Do not build scheduling configuration in this feature. Temporal schedules stay
  separate.
- Do not make source files editable from the UI.
- Do not store large downloaded payloads in Postgres.
- Do not introduce a generic workflow registry abstraction.
- Do not design a combined cross-source company explorer in this feature.
- Do not replace ClickHouse import schemas.

## Decision

Add first-class source file definitions and immutable per-file download runs in
Postgres.

```text
embedded source catalog JSON
  -> startup sync
  -> data_sources
  -> data_source_files

file download workflow
  -> trigger boundary creates data_source_file_runs row
  -> workflow id = company-source-file-run-<file_run_id>
  -> data_source_file_runs
  -> filesystem artifact path

full source download workflow
  -> trigger boundary creates data_source_action_runs row
  -> workflow id = company-source-action-run-<action_run_id>
  -> one data_source_file_runs row per expected file

ClickHouse import workflow
  -> selects successful file runs
  -> imports from those paths
  -> records selected file_run_ids
```

This makes status, history, missing-file detection, and targeted retry
queryable without decoding action-run JSON.

## Temporal Identity Model

Postgres creates the product run identity before Temporal starts. Temporal
workflow IDs are derived from that identity.

```text
data_source_action_runs.id
  -> company-source-action-run-<action_run_id>

data_source_file_runs.id
  -> company-source-file-run-<file_run_id>
```

This lets the API describe Temporal execution by action or file run ID without
searching Temporal:

```text
action_run_id -> workflow_id -> DescribeWorkflowExecution
file_run_id   -> workflow_id -> DescribeWorkflowExecution
```

Postgres remains the product status store because it holds file paths,
checksums, missing-file state, safe errors, and import metadata. Temporal is the
runtime lifecycle source for queued/running/completed workflow state.

Manual API triggers should create the run row before starting the workflow. If
Temporal start fails, the API marks that row `failed` with a safe start error.
Run creation queries must allow `temporal_workflow_id` to be set before
Temporal starts and `temporal_run_id` to be filled after `ExecuteWorkflow`
returns.

Scheduled triggers should follow the same identity rule. If a Temporal schedule
cannot create the action run before starting the real workflow, it should start a
small launcher workflow whose only job is to create the product run row and then
start the real child workflow with the deterministic workflow ID. The action run
stored in Postgres points to the real child workflow, not the launcher.

## Alternatives Considered

### Store File State In `data_source_action_runs.result`

This is simple for the downloader but poor for the product. The UI would need to
parse JSON blobs to answer normal questions such as "which files are missing?"
or "show the last 20 runs for this code list". Retrying one file would not have
a stable identity.

### Scan The Filesystem

Filesystem scanning can tell us what exists, but it cannot reliably answer why a
file is missing, which Temporal run created it, what error occurred, or which
file version was imported. It also does not work well once workers and API
processes run in different places.

### First-Class File Definitions And File Runs

This is the selected approach. It adds a small amount of database structure and
keeps the operational behavior clear.

## Database Schema

### `data_source_files`

`data_source_files` stores the catalog of files a source knows how to download.
Rows are synced from the embedded source catalog on scheduler startup.

```sql
CREATE TABLE data_source_files (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_id UUID NOT NULL REFERENCES data_sources(id) ON DELETE CASCADE,
  file_key TEXT NOT NULL,
  display_name TEXT NOT NULL,
  description TEXT,
  kind TEXT NOT NULL,
  required BOOLEAN NOT NULL DEFAULT true,
  relative_path TEXT NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT true,
  sort_order INTEGER NOT NULL DEFAULT 0,
  config JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (source_id, file_key),
  CONSTRAINT chk_data_source_files_file_key CHECK (file_key <> ''),
  CONSTRAINT chk_data_source_files_display_name CHECK (display_name <> ''),
  CONSTRAINT chk_data_source_files_relative_path CHECK (relative_path <> ''),
  CONSTRAINT chk_data_source_files_kind CHECK (
    kind IN ('source_snapshot', 'code_list', 'reference_data', 'archive')
  ),
  CONSTRAINT chk_data_source_files_config_object CHECK (jsonb_typeof(config) = 'object')
);
```

`file_key` is the stable API and workflow identifier. It should be short and
source-specific, for example `source`, `codelist_REK_en`, or
`codelist_STATUS3_en`.

`relative_path` is the path where the downloader writes the file inside that
file run directory.

### `data_source_file_runs`

`data_source_file_runs` stores every attempt to download one file.

```sql
CREATE TABLE data_source_file_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_id UUID NOT NULL REFERENCES data_sources(id) ON DELETE CASCADE,
  source_file_id UUID NOT NULL REFERENCES data_source_files(id) ON DELETE CASCADE,
  parent_action_run_id UUID REFERENCES data_source_action_runs(id) ON DELETE SET NULL,
  status TEXT NOT NULL,
  temporal_workflow_id TEXT,
  temporal_run_id TEXT,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  path TEXT,
  content_sha256 TEXT,
  content_length_bytes BIGINT,
  records_written BIGINT,
  error_message TEXT,
  log JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_data_source_file_runs_status CHECK (
    status IN ('running', 'succeeded', 'failed', 'missing', 'skipped', 'cancelled')
  ),
  CONSTRAINT chk_data_source_file_runs_log_array CHECK (jsonb_typeof(log) = 'array')
);
```

Suggested indexes:

```sql
CREATE INDEX idx_data_source_file_runs_file_started
  ON data_source_file_runs (source_file_id, started_at DESC);

CREATE INDEX idx_data_source_file_runs_source_status
  ON data_source_file_runs (source_id, status, started_at DESC);

CREATE INDEX idx_data_source_file_runs_parent_action
  ON data_source_file_runs (parent_action_run_id)
  WHERE parent_action_run_id IS NOT NULL;
```

## Source Catalog

The existing embedded source JSON is extended with a `files` array.

```json
{
  "name": "finland_prhytj",
  "country": "finland",
  "source": "prhytj",
  "registry_key": "finland/prhytj",
  "files": [
    {
      "file_key": "source",
      "display_name": "Company source snapshot",
      "description": "Raw PRH YTJ company snapshot preserved as NDJSON.",
      "kind": "source_snapshot",
      "required": true,
      "relative_path": "source.ndjson",
      "enabled": true,
      "sort_order": 10
    },
    {
      "file_key": "codelist_REK_en",
      "display_name": "Register code list",
      "description": "English labels for PRH register codes.",
      "kind": "code_list",
      "required": true,
      "relative_path": "codelists/REK.en.tsv",
      "enabled": true,
      "sort_order": 20
    }
  ]
}
```

Startup sync should:

- validate every source spec
- validate every file spec
- upsert `data_sources`
- upsert `data_source_files`
- disable catalog file rows that are not present in the latest spec for that
  source
- prune data sources not present in the catalog using the existing catalog
  policy

Disabling removed file specs preserves historical file-run rows while making the
file unavailable for new downloads.

## Initial Source File Catalog

The first implementation should define files for the four active sources.

### Finland PRH YTJ

```text
source
codelist_REK_en
codelist_REK_KDI_en
codelist_VIRANOM_en
codelist_TLAJI_en
codelist_YRMU_en
codelist_STATUS3_en
codelist_KIELI_en
```

All are required for the first version. If a code list later proves optional,
the catalog can mark it optional without changing the file-run model.

### United States Colorado Entities

```text
source
```

### United States IRS EO BMF

```text
source
```

### United States SEC EDGAR

```text
source
```

## Filesystem Layout

Each file download writes to an immutable file-run directory. A file-specific
retry must not overwrite an earlier successful run.

```text
<source-runs-root>/
  finland/
    prhytj/
      files/
        source/
          20260610T120000Z-source-<uuid>/
            source.ndjson
        codelist_REK_en/
          20260610T120300Z-codelist_REK_en-<uuid>/
            codelists/
              REK.en.tsv
```

The database stores the absolute path to the final file. Import workflows should
not assume that all files live under the same directory.

## Source Package API

The source package changes from whole-source download to file-specific download.

```go
type Source interface {
    Key() Key
    DisplayName() string
    DownloadFile(ctx context.Context, opts DownloadFileOptions) (DownloadedFile, error)
    Import(ctx context.Context, opts ImportOptions) (ImportResult, error)
}
```

Download input:

```go
type DownloadFileOptions struct {
    FileKey           string
    FileKind          string
    RunDir            string
    RelativePath      string
    SourceURL         string
    UserAgentRequired bool
    Config            map[string]any
}
```

Download output:

```go
type DownloadedFile struct {
    FileKey            string
    Kind               string
    RunDir             string
    Path               string
    RelativePath       string
    ContentSHA256      string
    ContentLengthBytes int64
    RecordsWritten     int64
}
```

The shared `DownloadRun` helper becomes a coordinator that loads the file
definition and calls the concrete source's `DownloadFile`.

## Temporal Workflows And Activities

Use direct workflow and activity registration in the worker wiring.

### New Constants

```text
CompanySourceDownloadFileWorkflow
DownloadSourceFileActivity
```

The existing action keys remain:

```text
pull_source
import_clickhouse
```

No new editable action is required for individual file downloads. The file
endpoint starts the file workflow directly and records the run in
`data_source_file_runs`.

### `DownloadSourceFileWorkflow`

Input:

```go
type DownloadSourceFileInput struct {
    FileRunID         string `json:"file_run_id"`
    SourceName        string `json:"source_name"`
    FileKey           string `json:"file_key"`
    Trigger           string `json:"trigger"`
    ParentActionRunID string `json:"parent_action_run_id,omitempty"`
}
```

Result:

```go
type DownloadSourceFileResult struct {
    FileRunID          string `json:"file_run_id"`
    SourceName         string `json:"source_name"`
    FileKey            string `json:"file_key"`
    Path               string `json:"path"`
    ContentSHA256      string `json:"content_sha256"`
    ContentLengthBytes int64  `json:"content_length_bytes"`
    RecordsWritten     int64  `json:"records_written"`
}
```

Activity behavior:

1. Load the existing `data_source_file_runs` row by `FileRunID`.
2. Validate that it belongs to `SourceName` and `FileKey`.
3. Load source and file definition.
4. Create immutable file-run directory.
5. Call source `DownloadFile`.
6. Finish file run as `succeeded` with path, checksum, size, rows, and log.
7. On error, finish file run as `failed` with safe error message and log.

The activity wraps lower-level errors. The HTTP/worker boundary logs once.

### `DownloadSourceWorkflow`

The whole-source download workflow remains the target for the "Download all"
button and scheduled source pulls.

Input:

```go
type SyncSourceDownloadInput struct {
    ActionRunID string `json:"action_run_id"`
    SourceName  string `json:"source_name"`
    Trigger     string `json:"trigger"`
}
```

Behavior:

1. Load the existing `data_source_action_runs` row by `ActionRunID`.
2. Validate that it is the `pull_source` action for `SourceName`.
3. Load enabled file definitions for the source.
4. Call an activity to create one `data_source_file_runs` row for each enabled
   file with `parent_action_run_id = ActionRunID`.
5. Start one file workflow per file using
   `company-source-file-run-<file_run_id>`.
6. If any required file fails, finish the parent action run as `failed`.
7. If all required files succeed, finish the parent action run as `succeeded`.

The result stores file-level summary:

```json
{
  "action_run_id": "...",
  "files": [
    {
      "file_key": "source",
      "file_run_id": "...",
      "path": "...",
      "records_written": 790016
    }
  ]
}
```

### `ImportSourceToClickHouseWorkflow`

Import should select files by file-run IDs, not by one `run_dir`.

Input:

```go
type ImportSourceToClickHouseInput struct {
    ActionRunID         string   `json:"action_run_id"`
    SourceName          string   `json:"source_name"`
    Trigger             string   `json:"trigger"`
    DownloadActionRunID string   `json:"download_action_run_id,omitempty"`
    FileRunIDs          []string `json:"file_run_ids,omitempty"`
    BatchSize           int      `json:"batch_size"`
    Limit               int64    `json:"limit"`
}
```

Selection rules:

1. If `FileRunIDs` is present, import exactly those file runs after validation.
2. Else if `DownloadActionRunID` is present, import successful file runs linked
   to that parent action run.
3. Else select the latest successful run for every required enabled file.

The import activity loads the existing `data_source_action_runs` row by
`ActionRunID`, validates that it is the `import_clickhouse` action for
`SourceName`, and finishes that same row when the import succeeds or fails.

Before import, validate:

- every required enabled file has one selected successful run
- every selected file run belongs to the same source
- every selected file path exists on disk

The import action result records `selected_file_run_ids` and imported table
metrics.

### `SyncSourceToClickHouseWorkflow`

Sync remains a composition workflow:

```text
download all files
  -> import file runs from that parent download action
```

For manual "download and import", the API should create the outer sync workflow
with a deterministic workflow ID based on an action-run row only if we decide to
persist sync as its own product run. In the first implementation, sync can remain
an orchestration convenience that creates and starts a normal download action run
and then a normal import action run. The durable product runs are still the
download action run, the import action run, and the file runs.

## HTTP API

Add file endpoints under the existing source API.

```text
GET  /api/v1/sources/{name}/files
GET  /api/v1/sources/{name}/files/{file_key}/runs?limit=50
POST /api/v1/sources/{name}/files/{file_key}/download
GET  /api/v1/source-action-runs/{id}/temporal-status
GET  /api/v1/source-file-runs/{id}/temporal-status
```

Existing source action trigger endpoints should create the corresponding
`data_source_action_runs` row before starting Temporal:

```text
POST /api/v1/sources/{name}/actions/pull_source/trigger
  -> create action run
  -> workflow id company-source-action-run-<action_run_id>
  -> start CompanySourceDownloadWorkflow with action_run_id

POST /api/v1/sources/{name}/actions/import_clickhouse/trigger
  -> create action run
  -> workflow id company-source-action-run-<action_run_id>
  -> start CompanySourceClickHouseImportWorkflow with action_run_id
```

The Temporal status endpoints describe the workflow associated with the run ID
and return both product and runtime status.

```json
{
  "id": "...",
  "db_status": "running",
  "temporal_status": "RUNNING",
  "workflow_id": "company-source-action-run-...",
  "workflow_run_id": "...",
  "started_at": "2026-06-10T12:00:00Z",
  "finished_at": null
}
```

### List Files Response

```json
{
  "items": [
    {
      "id": "...",
      "source_id": "...",
      "source_name": "finland_prhytj",
      "file_key": "source",
      "display_name": "Company source snapshot",
      "description": "Raw PRH YTJ company snapshot preserved as NDJSON.",
      "kind": "source_snapshot",
      "required": true,
      "relative_path": "source.ndjson",
      "enabled": true,
      "sort_order": 10,
      "latest_status": "succeeded",
      "missing": false,
      "latest_run": {
        "id": "...",
        "status": "succeeded",
        "started_at": "2026-06-10T12:00:00Z",
        "finished_at": "2026-06-10T12:05:00Z",
        "path": "/var/lib/corpscout/source-runs/...",
        "content_sha256": "...",
        "content_length_bytes": 123456,
        "records_written": 789
      }
    }
  ]
}
```

`missing` is computed by the API:

- `true` when no latest successful run exists
- `true` when the latest successful path is not present on the API host
- `false` otherwise

If API and worker run on different machines later, path existence can move to a
worker-side health activity. For the current deployment, the scheduler API can
check the mounted filesystem directly.

### File Run History Response

```json
{
  "items": [
    {
      "id": "...",
      "file_key": "source",
      "status": "failed",
      "started_at": "2026-06-10T11:00:00Z",
      "finished_at": "2026-06-10T11:01:00Z",
      "path": null,
      "error_message": "download source file: status 503",
      "log": []
    }
  ]
}
```

### Download One File Response

`POST /api/v1/sources/{name}/files/{file_key}/download` starts
`CompanySourceDownloadFileWorkflow` and returns the existing workflow-start
response shape.

The handler first creates a `data_source_file_runs` row with `running`, derives
the workflow ID from that row ID, starts Temporal, then stores the Temporal run
ID on the row. If Temporal start fails, it marks the file run as `failed`.

```json
{
  "status": "started",
  "workflow": "CompanySourceDownloadFileWorkflow",
  "task_queue": "corpscout-company-sources",
  "workflow_id": "...",
  "workflow_run_id": "..."
}
```

## UI Design

Add a file status section to the source detail experience. The first
implementation can place it in the existing Actions tab. If the tab becomes too
dense, split it into a dedicated Files tab afterward.

Top actions stay:

```text
Download all
Import
Download and import
Refresh
```

Add a files table:

```text
File
Kind
Required
Status
Last downloaded
Size
Rows
Path
Actions
```

Per-file actions:

```text
Download
View runs
```

Status display:

- `Missing` when `missing = true`
- `Succeeded` for latest successful run with existing file
- `Failed` for latest failed run
- `Running` for latest running run
- `Not downloaded` when no run exists

The UI should not include controls to edit file definitions. Definitions are
owned by source catalog JSON and source code.

## Logs

`data_source_file_runs.log` stores short structured events.

Example:

```json
[
  {
    "at": "2026-06-10T12:00:00Z",
    "level": "info",
    "message": "download started",
    "url": "https://avoindata.prh.fi/opendata-ytj-api/v3/companies"
  },
  {
    "at": "2026-06-10T12:05:00Z",
    "level": "info",
    "message": "file written",
    "bytes": 123456,
    "records_written": 789
  }
]
```

Do not log secrets, tokens, cookies, sensitive request bodies, or large response
bodies.

## Error Handling

Source download helpers should wrap errors with `github.com/cockroachdb/errors`.
They should not log every failure.

Boundary layers should log once:

- HTTP handlers log API failures before returning a safe response.
- Temporal activity boundary records safe error text in `data_source_file_runs`.
- Worker startup logs registration and fatal startup problems.

External API error responses should not leak stack traces.

## Testing Plan

Database tests:

- migration creates `data_source_files`
- migration creates `data_source_file_runs`
- constraints reject invalid file kinds and statuses
- latest-file query prefers newest successful run
- missing-file query handles no run and failed latest run

Catalog tests:

- embedded specs load with file definitions
- every spec validates at least one file
- startup sync upserts file definitions
- startup sync disables files removed from the catalog

Temporal tests:

- file download workflow calls file activity with source and file key
- file activity creates and finishes file run on success
- file activity finishes file run as failed on download error
- full download action links child file runs to parent action run
- full download fails when a required file fails
- import selects latest successful required files when no explicit file set is
  provided

HTTP API tests:

- list files returns file definitions with latest status
- list file runs clamps limit
- trigger one file starts `CompanySourceDownloadFileWorkflow`
- trigger one file creates a file run before starting Temporal
- trigger one file uses `company-source-file-run-<file_run_id>` as workflow ID
- trigger one file rejects unknown file key
- trigger source action creates an action run before starting Temporal
- trigger source action uses `company-source-action-run-<action_run_id>` as
  workflow ID
- Temporal status endpoint describes workflow execution by deterministic ID

UI tests:

- API types include source files and file runs
- Actions tab renders missing and succeeded file rows
- per-file download button calls the file endpoint
- import is disabled when required files are missing

## Rollout Plan

1. Add database migration and sqlc queries.
2. Extend source catalog structs, JSON specs, validation, and startup sync.
3. Add file-run persistence helpers through sqlc.
4. Refactor source download API to `DownloadFile`.
5. Implement file-specific Temporal workflow and activity.
6. Update full download workflow to orchestrate file downloads.
7. Update import workflow to select and validate file runs.
8. Add HTTP endpoints for source files.
9. Add UI file status table and per-file download trigger.
10. Update tests and run backend plus UI verification.

## Open Decisions Resolved By This Spec

- File definitions are in Postgres, but sourced from embedded catalog JSON.
- File definitions are read-only in the UI.
- File runs are immutable and per-file.
- Single-file retries do not mutate previous download directories.
- Import selects file runs explicitly and records selected IDs.
- The existing `pull_source` action remains the user-facing "download all"
  action.
