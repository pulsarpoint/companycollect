# Companysource Binary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build one `companysource` CLI and rewrite the existing Finland/US source modules so each `{country, source}` downloads source files, preserves source data into Parquet, generates ClickHouse migrations, and imports directly to ClickHouse.

**Architecture:** The new active ingestion path has only two source data stages: `download` and `export-parquet`. Each run is one flat folder that contains the downloaded source file(s), source-specific Parquet files, and one manifest. There is no country-level final export, reduced projection export, separate snapshot folder, separate processed folder, or hidden process/transformation stage.

**Tech Stack:** Go 1.26, `log/slog`, `github.com/cockroachdb/errors`, `parquet-go`, existing `companies/common/countryimport`, Dockerized `clickhouse-local`, Dockerized `clickhouse-client`, deterministic ClickHouse migration generation from Parquet.

---

## Hard Requirements

- Rewrite existing source modules to the new contract instead of wrapping the old multi-stage CLIs.
- Source modules expose only:
  - `Download`
  - `ExportParquet`
  - source metadata/config helpers
- `companysource` CLI exposes:
  - `download`
  - `export-parquet`
  - `generate-clickhouse-migration`
  - `import-clickhouse`
  - `list-sources`
  - `status`
- Do not add `build-export`, `sync`, `process`, or country-level final export commands to `companysource`.
- Do not produce reduced/company-only final Parquet as the main artifact.
- Preserve as much source information as possible in source-specific Parquet tables.
- Reuse existing downloaded source files and source Parquet exports by copying them into the new flat run-folder layout. Do not redownload Finland PRH YTJ or existing US sources just to adopt the new layout.
- Each source run uses one flat folder:

```text
companies/data/<country>/sources/<source>/runs/<run_id>/
  manifest.json
  source.ndjson
  raw_records.parquet
  companies.parquet
  company_names.parquet
  addresses.parquet
  websites.parquet
```

The exact Parquet file list is source-specific, but all files for the run live in the same folder. No `snapshots/`, `processed/`, `exports/`, `final/`, or nested run artifact folders in the new path.

## File Structure

- Create `companies/companysource/go.mod`: standalone module for the unified CLI.
- Create `companies/companysource/cmd/companysource/main.go`: CLI entry point and boundary logging.
- Create `companies/companysource/internal/cli/config.go`: flag parsing.
- Create `companies/companysource/internal/cli/run.go`: command dispatch.
- Create `companies/companysource/internal/source/source.go`: source adapter contract.
- Create `companies/companysource/internal/source/result.go`: shared result types.
- Create `companies/companysource/internal/registry/registry.go`: concrete registry keyed by `{country, source}`.
- Create `companies/companysource/internal/adapters/finland/prhytj.go`: rewritten Finland PRH YTJ adapter.
- Create `companies/companysource/internal/adapters/unitedstates/irseobmf.go`: rewritten IRS EO BMF adapter.
- Create `companies/companysource/internal/adapters/unitedstates/secedgar.go`: rewritten SEC EDGAR adapter.
- Create `companies/companysource/internal/adapters/unitedstates/coloradoentities.go`: rewritten Colorado entities adapter.
- Create `companies/common/companysource/runfolder.go`: shared run folder naming and manifest helpers.
- Create `companies/common/companysource/runfolder_test.go`: run folder tests.
- Modify `companies/finland/prhytj`: replace old snapshot/export/final flow with flat run folder download/export.
- Modify `companies/united_states/irseobmf`: replace old download/process/export flow with flat run folder download/export.
- Modify `companies/united_states/secedgar`: replace old download/process/export flow with flat run folder download/export.
- Modify `companies/united_states/coloradoentities`: replace old download/process/export flow with flat run folder download/export.
- Modify `corpscout/clickhouse`: expose deterministic migration generation and native import as reusable packages.
- Modify `corpscout/Makefile`: use `companysource` for ClickHouse generation/import.
- Modify docs in `companies/docs` to describe the new source run layout.
- Local data cleanup: copy existing ignored files from `companies/data/*/countrydata/sources/*/{snapshots,exports}` into `companies/data/<country>/sources/<source>/runs/<run_id>/`.

## New Source Contract

```go
package source

import "context"

type Key struct {
	Country string `json:"country"`
	Source  string `json:"source"`
}

type DownloadOptions struct {
	RunDir   string
	RunID    string
	MaxPages int
}

type ExportParquetOptions struct {
	RunDir string
	Limit  int64
}

type ClickHouseMigrationOptions struct {
	RunDir   string
	Database string
	Out      string
	DownOut  string
}

type ClickHouseImportOptions struct {
	RunDir              string
	Database            string
	ClickHouseNativeURL string
	SourceExportID      string
	ClickHouseImage     string
	DockerMount         string
}

type Adapter interface {
	Key() Key
	DisplayName() string
	Download(ctx context.Context, opts DownloadOptions) (DownloadResult, error)
	ExportParquet(ctx context.Context, opts ExportParquetOptions) (ExportParquetResult, error)
	GenerateClickHouseMigration(ctx context.Context, opts ClickHouseMigrationOptions) (ClickHouseMigrationResult, error)
	ImportClickHouse(ctx context.Context, opts ClickHouseImportOptions) (ClickHouseImportResult, error)
	Status(ctx context.Context, runDir string) (StatusResult, error)
}
```

This interface is justified because `companysource` has multiple real source implementations. Do not add extra interfaces inside source packages unless there are multiple real implementations.

## CLI Shape

```bash
companysource list-sources

companysource download \
  --country finland \
  --source prhytj \
  --run-dir /Users/graovic/pulsarpoint/ppoint/companycollect/companies/data/finland/sources/prhytj/runs/20260609T100000Z-prhytj

companysource export-parquet \
  --country finland \
  --source prhytj \
  --run-dir /Users/graovic/pulsarpoint/ppoint/companycollect/companies/data/finland/sources/prhytj/runs/20260609T100000Z-prhytj

companysource generate-clickhouse-migration \
  --country finland \
  --source prhytj \
  --run-dir /Users/graovic/pulsarpoint/ppoint/companycollect/companies/data/finland/sources/prhytj/runs/20260609T100000Z-prhytj \
  --database corpscout_sources \
  --out /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/clickhouse/migrations/000002_create_finland_prhytj_tables.up.sql \
  --down-out /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/clickhouse/migrations/000002_create_finland_prhytj_tables.down.sql

companysource import-clickhouse \
  --country finland \
  --source prhytj \
  --run-dir /Users/graovic/pulsarpoint/ppoint/companycollect/companies/data/finland/sources/prhytj/runs/20260609T100000Z-prhytj \
  --database corpscout_sources \
  --clickhouse-native-url 'clickhouse://host.docker.internal:9002?username=default&password=change-me&database=corpscout_sources' \
  --source-export-id 00000000-0000-0000-0000-000000000000
```

`--run-dir` is the only data location argument in the new CLI. It points to the flat folder containing source files, Parquet files, and manifest.

## Task 1: Add Shared Flat Run Folder Helpers

**Files:**
- Create: `companies/common/companysource/runfolder.go`
- Create: `companies/common/companysource/runfolder_test.go`
- Modify: `companies/common/go.mod`

- [ ] **Step 1: Write run folder tests**

Create `companies/common/companysource/runfolder_test.go`:

```go
package companysource

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDefaultRunDir(t *testing.T) {
	path := DefaultRunDir("/data", "finland", "prhytj", "20260609T100000Z-prhytj")
	require.Equal(t, "/data/finland/sources/prhytj/runs/20260609T100000Z-prhytj", path)
}

func TestSourceFileName(t *testing.T) {
	require.Equal(t, "source.ndjson", SourceFileName("ndjson"))
	require.Equal(t, "source.json", SourceFileName(".json"))
}
```

- [ ] **Step 2: Implement helpers**

Create `companies/common/companysource/runfolder.go`:

```go
package companysource

import (
	"path/filepath"
	"strings"
)

func DefaultRunDir(dataRoot string, country string, source string, runID string) string {
	return filepath.Join(dataRoot, country, "sources", source, "runs", runID)
}

func SourceFileName(ext string) string {
	ext = strings.TrimPrefix(strings.TrimSpace(ext), ".")
	if ext == "" {
		ext = "dat"
	}
	return "source." + ext
}

func ManifestPath(runDir string) string {
	return filepath.Join(runDir, "manifest.json")
}
```

- [ ] **Step 3: Run tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/companies/common
GOWORK=off go test ./...
```

Expected: pass.

- [ ] **Step 4: Commit**

```bash
git add companies/common/companysource companies/common/go.mod companies/common/go.sum
git commit -m "feat: add companysource run folder helpers"
```

## Task 2: Clean Up Existing Downloaded Data Into Flat Run Folders

**Files:**
- Local ignored data only; no committed data files expected.

This task preserves current local source downloads and current source-specific Parquet outputs. It deliberately ignores country-level final exports under `companies/data/finland/countrydata/final`.

- [ ] **Step 1: Relayout Finland PRH YTJ latest source export**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect

OLD_EXPORT="companies/data/finland/countrydata/sources/prhytj/exports/20260608T201348Z-prhytj"
RUN_ID="$(basename "$OLD_EXPORT")"
NEW_RUN="companies/data/finland/sources/prhytj/runs/$RUN_ID"
SOURCE_PATH="$(jq -r '.inputs[0].path' "$OLD_EXPORT/manifest.json")"

mkdir -p "$NEW_RUN"
cp "$SOURCE_PATH" "$NEW_RUN/source.ndjson"
cp "$OLD_EXPORT"/*.parquet "$NEW_RUN/"
cp "$OLD_EXPORT/manifest.json" "$NEW_RUN/manifest.legacy.json"
```

Expected flat folder:

```text
companies/data/finland/sources/prhytj/runs/20260608T201348Z-prhytj/
  source.ndjson
  raw_records.parquet
  companies.parquet
  company_names.parquet
  legal_forms.parquet
  industries.parquet
  addresses.parquet
  registered_entries.parquet
  tax_registrations.parquet
  websites.parquet
  manifest.legacy.json
```

The rewritten `ExportParquet` command will later regenerate `manifest.json` in this same folder. Keep `manifest.legacy.json` for traceability to the old artifact layout.

- [ ] **Step 2: Relayout US SEC EDGAR latest source export**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect

OLD_EXPORT="companies/data/united_states/countrydata/sources/secedgar/exports/20260608T203500Z-secedgar"
RUN_ID="$(basename "$OLD_EXPORT")"
NEW_RUN="companies/data/united_states/sources/secedgar/runs/$RUN_ID"
SOURCE_PATH="$(jq -r '.inputs[0].path' "$OLD_EXPORT/manifest.json")"

mkdir -p "$NEW_RUN"
cp "$SOURCE_PATH" "$NEW_RUN/source.json"
cp "$OLD_EXPORT"/*.parquet "$NEW_RUN/"
cp "$OLD_EXPORT/manifest.json" "$NEW_RUN/manifest.legacy.json"
```

Expected: `source.json`, the source-specific Parquet files, and `manifest.legacy.json` are all in one folder.

- [ ] **Step 3: Relayout US IRS EO BMF latest source export**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect

OLD_EXPORT="companies/data/united_states/countrydata/sources/irseobmf/exports/20260608T203550Z-irseobmf"
RUN_ID="$(basename "$OLD_EXPORT")"
NEW_RUN="companies/data/united_states/sources/irseobmf/runs/$RUN_ID"
SOURCE_PATH="$(jq -r '.inputs[0].path' "$OLD_EXPORT/manifest.json")"

mkdir -p "$NEW_RUN"
cp "$SOURCE_PATH" "$NEW_RUN/source.ndjson"
cp "$OLD_EXPORT"/*.parquet "$NEW_RUN/"
cp "$OLD_EXPORT/manifest.json" "$NEW_RUN/manifest.legacy.json"
```

The currently available full IRS source snapshot is NDJSON. Do not invent a CSV source file for this legacy run if the full downloaded CSV is not present locally. The rewritten downloader can use `source.csv` for future IRS runs.

- [ ] **Step 4: Relayout US Colorado entities latest source export**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect

OLD_EXPORT="companies/data/united_states/countrydata/sources/coloradoentities/exports/20260608T233837Z-coloradoentities"
RUN_ID="$(basename "$OLD_EXPORT")"
NEW_RUN="companies/data/united_states/sources/coloradoentities/runs/$RUN_ID"
SOURCE_PATH="$(jq -r '.inputs[0].path' "$OLD_EXPORT/manifest.json")"

mkdir -p "$NEW_RUN"
cp "$SOURCE_PATH" "$NEW_RUN/source.ndjson"
cp "$OLD_EXPORT"/*.parquet "$NEW_RUN/"
cp "$OLD_EXPORT/manifest.json" "$NEW_RUN/manifest.legacy.json"
```

- [ ] **Step 5: Verify the new folders are flat**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect

find companies/data/finland/sources/prhytj/runs/20260608T201348Z-prhytj -mindepth 2 -print
find companies/data/united_states/sources/secedgar/runs/20260608T203500Z-secedgar -mindepth 2 -print
find companies/data/united_states/sources/irseobmf/runs/20260608T203550Z-irseobmf -mindepth 2 -print
find companies/data/united_states/sources/coloradoentities/runs/20260608T233837Z-coloradoentities -mindepth 2 -print
```

Expected: no output. If any command prints nested files, the layout is not flat and must be fixed before continuing.

- [ ] **Step 6: Verify old final exports are not copied**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
find companies/data -path '*final*' -path '*sources/*/runs/*' -print
```

Expected: no output.

- [ ] **Step 7: Check git status**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git status --short
```

Expected: no new tracked files from `companies/data`, because `companies/data/` is ignored. The data relayout is local runtime cleanup, not a repository commit.

## Task 3: Create `companies/companysource` Module and CLI Parser

**Files:**
- Create: `companies/companysource/go.mod`
- Create: `companies/companysource/cmd/companysource/main.go`
- Create: `companies/companysource/internal/source/source.go`
- Create: `companies/companysource/internal/source/result.go`
- Create: `companies/companysource/internal/cli/config.go`
- Create: `companies/companysource/internal/cli/config_test.go`
- Create: `companies/companysource/internal/cli/run.go`

- [ ] **Step 1: Create module**

Create `companies/companysource/go.mod`:

```go
module github.com/pulsarpoint/companycollect/companies/companysource

go 1.26.1

require (
	github.com/cockroachdb/errors v1.13.0
	github.com/pulsarpoint/companycollect/companies/common v0.0.0
	github.com/pulsarpoint/companycollect/companies/finland v0.0.0
	github.com/pulsarpoint/companycollect/companies/united_states v0.0.0
	github.com/pulsarpoint/corpscout/clickhouse v0.0.0
)

replace github.com/pulsarpoint/companycollect/companies/common => ../common
replace github.com/pulsarpoint/companycollect/companies/finland => ../finland
replace github.com/pulsarpoint/companycollect/companies/united_states => ../united_states
replace github.com/pulsarpoint/corpscout/clickhouse => ../../corpscout/clickhouse
```

- [ ] **Step 2: Add source contract and results**

Create `companies/companysource/internal/source/source.go` and `result.go` using the contract from the "New Source Contract" section.

`result.go` should contain:

```go
package source

type DownloadResult struct {
	RunDir      string `json:"run_dir"`
	SourcePath  string `json:"source_path"`
	RecordsSeen int64  `json:"records_seen"`
}

type ExportParquetResult struct {
	RunDir          string   `json:"run_dir"`
	ManifestPath    string   `json:"manifest_path"`
	ParquetFiles     []string `json:"parquet_files"`
	RecordsSeen      int64    `json:"records_seen"`
	RecordsExported  int64    `json:"records_exported"`
	DecodeErrors     int64    `json:"decode_errors"`
}

type ClickHouseMigrationResult struct {
	UpPath   string `json:"up_path"`
	DownPath string `json:"down_path"`
}

type ClickHouseImportResult struct {
	ImportedTables int      `json:"imported_tables"`
	Tables         []string `json:"tables"`
}

type StatusResult struct {
	Status       string `json:"status"`
	RunDir       string `json:"run_dir"`
	ManifestPath string `json:"manifest_path,omitempty"`
}
```

- [ ] **Step 3: Add parser tests**

Create `companies/companysource/internal/cli/config_test.go`:

```go
package cli

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseDownload(t *testing.T) {
	cfg, err := parseArgs([]string{"download", "--country", "finland", "--source", "prhytj", "--run-dir", "/runs/fi"})
	require.NoError(t, err)
	require.Equal(t, "download", cfg.Command)
	require.Equal(t, "/runs/fi", cfg.RunDir)
}

func TestParseExportParquet(t *testing.T) {
	cfg, err := parseArgs([]string{"export-parquet", "--country", "finland", "--source", "prhytj", "--run-dir", "/runs/fi"})
	require.NoError(t, err)
	require.Equal(t, "export-parquet", cfg.Command)
}

func TestImportRequiresNativeURL(t *testing.T) {
	_, err := parseArgs([]string{"import-clickhouse", "--country", "finland", "--source", "prhytj", "--run-dir", "/runs/fi", "--database", "corpscout_sources", "--source-export-id", "00000000-0000-0000-0000-000000000000"})
	require.EqualError(t, err, "missing --clickhouse-native-url")
}
```

- [ ] **Step 4: Implement parser**

Create `companies/companysource/internal/cli/config.go` with commands:

```go
download
export-parquet
generate-clickhouse-migration
import-clickhouse
list-sources
status
```

Every command except `list-sources` requires `--country` and `--source`. Every command except `list-sources` requires `--run-dir`. `generate-clickhouse-migration` also requires `--database`, `--out`, and `--down-out`. `import-clickhouse` also requires `--database`, `--clickhouse-native-url`, and `--source-export-id`.

- [ ] **Step 5: Add boundary main**

Create `companies/companysource/cmd/companysource/main.go`:

```go
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"

	"github.com/pulsarpoint/companycollect/companies/companysource/internal/cli"
	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

func main() {
	result, err := cli.Run(context.Background(), os.Args[1:])
	if err != nil {
		slog.Error("run companysource command", "error_kind", countryimport.Classify(err), "error", err)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		slog.Error("write companysource result", "error", err)
		os.Exit(1)
	}
}
```

- [ ] **Step 6: Commit skeleton**

```bash
git add companies/companysource
git commit -m "feat: add companysource cli skeleton"
```

## Task 4: Rewrite Finland PRH YTJ Source to Flat Download and Parquet Export

**Files:**
- Modify: `companies/finland/prhytj/download.go`
- Modify: `companies/finland/prhytj/export.go`
- Modify: `companies/finland/prhytj/export_rows.go`
- Modify: `companies/finland/prhytj/parquet_writer.go`
- Modify: `companies/finland/prhytj/export_test.go`
- Modify: `companies/finland/prhytj/download_test.go`
- Remove active dependency from: `companies/finland/export.go`

- [ ] **Step 1: Change download output**

Rewrite PRH YTJ download so it writes the API snapshot directly into:

```text
<run-dir>/source.ndjson
```

No new code should write `snapshots/`.

- [ ] **Step 2: Change export input/output**

Rewrite PRH YTJ export so it reads:

```text
<run-dir>/source.ndjson
```

and writes Parquet files directly into the same `<run-dir>`:

```text
<run-dir>/raw_records.parquet
<run-dir>/companies.parquet
<run-dir>/company_names.parquet
<run-dir>/legal_forms.parquet
<run-dir>/industries.parquet
<run-dir>/addresses.parquet
<run-dir>/registered_entries.parquet
<run-dir>/tax_registrations.parquet
<run-dir>/websites.parquet
<run-dir>/manifest.json
```

- [ ] **Step 3: Preserve source data**

Keep `raw_records.parquet` as the complete source payload table with payload hash, raw payload, and source row metadata. The other Parquet tables should preserve all structured fields currently decoded by `CompanyRecord`; do not drop fields only because Corpscout does not need them yet.

- [ ] **Step 4: Remove final export from active path**

Do not call `finland.BuildFinalExport` from any new `companysource` path. Mark the old country final export command as deprecated in the old CLI only if it remains for compatibility.

- [ ] **Step 5: Verify Finland tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/companies/finland
GOWORK=off go test ./...
```

Expected: pass.

- [ ] **Step 6: Commit Finland rewrite**

```bash
git add companies/finland
git commit -m "feat: rewrite finland prhytj flat parquet export"
```

## Task 5: Rewrite United States Sources to Flat Download and Parquet Export

**Files:**
- Modify: `companies/united_states/irseobmf/*`
- Modify: `companies/united_states/secedgar/*`
- Modify: `companies/united_states/coloradoentities/*`
- Modify: `companies/united_states/cmd/united-states-countrydata/main.go`
- Modify: `companies/united_states/*_test.go`

- [ ] **Step 1: Rewrite IRS EO BMF**

Write the downloaded IRS source file into:

```text
<run-dir>/source.csv
```

Export preserved Parquet files into the same folder. Remove active use of `Process`; decode the source file during `ExportParquet`.

- [ ] **Step 2: Rewrite SEC EDGAR**

Write the downloaded SEC EDGAR source file into:

```text
<run-dir>/source.json
```

Export preserved Parquet files into the same folder. Remove active use of `Process`; decode the source file during `ExportParquet`.

- [ ] **Step 3: Rewrite Colorado entities**

Write the downloaded Colorado source file into:

```text
<run-dir>/source.ndjson
```

Export preserved Parquet files into the same folder. Remove active use of `Process`; decode the source file during `ExportParquet`.

- [ ] **Step 4: Keep raw source payload tables**

Each US source export must include a `raw_records.parquet` or equivalent table with enough original payload information to reconstruct/debug source rows.

- [ ] **Step 5: Verify US tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/companies/united_states
GOWORK=off go test ./...
```

Expected: pass.

- [ ] **Step 6: Commit US rewrite**

```bash
git add companies/united_states
git commit -m "feat: rewrite united states flat parquet exports"
```

## Task 6: Add Registry and Source Adapters

**Files:**
- Create: `companies/companysource/internal/registry/registry.go`
- Create: `companies/companysource/internal/registry/registry_test.go`
- Create: `companies/companysource/internal/adapters/finland/prhytj.go`
- Create: `companies/companysource/internal/adapters/unitedstates/irseobmf.go`
- Create: `companies/companysource/internal/adapters/unitedstates/secedgar.go`
- Create: `companies/companysource/internal/adapters/unitedstates/coloradoentities.go`
- Modify: `companies/companysource/internal/cli/run.go`

- [ ] **Step 1: Add registry tests**

Create tests that assert these keys exist:

```text
finland/prhytj
united_states/irseobmf
united_states/secedgar
united_states/coloradoentities
```

- [ ] **Step 2: Implement registry**

`registry.Default()` returns concrete adapters for the four current sources. `Get(country, source)` returns `unknown company source <country>/<source>` for missing keys.

- [ ] **Step 3: Implement adapters**

Each adapter calls the rewritten source package methods:

```go
Download(ctx, RunDir)
ExportParquet(ctx, RunDir)
```

Adapters should not call old `Process`, `BuildFinalExport`, or country final export APIs.

- [ ] **Step 4: Wire CLI dispatch**

`cli.Run` should dispatch:

```text
download -> adapter.Download
export-parquet -> adapter.ExportParquet
status -> adapter.Status
```

ClickHouse methods can return explicit unsupported errors until Task 7.

- [ ] **Step 5: Verify companysource tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/companies/companysource
GOWORK=off go test ./...
```

Expected: pass.

- [ ] **Step 6: Commit adapters**

```bash
git add companies/companysource
git commit -m "feat: register companysource adapters"
```

## Task 7: Expose ClickHouse Generation and Import as Reusable Packages

**Files:**
- Create: `corpscout/clickhouse/parquetddl`
- Create: `corpscout/clickhouse/chimport`
- Modify: `corpscout/clickhouse/tools/parquetddl`
- Modify: `corpscout/clickhouse/tools/chimport`
- Modify: `companies/companysource/internal/adapters/*`

- [ ] **Step 1: Extract migration generation package**

Move reusable logic from `corpscout/clickhouse/tools/parquetddl` to `corpscout/clickhouse/parquetddl`:

```go
type GenerateOptions struct {
	Source    string
	Database  string
	RunDir    string
	Config    string
	Out       string
	DownOut   string
}
```

The package reads Parquet files directly from `RunDir`.

- [ ] **Step 2: Extract import package**

Move reusable logic from `corpscout/clickhouse/tools/chimport` to `corpscout/clickhouse/chimport`:

```go
type ImportOptions struct {
	Database            string
	SourceExportID      string
	RunDir              string
	Config              string
	ClickHouseNativeURL string
	ClickHouseImage     string
	DockerMount         string
}
```

The package imports Parquet files directly from `RunDir`.

- [ ] **Step 3: Keep existing tools as wrappers**

`tools/parquetddl` and `tools/chimport` remain available, but contain only flag parsing and calls into the packages.

- [ ] **Step 4: Wire Finland ClickHouse methods**

Finland adapter calls:

```go
parquetddl.Generate(ctx, parquetddl.GenerateOptions{Source: "finland_prhytj", RunDir: opts.RunDir, Database: opts.Database, Config: ".../corpscout/clickhouse/sources/finland_prhytj.yaml", Out: opts.Out, DownOut: opts.DownOut})
chimport.Import(ctx, chimport.ImportOptions{RunDir: opts.RunDir, Database: opts.Database, Config: ".../corpscout/clickhouse/sources/finland_prhytj.yaml", ClickHouseNativeURL: opts.ClickHouseNativeURL, SourceExportID: opts.SourceExportID})
```

- [ ] **Step 5: Verify ClickHouse tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/clickhouse
GOWORK=off go test ./...
```

Expected: pass.

- [ ] **Step 6: Commit ClickHouse package extraction**

```bash
git add corpscout/clickhouse companies/companysource
git commit -m "feat: expose clickhouse source import packages"
```

## Task 8: Update Corpscout Make Targets

**Files:**
- Modify: `corpscout/Makefile`
- Modify: `corpscout/.env.example`

- [ ] **Step 1: Route Finland migration target through `companysource`**

`clickhouse-generate-finland-prhytj` should call:

```bash
GOWORK=off go run ../companies/companysource/cmd/companysource generate-clickhouse-migration \
  --country finland \
  --source prhytj \
  --run-dir "$(FINLAND_PRHYTJ_RUN_DIR)" \
  --database corpscout_sources \
  --out corpscout/clickhouse/migrations/000002_create_finland_prhytj_tables.up.sql \
  --down-out corpscout/clickhouse/migrations/000002_create_finland_prhytj_tables.down.sql
```

- [ ] **Step 2: Route Finland import target through `companysource`**

`clickhouse-import-finland-prhytj` should call:

```bash
GOWORK=off go run ../companies/companysource/cmd/companysource import-clickhouse \
  --country finland \
  --source prhytj \
  --run-dir "$(FINLAND_PRHYTJ_RUN_DIR)" \
  --database corpscout_sources \
  --clickhouse-native-url "$(CLICKHOUSE_NATIVE_URL)" \
  --source-export-id "$(FINLAND_PRHYTJ_SOURCE_EXPORT_ID)"
```

- [ ] **Step 3: Rename env variable**

Replace `FINLAND_PRHYTJ_EXPORT_DIR` with `FINLAND_PRHYTJ_RUN_DIR`.

- [ ] **Step 4: Commit Makefile update**

```bash
git add corpscout/Makefile corpscout/.env.example
git commit -m "chore: use companysource for clickhouse source targets"
```

## Task 9: Remove or Deprecate Old Country CLIs

**Files:**
- Modify: `companies/finland/cmd/finland-countrydata/main.go`
- Modify: `companies/united_states/cmd/united-states-countrydata/main.go`
- Modify: `companies/docs/countrydata-architecture.md`
- Modify: `companies/docs/countrydata-package-implementation-guide.md`

- [ ] **Step 1: Deprecate old CLIs**

Old country CLIs should print an error directing users to `companysource`. They should not continue implementing old `sync`, `process`, `build-export`, or final export behavior.

- [ ] **Step 2: Update docs**

Docs must state:

```text
Each source run is one flat folder containing downloaded source file(s), source-specific Parquet files, and manifest.json. There are no separate snapshot, processed, export, or final folders in the active companysource path.
```

- [ ] **Step 3: Commit old CLI deprecation**

```bash
git add companies/finland/cmd companies/united_states/cmd companies/docs
git commit -m "chore: deprecate old countrydata clis"
```

## Task 10: Add US ClickHouse Source Configs After Rewrites

**Files:**
- Create: `corpscout/clickhouse/sources/united_states_irseobmf.yaml`
- Create: `corpscout/clickhouse/sources/united_states_secedgar.yaml`
- Create: `corpscout/clickhouse/sources/united_states_coloradoentities.yaml`
- Create: generated migrations under `corpscout/clickhouse/migrations`
- Modify: US adapters to enable ClickHouse generation/import.

- [ ] **Step 1: Export rewritten US run folders**

Use the new `download` and `export-parquet` commands for each US source.

- [ ] **Step 2: Create source configs from actual Parquet files**

Use source-specific table names such as:

```text
us_irseobmf_raw_records
us_irseobmf_companies
us_secedgar_raw_records
us_secedgar_companies
us_coloradoentities_raw_records
us_coloradoentities_companies
```

- [ ] **Step 3: Generate and commit migrations**

Generate migrations using `companysource generate-clickhouse-migration` against each rewritten run folder.

- [ ] **Step 4: Commit US ClickHouse support**

```bash
git add corpscout/clickhouse/sources corpscout/clickhouse/migrations companies/companysource/internal/adapters/unitedstates
git commit -m "feat: add clickhouse support for united states sources"
```

## Task 11: End-to-End Verification

**Files:**
- No source edits expected.

- [ ] **Step 1: Run all source tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/companies/common
GOWORK=off go test ./...

cd /Users/graovic/pulsarpoint/ppoint/companycollect/companies/finland
GOWORK=off go test ./...

cd /Users/graovic/pulsarpoint/ppoint/companycollect/companies/united_states
GOWORK=off go test ./...

cd /Users/graovic/pulsarpoint/ppoint/companycollect/companies/companysource
GOWORK=off go test ./...
```

- [ ] **Step 2: Run ClickHouse tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/clickhouse
GOWORK=off go test ./...
```

- [ ] **Step 3: Verify Finland flat run folder**

Run:

```bash
find /Users/graovic/pulsarpoint/ppoint/companycollect/companies/data/finland/sources/prhytj/runs/<run-id> -maxdepth 1 -type f | sort
```

Expected files are all in the same folder: source file, Parquet files, and `manifest.json`.

- [ ] **Step 4: Verify ClickHouse import**

Import Finland PRH YTJ from the flat run folder and check row counts in ClickHouse. Counts should match the previous PRH YTJ source export unless source data changed.

- [ ] **Step 5: Run Corpscout scheduler tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make test
```

If the existing BRREG SQL text assertion still fails, record it as unrelated. Do not fix unrelated scheduler SQL in this plan.

## Self-Review Notes

- The plan now requires rewriting existing modules instead of wrapping old flows.
- The plan has no active `process`, `sync`, `build-export`, reduced export, or country final export path.
- The plan uses one flat run folder per source run.
- The plan keeps Parquet as the durable source handoff and ClickHouse as the query/import store.
- The plan keeps Corpscout as the owner of committed ClickHouse migration files while `companysource` generates and imports them.
