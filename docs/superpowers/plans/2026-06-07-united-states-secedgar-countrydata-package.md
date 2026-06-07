# United States SEC EDGAR Countrydata Package Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `companies/united_states` as a standalone Go countrydata module and implement the first USA source, SEC EDGAR `company_tickers.json`, end to end.

**Architecture:** Follow the Finland countrydata module shape: country-owned Go module, one country CLI, one source package per data source, source parquet exports, final country parquet exports, and manifests. Phase 1 implements only `secedgar`; later plans add `irseobmf` and `coloradoentities` without changing this boundary.

**Tech Stack:** Go, `log/slog`, `github.com/cockroachdb/errors`, `github.com/parquet-go/parquet-go`, `companies/common/countryimport`, JSON snapshots, parquet exports.

---

## File Structure

Create the USA country module:

```text
companies/united_states/
  go.mod
  README.md
  paths.go
  paths_test.go
  status.go
  status_test.go
  types.go
  export.go
  export_test.go
  cmd/
    united-states-countrydata/
      main.go
      main_test.go
  secedgar/
    README.md
    config.go
    config_test.go
    source.go
    types.go
    decode.go
    decode_test.go
    download.go
    download_test.go
    store.go
    process.go
    process_test.go
    export_rows.go
    export_rows_test.go
    parquet_writer.go
    parquet_writer_test.go
    export.go
    export_test.go
    live_integration_test.go
    testdata/
      company_tickers_sample.json
      company_tickers_bad_shape.json
```

Do not create `companies/go.mod`. Do not import `corpscout`, `scheduler`, sqlc,
or database types.

## Task 1: Scaffold USA Module And Layout

**Files:**
- Create: `companies/united_states/go.mod`
- Create: `companies/united_states/paths.go`
- Create: `companies/united_states/paths_test.go`
- Create: `companies/united_states/README.md`

- [ ] **Step 1: Write failing layout tests**

Create `companies/united_states/paths_test.go`:

```go
package unitedstates

import (
	"path/filepath"
	"testing"
)

func TestLayoutForDataDirUsesDefaultCountryDataRoot(t *testing.T) {
	layout := LayoutForDataDir("")

	if layout.DataDir != filepath.FromSlash("../data/united_states/countrydata") {
		t.Fatalf("DataDir = %q", layout.DataDir)
	}
	if got := layout.SourceDir(SourceSECEdgar); got != filepath.FromSlash("../data/united_states/countrydata/sources/secedgar") {
		t.Fatalf("SourceDir = %q", got)
	}
	if got := layout.SourceExportsDir(SourceSECEdgar); got != filepath.FromSlash("../data/united_states/countrydata/sources/secedgar/exports") {
		t.Fatalf("SourceExportsDir = %q", got)
	}
	if got := layout.FinalExportsDir(); got != filepath.FromSlash("../data/united_states/countrydata/final/exports") {
		t.Fatalf("FinalExportsDir = %q", got)
	}
}

func TestLayoutForDataDirUsesExplicitRoot(t *testing.T) {
	layout := LayoutForDataDir("/tmp/us-countrydata")

	if got := layout.SourceDir(SourceSECEdgar); got != filepath.FromSlash("/tmp/us-countrydata/sources/secedgar") {
		t.Fatalf("SourceDir = %q", got)
	}
}
```

- [ ] **Step 2: Run layout tests to verify RED**

Run:

```sh
cd companies/united_states
GOWORK=off go test ./... -run TestLayout -count=1
```

Expected: fail because `companies/united_states` and `LayoutForDataDir` do not exist.

- [ ] **Step 3: Create `go.mod`**

Create `companies/united_states/go.mod`:

```go
module github.com/pulsarpoint/companycollect/companies/united_states

go 1.26.1

require (
	github.com/cockroachdb/errors v1.13.0
	github.com/parquet-go/parquet-go v0.30.1
	github.com/pulsarpoint/companycollect/companies/common v0.0.0
)

replace github.com/pulsarpoint/companycollect/companies/common => ../common
```

- [ ] **Step 4: Create layout implementation**

Create `companies/united_states/paths.go`:

```go
package unitedstates

import (
	"path/filepath"
)

const (
	CountryISO2 = "US"

	SourceSECEdgar          = "secedgar"
	SourceIRSEOBMF          = "irseobmf"
	SourceColoradoEntities  = "coloradoentities"
	defaultCountryDataDir   = "../data/united_states/countrydata"
)

type Layout struct {
	DataDir string
}

func LayoutForDataDir(dataDir string) Layout {
	if dataDir == "" {
		dataDir = defaultCountryDataDir
	}
	return Layout{DataDir: filepath.Clean(dataDir)}
}

func (l Layout) SourceDir(source string) string {
	return filepath.Join(l.DataDir, "sources", source)
}

func (l Layout) SourceExportsDir(source string) string {
	return filepath.Join(l.SourceDir(source), "exports")
}

func (l Layout) FinalExportsDir() string {
	return filepath.Join(l.DataDir, "final", "exports")
}
```

- [ ] **Step 5: Add README**

Create `companies/united_states/README.md`:

````markdown
# United States Country Data

Standalone Go module for United States company data collection and export.

The module builds one country-level binary:

```bash
GOWORK=off go build -o ./bin/united-states-countrydata ./cmd/united-states-countrydata
```

Phase 1 implements SEC EDGAR:

```bash
GOWORK=off go run ./cmd/united-states-countrydata sync-source --source secedgar --data-dir ../data/united_states/countrydata
GOWORK=off go run ./cmd/united-states-countrydata status-source --source secedgar --data-dir ../data/united_states/countrydata
GOWORK=off go run ./cmd/united-states-countrydata build-export --data-dir ../data/united_states/countrydata
```

When `--data-dir` is omitted, the CLI uses `../data/united_states/countrydata`.
````

- [ ] **Step 6: Run layout tests to verify GREEN**

Run:

```sh
cd companies/united_states
GOWORK=off go test ./... -run TestLayout -count=1
```

Expected: pass.

- [ ] **Step 7: Commit task 1**

```sh
git add companies/united_states/go.mod companies/united_states/README.md companies/united_states/paths.go companies/united_states/paths_test.go
git commit -m "feat: scaffold United States countrydata module"
```

## Task 2: Add Source Status Handling

**Files:**
- Create: `companies/united_states/status.go`
- Create: `companies/united_states/status_test.go`

- [ ] **Step 1: Write failing status tests**

Create `companies/united_states/status_test.go`:

```go
package unitedstates

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

func TestSourceStatusFromLatestManifestReturnsMissing(t *testing.T) {
	status, err := SourceStatusFromLatestManifest(t.TempDir(), SourceSECEdgar)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.Status != "missing" {
		t.Fatalf("Status = %q", status.Status)
	}
}

func TestSourceStatusFromLatestManifestSkipsIncompleteNewerRun(t *testing.T) {
	root := t.TempDir()
	older := filepath.Join(root, "sources", SourceSECEdgar, "exports", "20260607T010000Z-secedgar")
	newer := filepath.Join(root, "sources", SourceSECEdgar, "exports", "20260607T020000Z-secedgar")
	if err := os.MkdirAll(older, 0o755); err != nil {
		t.Fatalf("mkdir older: %v", err)
	}
	if err := os.MkdirAll(newer, 0o755); err != nil {
		t.Fatalf("mkdir newer: %v", err)
	}

	manifest := countryimport.ExportManifest{
		ManifestVersion: countryimport.ExportManifestVersion,
		CountryISO2:     CountryISO2,
		SourceSlug:      ptrString(SourceSECEdgar),
		ExportKind:      "source",
		RunID:           "20260607T010000Z-secedgar",
		SchemaVersion:   "test.schema.v1",
		CreatedAt:       time.Date(2026, 6, 7, 1, 0, 1, 0, time.UTC),
		RecordsExported: 2,
	}
	if err := countryimport.SaveExportManifest(filepath.Join(older, "manifest.json"), manifest); err != nil {
		t.Fatalf("save manifest: %v", err)
	}

	status, err := SourceStatusFromLatestManifest(root, SourceSECEdgar)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.Status != "exported" {
		t.Fatalf("Status = %q", status.Status)
	}
	if status.LastExportManifestPath != filepath.Join(older, "manifest.json") {
		t.Fatalf("LastExportManifestPath = %q", status.LastExportManifestPath)
	}
}

func ptrString(value string) *string {
	return &value
}
```

- [ ] **Step 2: Run status tests to verify RED**

Run:

```sh
cd companies/united_states
GOWORK=off go test ./... -run TestSourceStatus -count=1
```

Expected: fail because `SourceStatusFromLatestManifest` is undefined.

- [ ] **Step 3: Implement status handling**

Create `companies/united_states/status.go`:

```go
package unitedstates

import (
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/cockroachdb/errors"
	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

type SourceStatus struct {
	SourceSlug             string    `json:"source_slug"`
	Status                 string    `json:"status"`
	LastExportedAt         time.Time `json:"last_exported_at,omitempty"`
	LastExportManifestPath string    `json:"last_export_manifest_path,omitempty"`
	RecordsSeen            int64     `json:"records_seen"`
	RecordsExported        int64     `json:"records_exported"`
	DecodeErrors           int64     `json:"decode_errors"`
	Warnings               []string  `json:"warnings,omitempty"`
}

func SourceStatusFromLatestManifest(dataDir string, source string) (SourceStatus, error) {
	exportsDir := LayoutForDataDir(dataDir).SourceExportsDir(source)
	manifestPath, err := latestManifestPath(exportsDir)
	if errors.Is(err, os.ErrNotExist) {
		return SourceStatus{SourceSlug: source, Status: "missing"}, nil
	}
	if err != nil {
		return SourceStatus{}, err
	}
	manifest, err := countryimport.LoadExportManifest(manifestPath)
	if err != nil {
		return SourceStatus{}, err
	}
	return SourceStatus{
		SourceSlug:             source,
		Status:                 "exported",
		LastExportedAt:         manifest.CreatedAt,
		LastExportManifestPath: manifestPath,
		RecordsSeen:            manifest.RecordsSeen,
		RecordsExported:        manifest.RecordsExported,
		DecodeErrors:           manifest.DecodeErrors,
		Warnings:               manifest.Warnings,
	}, nil
}

func latestManifestPath(exportsDir string) (string, error) {
	entries, err := os.ReadDir(exportsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return "", os.ErrNotExist
		}
		return "", errors.Wrap(err, "read source exports directory")
	}
	runDirs := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			runDirs = append(runDirs, entry.Name())
		}
	}
	sort.Strings(runDirs)
	for i := len(runDirs) - 1; i >= 0; i-- {
		manifestPath := filepath.Join(exportsDir, runDirs[i], "manifest.json")
		info, err := os.Stat(manifestPath)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return "", errors.Wrap(err, "stat source export manifest")
		}
		if !info.IsDir() {
			return manifestPath, nil
		}
	}
	return "", os.ErrNotExist
}
```

- [ ] **Step 4: Run status tests to verify GREEN**

Run:

```sh
cd companies/united_states
GOWORK=off go test ./... -run TestSourceStatus -count=1
```

Expected: pass.

- [ ] **Step 5: Commit task 2**

```sh
git add companies/united_states/status.go companies/united_states/status_test.go
git commit -m "feat: add United States source status"
```

## Task 3: Add SEC EDGAR Config, Types, And Decoder

**Files:**
- Create: `companies/united_states/secedgar/config.go`
- Create: `companies/united_states/secedgar/config_test.go`
- Create: `companies/united_states/secedgar/source.go`
- Create: `companies/united_states/secedgar/types.go`
- Create: `companies/united_states/secedgar/decode.go`
- Create: `companies/united_states/secedgar/decode_test.go`
- Create: `companies/united_states/secedgar/testdata/company_tickers_sample.json`
- Create: `companies/united_states/secedgar/testdata/company_tickers_bad_shape.json`

- [ ] **Step 1: Create SEC test fixtures**

Create `companies/united_states/secedgar/testdata/company_tickers_sample.json`:

```json
{
  "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
  "1": {"cik_str": 789019, "ticker": "MSFT", "title": "MICROSOFT CORP"},
  "2": {"cik_str": 1652044, "ticker": "GOOG", "title": "Alphabet Inc."}
}
```

Create `companies/united_states/secedgar/testdata/company_tickers_bad_shape.json`:

```json
[
  {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}
]
```

- [ ] **Step 2: Write failing config tests**

Create `companies/united_states/secedgar/config_test.go`:

```go
package secedgar

import (
	"testing"
	"time"
)

func TestConfigFromEnvAppliesDefaults(t *testing.T) {
	t.Setenv("SEC_EDGAR_BASE_URL", "")
	t.Setenv("SEC_EDGAR_DATA_DIR", "")
	t.Setenv("SEC_EDGAR_USER_AGENT", "")

	cfg := ConfigFromEnv()

	if cfg.BaseURL != DefaultDownloadURL {
		t.Fatalf("BaseURL = %q", cfg.BaseURL)
	}
	if cfg.DataDir != defaultDataDir {
		t.Fatalf("DataDir = %q", cfg.DataDir)
	}
	if cfg.UserAgent == "" {
		t.Fatal("UserAgent is empty")
	}
	if cfg.RequestTimeout <= 0 {
		t.Fatalf("RequestTimeout = %s", cfg.RequestTimeout)
	}
}

func TestConfigFromEnvHonorsOverrides(t *testing.T) {
	t.Setenv("SEC_EDGAR_BASE_URL", "https://example.test/company_tickers.json")
	t.Setenv("SEC_EDGAR_DATA_DIR", "/tmp/secedgar")
	t.Setenv("SEC_EDGAR_USER_AGENT", "corpscout-test/1.0 test@example.com")
	t.Setenv("SEC_EDGAR_REQUEST_TIMEOUT_SECONDS", "7")

	cfg := ConfigFromEnv()

	if cfg.BaseURL != "https://example.test/company_tickers.json" {
		t.Fatalf("BaseURL = %q", cfg.BaseURL)
	}
	if cfg.DataDir != "/tmp/secedgar" {
		t.Fatalf("DataDir = %q", cfg.DataDir)
	}
	if cfg.UserAgent != "corpscout-test/1.0 test@example.com" {
		t.Fatalf("UserAgent = %q", cfg.UserAgent)
	}
	if cfg.RequestTimeout != 7*time.Second {
		t.Fatalf("RequestTimeout = %s", cfg.RequestTimeout)
	}
}
```

- [ ] **Step 3: Write failing decoder tests**

Create `companies/united_states/secedgar/decode_test.go`:

```go
package secedgar

import (
	"os"
	"testing"
)

func TestDecodeCompanyTickersObjectPreservesRecords(t *testing.T) {
	payload, err := os.ReadFile("testdata/company_tickers_sample.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}

	records, err := DecodeCompanyTickers(payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(records) != 3 {
		t.Fatalf("len(records) = %d", len(records))
	}
	if records[0].CIK != 320193 || records[0].CIK10 != "0000320193" || records[0].Ticker != "AAPL" || records[0].Title != "Apple Inc." {
		t.Fatalf("records[0] = %#v", records[0])
	}
}

func TestDecodeCompanyTickersRejectsArrayShape(t *testing.T) {
	payload, err := os.ReadFile("testdata/company_tickers_bad_shape.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}

	_, err = DecodeCompanyTickers(payload)
	if err == nil {
		t.Fatal("expected error")
	}
}
```

- [ ] **Step 4: Run config/decode tests to verify RED**

Run:

```sh
cd companies/united_states
GOWORK=off go test ./secedgar -run 'TestConfig|TestDecode' -count=1
```

Expected: fail because `ConfigFromEnv` and `DecodeCompanyTickers` are undefined.

- [ ] **Step 5: Implement SEC config, source, types, and decoder**

Create `companies/united_states/secedgar/config.go`:

```go
package secedgar

import (
	"net/http"
	"os"
	"strconv"
	"time"

	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

const (
	SourceSlug         = "united_states_sec_edgar"
	SourceName         = "SEC EDGAR company tickers"
	DefaultDownloadURL = "https://www.sec.gov/files/company_tickers.json"
	defaultDataDir     = "../data/united_states/countrydata/sources/secedgar"
	defaultUserAgent   = "corpscout-countrydata/1.0 contact@example.com"
)

type Config struct {
	BaseURL        string
	DataDir        string
	UserAgent      string
	RequestTimeout time.Duration
	HTTPClient     *http.Client
	MetadataStore  countryimport.MetadataStore
}

func ConfigFromEnv() Config {
	return Config{
		BaseURL:        envString("SEC_EDGAR_BASE_URL", DefaultDownloadURL),
		DataDir:        envString("SEC_EDGAR_DATA_DIR", defaultDataDir),
		UserAgent:      envString("SEC_EDGAR_USER_AGENT", defaultUserAgent),
		RequestTimeout: envDurationSeconds("SEC_EDGAR_REQUEST_TIMEOUT_SECONDS", countryimport.DefaultRequestTimeout),
	}
}

func envString(name string, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func envDurationSeconds(name string, fallback time.Duration) time.Duration {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	seconds, err := strconv.Atoi(value)
	if err != nil || seconds <= 0 {
		return fallback
	}
	return time.Duration(seconds) * time.Second
}
```

Create `companies/united_states/secedgar/source.go`:

```go
package secedgar

import (
	"net/http"

	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

type Source struct {
	cfg            Config
	httpClient     *http.Client
	metadataStore  countryimport.MetadataStore
	latestDownload *countryimport.DownloadMetadata
}

func NewSource(cfg Config) *Source {
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: cfg.RequestTimeout}
	}
	store := cfg.MetadataStore
	if store == nil {
		store = countryimport.NoopMetadataStore{}
	}
	return &Source{cfg: cfg, httpClient: client, metadataStore: store}
}
```

Create `companies/united_states/secedgar/types.go`:

```go
package secedgar

type SourceRecord struct {
	CIK         int64  `json:"cik_str"`
	CIK10       string `json:"cik_10"`
	Ticker      string `json:"ticker"`
	Title       string `json:"title"`
	RawPayload  []byte `json:"-"`
	PayloadHash string `json:"-"`
}
```

Create `companies/united_states/secedgar/decode.go`:

```go
package secedgar

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"

	"github.com/cockroachdb/errors"
)

type tickerPayload struct {
	CIK    int64  `json:"cik_str"`
	Ticker string `json:"ticker"`
	Title  string `json:"title"`
}

func DecodeCompanyTickers(payload []byte) ([]SourceRecord, error) {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, errors.New("SEC company tickers payload must be a JSON object keyed by index")
	}

	var keyed map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &keyed); err != nil {
		return nil, errors.Wrap(err, "decode SEC company tickers object")
	}

	keys := make([]int, 0, len(keyed))
	byIndex := make(map[int]json.RawMessage, len(keyed))
	for key, raw := range keyed {
		index, err := strconv.Atoi(key)
		if err != nil {
			return nil, errors.Wrapf(err, "parse SEC ticker index %q", key)
		}
		keys = append(keys, index)
		byIndex[index] = raw
	}
	sort.Ints(keys)

	records := make([]SourceRecord, 0, len(keys))
	for _, index := range keys {
		raw := byIndex[index]
		var decoded tickerPayload
		if err := json.Unmarshal(raw, &decoded); err != nil {
			return nil, errors.Wrapf(err, "decode SEC ticker record %d", index)
		}
		rawHash := sha256.Sum256(raw)
		records = append(records, SourceRecord{
			CIK:         decoded.CIK,
			CIK10:       fmt.Sprintf("%010d", decoded.CIK),
			Ticker:      decoded.Ticker,
			Title:       decoded.Title,
			RawPayload:  append([]byte(nil), raw...),
			PayloadHash: hex.EncodeToString(rawHash[:]),
		})
	}

	return records, nil
}
```

- [ ] **Step 6: Run config/decode tests to verify GREEN**

Run:

```sh
cd companies/united_states
GOWORK=off go test ./secedgar -run 'TestConfig|TestDecode' -count=1
```

Expected: pass.

- [ ] **Step 7: Commit task 3**

```sh
git add companies/united_states/secedgar
git commit -m "feat: add SEC EDGAR config and decoder"
```

## Task 4: Implement SEC Download, Process, And Store

**Files:**
- Create: `companies/united_states/secedgar/download.go`
- Create: `companies/united_states/secedgar/download_test.go`
- Create: `companies/united_states/secedgar/process.go`
- Create: `companies/united_states/secedgar/process_test.go`
- Create: `companies/united_states/secedgar/store.go`

- [ ] **Step 1: Write failing download test**

Create `companies/united_states/secedgar/download_test.go`:

```go
package secedgar

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

func TestDownloadWritesSnapshotAndMetadata(t *testing.T) {
	payload, err := os.ReadFile("testdata/company_tickers_sample.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var sawUserAgent bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawUserAgent = r.Header.Get("User-Agent") == "corpscout-test/1.0 test@example.com"
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(payload)
	}))
	defer server.Close()

	dataDir := t.TempDir()
	source := NewSource(Config{
		BaseURL:   server.URL,
		DataDir:   dataDir,
		UserAgent: "corpscout-test/1.0 test@example.com",
	})

	result, err := source.Download(context.Background(), countryimport.DownloadOptions{})
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	if !sawUserAgent {
		t.Fatal("server did not receive configured User-Agent")
	}
	if result.RecordsSeen != 3 {
		t.Fatalf("RecordsSeen = %d", result.RecordsSeen)
	}
	if result.BytesDownloaded <= 0 || result.SHA256 == "" {
		t.Fatalf("missing size/hash: %#v", result)
	}
	if _, err := os.Stat(result.SnapshotPath); err != nil {
		t.Fatalf("snapshot stat: %v", err)
	}
	if filepath.Dir(result.SnapshotPath) != filepath.Join(dataDir, "snapshots") {
		t.Fatalf("SnapshotPath = %q", result.SnapshotPath)
	}
}
```

- [ ] **Step 2: Write failing process/store tests**

Create `companies/united_states/secedgar/process_test.go`:

```go
package secedgar

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

func TestProcessReadsSnapshotAndCountsRecords(t *testing.T) {
	payload, err := os.ReadFile("testdata/company_tickers_sample.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	snapshotPath := filepath.Join(t.TempDir(), "company_tickers.json")
	if err := os.WriteFile(snapshotPath, payload, 0o644); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	result, err := NewSource(Config{}).Process(context.Background(), countryimport.ProcessOptions{
		SnapshotPath: snapshotPath,
	})
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if result.RecordsProcessed != 3 || result.RecordsStored != 3 {
		t.Fatalf("process result = %#v", result)
	}
}

func TestProcessMissingSnapshotReturnsNoSnapshot(t *testing.T) {
	_, err := NewSource(Config{}).Process(context.Background(), countryimport.ProcessOptions{})
	if err == nil {
		t.Fatal("expected error")
	}
	if got := countryimport.Classify(err); got != countryimport.ErrorKindNoSnapshot {
		t.Fatalf("Classify(err) = %s", got)
	}
}
```

- [ ] **Step 3: Run download/process tests to verify RED**

Run:

```sh
cd companies/united_states
GOWORK=off go test ./secedgar -run 'TestDownload|TestProcess' -count=1
```

Expected: fail because `Download`, `Process`, and `Store` are undefined.

- [ ] **Step 4: Implement download**

Create `companies/united_states/secedgar/download.go`:

```go
package secedgar

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/errors"
	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

func (s *Source) Download(ctx context.Context, opts countryimport.DownloadOptions) (countryimport.DownloadResult, error) {
	if s == nil {
		return countryimport.DownloadResult{}, countryimport.WrapSourceError(countryimport.ErrorKindState, SourceSlug, "", "", 0, errors.New("nil SEC EDGAR source"))
	}

	startedAt := time.Now().UTC()
	dataDir := firstNonEmpty(opts.DataDir, s.cfg.DataDir, defaultDataDir)
	snapshotsDir := filepath.Join(dataDir, "snapshots")
	if err := os.MkdirAll(snapshotsDir, 0o755); err != nil {
		return countryimport.DownloadResult{}, countryimport.WrapSourceError(countryimport.ErrorKindFileIO, SourceSlug, "", snapshotsDir, 0, errors.Wrap(err, "create SEC snapshots directory"))
	}

	reqCtx := ctx
	cancel := func() {}
	timeout := firstDuration(opts.RequestTimeout, s.cfg.RequestTimeout, countryimport.DefaultRequestTimeout)
	if timeout > 0 {
		reqCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()

	url := firstNonEmpty(s.cfg.BaseURL, DefaultDownloadURL)
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return countryimport.DownloadResult{}, countryimport.WrapSourceError(countryimport.ErrorKindInvalidConfig, SourceSlug, url, "", 0, errors.Wrap(err, "create SEC request"))
	}
	userAgent := firstNonEmpty(opts.UserAgent, s.cfg.UserAgent, defaultUserAgent)
	req.Header.Set("User-Agent", userAgent)

	client := s.httpClient
	if client == nil {
		client = &http.Client{Timeout: timeout}
	}
	resp, err := client.Do(req)
	if err != nil {
		return countryimport.DownloadResult{}, countryimport.WrapSourceError(countryimport.Classify(err), SourceSlug, url, "", 0, errors.Wrap(err, "download SEC company tickers"))
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode > 299 {
		return countryimport.DownloadResult{}, countryimport.WrapSourceError(countryimport.ErrorKindHTTPStatus, SourceSlug, url, "", resp.StatusCode, errors.Newf("unexpected SEC HTTP status %d", resp.StatusCode))
	}

	tempFile, err := os.CreateTemp(snapshotsDir, ".sec_company_tickers_*.json.tmp")
	if err != nil {
		return countryimport.DownloadResult{}, countryimport.WrapSourceError(countryimport.ErrorKindFileIO, SourceSlug, url, snapshotsDir, 0, errors.Wrap(err, "create SEC temp snapshot"))
	}
	tempPath := tempFile.Name()
	keepTemp := false
	defer func() {
		_ = tempFile.Close()
		if !keepTemp {
			_ = os.Remove(tempPath)
		}
	}()

	hasher := sha256.New()
	bytesDownloaded, err := io.Copy(io.MultiWriter(tempFile, hasher), resp.Body)
	if err != nil {
		return countryimport.DownloadResult{}, countryimport.WrapSourceError(countryimport.ErrorKindFileIO, SourceSlug, url, tempPath, 0, errors.Wrap(err, "write SEC snapshot"))
	}
	if err := tempFile.Close(); err != nil {
		return countryimport.DownloadResult{}, countryimport.WrapSourceError(countryimport.ErrorKindFileIO, SourceSlug, url, tempPath, 0, errors.Wrap(err, "close SEC temp snapshot"))
	}

	payload, err := os.ReadFile(tempPath)
	if err != nil {
		return countryimport.DownloadResult{}, countryimport.WrapSourceError(countryimport.ErrorKindFileIO, SourceSlug, url, tempPath, 0, errors.Wrap(err, "read SEC temp snapshot"))
	}
	records, err := DecodeCompanyTickers(payload)
	if err != nil {
		return countryimport.DownloadResult{}, countryimport.WrapSourceError(countryimport.ErrorKindRemoteDecode, SourceSlug, url, tempPath, resp.StatusCode, errors.Wrap(err, "decode SEC temp snapshot"))
	}

	finalPath := filepath.Join(snapshotsDir, "sec_company_tickers_"+startedAt.Format("20060102T150405.000000000Z")+".json")
	if err := os.Rename(tempPath, finalPath); err != nil {
		return countryimport.DownloadResult{}, countryimport.WrapSourceError(countryimport.ErrorKindFileIO, SourceSlug, url, finalPath, 0, errors.Wrap(err, "rename SEC snapshot"))
	}
	keepTemp = true

	finishedAt := time.Now().UTC()
	result := countryimport.DownloadResult{
		SourceSlug:      SourceSlug,
		SnapshotPath:    finalPath,
		BytesDownloaded: bytesDownloaded,
		RecordsSeen:     int64(len(records)),
		PagesDownloaded: 1,
		SHA256:          hex.EncodeToString(hasher.Sum(nil)),
		StartedAt:       startedAt,
		FinishedAt:      finishedAt,
		Duration:        finishedAt.Sub(startedAt),
	}
	metadata := countryimport.DownloadMetadata{
		SourceSlug:      SourceSlug,
		SourceName:      SourceName,
		BaseURL:         url,
		SnapshotPath:    finalPath,
		StartedAt:       startedAt,
		FinishedAt:      finishedAt,
		DurationMS:      result.Duration.Milliseconds(),
		BytesDownloaded: result.BytesDownloaded,
		RecordsSeen:     result.RecordsSeen,
		PagesDownloaded: 1,
		SHA256:          result.SHA256,
		License:         "U.S. Government work / public domain",
	}
	if err := s.saveDownloadMetadata(ctx, metadata); err != nil {
		return result, err
	}
	return result, nil
}

func (s *Source) saveDownloadMetadata(ctx context.Context, metadata countryimport.DownloadMetadata) error {
	s.latestDownload = &metadata
	store := s.metadataStore
	if store == nil {
		store = countryimport.NoopMetadataStore{}
	}
	if err := store.SaveDownload(ctx, metadata); err != nil {
		return countryimport.WrapSourceError(countryimport.ErrorKindState, SourceSlug, "", metadata.SnapshotPath, 0, errors.Wrap(err, "save SEC download metadata"))
	}
	return nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func firstDuration(values ...time.Duration) time.Duration {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}
```

- [ ] **Step 5: Implement process and store**

Create `companies/united_states/secedgar/store.go`:

```go
package secedgar

import (
	"context"

	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

func (s *Source) Store(ctx context.Context, records []SourceRecord) (countryimport.StoreResult, error) {
	if err := ctx.Err(); err != nil {
		return countryimport.StoreResult{}, countryimport.WrapSourceError(countryimport.Classify(err), SourceSlug, "", "", 0, err)
	}
	return countryimport.StoreResult{
		RecordsReceived: int64(len(records)),
		RecordsStored:   int64(len(records)),
	}, nil
}
```

Create `companies/united_states/secedgar/process.go`:

```go
package secedgar

import (
	"context"
	"os"

	"github.com/cockroachdb/errors"
	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

func (s *Source) Process(ctx context.Context, opts countryimport.ProcessOptions) (countryimport.ProcessResult, error) {
	snapshotPath := opts.SnapshotPath
	if snapshotPath == "" && s != nil && s.latestDownload != nil {
		snapshotPath = s.latestDownload.SnapshotPath
	}
	if snapshotPath == "" {
		return countryimport.ProcessResult{}, countryimport.WrapSourceError(countryimport.ErrorKindNoSnapshot, SourceSlug, "", "", 0, errors.New("missing SEC snapshot"))
	}

	payload, err := os.ReadFile(snapshotPath)
	if err != nil {
		return countryimport.ProcessResult{}, countryimport.WrapSourceError(countryimport.ErrorKindNotFound, SourceSlug, "", snapshotPath, 0, errors.Wrap(err, "read SEC snapshot"))
	}
	records, err := DecodeCompanyTickers(payload)
	if err != nil {
		return countryimport.ProcessResult{}, countryimport.WrapSourceError(countryimport.ErrorKindRemoteDecode, SourceSlug, "", snapshotPath, 0, errors.Wrap(err, "decode SEC snapshot"))
	}
	storeResult, err := s.Store(ctx, records)
	if err != nil {
		return countryimport.ProcessResult{}, err
	}
	return countryimport.ProcessResult{
		SourceSlug:       SourceSlug,
		SnapshotPath:     snapshotPath,
		RecordsProcessed: int64(len(records)),
		RecordsStored:    storeResult.RecordsStored,
	}, nil
}
```

- [ ] **Step 6: Run download/process tests to verify GREEN**

Run:

```sh
cd companies/united_states
GOWORK=off go test ./secedgar -run 'TestDownload|TestProcess' -count=1
```

Expected: pass.

- [ ] **Step 7: Commit task 4**

```sh
git add companies/united_states/secedgar/download.go companies/united_states/secedgar/download_test.go companies/united_states/secedgar/process.go companies/united_states/secedgar/process_test.go companies/united_states/secedgar/store.go
git commit -m "feat: add SEC EDGAR download and process"
```

## Task 5: Implement SEC Source Parquet Export

**Files:**
- Create: `companies/united_states/secedgar/export_rows.go`
- Create: `companies/united_states/secedgar/export_rows_test.go`
- Create: `companies/united_states/secedgar/parquet_writer.go`
- Create: `companies/united_states/secedgar/parquet_writer_test.go`
- Create: `companies/united_states/secedgar/export.go`
- Create: `companies/united_states/secedgar/export_test.go`

- [ ] **Step 1: Write failing export row tests**

Create `companies/united_states/secedgar/export_rows_test.go`:

```go
package secedgar

import "testing"

func TestBuildExportRowsMapsSECRecords(t *testing.T) {
	rows := BuildExportRows([]SourceRecord{{
		CIK:         320193,
		CIK10:       "0000320193",
		Ticker:      "AAPL",
		Title:       "Apple Inc.",
		PayloadHash: "hash1",
	}})

	if len(rows.Companies) != 1 || rows.Companies[0].CompanyID != "CIK:0000320193" {
		t.Fatalf("Companies = %#v", rows.Companies)
	}
	if len(rows.Identifiers) != 2 {
		t.Fatalf("Identifiers = %#v", rows.Identifiers)
	}
	if rows.Tickers[0].Ticker != "AAPL" {
		t.Fatalf("Tickers = %#v", rows.Tickers)
	}
}
```

- [ ] **Step 2: Write failing export integration test**

Create `companies/united_states/secedgar/export_test.go`:

```go
package secedgar

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

func TestExportWritesParquetFilesAndManifest(t *testing.T) {
	payload, err := os.ReadFile("testdata/company_tickers_sample.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	dataDir := t.TempDir()
	snapshotPath := filepath.Join(dataDir, "snapshots", "company_tickers.json")
	if err := os.MkdirAll(filepath.Dir(snapshotPath), 0o755); err != nil {
		t.Fatalf("mkdir snapshots: %v", err)
	}
	if err := os.WriteFile(snapshotPath, payload, 0o644); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	result, err := NewSource(Config{}).Export(context.Background(), ExportOptions{
		DataDir:      dataDir,
		SnapshotPath: snapshotPath,
		RunID:        "test-secedgar",
	})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if result.RecordsExported != 3 || result.ManifestPath == "" {
		t.Fatalf("export result = %#v", result)
	}
	for _, name := range []string{"companies.parquet", "identifiers.parquet", "tickers.parquet", "source_evidence.parquet", "manifest.json"} {
		if _, err := os.Stat(filepath.Join(dataDir, "exports", "test-secedgar", name)); err != nil {
			t.Fatalf("missing %s: %v", name, err)
		}
	}
	manifest, err := countryimport.LoadExportManifest(result.ManifestPath)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if manifest.SourceSlug == nil || *manifest.SourceSlug != "secedgar" || manifest.ExportKind != "source" {
		t.Fatalf("manifest = %#v", manifest)
	}
}
```

- [ ] **Step 3: Run export tests to verify RED**

Run:

```sh
cd companies/united_states
GOWORK=off go test ./secedgar -run 'TestBuildExportRows|TestExportWrites' -count=1
```

Expected: fail because export row and parquet code is undefined.

- [ ] **Step 4: Implement export row structs and mapping**

Create `companies/united_states/secedgar/export_rows.go`:

```go
package secedgar

import "time"

type ExportRows struct {
	Companies      []CompanyRow
	Identifiers    []IdentifierRow
	Tickers        []TickerRow
	SourceEvidence []SourceEvidenceRow
}

type CompanyRow struct {
	CompanyID  string `parquet:"company_id"`
	CIK        string `parquet:"cik"`
	LegalName  string `parquet:"legal_name"`
	SourceSlug string `parquet:"source_slug"`
}

type IdentifierRow struct {
	CompanyID      string `parquet:"company_id"`
	IdentifierType string `parquet:"identifier_type"`
	Identifier     string `parquet:"identifier"`
	SourceSlug     string `parquet:"source_slug"`
}

type TickerRow struct {
	CompanyID  string `parquet:"company_id"`
	CIK        string `parquet:"cik"`
	Ticker     string `parquet:"ticker"`
	SourceSlug string `parquet:"source_slug"`
}

type SourceEvidenceRow struct {
	CompanyID   string    `parquet:"company_id"`
	SourceSlug  string    `parquet:"source_slug"`
	PayloadHash string    `parquet:"payload_hash"`
	ExportedAt  time.Time `parquet:"exported_at"`
}

func BuildExportRows(records []SourceRecord) ExportRows {
	exportedAt := time.Now().UTC()
	rows := ExportRows{}
	for _, record := range records {
		companyID := "CIK:" + record.CIK10
		rows.Companies = append(rows.Companies, CompanyRow{
			CompanyID:  companyID,
			CIK:        record.CIK10,
			LegalName:  record.Title,
			SourceSlug: SourceSlug,
		})
		rows.Identifiers = append(rows.Identifiers,
			IdentifierRow{CompanyID: companyID, IdentifierType: "cik", Identifier: record.CIK10, SourceSlug: SourceSlug},
			IdentifierRow{CompanyID: companyID, IdentifierType: "ticker", Identifier: record.Ticker, SourceSlug: SourceSlug},
		)
		rows.Tickers = append(rows.Tickers, TickerRow{
			CompanyID:  companyID,
			CIK:        record.CIK10,
			Ticker:     record.Ticker,
			SourceSlug: SourceSlug,
		})
		rows.SourceEvidence = append(rows.SourceEvidence, SourceEvidenceRow{
			CompanyID:   companyID,
			SourceSlug:  SourceSlug,
			PayloadHash: record.PayloadHash,
			ExportedAt:  exportedAt,
		})
	}
	return rows
}
```

- [ ] **Step 5: Implement parquet writer and export**

Create `companies/united_states/secedgar/parquet_writer.go`:

```go
package secedgar

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"

	"github.com/cockroachdb/errors"
	"github.com/parquet-go/parquet-go"
)

type WrittenFile struct {
	Path   string
	SHA256 string
	Rows   int64
}

func WriteParquetFile[T any](path string, rows []T) (WrittenFile, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return WrittenFile{}, errors.Wrapf(err, "create parquet directory %s", filepath.Dir(path))
	}
	temp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		return WrittenFile{}, errors.Wrap(err, "create parquet temp file")
	}
	tempPath := temp.Name()
	keepTemp := false
	defer func() {
		_ = temp.Close()
		if !keepTemp {
			_ = os.Remove(tempPath)
		}
	}()

	writer := parquet.NewGenericWriter[T](temp)
	if _, err := writer.Write(rows); err != nil {
		return WrittenFile{}, errors.Wrap(err, "write parquet rows")
	}
	if err := writer.Close(); err != nil {
		return WrittenFile{}, errors.Wrap(err, "close parquet writer")
	}
	if err := temp.Close(); err != nil {
		return WrittenFile{}, errors.Wrap(err, "close parquet temp file")
	}
	hash, err := hashFile(tempPath)
	if err != nil {
		return WrittenFile{}, err
	}
	if err := os.Rename(tempPath, path); err != nil {
		return WrittenFile{}, errors.Wrapf(err, "rename parquet file %s", path)
	}
	keepTemp = true
	return WrittenFile{Path: path, SHA256: hash, Rows: int64(len(rows))}, nil
}

func hashFile(path string) (string, error) {
	payload, err := os.ReadFile(path)
	if err != nil {
		return "", errors.Wrapf(err, "read file for hash %s", path)
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}
```

Create `companies/united_states/secedgar/export.go`:

```go
package secedgar

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/errors"
	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

type ExportOptions struct {
	DataDir      string
	SnapshotPath string
	RunID        string
}

const SourceExportSchemaVersion = "us.sec_edgar.source.v1"

type ExportResult struct {
	RunID           string
	ManifestPath    string
	RecordsSeen     int64
	RecordsExported int64
	DecodeErrors    int64
}

func (s *Source) Export(ctx context.Context, opts ExportOptions) (ExportResult, error) {
	if err := ctx.Err(); err != nil {
		return ExportResult{}, countryimport.WrapSourceError(countryimport.Classify(err), SourceSlug, "", "", 0, errors.Wrap(err, "export SEC EDGAR"))
	}
	startedAt := time.Now().UTC()
	dataDir := firstNonEmpty(opts.DataDir, s.cfg.DataDir, defaultDataDir)
	snapshotPath := opts.SnapshotPath
	if snapshotPath == "" && s.latestDownload != nil {
		snapshotPath = s.latestDownload.SnapshotPath
	}
	if snapshotPath == "" {
		return ExportResult{}, countryimport.WrapSourceError(countryimport.ErrorKindNoSnapshot, SourceSlug, "", "", 0, errors.New("missing SEC snapshot"))
	}
	payload, err := os.ReadFile(snapshotPath)
	if err != nil {
		return ExportResult{}, countryimport.WrapSourceError(countryimport.ErrorKindNotFound, SourceSlug, "", snapshotPath, 0, errors.Wrap(err, "read SEC snapshot"))
	}
	records, err := DecodeCompanyTickers(payload)
	if err != nil {
		return ExportResult{}, countryimport.WrapSourceError(countryimport.ErrorKindRemoteDecode, SourceSlug, "", snapshotPath, 0, errors.Wrap(err, "decode SEC snapshot"))
	}
	runID := opts.RunID
	if runID == "" {
		runID = startedAt.Format("20060102T150405Z") + "-secedgar"
	}
	exportDir := filepath.Join(dataDir, "exports", runID)
	rows := BuildExportRows(records)
	files := []countryimport.ExportFile{}
	written, err := WriteParquetFile(filepath.Join(exportDir, "companies.parquet"), rows.Companies)
	if err != nil {
		return ExportResult{}, err
	}
	files = append(files, countryimport.ExportFile{Name: "companies", Path: "companies.parquet", SHA256: written.SHA256, RowCount: written.Rows})
	written, err = WriteParquetFile(filepath.Join(exportDir, "identifiers.parquet"), rows.Identifiers)
	if err != nil {
		return ExportResult{}, err
	}
	files = append(files, countryimport.ExportFile{Name: "identifiers", Path: "identifiers.parquet", SHA256: written.SHA256, RowCount: written.Rows})
	written, err = WriteParquetFile(filepath.Join(exportDir, "tickers.parquet"), rows.Tickers)
	if err != nil {
		return ExportResult{}, err
	}
	files = append(files, countryimport.ExportFile{Name: "tickers", Path: "tickers.parquet", SHA256: written.SHA256, RowCount: written.Rows})
	written, err = WriteParquetFile(filepath.Join(exportDir, "source_evidence.parquet"), rows.SourceEvidence)
	if err != nil {
		return ExportResult{}, err
	}
	files = append(files, countryimport.ExportFile{Name: "source_evidence", Path: "source_evidence.parquet", SHA256: written.SHA256, RowCount: written.Rows})

	finishedAt := time.Now().UTC()
	manifest := countryimport.ExportManifest{
		ManifestVersion: countryimport.ExportManifestVersion,
		CountryISO2:     "US",
		SourceSlug:      ptrString("secedgar"),
		ExportKind:      "source",
		RunID:           runID,
		SchemaVersion:   SourceExportSchemaVersion,
		CreatedAt:       finishedAt,
		Files:           files,
		RecordsSeen:     int64(len(records)),
		RecordsExported: int64(len(records)),
	}
	manifestPath := filepath.Join(exportDir, "manifest.json")
	if err := countryimport.SaveExportManifest(manifestPath, manifest); err != nil {
		return ExportResult{}, errors.Wrap(err, "save SEC export manifest")
	}
	return ExportResult{RunID: runID, ManifestPath: manifestPath, RecordsSeen: int64(len(records)), RecordsExported: int64(len(records))}, nil
}

func ptrString(value string) *string {
	return &value
}
```

- [ ] **Step 6: Run export tests to verify GREEN**

Run:

```sh
cd companies/united_states
GOWORK=off go test ./secedgar -run 'TestBuildExportRows|TestExportWrites' -count=1
```

Expected: pass.

- [ ] **Step 7: Commit task 5**

```sh
git add companies/united_states/secedgar/export_rows.go companies/united_states/secedgar/export_rows_test.go companies/united_states/secedgar/parquet_writer.go companies/united_states/secedgar/parquet_writer_test.go companies/united_states/secedgar/export.go companies/united_states/secedgar/export_test.go
git commit -m "feat: export SEC EDGAR source parquet"
```

## Task 6: Implement Final USA Export From SEC Source Manifest

**Files:**
- Create: `companies/united_states/types.go`
- Create: `companies/united_states/export.go`
- Create: `companies/united_states/export_test.go`

- [ ] **Step 1: Write failing final export test**

Create `companies/united_states/export_test.go`:

```go
package unitedstates

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
	"github.com/pulsarpoint/companycollect/companies/united_states/secedgar"
)

func TestBuildFinalExportFromSECManifest(t *testing.T) {
	dataDir := t.TempDir()
	snapshotPath := filepath.Join(dataDir, "sources", SourceSECEdgar, "snapshots", "company_tickers.json")
	if err := os.MkdirAll(filepath.Dir(snapshotPath), 0o755); err != nil {
		t.Fatalf("mkdir snapshot: %v", err)
	}
	payload, err := os.ReadFile("secedgar/testdata/company_tickers_sample.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	if err := os.WriteFile(snapshotPath, payload, 0o644); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	_, err = secedgar.NewSource(secedgar.Config{}).Export(context.Background(), secedgar.ExportOptions{
		DataDir:      filepath.Join(dataDir, "sources", SourceSECEdgar),
		SnapshotPath: snapshotPath,
		RunID:        "test-secedgar",
	})
	if err != nil {
		t.Fatalf("source export: %v", err)
	}

	result, err := BuildFinalExport(context.Background(), BuildExportOptions{
		DataDir: dataDir,
		RunID:   "test-final",
	})
	if err != nil {
		t.Fatalf("build final: %v", err)
	}
	if result.RecordsExported != 3 {
		t.Fatalf("RecordsExported = %d", result.RecordsExported)
	}
	manifest, err := countryimport.LoadExportManifest(result.ManifestPath)
	if err != nil {
		t.Fatalf("load final manifest: %v", err)
	}
	if manifest.ExportKind != "final" || manifest.CountryISO2 != CountryISO2 {
		t.Fatalf("manifest = %#v", manifest)
	}
	for _, name := range []string{"companies.parquet", "company_names.parquet", "identifiers.parquet", "classifications.parquet", "source_evidence.parquet"} {
		if _, err := os.Stat(filepath.Join(dataDir, "final", "exports", "test-final", name)); err != nil {
			t.Fatalf("missing %s: %v", name, err)
		}
	}
}
```

- [ ] **Step 2: Run final export test to verify RED**

Run:

```sh
cd companies/united_states
GOWORK=off go test ./... -run TestBuildFinalExportFromSECManifest -count=1
```

Expected: fail because `BuildFinalExport` is undefined.

- [ ] **Step 3: Implement final export types**

Create `companies/united_states/types.go`:

```go
package unitedstates

import "time"

type FinalCompanyRow struct {
	CompanyID       string `parquet:"company_id"`
	PrimaryID       string `parquet:"primary_id"`
	LegalName       string `parquet:"legal_name"`
	CountryISO2     string `parquet:"country_iso2"`
	IsPublicCompany bool   `parquet:"is_public_company"`
	IsNonprofit     bool   `parquet:"is_nonprofit"`
	IsTranslated    bool   `parquet:"is_translated"`
}

type FinalCompanyNameRow struct {
	CompanyID  string `parquet:"company_id"`
	Name       string `parquet:"name"`
	NameType   string `parquet:"name_type"`
	SourceSlug string `parquet:"source_slug"`
}

type FinalIdentifierRow struct {
	CompanyID      string `parquet:"company_id"`
	IdentifierType string `parquet:"identifier_type"`
	Identifier     string `parquet:"identifier"`
	SourceSlug     string `parquet:"source_slug"`
}

type FinalClassificationRow struct {
	CompanyID       string `parquet:"company_id"`
	IsPublicCompany bool   `parquet:"is_public_company"`
	IsNonprofit     bool   `parquet:"is_nonprofit"`
	SourceSlug      string `parquet:"source_slug"`
}

type FinalSourceEvidenceRow struct {
	CompanyID  string    `parquet:"company_id"`
	SourceSlug string    `parquet:"source_slug"`
	ExportedAt time.Time `parquet:"exported_at"`
}
```

- [ ] **Step 4: Implement final export builder**

Create `companies/united_states/export.go`:

```go
package unitedstates

import (
	"context"
	"path/filepath"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/parquet-go/parquet-go"
	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
	"github.com/pulsarpoint/companycollect/companies/united_states/secedgar"
)

type BuildExportOptions struct {
	DataDir string
	RunID   string
}

type BuildExportResult struct {
	RunID           string
	ManifestPath    string
	RecordsExported int64
}

func BuildFinalExport(ctx context.Context, opts BuildExportOptions) (BuildExportResult, error) {
	if err := ctx.Err(); err != nil {
		return BuildExportResult{}, errors.Wrap(err, "build United States final export")
	}
	startedAt := time.Now().UTC()
	layout := LayoutForDataDir(opts.DataDir)
	secStatus, err := SourceStatusFromLatestManifest(layout.DataDir, SourceSECEdgar)
	if err != nil {
		return BuildExportResult{}, err
	}
	if secStatus.Status != "exported" {
		return BuildExportResult{}, countryimport.WrapSourceError(countryimport.ErrorKindNoSnapshot, "united_states_final", "", layout.SourceExportsDir(SourceSECEdgar), 0, errors.New("missing SEC EDGAR source export"))
	}

	sourceManifest, err := countryimport.LoadExportManifest(secStatus.LastExportManifestPath)
	if err != nil {
		return BuildExportResult{}, errors.Wrap(err, "load SEC EDGAR source export manifest")
	}
	if sourceManifest.ExportKind != "source" || sourceManifest.SourceSlug == nil || *sourceManifest.SourceSlug != SourceSECEdgar {
		return BuildExportResult{}, errors.New("invalid SEC EDGAR source export manifest")
	}

	sourceDir := filepath.Dir(secStatus.LastExportManifestPath)
	records, err := readSECCompanies(filepath.Join(sourceDir, "companies.parquet"))
	if err != nil {
		return BuildExportResult{}, err
	}
	runID := opts.RunID
	if runID == "" {
		runID = startedAt.Format("20060102T150405Z") + "-united-states-final"
	}
	exportDir := filepath.Join(layout.FinalExportsDir(), runID)
	rows := finalRowsFromSEC(records)
	files := []countryimport.ExportFile{}
	written, err := secedgar.WriteParquetFile(filepath.Join(exportDir, "companies.parquet"), rows.Companies)
	if err != nil {
		return BuildExportResult{}, err
	}
	files = append(files, countryimport.ExportFile{Name: "companies", Path: "companies.parquet", SHA256: written.SHA256, RowCount: written.Rows})
	written, err = secedgar.WriteParquetFile(filepath.Join(exportDir, "company_names.parquet"), rows.Names)
	if err != nil {
		return BuildExportResult{}, err
	}
	files = append(files, countryimport.ExportFile{Name: "company_names", Path: "company_names.parquet", SHA256: written.SHA256, RowCount: written.Rows})
	written, err = secedgar.WriteParquetFile(filepath.Join(exportDir, "identifiers.parquet"), rows.Identifiers)
	if err != nil {
		return BuildExportResult{}, err
	}
	files = append(files, countryimport.ExportFile{Name: "identifiers", Path: "identifiers.parquet", SHA256: written.SHA256, RowCount: written.Rows})
	written, err = secedgar.WriteParquetFile(filepath.Join(exportDir, "classifications.parquet"), rows.Classifications)
	if err != nil {
		return BuildExportResult{}, err
	}
	files = append(files, countryimport.ExportFile{Name: "classifications", Path: "classifications.parquet", SHA256: written.SHA256, RowCount: written.Rows})
	written, err = secedgar.WriteParquetFile(filepath.Join(exportDir, "source_evidence.parquet"), rows.SourceEvidence)
	if err != nil {
		return BuildExportResult{}, err
	}
	files = append(files, countryimport.ExportFile{Name: "source_evidence", Path: "source_evidence.parquet", SHA256: written.SHA256, RowCount: written.Rows})

	manifest := countryimport.ExportManifest{
		ManifestVersion:  countryimport.ExportManifestVersion,
		CountryISO2:      CountryISO2,
		SourceSlug:       nil,
		ExportKind:       "final",
		RunID:            runID,
		SchemaVersion:    "us.final.v1",
		MergeRuleVersion: "us.merge.v1",
		CreatedAt:        time.Now().UTC(),
		Files:            files,
		SourceExportsUsed: []countryimport.SourceExportRef{{
			SourceSlug:   SourceSECEdgar,
			RunID:        sourceManifest.RunID,
			ManifestPath: secStatus.LastExportManifestPath,
		}},
		RecordsSeen:     int64(len(records)),
		RecordsExported: int64(len(records)),
	}
	manifestPath := filepath.Join(exportDir, "manifest.json")
	if err := countryimport.SaveExportManifest(manifestPath, manifest); err != nil {
		return BuildExportResult{}, errors.Wrap(err, "save United States final export manifest")
	}
	return BuildExportResult{RunID: runID, ManifestPath: manifestPath, RecordsExported: int64(len(records))}, nil
}

type finalRows struct {
	Companies      []FinalCompanyRow
	Names          []FinalCompanyNameRow
	Identifiers    []FinalIdentifierRow
	Classifications []FinalClassificationRow
	SourceEvidence []FinalSourceEvidenceRow
}

func finalRowsFromSEC(records []secedgar.CompanyRow) finalRows {
	exportedAt := time.Now().UTC()
	rows := finalRows{}
	for _, record := range records {
		rows.Companies = append(rows.Companies, FinalCompanyRow{
			CompanyID:       record.CompanyID,
			PrimaryID:       record.CompanyID,
			LegalName:       record.LegalName,
			CountryISO2:     CountryISO2,
			IsPublicCompany: true,
			IsNonprofit:     false,
			IsTranslated:    true,
		})
		rows.Names = append(rows.Names, FinalCompanyNameRow{CompanyID: record.CompanyID, Name: record.LegalName, NameType: "legal", SourceSlug: SourceSECEdgar})
		rows.Identifiers = append(rows.Identifiers, FinalIdentifierRow{CompanyID: record.CompanyID, IdentifierType: "cik", Identifier: record.CIK, SourceSlug: SourceSECEdgar})
		rows.Classifications = append(rows.Classifications, FinalClassificationRow{CompanyID: record.CompanyID, IsPublicCompany: true, IsNonprofit: false, SourceSlug: SourceSECEdgar})
		rows.SourceEvidence = append(rows.SourceEvidence, FinalSourceEvidenceRow{CompanyID: record.CompanyID, SourceSlug: SourceSECEdgar, ExportedAt: exportedAt})
	}
	return rows
}

func readSECCompanies(path string) ([]secedgar.CompanyRow, error) {
	rows, err := parquet.ReadFile[secedgar.CompanyRow](path)
	if err != nil {
		return nil, errors.Wrapf(err, "read SEC companies parquet %s", path)
	}
	return rows, nil
}
```

- [ ] **Step 5: Run final export test to verify GREEN**

Run:

```sh
cd companies/united_states
GOWORK=off go test ./... -run TestBuildFinalExportFromSECManifest -count=1
```

Expected: pass.

- [ ] **Step 6: Commit task 6**

```sh
git add companies/united_states/types.go companies/united_states/export.go companies/united_states/export_test.go
git commit -m "feat: build United States final export from SEC"
```

## Task 7: Implement Country CLI

**Files:**
- Create: `companies/united_states/cmd/united-states-countrydata/main.go`
- Create: `companies/united_states/cmd/united-states-countrydata/main_test.go`

- [ ] **Step 1: Write failing CLI parse tests**

Create `companies/united_states/cmd/united-states-countrydata/main_test.go`:

```go
package main

import "testing"

func TestParseArgsRequiresKnownSource(t *testing.T) {
	_, err := parseArgs([]string{"sync-source", "--source", "bad"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestParseArgsAcceptsSECEdgar(t *testing.T) {
	cfg, err := parseArgs([]string{"sync-source", "--source", "secedgar", "--data-dir", "/tmp/us"})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.command != "sync-source" || cfg.source != "secedgar" || cfg.dataDir != "/tmp/us" {
		t.Fatalf("cfg = %#v", cfg)
	}
}
```

- [ ] **Step 2: Run CLI tests to verify RED**

Run:

```sh
cd companies/united_states
GOWORK=off go test ./cmd/united-states-countrydata -run TestParseArgs -count=1
```

Expected: fail because CLI package does not exist.

- [ ] **Step 3: Implement CLI**

Create `companies/united_states/cmd/united-states-countrydata/main.go` following the Finland CLI structure with these concrete behaviors:

```go
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/cockroachdb/errors"
	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
	"github.com/pulsarpoint/companycollect/companies/united_states"
	"github.com/pulsarpoint/companycollect/companies/united_states/secedgar"
)

type cliConfig struct {
	command      string
	source       string
	envPath      string
	dataDir      string
	runID        string
	snapshotPath string
	maxPages     int
	chunkSize    int
	buildExport  bool
}

func main() {
	cfg, err := parseArgs(os.Args[1:])
	if err != nil {
		slog.Error("parse United States countrydata command", "error", err)
		os.Exit(2)
	}
	result, err := run(context.Background(), cfg)
	if err != nil {
		slog.Error("run United States countrydata command", "command", cfg.command, "source", cfg.source, "error_kind", countryimport.Classify(err), "error", err)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		slog.Error("write United States countrydata result", "error", err)
		os.Exit(1)
	}
}

func parseArgs(args []string) (cliConfig, error) {
	if len(args) == 0 {
		return cliConfig{}, fmt.Errorf("missing command")
	}
	command := args[0]
	switch command {
	case "sync-source", "status-source", "export-source", "status", "build-export", "sync":
	default:
		return cliConfig{}, fmt.Errorf("unknown command %q", command)
	}
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	cfg := cliConfig{command: command}
	flags.StringVar(&cfg.envPath, "env", "", "path to env file")
	flags.StringVar(&cfg.source, "source", "", "source slug")
	flags.StringVar(&cfg.dataDir, "data-dir", "", "United States countrydata directory")
	flags.StringVar(&cfg.runID, "run-id", "", "export run ID")
	flags.StringVar(&cfg.snapshotPath, "snapshot-path", "", "source snapshot path")
	flags.IntVar(&cfg.maxPages, "max-pages", 0, "maximum pages to download")
	flags.IntVar(&cfg.chunkSize, "chunk-size", 0, "records per chunk")
	flags.BoolVar(&cfg.buildExport, "build-export", false, "build final export after sync")
	if err := flags.Parse(args[1:]); err != nil {
		return cliConfig{}, err
	}
	if cfg.source != "" && cfg.source != unitedstates.SourceSECEdgar {
		return cliConfig{}, fmt.Errorf("unknown source %q", cfg.source)
	}
	if requiresSource(command) && cfg.source == "" {
		return cliConfig{}, fmt.Errorf("missing --source")
	}
	return cfg, nil
}

func requiresSource(command string) bool {
	switch command {
	case "sync-source", "status-source", "export-source", "sync":
		return true
	default:
		return false
	}
}

func run(ctx context.Context, cfg cliConfig) (map[string]any, error) {
	if cfg.envPath != "" {
		if err := countryimport.LoadEnvFile(cfg.envPath); err != nil {
			return nil, errors.Wrapf(err, "load env file %s", cfg.envPath)
		}
	}
	switch cfg.command {
	case "sync-source":
		return runSyncSource(ctx, cfg)
	case "export-source":
		return runExportSource(ctx, cfg)
	case "status-source":
		return runStatusSource(cfg)
	case "status":
		return runStatus(cfg)
	case "build-export":
		return runBuildExport(ctx, cfg)
	case "sync":
		result, err := runSyncSource(ctx, cfg)
		if err != nil {
			return nil, err
		}
		if cfg.buildExport {
			buildResult, err := buildFinalExport(ctx, cfg)
			if err != nil {
				return nil, errors.Wrap(err, "build United States final export")
			}
			result["final_manifest_path"] = buildResult.ManifestPath
			result["final_records_exported"] = buildResult.RecordsExported
			result["final_run_id"] = buildResult.RunID
		}
		return result, nil
	default:
		return nil, fmt.Errorf("unknown command %q", cfg.command)
	}
}
```

Add helper functions equivalent to Finland:

```go
func runSyncSource(ctx context.Context, cfg cliConfig) (map[string]any, error) {
	source := newSECSource()
	sourceDataDir := sourceDataDir(cfg)
	downloadResult, err := source.Download(ctx, countryimport.DownloadOptions{DataDir: sourceDataDir, MaxPages: cfg.maxPages})
	if err != nil {
		return nil, errors.Wrap(err, "download SEC EDGAR snapshot")
	}
	exportResult, err := source.Export(ctx, secedgar.ExportOptions{DataDir: sourceDataDir, SnapshotPath: downloadResult.SnapshotPath, RunID: cfg.runID})
	if err != nil {
		return nil, errors.Wrap(err, "export SEC EDGAR source")
	}
	return sourceResultMap(cfg.command, exportResult, map[string]any{
		"snapshot_path":      downloadResult.SnapshotPath,
		"records_downloaded": downloadResult.RecordsSeen,
	}), nil
}

func runExportSource(ctx context.Context, cfg cliConfig) (map[string]any, error) {
	result, err := newSECSource().Export(ctx, secedgar.ExportOptions{DataDir: sourceDataDir(cfg), SnapshotPath: cfg.snapshotPath, RunID: cfg.runID})
	if err != nil {
		return nil, errors.Wrap(err, "export SEC EDGAR source")
	}
	return sourceResultMap(cfg.command, result, map[string]any{"snapshot_path": cfg.snapshotPath}), nil
}

func runStatusSource(cfg cliConfig) (map[string]any, error) {
	status, err := unitedstates.SourceStatusFromLatestManifest(cfg.dataDir, cfg.source)
	if err != nil {
		return nil, errors.Wrap(err, "load SEC EDGAR source status")
	}
	return map[string]any{"command": cfg.command, "country_iso2": unitedstates.CountryISO2, "source": cfg.source, "status": status.Status, "source_manifest_path": status.LastExportManifestPath, "source_status": status}, nil
}

func runStatus(cfg cliConfig) (map[string]any, error) {
	secStatus, err := unitedstates.SourceStatusFromLatestManifest(cfg.dataDir, unitedstates.SourceSECEdgar)
	if err != nil {
		return nil, errors.Wrap(err, "load SEC EDGAR source status")
	}
	return map[string]any{"command": cfg.command, "country_iso2": unitedstates.CountryISO2, "status": "ok", "secedgar_source_manifest_path": secStatus.LastExportManifestPath, "sources": map[string]unitedstates.SourceStatus{unitedstates.SourceSECEdgar: secStatus}}, nil
}

func runBuildExport(ctx context.Context, cfg cliConfig) (map[string]any, error) {
	result, err := buildFinalExport(ctx, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "build United States final export")
	}
	return map[string]any{"command": cfg.command, "country_iso2": unitedstates.CountryISO2, "status": "ok", "run_id": result.RunID, "manifest_path": result.ManifestPath, "records_exported": result.RecordsExported, "final_manifest_path": result.ManifestPath}, nil
}

func newSECSource() *secedgar.Source {
	return secedgar.NewSource(secedgar.ConfigFromEnv())
}

func sourceDataDir(cfg cliConfig) string {
	return unitedstates.LayoutForDataDir(cfg.dataDir).SourceDir(unitedstates.SourceSECEdgar)
}

func buildFinalExport(ctx context.Context, cfg cliConfig) (unitedstates.BuildExportResult, error) {
	return unitedstates.BuildFinalExport(ctx, unitedstates.BuildExportOptions{DataDir: cfg.dataDir, RunID: cfg.runID})
}

func sourceResultMap(command string, result secedgar.ExportResult, extra map[string]any) map[string]any {
	response := map[string]any{"command": command, "country_iso2": unitedstates.CountryISO2, "source": unitedstates.SourceSECEdgar, "status": "ok", "run_id": result.RunID, "manifest_path": result.ManifestPath, "source_manifest_path": result.ManifestPath, "records_seen": result.RecordsSeen, "records_exported": result.RecordsExported, "decode_errors": result.DecodeErrors}
	for key, value := range extra {
		response[key] = value
	}
	return response
}
```

- [ ] **Step 4: Run CLI tests to verify GREEN**

Run:

```sh
cd companies/united_states
GOWORK=off go test ./cmd/united-states-countrydata -run TestParseArgs -count=1
```

Expected: pass.

- [ ] **Step 5: Commit task 7**

```sh
git add companies/united_states/cmd/united-states-countrydata
git commit -m "feat: add United States countrydata CLI"
```

## Task 8: Add SEC README, Live Test, And Full Verification

**Files:**
- Create: `companies/united_states/secedgar/README.md`
- Create: `companies/united_states/secedgar/live_integration_test.go`
- Modify: `companies/united_states/README.md`

- [ ] **Step 1: Add SEC source README**

Create `companies/united_states/secedgar/README.md`:

````markdown
# SEC EDGAR Countrydata Source

Source: `https://www.sec.gov/files/company_tickers.json`

This source downloads SEC EDGAR `company_tickers.json`, preserves it as a JSON
snapshot, exports source parquet tables, and contributes public-company CIK
records to the United States final export.

SEC requires a descriptive User-Agent. Configure it with:

```bash
export SEC_EDGAR_USER_AGENT="corpscout/1.0 contact@example.com"
```

Default fixture tests:

```bash
GOWORK=off go test ./secedgar -count=1
```

Gated live test:

```bash
COUNTRYDATA_SEC_EDGAR_LIVE=1 GOWORK=off go test -tags=integration ./secedgar -run TestLive -count=1 -v
```
````

- [ ] **Step 2: Add gated live test**

Create `companies/united_states/secedgar/live_integration_test.go`:

```go
//go:build integration

package secedgar

import (
	"context"
	"os"
	"testing"

	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

func TestLiveDownloadAndExport(t *testing.T) {
	if os.Getenv("COUNTRYDATA_SEC_EDGAR_LIVE") != "1" {
		t.Skip("set COUNTRYDATA_SEC_EDGAR_LIVE=1 to run live SEC EDGAR test")
	}
	t.Setenv("SEC_EDGAR_USER_AGENT", "corpscout-live-test/1.0 contact@example.com")

	dataDir := t.TempDir()
	source := NewSource(ConfigFromEnv())
	downloadResult, err := source.Download(context.Background(), countryimport.DownloadOptions{DataDir: dataDir})
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	if downloadResult.RecordsSeen == 0 {
		t.Fatal("RecordsSeen is zero")
	}
	exportResult, err := source.Export(context.Background(), ExportOptions{DataDir: dataDir, SnapshotPath: downloadResult.SnapshotPath})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if exportResult.RecordsExported != downloadResult.RecordsSeen {
		t.Fatalf("RecordsExported = %d, RecordsSeen = %d", exportResult.RecordsExported, downloadResult.RecordsSeen)
	}
}
```

- [ ] **Step 3: Update USA README with SEC command examples**

Modify `companies/united_states/README.md` to include:

````markdown
SEC EDGAR source sync:

```bash
export SEC_EDGAR_USER_AGENT="corpscout/1.0 contact@example.com"
GOWORK=off go run ./cmd/united-states-countrydata sync-source --source secedgar --data-dir ../data/united_states/countrydata
GOWORK=off go run ./cmd/united-states-countrydata status-source --source secedgar --data-dir ../data/united_states/countrydata
GOWORK=off go run ./cmd/united-states-countrydata build-export --data-dir ../data/united_states/countrydata
```
````

- [ ] **Step 4: Run full package tests**

Run:

```sh
cd companies/common
GOWORK=off go test ./... -count=1

cd ../united_states
GOWORK=off go test ./... -count=1
```

Expected: both commands pass.

- [ ] **Step 5: Build CLI**

Run:

```sh
cd companies/united_states
GOWORK=off go build -o ./bin/united-states-countrydata ./cmd/united-states-countrydata
rm -f ./bin/united-states-countrydata
rmdir ./bin 2>/dev/null || true
```

Expected: build succeeds and `bin/` is removed.

- [ ] **Step 6: Run local status command**

Run:

```sh
cd companies/united_states
GOWORK=off go run ./cmd/united-states-countrydata status-source --source secedgar
```

Expected: JSON output with `"source":"secedgar"` and either `"status":"missing"` or `"status":"ok"`.

- [ ] **Step 7: Confirm generated data is ignored**

Run:

```sh
git status --short
git diff --check
```

Expected: no generated `companies/data/united_states/countrydata` files are staged; `git diff --check` exits 0.

- [ ] **Step 8: Commit task 8**

```sh
git add companies/united_states/README.md companies/united_states/secedgar/README.md companies/united_states/secedgar/live_integration_test.go
git commit -m "docs: document SEC EDGAR countrydata source"
```

## Final Acceptance

- [ ] `companies/united_states` exists as its own Go module.
- [ ] `secedgar` supports config, download, process, store, source export, and live integration testing.
- [ ] `united-states-countrydata` supports `sync-source`, `export-source`, `status-source`, `status`, `build-export`, and `sync`.
- [ ] Source export writes `companies.parquet`, `identifiers.parquet`, `tickers.parquet`, `source_evidence.parquet`, and `manifest.json`.
- [ ] Final export can build from the latest valid SEC source manifest.
- [ ] `GOWORK=off go test ./... -count=1` passes in `companies/united_states`.
- [ ] `GOWORK=off go build -o ./bin/united-states-countrydata ./cmd/united-states-countrydata` passes.
- [ ] Live test is available but skipped unless `COUNTRYDATA_SEC_EDGAR_LIVE=1`.
