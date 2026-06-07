# Finland Countrydata CLI Parquet Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reorganize Finland PRH YTJ into a CLI-first countrydata package that can sync a source, report source/country status, export source-normalized Parquet, and build a final Finland Parquet export.

**Architecture:** Keep source-specific parsing and normalization inside `countrydata/finland/prhytj`. Add a country-level `countrydata/finland` package that merges source exports into final core exports. The central Corpscout database remains outside this plan; the contract produced here is Parquet plus `manifest.json`.

**Tech Stack:** Go 1.26.1, `log/slog`, `github.com/cockroachdb/errors`, `github.com/parquet-go/parquet-go@v0.30.1`, existing `countrydata/import` error and metadata patterns.

---

## Scope

This plan implements the Finland countrydata module only. It does not implement the central Corpscout Parquet importer, Temporal workflows, Docker images, or DuckDB processing. DuckDB remains optional internally; this first implementation uses pure Go and mandatory Parquet output.

The old `countrydata/cmd/prhytj-import` command and scheduler-owned Postgres sync remain in place during this migration.

## File Structure

Create or modify these files:

- Modify: `corpscout/countrydata/go.mod`
  - Add `github.com/parquet-go/parquet-go@v0.30.1`.
- Create: `corpscout/countrydata/import/export_manifest.go`
  - Shared manifest structs, file hash helpers, atomic JSON write/read.
- Create: `corpscout/countrydata/import/export_manifest_test.go`
  - Manifest round-trip and file hashing tests.
- Create: `corpscout/countrydata/finland/paths.go`
  - Resolves `/data/sources/prhytj` and `/data/final` layout.
- Create: `corpscout/countrydata/finland/status.go`
  - Country and source status structs and manifest-based status readers.
- Create: `corpscout/countrydata/finland/status_test.go`
  - Status behavior for missing and existing manifests.
- Create: `corpscout/countrydata/finland/types.go`
  - Final export row types and constants.
- Create: `corpscout/countrydata/finland/export.go`
  - Finland final export builder for PRH-only v1.
- Create: `corpscout/countrydata/finland/export_test.go`
  - Final export files, empty core files, and manifest tests.
- Create: `corpscout/countrydata/finland/prhytj/export_rows.go`
  - PRH source-normalized Parquet row types and projection logic.
- Create: `corpscout/countrydata/finland/prhytj/export_rows_test.go`
  - Real fixture projection tests.
- Create: `corpscout/countrydata/finland/prhytj/parquet_writer.go`
  - Typed Parquet writer helper.
- Create: `corpscout/countrydata/finland/prhytj/parquet_writer_test.go`
  - Parquet write/read tests.
- Create: `corpscout/countrydata/finland/prhytj/export.go`
  - `Source.Export` that reads snapshot NDJSON and writes source export files.
- Create: `corpscout/countrydata/finland/prhytj/export_test.go`
  - Export manifest, row counts, decode error, and hash tests.
- Create: `corpscout/countrydata/cmd/finland-countrydata/main.go`
  - New Finland country CLI.
- Create: `corpscout/countrydata/cmd/finland-countrydata/main_test.go`
  - Argument parsing and JSON result tests.
- Modify: `corpscout/countrydata/finland/prhytj/README.md`
  - Document new CLI and Parquet contract.

## Task 1: Add Shared Export Manifest Helpers

**Files:**
- Create: `corpscout/countrydata/import/export_manifest.go`
- Create: `corpscout/countrydata/import/export_manifest_test.go`

- [ ] **Step 1: Write failing manifest tests**

Create `corpscout/countrydata/import/export_manifest_test.go`:

```go
package countryimport

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestExportManifestRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	createdAt := time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC)
	manifest := ExportManifest{
		ManifestVersion: "countrydata.export.v1",
		CountryISO2:     "FI",
		SourceSlug:      ptrString("prhytj"),
		ExportKind:      "source",
		RunID:           "20260607T120000Z-prhytj",
		SchemaVersion:   "finland.prhytj.source.v1",
		CreatedAt:       createdAt,
		Files: []ExportFile{{
			Name:       "companies",
			Path:       "companies.parquet",
			RowCount:   1,
			SHA256:     "abcdef",
			SchemaHash: "schemahash",
		}},
		RecordsSeen:     1,
		RecordsExported: 1,
	}

	if err := SaveExportManifest(path, manifest); err != nil {
		t.Fatalf("save manifest: %v", err)
	}
	loaded, err := LoadExportManifest(path)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if loaded.ManifestVersion != manifest.ManifestVersion || loaded.RunID != manifest.RunID {
		t.Fatalf("loaded manifest mismatch: %#v", loaded)
	}
	if loaded.SourceSlug == nil || *loaded.SourceSlug != "prhytj" {
		t.Fatalf("SourceSlug = %#v, want prhytj", loaded.SourceSlug)
	}
}

func TestHashFileSHA256(t *testing.T) {
	path := filepath.Join(t.TempDir(), "file.txt")
	if err := os.WriteFile(path, []byte("hello"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	hash, size, err := HashFileSHA256(path)
	if err != nil {
		t.Fatalf("hash file: %v", err)
	}
	if size != 5 {
		t.Fatalf("size = %d, want 5", size)
	}
	if hash != "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824" {
		t.Fatalf("hash = %q", hash)
	}
}

func ptrString(value string) *string {
	return &value
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/countrydata
GOWORK=off go test ./import -run 'TestExportManifest|TestHashFile' -count=1 -v
```

Expected: FAIL because `ExportManifest`, `SaveExportManifest`, `LoadExportManifest`, and `HashFileSHA256` do not exist.

- [ ] **Step 3: Implement manifest helpers**

Create `corpscout/countrydata/import/export_manifest.go`:

```go
package countryimport

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/errors"
)

const ExportManifestVersion = "countrydata.export.v1"

type ExportManifest struct {
	ManifestVersion      string               `json:"manifest_version"`
	CountryISO2          string               `json:"country_iso2"`
	SourceSlug           *string              `json:"source_slug"`
	ExportKind           string               `json:"export_kind"`
	RunID                string               `json:"run_id"`
	SchemaVersion        string               `json:"schema_version"`
	MergeRuleVersion     string               `json:"merge_rule_version,omitempty"`
	CreatedAt            time.Time            `json:"created_at"`
	Inputs               []ExportInput        `json:"inputs,omitempty"`
	Files                []ExportFile         `json:"files"`
	SourceExportsUsed    []SourceExportRef    `json:"source_exports_used,omitempty"`
	RecordsSeen          int64                `json:"records_seen"`
	RecordsExported      int64                `json:"records_exported"`
	DecodeErrors         int64                `json:"decode_errors"`
	Warnings             []string             `json:"warnings,omitempty"`
	AdditionalProperties map[string]string    `json:"additional_properties,omitempty"`
}

type ExportInput struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

type ExportFile struct {
	Name       string `json:"name"`
	Path       string `json:"path"`
	RowCount   int64  `json:"row_count"`
	SHA256     string `json:"sha256"`
	SchemaHash string `json:"schema_hash"`
	Optional   bool   `json:"optional,omitempty"`
}

type SourceExportRef struct {
	SourceSlug   string `json:"source_slug"`
	RunID        string `json:"run_id"`
	ManifestPath string `json:"manifest_path"`
}

func SaveExportManifest(path string, manifest ExportManifest) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return errors.Wrap(err, "create manifest directory")
	}
	payload, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return errors.Wrap(err, "marshal export manifest")
	}
	payload = append(payload, '\n')
	tempPath := path + ".tmp"
	if err := os.WriteFile(tempPath, payload, 0o644); err != nil {
		return errors.Wrap(err, "write temporary export manifest")
	}
	if err := os.Rename(tempPath, path); err != nil {
		_ = os.Remove(tempPath)
		return errors.Wrap(err, "rename export manifest")
	}
	return nil
}

func LoadExportManifest(path string) (ExportManifest, error) {
	payload, err := os.ReadFile(path)
	if err != nil {
		return ExportManifest{}, errors.Wrap(err, "read export manifest")
	}
	var manifest ExportManifest
	if err := json.Unmarshal(payload, &manifest); err != nil {
		return ExportManifest{}, errors.Wrap(err, "decode export manifest")
	}
	return manifest, nil
}

func HashFileSHA256(path string) (string, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", 0, errors.Wrap(err, "open file for sha256")
	}
	defer file.Close()

	hasher := sha256.New()
	size, err := io.Copy(hasher, file)
	if err != nil {
		return "", 0, errors.Wrap(err, "hash file")
	}
	return hex.EncodeToString(hasher.Sum(nil)), size, nil
}
```

- [ ] **Step 4: Run tests to verify pass**

Run:

```bash
GOWORK=off go test ./import -run 'TestExportManifest|TestHashFile' -count=1 -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add corpscout/countrydata/import/export_manifest.go corpscout/countrydata/import/export_manifest_test.go
git commit -m "feat: add countrydata export manifest helpers"
```

## Task 2: Add Finland Layout and Status Primitives

**Files:**
- Create: `corpscout/countrydata/finland/paths.go`
- Create: `corpscout/countrydata/finland/status.go`
- Create: `corpscout/countrydata/finland/status_test.go`

- [ ] **Step 1: Write failing layout/status tests**

Create `corpscout/countrydata/finland/status_test.go`:

```go
package finland

import (
	"path/filepath"
	"testing"
	"time"

	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

func TestLayoutForDataDir(t *testing.T) {
	layout := LayoutForDataDir("/data")
	if layout.SourceDir(SourcePRHYTJ) != filepath.Join("/data", "sources", SourcePRHYTJ) {
		t.Fatalf("source dir = %q", layout.SourceDir(SourcePRHYTJ))
	}
	if layout.FinalDir != filepath.Join("/data", "final") {
		t.Fatalf("final dir = %q", layout.FinalDir)
	}
}

func TestSourceStatusFromMissingManifest(t *testing.T) {
	status, err := SourceStatusFromLatestManifest(t.TempDir(), SourcePRHYTJ)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.Status != "missing" {
		t.Fatalf("status = %q, want missing", status.Status)
	}
}

func TestSourceStatusFromLatestManifest(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "sources", SourcePRHYTJ, "exports", "run-1", "manifest.json")
	sourceSlug := SourcePRHYTJ
	if err := countryimport.SaveExportManifest(manifestPath, countryimport.ExportManifest{
		ManifestVersion: countryimport.ExportManifestVersion,
		CountryISO2:     CountryISO2,
		SourceSlug:      &sourceSlug,
		ExportKind:      "source",
		RunID:           "run-1",
		SchemaVersion:   "finland.prhytj.source.v1",
		CreatedAt:       time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC),
		Files:           []countryimport.ExportFile{{Name: "companies", Path: "companies.parquet", RowCount: 2}},
		RecordsSeen:     2,
		RecordsExported: 2,
	}); err != nil {
		t.Fatalf("save manifest: %v", err)
	}

	status, err := SourceStatusFromLatestManifest(dir, SourcePRHYTJ)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.Status != "exported" || status.LastExportManifestPath != manifestPath {
		t.Fatalf("status = %#v", status)
	}
	if status.RecordsExported != 2 {
		t.Fatalf("records exported = %d, want 2", status.RecordsExported)
	}
}

func writeTestFile(t *testing.T, path string, payload []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create test file directory: %v", err)
	}
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
GOWORK=off go test ./finland -run 'TestLayout|TestSourceStatus' -count=1 -v
```

Expected: FAIL because `LayoutForDataDir`, `SourcePRHYTJ`, and `SourceStatusFromLatestManifest` do not exist.

- [ ] **Step 3: Implement layout and status**

Create `corpscout/countrydata/finland/paths.go`:

```go
package finland

import (
	"path/filepath"
	"strings"
)

const (
	CountryISO2 = "FI"
	SourcePRHYTJ = "prhytj"
	defaultDataDir = "./data/countrydata/finland"
)

type Layout struct {
	DataDir string
	SourcesDir string
	FinalDir string
}

func LayoutForDataDir(dataDir string) Layout {
	root := strings.TrimSpace(dataDir)
	if root == "" {
		root = defaultDataDir
	}
	return Layout{
		DataDir: root,
		SourcesDir: filepath.Join(root, "sources"),
		FinalDir: filepath.Join(root, "final"),
	}
}

func (l Layout) SourceDir(sourceSlug string) string {
	return filepath.Join(l.SourcesDir, sourceSlug)
}

func (l Layout) SourceExportsDir(sourceSlug string) string {
	return filepath.Join(l.SourceDir(sourceSlug), "exports")
}

func (l Layout) FinalExportsDir() string {
	return filepath.Join(l.FinalDir, "exports")
}
```

Create `corpscout/countrydata/finland/status.go`:

```go
package finland

import (
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/cockroachdb/errors"
	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

type SourceStatus struct {
	SourceSlug string `json:"source_slug"`
	Status string `json:"status"`
	LastExportedAt time.Time `json:"last_exported_at,omitempty"`
	LastExportManifestPath string `json:"last_export_manifest_path,omitempty"`
	RecordsSeen int64 `json:"records_seen"`
	RecordsExported int64 `json:"records_exported"`
	DecodeErrors int64 `json:"decode_errors"`
	Warnings []string `json:"warnings,omitempty"`
}

func SourceStatusFromLatestManifest(dataDir string, sourceSlug string) (SourceStatus, error) {
	layout := LayoutForDataDir(dataDir)
	exportsDir := layout.SourceExportsDir(sourceSlug)
	manifestPath, err := latestManifestPath(exportsDir)
	if errors.Is(err, os.ErrNotExist) {
		return SourceStatus{SourceSlug: sourceSlug, Status: "missing"}, nil
	}
	if err != nil {
		return SourceStatus{}, err
	}
	manifest, err := countryimport.LoadExportManifest(manifestPath)
	if err != nil {
		return SourceStatus{}, err
	}
	return SourceStatus{
		SourceSlug: sourceSlug,
		Status: "exported",
		LastExportedAt: manifest.CreatedAt,
		LastExportManifestPath: manifestPath,
		RecordsSeen: manifest.RecordsSeen,
		RecordsExported: manifest.RecordsExported,
		DecodeErrors: manifest.DecodeErrors,
		Warnings: manifest.Warnings,
	}, nil
}

func latestManifestPath(exportsDir string) (string, error) {
	entries, err := os.ReadDir(exportsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return "", os.ErrNotExist
		}
		return "", errors.Wrap(err, "read exports directory")
	}
	runDirs := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			runDirs = append(runDirs, entry.Name())
		}
	}
	if len(runDirs) == 0 {
		return "", os.ErrNotExist
	}
	sort.Strings(runDirs)
	return filepath.Join(exportsDir, runDirs[len(runDirs)-1], "manifest.json"), nil
}
```

- [ ] **Step 4: Run tests to verify pass**

Run:

```bash
GOWORK=off go test ./finland -run 'TestLayout|TestSourceStatus' -count=1 -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add corpscout/countrydata/finland/paths.go corpscout/countrydata/finland/status.go corpscout/countrydata/finland/status_test.go
git commit -m "feat: add Finland countrydata layout status"
```

## Task 3: Add PRH Source Export Row Projection

**Files:**
- Create: `corpscout/countrydata/finland/prhytj/export_rows.go`
- Create: `corpscout/countrydata/finland/prhytj/export_rows_test.go`

- [ ] **Step 1: Write failing projection tests**

Create `corpscout/countrydata/finland/prhytj/export_rows_test.go`:

```go
package prhytj

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"
)

func TestProjectExportRowsFromRealSample(t *testing.T) {
	record := loadAnalysisSampleRecord(t)
	rows := ProjectExportRows(record, "run-1")

	if len(rows.Companies) != 1 {
		t.Fatalf("companies len = %d, want 1", len(rows.Companies))
	}
	company := rows.Companies[0]
	if company.CountryISO2 != "FI" || company.SourceSlug != SourceSlug {
		t.Fatalf("lineage = %#v", company)
	}
	if company.BusinessID != "0100130-4" || company.LegalName != "Dynava Oy" {
		t.Fatalf("company = %#v", company)
	}
	if company.VATID != "FI01001304" {
		t.Fatalf("VATID = %q", company.VATID)
	}
	if company.PrimaryIndustryCode != "82200" || company.PrimaryIndustryLabelEn != "Activities of call centres" {
		t.Fatalf("industry = %#v", company)
	}
	if company.WebsiteNormalizedURL != "https://www.dynava.fi" {
		t.Fatalf("website = %q", company.WebsiteNormalizedURL)
	}
	if len(rows.CompanyNames) != len(record.Names) {
		t.Fatalf("company names = %d, want %d", len(rows.CompanyNames), len(record.Names))
	}
	if len(rows.Addresses) != len(record.Addresses) {
		t.Fatalf("addresses = %d, want %d", len(rows.Addresses), len(record.Addresses))
	}
	if len(rows.TaxRegistrations) != 3 {
		t.Fatalf("tax registrations = %d, want 3", len(rows.TaxRegistrations))
	}
}

func loadAnalysisSampleRecord(t *testing.T) CompanyRecord {
	t.Helper()
	payload, err := os.ReadFile("../../../../companies/analysis/finland/data_model/sources/prh_ytj_v3/sample_record.json")
	if err != nil {
		t.Fatalf("read analysis sample: %v", err)
	}
	var record CompanyRecord
	if err := json.Unmarshal(payload, &record); err != nil {
		t.Fatalf("decode sample: %v", err)
	}
	record.RawPayload = payload
	sum := sha256.Sum256(payload)
	record.PayloadHash = hex.EncodeToString(sum[:])
	return record
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
GOWORK=off go test ./finland/prhytj -run TestProjectExportRowsFromRealSample -count=1 -v
```

Expected: FAIL because `ProjectExportRows` and row types do not exist.

- [ ] **Step 3: Implement source export rows**

Create `corpscout/countrydata/finland/prhytj/export_rows.go` with:

```go
package prhytj

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"strings"
	"time"
)

const SourceExportSchemaVersion = "finland.prhytj.source.v1"

type ExportRows struct {
	Companies []CompanyExportRow
	CompanyNames []CompanyNameExportRow
	LegalForms []LegalFormExportRow
	Industries []IndustryExportRow
	Addresses []AddressExportRow
	RegisteredEntries []RegisteredEntryExportRow
	TaxRegistrations []TaxRegistrationExportRow
	Websites []WebsiteExportRow
}

type CompanyExportRow struct {
	CountryISO2 string `parquet:"country_iso2"`
	SourceSlug string `parquet:"source_slug"`
	SourceRunID string `parquet:"source_run_id"`
	SourceRecordID string `parquet:"source_record_id"`
	SourceNativeID string `parquet:"source_native_id"`
	SourcePayloadHash string `parquet:"source_payload_hash"`
	SourceUpdatedAt string `parquet:"source_updated_at"`
	ExportedAt string `parquet:"exported_at"`
	SchemaVersion string `parquet:"schema_version"`
	BusinessID string `parquet:"business_id"`
	VATID string `parquet:"vat_id"`
	EUID string `parquet:"euid"`
	LegalName string `parquet:"legal_name"`
	LegalNameNormalized string `parquet:"legal_name_normalized"`
	LifecycleStatus string `parquet:"lifecycle_status"`
	IsActive bool `parquet:"is_active"`
	LegalFormCode string `parquet:"legal_form_code"`
	LegalFormLabel string `parquet:"legal_form_label"`
	LegalFormLabelEn string `parquet:"legal_form_label_en"`
	PrimaryIndustryCode string `parquet:"primary_industry_code"`
	PrimaryIndustryCodeSet string `parquet:"primary_industry_code_set"`
	PrimaryIndustryLabel string `parquet:"primary_industry_label"`
	PrimaryIndustryLabelEn string `parquet:"primary_industry_label_en"`
	PrimaryNACECode string `parquet:"primary_nace_code"`
	PrimaryNACERevision string `parquet:"primary_nace_revision"`
	WebsiteURL string `parquet:"website_url"`
	WebsiteNormalizedURL string `parquet:"website_normalized_url"`
	WebsiteHost string `parquet:"website_host"`
}

type CompanyNameExportRow struct {
	CountryISO2 string `parquet:"country_iso2"`
	SourceSlug string `parquet:"source_slug"`
	SourceRunID string `parquet:"source_run_id"`
	SourceRecordID string `parquet:"source_record_id"`
	SourceItemHash string `parquet:"source_item_hash"`
	BusinessID string `parquet:"business_id"`
	SourcePosition int32 `parquet:"source_position"`
	Name string `parquet:"name"`
	NameTypeCode string `parquet:"name_type_code"`
	RegisteredOn string `parquet:"registered_on"`
	EndedOn string `parquet:"ended_on"`
	IsCurrent bool `parquet:"is_current"`
	IsPrimary bool `parquet:"is_primary"`
}
```

Add the remaining row types in the same file with explicit Parquet column tags:

```go
type LegalFormExportRow struct {
	CountryISO2 string `parquet:"country_iso2"`
	SourceSlug string `parquet:"source_slug"`
	SourceRunID string `parquet:"source_run_id"`
	SourceRecordID string `parquet:"source_record_id"`
	SourceItemHash string `parquet:"source_item_hash"`
	BusinessID string `parquet:"business_id"`
	LegalFormCode string `parquet:"legal_form_code"`
	LegalFormLabel string `parquet:"legal_form_label"`
	LegalFormLabelEn string `parquet:"legal_form_label_en"`
	LegalFormLabelFi string `parquet:"legal_form_label_fi"`
	LegalFormLabelSv string `parquet:"legal_form_label_sv"`
	RegisteredOn string `parquet:"registered_on"`
	EndedOn string `parquet:"ended_on"`
}

type IndustryExportRow struct {
	CountryISO2 string `parquet:"country_iso2"`
	SourceSlug string `parquet:"source_slug"`
	SourceRunID string `parquet:"source_run_id"`
	SourceRecordID string `parquet:"source_record_id"`
	SourceItemHash string `parquet:"source_item_hash"`
	BusinessID string `parquet:"business_id"`
	SourceIndustryCode string `parquet:"source_industry_code"`
	SourceIndustryCodeSet string `parquet:"source_industry_code_set"`
	SourceIndustryLabel string `parquet:"source_industry_label"`
	SourceIndustryLabelEn string `parquet:"source_industry_label_en"`
	SourceIndustryLabelFi string `parquet:"source_industry_label_fi"`
	SourceIndustryLabelSv string `parquet:"source_industry_label_sv"`
	MappedNACECode string `parquet:"mapped_nace_code"`
	NACERevision string `parquet:"nace_revision"`
	IsPrimary bool `parquet:"is_primary"`
}

type AddressExportRow struct {
	CountryISO2 string `parquet:"country_iso2"`
	SourceSlug string `parquet:"source_slug"`
	SourceRunID string `parquet:"source_run_id"`
	SourceRecordID string `parquet:"source_record_id"`
	SourceItemHash string `parquet:"source_item_hash"`
	BusinessID string `parquet:"business_id"`
	SourcePosition int32 `parquet:"source_position"`
	AddressTypeCode int32 `parquet:"address_type_code"`
	AddressType string `parquet:"address_type"`
	Street string `parquet:"street"`
	PostCode string `parquet:"post_code"`
	CityFi string `parquet:"city_fi"`
	CitySv string `parquet:"city_sv"`
	MunicipalityCode string `parquet:"municipality_code"`
	RegisteredOn string `parquet:"registered_on"`
}

type RegisteredEntryExportRow struct {
	CountryISO2 string `parquet:"country_iso2"`
	SourceSlug string `parquet:"source_slug"`
	SourceRunID string `parquet:"source_run_id"`
	SourceRecordID string `parquet:"source_record_id"`
	SourceItemHash string `parquet:"source_item_hash"`
	BusinessID string `parquet:"business_id"`
	RegisterCode string `parquet:"register_code"`
	RegisterLabel string `parquet:"register_label"`
	EntryTypeCode string `parquet:"entry_type_code"`
	EntryTypeLabel string `parquet:"entry_type_label"`
	EntryTypeLabelEn string `parquet:"entry_type_label_en"`
	RegisteredOn string `parquet:"registered_on"`
	EndedOn string `parquet:"ended_on"`
	IsCurrent bool `parquet:"is_current"`
}

type TaxRegistrationExportRow struct {
	CountryISO2 string `parquet:"country_iso2"`
	SourceSlug string `parquet:"source_slug"`
	SourceRunID string `parquet:"source_run_id"`
	SourceRecordID string `parquet:"source_record_id"`
	SourceItemHash string `parquet:"source_item_hash"`
	BusinessID string `parquet:"business_id"`
	RegistrationType string `parquet:"registration_type"`
	RegisterCode string `parquet:"register_code"`
	CurrentRegistered bool `parquet:"current_registered"`
	FirstRegisteredOn string `parquet:"first_registered_on"`
	EndedOn string `parquet:"ended_on"`
}

type WebsiteExportRow struct {
	CountryISO2 string `parquet:"country_iso2"`
	SourceSlug string `parquet:"source_slug"`
	SourceRunID string `parquet:"source_run_id"`
	SourceRecordID string `parquet:"source_record_id"`
	SourceItemHash string `parquet:"source_item_hash"`
	BusinessID string `parquet:"business_id"`
	URL string `parquet:"url"`
	NormalizedURL string `parquet:"normalized_url"`
	Host string `parquet:"host"`
	Path string `parquet:"path"`
	RegisteredOn string `parquet:"registered_on"`
	EndedOn string `parquet:"ended_on"`
	IsCurrent bool `parquet:"is_current"`
	IsPrimary bool `parquet:"is_primary"`
}
```

Implement:

```go
func ProjectExportRows(record CompanyRecord, runID string) ExportRows {
	profile := record.ToProfile()
	sourceRecordID := strings.TrimSpace(record.BusinessID.Value)
	exportedAt := time.Now().UTC().Format(time.RFC3339)
	websiteHost, websitePath := websiteParts(profile.Website)

	rows := ExportRows{
		Companies: []CompanyExportRow{{
			CountryISO2: "FI",
			SourceSlug: SourceSlug,
			SourceRunID: runID,
			SourceRecordID: sourceRecordID,
			SourceNativeID: sourceRecordID,
			SourcePayloadHash: record.PayloadHash,
			SourceUpdatedAt: record.LastModified,
			ExportedAt: exportedAt,
			SchemaVersion: SourceExportSchemaVersion,
			BusinessID: sourceRecordID,
			VATID: profile.VATID,
			EUID: profile.EUID,
			LegalName: profile.LegalName,
			LegalNameNormalized: normalizedText(profile.LegalName),
			LifecycleStatus: lifecycleStatus(record),
			IsActive: profile.IsActive,
			LegalFormCode: profile.LegalFormCode,
			LegalFormLabel: profile.LegalForm,
			LegalFormLabelEn: descriptionByLanguage(currentCompanyForm(record.CompanyForms).Descriptions, "3"),
			PrimaryIndustryCode: record.MainBusinessLine.Type,
			PrimaryIndustryCodeSet: record.MainBusinessLine.TypeCodeSet,
			PrimaryIndustryLabel: profile.MainBusinessLine,
			PrimaryIndustryLabelEn: descriptionByLanguage(record.MainBusinessLine.Descriptions, "3"),
			PrimaryNACECode: mappedNACECode(record.MainBusinessLine.Type),
			PrimaryNACERevision: "2.1",
			WebsiteURL: record.Website.URL,
			WebsiteNormalizedURL: profile.Website,
			WebsiteHost: websiteHost,
		}},
	}
	rows.CompanyNames = projectNameRows(record, runID, sourceRecordID)
	rows.LegalForms = projectLegalFormRows(record, runID, sourceRecordID)
	rows.Industries = projectIndustryRows(record, runID, sourceRecordID)
	rows.Addresses = projectAddressRows(record, runID, sourceRecordID)
	rows.RegisteredEntries = projectRegisteredEntryRows(record, runID, sourceRecordID)
	rows.TaxRegistrations = projectTaxRegistrationRows(record, runID, sourceRecordID, profile.TaxRegistrations)
	rows.Websites = projectWebsiteRows(record, runID, sourceRecordID, websiteHost, websitePath)
	return rows
}
```

Implement helper functions in the same file:

```go
func sourceItemHash(kind string, businessID string, value any) string
func normalizedText(value string) string
func lifecycleStatus(record CompanyRecord) string
func mappedNACECode(sourceCode string) string
func websiteParts(normalizedURL string) (string, string)
func postOfficeCity(postOffices []PostOffice, languageCode string) string
func postOfficeMunicipalityCode(postOffices []PostOffice) string
```

Use `sha256` over `kind`, `businessID`, and marshaled item payload for `sourceItemHash`.

- [ ] **Step 4: Run tests to verify pass**

Run:

```bash
GOWORK=off go test ./finland/prhytj -run TestProjectExportRowsFromRealSample -count=1 -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add corpscout/countrydata/finland/prhytj/export_rows.go corpscout/countrydata/finland/prhytj/export_rows_test.go
git commit -m "feat: project Finland PRH source export rows"
```

## Task 4: Add Typed Parquet Writer

**Files:**
- Modify: `corpscout/countrydata/go.mod`
- Create: `corpscout/countrydata/finland/prhytj/parquet_writer.go`
- Create: `corpscout/countrydata/finland/prhytj/parquet_writer_test.go`

- [ ] **Step 1: Add dependency**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/countrydata
GOWORK=off go get github.com/parquet-go/parquet-go@v0.30.1
```

Expected: `go.mod` and `go.sum` update.

- [ ] **Step 2: Write failing Parquet writer test**

Create `corpscout/countrydata/finland/prhytj/parquet_writer_test.go`:

```go
package prhytj

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func TestWriteParquetRowsWritesReadableFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "companies.parquet")
	rows := []CompanyExportRow{{
		CountryISO2: "FI",
		SourceSlug: SourceSlug,
		SourceRunID: "run-1",
		BusinessID: "0100130-4",
		LegalName: "Dynava Oy",
		SchemaVersion: SourceExportSchemaVersion,
	}}

	if err := WriteParquetRows(path, rows); err != nil {
		t.Fatalf("write parquet: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat parquet: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("parquet file is empty")
	}
	handle, err := os.Open(path)
	if err != nil {
		t.Fatalf("open parquet handle: %v", err)
	}
	defer handle.Close()
	file, err := parquet.OpenFile(handle, info.Size())
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}
	if file.NumRows() != 1 {
		t.Fatalf("NumRows = %d, want 1", file.NumRows())
	}
}
```

- [ ] **Step 3: Run tests to verify failure**

Run:

```bash
GOWORK=off go test ./finland/prhytj -run TestWriteParquetRowsWritesReadableFile -count=1 -v
```

Expected: FAIL because `WriteParquetRows` does not exist.

- [ ] **Step 4: Implement Parquet writer**

Create `corpscout/countrydata/finland/prhytj/parquet_writer.go`:

```go
package prhytj

import (
	"os"
	"path/filepath"

	"github.com/cockroachdb/errors"
	"github.com/parquet-go/parquet-go"
)

func WriteParquetRows[T any](path string, rows []T) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return errors.Wrap(err, "create parquet directory")
	}
	tempPath := path + ".tmp"
	if err := parquet.WriteFile(tempPath, rows); err != nil {
		_ = os.Remove(tempPath)
		return errors.Wrap(err, "write parquet file")
	}
	if err := os.Rename(tempPath, path); err != nil {
		_ = os.Remove(tempPath)
		return errors.Wrap(err, "rename parquet file")
	}
	return nil
}
```

- [ ] **Step 5: Run tests to verify pass**

Run:

```bash
GOWORK=off go test ./finland/prhytj -run TestWriteParquetRowsWritesReadableFile -count=1 -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add corpscout/countrydata/go.mod corpscout/countrydata/go.sum corpscout/countrydata/finland/prhytj/parquet_writer.go corpscout/countrydata/finland/prhytj/parquet_writer_test.go
git commit -m "feat: add Finland PRH parquet writer"
```

## Task 5: Add PRH Source Export

**Files:**
- Create: `corpscout/countrydata/finland/prhytj/export.go`
- Create: `corpscout/countrydata/finland/prhytj/export_test.go`

- [ ] **Step 1: Write failing source export test**

Create `corpscout/countrydata/finland/prhytj/export_test.go`:

```go
package prhytj

import (
	"os"
	"path/filepath"
	"testing"

	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

func TestSourceExportWritesParquetFilesAndManifest(t *testing.T) {
	dataDir := t.TempDir()
	snapshotPath := filepath.Join(dataDir, "snapshots", "sample.ndjson")
	writeTestFile(t, snapshotPath, []byte(`{"businessId":{"value":"0100130-4"},"names":[{"name":"Dynava Oy","type":"1"}],"tradeRegisterStatus":"1","status":"2"}`+"\n"))

	source := NewSource(Config{DataDir: dataDir})
	result, err := source.Export(t.Context(), ExportOptions{
		DataDir: dataDir,
		SnapshotPath: snapshotPath,
		RunID: "run-1",
	})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if result.ManifestPath == "" {
		t.Fatal("manifest path is empty")
	}
	manifest, err := countryimport.LoadExportManifest(result.ManifestPath)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if manifest.ExportKind != "source" || manifest.RecordsExported != 1 {
		t.Fatalf("manifest = %#v", manifest)
	}
	for _, name := range []string{"companies", "company_names", "legal_forms", "industries", "addresses", "registered_entries", "tax_registrations", "websites"} {
		if exportFileByName(manifest.Files, name) == nil {
			t.Fatalf("missing export file %s", name)
		}
	}
}

func writeTestFile(t *testing.T, path string, payload []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create test file directory: %v", err)
	}
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}
}

func exportFileByName(files []countryimport.ExportFile, name string) *countryimport.ExportFile {
	for i := range files {
		if files[i].Name == name {
			return &files[i]
		}
	}
	return nil
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
GOWORK=off go test ./finland/prhytj -run TestSourceExportWritesParquetFilesAndManifest -count=1 -v
```

Expected: FAIL because `ExportOptions`, `ExportResult`, and `Source.Export` do not exist.

- [ ] **Step 3: Implement source export**

Create `corpscout/countrydata/finland/prhytj/export.go`:

```go
package prhytj

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

type ExportOptions struct {
	DataDir string
	SnapshotPath string
	RunID string
	Limit int64
}

type ExportResult struct {
	SourceSlug string `json:"source_slug"`
	RunID string `json:"run_id"`
	ManifestPath string `json:"manifest_path"`
	RecordsSeen int64 `json:"records_seen"`
	RecordsExported int64 `json:"records_exported"`
	DecodeErrors int64 `json:"decode_errors"`
}

func (s *Source) Export(ctx context.Context, opts ExportOptions) (ExportResult, error) {
	if s == nil {
		return ExportResult{}, countryimport.WrapSourceError(countryimport.ErrorKindState, SourceSlug, "", "", 0, errors.New("nil PRH YTJ source"))
	}
	runID := strings.TrimSpace(opts.RunID)
	if runID == "" {
		runID = time.Now().UTC().Format("20060102T150405Z") + "-prhytj"
	}
	dataDir := resolveString(opts.DataDir, s.cfg.DataDir, defaultDataDir)
	snapshotPath := strings.TrimSpace(opts.SnapshotPath)
	if snapshotPath == "" {
		resolved, err := latestSnapshotPath(filepath.Join(dataDir, "snapshots"))
		if err != nil {
			return ExportResult{}, err
		}
		snapshotPath = resolved
	}

	rows, recordsSeen, recordsExported, decodeErrors, err := readExportRows(ctx, snapshotPath, runID, opts.Limit)
	if err != nil {
		return ExportResult{}, err
	}

	exportDir := filepath.Join(dataDir, "exports", runID)
	files, err := writeSourceExportFiles(exportDir, rows)
	if err != nil {
		return ExportResult{}, countryimport.WrapSourceError(countryimport.ErrorKindFileIO, SourceSlug, "", exportDir, 0, err)
	}
	snapshotSHA, _, err := countryimport.HashFileSHA256(snapshotPath)
	if err != nil {
		return ExportResult{}, countryimport.WrapSourceError(countryimport.ErrorKindFileIO, SourceSlug, "", snapshotPath, 0, err)
	}
	sourceSlug := "prhytj"
	manifest := countryimport.ExportManifest{
		ManifestVersion: countryimport.ExportManifestVersion,
		CountryISO2: "FI",
		SourceSlug: &sourceSlug,
		ExportKind: "source",
		RunID: runID,
		SchemaVersion: SourceExportSchemaVersion,
		CreatedAt: time.Now().UTC(),
		Inputs: []countryimport.ExportInput{{Path: snapshotPath, SHA256: snapshotSHA}},
		Files: files,
		RecordsSeen: recordsSeen,
		RecordsExported: recordsExported,
		DecodeErrors: decodeErrors,
	}
	manifestPath := filepath.Join(exportDir, "manifest.json")
	if err := countryimport.SaveExportManifest(manifestPath, manifest); err != nil {
		return ExportResult{}, countryimport.WrapSourceError(countryimport.ErrorKindFileIO, SourceSlug, "", manifestPath, 0, err)
	}
	return ExportResult{SourceSlug: SourceSlug, RunID: runID, ManifestPath: manifestPath, RecordsSeen: recordsSeen, RecordsExported: recordsExported, DecodeErrors: decodeErrors}, nil
}
```

Implement private helpers in the same file:

```go
func readExportRows(ctx context.Context, snapshotPath string, runID string, limit int64) (ExportRows, int64, int64, int64, error)
func appendExportRows(dst *ExportRows, src ExportRows)
func writeSourceExportFiles(exportDir string, rows ExportRows) ([]countryimport.ExportFile, error)
func addExportFile[T any](files *[]countryimport.ExportFile, exportDir string, name string, rows []T) error
func schemaHashForRows[T any]() string
```

`readExportRows` must mirror `Process`: scan NDJSON line by line, log decode errors with `slog.WarnContext`, compute `RawPayload` and `PayloadHash`, continue after bad lines, and stop at `Limit`.

`writeSourceExportFiles` must write all eight PRH source export files, even when row slices are empty.

- [ ] **Step 4: Run tests to verify pass**

Run:

```bash
GOWORK=off go test ./finland/prhytj -run 'TestSourceExport|TestProjectExportRows|TestWriteParquetRows' -count=1 -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add corpscout/countrydata/finland/prhytj/export.go corpscout/countrydata/finland/prhytj/export_test.go
git commit -m "feat: export Finland PRH source parquet"
```

## Task 6: Add Finland Final Export Builder

**Files:**
- Create: `corpscout/countrydata/finland/types.go`
- Create: `corpscout/countrydata/finland/export.go`
- Create: `corpscout/countrydata/finland/export_test.go`

- [ ] **Step 1: Write failing final export test**

Create `corpscout/countrydata/finland/export_test.go`:

```go
package finland

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pulsarpoint/corpscout/countrydata/finland/prhytj"
	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

func TestBuildFinalExportFromPRHSourceManifest(t *testing.T) {
	dataDir := t.TempDir()
	source := prhytj.NewSource(prhytj.Config{DataDir: filepath.Join(dataDir, "sources", SourcePRHYTJ)})
	snapshotPath := filepath.Join(dataDir, "sources", SourcePRHYTJ, "snapshots", "sample.ndjson")
	writeTestFile(t, snapshotPath, []byte(`{"businessId":{"value":"0100130-4"},"names":[{"name":"Dynava Oy","type":"1"}],"tradeRegisterStatus":"1","status":"2"}`+"\n"))
	sourceResult, err := source.Export(t.Context(), prhytj.ExportOptions{DataDir: filepath.Join(dataDir, "sources", SourcePRHYTJ), SnapshotPath: snapshotPath, RunID: "source-run-1"})
	if err != nil {
		t.Fatalf("source export: %v", err)
	}

	result, err := BuildFinalExport(t.Context(), BuildExportOptions{
		DataDir: dataDir,
		RunID: "final-run-1",
		SourceManifestPaths: map[string]string{SourcePRHYTJ: sourceResult.ManifestPath},
	})
	if err != nil {
		t.Fatalf("build final export: %v", err)
	}
	manifest, err := countryimport.LoadExportManifest(result.ManifestPath)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if manifest.ExportKind != "final" || manifest.MergeRuleVersion != MergeRuleVersionV1 {
		t.Fatalf("manifest = %#v", manifest)
	}
	for _, file := range manifest.Files {
		path := filepath.Join(filepath.Dir(result.ManifestPath), file.Path)
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat final parquet %s: %v", file.Name, err)
		}
		if info.Size() == 0 {
			t.Fatalf("final parquet %s is empty", file.Name)
		}
	}
}

func writeTestFile(t *testing.T, path string, payload []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create test file directory: %v", err)
	}
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
GOWORK=off go test ./finland -run TestBuildFinalExportFromPRHSourceManifest -count=1 -v
```

Expected: FAIL because `BuildFinalExport` and final types do not exist.

- [ ] **Step 3: Implement final export types**

Create `corpscout/countrydata/finland/types.go`:

```go
package finland

const (
	FinalSchemaVersionV1 = "finland.final.v1"
	MergeRuleVersionV1 = "finland.merge.v1"
)

type FinalCompanyRow struct {
	CountryCompanyID string `parquet:"country_company_id"`
	CountryISO2 string `parquet:"country_iso2"`
	PrimarySourceSlug string `parquet:"primary_source_slug"`
	PrimarySourceRecordID string `parquet:"primary_source_record_id"`
	BusinessID string `parquet:"business_id"`
	LegalName string `parquet:"legal_name"`
	LegalNameEn string `parquet:"legal_name_en"`
	LegalNameNormalized string `parquet:"legal_name_normalized"`
	LifecycleStatus string `parquet:"lifecycle_status"`
	IsActive bool `parquet:"is_active"`
	VATID string `parquet:"vat_id"`
	EUID string `parquet:"euid"`
	LegalFormCode string `parquet:"legal_form_code"`
	LegalFormLabel string `parquet:"legal_form_label"`
	LegalFormLabelEn string `parquet:"legal_form_label_en"`
	PrimaryIndustryCode string `parquet:"primary_industry_code"`
	PrimaryNACECode string `parquet:"primary_nace_code"`
	PrimaryNACERevision string `parquet:"primary_nace_revision"`
	WebsiteNormalizedURL string `parquet:"website_normalized_url"`
	SourcePayloadHash string `parquet:"source_payload_hash"`
	ProfileHash string `parquet:"profile_hash"`
	MergeRuleVersion string `parquet:"merge_rule_version"`
	IsTranslated bool `parquet:"is_translated"`
	ExportedAt string `parquet:"exported_at"`
}
```

Create minimal final row types for names, identifiers, addresses, industries, websites, and source evidence with explicit `parquet` tags matching the spec.

- [ ] **Step 4: Implement final export builder**

Create `corpscout/countrydata/finland/export.go`:

```go
package finland

import (
	"context"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/parquet-go/parquet-go"
	"github.com/pulsarpoint/corpscout/countrydata/finland/prhytj"
	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

type BuildExportOptions struct {
	DataDir string
	RunID string
	SourceManifestPaths map[string]string
}

type BuildExportResult struct {
	RunID string `json:"run_id"`
	ManifestPath string `json:"manifest_path"`
	RecordsExported int64 `json:"records_exported"`
}

func BuildFinalExport(ctx context.Context, opts BuildExportOptions) (BuildExportResult, error) {
	if err := ctx.Err(); err != nil {
		return BuildExportResult{}, errors.Wrap(err, "build Finland final export")
	}
	layout := LayoutForDataDir(opts.DataDir)
	runID := strings.TrimSpace(opts.RunID)
	if runID == "" {
		runID = time.Now().UTC().Format("20060102T150405Z") + "-finland-final"
	}
	prhManifestPath := opts.SourceManifestPaths[SourcePRHYTJ]
	if prhManifestPath == "" {
		status, err := SourceStatusFromLatestManifest(opts.DataDir, SourcePRHYTJ)
		if err != nil {
			return BuildExportResult{}, err
		}
		prhManifestPath = status.LastExportManifestPath
	}
	if prhManifestPath == "" {
		return BuildExportResult{}, errors.New("missing PRH YTJ source export manifest")
	}
	sourceManifest, err := countryimport.LoadExportManifest(prhManifestPath)
	if err != nil {
		return BuildExportResult{}, err
	}
	sourceDir := filepath.Dir(prhManifestPath)
	companies, err := parquet.ReadFile[prhytj.CompanyExportRow](filepath.Join(sourceDir, "companies.parquet"))
	if err != nil {
		return BuildExportResult{}, errors.Wrap(err, "read PRH companies parquet")
	}
	finalRows := mapPRHCompaniesToFinal(companies)
	exportDir := filepath.Join(layout.FinalExportsDir(), runID)
	files, err := writeFinalCoreFiles(exportDir, finalRows)
	if err != nil {
		return BuildExportResult{}, err
	}
	manifestPath := filepath.Join(exportDir, "manifest.json")
	manifest := countryimport.ExportManifest{
		ManifestVersion: countryimport.ExportManifestVersion,
		CountryISO2: CountryISO2,
		SourceSlug: nil,
		ExportKind: "final",
		RunID: runID,
		SchemaVersion: FinalSchemaVersionV1,
		MergeRuleVersion: MergeRuleVersionV1,
		CreatedAt: time.Now().UTC(),
		Files: files,
		SourceExportsUsed: []countryimport.SourceExportRef{{SourceSlug: SourcePRHYTJ, RunID: sourceManifest.RunID, ManifestPath: prhManifestPath}},
		RecordsSeen: int64(len(companies)),
		RecordsExported: int64(len(finalRows.Companies)),
	}
	if err := countryimport.SaveExportManifest(manifestPath, manifest); err != nil {
		return BuildExportResult{}, err
	}
	return BuildExportResult{RunID: runID, ManifestPath: manifestPath, RecordsExported: int64(len(finalRows.Companies))}, nil
}
```

Implement helper functions in the same file:

```go
type finalRows struct {
	Companies []FinalCompanyRow
	CompanyNames []FinalCompanyNameRow
	Identifiers []FinalIdentifierRow
	Addresses []FinalAddressRow
	Industries []FinalIndustryRow
	Websites []FinalWebsiteRow
	SourceEvidence []FinalSourceEvidenceRow
}

func mapPRHCompaniesToFinal(companies []prhytj.CompanyExportRow) finalRows
func writeFinalCoreFiles(exportDir string, rows finalRows) ([]countryimport.ExportFile, error)
func profileHash(row FinalCompanyRow) string
```

For Finland v1, PRH is the only source. `country_company_id` is `FI:` plus `business_id`.

- [ ] **Step 5: Run tests to verify pass**

Run:

```bash
GOWORK=off go test ./finland -run 'TestBuildFinalExport|TestLayout|TestSourceStatus' -count=1 -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add corpscout/countrydata/finland/types.go corpscout/countrydata/finland/export.go corpscout/countrydata/finland/export_test.go
git commit -m "feat: build Finland final country export"
```

## Task 7: Add `finland-countrydata` CLI

**Files:**
- Create: `corpscout/countrydata/cmd/finland-countrydata/main.go`
- Create: `corpscout/countrydata/cmd/finland-countrydata/main_test.go`

- [ ] **Step 1: Write failing CLI parse tests**

Create `corpscout/countrydata/cmd/finland-countrydata/main_test.go`:

```go
package main

import "testing"

func TestParseArgsSyncSource(t *testing.T) {
	cfg, err := parseArgs([]string{"sync-source", "--source", "prhytj", "--data-dir", "/data", "--max-pages", "2"})
	if err != nil {
		t.Fatalf("parse args: %v", err)
	}
	if cfg.command != "sync-source" || cfg.source != "prhytj" || cfg.dataDir != "/data" || cfg.maxPages != 2 {
		t.Fatalf("cfg = %#v", cfg)
	}
}

func TestParseArgsBuildExport(t *testing.T) {
	cfg, err := parseArgs([]string{"build-export", "--data-dir", "/data", "--run-id", "final-run-1"})
	if err != nil {
		t.Fatalf("parse args: %v", err)
	}
	if cfg.command != "build-export" || cfg.runID != "final-run-1" {
		t.Fatalf("cfg = %#v", cfg)
	}
}

func TestParseArgsRejectsUnknownSource(t *testing.T) {
	_, err := parseArgs([]string{"sync-source", "--source", "unknown"})
	if err == nil {
		t.Fatal("parse args returned nil error")
	}
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
GOWORK=off go test ./cmd/finland-countrydata -count=1 -v
```

Expected: FAIL because the command does not exist.

- [ ] **Step 3: Implement CLI**

Create `corpscout/countrydata/cmd/finland-countrydata/main.go`:

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
	"github.com/pulsarpoint/corpscout/countrydata/finland"
	"github.com/pulsarpoint/corpscout/countrydata/finland/prhytj"
	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

type cliConfig struct {
	command string
	source string
	envPath string
	dataDir string
	runID string
	maxPages int
	chunkSize int
	buildExport bool
}

func main() {
	cfg, err := parseArgs(os.Args[1:])
	if err != nil {
		slog.Error("parse Finland countrydata command", "error", err)
		os.Exit(2)
	}
	result, err := run(context.Background(), cfg)
	if err != nil {
		slog.Error("run Finland countrydata command", "command", cfg.command, "source", cfg.source, "error_kind", countryimport.Classify(err), "error", err)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		slog.Error("write Finland countrydata result", "error", err)
		os.Exit(1)
	}
}
```

Implement:

```go
func parseArgs(args []string) (cliConfig, error)
func run(ctx context.Context, cfg cliConfig) (map[string]any, error)
func runSyncSource(ctx context.Context, cfg cliConfig) (map[string]any, error)
func runExportSource(ctx context.Context, cfg cliConfig) (map[string]any, error)
func runStatusSource(cfg cliConfig) (map[string]any, error)
func runStatus(cfg cliConfig) (map[string]any, error)
func runBuildExport(ctx context.Context, cfg cliConfig) (map[string]any, error)
```

Accepted commands:

```text
sync-source
status-source
export-source
status
build-export
sync
```

Rules:

- `--source` is required for `sync-source`, `status-source`, `export-source`, and `sync`.
- `--source` currently accepts only `prhytj`.
- `sync-source` runs PRH download then PRH export.
- `export-source` runs PRH export from latest snapshot or explicit `--snapshot-path` if added.
- `build-export` runs `finland.BuildFinalExport`.
- `sync --source prhytj --build-export` runs source sync then final export.
- JSON stdout must include `command`, `country_iso2`, `source`, `status`, and relevant manifest path.

- [ ] **Step 4: Run CLI tests**

Run:

```bash
GOWORK=off go test ./cmd/finland-countrydata -count=1 -v
```

Expected: PASS.

- [ ] **Step 5: Run package compile checks**

Run:

```bash
GOWORK=off go test ./finland ./finland/prhytj ./cmd/finland-countrydata -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add corpscout/countrydata/cmd/finland-countrydata
git commit -m "feat: add Finland countrydata CLI"
```

## Task 8: Documentation and Final Verification

**Files:**
- Modify: `corpscout/countrydata/finland/prhytj/README.md`

- [ ] **Step 1: Update README**

Add this section to `corpscout/countrydata/finland/prhytj/README.md`:

```markdown
## Finland countrydata CLI

The source can be run through the country-level CLI:

```bash
GOWORK=off go run ./cmd/finland-countrydata sync-source --source prhytj --data-dir ./data/countrydata/finland --max-pages 2
GOWORK=off go run ./cmd/finland-countrydata status-source --source prhytj --data-dir ./data/countrydata/finland
GOWORK=off go run ./cmd/finland-countrydata build-export --data-dir ./data/countrydata/finland
```

Source exports are written under:

```text
./data/countrydata/finland/sources/prhytj/exports/<run-id>/
```

Final country exports are written under:

```text
./data/countrydata/finland/final/exports/<run-id>/
```
```

- [ ] **Step 2: Run focused verification**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/countrydata
GOWORK=off go test ./import ./finland ./finland/prhytj ./cmd/finland-countrydata -count=1
```

Expected: PASS.

- [ ] **Step 3: Run full countrydata verification**

Run:

```bash
GOWORK=off go test ./... -count=1
```

Expected: PASS. Live integration tests are outside this command unless explicitly enabled through their build tags and environment variables.

- [ ] **Step 4: Commit**

```bash
git add corpscout/countrydata/finland/prhytj/README.md
git commit -m "docs: document Finland countrydata parquet CLI"
```

## Final Acceptance Criteria

The implementation is complete when:

- `finland-countrydata sync-source --source prhytj` downloads a PRH snapshot and writes source Parquet export files.
- `finland-countrydata status-source --source prhytj` returns source status as JSON.
- `finland-countrydata build-export` writes all required final core Parquet files and final `manifest.json`.
- Source and final manifests include file hashes, row counts, schema versions, run IDs, and lineage.
- Existing `prhytj` download/process tests still pass.
- No central Corpscout Postgres schema or scheduler adapter is required for the Finland package to run.

## Self-Review

Spec coverage:

- CLI-first execution contract: Task 7.
- Source sync/status/export: Tasks 5 and 7.
- Country final export: Task 6.
- Parquet plus manifest contract: Tasks 1, 4, 5, and 6.
- Required final core files: Task 6.
- Augmentation boundary: Task 3 source-only mapping and Task 6 PRH-only merge.
- Central importer exclusion: Scope section and final acceptance criteria.

Placeholder scan:

- The plan contains concrete file paths, commands, and code snippets; no unspecified implementation steps remain.

Type consistency:

- `ExportManifest`, `ExportFile`, and `SourceExportRef` are defined in Task 1 and used in Tasks 5 and 6.
- `SourcePRHYTJ`, `CountryISO2`, and layout helpers are defined in Task 2 and used in Tasks 6 and 7.
- `CompanyExportRow` is defined in Task 3 and read in Task 6.
