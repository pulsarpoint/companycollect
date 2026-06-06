# Finland PRH YTJ Import Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a standalone `countrydata` Go module for Finland PRH YTJ v3 that can download the paginated source as a bulk snapshot, process it in chunks, run independently from a CLI, and be called from Corpscout scheduler through a thin adapter.

**Architecture:** Source logic lives outside `scheduler/internal` in `corpscout/countrydata`. The shared `countrydata/import` package owns source-neutral options, classified errors, env loading, and metadata-store contracts, while `finland/prhytj` owns PRH-specific config, parsing, mapping, download, process, and store behavior. Scheduler imports the module and calls the same public methods as the CLI.

**Tech Stack:** Go 1.26.1, `log/slog`, `github.com/cockroachdb/errors`, standard-library `net/http`, `httptest`, `encoding/json`, local NDJSON snapshots, opt-in live integration tests.

---

## File Structure

Create the standalone module:

- Create: `countrydata/go.mod`
- Create: `countrydata/import/options.go`
- Create: `countrydata/import/errors.go`
- Create: `countrydata/import/metadata.go`
- Create: `countrydata/import/metadata_store.go`
- Create: `countrydata/import/env.go`
- Create: `countrydata/import/errors_test.go`
- Create: `countrydata/import/env_test.go`
- Create: `countrydata/finland/prhytj/config.go`
- Create: `countrydata/finland/prhytj/types.go`
- Create: `countrydata/finland/prhytj/mapping.go`
- Create: `countrydata/finland/prhytj/source.go`
- Create: `countrydata/finland/prhytj/download.go`
- Create: `countrydata/finland/prhytj/process.go`
- Create: `countrydata/finland/prhytj/store.go`
- Create: `countrydata/finland/prhytj/mapping_test.go`
- Create: `countrydata/finland/prhytj/download_test.go`
- Create: `countrydata/finland/prhytj/process_test.go`
- Create: `countrydata/finland/prhytj/live_integration_test.go`
- Create: `countrydata/finland/prhytj/testdata/prh_page_1.json`
- Create: `countrydata/finland/prhytj/testdata/prh_page_2.json`
- Create: `countrydata/finland/prhytj/testdata/prh_snapshot_mixed.ndjson`
- Create: `countrydata/cmd/prhytj-import/main.go`
- Create: `countrydata/cmd/prhytj-import/main_test.go`

Add the scheduler adapter:

- Modify: `scheduler/go.mod`
- Create: `scheduler/internal/countrydata/finland_prhytj.go`
- Create: `scheduler/internal/countrydata/finland_prhytj_test.go`

Do not modify existing country packages such as `scheduler/internal/france`, `scheduler/internal/se`, `scheduler/internal/brreg`, or unrelated e2e translation files.

---

### Task 1: Shared Country Import Foundation

**Files:**
- Create: `countrydata/go.mod`
- Create: `countrydata/import/options.go`
- Create: `countrydata/import/errors.go`
- Create: `countrydata/import/metadata.go`
- Create: `countrydata/import/metadata_store.go`
- Create: `countrydata/import/env.go`
- Test: `countrydata/import/errors_test.go`
- Test: `countrydata/import/env_test.go`

- [ ] **Step 1: Write failing shared package tests**

Create `countrydata/import/errors_test.go`:

```go
package countryimport

import (
	"context"
	"net/http"
	"testing"

	"github.com/cockroachdb/errors"
)

func TestSourceErrorKindClassification(t *testing.T) {
	err := WrapSourceError(ErrorKindHTTPStatus, "finland_prh_ytj_v3", "https://example.test", "", http.StatusNotFound, errors.New("not found"))

	if !IsKind(err, ErrorKindHTTPStatus) {
		t.Fatalf("expected http status kind, got %s", Classify(err))
	}
	if Classify(context.DeadlineExceeded) != ErrorKindTimeout {
		t.Fatalf("deadline should classify as timeout")
	}
	if Classify(errors.Wrap(err, "outer")) != ErrorKindHTTPStatus {
		t.Fatalf("wrapped source error should keep kind")
	}
}
```

Create `countrydata/import/env_test.go`:

```go
package countryimport

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadEnvFileSetsUnsetValues(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte("PRH_YTJ_PAGE_DELAY_MS=250\nPRH_YTJ_USER_AGENT=test-agent\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PRH_YTJ_USER_AGENT", "existing")

	if err := LoadEnvFile(path); err != nil {
		t.Fatalf("load env file: %v", err)
	}
	if got := os.Getenv("PRH_YTJ_PAGE_DELAY_MS"); got != "250" {
		t.Fatalf("expected page delay from env file, got %q", got)
	}
	if got := os.Getenv("PRH_YTJ_USER_AGENT"); got != "existing" {
		t.Fatalf("existing env should not be overwritten, got %q", got)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/countrydata
GOWORK=off go test ./import -count=1
```

Expected: FAIL because `go.mod` and the shared package do not exist.

- [ ] **Step 3: Implement shared package**

Create `countrydata/go.mod`:

```go
module github.com/pulsarpoint/corpscout/countrydata

go 1.26.1

require github.com/cockroachdb/errors v1.13.0
```

Create `countrydata/import/options.go`:

```go
package countryimport

import (
	"context"
	"time"
)

const (
	DefaultPageStart      = 1
	DefaultChunkSize      = 500
	DefaultRequestTimeout = 60 * time.Second
	DefaultPageDelay      = 500 * time.Millisecond
	DefaultUserAgent      = "corpscout-countrydata/1.0"
)

type BulkSource[T any] interface {
	Download(ctx context.Context, opts DownloadOptions) (DownloadResult, error)
	Process(ctx context.Context, opts ProcessOptions) (ProcessResult, error)
	Store(ctx context.Context, records []T) (StoreResult, error)
}

type DownloadOptions struct {
	DataDir        string
	MaxPages       int
	PageStart      int
	PageDelay      time.Duration
	RequestTimeout time.Duration
	UserAgent      string
	Force          bool
}

type DownloadResult struct {
	SourceSlug      string
	SnapshotPath    string
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
	SourceSlug       string
	SnapshotPath     string
	RecordsSeen      int64
	RecordsProcessed int64
	RecordsStored    int64
	DecodeErrors     int64
	ChunksProcessed  int64
	StartedAt        time.Time
	FinishedAt       time.Time
	Duration         time.Duration
}

type StoreResult struct {
	RecordsReceived int64
	RecordsStored   int64
}
```

Create `countrydata/import/errors.go`:

```go
package countryimport

import (
	"context"
	"fmt"
	"net"

	"github.com/cockroachdb/errors"
)

type ErrorKind string

const (
	ErrorKindUnknown       ErrorKind = "unknown"
	ErrorKindNotFound      ErrorKind = "not_found"
	ErrorKindNoSnapshot    ErrorKind = "no_snapshot"
	ErrorKindTimeout       ErrorKind = "timeout"
	ErrorKindHTTPStatus    ErrorKind = "http_status"
	ErrorKindRemoteDecode  ErrorKind = "remote_decode"
	ErrorKindLineDecode    ErrorKind = "line_decode"
	ErrorKindInvalidConfig ErrorKind = "invalid_config"
	ErrorKindFileIO        ErrorKind = "file_io"
	ErrorKindState         ErrorKind = "state"
)

type SourceError struct {
	Kind   ErrorKind
	Source string
	URL    string
	Path   string
	Status int
	Err    error
}

func (e *SourceError) Error() string {
	if e == nil {
		return ""
	}
	base := fmt.Sprintf("%s source error", e.Kind)
	if e.Source != "" {
		base += " for " + e.Source
	}
	if e.URL != "" {
		base += " url=" + e.URL
	}
	if e.Path != "" {
		base += " path=" + e.Path
	}
	if e.Status != 0 {
		base += fmt.Sprintf(" status=%d", e.Status)
	}
	if e.Err != nil {
		base += ": " + e.Err.Error()
	}
	return base
}

func (e *SourceError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func WrapSourceError(kind ErrorKind, source string, url string, path string, status int, err error) error {
	return &SourceError{Kind: kind, Source: source, URL: url, Path: path, Status: status, Err: err}
}

func IsKind(err error, kind ErrorKind) bool {
	return Classify(err) == kind
}

func Classify(err error) ErrorKind {
	if err == nil {
		return ErrorKindUnknown
	}
	var sourceErr *SourceError
	if errors.As(err, &sourceErr) {
		return sourceErr.Kind
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return ErrorKindTimeout
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return ErrorKindTimeout
	}
	return ErrorKindUnknown
}
```

Create `countrydata/import/metadata.go`:

```go
package countryimport

import "time"

type DownloadMetadata struct {
	SourceSlug           string         `json:"source_slug"`
	SourceName           string         `json:"source_name,omitempty"`
	BaseURL              string         `json:"base_url,omitempty"`
	SnapshotPath         string         `json:"snapshot_path"`
	StartedAt            time.Time      `json:"started_at"`
	FinishedAt           time.Time      `json:"finished_at"`
	DurationMS           int64          `json:"duration_ms"`
	BytesDownloaded      int64          `json:"bytes_downloaded"`
	RecordsSeen          int64          `json:"records_seen"`
	PagesDownloaded      int            `json:"pages_downloaded"`
	FirstPage            int            `json:"first_page,omitempty"`
	LastPage             int            `json:"last_page,omitempty"`
	TotalResultsReported *int64         `json:"total_results_reported,omitempty"`
	SHA256               string         `json:"sha256"`
	HTTPStatuses         map[string]int `json:"http_statuses,omitempty"`
	License              string         `json:"license,omitempty"`
	Attribution          string         `json:"attribution,omitempty"`
}

type ProcessMetadata struct {
	SourceSlug       string    `json:"source_slug"`
	SnapshotPath     string    `json:"snapshot_path"`
	StartedAt        time.Time `json:"started_at"`
	FinishedAt       time.Time `json:"finished_at"`
	DurationMS       int64     `json:"duration_ms"`
	RecordsSeen      int64     `json:"records_seen"`
	RecordsProcessed int64     `json:"records_processed"`
	RecordsStored    int64     `json:"records_stored"`
	DecodeErrors     int64     `json:"decode_errors"`
	ChunksProcessed  int64     `json:"chunks_processed"`
}
```

Create `countrydata/import/metadata_store.go`:

```go
package countryimport

import (
	"context"
)

type MetadataStore interface {
	SaveDownload(ctx context.Context, metadata DownloadMetadata) error
	SaveProcess(ctx context.Context, metadata ProcessMetadata) error
}

type NoopMetadataStore struct{}

func (NoopMetadataStore) SaveDownload(ctx context.Context, metadata DownloadMetadata) error {
	return nil
}

func (NoopMetadataStore) SaveProcess(ctx context.Context, metadata ProcessMetadata) error {
	return nil
}
```

Create `countrydata/import/env.go`:

```go
package countryimport

import (
	"bufio"
	"os"
	"strings"

	"github.com/cockroachdb/errors"
)

func LoadEnvFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return WrapSourceError(ErrorKindNotFound, "", "", path, 0, errors.Wrap(err, "open env file"))
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.Trim(strings.TrimSpace(value), `"'`)
		if key == "" || os.Getenv(key) != "" {
			continue
		}
		if err := os.Setenv(key, value); err != nil {
			return WrapSourceError(ErrorKindInvalidConfig, "", "", path, 0, errors.Wrapf(err, "set env %s", key))
		}
	}
	if err := scanner.Err(); err != nil {
		return WrapSourceError(ErrorKindFileIO, "", "", path, 0, errors.Wrap(err, "scan env file"))
	}
	return nil
}
```

- [ ] **Step 4: Run shared tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/countrydata
GOWORK=off go mod tidy
GOWORK=off go test ./import -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/countrydata/go.mod corpscout/countrydata/go.sum corpscout/countrydata/import
git commit -m "feat: add countrydata import foundation"
```

---

### Task 2: Finland PRH Types, Fixtures, And Mapping

**Files:**
- Create: `countrydata/finland/prhytj/types.go`
- Create: `countrydata/finland/prhytj/mapping.go`
- Create: `countrydata/finland/prhytj/testdata/prh_page_1.json`
- Create: `countrydata/finland/prhytj/testdata/prh_page_2.json`
- Test: `countrydata/finland/prhytj/mapping_test.go`

- [ ] **Step 1: Add real-shaped fixture pages**

Create `countrydata/finland/prhytj/testdata/prh_page_1.json`:

```json
{
  "totalResults": 3,
  "companies": [
    {
      "businessId": {"value": "0100130-4", "registrationDate": "1978-03-15", "source": "3"},
      "euId": {"value": "FIFPRO.0100130-4", "source": "1"},
      "names": [
        {"name": "Dynava Oy", "type": "1", "registrationDate": "2022-01-21", "version": 1, "source": "1"},
        {"name": "Oy Eniro Finland Ab", "type": "1", "registrationDate": "2000-09-22", "endDate": "2022-01-21", "version": 2, "source": "1"},
        {"name": "EDS Media", "type": "3", "registrationDate": "2009-11-30", "version": 1, "source": "1"}
      ],
      "mainBusinessLine": {
        "type": "82200",
        "typeCodeSet": "TOIMI4",
        "registrationDate": "2026-01-01",
        "source": "2",
        "descriptions": [
          {"languageCode": "3", "description": "Activities of call centres"},
          {"languageCode": "1", "description": "Puhelinpalvelukeskusten toiminta"}
        ]
      },
      "website": {"url": "www.dynava.fi", "registrationDate": "2025-09-29", "source": "0"},
      "companyForms": [
        {
          "type": "16",
          "version": 1,
          "source": "1",
          "descriptions": [
            {"languageCode": "3", "description": "Limited company"},
            {"languageCode": "1", "description": "Osakeyhtio"}
          ]
        }
      ],
      "companySituations": [],
      "registeredEntries": [
        {"type": "80", "registrationDate": "1994-06-01", "register": "6", "authority": "1", "descriptions": [{"languageCode": "3", "description": "VAT-liable for business activity"}]},
        {"type": "55", "registrationDate": "1995-03-01", "register": "5", "authority": "1", "descriptions": [{"languageCode": "3", "description": "Registered"}]},
        {"type": "41", "registrationDate": "1998-03-01", "register": "7", "authority": "1", "descriptions": [{"languageCode": "3", "description": "Registered"}]}
      ],
      "addresses": [
        {
          "type": 1,
          "street": "Valimotie",
          "postCode": "00380",
          "buildingNumber": "17-19",
          "registrationDate": "2025-09-29",
          "source": "0",
          "postOffices": [
            {"city": "HELSINGFORS", "languageCode": "2", "municipalityCode": "091"},
            {"city": "HELSINKI", "languageCode": "1", "municipalityCode": "091"}
          ]
        }
      ],
      "tradeRegisterStatus": "1",
      "status": "2",
      "registrationDate": "1973-08-10",
      "lastModified": "2026-01-02T03:04:05"
    },
    {
      "businessId": {"value": "0111111-1", "registrationDate": "1978-03-15", "source": "3"},
      "names": [{"name": "Ceased Example Oy", "type": "1", "registrationDate": "1990-01-01", "source": "1"}],
      "companyForms": [],
      "companySituations": [],
      "registeredEntries": [
        {"type": "80", "registrationDate": "1994-06-01", "endDate": "2001-01-01", "register": "6", "authority": "1"}
      ],
      "addresses": [],
      "tradeRegisterStatus": "4",
      "status": "2",
      "registrationDate": "1990-01-01",
      "endDate": "2002-02-02"
    }
  ]
}
```

Create `countrydata/finland/prhytj/testdata/prh_page_2.json`:

```json
{
  "companies": [
    {
      "businessId": {"value": "0222222-2", "registrationDate": "1978-03-15", "source": "3"},
      "names": [
        {"name": "Newest Primary Oy", "type": "1", "registrationDate": "2024-01-01", "source": "1"},
        {"name": "Older Primary Oy", "type": "1", "registrationDate": "2020-01-01", "source": "1"}
      ],
      "companyForms": [],
      "companySituations": [],
      "registeredEntries": [],
      "addresses": [],
      "tradeRegisterStatus": "1",
      "status": "2",
      "registrationDate": "2020-01-01",
      "unexpectedLiveField": {"kept": true}
    }
  ]
}
```

- [ ] **Step 2: Write failing mapping tests**

Create `countrydata/finland/prhytj/mapping_test.go`:

```go
package prhytj

import (
	"encoding/json"
	"os"
	"testing"
)

func TestPageDecodeAndProfileMappingUsesFinlandRules(t *testing.T) {
	page := readFixturePage(t, "testdata/prh_page_1.json")
	if page.TotalResults == nil || *page.TotalResults != 3 {
		t.Fatalf("expected totalResults 3, got %#v", page.TotalResults)
	}
	if len(page.Companies) != 2 {
		t.Fatalf("expected two companies, got %d", len(page.Companies))
	}

	profile := page.Companies[0].ToProfile()
	if profile.BusinessID != "0100130-4" {
		t.Fatalf("business id mismatch: %q", profile.BusinessID)
	}
	if profile.LegalName != "Dynava Oy" {
		t.Fatalf("legal name mismatch: %q", profile.LegalName)
	}
	if !profile.IsActive {
		t.Fatalf("expected active profile")
	}
	if profile.VATID != "FI01001304" {
		t.Fatalf("vat id mismatch: %q", profile.VATID)
	}
	if !profile.TaxRegistrations.VAT.Registered || !profile.TaxRegistrations.Employer.Registered || !profile.TaxRegistrations.Prepayment.Registered {
		t.Fatalf("expected active tax registration flags: %#v", profile.TaxRegistrations)
	}
	if profile.Website != "https://www.dynava.fi" {
		t.Fatalf("website should be normalized, got %q", profile.Website)
	}
	if len(profile.Addresses) != 1 || profile.Addresses[0].City != "HELSINKI" || profile.Addresses[0].CitySV != "HELSINGFORS" {
		t.Fatalf("address language mapping mismatch: %#v", profile.Addresses)
	}
}

func TestCeasedCompanyIsNotActiveAndEndedVATIsNotRegistered(t *testing.T) {
	page := readFixturePage(t, "testdata/prh_page_1.json")
	profile := page.Companies[1].ToProfile()

	if profile.IsActive {
		t.Fatalf("ceased company should not be active")
	}
	if profile.TaxRegistrations.VAT.Registered {
		t.Fatalf("ended VAT row should not be active")
	}
}

func TestMultipleCurrentPrimaryNamesPickLatestRegistrationDate(t *testing.T) {
	page := readFixturePage(t, "testdata/prh_page_2.json")
	profile := page.Companies[0].ToProfile()

	if profile.LegalName != "Newest Primary Oy" {
		t.Fatalf("expected newest primary name, got %q", profile.LegalName)
	}
}

func readFixturePage(t *testing.T, path string) Page {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var page Page
	if err := json.Unmarshal(payload, &page); err != nil {
		t.Fatal(err)
	}
	return page
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/countrydata
GOWORK=off go test ./finland/prhytj -run 'TestPageDecode|TestCeased|TestMultiple' -count=1
```

Expected: FAIL because the PRH package does not exist.

- [ ] **Step 4: Implement PRH types and mapping**

Create `countrydata/finland/prhytj/types.go` with source-native structs:

```go
package prhytj

import "encoding/json"

const (
	SourceSlug    = "finland_prh_ytj_v3"
	SourceName    = "PRH Open Data YTJ API v3 companies"
	DefaultBaseURL = "https://avoindata.prh.fi/opendata-ytj-api/v3/companies"
)

type Page struct {
	TotalResults *int64          `json:"totalResults,omitempty"`
	Companies    []CompanyRecord `json:"companies"`
}

type CompanyRecord struct {
	BusinessID          Identifier       `json:"businessId"`
	EUID                *Identifier      `json:"euId,omitempty"`
	Names               []Name           `json:"names,omitempty"`
	MainBusinessLine    *BusinessLine    `json:"mainBusinessLine,omitempty"`
	Website             *Website         `json:"website,omitempty"`
	CompanyForms        []CompanyForm    `json:"companyForms,omitempty"`
	CompanySituations   []json.RawMessage `json:"companySituations,omitempty"`
	RegisteredEntries   []RegisteredEntry `json:"registeredEntries,omitempty"`
	Addresses           []Address        `json:"addresses,omitempty"`
	TradeRegisterStatus string           `json:"tradeRegisterStatus,omitempty"`
	Status              string           `json:"status,omitempty"`
	RegistrationDate    string           `json:"registrationDate,omitempty"`
	EndDate             string           `json:"endDate,omitempty"`
	LastModified        string           `json:"lastModified,omitempty"`
	RawPayload          json.RawMessage  `json:"-"`
	PayloadHash         string           `json:"-"`
}

type Identifier struct {
	Value            string `json:"value,omitempty"`
	RegistrationDate string `json:"registrationDate,omitempty"`
	Source           string `json:"source,omitempty"`
}

type Name struct {
	Name             string `json:"name,omitempty"`
	Type             string `json:"type,omitempty"`
	RegistrationDate string `json:"registrationDate,omitempty"`
	EndDate          string `json:"endDate,omitempty"`
	Version          int    `json:"version,omitempty"`
	Source           string `json:"source,omitempty"`
}

type Description struct {
	LanguageCode string `json:"languageCode,omitempty"`
	Description  string `json:"description,omitempty"`
}

type BusinessLine struct {
	Type             string        `json:"type,omitempty"`
	Descriptions     []Description `json:"descriptions,omitempty"`
	TypeCodeSet      string        `json:"typeCodeSet,omitempty"`
	RegistrationDate string        `json:"registrationDate,omitempty"`
	Source           string        `json:"source,omitempty"`
}

type Website struct {
	URL              string `json:"url,omitempty"`
	RegistrationDate string `json:"registrationDate,omitempty"`
	Source           string `json:"source,omitempty"`
}

type CompanyForm struct {
	Type             string        `json:"type,omitempty"`
	Descriptions     []Description `json:"descriptions,omitempty"`
	RegistrationDate string        `json:"registrationDate,omitempty"`
	EndDate          string        `json:"endDate,omitempty"`
	Version          int           `json:"version,omitempty"`
	Source           string        `json:"source,omitempty"`
}

type RegisteredEntry struct {
	Type             string        `json:"type,omitempty"`
	Descriptions     []Description `json:"descriptions,omitempty"`
	RegistrationDate string        `json:"registrationDate,omitempty"`
	EndDate          string        `json:"endDate,omitempty"`
	Register         string        `json:"register,omitempty"`
	Authority        string        `json:"authority,omitempty"`
}

type Address struct {
	Type             int          `json:"type,omitempty"`
	Street           string       `json:"street,omitempty"`
	PostCode         string       `json:"postCode,omitempty"`
	PostOffices      []PostOffice `json:"postOffices,omitempty"`
	BuildingNumber   string       `json:"buildingNumber,omitempty"`
	Entrance         string       `json:"entrance,omitempty"`
	ApartmentNumber  string       `json:"apartmentNumber,omitempty"`
	PostOfficeBox    string       `json:"postOfficeBox,omitempty"`
	CO               string       `json:"co,omitempty"`
	RegistrationDate string       `json:"registrationDate,omitempty"`
	Source           string       `json:"source,omitempty"`
}

type PostOffice struct {
	City             string `json:"city,omitempty"`
	LanguageCode     string `json:"languageCode,omitempty"`
	MunicipalityCode string `json:"municipalityCode,omitempty"`
}
```

Create `countrydata/finland/prhytj/mapping.go` with `CompanyProfile`, `TaxRegistrations`, `ToProfile`, current-name selection, language description selection, VAT derivation, and website normalization.

- [ ] **Step 5: Run mapping tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/countrydata
GOWORK=off go test ./finland/prhytj -run 'TestPageDecode|TestCeased|TestMultiple' -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/countrydata/finland/prhytj
git commit -m "feat: add finland prh ytj record mapping"
```

---

### Task 3: Paginated PRH Download To Local Snapshot

**Files:**
- Create: `countrydata/finland/prhytj/config.go`
- Create: `countrydata/finland/prhytj/source.go`
- Create: `countrydata/finland/prhytj/download.go`
- Test: `countrydata/finland/prhytj/download_test.go`

- [ ] **Step 1: Write failing download test**

Create `countrydata/finland/prhytj/download_test.go`:

```go
package prhytj

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

func TestDownloadWritesPaginatedNDJSONSnapshotAndMetadata(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page := r.URL.Query().Get("page")
		if page == "" || page == "1" {
			http.ServeFile(w, r, "testdata/prh_page_1.json")
			return
		}
		if page == "2" {
			http.ServeFile(w, r, "testdata/prh_page_2.json")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"companies":[]}`))
	}))
	defer server.Close()

	dataDir := t.TempDir()
	store := &recordingMetadataStore{}
	source := NewSource(Config{
		BaseURL: server.URL,
		DataDir: dataDir,
		HTTPClient: server.Client(),
		MetadataStore: store,
	})

	result, err := source.Download(context.Background(), countryimport.DownloadOptions{
		MaxPages: 3,
		PageDelay: time.Nanosecond,
	})
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	if result.PagesDownloaded != 2 || result.RecordsSeen != 3 {
		t.Fatalf("unexpected result: %#v", result)
	}
	assertFileExists(t, result.SnapshotPath)
	if count := countLines(t, result.SnapshotPath); count != 3 {
		t.Fatalf("expected 3 ndjson lines, got %d", count)
	}
	if got := fileSHA256(t, result.SnapshotPath); got != result.SHA256 {
		t.Fatalf("sha mismatch: result=%s file=%s", result.SHA256, got)
	}

	if store.savedDownload == nil {
		t.Fatal("expected download metadata to be saved")
	}
	if store.savedDownload.SourceSlug != SourceSlug || store.savedDownload.PagesDownloaded != 2 || store.savedDownload.RecordsSeen != 3 {
		t.Fatalf("metadata mismatch: %#v", store.savedDownload)
	}
	if store.savedDownload.SnapshotPath != result.SnapshotPath || store.savedDownload.SHA256 != result.SHA256 {
		t.Fatalf("saved metadata should match result: metadata=%#v result=%#v", store.savedDownload, result)
	}
}

type recordingMetadataStore struct {
	savedDownload *countryimport.DownloadMetadata
	savedProcess  *countryimport.ProcessMetadata
}

func (s *recordingMetadataStore) SaveDownload(ctx context.Context, metadata countryimport.DownloadMetadata) error {
	s.savedDownload = &metadata
	return nil
}

func (s *recordingMetadataStore) SaveProcess(ctx context.Context, metadata countryimport.ProcessMetadata) error {
	s.savedProcess = &metadata
	return nil
}

func assertFileExists(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected file %s: %v", path, err)
	}
}

func countLines(t *testing.T, path string) int {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var count int
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		count++
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return count
}

func fileSHA256(t *testing.T, path string) string {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/countrydata
GOWORK=off go test ./finland/prhytj -run TestDownloadWritesPaginatedNDJSONSnapshotAndMetadata -count=1
```

Expected: FAIL because production files for source construction and download have not been created.

- [ ] **Step 3: Implement config, source, and download**

Create `countrydata/finland/prhytj/config.go`:

```go
package prhytj

import (
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

type Config struct {
	BaseURL        string
	DataDir        string
	PageDelay      time.Duration
	RequestTimeout time.Duration
	UserAgent      string
	HTTPClient     *http.Client
	MetadataStore  countryimport.MetadataStore
}

func ConfigFromEnv() Config {
	dataDir := envOr("PRH_YTJ_DATA_DIR", "./data/countrydata/finland/prhytj")
	return Config{
		BaseURL:        envOr("PRH_YTJ_BASE_URL", DefaultBaseURL),
		DataDir:        dataDir,
		PageDelay:      envDurationMillis("PRH_YTJ_PAGE_DELAY_MS", countryimport.DefaultPageDelay),
		RequestTimeout: envDurationSeconds("PRH_YTJ_REQUEST_TIMEOUT_SECONDS", countryimport.DefaultRequestTimeout),
		UserAgent:      envOr("PRH_YTJ_USER_AGENT", countryimport.DefaultUserAgent),
	}
}

func envOr(key string, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func envDurationMillis(key string, fallback time.Duration) time.Duration {
	value, err := strconv.Atoi(strings.TrimSpace(os.Getenv(key)))
	if err != nil || value <= 0 {
		return fallback
	}
	return time.Duration(value) * time.Millisecond
}

func envDurationSeconds(key string, fallback time.Duration) time.Duration {
	value, err := strconv.Atoi(strings.TrimSpace(os.Getenv(key)))
	if err != nil || value <= 0 {
		return fallback
	}
	return time.Duration(value) * time.Second
}
```

Create `countrydata/finland/prhytj/source.go` with concrete state:

```go
package prhytj

import (
	"context"
	"net/http"

	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

type Source struct {
	cfg            Config
	httpClient     *http.Client
	metadataStore countryimport.MetadataStore
	latestDownload *countryimport.DownloadMetadata
	latestProcess  *countryimport.ProcessMetadata
	StoreFunc      func(context.Context, []CompanyRecord) (countryimport.StoreResult, error)
}

func NewSource(cfg Config) *Source {
	if cfg.BaseURL == "" {
		cfg.BaseURL = DefaultBaseURL
	}
	if cfg.DataDir == "" {
		cfg.DataDir = "./data/countrydata/finland/prhytj"
	}
	if cfg.PageDelay == 0 {
		cfg.PageDelay = countryimport.DefaultPageDelay
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = countryimport.DefaultRequestTimeout
	}
	if cfg.UserAgent == "" {
		cfg.UserAgent = countryimport.DefaultUserAgent
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: cfg.RequestTimeout}
	}
	metadataStore := cfg.MetadataStore
	if metadataStore == nil {
		metadataStore = countryimport.NoopMetadataStore{}
	}
	return &Source{cfg: cfg, httpClient: client, metadataStore: metadataStore}
}
```

Create `countrydata/finland/prhytj/download.go` implementing the paginated loop, temporary file, hash computation, and a private `saveDownloadMetadata(ctx, metadata)` helper that calls `s.metadataStore.SaveDownload`. Use `json.RawMessage` for each company so the snapshot preserves source payloads exactly as one compact JSON object per line.

- [ ] **Step 4: Run download test**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/countrydata
GOWORK=off go test ./finland/prhytj -run TestDownloadWritesPaginatedNDJSONSnapshotAndMetadata -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/countrydata
git commit -m "feat: download finland prh ytj snapshots"
```

---

### Task 4: Chunked Snapshot Processing And Store Boundary

**Files:**
- Create: `countrydata/finland/prhytj/process.go`
- Create: `countrydata/finland/prhytj/store.go`
- Create: `countrydata/finland/prhytj/testdata/prh_snapshot_mixed.ndjson`
- Test: `countrydata/finland/prhytj/process_test.go`

- [ ] **Step 1: Add mixed NDJSON fixture**

Create `countrydata/finland/prhytj/testdata/prh_snapshot_mixed.ndjson` using two valid compact records from `prh_page_1.json`, one invalid line, and one valid record from `prh_page_2.json`:

```jsonl
{"businessId":{"value":"0100130-4","registrationDate":"1978-03-15","source":"3"},"names":[{"name":"Dynava Oy","type":"1","registrationDate":"2022-01-21","version":1,"source":"1"}],"companySituations":[],"registeredEntries":[],"addresses":[],"tradeRegisterStatus":"1","status":"2","registrationDate":"1973-08-10"}
{"businessId":{"value":"0111111-1","registrationDate":"1978-03-15","source":"3"},"names":[{"name":"Ceased Example Oy","type":"1","registrationDate":"1990-01-01","source":"1"}],"companySituations":[],"registeredEntries":[],"addresses":[],"tradeRegisterStatus":"4","status":"2","registrationDate":"1990-01-01","endDate":"2002-02-02"}
{"businessId":
{"businessId":{"value":"0222222-2","registrationDate":"1978-03-15","source":"3"},"names":[{"name":"Newest Primary Oy","type":"1","registrationDate":"2024-01-01","source":"1"}],"companySituations":[],"registeredEntries":[],"addresses":[],"tradeRegisterStatus":"1","status":"2","registrationDate":"2020-01-01"}
```

- [ ] **Step 2: Write failing process tests**

Create `countrydata/finland/prhytj/process_test.go`:

```go
package prhytj

import (
	"context"
	"path/filepath"
	"testing"

	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

func TestProcessReadsSnapshotInChunksAndContinuesAfterBadLine(t *testing.T) {
	var chunkSizes []int
	source := NewSource(Config{DataDir: t.TempDir()})
	source.StoreFunc = func(ctx context.Context, records []CompanyRecord) (countryimport.StoreResult, error) {
		chunkSizes = append(chunkSizes, len(records))
		return countryimport.StoreResult{RecordsReceived: int64(len(records)), RecordsStored: int64(len(records))}, nil
	}

	result, err := source.Process(context.Background(), countryimport.ProcessOptions{
		SnapshotPath: filepath.Join("testdata", "prh_snapshot_mixed.ndjson"),
		ChunkSize: 2,
	})
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if result.RecordsSeen != 4 || result.RecordsProcessed != 3 || result.DecodeErrors != 1 || result.RecordsStored != 3 {
		t.Fatalf("unexpected result: %#v", result)
	}
	if len(chunkSizes) != 2 || chunkSizes[0] != 2 || chunkSizes[1] != 1 {
		t.Fatalf("unexpected chunk sizes: %#v", chunkSizes)
	}
}

func TestProcessWithoutSnapshotReturnsNoSnapshotKind(t *testing.T) {
	source := NewSource(Config{DataDir: t.TempDir()})

	_, err := source.Process(context.Background(), countryimport.ProcessOptions{})
	if err == nil {
		t.Fatal("expected error")
	}
	if !countryimport.IsKind(err, countryimport.ErrorKindNoSnapshot) {
		t.Fatalf("expected no snapshot kind, got %s: %v", countryimport.Classify(err), err)
	}
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/countrydata
GOWORK=off go test ./finland/prhytj -run 'TestProcess' -count=1
```

Expected: FAIL because production files for snapshot processing and chunk storage have not been created.

- [ ] **Step 4: Implement `Process`, `Store`, and latest snapshot lookup**

Create `countrydata/finland/prhytj/store.go`:

```go
package prhytj

import (
	"context"

	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

func (s *Source) Store(ctx context.Context, records []CompanyRecord) (countryimport.StoreResult, error) {
	if s != nil && s.StoreFunc != nil {
		return s.StoreFunc(ctx, records)
	}
	count := int64(len(records))
	return countryimport.StoreResult{RecordsReceived: count, RecordsStored: count}, nil
}
```

Create `countrydata/finland/prhytj/process.go` that:

- resolves `SnapshotPath` or the latest in-memory download metadata
- scans NDJSON with a large scanner buffer such as `32 * 1024 * 1024`
- increments `RecordsSeen` for every line
- wraps line decode failures as warnings with `slog.WarnContext`
- continues after bad lines
- calls `Store` at configured chunk size
- saves process metadata through a private `saveProcessMetadata(ctx, metadata)` helper that calls `s.metadataStore.SaveProcess`

- [ ] **Step 5: Run process tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/countrydata
GOWORK=off go test ./finland/prhytj -run 'TestProcess' -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/countrydata/finland/prhytj
git commit -m "feat: process finland prh ytj snapshots"
```

---

### Task 5: Standalone PRH Import CLI

**Files:**
- Create: `countrydata/cmd/prhytj-import/main.go`
- Test: `countrydata/cmd/prhytj-import/main_test.go`

- [ ] **Step 1: Write failing CLI argument tests**

Create `countrydata/cmd/prhytj-import/main_test.go`:

```go
package main

import "testing"

func TestParseArgsRunCommand(t *testing.T) {
	cfg, err := parseArgs([]string{"run", "--env", ".env", "--data-dir", "/tmp/prh", "--max-pages", "2", "--chunk-size", "25"})
	if err != nil {
		t.Fatalf("parse args: %v", err)
	}
	if cfg.command != "run" || cfg.envPath != ".env" || cfg.dataDir != "/tmp/prh" || cfg.maxPages != 2 || cfg.chunkSize != 25 {
		t.Fatalf("unexpected config: %#v", cfg)
	}
}

func TestParseArgsRejectsUnknownCommand(t *testing.T) {
	if _, err := parseArgs([]string{"unknown"}); err == nil {
		t.Fatal("expected unknown command error")
	}
}
```

- [ ] **Step 2: Run CLI tests to verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/countrydata
GOWORK=off go test ./cmd/prhytj-import -count=1
```

Expected: FAIL because CLI does not exist.

- [ ] **Step 3: Implement CLI**

Create `countrydata/cmd/prhytj-import/main.go`:

```go
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"

	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
	"github.com/pulsarpoint/corpscout/countrydata/finland/prhytj"
)

type cliConfig struct {
	command   string
	envPath   string
	dataDir   string
	maxPages  int
	chunkSize int
}

func main() {
	cfg, err := parseArgs(os.Args[1:])
	if err != nil {
		slog.Error("parse prh ytj import command", "error", err)
		os.Exit(2)
	}
	if err := run(context.Background(), cfg); err != nil {
		slog.Error("run prh ytj import command", "command", cfg.command, "error", err, "error_kind", countryimport.Classify(err))
		os.Exit(1)
	}
}

func parseArgs(args []string) (cliConfig, error) {
	if len(args) == 0 {
		return cliConfig{}, fmt.Errorf("command is required: download, process, or run")
	}
	cfg := cliConfig{command: args[0]}
	if cfg.command != "download" && cfg.command != "process" && cfg.command != "run" {
		return cliConfig{}, fmt.Errorf("unknown command %q", cfg.command)
	}
	fs := flag.NewFlagSet("prhytj-import "+cfg.command, flag.ContinueOnError)
	fs.StringVar(&cfg.envPath, "env", "", "optional .env file")
	fs.StringVar(&cfg.dataDir, "data-dir", "", "data directory")
	fs.IntVar(&cfg.maxPages, "max-pages", 0, "maximum pages to download")
	fs.IntVar(&cfg.chunkSize, "chunk-size", 0, "records per process chunk")
	if err := fs.Parse(args[1:]); err != nil {
		return cliConfig{}, err
	}
	return cfg, nil
}
```

Add `run` to load env file, construct `prhytj.NewSource(prhytj.ConfigFromEnv())`, override data dir from flags, and call `Download`, `Process`, or both.

- [ ] **Step 4: Run CLI tests and build**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/countrydata
GOWORK=off go test ./cmd/prhytj-import -count=1
GOWORK=off go build ./cmd/prhytj-import
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/countrydata/cmd/prhytj-import
git commit -m "feat: add prh ytj import cli"
```

---

### Task 6: Live PRH Integration Tests

**Files:**
- Create: `countrydata/finland/prhytj/live_integration_test.go`

- [ ] **Step 1: Add gated live integration tests**

Create `countrydata/finland/prhytj/live_integration_test.go`:

```go
//go:build integration

package prhytj

	import (
		"context"
		"os"
		"testing"
		"time"

	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

func TestLivePRHYTJSmoke(t *testing.T) {
	if os.Getenv("COUNTRYDATA_PRH_YTJ_LIVE") != "1" {
		t.Skip("set COUNTRYDATA_PRH_YTJ_LIVE=1 to run live PRH smoke test")
	}
	dataDir := t.TempDir()
	source := NewSource(Config{
		BaseURL: DefaultBaseURL,
		DataDir: dataDir,
	})

	download, err := source.Download(context.Background(), countryimport.DownloadOptions{
		MaxPages: 2,
		PageDelay: 250 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("live smoke download: %v", err)
	}
	process, err := source.Process(context.Background(), countryimport.ProcessOptions{
		SnapshotPath: download.SnapshotPath,
		ChunkSize: 100,
	})
	if err != nil {
		t.Fatalf("live smoke process: %v", err)
	}
	if process.RecordsProcessed == 0 {
		t.Fatalf("expected processed records, got %#v", process)
	}
	t.Logf("live smoke pages=%d records=%d decode_errors=%d sha256=%s", download.PagesDownloaded, process.RecordsProcessed, process.DecodeErrors, download.SHA256)
}

func TestLivePRHYTJFullDataset(t *testing.T) {
	if os.Getenv("COUNTRYDATA_PRH_YTJ_LIVE_FULL") != "1" {
		t.Skip("set COUNTRYDATA_PRH_YTJ_LIVE_FULL=1 to run full live PRH dataset test")
	}
	dataDir := t.TempDir()
	source := NewSource(Config{
		BaseURL: DefaultBaseURL,
		DataDir: dataDir,
	})
	download, err := source.Download(context.Background(), countryimport.DownloadOptions{
		PageDelay: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("full live download: %v", err)
	}
	process, err := source.Process(context.Background(), countryimport.ProcessOptions{
		SnapshotPath: download.SnapshotPath,
		ChunkSize: 500,
	})
	if err != nil {
		t.Fatalf("full live process: %v", err)
	}
	t.Logf("full live pages=%d records=%d bytes=%d decode_errors=%d sha256=%s", download.PagesDownloaded, process.RecordsProcessed, download.BytesDownloaded, process.DecodeErrors, download.SHA256)
}
```

- [ ] **Step 2: Verify integration tests are skipped by default**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/countrydata
GOWORK=off go test -tags=integration ./finland/prhytj/... -run TestLivePRHYTJ -count=1
```

Expected: PASS with skipped tests when environment variables are not set.

- [ ] **Step 3: Run live smoke test manually**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/countrydata
COUNTRYDATA_PRH_YTJ_LIVE=1 GOWORK=off go test -tags=integration ./finland/prhytj/... -run TestLivePRHYTJSmoke -count=1 -v
```

Expected: PASS against the real PRH API. If this finds a real decode issue, capture the smallest representative record/page into `testdata` and add a regression assertion before continuing.

- [ ] **Step 4: Document full live test command in a package README**

Create `countrydata/finland/prhytj/README.md`:

```markdown
# Finland PRH YTJ v3 Import

Default tests use captured real-shaped fixtures and local HTTP servers.

Run a live smoke test:

```bash
COUNTRYDATA_PRH_YTJ_LIVE=1 GOWORK=off go test -tags=integration ./finland/prhytj/... -run TestLivePRHYTJSmoke -count=1 -v
```

Run the full live dataset test manually:

```bash
COUNTRYDATA_PRH_YTJ_LIVE_FULL=1 GOWORK=off go test -tags=integration ./finland/prhytj/... -run TestLivePRHYTJFullDataset -count=1 -v
```
```

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/countrydata/finland/prhytj
git commit -m "test: add live prh ytj integration coverage"
```

---

### Task 7: Thin Scheduler Adapter

**Files:**
- Modify: `scheduler/go.mod`
- Create: `scheduler/internal/countrydata/finland_prhytj.go`
- Test: `scheduler/internal/countrydata/finland_prhytj_test.go`

- [ ] **Step 1: Add scheduler adapter test**

Create `scheduler/internal/countrydata/finland_prhytj_test.go`:

```go
package countrydata

	import (
		"context"
		"net/http"
		"net/http/httptest"
		"testing"
		"time"
)

func TestFinlandPRHYTJImporterRunUsesSharedSourceMethods(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("page") == "2" {
			_, _ = w.Write([]byte(`{"companies":[]}`))
			return
		}
		_, _ = w.Write([]byte(`{"totalResults":1,"companies":[{"businessId":{"value":"0100130-4"},"names":[{"name":"Dynava Oy","type":"1","registrationDate":"2022-01-21"}],"companySituations":[],"registeredEntries":[],"addresses":[],"tradeRegisterStatus":"1","status":"2"}]}`))
	}))
	defer server.Close()

	dataDir := t.TempDir()
	importer := FinlandPRHYTJImporter{HTTPClient: server.Client()}
		result, err := importer.Run(context.Background(), FinlandPRHYTJImportInput{
			BaseURL: server.URL,
			DataDir: dataDir,
			MaxPages: 2,
			ChunkSize: 1,
			PageDelay: time.Nanosecond,
	})
	if err != nil {
		t.Fatalf("run importer: %v", err)
	}
	if result.Download.RecordsSeen != 1 || result.Process.RecordsProcessed != 1 {
		t.Fatalf("unexpected result: %#v", result)
	}
}
```

- [ ] **Step 2: Run adapter test to verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/countrydata -count=1
```

Expected: FAIL because adapter package and module dependency do not exist.

- [ ] **Step 3: Add local module dependency and adapter**

Modify `scheduler/go.mod`:

```go
require github.com/pulsarpoint/corpscout/countrydata v0.0.0

replace github.com/pulsarpoint/corpscout/countrydata => ../countrydata
```

Create `scheduler/internal/countrydata/finland_prhytj.go`:

```go
package countrydata

import (
	"context"
	"net/http"
	"time"

	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
	"github.com/pulsarpoint/corpscout/countrydata/finland/prhytj"
)

type FinlandPRHYTJImporter struct {
	HTTPClient *http.Client
}

type FinlandPRHYTJImportInput struct {
	BaseURL       string
	DataDir       string
	MaxPages      int
	ChunkSize     int
	PageDelay     time.Duration
	MetadataStore countryimport.MetadataStore
}

type FinlandPRHYTJImportResult struct {
	Download countryimport.DownloadResult
	Process  countryimport.ProcessResult
}

func (i FinlandPRHYTJImporter) Run(ctx context.Context, input FinlandPRHYTJImportInput) (FinlandPRHYTJImportResult, error) {
	source := prhytj.NewSource(prhytj.Config{
		BaseURL:       input.BaseURL,
		DataDir:       input.DataDir,
		PageDelay:     input.PageDelay,
		HTTPClient:    i.HTTPClient,
		MetadataStore: input.MetadataStore,
	})
	download, err := source.Download(ctx, countryimport.DownloadOptions{
		MaxPages:  input.MaxPages,
		PageDelay: input.PageDelay,
	})
	if err != nil {
		return FinlandPRHYTJImportResult{}, err
	}
	process, err := source.Process(ctx, countryimport.ProcessOptions{
		SnapshotPath: download.SnapshotPath,
		ChunkSize:    input.ChunkSize,
	})
	if err != nil {
		return FinlandPRHYTJImportResult{Download: download}, err
	}
	return FinlandPRHYTJImportResult{Download: download, Process: process}, nil
}
```

- [ ] **Step 4: Run scheduler adapter tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go mod tidy
GOWORK=off go test ./internal/countrydata -count=1
```

Expected: PASS.

- [ ] **Step 5: Run full verification**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/countrydata
GOWORK=off go test ./... -count=1

cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/countrydata -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/countrydata corpscout/scheduler/go.mod corpscout/scheduler/go.sum corpscout/scheduler/internal/countrydata
git commit -m "feat: add scheduler adapter for finland prh ytj import"
```

---

## Plan Self-Review

Spec coverage:

- Standalone countrydata module: Tasks 1-6.
- Shared public methods and source-independent option/result shape: Task 1.
- Finland PRH YTJ v3 only: Tasks 2-6.
- Paginated API as bulk snapshot: Task 3.
- Hash, size, duration, page count, record count, and metadata-store persistence: Tasks 1 and 3.
- Process latest snapshot in chunks and continue after bad lines: Task 4.
- Store no-op without DB: Task 4.
- Fixture tests based on real-shaped data: Task 2.
- Full local flow tests through HTTP server: Task 3 plus Task 4.
- Live smoke and full real remote tests: Task 6.
- Thin scheduler adapter outside source logic: Task 7.
- Logging and error handling rules: Tasks 1, 4, 5, and 7.

Placeholder scan:

- The plan contains no prohibited placeholder phrases.
- Every task includes concrete file paths, commands, expected outcomes, and code examples for the implementation worker.

Type consistency:

- Shared package import alias is consistently `countryimport`.
- Finland source slug is consistently `finland_prh_ytj_v3`.
- Public methods are consistently `Download`, `Process`, and `Store`; metadata saving is private to the source.
