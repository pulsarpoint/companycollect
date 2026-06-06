---
name: company-open-data-discovery
description: Use when investigating official or reliable public company registry data sources for a country, including APIs, open data portals, bulk downloads, licenses, source inventories, and separated analysis/data artifacts.
---

# Company Open Data Discovery Skill

## Purpose

Use this skill when the user asks to investigate how to pull company information for a specific country from public/open internet data sources.

The goal is to discover official or reliable sources for company data, document exactly how the search was performed, download available bulk data where legally allowed, collect API samples where APIs exist, and leave a reproducible investigation trail in a country-specific folder.

This skill is designed for a project folder like:

```text
/Users/graovic/pulsarpoint/ppoint/companycollect/companies/analysis/{country_name}
/Users/graovic/pulsarpoint/ppoint/companycollect/companies/data/{country_name}
```

Where `{country_name}` should be a safe folder slug, for example:

```text
serbia
norway
united_kingdom
germany
```

## When to use this skill

Use this skill when the task involves any of the following:

- Finding company registry data for a country
- Finding business register APIs
- Finding open data portals with company/legal entity datasets
- Downloading company bulk datasets such as CSV, JSON, XML, RDF, ZIP, XLSX, or database dumps
- Investigating VAT, tax, procurement, beneficial ownership, statistical, or official gazette datasets related to companies
- Producing a source inventory for later ingestion into Postgres, ClickHouse, DuckDB, or another data pipeline

Do not use this skill for private, leaked, credential-protected, CAPTCHA-protected, paid-only, or non-public datasets unless the user explicitly provides lawful access instructions.

## Required input

The user should provide at least:

```text
country_name: Serbia
```

Useful optional inputs:

```text
country_code: RS
language_preferences: Serbian, English
preferred_output_format: CSV/JSON/Postgres-ready
max_download_size_mb: 500
```

If optional inputs are missing, make reasonable defaults:

- Use the English country name and likely local-language names.
- Prefer official government sources first.
- Download metadata and small samples if the full dataset is very large.
- Do not invent data availability.

## Output directory structure

For every country investigation, create this folder structure:

```text
/Users/graovic/pulsarpoint/ppoint/companycollect/companies/analysis/{country_name}/
  README.md
  investigation.md
  search_attempts.md
  source_inventory.json
  source_inventory.md
  license_notes.md
  schema_notes.md
  scripts/
    downloader.go
    sources.example.json

/Users/graovic/pulsarpoint/ppoint/companycollect/companies/data/{country_name}/
  run.log
  raw/
    bulk/
    api/
    pages/
    samples/

  normalized/
    companies.sample.jsonl
    companies.sample.csv
```

The `analysis/` tree should contain tracked investigation notes and reproducible source definitions. The `data/` tree should contain downloaded artifacts, raw API responses, normalized samples, metadata JSON, and run logs. The full `companies/data/` directory can be added to `.gitignore`.

### File purpose

#### `README.md`

High-level summary for the country:

```markdown
# Company data sources for {Country}

## Status

- Official bulk data: found / not found / unclear
- Official API: found / not found / unclear
- Open data portal: found / not found / unclear
- License: known / unknown / restricted
- Recommended ingestion path: bulk / API / scrape not recommended / manual review needed

## Best source

Short explanation of the best source and why.

## Next action

Concrete next step for implementation.
```

#### `investigation.md`

Narrative report explaining what was found, what was not found, and what should be used.

#### `search_attempts.md`

A chronological log of all searches attempted.

Each entry must include:

```markdown
## Attempt {N}

- Date/time:
- Search engine or source:
- Query:
- Language:
- Why this query was tried:
- Top relevant URLs:
- Result:
- Decision:
```

#### `source_inventory.json`

Machine-readable inventory of all candidate sources.

Example:

```json
[
  {
    "country": "Serbia",
    "country_slug": "serbia",
    "source_name": "Example Business Register",
    "source_type": "official_registry",
    "organization": "Example Government Agency",
    "url": "https://example.gov/register",
    "bulk_download_url": "https://example.gov/register/companies.csv.zip",
    "api_base_url": null,
    "formats": ["csv", "zip"],
    "license": "unknown",
    "access": "public",
    "requires_authentication": false,
    "requires_payment": false,
    "rate_limits": null,
    "fields_observed": ["company_id", "name", "status", "address"],
    "data_freshness": "unknown",
    "downloaded_files": ["raw/bulk/example-companies.csv.zip"],
    "notes": "Official-looking source, but license must be confirmed."
  }
]
```

`downloaded_files` paths are relative to `/Users/graovic/pulsarpoint/ppoint/companycollect/companies/data/{country_name}` unless explicitly marked otherwise.

#### `license_notes.md`

Record license, terms of use, redistribution restrictions, attribution requirements, and uncertainty.

Never assume that public means freely reusable.

#### `schema_notes.md`

Describe observed fields, identifiers, date formats, encodings, and possible mapping to an internal company model.

Example internal model:

```text
company_id
registration_number
tax_id
vat_id
legal_name
normalized_name
company_type
status
incorporation_date
dissolution_date
registered_address
municipality
region
country
source_url
source_name
source_retrieved_at
raw_record
```

## Required research process

### 1. Prepare the country folders

Create the folder before downloading anything:

```bash
mkdir -p /Users/graovic/pulsarpoint/ppoint/companycollect/companies/analysis/{country_name}/scripts
mkdir -p /Users/graovic/pulsarpoint/ppoint/companycollect/companies/data/{country_name}/{raw/bulk,raw/api,raw/pages,raw/samples,normalized}
```

Use a safe slug for `{country_name}`:

- lowercase
- ASCII where practical
- spaces replaced with `_`
- no shell-special characters

Examples:

```text
Bosnia and Herzegovina -> bosnia_and_herzegovina
United Kingdom -> united_kingdom
Côte d'Ivoire -> cote_d_ivoire
```

### 2. Identify local terms

Before searching deeply, identify the country’s local-language terms for:

```text
company register
business register
legal entities register
open data
API
bulk download
CSV
XML
VAT register
tax identification number
beneficial ownership register
statistical business register
official gazette
procurement register
```

For each country, search in English and in the country’s official/local languages where possible.

### 3. Search official sources first

Prioritize sources in this order:

1. Official national business/company registry
2. Government open data portal
3. Statistical office business register
4. Tax/VAT lookup or VAT register
5. Beneficial ownership register
6. Public procurement supplier/company database
7. Official gazette/legal announcements
8. EU/international official sources, where relevant
9. Reliable third-party aggregators only as fallback or comparison

### 4. Required search query templates

Run and record searches like these.

Replace `{country}` and `{local_term}` with actual values.

```text
{country} company register API
{country} business register bulk download
{country} companies open data CSV
{country} legal entities register XML
{country} company registry dataset
{country} business registry open data
{country} VAT register API
{country} statistical business register open data
{country} beneficial ownership register API
site:gov {country} company register API
site:gov {country} business register CSV
site:data.gov {country} companies
site:*.gov.* company register {country}
```

Local-language examples:

```text
{local_term_for_company_register} API
{local_term_for_business_register} CSV
{local_term_for_legal_entities} open data
{local_term_for_company_register} preuzimanje podataka
{local_term_for_company_register} download XML
```

Also search exact file formats:

```text
{country} company register filetype:csv
{country} company register filetype:xml
{country} company register filetype:json
{country} company register filetype:xlsx
{country} company register filetype:zip
```

### 5. Inspect source pages

For each promising source, capture:

```text
source name
publisher/organization
official/non-official
URL
available formats
bulk download availability
API availability
authentication requirements
payment requirements
rate limits
license/terms
update frequency
field list
sample record
```

Save relevant HTML pages or API documentation pages under:

```text
/Users/graovic/pulsarpoint/ppoint/companycollect/companies/data/{country_name}/raw/pages/
```

### 6. Decide source status

Classify each candidate source as one of:

```text
recommended
useful_secondary_source
sample_only
blocked_by_authentication
blocked_by_payment
blocked_by_license_uncertainty
not_company_data
not_relevant
unavailable
```

Use `recommended` only when:

- the source is official or highly reliable
- data access is technically possible
- legal/terms status is acceptable or at least not obviously restrictive
- the data contains useful company identifiers or names

### 7. Download bulk data when available

When a public bulk file exists and downloading is allowed:

- Save it under `/Users/graovic/pulsarpoint/ppoint/companycollect/companies/data/{country_name}/raw/bulk/`
- Preserve the original filename where possible
- Save response metadata
- Record source URL and retrieval timestamp
- Do not modify raw files
- If compressed, keep the original archive and optionally extract a copy

Recommended metadata file per download:

```json
{
  "source_url": "https://example.gov/data/companies.zip",
  "retrieved_at": "2026-06-06T12:00:00Z",
  "http_status": 200,
  "content_type": "application/zip",
  "content_length": 12345678,
  "saved_as": "raw/bulk/companies.zip",
  "sha256": "..."
}
```

### 8. Pull API data when API exists

When an API exists:

- Save API documentation page or OpenAPI spec if available
- Save at least one sample request and response
- If pagination is clear, download a small bounded sample first
- Do not crawl unbounded APIs without a limit
- Respect rate limits
- Save raw API responses under `/Users/graovic/pulsarpoint/ppoint/companycollect/companies/data/{country_name}/raw/api/`

Recommended naming:

```text
/Users/graovic/pulsarpoint/ppoint/companycollect/companies/data/{country_name}/raw/api/{source_slug}_page_1.json
/Users/graovic/pulsarpoint/ppoint/companycollect/companies/data/{country_name}/raw/api/{source_slug}_page_2.json
/Users/graovic/pulsarpoint/ppoint/companycollect/companies/data/{country_name}/raw/api/{source_slug}_sample_company.json
```

### 9. Create normalized sample

Create a small normalized sample only after raw data is saved.

Write to:

```text
/Users/graovic/pulsarpoint/ppoint/companycollect/companies/data/{country_name}/normalized/companies.sample.jsonl
/Users/graovic/pulsarpoint/ppoint/companycollect/companies/data/{country_name}/normalized/companies.sample.csv
```

Each normalized record should preserve the raw source reference:

```json
{
  "company_id": "12345678",
  "registration_number": "12345678",
  "tax_id": null,
  "legal_name": "Example Company Ltd",
  "status": "active",
  "registered_address": "Example Street 1",
  "country": "Serbia",
  "source_name": "Example Business Register",
  "source_url": "https://example.gov/register",
  "source_retrieved_at": "2026-06-06T12:00:00Z",
  "raw_record": {}
}
```

### 10. Final answer format

When this skill is used, the final answer to the user must include:

```markdown
## Summary

Short country-specific conclusion.

## Best sources found

Table with source name, type, access method, format, license, and recommendation.

## What I tried

Summarized search strategy and important queries.

## Data saved

List files saved under both roots:

- Analysis: /Users/graovic/pulsarpoint/ppoint/companycollect/companies/analysis/{country_name}
- Data: /Users/graovic/pulsarpoint/ppoint/companycollect/companies/data/{country_name}

## Recommended ingestion approach

Explain whether to use bulk download, API polling, hybrid mode, or manual review.

## Open questions / risks

Mention license uncertainty, missing fields, rate limits, or unreliable data.
```

## Important rules

- Prefer official sources over aggregators.
- Never claim a source is official unless the publisher is clearly official.
- Never claim bulk/API access exists unless a working URL or documentation was found.
- Always record failed searches, not only successful ones.
- Always save raw data before transforming it.
- Always preserve source URL and retrieval timestamp.
- Always check license/terms and record uncertainty.
- Do not bypass authentication, payment, CAPTCHA, robots restrictions, or access controls.
- Do not scrape aggressively.
- Do not download extremely large files without noting size and confirming that it is reasonable for the environment.
- If no good source is found, say that clearly and document what was tried.

## Suggested `sources.example.json`

Create this file under:

```text
/Users/graovic/pulsarpoint/ppoint/companycollect/companies/analysis/{country_name}/scripts/sources.example.json
```

Example:

```json
[
  {
    "name": "Example bulk company register",
    "slug": "example_bulk_register",
    "type": "bulk",
    "url": "https://example.gov/data/companies.zip",
    "method": "GET",
    "headers": {},
    "max_pages": 0
  },
  {
    "name": "Example company API",
    "slug": "example_company_api",
    "type": "api_page",
    "url": "https://api.example.gov/companies",
    "method": "GET",
    "headers": {
      "Accept": "application/json"
    },
    "page_param": "page",
    "page_start": 1,
    "max_pages": 3
  }
]
```

## Small Go downloader/API collector

Create this file under:

```text
/Users/graovic/pulsarpoint/ppoint/companycollect/companies/analysis/{country_name}/scripts/downloader.go
```

This is intentionally simple. It supports:

- direct bulk download
- basic page-number API download
- metadata file creation
- SHA-256 hashing
- safe output under the country data folder

```go
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Source struct {
	Name      string            `json:"name"`
	Slug      string            `json:"slug"`
	Type      string            `json:"type"` // "bulk" or "api_page"
	URL       string            `json:"url"`
	Method    string            `json:"method"`
	Headers   map[string]string `json:"headers"`
	PageParam string            `json:"page_param"`
	PageStart int               `json:"page_start"`
	MaxPages  int               `json:"max_pages"`
}

type DownloadMeta struct {
	SourceName    string    `json:"source_name"`
	SourceURL     string    `json:"source_url"`
	RetrievedAt   time.Time `json:"retrieved_at"`
	HTTPStatus    int       `json:"http_status"`
	ContentType   string    `json:"content_type"`
	ContentLength int64     `json:"content_length"`
	SavedAs       string    `json:"saved_as"`
	SHA256        string    `json:"sha256"`
}

func CollectSource(ctx context.Context, client *http.Client, dataRoot string, src Source) error {
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}

	if src.Slug == "" {
		src.Slug = safeSlug(src.Name)
	}
	if src.Method == "" {
		src.Method = http.MethodGet
	}

	switch src.Type {
	case "bulk":
		out := filepath.Join(dataRoot, "raw", "bulk", src.Slug+extensionFromURL(src.URL))
		return downloadOne(ctx, client, src, src.URL, out)

	case "api_page":
		if src.PageParam == "" {
			return errors.New("api_page source requires page_param")
		}
		if src.PageStart == 0 {
			src.PageStart = 1
		}
		if src.MaxPages <= 0 {
			src.MaxPages = 1
		}

		for page := src.PageStart; page < src.PageStart+src.MaxPages; page++ {
			u, err := addQueryParam(src.URL, src.PageParam, fmt.Sprintf("%d", page))
			if err != nil {
				return err
			}
			out := filepath.Join(dataRoot, "raw", "api", fmt.Sprintf("%s_page_%d.json", src.Slug, page))
			if err := downloadOne(ctx, client, src, u, out); err != nil {
				return err
			}
			time.Sleep(500 * time.Millisecond)
		}
		return nil

	default:
		return fmt.Errorf("unsupported source type %q", src.Type)
	}
}

func downloadOne(ctx context.Context, client *http.Client, src Source, sourceURL string, outPath string) error {
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, src.Method, sourceURL, nil)
	if err != nil {
		return err
	}
	for k, v := range src.Headers {
		req.Header.Set(k, v)
	}
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", "companycollect/0.1 open-data-research")
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("download failed: %s returned HTTP %d", sourceURL, resp.StatusCode)
	}

	file, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer file.Close()

	h := sha256.New()
	written, err := io.Copy(io.MultiWriter(file, h), resp.Body)
	if err != nil {
		return err
	}

	meta := DownloadMeta{
		SourceName:    src.Name,
		SourceURL:     sourceURL,
		RetrievedAt:   time.Now().UTC(),
		HTTPStatus:    resp.StatusCode,
		ContentType:   resp.Header.Get("Content-Type"),
		ContentLength: written,
		SavedAs:       outPath,
		SHA256:        hex.EncodeToString(h.Sum(nil)),
	}

	metaBytes, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(outPath+".metadata.json", metaBytes, 0o644)
}

func addQueryParam(rawURL, key, value string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set(key, value)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func extensionFromURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ".dat"
	}
	ext := filepath.Ext(u.Path)
	if ext == "" || len(ext) > 10 {
		return ".dat"
	}
	return ext
}

func safeSlug(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	var b strings.Builder
	lastUnderscore := false
	for _, r := range s {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if ok {
			b.WriteRune(r)
			lastUnderscore = false
			continue
		}
		if !lastUnderscore {
			b.WriteByte('_')
			lastUnderscore = true
		}
	}
	return strings.Trim(b.String(), "_")
}
```

Example usage from another Go file:

```go
ctx := context.Background()
client := &http.Client{Timeout: 2 * time.Minute}

dataRoot := "/Users/graovic/pulsarpoint/ppoint/companycollect/companies/data/serbia"

src := Source{
    Name: "Example bulk company register",
    Slug: "example_bulk_register",
    Type: "bulk",
    URL:  "https://example.gov/data/companies.zip",
}

if err := CollectSource(ctx, client, dataRoot, src); err != nil {
    panic(err)
}
```

For API pagination:

```go
src := Source{
    Name:      "Example company API",
    Slug:      "example_company_api",
    Type:      "api_page",
    URL:       "https://api.example.gov/companies",
    PageParam: "page",
    PageStart: 1,
    MaxPages:  5,
    Headers: map[string]string{
        "Accept": "application/json",
    },
}

if err := CollectSource(ctx, client, dataRoot, src); err != nil {
    panic(err)
}
```

## Optional CLI wrapper

If a CLI is needed later, create a small command that:

1. Reads `/Users/graovic/pulsarpoint/ppoint/companycollect/companies/analysis/{country_name}/scripts/sources.example.json`
2. Loops through sources
3. Calls `CollectSource`
4. Writes `/Users/graovic/pulsarpoint/ppoint/companycollect/companies/data/{country_name}/run.log`
5. Updates `/Users/graovic/pulsarpoint/ppoint/companycollect/companies/analysis/{country_name}/source_inventory.json`

Suggested command shape:

```bash
go run ./scripts/downloader.go \
  --analysis-root /Users/graovic/pulsarpoint/ppoint/companycollect/companies/analysis/serbia \
  --data-root /Users/graovic/pulsarpoint/ppoint/companycollect/companies/data/serbia \
  --sources /Users/graovic/pulsarpoint/ppoint/companycollect/companies/analysis/serbia/scripts/sources.example.json
```

## Quality checklist

Before finishing, verify:

- [ ] Country analysis folder exists
- [ ] Country data folder exists
- [ ] Search attempts are documented
- [ ] Official sources were checked first
- [ ] Local-language searches were attempted
- [ ] Candidate sources are classified
- [ ] License/terms are recorded
- [ ] Bulk files are downloaded if available and allowed
- [ ] API samples are downloaded if available and allowed
- [ ] Raw files are never overwritten without reason
- [ ] Metadata JSON exists for downloads
- [ ] SHA-256 hashes are recorded
- [ ] Normalized sample exists when practical
- [ ] Final recommendation is clear
- [ ] Unknowns are explicitly marked as unknown
