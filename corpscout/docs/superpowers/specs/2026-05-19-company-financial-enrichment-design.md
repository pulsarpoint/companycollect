# Company Financial Enrichment Design

## Goal

Extend the company model with employee count, revenue, and profit data; normalise all revenue to USD; translate non-English metadata to English via a local LLM; add indexed scalar columns for efficient filtering; surface data-source capabilities so the UI can show which sources can fill missing fields for a given company.

## Architecture

Enrichment data flows from external data sources (Brreg Regnskapsregisteret today, others later) through a River background job into the `companies` table. A small `llm` package handles translation and a small `fxrates` package handles currency conversion. The scheduler's existing source-trigger machinery drives the enrichment jobs. The UI gains a filterable company list and a per-company "fill from source" panel.

## Tech Stack

Go 1.24+, PostgreSQL, River job queue, sqlc, React/TypeScript, shadcn/ui, OpenAI-compatible LLM API (Qwen3 6B at `http://100.77.62.33:8080`), ECB FX feed.

---

## Section 1: Data Model

### companies table additions

```sql
-- Financial estimates (rich JSONB context)
ALTER TABLE companies
  ADD COLUMN IF NOT EXISTS employee_estimate  JSONB,
  ADD COLUMN IF NOT EXISTS revenue_estimate   JSONB,
  ADD COLUMN IF NOT EXISTS profit_estimate    JSONB;

-- Scalar columns for efficient filtering/sorting
ALTER TABLE companies
  ADD COLUMN IF NOT EXISTS employee_count       INT,      -- extracted from employee_estimate.value
  ADD COLUMN IF NOT EXISTS revenue_usd          BIGINT,   -- always USD cents
  ADD COLUMN IF NOT EXISTS revenue_orig_amount  BIGINT,   -- original currency amount
  ADD COLUMN IF NOT EXISTS revenue_orig_currency TEXT,    -- ISO 4217, e.g. "NOK"
  ADD COLUMN IF NOT EXISTS profit_usd           BIGINT;   -- always USD cents

CREATE INDEX IF NOT EXISTS idx_companies_employee_count ON companies (employee_count)
  WHERE employee_count IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_companies_revenue_usd ON companies (revenue_usd)
  WHERE revenue_usd IS NOT NULL;
```

### JSONB estimate schema (all three fields share this shape)

```json
{
  "value":    500000000,
  "currency": "NOK",
  "year":     2023,
  "source":   "brreg_regnskap",
  "label":    "Operating revenue",
  "min":      null,
  "max":      null
}
```

`label` is always English (translated before storage). `value` is always in the original currency for the JSONB; the scalar `revenue_usd` / `profit_usd` columns hold the USD-normalised figure.

### data_sources table addition

```sql
ALTER TABLE data_sources
  ADD COLUMN IF NOT EXISTS capabilities TEXT[] NOT NULL DEFAULT '{}';
```

Example values per source:
- `brreg`: `'{employee_count, revenue, profit, company_name, org_number}'`
- `gleif`: `'{company_name, lei, legal_form, status, locations}'`
- `companies_house`: `'{employee_count, company_name, status, directors}'`

---

## Section 2: LLM Package (`scheduler/internal/llm`)

Single file, no external SDK, plain `net/http`.

```go
type Client struct {
    baseURL string  // e.g. http://100.77.62.33:8080
    model   string  // e.g. qwen3:6b
    http    *http.Client
}

func NewClient(baseURL, model string) *Client
func (c *Client) Complete(ctx context.Context, system, user string) (string, error)
func (c *Client) Translate(ctx context.Context, text string) (string, error)
```

Uses `POST /v1/chat/completions` (OpenAI-compatible):

```json
{
  "model": "qwen3:6b",
  "messages": [
    {"role": "system", "content": "..."},
    {"role": "user",   "content": "..."}
  ]
}
```

`Translate` calls `Complete` with system prompt:
> "Translate the following text to English. Return only the translated text, no explanations, no quotes."

Config via env vars `LLM_BASE_URL` (default `http://100.77.62.33:8080`) and `LLM_MODEL` (default `qwen3:6b`). No API key. No retries — callers decide on error strategy.

### Translation helper

```go
// maybeTranslate returns text unchanged if it is already ASCII-dominant (>80% ASCII runes).
// Otherwise calls the LLM to translate.
func maybeTranslate(ctx context.Context, c *llm.Client, text string) string
```

Fields translated before storage:
- `company_suggestions.proposed_display_name`
- Each element of `companies.industry_labels TEXT[]`
- `*.label` inside estimate JSONB fields (e.g. "Driftsinntekter" → "Operating revenue")

---

## Section 3: FX Rates Package (`scheduler/internal/fxrates`)

```go
type Rates struct { ... }

func Load(ctx context.Context) (*Rates, error)        // fetches ECB daily XML
func (r *Rates) ToUSD(amount int64, currency string) (int64, error)
```

Source: `https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml` — free, no key, updated each business day. Rates cached in memory for 24 h. All currencies are expressed relative to EUR in the ECB feed; USD conversion is EUR→USD then source-currency→EUR.

---

## Section 4: Brreg Financial Enrichment

### API

`GET https://data.brreg.no/regnskapsregisteret/regnskap/{orgNumber}` returns an array of annual accounts. The most recent year's `sumDriftsinntekter` (operating revenue) and `ordinaertResultatForSkattekostnad` (pre-tax profit) are extracted.

### River job

New job kind `enrich_company_financials`:

```go
type EnrichCompanyFinancialsArgs struct {
    CompanyID  string
    OrgNumber  string
    SourceName string  // "brreg"
}
```

Job steps:
1. Fetch accounts from Brreg Regnskapsregisteret
2. Extract revenue + profit figures and currency (NOK)
3. Convert to USD via `fxrates.ToUSD`
4. Translate labels via `llm.Translate`
5. Call `UpdateCompanyEnrichment` sqlc query (already generated, currently unused)

### sqlc query (already exists — `UpdateCompanyEnrichment`)

Will be wired up in the job worker. Parameters include all five columns: `employee_estimate`, `revenue_estimate`, `profit_estimate`, `employee_count`, `revenue_usd`.

---

## Section 5: Backend Endpoints

### PATCH /api/v1/companies/:id/financials

Manual enrichment trigger from the UI. Body:

```json
{
  "employee_count": 120,
  "revenue_usd": 5000000,
  "revenue_orig_amount": 52000000,
  "revenue_orig_currency": "NOK",
  "profit_usd": 800000,
  "source": "manual"
}
```

Validates ranges (employee_count ≥ 0, revenue ≥ 0), writes via `UpdateCompanyEnrichment`, returns updated company.

### GET /api/v1/companies — filter extensions

New query params added to existing endpoint:
- `min_employees=N` — `employee_count >= N`
- `max_employees=N` — `employee_count <= N`
- `min_revenue_usd=N` — `revenue_usd >= N`
- `max_revenue_usd=N` — `revenue_usd <= N`

### GET /api/v1/companies/:id/enrichment-sources

Returns data sources applicable for the company's country whose `capabilities` overlap with the company's missing fields.

```json
{
  "missing_fields": ["revenue", "profit"],
  "sources": [
    {
      "name": "brreg",
      "label": "Brønnøysund Register Centre",
      "can_provide": ["revenue", "profit", "employee_count"]
    }
  ]
}
```

### POST /api/v1/companies/:id/enrich-from-source

Body: `{"source": "brreg"}`. Enqueues `enrich_company_financials` River job. Returns `{"job_id": N}`.

---

## Section 6: Data Source Capabilities Metadata

Seeded at migration time for existing sources. Each source row gets its `capabilities` array populated in a data migration alongside the schema migration. New sources set their capabilities in their registration query.

The "missing fields" calculation compares a fixed field list `["employee_count", "revenue", "profit"]` against null/zero values in the company row.

---

## Section 7: UI

### Companies list — filter panel extension

Existing filter popover gains four new fields:
- Min employees / Max employees (number inputs)
- Min revenue USD / Max revenue USD (number inputs, formatted with M/B suffix)

These map to the four new query params.

### Company detail — financial data card

New card below the existing company info showing:
- Employee count (or "—")
- Revenue (formatted in USD + original currency if different)
- Profit (formatted in USD)
- Data year and source label

Edit button opens a dialog pre-filled with current values (manual override).

### Company detail — "Fill from source" panel

Shown only when `GET /api/v1/companies/:id/enrichment-sources` returns at least one source with overlapping capabilities. Shows:
- List of missing fields
- Per-source "Enrich" button that calls `POST /api/v1/companies/:id/enrich-from-source`
- Button disables and shows spinner while the job is enqueued; success toast on completion

---

## Error Handling

- LLM translation failure: log warning, store untranslated text (enrichment is best-effort)
- FX rate fetch failure: log error, skip revenue_usd write (retry on next job run)
- Brreg API 404: mark company as "no financial data available" via a null write (prevents repeated retries)
- PATCH /financials validation failure: 400 with field-level error message

---

## Testing

- `llm` package: mock HTTP server returning fixed JSON; test `Complete`, `Translate`, and `maybeTranslate`
- `fxrates` package: mock ECB XML response; test `ToUSD` for NOK→USD and unknown currency error
- Enrichment job: table-driven tests with mock Brreg API, mock LLM, mock FX rates
- Handler tests: `TestPatchFinancials_Validation`, `TestGetEnrichmentSources_NoSources`, `TestEnrichFromSource_EnqueuesJob`
