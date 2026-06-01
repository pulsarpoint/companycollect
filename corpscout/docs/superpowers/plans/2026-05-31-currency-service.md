# Currency Conversion Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a standalone Go HTTP service at `data-pipelines/services/currency-service/` that converts monetary amounts between ISO 4217 currencies for a requested date using ECB rates, returning decimal-safe results suitable for financial evidence storage.

**Architecture:** An HTTP server with three layers — `conversion` (decimal math + currency metadata), `rates` (ECB XML provider behind a `Provider` interface), and `cache` (in-memory singleflight cache with TTL for today, indefinite for historical dates). A `service` layer orchestrates these three to serve `/v1/convert` (batch) and `/v1/rates` (lookup) via the `httpapi` handlers.

**Tech Stack:** Go 1.26.1 · `github.com/shopspring/decimal` (decimal-safe math) · `github.com/bojanz/currency` (ISO 4217 minor units via CLDR) · `golang.org/x/sync/singleflight` (cache stampede prevention) · `github.com/cockroachdb/errors` (error wrapping) · `net/http` (stdlib router, Go 1.22+ patterns) · `log/slog` · `github.com/stretchr/testify`

---

## File Map

| Path | Responsibility |
|---|---|
| `cmd/currency-service/main.go` | Parse env, wire dependencies, start HTTP server with graceful shutdown |
| `internal/conversion/currency.go` | `MinorUnits(currency) (int, bool)` — thin wrapper over `bojanz/currency.GetDigits` |
| `internal/conversion/currency_test.go` | Tests for minor-unit lookup |
| `internal/conversion/conversion.go` | `Convert(amount, src, tgt, eurPer)` — decimal math through EUR |
| `internal/conversion/conversion_test.go` | Tests for all conversion paths |
| `internal/rates/provider.go` | `RateSheet` struct + `Provider` interface |
| `internal/rates/ecb.go` | ECB XML fetcher; parses daily + historical XML; `latest_on_or_before` |
| `internal/rates/ecb_test.go` | Tests with XML fixture |
| `internal/cache/cache.go` | In-memory `Cache`; singleflight; TTL for today, indefinite for history |
| `internal/cache/cache_test.go` | Hit/miss/TTL tests |
| `internal/service/service.go` | `Service`; per-item logic; error classification |
| `internal/service/service_test.go` | Identity, cross-currency, partial-failure, batch-too-large |
| `internal/httpapi/types.go` | JSON-tagged request/response structs matching the spec API |
| `internal/httpapi/handler.go` | `NewHandler`; routes; JSON encode/decode; maps to/from service types |
| `internal/httpapi/handler_test.go` | HTTP tests for all endpoints |
| `go.mod` | Module `github.com/pulsarpoint/currency-service`, Go 1.26.1 |
| `Dockerfile` | Multi-stage: `golang:1.26-alpine` → `alpine:3.20` |
| `Makefile` | `build`, `test`, `run`, `down`, `logs` |
| `.env.example` | All env var defaults documented |

---

## Build Note

This service has its own `go.mod`. Always run `make` targets from
`data-pipelines/services/currency-service/`, or pass `GOWORK=off` explicitly because the monorepo root has a `go.work` that would otherwise shadow this module.

---

### Task 1: Project scaffolding

**Files:**
- Create: `data-pipelines/services/currency-service/go.mod`
- Create: `data-pipelines/services/currency-service/Makefile`
- Create: `data-pipelines/services/currency-service/Dockerfile`
- Create: `data-pipelines/services/currency-service/.env.example`

- [ ] **Step 1: Create directory structure**

```bash
mkdir -p data-pipelines/services/currency-service/{cmd/currency-service,internal/{conversion,rates,cache,service,httpapi}}
```

- [ ] **Step 2: Write go.mod**

Create `data-pipelines/services/currency-service/go.mod`:

```
module github.com/pulsarpoint/currency-service

go 1.26.1

require (
	github.com/bojanz/currency v1.3.0
	github.com/cockroachdb/errors v1.13.0
	github.com/shopspring/decimal v1.4.0
	github.com/stretchr/testify v1.11.1
	golang.org/x/sync v0.20.0
)
```

- [ ] **Step 3: Download dependencies**

```bash
cd data-pipelines/services/currency-service
GOWORK=off go mod tidy
```

Expected: `go.sum` created, no errors.

- [ ] **Step 4: Write Makefile**

Create `data-pipelines/services/currency-service/Makefile`:

```makefile
.PHONY: build test run down logs

build:
	GOWORK=off go build -o bin/currency-service ./cmd/currency-service

test:
	GOWORK=off go test ./...

run:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f
```

- [ ] **Step 5: Write Dockerfile**

Create `data-pipelines/services/currency-service/Dockerfile`:

```dockerfile
FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN GOWORK=off CGO_ENABLED=0 go build -o /currency-service ./cmd/currency-service

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /currency-service /currency-service
ENTRYPOINT ["/currency-service"]
```

- [ ] **Step 6: Write .env.example**

Create `data-pipelines/services/currency-service/.env.example`:

```
CURRENCY_SERVICE_LISTEN_ADDR=:8097
CURRENCY_SERVICE_PROVIDER=ecb
CURRENCY_SERVICE_TODAY_TTL=6h
CURRENCY_SERVICE_REQUEST_TIMEOUT=30s
CURRENCY_SERVICE_MAX_BATCH_SIZE=1000
```

- [ ] **Step 7: Commit scaffolding**

```bash
git -C data-pipelines/services/currency-service add go.mod Makefile Dockerfile .env.example
git -C data-pipelines/services/currency-service commit -m "chore: scaffold currency-service project"
```

---

### Task 2: Currency metadata

**Files:**
- Create: `internal/conversion/currency.go`
- Create: `internal/conversion/currency_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/conversion/currency_test.go`:

```go
package conversion_test

import (
	"testing"

	"github.com/pulsarpoint/currency-service/internal/conversion"
	"github.com/stretchr/testify/require"
)

func TestMinorUnitsKnownCurrencies(t *testing.T) {
	cases := []struct {
		currency string
		want     int
	}{
		{"USD", 2},
		{"EUR", 2},
		{"NOK", 2},
		{"GBP", 2},
		{"JPY", 0},
		{"KRW", 0},
		{"ISK", 0}, // Icelandic króna — no subunit in use; CLDR=0, not 2
		{"KWD", 3},
		{"BHD", 3},
		{"OMR", 3},
	}
	for _, tc := range cases {
		t.Run(tc.currency, func(t *testing.T) {
			got, ok := conversion.MinorUnits(tc.currency)
			require.True(t, ok, "currency %s should be known", tc.currency)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestMinorUnitsUnknownCurrency(t *testing.T) {
	_, ok := conversion.MinorUnits("XYZ")
	require.False(t, ok)
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd data-pipelines/services/currency-service
GOWORK=off go test ./internal/conversion/ -v
```

Expected: compile error — `conversion.MinorUnits` not defined.

- [ ] **Step 3: Write currency metadata**

Create `internal/conversion/currency.go`:

```go
package conversion

import "github.com/bojanz/currency"

// MinorUnits returns the number of minor units for an ISO 4217 currency code.
// Returns false if the currency is not recognised.
// Delegates to bojanz/currency which uses CLDR data (handles ISK=0, KWD=3, JPY=0, etc.).
func MinorUnits(code string) (int, bool) {
	d, ok := currency.GetDigits(code)
	return int(d), ok
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
GOWORK=off go test ./internal/conversion/ -v -run TestMinorUnits
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/conversion/currency.go internal/conversion/currency_test.go
git commit -m "feat: add ISO 4217 minor-unit metadata"
```

---

### Task 3: Decimal conversion logic

**Files:**
- Create: `internal/conversion/conversion.go`
- Create: `internal/conversion/conversion_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `internal/conversion/conversion_test.go`:

```go
package conversion_test

import (
	"testing"

	"github.com/pulsarpoint/currency-service/internal/conversion"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func d(s string) decimal.Decimal {
	v, _ := decimal.NewFromString(s)
	return v
}

func TestConvertIdentity(t *testing.T) {
	eurPer := map[string]decimal.Decimal{
		"EUR": decimal.NewFromInt(1),
		"USD": d("1.09"),
		"NOK": d("11.50"),
	}
	rate, result, err := conversion.Convert(d("100.00"), "USD", "USD", eurPer)
	require.NoError(t, err)
	require.True(t, rate.Equal(decimal.NewFromInt(1)), "rate should be 1")
	require.True(t, result.Equal(d("100.00")), "converted should equal input")
}

func TestConvertNOKtoUSD(t *testing.T) {
	eurPer := map[string]decimal.Decimal{
		"EUR": decimal.NewFromInt(1),
		"USD": d("1.09"),
		"NOK": d("11.50"),
	}
	// NOK→USD rate = 1.09 / 11.50 = 0.094782608...
	// 100 NOK * rate = 9.478260869...
	rate, result, err := conversion.Convert(d("100.00"), "NOK", "USD", eurPer)
	require.NoError(t, err)
	require.False(t, rate.IsZero())
	// Round to 2 minor units for USD → 9.48
	rounded := conversion.RoundToMinorUnits(result, 2)
	require.Equal(t, "9.48", rounded.String())
}

func TestConvertUSDtoNOK(t *testing.T) {
	eurPer := map[string]decimal.Decimal{
		"EUR": decimal.NewFromInt(1),
		"USD": d("1.09"),
		"NOK": d("11.50"),
	}
	// USD→NOK rate = 11.50 / 1.09 = 10.5504587...
	// 100 USD → 1055.04587... NOK → rounded to 2 dp = 1055.05
	_, result, err := conversion.Convert(d("100.00"), "USD", "NOK", eurPer)
	require.NoError(t, err)
	rounded := conversion.RoundToMinorUnits(result, 2)
	require.Equal(t, "1055.05", rounded.String())
}

func TestConvertUnknownSourceCurrency(t *testing.T) {
	eurPer := map[string]decimal.Decimal{"EUR": decimal.NewFromInt(1), "USD": d("1.09")}
	_, _, err := conversion.Convert(d("100.00"), "XYZ", "USD", eurPer)
	require.Error(t, err)
}

func TestConvertUnknownTargetCurrency(t *testing.T) {
	eurPer := map[string]decimal.Decimal{"EUR": decimal.NewFromInt(1), "NOK": d("11.50")}
	_, _, err := conversion.Convert(d("100.00"), "NOK", "ABC", eurPer)
	require.Error(t, err)
}

func TestToMinorUnits(t *testing.T) {
	require.Equal(t, int64(948), conversion.ToMinorUnits(d("9.48"), 2))
	require.Equal(t, int64(9), conversion.ToMinorUnits(d("9.48"), 0))
	require.Equal(t, int64(9478), conversion.ToMinorUnits(d("9.478"), 3))
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
GOWORK=off go test ./internal/conversion/ -v
```

Expected: compile error — `conversion.Convert` not defined.

- [ ] **Step 3: Write conversion logic**

Create `internal/conversion/conversion.go`:

```go
package conversion

import (
	"fmt"

	"github.com/shopspring/decimal"
)

// Convert returns the exchange rate and converted amount from sourceCurrency to
// targetCurrency. eurPer[currency] is units of that currency per 1 EUR.
// Returns (rate, convertedAmount, error).
// For identity conversions (src == tgt) the rate is 1 and no eurPer lookup occurs.
func Convert(
	amount decimal.Decimal,
	sourceCurrency, targetCurrency string,
	eurPer map[string]decimal.Decimal,
) (rate decimal.Decimal, converted decimal.Decimal, err error) {
	if sourceCurrency == targetCurrency {
		return decimal.NewFromInt(1), amount, nil
	}
	srcRate, ok := eurPer[sourceCurrency]
	if !ok {
		return decimal.Zero, decimal.Zero, fmt.Errorf("currency %s not in rate sheet", sourceCurrency)
	}
	tgtRate, ok := eurPer[targetCurrency]
	if !ok {
		return decimal.Zero, decimal.Zero, fmt.Errorf("currency %s not in rate sheet", targetCurrency)
	}
	// rate = units of target per 1 unit of source = tgtRate / srcRate
	rate = tgtRate.Div(srcRate)
	converted = amount.Mul(rate)
	return rate, converted, nil
}

// RoundToMinorUnits rounds amount to the given number of minor units using
// half-up rounding.
func RoundToMinorUnits(amount decimal.Decimal, minorUnit int) decimal.Decimal {
	return amount.Round(int32(minorUnit))
}

// ToMinorUnits converts a decimal amount to an integer minor-unit value.
// e.g. 9.48 USD with minorUnit=2 → 948 cents.
func ToMinorUnits(amount decimal.Decimal, minorUnit int) int64 {
	multiplier := decimal.NewFromInt(1)
	for i := 0; i < minorUnit; i++ {
		multiplier = multiplier.Mul(decimal.NewFromInt(10))
	}
	return amount.Mul(multiplier).Round(0).IntPart()
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
GOWORK=off go test ./internal/conversion/ -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/conversion/conversion.go internal/conversion/conversion_test.go
git commit -m "feat: add decimal conversion math and minor-unit helpers"
```

---

### Task 4: ECB rate provider

**Files:**
- Create: `internal/rates/provider.go`
- Create: `internal/rates/ecb.go`
- Create: `internal/rates/ecb_test.go`

- [ ] **Step 1: Write the failing tests**

Create `internal/rates/ecb_test.go`:

```go
package rates_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pulsarpoint/currency-service/internal/rates"
	"github.com/stretchr/testify/require"
)

const ecbFixture = `<?xml version="1.0" encoding="UTF-8"?>
<gesmes:Envelope xmlns:gesmes="http://www.gesmes.org/xml/2002-08-01" xmlns="http://www.ecb.int/vocabulary/2002-08-01/eurofxref">
  <Cube>
    <Cube time="2026-05-20">
      <Cube currency="USD" rate="1.0900"/>
      <Cube currency="NOK" rate="11.5000"/>
    </Cube>
    <Cube time="2026-05-17">
      <Cube currency="USD" rate="1.0800"/>
      <Cube currency="NOK" rate="11.4000"/>
    </Cube>
  </Cube>
</gesmes:Envelope>`

func testServer(body string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(body))
	}))
}

func TestECBFetchRatesLatest(t *testing.T) {
	srv := testServer(ecbFixture)
	defer srv.Close()

	p := rates.NewECBProvider(rates.ECBConfig{DailyURL: srv.URL, HistoricalURL: srv.URL})
	sheet, err := p.FetchRates(context.Background(), time.Time{})
	require.NoError(t, err)
	require.Equal(t, "2026-05-20", sheet.EffectiveDate.Format("2006-01-02"))
	require.Equal(t, "EUR", sheet.BaseCurrency)
	usd := sheet.Rates["USD"]
	require.Equal(t, "1.09", usd.String())
}

func TestECBFetchRatesForDate(t *testing.T) {
	srv := testServer(ecbFixture)
	defer srv.Close()

	p := rates.NewECBProvider(rates.ECBConfig{DailyURL: srv.URL, HistoricalURL: srv.URL})
	date, _ := time.Parse("2006-01-02", "2026-05-19")
	sheet, err := p.FetchRates(context.Background(), date)
	require.NoError(t, err)
	// 2026-05-19 is a Monday; latest available on or before is 2026-05-17
	require.Equal(t, "2026-05-17", sheet.EffectiveDate.Format("2006-01-02"))
	nok := sheet.Rates["NOK"]
	require.Equal(t, "11.4", nok.String())
}

func TestECBFetchRatesNotFound(t *testing.T) {
	srv := testServer(ecbFixture)
	defer srv.Close()

	p := rates.NewECBProvider(rates.ECBConfig{DailyURL: srv.URL, HistoricalURL: srv.URL})
	date, _ := time.Parse("2006-01-02", "2020-01-01")
	_, err := p.FetchRates(context.Background(), date)
	require.Error(t, err)
	require.Contains(t, err.Error(), "rate_not_found")
}

func TestECBFetchRatesProviderError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	p := rates.NewECBProvider(rates.ECBConfig{DailyURL: srv.URL, HistoricalURL: srv.URL})
	_, err := p.FetchRates(context.Background(), time.Time{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "provider_unavailable")
}

func TestECBName(t *testing.T) {
	p := rates.NewECBProvider(rates.ECBConfig{})
	require.Equal(t, "ecb", p.Name())
}

// suppress unused import warning for strings
var _ = strings.NewReader
```

- [ ] **Step 2: Run test to verify it fails**

```bash
GOWORK=off go test ./internal/rates/ -v
```

Expected: compile error — `rates.NewECBProvider` not defined.

- [ ] **Step 3: Write provider interface and RateSheet**

Create `internal/rates/provider.go`:

```go
package rates

import (
	"context"
	"time"

	"github.com/shopspring/decimal"
)

// RateSheet is a full set of exchange rates from one provider for one effective date.
// Rates[currency] = units of that currency per 1 EUR (ECB convention).
// EUR itself is always present with rate 1.
type RateSheet struct {
	EffectiveDate time.Time
	BaseCurrency  string
	Rates         map[string]decimal.Decimal
	FetchedAt     time.Time
	SourceURL     string
}

// Provider fetches rate sheets from an exchange rate source.
// date == time.Time{} means "most recent available".
type Provider interface {
	Name() string
	FetchRates(ctx context.Context, date time.Time) (*RateSheet, error)
}
```

- [ ] **Step 4: Write ECB provider**

Create `internal/rates/ecb.go`:

```go
package rates

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/shopspring/decimal"
)

const (
	defaultECBDailyURL      = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"
	defaultECBHistoricalURL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.xml"
)

// ECBConfig allows overriding provider URLs (used in tests).
type ECBConfig struct {
	DailyURL      string
	HistoricalURL string
}

type ecbProvider struct {
	dailyURL      string
	historicalURL string
	client        *http.Client
}

// NewECBProvider creates an ECB rate provider. Pass zero-value ECBConfig for defaults.
func NewECBProvider(cfg ECBConfig) Provider {
	if cfg.DailyURL == "" {
		cfg.DailyURL = defaultECBDailyURL
	}
	if cfg.HistoricalURL == "" {
		cfg.HistoricalURL = defaultECBHistoricalURL
	}
	return &ecbProvider{
		dailyURL:      cfg.DailyURL,
		historicalURL: cfg.HistoricalURL,
		client:        &http.Client{Timeout: 30 * time.Second},
	}
}

func (p *ecbProvider) Name() string { return "ecb" }

// FetchRates fetches the ECB rate sheet. date == time.Time{} → use daily feed (latest).
// Otherwise fetches the historical feed and finds the latest rate on or before date.
func (p *ecbProvider) FetchRates(ctx context.Context, date time.Time) (*RateSheet, error) {
	isLatest := date.IsZero()
	url := p.historicalURL
	if isLatest {
		url = p.dailyURL
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("provider_unavailable: build request: %w", err)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("provider_unavailable: fetch ecb: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("provider_unavailable: ecb returned status %d", resp.StatusCode)
	}
	return p.parseResponse(resp.Body, url, date)
}

type ecbEnvelope struct {
	Cube ecbOuterCube `xml:"Cube"`
}

type ecbOuterCube struct {
	Days []ecbDayCube `xml:"Cube"`
}

type ecbDayCube struct {
	Time  string        `xml:"time,attr"`
	Rates []ecbRateCube `xml:"Cube"`
}

type ecbRateCube struct {
	Currency string `xml:"currency,attr"`
	Rate     string `xml:"rate,attr"`
}

func (p *ecbProvider) parseResponse(r io.Reader, sourceURL string, target time.Time) (*RateSheet, error) {
	var env ecbEnvelope
	if err := xml.NewDecoder(r).Decode(&env); err != nil {
		return nil, fmt.Errorf("provider_unavailable: parse ecb xml: %w", err)
	}

	var selected *ecbDayCube
	for i := range env.Cube.Days {
		day := &env.Cube.Days[i]
		if day.Time == "" {
			continue
		}
		if target.IsZero() {
			// daily feed: take first (and only) entry
			selected = day
			break
		}
		parsed, err := time.Parse("2006-01-02", day.Time)
		if err != nil {
			return nil, fmt.Errorf("provider_unavailable: parse date in feed: %w", err)
		}
		if !parsed.After(target) && (selected == nil || day.Time > selected.Time) {
			selected = day
		}
	}

	if selected == nil {
		return nil, fmt.Errorf("rate_not_found: no ECB rate available on or before %s", target.Format("2006-01-02"))
	}

	effectiveDate, err := time.Parse("2006-01-02", selected.Time)
	if err != nil {
		return nil, fmt.Errorf("provider_unavailable: parse effective date: %w", err)
	}

	rateMap := make(map[string]decimal.Decimal, len(selected.Rates)+1)
	rateMap["EUR"] = decimal.NewFromInt(1)
	for _, rc := range selected.Rates {
		d, err := decimal.NewFromString(rc.Rate)
		if err != nil {
			return nil, fmt.Errorf("provider_unavailable: parse rate for %s: %w", rc.Currency, err)
		}
		rateMap[rc.Currency] = d
	}

	return &RateSheet{
		EffectiveDate: effectiveDate,
		BaseCurrency:  "EUR",
		Rates:         rateMap,
		FetchedAt:     time.Now().UTC(),
		SourceURL:     sourceURL,
	}, nil
}
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
GOWORK=off go test ./internal/rates/ -v
```

Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/rates/provider.go internal/rates/ecb.go internal/rates/ecb_test.go
git commit -m "feat: add ECB rate provider with latest_on_or_before XML parsing"
```

---

### Task 5: In-memory cache

**Files:**
- Create: `internal/cache/cache.go`
- Create: `internal/cache/cache_test.go`

- [ ] **Step 1: Write the failing tests**

Create `internal/cache/cache_test.go`:

```go
package cache_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pulsarpoint/currency-service/internal/cache"
	"github.com/pulsarpoint/currency-service/internal/rates"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func stubSheet() *rates.RateSheet {
	return &rates.RateSheet{
		EffectiveDate: time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC),
		BaseCurrency:  "EUR",
		Rates:         map[string]decimal.Decimal{"EUR": decimal.NewFromInt(1)},
		FetchedAt:     time.Now(),
	}
}

func TestCacheMissCallsFetch(t *testing.T) {
	c := cache.New(6 * time.Hour)
	var calls atomic.Int32
	fetch := func(ctx context.Context) (*rates.RateSheet, error) {
		calls.Add(1)
		return stubSheet(), nil
	}
	sheet, hit, err := c.Get(context.Background(), "key1", false, fetch)
	require.NoError(t, err)
	require.False(t, hit)
	require.NotNil(t, sheet)
	require.Equal(t, int32(1), calls.Load())
}

func TestCacheHitReturnsCachedValue(t *testing.T) {
	c := cache.New(6 * time.Hour)
	var calls atomic.Int32
	fetch := func(ctx context.Context) (*rates.RateSheet, error) {
		calls.Add(1)
		return stubSheet(), nil
	}
	_, _, _ = c.Get(context.Background(), "key2", false, fetch)
	sheet, hit, err := c.Get(context.Background(), "key2", false, fetch)
	require.NoError(t, err)
	require.True(t, hit)
	require.NotNil(t, sheet)
	require.Equal(t, int32(1), calls.Load(), "fetch should only be called once")
}

func TestCacheTodayExpires(t *testing.T) {
	ttl := 50 * time.Millisecond
	c := cache.New(ttl)
	var calls atomic.Int32
	fetch := func(ctx context.Context) (*rates.RateSheet, error) {
		calls.Add(1)
		return stubSheet(), nil
	}
	_, _, _ = c.Get(context.Background(), "today", true, fetch) // today=true → use TTL
	time.Sleep(60 * time.Millisecond)
	_, hit, _ := c.Get(context.Background(), "today", true, fetch)
	require.False(t, hit, "entry should have expired")
	require.Equal(t, int32(2), calls.Load(), "fetch should be called again after TTL")
}

func TestCacheHistoricalNeverExpires(t *testing.T) {
	c := cache.New(50 * time.Millisecond)
	var calls atomic.Int32
	fetch := func(ctx context.Context) (*rates.RateSheet, error) {
		calls.Add(1)
		return stubSheet(), nil
	}
	_, _, _ = c.Get(context.Background(), "hist", false, fetch) // today=false → indefinite
	time.Sleep(60 * time.Millisecond)
	_, hit, _ := c.Get(context.Background(), "hist", false, fetch)
	require.True(t, hit, "historical entry should not expire")
	require.Equal(t, int32(1), calls.Load())
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
GOWORK=off go test ./internal/cache/ -v
```

Expected: compile error — `cache.New` not defined.

- [ ] **Step 3: Write cache implementation**

Create `internal/cache/cache.go`:

```go
package cache

import (
	"context"
	"sync"
	"time"

	"github.com/pulsarpoint/currency-service/internal/rates"
	"golang.org/x/sync/singleflight"
)

type entry struct {
	sheet     *rates.RateSheet
	expiresAt time.Time // zero means never expires
}

// Cache is a process-local in-memory store for rate sheets.
// today=true in Get means the entry uses the configured todayTTL;
// today=false means the entry is cached for the process lifetime.
type Cache struct {
	mu       sync.RWMutex
	entries  map[string]*entry
	todayTTL time.Duration
	group    singleflight.Group
}

// New creates a Cache with the given TTL for "today" entries.
func New(todayTTL time.Duration) *Cache {
	return &Cache{
		entries:  make(map[string]*entry),
		todayTTL: todayTTL,
	}
}

// Get returns a cached RateSheet or calls fetch. today=true applies the TTL.
// Returns (sheet, cacheHit, error).
func (c *Cache) Get(
	ctx context.Context,
	key string,
	today bool,
	fetch func(context.Context) (*rates.RateSheet, error),
) (*rates.RateSheet, bool, error) {
	c.mu.RLock()
	e, ok := c.entries[key]
	c.mu.RUnlock()

	if ok && (e.expiresAt.IsZero() || time.Now().Before(e.expiresAt)) {
		return e.sheet, true, nil
	}

	type result struct {
		sheet *rates.RateSheet
	}
	v, err, _ := c.group.Do(key, func() (interface{}, error) {
		sheet, err := fetch(ctx)
		if err != nil {
			return nil, err
		}
		ent := &entry{sheet: sheet}
		if today {
			ent.expiresAt = time.Now().Add(c.todayTTL)
		}
		c.mu.Lock()
		c.entries[key] = ent
		c.mu.Unlock()
		return &result{sheet: sheet}, nil
	})
	if err != nil {
		return nil, false, err
	}
	return v.(*result).sheet, false, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
GOWORK=off go test ./internal/cache/ -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/cache/cache.go internal/cache/cache_test.go
git commit -m "feat: add in-memory rate sheet cache with singleflight stampede protection"
```

---

### Task 6: Service layer

**Files:**
- Create: `internal/service/service.go`
- Create: `internal/service/service_test.go`

- [ ] **Step 1: Write the failing tests**

Create `internal/service/service_test.go`:

```go
package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/pulsarpoint/currency-service/internal/rates"
	"github.com/pulsarpoint/currency-service/internal/service"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

// stubProvider returns a fixed RateSheet regardless of date.
type stubProvider struct{ sheet *rates.RateSheet }

func (s *stubProvider) Name() string { return "ecb" }
func (s *stubProvider) FetchRates(_ context.Context, _ time.Time) (*rates.RateSheet, error) {
	return s.sheet, nil
}

func newStubSheet() *rates.RateSheet {
	return &rates.RateSheet{
		EffectiveDate: time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC),
		BaseCurrency:  "EUR",
		Rates: map[string]decimal.Decimal{
			"EUR": decimal.NewFromInt(1),
			"USD": mustDecimal("1.09"),
			"NOK": mustDecimal("11.50"),
		},
		FetchedAt: time.Now(),
	}
}

func mustDecimal(s string) decimal.Decimal {
	d, _ := decimal.NewFromString(s)
	return d
}

func newService() *service.Service {
	p := &stubProvider{sheet: newStubSheet()}
	return service.New(service.Config{
		Providers:    []rates.Provider{p},
		TodayTTL:     6 * time.Hour,
		MaxBatchSize: 10,
	})
}

func TestConvertIdentityItem(t *testing.T) {
	svc := newService()
	req := service.ConvertRequest{
		Provider:   "ecb",
		DatePolicy: "latest_on_or_before",
		Items: []service.ConvertItem{
			{ID: "item1", Amount: "100.00", SourceCurrency: "USD", TargetCurrency: "USD", Date: "2024-12-31"},
		},
	}
	resp, err := svc.Convert(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, 1, len(resp.Results))
	r := resp.Results[0]
	require.Equal(t, "succeeded", r.Status)
	require.Equal(t, "1", r.Rate)
	require.Equal(t, "100.00", r.ConvertedAmount)
	require.True(t, r.IdentityConversion)
}

func TestConvertNOKtoUSD(t *testing.T) {
	svc := newService()
	req := service.ConvertRequest{
		Provider:   "ecb",
		DatePolicy: "latest_on_or_before",
		Items: []service.ConvertItem{
			{ID: "item2", Amount: "11825000.00", SourceCurrency: "NOK", TargetCurrency: "USD", Date: "2024-12-31"},
		},
	}
	resp, err := svc.Convert(context.Background(), req)
	require.NoError(t, err)
	r := resp.Results[0]
	require.Equal(t, "succeeded", r.Status)
	require.NotEmpty(t, r.ConvertedAmount)
	require.Equal(t, 2, r.TargetMinorUnit)
	require.Equal(t, "USD", r.TargetCurrency)
}

func TestConvertUnsupportedCurrency(t *testing.T) {
	svc := newService()
	req := service.ConvertRequest{
		Provider:   "ecb",
		DatePolicy: "latest_on_or_before",
		Items: []service.ConvertItem{
			{ID: "bad", Amount: "12.00", SourceCurrency: "XYZ", TargetCurrency: "USD", Date: "2024-12-31"},
		},
	}
	resp, err := svc.Convert(context.Background(), req)
	require.NoError(t, err)
	r := resp.Results[0]
	require.Equal(t, "failed", r.Status)
	require.Equal(t, "unsupported_currency", r.Err.Code)
	require.Equal(t, "do_not_retry", r.Err.RetryStrategy)
}

func TestConvertInvalidAmount(t *testing.T) {
	svc := newService()
	req := service.ConvertRequest{
		Provider:   "ecb",
		DatePolicy: "latest_on_or_before",
		Items: []service.ConvertItem{
			{ID: "bad2", Amount: "not-a-number", SourceCurrency: "NOK", TargetCurrency: "USD", Date: "2024-12-31"},
		},
	}
	resp, err := svc.Convert(context.Background(), req)
	require.NoError(t, err)
	r := resp.Results[0]
	require.Equal(t, "failed", r.Status)
	require.Equal(t, "invalid_amount", r.Err.Code)
}

func TestConvertMixedBatch(t *testing.T) {
	svc := newService()
	req := service.ConvertRequest{
		Provider:   "ecb",
		DatePolicy: "latest_on_or_before",
		Items: []service.ConvertItem{
			{ID: "ok", Amount: "100.00", SourceCurrency: "NOK", TargetCurrency: "USD", Date: "2024-12-31"},
			{ID: "fail", Amount: "bad", SourceCurrency: "NOK", TargetCurrency: "USD", Date: "2024-12-31"},
		},
	}
	resp, err := svc.Convert(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, 2, len(resp.Results))
	require.Equal(t, 1, resp.ItemsCompleted)
	require.Equal(t, 1, resp.ItemsFailed)
}

func TestConvertBatchTooLarge(t *testing.T) {
	svc := newService()
	items := make([]service.ConvertItem, 11)
	for i := range items {
		items[i] = service.ConvertItem{ID: "x", Amount: "1.00", SourceCurrency: "USD", TargetCurrency: "USD", Date: "2024-12-31"}
	}
	_, err := svc.Convert(context.Background(), service.ConvertRequest{
		Provider: "ecb", DatePolicy: "latest_on_or_before", Items: items,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "batch_too_large")
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
GOWORK=off go test ./internal/service/ -v
```

Expected: compile error — `service.Service` not defined.

- [ ] **Step 3: Write service implementation**

Create `internal/service/service.go`:

```go
package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pulsarpoint/currency-service/internal/cache"
	"github.com/pulsarpoint/currency-service/internal/conversion"
	"github.com/pulsarpoint/currency-service/internal/rates"
	"github.com/shopspring/decimal"
)

// ConvertItem is one conversion request from the caller.
type ConvertItem struct {
	ID             string
	Amount         string // decimal string
	SourceCurrency string
	TargetCurrency string
	Date           string // YYYY-MM-DD; empty = latest
}

// ConvertResult holds the outcome for one item; Err is non-nil on failure.
type ConvertResult struct {
	ID                  string
	Status              string // "succeeded" or "failed"
	Amount              string
	SourceCurrency      string
	TargetCurrency      string
	RequestedDate       string
	RateDate            string
	Rate                string
	ConvertedAmount     string
	ConvertedMinorUnits int64
	TargetMinorUnit     int
	IdentityConversion  bool
	BaseCurrency        string
	Err                 *ServiceError
}

// ServiceError describes a per-item failure.
type ServiceError struct {
	Code          string
	Message       string
	Category      string // "validation" or "provider"
	RetryStrategy string // "do_not_retry" or "retry"
}

// ConvertRequest is the batch conversion request.
type ConvertRequest struct {
	Provider   string
	DatePolicy string
	Items      []ConvertItem
}

// ConvertResponse is the batch conversion response.
type ConvertResponse struct {
	Provider       string
	DatePolicy     string
	Results        []ConvertResult
	ItemsSeen      int
	ItemsCompleted int
	ItemsFailed    int
}

// RateLookupRequest is a single rate query.
type RateLookupRequest struct {
	Provider       string
	DatePolicy     string
	SourceCurrency string
	TargetCurrency string
	Date           string
}

// RateLookupResponse is the result of a rate query.
type RateLookupResponse struct {
	Provider       string
	DatePolicy     string
	SourceCurrency string
	TargetCurrency string
	RequestedDate  string
	RateDate       string
	Rate           string
	BaseCurrency   string
	CacheHit       bool
	CacheKey       string
}

// Config holds Service construction parameters.
type Config struct {
	Providers    []rates.Provider
	TodayTTL     time.Duration
	MaxBatchSize int
}

// Service is the core orchestrator for currency conversion.
type Service struct {
	providers    map[string]rates.Provider
	cache        *cache.Cache
	maxBatchSize int
}

// New creates a Service. All providers in cfg.Providers are registered by name.
func New(cfg Config) *Service {
	ps := make(map[string]rates.Provider, len(cfg.Providers))
	for _, p := range cfg.Providers {
		ps[p.Name()] = p
	}
	return &Service{
		providers:    ps,
		cache:        cache.New(cfg.TodayTTL),
		maxBatchSize: cfg.MaxBatchSize,
	}
}

// Convert processes a batch of conversion items. Each item succeeds or fails
// independently. Returns an error only for request-level failures (e.g. batch
// too large, unknown provider).
func (s *Service) Convert(ctx context.Context, req ConvertRequest) (ConvertResponse, error) {
	providerName := req.Provider
	if providerName == "" {
		providerName = "ecb"
	}
	datePolicy := req.DatePolicy
	if datePolicy == "" {
		datePolicy = "latest_on_or_before"
	}
	if len(req.Items) > s.maxBatchSize {
		return ConvertResponse{}, fmt.Errorf("batch_too_large: max %d items, got %d", s.maxBatchSize, len(req.Items))
	}
	provider, ok := s.providers[providerName]
	if !ok {
		return ConvertResponse{}, fmt.Errorf("unknown provider %q", providerName)
	}

	results := make([]ConvertResult, 0, len(req.Items))
	completed, failed := 0, 0
	today := time.Now().UTC().Format("2006-01-02")

	for _, item := range req.Items {
		r := s.convertItem(ctx, item, provider, providerName, datePolicy, today)
		results = append(results, r)
		if r.Status == "succeeded" {
			completed++
		} else {
			failed++
		}
	}

	return ConvertResponse{
		Provider:       providerName,
		DatePolicy:     datePolicy,
		Results:        results,
		ItemsSeen:      len(req.Items),
		ItemsCompleted: completed,
		ItemsFailed:    failed,
	}, nil
}

func (s *Service) convertItem(
	ctx context.Context,
	item ConvertItem,
	provider rates.Provider,
	providerName, datePolicy, today string,
) ConvertResult {
	base := ConvertResult{
		ID:             item.ID,
		Amount:         item.Amount,
		SourceCurrency: strings.ToUpper(item.SourceCurrency),
		TargetCurrency: strings.ToUpper(item.TargetCurrency),
		RequestedDate:  item.Date,
	}

	// Validate amount.
	amount, err := decimal.NewFromString(item.Amount)
	if err != nil {
		base.Status = "failed"
		base.Err = &ServiceError{
			Code: "invalid_amount", Message: fmt.Sprintf("cannot parse amount %q", item.Amount),
			Category: "validation", RetryStrategy: "do_not_retry",
		}
		return base
	}

	src := strings.ToUpper(item.SourceCurrency)
	tgt := strings.ToUpper(item.TargetCurrency)

	// Validate currencies are known before provider fetch.
	minorUnit, ok := conversion.MinorUnits(tgt)
	if !ok {
		base.Status = "failed"
		base.Err = &ServiceError{
			Code:    "unsupported_currency",
			Message: fmt.Sprintf("currency %s is not available from provider %s", tgt, providerName),
			Category: "validation", RetryStrategy: "do_not_retry",
		}
		return base
	}
	if _, ok := conversion.MinorUnits(src); !ok {
		base.Status = "failed"
		base.Err = &ServiceError{
			Code:    "unsupported_currency",
			Message: fmt.Sprintf("currency %s is not available from provider %s", src, providerName),
			Category: "validation", RetryStrategy: "do_not_retry",
		}
		return base
	}

	// Identity conversion — no provider fetch needed.
	if src == tgt {
		rounded := conversion.RoundToMinorUnits(amount, minorUnit)
		base.Status = "succeeded"
		base.RateDate = item.Date
		base.Rate = "1"
		base.ConvertedAmount = rounded.String()
		base.ConvertedMinorUnits = conversion.ToMinorUnits(rounded, minorUnit)
		base.TargetMinorUnit = minorUnit
		base.IdentityConversion = true
		return base
	}

	// Fetch rates via cache.
	cacheKey := fmt.Sprintf("%s:%s:%s", providerName, item.Date, datePolicy)
	isToday := item.Date == today || item.Date == ""
	var requestedDate time.Time
	if item.Date != "" {
		requestedDate, err = time.Parse("2006-01-02", item.Date)
		if err != nil {
			base.Status = "failed"
			base.Err = &ServiceError{
				Code: "invalid_date", Message: fmt.Sprintf("invalid date %q, use YYYY-MM-DD", item.Date),
				Category: "validation", RetryStrategy: "do_not_retry",
			}
			return base
		}
	}

	sheet, _, fetchErr := s.cache.Get(ctx, cacheKey, isToday, func(ctx context.Context) (*rates.RateSheet, error) {
		return provider.FetchRates(ctx, requestedDate)
	})
	if fetchErr != nil {
		code := "provider_unavailable"
		if strings.Contains(fetchErr.Error(), "rate_not_found") {
			code = "rate_not_found"
		}
		base.Status = "failed"
		base.Err = &ServiceError{
			Code: code, Message: fetchErr.Error(), Category: "provider", RetryStrategy: "retry",
		}
		return base
	}

	// Convert using the rate sheet.
	rate, converted, convErr := conversion.Convert(amount, src, tgt, sheet.Rates)
	if convErr != nil {
		base.Status = "failed"
		base.Err = &ServiceError{
			Code:    "unsupported_currency",
			Message: fmt.Sprintf("currency not in provider rate sheet: %v", convErr),
			Category: "validation", RetryStrategy: "do_not_retry",
		}
		return base
	}

	rounded := conversion.RoundToMinorUnits(converted, minorUnit)
	base.Status = "succeeded"
	base.RateDate = sheet.EffectiveDate.Format("2006-01-02")
	base.Rate = rate.String()
	base.ConvertedAmount = rounded.String()
	base.ConvertedMinorUnits = conversion.ToMinorUnits(rounded, minorUnit)
	base.TargetMinorUnit = minorUnit
	base.BaseCurrency = sheet.BaseCurrency
	return base
}

// RateLookup fetches the exchange rate for a single pair without converting an amount.
func (s *Service) RateLookup(ctx context.Context, req RateLookupRequest) (RateLookupResponse, error) {
	providerName := req.Provider
	if providerName == "" {
		providerName = "ecb"
	}
	datePolicy := req.DatePolicy
	if datePolicy == "" {
		datePolicy = "latest_on_or_before"
	}
	provider, ok := s.providers[providerName]
	if !ok {
		return RateLookupResponse{}, fmt.Errorf("unknown provider %q", providerName)
	}

	src := strings.ToUpper(req.SourceCurrency)
	tgt := strings.ToUpper(req.TargetCurrency)
	today := time.Now().UTC().Format("2006-01-02")
	isToday := req.Date == today || req.Date == ""
	cacheKey := fmt.Sprintf("%s:%s:%s", providerName, req.Date, datePolicy)

	var requestedDate time.Time
	var err error
	if req.Date != "" {
		requestedDate, err = time.Parse("2006-01-02", req.Date)
		if err != nil {
			return RateLookupResponse{}, fmt.Errorf("invalid_date: %w", err)
		}
	}

	sheet, hit, fetchErr := s.cache.Get(ctx, cacheKey, isToday, func(ctx context.Context) (*rates.RateSheet, error) {
		return provider.FetchRates(ctx, requestedDate)
	})
	if fetchErr != nil {
		return RateLookupResponse{}, fetchErr
	}

	rate, _, convErr := conversion.Convert(decimal.NewFromInt(1), src, tgt, sheet.Rates)
	if convErr != nil {
		return RateLookupResponse{}, fmt.Errorf("unsupported_currency: %w", convErr)
	}

	return RateLookupResponse{
		Provider:       providerName,
		DatePolicy:     datePolicy,
		SourceCurrency: src,
		TargetCurrency: tgt,
		RequestedDate:  req.Date,
		RateDate:       sheet.EffectiveDate.Format("2006-01-02"),
		Rate:           rate.String(),
		BaseCurrency:   sheet.BaseCurrency,
		CacheHit:       hit,
		CacheKey:       cacheKey,
	}, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
GOWORK=off go test ./internal/service/ -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/service/service.go internal/service/service_test.go
git commit -m "feat: add service layer — batch convert and rate lookup orchestration"
```

---

### Task 7: HTTP types

**Files:**
- Create: `internal/httpapi/types.go`

- [ ] **Step 1: Write types**

Create `internal/httpapi/types.go`:

```go
package httpapi

// ConvertRequest is the JSON body for POST /v1/convert.
type ConvertRequest struct {
	Provider   string            `json:"provider"`
	DatePolicy string            `json:"date_policy"`
	Items      []ConvertItemJSON `json:"items"`
}

// ConvertItemJSON is one item in a batch conversion request.
type ConvertItemJSON struct {
	ID             string `json:"id"`
	Amount         string `json:"amount"`
	SourceCurrency string `json:"source_currency"`
	TargetCurrency string `json:"target_currency"`
	Date           string `json:"date"`
}

// ConvertResponse is the JSON body for POST /v1/convert response.
type ConvertResponse struct {
	SchemaVersion  string              `json:"schema_version"`
	Provider       string              `json:"provider"`
	DatePolicy     string              `json:"date_policy"`
	ItemsSeen      int                 `json:"items_seen"`
	ItemsCompleted int                 `json:"items_completed"`
	ItemsFailed    int                 `json:"items_failed"`
	Results        []ConvertResultJSON `json:"results"`
	DurationMs     int64               `json:"duration_ms"`
}

// ConvertResultJSON is one result in the convert response.
// ErrorJSON is nil for succeeded items; all other fields are zero for failed items.
type ConvertResultJSON struct {
	ID                  string            `json:"id"`
	Status              string            `json:"status"`
	Amount              string            `json:"amount"`
	SourceCurrency      string            `json:"source_currency"`
	TargetCurrency      string            `json:"target_currency"`
	RequestedDate       string            `json:"requested_date"`
	RateDate            string            `json:"rate_date,omitempty"`
	Rate                string            `json:"rate,omitempty"`
	ConvertedAmount     string            `json:"converted_amount,omitempty"`
	ConvertedMinorUnits int64             `json:"converted_minor_units,omitempty"`
	TargetMinorUnit     int               `json:"target_minor_unit,omitempty"`
	Metadata            map[string]any    `json:"metadata,omitempty"`
	Error               *ErrorJSON        `json:"error,omitempty"`
}

// ErrorJSON describes a per-item failure.
type ErrorJSON struct {
	Code          string `json:"code"`
	Message       string `json:"message"`
	Category      string `json:"category"`
	RetryStrategy string `json:"retry_strategy"`
}

// RatesResponse is the JSON body for GET /v1/rates.
type RatesResponse struct {
	SchemaVersion  string        `json:"schema_version"`
	Provider       string        `json:"provider"`
	DatePolicy     string        `json:"date_policy"`
	SourceCurrency string        `json:"source_currency"`
	TargetCurrency string        `json:"target_currency"`
	RequestedDate  string        `json:"requested_date"`
	RateDate       string        `json:"rate_date"`
	Rate           string        `json:"rate"`
	BaseCurrency   string        `json:"base_currency"`
	Cache          CacheInfoJSON `json:"cache"`
}

// CacheInfoJSON is cache diagnostics in the rates response.
type CacheInfoJSON struct {
	Hit bool   `json:"hit"`
	Key string `json:"key"`
}

// HealthResponse is the JSON body for GET /healthz.
type HealthResponse struct {
	Status string `json:"status"`
}
```

- [ ] **Step 2: Compile-check**

```bash
GOWORK=off go build ./internal/httpapi/
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add internal/httpapi/types.go
git commit -m "feat: add HTTP API JSON types"
```

---

### Task 8: HTTP handlers

**Files:**
- Create: `internal/httpapi/handler.go`
- Create: `internal/httpapi/handler_test.go`

- [ ] **Step 1: Write the failing tests**

Create `internal/httpapi/handler_test.go`:

```go
package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pulsarpoint/currency-service/internal/httpapi"
	"github.com/pulsarpoint/currency-service/internal/rates"
	"github.com/pulsarpoint/currency-service/internal/service"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

type stubProvider struct{}

func (s *stubProvider) Name() string { return "ecb" }
func (s *stubProvider) FetchRates(_ context.Context, _ time.Time) (*rates.RateSheet, error) {
	return &rates.RateSheet{
		EffectiveDate: time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC),
		BaseCurrency:  "EUR",
		Rates: map[string]decimal.Decimal{
			"EUR": decimal.NewFromInt(1),
			"USD": mustD("1.09"),
			"NOK": mustD("11.50"),
		},
		FetchedAt: time.Now(),
	}, nil
}

func mustD(s string) decimal.Decimal {
	d, _ := decimal.NewFromString(s)
	return d
}

func newTestHandler() http.Handler {
	svc := service.New(service.Config{
		Providers:    []rates.Provider{&stubProvider{}},
		TodayTTL:     time.Hour,
		MaxBatchSize: 100,
	})
	return httpapi.NewHandler(svc)
}

func TestHealthz(t *testing.T) {
	h := newTestHandler()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	var body httpapi.HealthResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
	require.Equal(t, "ok", body.Status)
}

func TestConvertEndpoint(t *testing.T) {
	h := newTestHandler()
	reqBody := httpapi.ConvertRequest{
		Provider:   "ecb",
		DatePolicy: "latest_on_or_before",
		Items: []httpapi.ConvertItemJSON{
			{ID: "test-1", Amount: "100.00", SourceCurrency: "NOK", TargetCurrency: "USD", Date: "2024-12-31"},
		},
	}
	b, _ := json.Marshal(reqBody)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/convert", bytes.NewReader(b)))
	require.Equal(t, http.StatusOK, rec.Code)
	var resp httpapi.ConvertResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	require.Equal(t, "currency-service.convert.v1", resp.SchemaVersion)
	require.Equal(t, 1, resp.ItemsSeen)
	require.Equal(t, 1, resp.ItemsCompleted)
	require.Equal(t, 0, resp.ItemsFailed)
	require.Equal(t, "succeeded", resp.Results[0].Status)
}

func TestConvertEndpointMixedBatch(t *testing.T) {
	h := newTestHandler()
	reqBody := httpapi.ConvertRequest{
		Provider:   "ecb",
		DatePolicy: "latest_on_or_before",
		Items: []httpapi.ConvertItemJSON{
			{ID: "ok", Amount: "100.00", SourceCurrency: "NOK", TargetCurrency: "USD", Date: "2024-12-31"},
			{ID: "bad", Amount: "bad", SourceCurrency: "NOK", TargetCurrency: "USD", Date: "2024-12-31"},
		},
	}
	b, _ := json.Marshal(reqBody)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/convert", bytes.NewReader(b)))
	require.Equal(t, http.StatusOK, rec.Code)
	var resp httpapi.ConvertResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	require.Equal(t, 1, resp.ItemsCompleted)
	require.Equal(t, 1, resp.ItemsFailed)
}

func TestRatesEndpoint(t *testing.T) {
	h := newTestHandler()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/rates?source=NOK&target=USD&date=2024-12-31&provider=ecb&date_policy=latest_on_or_before", nil)
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	var resp httpapi.RatesResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	require.Equal(t, "currency-service.rates.v1", resp.SchemaVersion)
	require.Equal(t, "NOK", resp.SourceCurrency)
	require.Equal(t, "USD", resp.TargetCurrency)
	require.NotEmpty(t, resp.Rate)
}

func TestConvertBatchTooLarge(t *testing.T) {
	h := newTestHandler()
	items := make([]httpapi.ConvertItemJSON, 101)
	for i := range items {
		items[i] = httpapi.ConvertItemJSON{ID: "x", Amount: "1", SourceCurrency: "USD", TargetCurrency: "USD", Date: "2024-12-31"}
	}
	reqBody := httpapi.ConvertRequest{Provider: "ecb", DatePolicy: "latest_on_or_before", Items: items}
	b, _ := json.Marshal(reqBody)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/convert", bytes.NewReader(b)))
	require.Equal(t, http.StatusBadRequest, rec.Code)
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
GOWORK=off go test ./internal/httpapi/ -v
```

Expected: compile error — `httpapi.NewHandler` not defined.

- [ ] **Step 3: Write HTTP handler**

Create `internal/httpapi/handler.go`:

```go
package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/pulsarpoint/currency-service/internal/service"
)

// NewHandler returns an http.Handler with all routes registered.
func NewHandler(svc *service.Service) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleHealthz)
	mux.HandleFunc("POST /v1/convert", handleConvert(svc))
	mux.HandleFunc("GET /v1/rates", handleRates(svc))
	return mux
}

func handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, HealthResponse{Status: "ok"})
}

func handleConvert(svc *service.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		var req ConvertRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}

		svcReq := service.ConvertRequest{
			Provider:   req.Provider,
			DatePolicy: req.DatePolicy,
			Items:      make([]service.ConvertItem, len(req.Items)),
		}
		for i, item := range req.Items {
			svcReq.Items[i] = service.ConvertItem{
				ID:             item.ID,
				Amount:         item.Amount,
				SourceCurrency: strings.ToUpper(item.SourceCurrency),
				TargetCurrency: strings.ToUpper(item.TargetCurrency),
				Date:           item.Date,
			}
		}

		svcResp, err := svc.Convert(r.Context(), svcReq)
		if err != nil {
			if strings.Contains(err.Error(), "batch_too_large") {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			slog.Error("convert request failed", "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		results := make([]ConvertResultJSON, len(svcResp.Results))
		for i, r := range svcResp.Results {
			jr := ConvertResultJSON{
				ID:             r.ID,
				Status:         r.Status,
				Amount:         r.Amount,
				SourceCurrency: r.SourceCurrency,
				TargetCurrency: r.TargetCurrency,
				RequestedDate:  r.RequestedDate,
			}
			if r.Status == "succeeded" {
				jr.RateDate = r.RateDate
				jr.Rate = r.Rate
				jr.ConvertedAmount = r.ConvertedAmount
				jr.ConvertedMinorUnits = r.ConvertedMinorUnits
				jr.TargetMinorUnit = r.TargetMinorUnit
				meta := map[string]any{"provider": svcResp.Provider}
				if r.IdentityConversion {
					meta["identity_conversion"] = true
				} else if r.BaseCurrency != "" {
					meta["base_currency"] = r.BaseCurrency
				}
				jr.Metadata = meta
			} else if r.Err != nil {
				jr.Error = &ErrorJSON{
					Code:          r.Err.Code,
					Message:       r.Err.Message,
					Category:      r.Err.Category,
					RetryStrategy: r.Err.RetryStrategy,
				}
			}
			results[i] = jr
		}

		writeJSON(w, http.StatusOK, ConvertResponse{
			SchemaVersion:  "currency-service.convert.v1",
			Provider:       svcResp.Provider,
			DatePolicy:     svcResp.DatePolicy,
			ItemsSeen:      svcResp.ItemsSeen,
			ItemsCompleted: svcResp.ItemsCompleted,
			ItemsFailed:    svcResp.ItemsFailed,
			Results:        results,
			DurationMs:     time.Since(start).Milliseconds(),
		})
	}
}

func handleRates(svc *service.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		req := service.RateLookupRequest{
			Provider:       q.Get("provider"),
			DatePolicy:     q.Get("date_policy"),
			SourceCurrency: strings.ToUpper(q.Get("source")),
			TargetCurrency: strings.ToUpper(q.Get("target")),
			Date:           q.Get("date"),
		}
		resp, err := svc.RateLookup(r.Context(), req)
		if err != nil {
			slog.Error("rate lookup failed", "error", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusOK, RatesResponse{
			SchemaVersion:  "currency-service.rates.v1",
			Provider:       resp.Provider,
			DatePolicy:     resp.DatePolicy,
			SourceCurrency: resp.SourceCurrency,
			TargetCurrency: resp.TargetCurrency,
			RequestedDate:  resp.RequestedDate,
			RateDate:       resp.RateDate,
			Rate:           resp.Rate,
			BaseCurrency:   resp.BaseCurrency,
			Cache:          CacheInfoJSON{Hit: resp.CacheHit, Key: resp.CacheKey},
		})
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("encode response", "error", err)
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
GOWORK=off go test ./internal/httpapi/ -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/httpapi/handler.go internal/httpapi/handler_test.go
git commit -m "feat: add HTTP handlers for /healthz, /v1/convert, /v1/rates"
```

---

### Task 9: main.go wiring

**Files:**
- Create: `cmd/currency-service/main.go`

- [ ] **Step 1: Write main.go**

Create `cmd/currency-service/main.go`:

```go
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pulsarpoint/currency-service/internal/httpapi"
	"github.com/pulsarpoint/currency-service/internal/rates"
	"github.com/pulsarpoint/currency-service/internal/service"
)

func main() {
	listenAddr    := getEnv("CURRENCY_SERVICE_LISTEN_ADDR", ":8097")
	providerName  := getEnv("CURRENCY_SERVICE_PROVIDER", "ecb")
	todayTTLStr   := getEnv("CURRENCY_SERVICE_TODAY_TTL", "6h")
	maxBatchSizeStr := getEnv("CURRENCY_SERVICE_MAX_BATCH_SIZE", "1000")

	todayTTL, err := time.ParseDuration(todayTTLStr)
	if err != nil {
		slog.Error("invalid CURRENCY_SERVICE_TODAY_TTL", "value", todayTTLStr)
		os.Exit(1)
	}

	maxBatchSize := 1000
	if _, err := fmt.Sscanf(maxBatchSizeStr, "%d", &maxBatchSize); err != nil {
		slog.Error("invalid CURRENCY_SERVICE_MAX_BATCH_SIZE", "value", maxBatchSizeStr)
		os.Exit(1)
	}

	var providers []rates.Provider
	switch providerName {
	case "ecb":
		providers = append(providers, rates.NewECBProvider(rates.ECBConfig{}))
	default:
		slog.Error("unknown provider", "provider", providerName)
		os.Exit(1)
	}

	svc := service.New(service.Config{
		Providers:    providers,
		TodayTTL:     todayTTL,
		MaxBatchSize: maxBatchSize,
	})

	handler := httpapi.NewHandler(svc)
	server := &http.Server{
		Addr:         listenAddr,
		Handler:      handler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		slog.Info("currency-service starting", "addr", listenAddr, "provider", providerName)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown error", "error", err)
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
```

- [ ] **Step 2: Add missing import**

`main.go` uses `fmt.Sscanf` — add `"fmt"` to imports. Final imports block:

```go
import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pulsarpoint/currency-service/internal/httpapi"
	"github.com/pulsarpoint/currency-service/internal/rates"
	"github.com/pulsarpoint/currency-service/internal/service"
)
```

- [ ] **Step 3: Build the binary**

```bash
cd data-pipelines/services/currency-service
GOWORK=off go build -o bin/currency-service ./cmd/currency-service
```

Expected: `bin/currency-service` created, no errors.

- [ ] **Step 4: Run all tests**

```bash
GOWORK=off go test ./... -race
```

Expected: all PASS with race detector.

- [ ] **Step 5: Smoke-test the binary**

```bash
./bin/currency-service &
sleep 1
curl -s http://localhost:8097/healthz
kill %1
```

Expected: `{"status":"ok"}`.

- [ ] **Step 6: Commit**

```bash
git add cmd/currency-service/main.go
git commit -m "feat: wire main.go — env config, graceful shutdown, ECB provider"
```

---

### Task 10: Docker-compose integration

**Files:**
- Modify: `data-pipelines/services/docker-compose.yml`
- Create: `data-pipelines/services/currency-service/.env.example` (already created in Task 1)

- [ ] **Step 1: Add currency-service to docker-compose.yml**

In `data-pipelines/services/docker-compose.yml`, append:

```yaml
  currency-service:
    image: ghcr.io/pulsarpoint/corpscout-currency-service:${SERVICES_IMAGE_TAG:-latest}
    build:
      context: ./currency-service
      dockerfile: Dockerfile
    env_file:
      - path: ./currency-service/.env
        required: false
    ports:
      - "${CURRENCY_SERVICE_PORT:-8097}:8097"
    healthcheck:
      test:
        - CMD
        - wget
        - -qO-
        - http://127.0.0.1:8097/healthz
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 5s
    restart: unless-stopped
```

Note: uses `wget` (available in `alpine:3.20`) instead of Python.

- [ ] **Step 2: Build Docker image locally**

```bash
cd data-pipelines/services
docker compose build currency-service
```

Expected: build succeeds, image created.

- [ ] **Step 3: Start and health-check**

```bash
docker compose up -d currency-service
sleep 3
curl -s http://localhost:8097/healthz
docker compose logs currency-service
```

Expected: `{"status":"ok"}`, no error logs.

- [ ] **Step 4: Smoke-test convert endpoint**

```bash
curl -s -X POST http://localhost:8097/v1/convert \
  -H 'Content-Type: application/json' \
  -d '{
    "provider": "ecb",
    "date_policy": "latest_on_or_before",
    "items": [
      {"id": "smoke-1", "amount": "100.00", "source_currency": "USD", "target_currency": "USD", "date": "2024-12-31"},
      {"id": "smoke-2", "amount": "11825000.00", "source_currency": "NOK", "target_currency": "USD", "date": "2024-12-31"}
    ]
  }' | jq .
```

Expected: `items_seen: 2`, `items_completed: 2`, both results `"status": "succeeded"`.

- [ ] **Step 5: Stop container**

```bash
docker compose stop currency-service
```

- [ ] **Step 6: Commit**

```bash
git add data-pipelines/services/docker-compose.yml
git commit -m "feat: add currency-service to docker-compose on port 8097"
```

---

## Self-Review Against Spec

| Spec requirement | Covered in task |
|---|---|
| `GET /healthz` → `{"status":"ok"}` | Task 8 handler + Task 8 test |
| `POST /v1/convert` batch 1–1000 items | Task 6 service + Task 8 handler |
| `GET /v1/rates` diagnostic endpoint | Task 6 service + Task 8 handler |
| Per-item success/fail independence | Task 6 `convertItem`, Task 6 test `TestConvertMixedBatch` |
| Identity conversion (src==tgt), rate=1, no fetch | Task 3 + Task 6 |
| ECB provider, XML parsing, latest_on_or_before | Task 4 |
| Decimal-safe math, no float64 | Tasks 3, 4 — `shopspring/decimal` throughout |
| `converted_minor_units` integer | Task 3 `ToMinorUnits` |
| `target_minor_unit` from metadata table | Task 2 `MinorUnits` |
| Error codes: `unsupported_currency`, `invalid_amount`, `invalid_date`, `rate_not_found`, `provider_unavailable`, `batch_too_large` | Task 6 `convertItem` |
| `retry_strategy: do_not_retry` for validation errors | Task 6 |
| `retry_strategy: retry` for provider errors | Task 6 |
| In-memory cache keyed by `provider:date:date_policy` | Task 5 |
| Singleflight stampede prevention | Task 5 `singleflight.Group` |
| Today TTL configurable, historical indefinite | Task 5 |
| `schema_version` in responses | Task 7 types + Task 8 handler |
| `duration_ms` in convert response | Task 8 handler |
| `cache.hit` and `cache.key` in rates response | Task 6 `RateLookup` + Task 8 |
| Env vars: LISTEN_ADDR, PROVIDER, TODAY_TTL, REQUEST_TIMEOUT, MAX_BATCH_SIZE | Task 9 |
| Docker image + docker-compose on port 8097 | Task 1 Dockerfile + Task 10 |
| `slog` logging, `cockroachdb/errors` style errors | Tasks 4, 8, 9 |
