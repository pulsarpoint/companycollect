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

// ECBConfig allows overriding provider URLs and HTTP timeout (used in tests).
type ECBConfig struct {
	DailyURL       string
	HistoricalURL  string
	RequestTimeout time.Duration
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
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 30 * time.Second
	}
	return &ecbProvider{
		dailyURL:      cfg.DailyURL,
		historicalURL: cfg.HistoricalURL,
		client:        &http.Client{Timeout: cfg.RequestTimeout},
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
