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
