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
	require.Equal(t, "1120804.35", r.ConvertedAmount)
	require.Equal(t, int64(112080435), r.ConvertedMinorUnits)
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

func TestRateLookup(t *testing.T) {
	svc := newService()
	req := service.RateLookupRequest{
		Provider:       "ecb",
		DatePolicy:     "latest_on_or_before",
		SourceCurrency: "NOK",
		TargetCurrency: "USD",
		Date:           "2024-12-31",
	}
	resp, err := svc.RateLookup(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, "ecb", resp.Provider)
	require.Equal(t, "NOK", resp.SourceCurrency)
	require.Equal(t, "USD", resp.TargetCurrency)
	require.NotEmpty(t, resp.Rate)
	require.Equal(t, "EUR", resp.BaseCurrency)
	require.Equal(t, "ecb:2024-12-31:latest_on_or_before", resp.CacheKey)
}
