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
