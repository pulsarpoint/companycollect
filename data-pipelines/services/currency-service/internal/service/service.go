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
			Code:          "unsupported_currency",
			Message:       fmt.Sprintf("currency %s is not available from provider %s", tgt, providerName),
			Category:      "validation",
			RetryStrategy: "do_not_retry",
		}
		return base
	}
	if _, ok := conversion.MinorUnits(src); !ok {
		base.Status = "failed"
		base.Err = &ServiceError{
			Code:          "unsupported_currency",
			Message:       fmt.Sprintf("currency %s is not available from provider %s", src, providerName),
			Category:      "validation",
			RetryStrategy: "do_not_retry",
		}
		return base
	}

	// Identity conversion — no provider fetch needed.
	if src == tgt {
		rounded := conversion.RoundToMinorUnits(amount, minorUnit)
		base.Status = "succeeded"
		rateDate := item.Date
		if rateDate == "" {
			rateDate = today
		}
		base.RateDate = rateDate
		base.Rate = "1"
		base.ConvertedAmount = rounded.StringFixed(int32(minorUnit))
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
		var dateErr error
		requestedDate, dateErr = time.Parse("2006-01-02", item.Date)
		if dateErr != nil {
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
			Code:          "unsupported_currency",
			Message:       fmt.Sprintf("currency not in provider rate sheet: %v", convErr),
			Category:      "validation",
			RetryStrategy: "do_not_retry",
		}
		return base
	}

	rounded := conversion.RoundToMinorUnits(converted, minorUnit)
	base.Status = "succeeded"
	base.RateDate = sheet.EffectiveDate.Format("2006-01-02")
	base.Rate = rate.String()
	base.ConvertedAmount = rounded.StringFixed(int32(minorUnit))
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
