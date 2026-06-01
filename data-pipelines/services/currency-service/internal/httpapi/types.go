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
	ID                  string         `json:"id"`
	Status              string         `json:"status"`
	Amount              string         `json:"amount"`
	SourceCurrency      string         `json:"source_currency"`
	TargetCurrency      string         `json:"target_currency"`
	RequestedDate       string         `json:"requested_date"`
	RateDate            string         `json:"rate_date,omitempty"`
	Rate                string         `json:"rate,omitempty"`
	ConvertedAmount     string         `json:"converted_amount,omitempty"`
	ConvertedMinorUnits int64          `json:"converted_minor_units,omitempty"`
	TargetMinorUnit     int            `json:"target_minor_unit,omitempty"`
	Metadata            map[string]any `json:"metadata,omitempty"`
	Error               *ErrorJSON     `json:"error,omitempty"`
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
