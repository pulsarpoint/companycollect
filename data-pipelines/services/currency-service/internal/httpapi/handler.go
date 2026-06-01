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
		for i, res := range svcResp.Results {
			jr := ConvertResultJSON{
				ID:             res.ID,
				Status:         res.Status,
				Amount:         res.Amount,
				SourceCurrency: res.SourceCurrency,
				TargetCurrency: res.TargetCurrency,
				RequestedDate:  res.RequestedDate,
			}
			if res.Status == "succeeded" {
				jr.RateDate = res.RateDate
				jr.Rate = res.Rate
				jr.ConvertedAmount = res.ConvertedAmount
				jr.ConvertedMinorUnits = res.ConvertedMinorUnits
				jr.TargetMinorUnit = res.TargetMinorUnit
				meta := map[string]any{"provider": svcResp.Provider}
				if res.IdentityConversion {
					meta["identity_conversion"] = true
				} else if res.BaseCurrency != "" {
					meta["base_currency"] = res.BaseCurrency
				}
				jr.Metadata = meta
			} else if res.Err != nil {
				jr.Error = &ErrorJSON{
					Code:          res.Err.Code,
					Message:       res.Err.Message,
					Category:      res.Err.Category,
					RetryStrategy: res.Err.RetryStrategy,
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
			msg := err.Error()
			if isClientError(msg) {
				http.Error(w, msg, http.StatusBadRequest)
			} else {
				slog.Error("rate lookup failed", "error", err)
				http.Error(w, "internal error", http.StatusInternalServerError)
			}
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

// isClientError returns true for error codes that indicate a caller mistake
// (bad currency, bad date, unknown provider) rather than an infrastructure failure.
func isClientError(msg string) bool {
	return strings.Contains(msg, "unsupported_currency") ||
		strings.Contains(msg, "invalid_date") ||
		strings.Contains(msg, "unknown provider")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("encode response", "error", err)
	}
}
