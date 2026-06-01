package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/pulsarpoint/brreg-financial-service/internal/models"
)

// NewHandler returns an http.Handler with all routes registered.
func NewHandler(svc LookupService) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleHealthz)
	mux.HandleFunc("POST /v1/brreg/financials/lookup", handleLookup(svc))
	return mux
}

func handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func handleLookup(svc LookupService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		var req models.LookupRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}

		if len(req.Records) == 0 {
			http.Error(w, "records must contain at least 1 entry", http.StatusBadRequest)
			return
		}

		resp, err := svc.Lookup(r.Context(), req)
		if err != nil {
			msg := err.Error()
			if strings.HasPrefix(msg, "batch_too_large") || strings.HasPrefix(msg, "invalid_organization_number") {
				http.Error(w, msg, http.StatusBadRequest)
				return
			}
			slog.Error("lookup request failed", "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		resp.DurationMs = time.Since(start).Milliseconds()
		writeJSON(w, http.StatusOK, resp)
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("encode response", "error", err)
	}
}
