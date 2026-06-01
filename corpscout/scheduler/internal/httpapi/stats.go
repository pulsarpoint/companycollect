package httpapi

import (
	"log/slog"
	"net/http"
)

type statsResponse struct {
	TotalCompanies   int64 `json:"total_companies"`
	TotalDomains     int64 `json:"total_domains"`
	ActiveDomains    int64 `json:"active_domains"`
	PendingReview    int64 `json:"pending_review"`
	PendingRawInputs int64 `json:"pending_raw_inputs"`
	EnabledSources   int64 `json:"enabled_sources"`
}

func (h *Handlers) handleStats(w http.ResponseWriter, r *http.Request) {
	stats, err := h.db.GetStats(r.Context())
	if err != nil {
		slog.Error("get stats", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, statsResponse{
		TotalCompanies:   stats.TotalCompanies,
		TotalDomains:     stats.TotalDomains,
		ActiveDomains:    stats.ActiveDomains,
		PendingReview:    stats.PendingReview,
		PendingRawInputs: stats.PendingRawInputs,
		EnabledSources:   stats.EnabledSources,
	})
}
