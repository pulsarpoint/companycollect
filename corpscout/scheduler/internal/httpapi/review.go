package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

type reviewItem struct {
	db.ListDomainsRow
	Evidence json.RawMessage `json:"evidence"`
}

type reviewListResponse struct {
	Items []reviewItem `json:"items"`
	Total int64        `json:"total"`
	Page  int          `json:"page"`
	Limit int          `json:"limit"`
}

type reviewIDsResponse struct {
	IDs []string `json:"ids"`
}

type reviewStatusResponse struct {
	Status string `json:"status"`
}

type bulkReviewResponse struct {
	Updated int `json:"updated"`
	Skipped int `json:"skipped"`
}

func (h *Handlers) handleListReview(w http.ResponseWriter, r *http.Request) {
	page := queryInt(r, "page", 1)
	limit := min(queryInt(r, "limit", 50), 200)
	offset := int32((page - 1) * limit)
	status := "needs_review"

	var minConf *int16
	if s := r.URL.Query().Get("min_confidence"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			v := int16(n)
			minConf = &v
		}
	}

	params := db.ListDomainsParams{
		Status:        &status,
		Signal:        queryString(r, "signal"),
		MinConfidence: minConf,
		Q:             queryString(r, "q"),
		Offset:        offset,
		Limit:         int32(limit),
	}

	items, err := h.db.ListDomains(r.Context(), params)
	if err != nil {
		slog.Error("list review queue", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	total, err := h.db.CountDomains(r.Context(), db.CountDomainsParams{
		Status:        &status,
		Signal:        params.Signal,
		MinConfidence: params.MinConfidence,
		Q:             params.Q,
	})
	if err != nil {
		slog.Error("count review queue", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	out := make([]reviewItem, len(items))
	for i, it := range items {
		ev := json.RawMessage(it.Evidence)
		if len(ev) == 0 {
			ev = json.RawMessage("null")
		}
		out[i] = reviewItem{ListDomainsRow: it, Evidence: ev}
	}
	writeJSON(w, http.StatusOK, reviewListResponse{
		Items: out,
		Total: total,
		Page:  page,
		Limit: limit,
	})
}

func (h *Handlers) handleListReviewIDs(w http.ResponseWriter, r *http.Request) {
	status := "needs_review"
	var minConf *int16
	if s := r.URL.Query().Get("min_confidence"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			v := int16(n)
			minConf = &v
		}
	}
	params := db.ListReviewCandidateIDsParams{
		Status:        &status,
		Signal:        queryString(r, "signal"),
		MinConfidence: minConf,
		Q:             queryString(r, "q"),
	}
	ids, err := h.db.ListReviewCandidateIDs(r.Context(), params)
	if err != nil {
		slog.Error("list review candidate ids", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, reviewIDsResponse{IDs: uuidStrings(ids)})
}

func (h *Handlers) handleCreateReview(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}

	action, err := decodeActionRequest(r, reviewBulkActions, reviewBulkActionMessage)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	status, _ := reviewActionToStatus(action)

	if err := h.db.ReviewCompanyDomain(r.Context(), db.ReviewCompanyDomainParams{
		ID:     id,
		Status: status,
	}); err != nil {
		slog.Error("review company domain", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, reviewStatusResponse{Status: "ok"})
}

func (h *Handlers) handleBulkReview(w http.ResponseWriter, r *http.Request) {
	body, err := decodeBulkActionRequest(r, reviewBulkActions, reviewBulkActionMessage)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	status, _ := reviewActionToStatus(body.Action)

	updated := 0
	for _, idStr := range body.IDs {
		id, err := uuid.Parse(idStr)
		if err != nil {
			continue
		}
		if err := h.db.ReviewCompanyDomain(r.Context(), db.ReviewCompanyDomainParams{
			ID:     id,
			Status: status,
		}); err != nil {
			slog.Error("bulk review company domain", "id", id, "error", err)
			continue
		}
		updated++
	}
	skipped := len(body.IDs) - updated
	writeJSON(w, http.StatusOK, bulkReviewResponse{Updated: updated, Skipped: skipped})
}

func reviewActionToStatus(action string) (string, bool) {
	switch action {
	case "approved":
		return "active", true
	case "rejected":
		return "rejected", true
	case "superseded":
		return "superseded", true
	default:
		return "", false
	}
}
