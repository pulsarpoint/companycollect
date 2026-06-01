package httpapi

import (
	"log/slog"
	"net/http"

	"github.com/cockroachdb/errors"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	pgx "github.com/jackc/pgx/v5"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

type brregRawRecordListResponse struct {
	Items []db.BrregWorkflowVRawRecordList `json:"items"`
	Total int64                            `json:"total"`
	Page  int                              `json:"page"`
	Limit int                              `json:"limit"`
}

func (h *Handlers) handleListBrregRawRecords(w http.ResponseWriter, r *http.Request) {
	if h.db == nil {
		writeError(w, http.StatusServiceUnavailable, "database querier not available")
		return
	}

	params := brregRawRecordListParamsFromRequest(r)
	countParams := db.CountBrregWorkflowRawRecordsParams{
		Query:             params.Query,
		LifecycleState:    params.LifecycleState,
		TranslationStatus: params.TranslationStatus,
		DomainStatus:      params.DomainStatus,
		FinancialStatus:   params.FinancialStatus,
		EnhancedStatus:    params.EnhancedStatus,
	}
	total, err := h.db.CountBrregWorkflowRawRecords(r.Context(), countParams)
	if err != nil {
		slog.Error("count brreg raw records", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	items, err := h.db.ListBrregWorkflowRawRecords(r.Context(), params)
	if err != nil {
		slog.Error("list brreg raw records", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if items == nil {
		items = []db.BrregWorkflowVRawRecordList{}
	}

	writeJSON(w, http.StatusOK, brregRawRecordListResponse{
		Items: items,
		Total: total,
		Page:  queryInt(r, "page", 1),
		Limit: int(params.Limit),
	})
}

func (h *Handlers) handleGetBrregRawRecord(w http.ResponseWriter, r *http.Request) {
	if h.db == nil {
		writeError(w, http.StatusServiceUnavailable, "database querier not available")
		return
	}
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "id must be a valid UUID")
		return
	}

	row, err := h.db.GetBrregWorkflowRawRecordDetail(r.Context(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "brreg raw record not found")
			return
		}
		slog.Error("get brreg raw record", "id", id.String(), "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, row)
}

func brregRawRecordListParamsFromRequest(r *http.Request) db.ListBrregWorkflowRawRecordsParams {
	page := queryInt(r, "page", 1)
	pageSize := min(queryInt(r, "limit", 50), 200)
	return db.ListBrregWorkflowRawRecordsParams{
		Query:             queryString(r, "q"),
		LifecycleState:    firstQueryString(r, "state", "lifecycle_state"),
		TranslationStatus: queryString(r, "translation_status"),
		DomainStatus:      queryString(r, "domain_status"),
		FinancialStatus:   queryString(r, "financial_status"),
		EnhancedStatus:    queryString(r, "enhanced_status"),
		SortBy:            brregRawRecordSortBy(r.URL.Query().Get("sort")),
		SortDir:           brregRawRecordSortDir(r.URL.Query().Get("dir")),
		Offset:            int32((page - 1) * pageSize),
		Limit:             int32(pageSize),
	}
}

func firstQueryString(r *http.Request, keys ...string) *string {
	for _, key := range keys {
		if value := queryString(r, key); value != nil {
			return value
		}
	}
	return nil
}

func brregRawRecordSortBy(value string) string {
	switch value {
	case "organization", "website", "state", "translation_status", "domain_status", "financial_status", "enhanced_status", "last_seen_at":
		return value
	default:
		return "last_seen_at"
	}
}

func brregRawRecordSortDir(value string) string {
	if value == "asc" {
		return "asc"
	}
	return "desc"
}
