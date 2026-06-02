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

type brregSourceEntryListResponse struct {
	Items []db.ListBrregSourceEntriesRow `json:"items"`
	Total int64                          `json:"total"`
	Page  int                            `json:"page"`
	Limit int                            `json:"limit"`
}

func (h *Handlers) handleListBrregSourceEntries(w http.ResponseWriter, r *http.Request) {
	if h.db == nil {
		writeError(w, http.StatusServiceUnavailable, "database querier not available")
		return
	}
	params := brregSourceEntryListParamsFromRequest(r)
	countParams := db.CountBrregSourceEntriesParams{
		Query:              params.Query,
		LifecycleStatus:    params.LifecycleStatus,
		RegistrationStatus: params.RegistrationStatus,
		TranslationStatus:  params.TranslationStatus,
	}
	total, err := h.db.CountBrregSourceEntries(r.Context(), countParams)
	if err != nil {
		slog.Error("count brreg source entries", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	items, err := h.db.ListBrregSourceEntries(r.Context(), params)
	if err != nil {
		slog.Error("list brreg source entries", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if items == nil {
		items = []db.ListBrregSourceEntriesRow{}
	}

	writeJSON(w, http.StatusOK, brregSourceEntryListResponse{
		Items: items,
		Total: total,
		Page:  queryInt(r, "page", 1),
		Limit: int(params.Limit),
	})
}

func (h *Handlers) handleGetBrregSourceCompanyDetail(w http.ResponseWriter, r *http.Request) {
	if h.db == nil {
		writeError(w, http.StatusServiceUnavailable, "database querier not available")
		return
	}
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "id must be a valid UUID")
		return
	}

	row, err := h.db.GetBrregSourceCompanyDetail(r.Context(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "brreg source company not found")
			return
		}
		slog.Error("get brreg source company detail", "id", id.String(), "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, row)
}

func brregSourceEntryListParamsFromRequest(r *http.Request) db.ListBrregSourceEntriesParams {
	page := queryInt(r, "page", 1)
	pageSize := min(queryInt(r, "limit", 50), 200)
	return db.ListBrregSourceEntriesParams{
		Query:              queryString(r, "q"),
		LifecycleStatus:    firstQueryString(r, "state", "lifecycle_state", "lifecycle_status"),
		RegistrationStatus: queryString(r, "registration_status"),
		TranslationStatus:  queryString(r, "translation_status"),
		SortBy:             brregSourceEntrySortBy(r.URL.Query().Get("sort")),
		SortDir:            brregSourceEntrySortDir(r.URL.Query().Get("dir")),
		Offset:             int32((page - 1) * pageSize),
		Limit:              int32(pageSize),
	}
}

func brregSourceEntrySortBy(value string) string {
	switch value {
	case "organization", "industry", "location", "employees", "revenue", "translation_missing", "updated_at":
		return value
	default:
		return "updated_at"
	}
}

func brregSourceEntrySortDir(value string) string {
	if value == "asc" {
		return "asc"
	}
	return "desc"
}
