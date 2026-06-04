package httpapi

import (
	"log/slog"
	"net/http"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

type ariregisterSourceEntryListResponse struct {
	Items []db.AriregisterSourceMvCompanyExplorer `json:"items"`
	Total int64                                   `json:"total"`
	Page  int                                     `json:"page"`
	Limit int                                     `json:"limit"`
}

func (h *Handlers) handleListAriregisterSourceEntries(w http.ResponseWriter, r *http.Request) {
	if h.db == nil {
		writeError(w, http.StatusServiceUnavailable, "database querier not available")
		return
	}
	params := ariregisterSourceEntryListParamsFromRequest(r)
	countParams := db.CountAriregisterSourceEntriesParams{
		Query:              params.Query,
		LifecycleStatus:    params.LifecycleStatus,
		RegistrationStatus: params.RegistrationStatus,
		TranslationStatus:  params.TranslationStatus,
	}
	total, err := h.db.CountAriregisterSourceEntries(r.Context(), countParams)
	if err != nil {
		slog.Error("count ariregister source entries", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	items, err := h.db.ListAriregisterSourceEntries(r.Context(), params)
	if err != nil {
		slog.Error("list ariregister source entries", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if items == nil {
		items = []db.AriregisterSourceMvCompanyExplorer{}
	}

	writeJSON(w, http.StatusOK, ariregisterSourceEntryListResponse{
		Items: items,
		Total: total,
		Page:  queryInt(r, "page", 1),
		Limit: int(params.Limit),
	})
}

func ariregisterSourceEntryListParamsFromRequest(r *http.Request) db.ListAriregisterSourceEntriesParams {
	page := queryInt(r, "page", 1)
	pageSize := min(queryInt(r, "limit", 50), 200)
	return db.ListAriregisterSourceEntriesParams{
		Query:              queryString(r, "q"),
		LifecycleStatus:    firstQueryString(r, "state", "lifecycle_state", "lifecycle_status"),
		RegistrationStatus: queryString(r, "registration_status"),
		TranslationStatus:  ariregisterSourceEntryTranslationStatus(r.URL.Query().Get("translation_status")),
		SortBy:             ariregisterSourceEntrySortBy(r.URL.Query().Get("sort")),
		SortDir:            sourceEntrySortDir(r.URL.Query().Get("dir")),
		Offset:             int32((page - 1) * pageSize),
		Limit:              int32(pageSize),
	}
}

func ariregisterSourceEntryTranslationStatus(value string) *string {
	switch value {
	case "missing", "complete":
		return &value
	default:
		return nil
	}
}

func ariregisterSourceEntrySortBy(value string) string {
	switch value {
	case "organization", "updated_at":
		return value
	default:
		return "updated_at"
	}
}

func sourceEntrySortDir(value string) string {
	if value == "asc" {
		return "asc"
	}
	return "desc"
}
