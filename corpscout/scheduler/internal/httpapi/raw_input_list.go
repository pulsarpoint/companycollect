package httpapi

import (
	"log/slog"
	"net/http"
)

type rawInputListResponse struct {
	Items []rawInputRow `json:"items"`
	Total int64         `json:"total"`
	Page  int           `json:"page"`
	Limit int           `json:"limit"`
}

// handleListRawInputs returns a unified paginated view of all raw_inputs tables.
// Query params: source, status, translation_status, q (name search), sort (name|source|created_at|status), dir (asc|desc), page, limit.
func (h *Handlers) handleListRawInputs(w http.ResponseWriter, r *http.Request) {
	if h.pool == nil {
		writeError(w, http.StatusServiceUnavailable, "database pool not available")
		return
	}

	params, ok := rawInputListParamsFromRequest(r)
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid has_suggestion filter")
		return
	}

	query := buildRawInputListQuery(params)
	if query.empty {
		writeJSON(w, http.StatusOK, rawInputListResponse{
			Items: []rawInputRow{},
			Total: 0,
			Page:  params.page,
			Limit: params.pageSize,
		})
		return
	}

	var total int64
	if err := h.pool.QueryRow(r.Context(), query.countSQL, query.args...).Scan(&total); err != nil {
		slog.Error("list raw inputs count", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	rows, err := h.pool.Query(r.Context(), query.dataSQL, query.dataArgs...)
	if err != nil {
		slog.Error("list raw inputs", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	defer rows.Close()

	items, err := scanRawInputListRows(rows)
	if err != nil {
		slog.Error("raw input rows iter", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	writeJSON(w, http.StatusOK, rawInputListResponse{
		Items: items,
		Total: total,
		Page:  params.page,
		Limit: params.pageSize,
	})
}
