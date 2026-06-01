package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"strings"

	"github.com/google/uuid"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/service"
)

type companySuggestionListResponse struct {
	Items []db.ListCompanySuggestionReviewsRow `json:"items"`
	Page  int                                  `json:"page"`
	Limit int                                  `json:"limit"`
	Total int32                                `json:"total"`
}

type companySuggestionIDsResponse struct {
	IDs []string `json:"ids"`
}

type bulkCompanySuggestionsResponse struct {
	Updated int `json:"updated"`
	Skipped int `json:"skipped"`
}

func (h *Handlers) handleListCompanySuggestions(w http.ResponseWriter, r *http.Request) {
	page := queryInt(r, "page", 1)
	limit := min(queryInt(r, "limit", 20), 100)
	offset := int32((page - 1) * limit)
	status := optionalQueryString(r, "status")
	sourceType := optionalQueryString(r, "source_type")
	q := optionalQueryString(r, "q")

	items, err := h.db.ListCompanySuggestionReviews(r.Context(), db.ListCompanySuggestionReviewsParams{
		Status:     status,
		SourceType: sourceType,
		Q:          q,
		Offset:     offset,
		Limit:      int32(limit),
	})
	if err != nil {
		slog.Error("list company suggestions", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	total, err := h.db.CountCompanySuggestionReviews(r.Context(), db.CountCompanySuggestionReviewsParams{
		Status:     status,
		SourceType: sourceType,
		Q:          q,
	})
	if err != nil {
		slog.Error("count company suggestions", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if items == nil {
		items = []db.ListCompanySuggestionReviewsRow{}
	}
	writeJSON(w, http.StatusOK, companySuggestionListResponse{
		Items: items,
		Page:  page,
		Limit: limit,
		Total: total,
	})
}

func (h *Handlers) handleListCompanySuggestionIDs(w http.ResponseWriter, r *http.Request) {
	ids, err := h.db.ListCompanySuggestionReviewIDs(r.Context())
	if err != nil {
		slog.Error("list company suggestion ids", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, companySuggestionIDsResponse{IDs: uuidStrings(ids)})
}

func (h *Handlers) handleBulkCompanySuggestions(w http.ResponseWriter, r *http.Request) {
	body, err := decodeBulkActionRequest(r, approveRejectBulkActions, approveRejectBulkActionMessage)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if h.pool == nil {
		writeError(w, http.StatusServiceUnavailable, "database pool not available")
		return
	}
	updated, skipped := 0, 0
	for _, idStr := range body.IDs {
		id, err := uuid.Parse(idStr)
		if err != nil {
			skipped++
			continue
		}
		items, err := h.pendingCompanySuggestionReviewItems(r.Context(), id)
		if err != nil {
			slog.Error("list pending company suggestion sections", "id", id, "error", err)
			skipped++
			continue
		}
		if len(items) == 0 {
			skipped++
			continue
		}
		if body.Action == "approve" {
			if err := service.ApplyCompanySuggestionSections(r.Context(), h.pool, id, items, "ops", ""); err != nil {
				slog.Error("bulk approve company suggestion", "id", id, "error", err)
				skipped++
				continue
			}
		} else {
			if err := service.RejectCompanySuggestionSections(r.Context(), h.pool, id, items, "ops", ""); err != nil {
				slog.Error("bulk reject company suggestion", "id", id, "error", err)
				skipped++
				continue
			}
		}
		updated++
	}
	writeJSON(w, http.StatusOK, bulkCompanySuggestionsResponse{Updated: updated, Skipped: skipped})
}

func (h *Handlers) pendingCompanySuggestionReviewItems(ctx context.Context, suggestionID uuid.UUID) ([]service.CompanySuggestionReviewItem, error) {
	rows, err := h.db.ListPendingCompanySuggestionReviewItems(ctx, suggestionID)
	if err != nil {
		return nil, err
	}
	items := make([]service.CompanySuggestionReviewItem, 0, len(rows))
	for _, row := range rows {
		items = append(items, service.CompanySuggestionReviewItem{Table: row.SectionTable, ID: row.ID})
	}
	return items, nil
}

func optionalQueryString(r *http.Request, key string) *string {
	value := strings.TrimSpace(r.URL.Query().Get(key))
	if value == "" {
		return nil
	}
	return &value
}
