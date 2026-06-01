package httpapi

import (
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

type pendingFinancialsResponse struct {
	Items []db.ListPendingCompanyFinancialsRow `json:"items"`
	Total int32                                `json:"total"`
	Page  int                                  `json:"page"`
	Limit int                                  `json:"limit"`
}

type pendingFinancialIDsResponse struct {
	IDs []string `json:"ids"`
}

type companyFinancialsResponse struct {
	Items []db.CompanyFinancial `json:"items"`
}

// handleListPendingFinancials returns paginated pending (suggested) company financials.
// GET /api/v1/financials/review?page=1&limit=50
func (h *Handlers) handleListPendingFinancials(w http.ResponseWriter, r *http.Request) {
	page := queryInt(r, "page", 1)
	limit := min(queryInt(r, "limit", 50), 200)
	offset := int32((page - 1) * limit)

	items, err := h.db.ListPendingCompanyFinancials(r.Context(), db.ListPendingCompanyFinancialsParams{
		Offset: offset,
		Limit:  int32(limit),
	})
	if err != nil {
		slog.Error("list pending company financials", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	total, err := h.db.CountPendingCompanyFinancials(r.Context())
	if err != nil {
		slog.Error("count pending company financials", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	writeJSON(w, http.StatusOK, pendingFinancialsResponse{
		Items: items,
		Total: total,
		Page:  page,
		Limit: limit,
	})
}

// handleListPendingFinancialIDs returns all pending financial IDs (for bulk selection).
// GET /api/v1/financials/review/ids
func (h *Handlers) handleListPendingFinancialIDs(w http.ResponseWriter, r *http.Request) {
	ids, err := h.db.ListPendingCompanyFinancialIDs(r.Context())
	if err != nil {
		slog.Error("list pending company financial ids", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	writeJSON(w, http.StatusOK, pendingFinancialIDsResponse{IDs: uuidStrings(ids)})
}

// handleBulkReviewFinancials approves or rejects multiple financials in one call.
// POST /api/v1/financials/review/bulk
// Body: {"ids": ["uuid", ...], "action": "approve"|"reject"}
func (h *Handlers) handleBulkReviewFinancials(w http.ResponseWriter, r *http.Request) {
	body, err := decodeBulkActionRequest(r, approveRejectBulkActions, approveRejectBulkActionMessage)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	status := financialReviewActionToStatus(body.Action)

	uuids := make([]uuid.UUID, 0, len(body.IDs))
	for _, idStr := range body.IDs {
		id, err := uuid.Parse(idStr)
		if err != nil {
			continue
		}
		uuids = append(uuids, id)
	}

	if err := h.db.BulkUpdateCompanyFinancialStatus(r.Context(), db.BulkUpdateCompanyFinancialStatusParams{
		Status:     status,
		ReviewedBy: nil,
		Ids:        uuids,
	}); err != nil {
		slog.Error("bulk update company financial status", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func financialReviewActionToStatus(action string) string {
	if action == "approve" {
		return "approved"
	}
	return "rejected"
}

// handleListCompanyFinancials returns all financial records for a company (any status).
// GET /api/v1/companies/:id/financials
func (h *Handlers) handleListCompanyFinancials(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}

	items, err := h.db.ListCompanyFinancials(r.Context(), id)
	if err != nil {
		slog.Error("list company financials", "company_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	if items == nil {
		items = []db.CompanyFinancial{}
	}

	writeJSON(w, http.StatusOK, companyFinancialsResponse{Items: items})
}

// handleReviewFinancial approves or rejects a single financial record.
// POST /api/v1/financials/:id/review
// Body: {"action": "approve"|"reject"}
func (h *Handlers) handleReviewFinancial(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}

	action, err := decodeActionRequest(r, approveRejectBulkActions, approveRejectBulkActionMessage)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	switch action {
	case "approve":
		if err := h.db.ApproveCompanyFinancial(r.Context(), db.ApproveCompanyFinancialParams{
			ReviewedBy: nil,
			ID:         id,
		}); err != nil {
			slog.Error("approve company financial", "id", id, "error", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
	case "reject":
		if err := h.db.RejectCompanyFinancial(r.Context(), db.RejectCompanyFinancialParams{
			ReviewedBy: nil,
			ID:         id,
		}); err != nil {
			slog.Error("reject company financial", "id", id, "error", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
	}

	w.WriteHeader(http.StatusNoContent)
}
