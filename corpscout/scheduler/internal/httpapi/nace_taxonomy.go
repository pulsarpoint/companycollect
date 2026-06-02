package httpapi

import (
	"log/slog"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/nacetaxonomy"
)

type listNACERevisionsResponse struct {
	Items []db.VNaceTaxonomyState `json:"items"`
}

type listNACECodeChildrenResponse struct {
	Items []naceCodeResponse `json:"items"`
}

type naceCodeResponse struct {
	ID               uuid.UUID `json:"id"`
	ClassificationID uuid.UUID `json:"classification_id"`
	Code             string    `json:"code"`
	NormalizedCode   string    `json:"normalized_code"`
	Level            int16     `json:"level"`
	LevelName        string    `json:"level_name"`
	ParentCode       *string   `json:"parent_code"`
	ParentID         *string   `json:"parent_id"`
	Title            string    `json:"title"`
	Description      *string   `json:"description"`
	Includes         *string   `json:"includes"`
	Excludes         *string   `json:"excludes"`
	Active           bool      `json:"active"`
	HasChildren      bool      `json:"has_children"`
}

func (h *Handlers) handleListNACERevisions(w http.ResponseWriter, r *http.Request) {
	if h.db == nil {
		writeError(w, http.StatusServiceUnavailable, "database not available")
		return
	}
	items, err := h.db.ListNACETaxonomyState(r.Context())
	if err != nil {
		slog.Error("list nace revisions", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list NACE revisions")
		return
	}
	writeJSON(w, http.StatusOK, listNACERevisionsResponse{Items: items})
}

func (h *Handlers) handleListNACECodeChildren(w http.ResponseWriter, r *http.Request) {
	if h.db == nil {
		writeError(w, http.StatusServiceUnavailable, "database not available")
		return
	}
	params, err := parseListNACECodeChildrenParams(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	rows, err := h.db.ListNACECodeChildren(r.Context(), params)
	if err != nil {
		slog.Error("list nace code children", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list NACE codes")
		return
	}
	items := make([]naceCodeResponse, 0, len(rows))
	for _, row := range rows {
		items = append(items, naceCodeResponseFromRow(row))
	}
	writeJSON(w, http.StatusOK, listNACECodeChildrenResponse{Items: items})
}

func parseListNACECodeChildrenParams(r *http.Request) (db.ListNACECodeChildrenParams, error) {
	revision := strings.TrimSpace(r.URL.Query().Get("revision"))
	if revision == "" {
		revision = nacetaxonomy.DefaultRevision
	}
	params := db.ListNACECodeChildrenParams{Revision: revision}
	parentID := strings.TrimSpace(r.URL.Query().Get("parent_id"))
	if parentID == "" {
		return params, nil
	}
	parsed, err := uuid.Parse(parentID)
	if err != nil {
		return db.ListNACECodeChildrenParams{}, errInvalidParentID
	}
	params.ParentID = pgtype.UUID{Bytes: parsed, Valid: true}
	return params, nil
}

func naceCodeResponseFromRow(row db.ListNACECodeChildrenRow) naceCodeResponse {
	var parentID *string
	if row.ParentID.Valid {
		value := uuid.UUID(row.ParentID.Bytes).String()
		parentID = &value
	}
	return naceCodeResponse{
		ID:               row.ID,
		ClassificationID: row.ClassificationID,
		Code:             row.Code,
		NormalizedCode:   row.NormalizedCode,
		Level:            row.Level,
		LevelName:        row.LevelName,
		ParentCode:       row.ParentCode,
		ParentID:         parentID,
		Title:            row.Title,
		Description:      row.Description,
		Includes:         row.Includes,
		Excludes:         row.Excludes,
		Active:           row.Active,
		HasChildren:      row.HasChildren,
	}
}

type invalidParentIDError struct{}

func (invalidParentIDError) Error() string { return "parent_id must be a valid UUID" }

var errInvalidParentID invalidParentIDError
