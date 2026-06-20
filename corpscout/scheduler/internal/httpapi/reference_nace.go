package httpapi

import (
	"log/slog"
	"net/http"
	"strings"

	ch "github.com/pulsarpoint/corpscout/scheduler/internal/clickhouse"
)

type referenceNACECode struct {
	Revision             string `json:"revision"`
	Code                 string `json:"code"`
	NormalizedCode       string `json:"normalized_code"`
	Level                uint8  `json:"level"`
	LevelName            string `json:"level_name"`
	ParentCode           string `json:"parent_code"`
	ParentNormalizedCode string `json:"parent_normalized_code"`
	Title                string `json:"title"`
	SectionCode          string `json:"section_code"`
	DivisionCode         string `json:"division_code"`
	GroupCode            string `json:"group_code"`
	ClassCode            string `json:"class_code"`
}

type referenceNACEListResponse struct {
	Items []referenceNACECode `json:"items"`
}

func (h *Handlers) handleListReferenceNACE(w http.ResponseWriter, r *http.Request) {
	if strings.TrimSpace(h.clickHouseURL) == "" {
		writeError(w, http.StatusServiceUnavailable, "clickhouse is not configured")
		return
	}

	reader, err := ch.OpenReader(r.Context(), h.clickHouseURL)
	if err != nil {
		slog.ErrorContext(r.Context(), "open clickhouse reference nace", "error", err)
		writeError(w, http.StatusServiceUnavailable, "clickhouse unavailable")
		return
	}
	defer reader.Close()

	revision := parseReferenceNACERevision(r.URL.Query().Get("revision"))
	rows, err := reader.Query(r.Context(), buildReferenceNACEListQuery("corpscout"), revision)
	if err != nil {
		slog.ErrorContext(r.Context(), "list clickhouse reference nace", "revision", revision, "error", err)
		writeError(w, http.StatusInternalServerError, "list reference NACE failed")
		return
	}
	defer rows.Close()

	items := make([]referenceNACECode, 0)
	for rows.Next() {
		var item referenceNACECode
		if err := rows.Scan(
			&item.Revision,
			&item.Code,
			&item.NormalizedCode,
			&item.Level,
			&item.LevelName,
			&item.ParentCode,
			&item.ParentNormalizedCode,
			&item.Title,
			&item.SectionCode,
			&item.DivisionCode,
			&item.GroupCode,
			&item.ClassCode,
		); err != nil {
			slog.ErrorContext(r.Context(), "scan clickhouse reference nace", "revision", revision, "error", err)
			writeError(w, http.StatusInternalServerError, "scan reference NACE failed")
			return
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		slog.ErrorContext(r.Context(), "read clickhouse reference nace", "revision", revision, "error", err)
		writeError(w, http.StatusInternalServerError, "read reference NACE failed")
		return
	}

	writeJSON(w, http.StatusOK, referenceNACEListResponse{Items: items})
}

func parseReferenceNACERevision(value string) string {
	revision := strings.TrimSpace(value)
	if revision == "" {
		return "2.1"
	}
	return revision
}

func buildReferenceNACEListQuery(database string) string {
	return `SELECT
  revision,
  code,
  normalized_code,
  level,
  level_name,
  ifNull(parent_code, '') AS parent_code,
  ifNull(parent_normalized_code, '') AS parent_normalized_code,
  title,
  ifNull(section_code, '') AS section_code,
  ifNull(division_code, '') AS division_code,
  ifNull(group_code, '') AS group_code,
  ifNull(class_code, '') AS class_code
FROM ` + ch.QualifiedTable(database, "nace_codes") + `
WHERE revision = ?
  AND active = true
ORDER BY level ASC, normalized_code ASC
LIMIT 5000`
}
