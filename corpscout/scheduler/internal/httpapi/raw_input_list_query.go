package httpapi

import (
	"database/sql"
	"fmt"
	"net/http"
	"strings"
	"time"

	pgx "github.com/jackc/pgx/v5"
)

type rawInputRow struct {
	ID                string    `json:"id"`
	Source            string    `json:"source"`
	Name              string    `json:"name"`
	NativeID          string    `json:"native_id"`
	Status            string    `json:"status"`
	TranslationStatus *string   `json:"translation_status,omitempty"`
	HasSuggestion     bool      `json:"has_suggestion"`
	State             string    `json:"state"`
	CreatedAt         time.Time `json:"created_at"`
}

type rawInputListParams struct {
	page                    int
	pageSize                int
	offset                  int
	sourceFilter            string
	statusFilter            string
	translationStatusFilter string
	hasSuggestionFilter     string
	nameQuery               string
	sortBy                  string
	sortDir                 string
}

type rawInputListQuery struct {
	countSQL string
	dataSQL  string
	args     []any
	dataArgs []any
	empty    bool
}

func rawInputListParamsFromRequest(r *http.Request) (rawInputListParams, bool) {
	page := queryInt(r, "page", 1)
	pageSize := min(queryInt(r, "limit", 50), 200)
	statusFilter := r.URL.Query().Get("processing_status")
	if statusFilter == "" {
		statusFilter = r.URL.Query().Get("status")
	}
	hasSuggestionFilter := r.URL.Query().Get("has_suggestion")
	if hasSuggestionFilter != "" && hasSuggestionFilter != "true" && hasSuggestionFilter != "false" {
		return rawInputListParams{}, false
	}
	sortBy := r.URL.Query().Get("sort")
	if !validRawInputListSort(sortBy) {
		sortBy = "created_at"
	}
	sortDir := r.URL.Query().Get("dir")
	if sortDir != "asc" {
		sortDir = "desc"
	}
	return rawInputListParams{
		page:                    page,
		pageSize:                pageSize,
		offset:                  (page - 1) * pageSize,
		sourceFilter:            r.URL.Query().Get("source"),
		statusFilter:            statusFilter,
		translationStatusFilter: r.URL.Query().Get("translation_status"),
		hasSuggestionFilter:     hasSuggestionFilter,
		nameQuery:               r.URL.Query().Get("q"),
		sortBy:                  sortBy,
		sortDir:                 sortDir,
	}, true
}

func validRawInputListSort(sortBy string) bool {
	switch sortBy {
	case "name", "source", "created_at", "status", "state":
		return true
	default:
		return false
	}
}

func buildRawInputListQuery(params rawInputListParams) rawInputListQuery {
	var args []any

	var statusExpr string
	if params.statusFilter != "" {
		args = append(args, params.statusFilter)
		statusExpr = fmt.Sprintf("$%d", len(args))
	}

	var nameExpr string
	if params.nameQuery != "" {
		args = append(args, "%"+params.nameQuery+"%")
		nameExpr = fmt.Sprintf("$%d", len(args))
	}
	var translationExpr string
	if params.translationStatusFilter != "" {
		args = append(args, params.translationStatusFilter)
		translationExpr = fmt.Sprintf("ri.translation_status = $%d", len(args))
	}

	buildWhere := func(parts []string) string {
		if len(parts) == 0 {
			return ""
		}
		return "WHERE " + strings.Join(parts, " AND ")
	}

	var countSubs []string
	var dataSubs []string
	for _, src := range rawInputSources {
		if params.sourceFilter != "" && params.sourceFilter != src.source {
			continue
		}
		if params.translationStatusFilter != "" && !src.translated {
			continue
		}

		var extra []string
		if statusExpr != "" {
			extra = append(extra, fmt.Sprintf("%s = %s", rawInputAliasedExpr(rawInputSourceExpr(src.statusExpr, "processing_status")), statusExpr))
		}
		if nameExpr != "" {
			extra = append(extra, fmt.Sprintf("%s ILIKE %s", rawInputAliasedExpr(src.nameColumn), nameExpr))
		}
		suggestionExistsClause := fmt.Sprintf(
			`EXISTS (
				SELECT 1
				FROM suggestions s
				WHERE s.source_input_table = '%s'
				  AND s.source_input_id = ri.id::text
			)`,
			rawInputSuggestionTableName(src),
		)
		if params.hasSuggestionFilter != "" {
			existsClause := suggestionExistsClause
			if params.hasSuggestionFilter == "false" {
				existsClause = "NOT " + existsClause
			}
			extra = append(extra, existsClause)
		}
		translationSelect := "NULL::text AS translation_status"
		if src.translated {
			translationSelect = "ri.translation_status"
			if translationExpr != "" {
				extra = append(extra, translationExpr)
			}
		}
		whereSQL := buildWhere(extra)
		countSubs = append(countSubs, fmt.Sprintf(
			`SELECT 1 FROM %s ri %s`,
			src.tableName,
			whereSQL,
		))
		dataSubs = append(dataSubs, fmt.Sprintf(
			`SELECT ri.id::text, '%s' AS source, COALESCE(%s, '') AS name, COALESCE(%s, '') AS native_id, %s AS status, %s, %s AS has_suggestion, %s AS state, %s AS created_at FROM %s ri %s`,
			src.source,
			rawInputAliasedExpr(src.nameColumn),
			rawInputAliasedExpr(src.nativeColumn),
			rawInputAliasedExpr(rawInputSourceExpr(src.statusExpr, "processing_status")),
			translationSelect,
			suggestionExistsClause,
			rawInputAliasedExpr(rawInputSourceExpr(src.stateExpr, rawInputSourceExpr(src.statusExpr, "processing_status"))),
			rawInputAliasedExpr(rawInputSourceExpr(src.createdAtExpr, "created_at")),
			src.tableName,
			whereSQL,
		))
	}
	if len(dataSubs) == 0 {
		return rawInputListQuery{empty: true}
	}

	countUnion := strings.Join(countSubs, " UNION ALL ")
	dataUnion := strings.Join(dataSubs, " UNION ALL ")
	dataArgs := append(args, params.pageSize, params.offset)
	return rawInputListQuery{
		countSQL: fmt.Sprintf("SELECT COUNT(*) FROM (%s) t", countUnion),
		dataSQL: fmt.Sprintf(
			`WITH raw_input_page AS (
				SELECT *
				FROM (%s) t
				ORDER BY %s %s
				LIMIT $%d OFFSET $%d
			)
			SELECT
				p.id,
				p.source,
				p.name,
				p.native_id,
				p.status,
				p.translation_status,
				p.has_suggestion,
				p.state,
				p.created_at
			FROM raw_input_page p
			ORDER BY p.%s %s`,
			dataUnion, params.sortBy, params.sortDir, len(args)+1, len(args)+2, params.sortBy, params.sortDir,
		),
		args:     args,
		dataArgs: dataArgs,
	}
}

func rawInputSourceExpr(expr string, fallback string) string {
	if strings.TrimSpace(expr) != "" {
		return expr
	}
	return fallback
}

func rawInputAliasedExpr(expr string) string {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return expr
	}
	if expr == "0" ||
		strings.HasPrefix(expr, "'") ||
		strings.Contains(expr, "(") ||
		strings.Contains(expr, "::") {
		return expr
	}
	return "ri." + expr
}

func scanRawInputListRows(rows pgx.Rows) ([]rawInputRow, error) {
	items := []rawInputRow{}
	for rows.Next() {
		var row rawInputRow
		var translationStatus sql.NullString
		if err := rows.Scan(
			&row.ID,
			&row.Source,
			&row.Name,
			&row.NativeID,
			&row.Status,
			&translationStatus,
			&row.HasSuggestion,
			&row.State,
			&row.CreatedAt,
		); err != nil {
			return nil, err
		}
		if translationStatus.Valid {
			row.TranslationStatus = &translationStatus.String
		}
		items = append(items, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}
