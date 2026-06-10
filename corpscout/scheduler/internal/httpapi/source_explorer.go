package httpapi

import (
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/go-chi/chi/v5"

	ch "github.com/pulsarpoint/corpscout/scheduler/internal/clickhouse"
)

const (
	defaultSourceExplorerLimit = 50
	maxSourceExplorerLimit     = 200
)

type sourceExplorerCompany struct {
	BusinessID                         string    `json:"business_id"`
	CountryISO2                        string    `json:"country_iso2"`
	SourceSlug                         string    `json:"source_slug"`
	SourceRunID                        string    `json:"source_run_id"`
	SourceRecordID                     string    `json:"source_record_id"`
	Name                               string    `json:"name"`
	RegistrationDate                   string    `json:"registration_date"`
	EndDate                            string    `json:"end_date"`
	StatusCode                         string    `json:"status_code"`
	StatusDescription                  string    `json:"status_description"`
	TradeRegisterStatusCode            string    `json:"trade_register_status_code"`
	TradeRegisterStatusDescription     string    `json:"trade_register_status_description"`
	LifecycleStatus                    string    `json:"lifecycle_status"`
	IsActive                           bool      `json:"is_active"`
	MainBusinessLineCode               string    `json:"main_business_line_code"`
	MainBusinessLineDescriptionEnglish string    `json:"main_business_line_description_en"`
	CompanyFormDescriptionEnglish      string    `json:"company_form_description_en"`
	Website                            string    `json:"website"`
	NameHistoryCount                   uint64    `json:"name_history_count"`
	RegisteredEntryCount               uint64    `json:"registered_entry_count"`
	AddressCount                       uint64    `json:"address_count"`
	LatestIngestedAt                   time.Time `json:"latest_ingested_at"`
}

type sourceExplorerCompanyListResponse struct {
	Items []sourceExplorerCompany `json:"items"`
	Total uint64                  `json:"total"`
}

type sourceExplorerCompanyQuery struct {
	Limit           int
	Offset          int
	Search          string
	Active          string
	LifecycleStatus string
	Sort            string
	Direction       string
}

func (h *Handlers) handleListSourceExplorerCompanies(w http.ResponseWriter, r *http.Request) {
	sourceName := chi.URLParam(r, "name")
	if sourceName != "finland_prhytj" {
		writeError(w, http.StatusNotFound, "source explorer not available")
		return
	}
	if strings.TrimSpace(h.clickHouseURL) == "" {
		writeError(w, http.StatusServiceUnavailable, "clickhouse is not configured")
		return
	}

	reader, err := ch.OpenReader(r.Context(), h.clickHouseURL)
	if err != nil {
		slog.ErrorContext(r.Context(), "open clickhouse source explorer", "source", sourceName, "error", err)
		writeError(w, http.StatusServiceUnavailable, "clickhouse unavailable")
		return
	}
	defer reader.Close()

	params := parseSourceExplorerCompanyQuery(r)
	view := ch.QualifiedTable(reader.Database(), "fi_prhytj_company_explorer")
	countQuery, countArgs := buildSourceExplorerCompanyCountQuery(view, params)
	var total uint64
	if err := reader.QueryRow(r.Context(), countQuery, countArgs...).Scan(&total); err != nil {
		slog.ErrorContext(r.Context(), "count source explorer companies", "source", sourceName, "error", err)
		writeError(w, http.StatusInternalServerError, "count source explorer companies failed")
		return
	}

	listQuery, listArgs, err := buildSourceExplorerCompanyListQuery(view, params)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	rows, err := reader.Query(r.Context(), listQuery, listArgs...)
	if err != nil {
		slog.ErrorContext(r.Context(), "list source explorer companies", "source", sourceName, "error", err)
		writeError(w, http.StatusInternalServerError, "list source explorer companies failed")
		return
	}
	defer rows.Close()

	items := make([]sourceExplorerCompany, 0, params.Limit)
	for rows.Next() {
		var item sourceExplorerCompany
		if err := rows.Scan(
			&item.BusinessID,
			&item.CountryISO2,
			&item.SourceSlug,
			&item.SourceRunID,
			&item.SourceRecordID,
			&item.Name,
			&item.RegistrationDate,
			&item.EndDate,
			&item.StatusCode,
			&item.StatusDescription,
			&item.TradeRegisterStatusCode,
			&item.TradeRegisterStatusDescription,
			&item.LifecycleStatus,
			&item.IsActive,
			&item.MainBusinessLineCode,
			&item.MainBusinessLineDescriptionEnglish,
			&item.CompanyFormDescriptionEnglish,
			&item.Website,
			&item.NameHistoryCount,
			&item.RegisteredEntryCount,
			&item.AddressCount,
			&item.LatestIngestedAt,
		); err != nil {
			slog.ErrorContext(r.Context(), "scan source explorer company", "source", sourceName, "error", err)
			writeError(w, http.StatusInternalServerError, "scan source explorer companies failed")
			return
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		slog.ErrorContext(r.Context(), "read source explorer companies", "source", sourceName, "error", err)
		writeError(w, http.StatusInternalServerError, "read source explorer companies failed")
		return
	}

	writeJSON(w, http.StatusOK, sourceExplorerCompanyListResponse{Items: items, Total: total})
}

func parseSourceExplorerCompanyQuery(r *http.Request) sourceExplorerCompanyQuery {
	query := r.URL.Query()
	limit := parseBoundedLimit(query.Get("limit"), defaultSourceExplorerLimit, maxSourceExplorerLimit)
	offset, err := strconv.Atoi(strings.TrimSpace(query.Get("offset")))
	if err != nil || offset < 0 {
		offset = 0
	}
	direction := strings.ToLower(strings.TrimSpace(query.Get("dir")))
	if direction != "desc" {
		direction = "asc"
	}
	return sourceExplorerCompanyQuery{
		Limit:           limit,
		Offset:          offset,
		Search:          strings.TrimSpace(query.Get("q")),
		Active:          strings.ToLower(strings.TrimSpace(query.Get("active"))),
		LifecycleStatus: strings.ToLower(strings.TrimSpace(query.Get("lifecycle_status"))),
		Sort:            strings.TrimSpace(query.Get("sort")),
		Direction:       direction,
	}
}

func buildSourceExplorerCompanyCountQuery(view string, params sourceExplorerCompanyQuery) (string, []any) {
	where, args := sourceExplorerCompanyWhere(params)
	return "SELECT count() FROM " + view + where, args
}

func buildSourceExplorerCompanyListQuery(view string, params sourceExplorerCompanyQuery) (string, []any, error) {
	orderBy, err := sourceExplorerCompanyOrderBy(params.Sort)
	if err != nil {
		return "", nil, err
	}
	where, args := sourceExplorerCompanyWhere(params)
	args = append(args, params.Limit, params.Offset)
	query := `SELECT
  business_id,
  ifNull(country_iso2, ''),
  ifNull(source_slug, ''),
  ifNull(source_run_id, ''),
  ifNull(source_record_id, ''),
  ifNull(name, ''),
  ifNull(registration_date, ''),
  ifNull(end_date, ''),
  ifNull(status_code, ''),
  ifNull(status_description, ''),
  ifNull(trade_register_status_code, ''),
  ifNull(trade_register_status_description, ''),
  ifNull(lifecycle_status, ''),
  ifNull(is_active, false),
  ifNull(main_business_line_code, ''),
  ifNull(main_business_line_description_en, ''),
  ifNull(company_form_description_en, ''),
  ifNull(website, ''),
  length(ifNull(name_history, [])),
  length(ifNull(registered_entries, [])),
  length(ifNull(addresses, [])),
  latest_ingested_at
FROM ` + view + where + " ORDER BY " + orderBy + " " + params.Direction + ", business_id ASC LIMIT ? OFFSET ?"
	return query, args, nil
}

func sourceExplorerCompanyWhere(params sourceExplorerCompanyQuery) (string, []any) {
	clauses := make([]string, 0)
	args := make([]any, 0)
	if params.Search != "" {
		clauses = append(clauses, `(positionCaseInsensitiveUTF8(ifNull(business_id, ''), ?) > 0 OR positionCaseInsensitiveUTF8(ifNull(name, ''), ?) > 0 OR positionCaseInsensitiveUTF8(ifNull(main_business_line_description_en, ''), ?) > 0 OR positionCaseInsensitiveUTF8(ifNull(company_form_description_en, ''), ?) > 0 OR positionCaseInsensitiveUTF8(ifNull(website, ''), ?) > 0)`)
		for i := 0; i < 5; i++ {
			args = append(args, params.Search)
		}
	}
	switch params.Active {
	case "true", "1", "active":
		clauses = append(clauses, "ifNull(is_active, false) = true")
	case "false", "0", "inactive":
		clauses = append(clauses, "ifNull(is_active, false) = false")
	}
	if params.LifecycleStatus != "" {
		clauses = append(clauses, "ifNull(lifecycle_status, '') = ?")
		args = append(args, params.LifecycleStatus)
	}
	if len(clauses) == 0 {
		return "", args
	}
	return " WHERE " + strings.Join(clauses, " AND "), args
}

func sourceExplorerCompanyOrderBy(sort string) (string, error) {
	switch sort {
	case "", "name":
		return "(ifNull(name, '') = '') ASC, lowerUTF8(ifNull(name, ''))", nil
	case "business_id":
		return "business_id", nil
	case "registration_date":
		return "ifNull(registration_date, '')", nil
	case "end_date":
		return "ifNull(end_date, '')", nil
	case "lifecycle_status":
		return "ifNull(lifecycle_status, '')", nil
	case "main_business_line_description_en":
		return "(ifNull(main_business_line_description_en, '') = '') ASC, lowerUTF8(ifNull(main_business_line_description_en, ''))", nil
	case "company_form_description_en":
		return "(ifNull(company_form_description_en, '') = '') ASC, lowerUTF8(ifNull(company_form_description_en, ''))", nil
	case "latest_ingested_at":
		return "latest_ingested_at", nil
	default:
		return "", errors.Errorf("unsupported sort column %q", sort)
	}
}
