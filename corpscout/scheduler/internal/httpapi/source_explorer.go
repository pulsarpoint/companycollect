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
	finlandPRHYTJExplorerTable = "fi_prhytj_company_explorer_cache"
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
	MainBusinessLineCodeSet            string    `json:"main_business_line_code_set"`
	NACERevision                       string    `json:"nace_revision"`
	NACECode                           string    `json:"nace_code"`
	NACESectionCode                    string    `json:"nace_section_code"`
	NACEDivisionCode                   string    `json:"nace_division_code"`
	NACEGroupCode                      string    `json:"nace_group_code"`
	NACEClassCode                      string    `json:"nace_class_code"`
	NACETitleEnglish                   string    `json:"nace_title_en"`
	NACEMappingStatus                  string    `json:"nace_mapping_status"`
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

type sourceExplorerFormFilterOption struct {
	Code        string `json:"code"`
	Description string `json:"description"`
	Count       uint64 `json:"count"`
}

type sourceExplorerIndustryFilterOption struct {
	ID             string `json:"id"`
	Kind           string `json:"kind"`
	FilterValue    string `json:"filter_value"`
	Revision       string `json:"revision"`
	LevelName      string `json:"level_name"`
	Code           string `json:"code"`
	CodeSet        string `json:"code_set"`
	Title          string `json:"title"`
	Breadcrumb     string `json:"breadcrumb"`
	MappedNACECode string `json:"mapped_nace_code"`
	Count          uint64 `json:"count"`
	SearchText     string `json:"search_text"`
}

type sourceExplorerFilterOptionsResponse struct {
	Forms           []sourceExplorerFormFilterOption     `json:"forms"`
	IndustryOptions []sourceExplorerIndustryFilterOption `json:"industry_options"`
}

type sourceExplorerSourceIndustryFilter struct {
	CodeSet string
	Code    string
}

type sourceExplorerCompanyQuery struct {
	Limit            int
	Offset           int
	Search           string
	Active           string
	LifecycleStatus  string
	CompanyFormCodes []string
	IndustryNACE     []string
	SourceIndustries []sourceExplorerSourceIndustryFilter
	Sort             string
	Direction        string
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
	table := ch.QualifiedTable(reader.Database(), finlandPRHYTJExplorerTable)
	countQuery, countArgs := buildSourceExplorerCompanyCountQuery(table, params)
	var total uint64
	if err := reader.QueryRow(r.Context(), countQuery, countArgs...).Scan(&total); err != nil {
		slog.ErrorContext(r.Context(), "count source explorer companies", "source", sourceName, "error", err)
		writeError(w, http.StatusInternalServerError, "count source explorer companies failed")
		return
	}

	listQuery, listArgs, err := buildSourceExplorerCompanyListQuery(table, params)
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
			&item.MainBusinessLineCodeSet,
			&item.NACERevision,
			&item.NACECode,
			&item.NACESectionCode,
			&item.NACEDivisionCode,
			&item.NACEGroupCode,
			&item.NACEClassCode,
			&item.NACETitleEnglish,
			&item.NACEMappingStatus,
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

func (h *Handlers) handleListSourceExplorerFilterOptions(w http.ResponseWriter, r *http.Request) {
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
		slog.ErrorContext(r.Context(), "open clickhouse source explorer filters", "source", sourceName, "error", err)
		writeError(w, http.StatusServiceUnavailable, "clickhouse unavailable")
		return
	}
	defer reader.Close()

	table := ch.QualifiedTable(reader.Database(), finlandPRHYTJExplorerTable)
	formRows, err := reader.Query(r.Context(), buildSourceExplorerFormFilterOptionsQuery(table))
	if err != nil {
		slog.ErrorContext(r.Context(), "list source explorer filter options", "source", sourceName, "error", err)
		writeError(w, http.StatusInternalServerError, "list source explorer filter options failed")
		return
	}

	formOptions := make([]sourceExplorerFormFilterOption, 0)
	for formRows.Next() {
		var option sourceExplorerFormFilterOption
		if err := formRows.Scan(&option.Code, &option.Description, &option.Count); err != nil {
			formRows.Close()
			slog.ErrorContext(r.Context(), "scan source explorer filter option", "source", sourceName, "error", err)
			writeError(w, http.StatusInternalServerError, "scan source explorer filter options failed")
			return
		}
		formOptions = append(formOptions, option)
	}
	if err := formRows.Err(); err != nil {
		formRows.Close()
		slog.ErrorContext(r.Context(), "read source explorer filter options", "source", sourceName, "error", err)
		writeError(w, http.StatusInternalServerError, "read source explorer filter options failed")
		return
	}
	formRows.Close()

	industryRows, err := reader.Query(r.Context(), buildSourceExplorerIndustryFilterOptionsQuery(table))
	if err != nil {
		slog.ErrorContext(r.Context(), "list source explorer industry filter options", "source", sourceName, "error", err)
		writeError(w, http.StatusInternalServerError, "list source explorer industry filter options failed")
		return
	}
	defer industryRows.Close()

	industryOptions := make([]sourceExplorerIndustryFilterOption, 0)
	for industryRows.Next() {
		var option sourceExplorerIndustryFilterOption
		if err := industryRows.Scan(
			&option.ID,
			&option.Kind,
			&option.FilterValue,
			&option.Revision,
			&option.LevelName,
			&option.Code,
			&option.CodeSet,
			&option.Title,
			&option.Breadcrumb,
			&option.MappedNACECode,
			&option.Count,
			&option.SearchText,
		); err != nil {
			slog.ErrorContext(r.Context(), "scan source explorer industry filter option", "source", sourceName, "error", err)
			writeError(w, http.StatusInternalServerError, "scan source explorer industry filter options failed")
			return
		}
		industryOptions = append(industryOptions, option)
	}
	if err := industryRows.Err(); err != nil {
		slog.ErrorContext(r.Context(), "read source explorer industry filter options", "source", sourceName, "error", err)
		writeError(w, http.StatusInternalServerError, "read source explorer industry filter options failed")
		return
	}

	writeJSON(w, http.StatusOK, sourceExplorerFilterOptionsResponse{Forms: formOptions, IndustryOptions: industryOptions})
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
		Limit:            limit,
		Offset:           offset,
		Search:           strings.TrimSpace(query.Get("q")),
		Active:           strings.ToLower(strings.TrimSpace(query.Get("active"))),
		LifecycleStatus:  strings.ToLower(strings.TrimSpace(query.Get("lifecycle_status"))),
		CompanyFormCodes: parseSourceExplorerStringList(query["form"], 100),
		IndustryNACE:     parseSourceExplorerStringList(query["industry_nace"], 100),
		SourceIndustries: parseSourceExplorerSourceIndustries(query["source_industry"], 100),
		Sort:             strings.TrimSpace(query.Get("sort")),
		Direction:        direction,
	}
}

func buildSourceExplorerFormFilterOptionsQuery(table string) string {
	return `SELECT
  form_code,
  argMax(description, latest_ingested_at) AS description,
  count() AS company_count
FROM (
  SELECT
    ifNull(company_form_code, '') AS form_code,
    ifNull(company_form_description_en, '') AS description,
    latest_ingested_at
  FROM ` + table + `
  WHERE ifNull(company_form_code, '') != ''
)
GROUP BY form_code
ORDER BY lowerUTF8(description), form_code
LIMIT 1000`
}

func buildSourceExplorerIndustryFilterOptionsQuery(table string) string {
	naceReferenceTable := ch.QualifiedTable("corpscout_reference", "nace_codes")
	return `SELECT
  id,
  kind,
  filter_value,
  revision,
  level_name,
  code,
  code_set,
  title,
  breadcrumb,
  mapped_nace_code,
  company_count,
  search_text
FROM (
  SELECT
    concat('nace:', cache.revision, ':section:', cache.code) AS id,
    'nace' AS kind,
    cache.code AS filter_value,
    cache.revision AS revision,
    'section' AS level_name,
    cache.code AS code,
    '' AS code_set,
    ifNull(nace_ref.title, cache.code) AS title,
    ifNull(nace_ref.title, cache.code) AS breadcrumb,
    cache.code AS mapped_nace_code,
    cache.company_count AS company_count,
    concat(cache.code, ' ', ifNull(nace_ref.title, cache.code)) AS search_text
  FROM (
    SELECT
      ifNull(nace_revision, '') AS revision,
      ifNull(nace_section_code, '') AS code,
      count() AS company_count
    FROM ` + table + `
    WHERE ifNull(nace_revision, '') != ''
      AND ifNull(nace_section_code, '') != ''
    GROUP BY revision, code
  ) AS cache
  LEFT JOIN ` + naceReferenceTable + ` AS nace_ref
    ON nace_ref.revision = cache.revision
   AND nace_ref.level_name = 'section'
   AND nace_ref.code = cache.code

  UNION ALL

  SELECT
    concat('nace:', cache.revision, ':division:', cache.code) AS id,
    'nace' AS kind,
    cache.code AS filter_value,
    cache.revision AS revision,
    'division' AS level_name,
    cache.code AS code,
    '' AS code_set,
    ifNull(nace_ref.title, cache.code) AS title,
    ifNull(nace_ref.title, cache.code) AS breadcrumb,
    cache.code AS mapped_nace_code,
    cache.company_count AS company_count,
    concat(cache.code, ' ', ifNull(nace_ref.title, cache.code)) AS search_text
  FROM (
    SELECT
      ifNull(nace_revision, '') AS revision,
      ifNull(nace_division_code, '') AS code,
      count() AS company_count
    FROM ` + table + `
    WHERE ifNull(nace_revision, '') != ''
      AND ifNull(nace_division_code, '') != ''
    GROUP BY revision, code
  ) AS cache
  LEFT JOIN ` + naceReferenceTable + ` AS nace_ref
    ON nace_ref.revision = cache.revision
   AND nace_ref.level_name = 'division'
   AND nace_ref.code = cache.code

  UNION ALL

  SELECT
    concat('nace:', cache.revision, ':group:', cache.code) AS id,
    'nace' AS kind,
    cache.code AS filter_value,
    cache.revision AS revision,
    'group' AS level_name,
    cache.code AS code,
    '' AS code_set,
    ifNull(nace_ref.title, cache.code) AS title,
    ifNull(nace_ref.title, cache.code) AS breadcrumb,
    cache.code AS mapped_nace_code,
    cache.company_count AS company_count,
    concat(cache.code, ' ', ifNull(nace_ref.title, cache.code)) AS search_text
  FROM (
    SELECT
      ifNull(nace_revision, '') AS revision,
      ifNull(nace_group_code, '') AS code,
      count() AS company_count
    FROM ` + table + `
    WHERE ifNull(nace_revision, '') != ''
      AND ifNull(nace_group_code, '') != ''
    GROUP BY revision, code
  ) AS cache
  LEFT JOIN ` + naceReferenceTable + ` AS nace_ref
    ON nace_ref.revision = cache.revision
   AND nace_ref.level_name = 'group'
   AND nace_ref.code = cache.code

  UNION ALL

  SELECT
    concat('nace:', revision, ':class:', code) AS id,
    'nace' AS kind,
    code AS filter_value,
    revision,
    'class' AS level_name,
    code,
    '' AS code_set,
    title,
    title AS breadcrumb,
    code AS mapped_nace_code,
    company_count,
    concat(code, ' ', title) AS search_text
  FROM (
    SELECT
      ifNull(nace_revision, '') AS revision,
      ifNull(nace_class_code, '') AS code,
      argMax(ifNull(nace_title_en, ''), latest_ingested_at) AS title,
      count() AS company_count
    FROM ` + table + `
    WHERE ifNull(nace_revision, '') != ''
      AND ifNull(nace_class_code, '') != ''
    GROUP BY revision, code
  ) AS class_cache

  UNION ALL

  SELECT
    concat('source_industry:', code_set, ':', code) AS id,
    'source_industry' AS kind,
    concat(code_set, ':', code) AS filter_value,
    revision,
    '' AS level_name,
    code,
    code_set,
    title,
    concat(code_set, ' ', code) AS breadcrumb,
    mapped_nace_code,
    company_count,
    concat(code_set, ' ', code, ' ', title, ' ', mapped_nace_code) AS search_text
  FROM (
    SELECT
      ifNull(main_business_line_code_set, '') AS code_set,
      ifNull(main_business_line_code, '') AS code,
      argMax(ifNull(main_business_line_description_en, ''), latest_ingested_at) AS title,
      argMax(ifNull(nace_revision, ''), latest_ingested_at) AS revision,
      argMax(ifNull(nace_code, ''), latest_ingested_at) AS mapped_nace_code,
      count() AS company_count
    FROM ` + table + `
    WHERE ifNull(main_business_line_code_set, '') != ''
      AND ifNull(main_business_line_code, '') != ''
    GROUP BY code_set, code
  ) AS source_cache
) AS industry_options
ORDER BY company_count DESC, lowerUTF8(title), code
LIMIT 5000`
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
  ifNull(business_id, ''),
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
  ifNull(main_business_line_code_set, ''),
  ifNull(nace_revision, ''),
  ifNull(nace_code, ''),
  ifNull(nace_section_code, ''),
  ifNull(nace_division_code, ''),
  ifNull(nace_group_code, ''),
  ifNull(nace_class_code, ''),
  ifNull(nace_title_en, ''),
  ifNull(nace_mapping_status, ''),
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
	if len(params.CompanyFormCodes) > 0 {
		clauses = append(clauses, "ifNull(company_form_code, '') IN ("+queryPlaceholders(len(params.CompanyFormCodes))+")")
		for _, code := range params.CompanyFormCodes {
			args = append(args, code)
		}
	}
	if len(params.IndustryNACE) > 0 {
		naceClauses := make([]string, 0, len(params.IndustryNACE))
		for _, value := range params.IndustryNACE {
			column, ok := sourceExplorerNACEFilterColumn(value)
			if !ok {
				continue
			}
			naceClauses = append(naceClauses, "ifNull("+column+", '') = ?")
			if column == "nace_section_code" {
				args = append(args, strings.ToUpper(value))
				continue
			}
			args = append(args, value)
		}
		if len(naceClauses) > 0 {
			clauses = append(clauses, "("+strings.Join(naceClauses, " OR ")+")")
		}
	}
	if len(params.SourceIndustries) > 0 {
		sourceIndustryClauses := make([]string, 0, len(params.SourceIndustries))
		for _, filter := range params.SourceIndustries {
			sourceIndustryClauses = append(sourceIndustryClauses, "(ifNull(main_business_line_code_set, '') = ? AND ifNull(main_business_line_code, '') = ?)")
			args = append(args, filter.CodeSet, filter.Code)
		}
		clauses = append(clauses, "("+strings.Join(sourceIndustryClauses, " OR ")+")")
	}
	if len(clauses) == 0 {
		return "", args
	}
	return " WHERE " + strings.Join(clauses, " AND "), args
}

func parseSourceExplorerStringList(values []string, maxItems int) []string {
	if maxItems <= 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, raw := range values {
		for _, value := range strings.Split(raw, ",") {
			trimmed := strings.TrimSpace(value)
			if trimmed == "" {
				continue
			}
			if _, ok := seen[trimmed]; ok {
				continue
			}
			seen[trimmed] = struct{}{}
			result = append(result, trimmed)
			if len(result) >= maxItems {
				return result
			}
		}
	}
	return result
}

func parseSourceExplorerSourceIndustries(values []string, maxItems int) []sourceExplorerSourceIndustryFilter {
	if maxItems <= 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	result := make([]sourceExplorerSourceIndustryFilter, 0, len(values))
	for _, raw := range values {
		for _, value := range strings.Split(raw, ",") {
			trimmed := strings.TrimSpace(value)
			if trimmed == "" {
				continue
			}
			codeSet, code, ok := strings.Cut(trimmed, ":")
			if !ok {
				continue
			}
			codeSet = strings.TrimSpace(codeSet)
			code = strings.TrimSpace(code)
			if codeSet == "" || code == "" {
				continue
			}
			key := codeSet + ":" + code
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			result = append(result, sourceExplorerSourceIndustryFilter{
				CodeSet: codeSet,
				Code:    code,
			})
			if len(result) >= maxItems {
				return result
			}
		}
	}
	return result
}

func sourceExplorerNACEFilterColumn(value string) (string, bool) {
	if len(value) == 1 {
		char := value[0]
		if (char >= 'A' && char <= 'Z') || (char >= 'a' && char <= 'z') {
			return "nace_section_code", true
		}
		return "", false
	}
	if len(value) == 2 && isASCIIInteger(value) {
		return "nace_division_code", true
	}
	if len(value) == 4 && value[2] == '.' && isASCIIInteger(value[:2]) && isASCIIInteger(value[3:]) {
		return "nace_group_code", true
	}
	if len(value) == 5 && value[2] == '.' && isASCIIInteger(value[:2]) && isASCIIInteger(value[3:]) {
		return "nace_class_code", true
	}
	return "", false
}

func isASCIIInteger(value string) bool {
	if value == "" {
		return false
	}
	for i := 0; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}

func queryPlaceholders(count int) string {
	if count <= 0 {
		return ""
	}
	parts := make([]string, count)
	for i := range parts {
		parts[i] = "?"
	}
	return strings.Join(parts, ", ")
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
