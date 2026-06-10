package prhytj

import (
	"context"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"

	ch "github.com/pulsarpoint/corpscout/scheduler/internal/clickhouse"
)

const (
	CompanyExplorerViewTable  = "fi_prhytj_company_explorer"
	CompanyExplorerCacheTable = "fi_prhytj_company_explorer_cache"
)

var companyExplorerCacheColumns = []string{
	"business_id",
	"country_iso2",
	"source_slug",
	"source_run_id",
	"source_record_id",
	"name",
	"registration_date",
	"end_date",
	"status_code",
	"status_description",
	"trade_register_status_code",
	"trade_register_status_description",
	"lifecycle_status",
	"is_active",
	"main_business_line_code",
	"main_business_line_code_set",
	"main_business_line_description_en",
	"company_form_code",
	"company_form_description_en",
	"company_situation_code",
	"company_situation_description_en",
	"website",
	"name_history",
	"registered_entries",
	"company_forms",
	"company_situations",
	"websites",
	"addresses",
	"source_payload_hash",
	"latest_ingested_at",
	"nace_revision",
	"nace_code",
	"nace_normalized_code",
	"nace_section_code",
	"nace_division_code",
	"nace_group_code",
	"nace_class_code",
	"nace_title_en",
	"nace_mapping_method",
	"nace_mapping_status",
	"refreshed_at",
}

type CompanyExplorerCacheRefreshResult struct {
	CacheTable  string    `json:"cache_table"`
	Rows        uint64    `json:"rows"`
	RefreshedAt time.Time `json:"refreshed_at"`
}

func RefreshCompanyExplorerCache(ctx context.Context, clickHouseNativeURL string) (CompanyExplorerCacheRefreshResult, error) {
	if strings.TrimSpace(clickHouseNativeURL) == "" {
		return CompanyExplorerCacheRefreshResult{}, errors.New("clickhouse native url is required")
	}
	writer, err := ch.Open(ctx, clickHouseNativeURL)
	if err != nil {
		return CompanyExplorerCacheRefreshResult{}, errors.Wrap(err, "open clickhouse writer")
	}
	defer writer.Close()

	tempTable := CompanyExplorerCacheTable + "_refresh_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	database := writer.Database()
	tempQualified := ch.QualifiedTable(database, tempTable)
	cacheQualified := ch.QualifiedTable(database, CompanyExplorerCacheTable)
	viewQualified := ch.QualifiedTable(database, CompanyExplorerViewTable)

	if err := writer.Exec(ctx, "DROP TABLE IF EXISTS "+tempQualified); err != nil {
		return CompanyExplorerCacheRefreshResult{}, errors.Wrap(err, "drop stale explorer cache refresh table")
	}
	tempCreated := false
	defer func() {
		if tempCreated {
			_ = writer.Exec(context.Background(), "DROP TABLE IF EXISTS "+tempQualified)
		}
	}()

	if err := writer.Exec(ctx, "CREATE TABLE "+tempQualified+" AS "+cacheQualified); err != nil {
		return CompanyExplorerCacheRefreshResult{}, errors.Wrap(err, "create explorer cache refresh table")
	}
	tempCreated = true
	if err := writer.Exec(ctx, ch.BuildTruncateQuery(database, tempTable)); err != nil {
		return CompanyExplorerCacheRefreshResult{}, errors.Wrap(err, "clear explorer cache refresh table")
	}

	refreshedAt := time.Now().UTC().Truncate(time.Millisecond)
	if err := writer.Exec(ctx, buildCompanyExplorerCacheInsertQuery(database, tempTable, viewQualified, refreshedAt)); err != nil {
		return CompanyExplorerCacheRefreshResult{}, errors.Wrap(err, "load explorer cache refresh table")
	}

	var rows uint64
	if err := writer.QueryRow(ctx, "SELECT count() FROM "+tempQualified).Scan(&rows); err != nil {
		return CompanyExplorerCacheRefreshResult{}, errors.Wrap(err, "count explorer cache refresh table")
	}
	if err := writer.Exec(ctx, "EXCHANGE TABLES "+cacheQualified+" AND "+tempQualified); err != nil {
		return CompanyExplorerCacheRefreshResult{}, errors.Wrap(err, "swap explorer cache table")
	}

	return CompanyExplorerCacheRefreshResult{
		CacheTable:  database + "." + CompanyExplorerCacheTable,
		Rows:        rows,
		RefreshedAt: refreshedAt,
	}, nil
}

func buildCompanyExplorerCacheInsertQuery(database string, table string, view string, refreshedAt time.Time) string {
	quotedColumns := make([]string, 0, len(companyExplorerCacheColumns))
	selectColumns := make([]string, 0, len(companyExplorerCacheColumns))
	for _, column := range companyExplorerCacheColumns {
		quotedColumns = append(quotedColumns, ch.QuoteIdent(column))
		selectColumns = append(selectColumns, companyExplorerCacheSelectExpression(column, refreshedAt))
	}
	return "INSERT INTO " + ch.QualifiedTable(database, table) + " (" + strings.Join(quotedColumns, ", ") + ")\n" +
		"SELECT " + strings.Join(selectColumns, ", ") + "\n" +
		"FROM " + view + " AS explorer\n" +
		"LEFT JOIN " + ch.QualifiedTable(database, IndustryNACEMappingTable) + " AS industry_mapping\n" +
		"  ON ifNull(industry_mapping.source_code_set, '') = ifNull(explorer.main_business_line_code_set, '')\n" +
		" AND ifNull(industry_mapping.source_code, '') = ifNull(explorer.main_business_line_code, '')"
}

func companyExplorerCacheSelectExpression(column string, refreshedAt time.Time) string {
	switch column {
	case "refreshed_at":
		return clickHouseDateTime64Literal(refreshedAt) + " AS " + ch.QuoteIdent(column)
	case "nace_revision", "nace_code", "nace_normalized_code", "nace_section_code", "nace_division_code", "nace_group_code", "nace_class_code", "nace_title_en":
		return "industry_mapping." + ch.QuoteIdent(column) + " AS " + ch.QuoteIdent(column)
	case "nace_mapping_method":
		return "industry_mapping.mapping_method AS " + ch.QuoteIdent(column)
	case "nace_mapping_status":
		return "industry_mapping.mapping_status AS " + ch.QuoteIdent(column)
	default:
		return "explorer." + ch.QuoteIdent(column) + " AS " + ch.QuoteIdent(column)
	}
}

func clickHouseDateTime64Literal(value time.Time) string {
	return "toDateTime64('" + value.UTC().Format("2006-01-02 15:04:05.000") + "', 3, 'UTC')"
}
