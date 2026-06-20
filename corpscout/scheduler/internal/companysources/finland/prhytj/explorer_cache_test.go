package prhytj

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBuildCompanyExplorerCacheInsertQuery(t *testing.T) {
	refreshedAt := time.Date(2026, 6, 10, 12, 30, 1, 234000000, time.UTC)
	query := buildCompanyExplorerCacheInsertQuery("corpscout", "fi_prhytj_company_explorer_cache_refresh_test", "`corpscout`.`fi_prhytj_company_explorer`", refreshedAt)

	require.Contains(t, query, "INSERT INTO `corpscout`.`fi_prhytj_company_explorer_cache_refresh_test`")
	require.Contains(t, query, "`business_id`")
	require.Contains(t, query, "toDateTime64('2026-06-10 12:30:01.234', 3, 'UTC') AS `refreshed_at`")
	require.Contains(t, query, "FROM `corpscout`.`fi_prhytj_company_explorer`")
	require.Equal(t, 2, strings.Count(query, "`refreshed_at`"))
}

func TestBuildCompanyExplorerCacheInsertQueryJoinsIndustryNACEMapping(t *testing.T) {
	refreshedAt := time.Date(2026, 6, 10, 14, 0, 0, 0, time.UTC)
	query := buildCompanyExplorerCacheInsertQuery("corpscout", "fi_prhytj_company_explorer_cache_refresh_test", "`corpscout`.`fi_prhytj_company_explorer`", refreshedAt)

	require.Contains(t, query, "LEFT JOIN `corpscout`.`fi_prhytj_industry_nace_mappings` AS industry_mapping")
	require.Contains(t, query, "industry_mapping.`nace_revision` AS `nace_revision`")
	require.Contains(t, query, "industry_mapping.`nace_code` AS `nace_code`")
	require.Contains(t, query, "industry_mapping.`nace_title_en` AS `nace_title_en`")
	require.Contains(t, query, "industry_mapping.mapping_status AS `nace_mapping_status`")
	require.Contains(t, query, "ifNull(industry_mapping.source_code_set, '') = ifNull(explorer.main_business_line_code_set, '')")
	require.Contains(t, query, "ifNull(industry_mapping.source_code, '') = ifNull(explorer.main_business_line_code, '')")
}
