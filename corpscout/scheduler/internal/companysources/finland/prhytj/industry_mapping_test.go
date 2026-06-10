package prhytj

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBuildIndustryNACEMappingInsertQuery(t *testing.T) {
	mappedAt := time.Date(2026, 6, 10, 13, 10, 5, 987000000, time.UTC)
	query := buildIndustryNACEMappingInsertQuery("corpscout_sources", "fi_prhytj_industry_nace_mappings_refresh_test", mappedAt)

	require.Contains(t, query, "INSERT INTO `corpscout_sources`.`fi_prhytj_industry_nace_mappings_refresh_test`")
	require.Contains(t, query, "`corpscout_sources`.`fi_prhytj_business_lines`")
	require.Contains(t, query, "`corpscout_sources`.`fi_prhytj_business_line_descriptions`")
	require.Contains(t, query, "`corpscout_reference`.`nace_codes`")
	require.Contains(t, query, "source_code_set = 'TOIMI4', '2.1'")
	require.Contains(t, query, "source_code_set = 'TOIMI3', '2'")
	require.Contains(t, query, "toDateTime64('2026-06-10 13:10:05.987', 3, 'UTC') AS `mapped_at`")
	require.Contains(t, query, "'toimi_5_digit_prefix'")
	require.Contains(t, query, "'unsupported_code_set'")
	require.Equal(t, 2, strings.Count(query, "`mapped_at`"))
}

func TestIndustryNACEMappingConstants(t *testing.T) {
	require.Equal(t, "fi_prhytj_industry_nace_mappings", IndustryNACEMappingTable)
	require.Contains(t, industryNACEMappingColumns, "source_code_set")
	require.Contains(t, industryNACEMappingColumns, "nace_class_code")
	require.Contains(t, industryNACEMappingColumns, "mapping_status")
}
