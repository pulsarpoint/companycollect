package prhytj

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBuildCompanyExplorerCacheInsertQuery(t *testing.T) {
	refreshedAt := time.Date(2026, 6, 10, 12, 30, 1, 234000000, time.UTC)
	query := buildCompanyExplorerCacheInsertQuery("corpscout_sources", "fi_prhytj_company_explorer_cache_refresh_test", "`corpscout_sources`.`fi_prhytj_company_explorer`", refreshedAt)

	require.Contains(t, query, "INSERT INTO `corpscout_sources`.`fi_prhytj_company_explorer_cache_refresh_test`")
	require.Contains(t, query, "`business_id`")
	require.Contains(t, query, "toDateTime64('2026-06-10 12:30:01.234', 3, 'UTC') AS `refreshed_at`")
	require.Contains(t, query, "FROM `corpscout_sources`.`fi_prhytj_company_explorer`")
	require.Equal(t, 2, strings.Count(query, "`refreshed_at`"))
}
