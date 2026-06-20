package db

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClickHouseFinlandPRHXBRLMigrationShape(t *testing.T) {
	up, err := os.ReadFile(filepath.Join("..", "..", "..", "clickhouse", "migrations", "000011_create_finland_prh_xbrl_financial_tables.up.sql"))
	require.NoError(t, err)
	down, err := os.ReadFile(filepath.Join("..", "..", "..", "clickhouse", "migrations", "000011_create_finland_prh_xbrl_financial_tables.down.sql"))
	require.NoError(t, err)

	sql := string(up)
	for _, needle := range []string{
		"CREATE TABLE IF NOT EXISTS `corpscout`.`fi_prh_xbrl_statement_documents`",
		"CREATE TABLE IF NOT EXISTS `corpscout`.`fi_prh_xbrl_contexts`",
		"CREATE TABLE IF NOT EXISTS `corpscout`.`fi_prh_xbrl_units`",
		"CREATE TABLE IF NOT EXISTS `corpscout`.`fi_prh_xbrl_facts_raw`",
		"CREATE TABLE IF NOT EXISTS `corpscout`.`fi_prh_xbrl_taxonomy_code_map`",
		"CREATE TABLE IF NOT EXISTS `corpscout`.`fi_prh_xbrl_metrics_long_v1`",
		"`statement_key` String",
		"`business_id` String",
		"`financial_date` Date",
		"`period_start` Nullable(Date)",
		"`period_end` Nullable(Date)",
		"`concept_qname` LowCardinality(String)",
		"`mcy_member_code` Nullable(String)",
		"`ref_member_code` Nullable(String)",
		"`period_reference` LowCardinality(String)",
		"`metric_key` LowCardinality(String)",
		"ENGINE = ReplacingMergeTree(`parsed_at`)",
		"ENGINE = ReplacingMergeTree(`loaded_at`)",
		"ENGINE = ReplacingMergeTree(`derived_at`)",
		"ORDER BY (`business_id`, `financial_date`, `concept_qname`, `mcy_member_code`, `ref_member_code`, `fact_ordinal`)",
	} {
		require.Contains(t, sql, needle)
	}

	downSQL := string(down)
	for _, needle := range []string{
		"DROP TABLE IF EXISTS `corpscout`.`fi_prh_xbrl_metrics_long_v1`",
		"DROP TABLE IF EXISTS `corpscout`.`fi_prh_xbrl_facts_raw`",
		"DROP TABLE IF EXISTS `corpscout`.`fi_prh_xbrl_taxonomy_code_map`",
		"DROP TABLE IF EXISTS `corpscout`.`fi_prh_xbrl_units`",
		"DROP TABLE IF EXISTS `corpscout`.`fi_prh_xbrl_contexts`",
		"DROP TABLE IF EXISTS `corpscout`.`fi_prh_xbrl_statement_documents`",
	} {
		require.Contains(t, downSQL, needle)
	}
}
