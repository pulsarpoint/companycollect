package prhytj

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizedTablesDoNotIncludeRawOrCompanySummaryTables(t *testing.T) {
	require.NotContains(t, NormalizedTableNames(), "fi_prhytj_"+"raw_records")
	require.NotContains(t, NormalizedTableNames(), "fi_prhytj_"+"companies")
	require.Equal(t, []string{
		"fi_prhytj_identifiers",
		"fi_prhytj_statuses",
		"fi_prhytj_names",
		"fi_prhytj_business_lines",
		"fi_prhytj_business_line_descriptions",
		"fi_prhytj_websites",
		"fi_prhytj_company_forms",
		"fi_prhytj_company_form_descriptions",
		"fi_prhytj_company_situations",
		"fi_prhytj_company_situation_descriptions",
		"fi_prhytj_registered_entries",
		"fi_prhytj_registered_entry_descriptions",
		"fi_prhytj_addresses",
		"fi_prhytj_address_post_offices",
	}, NormalizedTableNames())
}

func TestEveryNormalizedTableHasLineageColumns(t *testing.T) {
	for _, table := range NormalizedTables() {
		for _, column := range []string{
			"country_iso2",
			"source_slug",
			"source_run_id",
			"source_record_id",
			"business_id",
			"source_line_number",
			"source_payload_hash",
			"ingested_at",
			"source_export_id",
		} {
			require.Containsf(t, table.Columns, column, "%s missing %s", table.Name, column)
		}
		require.NotContains(t, table.Columns, "raw_payload_"+"json")
	}
}

func TestClickHouseRowsMatchDeclaredColumns(t *testing.T) {
	require.ElementsMatch(t, identifierColumns, mapKeys(IdentifierRow{}.ClickHouseRow()))
	require.ElementsMatch(t, statusColumns, mapKeys(StatusRow{}.ClickHouseRow()))
	require.ElementsMatch(t, nameColumns, mapKeys(NameRow{}.ClickHouseRow()))
	require.ElementsMatch(t, businessLineColumns, mapKeys(BusinessLineRow{}.ClickHouseRow()))
	require.ElementsMatch(t, businessLineDescriptionColumns, mapKeys(BusinessLineDescriptionRow{}.ClickHouseRow()))
	require.ElementsMatch(t, websiteColumns, mapKeys(WebsiteRow{}.ClickHouseRow()))
	require.ElementsMatch(t, companyFormColumns, mapKeys(CompanyFormRow{}.ClickHouseRow()))
	require.ElementsMatch(t, companyFormDescriptionColumns, mapKeys(CompanyFormDescriptionRow{}.ClickHouseRow()))
	require.ElementsMatch(t, companySituationColumns, mapKeys(CompanySituationRow{}.ClickHouseRow()))
	require.ElementsMatch(t, companySituationDescriptionColumns, mapKeys(CompanySituationDescriptionRow{}.ClickHouseRow()))
	require.ElementsMatch(t, registeredEntryColumns, mapKeys(RegisteredEntryRow{}.ClickHouseRow()))
	require.ElementsMatch(t, registeredEntryDescriptionColumns, mapKeys(RegisteredEntryDescriptionRow{}.ClickHouseRow()))
	require.ElementsMatch(t, addressColumns, mapKeys(AddressRow{}.ClickHouseRow()))
	require.ElementsMatch(t, addressPostOfficeColumns, mapKeys(AddressPostOfficeRow{}.ClickHouseRow()))
}

func TestMigrationColumnsAndTypesMatchDeclaredSchema(t *testing.T) {
	migrationPath := filepath.Join("..", "..", "..", "..", "..", "clickhouse", "migrations", "000004_replace_finland_prhytj_normalized_tables.up.sql")
	body, err := os.ReadFile(migrationPath)
	require.NoError(t, err)
	columnsByTable := migrationColumnsAndTypes(string(body))

	for _, table := range NormalizedTables() {
		actual := columnsByTable[table.Name]
		require.NotEmpty(t, actual, table.Name)
		require.ElementsMatch(t, table.Columns, mapKeys(actual), table.Name)
		require.Equal(t, table.ColumnTypes, actual, table.Name)
	}
}

func mapKeys[T any](row map[string]T) []string {
	keys := make([]string, 0, len(row))
	for key := range row {
		keys = append(keys, key)
	}
	return keys
}

func migrationColumnsAndTypes(sql string) map[string]map[string]string {
	result := map[string]map[string]string{}
	var currentTable string
	for _, line := range strings.Split(sql, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prhytj_") {
			parts := strings.Split(trimmed, "`")
			if len(parts) >= 4 {
				currentTable = parts[3]
				result[currentTable] = map[string]string{}
			}
			continue
		}
		if currentTable == "" {
			continue
		}
		if strings.HasPrefix(trimmed, ")") {
			currentTable = ""
			continue
		}
		if strings.HasPrefix(trimmed, "`") {
			parts := strings.Split(trimmed, "`")
			if len(parts) >= 2 {
				result[currentTable][parts[1]] = strings.TrimSuffix(strings.TrimSpace(parts[2]), ",")
			}
		}
	}
	return result
}
