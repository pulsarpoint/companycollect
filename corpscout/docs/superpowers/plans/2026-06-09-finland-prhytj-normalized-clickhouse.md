# Finland PRH YTJ Normalized ClickHouse Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the temporary Finland PRH YTJ two-table ClickHouse import with a normalized-only source schema and importer.

**Architecture:** Keep PRH raw JSON only in the downloaded run folder. Corpscout parses `source.ndjson`, attaches line/hash lineage, normalizes every known PRH structure into source-specific ClickHouse tables, and imports those tables in batches. Because `000002_create_finland_prhytj_tables` has already been applied remotely, add `000004_replace_finland_prhytj_normalized_tables` to drop old PRH tables and create the new normalized schema.

**Tech Stack:** Go 1.26.1, `github.com/cockroachdb/errors`, `github.com/ClickHouse/clickhouse-go/v2`, ClickHouse SQL migrations, existing Corpscout Makefile.

---

## Scope

Implement this for `finland/prhytj` only.

This plan removes active use of:

- `fi_prhytj_raw_records`
- `fi_prhytj_companies`

This plan creates/imports normalized tables:

- `fi_prhytj_identifiers`
- `fi_prhytj_statuses`
- `fi_prhytj_names`
- `fi_prhytj_business_lines`
- `fi_prhytj_business_line_descriptions`
- `fi_prhytj_websites`
- `fi_prhytj_company_forms`
- `fi_prhytj_company_form_descriptions`
- `fi_prhytj_company_situations`
- `fi_prhytj_company_situation_descriptions`
- `fi_prhytj_registered_entries`
- `fi_prhytj_registered_entry_descriptions`
- `fi_prhytj_addresses`
- `fi_prhytj_address_post_offices`

Do not touch unrelated dirty workspace files. Stage only files listed in each task.

## File Structure

- `corpscout/clickhouse/migrations/000004_replace_finland_prhytj_normalized_tables.up.sql`
  - Drops old PRH tables and creates the normalized schema.
- `corpscout/clickhouse/migrations/000004_replace_finland_prhytj_normalized_tables.down.sql`
  - Drops the normalized replacement tables. Do not recreate old raw/company tables.
- `corpscout/scheduler/internal/db/clickhouse_finland_prhytj_normalized_migration_test.go`
  - Text-level migration contract test.
- `corpscout/scheduler/internal/companysources/finland/prhytj/parser.go`
  - Change parser callback from `CompanyRecord` to `ParsedRecord`.
- `corpscout/scheduler/internal/companysources/finland/prhytj/parser_test.go`
  - Verify line number and payload hash are provided by parser.
- `corpscout/scheduler/internal/companysources/finland/prhytj/rows.go`
  - Table names, typed row structs, column lists, `ClickHouseRow` methods, table batch metadata.
- `corpscout/scheduler/internal/companysources/finland/prhytj/rows_test.go`
  - Column/table contract tests and guards against old table names.
- `corpscout/scheduler/internal/companysources/finland/prhytj/normalize.go`
  - `CompanyRecord` to normalized row batch mapping.
- `corpscout/scheduler/internal/companysources/finland/prhytj/normalize_test.go`
  - Tests for every PRH nested structure.
- `corpscout/scheduler/internal/clickhouse/writer.go`
  - Native Go ClickHouse writer using `github.com/ClickHouse/clickhouse-go/v2`.
- `corpscout/scheduler/internal/clickhouse/writer_test.go`
  - URL/query/insert-shape tests for the native writer without requiring a live server.
- `corpscout/scheduler/internal/clickhouseclient/json_each_row.go`
  - Delete after PRH importer stops using it.
- `corpscout/scheduler/internal/clickhouseclient/json_each_row_test.go`
  - Delete after native writer tests exist.
- `corpscout/scheduler/internal/clickhouseclient/url.go`
  - Delete after native writer owns ClickHouse URL parsing.
- `corpscout/scheduler/internal/companysources/finland/prhytj/import.go`
  - Batch parser/normalizer/importer orchestration through the native ClickHouse writer. Remove old raw/company row builders.
- `corpscout/scheduler/internal/companysources/finland/prhytj/import_test.go`
  - Import result/table-name tests updated for normalized tables.
- `corpscout/scheduler/internal/companysources/unitedstates/coloradoentities/import.go`
  - Switch existing U.S. Colorado importer from Docker client to native writer.
- `corpscout/scheduler/internal/companysources/unitedstates/irseobmf/import.go`
  - Switch existing IRS EO BMF importer from Docker client to native writer.
- `corpscout/scheduler/internal/companysources/unitedstates/secedgar/import.go`
  - Switch existing SEC EDGAR importer from Docker client to native writer.
- `corpscout/scheduler/internal/companysources/source.go`
  - Remove `ClickHouseImage` from source import request/options structs.
- `corpscout/scheduler/internal/companysources/importer.go`
  - Stop passing `ClickHouseImage`.
- `corpscout/scheduler/cmd/corpscout-source/main.go`
  - Remove `--clickhouse-image` CLI flags and config fields.
- `corpscout/clickhouse/sources/finland_prhytj.yaml`
  - Delete stale pilot config that still points at old PRH raw/company tables.
- `corpscout/clickhouse/tools/chimport/main.go`
  - Delete stale Docker/clickhouse-local import CLI.
- `corpscout/clickhouse/tools/chimport/importer.go`
  - Delete stale Docker/clickhouse-local import implementation.
- `corpscout/clickhouse/tools/chimport/importer_test.go`
  - Delete stale Docker/clickhouse-local import tests.

## Task 1: Add ClickHouse Migration Contract Test

**Files:**
- Create: `corpscout/scheduler/internal/db/clickhouse_finland_prhytj_normalized_migration_test.go`

- [ ] **Step 1: Write failing migration contract test**

Create `corpscout/scheduler/internal/db/clickhouse_finland_prhytj_normalized_migration_test.go`:

```go
package db

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClickHouseFinlandPRHYTJNormalizedMigrationReplacesOldTables(t *testing.T) {
	upPath := filepath.Join("..", "..", "..", "clickhouse", "migrations", "000004_replace_finland_prhytj_normalized_tables.up.sql")
	downPath := filepath.Join("..", "..", "..", "clickhouse", "migrations", "000004_replace_finland_prhytj_normalized_tables.down.sql")

	up, err := os.ReadFile(upPath)
	require.NoError(t, err)
	down, err := os.ReadFile(downPath)
	require.NoError(t, err)

	upSQL := string(up)
	downSQL := string(down)
	require.Contains(t, upSQL, "DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_raw_records`")
	require.Contains(t, upSQL, "DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_companies`")
	require.NotContains(t, upSQL, "CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prhytj_raw_records`")
	require.NotContains(t, upSQL, "CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prhytj_companies`")

	for _, table := range []string{
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
	} {
		require.Contains(t, upSQL, "CREATE TABLE IF NOT EXISTS `corpscout_sources`.`"+table+"`")
		require.Contains(t, downSQL, "DROP TABLE IF EXISTS `corpscout_sources`.`"+table+"`")
	}

	require.Equal(t, 14, strings.Count(upSQL, "CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prhytj_"))
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db -run TestClickHouseFinlandPRHYTJNormalizedMigrationReplacesOldTables -count=1 -v
```

Expected: FAIL because migration `000004_replace_finland_prhytj_normalized_tables` does not exist.

- [ ] **Step 3: Commit test only**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/scheduler/internal/db/clickhouse_finland_prhytj_normalized_migration_test.go
git commit -m "test: specify normalized finland prhytj clickhouse migration"
```

## Task 2: Add Replacement ClickHouse Migration

**Files:**
- Create: `corpscout/clickhouse/migrations/000004_replace_finland_prhytj_normalized_tables.up.sql`
- Create: `corpscout/clickhouse/migrations/000004_replace_finland_prhytj_normalized_tables.down.sql`

- [ ] **Step 1: Create `up` migration**

Create `corpscout/clickhouse/migrations/000004_replace_finland_prhytj_normalized_tables.up.sql` with this structure:

```sql
DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_raw_records`;
DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_companies`;
DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_addresses`;
DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_company_names`;
DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_industries`;
DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_legal_forms`;
DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_registered_entries`;
DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_tax_registrations`;
DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_websites`;

CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prhytj_identifiers` (
  `country_iso2` Nullable(String),
  `source_slug` Nullable(String),
  `source_run_id` Nullable(String),
  `source_record_id` Nullable(String),
  `business_id` Nullable(String),
  `source_line_number` Nullable(Int64),
  `source_payload_hash` Nullable(String),
  `source_item_hash` Nullable(String),
  `source_position` Nullable(Int32),
  `identifier_scope` Nullable(String),
  `identifier_type` Nullable(String),
  `identifier_value` Nullable(String),
  `registered_on` Nullable(String),
  `ended_on` Nullable(String),
  `source` Nullable(String),
  `is_primary_business_id` Nullable(Bool),
  `ingested_at` DateTime64(3, 'UTC'),
  `source_export_id` UUID
)
ENGINE = ReplacingMergeTree
ORDER BY (`business_id`, `identifier_scope`, `identifier_value`, `source_item_hash`)
SETTINGS allow_nullable_key = 1;
```

Create the remaining 13 tables in the same migration. Use the column lists from
Task 4 as the exact migration column names, use `ReplacingMergeTree`, include
`SETTINGS allow_nullable_key = 1`, and use these `ORDER BY` keys:

```text
fi_prhytj_statuses: (`business_id`, `source_run_id`, `source_payload_hash`)
fi_prhytj_names: (`business_id`, `source_position`, `source_item_hash`)
fi_prhytj_business_lines: (`business_id`, `source_item_hash`)
fi_prhytj_business_line_descriptions: (`business_id`, `business_line_item_hash`, `language_code`, `source_item_hash`)
fi_prhytj_websites: (`host`, `business_id`, `source_item_hash`)
fi_prhytj_company_forms: (`business_id`, `source_position`, `form_type_code`, `source_item_hash`)
fi_prhytj_company_form_descriptions: (`business_id`, `company_form_item_hash`, `language_code`, `source_item_hash`)
fi_prhytj_company_situations: (`business_id`, `source_position`, `situation_type_code`, `source_item_hash`)
fi_prhytj_company_situation_descriptions: (`business_id`, `company_situation_item_hash`, `language_code`, `source_item_hash`)
fi_prhytj_registered_entries: (`business_id`, `source_position`, `register_code`, `entry_type_code`, `source_item_hash`)
fi_prhytj_registered_entry_descriptions: (`business_id`, `registered_entry_item_hash`, `language_code`, `source_item_hash`)
fi_prhytj_addresses: (`business_id`, `source_position`, `address_type_code`, `source_item_hash`)
fi_prhytj_address_post_offices: (`business_id`, `address_item_hash`, `language_code`, `source_item_hash`)
```

- [ ] **Step 2: Create `down` migration**

Create `corpscout/clickhouse/migrations/000004_replace_finland_prhytj_normalized_tables.down.sql`:

```sql
DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_address_post_offices`;
DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_addresses`;
DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_registered_entry_descriptions`;
DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_registered_entries`;
DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_company_situation_descriptions`;
DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_company_situations`;
DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_company_form_descriptions`;
DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_company_forms`;
DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_websites`;
DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_business_line_descriptions`;
DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_business_lines`;
DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_names`;
DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_statuses`;
DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_identifiers`;
```

Do not recreate `fi_prhytj_raw_records` or `fi_prhytj_companies` in this down migration.

- [ ] **Step 3: Verify migration contract**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db -run TestClickHouseFinlandPRHYTJNormalizedMigrationReplacesOldTables -count=1 -v
```

Expected: PASS.

- [ ] **Step 4: Verify migration command dry-run**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make -n clickhouse-migrate-up
```

Expected: output contains `--add-host companycollect:100.85.212.113` and `migrate/migrate:v4.17.0`.

- [ ] **Step 5: Commit migration**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/clickhouse/migrations/000004_replace_finland_prhytj_normalized_tables.up.sql \
        corpscout/clickhouse/migrations/000004_replace_finland_prhytj_normalized_tables.down.sql
git commit -m "feat: replace finland prhytj clickhouse tables"
```

## Task 3: Add Parsed Record Lineage To Parser

**Files:**
- Modify: `corpscout/scheduler/internal/companysources/finland/prhytj/parser.go`
- Modify: `corpscout/scheduler/internal/companysources/finland/prhytj/parser_test.go`

- [ ] **Step 1: Write failing parser lineage test**

Replace `TestParseSnapshotPreservesRawPayloadAndHash` with:

```go
func TestParseSnapshotReturnsLineageForEachRecord(t *testing.T) {
	payload, err := os.ReadFile(filepath.Join("testdata", "prh_snapshot_mixed.ndjson"))
	require.NoError(t, err)
	sourcePath := filepath.Join(t.TempDir(), "source.ndjson")
	firstPayloadLine := strings.SplitN(string(payload), "\n", 2)[0]
	sourceLine := "  " + firstPayloadLine + "  \n"
	require.NoError(t, os.WriteFile(sourcePath, []byte(sourceLine), 0o644))

	var records []ParsedRecord
	err = ParseSnapshot(context.Background(), sourcePath, func(record ParsedRecord) error {
		records = append(records, record)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, int64(1), records[0].LineNumber)
	require.Len(t, records[0].PayloadHash, 64)
	expectedHash := sha256.Sum256([]byte(strings.TrimSuffix(sourceLine, "\n")))
	require.Equal(t, hex.EncodeToString(expectedHash[:]), records[0].PayloadHash)
	require.NotEmpty(t, records[0].Record.BusinessID.Value)
}
```

Add `crypto/sha256` and `encoding/hex` to the test imports if they are not
already present. The test intentionally wraps the JSON line in spaces so hashing
`bytes.TrimSpace(scanner.Bytes())` fails; lineage must hash the physical scanner
line bytes as read from `source.ndjson`, excluding only the newline consumed by
`bufio.Scanner`.

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/finland/prhytj -run TestParseSnapshotReturnsLineageForEachRecord -count=1 -v
```

Expected: FAIL because `ParsedRecord` does not exist and `ParseSnapshot` still handles `CompanyRecord`.

- [ ] **Step 3: Implement parser lineage**

In `parser.go`, add:

```go
type ParsedRecord struct {
	LineNumber  int64
	PayloadHash string
	Record      CompanyRecord
}
```

Change the parser signature to:

```go
func ParseSnapshot(ctx context.Context, path string, handle func(ParsedRecord) error) error
```

Inside the scanner loop, increment a physical line counter before blank-line handling:

```go
var lineNumber int64
for scanner.Scan() {
	lineNumber++
	if err := ctx.Err(); err != nil {
		return err
	}
	rawLine := append([]byte(nil), scanner.Bytes()...)
	trimmed := bytes.TrimSpace(rawLine)
	if len(trimmed) == 0 {
		continue
	}
	var record CompanyRecord
	if err := json.Unmarshal(trimmed, &record); err != nil {
		return errors.Wrap(err, "decode PRH YTJ record")
	}
	sum := sha256.Sum256(rawLine)
	record.RawPayload = trimmed
	record.PayloadHash = hex.EncodeToString(sum[:])
	if err := handle(ParsedRecord{
		LineNumber:  lineNumber,
		PayloadHash: hex.EncodeToString(sum[:]),
		Record:      record,
	}); err != nil {
		return err
	}
}
```

Keep `record.RawPayload` and `record.PayloadHash` for compatibility during this task, but later row builders must not insert raw payload JSON into ClickHouse.

- [ ] **Step 4: Run parser test**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/finland/prhytj -run TestParseSnapshotReturnsLineageForEachRecord -count=1 -v
```

Expected: PASS.

- [ ] **Step 5: Commit parser lineage**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/scheduler/internal/companysources/finland/prhytj/parser.go \
        corpscout/scheduler/internal/companysources/finland/prhytj/parser_test.go
git commit -m "feat: add finland prhytj parser lineage"
```

## Task 4: Add Normalized Row Model And Column Lists

**Files:**
- Create: `corpscout/scheduler/internal/companysources/finland/prhytj/rows.go`
- Create: `corpscout/scheduler/internal/companysources/finland/prhytj/rows_test.go`

- [ ] **Step 1: Write row contract tests**

Create `rows_test.go`:

```go
package prhytj

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizedTablesDoNotIncludeRawOrCompanySummaryTables(t *testing.T) {
	require.NotContains(t, NormalizedTableNames(), "fi_prhytj_raw_records")
	require.NotContains(t, NormalizedTableNames(), "fi_prhytj_companies")
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
		require.NotContains(t, table.Columns, "raw_payload_json")
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
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/finland/prhytj -run 'TestNormalizedTables|TestEveryNormalizedTable|TestClickHouseRowsMatchDeclaredColumns|TestMigrationColumnsAndTypesMatchDeclaredSchema' -count=1 -v
```

Expected: FAIL because `NormalizedTableNames` and `NormalizedTables` do not exist.

- [ ] **Step 3: Implement `rows.go`**

Create `rows.go` with:

```go
package prhytj

type NormalizedTable struct {
	Name        string
	Columns     []string
	ColumnTypes map[string]string
}

const (
	identifiersTable = "fi_prhytj_identifiers"
	statusesTable = "fi_prhytj_statuses"
	namesTable = "fi_prhytj_names"
	businessLinesTable = "fi_prhytj_business_lines"
	businessLineDescriptionsTable = "fi_prhytj_business_line_descriptions"
	websitesTable = "fi_prhytj_websites"
	companyFormsTable = "fi_prhytj_company_forms"
	companyFormDescriptionsTable = "fi_prhytj_company_form_descriptions"
	companySituationsTable = "fi_prhytj_company_situations"
	companySituationDescriptionsTable = "fi_prhytj_company_situation_descriptions"
	registeredEntriesTable = "fi_prhytj_registered_entries"
	registeredEntryDescriptionsTable = "fi_prhytj_registered_entry_descriptions"
	addressesTable = "fi_prhytj_addresses"
	addressPostOfficesTable = "fi_prhytj_address_post_offices"
)

var identifierColumns = []string{"country_iso2", "source_slug", "source_run_id", "source_record_id", "business_id", "source_line_number", "source_payload_hash", "source_item_hash", "source_position", "identifier_scope", "identifier_type", "identifier_value", "registered_on", "ended_on", "source", "is_primary_business_id", "ingested_at", "source_export_id"}
var statusColumns = []string{"country_iso2", "source_slug", "source_run_id", "source_record_id", "business_id", "source_line_number", "source_payload_hash", "trade_register_status", "status", "registration_date", "end_date", "last_modified", "lifecycle_status", "is_active", "ingested_at", "source_export_id"}
var nameColumns = []string{"country_iso2", "source_slug", "source_run_id", "source_record_id", "business_id", "source_line_number", "source_payload_hash", "source_item_hash", "source_position", "name", "name_type_code", "version", "registered_on", "ended_on", "is_current", "is_primary", "ingested_at", "source_export_id"}
var businessLineColumns = []string{"country_iso2", "source_slug", "source_run_id", "source_record_id", "business_id", "source_line_number", "source_payload_hash", "source_item_hash", "business_line_type", "business_line_code_set", "registered_on", "source", "is_primary", "ingested_at", "source_export_id"}
var businessLineDescriptionColumns = []string{"country_iso2", "source_slug", "source_run_id", "source_record_id", "business_id", "source_line_number", "source_payload_hash", "source_item_hash", "business_line_item_hash", "source_position", "language_code", "description", "ingested_at", "source_export_id"}
var websiteColumns = []string{"country_iso2", "source_slug", "source_run_id", "source_record_id", "business_id", "source_line_number", "source_payload_hash", "source_item_hash", "url", "normalized_url", "host", "path", "registered_on", "ended_on", "is_current", "is_primary", "ingested_at", "source_export_id"}
var companyFormColumns = []string{"country_iso2", "source_slug", "source_run_id", "source_record_id", "business_id", "source_line_number", "source_payload_hash", "source_item_hash", "source_position", "form_type_code", "version", "registered_on", "ended_on", "source", "is_current", "ingested_at", "source_export_id"}
var companyFormDescriptionColumns = []string{"country_iso2", "source_slug", "source_run_id", "source_record_id", "business_id", "source_line_number", "source_payload_hash", "source_item_hash", "company_form_item_hash", "source_position", "language_code", "description", "ingested_at", "source_export_id"}
var companySituationColumns = []string{"country_iso2", "source_slug", "source_run_id", "source_record_id", "business_id", "source_line_number", "source_payload_hash", "source_item_hash", "source_position", "situation_type_code", "registered_on", "ended_on", "is_current", "ingested_at", "source_export_id"}
var companySituationDescriptionColumns = []string{"country_iso2", "source_slug", "source_run_id", "source_record_id", "business_id", "source_line_number", "source_payload_hash", "source_item_hash", "company_situation_item_hash", "source_position", "language_code", "description", "ingested_at", "source_export_id"}
var registeredEntryColumns = []string{"country_iso2", "source_slug", "source_run_id", "source_record_id", "business_id", "source_line_number", "source_payload_hash", "source_item_hash", "source_position", "entry_type_code", "register_code", "authority", "registered_on", "ended_on", "is_current", "ingested_at", "source_export_id"}
var registeredEntryDescriptionColumns = []string{"country_iso2", "source_slug", "source_run_id", "source_record_id", "business_id", "source_line_number", "source_payload_hash", "source_item_hash", "registered_entry_item_hash", "source_position", "language_code", "description", "ingested_at", "source_export_id"}
var addressColumns = []string{"country_iso2", "source_slug", "source_run_id", "source_record_id", "business_id", "source_line_number", "source_payload_hash", "source_item_hash", "source_position", "address_type_code", "street", "post_code", "building_number", "entrance", "apartment_number", "post_office_box", "co", "country", "registered_on", "source", "ingested_at", "source_export_id"}
var addressPostOfficeColumns = []string{"country_iso2", "source_slug", "source_run_id", "source_record_id", "business_id", "source_line_number", "source_payload_hash", "source_item_hash", "address_item_hash", "source_position", "language_code", "city", "municipality_code", "ingested_at", "source_export_id"}

var identifierColumnTypes = map[string]string{"country_iso2": "String", "source_slug": "String", "source_run_id": "String", "source_record_id": "String", "business_id": "String", "source_line_number": "Int64", "source_payload_hash": "String", "source_item_hash": "String", "source_position": "Int32", "identifier_scope": "String", "identifier_type": "String", "identifier_value": "String", "registered_on": "Nullable(Date)", "ended_on": "Nullable(Date)", "source": "String", "is_primary_business_id": "Bool", "ingested_at": "DateTime64(3, 'UTC')", "source_export_id": "UUID"}

// Add one ColumnTypes map per normalized table. The map values must exactly
// match the ClickHouse migration type fragments, for example
// "DateTime64(3, 'UTC')", "Nullable(Date)", "Int32", "UUID", "Bool".

func NormalizedTables() []NormalizedTable {
	return []NormalizedTable{
		{Name: identifiersTable, Columns: identifierColumns, ColumnTypes: identifierColumnTypes},
		{Name: statusesTable, Columns: statusColumns, ColumnTypes: statusColumnTypes},
		{Name: namesTable, Columns: nameColumns, ColumnTypes: nameColumnTypes},
		{Name: businessLinesTable, Columns: businessLineColumns, ColumnTypes: businessLineColumnTypes},
		{Name: businessLineDescriptionsTable, Columns: businessLineDescriptionColumns, ColumnTypes: businessLineDescriptionColumnTypes},
		{Name: websitesTable, Columns: websiteColumns, ColumnTypes: websiteColumnTypes},
		{Name: companyFormsTable, Columns: companyFormColumns, ColumnTypes: companyFormColumnTypes},
		{Name: companyFormDescriptionsTable, Columns: companyFormDescriptionColumns, ColumnTypes: companyFormDescriptionColumnTypes},
		{Name: companySituationsTable, Columns: companySituationColumns, ColumnTypes: companySituationColumnTypes},
		{Name: companySituationDescriptionsTable, Columns: companySituationDescriptionColumns, ColumnTypes: companySituationDescriptionColumnTypes},
		{Name: registeredEntriesTable, Columns: registeredEntryColumns, ColumnTypes: registeredEntryColumnTypes},
		{Name: registeredEntryDescriptionsTable, Columns: registeredEntryDescriptionColumns, ColumnTypes: registeredEntryDescriptionColumnTypes},
		{Name: addressesTable, Columns: addressColumns, ColumnTypes: addressColumnTypes},
		{Name: addressPostOfficesTable, Columns: addressPostOfficeColumns, ColumnTypes: addressPostOfficeColumnTypes},
	}
}

func NormalizedTableNames() []string {
	tables := NormalizedTables()
	names := make([]string, 0, len(tables))
	for _, table := range tables {
		names = append(names, table.Name)
	}
	return names
}
```

Create typed row structs in this file. Use one struct per table with fields
matching that table's column list, and add `ClickHouseRow() map[string]any`
methods. Use these struct names: `IdentifierRow`, `StatusRow`, `NameRow`,
`BusinessLineRow`, `BusinessLineDescriptionRow`, `WebsiteRow`,
`CompanyFormRow`, `CompanyFormDescriptionRow`, `CompanySituationRow`,
`CompanySituationDescriptionRow`, `RegisteredEntryRow`,
`RegisteredEntryDescriptionRow`, `AddressRow`, `AddressPostOfficeRow`.

- [ ] **Step 4: Run row tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/finland/prhytj -run 'TestNormalizedTables|TestEveryNormalizedTable' -count=1 -v
```

Expected: PASS.

- [ ] **Step 5: Commit row model**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/scheduler/internal/companysources/finland/prhytj/rows.go \
        corpscout/scheduler/internal/companysources/finland/prhytj/rows_test.go
git commit -m "feat: define finland prhytj normalized rows"
```

## Task 5: Add Normalizer For All PRH Structures

**Files:**
- Create: `corpscout/scheduler/internal/companysources/finland/prhytj/normalize.go`
- Create: `corpscout/scheduler/internal/companysources/finland/prhytj/normalize_test.go`

- [ ] **Step 1: Write failing normalization coverage test**

Create `normalize_test.go` with one full source record and assertions for every normalized table:

```go
package prhytj

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestNormalizeParsedRecordCoversAllPRHStructures(t *testing.T) {
	raw := []byte(`{
		"businessId":{"type":"BusinessId","value":"0100130-4","registrationDate":"1981-05-12","source":"3"},
		"euId":{"type":"EUID","value":"FIFPRO.0100130-4"},
		"identifiers":[{"type":"VAT","value":"FI01001304","registrationDate":"1994-06-01","source":"3"}],
		"names":[{"name":"Dynava Oy","type":"1","version":1,"registrationDate":"1981-05-12","endDate":""}],
		"mainBusinessLine":{"type":"82200","typeCodeSet":"TOL2008","registrationDate":"2020-01-01","source":"3","descriptions":[{"languageCode":"1","description":"Puhelinpalvelukeskusten toiminta"},{"languageCode":"3","description":"Activities of call centres"}]},
		"website":{"url":"www.dynava.fi","registrationDate":"2020-01-01","endDate":""},
		"companyForms":[{"type":"16","version":1,"registrationDate":"1981-05-12","source":"3","descriptions":[{"languageCode":"1","description":"Osakeyhtiö"},{"languageCode":"3","description":"Limited company"}]}],
		"companySituations":[{"type":"01","registrationDate":"2021-01-01","descriptions":[{"languageCode":"3","description":"Normal"}]}],
		"registeredEntries":[{"type":"1","register":"1","authority":"PRH","registrationDate":"1981-05-12","descriptions":[{"languageCode":"3","description":"Registered"}]}],
		"addresses":[{"type":1,"street":"Test street","postCode":"00100","buildingNumber":"1","country":"FI","registrationDate":"2020-01-01","source":"3","postOffices":[{"languageCode":"1","city":"Helsinki","municipalityCode":"091"},{"languageCode":"2","city":"Helsingfors","municipalityCode":"091"}]}],
		"tradeRegisterStatus":"1",
		"status":"1",
		"registrationDate":"1981-05-12",
		"endDate":"",
		"lastModified":"2026-01-01T00:00:00Z"
	}`)
	var record CompanyRecord
	require.NoError(t, json.Unmarshal(raw, &record))

	entry := NormalizeParsedRecord(RunContext{
		RunID:          "run-1",
		SourceExportID: uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		IngestedAt:     time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC),
	}, ParsedRecord{
		LineNumber:  7,
		PayloadHash: "payload-hash",
		Record:      record,
	})

	require.Len(t, entry.Identifiers, 3)
	require.NotNil(t, entry.Status)
	require.Len(t, entry.Names, 1)
	require.NotNil(t, entry.BusinessLine)
	require.Len(t, entry.BusinessLineDescriptions, 2)
	require.NotNil(t, entry.Website)
	require.Len(t, entry.CompanyForms, 1)
	require.Len(t, entry.CompanyFormDescriptions, 2)
	require.Len(t, entry.CompanySituations, 1)
	require.Len(t, entry.CompanySituationDescriptions, 1)
	require.Len(t, entry.RegisteredEntries, 1)
	require.Len(t, entry.RegisteredEntryDescriptions, 1)
	require.Len(t, entry.Addresses, 1)
	require.Len(t, entry.AddressPostOffices, 2)

	require.Equal(t, int64(7), entry.Names[0].SourceLineNumber)
	require.Equal(t, "payload-hash", entry.Names[0].SourcePayloadHash)
	require.NotEmpty(t, entry.Names[0].SourceItemHash)
}

func TestNormalizedEntryKeepsRowsGroupedBySourceRecord(t *testing.T) {
	entry := NormalizedEntry{
		Identifiers: []IdentifierRow{{BusinessID: "0100130-4"}, {BusinessID: "0100130-4"}},
		Status:      &StatusRow{BusinessID: "0100130-4"},
		Names:       []NameRow{{BusinessID: "0100130-4"}},
	}

	require.Len(t, entry.Identifiers, 2)
	require.NotNil(t, entry.Status)
	require.Equal(t, "0100130-4", entry.Status.BusinessID)
	require.Len(t, entry.Names, 1)
	require.Equal(t, "0100130-4", entry.Names[0].BusinessID)
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/finland/prhytj -run 'TestNormalizeParsedRecordCoversAllPRHStructures|TestNormalizedEntryKeepsRowsGroupedBySourceRecord' -count=1 -v
```

Expected: FAIL because `NormalizeParsedRecord`, `NormalizedEntry`, and `RunContext` do not exist.

- [ ] **Step 3: Implement normalizer**

Create `normalize.go` with:

```go
package prhytj

import (
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

type RunContext struct {
	RunID          string
	SourceExportID uuid.UUID
	IngestedAt     time.Time
}

type NormalizedEntry struct {
	Identifiers []IdentifierRow
	Status *StatusRow
	Names []NameRow
	BusinessLine *BusinessLineRow
	BusinessLineDescriptions []BusinessLineDescriptionRow
	Website *WebsiteRow
	CompanyForms []CompanyFormRow
	CompanyFormDescriptions []CompanyFormDescriptionRow
	CompanySituations []CompanySituationRow
	CompanySituationDescriptions []CompanySituationDescriptionRow
	RegisteredEntries []RegisteredEntryRow
	RegisteredEntryDescriptions []RegisteredEntryDescriptionRow
	Addresses []AddressRow
	AddressPostOffices []AddressPostOfficeRow
}

func NormalizeParsedRecord(run RunContext, parsed ParsedRecord) NormalizedEntry {
	record := parsed.Record
	businessID := record.BusinessID.Value
	// Return one entry for this one source record.
	// Put all rows derived from this source record under this entry.
	// Use common lineage on every row:
	// FI, SourceKey, run.RunID, businessID, parsed.LineNumber, parsed.PayloadHash, clickHouseTimestamp(run.IngestedAt), run.SourceExportID.
	return NormalizedEntry{}
}
```

Implement every append path tested in Step 1:

- business ID identifier row
- EUID identifier row when present
- each `Identifiers[]` row
- one status row
- each `Names[]` row
- one business line row and every business line description row
- one website row when URL is non-empty
- each company form row and every form description row
- each company situation row and every situation description row
- each registered entry row and every entry description row
- each address row and every post office row

Use helper functions:

```go
func sourceItemHash(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}

func position(index int) int32 {
	return int32(index + 1)
}

func isCurrent(endDate string) bool {
	return strings.TrimSpace(endDate) == ""
}

func normalizedURLParts(raw string) (normalized string, host string, path string) {
	normalized = normalizeWebsite(raw)
	parsed, err := url.Parse(normalized)
	if err != nil {
		return normalized, "", ""
	}
	return normalized, parsed.Hostname(), parsed.EscapedPath()
}

func intString(value int) string {
	return strconv.Itoa(value)
}
```

- [ ] **Step 4: Run normalizer test**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/finland/prhytj -run 'TestNormalizeParsedRecordCoversAllPRHStructures|TestNormalizedEntryKeepsRowsGroupedBySourceRecord' -count=1 -v
```

Expected: PASS.

- [ ] **Step 5: Commit normalizer**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/scheduler/internal/companysources/finland/prhytj/normalize.go \
        corpscout/scheduler/internal/companysources/finland/prhytj/normalize_test.go
git commit -m "feat: normalize finland prhytj source records"
```

## Task 6: Add Native Go ClickHouse Writer

**Files:**
- Modify: `corpscout/scheduler/go.mod`
- Modify: `corpscout/scheduler/go.sum`
- Create: `corpscout/scheduler/internal/clickhouse/writer.go`
- Create: `corpscout/scheduler/internal/clickhouse/writer_test.go`

- [ ] **Step 1: Add ClickHouse Go driver dependency**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go get github.com/ClickHouse/clickhouse-go/v2@latest
```

Expected: `go.mod` includes `github.com/ClickHouse/clickhouse-go/v2`.

- [ ] **Step 2: Write failing native writer tests**

Create `corpscout/scheduler/internal/clickhouse/writer_test.go`:

```go
package clickhouse

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestParseNativeURL(t *testing.T) {
	target, err := ParseNativeURL("clickhouse://companycollect:9002?username=default&password=change-me&database=corpscout_sources")
	require.NoError(t, err)
	require.Equal(t, Target{
		Host:     "companycollect",
		Port:     "9002",
		Username: "default",
		Password: "change-me",
		Database: "corpscout_sources",
	}, target)
}

func TestBuildInsertQuery(t *testing.T) {
	query := BuildInsertQuery("corpscout_sources", "fi_prhytj_identifiers", []string{"business_id", "identifier_value"})
	require.Equal(t, "INSERT INTO `corpscout_sources`.`fi_prhytj_identifiers` (`business_id`, `identifier_value`)", query)
}

func TestInsertValuesFollowColumnOrder(t *testing.T) {
	values := insertValues([]string{"business_id", "identifier_value"}, map[string]any{
		"identifier_value": "FI01001304",
		"business_id":      "0100130-4",
	})
	require.Equal(t, []any{"0100130-4", "FI01001304"}, values)
}

func TestWriterInsertRoundTrip(t *testing.T) {
	rawURL := os.Getenv("CLICKHOUSE_TEST_NATIVE_URL")
	if rawURL == "" {
		t.Skip("CLICKHOUSE_TEST_NATIVE_URL not set")
	}

	ctx := context.Background()
	writer, err := Open(ctx, rawURL)
	require.NoError(t, err)
	defer writer.Close()

	table := "writer_insert_round_trip"
	require.NoError(t, writer.conn.Exec(ctx, "DROP TABLE IF EXISTS "+quoteIdent(writer.database)+"."+quoteIdent(table)))
	require.NoError(t, writer.conn.Exec(ctx, "CREATE TABLE "+quoteIdent(writer.database)+"."+quoteIdent(table)+" (`business_id` String, `source_export_id` UUID, `ingested_at` DateTime64(3, 'UTC')) ENGINE = Memory"))
	defer writer.conn.Exec(ctx, "DROP TABLE IF EXISTS "+quoteIdent(writer.database)+"."+quoteIdent(table))

	exportID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	ingestedAt := time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC)
	require.NoError(t, writer.Insert(ctx, Insert{
		Table:   table,
		Columns: []string{"business_id", "source_export_id", "ingested_at"},
		Rows: []map[string]any{{
			"business_id":       "0100130-4",
			"source_export_id":  exportID,
			"ingested_at":       ingestedAt,
		}},
	}))

	var count uint64
	require.NoError(t, writer.conn.QueryRow(ctx, "SELECT count() FROM "+quoteIdent(writer.database)+"."+quoteIdent(table)).Scan(&count))
	require.Equal(t, uint64(1), count)
}
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/clickhouse -run 'TestParseNativeURL|TestBuildInsertQuery|TestInsertValuesFollowColumnOrder|TestWriterInsertRoundTrip' -count=1 -v
```

Expected: FAIL because package `internal/clickhouse` does not exist.

- [ ] **Step 4: Implement native writer**

Create `corpscout/scheduler/internal/clickhouse/writer.go`:

```go
package clickhouse

import (
	"context"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/cockroachdb/errors"
)

type Target struct {
	Host     string
	Port     string
	Username string
	Password string
	Database string
}

type Insert struct {
	Table   string
	Columns []string
	Rows    []map[string]any
}

type Writer struct {
	conn     driver.Conn
	database string
}

func Open(ctx context.Context, rawURL string) (*Writer, error) {
	target, err := ParseNativeURL(rawURL)
	if err != nil {
		return nil, err
	}
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{net.JoinHostPort(target.Host, target.Port)},
		Auth: clickhouse.Auth{
			Database: target.Database,
			Username: target.Username,
			Password: target.Password,
		},
		DialTimeout: 10 * time.Second,
	})
	if err != nil {
		return nil, errors.Wrap(err, "open clickhouse connection")
	}
	if err := conn.Ping(ctx); err != nil {
		return nil, errors.Wrap(err, "ping clickhouse")
	}
	return &Writer{conn: conn, database: target.Database}, nil
}

func (w *Writer) Close() error {
	if w == nil || w.conn == nil {
		return nil
	}
	return w.conn.Close()
}

func (w *Writer) Insert(ctx context.Context, insert Insert) error {
	if len(insert.Rows) == 0 {
		return nil
	}
	query := BuildInsertQuery(w.database, insert.Table, insert.Columns)
	batch, err := w.conn.PrepareBatch(ctx, query)
	if err != nil {
		return errors.Wrap(err, "prepare clickhouse insert batch")
	}
	for _, row := range insert.Rows {
		if err := batch.Append(insertValues(insert.Columns, row)...); err != nil {
			return errors.Wrap(err, "append clickhouse insert row")
		}
	}
	if err := batch.Send(); err != nil {
		return errors.Wrap(err, "send clickhouse insert batch")
	}
	return nil
}

func ParseNativeURL(rawURL string) (Target, error) {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return Target{}, errors.Wrap(err, "parse clickhouse native url")
	}
	if parsed.Scheme != "clickhouse" {
		return Target{}, errors.Errorf("clickhouse native url must use clickhouse scheme, got %q", parsed.Scheme)
	}
	target := Target{
		Host:     parsed.Hostname(),
		Port:     parsed.Port(),
		Username: parsed.Query().Get("username"),
		Password: parsed.Query().Get("password"),
		Database: parsed.Query().Get("database"),
	}
	if parsed.User != nil {
		if target.Username == "" {
			target.Username = parsed.User.Username()
		}
		if password, ok := parsed.User.Password(); ok && target.Password == "" {
			target.Password = password
		}
	}
	if target.Port == "" {
		target.Port = "9000"
	}
	if target.Username == "" {
		target.Username = "default"
	}
	if target.Database == "" {
		target.Database = strings.TrimPrefix(parsed.EscapedPath(), "/")
	}
	if target.Host == "" {
		return Target{}, errors.New("clickhouse native url host is required")
	}
	if target.Database == "" {
		return Target{}, errors.New("clickhouse native url database is required")
	}
	return target, nil
}

func BuildInsertQuery(database string, table string, columns []string) string {
	quotedColumns := make([]string, 0, len(columns))
	for _, column := range columns {
		quotedColumns = append(quotedColumns, quoteIdent(column))
	}
	return "INSERT INTO " + quoteIdent(database) + "." + quoteIdent(table) + " (" + strings.Join(quotedColumns, ", ") + ")"
}

func insertValues(columns []string, row map[string]any) []any {
	values := make([]any, 0, len(columns))
	for _, column := range columns {
		values = append(values, row[column])
	}
	return values
}

func quoteIdent(value string) string {
	escaped := strings.NewReplacer(`\`, `\\`, "`", "\\`").Replace(value)
	return "`" + escaped + "`"
}
```

- [ ] **Step 5: Run native writer tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/clickhouse -count=1 -v
```

Expected: PASS.

- [ ] **Step 6: Commit native writer**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/scheduler/go.mod \
        corpscout/scheduler/go.sum \
        corpscout/scheduler/internal/clickhouse/writer.go \
        corpscout/scheduler/internal/clickhouse/writer_test.go
git commit -m "feat: add native clickhouse writer"
```

## Task 7: Refactor Importer To Write Normalized Tables Only

**Files:**
- Modify: `corpscout/scheduler/internal/companysources/finland/prhytj/import.go`
- Modify: `corpscout/scheduler/internal/companysources/finland/prhytj/import_test.go`

- [ ] **Step 1: Replace old importer tests**

Remove tests that reference:

- `companyRow`
- `companyColumns`
- `rawRecordRow`
- `rawRecordColumns`
- `raw_payload_json`

Add:

```go
func TestImportedTablesAreNormalizedOnly(t *testing.T) {
	require.NotContains(t, NormalizedTableNames(), "fi_prhytj_raw_records")
	require.NotContains(t, NormalizedTableNames(), "fi_prhytj_companies")
	require.Contains(t, NormalizedTableNames(), "fi_prhytj_identifiers")
	require.Contains(t, NormalizedTableNames(), "fi_prhytj_address_post_offices")
}
```

- [ ] **Step 2: Run test to verify current code fails**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/finland/prhytj -run TestImportedTablesAreNormalizedOnly -count=1 -v
```

Expected before importer cleanup: PASS may already happen from Task 4, but package-wide tests should still fail until old tests/code references are removed:

```bash
GOWORK=off go test ./internal/companysources/finland/prhytj -count=1 -v
```

- [ ] **Step 3: Replace importer implementation**

In `import.go`:

- remove `rawRecordsTable`
- remove `companiesTable`
- remove `rawRecordColumns`
- remove `companyColumns`
- remove `rawRecordRow`
- remove `companyRow`
- remove helper code only used by those row builders if unused after `normalize.go`
- remove the import of `github.com/pulsarpoint/corpscout/scheduler/internal/clickhouseclient`
- add an import alias for the native writer:

```go
chwriter "github.com/pulsarpoint/corpscout/scheduler/internal/clickhouse"
```

Change `Import` to:

```go
func (Source) Import(ctx context.Context, opts companysources.ImportOptions) (companysources.ImportResult, error) {
	if opts.RunDir == "" {
		return companysources.ImportResult{}, errors.New("run dir is required")
	}
	if opts.ClickHouseNativeURL == "" {
		return companysources.ImportResult{}, errors.New("clickhouse native url is required")
	}

	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	sourceExportID := uuid.New()
	runID := filepath.Base(opts.RunDir)
	snapshotPath := filepath.Join(opts.RunDir, "source.ndjson")
	run := RunContext{RunID: runID, SourceExportID: sourceExportID, IngestedAt: time.Now().UTC()}
	writer, err := chwriter.Open(ctx, opts.ClickHouseNativeURL)
	if err != nil {
		return companysources.ImportResult{}, err
	}
	defer writer.Close()

	entries := make([]NormalizedEntry, 0, batchSize)
	var seen int64

	flush := func() error {
		if len(entries) == 0 {
			return nil
		}
		if err := flushNormalizedEntries(ctx, writer, entries); err != nil {
			return err
		}
		entries = entries[:0]
		return nil
	}

	err := ParseSnapshot(ctx, snapshotPath, func(record ParsedRecord) error {
		if opts.Limit > 0 && seen >= opts.Limit {
			return nil
		}
		entries = append(entries, NormalizeParsedRecord(run, record))
		seen++
		if len(entries) < batchSize {
			return nil
		}
		return flush()
	})
	if err != nil {
		return companysources.ImportResult{}, err
	}
	if err := flush(); err != nil {
		return companysources.ImportResult{}, err
	}

	return companysources.ImportResult{
		RunDir:         opts.RunDir,
		ImportedTables: NormalizedTableNames(),
		ImportedRows:   seen,
	}, nil
}
```

Add `flushNormalizedEntries` that receives `[]NormalizedEntry`, builds one
`chwriter.Insert` per table, and calls `writer.Insert` for each non-empty table
insert. Do not introduce a `NormalizedBatch` struct.

Use this signature:

```go
func flushNormalizedEntries(ctx context.Context, writer *chwriter.Writer, entries []NormalizedEntry) error
```

Inside `flushNormalizedEntries`, build table rows directly from entries:

```go
identifierRows := make([]map[string]any, 0)
for _, entry := range entries {
	for _, row := range entry.Identifiers {
		identifierRows = append(identifierRows, row.ClickHouseRow())
	}
}
if len(identifierRows) > 0 {
	if err := writer.Insert(ctx, chwriter.Insert{
		Table:   identifiersTable,
		Columns: identifierColumns,
		Rows:    identifierRows,
	}); err != nil {
		return err
	}
}
```

Repeat that direct per-table extraction for every row group in `NormalizedEntry`:

- `Identifiers`
- `Status` when non-nil
- `Names`
- `BusinessLine` when non-nil
- `BusinessLineDescriptions`
- `Website` when non-nil
- `CompanyForms`
- `CompanyFormDescriptions`
- `CompanySituations`
- `CompanySituationDescriptions`
- `RegisteredEntries`
- `RegisteredEntryDescriptions`
- `Addresses`
- `AddressPostOffices`

The only structure crossing from normalization to import should be
`[]NormalizedEntry`.


Do not call `clickhouseclient.ExecuteInsert` anywhere in the PRH package.

- [ ] **Step 4: Run PRH package tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/finland/prhytj -count=1 -v
```

Expected: PASS.

- [ ] **Step 5: Commit importer refactor**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/scheduler/internal/companysources/finland/prhytj/import.go \
        corpscout/scheduler/internal/companysources/finland/prhytj/import_test.go
git commit -m "feat: import finland prhytj normalized tables"
```

## Task 8: Migrate Existing U.S. Importers To Native Writer And Remove Docker Option

**Files:**
- Modify: `corpscout/scheduler/internal/companysources/source.go`
- Modify: `corpscout/scheduler/internal/companysources/importer.go`
- Modify: `corpscout/scheduler/cmd/corpscout-source/main.go`
- Modify: `corpscout/scheduler/internal/companysources/unitedstates/coloradoentities/import.go`
- Modify: `corpscout/scheduler/internal/companysources/unitedstates/irseobmf/import.go`
- Modify: `corpscout/scheduler/internal/companysources/unitedstates/secedgar/import.go`

- [ ] **Step 1: Write failing guard for stale Docker option**

Add this test to `corpscout/scheduler/cmd/corpscout-source/main_test.go`:

```go
func TestImportRunRejectsClickHouseImageFlag(t *testing.T) {
	var output bytes.Buffer
	err := run([]string{
		"import-run",
		"--country", "finland",
		"--source", "prhytj",
		"--run-dir", "/tmp/run",
		"--clickhouse-native-url", "clickhouse://companycollect:9002?username=default&database=corpscout_sources",
		"--clickhouse-image", "clickhouse/clickhouse-server:26.5",
	}, &output)
	require.Error(t, err)
	require.Contains(t, err.Error(), "flag provided but not defined")
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./cmd/corpscout-source -run TestImportRunRejectsClickHouseImageFlag -count=1 -v
```

Expected: FAIL because `--clickhouse-image` is still accepted.

- [ ] **Step 3: Remove `ClickHouseImage` from shared request/options**

In `corpscout/scheduler/internal/companysources/source.go`, remove:

```go
ClickHouseImage string
```

from:

- `ImportOptions`
- `ImportRunRequest`
- `ImportChangedRunsRequest`

In `corpscout/scheduler/internal/companysources/importer.go`, remove all
assignments of `ClickHouseImage:`.

In `corpscout/scheduler/cmd/corpscout-source/main.go`, remove:

- `ClickHouseImage string` from `importRunConfig`
- `ClickHouseImage string` from `importRunsConfig`
- both `fs.StringVar(&cfg.ClickHouseImage, "clickhouse-image", "", "ClickHouse Docker image")` lines
- `ClickHouseImage: cfg.ClickHouseImage` in `ImportRunRequest`
- `ClickHouseImage: cfg.ClickHouseImage` in `ImportChangedRunsRequest`

- [ ] **Step 4: Migrate U.S. importers to native writer**

In each file:

- `corpscout/scheduler/internal/companysources/unitedstates/coloradoentities/import.go`
- `corpscout/scheduler/internal/companysources/unitedstates/irseobmf/import.go`
- `corpscout/scheduler/internal/companysources/unitedstates/secedgar/import.go`

replace:

```go
"github.com/pulsarpoint/corpscout/scheduler/internal/clickhouseclient"
```

with:

```go
chwriter "github.com/pulsarpoint/corpscout/scheduler/internal/clickhouse"
```

Open a native writer once near the start of `Import`, after validating
`ClickHouseNativeURL`:

```go
writer, err := chwriter.Open(ctx, opts.ClickHouseNativeURL)
if err != nil {
	return companysources.ImportResult{}, err
}
defer writer.Close()
```

Change `flushRows` signature from:

```go
func flushRows(ctx context.Context, opts companysources.ImportOptions, table string, columns []string, rows []map[string]any) error
```

to:

```go
func flushRows(ctx context.Context, writer *chwriter.Writer, table string, columns []string, rows []map[string]any) error
```

and implement:

```go
func flushRows(ctx context.Context, writer *chwriter.Writer, table string, columns []string, rows []map[string]any) error {
	return writer.Insert(ctx, chwriter.Insert{
		Table:   table,
		Columns: columns,
		Rows:    rows,
	})
}
```

Update every call from:

```go
flushRows(ctx, opts, rawRecordsTable, rawRecordColumns, rawRows)
```

to:

```go
flushRows(ctx, writer, rawRecordsTable, rawRecordColumns, rawRows)
```

- [ ] **Step 5: Run focused tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./cmd/corpscout-source ./internal/companysources ./internal/companysources/unitedstates/... -count=1
```

Expected: PASS.

- [ ] **Step 6: Verify no `ClickHouseImage` references remain**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
rg "ClickHouseImage|clickhouse-image" corpscout/scheduler
```

Expected: no matches.

- [ ] **Step 7: Commit native migration of remaining importers**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/scheduler/internal/companysources/source.go \
        corpscout/scheduler/internal/companysources/importer.go \
        corpscout/scheduler/cmd/corpscout-source/main.go \
        corpscout/scheduler/cmd/corpscout-source/main_test.go \
        corpscout/scheduler/internal/companysources/unitedstates/coloradoentities/import.go \
        corpscout/scheduler/internal/companysources/unitedstates/irseobmf/import.go \
        corpscout/scheduler/internal/companysources/unitedstates/secedgar/import.go
git commit -m "refactor: use native clickhouse writer for source imports"
```

## Task 9: Remove Docker ClickHouse Client Package

**Files:**
- Delete: `corpscout/scheduler/internal/clickhouseclient/json_each_row.go`
- Delete: `corpscout/scheduler/internal/clickhouseclient/json_each_row_test.go`
- Delete: `corpscout/scheduler/internal/clickhouseclient/url.go`

- [ ] **Step 1: Verify no production imports remain**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
rg "internal/clickhouseclient|clickhouseclient\\." corpscout/scheduler
```

Expected before deletion: only files under `corpscout/scheduler/internal/clickhouseclient` match.

- [ ] **Step 2: Delete old package files**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
rm corpscout/scheduler/internal/clickhouseclient/json_each_row.go \
   corpscout/scheduler/internal/clickhouseclient/json_each_row_test.go \
   corpscout/scheduler/internal/clickhouseclient/url.go
```

- [ ] **Step 3: Verify no references remain**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
rg "internal/clickhouseclient|clickhouseclient\\." corpscout/scheduler
```

Expected: no matches.

- [ ] **Step 4: Run focused tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/clickhouse ./internal/companysources/finland/prhytj ./cmd/corpscout-source -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit old client removal**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add -u corpscout/scheduler/internal/clickhouseclient
git commit -m "refactor: remove docker clickhouse client"
```

## Task 10: Remove Stale ClickHouse Pilot Import Config And Tool

**Files:**
- Delete: `corpscout/clickhouse/sources/finland_prhytj.yaml`
- Delete: `corpscout/clickhouse/tools/chimport/main.go`
- Delete: `corpscout/clickhouse/tools/chimport/importer.go`
- Delete: `corpscout/clickhouse/tools/chimport/importer_test.go`

- [ ] **Step 1: Verify the stale config/tool still carries old table names**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
rg "fi_prhytj_raw_records|fi_prhytj_companies|ClickHouseImage|clickhouse-image" corpscout/clickhouse/sources corpscout/clickhouse/tools/chimport
```

Expected before cleanup: matches in `finland_prhytj.yaml` and `tools/chimport`.

- [ ] **Step 2: Delete the stale pilot import path**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
rm corpscout/clickhouse/sources/finland_prhytj.yaml \
   corpscout/clickhouse/tools/chimport/main.go \
   corpscout/clickhouse/tools/chimport/importer.go \
   corpscout/clickhouse/tools/chimport/importer_test.go
```

Do not delete ClickHouse migrations. The old applied migration remains history;
the replacement migration is responsible for dropping old active tables.

- [ ] **Step 3: Verify no live stale config/tool references remain**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
rg "fi_prhytj_raw_records|fi_prhytj_companies|ClickHouseImage|clickhouse-image" corpscout/clickhouse corpscout/scheduler/internal/companysources/finland/prhytj -g '!migrations/**' -g '!docs/**'
```

Expected: no matches.

- [ ] **Step 4: Run remaining ClickHouse tool tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/clickhouse
GOWORK=off go test ./... -count=1
```

Expected: PASS for remaining packages.

- [ ] **Step 5: Commit stale pilot cleanup**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add -u corpscout/clickhouse/sources/finland_prhytj.yaml \
           corpscout/clickhouse/tools/chimport
git commit -m "refactor: remove stale clickhouse parquet import pilot"
```

## Task 11: Apply Migration And Verify Limited Remote Import

**Files:**
- No planned source edits.

- [ ] **Step 1: Run focused tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db ./internal/clickhouse ./internal/companysources/finland/prhytj ./cmd/corpscout-source -count=1
CLICKHOUSE_NATIVE_URL="${CLICKHOUSE_NATIVE_URL:-$(sed -n 's/^CLICKHOUSE_NATIVE_URL=//p' ../.env 2>/dev/null | tail -n 1)}"
test -n "$CLICKHOUSE_NATIVE_URL"
CLICKHOUSE_TEST_NATIVE_URL="$CLICKHOUSE_NATIVE_URL" GOWORK=off go test ./internal/clickhouse -run TestWriterInsertRoundTrip -count=1 -v
```

Expected: PASS.

- [ ] **Step 2: Apply remote ClickHouse migration**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make clickhouse-migrate-up
```

Expected: applies version 4 or returns `no change` if already applied.

- [ ] **Step 3: Import a limited sample**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make source-import-run SOURCE_LIMIT=100 SOURCE_BATCH_SIZE=50
```

Expected: JSON output has `ImportedRows: 100` and `ImportedTables` contains normalized table names only.

- [ ] **Step 4: Verify old tables are gone and normalized tables have rows**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
docker run --rm --add-host companycollect:100.85.212.113 clickhouse/clickhouse-server:26.5 clickhouse-client \
  --host companycollect \
  --port 9002 \
  --user default \
  --password password123 \
  --database corpscout_sources \
  --query "SELECT name FROM system.tables WHERE database = 'corpscout_sources' AND name IN ('fi_prhytj_raw_records', 'fi_prhytj_companies') ORDER BY name"
```

Expected: no rows.

Then run:

```bash
docker run --rm --add-host companycollect:100.85.212.113 clickhouse/clickhouse-server:26.5 clickhouse-client \
  --host companycollect \
  --port 9002 \
  --user default \
  --password password123 \
  --database corpscout_sources \
  --query "SELECT (SELECT count() FROM fi_prhytj_identifiers), (SELECT count() FROM fi_prhytj_statuses), (SELECT count() FROM fi_prhytj_names)"
```

Expected: each count is at least `100`.

- [ ] **Step 5: Commit verification fixes only if needed**

If verification required source changes:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git status --short -uall
git add corpscout/clickhouse corpscout/scheduler/internal/companysources/finland/prhytj corpscout/scheduler/internal/db
git commit -m "fix: complete finland prhytj normalized import"
```

If no files changed, do not create an empty commit.

## Task 12: Final Verification

**Files:**
- No planned source edits.

- [ ] **Step 1: Run final focused tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db ./internal/clickhouse ./internal/companysources/... ./cmd/corpscout-source -count=1
```

Expected: PASS.

- [ ] **Step 2: Verify old names do not appear in live PRH import code or stale pilot tooling**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
rg "fi_prhytj_raw_records|fi_prhytj_companies|raw_payload_json|rawRecordRow|companyRow|rawRecordColumns|companyColumns|clickhouseclient|NormalizedBatch|ClickHouseImage|clickhouse-image" corpscout/clickhouse corpscout/scheduler/internal/companysources/finland/prhytj -g '!migrations/**' -g '!docs/**'
```

Expected: no matches.

- [ ] **Step 3: Verify Makefile dry-runs still point at Corpscout CLI**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make -n clickhouse-migrate-up
make -n source-import-run
```

Expected:

- `clickhouse-migrate-up` uses `--add-host companycollect:100.85.212.113`
- `source-import-run` runs `go run ./cmd/corpscout-source import-run`

- [ ] **Step 4: Check git status without touching unrelated dirty files**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git status --short -uall
```

Expected: only unrelated pre-existing workspace changes remain.
