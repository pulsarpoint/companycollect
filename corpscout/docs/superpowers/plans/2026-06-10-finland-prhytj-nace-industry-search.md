# Finland PRH YTJ NACE Industry Search Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add NACE-backed industry search and filtering to the Finland PRH YTJ explorer while preserving original PRH YTJ `TOIMI*` industry values.

**Architecture:** ClickHouse owns the source-specific analytics projection. A Finland-specific mapping action builds `fi_prhytj_industry_nace_mappings` from distinct PRH YTJ business lines and ClickHouse NACE reference rows, then the explorer cache refresh copies mapped NACE hierarchy fields into `fi_prhytj_company_explorer_cache`. The UI reads one combined searchable industry option list and writes selected filters to the URL.

**Tech Stack:** Go, Temporal Go SDK, ClickHouse native driver, PostgreSQL/sqlc migrations, React Router, TypeScript, shadcn/ui, TanStack table.

---

## Preconditions

The current explorer-cache/filter work in the working tree is a prerequisite for this plan:

- `fi_prhytj_company_explorer_cache` exists.
- `/api/v1/sources/{name}/explorer/filter-options` exists.
- `/api/v1/sources/{name}/explorer/companies` reads from the cache table.
- `refresh_explorer_cache` source action exists.

Before executing this plan, commit or otherwise preserve those prerequisite changes. Do not revert or overwrite them while implementing NACE industry search.

## File Structure

Create or modify these files:

- Create: `clickhouse/migrations/000010_finland_prhytj_industry_nace_mapping.up.sql`
- Create: `clickhouse/migrations/000010_finland_prhytj_industry_nace_mapping.down.sql`
- Modify: `scheduler/internal/db/clickhouse_finland_prhytj_explorer_cache_migration_test.go`
- Create: `database/migrations/000117_source_industry_nace_mapping_action.up.sql`
- Create: `database/migrations/000117_source_industry_nace_mapping_action.down.sql`
- Create: `scheduler/internal/db/source_industry_nace_mapping_action_migration_test.go`
- Create: `scheduler/internal/companysources/finland/prhytj/industry_mapping.go`
- Create: `scheduler/internal/companysources/finland/prhytj/industry_mapping_test.go`
- Modify: `scheduler/internal/companysources/finland/prhytj/explorer_cache.go`
- Modify: `scheduler/internal/companysources/finland/prhytj/explorer_cache_test.go`
- Modify: `scheduler/internal/temporal/workflow/companysources/workflow.go`
- Modify: `scheduler/internal/temporal/workflow/companysources/workflow_test.go`
- Modify: `scheduler/internal/temporal/actions/companysources/actions.go`
- Modify: `scheduler/internal/app/temporal.go`
- Modify: `scheduler/internal/httpapi/source_actions.go`
- Modify: `scheduler/internal/httpapi/source_actions_test.go`
- Create: `scheduler/internal/httpapi/reference_nace.go`
- Create: `scheduler/internal/httpapi/reference_nace_test.go`
- Modify: `scheduler/internal/httpapi/handlers.go`
- Modify: `scheduler/internal/httpapi/source_explorer.go`
- Modify: `scheduler/internal/httpapi/source_explorer_internal_test.go`
- Modify: `ui/app/types/api.ts`
- Modify: `ui/app/lib/api.ts`
- Modify: `ui/app/components/app/source-detail/ActionsTab.tsx`
- Modify: `ui/app/components/app/source-detail/FinlandPRHYTJExplorerTab.tsx`
- Modify: `ui/app/components/app/source-detail/SourceExplorerFiltersSheet.tsx`

## Task 1: Add ClickHouse Mapping Schema

**Files:**
- Create: `clickhouse/migrations/000010_finland_prhytj_industry_nace_mapping.up.sql`
- Create: `clickhouse/migrations/000010_finland_prhytj_industry_nace_mapping.down.sql`
- Modify: `scheduler/internal/db/clickhouse_finland_prhytj_explorer_cache_migration_test.go`

- [ ] **Step 1: Write failing migration shape test**

Append this test to `scheduler/internal/db/clickhouse_finland_prhytj_explorer_cache_migration_test.go`:

```go
func TestClickHouseFinlandPRHYTJIndustryNACEMappingMigrationShape(t *testing.T) {
	up, err := os.ReadFile(filepath.Join("..", "..", "..", "clickhouse", "migrations", "000010_finland_prhytj_industry_nace_mapping.up.sql"))
	require.NoError(t, err)
	down, err := os.ReadFile(filepath.Join("..", "..", "..", "clickhouse", "migrations", "000010_finland_prhytj_industry_nace_mapping.down.sql"))
	require.NoError(t, err)

	sql := string(up)
	for _, needle := range []string{
		"CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prhytj_industry_nace_mappings`",
		"`source_code_set` Nullable(String)",
		"`source_code` Nullable(String)",
		"`source_code_prefix4` Nullable(String)",
		"`source_code_dotted4` Nullable(String)",
		"`source_extra_digit` Nullable(String)",
		"`nace_revision` Nullable(String)",
		"`nace_code` Nullable(String)",
		"`nace_section_code` Nullable(String)",
		"`nace_division_code` Nullable(String)",
		"`nace_group_code` Nullable(String)",
		"`nace_class_code` Nullable(String)",
		"`mapping_status` LowCardinality(String)",
		"ENGINE = ReplacingMergeTree(`mapped_at`)",
		"ADD COLUMN IF NOT EXISTS `nace_revision` Nullable(String)",
		"ADD COLUMN IF NOT EXISTS `nace_mapping_status` Nullable(String)",
	} {
		require.Contains(t, sql, needle)
	}
	require.Contains(t, string(down), "DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_industry_nace_mappings`")
	require.Contains(t, string(down), "DROP COLUMN IF EXISTS `nace_revision`")
	require.Contains(t, string(down), "DROP COLUMN IF EXISTS `nace_mapping_status`")
}
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db -run TestClickHouseFinlandPRHYTJIndustryNACEMappingMigrationShape -count=1
```

Expected: fail because the migration files do not exist.

- [ ] **Step 3: Add ClickHouse up migration**

Create `clickhouse/migrations/000010_finland_prhytj_industry_nace_mapping.up.sql`:

```sql
CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prhytj_industry_nace_mappings` (
  `source_code_set` Nullable(String),
  `source_code` Nullable(String),
  `source_code_prefix4` Nullable(String),
  `source_code_dotted4` Nullable(String),
  `source_extra_digit` Nullable(String),
  `source_description_en` Nullable(String),
  `nace_revision` Nullable(String),
  `nace_code` Nullable(String),
  `nace_normalized_code` Nullable(String),
  `nace_section_code` Nullable(String),
  `nace_division_code` Nullable(String),
  `nace_group_code` Nullable(String),
  `nace_class_code` Nullable(String),
  `nace_title_en` Nullable(String),
  `mapping_method` LowCardinality(String),
  `mapping_status` LowCardinality(String),
  `mapped_at` DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(`mapped_at`)
ORDER BY (`source_code_set`, `source_code`)
SETTINGS allow_nullable_key = 1;

ALTER TABLE `corpscout_sources`.`fi_prhytj_company_explorer_cache`
  ADD COLUMN IF NOT EXISTS `nace_revision` Nullable(String),
  ADD COLUMN IF NOT EXISTS `nace_code` Nullable(String),
  ADD COLUMN IF NOT EXISTS `nace_normalized_code` Nullable(String),
  ADD COLUMN IF NOT EXISTS `nace_section_code` Nullable(String),
  ADD COLUMN IF NOT EXISTS `nace_division_code` Nullable(String),
  ADD COLUMN IF NOT EXISTS `nace_group_code` Nullable(String),
  ADD COLUMN IF NOT EXISTS `nace_class_code` Nullable(String),
  ADD COLUMN IF NOT EXISTS `nace_title_en` Nullable(String),
  ADD COLUMN IF NOT EXISTS `nace_mapping_method` Nullable(String),
  ADD COLUMN IF NOT EXISTS `nace_mapping_status` Nullable(String);
```

- [ ] **Step 4: Add ClickHouse down migration**

Create `clickhouse/migrations/000010_finland_prhytj_industry_nace_mapping.down.sql`:

```sql
ALTER TABLE `corpscout_sources`.`fi_prhytj_company_explorer_cache`
  DROP COLUMN IF EXISTS `nace_revision`,
  DROP COLUMN IF EXISTS `nace_code`,
  DROP COLUMN IF EXISTS `nace_normalized_code`,
  DROP COLUMN IF EXISTS `nace_section_code`,
  DROP COLUMN IF EXISTS `nace_division_code`,
  DROP COLUMN IF EXISTS `nace_group_code`,
  DROP COLUMN IF EXISTS `nace_class_code`,
  DROP COLUMN IF EXISTS `nace_title_en`,
  DROP COLUMN IF EXISTS `nace_mapping_method`,
  DROP COLUMN IF EXISTS `nace_mapping_status`;

DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_industry_nace_mappings`;
```

- [ ] **Step 5: Run test and verify it passes**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db -run TestClickHouseFinlandPRHYTJIndustryNACEMappingMigrationShape -count=1
```

Expected: pass.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add clickhouse/migrations/000010_finland_prhytj_industry_nace_mapping.* scheduler/internal/db/clickhouse_finland_prhytj_explorer_cache_migration_test.go
git commit -m "feat: add finland industry nace mapping schema"
```

## Task 2: Implement Finland Industry Mapping Refresh

**Files:**
- Create: `scheduler/internal/companysources/finland/prhytj/industry_mapping.go`
- Create: `scheduler/internal/companysources/finland/prhytj/industry_mapping_test.go`

- [ ] **Step 1: Write failing tests for mapping SQL**

Create `scheduler/internal/companysources/finland/prhytj/industry_mapping_test.go`:

```go
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
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/finland/prhytj -run 'TestBuildIndustryNACEMappingInsertQuery|TestIndustryNACEMappingConstants' -count=1
```

Expected: fail because constants and query builder do not exist.

- [ ] **Step 3: Implement mapping refresh package**

Create `scheduler/internal/companysources/finland/prhytj/industry_mapping.go`:

```go
package prhytj

import (
	"context"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"

	ch "github.com/pulsarpoint/corpscout/scheduler/internal/clickhouse"
)

const IndustryNACEMappingTable = "fi_prhytj_industry_nace_mappings"

var industryNACEMappingColumns = []string{
	"source_code_set",
	"source_code",
	"source_code_prefix4",
	"source_code_dotted4",
	"source_extra_digit",
	"source_description_en",
	"nace_revision",
	"nace_code",
	"nace_normalized_code",
	"nace_section_code",
	"nace_division_code",
	"nace_group_code",
	"nace_class_code",
	"nace_title_en",
	"mapping_method",
	"mapping_status",
	"mapped_at",
}

type IndustryNACEMappingRefreshResult struct {
	MappingTable string    `json:"mapping_table"`
	Rows         uint64    `json:"rows"`
	MappedRows   uint64    `json:"mapped_rows"`
	UnmappedRows uint64    `json:"unmapped_rows"`
	MappedAt     time.Time `json:"mapped_at"`
}

func RefreshIndustryNACEMappings(ctx context.Context, clickHouseNativeURL string) (IndustryNACEMappingRefreshResult, error) {
	if strings.TrimSpace(clickHouseNativeURL) == "" {
		return IndustryNACEMappingRefreshResult{}, errors.New("clickhouse native url is required")
	}
	writer, err := ch.Open(ctx, clickHouseNativeURL)
	if err != nil {
		return IndustryNACEMappingRefreshResult{}, errors.Wrap(err, "open clickhouse writer")
	}
	defer writer.Close()

	var naceClasses uint64
	if err := writer.QueryRow(ctx, "SELECT count() FROM `corpscout_reference`.`nace_codes` WHERE revision IN ('2', '2.1') AND level_name = 'class' AND active = true").Scan(&naceClasses); err != nil {
		return IndustryNACEMappingRefreshResult{}, errors.Wrap(err, "count clickhouse nace class rows")
	}
	if naceClasses == 0 {
		return IndustryNACEMappingRefreshResult{}, errors.New("clickhouse nace class reference rows are empty")
	}

	tempTable := IndustryNACEMappingTable + "_refresh_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	database := writer.Database()
	tempQualified := ch.QualifiedTable(database, tempTable)
	mappingQualified := ch.QualifiedTable(database, IndustryNACEMappingTable)

	if err := writer.Exec(ctx, "DROP TABLE IF EXISTS "+tempQualified); err != nil {
		return IndustryNACEMappingRefreshResult{}, errors.Wrap(err, "drop stale industry mapping refresh table")
	}
	tempCreated := false
	defer func() {
		if tempCreated {
			_ = writer.Exec(context.Background(), "DROP TABLE IF EXISTS "+tempQualified)
		}
	}()

	if err := writer.Exec(ctx, "CREATE TABLE "+tempQualified+" AS "+mappingQualified); err != nil {
		return IndustryNACEMappingRefreshResult{}, errors.Wrap(err, "create industry mapping refresh table")
	}
	tempCreated = true
	if err := writer.Exec(ctx, ch.BuildTruncateQuery(database, tempTable)); err != nil {
		return IndustryNACEMappingRefreshResult{}, errors.Wrap(err, "clear industry mapping refresh table")
	}

	mappedAt := time.Now().UTC().Truncate(time.Millisecond)
	if err := writer.Exec(ctx, buildIndustryNACEMappingInsertQuery(database, tempTable, mappedAt)); err != nil {
		return IndustryNACEMappingRefreshResult{}, errors.Wrap(err, "load industry mapping refresh table")
	}

	var rows, mappedRows, unmappedRows uint64
	if err := writer.QueryRow(ctx, "SELECT count(), countIf(mapping_status = 'mapped'), countIf(mapping_status = 'unmapped') FROM "+tempQualified).Scan(&rows, &mappedRows, &unmappedRows); err != nil {
		return IndustryNACEMappingRefreshResult{}, errors.Wrap(err, "count industry mapping refresh table")
	}
	if err := writer.Exec(ctx, "EXCHANGE TABLES "+mappingQualified+" AND "+tempQualified); err != nil {
		return IndustryNACEMappingRefreshResult{}, errors.Wrap(err, "swap industry mapping table")
	}

	return IndustryNACEMappingRefreshResult{
		MappingTable: database + "." + IndustryNACEMappingTable,
		Rows:         rows,
		MappedRows:   mappedRows,
		UnmappedRows: unmappedRows,
		MappedAt:     mappedAt,
	}, nil
}

func buildIndustryNACEMappingInsertQuery(database string, table string, mappedAt time.Time) string {
	return `INSERT INTO ` + ch.QualifiedTable(database, table) + ` (` + industryNACEMappingColumnList() + `)
WITH
source_industries AS (
  SELECT
    ifNull(bl.business_line_code_set, '') AS source_code_set,
    ifNull(bl.business_line_type, '') AS source_code,
    nullIf(argMaxIf(d.description, bl.ingested_at, ifNull(d.description, '') != ''), '') AS source_description_en
  FROM ` + ch.QualifiedTable(database, businessLinesTable) + ` AS bl
  LEFT JOIN ` + ch.QualifiedTable(database, businessLineDescriptionsTable) + ` AS d
    ON d.business_id = bl.business_id
   AND d.source_run_id = bl.source_run_id
   AND d.business_line_item_hash = bl.source_item_hash
   AND d.language_code = '3'
  WHERE ifNull(bl.business_line_type, '') != ''
  GROUP BY source_code_set, source_code
),
candidates AS (
  SELECT
    source_code_set,
    source_code,
    if(length(source_code) >= 4, substring(source_code, 1, 4), '') AS source_code_prefix4,
    if(length(source_code) >= 4, concat(substring(source_code, 1, 2), '.', substring(source_code, 3, 2)), '') AS source_code_dotted4,
    if(length(source_code) >= 5, substring(source_code, 5), '') AS source_extra_digit,
    source_description_en,
    multiIf(source_code_set = 'TOIMI4', '2.1', source_code_set = 'TOIMI3', '2', '') AS target_revision
  FROM source_industries
)
SELECT
  nullIf(c.source_code_set, '') AS `source_code_set`,
  nullIf(c.source_code, '') AS `source_code`,
  nullIf(c.source_code_prefix4, '') AS `source_code_prefix4`,
  nullIf(c.source_code_dotted4, '') AS `source_code_dotted4`,
  nullIf(c.source_extra_digit, '') AS `source_extra_digit`,
  c.source_description_en AS `source_description_en`,
  nullIf(n.revision, '') AS `nace_revision`,
  nullIf(n.code, '') AS `nace_code`,
  nullIf(n.normalized_code, '') AS `nace_normalized_code`,
  n.section_code AS `nace_section_code`,
  n.division_code AS `nace_division_code`,
  n.group_code AS `nace_group_code`,
  n.class_code AS `nace_class_code`,
  nullIf(n.title, '') AS `nace_title_en`,
  if(ifNull(n.code, '') != '', 'toimi_5_digit_prefix', if(c.target_revision != '' AND c.source_code_prefix4 != '', 'toimi_prefix_unmatched', 'unsupported_code_set')) AS `mapping_method`,
  if(ifNull(n.code, '') != '', 'mapped', 'unmapped') AS `mapping_status`,
  ` + clickHouseDateTime64Literal(mappedAt) + ` AS ` + ch.QuoteIdent("mapped_at") + `
FROM candidates AS c
LEFT JOIN ` + ch.QualifiedTable("corpscout_reference", "nace_codes") + ` AS n
  ON n.revision = c.target_revision
 AND n.level_name = 'class'
 AND n.normalized_code = c.source_code_prefix4
 AND n.active = true`
}

func industryNACEMappingColumnList() string {
	quoted := make([]string, 0, len(industryNACEMappingColumns))
	for _, column := range industryNACEMappingColumns {
		quoted = append(quoted, ch.QuoteIdent(column))
	}
	return strings.Join(quoted, ", ")
}
```

- [ ] **Step 4: Run tests and verify they pass**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/finland/prhytj -run 'TestBuildIndustryNACEMappingInsertQuery|TestIndustryNACEMappingConstants' -count=1
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add scheduler/internal/companysources/finland/prhytj/industry_mapping.go scheduler/internal/companysources/finland/prhytj/industry_mapping_test.go
git commit -m "feat: map finland source industries to nace"
```

## Task 3: Enrich Explorer Cache With NACE Fields

**Files:**
- Modify: `scheduler/internal/companysources/finland/prhytj/explorer_cache.go`
- Modify: `scheduler/internal/companysources/finland/prhytj/explorer_cache_test.go`

- [ ] **Step 1: Write failing cache query test**

Append this test to `scheduler/internal/companysources/finland/prhytj/explorer_cache_test.go`:

```go
func TestBuildCompanyExplorerCacheInsertQueryJoinsIndustryNACEMapping(t *testing.T) {
	refreshedAt := time.Date(2026, 6, 10, 14, 0, 0, 0, time.UTC)
	query := buildCompanyExplorerCacheInsertQuery("corpscout_sources", "fi_prhytj_company_explorer_cache_refresh_test", "`corpscout_sources`.`fi_prhytj_company_explorer`", refreshedAt)

	require.Contains(t, query, "LEFT JOIN `corpscout_sources`.`fi_prhytj_industry_nace_mappings` AS industry_mapping")
	require.Contains(t, query, "industry_mapping.`nace_revision` AS `nace_revision`")
	require.Contains(t, query, "industry_mapping.`nace_code` AS `nace_code`")
	require.Contains(t, query, "industry_mapping.`nace_title_en` AS `nace_title_en`")
	require.Contains(t, query, "industry_mapping.mapping_status AS `nace_mapping_status`")
	require.Contains(t, query, "ifNull(industry_mapping.source_code_set, '') = ifNull(explorer.main_business_line_code_set, '')")
	require.Contains(t, query, "ifNull(industry_mapping.source_code, '') = ifNull(explorer.main_business_line_code, '')")
}
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/finland/prhytj -run TestBuildCompanyExplorerCacheInsertQueryJoinsIndustryNACEMapping -count=1
```

Expected: fail because the cache query does not join the mapping table yet.

- [ ] **Step 3: Add NACE columns to cache column list**

In `scheduler/internal/companysources/finland/prhytj/explorer_cache.go`, add these entries before `refreshed_at` in `companyExplorerCacheColumns`:

```go
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
```

- [ ] **Step 4: Route mapped columns to mapping alias**

Still in `explorer_cache.go`, add this helper:

```go
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
```

- [ ] **Step 5: Update cache insert query to join mapping table**

Replace `buildCompanyExplorerCacheInsertQuery` with:

```go
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
```

- [ ] **Step 6: Run cache tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/finland/prhytj -run 'TestBuildCompanyExplorerCacheInsertQuery|TestBuildCompanyExplorerCacheInsertQueryJoinsIndustryNACEMapping' -count=1
```

Expected: pass.

- [ ] **Step 7: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add scheduler/internal/companysources/finland/prhytj/explorer_cache.go scheduler/internal/companysources/finland/prhytj/explorer_cache_test.go
git commit -m "feat: enrich finland explorer cache with nace"
```

## Task 4: Add Temporal Source Action For Industry Mapping

**Files:**
- Create: `database/migrations/000117_source_industry_nace_mapping_action.up.sql`
- Create: `database/migrations/000117_source_industry_nace_mapping_action.down.sql`
- Create: `scheduler/internal/db/source_industry_nace_mapping_action_migration_test.go`
- Modify: `scheduler/internal/temporal/workflow/companysources/workflow.go`
- Modify: `scheduler/internal/temporal/workflow/companysources/workflow_test.go`
- Modify: `scheduler/internal/temporal/actions/companysources/actions.go`
- Modify: `scheduler/internal/app/temporal.go`
- Modify: `scheduler/internal/httpapi/source_actions.go`
- Modify: `scheduler/internal/httpapi/source_actions_test.go`

- [ ] **Step 1: Write failing Postgres migration test**

Create `scheduler/internal/db/source_industry_nace_mapping_action_migration_test.go`:

```go
package db_test

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSourceIndustryNACEMappingActionMigrationShape(t *testing.T) {
	up, err := os.ReadFile("../../../database/migrations/000117_source_industry_nace_mapping_action.up.sql")
	require.NoError(t, err)
	down, err := os.ReadFile("../../../database/migrations/000117_source_industry_nace_mapping_action.down.sql")
	require.NoError(t, err)

	sql := string(up)
	for _, needle := range []string{
		"DROP CONSTRAINT IF EXISTS chk_data_source_actions_action",
		"DROP CONSTRAINT IF EXISTS chk_data_source_action_runs_action",
		"'map_industries_to_nace'",
		"'Map industries to NACE'",
		"'CompanySourceIndustryNACEMappingWorkflow'",
		"'finland/prhytj'",
		"ON CONFLICT (source_id, action) DO UPDATE",
	} {
		require.Contains(t, sql, needle)
	}
	require.Contains(t, string(down), "DELETE FROM data_source_action_runs")
	require.Contains(t, string(down), "DELETE FROM data_source_actions")
	require.Equal(t, 2, strings.Count(string(down), "action IN ('pull_source', 'import_clickhouse', 'refresh_explorer_cache')"))
}
```

- [ ] **Step 2: Run migration test and verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db -run TestSourceIndustryNACEMappingActionMigrationShape -count=1
```

Expected: fail because migration files do not exist.

- [ ] **Step 3: Add Postgres action migration**

Create `database/migrations/000117_source_industry_nace_mapping_action.up.sql`:

```sql
ALTER TABLE data_source_actions
  DROP CONSTRAINT IF EXISTS chk_data_source_actions_action;

ALTER TABLE data_source_action_runs
  DROP CONSTRAINT IF EXISTS chk_data_source_action_runs_action;

ALTER TABLE data_source_actions
  ADD CONSTRAINT chk_data_source_actions_action CHECK (
    action IN ('pull_source', 'import_clickhouse', 'refresh_explorer_cache', 'map_industries_to_nace')
  );

ALTER TABLE data_source_action_runs
  ADD CONSTRAINT chk_data_source_action_runs_action CHECK (
    action IN ('pull_source', 'import_clickhouse', 'refresh_explorer_cache', 'map_industries_to_nace')
  );

INSERT INTO data_source_actions (
  source_id, action, display_name, temporal_workflow_type, temporal_task_queue
)
SELECT
  ds.id,
  'map_industries_to_nace',
  'Map industries to NACE',
  'CompanySourceIndustryNACEMappingWorkflow',
  'corpscout-company-sources'
FROM data_sources ds
WHERE ds.registry_key = 'finland/prhytj'
ON CONFLICT (source_id, action) DO UPDATE SET
  display_name = EXCLUDED.display_name,
  temporal_workflow_type = EXCLUDED.temporal_workflow_type,
  temporal_task_queue = EXCLUDED.temporal_task_queue,
  enabled = true,
  updated_at = now();
```

Create `database/migrations/000117_source_industry_nace_mapping_action.down.sql`:

```sql
DELETE FROM data_source_action_runs
WHERE action = 'map_industries_to_nace';

DELETE FROM data_source_actions
WHERE action = 'map_industries_to_nace';

ALTER TABLE data_source_actions
  DROP CONSTRAINT IF EXISTS chk_data_source_actions_action;

ALTER TABLE data_source_action_runs
  DROP CONSTRAINT IF EXISTS chk_data_source_action_runs_action;

ALTER TABLE data_source_actions
  ADD CONSTRAINT chk_data_source_actions_action CHECK (
    action IN ('pull_source', 'import_clickhouse', 'refresh_explorer_cache')
  );

ALTER TABLE data_source_action_runs
  ADD CONSTRAINT chk_data_source_action_runs_action CHECK (
    action IN ('pull_source', 'import_clickhouse', 'refresh_explorer_cache')
  );
```

- [ ] **Step 4: Add workflow constants, input, result, and workflow**

Modify `scheduler/internal/temporal/workflow/companysources/workflow.go`:

```go
const (
	MapSourceIndustriesToNACEWorkflowName = "CompanySourceIndustryNACEMappingWorkflow"
	MapSourceIndustriesToNACEActivityName = "MapSourceIndustriesToNACEActivity"
	ActionMapIndustriesToNACE             = "map_industries_to_nace"
)

type MapSourceIndustriesToNACEInput struct {
	ActionRunID string `json:"action_run_id"`
	SourceName  string `json:"source_name"`
	Trigger     string `json:"trigger"`
}

type MapSourceIndustriesToNACEResult struct {
	ActionRunID  string `json:"action_run_id"`
	SourceName   string `json:"source_name"`
	MappingTable string `json:"mapping_table"`
	Rows         uint64 `json:"rows"`
	MappedRows   uint64 `json:"mapped_rows"`
	UnmappedRows uint64 `json:"unmapped_rows"`
	MappedAt     string `json:"mapped_at"`
}

func MapSourceIndustriesToNACE(ctx workflow.Context, input MapSourceIndustriesToNACEInput) (MapSourceIndustriesToNACEResult, error) {
	ctx = withSourceActivityOptions(ctx, 30*time.Minute)
	var result MapSourceIndustriesToNACEResult
	if err := workflow.ExecuteActivity(ctx, MapSourceIndustriesToNACEActivityName, input).Get(ctx, &result); err != nil {
		return MapSourceIndustriesToNACEResult{}, errors.Wrap(err, "map source industries to nace activity")
	}
	return result, nil
}
```

Merge the constants into the existing `const` block rather than creating duplicate `const` blocks.

- [ ] **Step 5: Add workflow test**

Append this test to `scheduler/internal/temporal/workflow/companysources/workflow_test.go`:

```go
func TestMapSourceIndustriesToNACERunsActivity(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflowWithOptions(MapSourceIndustriesToNACE, workflow.RegisterOptions{Name: MapSourceIndustriesToNACEWorkflowName})
	env.RegisterActivityWithOptions(func(input MapSourceIndustriesToNACEInput) (MapSourceIndustriesToNACEResult, error) {
		require.Equal(t, MapSourceIndustriesToNACEInput{
			ActionRunID: "action-run-1",
			SourceName:  "finland_prhytj",
			Trigger:     "manual",
		}, input)
		return MapSourceIndustriesToNACEResult{
			ActionRunID:  "action-run-1",
			SourceName:   "finland_prhytj",
			MappingTable: "corpscout_sources.fi_prhytj_industry_nace_mappings",
			Rows:         3,
			MappedRows:   2,
			UnmappedRows: 1,
			MappedAt:     "2026-06-10T10:00:00Z",
		}, nil
	}, activity.RegisterOptions{Name: MapSourceIndustriesToNACEActivityName})

	env.ExecuteWorkflow(MapSourceIndustriesToNACEWorkflowName, MapSourceIndustriesToNACEInput{
		ActionRunID: "action-run-1",
		SourceName:  "finland_prhytj",
		Trigger:     "manual",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result MapSourceIndustriesToNACEResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, uint64(2), result.MappedRows)
	require.Equal(t, "corpscout_sources.fi_prhytj_industry_nace_mappings", result.MappingTable)
}
```

- [ ] **Step 6: Add activity implementation**

Modify `scheduler/internal/temporal/actions/companysources/actions.go`:

```go
type MapSourceIndustriesToNACEInput = sourceworkflow.MapSourceIndustriesToNACEInput
type MapSourceIndustriesToNACEResult = sourceworkflow.MapSourceIndustriesToNACEResult

func (a *Actions) MapSourceIndustriesToNACEActivity(ctx context.Context, input MapSourceIndustriesToNACEInput) (MapSourceIndustriesToNACEResult, error) {
	if a == nil || a.pool == nil {
		return MapSourceIndustriesToNACEResult{}, errors.New("company source database is not available")
	}
	actionRunID, err := uuid.Parse(input.ActionRunID)
	if err != nil {
		return MapSourceIndustriesToNACEResult{}, errors.Wrap(err, "parse action run id")
	}

	queries := db.New(a.pool)
	workflowID, runID := workflowExecutionFromContext(ctx)

	actionRun, err := queries.GetSourceActionRun(ctx, actionRunID)
	if err != nil {
		return MapSourceIndustriesToNACEResult{}, errors.Wrap(err, "load industry mapping action run")
	}
	if actionRun.Action != sourceworkflow.ActionMapIndustriesToNACE {
		return MapSourceIndustriesToNACEResult{}, errors.Errorf("action run %s has action %q", actionRun.ID, actionRun.Action)
	}
	action, err := queries.GetSourceActionByName(ctx, db.GetSourceActionByNameParams{
		Name:   input.SourceName,
		Action: sourceworkflow.ActionMapIndustriesToNACE,
	})
	if err != nil {
		return MapSourceIndustriesToNACEResult{}, errors.Wrap(err, "load industry mapping action")
	}
	if actionRun.SourceID != action.SourceID || actionRun.ActionID != action.ID {
		return MapSourceIndustriesToNACEResult{}, errors.Errorf("action run %s does not belong to %s industry mapping action", actionRun.ID, input.SourceName)
	}
	if err := validateStoredWorkflowID(actionRun.TemporalWorkflowID, workflowID, "source action run", actionRun.ID); err != nil {
		return MapSourceIndustriesToNACEResult{}, err
	}
	if runID != "" {
		if err := queries.UpdateSourceActionRunTemporalRunID(ctx, db.UpdateSourceActionRunTemporalRunIDParams{
			TemporalRunID: optionalStringPointer(runID),
			ID:            actionRunID,
		}); err != nil {
			return MapSourceIndustriesToNACEResult{}, errors.Wrap(err, "update industry mapping action temporal run id")
		}
	}

	result := MapSourceIndustriesToNACEResult{
		ActionRunID: actionRun.ID.String(),
		SourceName:  input.SourceName,
	}
	mapped, err := a.mapSourceIndustriesToNACE(ctx, input.SourceName)
	if err != nil {
		finishErr := a.finishActionRunFailed(actionRun.ID, err)
		return result, combineWithFinishError(errors.Wrap(err, "map source industries to nace"), finishErr)
	}
	result.MappingTable = mapped.MappingTable
	result.Rows = mapped.Rows
	result.MappedRows = mapped.MappedRows
	result.UnmappedRows = mapped.UnmappedRows
	result.MappedAt = mapped.MappedAt.Format(time.RFC3339Nano)

	if _, err := queries.FinishSourceActionRun(ctx, db.FinishSourceActionRunParams{
		Status:       sourceworkflow.StatusSucceeded,
		Result:       marshalActionResult(result),
		ErrorMessage: "",
		ID:           actionRun.ID,
	}); err != nil {
		return result, errors.Wrap(err, "finish industry mapping action run")
	}
	return result, nil
}

func (a *Actions) mapSourceIndustriesToNACE(ctx context.Context, sourceName string) (prhytj.IndustryNACEMappingRefreshResult, error) {
	switch sourceName {
	case "finland_prhytj":
		return prhytj.RefreshIndustryNACEMappings(ctx, a.clickHouseNativeURL)
	default:
		return prhytj.IndustryNACEMappingRefreshResult{}, errors.Errorf("source industry nace mapping is not implemented for %s", sourceName)
	}
}
```

- [ ] **Step 7: Register workflow and activity**

Modify `scheduler/internal/app/temporal.go` where company source workflows and activities are registered:

```go
worker.RegisterWorkflowWithOptions(companysourceworkflows.MapSourceIndustriesToNACE, workflow.RegisterOptions{Name: companysourceworkflows.MapSourceIndustriesToNACEWorkflowName})
worker.RegisterActivityWithOptions(companySourceActions.MapSourceIndustriesToNACEActivity, activity.RegisterOptions{Name: companysourceworkflows.MapSourceIndustriesToNACEActivityName})
```

- [ ] **Step 8: Wire HTTP trigger switch**

Modify `scheduler/internal/httpapi/source_actions.go`:

```go
case companysourceworkflows.ActionMapIndustriesToNACE:
	return nonEmptyString(configuredWorkflow, companysourceworkflows.MapSourceIndustriesToNACEWorkflowName), nil
```

and:

```go
case companysourceworkflows.ActionMapIndustriesToNACE:
	return companysourceworkflows.MapSourceIndustriesToNACEInput{
		ActionRunID: actionRunID,
		SourceName:  sourceName,
		Trigger:     req.Trigger,
	}, nil
```

Add or extend tests in `scheduler/internal/httpapi/source_actions_test.go` so `sourceActionWorkflowName` and `sourceActionWorkflowInput` accept `map_industries_to_nace`.

- [ ] **Step 9: Run focused tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db -run TestSourceIndustryNACEMappingActionMigrationShape -count=1
GOWORK=off go test ./internal/temporal/workflow/companysources -run TestMapSourceIndustriesToNACERunsActivity -count=1
GOWORK=off go test ./internal/httpapi -run 'TestSourceActionWorkflow' -count=1
```

Expected: all pass.

- [ ] **Step 10: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add database/migrations/000117_source_industry_nace_mapping_action.* scheduler/internal/db/source_industry_nace_mapping_action_migration_test.go scheduler/internal/temporal/workflow/companysources/workflow.go scheduler/internal/temporal/workflow/companysources/workflow_test.go scheduler/internal/temporal/actions/companysources/actions.go scheduler/internal/app/temporal.go scheduler/internal/httpapi/source_actions.go scheduler/internal/httpapi/source_actions_test.go
git commit -m "feat: add industry nace mapping source action"
```

## Task 5: Add ClickHouse NACE Reference API

**Files:**
- Create: `scheduler/internal/httpapi/reference_nace.go`
- Create: `scheduler/internal/httpapi/reference_nace_test.go`
- Modify: `scheduler/internal/httpapi/handlers.go`

- [ ] **Step 1: Write failing query shape test**

Create `scheduler/internal/httpapi/reference_nace_test.go`:

```go
package httpapi

import (
	"strings"
	"testing"
)

func TestBuildReferenceNACEListQuery(t *testing.T) {
	query := buildReferenceNACEListQuery("corpscout_reference")
	for _, needle := range []string{
		"FROM `corpscout_reference`.`nace_codes`",
		"WHERE revision = ?",
		"active = true",
		"ORDER BY level ASC, normalized_code ASC",
		"LIMIT 5000",
	} {
		if !strings.Contains(query, needle) {
			t.Fatalf("buildReferenceNACEListQuery() = %q, missing %q", query, needle)
		}
	}
}

func TestParseReferenceNACERevision(t *testing.T) {
	if got := parseReferenceNACERevision(""); got != "2.1" {
		t.Fatalf("parseReferenceNACERevision() = %q, want 2.1", got)
	}
	if got := parseReferenceNACERevision(" 2 "); got != "2" {
		t.Fatalf("parseReferenceNACERevision() = %q, want 2", got)
	}
}
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/httpapi -run 'TestBuildReferenceNACEListQuery|TestParseReferenceNACERevision' -count=1
```

Expected: fail because the functions do not exist.

- [ ] **Step 3: Implement ClickHouse reference handler**

Create `scheduler/internal/httpapi/reference_nace.go`:

```go
package httpapi

import (
	"log/slog"
	"net/http"
	"strings"

	ch "github.com/pulsarpoint/corpscout/scheduler/internal/clickhouse"
)

type referenceNACECode struct {
	Revision             string `json:"revision"`
	Code                 string `json:"code"`
	NormalizedCode       string `json:"normalized_code"`
	Level                uint8  `json:"level"`
	LevelName            string `json:"level_name"`
	ParentCode           string `json:"parent_code"`
	ParentNormalizedCode string `json:"parent_normalized_code"`
	Title                string `json:"title"`
	SectionCode          string `json:"section_code"`
	DivisionCode         string `json:"division_code"`
	GroupCode            string `json:"group_code"`
	ClassCode            string `json:"class_code"`
}

type referenceNACEListResponse struct {
	Items []referenceNACECode `json:"items"`
}

func (h *Handlers) handleListReferenceNACE(w http.ResponseWriter, r *http.Request) {
	if strings.TrimSpace(h.clickHouseURL) == "" {
		writeError(w, http.StatusServiceUnavailable, "clickhouse is not configured")
		return
	}
	reader, err := ch.OpenReader(r.Context(), h.clickHouseURL)
	if err != nil {
		slog.ErrorContext(r.Context(), "open clickhouse reference nace", "error", err)
		writeError(w, http.StatusServiceUnavailable, "clickhouse unavailable")
		return
	}
	defer reader.Close()

	revision := parseReferenceNACERevision(r.URL.Query().Get("revision"))
	rows, err := reader.Query(r.Context(), buildReferenceNACEListQuery("corpscout_reference"), revision)
	if err != nil {
		slog.ErrorContext(r.Context(), "list clickhouse reference nace", "revision", revision, "error", err)
		writeError(w, http.StatusInternalServerError, "list reference NACE failed")
		return
	}
	defer rows.Close()

	items := make([]referenceNACECode, 0)
	for rows.Next() {
		var item referenceNACECode
		if err := rows.Scan(
			&item.Revision,
			&item.Code,
			&item.NormalizedCode,
			&item.Level,
			&item.LevelName,
			&item.ParentCode,
			&item.ParentNormalizedCode,
			&item.Title,
			&item.SectionCode,
			&item.DivisionCode,
			&item.GroupCode,
			&item.ClassCode,
		); err != nil {
			slog.ErrorContext(r.Context(), "scan clickhouse reference nace", "revision", revision, "error", err)
			writeError(w, http.StatusInternalServerError, "scan reference NACE failed")
			return
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		slog.ErrorContext(r.Context(), "read clickhouse reference nace", "revision", revision, "error", err)
		writeError(w, http.StatusInternalServerError, "read reference NACE failed")
		return
	}

	writeJSON(w, http.StatusOK, referenceNACEListResponse{Items: items})
}

func parseReferenceNACERevision(value string) string {
	revision := strings.TrimSpace(value)
	if revision == "" {
		return "2.1"
	}
	return revision
}

func buildReferenceNACEListQuery(database string) string {
	return `SELECT
  revision,
  code,
  normalized_code,
  level,
  level_name,
  ifNull(parent_code, '') AS parent_code,
  ifNull(parent_normalized_code, '') AS parent_normalized_code,
  title,
  ifNull(section_code, '') AS section_code,
  ifNull(division_code, '') AS division_code,
  ifNull(group_code, '') AS group_code,
  ifNull(class_code, '') AS class_code
FROM ` + ch.QualifiedTable(database, "nace_codes") + `
WHERE revision = ?
  AND active = true
ORDER BY level ASC, normalized_code ASC
LIMIT 5000`
}
```

- [ ] **Step 4: Register route**

Modify `scheduler/internal/httpapi/handlers.go` inside `/api/v1` routes:

```go
r.Get("/reference/nace", h.handleListReferenceNACE)
```

- [ ] **Step 5: Run tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/httpapi -run 'TestBuildReferenceNACEListQuery|TestParseReferenceNACERevision' -count=1
```

Expected: pass.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add scheduler/internal/httpapi/reference_nace.go scheduler/internal/httpapi/reference_nace_test.go scheduler/internal/httpapi/handlers.go
git commit -m "feat: expose clickhouse nace reference"
```

## Task 6: Add NACE And Source Industry Explorer Filters

**Files:**
- Modify: `scheduler/internal/httpapi/source_explorer.go`
- Modify: `scheduler/internal/httpapi/source_explorer_internal_test.go`

- [ ] **Step 1: Write failing backend filter tests**

Append these tests to `scheduler/internal/httpapi/source_explorer_internal_test.go`:

```go
func TestBuildSourceExplorerCompanyListQueryUsesIndustryNACEFilters(t *testing.T) {
	query, args, err := buildSourceExplorerCompanyListQuery("`corpscout_sources`.`fi_prhytj_company_explorer_cache`", sourceExplorerCompanyQuery{
		Limit:        50,
		IndustryNACE: []string{"68", "68.20", "L"},
		Sort:         "name",
		Direction:    "asc",
	})
	if err != nil {
		t.Fatalf("buildSourceExplorerCompanyListQuery() error = %v", err)
	}
	for _, needle := range []string{
		"ifNull(nace_division_code, '') = ?",
		"ifNull(nace_class_code, '') = ?",
		"ifNull(nace_section_code, '') = ?",
	} {
		if !strings.Contains(query, needle) {
			t.Fatalf("buildSourceExplorerCompanyListQuery() query = %q, missing %q", query, needle)
		}
	}
	if len(args) != 5 {
		t.Fatalf("buildSourceExplorerCompanyListQuery() args length = %d, want 5", len(args))
	}
}

func TestBuildSourceExplorerCompanyListQueryUsesSourceIndustryFilters(t *testing.T) {
	query, args, err := buildSourceExplorerCompanyListQuery("`corpscout_sources`.`fi_prhytj_company_explorer_cache`", sourceExplorerCompanyQuery{
		Limit:            50,
		SourceIndustries: []sourceExplorerSourceIndustryFilter{{CodeSet: "TOIMI4", Code: "68203"}},
		Sort:             "name",
		Direction:        "asc",
	})
	if err != nil {
		t.Fatalf("buildSourceExplorerCompanyListQuery() error = %v", err)
	}
	if !strings.Contains(query, "ifNull(main_business_line_code_set, '') = ? AND ifNull(main_business_line_code, '') = ?") {
		t.Fatalf("buildSourceExplorerCompanyListQuery() query = %q, want source industry predicate", query)
	}
	if len(args) != 4 {
		t.Fatalf("buildSourceExplorerCompanyListQuery() args length = %d, want 4", len(args))
	}
	if args[0] != "TOIMI4" || args[1] != "68203" {
		t.Fatalf("buildSourceExplorerCompanyListQuery() args = %#v, want source industry values first", args)
	}
}

func TestParseSourceExplorerIndustryFilters(t *testing.T) {
	got := parseSourceExplorerSourceIndustries([]string{"TOIMI4:68203", "bad", "TOIMI3:64190"}, 10)
	if len(got) != 2 {
		t.Fatalf("parseSourceExplorerSourceIndustries() length = %d, want 2", len(got))
	}
	if got[0].CodeSet != "TOIMI4" || got[0].Code != "68203" {
		t.Fatalf("parseSourceExplorerSourceIndustries()[0] = %#v", got[0])
	}
}

func TestBuildSourceExplorerIndustryFilterOptionsQuery(t *testing.T) {
	query := buildSourceExplorerIndustryFilterOptionsQuery("`corpscout_sources`.`fi_prhytj_company_explorer_cache`")
	for _, needle := range []string{
		"'nace' AS kind",
		"'source_industry' AS kind",
		"`corpscout_reference`.`nace_codes`",
		"nace_division_code",
		"main_business_line_code_set",
		"company_count",
		"UNION ALL",
	} {
		if !strings.Contains(query, needle) {
			t.Fatalf("buildSourceExplorerIndustryFilterOptionsQuery() = %q, missing %q", query, needle)
		}
	}
}
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/httpapi -run 'TestBuildSourceExplorerCompanyListQueryUsesIndustryNACEFilters|TestBuildSourceExplorerCompanyListQueryUsesSourceIndustryFilters|TestParseSourceExplorerIndustryFilters|TestBuildSourceExplorerIndustryFilterOptionsQuery' -count=1
```

Expected: fail because types and query builders do not exist.

- [ ] **Step 3: Add backend response and query types**

Modify `scheduler/internal/httpapi/source_explorer.go`:

```go
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

type sourceExplorerSourceIndustryFilter struct {
	CodeSet string
	Code    string
}

type sourceExplorerFilterOptionsResponse struct {
	Forms           []sourceExplorerFormFilterOption     `json:"forms"`
	IndustryOptions []sourceExplorerIndustryFilterOption `json:"industry_options"`
}
```

Extend `sourceExplorerCompanyQuery`:

```go
IndustryNACE     []string
SourceIndustries []sourceExplorerSourceIndustryFilter
```

Extend `parseSourceExplorerCompanyQuery`:

```go
IndustryNACE:     parseSourceExplorerStringList(query["industry_nace"], 100),
SourceIndustries: parseSourceExplorerSourceIndustries(query["source_industry"], 100),
```

- [ ] **Step 4: Add source industry parser and NACE predicate helper**

Add these helpers to `source_explorer.go`:

```go
func parseSourceExplorerSourceIndustries(values []string, maxItems int) []sourceExplorerSourceIndustryFilter {
	rawValues := parseSourceExplorerStringList(values, maxItems)
	result := make([]sourceExplorerSourceIndustryFilter, 0, len(rawValues))
	for _, raw := range rawValues {
		codeSet, code, ok := strings.Cut(raw, ":")
		codeSet = strings.TrimSpace(codeSet)
		code = strings.TrimSpace(code)
		if !ok || codeSet == "" || code == "" {
			continue
		}
		result = append(result, sourceExplorerSourceIndustryFilter{CodeSet: codeSet, Code: code})
	}
	return result
}

func sourceExplorerNACEFilterColumn(value string) (string, bool) {
	trimmed := strings.TrimSpace(value)
	if len(trimmed) == 1 {
		letter := trimmed[0]
		if (letter >= 'A' && letter <= 'Z') || (letter >= 'a' && letter <= 'z') {
			return "nace_section_code", true
		}
	}
	if len(trimmed) == 2 && isASCIIInteger(trimmed) {
		return "nace_division_code", true
	}
	if len(trimmed) == 4 && trimmed[2] == '.' && isASCIIInteger(trimmed[:2]+trimmed[3:]) {
		return "nace_group_code", true
	}
	if len(trimmed) == 5 && trimmed[2] == '.' && isASCIIInteger(trimmed[:2]+trimmed[3:]) {
		return "nace_class_code", true
	}
	return "", false
}

func isASCIIInteger(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
```

- [ ] **Step 5: Add filters to WHERE clause**

In `sourceExplorerCompanyWhere`, add after form filters:

```go
if len(params.IndustryNACE) > 0 {
	naceClauses := make([]string, 0, len(params.IndustryNACE))
	for _, value := range params.IndustryNACE {
		column, ok := sourceExplorerNACEFilterColumn(value)
		if !ok {
			continue
		}
		naceClauses = append(naceClauses, "ifNull("+column+", '') = ?")
		args = append(args, strings.ToUpper(strings.TrimSpace(value)))
	}
	if len(naceClauses) > 0 {
		clauses = append(clauses, "("+strings.Join(naceClauses, " OR ")+")")
	}
}
if len(params.SourceIndustries) > 0 {
	sourceClauses := make([]string, 0, len(params.SourceIndustries))
	for _, industry := range params.SourceIndustries {
		sourceClauses = append(sourceClauses, "(ifNull(main_business_line_code_set, '') = ? AND ifNull(main_business_line_code, '') = ?)")
		args = append(args, industry.CodeSet, industry.Code)
	}
	clauses = append(clauses, "("+strings.Join(sourceClauses, " OR ")+")")
}
```

- [ ] **Step 6: Add NACE fields to company response and SELECT**

Extend `sourceExplorerCompany`:

```go
MainBusinessLineCodeSet string `json:"main_business_line_code_set"`
NACERevision            string `json:"nace_revision"`
NACECode                string `json:"nace_code"`
NACESectionCode         string `json:"nace_section_code"`
NACEDivisionCode        string `json:"nace_division_code"`
NACEGroupCode           string `json:"nace_group_code"`
NACEClassCode           string `json:"nace_class_code"`
NACETitleEnglish        string `json:"nace_title_en"`
NACEMappingStatus       string `json:"nace_mapping_status"`
```

In `buildSourceExplorerCompanyListQuery`, select and scan these fields immediately after `main_business_line_description_en`:

```sql
ifNull(main_business_line_code_set, ''),
ifNull(nace_revision, ''),
ifNull(nace_code, ''),
ifNull(nace_section_code, ''),
ifNull(nace_division_code, ''),
ifNull(nace_group_code, ''),
ifNull(nace_class_code, ''),
ifNull(nace_title_en, ''),
ifNull(nace_mapping_status, ''),
```

Update `rows.Scan` in the same order.

- [ ] **Step 7: Split filter options queries**

Rename existing `buildSourceExplorerFilterOptionsQuery` to:

```go
func buildSourceExplorerFormFilterOptionsQuery(table string) string
```

Add:

```go
func buildSourceExplorerIndustryFilterOptionsQuery(table string) string {
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
    concat('nace:', ifNull(cache.nace_revision, ''), ':', ifNull(cache.nace_section_code, '')) AS id,
    'nace' AS kind,
    ifNull(cache.nace_section_code, '') AS filter_value,
    ifNull(cache.nace_revision, '') AS revision,
    'section' AS level_name,
    ifNull(cache.nace_section_code, '') AS code,
    '' AS code_set,
    ifNull(reference.title, ifNull(cache.nace_section_code, '')) AS title,
    '' AS breadcrumb,
    '' AS mapped_nace_code,
    count() AS company_count,
    lowerUTF8(concat(ifNull(cache.nace_section_code, ''), ' ', ifNull(reference.title, ''))) AS search_text
  FROM ` + table + ` AS cache
  LEFT JOIN ` + ch.QualifiedTable("corpscout_reference", "nace_codes") + ` AS reference
    ON reference.revision = cache.nace_revision
   AND reference.level_name = 'section'
   AND reference.code = cache.nace_section_code
  WHERE ifNull(cache.nace_section_code, '') != ''
  GROUP BY revision, code, title
  UNION ALL
  SELECT
    concat('nace:', ifNull(cache.nace_revision, ''), ':', ifNull(cache.nace_division_code, '')) AS id,
    'nace' AS kind,
    ifNull(cache.nace_division_code, '') AS filter_value,
    ifNull(cache.nace_revision, '') AS revision,
    'division' AS level_name,
    ifNull(cache.nace_division_code, '') AS code,
    '' AS code_set,
    ifNull(reference.title, ifNull(cache.nace_division_code, '')) AS title,
    ifNull(cache.nace_section_code, '') AS breadcrumb,
    '' AS mapped_nace_code,
    count() AS company_count,
    lowerUTF8(concat(ifNull(cache.nace_division_code, ''), ' ', ifNull(reference.title, ''), ' ', ifNull(cache.nace_section_code, ''))) AS search_text
  FROM ` + table + ` AS cache
  LEFT JOIN ` + ch.QualifiedTable("corpscout_reference", "nace_codes") + ` AS reference
    ON reference.revision = cache.nace_revision
   AND reference.level_name = 'division'
   AND reference.code = cache.nace_division_code
  WHERE ifNull(cache.nace_division_code, '') != ''
  GROUP BY revision, code, title, breadcrumb
  UNION ALL
  SELECT
    concat('nace:', ifNull(cache.nace_revision, ''), ':', ifNull(cache.nace_group_code, '')) AS id,
    'nace' AS kind,
    ifNull(cache.nace_group_code, '') AS filter_value,
    ifNull(cache.nace_revision, '') AS revision,
    'group' AS level_name,
    ifNull(cache.nace_group_code, '') AS code,
    '' AS code_set,
    ifNull(reference.title, ifNull(cache.nace_group_code, '')) AS title,
    concat(ifNull(cache.nace_section_code, ''), ' / ', ifNull(cache.nace_division_code, '')) AS breadcrumb,
    '' AS mapped_nace_code,
    count() AS company_count,
    lowerUTF8(concat(ifNull(cache.nace_group_code, ''), ' ', ifNull(reference.title, ''), ' ', ifNull(cache.nace_section_code, ''), ' ', ifNull(cache.nace_division_code, ''))) AS search_text
  FROM ` + table + ` AS cache
  LEFT JOIN ` + ch.QualifiedTable("corpscout_reference", "nace_codes") + ` AS reference
    ON reference.revision = cache.nace_revision
   AND reference.level_name = 'group'
   AND reference.code = cache.nace_group_code
  WHERE ifNull(cache.nace_group_code, '') != ''
  GROUP BY revision, code, title, breadcrumb
  UNION ALL
  SELECT
    concat('nace:', ifNull(cache.nace_revision, ''), ':', ifNull(cache.nace_class_code, '')) AS id,
    'nace' AS kind,
    ifNull(cache.nace_class_code, '') AS filter_value,
    ifNull(cache.nace_revision, '') AS revision,
    'class' AS level_name,
    ifNull(cache.nace_class_code, '') AS code,
    '' AS code_set,
    ifNull(cache.nace_title_en, ifNull(cache.nace_class_code, '')) AS title,
    concat(ifNull(cache.nace_section_code, ''), ' / ', ifNull(cache.nace_division_code, ''), ' / ', ifNull(cache.nace_group_code, '')) AS breadcrumb,
    ifNull(cache.nace_class_code, '') AS mapped_nace_code,
    count() AS company_count,
    lowerUTF8(concat(ifNull(cache.nace_class_code, ''), ' ', ifNull(cache.nace_title_en, ''), ' ', ifNull(cache.nace_section_code, ''), ' ', ifNull(cache.nace_division_code, ''), ' ', ifNull(cache.nace_group_code, ''))) AS search_text
  FROM ` + table + ` AS cache
  WHERE ifNull(cache.nace_class_code, '') != ''
  GROUP BY revision, code, title, breadcrumb
  UNION ALL
  SELECT
    concat('source:', ifNull(cache.main_business_line_code_set, ''), ':', ifNull(cache.main_business_line_code, '')) AS id,
    'source_industry' AS kind,
    concat(ifNull(cache.main_business_line_code_set, ''), ':', ifNull(cache.main_business_line_code, '')) AS filter_value,
    ifNull(cache.nace_revision, '') AS revision,
    '' AS level_name,
    ifNull(cache.main_business_line_code, '') AS code,
    ifNull(cache.main_business_line_code_set, '') AS code_set,
    ifNull(cache.main_business_line_description_en, '') AS title,
    '' AS breadcrumb,
    ifNull(cache.nace_code, '') AS mapped_nace_code,
    count() AS company_count,
    lowerUTF8(concat(ifNull(cache.main_business_line_code, ''), ' ', ifNull(cache.main_business_line_code_set, ''), ' ', ifNull(cache.main_business_line_description_en, ''), ' ', ifNull(cache.nace_code, ''))) AS search_text
  FROM ` + table + ` AS cache
  WHERE ifNull(cache.main_business_line_code, '') != ''
  GROUP BY revision, code, code_set, title, mapped_nace_code
)
ORDER BY company_count DESC, lowerUTF8(title), code
LIMIT 5000`
}
```

- [ ] **Step 8: Load both forms and industries in handler**

In `handleListSourceExplorerFilterOptions`, run the form query first and scan forms as today. Then run `buildSourceExplorerIndustryFilterOptionsQuery(table)` and scan into `[]sourceExplorerIndustryFilterOption`. Return:

```go
writeJSON(w, http.StatusOK, sourceExplorerFilterOptionsResponse{
	Forms:           formOptions,
	IndustryOptions: industryOptions,
})
```

- [ ] **Step 9: Run backend tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/httpapi -run 'TestBuildSourceExplorerCompanyListQuery|TestParseSourceExplorer|TestBuildSourceExplorer.*FilterOptionsQuery' -count=1
```

Expected: pass.

- [ ] **Step 10: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add scheduler/internal/httpapi/source_explorer.go scheduler/internal/httpapi/source_explorer_internal_test.go
git commit -m "feat: add finland explorer industry filters"
```

## Task 7: Add UI API Types And Source Action Button

**Files:**
- Modify: `ui/app/types/api.ts`
- Modify: `ui/app/lib/api.ts`
- Modify: `ui/app/components/app/source-detail/ActionsTab.tsx`

- [ ] **Step 1: Update API types**

Modify `ui/app/types/api.ts`:

```ts
export type SourceActionName =
  | "pull_source"
  | "import_clickhouse"
  | "refresh_explorer_cache"
  | "map_industries_to_nace";

export interface SourceAction {
  id: string;
  source_id: string;
  source_name: string;
  action: SourceActionName;
  display_name: string;
  temporal_workflow_type: string;
  temporal_task_queue: string | null;
  enabled: boolean;
  config: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

export interface SourceActionRun {
  id: string;
  source_id: string;
  source_name: string;
  action_id: string;
  action: SourceActionName;
  status: SourceRunStatus;
  temporal_workflow_id: string | null;
  temporal_run_id: string | null;
  started_at: string;
  finished_at: string | null;
  input: Record<string, unknown>;
  result: Record<string, unknown>;
  error_message: string | null;
  created_at: string;
}
```

Add NACE fields:

```ts
export interface SourceExplorerCompany {
  business_id: string;
  country_iso2: string;
  source_slug: string;
  source_run_id: string;
  source_record_id: string;
  name: string;
  registration_date: string;
  end_date: string;
  status_code: string;
  status_description: string;
  trade_register_status_code: string;
  trade_register_status_description: string;
  lifecycle_status: string;
  is_active: boolean;
  main_business_line_code: string;
  main_business_line_code_set: string;
  main_business_line_description_en: string;
  nace_revision: string;
  nace_code: string;
  nace_section_code: string;
  nace_division_code: string;
  nace_group_code: string;
  nace_class_code: string;
  nace_title_en: string;
  nace_mapping_status: string;
  company_form_description_en: string;
  website: string;
  name_history_count: number;
  registered_entry_count: number;
  address_count: number;
  latest_ingested_at: string;
}
```

Add industry option types:

```ts
export interface SourceExplorerIndustryFilterOption {
  id: string;
  kind: "nace" | "source_industry";
  filter_value: string;
  revision: string;
  level_name: string;
  code: string;
  code_set: string;
  title: string;
  breadcrumb: string;
  mapped_nace_code: string;
  count: number;
  search_text: string;
}

export interface SourceExplorerFilterOptionsResponse {
  forms: SourceExplorerFormFilterOption[];
  industry_options: SourceExplorerIndustryFilterOption[];
}
```

- [ ] **Step 2: Update API client**

Modify `ui/app/lib/api.ts`:

```ts
import type { SourceActionName } from "~/types/api";
```

Use `SourceActionName` in `triggerSourceAction`:

```ts
triggerSourceAction: (
  name: string,
  action: SourceActionName,
  body: {
    trigger?: "manual";
    download_action_run_id?: string;
    batch_size?: number;
    limit?: number;
  } = {},
) =>
  post<StartWorkflowResponse>(
    `/sources/${name}/actions/${action}/trigger`,
    body,
  ),
```

Extend `getSourceExplorerCompanies` params:

```ts
industry_nace?: string[];
source_industry?: string[];
```

Append repeated params:

```ts
for (const value of params.industry_nace ?? []) {
  if (value) qs.append("industry_nace", value);
}
for (const value of params.source_industry ?? []) {
  if (value) qs.append("source_industry", value);
}
```

- [ ] **Step 3: Add action button**

Modify `ui/app/components/app/source-detail/ActionsTab.tsx`:

```ts
type TriggerKey =
  | "pull_source"
  | "import_clickhouse"
  | "refresh_explorer_cache"
  | "map_industries_to_nace"
  | "sync";
```

Add enabled state:

```ts
const industryMappingEnabled =
  actionsByKey.get("map_industries_to_nace")?.enabled ?? false;
```

Add trigger function:

```ts
function triggerIndustryMapping() {
  void runAndRefresh("map_industries_to_nace", () =>
    api.triggerSourceAction(source.name, "map_industries_to_nace", {
      trigger: "manual",
    }),
  );
}
```

Add button next to refresh explorer:

```tsx
{actionsByKey.has("map_industries_to_nace") ? (
  <Button
    size="sm"
    variant="outline"
    onClick={triggerIndustryMapping}
    disabled={busy || loading || !industryMappingEnabled}
  >
    <RefreshCw className="size-4" />
    Map industries
  </Button>
) : null}
```

- [ ] **Step 4: Run typecheck**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/ui
npm run typecheck
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add ui/app/types/api.ts ui/app/lib/api.ts ui/app/components/app/source-detail/ActionsTab.tsx
git commit -m "feat: add industry mapping action to source UI"
```

## Task 8: Add Industry Dropdown To Explorer Filters

**Files:**
- Modify: `ui/app/components/app/source-detail/FinlandPRHYTJExplorerTab.tsx`
- Modify: `ui/app/components/app/source-detail/SourceExplorerFiltersSheet.tsx`

- [ ] **Step 1: Extend explorer tab URL state**

Modify `ui/app/components/app/source-detail/FinlandPRHYTJExplorerTab.tsx`:

```ts
import type {
  DataSource,
  SourceExplorerCompany,
  SourceExplorerFormFilterOption,
  SourceExplorerIndustryFilterOption,
} from "~/types/api";
```

Add state:

```ts
const [industryOptions, setIndustryOptions] = useState<SourceExplorerIndustryFilterOption[]>([]);
const industryNACE = useMemo(
  () => paramList(searchParams.getAll("industry_nace")),
  [searchParams],
);
const sourceIndustries = useMemo(
  () => paramList(searchParams.getAll("source_industry")),
  [searchParams],
);
const activeFilterCount =
  (activeOnly ? 1 : 0) + formCodes.length + industryNACE.length + sourceIndustries.length;
```

Update `api.getSourceExplorerCompanies` call:

```ts
industry_nace: industryNACE,
source_industry: sourceIndustries,
```

Update dependencies:

```ts
}, [activeOnly, formCodes, industryNACE, page, q, sortDir, sortKey, source.name, sourceIndustries]);
```

When loading filter options:

```ts
if (!ignore) {
  setFormOptions(Array.isArray(res.forms) ? res.forms : []);
  setIndustryOptions(Array.isArray(res.industry_options) ? res.industry_options : []);
}
```

When failing:

```ts
setIndustryOptions([]);
```

Update `clearFilters`:

```ts
next.delete("industry_nace");
next.delete("source_industry");
```

Update `applyFilters` signature and body:

```ts
function applyFilters(value: {
  activeOnly: boolean;
  formCodes: string[];
  industryNACE: string[];
  sourceIndustries: string[];
}) {
  const next = new URLSearchParams(searchParams);
  if (value.activeOnly) next.set("active", "true");
  else next.delete("active");
  setRepeatedParam(next, "form", value.formCodes);
  setRepeatedParam(next, "industry_nace", value.industryNACE);
  setRepeatedParam(next, "source_industry", value.sourceIndustries);
  next.delete("page");
  setSearchParams(next, { replace: true });
}
```

Update clear button condition:

```tsx
{(q || activeOnly || formCodes.length > 0 || industryNACE.length > 0 || sourceIndustries.length > 0) ? (
```

Pass filter sheet props:

```tsx
<SourceExplorerFiltersSheet
  open={filtersOpen}
  onOpenChange={setFiltersOpen}
  forms={formOptions}
  industryOptions={industryOptions}
  value={{ activeOnly, formCodes, industryNACE, sourceIndustries }}
  loading={loadingFilters}
  error={filterError}
  onApply={applyFilters}
  onClear={clearFilters}
/>
```

- [ ] **Step 2: Show NACE in the business line column**

In the `Business Line` cell, render source code set and NACE mapping:

```tsx
<span className="font-mono text-xs text-muted-foreground">
  {emptyText(row.original.main_business_line_code_set)} {emptyText(row.original.main_business_line_code)}
</span>
{row.original.nace_code ? (
  <span className="text-xs text-muted-foreground">
    NACE {row.original.nace_code}
    {row.original.nace_title_en ? ` · ${row.original.nace_title_en}` : ""}
  </span>
) : null}
```

- [ ] **Step 3: Extend filter sheet props and draft state**

Modify `ui/app/components/app/source-detail/SourceExplorerFiltersSheet.tsx`:

```ts
import type {
  SourceExplorerFormFilterOption,
  SourceExplorerIndustryFilterOption,
} from "~/types/api";
```

Extend value:

```ts
interface SourceExplorerFiltersValue {
  activeOnly: boolean;
  formCodes: string[];
  industryNACE: string[];
  sourceIndustries: string[];
}
```

Extend props:

```ts
industryOptions: SourceExplorerIndustryFilterOption[];
```

Add state:

```ts
const [draftIndustryNACE, setDraftIndustryNACE] = useState(value.industryNACE);
const [draftSourceIndustries, setDraftSourceIndustries] = useState(value.sourceIndustries);
const [industrySearch, setIndustrySearch] = useState("");
```

Reset state when opening:

```ts
setDraftIndustryNACE(value.industryNACE);
setDraftSourceIndustries(value.sourceIndustries);
setIndustrySearch("");
```

- [ ] **Step 4: Add industry option helpers**

Add helpers:

```ts
function industryOptionKey(option: SourceExplorerIndustryFilterOption): string {
  return `${option.kind}:${option.filter_value}`;
}

function industryOptionLabel(option: SourceExplorerIndustryFilterOption): string {
  const code = option.kind === "source_industry"
    ? `${option.code_set} ${option.code}`
    : `NACE ${option.code}`;
  return option.title ? `${option.title} (${code})` : code;
}

function selectedIndustryKeys(industryNACE: string[], sourceIndustries: string[]): Set<string> {
  const keys = new Set<string>();
  for (const value of industryNACE) keys.add(`nace:${value}`);
  for (const value of sourceIndustries) keys.add(`source_industry:${value}`);
  return keys;
}
```

Add derived values:

```ts
const selectedIndustryOptionKeys = useMemo(
  () => selectedIndustryKeys(draftIndustryNACE, draftSourceIndustries),
  [draftIndustryNACE, draftSourceIndustries],
);

const selectedIndustries = useMemo(
  () =>
    industryOptions.filter((option) =>
      selectedIndustryOptionKeys.has(industryOptionKey(option)),
    ),
  [industryOptions, selectedIndustryOptionKeys],
);

const filteredIndustries = useMemo(() => {
  const query = industrySearch.trim().toLowerCase();
  const options = query
    ? industryOptions.filter((option) =>
        `${option.search_text} ${option.title} ${option.code} ${option.code_set}`
          .toLowerCase()
          .includes(query),
      )
    : industryOptions;
  return options.slice(0, 200);
}, [industryOptions, industrySearch]);
```

Add toggle:

```ts
function toggleIndustry(option: SourceExplorerIndustryFilterOption, checked: boolean) {
  if (option.kind === "nace") {
    setDraftIndustryNACE((current) => {
      if (checked) return current.includes(option.filter_value) ? current : [...current, option.filter_value];
      return current.filter((item) => item !== option.filter_value);
    });
    return;
  }
  setDraftSourceIndustries((current) => {
    if (checked) return current.includes(option.filter_value) ? current : [...current, option.filter_value];
    return current.filter((item) => item !== option.filter_value);
  });
}
```

- [ ] **Step 5: Add Industry section markup**

Add this section below the Form section:

```tsx
<Separator />

<section className="flex flex-col gap-3">
  <div className="flex items-center justify-between gap-3">
    <div>
      <h3 className="text-sm font-medium">Industry</h3>
      <p className="text-xs text-muted-foreground">
        Search NACE hierarchy or original Finland industry values.
      </p>
    </div>
    {selectedIndustryOptionKeys.size > 0 ? (
      <Badge variant="secondary">
        {selectedIndustryOptionKeys.size.toLocaleString()} selected
      </Badge>
    ) : null}
  </div>

  {selectedIndustries.length > 0 ? (
    <div className="flex flex-wrap gap-2">
      {selectedIndustries.map((option) => (
        <Badge key={industryOptionKey(option)} variant="outline" asChild>
          <button
            type="button"
            onClick={() => toggleIndustry(option, false)}
            aria-label={`Remove ${industryOptionLabel(option)}`}
          >
            <span>{industryOptionLabel(option)}</span>
            <X />
          </button>
        </Badge>
      ))}
    </div>
  ) : null}

  <Input
    value={industrySearch}
    onChange={(event) => setIndustrySearch(event.target.value)}
    placeholder="Search industries, NACE, TOIMI"
  />

  <div className="max-h-96 overflow-y-auto rounded-md border">
    {loading ? (
      <div className="p-3 text-sm text-muted-foreground">Loading industries...</div>
    ) : error ? (
      <div className="p-3 text-sm text-muted-foreground">{error}</div>
    ) : filteredIndustries.length > 0 ? (
      <div className="flex flex-col">
        {filteredIndustries.map((option) => {
          const checked = selectedIndustryOptionKeys.has(industryOptionKey(option));
          return (
            <label
              key={industryOptionKey(option)}
              className="flex cursor-pointer items-start gap-3 border-b p-3 text-sm last:border-b-0 hover:bg-muted/40"
            >
              <Checkbox
                checked={checked}
                onCheckedChange={(nextChecked) =>
                  toggleIndustry(option, nextChecked === true)
                }
              />
              <span className="flex min-w-0 flex-1 flex-col gap-1">
                <span className="truncate font-medium">
                  {option.title || option.code}
                </span>
                <span className="font-mono text-xs text-muted-foreground">
                  {option.kind === "nace"
                    ? `NACE ${option.code}${option.revision ? ` · Rev. ${option.revision}` : ""}`
                    : `${option.code_set} ${option.code}${option.mapped_nace_code ? ` · NACE ${option.mapped_nace_code}` : ""}`}
                </span>
                {option.breadcrumb ? (
                  <span className="text-xs text-muted-foreground">
                    {option.breadcrumb}
                  </span>
                ) : null}
              </span>
              <span className="text-xs text-muted-foreground">
                {option.count.toLocaleString()}
              </span>
            </label>
          );
        })}
      </div>
    ) : (
      <div className="p-3 text-sm text-muted-foreground">No industries found.</div>
    )}
  </div>
</section>
```

- [ ] **Step 6: Include industry filters in apply and clear**

Update `clearDraft`:

```ts
setDraftIndustryNACE([]);
setDraftSourceIndustries([]);
setIndustrySearch("");
```

Update `applyFilters`:

```ts
onApply({
  activeOnly: draftActiveOnly,
  formCodes: draftFormCodes,
  industryNACE: draftIndustryNACE,
  sourceIndustries: draftSourceIndustries,
});
```

- [ ] **Step 7: Run typecheck**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/ui
npm run typecheck
```

Expected: pass.

- [ ] **Step 8: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add ui/app/components/app/source-detail/FinlandPRHYTJExplorerTab.tsx ui/app/components/app/source-detail/SourceExplorerFiltersSheet.tsx
git commit -m "feat: add industry filters to finland explorer"
```

## Task 9: End-To-End Verification

**Files:**
- No planned source edits.

- [ ] **Step 1: Run backend test suite**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./... -count=1
```

Expected: pass.

- [ ] **Step 2: Run frontend typecheck**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/ui
npm run typecheck
```

Expected: pass.

- [ ] **Step 3: Run whitespace check**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git diff --check
```

Expected: no output.

- [ ] **Step 4: Apply migrations against the target environments**

Run Postgres migrations:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make migrate-up
```

Run ClickHouse migrations against the remote ClickHouse server. The `.env` file or shell environment must set `CLICKHOUSE_MIGRATE_URL` to the remote server, not the local Docker hostname:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make clickhouse-migrate-up
```

Expected: migrations apply without error and these tables/columns exist:

```sql
SHOW TABLES FROM corpscout_sources LIKE 'fi_prhytj_industry_nace_mappings';
DESCRIBE TABLE corpscout_sources.fi_prhytj_company_explorer_cache;
```

- [ ] **Step 5: Run NACE sync if reference tables are empty**

From the UI or API, run:

```text
POST /api/v1/workflows/nace/clickhouse-sync
```

Expected: `corpscout_reference.nace_codes` contains active class rows for revisions `2` and `2.1`.

- [ ] **Step 6: Trigger mapping action**

From `/sources/finland_prhytj/actions`, click `Map industries`.

Expected: source action run succeeds and result contains nonzero `rows`, with `mapped_rows` greater than zero.

- [ ] **Step 7: Refresh explorer cache**

From `/sources/finland_prhytj/actions`, click `Refresh explorer`.

Expected: source action run succeeds and cache rows have NACE fields:

```sql
SELECT
  countIf(nace_mapping_status = 'mapped') AS mapped,
  countIf(nace_mapping_status = 'unmapped') AS unmapped
FROM corpscout_sources.fi_prhytj_company_explorer_cache;
```

- [ ] **Step 8: Verify UI filtering**

Open:

```text
http://127.0.0.1:5173/sources/finland_prhytj/explorer
```

Expected:

- Filter sheet opens.
- Industry section shows NACE and source industry options.
- Searching `real estate` shows NACE/source options.
- Selecting NACE `68` writes `industry_nace=68` into the URL.
- Selecting `TOIMI4 68203` writes `source_industry=TOIMI4%3A68203` into the URL.
- Table reloads with filtered results.

- [ ] **Step 9: Commit verification fixes if any were needed**

If verification required small code fixes, commit them:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add .
git commit -m "fix: verify finland nace industry search"
```

Skip this commit when verification required no source changes.
