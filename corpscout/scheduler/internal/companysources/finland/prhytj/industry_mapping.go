package prhytj

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"

	ch "github.com/pulsarpoint/corpscout/scheduler/internal/clickhouse"
)

const (
	IndustryNACEMappingTable = "fi_prhytj_industry_nace_mappings"
	naceReferenceDatabase    = "corpscout"
	naceCodesTable           = "nace_codes"
)

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

	var activeNACEClassRows uint64
	if err := writer.QueryRow(ctx, "SELECT count() FROM "+ch.QualifiedTable(naceReferenceDatabase, naceCodesTable)+" WHERE `revision` IN ('2', '2.1') AND `level_name` = 'class' AND `active` = true").Scan(&activeNACEClassRows); err != nil {
		return IndustryNACEMappingRefreshResult{}, errors.Wrap(err, "count active NACE class reference rows")
	}
	if activeNACEClassRows == 0 {
		return IndustryNACEMappingRefreshResult{}, errors.New("active NACE class reference rows are required for revisions 2 or 2.1")
	}

	tempTable := IndustryNACEMappingTable + "_refresh_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	database := writer.Database()
	tempQualified := ch.QualifiedTable(database, tempTable)
	mappingQualified := ch.QualifiedTable(database, IndustryNACEMappingTable)

	if err := writer.Exec(ctx, "DROP TABLE IF EXISTS "+tempQualified); err != nil {
		return IndustryNACEMappingRefreshResult{}, errors.Wrap(err, "drop stale industry NACE mapping refresh table")
	}
	tempCreated := false
	defer func() {
		if tempCreated {
			_ = writer.Exec(context.Background(), "DROP TABLE IF EXISTS "+tempQualified)
		}
	}()

	if err := writer.Exec(ctx, "CREATE TABLE "+tempQualified+" AS "+mappingQualified); err != nil {
		return IndustryNACEMappingRefreshResult{}, errors.Wrap(err, "create industry NACE mapping refresh table")
	}
	tempCreated = true
	if err := writer.Exec(ctx, ch.BuildTruncateQuery(database, tempTable)); err != nil {
		return IndustryNACEMappingRefreshResult{}, errors.Wrap(err, "clear industry NACE mapping refresh table")
	}

	mappedAt := time.Now().UTC().Truncate(time.Millisecond)
	if err := writer.Exec(ctx, buildIndustryNACEMappingInsertQuery(database, tempTable, mappedAt)); err != nil {
		return IndustryNACEMappingRefreshResult{}, errors.Wrap(err, "load industry NACE mapping refresh table")
	}

	var rows, mappedRows, unmappedRows uint64
	if err := writer.QueryRow(ctx, "SELECT count(), countIf(`mapping_status` = 'mapped'), countIf(`mapping_status` = 'unmapped') FROM "+tempQualified).Scan(&rows, &mappedRows, &unmappedRows); err != nil {
		return IndustryNACEMappingRefreshResult{}, errors.Wrap(err, "count industry NACE mapping refresh table")
	}
	if err := writer.Exec(ctx, "EXCHANGE TABLES "+mappingQualified+" AND "+tempQualified); err != nil {
		return IndustryNACEMappingRefreshResult{}, errors.Wrap(err, "swap industry NACE mapping table")
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
	return fmt.Sprintf(`INSERT INTO %s (%s)
WITH
descriptions_en AS (
  SELECT
    business_id,
    source_run_id,
    business_line_item_hash,
    argMax(nullIf(description, ''), ingested_at) AS source_description_en
  FROM %s
  WHERE language_code = '3'
  GROUP BY business_id, source_run_id, business_line_item_hash
),
raw_industries AS (
  SELECT
    nullIf(bl.business_line_code_set, '') AS source_code_set,
    nullIf(bl.business_line_type, '') AS source_code,
    d.source_description_en AS source_description_en,
    bl.ingested_at AS ingested_at
  FROM %s AS bl
  LEFT JOIN descriptions_en AS d
    ON d.business_id = bl.business_id
   AND d.source_run_id = bl.source_run_id
   AND d.business_line_item_hash = bl.source_item_hash
  WHERE nullIf(bl.business_line_type, '') IS NOT NULL
),
industries AS (
  SELECT
    source_code_set,
    source_code,
    argMax(source_description_en, ingested_at) AS source_description_en
  FROM raw_industries
  GROUP BY source_code_set, source_code
),
candidates AS (
  SELECT
    source_code_set,
    source_code,
    if(length(ifNull(source_code, '')) >= 4, substring(ifNull(source_code, ''), 1, 4), NULL) AS source_code_prefix4,
    if(length(ifNull(source_code, '')) >= 4, concat(substring(ifNull(source_code, ''), 1, 2), '.', substring(ifNull(source_code, ''), 3, 2)), NULL) AS source_code_dotted4,
    if(length(ifNull(source_code, '')) >= 5, substring(ifNull(source_code, ''), 5), NULL) AS source_extra_digit,
    source_description_en,
    multiIf(source_code_set = 'TOIMI4', '2.1', source_code_set = 'TOIMI3', '2', '') AS candidate_revision
  FROM industries
)
SELECT
  source_code_set,
  source_code,
  source_code_prefix4,
  source_code_dotted4,
  source_extra_digit,
  source_description_en,
  nullIf(candidate_revision, '') AS nace_revision,
  nullIf(nace.code, '') AS nace_code,
  nullIf(nace.normalized_code, '') AS nace_normalized_code,
  nullIf(nace.section_code, '') AS nace_section_code,
  nullIf(nace.division_code, '') AS nace_division_code,
  nullIf(nace.group_code, '') AS nace_group_code,
  nullIf(nace.class_code, '') AS nace_class_code,
  nullIf(nace.title, '') AS nace_title_en,
  multiIf(nace.normalized_code != '', 'toimi_5_digit_prefix', candidate_revision != '' AND source_code_prefix4 IS NOT NULL, 'toimi_prefix_unmatched', 'unsupported_code_set') AS mapping_method,
  if(nace.normalized_code != '', 'mapped', 'unmapped') AS mapping_status,
  %s AS %s
FROM candidates
LEFT JOIN %s AS nace
  ON nace.revision = candidate_revision
 AND nace.level_name = 'class'
 AND nace.normalized_code = source_code_prefix4
 AND nace.active = true`,
		ch.QualifiedTable(database, table),
		industryNACEMappingColumnList(),
		ch.QualifiedTable(database, businessLineDescriptionsTable),
		ch.QualifiedTable(database, businessLinesTable),
		clickHouseDateTime64Literal(mappedAt),
		ch.QuoteIdent("mapped_at"),
		ch.QualifiedTable(naceReferenceDatabase, naceCodesTable),
	)
}

func industryNACEMappingColumnList() string {
	quotedColumns := make([]string, 0, len(industryNACEMappingColumns))
	for _, column := range industryNACEMappingColumns {
		quotedColumns = append(quotedColumns, ch.QuoteIdent(column))
	}
	return strings.Join(quotedColumns, ", ")
}
