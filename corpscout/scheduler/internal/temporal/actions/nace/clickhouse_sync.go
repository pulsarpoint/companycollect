package nace

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	chwriter "github.com/pulsarpoint/corpscout/scheduler/internal/clickhouse"
	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	naceworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/temporal/workflow/nace"
)

const clickHouseReferenceDatabase = "corpscout"

func (a *Actions) SyncNACEToClickHouseActivity(ctx context.Context, input SyncNACEToClickHouseActivityInput) (SyncNACEToClickHouseActivityResult, error) {
	_ = input
	if a == nil || a.pool == nil {
		return SyncNACEToClickHouseActivityResult{}, errors.New("nace taxonomy database is not available")
	}
	if a.clickHouseNativeURL == "" {
		return SyncNACEToClickHouseActivityResult{}, errors.New("clickhouse native url is required")
	}

	queries := db.New(a.pool)
	classifications, err := queries.ListNACEClassificationsForClickHouse(ctx)
	if err != nil {
		return SyncNACEToClickHouseActivityResult{}, errors.Wrap(err, "list nace classifications for clickhouse")
	}
	codes, err := queries.ListNACECodesForClickHouse(ctx)
	if err != nil {
		return SyncNACEToClickHouseActivityResult{}, errors.Wrap(err, "list nace codes for clickhouse")
	}
	aliases, err := queries.ListNACECodeAliasesForClickHouse(ctx)
	if err != nil {
		return SyncNACEToClickHouseActivityResult{}, errors.Wrap(err, "list nace aliases for clickhouse")
	}

	writer, err := chwriter.Open(ctx, a.clickHouseNativeURL)
	if err != nil {
		return SyncNACEToClickHouseActivityResult{}, errors.Wrap(err, "open clickhouse writer")
	}
	defer func() { _ = writer.Close() }()

	syncedAt := time.Now().UTC()
	if err := writer.TruncateTablesIn(ctx, clickHouseReferenceDatabase, []string{"nace_code_aliases", "nace_codes", "nace_classifications"}); err != nil {
		return SyncNACEToClickHouseActivityResult{}, err
	}
	if err := writer.InsertInto(ctx, clickHouseReferenceDatabase, chwriter.Insert{
		Table:   "nace_classifications",
		Columns: []string{"revision", "name", "valid_from", "valid_to", "source_url", "active_codes", "inactive_codes", "synced_at"},
		Rows:    buildNACEClassificationClickHouseRows(classifications, syncedAt),
	}); err != nil {
		return SyncNACEToClickHouseActivityResult{}, errors.Wrap(err, "insert clickhouse nace classifications")
	}
	if err := writer.InsertInto(ctx, clickHouseReferenceDatabase, chwriter.Insert{
		Table:   "nace_codes",
		Columns: []string{"revision", "code", "normalized_code", "level", "level_name", "parent_code", "parent_normalized_code", "title", "description", "active", "section_code", "division_code", "group_code", "class_code", "synced_at"},
		Rows:    buildNACECodeClickHouseRows(codes, syncedAt),
	}); err != nil {
		return SyncNACEToClickHouseActivityResult{}, errors.Wrap(err, "insert clickhouse nace codes")
	}
	if err := writer.InsertInto(ctx, clickHouseReferenceDatabase, chwriter.Insert{
		Table:   "nace_code_aliases",
		Columns: []string{"revision", "code", "alias_type", "alias_code", "normalized_alias_code", "synced_at"},
		Rows:    buildNACEAliasClickHouseRows(aliases, syncedAt),
	}); err != nil {
		return SyncNACEToClickHouseActivityResult{}, errors.Wrap(err, "insert clickhouse nace aliases")
	}

	return SyncNACEToClickHouseActivityResult{
		Status:                naceworkflow.SyncStatusSucceeded,
		ClassificationsSynced: len(classifications),
		CodesSynced:           len(codes),
		AliasesSynced:         len(aliases),
		Message:               "nace taxonomy synced to clickhouse",
	}, nil
}

func buildNACEClassificationClickHouseRows(rows []db.ListNACEClassificationsForClickHouseRow, syncedAt time.Time) []map[string]any {
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		out = append(out, map[string]any{
			"revision":       row.Revision,
			"name":           row.Name,
			"valid_from":     pgDateValue(row.ValidFrom),
			"valid_to":       pgDateValue(row.ValidTo),
			"source_url":     row.SourceUrl,
			"active_codes":   uint64(row.ActiveCodes),
			"inactive_codes": uint64(row.InactiveCodes),
			"synced_at":      syncedAt,
		})
	}
	return out
}

func buildNACECodeClickHouseRows(rows []db.ListNACECodesForClickHouseRow, syncedAt time.Time) []map[string]any {
	byID := make(map[uuid.UUID]db.ListNACECodesForClickHouseRow, len(rows))
	for _, row := range rows {
		byID[row.ID] = row
	}

	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		hierarchy := naceHierarchy(row, byID)
		out = append(out, map[string]any{
			"revision":               row.Revision,
			"code":                   row.Code,
			"normalized_code":        row.NormalizedCode,
			"level":                  uint8(row.Level),
			"level_name":             row.LevelName,
			"parent_code":            row.ParentCode,
			"parent_normalized_code": row.ParentNormalizedCode,
			"title":                  row.Title,
			"description":            row.Description,
			"active":                 row.Active,
			"section_code":           hierarchy.SectionCode,
			"division_code":          hierarchy.DivisionCode,
			"group_code":             hierarchy.GroupCode,
			"class_code":             hierarchy.ClassCode,
			"synced_at":              syncedAt,
		})
	}
	return out
}

func buildNACEAliasClickHouseRows(rows []db.ListNACECodeAliasesForClickHouseRow, syncedAt time.Time) []map[string]any {
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		out = append(out, map[string]any{
			"revision":              row.Revision,
			"code":                  row.Code,
			"alias_type":            row.AliasType,
			"alias_code":            row.AliasCode,
			"normalized_alias_code": row.NormalizedAliasCode,
			"synced_at":             syncedAt,
		})
	}
	return out
}

type naceCodeHierarchy struct {
	SectionCode  *string
	DivisionCode *string
	GroupCode    *string
	ClassCode    *string
}

func naceHierarchy(row db.ListNACECodesForClickHouseRow, byID map[uuid.UUID]db.ListNACECodesForClickHouseRow) naceCodeHierarchy {
	var hierarchy naceCodeHierarchy
	seen := map[uuid.UUID]bool{}
	current := row
	for {
		assignNACEHierarchyCode(&hierarchy, current)
		if !current.ParentID.Valid {
			break
		}
		parentID := uuid.UUID(current.ParentID.Bytes)
		if seen[parentID] {
			break
		}
		seen[parentID] = true
		parent, ok := byID[parentID]
		if !ok {
			break
		}
		current = parent
	}
	return hierarchy
}

func assignNACEHierarchyCode(h *naceCodeHierarchy, row db.ListNACECodesForClickHouseRow) {
	code := row.Code
	switch row.LevelName {
	case "section":
		h.SectionCode = &code
	case "division":
		h.DivisionCode = &code
	case "group":
		h.GroupCode = &code
	case "class":
		h.ClassCode = &code
	}
}

func pgDateValue(value pgtype.Date) any {
	if !value.Valid {
		return nil
	}
	return value.Time
}
