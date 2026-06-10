package nace

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

func TestNACEClickHouseCodeRowsDeriveHierarchy(t *testing.T) {
	sectionID := uuid.New()
	divisionID := uuid.New()
	groupID := uuid.New()
	classID := uuid.New()
	rows := []db.ListNACECodesForClickHouseRow{
		{Revision: "2.1", ID: sectionID, Code: "N", NormalizedCode: "N", Level: 1, LevelName: "section", Title: "Administrative and support service activities", Active: true},
		{Revision: "2.1", ID: divisionID, Code: "82", NormalizedCode: "82", Level: 2, LevelName: "division", ParentCode: stringPtr("N"), ParentID: pgtype.UUID{Bytes: sectionID, Valid: true}, Title: "Office administrative, office support and other business support activities", Active: true},
		{Revision: "2.1", ID: groupID, Code: "82.2", NormalizedCode: "822", Level: 3, LevelName: "group", ParentCode: stringPtr("82"), ParentID: pgtype.UUID{Bytes: divisionID, Valid: true}, Title: "Activities of call centres", Active: true},
		{Revision: "2.1", ID: classID, Code: "82.20", NormalizedCode: "8220", Level: 4, LevelName: "class", ParentCode: stringPtr("82.2"), ParentID: pgtype.UUID{Bytes: groupID, Valid: true}, Title: "Activities of call centres", Active: true},
	}

	got := buildNACECodeClickHouseRows(rows, testSyncTime())

	require.Len(t, got, 4)
	classRow := got[3]
	require.Equal(t, stringPtr("N"), classRow["section_code"])
	require.Equal(t, stringPtr("82"), classRow["division_code"])
	require.Equal(t, stringPtr("82.2"), classRow["group_code"])
	require.Equal(t, stringPtr("82.20"), classRow["class_code"])
}

func stringPtr(value string) *string { return &value }

func testSyncTime() time.Time {
	return time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
}
