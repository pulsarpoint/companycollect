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
	target, err := ParseNativeURL("clickhouse://companycollect:9002?username=default&password=change-me&database=corpscout")
	require.NoError(t, err)
	require.Equal(t, Target{
		Host:     "companycollect",
		Port:     "9002",
		Username: "default",
		Password: "change-me",
		Database: "corpscout",
	}, target)
}

func TestBuildInsertQuery(t *testing.T) {
	query := BuildInsertQuery("corpscout", "fi_prhytj_identifiers", []string{"business_id", "identifier_value"})
	require.Equal(t, "INSERT INTO `corpscout`.`fi_prhytj_identifiers` (`business_id`, `identifier_value`)", query)
}

func TestBuildInsertQuerySupportsReferenceDatabase(t *testing.T) {
	query := BuildInsertQuery("corpscout", "nace_codes", []string{"revision", "code"})
	require.Equal(t, "INSERT INTO `corpscout`.`nace_codes` (`revision`, `code`)", query)
}

func TestBuildTruncateQuery(t *testing.T) {
	query := BuildTruncateQuery("corpscout", "fi_prhytj_identifiers")
	require.Equal(t, "TRUNCATE TABLE IF EXISTS `corpscout`.`fi_prhytj_identifiers`", query)
}

func TestBuildTruncateQuerySupportsReferenceDatabase(t *testing.T) {
	query := BuildTruncateQuery("corpscout", "nace_codes")
	require.Equal(t, "TRUNCATE TABLE IF EXISTS `corpscout`.`nace_codes`", query)
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
	require.NoError(t, writer.conn.Exec(ctx, "DROP TABLE IF EXISTS "+QualifiedTable(writer.database, table)))
	require.NoError(t, writer.conn.Exec(ctx, "CREATE TABLE "+QualifiedTable(writer.database, table)+" (`business_id` String, `source_export_id` UUID, `ingested_at` DateTime64(3, 'UTC')) ENGINE = Memory"))
	defer writer.conn.Exec(ctx, "DROP TABLE IF EXISTS "+QualifiedTable(writer.database, table))

	exportID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	ingestedAt := time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC)
	require.NoError(t, writer.Insert(ctx, Insert{
		Table:   table,
		Columns: []string{"business_id", "source_export_id", "ingested_at"},
		Rows: []map[string]any{{
			"business_id":      "0100130-4",
			"source_export_id": exportID,
			"ingested_at":      ingestedAt,
		}},
	}))

	var count uint64
	require.NoError(t, writer.conn.QueryRow(ctx, "SELECT count() FROM "+QualifiedTable(writer.database, table)).Scan(&count))
	require.Equal(t, uint64(1), count)
}
