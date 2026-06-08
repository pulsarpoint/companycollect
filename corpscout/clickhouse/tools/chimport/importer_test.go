package main

import (
	"io"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBuildNativeSelectSQL(t *testing.T) {
	table := TableConfig{
		Parquet: "companies.parquet",
		Table:   "fi_prhytj_companies",
		InjectColumns: map[string]string{
			"source_export_id": "UUID",
			"ingested_at":      "DateTime64(3, 'UTC')",
		},
	}

	sql, err := buildNativeSelectSQL(table, "/exports/companies.parquet", "11111111-1111-1111-1111-111111111111")
	require.NoError(t, err)

	require.Contains(t, sql, "SELECT *, now64(3) AS `ingested_at`, toUUID('11111111-1111-1111-1111-111111111111') AS `source_export_id`")
	require.Contains(t, sql, "FROM file('/exports/companies.parquet', Parquet)")
	require.Contains(t, sql, "FORMAT Native")
}

func TestBuildNativeSelectSQLRejectsUnknownInjectedColumn(t *testing.T) {
	table := TableConfig{
		Table: "fi_prhytj_companies",
		InjectColumns: map[string]string{
			"source_export_id": "UUID",
			"unexpected":       "String",
		},
	}

	_, err := buildNativeSelectSQL(table, "/exports/companies.parquet", "11111111-1111-1111-1111-111111111111")
	require.EqualError(t, err, "table fi_prhytj_companies unknown injected column unexpected")
}

func TestBuildNativeInsertSQL(t *testing.T) {
	require.Equal(
		t,
		"INSERT INTO `corpscout_sources`.`fi_prhytj_companies` FORMAT Native",
		buildNativeInsertSQL("corpscout_sources", "fi_prhytj_companies"),
	)
}

func TestBuildTruncateSQL(t *testing.T) {
	require.Equal(
		t,
		"TRUNCATE TABLE `corpscout_sources`.`fi_prhytj_companies`",
		buildTruncateSQL("corpscout_sources", "fi_prhytj_companies"),
	)
}

func TestRunNativePipelineReturnsClientErrorWhenClientExitsEarly(t *testing.T) {
	reader, writer := io.Pipe()
	localCmd := exec.Command("sh", "-c", "yes | head -c 10000000")
	clientCmd := exec.Command("sh", "-c", "echo client failed >&2; exit 7")
	localCmd.Stdout = writer
	clientCmd.Stdin = reader

	done := make(chan error, 1)
	go func() {
		done <- runNativePipeline(localCmd, clientCmd, reader, writer)
	}()

	select {
	case err := <-done:
		require.Error(t, err)
		require.Contains(t, err.Error(), "clickhouse-client import failed")
		require.Contains(t, err.Error(), "client failed")
	case <-time.After(5 * time.Second):
		t.Fatal("pipeline did not return after client exited")
	}
}

func TestDockerMountRoot(t *testing.T) {
	require.Equal(t, "/Users", dockerMountRoot("/Users/graovic/export/companies.parquet"))
	require.Equal(t, "/", dockerMountRoot("/var/tmp/export/companies.parquet"))
}

func TestClickHouseEscaping(t *testing.T) {
	require.Equal(t, "`odd\\\\\\`name`", quoteIdent("odd\\`name"))
	require.Equal(t, "'/exports/odd\\\\path\\'s.parquet'", clickHouseStringLiteral("/exports/odd\\path's.parquet"))
}

func TestNativeSelectSQLDoesNotUseServerSideTableFunctionForInsert(t *testing.T) {
	table := TableConfig{
		Table: "fi_prhytj_raw_records",
		InjectColumns: map[string]string{
			"source_export_id": "UUID",
		},
	}

	sql, err := buildNativeSelectSQL(table, "/exports/raw_records.parquet", "00000000-0000-0000-0000-000000000000")
	require.NoError(t, err)
	require.True(t, strings.HasSuffix(sql, "FORMAT Native"))
}
