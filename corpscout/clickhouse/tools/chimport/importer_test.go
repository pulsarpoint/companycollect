package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildInsertSQL(t *testing.T) {
	table := TableConfig{
		Parquet: "companies.parquet",
		Table:   "fi_prhytj_companies",
		InjectColumns: map[string]string{
			"source_export_id": "UUID",
			"ingested_at":      "DateTime64(3, 'UTC')",
		},
	}

	sql, err := buildInsertSQL("corpscout_sources", table, "11111111-1111-1111-1111-111111111111")
	require.NoError(t, err)

	require.Contains(t, sql, "INSERT INTO `corpscout_sources`.`fi_prhytj_companies`")
	require.Contains(t, sql, "SELECT *, now64(3) AS `ingested_at`, toUUID('11111111-1111-1111-1111-111111111111') AS `source_export_id`")
	require.Contains(t, sql, "FROM input_file")
	require.NotContains(t, strings.ToLower(sql), "file(")
}

func TestBuildInsertSQLRejectsUnknownInjectedColumn(t *testing.T) {
	table := TableConfig{
		Table: "fi_prhytj_companies",
		InjectColumns: map[string]string{
			"source_export_id": "UUID",
			"unexpected":       "String",
		},
	}

	_, err := buildInsertSQL("corpscout_sources", table, "11111111-1111-1111-1111-111111111111")
	require.EqualError(t, err, "table fi_prhytj_companies unknown injected column unexpected")
}

func TestExecuteParquetImportPostsMultipartExternalTable(t *testing.T) {
	dir := t.TempDir()
	parquetPath := filepath.Join(dir, "companies.parquet")
	require.NoError(t, os.WriteFile(parquetPath, []byte("fake parquet"), 0o600))

	const sql = "INSERT INTO `corpscout_sources`.`fi_prhytj_companies` SELECT * FROM input_file"
	var sawRequest bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawRequest = true
		require.Equal(t, http.MethodPost, r.Method)
		require.Contains(t, r.Header.Get("Content-Type"), "multipart/form-data")
		require.Equal(t, "Parquet", r.URL.Query().Get("input_file_format"))
		require.Equal(t, "auto", r.URL.Query().Get("input_file_structure"))
		require.Equal(t, "10737418240", r.URL.Query().Get("http_max_multipart_form_data_size"))

		require.NoError(t, r.ParseMultipartForm(1<<20))
		require.Equal(t, []string{sql}, r.MultipartForm.Value["query"])

		files := r.MultipartForm.File["input_file"]
		require.Len(t, files, 1)
		require.Equal(t, "companies.parquet", files[0].Filename)

		file, err := files[0].Open()
		require.NoError(t, err)
		defer file.Close()
		body, err := io.ReadAll(file)
		require.NoError(t, err)
		require.Equal(t, "fake parquet", string(body))

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	require.NoError(t, executeParquetImport(server.URL, sql, parquetPath))
	require.True(t, sawRequest)
}

func TestExecuteParquetImportReportsClickHouseStatusAndBody(t *testing.T) {
	dir := t.TempDir()
	parquetPath := filepath.Join(dir, "companies.parquet")
	require.NoError(t, os.WriteFile(parquetPath, []byte("fake parquet"), 0o600))

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("bad external table"))
	}))
	defer server.Close()

	err := executeParquetImport(server.URL, "SELECT * FROM input_file", parquetPath)
	require.Error(t, err)
	require.Contains(t, err.Error(), "status=400 Bad Request")
	require.Contains(t, err.Error(), "bad external table")
}
