package main

import (
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
)

type TableConfig struct {
	Parquet       string            `yaml:"parquet"`
	Table         string            `yaml:"table"`
	InjectColumns map[string]string `yaml:"inject_columns"`
}

func buildInsertSQL(database string, table TableConfig, sourceExportID string) (string, error) {
	var injected []string
	for name := range table.InjectColumns {
		switch name {
		case "source_export_id", "ingested_at":
			injected = append(injected, name)
		default:
			return "", errors.Errorf("table %s unknown injected column %s", table.Table, name)
		}
	}
	sort.Strings(injected)

	selectParts := []string{"*"}
	for _, name := range injected {
		switch name {
		case "ingested_at":
			selectParts = append(selectParts, "now64(3) AS "+quoteIdent(name))
		case "source_export_id":
			selectParts = append(selectParts, "toUUID('"+escapeSQL(sourceExportID)+"') AS "+quoteIdent(name))
		}
	}

	return "INSERT INTO " + quoteIdent(database) + "." + quoteIdent(table.Table) + "\n" +
		"SELECT " + strings.Join(selectParts, ", ") + "\n" +
		"FROM input_file;\n", nil
}

func executeParquetImport(clickhouseURL string, sql string, parquetPath string) error {
	requestURL, err := externalTableURL(clickhouseURL)
	if err != nil {
		return err
	}

	bodyReader, bodyWriter := io.Pipe()
	form := multipart.NewWriter(bodyWriter)
	copyDone := make(chan error, 1)
	go func() {
		copyDone <- writeMultipartImportBody(form, bodyWriter, sql, parquetPath)
	}()

	req, err := http.NewRequest(http.MethodPost, requestURL, bodyReader)
	if err != nil {
		_ = bodyReader.Close()
		return errors.Wrap(err, "create clickhouse import request")
	}
	req.Header.Set("Content-Type", form.FormDataContentType())

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		_ = bodyReader.Close()
		<-copyDone
		return errors.Wrap(err, "execute clickhouse parquet import")
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		<-copyDone
		return errors.Errorf("clickhouse parquet import failed status=%s body=%s", resp.Status, strings.TrimSpace(string(body)))
	}
	if copyErr := <-copyDone; copyErr != nil {
		return copyErr
	}
	return nil
}

func fetchExternalTableStructure(clickhouseURL string, database string, table string, injectedColumns map[string]string) (string, error) {
	var excluded []string
	for name := range injectedColumns {
		excluded = append(excluded, name)
	}
	sort.Strings(excluded)

	var where []string
	where = append(where,
		"database = '"+escapeSQL(database)+"'",
		"`table` = '"+escapeSQL(table)+"'",
	)
	if len(excluded) > 0 {
		var quoted []string
		for _, name := range excluded {
			quoted = append(quoted, "'"+escapeSQL(name)+"'")
		}
		where = append(where, "name NOT IN ("+strings.Join(quoted, ", ")+")")
	}

	sql := "SELECT name, type\n" +
		"FROM system.columns\n" +
		"WHERE " + strings.Join(where, " AND ") + "\n" +
		"ORDER BY position\n" +
		"FORMAT TSV"

	body, err := executeTextQuery(clickhouseURL, sql)
	if err != nil {
		return "", errors.Wrapf(err, "describe clickhouse table %s.%s", database, table)
	}

	var columns []string
	for _, line := range strings.Split(strings.TrimSpace(body), "\n") {
		if line == "" {
			continue
		}
		fields := strings.SplitN(line, "\t", 2)
		if len(fields) != 2 {
			return "", errors.Errorf("unexpected system.columns row for %s.%s: %q", database, table, line)
		}
		columns = append(columns, fields[0]+" "+fields[1])
	}
	if len(columns) == 0 {
		return "", errors.Errorf("table %s.%s has no parquet columns", database, table)
	}
	return strings.Join(columns, ", "), nil
}

func executeTextQuery(clickhouseURL string, sql string) (string, error) {
	req, err := http.NewRequest(http.MethodPost, clickhouseURL, strings.NewReader(sql))
	if err != nil {
		return "", errors.Wrap(err, "create clickhouse query request")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", errors.Wrap(err, "execute clickhouse query")
	}
	defer resp.Body.Close()

	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 300 {
		return "", errors.Errorf("clickhouse query failed status=%s body=%s", resp.Status, strings.TrimSpace(string(body)))
	}
	if readErr != nil {
		return "", errors.Wrap(readErr, "read clickhouse query response")
	}
	return string(body), nil
}

func writeMultipartImportBody(form *multipart.Writer, bodyWriter *io.PipeWriter, sql string, parquetPath string) (result error) {
	defer func() {
		closeErr := form.Close()
		if result == nil {
			result = closeErr
		}
		if result != nil {
			_ = bodyWriter.CloseWithError(result)
			return
		}
		_ = bodyWriter.Close()
	}()

	if err := form.WriteField("query", sql); err != nil {
		result = errors.Wrap(err, "write clickhouse query form field")
		return result
	}

	file, err := os.Open(parquetPath)
	if err != nil {
		result = errors.Wrapf(err, "open parquet file %s", parquetPath)
		return result
	}
	defer file.Close()

	part, err := form.CreateFormFile("input_file", filepath.Base(parquetPath))
	if err != nil {
		result = errors.Wrap(err, "create clickhouse external table form file")
		return result
	}
	if _, err := io.Copy(part, file); err != nil {
		result = errors.Wrapf(err, "stream parquet file %s", parquetPath)
		return result
	}
	return nil
}

func externalTableURL(clickhouseURL string) (string, error) {
	parsed, err := url.Parse(clickhouseURL)
	if err != nil {
		return "", errors.Wrap(err, "parse clickhouse url")
	}
	query := parsed.Query()
	query.Set("input_file_format", "Parquet")
	if query.Get("http_max_multipart_form_data_size") == "" {
		query.Set("http_max_multipart_form_data_size", "10737418240")
	}
	if query.Get("input_file_structure") == "" {
		query.Set("input_file_structure", "auto")
	}
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func withExternalTableStructure(clickhouseURL string, structure string) (string, error) {
	parsed, err := url.Parse(clickhouseURL)
	if err != nil {
		return "", errors.Wrap(err, "parse clickhouse url")
	}
	query := parsed.Query()
	query.Set("input_file_structure", structure)
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func quoteIdent(value string) string {
	return "`" + strings.ReplaceAll(value, "`", "``") + "`"
}

func escapeSQL(value string) string {
	return strings.ReplaceAll(value, "'", "\\'")
}
