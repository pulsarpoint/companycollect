package prhytj

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"

	chwriter "github.com/pulsarpoint/corpscout/scheduler/internal/clickhouse"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
)

const codeListsTable = "fi_prhytj_code_lists"

var codeListColumns = []string{
	"country_iso2",
	"source_slug",
	"source_run_id",
	"file_run_id",
	"file_key",
	"code_list",
	"language_code",
	"code",
	"description",
	"source_line_number",
	"source_payload_hash",
	"ingested_at",
	"source_export_id",
}

var codeListColumnTypes = map[string]string{
	"country_iso2":        "Nullable(String)",
	"source_slug":         "Nullable(String)",
	"source_run_id":       "Nullable(String)",
	"file_run_id":         "Nullable(String)",
	"file_key":            "Nullable(String)",
	"code_list":           "Nullable(String)",
	"language_code":       "Nullable(String)",
	"code":                "Nullable(String)",
	"description":         "Nullable(String)",
	"source_line_number":  "Nullable(Int64)",
	"source_payload_hash": "Nullable(String)",
	"ingested_at":         "DateTime64(3, 'UTC')",
	"source_export_id":    "UUID",
}

type CodeListRow struct {
	CountryISO2       string
	SourceSlug        string
	SourceRunID       string
	FileRunID         string
	FileKey           string
	CodeList          string
	LanguageCode      string
	Code              string
	Description       string
	SourceLineNumber  int64
	SourcePayloadHash string
	IngestedAt        time.Time
	SourceExportID    uuid.UUID
}

func (r CodeListRow) ClickHouseRow() map[string]any {
	return map[string]any{
		"country_iso2":        r.CountryISO2,
		"source_slug":         r.SourceSlug,
		"source_run_id":       r.SourceRunID,
		"file_run_id":         r.FileRunID,
		"file_key":            r.FileKey,
		"code_list":           r.CodeList,
		"language_code":       r.LanguageCode,
		"code":                r.Code,
		"description":         r.Description,
		"source_line_number":  r.SourceLineNumber,
		"source_payload_hash": r.SourcePayloadHash,
		"ingested_at":         r.IngestedAt.UTC(),
		"source_export_id":    r.SourceExportID,
	}
}

func CodeListTables() []NormalizedTable {
	return []NormalizedTable{
		{Name: codeListsTable, Columns: codeListColumns, ColumnTypes: codeListColumnTypes},
	}
}

func CodeListTableNames() []string {
	return []string{codeListsTable}
}

func importCodeLists(ctx context.Context, writer *chwriter.Writer, files []companysources.SelectedSourceFile, truncate bool, batchSize int) (int64, error) {
	if len(files) == 0 {
		return 0, nil
	}
	if batchSize <= 0 {
		batchSize = 1000
	}
	if truncate {
		if err := writer.TruncateTables(ctx, CodeListTableNames()); err != nil {
			return 0, err
		}
	}

	run := codeListRunContext(files[0])
	rows := make([]map[string]any, 0, batchSize)
	var imported int64
	flush := func() error {
		if len(rows) == 0 {
			return nil
		}
		if err := writer.Insert(ctx, chwriter.Insert{
			Table:   codeListsTable,
			Columns: codeListColumns,
			Rows:    rows,
		}); err != nil {
			return err
		}
		rows = rows[:0]
		return nil
	}

	for _, file := range files {
		meta := codeListFileMetadata(file)
		if err := parseCodeListFile(ctx, file.Path, run, meta, func(row CodeListRow) error {
			rows = append(rows, row.ClickHouseRow())
			imported++
			if len(rows) < batchSize {
				return nil
			}
			return flush()
		}); err != nil {
			return imported, err
		}
	}
	if err := flush(); err != nil {
		return imported, err
	}
	return imported, nil
}

type codeListRun struct {
	SourceRunID    string
	IngestedAt     time.Time
	SourceExportID uuid.UUID
}

type codeListMetadata struct {
	FileRunID    string
	FileKey      string
	CodeList     string
	LanguageCode string
}

func codeListRunContext(file companysources.SelectedSourceFile) codeListRun {
	sourceRunID := strings.TrimSpace(file.FileRunID)
	if sourceRunID == "" && file.Path != "" {
		sourceRunID = filepath.Base(filepath.Dir(file.Path))
	}
	return codeListRun{
		SourceRunID:    sourceRunID,
		IngestedAt:     time.Now().UTC(),
		SourceExportID: uuid.New(),
	}
}

func codeListFileMetadata(file companysources.SelectedSourceFile) codeListMetadata {
	codeList, _ := file.Config["code"].(string)
	languageCode, _ := file.Config["lang"].(string)
	if codeList == "" || languageCode == "" {
		parsedCode, parsedLang := parseCodeListFileKey(file.FileKey)
		if codeList == "" {
			codeList = parsedCode
		}
		if languageCode == "" {
			languageCode = parsedLang
		}
	}
	if languageCode == "" {
		languageCode = "en"
	}
	return codeListMetadata{
		FileRunID:    file.FileRunID,
		FileKey:      file.FileKey,
		CodeList:     codeList,
		LanguageCode: languageCode,
	}
}

func parseCodeListFileKey(fileKey string) (string, string) {
	trimmed := strings.TrimPrefix(fileKey, "codelist_")
	parts := strings.Split(trimmed, "_")
	if len(parts) < 2 {
		return trimmed, ""
	}
	return strings.Join(parts[:len(parts)-1], "_"), parts[len(parts)-1]
}

func parseCodeListFile(ctx context.Context, path string, run codeListRun, meta codeListMetadata, emit func(CodeListRow) error) error {
	file, err := os.Open(path)
	if err != nil {
		return errors.Wrap(err, "open PRH YTJ code list")
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineNumber := int64(0)
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		lineNumber++
		line := strings.TrimRight(scanner.Text(), "\r")
		if strings.TrimSpace(line) == "" {
			continue
		}
		parts := strings.SplitN(line, "\t", 2)
		if len(parts) != 2 {
			return errors.Errorf("malformed PRH YTJ code list row %d in %s", lineNumber, path)
		}
		if err := emit(CodeListRow{
			CountryISO2:       "FI",
			SourceSlug:        SourceKey,
			SourceRunID:       run.SourceRunID,
			FileRunID:         meta.FileRunID,
			FileKey:           meta.FileKey,
			CodeList:          meta.CodeList,
			LanguageCode:      meta.LanguageCode,
			Code:              strings.TrimSpace(parts[0]),
			Description:       strings.TrimSpace(parts[1]),
			SourceLineNumber:  lineNumber,
			SourcePayloadHash: sourceItemHash(meta.FileKey, line),
			IngestedAt:        run.IngestedAt,
			SourceExportID:    run.SourceExportID,
		}); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return errors.Wrap(err, "read PRH YTJ code list")
	}
	return nil
}
