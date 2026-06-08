package coloradoentities

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

// ExportOptions configures a source parquet export.
type ExportOptions struct {
	DataDir      string
	SnapshotPath string
	RunID        string
	Limit        int64
}

// ExportResult summarizes a source parquet export.
type ExportResult struct {
	SourceSlug      string `json:"source_slug"`
	RunID           string `json:"run_id"`
	ManifestPath    string `json:"manifest_path"`
	RecordsSeen     int64  `json:"records_seen"`
	RecordsExported int64  `json:"records_exported"`
	DecodeErrors    int64  `json:"decode_errors"`
}

// Export reads the NDJSON snapshot, projects records into source export rows,
// writes parquet tables under exports/{run_id}/, and writes manifest.json last.
func (s *Source) Export(ctx context.Context, opts ExportOptions) (ExportResult, error) {
	if s == nil {
		return ExportResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindState, SourceSlug, "", "", 0,
			errors.New("nil Colorado entities source"),
		)
	}

	runID := strings.TrimSpace(opts.RunID)
	if runID == "" {
		runID = time.Now().UTC().Format("20060102T150405Z") + "-coloradoentities"
	}

	dataDir := resolveString(opts.DataDir, s.cfg.DataDir, defaultDataDir)
	snapshotPath := strings.TrimSpace(opts.SnapshotPath)
	if snapshotPath == "" {
		var err error
		snapshotPath, err = latestSnapshotPath(filepath.Join(dataDir, "snapshots"))
		if err != nil {
			return ExportResult{}, err
		}
	}

	rows, recordsSeen, recordsExported, decodeErrors, err := readExportRows(ctx, snapshotPath, runID, opts.Limit)
	result := ExportResult{
		SourceSlug:      SourceSlug,
		RunID:           runID,
		RecordsSeen:     recordsSeen,
		RecordsExported: recordsExported,
		DecodeErrors:    decodeErrors,
	}
	if err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, processContextError(err, snapshotPath)
	}

	exportDir := filepath.Join(dataDir, "exports", runID)
	files, err := writeSourceExportFiles(ctx, exportDir, rows)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return result, processContextError(ctxErr, exportDir)
		}
		return result, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO, SourceSlug, "", exportDir, 0,
			errors.Wrap(err, "write Colorado source export files"),
		)
	}

	snapshotSHA, _, err := countryimport.HashFileSHA256(snapshotPath)
	if err != nil {
		return result, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO, SourceSlug, "", snapshotPath, 0,
			errors.Wrap(err, "hash Colorado snapshot"),
		)
	}

	sourceSlug := SourceKey
	manifestPath := filepath.Join(exportDir, "manifest.json")
	manifest := countryimport.ExportManifest{
		ManifestVersion: countryimport.ExportManifestVersion,
		CountryISO2:     "US",
		SourceSlug:      &sourceSlug,
		ExportKind:      "source",
		RunID:           runID,
		SchemaVersion:   SourceExportSchemaVersion,
		CreatedAt:       time.Now().UTC(),
		Inputs: []countryimport.ExportInput{{
			Path:   snapshotPath,
			SHA256: snapshotSHA,
		}},
		Files:           files,
		RecordsSeen:     recordsSeen,
		RecordsExported: recordsExported,
		DecodeErrors:    decodeErrors,
	}
	if err := ctx.Err(); err != nil {
		return result, processContextError(err, manifestPath)
	}
	if err := countryimport.SaveExportManifest(manifestPath, manifest); err != nil {
		return result, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO, SourceSlug, "", manifestPath, 0,
			errors.Wrap(err, "save Colorado source export manifest"),
		)
	}

	result.ManifestPath = manifestPath
	return result, nil
}

func readExportRows(ctx context.Context, snapshotPath string, runID string, limit int64) (ExportRows, int64, int64, int64, error) {
	var rows ExportRows
	file, err := os.Open(snapshotPath)
	if err != nil {
		if os.IsNotExist(err) {
			return rows, 0, 0, 0, countryimport.WrapSourceError(
				countryimport.ErrorKindNoSnapshot, SourceSlug, "", snapshotPath, 0,
				errors.Wrap(err, "open Colorado snapshot"),
			)
		}
		return rows, 0, 0, 0, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO, SourceSlug, "", snapshotPath, 0,
			errors.Wrap(err, "open Colorado snapshot"),
		)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), maxSnapshotLineBytes)

	var recordsSeen int64
	var recordsExported int64
	var decodeErrors int64
	var lineNumber int64
	for {
		if err := ctx.Err(); err != nil {
			return rows, recordsSeen, recordsExported, decodeErrors, processContextError(err, snapshotPath)
		}
		if !scanner.Scan() {
			break
		}
		text := strings.TrimSpace(scanner.Text())
		if text == "" {
			continue
		}
		lineNumber++
		recordsSeen++

		rawLine := append([]byte(nil), scanner.Bytes()...)
		var record ColoradoEntityRecord
		if err := json.Unmarshal([]byte(text), &record); err != nil {
			decodeErrors++
			slog.WarnContext(ctx, "decode Colorado export line",
				"source", SourceSlug, "line", lineNumber, "error", err)
			continue
		}
		record.RawPayload = rawLine
		payloadHash := sha256.Sum256(rawLine)
		record.PayloadHash = hex.EncodeToString(payloadHash[:])

		appendExportRows(&rows, ProjectExportRows(record, runID))
		recordsExported++
		if limit > 0 && recordsExported >= limit {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return rows, recordsSeen, recordsExported, decodeErrors, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO, SourceSlug, "", snapshotPath, 0,
			errors.Wrap(err, "scan Colorado snapshot"),
		)
	}
	if err := ctx.Err(); err != nil {
		return rows, recordsSeen, recordsExported, decodeErrors, processContextError(err, snapshotPath)
	}

	return rows, recordsSeen, recordsExported, decodeErrors, nil
}

func writeSourceExportFiles(ctx context.Context, exportDir string, rows ExportRows) ([]countryimport.ExportFile, error) {
	files := make([]countryimport.ExportFile, 0, 7)
	steps := []struct {
		name string
		fn   func() error
	}{
		{"companies", func() error { return addExportFile(&files, exportDir, "companies", rows.Companies) }},
		{"company_names", func() error { return addExportFile(&files, exportDir, "company_names", rows.CompanyNames) }},
		{"legal_forms", func() error { return addExportFile(&files, exportDir, "legal_forms", rows.LegalForms) }},
		{"addresses", func() error { return addExportFile(&files, exportDir, "addresses", rows.Addresses) }},
		{"registered_agents", func() error { return addExportFile(&files, exportDir, "registered_agents", rows.RegisteredAgents) }},
		{"identifiers", func() error { return addExportFile(&files, exportDir, "identifiers", rows.Identifiers) }},
		{"source_evidence", func() error { return addExportFile(&files, exportDir, "source_evidence", rows.SourceEvidence) }},
	}
	for _, step := range steps {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := step.fn(); err != nil {
			return nil, err
		}
	}
	return files, nil
}

func addExportFile[T any](files *[]countryimport.ExportFile, exportDir string, name string, rows []T) error {
	path := filepath.Join(exportDir, name+".parquet")
	if err := WriteParquetRows(path, rows); err != nil {
		return errors.Wrapf(err, "write %s parquet", name)
	}
	hash, _, err := countryimport.HashFileSHA256(path)
	if err != nil {
		return errors.Wrapf(err, "hash %s parquet", name)
	}

	*files = append(*files, countryimport.ExportFile{
		Name:       name,
		Path:       filepath.Base(path),
		RowCount:   int64(len(rows)),
		SHA256:     hash,
		SchemaHash: schemaHashForRows[T](),
	})
	return nil
}

func schemaHashForRows[T any]() string {
	rowType := reflect.TypeFor[T]()
	hasher := sha256.New()
	if rowType.Kind() != reflect.Struct {
		hasher.Write([]byte(rowType.String()))
		return hex.EncodeToString(hasher.Sum(nil))
	}

	hasher.Write([]byte("struct"))
	for i := 0; i < rowType.NumField(); i++ {
		field := rowType.Field(i)
		if !field.IsExported() {
			continue
		}
		hasher.Write([]byte{0})
		hasher.Write([]byte(field.Name))
		hasher.Write([]byte{0})
		hasher.Write([]byte(field.Type.String()))
		hasher.Write([]byte{0})
		hasher.Write([]byte(field.Tag.Get("parquet")))
	}
	return hex.EncodeToString(hasher.Sum(nil))
}
