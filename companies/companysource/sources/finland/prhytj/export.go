package prhytj

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
	"github.com/parquet-go/parquet-go"
	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
	sourcespec "github.com/pulsarpoint/companycollect/companies/companysource/internal/source"
)

func (s *Source) ExportParquet(ctx context.Context, opts sourcespec.ExportParquetOptions) (sourcespec.ExportParquetResult, error) {
	if s == nil {
		return sourcespec.ExportParquetResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindState,
			SourceSlug,
			"",
			"",
			0,
			errors.New("nil PRH YTJ source"),
		)
	}

	runDir := strings.TrimSpace(opts.RunDir)
	if runDir == "" {
		return sourcespec.ExportParquetResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindInvalidConfig,
			SourceSlug,
			"",
			"",
			0,
			errors.New("run dir is required"),
		)
	}
	sourcePath := filepath.Join(runDir, "source.ndjson")
	sourceSHA, _, err := countryimport.HashFileSHA256(sourcePath)
	if err != nil {
		return sourcespec.ExportParquetResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO,
			SourceSlug,
			"",
			sourcePath,
			0,
			errors.Wrap(err, "hash PRH source file"),
		)
	}

	runID := filepath.Base(runDir)
	exportedAt := time.Now().UTC().Format(time.RFC3339)
	rows, recordsSeen, recordsExported, decodeErrors, err := readExportRows(ctx, sourcePath, runID, exportedAt, opts.Limit)
	result := sourcespec.ExportParquetResult{
		RunDir:          runDir,
		RecordsSeen:     recordsSeen,
		RecordsExported: recordsExported,
		DecodeErrors:    decodeErrors,
	}
	if err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, processContextError(err, sourcePath)
	}

	files, err := writeSourceExportFiles(ctx, runDir, rows, sourcePath, sourceSHA, runID, exportedAt, opts.Limit)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return result, processContextError(ctxErr, runDir)
		}
		return result, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO,
			SourceSlug,
			"",
			runDir,
			0,
			errors.Wrap(err, "write PRH source parquet files"),
		)
	}

	sourceSlug := SourceSlug
	manifestPath := filepath.Join(runDir, "manifest.json")
	manifest := countryimport.ExportManifest{
		ManifestVersion: countryimport.ExportManifestVersion,
		CountryISO2:     "FI",
		SourceSlug:      &sourceSlug,
		ExportKind:      "source",
		RunID:           runID,
		SchemaVersion:   SourceExportSchemaVersion,
		CreatedAt:       time.Now().UTC(),
		Inputs: []countryimport.ExportInput{{
			Path:   filepath.Base(sourcePath),
			SHA256: sourceSHA,
		}},
		Files:           files,
		RecordsSeen:     recordsSeen,
		RecordsExported: recordsExported,
		DecodeErrors:    decodeErrors,
	}
	if err := countryimport.SaveExportManifest(manifestPath, manifest); err != nil {
		return result, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO,
			SourceSlug,
			"",
			manifestPath,
			0,
			errors.Wrap(err, "save PRH source manifest"),
		)
	}

	result.ManifestPath = manifestPath
	for _, file := range files {
		result.ParquetFiles = append(result.ParquetFiles, filepath.Join(runDir, file.Path))
	}
	return result, nil
}

func readExportRows(ctx context.Context, snapshotPath string, runID string, exportedAt string, limit int64) (ExportRows, int64, int64, int64, error) {
	var rows ExportRows
	file, err := os.Open(snapshotPath)
	if err != nil {
		if os.IsNotExist(err) {
			return rows, 0, 0, 0, countryimport.WrapSourceError(
				countryimport.ErrorKindNoSnapshot,
				SourceSlug,
				"",
				snapshotPath,
				0,
				errors.Wrap(err, "open PRH snapshot"),
			)
		}
		return rows, 0, 0, 0, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO,
			SourceSlug,
			"",
			snapshotPath,
			0,
			errors.Wrap(err, "open PRH snapshot"),
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
		lineNumber++
		recordsSeen++

		rawLine := append([]byte(nil), scanner.Bytes()...)
		var record CompanyRecord
		if err := json.Unmarshal(rawLine, &record); err != nil {
			decodeErrors++
			slog.WarnContext(ctx, "decode PRH YTJ export line",
				"source", SourceSlug,
				"line", lineNumber,
				"error", err,
			)
			continue
		}

		record.RawPayload = rawLine
		payloadHash := sha256.Sum256(rawLine)
		record.PayloadHash = hex.EncodeToString(payloadHash[:])

		projected := ProjectExportRowsAt(record, runID, exportedAt)
		appendExportRows(&rows, projected)
		recordsExported++
		if limit > 0 && recordsExported >= limit {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return rows, recordsSeen, recordsExported, decodeErrors, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO,
			SourceSlug,
			"",
			snapshotPath,
			0,
			errors.Wrap(err, "scan PRH snapshot"),
		)
	}
	if err := ctx.Err(); err != nil {
		return rows, recordsSeen, recordsExported, decodeErrors, processContextError(err, snapshotPath)
	}

	return rows, recordsSeen, recordsExported, decodeErrors, nil
}

func appendExportRows(dst *ExportRows, src ExportRows) {
	dst.RawRecords = append(dst.RawRecords, src.RawRecords...)
	dst.Companies = append(dst.Companies, src.Companies...)
	dst.CompanyNames = append(dst.CompanyNames, src.CompanyNames...)
	dst.LegalForms = append(dst.LegalForms, src.LegalForms...)
	dst.Industries = append(dst.Industries, src.Industries...)
	dst.Addresses = append(dst.Addresses, src.Addresses...)
	dst.RegisteredEntries = append(dst.RegisteredEntries, src.RegisteredEntries...)
	dst.TaxRegistrations = append(dst.TaxRegistrations, src.TaxRegistrations...)
	dst.Websites = append(dst.Websites, src.Websites...)
}

func writeSourceExportFiles(ctx context.Context, exportDir string, rows ExportRows, snapshotPath string, snapshotSHA256 string, runID string, exportedAt string, limit int64) ([]countryimport.ExportFile, error) {
	files := make([]countryimport.ExportFile, 0, 9)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := addRawRecordsExportFile(ctx, &files, exportDir, snapshotPath, snapshotSHA256, runID, exportedAt, limit); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := addExportFile(&files, exportDir, "companies", rows.Companies); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := addExportFile(&files, exportDir, "company_names", rows.CompanyNames); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := addExportFile(&files, exportDir, "legal_forms", rows.LegalForms); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := addExportFile(&files, exportDir, "industries", rows.Industries); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := addExportFile(&files, exportDir, "addresses", rows.Addresses); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := addExportFile(&files, exportDir, "registered_entries", rows.RegisteredEntries); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := addExportFile(&files, exportDir, "tax_registrations", rows.TaxRegistrations); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := addExportFile(&files, exportDir, "websites", rows.Websites); err != nil {
		return nil, err
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

func addRawRecordsExportFile(ctx context.Context, files *[]countryimport.ExportFile, exportDir string, snapshotPath string, snapshotSHA256 string, runID string, exportedAt string, limit int64) error {
	path := filepath.Join(exportDir, "raw_records.parquet")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return errors.Wrap(err, "create raw records parquet directory")
	}
	tempFile, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*.tmp")
	if err != nil {
		return errors.Wrap(err, "create raw records temporary parquet file")
	}
	tempPath := tempFile.Name()
	removeTemp := func() {
		_ = os.Remove(tempPath)
	}

	writer := parquet.NewGenericWriter[RawRecordExportRow](tempFile)
	rowCount, writeErr := writeRawRecordsRows(ctx, writer, snapshotPath, snapshotSHA256, runID, exportedAt, limit)
	closeWriterErr := writer.Close()
	closeFileErr := tempFile.Close()
	if writeErr != nil {
		removeTemp()
		return errors.Wrap(errors.CombineErrors(writeErr, errors.CombineErrors(closeWriterErr, closeFileErr)), "write raw records parquet")
	}
	if closeWriterErr != nil {
		removeTemp()
		return errors.Wrap(errors.CombineErrors(closeWriterErr, closeFileErr), "close raw records parquet writer")
	}
	if closeFileErr != nil {
		removeTemp()
		return errors.Wrap(closeFileErr, "close raw records parquet file")
	}
	if err := os.Rename(tempPath, path); err != nil {
		removeTemp()
		return errors.Wrap(err, "rename raw records parquet file")
	}

	hash, _, err := countryimport.HashFileSHA256(path)
	if err != nil {
		return errors.Wrap(err, "hash raw records parquet")
	}
	*files = append(*files, countryimport.ExportFile{
		Name:       "raw_records",
		Path:       filepath.Base(path),
		RowCount:   rowCount,
		SHA256:     hash,
		SchemaHash: schemaHashForRows[RawRecordExportRow](),
	})
	return nil
}

func writeRawRecordsRows(ctx context.Context, writer *parquet.GenericWriter[RawRecordExportRow], snapshotPath string, snapshotSHA256 string, runID string, exportedAt string, limit int64) (int64, error) {
	file, err := os.Open(snapshotPath)
	if err != nil {
		return 0, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO,
			SourceSlug,
			"",
			snapshotPath,
			0,
			errors.Wrap(err, "open PRH snapshot for raw export"),
		)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), maxSnapshotLineBytes)

	var rowCount int64
	var lineNumber int64
	for {
		if err := ctx.Err(); err != nil {
			return rowCount, processContextError(err, snapshotPath)
		}
		if !scanner.Scan() {
			break
		}
		lineNumber++

		rawLine := append([]byte(nil), scanner.Bytes()...)
		var record CompanyRecord
		if err := json.Unmarshal(rawLine, &record); err != nil {
			continue
		}
		record.RawPayload = rawLine
		payloadHash := sha256.Sum256(rawLine)
		record.PayloadHash = hex.EncodeToString(payloadHash[:])

		row := ProjectRawRecordExportRow(record, runID, snapshotPath, snapshotSHA256, lineNumber, exportedAt)
		written, err := writer.Write([]RawRecordExportRow{row})
		if err != nil {
			return rowCount, errors.Wrap(err, "write raw records parquet row")
		}
		if written != 1 {
			return rowCount, errors.Errorf("write raw records parquet row wrote %d rows", written)
		}
		rowCount++
		if limit > 0 && rowCount >= limit {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return rowCount, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO,
			SourceSlug,
			"",
			snapshotPath,
			0,
			errors.Wrap(err, "scan PRH snapshot for raw export"),
		)
	}
	return rowCount, nil
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
