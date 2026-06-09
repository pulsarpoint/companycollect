package secedgar

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
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
			errors.New("nil SEC EDGAR source"),
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
	runID := filepath.Base(runDir)
	snapshotPath := filepath.Join(runDir, "source.json")

	rows, recordsSeen, recordsExported, decodeErrors, err := readExportRows(ctx, snapshotPath, runID, opts.Limit)
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
		return result, secContextError(err, snapshotPath)
	}

	files, err := writeSourceExportFiles(ctx, runDir, rows)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return result, secContextError(ctxErr, runDir)
		}
		return result, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO,
			SourceSlug,
			"",
			runDir,
			0,
			errors.Wrap(err, "write SEC EDGAR source export files"),
		)
	}

	snapshotSHA, _, err := countryimport.HashFileSHA256(snapshotPath)
	if err != nil {
		return result, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO,
			SourceSlug,
			"",
			snapshotPath,
			0,
			errors.Wrap(err, "hash SEC EDGAR snapshot"),
		)
	}

	sourceSlug := SourceSlug
	manifestPath := filepath.Join(runDir, "manifest.json")
	manifest := countryimport.ExportManifest{
		ManifestVersion: countryimport.ExportManifestVersion,
		CountryISO2:     "US",
		SourceSlug:      &sourceSlug,
		ExportKind:      "source",
		RunID:           runID,
		SchemaVersion:   SourceExportSchemaVersion,
		CreatedAt:       time.Now().UTC(),
		Inputs: []countryimport.ExportInput{{
			Path:   filepath.Base(snapshotPath),
			SHA256: snapshotSHA,
		}},
		Files:           files,
		RecordsSeen:     recordsSeen,
		RecordsExported: recordsExported,
		DecodeErrors:    decodeErrors,
	}
	if err := ctx.Err(); err != nil {
		return result, secContextError(err, manifestPath)
	}
	if err := countryimport.SaveExportManifest(manifestPath, manifest); err != nil {
		return result, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO,
			SourceSlug,
			"",
			manifestPath,
			0,
			errors.Wrap(err, "save SEC EDGAR source export manifest"),
		)
	}

	result.ManifestPath = manifestPath
	for _, file := range files {
		result.ParquetFiles = append(result.ParquetFiles, filepath.Join(runDir, file.Path))
	}
	return result, nil
}

func readExportRows(ctx context.Context, snapshotPath string, runID string, limit int64) (ExportRows, int64, int64, int64, error) {
	var rows ExportRows
	if err := ctx.Err(); err != nil {
		return rows, 0, 0, 0, secContextError(err, snapshotPath)
	}

	payload, err := os.ReadFile(snapshotPath)
	if err != nil {
		if os.IsNotExist(err) {
			return rows, 0, 0, 0, countryimport.WrapSourceError(
				countryimport.ErrorKindNoSnapshot,
				SourceSlug,
				"",
				snapshotPath,
				0,
				errors.Wrap(err, "read SEC EDGAR snapshot"),
			)
		}
		return rows, 0, 0, 0, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO,
			SourceSlug,
			"",
			snapshotPath,
			0,
			errors.Wrap(err, "read SEC EDGAR snapshot"),
		)
	}

	records, err := DecodeCompanyTickers(payload)
	if err != nil {
		return rows, 0, 0, 1, countryimport.WrapSourceError(
			countryimport.ErrorKindRemoteDecode,
			SourceSlug,
			"",
			snapshotPath,
			0,
			errors.Wrap(err, "decode SEC EDGAR snapshot"),
		)
	}

	recordsSeen := int64(len(records))
	if limit > 0 && int64(len(records)) > limit {
		records = records[:limit]
	}
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return rows, recordsSeen, int64(len(rows.Companies)), 0, secContextError(err, snapshotPath)
		}
		appendExportRows(&rows, ProjectExportRows(record, runID))
	}

	return rows, recordsSeen, int64(len(rows.Companies)), 0, nil
}

func writeSourceExportFiles(ctx context.Context, exportDir string, rows ExportRows) ([]countryimport.ExportFile, error) {
	files := make([]countryimport.ExportFile, 0, 4)
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
	if err := addExportFile(&files, exportDir, "identifiers", rows.Identifiers); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := addExportFile(&files, exportDir, "source_evidence", rows.SourceEvidence); err != nil {
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

func payloadHash(values ...string) string {
	hasher := sha256.New()
	for _, value := range values {
		hasher.Write([]byte(value))
		hasher.Write([]byte{0})
	}
	return hex.EncodeToString(hasher.Sum(nil))
}
