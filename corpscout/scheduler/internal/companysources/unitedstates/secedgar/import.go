package secedgar

import (
	"context"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/pulsarpoint/corpscout/scheduler/internal/clickhouseclient"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
)

const rawRecordsTable = "us_secedgar_raw_records"
const companiesTable = "us_secedgar_companies"
const sourceExportSchemaVersion = "united_states.secedgar.source.v1"
const zeroSourceExportID = "00000000-0000-0000-0000-000000000000"

var rawRecordColumns = []string{
	"country_iso2",
	"source_slug",
	"source_run_id",
	"source_record_id",
	"source_native_id",
	"source_payload_hash",
	"cik10",
	"snapshot_path",
	"snapshot_sha256",
	"snapshot_line_number",
	"raw_payload_json",
	"schema_version",
	"exported_at",
	"ingested_at",
	"source_export_id",
}

var companyColumns = []string{
	"country_iso2",
	"source_slug",
	"source_run_id",
	"source_record_id",
	"source_native_id",
	"source_payload_hash",
	"exported_at",
	"schema_version",
	"cik",
	"cik10",
	"ticker",
	"legal_name",
	"legal_name_normalized",
	"ingested_at",
	"source_export_id",
}

type Source struct{}

func (Source) Key() companysources.Key {
	return companysources.Key{Country: "united_states", Source: SourceKey}
}

func (Source) DisplayName() string {
	return SourceName
}

func (Source) Import(ctx context.Context, opts companysources.ImportOptions) (companysources.ImportResult, error) {
	if opts.RunDir == "" {
		return companysources.ImportResult{}, errors.New("run dir is required")
	}
	if opts.ClickHouseNativeURL == "" {
		return companysources.ImportResult{}, errors.New("clickhouse native url is required")
	}
	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	sourceExportID := uuid.NewString()
	snapshotPath := filepath.Join(opts.RunDir, "source.json")
	rawRows := make([]map[string]any, 0, batchSize)
	companyRows := make([]map[string]any, 0, batchSize)
	var seen int64

	err := ParseSnapshot(ctx, snapshotPath, func(record CompanyTickerRecord) error {
		if opts.Limit > 0 && seen >= opts.Limit {
			return nil
		}
		seen++
		ingestedAt := clickHouseTimestamp(time.Now().UTC())
		runID := filepath.Base(opts.RunDir)

		rawRow := rawRecordRow(runID, snapshotPath, "", record)
		rawRow["source_export_id"] = sourceExportID
		rawRow["ingested_at"] = ingestedAt
		companyRow := companyRow(runID, record)
		companyRow["source_export_id"] = sourceExportID
		companyRow["ingested_at"] = ingestedAt
		rawRows = append(rawRows, rawRow)
		companyRows = append(companyRows, companyRow)

		if len(companyRows) < batchSize {
			return nil
		}
		if err := flushRows(ctx, opts, rawRecordsTable, rawRecordColumns, rawRows); err != nil {
			return err
		}
		if err := flushRows(ctx, opts, companiesTable, companyColumns, companyRows); err != nil {
			return err
		}
		rawRows = rawRows[:0]
		companyRows = companyRows[:0]
		return nil
	})
	if err != nil {
		return companysources.ImportResult{}, err
	}
	if len(companyRows) > 0 {
		if err := flushRows(ctx, opts, rawRecordsTable, rawRecordColumns, rawRows); err != nil {
			return companysources.ImportResult{}, err
		}
		if err := flushRows(ctx, opts, companiesTable, companyColumns, companyRows); err != nil {
			return companysources.ImportResult{}, err
		}
	}

	return companysources.ImportResult{
		RunDir:         opts.RunDir,
		ImportedTables: []string{rawRecordsTable, companiesTable},
		ImportedRows:   seen,
	}, nil
}

func flushRows(ctx context.Context, opts companysources.ImportOptions, table string, columns []string, rows []map[string]any) error {
	return clickhouseclient.ExecuteInsert(ctx, opts.ClickHouseNativeURL, opts.ClickHouseImage, clickhouseclient.Insert{
		Database: databaseName,
		Table:    table,
		Columns:  columns,
		Rows:     rows,
	})
}

func rawRecordRow(runID string, snapshotPath string, snapshotSHA256 string, record CompanyTickerRecord) map[string]any {
	now := clickHouseTimestamp(time.Now().UTC())
	sourceRecordID := strconv.Itoa(record.SourceIndex)
	return map[string]any{
		"country_iso2":         countryISO2,
		"source_slug":          SourceSlug,
		"source_run_id":        runID,
		"source_record_id":     sourceRecordID,
		"source_native_id":     record.CIK10,
		"source_payload_hash":  record.PayloadHash,
		"cik10":                record.CIK10,
		"snapshot_path":        snapshotPath,
		"snapshot_sha256":      snapshotSHA256,
		"snapshot_line_number": int64(record.SourceIndex),
		"raw_payload_json":     string(record.RawPayload),
		"schema_version":       sourceExportSchemaVersion,
		"exported_at":          now,
		"ingested_at":          now,
		"source_export_id":     zeroSourceExportID,
	}
}

func companyRow(runID string, record CompanyTickerRecord) map[string]any {
	now := clickHouseTimestamp(time.Now().UTC())
	sourceRecordID := strconv.Itoa(record.SourceIndex)
	return map[string]any{
		"country_iso2":          countryISO2,
		"source_slug":           SourceSlug,
		"source_run_id":         runID,
		"source_record_id":      sourceRecordID,
		"source_native_id":      record.CIK10,
		"source_payload_hash":   record.PayloadHash,
		"exported_at":           now,
		"schema_version":        sourceExportSchemaVersion,
		"cik":                   int64(record.CIK),
		"cik10":                 record.CIK10,
		"ticker":                strings.TrimSpace(record.Ticker),
		"legal_name":            strings.TrimSpace(record.Title),
		"legal_name_normalized": normalizeText(record.Title),
		"ingested_at":           now,
		"source_export_id":      zeroSourceExportID,
	}
}

func normalizeText(value string) string {
	return strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(value)), " "))
}

func clickHouseTimestamp(value time.Time) string {
	return value.UTC().Format("2006-01-02 15:04:05.000")
}
