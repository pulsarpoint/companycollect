package irseobmf

import (
	"context"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	chwriter "github.com/pulsarpoint/corpscout/scheduler/internal/clickhouse"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
)

const rawRecordsTable = "us_irseobmf_raw_records"
const companiesTable = "us_irseobmf_companies"
const sourceExportSchemaVersion = "united_states.irseobmf.source.v1"
const zeroSourceExportID = "00000000-0000-0000-0000-000000000000"

var rawRecordColumns = []string{
	"country_iso2",
	"source_slug",
	"source_run_id",
	"source_record_id",
	"source_native_id",
	"source_payload_hash",
	"ein",
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
	"ein",
	"legal_name",
	"legal_name_normalized",
	"sort_name",
	"in_care_of",
	"is_nonprofit",
	"exempt_status_code",
	"is_active_exempt",
	"subsection_code",
	"organization_code",
	"foundation_code",
	"ntee_code",
	"group_exemption_number",
	"ruling_date",
	"mailing_state",
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

	snapshotPath, err := opts.RequireFilePath("source")
	if err != nil {
		return companysources.ImportResult{}, err
	}

	writer, err := chwriter.Open(ctx, opts.ClickHouseNativeURL)
	if err != nil {
		return companysources.ImportResult{}, err
	}
	defer writer.Close()

	sourceExportID := uuid.New()
	rawRows := make([]map[string]any, 0, batchSize)
	companyRows := make([]map[string]any, 0, batchSize)
	var seen int64
	var lineNumber int64

	err = ParseSnapshot(ctx, snapshotPath, func(record IrsEoBmfRecord) error {
		lineNumber++
		if opts.Limit > 0 && seen >= opts.Limit {
			return nil
		}
		seen++
		ingestedAt := clickHouseTimestamp(time.Now().UTC())
		runID := filepath.Base(opts.RunDir)

		rawRow := rawRecordRow(runID, snapshotPath, "", lineNumber, record)
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
		if err := flushRows(ctx, writer, rawRecordsTable, rawRecordColumns, rawRows); err != nil {
			return err
		}
		if err := flushRows(ctx, writer, companiesTable, companyColumns, companyRows); err != nil {
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
		if err := flushRows(ctx, writer, rawRecordsTable, rawRecordColumns, rawRows); err != nil {
			return companysources.ImportResult{}, err
		}
		if err := flushRows(ctx, writer, companiesTable, companyColumns, companyRows); err != nil {
			return companysources.ImportResult{}, err
		}
	}

	return companysources.ImportResult{
		RunDir:         opts.RunDir,
		ImportedTables: []string{rawRecordsTable, companiesTable},
		ImportedRows:   seen,
	}, nil
}

func flushRows(ctx context.Context, writer *chwriter.Writer, table string, columns []string, rows []map[string]any) error {
	return writer.Insert(ctx, chwriter.Insert{
		Table:   table,
		Columns: columns,
		Rows:    rows,
	})
}

func rawRecordRow(runID string, snapshotPath string, snapshotSHA256 string, lineNumber int64, record IrsEoBmfRecord) map[string]any {
	now := clickHouseTimestamp(time.Now().UTC())
	ein := strings.TrimSpace(record.EIN)
	return map[string]any{
		"country_iso2":         countryISO2,
		"source_slug":          SourceSlug,
		"source_run_id":        runID,
		"source_record_id":     ein,
		"source_native_id":     ein,
		"source_payload_hash":  record.PayloadHash,
		"ein":                  ein,
		"snapshot_path":        snapshotPath,
		"snapshot_sha256":      snapshotSHA256,
		"snapshot_line_number": lineNumber,
		"raw_payload_json":     string(record.RawPayload),
		"schema_version":       sourceExportSchemaVersion,
		"exported_at":          now,
		"ingested_at":          now,
		"source_export_id":     zeroSourceExportID,
	}
}

func companyRow(runID string, record IrsEoBmfRecord) map[string]any {
	now := clickHouseTimestamp(time.Now().UTC())
	ein := strings.TrimSpace(record.EIN)
	return map[string]any{
		"country_iso2":           countryISO2,
		"source_slug":            SourceSlug,
		"source_run_id":          runID,
		"source_record_id":       ein,
		"source_native_id":       ein,
		"source_payload_hash":    record.PayloadHash,
		"exported_at":            now,
		"schema_version":         sourceExportSchemaVersion,
		"ein":                    ein,
		"legal_name":             strings.TrimSpace(record.Name),
		"legal_name_normalized":  normalizeText(record.Name),
		"sort_name":              strings.TrimSpace(record.SortName),
		"in_care_of":             strings.TrimSpace(record.InCareOf),
		"is_nonprofit":           true,
		"exempt_status_code":     strings.TrimSpace(record.Status),
		"is_active_exempt":       isActiveExemptStatus(record.Status),
		"subsection_code":        strings.TrimSpace(record.Subsection),
		"organization_code":      strings.TrimSpace(record.Organization),
		"foundation_code":        strings.TrimSpace(record.Foundation),
		"ntee_code":              strings.TrimSpace(record.NTEECD),
		"group_exemption_number": strings.TrimSpace(record.Group),
		"ruling_date":            strings.TrimSpace(record.Ruling),
		"mailing_state":          strings.TrimSpace(record.State),
		"ingested_at":            now,
		"source_export_id":       zeroSourceExportID,
	}
}

func isActiveExemptStatus(status string) bool {
	return strings.TrimSpace(status) == "01"
}

func normalizeText(value string) string {
	return strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(value)), " "))
}

func clickHouseTimestamp(value time.Time) string {
	return value.UTC().Format("2006-01-02 15:04:05.000")
}
