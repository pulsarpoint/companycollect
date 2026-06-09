package coloradoentities

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

const rawRecordsTable = "us_coloradoentities_raw_records"
const companiesTable = "us_coloradoentities_companies"
const sourceExportSchemaVersion = "united_states.coloradoentities.source.v1"
const zeroSourceExportID = "00000000-0000-0000-0000-000000000000"

var rawRecordColumns = []string{
	"country_iso2",
	"source_slug",
	"source_run_id",
	"source_record_id",
	"source_native_id",
	"source_payload_hash",
	"entity_id",
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
	"entity_id",
	"legal_name",
	"legal_name_normalized",
	"legal_name_raw",
	"entity_status_raw",
	"entity_status_normalized",
	"is_active",
	"entity_type_code",
	"jurisdiction_of_formation",
	"is_foreign",
	"formation_date",
	"formation_date_raw",
	"principal_city",
	"principal_state",
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

	writer, err := chwriter.Open(ctx, opts.ClickHouseNativeURL)
	if err != nil {
		return companysources.ImportResult{}, err
	}
	defer writer.Close()

	sourceExportID := uuid.New()
	snapshotPath := filepath.Join(opts.RunDir, "source.ndjson")
	rawRows := make([]map[string]any, 0, batchSize)
	companyRows := make([]map[string]any, 0, batchSize)
	var seen int64
	var lineNumber int64

	err = ParseSnapshot(ctx, snapshotPath, func(record ColoradoEntityRecord) error {
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

func rawRecordRow(runID string, snapshotPath string, snapshotSHA256 string, lineNumber int64, record ColoradoEntityRecord) map[string]any {
	now := clickHouseTimestamp(time.Now().UTC())
	entityID := strings.TrimSpace(record.EntityID)
	return map[string]any{
		"country_iso2":         countryISO2,
		"source_slug":          SourceSlug,
		"source_run_id":        runID,
		"source_record_id":     entityID,
		"source_native_id":     entityID,
		"source_payload_hash":  record.PayloadHash,
		"entity_id":            entityID,
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

func companyRow(runID string, record ColoradoEntityRecord) map[string]any {
	now := clickHouseTimestamp(time.Now().UTC())
	entityID := strings.TrimSpace(record.EntityID)
	legalName := cleanEntityName(record.Name)
	statusNormalized, isActive := normalizeStatus(record.EntityStatus)
	formationDate := normalizeDate(record.EntityFormDate)
	return map[string]any{
		"country_iso2":              countryISO2,
		"source_slug":               SourceSlug,
		"source_run_id":             runID,
		"source_record_id":          entityID,
		"source_native_id":          entityID,
		"source_payload_hash":       record.PayloadHash,
		"exported_at":               now,
		"schema_version":            sourceExportSchemaVersion,
		"entity_id":                 entityID,
		"legal_name":                legalName,
		"legal_name_normalized":     normalizeText(legalName),
		"legal_name_raw":            strings.TrimSpace(record.Name),
		"entity_status_raw":         strings.TrimSpace(record.EntityStatus),
		"entity_status_normalized":  statusNormalized,
		"is_active":                 isActive,
		"entity_type_code":          strings.TrimSpace(record.EntityType),
		"jurisdiction_of_formation": strings.TrimSpace(record.JurisdictionOfFormation),
		"is_foreign":                isForeignEntity(record.JurisdictionOfFormation),
		"formation_date":            formationDate,
		"formation_date_raw":        strings.TrimSpace(record.EntityFormDate),
		"principal_city":            strings.TrimSpace(record.PrincipalCity),
		"principal_state":           strings.TrimSpace(record.PrincipalState),
		"ingested_at":               now,
		"source_export_id":          zeroSourceExportID,
	}
}

func cleanEntityName(value string) string {
	return strings.TrimSpace(value)
}

func normalizeStatus(value string) (string, bool) {
	normalized := strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(value)), " "))
	switch normalized {
	case "good standing", "exists", "active":
		return "active", true
	case "":
		return "", false
	default:
		return normalized, false
	}
}

func isForeignEntity(jurisdiction string) bool {
	jurisdiction = strings.TrimSpace(strings.ToLower(jurisdiction))
	return jurisdiction != "" && jurisdiction != "colorado" && jurisdiction != "co"
}

func normalizeDate(value string) string {
	value = strings.TrimSpace(value)
	if len(value) >= len("2006-01-02") {
		return value[:len("2006-01-02")]
	}
	return value
}

func normalizeText(value string) string {
	return strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(value)), " "))
}

func clickHouseTimestamp(value time.Time) string {
	return value.UTC().Format("2006-01-02 15:04:05.000")
}
