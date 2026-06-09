package prhytj

import (
	"context"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/pulsarpoint/corpscout/scheduler/internal/clickhouseclient"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
)

const database = "corpscout_sources"
const rawRecordsTable = "fi_prhytj_raw_records"
const companiesTable = "fi_prhytj_companies"
const sourceExportSchemaVersion = "v1"
const zeroSourceExportID = "00000000-0000-0000-0000-000000000000"

var rawRecordColumns = []string{
	"country_iso2",
	"source_slug",
	"source_run_id",
	"source_record_id",
	"business_id",
	"source_payload_hash",
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
	"source_updated_at",
	"exported_at",
	"schema_version",
	"business_id",
	"vat_id",
	"euid",
	"legal_name",
	"legal_name_normalized",
	"lifecycle_status",
	"is_active",
	"legal_form_code",
	"legal_form_label",
	"legal_form_label_en",
	"primary_industry_code",
	"primary_industry_code_set",
	"primary_industry_label",
	"primary_industry_label_en",
	"primary_nace_code",
	"primary_nace_revision",
	"website_url",
	"website_normalized_url",
	"website_host",
	"ingested_at",
	"source_export_id",
}

type Source struct{}

func (Source) Key() companysources.Key {
	return companysources.Key{Country: "finland", Source: SourceKey}
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
	snapshotPath := filepath.Join(opts.RunDir, "source.ndjson")
	rawRows := make([]map[string]any, 0, batchSize)
	companyRows := make([]map[string]any, 0, batchSize)
	var seen int64
	var lineNumber int64

	err := ParseSnapshot(ctx, snapshotPath, func(record CompanyRecord) error {
		lineNumber++
		if opts.Limit > 0 && seen >= opts.Limit {
			return nil
		}
		seen++
		ingestedAt := clickHouseTimestamp(time.Now().UTC())

		rawRow := rawRecordRow(filepath.Base(opts.RunDir), snapshotPath, "", lineNumber, record)
		rawRow["source_export_id"] = sourceExportID
		rawRow["ingested_at"] = ingestedAt
		companyRow := companyRow(filepath.Base(opts.RunDir), record)
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
		Database: database,
		Table:    table,
		Columns:  columns,
		Rows:     rows,
	})
}

func rawRecordRow(runID string, snapshotPath string, snapshotSHA256 string, lineNumber int64, record CompanyRecord) map[string]any {
	now := clickHouseTimestamp(time.Now().UTC())
	return map[string]any{
		"country_iso2":         "FI",
		"source_slug":          SourceKey,
		"source_run_id":        runID,
		"source_record_id":     record.BusinessID.Value,
		"business_id":          record.BusinessID.Value,
		"source_payload_hash":  record.PayloadHash,
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

func companyRow(runID string, record CompanyRecord) map[string]any {
	now := clickHouseTimestamp(time.Now().UTC())
	legalName := currentLegalName(record.Names)
	currentForm := currentCompanyForm(record.CompanyForms)
	websiteURL := strings.TrimSpace(record.Website.URL)
	normalizedWebsiteURL := normalizeWebsite(websiteURL)
	return map[string]any{
		"country_iso2":              "FI",
		"source_slug":               SourceKey,
		"source_run_id":             runID,
		"source_record_id":          record.BusinessID.Value,
		"source_native_id":          record.BusinessID.Value,
		"source_payload_hash":       record.PayloadHash,
		"source_updated_at":         record.LastModified,
		"exported_at":               now,
		"schema_version":            sourceExportSchemaVersion,
		"business_id":               record.BusinessID.Value,
		"vat_id":                    vatID(record.BusinessID.Value),
		"euid":                      identifierPointerValue(record.EUID),
		"legal_name":                legalName,
		"legal_name_normalized":     normalizeName(legalName),
		"lifecycle_status":          lifecycleStatus(record),
		"is_active":                 lifecycleStatus(record) == "active",
		"legal_form_code":           currentForm.Type,
		"legal_form_label":          preferredDescription(currentForm.Descriptions),
		"legal_form_label_en":       description(record.CompanyForms, currentForm.Type, "3"),
		"primary_industry_code":     record.MainBusinessLine.Type,
		"primary_industry_code_set": record.MainBusinessLine.TypeCodeSet,
		"primary_industry_label":    preferredDescription(record.MainBusinessLine.Descriptions),
		"primary_industry_label_en": descriptionByLanguage(record.MainBusinessLine.Descriptions, "3"),
		"primary_nace_code":         "",
		"primary_nace_revision":     "",
		"website_url":               websiteURL,
		"website_normalized_url":    normalizedWebsiteURL,
		"website_host":              websiteHost(normalizedWebsiteURL),
		"ingested_at":               now,
		"source_export_id":          zeroSourceExportID,
	}
}

func identifierPointerValue(value *Identifier) string {
	if value == nil {
		return ""
	}
	return value.Value
}

func currentLegalName(names []Name) string {
	var selected *Name
	for i := range names {
		name := &names[i]
		if name.Type != "1" || name.EndDate != "" || name.Name == "" {
			continue
		}
		if selected == nil || name.RegistrationDate > selected.RegistrationDate {
			selected = name
		}
	}
	if selected != nil {
		return selected.Name
	}

	for _, name := range names {
		if name.Type == "1" && name.Name != "" {
			return name.Name
		}
	}
	for _, name := range names {
		if name.Name != "" {
			return name.Name
		}
	}
	return ""
}

func currentCompanyForm(forms []CompanyForm) CompanyForm {
	var selected *CompanyForm
	for i := range forms {
		form := &forms[i]
		if form.EndDate != "" {
			continue
		}
		if selected == nil || form.RegistrationDate > selected.RegistrationDate {
			selected = form
		}
	}
	if selected != nil {
		return *selected
	}

	for i := range forms {
		form := &forms[i]
		if selected == nil || form.RegistrationDate > selected.RegistrationDate {
			selected = form
		}
	}
	if selected != nil {
		return *selected
	}

	return CompanyForm{}
}

func preferredDescription(descriptions []Description) string {
	for _, languageCode := range []string{"1", "3", "2"} {
		if description := descriptionByLanguage(descriptions, languageCode); description != "" {
			return description
		}
	}
	return ""
}

func descriptionByLanguage(descriptions []Description, languageCode string) string {
	for _, description := range descriptions {
		if description.LanguageCode == languageCode {
			return description.Description
		}
	}
	return ""
}

func normalizeWebsite(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	parsed, err := url.Parse(trimmed)
	if err == nil && parsed.Scheme != "" {
		return trimmed
	}
	return "https://" + trimmed
}

func vatID(businessID string) string {
	digits := strings.Builder{}
	for _, character := range businessID {
		if character >= '0' && character <= '9' {
			digits.WriteRune(character)
		}
	}
	if digits.Len() == 0 {
		return ""
	}
	return "FI" + digits.String()
}

func lifecycleStatus(record CompanyRecord) string {
	if record.EndDate != "" || record.TradeRegisterStatus == "3" {
		return "ceased"
	}
	return "active"
}

func description(forms []CompanyForm, formType string, language string) string {
	for _, form := range forms {
		if form.Type == formType {
			return descriptionByLanguage(form.Descriptions, language)
		}
	}
	return ""
}

func websiteHost(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return parsed.Hostname()
}

func normalizeName(value string) string {
	return strings.Join(strings.Fields(strings.ToLower(strings.TrimSpace(value))), " ")
}

func clickHouseTimestamp(value time.Time) string {
	return value.UTC().Format("2006-01-02 15:04:05.000")
}
