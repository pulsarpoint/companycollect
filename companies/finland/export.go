package finland

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/parquet-go/parquet-go"
	"github.com/pulsarpoint/companycollect/companies/finland/prhytj"
	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

type BuildExportOptions struct {
	DataDir             string
	RunID               string
	SourceManifestPaths map[string]string
}

type BuildExportResult struct {
	RunID           string `json:"run_id"`
	ManifestPath    string `json:"manifest_path"`
	RecordsExported int64  `json:"records_exported"`
}

type finalRows struct {
	Companies      []FinalCompanyRow
	CompanyNames   []FinalCompanyNameRow
	Identifiers    []FinalIdentifierRow
	Addresses      []FinalAddressRow
	Industries     []FinalIndustryRow
	Websites       []FinalWebsiteRow
	SourceEvidence []FinalSourceEvidenceRow
}

func BuildFinalExport(ctx context.Context, opts BuildExportOptions) (BuildExportResult, error) {
	if err := ctx.Err(); err != nil {
		return BuildExportResult{}, err
	}

	layout := LayoutForDataDir(opts.DataDir)
	runID := strings.TrimSpace(opts.RunID)
	if runID == "" {
		runID = time.Now().UTC().Format("20060102T150405Z") + "-finland-final"
	}

	prhManifestPath := strings.TrimSpace(opts.SourceManifestPaths[SourcePRHYTJ])
	if prhManifestPath == "" {
		status, err := SourceStatusFromLatestManifest(opts.DataDir, SourcePRHYTJ)
		if err != nil {
			return BuildExportResult{}, errors.Wrap(err, "load latest PRH YTJ source export status")
		}
		prhManifestPath = strings.TrimSpace(status.LastExportManifestPath)
	}
	if prhManifestPath == "" {
		return BuildExportResult{}, errors.New("missing PRH YTJ source export manifest")
	}

	sourceManifest, err := countryimport.LoadExportManifest(prhManifestPath)
	if err != nil {
		return BuildExportResult{}, errors.Wrap(err, "load PRH YTJ source export manifest")
	}
	if err := validatePRHSourceManifest(sourceManifest); err != nil {
		return BuildExportResult{}, errors.Wrap(err, "validate PRH YTJ source export manifest")
	}
	if err := ctx.Err(); err != nil {
		return BuildExportResult{}, err
	}

	companiesFile, err := sourceManifestFile(sourceManifest, "companies")
	if err != nil {
		return BuildExportResult{}, errors.Wrap(err, "find PRH YTJ companies file in source manifest")
	}
	companiesPath, err := resolveManifestFilePath(filepath.Dir(prhManifestPath), companiesFile.Path)
	if err != nil {
		return BuildExportResult{}, errors.Wrap(err, "resolve PRH YTJ companies file path")
	}
	companiesSHA, _, err := countryimport.HashFileSHA256(companiesPath)
	if err != nil {
		return BuildExportResult{}, errors.Wrap(err, "hash PRH YTJ companies parquet")
	}
	if companiesSHA != companiesFile.SHA256 {
		return BuildExportResult{}, errors.Errorf("PRH YTJ companies parquet SHA256 mismatch: manifest has %s, actual is %s", companiesFile.SHA256, companiesSHA)
	}

	companies, err := parquet.ReadFile[prhytj.CompanyExportRow](companiesPath)
	if err != nil {
		return BuildExportResult{}, errors.Wrap(err, "read PRH YTJ companies parquet")
	}
	if err := ctx.Err(); err != nil {
		return BuildExportResult{}, err
	}

	rows := mapPRHCompaniesToFinal(companies)
	exportDir := filepath.Join(layout.FinalExportsDir(), runID)
	files, err := writeFinalCoreFiles(ctx, exportDir, rows)
	if err != nil {
		return BuildExportResult{}, errors.Wrap(err, "write Finland final export files")
	}
	if err := ctx.Err(); err != nil {
		return BuildExportResult{}, err
	}

	manifestPath := filepath.Join(exportDir, "manifest.json")
	manifest := countryimport.ExportManifest{
		ManifestVersion:  countryimport.ExportManifestVersion,
		CountryISO2:      CountryISO2,
		SourceSlug:       nil,
		ExportKind:       "final",
		RunID:            runID,
		SchemaVersion:    FinalSchemaVersionV1,
		MergeRuleVersion: MergeRuleVersionV1,
		CreatedAt:        time.Now().UTC(),
		Files:            files,
		SourceExportsUsed: []countryimport.SourceExportRef{{
			SourceSlug:   SourcePRHYTJ,
			RunID:        sourceManifest.RunID,
			ManifestPath: prhManifestPath,
		}},
		RecordsSeen:     int64(len(companies)),
		RecordsExported: int64(len(rows.Companies)),
	}
	if err := countryimport.SaveExportManifest(manifestPath, manifest); err != nil {
		return BuildExportResult{}, errors.Wrap(err, "save Finland final export manifest")
	}

	return BuildExportResult{
		RunID:           runID,
		ManifestPath:    manifestPath,
		RecordsExported: int64(len(rows.Companies)),
	}, nil
}

func validatePRHSourceManifest(manifest countryimport.ExportManifest) error {
	if manifest.ManifestVersion != countryimport.ExportManifestVersion {
		return errors.Errorf("invalid manifest version %q, want %q", manifest.ManifestVersion, countryimport.ExportManifestVersion)
	}
	if manifest.ExportKind != "source" {
		return errors.Errorf("invalid export kind %q, want source", manifest.ExportKind)
	}
	if manifest.CountryISO2 != CountryISO2 {
		return errors.Errorf("invalid country %q, want %q", manifest.CountryISO2, CountryISO2)
	}
	if manifest.SourceSlug == nil || *manifest.SourceSlug != SourcePRHYTJ {
		return errors.Errorf("invalid source slug %v, want %q", manifest.SourceSlug, SourcePRHYTJ)
	}
	if manifest.SchemaVersion != prhytj.SourceExportSchemaVersion {
		return errors.Errorf("invalid schema version %q, want %q", manifest.SchemaVersion, prhytj.SourceExportSchemaVersion)
	}
	return nil
}

func sourceManifestFile(manifest countryimport.ExportManifest, name string) (countryimport.ExportFile, error) {
	for _, file := range manifest.Files {
		if file.Name == name {
			return file, nil
		}
	}
	return countryimport.ExportFile{}, errors.Errorf("missing %q file", name)
}

func resolveManifestFilePath(manifestDir string, manifestFilePath string) (string, error) {
	if strings.TrimSpace(manifestFilePath) == "" {
		return "", errors.New("manifest file path is empty")
	}
	if filepath.IsAbs(manifestFilePath) {
		return "", errors.Errorf("manifest file path must be relative: %q", manifestFilePath)
	}
	if !filepath.IsLocal(manifestFilePath) {
		return "", errors.Errorf("manifest file path escapes manifest directory: %q", manifestFilePath)
	}
	return filepath.Join(manifestDir, manifestFilePath), nil
}

func mapPRHCompaniesToFinal(companies []prhytj.CompanyExportRow) finalRows {
	rows := finalRows{
		Companies:      make([]FinalCompanyRow, 0, len(companies)),
		CompanyNames:   make([]FinalCompanyNameRow, 0, len(companies)),
		Identifiers:    make([]FinalIdentifierRow, 0, len(companies)*3),
		Industries:     make([]FinalIndustryRow, 0, len(companies)),
		Websites:       make([]FinalWebsiteRow, 0, len(companies)),
		SourceEvidence: make([]FinalSourceEvidenceRow, 0, len(companies)),
	}
	exportedAt := time.Now().UTC().Format(time.RFC3339)
	for _, company := range companies {
		businessID := strings.TrimSpace(company.BusinessID)
		countryCompanyID := "FI:" + businessID
		finalCompany := FinalCompanyRow{
			CountryCompanyID:      countryCompanyID,
			CountryISO2:           CountryISO2,
			PrimarySourceSlug:     SourcePRHYTJ,
			PrimarySourceRecordID: company.SourceRecordID,
			BusinessID:            businessID,
			LegalName:             company.LegalName,
			LegalNameEn:           "",
			LegalNameNormalized:   company.LegalNameNormalized,
			LifecycleStatus:       company.LifecycleStatus,
			IsActive:              company.IsActive,
			VATID:                 company.VATID,
			EUID:                  company.EUID,
			LegalFormCode:         company.LegalFormCode,
			LegalFormLabel:        company.LegalFormLabel,
			LegalFormLabelEn:      company.LegalFormLabelEn,
			PrimaryIndustryCode:   company.PrimaryIndustryCode,
			PrimaryNACECode:       company.PrimaryNACECode,
			PrimaryNACERevision:   company.PrimaryNACERevision,
			WebsiteNormalizedURL:  company.WebsiteNormalizedURL,
			SourcePayloadHash:     company.SourcePayloadHash,
			MergeRuleVersion:      MergeRuleVersionV1,
			IsTranslated:          false,
			ExportedAt:            exportedAt,
		}
		finalCompany.ProfileHash = profileHash(finalCompany)
		rows.Companies = append(rows.Companies, finalCompany)

		if strings.TrimSpace(company.LegalName) != "" {
			rows.CompanyNames = append(rows.CompanyNames, FinalCompanyNameRow{
				CountryCompanyID:      countryCompanyID,
				CountryISO2:           CountryISO2,
				SourceSlug:            SourcePRHYTJ,
				SourceRecordID:        company.SourceRecordID,
				Name:                  company.LegalName,
				NameType:              "legal",
				Language:              "",
				IsPrimary:             true,
				IsTranslated:          false,
				PrimarySourceRecordID: company.SourceRecordID,
			})
		}
		rows.Identifiers = appendIdentifier(rows.Identifiers, countryCompanyID, company.SourceRecordID, "business_id", businessID, true)
		rows.Identifiers = appendIdentifier(rows.Identifiers, countryCompanyID, company.SourceRecordID, "vat_id", company.VATID, false)
		rows.Identifiers = appendIdentifier(rows.Identifiers, countryCompanyID, company.SourceRecordID, "euid", company.EUID, false)
		if strings.TrimSpace(company.PrimaryIndustryCode) != "" || strings.TrimSpace(company.PrimaryNACECode) != "" {
			rows.Industries = append(rows.Industries, FinalIndustryRow{
				CountryCompanyID: countryCompanyID,
				CountryISO2:      CountryISO2,
				SourceSlug:       SourcePRHYTJ,
				SourceRecordID:   company.SourceRecordID,
				IndustryCode:     company.PrimaryIndustryCode,
				NACECode:         company.PrimaryNACECode,
				NACERevision:     company.PrimaryNACERevision,
				IsPrimary:        true,
			})
		}
		if strings.TrimSpace(company.WebsiteNormalizedURL) != "" {
			rows.Websites = append(rows.Websites, FinalWebsiteRow{
				CountryCompanyID: countryCompanyID,
				CountryISO2:      CountryISO2,
				SourceSlug:       SourcePRHYTJ,
				SourceRecordID:   company.SourceRecordID,
				NormalizedURL:    company.WebsiteNormalizedURL,
				IsPrimary:        true,
			})
		}
		rows.SourceEvidence = append(rows.SourceEvidence, FinalSourceEvidenceRow{
			CountryCompanyID:  countryCompanyID,
			CountryISO2:       CountryISO2,
			SourceSlug:        SourcePRHYTJ,
			SourceRecordID:    company.SourceRecordID,
			SourcePayloadHash: company.SourcePayloadHash,
			MergeRuleVersion:  MergeRuleVersionV1,
		})
	}
	return rows
}

func appendIdentifier(rows []FinalIdentifierRow, countryCompanyID string, sourceRecordID string, identifierType string, value string, isPrimary bool) []FinalIdentifierRow {
	value = strings.TrimSpace(value)
	if value == "" {
		return rows
	}
	return append(rows, FinalIdentifierRow{
		CountryCompanyID: countryCompanyID,
		CountryISO2:      CountryISO2,
		SourceSlug:       SourcePRHYTJ,
		SourceRecordID:   sourceRecordID,
		IdentifierType:   identifierType,
		IdentifierValue:  value,
		IsPrimary:        isPrimary,
	})
}

func writeFinalCoreFiles(ctx context.Context, exportDir string, rows finalRows) ([]countryimport.ExportFile, error) {
	files := make([]countryimport.ExportFile, 0, 7)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := addFinalExportFile(&files, exportDir, "companies", rows.Companies); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := addFinalExportFile(&files, exportDir, "company_names", rows.CompanyNames); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := addFinalExportFile(&files, exportDir, "identifiers", rows.Identifiers); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := addFinalExportFile(&files, exportDir, "addresses", rows.Addresses); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := addFinalExportFile(&files, exportDir, "industries", rows.Industries); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := addFinalExportFile(&files, exportDir, "websites", rows.Websites); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := addFinalExportFile(&files, exportDir, "source_evidence", rows.SourceEvidence); err != nil {
		return nil, err
	}
	return files, nil
}

func addFinalExportFile[T any](files *[]countryimport.ExportFile, exportDir string, name string, rows []T) error {
	path := filepath.Join(exportDir, name+".parquet")
	if err := prhytj.WriteParquetRows(path, rows); err != nil {
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

func profileHash(row FinalCompanyRow) string {
	payload, err := json.Marshal(struct {
		CountryCompanyID      string `json:"country_company_id"`
		CountryISO2           string `json:"country_iso2"`
		PrimarySourceSlug     string `json:"primary_source_slug"`
		PrimarySourceRecordID string `json:"primary_source_record_id"`
		BusinessID            string `json:"business_id"`
		LegalName             string `json:"legal_name"`
		LegalNameEn           string `json:"legal_name_en"`
		LegalNameNormalized   string `json:"legal_name_normalized"`
		LifecycleStatus       string `json:"lifecycle_status"`
		IsActive              bool   `json:"is_active"`
		VATID                 string `json:"vat_id"`
		EUID                  string `json:"euid"`
		LegalFormCode         string `json:"legal_form_code"`
		LegalFormLabel        string `json:"legal_form_label"`
		LegalFormLabelEn      string `json:"legal_form_label_en"`
		PrimaryIndustryCode   string `json:"primary_industry_code"`
		PrimaryNACECode       string `json:"primary_nace_code"`
		PrimaryNACERevision   string `json:"primary_nace_revision"`
		WebsiteNormalizedURL  string `json:"website_normalized_url"`
		SourcePayloadHash     string `json:"source_payload_hash"`
		MergeRuleVersion      string `json:"merge_rule_version"`
		IsTranslated          bool   `json:"is_translated"`
	}{
		CountryCompanyID:      row.CountryCompanyID,
		CountryISO2:           row.CountryISO2,
		PrimarySourceSlug:     row.PrimarySourceSlug,
		PrimarySourceRecordID: row.PrimarySourceRecordID,
		BusinessID:            row.BusinessID,
		LegalName:             row.LegalName,
		LegalNameEn:           row.LegalNameEn,
		LegalNameNormalized:   row.LegalNameNormalized,
		LifecycleStatus:       row.LifecycleStatus,
		IsActive:              row.IsActive,
		VATID:                 row.VATID,
		EUID:                  row.EUID,
		LegalFormCode:         row.LegalFormCode,
		LegalFormLabel:        row.LegalFormLabel,
		LegalFormLabelEn:      row.LegalFormLabelEn,
		PrimaryIndustryCode:   row.PrimaryIndustryCode,
		PrimaryNACECode:       row.PrimaryNACECode,
		PrimaryNACERevision:   row.PrimaryNACERevision,
		WebsiteNormalizedURL:  row.WebsiteNormalizedURL,
		SourcePayloadHash:     row.SourcePayloadHash,
		MergeRuleVersion:      row.MergeRuleVersion,
		IsTranslated:          row.IsTranslated,
	})
	if err != nil {
		payload = []byte{}
	}
	hash := sha256.Sum256(payload)
	return hex.EncodeToString(hash[:])
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
