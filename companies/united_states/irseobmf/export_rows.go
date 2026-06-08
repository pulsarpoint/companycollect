package irseobmf

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"
)

// SourceExportSchemaVersion identifies the IRS EO BMF source-export row shape.
const SourceExportSchemaVersion = "united_states.irseobmf.source.v1"

// ExportRows holds all source-export tables produced from EO BMF records.
type ExportRows struct {
	Companies       []CompanyExportRow
	CompanyNames    []CompanyNameExportRow
	Addresses       []AddressExportRow
	Classifications []ClassificationExportRow
	Financials      []FinancialExportRow
	Identifiers     []IdentifierExportRow
	SourceEvidence  []SourceEvidenceExportRow
}

// CompanyExportRow is the denormalized core nonprofit record.
type CompanyExportRow struct {
	CountryISO2         string `parquet:"country_iso2"`
	SourceSlug          string `parquet:"source_slug"`
	SourceRunID         string `parquet:"source_run_id"`
	SourceRecordID      string `parquet:"source_record_id"`
	SourceNativeID      string `parquet:"source_native_id"`
	SourcePayloadHash   string `parquet:"source_payload_hash"`
	ExportedAt          string `parquet:"exported_at"`
	SchemaVersion       string `parquet:"schema_version"`
	EIN                 string `parquet:"ein"`
	LegalName           string `parquet:"legal_name"`
	LegalNameNormalized string `parquet:"legal_name_normalized"`
	SortName            string `parquet:"sort_name"`
	InCareOf            string `parquet:"in_care_of"`
	IsNonprofit         bool   `parquet:"is_nonprofit"`
	ExemptStatusCode    string `parquet:"exempt_status_code"`
	IsActiveExempt      bool   `parquet:"is_active_exempt"`
	SubsectionCode      string `parquet:"subsection_code"`
	OrganizationCode    string `parquet:"organization_code"`
	FoundationCode      string `parquet:"foundation_code"`
	NTEECode            string `parquet:"ntee_code"`
	GroupExemptionNo    string `parquet:"group_exemption_number"`
	RulingDate          string `parquet:"ruling_date"`
	MailingState        string `parquet:"mailing_state"`
}

// CompanyNameExportRow is one name per row (legal NAME and secondary SORT_NAME).
type CompanyNameExportRow struct {
	CountryISO2    string `parquet:"country_iso2"`
	SourceSlug     string `parquet:"source_slug"`
	SourceRunID    string `parquet:"source_run_id"`
	SourceRecordID string `parquet:"source_record_id"`
	SourceItemHash string `parquet:"source_item_hash"`
	EIN            string `parquet:"ein"`
	Name           string `parquet:"name"`
	NameType       string `parquet:"name_type"`
	IsPrimary      bool   `parquet:"is_primary"`
}

// AddressExportRow captures the IRS mailing address.
type AddressExportRow struct {
	CountryISO2    string `parquet:"country_iso2"`
	SourceSlug     string `parquet:"source_slug"`
	SourceRunID    string `parquet:"source_run_id"`
	SourceRecordID string `parquet:"source_record_id"`
	SourceItemHash string `parquet:"source_item_hash"`
	EIN            string `parquet:"ein"`
	AddressRole    string `parquet:"address_role"`
	InCareOf       string `parquet:"in_care_of"`
	Street         string `parquet:"street"`
	City           string `parquet:"city"`
	State          string `parquet:"state"`
	ZipCode        string `parquet:"zip_code"`
	CountryCode    string `parquet:"country_code"`
}

// ClassificationExportRow preserves the EO BMF coded classification fields.
type ClassificationExportRow struct {
	CountryISO2         string `parquet:"country_iso2"`
	SourceSlug          string `parquet:"source_slug"`
	SourceRunID         string `parquet:"source_run_id"`
	SourceRecordID      string `parquet:"source_record_id"`
	SourceItemHash      string `parquet:"source_item_hash"`
	EIN                 string `parquet:"ein"`
	SubsectionCode      string `parquet:"subsection_code"`
	ClassificationCode  string `parquet:"classification_code"`
	AffiliationCode     string `parquet:"affiliation_code"`
	FoundationCode      string `parquet:"foundation_code"`
	OrganizationCode    string `parquet:"organization_code"`
	DeductibilityCode   string `parquet:"deductibility_code"`
	NTEECode            string `parquet:"ntee_code"`
	ActivityCodesLegacy string `parquet:"activity_codes_legacy"`
	FilingReqCode       string `parquet:"filing_requirement_code"`
	PFFilingReqCode     string `parquet:"pf_filing_requirement_code"`
}

// FinancialExportRow is the sparse nonprofit financial snapshot. It is only
// emitted when a tax period or any amount is present.
type FinancialExportRow struct {
	CountryISO2        string `parquet:"country_iso2"`
	SourceSlug         string `parquet:"source_slug"`
	SourceRunID        string `parquet:"source_run_id"`
	SourceRecordID     string `parquet:"source_record_id"`
	SourceItemHash     string `parquet:"source_item_hash"`
	EIN                string `parquet:"ein"`
	TaxPeriod          string `parquet:"tax_period"`
	AccountingPeriodMM string `parquet:"accounting_period_month"`
	AssetCode          string `parquet:"asset_code"`
	IncomeCode         string `parquet:"income_code"`
	AssetAmount        int64  `parquet:"asset_amount"`
	AssetAmountPresent bool   `parquet:"asset_amount_present"`
	IncomeAmount       int64  `parquet:"income_amount"`
	IncomePresent      bool   `parquet:"income_amount_present"`
	RevenueAmount      int64  `parquet:"revenue_amount"`
	RevenuePresent     bool   `parquet:"revenue_amount_present"`
}

// IdentifierExportRow carries the EIN and the group exemption number.
type IdentifierExportRow struct {
	CountryISO2      string `parquet:"country_iso2"`
	SourceSlug       string `parquet:"source_slug"`
	SourceRunID      string `parquet:"source_run_id"`
	SourceRecordID   string `parquet:"source_record_id"`
	SourceItemHash   string `parquet:"source_item_hash"`
	EIN              string `parquet:"ein"`
	IdentifierType   string `parquet:"identifier_type"`
	IdentifierValue  string `parquet:"identifier_value"`
	IdentifierScheme string `parquet:"identifier_scheme"`
	IsPrimary        bool   `parquet:"is_primary"`
}

// SourceEvidenceExportRow keeps full source lineage and the raw payload.
type SourceEvidenceExportRow struct {
	CountryISO2        string `parquet:"country_iso2"`
	SourceSlug         string `parquet:"source_slug"`
	SourceRunID        string `parquet:"source_run_id"`
	SourceRecordID     string `parquet:"source_record_id"`
	SourceNativeID     string `parquet:"source_native_id"`
	SourcePayloadHash  string `parquet:"source_payload_hash"`
	EIN                string `parquet:"ein"`
	EvidenceType       string `parquet:"evidence_type"`
	Evidence           string `parquet:"evidence"`
	EvidenceCapturedAt string `parquet:"evidence_captured_at"`
}

// ProjectExportRows projects one source-native record into all export tables.
func ProjectExportRows(record IrsEoBmfRecord, runID string) ExportRows {
	ein := record.EIN
	exportedAt := time.Now().UTC().Format(time.RFC3339)

	rows := ExportRows{
		Companies: []CompanyExportRow{{
			CountryISO2:         "US",
			SourceSlug:          SourceSlug,
			SourceRunID:         runID,
			SourceRecordID:      ein,
			SourceNativeID:      ein,
			SourcePayloadHash:   record.PayloadHash,
			ExportedAt:          exportedAt,
			SchemaVersion:       SourceExportSchemaVersion,
			EIN:                 ein,
			LegalName:           record.Name,
			LegalNameNormalized: normalizedText(record.Name),
			SortName:            record.SortName,
			InCareOf:            record.InCareOf,
			IsNonprofit:         true,
			ExemptStatusCode:    record.Status,
			IsActiveExempt:      isActiveExemptStatus(record.Status),
			SubsectionCode:      record.Subsection,
			OrganizationCode:    record.Organization,
			FoundationCode:      record.Foundation,
			NTEECode:            record.NTEECD,
			GroupExemptionNo:    record.Group,
			RulingDate:          record.Ruling,
			MailingState:        record.State,
		}},
		Classifications: []ClassificationExportRow{{
			CountryISO2:         "US",
			SourceSlug:          SourceSlug,
			SourceRunID:         runID,
			SourceRecordID:      ein,
			SourceItemHash:      sourceItemHash("classification", ein, record.Subsection, record.Classification, record.NTEECD),
			EIN:                 ein,
			SubsectionCode:      record.Subsection,
			ClassificationCode:  record.Classification,
			AffiliationCode:     record.Affiliation,
			FoundationCode:      record.Foundation,
			OrganizationCode:    record.Organization,
			DeductibilityCode:   record.Deductibility,
			NTEECode:            record.NTEECD,
			ActivityCodesLegacy: record.Activity,
			FilingReqCode:       record.FilingReqCD,
			PFFilingReqCode:     record.PFFilingReqCD,
		}},
	}

	// Legal name.
	if strings.TrimSpace(record.Name) != "" {
		rows.CompanyNames = append(rows.CompanyNames, CompanyNameExportRow{
			CountryISO2:    "US",
			SourceSlug:     SourceSlug,
			SourceRunID:    runID,
			SourceRecordID: ein,
			SourceItemHash: sourceItemHash("company_name", ein, "legal", record.Name),
			EIN:            ein,
			Name:           record.Name,
			NameType:       "legal",
			IsPrimary:      true,
		})
	}
	// Secondary sort/chapter name.
	if strings.TrimSpace(record.SortName) != "" {
		rows.CompanyNames = append(rows.CompanyNames, CompanyNameExportRow{
			CountryISO2:    "US",
			SourceSlug:     SourceSlug,
			SourceRunID:    runID,
			SourceRecordID: ein,
			SourceItemHash: sourceItemHash("company_name", ein, "sort", record.SortName),
			EIN:            ein,
			Name:           record.SortName,
			NameType:       "sort",
			IsPrimary:      false,
		})
	}

	// IRS mailing address, only when any address component is present.
	if hasAnyAddress(record) {
		rows.Addresses = append(rows.Addresses, AddressExportRow{
			CountryISO2:    "US",
			SourceSlug:     SourceSlug,
			SourceRunID:    runID,
			SourceRecordID: ein,
			SourceItemHash: sourceItemHash("address", ein, "irs_mailing", record.Street, record.City, record.State, record.Zip),
			EIN:            ein,
			AddressRole:    "irs_mailing",
			InCareOf:       record.InCareOf,
			Street:         record.Street,
			City:           record.City,
			State:          record.State,
			ZipCode:        record.Zip,
			CountryCode:    "US",
		})
	}

	// Sparse financials.
	assetAmt, assetPresent := parseAmount(record.AssetAmt)
	incomeAmt, incomePresent := parseAmount(record.IncomeAmt)
	revenueAmt, revenuePresent := parseAmount(record.RevenueAmt)
	if assetPresent || incomePresent || revenuePresent || strings.TrimSpace(record.TaxPeriod) != "" {
		rows.Financials = append(rows.Financials, FinancialExportRow{
			CountryISO2:        "US",
			SourceSlug:         SourceSlug,
			SourceRunID:        runID,
			SourceRecordID:     ein,
			SourceItemHash:     sourceItemHash("financial", ein, record.TaxPeriod, record.AssetAmt, record.IncomeAmt, record.RevenueAmt),
			EIN:                ein,
			TaxPeriod:          record.TaxPeriod,
			AccountingPeriodMM: record.AcctPD,
			AssetCode:          record.AssetCD,
			IncomeCode:         record.IncomeCD,
			AssetAmount:        assetAmt,
			AssetAmountPresent: assetPresent,
			IncomeAmount:       incomeAmt,
			IncomePresent:      incomePresent,
			RevenueAmount:      revenueAmt,
			RevenuePresent:     revenuePresent,
		})
	}

	// Identifiers: EIN (primary) and a group exemption number when present.
	rows.Identifiers = append(rows.Identifiers, IdentifierExportRow{
		CountryISO2:      "US",
		SourceSlug:       SourceSlug,
		SourceRunID:      runID,
		SourceRecordID:   ein,
		SourceItemHash:   sourceItemHash("identifier", ein, "ein", ein),
		EIN:              ein,
		IdentifierType:   "ein",
		IdentifierValue:  ein,
		IdentifierScheme: "us_irs_ein",
		IsPrimary:        true,
	})
	if group := strings.TrimSpace(record.Group); group != "" && group != "0000" {
		rows.Identifiers = append(rows.Identifiers, IdentifierExportRow{
			CountryISO2:      "US",
			SourceSlug:       SourceSlug,
			SourceRunID:      runID,
			SourceRecordID:   ein,
			SourceItemHash:   sourceItemHash("identifier", ein, "group_exemption", group),
			EIN:              ein,
			IdentifierType:   "group_exemption_number",
			IdentifierValue:  group,
			IdentifierScheme: "us_irs_gen",
			IsPrimary:        false,
		})
	}

	rows.SourceEvidence = append(rows.SourceEvidence, SourceEvidenceExportRow{
		CountryISO2:        "US",
		SourceSlug:         SourceSlug,
		SourceRunID:        runID,
		SourceRecordID:     ein,
		SourceNativeID:     ein,
		SourcePayloadHash:  record.PayloadHash,
		EIN:                ein,
		EvidenceType:       "irs_eo_bmf_record",
		Evidence:           string(record.RawPayload),
		EvidenceCapturedAt: exportedAt,
	})

	return rows
}

func hasAnyAddress(record IrsEoBmfRecord) bool {
	return strings.TrimSpace(record.Street) != "" ||
		strings.TrimSpace(record.City) != "" ||
		strings.TrimSpace(record.State) != "" ||
		strings.TrimSpace(record.Zip) != ""
}

func appendExportRows(dst *ExportRows, src ExportRows) {
	dst.Companies = append(dst.Companies, src.Companies...)
	dst.CompanyNames = append(dst.CompanyNames, src.CompanyNames...)
	dst.Addresses = append(dst.Addresses, src.Addresses...)
	dst.Classifications = append(dst.Classifications, src.Classifications...)
	dst.Financials = append(dst.Financials, src.Financials...)
	dst.Identifiers = append(dst.Identifiers, src.Identifiers...)
	dst.SourceEvidence = append(dst.SourceEvidence, src.SourceEvidence...)
}

func normalizedText(value string) string {
	return strings.ToLower(strings.Join(strings.Fields(value), " "))
}

func sourceItemHash(kind string, sourceRecordID string, values ...any) string {
	payload, err := json.Marshal(values)
	if err != nil {
		payload = []byte{}
	}
	return payloadHash(kind, sourceRecordID, string(payload))
}

func payloadHash(values ...string) string {
	hasher := sha256.New()
	for _, value := range values {
		hasher.Write([]byte(value))
		hasher.Write([]byte{0})
	}
	return hex.EncodeToString(hasher.Sum(nil))
}
