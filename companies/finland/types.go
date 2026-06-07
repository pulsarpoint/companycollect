package finland

const (
	FinalSchemaVersionV1 = "finland.final.v1"
	MergeRuleVersionV1   = "finland.merge.v1"
)

type FinalCompanyRow struct {
	CountryCompanyID      string `parquet:"country_company_id"`
	CountryISO2           string `parquet:"country_iso2"`
	PrimarySourceSlug     string `parquet:"primary_source_slug"`
	PrimarySourceRecordID string `parquet:"primary_source_record_id"`
	BusinessID            string `parquet:"business_id"`
	LegalName             string `parquet:"legal_name"`
	LegalNameEn           string `parquet:"legal_name_en"`
	LegalNameNormalized   string `parquet:"legal_name_normalized"`
	LifecycleStatus       string `parquet:"lifecycle_status"`
	IsActive              bool   `parquet:"is_active"`
	VATID                 string `parquet:"vat_id"`
	EUID                  string `parquet:"euid"`
	LegalFormCode         string `parquet:"legal_form_code"`
	LegalFormLabel        string `parquet:"legal_form_label"`
	LegalFormLabelEn      string `parquet:"legal_form_label_en"`
	PrimaryIndustryCode   string `parquet:"primary_industry_code"`
	PrimaryNACECode       string `parquet:"primary_nace_code"`
	PrimaryNACERevision   string `parquet:"primary_nace_revision"`
	WebsiteNormalizedURL  string `parquet:"website_normalized_url"`
	SourcePayloadHash     string `parquet:"source_payload_hash"`
	ProfileHash           string `parquet:"profile_hash"`
	MergeRuleVersion      string `parquet:"merge_rule_version"`
	IsTranslated          bool   `parquet:"is_translated"`
	ExportedAt            string `parquet:"exported_at"`
}

type FinalCompanyNameRow struct {
	CountryCompanyID      string `parquet:"country_company_id"`
	CountryISO2           string `parquet:"country_iso2"`
	SourceSlug            string `parquet:"source_slug"`
	SourceRecordID        string `parquet:"source_record_id"`
	Name                  string `parquet:"name"`
	NameType              string `parquet:"name_type"`
	Language              string `parquet:"language"`
	IsPrimary             bool   `parquet:"is_primary"`
	IsTranslated          bool   `parquet:"is_translated"`
	PrimarySourceRecordID string `parquet:"primary_source_record_id"`
}

type FinalIdentifierRow struct {
	CountryCompanyID string `parquet:"country_company_id"`
	CountryISO2      string `parquet:"country_iso2"`
	SourceSlug       string `parquet:"source_slug"`
	SourceRecordID   string `parquet:"source_record_id"`
	IdentifierType   string `parquet:"identifier_type"`
	IdentifierValue  string `parquet:"identifier_value"`
	IsPrimary        bool   `parquet:"is_primary"`
}

type FinalAddressRow struct {
	CountryCompanyID string `parquet:"country_company_id"`
	CountryISO2      string `parquet:"country_iso2"`
	SourceSlug       string `parquet:"source_slug"`
	SourceRecordID   string `parquet:"source_record_id"`
	AddressType      string `parquet:"address_type"`
	Street           string `parquet:"street"`
	PostCode         string `parquet:"post_code"`
	City             string `parquet:"city"`
	Country          string `parquet:"country"`
	IsPrimary        bool   `parquet:"is_primary"`
}

type FinalIndustryRow struct {
	CountryCompanyID string `parquet:"country_company_id"`
	CountryISO2      string `parquet:"country_iso2"`
	SourceSlug       string `parquet:"source_slug"`
	SourceRecordID   string `parquet:"source_record_id"`
	IndustryCode     string `parquet:"industry_code"`
	NACECode         string `parquet:"nace_code"`
	NACERevision     string `parquet:"nace_revision"`
	IsPrimary        bool   `parquet:"is_primary"`
}

type FinalWebsiteRow struct {
	CountryCompanyID string `parquet:"country_company_id"`
	CountryISO2      string `parquet:"country_iso2"`
	SourceSlug       string `parquet:"source_slug"`
	SourceRecordID   string `parquet:"source_record_id"`
	NormalizedURL    string `parquet:"normalized_url"`
	IsPrimary        bool   `parquet:"is_primary"`
}

type FinalSourceEvidenceRow struct {
	CountryCompanyID  string `parquet:"country_company_id"`
	CountryISO2       string `parquet:"country_iso2"`
	SourceSlug        string `parquet:"source_slug"`
	SourceRecordID    string `parquet:"source_record_id"`
	SourcePayloadHash string `parquet:"source_payload_hash"`
	MergeRuleVersion  string `parquet:"merge_rule_version"`
}
