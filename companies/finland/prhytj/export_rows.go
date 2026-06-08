package prhytj

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"strings"
	"time"
)

const SourceExportSchemaVersion = "finland.prhytj.source.v1"

type ExportRows struct {
	RawRecords        []RawRecordExportRow
	Companies         []CompanyExportRow
	CompanyNames      []CompanyNameExportRow
	LegalForms        []LegalFormExportRow
	Industries        []IndustryExportRow
	Addresses         []AddressExportRow
	RegisteredEntries []RegisteredEntryExportRow
	TaxRegistrations  []TaxRegistrationExportRow
	Websites          []WebsiteExportRow
}

type RawRecordExportRow struct {
	CountryISO2        string `parquet:"country_iso2"`
	SourceSlug         string `parquet:"source_slug"`
	SourceRunID        string `parquet:"source_run_id"`
	SourceRecordID     string `parquet:"source_record_id"`
	BusinessID         string `parquet:"business_id"`
	SourcePayloadHash  string `parquet:"source_payload_hash"`
	SnapshotPath       string `parquet:"snapshot_path"`
	SnapshotSHA256     string `parquet:"snapshot_sha256"`
	SnapshotLineNumber int64  `parquet:"snapshot_line_number"`
	RawPayloadJSON     string `parquet:"raw_payload_json"`
	SchemaVersion      string `parquet:"schema_version"`
	ExportedAt         string `parquet:"exported_at"`
}

type CompanyExportRow struct {
	CountryISO2            string `parquet:"country_iso2"`
	SourceSlug             string `parquet:"source_slug"`
	SourceRunID            string `parquet:"source_run_id"`
	SourceRecordID         string `parquet:"source_record_id"`
	SourceNativeID         string `parquet:"source_native_id"`
	SourcePayloadHash      string `parquet:"source_payload_hash"`
	SourceUpdatedAt        string `parquet:"source_updated_at"`
	ExportedAt             string `parquet:"exported_at"`
	SchemaVersion          string `parquet:"schema_version"`
	BusinessID             string `parquet:"business_id"`
	VATID                  string `parquet:"vat_id"`
	EUID                   string `parquet:"euid"`
	LegalName              string `parquet:"legal_name"`
	LegalNameNormalized    string `parquet:"legal_name_normalized"`
	LifecycleStatus        string `parquet:"lifecycle_status"`
	IsActive               bool   `parquet:"is_active"`
	LegalFormCode          string `parquet:"legal_form_code"`
	LegalFormLabel         string `parquet:"legal_form_label"`
	LegalFormLabelEn       string `parquet:"legal_form_label_en"`
	PrimaryIndustryCode    string `parquet:"primary_industry_code"`
	PrimaryIndustryCodeSet string `parquet:"primary_industry_code_set"`
	PrimaryIndustryLabel   string `parquet:"primary_industry_label"`
	PrimaryIndustryLabelEn string `parquet:"primary_industry_label_en"`
	PrimaryNACECode        string `parquet:"primary_nace_code"`
	PrimaryNACERevision    string `parquet:"primary_nace_revision"`
	WebsiteURL             string `parquet:"website_url"`
	WebsiteNormalizedURL   string `parquet:"website_normalized_url"`
	WebsiteHost            string `parquet:"website_host"`
}

type CompanyNameExportRow struct {
	CountryISO2    string `parquet:"country_iso2"`
	SourceSlug     string `parquet:"source_slug"`
	SourceRunID    string `parquet:"source_run_id"`
	SourceRecordID string `parquet:"source_record_id"`
	SourceItemHash string `parquet:"source_item_hash"`
	BusinessID     string `parquet:"business_id"`
	SourcePosition int32  `parquet:"source_position"`
	Name           string `parquet:"name"`
	NameTypeCode   string `parquet:"name_type_code"`
	RegisteredOn   string `parquet:"registered_on"`
	EndedOn        string `parquet:"ended_on"`
	IsCurrent      bool   `parquet:"is_current"`
	IsPrimary      bool   `parquet:"is_primary"`
}

type LegalFormExportRow struct {
	CountryISO2      string `parquet:"country_iso2"`
	SourceSlug       string `parquet:"source_slug"`
	SourceRunID      string `parquet:"source_run_id"`
	SourceRecordID   string `parquet:"source_record_id"`
	SourceItemHash   string `parquet:"source_item_hash"`
	BusinessID       string `parquet:"business_id"`
	LegalFormCode    string `parquet:"legal_form_code"`
	LegalFormLabel   string `parquet:"legal_form_label"`
	LegalFormLabelEn string `parquet:"legal_form_label_en"`
	LegalFormLabelFi string `parquet:"legal_form_label_fi"`
	LegalFormLabelSv string `parquet:"legal_form_label_sv"`
	RegisteredOn     string `parquet:"registered_on"`
	EndedOn          string `parquet:"ended_on"`
}

type IndustryExportRow struct {
	CountryISO2           string `parquet:"country_iso2"`
	SourceSlug            string `parquet:"source_slug"`
	SourceRunID           string `parquet:"source_run_id"`
	SourceRecordID        string `parquet:"source_record_id"`
	SourceItemHash        string `parquet:"source_item_hash"`
	BusinessID            string `parquet:"business_id"`
	SourceIndustryCode    string `parquet:"source_industry_code"`
	SourceIndustryCodeSet string `parquet:"source_industry_code_set"`
	SourceIndustryLabel   string `parquet:"source_industry_label"`
	SourceIndustryLabelEn string `parquet:"source_industry_label_en"`
	SourceIndustryLabelFi string `parquet:"source_industry_label_fi"`
	SourceIndustryLabelSv string `parquet:"source_industry_label_sv"`
	MappedNACECode        string `parquet:"mapped_nace_code"`
	NACERevision          string `parquet:"nace_revision"`
	IsPrimary             bool   `parquet:"is_primary"`
}

type AddressExportRow struct {
	CountryISO2      string `parquet:"country_iso2"`
	SourceSlug       string `parquet:"source_slug"`
	SourceRunID      string `parquet:"source_run_id"`
	SourceRecordID   string `parquet:"source_record_id"`
	SourceItemHash   string `parquet:"source_item_hash"`
	BusinessID       string `parquet:"business_id"`
	SourcePosition   int32  `parquet:"source_position"`
	AddressTypeCode  int32  `parquet:"address_type_code"`
	AddressType      string `parquet:"address_type"`
	Street           string `parquet:"street"`
	BuildingNumber   string `parquet:"building_number"`
	Entrance         string `parquet:"entrance"`
	ApartmentNumber  string `parquet:"apartment_number"`
	PostOfficeBox    string `parquet:"post_office_box"`
	CO               string `parquet:"co"`
	PostCode         string `parquet:"post_code"`
	CityFi           string `parquet:"city_fi"`
	CitySv           string `parquet:"city_sv"`
	MunicipalityCode string `parquet:"municipality_code"`
	Country          string `parquet:"country"`
	RegisteredOn     string `parquet:"registered_on"`
}

type RegisteredEntryExportRow struct {
	CountryISO2      string `parquet:"country_iso2"`
	SourceSlug       string `parquet:"source_slug"`
	SourceRunID      string `parquet:"source_run_id"`
	SourceRecordID   string `parquet:"source_record_id"`
	SourceItemHash   string `parquet:"source_item_hash"`
	BusinessID       string `parquet:"business_id"`
	RegisterCode     string `parquet:"register_code"`
	RegisterLabel    string `parquet:"register_label"`
	Authority        string `parquet:"authority"`
	EntryTypeCode    string `parquet:"entry_type_code"`
	EntryTypeLabel   string `parquet:"entry_type_label"`
	EntryTypeLabelEn string `parquet:"entry_type_label_en"`
	RegisteredOn     string `parquet:"registered_on"`
	EndedOn          string `parquet:"ended_on"`
	IsCurrent        bool   `parquet:"is_current"`
}

type TaxRegistrationExportRow struct {
	CountryISO2       string `parquet:"country_iso2"`
	SourceSlug        string `parquet:"source_slug"`
	SourceRunID       string `parquet:"source_run_id"`
	SourceRecordID    string `parquet:"source_record_id"`
	SourceItemHash    string `parquet:"source_item_hash"`
	BusinessID        string `parquet:"business_id"`
	RegistrationType  string `parquet:"registration_type"`
	RegisterCode      string `parquet:"register_code"`
	CurrentRegistered bool   `parquet:"current_registered"`
	FirstRegisteredOn string `parquet:"first_registered_on"`
	EndedOn           string `parquet:"ended_on"`
}

type WebsiteExportRow struct {
	CountryISO2    string `parquet:"country_iso2"`
	SourceSlug     string `parquet:"source_slug"`
	SourceRunID    string `parquet:"source_run_id"`
	SourceRecordID string `parquet:"source_record_id"`
	SourceItemHash string `parquet:"source_item_hash"`
	BusinessID     string `parquet:"business_id"`
	URL            string `parquet:"url"`
	NormalizedURL  string `parquet:"normalized_url"`
	Host           string `parquet:"host"`
	Path           string `parquet:"path"`
	RegisteredOn   string `parquet:"registered_on"`
	EndedOn        string `parquet:"ended_on"`
	IsCurrent      bool   `parquet:"is_current"`
	IsPrimary      bool   `parquet:"is_primary"`
}

func ProjectExportRows(record CompanyRecord, runID string) ExportRows {
	profile := record.ToProfile()
	sourceRecordID := strings.TrimSpace(record.BusinessID.Value)
	exportedAt := time.Now().UTC().Format(time.RFC3339)
	websiteHost, websitePath := websiteParts(profile.Website)
	primaryNACECode := mappedNACECode(record.MainBusinessLine.Type)
	primaryNACERevision := ""
	if primaryNACECode != "" {
		primaryNACERevision = "2.1"
	}

	rows := ExportRows{
		Companies: []CompanyExportRow{{
			CountryISO2:            "FI",
			SourceSlug:             SourceSlug,
			SourceRunID:            runID,
			SourceRecordID:         sourceRecordID,
			SourceNativeID:         sourceRecordID,
			SourcePayloadHash:      record.PayloadHash,
			SourceUpdatedAt:        record.LastModified,
			ExportedAt:             exportedAt,
			SchemaVersion:          SourceExportSchemaVersion,
			BusinessID:             sourceRecordID,
			VATID:                  profile.VATID,
			EUID:                   profile.EUID,
			LegalName:              profile.LegalName,
			LegalNameNormalized:    normalizedText(profile.LegalName),
			LifecycleStatus:        lifecycleStatus(record),
			IsActive:               profile.IsActive,
			LegalFormCode:          profile.LegalFormCode,
			LegalFormLabel:         profile.LegalForm,
			LegalFormLabelEn:       descriptionByLanguage(currentCompanyForm(record.CompanyForms).Descriptions, "3"),
			PrimaryIndustryCode:    record.MainBusinessLine.Type,
			PrimaryIndustryCodeSet: record.MainBusinessLine.TypeCodeSet,
			PrimaryIndustryLabel:   profile.MainBusinessLine,
			PrimaryIndustryLabelEn: descriptionByLanguage(record.MainBusinessLine.Descriptions, "3"),
			PrimaryNACECode:        primaryNACECode,
			PrimaryNACERevision:    primaryNACERevision,
			WebsiteURL:             record.Website.URL,
			WebsiteNormalizedURL:   profile.Website,
			WebsiteHost:            websiteHost,
		}},
	}
	rows.CompanyNames = projectNameRows(record, runID, sourceRecordID)
	rows.LegalForms = projectLegalFormRows(record, runID, sourceRecordID)
	rows.Industries = projectIndustryRows(record, runID, sourceRecordID)
	rows.Addresses = projectAddressRows(record, runID, sourceRecordID)
	rows.RegisteredEntries = projectRegisteredEntryRows(record, runID, sourceRecordID)
	rows.TaxRegistrations = projectTaxRegistrationRows(record, runID, sourceRecordID, profile.TaxRegistrations)
	rows.Websites = projectWebsiteRows(record, runID, sourceRecordID, profile.Website, websiteHost, websitePath)
	return rows
}

func ProjectRawRecordExportRow(record CompanyRecord, runID string, snapshotPath string, snapshotSHA256 string, lineNumber int64, exportedAt string) RawRecordExportRow {
	businessID := strings.TrimSpace(record.BusinessID.Value)
	return RawRecordExportRow{
		CountryISO2:        "FI",
		SourceSlug:         SourceSlug,
		SourceRunID:        runID,
		SourceRecordID:     businessID,
		BusinessID:         businessID,
		SourcePayloadHash:  record.PayloadHash,
		SnapshotPath:       snapshotPath,
		SnapshotSHA256:     snapshotSHA256,
		SnapshotLineNumber: lineNumber,
		RawPayloadJSON:     string(record.RawPayload),
		SchemaVersion:      SourceExportSchemaVersion,
		ExportedAt:         exportedAt,
	}
}

func sourceItemHash(kind string, businessID string, value any) string {
	payload, err := json.Marshal(value)
	if err != nil {
		payload = []byte{}
	}
	hash := sha256.New()
	hash.Write([]byte(kind))
	hash.Write([]byte{0})
	hash.Write([]byte(businessID))
	hash.Write([]byte{0})
	hash.Write(payload)
	sum := hash.Sum(nil)
	return hex.EncodeToString(sum)
}

func normalizedText(value string) string {
	return strings.ToLower(strings.Join(strings.Fields(value), " "))
}

func lifecycleStatus(record CompanyRecord) string {
	if strings.TrimSpace(record.EndDate) != "" {
		return "ceased"
	}
	if strings.TrimSpace(record.TradeRegisterStatus) == "1" {
		return "active"
	}
	if strings.TrimSpace(record.Status) != "" {
		return "inactive"
	}
	return "unknown"
}

func mappedNACECode(sourceCode string) string {
	return strings.TrimSpace(sourceCode)
}

func websiteParts(normalizedURL string) (string, string) {
	parsed, err := url.Parse(strings.TrimSpace(normalizedURL))
	if err != nil {
		return "", ""
	}
	return parsed.Hostname(), parsed.EscapedPath()
}

func postOfficeCity(postOffices []PostOffice, languageCode string) string {
	for _, postOffice := range postOffices {
		if postOffice.LanguageCode == languageCode {
			return postOffice.City
		}
	}
	return ""
}

func postOfficeMunicipalityCode(postOffices []PostOffice) string {
	for _, postOffice := range postOffices {
		if postOffice.MunicipalityCode != "" {
			return postOffice.MunicipalityCode
		}
	}
	return ""
}

func projectNameRows(record CompanyRecord, runID string, businessID string) []CompanyNameExportRow {
	rows := make([]CompanyNameExportRow, 0, len(record.Names))
	primaryName := currentLegalName(record.Names)
	for index, name := range record.Names {
		sourcePosition := int32(index + 1)
		rows = append(rows, CompanyNameExportRow{
			CountryISO2:    "FI",
			SourceSlug:     SourceSlug,
			SourceRunID:    runID,
			SourceRecordID: businessID,
			SourceItemHash: sourceItemHash("company_name", businessID, struct {
				SourcePosition int32 `json:"source_position"`
				Name           Name  `json:"name"`
			}{SourcePosition: sourcePosition, Name: name}),
			BusinessID:     businessID,
			SourcePosition: sourcePosition,
			Name:           name.Name,
			NameTypeCode:   name.Type,
			RegisteredOn:   name.RegistrationDate,
			EndedOn:        name.EndDate,
			IsCurrent:      name.EndDate == "",
			IsPrimary:      name.Type == "1" && name.EndDate == "" && name.Name == primaryName,
		})
	}
	return rows
}

func projectLegalFormRows(record CompanyRecord, runID string, businessID string) []LegalFormExportRow {
	rows := make([]LegalFormExportRow, 0, len(record.CompanyForms))
	for index, form := range record.CompanyForms {
		rows = append(rows, LegalFormExportRow{
			CountryISO2:    "FI",
			SourceSlug:     SourceSlug,
			SourceRunID:    runID,
			SourceRecordID: businessID,
			SourceItemHash: sourceItemHash("legal_form", businessID, struct {
				SourcePosition int         `json:"source_position"`
				CompanyForm    CompanyForm `json:"company_form"`
			}{SourcePosition: index + 1, CompanyForm: form}),
			BusinessID:       businessID,
			LegalFormCode:    form.Type,
			LegalFormLabel:   preferredDescription(form.Descriptions),
			LegalFormLabelEn: descriptionByLanguage(form.Descriptions, "3"),
			LegalFormLabelFi: descriptionByLanguage(form.Descriptions, "1"),
			LegalFormLabelSv: descriptionByLanguage(form.Descriptions, "2"),
			RegisteredOn:     form.RegistrationDate,
			EndedOn:          form.EndDate,
		})
	}
	return rows
}

func projectIndustryRows(record CompanyRecord, runID string, businessID string) []IndustryExportRow {
	line := record.MainBusinessLine
	if line.Type == "" && len(line.Descriptions) == 0 {
		return nil
	}
	mappedCode := mappedNACECode(line.Type)
	naceRevision := ""
	if mappedCode != "" {
		naceRevision = "2.1"
	}
	return []IndustryExportRow{{
		CountryISO2:           "FI",
		SourceSlug:            SourceSlug,
		SourceRunID:           runID,
		SourceRecordID:        businessID,
		SourceItemHash:        sourceItemHash("industry", businessID, line),
		BusinessID:            businessID,
		SourceIndustryCode:    line.Type,
		SourceIndustryCodeSet: line.TypeCodeSet,
		SourceIndustryLabel:   preferredDescription(line.Descriptions),
		SourceIndustryLabelEn: descriptionByLanguage(line.Descriptions, "3"),
		SourceIndustryLabelFi: descriptionByLanguage(line.Descriptions, "1"),
		SourceIndustryLabelSv: descriptionByLanguage(line.Descriptions, "2"),
		MappedNACECode:        mappedCode,
		NACERevision:          naceRevision,
		IsPrimary:             true,
	}}
}

func projectAddressRows(record CompanyRecord, runID string, businessID string) []AddressExportRow {
	rows := make([]AddressExportRow, 0, len(record.Addresses))
	for index, address := range record.Addresses {
		sourcePosition := int32(index + 1)
		rows = append(rows, AddressExportRow{
			CountryISO2:    "FI",
			SourceSlug:     SourceSlug,
			SourceRunID:    runID,
			SourceRecordID: businessID,
			SourceItemHash: sourceItemHash("address", businessID, struct {
				SourcePosition int32   `json:"source_position"`
				Address        Address `json:"address"`
			}{SourcePosition: sourcePosition, Address: address}),
			BusinessID:       businessID,
			SourcePosition:   sourcePosition,
			AddressTypeCode:  int32(address.Type),
			AddressType:      addressTypeLabel(address.Type),
			Street:           address.Street,
			BuildingNumber:   address.BuildingNumber,
			Entrance:         address.Entrance,
			ApartmentNumber:  address.ApartmentNumber,
			PostOfficeBox:    address.PostOfficeBox,
			CO:               address.CO,
			PostCode:         address.PostCode,
			CityFi:           postOfficeCity(address.PostOffices, "1"),
			CitySv:           postOfficeCity(address.PostOffices, "2"),
			MunicipalityCode: postOfficeMunicipalityCode(address.PostOffices),
			Country:          address.Country,
			RegisteredOn:     address.RegistrationDate,
		})
	}
	return rows
}

func projectRegisteredEntryRows(record CompanyRecord, runID string, businessID string) []RegisteredEntryExportRow {
	rows := make([]RegisteredEntryExportRow, 0, len(record.RegisteredEntries))
	for index, entry := range record.RegisteredEntries {
		rows = append(rows, RegisteredEntryExportRow{
			CountryISO2:    "FI",
			SourceSlug:     SourceSlug,
			SourceRunID:    runID,
			SourceRecordID: businessID,
			SourceItemHash: sourceItemHash("registered_entry", businessID, struct {
				SourcePosition int             `json:"source_position"`
				Entry          RegisteredEntry `json:"entry"`
			}{SourcePosition: index + 1, Entry: entry}),
			BusinessID:       businessID,
			RegisterCode:     entry.Register,
			RegisterLabel:    registerLabel(entry.Register),
			Authority:        entry.Authority,
			EntryTypeCode:    entry.Type,
			EntryTypeLabel:   preferredDescription(entry.Descriptions),
			EntryTypeLabelEn: descriptionByLanguage(entry.Descriptions, "3"),
			RegisteredOn:     entry.RegistrationDate,
			EndedOn:          entry.EndDate,
			IsCurrent:        entry.EndDate == "",
		})
	}
	return rows
}

func projectTaxRegistrationRows(record CompanyRecord, runID string, businessID string, registrations TaxRegistrations) []TaxRegistrationExportRow {
	taxRows := []struct {
		Type         string
		RegisterCode string
		Registration TaxRegistration
	}{
		{Type: "VAT", RegisterCode: "6", Registration: registrations.VAT},
		{Type: "Employer", RegisterCode: "5", Registration: registrations.Employer},
		{Type: "Prepayment", RegisterCode: "7", Registration: registrations.Prepayment},
	}
	rows := make([]TaxRegistrationExportRow, 0, len(taxRows))
	for _, taxRow := range taxRows {
		firstRegisteredOn := taxRow.Registration.RegistrationDate
		if earliestActive := earliestActiveRegisteredEntryDate(record.RegisteredEntries, taxRow.RegisterCode); earliestActive != "" {
			firstRegisteredOn = earliestActive
		}
		rows = append(rows, TaxRegistrationExportRow{
			CountryISO2:    "FI",
			SourceSlug:     SourceSlug,
			SourceRunID:    runID,
			SourceRecordID: businessID,
			SourceItemHash: sourceItemHash("tax_registration", businessID, struct {
				Type              string          `json:"type"`
				RegisterCode      string          `json:"register_code"`
				Registration      TaxRegistration `json:"registration"`
				FirstRegisteredOn string          `json:"first_registered_on"`
			}{
				Type:              taxRow.Type,
				RegisterCode:      taxRow.RegisterCode,
				Registration:      taxRow.Registration,
				FirstRegisteredOn: firstRegisteredOn,
			}),
			BusinessID:        businessID,
			RegistrationType:  taxRow.Type,
			RegisterCode:      taxRow.RegisterCode,
			CurrentRegistered: taxRow.Registration.Registered,
			FirstRegisteredOn: firstRegisteredOn,
			EndedOn:           taxRow.Registration.EndDate,
		})
	}
	return rows
}

func earliestActiveRegisteredEntryDate(entries []RegisteredEntry, registerCode string) string {
	earliest := ""
	for _, entry := range entries {
		if entry.Register != registerCode || entry.EndDate != "" || entry.RegistrationDate == "" {
			continue
		}
		if earliest == "" || entry.RegistrationDate < earliest {
			earliest = entry.RegistrationDate
		}
	}
	return earliest
}

func projectWebsiteRows(record CompanyRecord, runID string, businessID string, normalizedWebsite string, websiteHost string, websitePath string) []WebsiteExportRow {
	if normalizedWebsite == "" {
		return nil
	}
	return []WebsiteExportRow{{
		CountryISO2:    "FI",
		SourceSlug:     SourceSlug,
		SourceRunID:    runID,
		SourceRecordID: businessID,
		SourceItemHash: sourceItemHash("website", businessID, record.Website),
		BusinessID:     businessID,
		URL:            record.Website.URL,
		NormalizedURL:  normalizedWebsite,
		Host:           websiteHost,
		Path:           websitePath,
		RegisteredOn:   record.Website.RegistrationDate,
		EndedOn:        record.Website.EndDate,
		IsCurrent:      record.Website.EndDate == "",
		IsPrimary:      true,
	}}
}

func addressTypeLabel(addressType int) string {
	switch addressType {
	case 1:
		return "visiting"
	case 2:
		return "postal"
	default:
		return ""
	}
}

func registerLabel(registerCode string) string {
	switch registerCode {
	case "1":
		return "Business Information System"
	case "4":
		return "Trade Register"
	case "5":
		return "Employer Register"
	case "6":
		return "VAT Register"
	case "7":
		return "Prepayment Register"
	default:
		return ""
	}
}
