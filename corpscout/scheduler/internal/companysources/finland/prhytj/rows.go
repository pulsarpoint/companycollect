package prhytj

import (
	"time"

	"github.com/google/uuid"
)

const (
	identifiersTable                  = "fi_prhytj_identifiers"
	statusesTable                     = "fi_prhytj_statuses"
	namesTable                        = "fi_prhytj_names"
	businessLinesTable                = "fi_prhytj_business_lines"
	businessLineDescriptionsTable     = "fi_prhytj_business_line_descriptions"
	websitesTable                     = "fi_prhytj_websites"
	companyFormsTable                 = "fi_prhytj_company_forms"
	companyFormDescriptionsTable      = "fi_prhytj_company_form_descriptions"
	companySituationsTable            = "fi_prhytj_company_situations"
	companySituationDescriptionsTable = "fi_prhytj_company_situation_descriptions"
	registeredEntriesTable            = "fi_prhytj_registered_entries"
	registeredEntryDescriptionsTable  = "fi_prhytj_registered_entry_descriptions"
	addressesTable                    = "fi_prhytj_addresses"
	addressPostOfficesTable           = "fi_prhytj_address_post_offices"
)

type NormalizedTable struct {
	Name        string
	Columns     []string
	ColumnTypes map[string]string
}

var identifierColumns = []string{"country_iso2", "source_slug", "source_run_id", "source_record_id", "business_id", "source_line_number", "source_payload_hash", "source_item_hash", "source_position", "identifier_scope", "identifier_type", "identifier_value", "registered_on", "ended_on", "source", "is_primary_business_id", "ingested_at", "source_export_id"}
var statusColumns = []string{"country_iso2", "source_slug", "source_run_id", "source_record_id", "business_id", "source_line_number", "source_payload_hash", "trade_register_status", "status", "registration_date", "end_date", "last_modified", "lifecycle_status", "is_active", "ingested_at", "source_export_id"}
var nameColumns = []string{"country_iso2", "source_slug", "source_run_id", "source_record_id", "business_id", "source_line_number", "source_payload_hash", "source_item_hash", "source_position", "name", "name_type_code", "version", "registered_on", "ended_on", "is_current", "is_primary", "ingested_at", "source_export_id"}
var businessLineColumns = []string{"country_iso2", "source_slug", "source_run_id", "source_record_id", "business_id", "source_line_number", "source_payload_hash", "source_item_hash", "business_line_type", "business_line_code_set", "registered_on", "source", "is_primary", "ingested_at", "source_export_id"}
var businessLineDescriptionColumns = []string{"country_iso2", "source_slug", "source_run_id", "source_record_id", "business_id", "source_line_number", "source_payload_hash", "source_item_hash", "business_line_item_hash", "source_position", "language_code", "description", "ingested_at", "source_export_id"}
var websiteColumns = []string{"country_iso2", "source_slug", "source_run_id", "source_record_id", "business_id", "source_line_number", "source_payload_hash", "source_item_hash", "url", "normalized_url", "host", "path", "registered_on", "ended_on", "is_current", "is_primary", "ingested_at", "source_export_id"}
var companyFormColumns = []string{"country_iso2", "source_slug", "source_run_id", "source_record_id", "business_id", "source_line_number", "source_payload_hash", "source_item_hash", "source_position", "form_type_code", "version", "registered_on", "ended_on", "source", "is_current", "ingested_at", "source_export_id"}
var companyFormDescriptionColumns = []string{"country_iso2", "source_slug", "source_run_id", "source_record_id", "business_id", "source_line_number", "source_payload_hash", "source_item_hash", "company_form_item_hash", "source_position", "language_code", "description", "ingested_at", "source_export_id"}
var companySituationColumns = []string{"country_iso2", "source_slug", "source_run_id", "source_record_id", "business_id", "source_line_number", "source_payload_hash", "source_item_hash", "source_position", "situation_type_code", "registered_on", "ended_on", "is_current", "ingested_at", "source_export_id"}
var companySituationDescriptionColumns = []string{"country_iso2", "source_slug", "source_run_id", "source_record_id", "business_id", "source_line_number", "source_payload_hash", "source_item_hash", "company_situation_item_hash", "source_position", "language_code", "description", "ingested_at", "source_export_id"}
var registeredEntryColumns = []string{"country_iso2", "source_slug", "source_run_id", "source_record_id", "business_id", "source_line_number", "source_payload_hash", "source_item_hash", "source_position", "entry_type_code", "register_code", "authority", "registered_on", "ended_on", "is_current", "ingested_at", "source_export_id"}
var registeredEntryDescriptionColumns = []string{"country_iso2", "source_slug", "source_run_id", "source_record_id", "business_id", "source_line_number", "source_payload_hash", "source_item_hash", "registered_entry_item_hash", "source_position", "language_code", "description", "ingested_at", "source_export_id"}
var addressColumns = []string{"country_iso2", "source_slug", "source_run_id", "source_record_id", "business_id", "source_line_number", "source_payload_hash", "source_item_hash", "source_position", "address_type_code", "street", "post_code", "building_number", "entrance", "apartment_number", "post_office_box", "co", "country", "registered_on", "source", "ingested_at", "source_export_id"}
var addressPostOfficeColumns = []string{"country_iso2", "source_slug", "source_run_id", "source_record_id", "business_id", "source_line_number", "source_payload_hash", "source_item_hash", "address_item_hash", "source_position", "language_code", "city", "municipality_code", "ingested_at", "source_export_id"}

var commonColumnTypes = map[string]string{
	"country_iso2":        "Nullable(String)",
	"source_slug":         "Nullable(String)",
	"source_run_id":       "Nullable(String)",
	"source_record_id":    "Nullable(String)",
	"business_id":         "Nullable(String)",
	"source_line_number":  "Nullable(Int64)",
	"source_payload_hash": "Nullable(String)",
	"source_item_hash":    "Nullable(String)",
	"source_position":     "Nullable(Int32)",
	"ingested_at":         "DateTime64(3, 'UTC')",
	"source_export_id":    "UUID",
}

var identifierColumnTypes = columnTypes(commonColumnTypes, map[string]string{
	"identifier_scope":       "Nullable(String)",
	"identifier_type":        "Nullable(String)",
	"identifier_value":       "Nullable(String)",
	"registered_on":          "Nullable(String)",
	"ended_on":               "Nullable(String)",
	"source":                 "Nullable(String)",
	"is_primary_business_id": "Nullable(Bool)",
})

var statusColumnTypes = columnTypes(commonColumnTypes, map[string]string{
	"trade_register_status": "Nullable(String)",
	"status":                "Nullable(String)",
	"registration_date":     "Nullable(String)",
	"end_date":              "Nullable(String)",
	"last_modified":         "Nullable(String)",
	"lifecycle_status":      "Nullable(String)",
	"is_active":             "Nullable(Bool)",
})

var nameColumnTypes = columnTypes(commonColumnTypes, map[string]string{
	"name":           "Nullable(String)",
	"name_type_code": "Nullable(String)",
	"version":        "Nullable(Int32)",
	"registered_on":  "Nullable(String)",
	"ended_on":       "Nullable(String)",
	"is_current":     "Nullable(Bool)",
	"is_primary":     "Nullable(Bool)",
})

var businessLineColumnTypes = columnTypes(commonColumnTypes, map[string]string{
	"business_line_type":     "Nullable(String)",
	"business_line_code_set": "Nullable(String)",
	"registered_on":          "Nullable(String)",
	"source":                 "Nullable(String)",
	"is_primary":             "Nullable(Bool)",
})

var businessLineDescriptionColumnTypes = columnTypes(commonColumnTypes, map[string]string{
	"business_line_item_hash": "Nullable(String)",
	"language_code":           "Nullable(String)",
	"description":             "Nullable(String)",
})

var websiteColumnTypes = columnTypes(commonColumnTypes, map[string]string{
	"url":            "Nullable(String)",
	"normalized_url": "Nullable(String)",
	"host":           "Nullable(String)",
	"path":           "Nullable(String)",
	"registered_on":  "Nullable(String)",
	"ended_on":       "Nullable(String)",
	"is_current":     "Nullable(Bool)",
	"is_primary":     "Nullable(Bool)",
})

var companyFormColumnTypes = columnTypes(commonColumnTypes, map[string]string{
	"form_type_code": "Nullable(String)",
	"version":        "Nullable(Int32)",
	"registered_on":  "Nullable(String)",
	"ended_on":       "Nullable(String)",
	"source":         "Nullable(String)",
	"is_current":     "Nullable(Bool)",
})

var companyFormDescriptionColumnTypes = columnTypes(commonColumnTypes, map[string]string{
	"company_form_item_hash": "Nullable(String)",
	"language_code":          "Nullable(String)",
	"description":            "Nullable(String)",
})

var companySituationColumnTypes = columnTypes(commonColumnTypes, map[string]string{
	"situation_type_code": "Nullable(String)",
	"registered_on":       "Nullable(String)",
	"ended_on":            "Nullable(String)",
	"is_current":          "Nullable(Bool)",
})

var companySituationDescriptionColumnTypes = columnTypes(commonColumnTypes, map[string]string{
	"company_situation_item_hash": "Nullable(String)",
	"language_code":               "Nullable(String)",
	"description":                 "Nullable(String)",
})

var registeredEntryColumnTypes = columnTypes(commonColumnTypes, map[string]string{
	"entry_type_code": "Nullable(String)",
	"register_code":   "Nullable(String)",
	"authority":       "Nullable(String)",
	"registered_on":   "Nullable(String)",
	"ended_on":        "Nullable(String)",
	"is_current":      "Nullable(Bool)",
})

var registeredEntryDescriptionColumnTypes = columnTypes(commonColumnTypes, map[string]string{
	"registered_entry_item_hash": "Nullable(String)",
	"language_code":              "Nullable(String)",
	"description":                "Nullable(String)",
})

var addressColumnTypes = columnTypes(commonColumnTypes, map[string]string{
	"address_type_code": "Nullable(Int32)",
	"street":            "Nullable(String)",
	"post_code":         "Nullable(String)",
	"building_number":   "Nullable(String)",
	"entrance":          "Nullable(String)",
	"apartment_number":  "Nullable(String)",
	"post_office_box":   "Nullable(String)",
	"co":                "Nullable(String)",
	"country":           "Nullable(String)",
	"registered_on":     "Nullable(String)",
	"source":            "Nullable(String)",
})

var addressPostOfficeColumnTypes = columnTypes(commonColumnTypes, map[string]string{
	"address_item_hash": "Nullable(String)",
	"language_code":     "Nullable(String)",
	"city":              "Nullable(String)",
	"municipality_code": "Nullable(String)",
})

func NormalizedTables() []NormalizedTable {
	return []NormalizedTable{
		{Name: identifiersTable, Columns: identifierColumns, ColumnTypes: filterColumnTypes(identifierColumns, identifierColumnTypes)},
		{Name: statusesTable, Columns: statusColumns, ColumnTypes: filterColumnTypes(statusColumns, statusColumnTypes)},
		{Name: namesTable, Columns: nameColumns, ColumnTypes: filterColumnTypes(nameColumns, nameColumnTypes)},
		{Name: businessLinesTable, Columns: businessLineColumns, ColumnTypes: filterColumnTypes(businessLineColumns, businessLineColumnTypes)},
		{Name: businessLineDescriptionsTable, Columns: businessLineDescriptionColumns, ColumnTypes: filterColumnTypes(businessLineDescriptionColumns, businessLineDescriptionColumnTypes)},
		{Name: websitesTable, Columns: websiteColumns, ColumnTypes: filterColumnTypes(websiteColumns, websiteColumnTypes)},
		{Name: companyFormsTable, Columns: companyFormColumns, ColumnTypes: filterColumnTypes(companyFormColumns, companyFormColumnTypes)},
		{Name: companyFormDescriptionsTable, Columns: companyFormDescriptionColumns, ColumnTypes: filterColumnTypes(companyFormDescriptionColumns, companyFormDescriptionColumnTypes)},
		{Name: companySituationsTable, Columns: companySituationColumns, ColumnTypes: filterColumnTypes(companySituationColumns, companySituationColumnTypes)},
		{Name: companySituationDescriptionsTable, Columns: companySituationDescriptionColumns, ColumnTypes: filterColumnTypes(companySituationDescriptionColumns, companySituationDescriptionColumnTypes)},
		{Name: registeredEntriesTable, Columns: registeredEntryColumns, ColumnTypes: filterColumnTypes(registeredEntryColumns, registeredEntryColumnTypes)},
		{Name: registeredEntryDescriptionsTable, Columns: registeredEntryDescriptionColumns, ColumnTypes: filterColumnTypes(registeredEntryDescriptionColumns, registeredEntryDescriptionColumnTypes)},
		{Name: addressesTable, Columns: addressColumns, ColumnTypes: filterColumnTypes(addressColumns, addressColumnTypes)},
		{Name: addressPostOfficesTable, Columns: addressPostOfficeColumns, ColumnTypes: filterColumnTypes(addressPostOfficeColumns, addressPostOfficeColumnTypes)},
	}
}

func NormalizedTableNames() []string {
	tables := NormalizedTables()
	names := make([]string, 0, len(tables))
	for _, table := range tables {
		names = append(names, table.Name)
	}
	return names
}

type Lineage struct {
	CountryISO2       string
	SourceSlug        string
	SourceRunID       string
	SourceRecordID    string
	BusinessID        string
	SourceLineNumber  int64
	SourcePayloadHash string
	IngestedAt        time.Time
	SourceExportID    uuid.UUID
}

type ItemLineage struct {
	Lineage
	SourceItemHash string
	SourcePosition int32
}

type IdentifierRow struct {
	ItemLineage
	IdentifierScope     string
	IdentifierType      string
	IdentifierValue     string
	RegisteredOn        string
	EndedOn             string
	Source              string
	IsPrimaryBusinessID bool
}

type StatusRow struct {
	Lineage
	TradeRegisterStatus string
	Status              string
	RegistrationDate    string
	EndDate             string
	LastModified        string
	LifecycleStatus     string
	IsActive            bool
}

type NameRow struct {
	ItemLineage
	Name         string
	NameTypeCode string
	Version      int32
	RegisteredOn string
	EndedOn      string
	IsCurrent    bool
	IsPrimary    bool
}

type BusinessLineRow struct {
	Lineage
	SourceItemHash      string
	BusinessLineType    string
	BusinessLineCodeSet string
	RegisteredOn        string
	Source              string
	IsPrimary           bool
}

type BusinessLineDescriptionRow struct {
	ItemLineage
	BusinessLineItemHash string
	LanguageCode         string
	Description          string
}

type WebsiteRow struct {
	Lineage
	SourceItemHash string
	URL            string
	NormalizedURL  string
	Host           string
	Path           string
	RegisteredOn   string
	EndedOn        string
	IsCurrent      bool
	IsPrimary      bool
}

type CompanyFormRow struct {
	ItemLineage
	FormTypeCode string
	Version      int32
	RegisteredOn string
	EndedOn      string
	Source       string
	IsCurrent    bool
}

type CompanyFormDescriptionRow struct {
	ItemLineage
	CompanyFormItemHash string
	LanguageCode        string
	Description         string
}

type CompanySituationRow struct {
	ItemLineage
	SituationTypeCode string
	RegisteredOn      string
	EndedOn           string
	IsCurrent         bool
}

type CompanySituationDescriptionRow struct {
	ItemLineage
	CompanySituationItemHash string
	LanguageCode             string
	Description              string
}

type RegisteredEntryRow struct {
	ItemLineage
	EntryTypeCode string
	RegisterCode  string
	Authority     string
	RegisteredOn  string
	EndedOn       string
	IsCurrent     bool
}

type RegisteredEntryDescriptionRow struct {
	ItemLineage
	RegisteredEntryItemHash string
	LanguageCode            string
	Description             string
}

type AddressRow struct {
	ItemLineage
	AddressTypeCode int32
	Street          string
	PostCode        string
	BuildingNumber  string
	Entrance        string
	ApartmentNumber string
	PostOfficeBox   string
	CO              string
	Country         string
	RegisteredOn    string
	Source          string
}

type AddressPostOfficeRow struct {
	ItemLineage
	AddressItemHash  string
	LanguageCode     string
	City             string
	MunicipalityCode string
}

func (r IdentifierRow) ClickHouseRow() map[string]any {
	row := itemLineageRow(r.ItemLineage)
	row["identifier_scope"] = r.IdentifierScope
	row["identifier_type"] = r.IdentifierType
	row["identifier_value"] = r.IdentifierValue
	row["registered_on"] = r.RegisteredOn
	row["ended_on"] = r.EndedOn
	row["source"] = r.Source
	row["is_primary_business_id"] = r.IsPrimaryBusinessID
	return row
}

func (r StatusRow) ClickHouseRow() map[string]any {
	row := lineageRow(r.Lineage)
	row["trade_register_status"] = r.TradeRegisterStatus
	row["status"] = r.Status
	row["registration_date"] = r.RegistrationDate
	row["end_date"] = r.EndDate
	row["last_modified"] = r.LastModified
	row["lifecycle_status"] = r.LifecycleStatus
	row["is_active"] = r.IsActive
	return row
}

func (r NameRow) ClickHouseRow() map[string]any {
	row := itemLineageRow(r.ItemLineage)
	row["name"] = r.Name
	row["name_type_code"] = r.NameTypeCode
	row["version"] = r.Version
	row["registered_on"] = r.RegisteredOn
	row["ended_on"] = r.EndedOn
	row["is_current"] = r.IsCurrent
	row["is_primary"] = r.IsPrimary
	return row
}

func (r BusinessLineRow) ClickHouseRow() map[string]any {
	row := lineageRow(r.Lineage)
	row["source_item_hash"] = r.SourceItemHash
	row["business_line_type"] = r.BusinessLineType
	row["business_line_code_set"] = r.BusinessLineCodeSet
	row["registered_on"] = r.RegisteredOn
	row["source"] = r.Source
	row["is_primary"] = r.IsPrimary
	return row
}

func (r BusinessLineDescriptionRow) ClickHouseRow() map[string]any {
	row := itemLineageRow(r.ItemLineage)
	row["business_line_item_hash"] = r.BusinessLineItemHash
	row["language_code"] = r.LanguageCode
	row["description"] = r.Description
	return row
}

func (r WebsiteRow) ClickHouseRow() map[string]any {
	row := lineageRow(r.Lineage)
	row["source_item_hash"] = r.SourceItemHash
	row["url"] = r.URL
	row["normalized_url"] = r.NormalizedURL
	row["host"] = r.Host
	row["path"] = r.Path
	row["registered_on"] = r.RegisteredOn
	row["ended_on"] = r.EndedOn
	row["is_current"] = r.IsCurrent
	row["is_primary"] = r.IsPrimary
	return row
}

func (r CompanyFormRow) ClickHouseRow() map[string]any {
	row := itemLineageRow(r.ItemLineage)
	row["form_type_code"] = r.FormTypeCode
	row["version"] = r.Version
	row["registered_on"] = r.RegisteredOn
	row["ended_on"] = r.EndedOn
	row["source"] = r.Source
	row["is_current"] = r.IsCurrent
	return row
}

func (r CompanyFormDescriptionRow) ClickHouseRow() map[string]any {
	row := itemLineageRow(r.ItemLineage)
	row["company_form_item_hash"] = r.CompanyFormItemHash
	row["language_code"] = r.LanguageCode
	row["description"] = r.Description
	return row
}

func (r CompanySituationRow) ClickHouseRow() map[string]any {
	row := itemLineageRow(r.ItemLineage)
	row["situation_type_code"] = r.SituationTypeCode
	row["registered_on"] = r.RegisteredOn
	row["ended_on"] = r.EndedOn
	row["is_current"] = r.IsCurrent
	return row
}

func (r CompanySituationDescriptionRow) ClickHouseRow() map[string]any {
	row := itemLineageRow(r.ItemLineage)
	row["company_situation_item_hash"] = r.CompanySituationItemHash
	row["language_code"] = r.LanguageCode
	row["description"] = r.Description
	return row
}

func (r RegisteredEntryRow) ClickHouseRow() map[string]any {
	row := itemLineageRow(r.ItemLineage)
	row["entry_type_code"] = r.EntryTypeCode
	row["register_code"] = r.RegisterCode
	row["authority"] = r.Authority
	row["registered_on"] = r.RegisteredOn
	row["ended_on"] = r.EndedOn
	row["is_current"] = r.IsCurrent
	return row
}

func (r RegisteredEntryDescriptionRow) ClickHouseRow() map[string]any {
	row := itemLineageRow(r.ItemLineage)
	row["registered_entry_item_hash"] = r.RegisteredEntryItemHash
	row["language_code"] = r.LanguageCode
	row["description"] = r.Description
	return row
}

func (r AddressRow) ClickHouseRow() map[string]any {
	row := itemLineageRow(r.ItemLineage)
	row["address_type_code"] = r.AddressTypeCode
	row["street"] = r.Street
	row["post_code"] = r.PostCode
	row["building_number"] = r.BuildingNumber
	row["entrance"] = r.Entrance
	row["apartment_number"] = r.ApartmentNumber
	row["post_office_box"] = r.PostOfficeBox
	row["co"] = r.CO
	row["country"] = r.Country
	row["registered_on"] = r.RegisteredOn
	row["source"] = r.Source
	return row
}

func (r AddressPostOfficeRow) ClickHouseRow() map[string]any {
	row := itemLineageRow(r.ItemLineage)
	row["address_item_hash"] = r.AddressItemHash
	row["language_code"] = r.LanguageCode
	row["city"] = r.City
	row["municipality_code"] = r.MunicipalityCode
	return row
}

func lineageRow(r Lineage) map[string]any {
	return map[string]any{
		"country_iso2":        r.CountryISO2,
		"source_slug":         r.SourceSlug,
		"source_run_id":       r.SourceRunID,
		"source_record_id":    r.SourceRecordID,
		"business_id":         r.BusinessID,
		"source_line_number":  r.SourceLineNumber,
		"source_payload_hash": r.SourcePayloadHash,
		"ingested_at":         r.IngestedAt,
		"source_export_id":    r.SourceExportID,
	}
}

func itemLineageRow(r ItemLineage) map[string]any {
	row := lineageRow(r.Lineage)
	row["source_item_hash"] = r.SourceItemHash
	row["source_position"] = r.SourcePosition
	return row
}

func columnTypes(base map[string]string, extra map[string]string) map[string]string {
	merged := make(map[string]string, len(base)+len(extra))
	for key, value := range base {
		merged[key] = value
	}
	for key, value := range extra {
		merged[key] = value
	}
	return merged
}

func filterColumnTypes(columns []string, types map[string]string) map[string]string {
	filtered := make(map[string]string, len(columns))
	for _, column := range columns {
		filtered[column] = types[column]
	}
	return filtered
}
