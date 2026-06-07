package prhytj

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"
)

func TestProjectExportRowsFromRealSample(t *testing.T) {
	record := loadAnalysisSampleRecord(t)
	rows := ProjectExportRows(record, "run-1")

	if len(rows.Companies) != 1 {
		t.Fatalf("companies len = %d, want 1", len(rows.Companies))
	}
	company := rows.Companies[0]
	if company.CountryISO2 != "FI" || company.SourceSlug != SourceSlug {
		t.Fatalf("lineage = %#v", company)
	}
	if company.BusinessID != "0100130-4" || company.LegalName != "Dynava Oy" {
		t.Fatalf("company = %#v", company)
	}
	if company.VATID != "FI01001304" {
		t.Fatalf("VATID = %q", company.VATID)
	}
	if company.PrimaryIndustryCode != "82200" || company.PrimaryIndustryLabelEn != "Activities of call centres" {
		t.Fatalf("industry = %#v", company)
	}
	if company.WebsiteNormalizedURL != "https://www.dynava.fi" {
		t.Fatalf("website = %q", company.WebsiteNormalizedURL)
	}
	if len(rows.CompanyNames) != len(record.Names) {
		t.Fatalf("company names = %d, want %d", len(rows.CompanyNames), len(record.Names))
	}
	if len(rows.Addresses) != len(record.Addresses) {
		t.Fatalf("addresses = %d, want %d", len(rows.Addresses), len(record.Addresses))
	}
	if len(rows.TaxRegistrations) != 3 {
		t.Fatalf("tax registrations = %d, want 3", len(rows.TaxRegistrations))
	}
	var vatTaxRow *TaxRegistrationExportRow
	for i := range rows.TaxRegistrations {
		if rows.TaxRegistrations[i].RegistrationType == "VAT" {
			vatTaxRow = &rows.TaxRegistrations[i]
			break
		}
	}
	if vatTaxRow == nil {
		t.Fatalf("VAT tax registration row missing: %#v", rows.TaxRegistrations)
	}
	if vatTaxRow.RegisterCode != "6" || !vatTaxRow.CurrentRegistered || vatTaxRow.FirstRegisteredOn != "1994-06-01" {
		t.Fatalf("VAT tax registration row = %#v", *vatTaxRow)
	}
	if vatTaxRow.SourceItemHash == "" {
		t.Fatalf("VAT tax registration source item hash is empty")
	}
	firstAddress := rows.Addresses[0]
	if firstAddress.BuildingNumber != "17-19" || firstAddress.Country != "" || firstAddress.CityFi != "HELSINKI" || firstAddress.CitySv != "HELSINGFORS" || firstAddress.MunicipalityCode != "091" {
		t.Fatalf("first address = %#v", firstAddress)
	}
	if firstAddress.SourceItemHash == "" {
		t.Fatalf("first address source item hash is empty")
	}
	var authorityRegisteredEntry *RegisteredEntryExportRow
	var registerOneEntry *RegisteredEntryExportRow
	for i := range rows.RegisteredEntries {
		if rows.RegisteredEntries[i].Authority == "1" && rows.RegisteredEntries[i].RegisterCode == "4" {
			authorityRegisteredEntry = &rows.RegisteredEntries[i]
		}
		if rows.RegisteredEntries[i].RegisterCode == "1" {
			registerOneEntry = &rows.RegisteredEntries[i]
		}
	}
	if authorityRegisteredEntry == nil {
		t.Fatalf("registered entry with authority 1 and register 4 missing: %#v", rows.RegisteredEntries)
	}
	if authorityRegisteredEntry.SourceItemHash == "" {
		t.Fatalf("authority registered entry source item hash is empty")
	}
	if registerOneEntry == nil {
		t.Fatalf("registered entry with register 1 missing: %#v", rows.RegisteredEntries)
	}
	if len(rows.Websites) != 1 {
		t.Fatalf("websites = %d, want 1", len(rows.Websites))
	}
	website := rows.Websites[0]
	if website.Host != "www.dynava.fi" || website.Path != "" || !website.IsCurrent || !website.IsPrimary {
		t.Fatalf("website row = %#v", website)
	}
	if website.SourceItemHash == "" {
		t.Fatalf("website source item hash is empty")
	}

	repeatedRows := ProjectExportRows(record, "run-1")
	repeatedVATTaxRow := taxRegistrationRow(repeatedRows.TaxRegistrations, "VAT")
	repeatedAuthorityRegisteredEntry := registeredEntryRow(repeatedRows.RegisteredEntries, "4", "1")
	if repeatedVATTaxRow == nil || repeatedAuthorityRegisteredEntry == nil {
		t.Fatalf("repeated projection missing tested rows")
	}
	if repeatedVATTaxRow.SourceItemHash != vatTaxRow.SourceItemHash ||
		repeatedRows.Addresses[0].SourceItemHash != firstAddress.SourceItemHash ||
		repeatedAuthorityRegisteredEntry.SourceItemHash != authorityRegisteredEntry.SourceItemHash ||
		repeatedRows.Websites[0].SourceItemHash != website.SourceItemHash {
		t.Fatalf("source item hashes changed between repeated projections")
	}
}

func taxRegistrationRow(rows []TaxRegistrationExportRow, registrationType string) *TaxRegistrationExportRow {
	for i := range rows {
		if rows[i].RegistrationType == registrationType {
			return &rows[i]
		}
	}
	return nil
}

func registeredEntryRow(rows []RegisteredEntryExportRow, registerCode string, authority string) *RegisteredEntryExportRow {
	for i := range rows {
		if rows[i].RegisterCode == registerCode && rows[i].Authority == authority {
			return &rows[i]
		}
	}
	return nil
}

func loadAnalysisSampleRecord(t *testing.T) CompanyRecord {
	t.Helper()
	payload, err := os.ReadFile("../../../../companies/analysis/finland/data_model/sources/prh_ytj_v3/sample_record.json")
	if err != nil {
		t.Fatalf("read analysis sample: %v", err)
	}
	var record CompanyRecord
	if err := json.Unmarshal(payload, &record); err != nil {
		t.Fatalf("decode sample: %v", err)
	}
	record.RawPayload = payload
	sum := sha256.Sum256(payload)
	record.PayloadHash = hex.EncodeToString(sum[:])
	return record
}
