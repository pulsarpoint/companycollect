package prhytj

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
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
	if company.PrimaryIndustryCode != "62010" || company.PrimaryIndustryLabelEn != "Computer programming activities" {
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
	if vatTaxRow.RegisterCode != "6" || !vatTaxRow.CurrentRegistered || vatTaxRow.FirstRegisteredOn != "1995-01-01" {
		t.Fatalf("VAT tax registration row = %#v", *vatTaxRow)
	}
	if vatTaxRow.SourceItemHash == "" {
		t.Fatalf("VAT tax registration source item hash is empty")
	}
	firstAddress := rows.Addresses[0]
	if firstAddress.BuildingNumber != "1" || firstAddress.Country != "FI" || firstAddress.CityFi != "HELSINKI" || firstAddress.CitySv != "HELSINGFORS" || firstAddress.MunicipalityCode != "091" {
		t.Fatalf("first address = %#v", firstAddress)
	}
	if firstAddress.SourceItemHash == "" {
		t.Fatalf("first address source item hash is empty")
	}
	if len(rows.RegisteredEntries) == 0 {
		t.Fatalf("registered entries missing")
	}
	registeredEntry := rows.RegisteredEntries[0]
	if registeredEntry.SourceItemHash == "" {
		t.Fatalf("registered entry source item hash is empty")
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
	if repeatedVATTaxRow == nil || len(repeatedRows.RegisteredEntries) == 0 {
		t.Fatalf("repeated projection missing tested rows")
	}
	if repeatedVATTaxRow.SourceItemHash != vatTaxRow.SourceItemHash ||
		repeatedRows.Addresses[0].SourceItemHash != firstAddress.SourceItemHash ||
		repeatedRows.RegisteredEntries[0].SourceItemHash != registeredEntry.SourceItemHash ||
		repeatedRows.Websites[0].SourceItemHash != website.SourceItemHash {
		t.Fatalf("source item hashes changed between repeated projections")
	}
}

func TestProjectRawRecordExportRowPreservesPayload(t *testing.T) {
	raw := []byte(`{"businessId":{"value":"1234567-8"},"extra":{"field":"kept"}}`)
	record := CompanyRecord{
		BusinessID:  Identifier{Value: "1234567-8"},
		RawPayload:  raw,
		PayloadHash: "hash",
	}

	row := ProjectRawRecordExportRow(record, "run-1", "/snap.ndjson", "snapshot-hash", 42, "2026-06-08T00:00:00Z")

	if row.CountryISO2 != "FI" {
		t.Fatalf("country ISO2 = %q, want FI", row.CountryISO2)
	}
	if row.SourceSlug != SourceSlug {
		t.Fatalf("source slug = %q, want %q", row.SourceSlug, SourceSlug)
	}
	if row.SourceRunID != "run-1" {
		t.Fatalf("source run ID = %q, want run-1", row.SourceRunID)
	}
	if row.SourceRecordID != "1234567-8" || row.BusinessID != "1234567-8" {
		t.Fatalf("record IDs = source %q business %q, want 1234567-8", row.SourceRecordID, row.BusinessID)
	}
	if row.SourcePayloadHash != "hash" {
		t.Fatalf("source payload hash = %q, want hash", row.SourcePayloadHash)
	}
	if row.SnapshotPath != "/snap.ndjson" {
		t.Fatalf("snapshot path = %q, want /snap.ndjson", row.SnapshotPath)
	}
	if row.SnapshotSHA256 != "snapshot-hash" {
		t.Fatalf("snapshot sha = %q, want snapshot-hash", row.SnapshotSHA256)
	}
	if row.SnapshotLineNumber != 42 {
		t.Fatalf("snapshot line number = %d, want 42", row.SnapshotLineNumber)
	}
	if row.RawPayloadJSON != string(raw) {
		t.Fatalf("raw payload = %q, want exact payload %q", row.RawPayloadJSON, string(raw))
	}
	if row.SchemaVersion != SourceExportSchemaVersion {
		t.Fatalf("schema version = %q, want %q", row.SchemaVersion, SourceExportSchemaVersion)
	}
	if row.ExportedAt != "2026-06-08T00:00:00Z" {
		t.Fatalf("exported at = %q, want 2026-06-08T00:00:00Z", row.ExportedAt)
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
	payload, err := os.ReadFile(filepath.Join("testdata", "prh_page_1.json"))
	if err != nil {
		t.Fatalf("read analysis sample: %v", err)
	}
	var page downloadEnvelope
	if err := json.Unmarshal(payload, &page); err != nil {
		t.Fatalf("decode sample: %v", err)
	}
	if len(page.Companies) == 0 {
		t.Fatalf("sample page has no companies")
	}
	raw := []byte(page.Companies[0])
	var record CompanyRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		t.Fatalf("decode sample company: %v", err)
	}
	record.RawPayload = raw
	sum := sha256.Sum256(raw)
	record.PayloadHash = hex.EncodeToString(sum[:])
	return record
}
