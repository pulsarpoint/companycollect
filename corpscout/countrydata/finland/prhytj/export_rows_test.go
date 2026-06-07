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
