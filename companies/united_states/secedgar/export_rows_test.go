package secedgar

import "testing"

func TestProjectExportRowsNormalizesSECCompany(t *testing.T) {
	record := CompanyTickerRecord{
		SourceIndex: 1,
		CIK:         320193,
		CIKString:   "320193",
		CIK10:       "0000320193",
		Ticker:      "AAPL",
		Title:       "Apple   Inc.",
		PayloadHash: "payload-hash",
	}

	rows := ProjectExportRows(record, "run-1")
	if len(rows.Companies) != 1 {
		t.Fatalf("companies len = %d, want 1", len(rows.Companies))
	}
	company := rows.Companies[0]
	if company.CountryISO2 != "US" || company.SourceSlug != SourceSlug || company.SourceRunID != "run-1" {
		t.Fatalf("company lineage = %#v", company)
	}
	if company.SourceRecordID != "1" || company.SourceNativeID != "0000320193" {
		t.Fatalf("source ids = %#v", company)
	}
	if company.CIK != 320193 || company.CIK10 != "0000320193" || company.Ticker != "AAPL" {
		t.Fatalf("company identifiers = %#v", company)
	}
	if company.LegalName != "Apple   Inc." || company.LegalNameNormalized != "apple inc." {
		t.Fatalf("company names = %#v", company)
	}
	if len(rows.CompanyNames) != 1 || !rows.CompanyNames[0].IsPrimary {
		t.Fatalf("company names = %#v, want one primary row", rows.CompanyNames)
	}
	if len(rows.Identifiers) != 2 {
		t.Fatalf("identifiers len = %d, want 2", len(rows.Identifiers))
	}
	if rows.Identifiers[0].IdentifierType != "cik10" || rows.Identifiers[1].IdentifierType != "ticker" {
		t.Fatalf("identifiers = %#v", rows.Identifiers)
	}
	if len(rows.SourceEvidence) != 1 || rows.SourceEvidence[0].SourcePayloadHash != "payload-hash" {
		t.Fatalf("source evidence = %#v", rows.SourceEvidence)
	}
}
