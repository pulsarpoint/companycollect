package prhytj

import (
	"encoding/json"
	"os"
	"testing"
)

func TestPageDecodeAndProfileMappingUsesFinlandRules(t *testing.T) {
	page := decodeTestPage(t, "testdata/prh_page_1.json")

	if page.TotalResults == nil || *page.TotalResults != 3 {
		t.Fatalf("totalResults = %#v, want 3", page.TotalResults)
	}
	if len(page.Companies) != 2 {
		t.Fatalf("companies length = %d, want 2", len(page.Companies))
	}
	if page.Companies[0].BusinessID.Value != "0100130-4" {
		t.Fatalf("source BusinessID.Value = %q, want %q", page.Companies[0].BusinessID.Value, "0100130-4")
	}
	if page.Companies[0].EUID == nil {
		t.Fatal("source EUID = nil, want identifier")
	}
	if page.Companies[0].EUID.Value != "FIFPRO.0100130-4" {
		t.Fatalf("source EUID.Value = %q, want %q", page.Companies[0].EUID.Value, "FIFPRO.0100130-4")
	}
	if page.Companies[0].Names[0].Version != 1 {
		t.Fatalf("source Names[0].Version = %d, want 1", page.Companies[0].Names[0].Version)
	}
	if page.Companies[0].CompanyForms[0].Version != 1 {
		t.Fatalf("source CompanyForms[0].Version = %d, want 1", page.Companies[0].CompanyForms[0].Version)
	}

	profile := page.Companies[0].ToProfile()

	if profile.BusinessID != "0100130-4" {
		t.Fatalf("BusinessID = %q, want %q", profile.BusinessID, "0100130-4")
	}
	if profile.LegalName != "Dynava Oy" {
		t.Fatalf("LegalName = %q, want %q", profile.LegalName, "Dynava Oy")
	}
	if profile.EUID != "FIFPRO.0100130-4" {
		t.Fatalf("EUID = %q, want %q", profile.EUID, "FIFPRO.0100130-4")
	}
	if !profile.IsActive {
		t.Fatal("IsActive = false, want true")
	}
	if profile.VATID != "FI01001304" {
		t.Fatalf("VATID = %q, want %q", profile.VATID, "FI01001304")
	}
	if !profile.TaxRegistrations.VAT.Registered {
		t.Fatal("VAT.Registered = false, want true")
	}
	if !profile.TaxRegistrations.Employer.Registered {
		t.Fatal("Employer.Registered = false, want true")
	}
	if !profile.TaxRegistrations.Prepayment.Registered {
		t.Fatal("Prepayment.Registered = false, want true")
	}
	if profile.Website != "https://www.dynava.fi" {
		t.Fatalf("Website = %q, want %q", profile.Website, "https://www.dynava.fi")
	}
	if profile.MainBusinessLineCode != "62010" {
		t.Fatalf("MainBusinessLineCode = %q, want %q", profile.MainBusinessLineCode, "62010")
	}
	if len(profile.Addresses) != 1 {
		t.Fatalf("Addresses length = %d, want 1", len(profile.Addresses))
	}
	if profile.Addresses[0].City != "HELSINKI" {
		t.Fatalf("Addresses[0].City = %q, want %q", profile.Addresses[0].City, "HELSINKI")
	}
	if profile.Addresses[0].CitySV != "HELSINGFORS" {
		t.Fatalf("Addresses[0].CitySV = %q, want %q", profile.Addresses[0].CitySV, "HELSINGFORS")
	}
}

func TestCeasedCompanyIsNotActiveAndEndedVATIsNotRegistered(t *testing.T) {
	page := decodeTestPage(t, "testdata/prh_page_1.json")
	if len(page.Companies) < 2 {
		t.Fatalf("companies length = %d, want at least 2", len(page.Companies))
	}

	profile := page.Companies[1].ToProfile()

	if profile.IsActive {
		t.Fatal("IsActive = true, want false")
	}
	if profile.TaxRegistrations.VAT.Registered {
		t.Fatal("VAT.Registered = true, want false")
	}
}

func TestMultipleCurrentPrimaryNamesPickLatestRegistrationDate(t *testing.T) {
	page := decodeTestPage(t, "testdata/prh_page_2.json")
	if len(page.Companies) != 1 {
		t.Fatalf("companies length = %d, want 1", len(page.Companies))
	}

	profile := page.Companies[0].ToProfile()

	if profile.LegalName != "Newest Primary Oy" {
		t.Fatalf("LegalName = %q, want %q", profile.LegalName, "Newest Primary Oy")
	}
}

func decodeTestPage(t *testing.T, path string) Page {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}

	var page Page
	if err := json.Unmarshal(data, &page); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}

	return page
}
