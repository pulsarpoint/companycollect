package irseobmf

import "testing"

func TestProjectExportRowsFullNonprofit(t *testing.T) {
	record := IrsEoBmfRecord{
		EIN:          "010011694",
		Name:         "MASSACHUSETTS MODERATORS ASSOCIATION INC",
		InCareOf:     "% JOHN FALLON",
		Street:       "PO BOX 1281",
		City:         "HAVERHILL",
		State:        "MA",
		Zip:          "01831-1781",
		Group:        "0000",
		Subsection:   "03",
		Organization: "5",
		Foundation:   "16",
		Status:       "01",
		TaxPeriod:    "202509",
		AssetAmt:     "65979",
		IncomeAmt:    "55534",
		RevenueAmt:   "41497",
		NTEECD:       "S19",
		PayloadHash:  "payload-hash",
		RawPayload:   []byte(`{"EIN":"010011694"}`),
	}

	rows := ProjectExportRows(record, "run-1")

	if len(rows.Companies) != 1 {
		t.Fatalf("companies len = %d, want 1", len(rows.Companies))
	}
	company := rows.Companies[0]
	if company.CountryISO2 != "US" || company.SourceSlug != SourceSlug || company.SourceRunID != "run-1" {
		t.Fatalf("company lineage = %#v", company)
	}
	if company.EIN != "010011694" || company.SourceNativeID != "010011694" {
		t.Fatalf("company ids = %#v", company)
	}
	if !company.IsActiveExempt || !company.IsNonprofit {
		t.Fatalf("company flags = %#v", company)
	}
	if company.LegalNameNormalized != "massachusetts moderators association inc" {
		t.Fatalf("normalized name = %q", company.LegalNameNormalized)
	}

	if len(rows.CompanyNames) != 1 || !rows.CompanyNames[0].IsPrimary {
		t.Fatalf("company names = %#v, want one primary legal row", rows.CompanyNames)
	}
	if len(rows.Addresses) != 1 || rows.Addresses[0].AddressRole != "irs_mailing" {
		t.Fatalf("addresses = %#v", rows.Addresses)
	}
	if len(rows.Classifications) != 1 || rows.Classifications[0].SubsectionCode != "03" {
		t.Fatalf("classifications = %#v", rows.Classifications)
	}
	if len(rows.Financials) != 1 {
		t.Fatalf("financials len = %d, want 1", len(rows.Financials))
	}
	fin := rows.Financials[0]
	if !fin.AssetAmountPresent || fin.AssetAmount != 65979 || fin.TaxPeriod != "202509" {
		t.Fatalf("financials = %#v", fin)
	}
	if len(rows.Identifiers) != 1 || rows.Identifiers[0].IdentifierType != "ein" || !rows.Identifiers[0].IsPrimary {
		t.Fatalf("identifiers = %#v, want single primary EIN", rows.Identifiers)
	}
	if len(rows.SourceEvidence) != 1 || rows.SourceEvidence[0].SourcePayloadHash != "payload-hash" {
		t.Fatalf("source evidence = %#v", rows.SourceEvidence)
	}
}

func TestProjectExportRowsSparseAndGroupMember(t *testing.T) {
	// 990-N style: blank financials, blank NTEE -> no financials row.
	sparse := IrsEoBmfRecord{EIN: "010018605", Name: "SAMPLE SMALL CHARITY INC", Status: "01"}
	rows := ProjectExportRows(sparse, "run-1")
	if len(rows.Financials) != 0 {
		t.Fatalf("financials len = %d, want 0 for blank financials and tax period", len(rows.Financials))
	}
	if len(rows.Addresses) != 0 {
		t.Fatalf("addresses len = %d, want 0 for blank address", len(rows.Addresses))
	}
	if len(rows.Identifiers) != 1 {
		t.Fatalf("identifiers len = %d, want 1 (EIN only, no group)", len(rows.Identifiers))
	}

	// Group member with SORT_NAME -> secondary name + group identifier.
	member := IrsEoBmfRecord{
		EIN: "010018830", Name: "AMERICAN LEGION", SortName: "22 DEPT OF MAINE",
		Group: "3125", Subsection: "19", Status: "01", TaxPeriod: "202412", AssetAmt: "408703",
	}
	memberRows := ProjectExportRows(member, "run-1")
	if len(memberRows.CompanyNames) != 2 {
		t.Fatalf("company names len = %d, want 2 (legal + sort)", len(memberRows.CompanyNames))
	}
	if memberRows.CompanyNames[1].NameType != "sort" {
		t.Fatalf("second name type = %q, want sort", memberRows.CompanyNames[1].NameType)
	}
	if len(memberRows.Identifiers) != 2 {
		t.Fatalf("identifiers len = %d, want 2 (EIN + group exemption)", len(memberRows.Identifiers))
	}
	if memberRows.Identifiers[1].IdentifierType != "group_exemption_number" {
		t.Fatalf("second identifier = %#v, want group exemption", memberRows.Identifiers[1])
	}
}
