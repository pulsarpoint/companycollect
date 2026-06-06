package irseobmf

import "testing"

func TestActiveNonprofitWithFinancialsMapsToProfile(t *testing.T) {
	record := IrsEoBmfRecord{
		EIN:          "010011694",
		Name:         "MASSACHUSETTS MODERATORS ASSOCIATION INC",
		ICO:          "% JOHN FALLON",
		Street:       "PO BOX 1281",
		City:         "HAVERHILL",
		State:        "MA",
		ZIP:          "01831-1781",
		Group:        "0000",
		Subsection:   "03",
		Ruling:       "202109",
		Foundation:   "16",
		Organization: "5",
		Status:       "01",
		TaxPeriod:    "202509",
		AssetAmt:     "65979",
		IncomeAmt:    "55534",
		RevenueAmt:   "41497",
		NteeCd:       "S19",
	}

	profile := record.ToProfile()

	if profile.EIN != "010011694" {
		t.Fatalf("EIN = %q, want %q", profile.EIN, "010011694")
	}
	if profile.PrimaryID != "EIN010011694" {
		t.Fatalf("PrimaryID = %q, want %q", profile.PrimaryID, "EIN010011694")
	}
	if profile.LegalName != "MASSACHUSETTS MODERATORS ASSOCIATION INC" {
		t.Fatalf("LegalName = %q", profile.LegalName)
	}
	if !profile.IsNonprofit || !profile.IsExemptStatusActive {
		t.Fatalf("IsNonprofit=%v IsExemptStatusActive=%v, want both true", profile.IsNonprofit, profile.IsExemptStatusActive)
	}
	if profile.NTEECode != "S19" {
		t.Fatalf("NTEECode = %q, want S19", profile.NTEECode)
	}
	if profile.IRSRulingDate != "202109" {
		t.Fatalf("IRSRulingDate = %q, want 202109", profile.IRSRulingDate)
	}
	if profile.Address.City != "HAVERHILL" || profile.Address.ZIP != "01831-1781" {
		t.Fatalf("Address = %#v", profile.Address)
	}
	if profile.Financials.AssetAmount == nil || *profile.Financials.AssetAmount != 65979 {
		t.Fatalf("AssetAmount = %v, want 65979", profile.Financials.AssetAmount)
	}
	if profile.Financials.TaxPeriod != "202509" {
		t.Fatalf("TaxPeriod = %q, want 202509", profile.Financials.TaxPeriod)
	}
	if len(profile.AlternateNames) != 0 {
		t.Fatalf("AlternateNames = %v, want empty", profile.AlternateNames)
	}
}

func TestBlankFinancialsBecomeNilNotZero(t *testing.T) {
	record := IrsEoBmfRecord{EIN: "010011695", Name: "EXAMPLE SMALL CHARITY", Status: "01", AssetAmt: "", IncomeAmt: "  ", RevenueAmt: ""}

	profile := record.ToProfile()

	if profile.Financials.AssetAmount != nil || profile.Financials.IncomeAmount != nil || profile.Financials.RevenueAmount != nil {
		t.Fatalf("blank amounts should map to nil, got %#v", profile.Financials)
	}
}

func TestGroupMemberSortNameBecomesAlternateName(t *testing.T) {
	record := IrsEoBmfRecord{EIN: "010011697", Name: "NATIONAL ASSOCIATION GROUP", Group: "1234", Status: "01", SortName: "LOCAL CHAPTER 5"}

	profile := record.ToProfile()

	if len(profile.AlternateNames) != 1 || profile.AlternateNames[0] != "LOCAL CHAPTER 5" {
		t.Fatalf("AlternateNames = %v, want [LOCAL CHAPTER 5]", profile.AlternateNames)
	}
}

func TestInactiveExemptStatusIsNotActive(t *testing.T) {
	record := IrsEoBmfRecord{EIN: "010011699", Name: "REVOKED ORG", Status: "20"}

	if record.ToProfile().IsExemptStatusActive {
		t.Fatal("IsExemptStatusActive = true, want false for STATUS != 01")
	}
}

func TestNormalizeEINPadsAndGuards(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{in: "010011694", want: "010011694"},
		{in: "11694", want: "000011694"},
		{in: "10-0011694", want: "100011694"},
		{in: "", want: ""},
	}
	for _, test := range tests {
		if got := normalizeEIN(test.in); got != test.want {
			t.Fatalf("normalizeEIN(%q) = %q, want %q", test.in, got, test.want)
		}
	}
}
