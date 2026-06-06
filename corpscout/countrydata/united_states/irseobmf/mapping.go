package irseobmf

import (
	"strconv"
	"strings"
)

// CompanyProfile is the derived, Corpscout-friendly view of one EO BMF row.
// EO BMF describes tax-exempt status and nonprofit financials — not corporate
// standing or incorporation — so the profile deliberately keeps those distinct.
type CompanyProfile struct {
	// EIN is the 9-character zero-padded Employer Identification Number.
	EIN string
	// PrimaryID is the canonical identifier for this source (namespaced EIN).
	PrimaryID string
	// LegalName is NAME (uppercase as published).
	LegalName string
	// AlternateNames carries SORT_NAME (chapter/group member name) when present.
	AlternateNames []string
	// IsNonprofit is always true for records present in EO BMF.
	IsNonprofit bool
	// ExemptStatusCode is the raw STATUS value; IsExemptStatusActive is STATUS=01.
	ExemptStatusCode     string
	IsExemptStatusActive bool
	Subsection           string
	OrganizationCode     string
	FoundationCode       string
	// NTEECode is the preferred activity classification (over legacy ACTIVITY).
	NTEECode string
	Address  ProfileAddress
	// IRSRulingDate is the YYYYMM recognition date — an approximate proxy, NOT an
	// incorporation/formation date.
	IRSRulingDate string
	Financials    NonprofitFinancials
}

type ProfileAddress struct {
	InCareOf string
	Street   string
	City     string
	State    string
	ZIP      string
}

type NonprofitFinancials struct {
	AssetAmount   *int64
	IncomeAmount  *int64
	RevenueAmount *int64
	// TaxPeriod is the YYYYMM period the amounts describe.
	TaxPeriod string
}

func (record IrsEoBmfRecord) ToProfile() CompanyProfile {
	ein := normalizeEIN(record.EIN)
	profile := CompanyProfile{
		EIN:                  ein,
		PrimaryID:            primaryID(ein),
		LegalName:            strings.TrimSpace(record.Name),
		IsNonprofit:          true,
		ExemptStatusCode:     strings.TrimSpace(record.Status),
		IsExemptStatusActive: strings.TrimSpace(record.Status) == "01",
		Subsection:           strings.TrimSpace(record.Subsection),
		OrganizationCode:     strings.TrimSpace(record.Organization),
		FoundationCode:       strings.TrimSpace(record.Foundation),
		NTEECode:             strings.TrimSpace(record.NteeCd),
		Address: ProfileAddress{
			InCareOf: strings.TrimSpace(record.ICO),
			Street:   strings.TrimSpace(record.Street),
			City:     strings.TrimSpace(record.City),
			State:    strings.TrimSpace(record.State),
			ZIP:      strings.TrimSpace(record.ZIP),
		},
		IRSRulingDate: strings.TrimSpace(record.Ruling),
		Financials: NonprofitFinancials{
			AssetAmount:   parseAmount(record.AssetAmt),
			IncomeAmount:  parseAmount(record.IncomeAmt),
			RevenueAmount: parseAmount(record.RevenueAmt),
			TaxPeriod:     strings.TrimSpace(record.TaxPeriod),
		},
	}
	if sortName := strings.TrimSpace(record.SortName); sortName != "" {
		profile.AlternateNames = append(profile.AlternateNames, sortName)
	}
	return profile
}

// normalizeEIN keeps only digits and left-pads to 9 characters, the canonical
// EIN width. An over-length value is returned as-is rather than truncated; an
// empty value yields an empty string.
func normalizeEIN(ein string) string {
	digits := strings.Builder{}
	for _, character := range ein {
		if character >= '0' && character <= '9' {
			digits.WriteRune(character)
		}
	}
	value := digits.String()
	if value == "" {
		return ""
	}
	if len(value) < 9 {
		return strings.Repeat("0", 9-len(value)) + value
	}
	return value
}

func primaryID(ein string) string {
	if ein == "" {
		return ""
	}
	return "EIN" + ein
}

// parseAmount converts a whole-USD amount column to *int64, returning nil for a
// blank or unparseable value so "absent" stays distinct from a real zero.
func parseAmount(raw string) *int64 {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil
	}
	value, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return nil
	}
	return &value
}
