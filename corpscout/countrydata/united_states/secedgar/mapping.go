package secedgar

import (
	"fmt"
	"strings"
)

// CompanyProfile is the derived, Corpscout-friendly view of one SEC ticker
// record. company_tickers.json carries only three fields, so the profile is
// intentionally small; richer attributes (address, SIC, state of incorporation,
// former names, financials) require the data.sec.gov submissions/companyfacts
// APIs and are planning-only per the data-model analysis.
type CompanyProfile struct {
	// CIK is the canonical zero-padded Central Index Key, e.g. "CIK0001045810".
	CIK string
	// CIKNumber is the raw integer CIK as published in the ticker file.
	CIKNumber int64
	// Ticker is the primary stock-exchange symbol; may be empty.
	Ticker string
	// LegalName is the SEC filer name (title); casing is inconsistent upstream.
	LegalName string
	// IsPublicCompany is always true for records present in EDGAR.
	IsPublicCompany bool
	// PrimaryID is the canonical identifier for a public company (the padded CIK).
	PrimaryID string
}

func (record SecTickerRecord) ToProfile() CompanyProfile {
	cik := canonicalCIK(record.CIKStr)
	return CompanyProfile{
		CIK:             cik,
		CIKNumber:       record.CIKStr,
		Ticker:          strings.TrimSpace(record.Ticker),
		LegalName:       strings.TrimSpace(record.Title),
		IsPublicCompany: true,
		PrimaryID:       cik,
	}
}

// canonicalCIK zero-pads the integer CIK to 10 digits and prefixes "CIK", the
// form used by the data.sec.gov endpoints (e.g. CIK0001045810). A non-positive
// CIK yields an empty string.
func canonicalCIK(cik int64) string {
	if cik <= 0 {
		return ""
	}
	return fmt.Sprintf("CIK%010d", cik)
}
