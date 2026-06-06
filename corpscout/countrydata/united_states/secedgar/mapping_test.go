package secedgar

import (
	"os"
	"testing"
)

func TestParseTickerFileOrdersByIndexAndMapsProfile(t *testing.T) {
	records := decodeTestTickerFile(t, "testdata/company_tickers.json")

	if len(records) != 4 {
		t.Fatalf("records length = %d, want 4", len(records))
	}
	// Records are ordered by the numeric index key, so NVIDIA (index "0") is first.
	if records[0].CIKStr != 1045810 || records[0].Title != "NVIDIA CORP" {
		t.Fatalf("records[0] = %#v, want NVIDIA", records[0])
	}

	profile := records[0].ToProfile()
	if profile.CIK != "CIK0001045810" {
		t.Fatalf("CIK = %q, want %q", profile.CIK, "CIK0001045810")
	}
	if profile.PrimaryID != "CIK0001045810" {
		t.Fatalf("PrimaryID = %q, want %q", profile.PrimaryID, "CIK0001045810")
	}
	if profile.CIKNumber != 1045810 {
		t.Fatalf("CIKNumber = %d, want 1045810", profile.CIKNumber)
	}
	if profile.Ticker != "NVDA" {
		t.Fatalf("Ticker = %q, want %q", profile.Ticker, "NVDA")
	}
	if profile.LegalName != "NVIDIA CORP" {
		t.Fatalf("LegalName = %q, want %q", profile.LegalName, "NVIDIA CORP")
	}
	if !profile.IsPublicCompany {
		t.Fatal("IsPublicCompany = false, want true")
	}
}

func TestProfileHandlesMixedCaseAndMissingTicker(t *testing.T) {
	records := decodeTestTickerFile(t, "testdata/company_tickers.json")

	// Index "1" is the mixed-case "Apple Inc." name variant.
	apple := records[1].ToProfile()
	if apple.LegalName != "Apple Inc." {
		t.Fatalf("LegalName = %q, want %q", apple.LegalName, "Apple Inc.")
	}
	if apple.CIK != "CIK0000320193" {
		t.Fatalf("CIK = %q, want %q", apple.CIK, "CIK0000320193")
	}

	// Index "2" is the missing/empty ticker variant.
	alphabet := records[2].ToProfile()
	if alphabet.Ticker != "" {
		t.Fatalf("Ticker = %q, want empty", alphabet.Ticker)
	}
	if alphabet.CIK != "CIK0001652044" {
		t.Fatalf("CIK = %q, want %q", alphabet.CIK, "CIK0001652044")
	}
}

func TestParseTickerFileRejectsNonObject(t *testing.T) {
	for _, body := range []string{`[]`, `null`, `42`, `"x"`} {
		if _, err := ParseTickerFile([]byte(body)); err == nil {
			t.Fatalf("ParseTickerFile(%q) returned nil error, want error", body)
		}
	}
}

func TestCanonicalCIKZeroPadsAndGuardsNonPositive(t *testing.T) {
	if got := canonicalCIK(320193); got != "CIK0000320193" {
		t.Fatalf("canonicalCIK(320193) = %q, want %q", got, "CIK0000320193")
	}
	if got := canonicalCIK(0); got != "" {
		t.Fatalf("canonicalCIK(0) = %q, want empty", got)
	}
}

func decodeTestTickerFile(t *testing.T, path string) []SecTickerRecord {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	records, err := ParseTickerFile(data)
	if err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	return records
}
