package samgoventity

import (
	"encoding/json"
	"os"
	"testing"
)

func loadFixtureRecords(t *testing.T) []SamEntityRecord {
	t.Helper()
	data, err := os.ReadFile("testdata/sam_page.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var envelope struct {
		EntityData []SamEntityRecord `json:"entityData"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	if len(envelope.EntityData) != 2 {
		t.Fatalf("fixture records = %d, want 2", len(envelope.EntityData))
	}
	return envelope.EntityData
}

func TestActiveDomesticEntityMapsToProfile(t *testing.T) {
	profile := loadFixtureRecords(t)[0].ToProfile()

	if profile.UeiSAM != "ABC123DEF456" {
		t.Fatalf("UeiSAM = %q", profile.UeiSAM)
	}
	if profile.PrimaryID != "UEI:ABC123DEF456" {
		t.Fatalf("PrimaryID = %q, want UEI:ABC123DEF456", profile.PrimaryID)
	}
	if profile.CageCode != "1ABC2" {
		t.Fatalf("CageCode = %q, want 1ABC2", profile.CageCode)
	}
	if profile.LegalName != "EXAMPLE ANALYTICS LLC" {
		t.Fatalf("LegalName = %q", profile.LegalName)
	}
	if !profile.IsFederalRegistrant || !profile.IsSamActive {
		t.Fatalf("IsFederalRegistrant=%v IsSamActive=%v, want both true", profile.IsFederalRegistrant, profile.IsSamActive)
	}
	if profile.StateOfIncorporation != "CO" || profile.CountryOfIncorporation != "USA" {
		t.Fatalf("incorporation = %q/%q, want CO/USA", profile.StateOfIncorporation, profile.CountryOfIncorporation)
	}
	if profile.EntityURL != "https://example-analytics.com" {
		t.Fatalf("EntityURL = %q", profile.EntityURL)
	}
	if profile.PhysicalAddress.City != "Denver" || profile.PhysicalAddress.Country != "USA" {
		t.Fatalf("PhysicalAddress = %#v", profile.PhysicalAddress)
	}
	if profile.MailingAddress.Line1 != "PO Box 50" {
		t.Fatalf("MailingAddress = %#v", profile.MailingAddress)
	}
	if profile.PrimaryNaics != "541511" {
		t.Fatalf("PrimaryNaics = %q, want 541511", profile.PrimaryNaics)
	}
	if len(profile.NaicsCodes) != 2 {
		t.Fatalf("NaicsCodes len = %d, want 2", len(profile.NaicsCodes))
	}
	if profile.NaicsCodes[0].Code != "541511" || !profile.NaicsCodes[0].SbaSmallBusiness {
		t.Fatalf("NaicsCodes[0] = %#v, want 541511 small-business", profile.NaicsCodes[0])
	}
	if profile.NaicsCodes[1].SbaSmallBusiness {
		t.Fatalf("NaicsCodes[1].SbaSmallBusiness = true, want false (N)")
	}
	if len(profile.AlternateNames) != 0 {
		t.Fatalf("AlternateNames = %v, want empty", profile.AlternateNames)
	}
}

func TestExpiredForeignEntityMapping(t *testing.T) {
	profile := loadFixtureRecords(t)[1].ToProfile()

	if profile.IsSamActive {
		t.Fatal("IsSamActive = true, want false for Expired")
	}
	if profile.SamRegistrationStatus != "Expired" {
		t.Fatalf("SamRegistrationStatus = %q, want Expired", profile.SamRegistrationStatus)
	}
	if profile.CountryOfIncorporation != "CAN" {
		t.Fatalf("CountryOfIncorporation = %q, want CAN", profile.CountryOfIncorporation)
	}
	if profile.StateOfIncorporation != "" {
		t.Fatalf("StateOfIncorporation = %q, want empty", profile.StateOfIncorporation)
	}
	if len(profile.AlternateNames) != 1 || profile.AlternateNames[0] != "GlobalWidgets" {
		t.Fatalf("AlternateNames = %v, want [GlobalWidgets] (dbaName)", profile.AlternateNames)
	}
	if profile.CageCode != "" {
		t.Fatalf("CageCode = %q, want empty", profile.CageCode)
	}
	if profile.PhysicalAddress.Line2 != "Suite 9" {
		t.Fatalf("PhysicalAddress.Line2 = %q, want Suite 9", profile.PhysicalAddress.Line2)
	}
	if !profile.MailingAddress.IsZero() {
		t.Fatalf("MailingAddress = %#v, want zero (absent in record)", profile.MailingAddress)
	}
}

func TestPrimaryIDEmptyForBlankUEI(t *testing.T) {
	if got := primaryID(""); got != "" {
		t.Fatalf("primaryID(\"\") = %q, want empty", got)
	}
	if got := (SamEntityRecord{}).ToProfile().PrimaryID; got != "" {
		t.Fatalf("empty record PrimaryID = %q, want empty", got)
	}
}
