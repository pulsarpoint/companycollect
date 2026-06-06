package coloradoentities

import (
	"encoding/json"
	"os"
	"testing"
)

func loadFixtureRecords(t *testing.T) []ColoradoEntityRecord {
	t.Helper()
	data, err := os.ReadFile("testdata/co_entities.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var records []ColoradoEntityRecord
	if err := json.Unmarshal(data, &records); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	if len(records) != 4 {
		t.Fatalf("fixture records = %d, want 4", len(records))
	}
	return records
}

func TestGoodStandingDomesticLLCMapsToProfile(t *testing.T) {
	profile := loadFixtureRecords(t)[0].ToProfile()

	if profile.EntityID != "20251665680" {
		t.Fatalf("EntityID = %q", profile.EntityID)
	}
	if profile.PrimaryID != "CO:20251665680" {
		t.Fatalf("PrimaryID = %q, want CO:20251665680", profile.PrimaryID)
	}
	if profile.LegalName != "KYLDERON MIST VALLEY LLC" {
		t.Fatalf("LegalName = %q", profile.LegalName)
	}
	if !profile.IsActive {
		t.Fatal("IsActive = false, want true for Good Standing")
	}
	if profile.IsForeign {
		t.Fatal("IsForeign = true, want false for CO jurisdiction")
	}
	if profile.LegalForm != "Domestic Limited Liability Company" {
		t.Fatalf("LegalForm = %q", profile.LegalForm)
	}
	if profile.FormationDate != "2025-06-16" {
		t.Fatalf("FormationDate = %q, want 2025-06-16", profile.FormationDate)
	}
	if profile.PrincipalAddress.City != "Delta" {
		t.Fatalf("PrincipalAddress.City = %q", profile.PrincipalAddress.City)
	}
	if !profile.MailingAddress.IsZero() {
		t.Fatalf("MailingAddress = %#v, want zero", profile.MailingAddress)
	}
	if profile.RegisteredAgent.IsOrganization || profile.RegisteredAgent.Name != "KEQIANG DENG" {
		t.Fatalf("RegisteredAgent = %#v, want individual KEQIANG DENG", profile.RegisteredAgent)
	}
	if len(profile.AlternateNames) != 0 {
		t.Fatalf("AlternateNames = %v, want empty", profile.AlternateNames)
	}
}

func TestDelinquentNameAnnotationStrippedAndKeptAsAlternate(t *testing.T) {
	profile := loadFixtureRecords(t)[1].ToProfile()

	if profile.LegalName != "SOUTHWEST CONTRACTING, LLC" {
		t.Fatalf("LegalName = %q, want %q", profile.LegalName, "SOUTHWEST CONTRACTING, LLC")
	}
	if len(profile.AlternateNames) != 1 || profile.AlternateNames[0] != "SOUTHWEST CONTRACTING, LLC, Delinquent May 1, 2016" {
		t.Fatalf("AlternateNames = %v, want raw annotated name", profile.AlternateNames)
	}
	if profile.IsActive {
		t.Fatal("IsActive = true, want false for Delinquent")
	}
	if profile.MailingAddress.Line1 != "PO BOX 719" || profile.MailingAddress.City != "CORTEZ" {
		t.Fatalf("MailingAddress = %#v", profile.MailingAddress)
	}
	if profile.RegisteredAgent.IsOrganization || profile.RegisteredAgent.Name != "Steven G Franchini" {
		t.Fatalf("RegisteredAgent = %#v, want individual Steven G Franchini", profile.RegisteredAgent)
	}
}

func TestForeignEntityWithOrganizationAgent(t *testing.T) {
	profile := loadFixtureRecords(t)[2].ToProfile()

	if !profile.IsForeign {
		t.Fatal("IsForeign = false, want true for TX jurisdiction")
	}
	if profile.JurisdictionOfFormation != "TX" {
		t.Fatalf("JurisdictionOfFormation = %q, want TX", profile.JurisdictionOfFormation)
	}
	if profile.LegalForm != "Foreign Profit Corporation" {
		t.Fatalf("LegalForm = %q", profile.LegalForm)
	}
	if !profile.RegisteredAgent.IsOrganization || profile.RegisteredAgent.Name != "C T CORPORATION SYSTEM" {
		t.Fatalf("RegisteredAgent = %#v, want org C T CORPORATION SYSTEM", profile.RegisteredAgent)
	}
}

func TestSparseRecordMapsWithoutPanicking(t *testing.T) {
	profile := loadFixtureRecords(t)[3].ToProfile()

	if profile.LegalName != "The Luxe Lab LLC" || !profile.IsActive {
		t.Fatalf("profile = %#v", profile)
	}
	if !profile.PrincipalAddress.IsZero() || !profile.MailingAddress.IsZero() {
		t.Fatalf("expected zero addresses, got principal=%#v mailing=%#v", profile.PrincipalAddress, profile.MailingAddress)
	}
	if profile.RegisteredAgent.Name != "" {
		t.Fatalf("RegisteredAgent.Name = %q, want empty", profile.RegisteredAgent.Name)
	}
}

func TestDateOnlyAndPrimaryIDHelpers(t *testing.T) {
	if got := dateOnly("1978-02-28T00:00:00.000"); got != "1978-02-28" {
		t.Fatalf("dateOnly = %q, want 1978-02-28", got)
	}
	if got := dateOnly("2025-01-02"); got != "2025-01-02" {
		t.Fatalf("dateOnly = %q, want 2025-01-02", got)
	}
	if got := primaryID(""); got != "" {
		t.Fatalf("primaryID(\"\") = %q, want empty", got)
	}
}
