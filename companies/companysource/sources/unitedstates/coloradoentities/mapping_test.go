package coloradoentities

import "testing"

func TestCleanEntityNameStripsStatusAnnotation(t *testing.T) {
	cases := map[string]string{
		"SOUTHWEST CONTRACTING, LLC, Delinquent May 1, 2016": "SOUTHWEST CONTRACTING, LLC",
		"KYLDERON MIST VALLEY LLC":                           "KYLDERON MIST VALLEY LLC",
		"ACME INC, Dissolved Jan 2, 2020":                    "ACME INC",
		"PLAIN NAME, LLC":                                    "PLAIN NAME, LLC",
	}
	for in, want := range cases {
		if got := CleanEntityName(in); got != want {
			t.Fatalf("CleanEntityName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNormalizeStatus(t *testing.T) {
	if status, active := NormalizeStatus("Good Standing"); status != "active" || !active {
		t.Fatalf("NormalizeStatus(Good Standing) = %q,%v want active,true", status, active)
	}
	if status, active := NormalizeStatus("Delinquent"); status != "inactive" || active {
		t.Fatalf("NormalizeStatus(Delinquent) = %q,%v want inactive,false", status, active)
	}
	if status, active := NormalizeStatus(""); status != "unknown" || active {
		t.Fatalf("NormalizeStatus(empty) = %q,%v want unknown,false", status, active)
	}
}

func TestParseFormationDate(t *testing.T) {
	if date, ok := ParseFormationDate("2025-06-16T00:00:00.000"); !ok || date != "2025-06-16" {
		t.Fatalf("ParseFormationDate = %q,%v want 2025-06-16,true", date, ok)
	}
	if date, ok := ParseFormationDate("1978-02-28"); !ok || date != "1978-02-28" {
		t.Fatalf("ParseFormationDate date-only = %q,%v", date, ok)
	}
	if _, ok := ParseFormationDate(""); ok {
		t.Fatal("ParseFormationDate(empty) returned ok=true")
	}
	if _, ok := ParseFormationDate("not-a-date"); ok {
		t.Fatal("ParseFormationDate(garbage) returned ok=true")
	}
}

func TestGlobalCompanyIDAndForeign(t *testing.T) {
	if got := GlobalCompanyID("20251665680"); got != "CO:20251665680" {
		t.Fatalf("GlobalCompanyID = %q, want CO:20251665680", got)
	}
	if GlobalCompanyID("") != "" {
		t.Fatal("GlobalCompanyID(empty) should be empty")
	}
	if !IsForeignEntity("TX") {
		t.Fatal("TX should be foreign")
	}
	if IsForeignEntity("CO") || IsForeignEntity("") {
		t.Fatal("CO/empty should not be foreign")
	}
}

func TestAgentTypeAndDisplayName(t *testing.T) {
	person := ColoradoEntityRecord{AgentFirstName: "Steven", AgentMiddleName: "G", AgentLastName: "Franchini"}
	if AgentType(person) != "person" {
		t.Fatalf("AgentType person = %q", AgentType(person))
	}
	if got := AgentDisplayName(person); got != "Steven G Franchini" {
		t.Fatalf("AgentDisplayName person = %q", got)
	}

	org := ColoradoEntityRecord{AgentOrganizationName: "C T CORPORATION SYSTEM"}
	if AgentType(org) != "organization" {
		t.Fatalf("AgentType org = %q", AgentType(org))
	}
	if got := AgentDisplayName(org); got != "C T CORPORATION SYSTEM" {
		t.Fatalf("AgentDisplayName org = %q", got)
	}

	if AgentType(ColoradoEntityRecord{}) != "" {
		t.Fatal("AgentType empty should be empty")
	}
}
