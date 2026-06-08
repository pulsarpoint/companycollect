package coloradoentities

import "testing"

func TestProjectExportRowsGoodStandingDomesticPersonAgent(t *testing.T) {
	record := ColoradoEntityRecord{
		EntityID:                "20251665680",
		Name:                    "KYLDERON MIST VALLEY LLC",
		PrincipalAddress1:       "660 Willow Wood Ln",
		PrincipalCity:           "Delta",
		PrincipalState:          "CO",
		PrincipalZipCode:        "81416",
		PrincipalCountry:        "US",
		EntityStatus:            "Good Standing",
		JurisdictionOfFormation: "CO",
		EntityType:              "DLLC",
		AgentFirstName:          "KEQIANG",
		AgentLastName:           "DENG",
		AgentPrincipalAddress1:  "660 Willow Wood Ln",
		AgentPrincipalCity:      "Delta",
		AgentPrincipalState:     "CO",
		AgentPrincipalZipCode:   "81416",
		EntityFormDate:          "2025-06-16T00:00:00.000",
		PayloadHash:             "payload-hash",
		RawPayload:              []byte(`{"entityid":"20251665680"}`),
	}

	rows := ProjectExportRows(record, "run-1")

	if len(rows.Companies) != 1 {
		t.Fatalf("companies len = %d, want 1", len(rows.Companies))
	}
	company := rows.Companies[0]
	if company.GlobalCompanyID != "CO:20251665680" || company.EntityID != "20251665680" {
		t.Fatalf("company ids = %#v", company)
	}
	if !company.IsActive || company.IsForeign {
		t.Fatalf("company flags = %#v", company)
	}
	if company.FormationDate != "2025-06-16" {
		t.Fatalf("formation date = %q, want 2025-06-16", company.FormationDate)
	}

	// Clean name == raw name, so only one name row.
	if len(rows.CompanyNames) != 1 || !rows.CompanyNames[0].IsPrimary {
		t.Fatalf("company names = %#v, want one primary legal row", rows.CompanyNames)
	}
	if len(rows.LegalForms) != 1 || rows.LegalForms[0].EntityTypeCode != "DLLC" {
		t.Fatalf("legal forms = %#v", rows.LegalForms)
	}
	// Principal + agent_principal addresses.
	if len(rows.Addresses) != 2 {
		t.Fatalf("addresses len = %d, want 2", len(rows.Addresses))
	}
	if len(rows.RegisteredAgents) != 1 || rows.RegisteredAgents[0].AgentType != "person" {
		t.Fatalf("registered agents = %#v", rows.RegisteredAgents)
	}
	if rows.RegisteredAgents[0].DisplayName != "KEQIANG DENG" {
		t.Fatalf("agent display name = %q", rows.RegisteredAgents[0].DisplayName)
	}
	if len(rows.Identifiers) != 1 || rows.Identifiers[0].IdentifierValue != "CO:20251665680" {
		t.Fatalf("identifiers = %#v", rows.Identifiers)
	}
	if len(rows.SourceEvidence) != 1 || rows.SourceEvidence[0].SourcePayloadHash != "payload-hash" {
		t.Fatalf("source evidence = %#v", rows.SourceEvidence)
	}
}

func TestProjectExportRowsDelinquentNameVariant(t *testing.T) {
	record := ColoradoEntityRecord{
		EntityID:                "19871342214",
		Name:                    "SOUTHWEST CONTRACTING, LLC, Delinquent May 1, 2016",
		PrincipalAddress1:       "123 Old Rd",
		PrincipalCity:           "Cortez",
		PrincipalState:          "CO",
		PrincipalZipCode:        "81321",
		MailingAddress1:         "PO BOX 719",
		MailingCity:             "CORTEZ",
		MailingState:            "CO",
		MailingZipCode:          "81321",
		EntityStatus:            "Delinquent",
		JurisdictionOfFormation: "CO",
		EntityType:              "DLLC",
		AgentFirstName:          "Steven",
		AgentMiddleName:         "G",
		AgentLastName:           "Franchini",
	}

	rows := ProjectExportRows(record, "run-1")

	company := rows.Companies[0]
	if company.LegalName != "SOUTHWEST CONTRACTING, LLC" {
		t.Fatalf("legal name = %q, want stripped", company.LegalName)
	}
	if company.IsActive {
		t.Fatal("delinquent entity should not be active")
	}
	// Clean legal + raw source_variant.
	if len(rows.CompanyNames) != 2 {
		t.Fatalf("company names len = %d, want 2 (legal + source_variant)", len(rows.CompanyNames))
	}
	if rows.CompanyNames[1].NameType != "source_variant" {
		t.Fatalf("second name type = %q, want source_variant", rows.CompanyNames[1].NameType)
	}
	// principal + mailing + agent_principal? agent has no address here -> principal + mailing only.
	if len(rows.Addresses) != 2 {
		t.Fatalf("addresses len = %d, want 2 (principal + mailing)", len(rows.Addresses))
	}
}

func TestProjectExportRowsForeignOrganizationAgent(t *testing.T) {
	record := ColoradoEntityRecord{
		EntityID:                "20251955891",
		Name:                    "FIRST HEALTH LIFE & HEALTH INSURANCE COMPANY",
		PrincipalState:          "IL",
		EntityStatus:            "Good Standing",
		JurisdictionOfFormation: "TX",
		EntityType:              "FPC",
		AgentOrganizationName:   "C T CORPORATION SYSTEM",
		AgentPrincipalAddress1:  "7700 E Arapahoe Rd Ste 220",
		AgentPrincipalCity:      "Centennial",
		AgentPrincipalState:     "CO",
		AgentPrincipalZipCode:   "80112",
	}

	rows := ProjectExportRows(record, "run-1")
	if !rows.Companies[0].IsForeign {
		t.Fatal("entity formed in TX should be foreign")
	}
	if !rows.LegalForms[0].IsForeign {
		t.Fatal("legal form should be flagged foreign")
	}
	if len(rows.RegisteredAgents) != 1 || rows.RegisteredAgents[0].AgentType != "organization" {
		t.Fatalf("registered agents = %#v", rows.RegisteredAgents)
	}
}
