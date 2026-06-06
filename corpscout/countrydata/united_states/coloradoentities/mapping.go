package coloradoentities

import "strings"

// CompanyProfile is the derived, Corpscout-friendly view of one Colorado entity.
// Colorado is a state register, so this is the first source with authoritative
// private-company fields: legal form, corporate standing, formation date, and a
// registered agent (the agent is a service-of-process contact, NOT an owner).
type CompanyProfile struct {
	EntityID string
	// PrimaryID is globally unique: entityid is unique only within Colorado.
	PrimaryID string
	// LegalName has any appended status annotation stripped.
	LegalName string
	// AlternateNames keeps the raw entityname when it differed (carried a note).
	AlternateNames []string
	// IsActive is true for "Good Standing"; Delinquent/Dissolved/etc. are not.
	IsActive  bool
	StatusRaw string

	LegalFormCode string
	LegalForm     string

	JurisdictionOfFormation string
	// IsForeign is true when the entity was formed outside Colorado.
	IsForeign bool

	// FormationDate is the date portion (YYYY-MM-DD) of entityformdate.
	FormationDate string

	PrincipalAddress ProfileAddress
	MailingAddress   ProfileAddress
	RegisteredAgent  RegisteredAgent
}

type ProfileAddress struct {
	Line1   string
	Line2   string
	City    string
	State   string
	ZIP     string
	Country string
}

func (a ProfileAddress) IsZero() bool {
	return a == ProfileAddress{}
}

type RegisteredAgent struct {
	IsOrganization   bool
	Name             string
	PrincipalAddress ProfileAddress
	MailingAddress   ProfileAddress
}

func (record ColoradoEntityRecord) ToProfile() CompanyProfile {
	rawName := strings.TrimSpace(record.EntityName)
	status := strings.TrimSpace(record.EntityStatus)
	legalName := cleanEntityName(rawName, status)

	profile := CompanyProfile{
		EntityID:                strings.TrimSpace(record.EntityID),
		PrimaryID:               primaryID(record.EntityID),
		LegalName:               legalName,
		IsActive:                strings.EqualFold(status, "Good Standing"),
		StatusRaw:               status,
		LegalFormCode:           strings.TrimSpace(record.EntityType),
		LegalForm:               decodeLegalForm(record.EntityType),
		JurisdictionOfFormation: strings.TrimSpace(record.JurisdictonOfFormation),
		IsForeign:               isForeign(record.JurisdictonOfFormation),
		FormationDate:           dateOnly(record.EntityFormDate),
		PrincipalAddress: ProfileAddress{
			Line1:   strings.TrimSpace(record.PrincipalAddress1),
			Line2:   strings.TrimSpace(record.PrincipalAddress2),
			City:    strings.TrimSpace(record.PrincipalCity),
			State:   strings.TrimSpace(record.PrincipalState),
			ZIP:     strings.TrimSpace(record.PrincipalZipcode),
			Country: strings.TrimSpace(record.PrincipalCountry),
		},
		MailingAddress: ProfileAddress{
			Line1:   strings.TrimSpace(record.MailingAddress1),
			City:    strings.TrimSpace(record.MailingCity),
			State:   strings.TrimSpace(record.MailingState),
			ZIP:     strings.TrimSpace(record.MailingZipcode),
			Country: strings.TrimSpace(record.MailingCountry),
		},
		RegisteredAgent: registeredAgent(record),
	}

	if legalName != rawName && rawName != "" {
		profile.AlternateNames = append(profile.AlternateNames, rawName)
	}
	return profile
}

func primaryID(entityID string) string {
	trimmed := strings.TrimSpace(entityID)
	if trimmed == "" {
		return ""
	}
	return "CO:" + trimmed
}

// cleanEntityName strips a trailing ", <status> ..." annotation that Colorado
// appends to non-good-standing names (e.g. ", Delinquent May 1, 2016").
func cleanEntityName(name string, status string) string {
	if name == "" || status == "" || strings.EqualFold(status, "Good Standing") {
		return name
	}
	marker := ", " + status
	if idx := indexFold(name, marker); idx >= 0 {
		return strings.TrimSpace(name[:idx])
	}
	return name
}

// indexFold finds the first case-insensitive occurrence of substr in s.
func indexFold(s string, substr string) int {
	lowerS := strings.ToLower(s)
	lowerSub := strings.ToLower(substr)
	return strings.Index(lowerS, lowerSub)
}

func isForeign(jurisdiction string) bool {
	trimmed := strings.TrimSpace(jurisdiction)
	return trimmed != "" && !strings.EqualFold(trimmed, "CO")
}

// dateOnly returns the YYYY-MM-DD portion of a Socrata datetime such as
// "1978-02-28T00:00:00.000" (the time component is always midnight filler).
func dateOnly(value string) string {
	trimmed := strings.TrimSpace(value)
	if idx := strings.IndexByte(trimmed, 'T'); idx >= 0 {
		return trimmed[:idx]
	}
	return trimmed
}

func registeredAgent(record ColoradoEntityRecord) RegisteredAgent {
	agent := RegisteredAgent{
		PrincipalAddress: ProfileAddress{
			Line1:   strings.TrimSpace(record.AgentPrincipalAddress1),
			City:    strings.TrimSpace(record.AgentPrincipalCity),
			State:   strings.TrimSpace(record.AgentPrincipalState),
			ZIP:     strings.TrimSpace(record.AgentPrincipalZipcode),
			Country: strings.TrimSpace(record.AgentPrincipalCountry),
		},
		MailingAddress: ProfileAddress{
			Line1:   strings.TrimSpace(record.AgentMailingAddress1),
			City:    strings.TrimSpace(record.AgentMailingCity),
			State:   strings.TrimSpace(record.AgentMailingState),
			ZIP:     strings.TrimSpace(record.AgentMailingZipcode),
			Country: strings.TrimSpace(record.AgentMailingCountry),
		},
	}

	if org := strings.TrimSpace(record.AgentOrganizationName); org != "" {
		agent.IsOrganization = true
		agent.Name = org
		return agent
	}

	parts := make([]string, 0, 3)
	for _, part := range []string{record.AgentFirstName, record.AgentMiddleName, record.AgentLastName} {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			parts = append(parts, trimmed)
		}
	}
	agent.Name = strings.Join(parts, " ")
	return agent
}

// decodeLegalForm maps the most common Colorado SoS entity-type codes to a human
// legal form. Unknown codes return "" (the raw code stays in LegalFormCode).
func decodeLegalForm(code string) string {
	switch strings.ToUpper(strings.TrimSpace(code)) {
	case "DLLC":
		return "Domestic Limited Liability Company"
	case "FLLC":
		return "Foreign Limited Liability Company"
	case "DPC":
		return "Domestic Profit Corporation"
	case "FPC":
		return "Foreign Profit Corporation"
	case "DNC":
		return "Domestic Nonprofit Corporation"
	case "FNC":
		return "Foreign Nonprofit Corporation"
	case "DLP":
		return "Domestic Limited Partnership"
	case "FLP":
		return "Foreign Limited Partnership"
	case "DLLP":
		return "Domestic Limited Liability Partnership"
	case "FLLP":
		return "Foreign Limited Liability Partnership"
	default:
		return ""
	}
}
