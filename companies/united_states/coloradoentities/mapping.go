package coloradoentities

import (
	"strings"
	"time"
)

// formationDateLayouts lists the timestamp formats observed in the SODA
// entityformdate field. The documented format carries .000 milliseconds and a
// midnight time component that is filler; the date portion is the meaning.
var formationDateLayouts = []string{
	"2006-01-02T15:04:05.000",
	"2006-01-02T15:04:05",
	"2006-01-02T15:04:05.000Z07:00",
	"2006-01-02T15:04:05Z07:00",
	"2006-01-02",
}

// statusAnnotationPrefixes are the status words that the source sometimes
// appends to entityname (e.g. ", Delinquent May 1, 2016"). A trailing
// ", <word> ..." segment beginning with one of these is stripped from the
// display/legal name.
var statusAnnotationPrefixes = []string{
	"delinquent",
	"dissolved",
	"withdrawn",
	"noncompliant",
	"expired",
	"revoked",
	"administratively dissolved",
}

// CleanEntityName strips a trailing delinquency/status annotation from the
// entity name. The raw value is preserved separately as a source variant. The
// cut is made at the earliest ", <status word>" marker so commas inside the
// annotation's own date (e.g. "May 1, 2016") are not mistaken for the marker.
func CleanEntityName(name string) string {
	trimmed := strings.TrimSpace(name)
	lower := strings.ToLower(trimmed)
	cut := -1
	for _, prefix := range statusAnnotationPrefixes {
		marker := ", " + prefix
		if idx := strings.Index(lower, marker); idx >= 0 && (cut < 0 || idx < cut) {
			cut = idx
		}
	}
	if cut < 0 {
		return trimmed
	}
	return strings.TrimSpace(trimmed[:cut])
}

// NormalizeStatus maps the free-text entitystatus to a normalized status string
// and an is-active flag. "Good Standing" is active; everything else is treated
// as inactive/at-risk.
func NormalizeStatus(status string) (string, bool) {
	normalized := strings.ToLower(strings.TrimSpace(status))
	if normalized == "" {
		return "unknown", false
	}
	if normalized == "good standing" {
		return "active", true
	}
	return "inactive", false
}

// ParseFormationDate extracts the YYYY-MM-DD date portion of entityformdate.
// It returns the date string and whether parsing succeeded.
func ParseFormationDate(value string) (string, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", false
	}
	for _, layout := range formationDateLayouts {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed.Format("2006-01-02"), true
		}
	}
	return "", false
}

// GlobalCompanyID builds the cross-state-unique id for a Colorado entity.
func GlobalCompanyID(entityID string) string {
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		return ""
	}
	return StateCode + ":" + entityID
}

// IsForeignEntity reports whether the entity was formed in another state and is
// merely registered to do business in Colorado.
func IsForeignEntity(jurisdiction string) bool {
	jurisdiction = strings.ToUpper(strings.TrimSpace(jurisdiction))
	return jurisdiction != "" && jurisdiction != StateCode
}

// AgentType classifies the registered agent as a person, an organization, or
// none, based on which name fields are populated.
func AgentType(record ColoradoEntityRecord) string {
	if strings.TrimSpace(record.AgentOrganizationName) != "" {
		return "organization"
	}
	if strings.TrimSpace(record.AgentFirstName) != "" ||
		strings.TrimSpace(record.AgentMiddleName) != "" ||
		strings.TrimSpace(record.AgentLastName) != "" {
		return "person"
	}
	return ""
}

// AgentDisplayName builds a single display name for the registered agent.
func AgentDisplayName(record ColoradoEntityRecord) string {
	if org := strings.TrimSpace(record.AgentOrganizationName); org != "" {
		return org
	}
	parts := make([]string, 0, 3)
	for _, part := range []string{record.AgentFirstName, record.AgentMiddleName, record.AgentLastName} {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			parts = append(parts, trimmed)
		}
	}
	return strings.Join(parts, " ")
}
