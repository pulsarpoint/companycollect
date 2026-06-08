package coloradoentities

import "encoding/json"

const (
	// SourceKey is the stable public source key used in manifests and CLI output.
	SourceKey = "coloradoentities"
	// SourceSlug is the fully-qualified source identity used in error wrapping.
	SourceSlug = "united_states_colorado_business_entities"
	// SourceName is a human-readable source name.
	SourceName = "Colorado Business Entities"

	// DefaultBaseURL is the Socrata SODA endpoint for the Colorado business
	// entities dataset (4ykn-tg5h).
	DefaultBaseURL = "https://data.colorado.gov/resource/4ykn-tg5h.json"
	// DefaultPageSize is the SODA $limit used per request.
	DefaultPageSize = 1000

	// StateCode is the USPS code Colorado entity ids are scoped to.
	StateCode = "CO"
)

// ColoradoEntityRecord is one source-native Colorado business entity. Field
// names follow the SODA JSON keys exactly, including the source's misspelled
// "jurisdictonofformation" key, so the NDJSON snapshot round-trips.
type ColoradoEntityRecord struct {
	EntityID string `json:"entityid"`
	Name     string `json:"entityname"`

	PrincipalAddress1 string `json:"principaladdress1,omitempty"`
	PrincipalAddress2 string `json:"principaladdress2,omitempty"`
	PrincipalCity     string `json:"principalcity,omitempty"`
	PrincipalState    string `json:"principalstate,omitempty"`
	PrincipalZipCode  string `json:"principalzipcode,omitempty"`
	PrincipalCountry  string `json:"principalcountry,omitempty"`

	MailingAddress1 string `json:"mailingaddress1,omitempty"`
	MailingCity     string `json:"mailingcity,omitempty"`
	MailingState    string `json:"mailingstate,omitempty"`
	MailingZipCode  string `json:"mailingzipcode,omitempty"`
	MailingCountry  string `json:"mailingcountry,omitempty"`

	EntityStatus string `json:"entitystatus,omitempty"`
	// JurisdictionOfFormation maps the source-misspelled key as documented.
	JurisdictionOfFormation string `json:"jurisdictonofformation,omitempty"`
	EntityType              string `json:"entitytype,omitempty"`

	AgentFirstName        string `json:"agentfirstname,omitempty"`
	AgentMiddleName       string `json:"agentmiddlename,omitempty"`
	AgentLastName         string `json:"agentlastname,omitempty"`
	AgentOrganizationName string `json:"agentorganizationname,omitempty"`

	AgentPrincipalAddress1 string `json:"agentprincipaladdress1,omitempty"`
	AgentPrincipalCity     string `json:"agentprincipalcity,omitempty"`
	AgentPrincipalState    string `json:"agentprincipalstate,omitempty"`
	AgentPrincipalZipCode  string `json:"agentprincipalzipcode,omitempty"`
	AgentPrincipalCountry  string `json:"agentprincipalcountry,omitempty"`

	EntityFormDate string `json:"entityformdate,omitempty"`

	// RawPayload and PayloadHash are populated during process/export from the
	// snapshot line; they are not part of the serialized NDJSON record.
	RawPayload  json.RawMessage `json:"-"`
	PayloadHash string          `json:"-"`
}
