package coloradoentities

const (
	SourceSlug = "united_states_colorado_business_entities"
	SourceName = "Colorado Business Entities (Colorado SoS / Socrata SODA)"
	// DefaultBaseURL is the Socrata SODA endpoint for dataset 4ykn-tg5h, which
	// returns a top-level JSON array of entity objects.
	DefaultBaseURL = "https://data.colorado.gov/resource/4ykn-tg5h.json"
	// DefaultPageSize is the Socrata $limit used per request.
	DefaultPageSize = 1000
)

// ColoradoEntityRecord is one Colorado business entity. Fields are sparse: most
// optional fields are absent on a given record. Note jurisdictonofformation is
// MISSPELLED in the source (missing an "i") — the exact key is preserved.
type ColoradoEntityRecord struct {
	EntityID   string `json:"entityid"`
	EntityName string `json:"entityname"`

	PrincipalAddress1 string `json:"principaladdress1,omitempty"`
	PrincipalAddress2 string `json:"principaladdress2,omitempty"`
	PrincipalCity     string `json:"principalcity,omitempty"`
	PrincipalState    string `json:"principalstate,omitempty"`
	PrincipalZipcode  string `json:"principalzipcode,omitempty"`
	PrincipalCountry  string `json:"principalcountry,omitempty"`

	MailingAddress1 string `json:"mailingaddress1,omitempty"`
	MailingCity     string `json:"mailingcity,omitempty"`
	MailingState    string `json:"mailingstate,omitempty"`
	MailingZipcode  string `json:"mailingzipcode,omitempty"`
	MailingCountry  string `json:"mailingcountry,omitempty"`

	EntityStatus           string `json:"entitystatus,omitempty"`
	JurisdictonOfFormation string `json:"jurisdictonofformation,omitempty"`
	EntityType             string `json:"entitytype,omitempty"`

	AgentFirstName        string `json:"agentfirstname,omitempty"`
	AgentMiddleName       string `json:"agentmiddlename,omitempty"`
	AgentLastName         string `json:"agentlastname,omitempty"`
	AgentOrganizationName string `json:"agentorganizationname,omitempty"`

	AgentPrincipalAddress1 string `json:"agentprincipaladdress1,omitempty"`
	AgentPrincipalCity     string `json:"agentprincipalcity,omitempty"`
	AgentPrincipalState    string `json:"agentprincipalstate,omitempty"`
	AgentPrincipalZipcode  string `json:"agentprincipalzipcode,omitempty"`
	AgentPrincipalCountry  string `json:"agentprincipalcountry,omitempty"`

	AgentMailingAddress1 string `json:"agentmailingaddress1,omitempty"`
	AgentMailingCity     string `json:"agentmailingcity,omitempty"`
	AgentMailingState    string `json:"agentmailingstate,omitempty"`
	AgentMailingZipcode  string `json:"agentmailingzipcode,omitempty"`
	AgentMailingCountry  string `json:"agentmailingcountry,omitempty"`

	EntityFormDate string `json:"entityformdate,omitempty"`

	// RawPayload is the exact source NDJSON line the record was decoded from and
	// PayloadHash is its SHA-256, both populated by Process for raw storage. They
	// are not part of the source JSON.
	RawPayload  []byte `json:"-"`
	PayloadHash string `json:"-"`
}
