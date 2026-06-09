package coloradoentities

import "encoding/json"

const (
	SourceKey    = "coloradoentities"
	SourceSlug   = "united_states_colorado_business_entities"
	SourceName   = "Colorado Business Entities"
	StateCode    = "CO"
	countryISO2  = "US"
	databaseName = "corpscout_sources"
)

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

	EntityStatus            string `json:"entitystatus,omitempty"`
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

	RawPayload  json.RawMessage `json:"-"`
	PayloadHash string          `json:"-"`
}
