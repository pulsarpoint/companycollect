package prhytj

const (
	SourceSlug     = "finland_prh_ytj_v3"
	SourceName     = "PRH Open Data YTJ API v3 companies"
	DefaultBaseURL = "https://avoindata.prh.fi/opendata-ytj-api/v3/companies"
)

type Page struct {
	TotalResults int             `json:"totalResults"`
	Companies    []CompanyRecord `json:"companies"`
}

type CompanyRecord struct {
	BusinessID          Identifier         `json:"businessId"`
	EUID                *Identifier        `json:"euId"`
	Identifiers         []Identifier       `json:"identifiers"`
	Names               []Name             `json:"names"`
	MainBusinessLine    BusinessLine       `json:"mainBusinessLine"`
	Website             Website            `json:"website"`
	CompanyForms        []CompanyForm      `json:"companyForms"`
	CompanySituations   []CompanySituation `json:"companySituations"`
	RegisteredEntries   []RegisteredEntry  `json:"registeredEntries"`
	Addresses           []Address          `json:"addresses"`
	TradeRegisterStatus string             `json:"tradeRegisterStatus"`
	Status              string             `json:"status"`
	RegistrationDate    string             `json:"registrationDate"`
	EndDate             string             `json:"endDate"`
	LastModified        string             `json:"lastModified"`
	RawPayload          []byte             `json:"-"`
	PayloadHash         string             `json:"-"`
}

type Identifier struct {
	Type             string `json:"type"`
	Value            string `json:"value"`
	RegistrationDate string `json:"registrationDate"`
	EndDate          string `json:"endDate"`
	Source           string `json:"source"`
}

type Name struct {
	Name             string `json:"name"`
	Type             string `json:"type"`
	Version          int    `json:"version"`
	RegistrationDate string `json:"registrationDate"`
	EndDate          string `json:"endDate"`
}

type Description struct {
	LanguageCode string `json:"languageCode"`
	Description  string `json:"description"`
}

type BusinessLine struct {
	Type             string        `json:"type"`
	TypeCodeSet      string        `json:"typeCodeSet"`
	RegistrationDate string        `json:"registrationDate"`
	Source           string        `json:"source"`
	Descriptions     []Description `json:"descriptions"`
}

type Website struct {
	URL              string `json:"url"`
	RegistrationDate string `json:"registrationDate"`
	EndDate          string `json:"endDate"`
}

type CompanyForm struct {
	Type             string        `json:"type"`
	Version          int           `json:"version"`
	RegistrationDate string        `json:"registrationDate"`
	EndDate          string        `json:"endDate"`
	Source           string        `json:"source"`
	Descriptions     []Description `json:"descriptions"`
}

type CompanySituation struct {
	Type             string        `json:"type"`
	RegistrationDate string        `json:"registrationDate"`
	EndDate          string        `json:"endDate"`
	Descriptions     []Description `json:"descriptions"`
}

type RegisteredEntry struct {
	Type             string        `json:"type"`
	Descriptions     []Description `json:"descriptions"`
	RegistrationDate string        `json:"registrationDate"`
	EndDate          string        `json:"endDate"`
	Register         string        `json:"register"`
	Authority        string        `json:"authority"`
}

type Address struct {
	Type             int          `json:"type"`
	Street           string       `json:"street"`
	PostCode         string       `json:"postCode"`
	BuildingNumber   string       `json:"buildingNumber"`
	Entrance         string       `json:"entrance"`
	ApartmentNumber  string       `json:"apartmentNumber"`
	PostOfficeBox    string       `json:"postOfficeBox"`
	CO               string       `json:"co"`
	Country          string       `json:"country"`
	RegistrationDate string       `json:"registrationDate"`
	Source           string       `json:"source"`
	PostOffices      []PostOffice `json:"postOffices"`
}

type PostOffice struct {
	LanguageCode     string `json:"languageCode"`
	City             string `json:"city"`
	MunicipalityCode string `json:"municipalityCode"`
}
