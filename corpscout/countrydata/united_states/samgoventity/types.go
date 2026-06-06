package samgoventity

const (
	SourceSlug = "united_states_sam_gov_entity"
	SourceName = "SAM.gov Entity Management (System for Award Management)"
	// DefaultBaseURL is the SAM.gov Entity Management v3 endpoint.
	DefaultBaseURL = "https://api.sam.gov/entity-information/v3/entities"
	// DefaultPageSize is the Socrata-style page size. The SAM entity API caps
	// "size" at 10 (size>10 returns HTTP 400), verified against a live response.
	DefaultPageSize = 10
	// DefaultSamRegistered scopes results to entities actively registered in SAM.
	DefaultSamRegistered = "Yes"
)

// SamEntityRecord is one SAM.gov entity. The shape was verified against a live
// Public-extract response. pointsOfContact is intentionally NOT modeled: it
// carries personal contact data not needed for a company profile.
type SamEntityRecord struct {
	EntityRegistration EntityRegistration `json:"entityRegistration"`
	CoreData           CoreData           `json:"coreData"`
	Assertions         Assertions         `json:"assertions"`
}

type EntityRegistration struct {
	SamRegistered              string `json:"samRegistered"`
	UeiSAM                     string `json:"ueiSAM"`
	CageCode                   string `json:"cageCode"`
	LegalBusinessName          string `json:"legalBusinessName"`
	DbaName                    string `json:"dbaName"`
	RegistrationStatus         string `json:"registrationStatus"`
	RegistrationDate           string `json:"registrationDate"`
	RegistrationExpirationDate string `json:"registrationExpirationDate"`
	LastUpdateDate             string `json:"lastUpdateDate"`
	UeiStatus                  string `json:"ueiStatus"`
	PurposeOfRegistrationCode  string `json:"purposeOfRegistrationCode"`
	PurposeOfRegistrationDesc  string `json:"purposeOfRegistrationDesc"`
}

type CoreData struct {
	PhysicalAddress    SamAddress         `json:"physicalAddress"`
	MailingAddress     SamAddress         `json:"mailingAddress"`
	EntityInformation  EntityInformation  `json:"entityInformation"`
	GeneralInformation GeneralInformation `json:"generalInformation"`
}

type SamAddress struct {
	AddressLine1        string `json:"addressLine1"`
	AddressLine2        string `json:"addressLine2"`
	City                string `json:"city"`
	StateOrProvinceCode string `json:"stateOrProvinceCode"`
	ZipCode             string `json:"zipCode"`
	ZipCodePlus4        string `json:"zipCodePlus4"`
	CountryCode         string `json:"countryCode"`
}

type EntityInformation struct {
	EntityURL              string `json:"entityURL"`
	EntityStartDate        string `json:"entityStartDate"`
	FiscalYearEndCloseDate string `json:"fiscalYearEndCloseDate"`
}

type GeneralInformation struct {
	EntityStructureCode        string `json:"entityStructureCode"`
	EntityStructureDesc        string `json:"entityStructureDesc"`
	EntityTypeCode             string `json:"entityTypeCode"`
	EntityTypeDesc             string `json:"entityTypeDesc"`
	ProfitStructureCode        string `json:"profitStructureCode"`
	ProfitStructureDesc        string `json:"profitStructureDesc"`
	StateOfIncorporationCode   string `json:"stateOfIncorporationCode"`
	StateOfIncorporationDesc   string `json:"stateOfIncorporationDesc"`
	CountryOfIncorporationCode string `json:"countryOfIncorporationCode"`
	CountryOfIncorporationDesc string `json:"countryOfIncorporationDesc"`
}

type Assertions struct {
	GoodsAndServices GoodsAndServices `json:"goodsAndServices"`
}

type GoodsAndServices struct {
	PrimaryNaics string       `json:"primaryNaics"`
	NaicsList    []NaicsEntry `json:"naicsList"`
}

type NaicsEntry struct {
	NaicsCode        string `json:"naicsCode"`
	NaicsDescription string `json:"naicsDescription"`
	SbaSmallBusiness string `json:"sbaSmallBusiness"`
	NaicsException   string `json:"naicsException"`
}
