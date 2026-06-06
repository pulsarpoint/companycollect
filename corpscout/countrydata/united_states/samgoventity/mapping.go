package samgoventity

import "strings"

// CompanyProfile is the derived, Corpscout-friendly view of one SAM.gov entity.
// SAM describes *federal-award eligibility* — its registration status is NOT
// state corporate standing, so the two are kept distinct. EIN/TIN is sensitive
// and redacted in the Public extract, so it is intentionally absent here.
type CompanyProfile struct {
	UeiSAM string
	// PrimaryID is the namespaced Unique Entity ID, e.g. "UEI:ABC123DEF456".
	PrimaryID string
	CageCode  string
	LegalName string
	// AlternateNames carries dbaName when present.
	AlternateNames []string
	// IsFederalRegistrant is true for any entity present in SAM.
	IsFederalRegistrant bool
	// SamRegistrationStatus is the raw status (Active, Expired, ...).
	SamRegistrationStatus string
	// IsSamActive is true when the SAM registration is Active (federal
	// eligibility only — not corporate standing).
	IsSamActive                bool
	RegistrationDate           string
	RegistrationExpirationDate string

	EntityStructure        string
	ProfitStructure        string
	StateOfIncorporation   string
	CountryOfIncorporation string
	EntityURL              string

	PhysicalAddress ProfileAddress
	MailingAddress  ProfileAddress

	PrimaryNaics string
	NaicsCodes   []NaicsCode
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

type NaicsCode struct {
	Code             string
	Description      string
	SbaSmallBusiness bool
}

func (record SamEntityRecord) ToProfile() CompanyProfile {
	reg := record.EntityRegistration
	general := record.CoreData.GeneralInformation

	uei := strings.TrimSpace(reg.UeiSAM)
	profile := CompanyProfile{
		UeiSAM:                     uei,
		PrimaryID:                  primaryID(uei),
		CageCode:                   strings.TrimSpace(reg.CageCode),
		LegalName:                  strings.TrimSpace(reg.LegalBusinessName),
		IsFederalRegistrant:        true,
		SamRegistrationStatus:      strings.TrimSpace(reg.RegistrationStatus),
		IsSamActive:                strings.EqualFold(strings.TrimSpace(reg.RegistrationStatus), "Active"),
		RegistrationDate:           strings.TrimSpace(reg.RegistrationDate),
		RegistrationExpirationDate: strings.TrimSpace(reg.RegistrationExpirationDate),
		EntityStructure:            strings.TrimSpace(general.EntityStructureDesc),
		ProfitStructure:            strings.TrimSpace(general.ProfitStructureDesc),
		StateOfIncorporation:       strings.TrimSpace(general.StateOfIncorporationCode),
		CountryOfIncorporation:     strings.TrimSpace(general.CountryOfIncorporationCode),
		EntityURL:                  strings.TrimSpace(record.CoreData.EntityInformation.EntityURL),
		PhysicalAddress:            profileAddress(record.CoreData.PhysicalAddress),
		MailingAddress:             profileAddress(record.CoreData.MailingAddress),
		PrimaryNaics:               strings.TrimSpace(record.Assertions.GoodsAndServices.PrimaryNaics),
		NaicsCodes:                 naicsCodes(record.Assertions.GoodsAndServices.NaicsList),
	}

	if dba := strings.TrimSpace(reg.DbaName); dba != "" {
		profile.AlternateNames = append(profile.AlternateNames, dba)
	}
	return profile
}

func primaryID(uei string) string {
	trimmed := strings.TrimSpace(uei)
	if trimmed == "" {
		return ""
	}
	return "UEI:" + trimmed
}

func profileAddress(address SamAddress) ProfileAddress {
	return ProfileAddress{
		Line1:   strings.TrimSpace(address.AddressLine1),
		Line2:   strings.TrimSpace(address.AddressLine2),
		City:    strings.TrimSpace(address.City),
		State:   strings.TrimSpace(address.StateOrProvinceCode),
		ZIP:     strings.TrimSpace(address.ZipCode),
		Country: strings.TrimSpace(address.CountryCode),
	}
}

func naicsCodes(list []NaicsEntry) []NaicsCode {
	if len(list) == 0 {
		return nil
	}
	codes := make([]NaicsCode, 0, len(list))
	for _, entry := range list {
		code := strings.TrimSpace(entry.NaicsCode)
		if code == "" {
			continue
		}
		codes = append(codes, NaicsCode{
			Code:             code,
			Description:      strings.TrimSpace(entry.NaicsDescription),
			SbaSmallBusiness: strings.EqualFold(strings.TrimSpace(entry.SbaSmallBusiness), "Y"),
		})
	}
	return codes
}
