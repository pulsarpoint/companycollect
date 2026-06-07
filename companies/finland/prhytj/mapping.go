package prhytj

import (
	"net/url"
	"strings"
)

type CompanyProfile struct {
	BusinessID           string
	EUID                 string
	VATID                string
	LegalName            string
	IsActive             bool
	Website              string
	LegalForm            string
	LegalFormCode        string
	MainBusinessLine     string
	MainBusinessLineCode string
	TaxRegistrations     TaxRegistrations
	Addresses            []ProfileAddress
	LastModified         string
}

type TaxRegistrations struct {
	VAT        TaxRegistration
	Employer   TaxRegistration
	Prepayment TaxRegistration
}

type TaxRegistration struct {
	Registered       bool
	RegistrationDate string
	EndDate          string
}

type ProfileAddress struct {
	Type             int
	Street           string
	PostCode         string
	Country          string
	City             string
	CitySV           string
	MunicipalityCode string
}

func (record CompanyRecord) ToProfile() CompanyProfile {
	currentForm := currentCompanyForm(record.CompanyForms)

	return CompanyProfile{
		BusinessID:           record.BusinessID.Value,
		EUID:                 identifierValue(record.EUID),
		VATID:                vatID(record.BusinessID.Value),
		LegalName:            currentLegalName(record.Names),
		IsActive:             record.TradeRegisterStatus == "1" && record.EndDate == "",
		Website:              normalizeWebsite(record.Website.URL),
		LegalForm:            preferredDescription(currentForm.Descriptions),
		LegalFormCode:        currentForm.Type,
		MainBusinessLine:     preferredDescription(record.MainBusinessLine.Descriptions),
		MainBusinessLineCode: record.MainBusinessLine.Type,
		TaxRegistrations: TaxRegistrations{
			VAT:        taxRegistration(record.RegisteredEntries, "6"),
			Employer:   taxRegistration(record.RegisteredEntries, "5"),
			Prepayment: taxRegistration(record.RegisteredEntries, "7"),
		},
		Addresses:    profileAddresses(record.Addresses),
		LastModified: record.LastModified,
	}
}

func identifierValue(identifier *Identifier) string {
	if identifier == nil {
		return ""
	}
	return identifier.Value
}

func currentLegalName(names []Name) string {
	var selected *Name
	for i := range names {
		name := &names[i]
		if name.Type != "1" || name.EndDate != "" || name.Name == "" {
			continue
		}
		if selected == nil || name.RegistrationDate > selected.RegistrationDate {
			selected = name
		}
	}
	if selected != nil {
		return selected.Name
	}

	for i := range names {
		if names[i].Type == "1" && names[i].Name != "" {
			return names[i].Name
		}
	}
	for i := range names {
		if names[i].Name != "" {
			return names[i].Name
		}
	}

	return ""
}

func currentCompanyForm(forms []CompanyForm) CompanyForm {
	var selected *CompanyForm
	for i := range forms {
		form := &forms[i]
		if form.EndDate != "" {
			continue
		}
		if selected == nil || form.RegistrationDate > selected.RegistrationDate {
			selected = form
		}
	}
	if selected != nil {
		return *selected
	}

	for i := range forms {
		form := &forms[i]
		if selected == nil || form.RegistrationDate > selected.RegistrationDate {
			selected = form
		}
	}
	if selected != nil {
		return *selected
	}

	return CompanyForm{}
}

func preferredDescription(descriptions []Description) string {
	for _, languageCode := range []string{"1", "3", "2"} {
		if description := descriptionByLanguage(descriptions, languageCode); description != "" {
			return description
		}
	}
	return ""
}

func descriptionByLanguage(descriptions []Description, languageCode string) string {
	for _, description := range descriptions {
		if description.LanguageCode == languageCode {
			return description.Description
		}
	}
	return ""
}

func normalizeWebsite(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}

	parsed, err := url.Parse(trimmed)
	if err == nil && parsed.Scheme != "" {
		return trimmed
	}

	return "https://" + trimmed
}

func taxRegistration(entries []RegisteredEntry, register string) TaxRegistration {
	var selected *RegisteredEntry
	for i := range entries {
		entry := &entries[i]
		if entry.Register != register || entry.EndDate != "" {
			continue
		}
		if selected == nil || entry.RegistrationDate > selected.RegistrationDate {
			selected = entry
		}
	}
	if selected != nil {
		return TaxRegistration{
			Registered:       true,
			RegistrationDate: selected.RegistrationDate,
			EndDate:          selected.EndDate,
		}
	}

	for i := range entries {
		entry := &entries[i]
		if entry.Register != register {
			continue
		}
		if selected == nil || entry.RegistrationDate > selected.RegistrationDate {
			selected = entry
		}
	}
	if selected != nil {
		return TaxRegistration{
			Registered:       false,
			RegistrationDate: selected.RegistrationDate,
			EndDate:          selected.EndDate,
		}
	}

	return TaxRegistration{}
}

func profileAddresses(addresses []Address) []ProfileAddress {
	profileAddresses := make([]ProfileAddress, 0, len(addresses))
	for _, address := range addresses {
		profileAddresses = append(profileAddresses, profileAddress(address))
	}
	return profileAddresses
}

func profileAddress(address Address) ProfileAddress {
	profile := ProfileAddress{
		Type:     address.Type,
		Street:   address.Street,
		PostCode: address.PostCode,
		Country:  address.Country,
	}

	for _, postOffice := range address.PostOffices {
		switch postOffice.LanguageCode {
		case "1":
			profile.City = postOffice.City
		case "2":
			profile.CitySV = postOffice.City
		}
		if profile.MunicipalityCode == "" {
			profile.MunicipalityCode = postOffice.MunicipalityCode
		}
	}

	return profile
}

func vatID(businessID string) string {
	digits := strings.Builder{}
	for _, character := range businessID {
		if character >= '0' && character <= '9' {
			digits.WriteRune(character)
		}
	}
	if digits.Len() == 0 {
		return ""
	}
	return "FI" + digits.String()
}
