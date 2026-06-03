package companydata

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

func (data *CompanyData) TranslationTerms() []TranslationTerm {
	if data == nil {
		return nil
	}
	collector := newTranslationTermCollector()
	collector.addIfMissing(data.Company.ShortDescription, data.Company.ShortDescriptionEN)
	collector.addIfMissing(data.Company.Description, data.Company.DescriptionEN)
	collector.addIfMissing(data.Company.RegistrationStatusLabel, data.Company.RegistrationStatusLabelEN)
	collector.addIfMissing(data.Company.OrganizationFormLabel, data.Company.OrganizationFormLabelEN)
	collector.addIfMissing(data.Company.ResponseClass, data.Company.ResponseClassEN)
	collector.addIfMissing(data.Company.ActivityDescription, data.Company.ActivityDescriptionEN)
	collector.addIfMissing(data.Company.StatutoryPurpose, data.Company.StatutoryPurposeEN)
	for _, address := range data.Addresses {
		collector.addIfMissing(address.Country, address.CountryEN)
	}
	for _, industry := range data.Industries {
		collector.addIfMissing(industry.SourceLabel, industry.SourceLabelEN)
	}
	for _, website := range data.Websites {
		collector.addIfMissing(website.Title, website.TitleEN)
		collector.addIfMissing(website.Description, website.DescriptionEN)
	}
	for _, contact := range data.Contacts {
		collector.addIfMissing(contact.Label, contact.LabelEN)
	}
	for _, capital := range data.Capital {
		collector.addIfMissing(capital.CapitalType, capital.CapitalTypeEN)
	}
	for _, role := range data.Roles {
		collector.addIfMissing(role.RoleLabel, role.RoleLabelEN)
		collector.addIfMissing(role.RoleGroup, role.RoleGroupEN)
	}
	return collector.terms
}

func (data *CompanyData) ApplyTranslations(translations []TermTranslation) ApplyTranslationsResult {
	if data == nil {
		return ApplyTranslationsResult{TermsWithoutMatch: int32(len(translations))}
	}
	result := ApplyTranslationsResult{}
	for _, translation := range translations {
		translatedText := strings.TrimSpace(translation.TranslatedText)
		if translatedText == "" {
			result.TermsWithoutMatch++
			continue
		}
		key := translationTermKey(translation.SourceText)
		applied := data.applyTranslationByKey(key, translatedText)
		if applied == 0 {
			result.TermsWithoutMatch++
			continue
		}
		result.FieldsApplied += applied
	}
	return result
}

func (data *CompanyData) TranslationComplete() bool {
	return len(data.TranslationTerms()) == 0
}

func (data *CompanyData) MissingTranslationFieldCount() int32 {
	if data == nil {
		return 0
	}
	var count int32
	if translationFieldMissing(data.Company.ShortDescription, data.Company.ShortDescriptionEN) {
		count++
	}
	if translationFieldMissing(data.Company.Description, data.Company.DescriptionEN) {
		count++
	}
	if translationFieldMissing(data.Company.RegistrationStatusLabel, data.Company.RegistrationStatusLabelEN) {
		count++
	}
	if translationFieldMissing(data.Company.OrganizationFormLabel, data.Company.OrganizationFormLabelEN) {
		count++
	}
	if translationFieldMissing(data.Company.ResponseClass, data.Company.ResponseClassEN) {
		count++
	}
	if translationFieldMissing(data.Company.ActivityDescription, data.Company.ActivityDescriptionEN) {
		count++
	}
	if translationFieldMissing(data.Company.StatutoryPurpose, data.Company.StatutoryPurposeEN) {
		count++
	}
	for _, address := range data.Addresses {
		if translationFieldMissing(address.Country, address.CountryEN) {
			count++
		}
	}
	for _, industry := range data.Industries {
		if translationFieldMissing(industry.SourceLabel, industry.SourceLabelEN) {
			count++
		}
	}
	for _, website := range data.Websites {
		if translationFieldMissing(website.Title, website.TitleEN) {
			count++
		}
		if translationFieldMissing(website.Description, website.DescriptionEN) {
			count++
		}
	}
	for _, contact := range data.Contacts {
		if translationFieldMissing(contact.Label, contact.LabelEN) {
			count++
		}
	}
	for _, capital := range data.Capital {
		if translationFieldMissing(capital.CapitalType, capital.CapitalTypeEN) {
			count++
		}
	}
	for _, role := range data.Roles {
		if translationFieldMissing(role.RoleLabel, role.RoleLabelEN) {
			count++
		}
		if translationFieldMissing(role.RoleGroup, role.RoleGroupEN) {
			count++
		}
	}
	return count
}

func (data *CompanyData) applyTranslationByKey(key string, translatedText string) int32 {
	var applied int32
	if translationMissingFor(data.Company.ShortDescription, data.Company.ShortDescriptionEN, key) {
		data.Company.ShortDescriptionEN = translatedText
		applied++
	}
	if translationMissingFor(data.Company.Description, data.Company.DescriptionEN, key) {
		data.Company.DescriptionEN = translatedText
		applied++
	}
	if translationMissingFor(data.Company.RegistrationStatusLabel, data.Company.RegistrationStatusLabelEN, key) {
		data.Company.RegistrationStatusLabelEN = translatedText
		applied++
	}
	if translationMissingFor(data.Company.OrganizationFormLabel, data.Company.OrganizationFormLabelEN, key) {
		data.Company.OrganizationFormLabelEN = translatedText
		applied++
	}
	if translationMissingFor(data.Company.ResponseClass, data.Company.ResponseClassEN, key) {
		data.Company.ResponseClassEN = translatedText
		applied++
	}
	if translationMissingFor(data.Company.ActivityDescription, data.Company.ActivityDescriptionEN, key) {
		data.Company.ActivityDescriptionEN = translatedText
		applied++
	}
	if translationMissingFor(data.Company.StatutoryPurpose, data.Company.StatutoryPurposeEN, key) {
		data.Company.StatutoryPurposeEN = translatedText
		applied++
	}
	for idx := range data.Addresses {
		if translationMissingFor(data.Addresses[idx].Country, data.Addresses[idx].CountryEN, key) {
			data.Addresses[idx].CountryEN = translatedText
			applied++
		}
	}
	for idx := range data.Industries {
		if translationMissingFor(data.Industries[idx].SourceLabel, data.Industries[idx].SourceLabelEN, key) {
			data.Industries[idx].SourceLabelEN = translatedText
			applied++
		}
	}
	for idx := range data.Websites {
		if translationMissingFor(data.Websites[idx].Title, data.Websites[idx].TitleEN, key) {
			data.Websites[idx].TitleEN = translatedText
			applied++
		}
		if translationMissingFor(data.Websites[idx].Description, data.Websites[idx].DescriptionEN, key) {
			data.Websites[idx].DescriptionEN = translatedText
			applied++
		}
	}
	for idx := range data.Contacts {
		if translationMissingFor(data.Contacts[idx].Label, data.Contacts[idx].LabelEN, key) {
			data.Contacts[idx].LabelEN = translatedText
			applied++
		}
	}
	for idx := range data.Capital {
		if translationMissingFor(data.Capital[idx].CapitalType, data.Capital[idx].CapitalTypeEN, key) {
			data.Capital[idx].CapitalTypeEN = translatedText
			applied++
		}
	}
	for idx := range data.Roles {
		if translationMissingFor(data.Roles[idx].RoleLabel, data.Roles[idx].RoleLabelEN, key) {
			data.Roles[idx].RoleLabelEN = translatedText
			applied++
		}
		if translationMissingFor(data.Roles[idx].RoleGroup, data.Roles[idx].RoleGroupEN, key) {
			data.Roles[idx].RoleGroupEN = translatedText
			applied++
		}
	}
	return applied
}

func translationFieldMissing(sourceText string, translatedText string) bool {
	return normalizeTranslationText(sourceText) != "" && strings.TrimSpace(translatedText) == ""
}

func translationMissingFor(sourceText string, translatedText string, key string) bool {
	return translationFieldMissing(sourceText, translatedText) && translationTermKey(sourceText) == key
}

type translationTermCollector struct {
	seen  map[string]struct{}
	terms []TranslationTerm
}

func newTranslationTermCollector() translationTermCollector {
	return translationTermCollector{
		seen:  make(map[string]struct{}),
		terms: make([]TranslationTerm, 0),
	}
}

func (collector *translationTermCollector) addIfMissing(sourceText string, translatedText string) {
	normalized := normalizeTranslationText(sourceText)
	if normalized == "" || strings.TrimSpace(translatedText) != "" {
		return
	}
	key := translationTermKey(sourceText)
	if _, ok := collector.seen[key]; ok {
		return
	}
	collector.seen[key] = struct{}{}
	collector.terms = append(collector.terms, TranslationTerm{
		Key:            key,
		SourceText:     strings.TrimSpace(sourceText),
		NormalizedText: normalized,
	})
}

func translationTermKey(sourceText string) string {
	sum := sha256.Sum256([]byte(normalizeTranslationText(sourceText)))
	return hex.EncodeToString(sum[:])
}

func normalizeTranslationText(sourceText string) string {
	return strings.ToLower(strings.TrimSpace(sourceText))
}
