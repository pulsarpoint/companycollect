package sourceprofile

import (
	"bytes"
	"encoding/json"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
)

const (
	defaultCountryISO2    = "NO"
	sourceProfileVersion  = "brreg.source_profile.v1"
	defaultProfileTrigger = "manual"
)

var (
	numericTextPattern = regexp.MustCompile(`^-?[0-9]+([.][0-9]+)?$`)
	integerTextPattern = regexp.MustCompile(`^[0-9]+$`)
	naceCodePattern    = regexp.MustCompile(`[^0-9A-Z]`)
)

type Command struct {
	Trigger string
	Records []RawRecord
}

type RawRecord struct {
	ID                 uuid.UUID
	SourceNativeID     string
	OrganizationNumber string
	OrganizationName   string
	RegistrationStatus string
	Website            string
	CountryISO2        string
	SourceUpdatedAt    *time.Time
	RawPayload         json.RawMessage
	PayloadHash        string
}

type Batch struct {
	Companies  []CompanyRow
	Addresses  []AddressRow
	Industries []IndustryRow
	Websites   []WebsiteRow
	Domains    []DomainRow
	Contacts   []ContactRow
	Capital    []CapitalRow
}

type CompanyRow struct {
	RawRecordID                    uuid.UUID
	SourceNativeID                 string
	OrganizationNumber             string
	CountryISO2                    string
	OrganizationName               string
	OrganizationNameNormalized     string
	RegistrationStatus             string
	RegistrationStatusLabel        string
	LifecycleStatus                string
	OrganizationFormCode           string
	OrganizationFormLabel          string
	LanguageCode                   string
	ResponseClass                  string
	FoundedDate                    string
	UnitRegistryRegisteredAt       string
	EnterpriseRegistryRegisteredAt string
	VATRegistryRegisteredAt        string
	VATRegistryUnitRegisteredAt    string
	ArticlesDate                   string
	LastAnnualReportYear           string
	ActivityDescription            string
	StatutoryPurpose               string
	IsBankrupt                     *bool
	IsInGroup                      *bool
	IsUnderLiquidation             *bool
	IsForcedDissolution            *bool
	HasRegisteredEmployees         *bool
	InVATRegister                  *bool
	InBusinessRegister             *bool
	InVoluntaryRegister            *bool
	InFoundationRegister           *bool
	InPartyRegister                *bool
	SourceUpdatedAt                *time.Time
	PayloadHash                    string
	NormalizedPayload              json.RawMessage
	RawCompanyPayload              json.RawMessage
	Evidence                       json.RawMessage
	Metadata                       json.RawMessage
}

type AddressRow struct {
	OrganizationNumber string
	RawRecordID        uuid.UUID
	AddressType        string
	StreetLines        []string
	StreetText         string
	PostalCode         string
	City               string
	Municipality       string
	MunicipalityNumber string
	Country            string
	CountryCode        string
	FormattedAddress   string
	RawAddressPayload  json.RawMessage
	Evidence           json.RawMessage
}

type IndustryRow struct {
	OrganizationNumber string
	RawRecordID        uuid.UUID
	ClassificationType string
	SourceField        string
	Position           int16
	SourceCode         string
	SourceLabel        string
	NormalizedCode     string
	MappedNACECode     string
	MappingMethod      string
	IsPrimary          bool
	RawIndustryPayload json.RawMessage
	Evidence           json.RawMessage
}

type WebsiteRow struct {
	OrganizationNumber string
	RawRecordID        uuid.UUID
	URL                string
	NormalizedURL      string
	Host               string
	WebsiteType        string
	Source             string
	Status             string
	Confidence         int16
	IsPrimary          bool
	Evidence           json.RawMessage
}

type DomainRow struct {
	OrganizationNumber string
	RawRecordID        uuid.UUID
	WebsiteNormalized  string
	Domain             string
	NormalizedDomain   string
	RegistrableDomain  string
	DomainType         string
	Source             string
	Status             string
	Confidence         int16
	IsPrimary          bool
	BestSignal         string
	Evidence           json.RawMessage
}

type ContactRow struct {
	OrganizationNumber string
	RawRecordID        uuid.UUID
	ContactType        string
	Value              string
	NormalizedValue    string
	Label              string
	Source             string
	Status             string
	Confidence         int16
	IsPrimary          bool
	Evidence           json.RawMessage
}

type CapitalRow struct {
	OrganizationNumber string
	RawRecordID        uuid.UUID
	CapitalType        string
	OriginalAmountText string
	OriginalCurrency   string
	IntroducedAt       string
	ShareCountText     string
	RawCapitalPayload  json.RawMessage
	Evidence           json.RawMessage
}

func BuildBatch(command Command) (Batch, error) {
	trigger := strings.TrimSpace(command.Trigger)
	if trigger == "" {
		trigger = defaultProfileTrigger
	}
	batch := Batch{
		Companies:  make([]CompanyRow, 0, len(command.Records)),
		Addresses:  make([]AddressRow, 0, len(command.Records)*2),
		Industries: make([]IndustryRow, 0, len(command.Records)),
		Websites:   make([]WebsiteRow, 0, len(command.Records)),
		Domains:    make([]DomainRow, 0, len(command.Records)),
		Contacts:   make([]ContactRow, 0, len(command.Records)),
		Capital:    make([]CapitalRow, 0, len(command.Records)),
	}
	for _, record := range command.Records {
		if record.ID == uuid.Nil {
			return Batch{}, errors.New("raw record id is required")
		}
		if strings.TrimSpace(record.OrganizationNumber) == "" {
			return Batch{}, errors.New("organization number is required")
		}
		payload, err := decodePayload(record.RawPayload)
		if err != nil {
			return Batch{}, errors.Wrap(err, "decode brreg raw payload")
		}
		rawPayload := canonicalRawPayload(record.RawPayload)
		batch.Companies = append(batch.Companies, buildCompany(record, payload, rawPayload, trigger))
		batch.Addresses = append(batch.Addresses, buildAddresses(record, payload)...)
		batch.Industries = append(batch.Industries, buildIndustries(record, payload)...)
		websites := buildWebsites(record, payload)
		batch.Websites = append(batch.Websites, websites...)
		batch.Domains = append(batch.Domains, buildDomains(record, websites)...)
		batch.Contacts = append(batch.Contacts, buildContacts(record, payload)...)
		if capital, ok := buildCapital(record, payload); ok {
			batch.Capital = append(batch.Capital, capital)
		}
	}
	return batch, nil
}

func buildCompany(record RawRecord, payload map[string]any, rawPayload json.RawMessage, trigger string) CompanyRow {
	orgPayload := object(payload, "organisasjonsform")
	organizationName := firstNonEmpty(record.OrganizationName, stringValue(payload, "navn"), record.OrganizationNumber)
	registrationStatus := strings.TrimSpace(record.RegistrationStatus)
	return CompanyRow{
		RawRecordID:                    record.ID,
		SourceNativeID:                 firstNonEmpty(record.SourceNativeID, record.OrganizationNumber),
		OrganizationNumber:             strings.TrimSpace(record.OrganizationNumber),
		CountryISO2:                    firstNonEmpty(record.CountryISO2, defaultCountryISO2),
		OrganizationName:               organizationName,
		OrganizationNameNormalized:     strings.ToLower(organizationName),
		RegistrationStatus:             registrationStatus,
		RegistrationStatusLabel:        registrationStatusLabel(registrationStatus),
		LifecycleStatus:                lifecycleStatus(registrationStatus, payload),
		OrganizationFormCode:           stringValue(orgPayload, "kode"),
		OrganizationFormLabel:          stringValue(orgPayload, "beskrivelse"),
		LanguageCode:                   stringValue(payload, "maalform"),
		ResponseClass:                  stringValue(payload, "respons_klasse"),
		FoundedDate:                    validDateText(stringValue(payload, "stiftelsesdato")),
		UnitRegistryRegisteredAt:       validDateText(stringValue(payload, "registreringsdatoEnhetsregisteret")),
		EnterpriseRegistryRegisteredAt: validDateText(stringValue(payload, "registreringsdatoForetaksregisteret")),
		VATRegistryRegisteredAt:        validDateText(stringValue(payload, "registreringsdatoMerverdiavgiftsregisteret")),
		VATRegistryUnitRegisteredAt:    validDateText(stringValue(payload, "registreringsdatoMerverdiavgiftsregisteretEnhetsregisteret")),
		ArticlesDate:                   validDateText(stringValue(payload, "vedtektsdato")),
		LastAnnualReportYear:           validYearText(stringValue(payload, "sisteInnsendteAarsregnskap")),
		ActivityDescription:            joinedTextArray(payload, "aktivitet"),
		StatutoryPurpose:               joinedTextArray(payload, "vedtektsfestetFormaal"),
		IsBankrupt:                     boolPtr(payload, "konkurs"),
		IsInGroup:                      boolPtr(payload, "erIKonsern"),
		IsUnderLiquidation:             boolPtr(payload, "underAvvikling"),
		IsForcedDissolution:            boolPtr(payload, "underTvangsavviklingEllerTvangsopplosning"),
		HasRegisteredEmployees:         boolPtr(payload, "harRegistrertAntallAnsatte"),
		InVATRegister:                  boolPtr(payload, "registrertIMvaregisteret"),
		InBusinessRegister:             boolPtr(payload, "registrertIForetaksregisteret"),
		InVoluntaryRegister:            boolPtr(payload, "registrertIFrivillighetsregisteret"),
		InFoundationRegister:           boolPtr(payload, "registrertIStiftelsesregisteret"),
		InPartyRegister:                boolPtr(payload, "registrertIPartiregisteret"),
		SourceUpdatedAt:                record.SourceUpdatedAt,
		PayloadHash:                    record.PayloadHash,
		NormalizedPayload:              mustJSON(map[string]any{"source": "brreg", "version": sourceProfileVersion}),
		RawCompanyPayload:              rawPayload,
		Evidence:                       mustJSON(map[string]any{"source": "brreg_workflow.raw_records", "raw_record_id": record.ID.String()}),
		Metadata:                       mustJSON(map[string]any{"trigger": trigger}),
	}
}

func buildAddresses(record RawRecord, payload map[string]any) []AddressRow {
	addresses := make([]AddressRow, 0, 2)
	if address, ok := objectOk(payload, "forretningsadresse"); ok {
		addresses = append(addresses, buildAddress(record, "business", address))
	}
	if address, ok := objectOk(payload, "postadresse"); ok {
		addresses = append(addresses, buildAddress(record, "postal", address))
	}
	return addresses
}

func buildAddress(record RawRecord, addressType string, address map[string]any) AddressRow {
	streetLines := stringArray(address, "adresse")
	streetText := strings.Join(streetLines, ", ")
	postalCity := strings.TrimSpace(strings.Join(nonEmpty([]string{stringValue(address, "postnummer"), stringValue(address, "poststed")}), " "))
	formatted := strings.Join(nonEmpty([]string{streetText, postalCity, stringValue(address, "land")}), ", ")
	return AddressRow{
		OrganizationNumber: strings.TrimSpace(record.OrganizationNumber),
		RawRecordID:        record.ID,
		AddressType:        addressType,
		StreetLines:        streetLines,
		StreetText:         streetText,
		PostalCode:         stringValue(address, "postnummer"),
		City:               stringValue(address, "poststed"),
		Municipality:       stringValue(address, "kommune"),
		MunicipalityNumber: stringValue(address, "kommunenummer"),
		Country:            stringValue(address, "land"),
		CountryCode:        stringValue(address, "landkode"),
		FormattedAddress:   formatted,
		RawAddressPayload:  mustJSON(address),
		Evidence:           mustJSON(map[string]any{"source": "brreg_raw_payload", "field": addressType}),
	}
}

func buildIndustries(record RawRecord, payload map[string]any) []IndustryRow {
	sections := []struct {
		field              string
		classificationType string
		position           int16
	}{
		{field: "naeringskode1", classificationType: "industry", position: 1},
		{field: "naeringskode2", classificationType: "industry", position: 2},
		{field: "naeringskode3", classificationType: "industry", position: 3},
		{field: "hjelpeenhetskode", classificationType: "helper_unit", position: 1},
		{field: "institusjonellSektorkode", classificationType: "institutional_sector", position: 1},
	}
	industries := make([]IndustryRow, 0, len(sections))
	for _, section := range sections {
		rawSection, ok := objectOk(payload, section.field)
		if !ok {
			continue
		}
		sourceCode := stringValue(rawSection, "kode")
		if sourceCode == "" {
			continue
		}
		normalized := naceCodePattern.ReplaceAllString(strings.ToUpper(sourceCode), "")
		mappedCode, mappingMethod := mappedNACECode(section.classificationType, normalized)
		industries = append(industries, IndustryRow{
			OrganizationNumber: strings.TrimSpace(record.OrganizationNumber),
			RawRecordID:        record.ID,
			ClassificationType: section.classificationType,
			SourceField:        section.field,
			Position:           section.position,
			SourceCode:         sourceCode,
			SourceLabel:        stringValue(rawSection, "beskrivelse"),
			NormalizedCode:     normalized,
			MappedNACECode:     mappedCode,
			MappingMethod:      mappingMethod,
			IsPrimary:          section.classificationType == "industry" && section.position == 1,
			RawIndustryPayload: mustJSON(rawSection),
			Evidence:           mustJSON(map[string]any{"source": "brreg_raw_payload", "source_field": section.field, "normalized_source_code": normalized}),
		})
	}
	return industries
}

func buildWebsites(record RawRecord, payload map[string]any) []WebsiteRow {
	rawWebsite := firstNonEmpty(record.Website, stringValue(payload, "hjemmeside"))
	if rawWebsite == "" {
		return nil
	}
	normalizedURL := normalizeURL(rawWebsite)
	if normalizedURL == "" {
		return nil
	}
	host := hostFromURL(normalizedURL)
	if host == "" {
		return nil
	}
	return []WebsiteRow{{
		OrganizationNumber: strings.TrimSpace(record.OrganizationNumber),
		RawRecordID:        record.ID,
		URL:                normalizedURL,
		NormalizedURL:      strings.TrimRight(strings.ToLower(normalizedURL), "/"),
		Host:               host,
		WebsiteType:        websiteType(host),
		Source:             "brreg",
		Status:             "active",
		Confidence:         90,
		IsPrimary:          true,
		Evidence:           mustJSON(map[string]any{"source": "brreg_website"}),
	}}
}

func buildDomains(record RawRecord, websites []WebsiteRow) []DomainRow {
	domains := make([]DomainRow, 0, len(websites))
	for _, website := range websites {
		domain := strings.TrimPrefix(website.Host, "www.")
		if domain == "" || !strings.Contains(domain, ".") || excludedOfficialDomain(domain) {
			continue
		}
		domains = append(domains, DomainRow{
			OrganizationNumber: strings.TrimSpace(record.OrganizationNumber),
			RawRecordID:        record.ID,
			WebsiteNormalized:  website.NormalizedURL,
			Domain:             domain,
			NormalizedDomain:   domain,
			RegistrableDomain:  domain,
			DomainType:         "official",
			Source:             "brreg_website",
			Status:             "active",
			Confidence:         90,
			IsPrimary:          true,
			BestSignal:         "brreg_website",
			Evidence:           mustJSON(map[string]any{"source": "brreg_website"}),
		})
	}
	return domains
}

func buildContacts(record RawRecord, payload map[string]any) []ContactRow {
	definitions := []struct {
		field       string
		contactType string
		normalize   func(string) string
	}{
		{field: "telefon", contactType: "phone", normalize: normalizePhone},
		{field: "mobil", contactType: "mobile", normalize: normalizePhone},
		{field: "epostadresse", contactType: "email", normalize: func(value string) string { return strings.ToLower(value) }},
	}
	contacts := make([]ContactRow, 0, len(definitions))
	for _, definition := range definitions {
		value := stringValue(payload, definition.field)
		if value == "" {
			continue
		}
		contacts = append(contacts, ContactRow{
			OrganizationNumber: strings.TrimSpace(record.OrganizationNumber),
			RawRecordID:        record.ID,
			ContactType:        definition.contactType,
			Value:              value,
			NormalizedValue:    definition.normalize(value),
			Label:              definition.contactType,
			Source:             "brreg",
			Status:             "active",
			Confidence:         90,
			IsPrimary:          true,
			Evidence:           mustJSON(map[string]any{"source": "brreg_raw_payload"}),
		})
	}
	return contacts
}

func buildCapital(record RawRecord, payload map[string]any) (CapitalRow, bool) {
	capital, ok := objectOk(payload, "kapital")
	if !ok {
		return CapitalRow{}, false
	}
	amount := numericText(stringValue(capital, "belop"))
	shareCount := integerText(stringValue(capital, "antallAksjer"))
	return CapitalRow{
		OrganizationNumber: strings.TrimSpace(record.OrganizationNumber),
		RawRecordID:        record.ID,
		CapitalType:        stringValue(capital, "type"),
		OriginalAmountText: amount,
		OriginalCurrency:   stringValue(capital, "valuta"),
		IntroducedAt:       validDateText(stringValue(capital, "innfortDato")),
		ShareCountText:     shareCount,
		RawCapitalPayload:  mustJSON(capital),
		Evidence:           mustJSON(map[string]any{"source": "brreg_raw_payload", "field": "kapital"}),
	}, true
}

func decodePayload(raw json.RawMessage) (map[string]any, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return map[string]any{}, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var payload map[string]any
	if err := decoder.Decode(&payload); err != nil {
		return nil, err
	}
	if payload == nil {
		payload = map[string]any{}
	}
	return payload, nil
}

func canonicalRawPayload(raw json.RawMessage) json.RawMessage {
	if len(bytes.TrimSpace(raw)) == 0 {
		return json.RawMessage(`{}`)
	}
	return raw
}

func object(payload map[string]any, key string) map[string]any {
	value, _ := objectOk(payload, key)
	return value
}

func objectOk(payload map[string]any, key string) (map[string]any, bool) {
	raw, ok := payload[key]
	if !ok || raw == nil {
		return nil, false
	}
	object, ok := raw.(map[string]any)
	return object, ok
}

func stringValue(payload map[string]any, key string) string {
	raw, ok := payload[key]
	if !ok || raw == nil {
		return ""
	}
	switch value := raw.(type) {
	case string:
		return strings.TrimSpace(value)
	case json.Number:
		return strings.TrimSpace(value.String())
	case float64:
		return strings.TrimRight(strings.TrimRight(strings.TrimSpace(formatFloat(value)), "0"), ".")
	case bool:
		if value {
			return "true"
		}
		return "false"
	default:
		return ""
	}
}

func stringArray(payload map[string]any, key string) []string {
	raw, ok := payload[key]
	if !ok || raw == nil {
		return nil
	}
	values, ok := raw.([]any)
	if !ok {
		return nil
	}
	result := make([]string, 0, len(values))
	for _, value := range values {
		text, ok := value.(string)
		if !ok {
			continue
		}
		text = strings.TrimSpace(text)
		if text != "" {
			result = append(result, text)
		}
	}
	return result
}

func joinedTextArray(payload map[string]any, key string) string {
	return strings.Join(stringArray(payload, key), "\n")
}

func boolPtr(payload map[string]any, key string) *bool {
	raw, ok := payload[key]
	if !ok || raw == nil {
		return nil
	}
	value, ok := raw.(bool)
	if !ok {
		return nil
	}
	return &value
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func nonEmpty(values []string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			result = append(result, value)
		}
	}
	return result
}

func registrationStatusLabel(status string) string {
	switch status {
	case "active":
		return "active"
	case "inactive":
		return "inactive"
	default:
		return status
	}
}

func lifecycleStatus(registrationStatus string, payload map[string]any) string {
	if value := boolPtr(payload, "konkurs"); value != nil && *value {
		return "bankrupt"
	}
	if value := boolPtr(payload, "underTvangsavviklingEllerTvangsopplosning"); value != nil && *value {
		return "forced_dissolution"
	}
	if value := boolPtr(payload, "underAvvikling"); value != nil && *value {
		return "liquidating"
	}
	switch registrationStatus {
	case "active":
		return "active"
	case "inactive":
		return "inactive"
	default:
		return "unknown"
	}
}

func validDateText(value string) string {
	if len(value) != len("2006-01-02") {
		return ""
	}
	if _, err := time.Parse("2006-01-02", value); err != nil {
		return ""
	}
	return value
}

func validYearText(value string) string {
	if len(value) != 4 {
		return ""
	}
	for _, char := range value {
		if char < '0' || char > '9' {
			return ""
		}
	}
	return value
}

func mappedNACECode(classificationType string, normalized string) (string, string) {
	if classificationType != "industry" && classificationType != "helper_unit" {
		return "", ""
	}
	if len(normalized) == 5 && integerTextPattern.MatchString(normalized) {
		return normalized[:2] + "." + normalized[2:4], "sn_level_5_to_nace_class"
	}
	if len(normalized) == 4 && integerTextPattern.MatchString(normalized) {
		return normalized[:2] + "." + normalized[2:4], "nace_exact"
	}
	return "", ""
}

func normalizeURL(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if !strings.Contains(value, "://") {
		value = "https://" + value
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" {
		return ""
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	parsed.Host = strings.ToLower(parsed.Host)
	return parsed.String()
}

func hostFromURL(value string) string {
	parsed, err := url.Parse(value)
	if err != nil {
		return ""
	}
	return strings.ToLower(parsed.Host)
}

func websiteType(host string) string {
	switch strings.TrimPrefix(strings.ToLower(host), "www.") {
	case "facebook.com", "instagram.com", "linkedin.com", "x.com", "twitter.com", "youtube.com":
		return "social_profile"
	default:
		return "official_site"
	}
}

func excludedOfficialDomain(domain string) bool {
	switch strings.ToLower(domain) {
	case "facebook.com", "instagram.com", "linkedin.com", "x.com", "twitter.com", "youtube.com",
		"proff.no", "brreg.no", "gulesider.no", "1881.no", "yra.no":
		return true
	default:
		return false
	}
}

func normalizePhone(value string) string {
	var builder strings.Builder
	for _, char := range value {
		if (char >= '0' && char <= '9') || char == '+' {
			builder.WriteRune(char)
		}
	}
	return builder.String()
}

func numericText(value string) string {
	if numericTextPattern.MatchString(value) {
		return value
	}
	return ""
}

func integerText(value string) string {
	if integerTextPattern.MatchString(value) {
		return value
	}
	return ""
}

func formatFloat(value float64) string {
	data, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return string(data)
}

func mustJSON(value any) json.RawMessage {
	data, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return data
}
