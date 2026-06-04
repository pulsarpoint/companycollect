package sourceprofile

import (
	"bytes"
	"encoding/json"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
)

const (
	defaultCountryISO2    = "EE"
	sourceProfileVersion  = "ariregister.source_profile.v1"
	defaultProfileTrigger = "manual"
)

var (
	numericTextPattern = regexp.MustCompile(`^-?[0-9]+([.][0-9]+)?$`)
	integerTextPattern = regexp.MustCompile(`^-?[0-9]+$`)
)

type Command struct {
	Trigger string
	Records []RawRecord
}

type RawRecord struct {
	ID                 uuid.UUID
	SourceNativeID     string
	RegistryCode       string
	LegalName          string
	RegistrationStatus string
	LegalForm          string
	Website            string
	Email              string
	Phone              string
	CountryISO2        string
	SourceUpdatedAt    *time.Time
	RawPayload         json.RawMessage
	PayloadHash        string
}

type Batch struct {
	Companies            []CompanyRow
	CompanyNames         []CompanyNameRow
	CompanyStatuses      []CompanyStatusRow
	LegalForms           []LegalFormRow
	Addresses            []AddressRow
	Contacts             []ContactRow
	Websites             []WebsiteRow
	Domains              []DomainRow
	Industries           []IndustryRow
	Capital              []CapitalRow
	FinancialYearPeriods []FinancialYearPeriodRow
	AnnualReports        []AnnualReportRow
	Articles             []ArticleRow
	RegistryNotes        []RegistryNoteRow
}

type CompanyRow struct {
	RawRecordID                                uuid.UUID
	RegistryCode                               string
	SourceNativeID                             string
	CountryISO2                                string
	LegalName                                  string
	LegalNameNormalized                        string
	LegalNameEn                                string
	RegistrationStatus                         string
	RegistrationStatusLabel                    string
	RegistrationStatusLabelEn                  string
	LifecycleStatus                            string
	LegalFormCode                              string
	LegalFormNumber                            *int
	LegalFormLabel                             string
	LegalFormLabelEn                           string
	LegalFormSubtype                           string
	LegalFormSubtypeLabel                      string
	LegalFormSubtypeLabelEn                    string
	RegionCode                                 *int
	RegionLabel                                string
	RegionLabelEn                              string
	RegionLabelLong                            string
	RegionLabelLongEn                          string
	ActiveLabel                                string
	ActiveLabelEn                              string
	FirstRegisteredOn                          string
	DeletedOn                                  string
	EVKSRegisteredAt                           string
	HasMissingBeneficialOwnerDiscrepancyNotice *bool
	FoundedWithoutContribution                 *bool
	WaivedFormRequirements                     *bool
	IsAccountingRequired                       *bool
	ReportsBeneficialOwners                    *bool
	IsActive                                   *bool
	LastAnnualReportYear                       *int
	EmployeeCount                              *int
	EmployeeCountSource                        string
	EmployeeBand                               string
	SourceUpdatedAt                            *time.Time
	PayloadHash                                string
	ProfileVersion                             string
	RowStatus                                  string
	NormalizedPayload                          json.RawMessage
	RawCompanyPayload                          json.RawMessage
	Evidence                                   json.RawMessage
	Metadata                                   json.RawMessage
}

type CompanyNameRow struct {
	RegistryCode   string
	RawRecordID    uuid.UUID
	SourceEntryID  *int64
	CardRegion     *int
	CardNumber     *int
	CardType       string
	EntryNumber    *int
	Name           string
	NameEn         string
	StartedOn      string
	EndedOn        string
	RawNamePayload json.RawMessage
	Evidence       json.RawMessage
	Metadata       json.RawMessage
}

type CompanyStatusRow struct {
	RegistryCode     string
	RawRecordID      uuid.UUID
	CardRegion       *int
	CardNumber       *int
	CardType         string
	EntryNumber      *int
	StatusCode       string
	StatusLabel      string
	StatusLabelEn    string
	StartedOn        string
	RawStatusPayload json.RawMessage
	Evidence         json.RawMessage
	Metadata         json.RawMessage
}

type LegalFormRow struct {
	RegistryCode            string
	RawRecordID             uuid.UUID
	SourceEntryID           *int64
	CardRegion              *int
	CardNumber              *int
	CardType                string
	EntryNumber             *int
	LegalFormCode           string
	LegalFormNumber         *int
	LegalFormLabel          string
	LegalFormLabelEn        string
	LegalFormSubtype        string
	LegalFormSubtypeLabel   string
	LegalFormSubtypeLabelEn string
	StartedOn               string
	EndedOn                 string
	RawLegalFormPayload     json.RawMessage
	Evidence                json.RawMessage
	Metadata                json.RawMessage
}

type AddressRow struct {
	RegistryCode                string
	RawRecordID                 uuid.UUID
	SourceEntryID               *int64
	AddressType                 string
	CountryCode                 string
	CountryLabel                string
	CountryLabelEn              string
	EHAKCode                    string
	EHAKName                    string
	EHAKNameEn                  string
	StreetText                  string
	StreetTextEn                string
	PostalCode                  string
	ADSOID                      string
	ADRID                       *int64
	NormalizedFullAddress       string
	NormalizedFullAddressEn     string
	NormalizedFullAddressDetail string
	CodeAddress                 string
	ADOBID                      string
	ADSType                     string
	StartedOn                   string
	EndedOn                     string
	RawAddressPayload           json.RawMessage
	Evidence                    json.RawMessage
	Metadata                    json.RawMessage
}

type ContactRow struct {
	RegistryCode       string
	RawRecordID        uuid.UUID
	SourceEntryID      *int64
	ContactType        string
	ContactTypeLabel   string
	ContactTypeLabelEn string
	Value              string
	NormalizedValue    string
	Source             string
	Status             string
	IsPrimary          bool
	EndedOn            string
	Evidence           json.RawMessage
	RawContactPayload  json.RawMessage
	Metadata           json.RawMessage
}

type WebsiteRow struct {
	RegistryCode  string
	RawRecordID   uuid.UUID
	URL           string
	NormalizedURL string
	Host          string
	Path          string
	WebsiteType   string
	Source        string
	Status        string
	Confidence    int16
	IsPrimary     bool
	Title         string
	TitleEn       string
	Description   string
	DescriptionEn string
	Evidence      json.RawMessage
	Metadata      json.RawMessage
}

type DomainRow struct {
	RegistryCode      string
	RawRecordID       uuid.UUID
	WebsiteNormalized string
	Domain            string
	NormalizedDomain  string
	RegistrableDomain string
	DomainType        string
	Source            string
	Status            string
	Confidence        int16
	IsPrimary         bool
	BestSignal        string
	Evidence          json.RawMessage
	Metadata          json.RawMessage
}

type IndustryRow struct {
	RegistryCode        string
	RawRecordID         uuid.UUID
	SourceEntryID       *int64
	ClassificationType  string
	SourceField         string
	Position            int16
	EMTAKCode           string
	EMTAKLabel          string
	EMTAKLabelEn        string
	EMTAKVersion        *int
	EMTAKVersionLabel   string
	EMTAKVersionLabelEn string
	NACECode            string
	NACERevision        string
	NACETitle           string
	NACETitleEn         string
	MappingMethod       string
	MappingConfidence   *float64
	IsPrimary           bool
	StartedOn           string
	EndedOn             string
	RawIndustryPayload  json.RawMessage
	Evidence            json.RawMessage
	Metadata            json.RawMessage
}

type CapitalRow struct {
	RegistryCode           string
	RawRecordID            uuid.UUID
	SourceEntryID          *int64
	CapitalAmount          string
	CapitalCurrency        string
	CapitalCurrencyLabel   string
	CapitalCurrencyLabelEn string
	IntroducedOn           string
	EndedOn                string
	RawCapitalPayload      json.RawMessage
	Evidence               json.RawMessage
	Metadata               json.RawMessage
}

type FinancialYearPeriodRow struct {
	RegistryCode        string
	RawRecordID         uuid.UUID
	SourceEntryID       *int64
	PeriodStartMonthDay string
	PeriodEndMonthDay   string
	StartedOn           string
	EndedOn             string
	RawPeriodPayload    json.RawMessage
	Evidence            json.RawMessage
	Metadata            json.RawMessage
}

type AnnualReportRow struct {
	RegistryCode           string
	RawRecordID            uuid.UUID
	SourceEntryID          *int64
	FiscalYear             *int
	PeriodStart            string
	PeriodEnd              string
	EmployeeCount          *int
	ReportAddress          string
	ReportAddressEn        string
	ActivityEMTAKCode      string
	ActivityLabel          string
	ActivityLabelEn        string
	ActivityVersion        string
	ActivityVersionLabel   string
	ActivityVersionLabelEn string
	ActivityNACECode       string
	RawReportPayload       json.RawMessage
	Evidence               json.RawMessage
	Metadata               json.RawMessage
}

type ArticleRow struct {
	RegistryCode          string
	RawRecordID           uuid.UUID
	SourceEntryID         *int64
	ConfirmedOn           string
	ChangedOn             string
	Explanation           string
	ExplanationEn         string
	ContainsSpecialRights *bool
	StartedOn             string
	EndedOn               string
	RawArticlesPayload    json.RawMessage
	Evidence              json.RawMessage
	Metadata              json.RawMessage
}

type RegistryNoteRow struct {
	RegistryCode    string
	RawRecordID     uuid.UUID
	SourceEntryID   *int64
	CardRegion      *int
	CardNumber      *int
	CardType        string
	EntryNumber     *int
	ColumnNumber    *int
	NoteType        string
	NoteTypeLabel   string
	NoteTypeLabelEn string
	NoteText        string
	NoteTextEn      string
	StartedOn       string
	EndedOn         string
	RawNotePayload  json.RawMessage
	Evidence        json.RawMessage
	Metadata        json.RawMessage
}

type cardFields struct {
	SourceEntryID *int64
	CardRegion    *int
	CardNumber    *int
	CardType      string
	EntryNumber   *int
	StartedOn     string
	EndedOn       string
}

func BuildBatch(command Command) (Batch, error) {
	trigger := strings.TrimSpace(command.Trigger)
	if trigger == "" {
		trigger = defaultProfileTrigger
	}
	batch := Batch{
		Companies:            make([]CompanyRow, 0, len(command.Records)),
		CompanyNames:         make([]CompanyNameRow, 0, len(command.Records)),
		CompanyStatuses:      make([]CompanyStatusRow, 0, len(command.Records)),
		LegalForms:           make([]LegalFormRow, 0, len(command.Records)),
		Addresses:            make([]AddressRow, 0, len(command.Records)),
		Contacts:             make([]ContactRow, 0, len(command.Records)),
		Websites:             make([]WebsiteRow, 0, len(command.Records)),
		Domains:              make([]DomainRow, 0, len(command.Records)),
		Industries:           make([]IndustryRow, 0, len(command.Records)),
		Capital:              make([]CapitalRow, 0, len(command.Records)),
		FinancialYearPeriods: make([]FinancialYearPeriodRow, 0, len(command.Records)),
		AnnualReports:        make([]AnnualReportRow, 0, len(command.Records)),
		Articles:             make([]ArticleRow, 0, len(command.Records)),
		RegistryNotes:        make([]RegistryNoteRow, 0, len(command.Records)),
	}
	for _, record := range command.Records {
		if record.ID == uuid.Nil {
			return Batch{}, errors.New("raw record id is required")
		}
		payload, err := decodePayload(record.RawPayload)
		if err != nil {
			return Batch{}, errors.Wrap(err, "decode ariregister raw payload")
		}
		generalData := object(payload, "yldandmed")
		registryCode := firstNonEmpty(stringValue(payload, "ariregistri_kood"), record.RegistryCode)
		if registryCode == "" {
			return Batch{}, errors.New("registry code is required")
		}
		rawPayload := canonicalRawPayload(record.RawPayload)
		metadata := mustJSON(map[string]any{"trigger": trigger})

		annualReports := buildAnnualReports(record, registryCode, generalData, metadata)
		contacts := buildContacts(record, registryCode, generalData, metadata)
		websites := buildWebsites(record, registryCode, contacts, metadata)

		batch.Companies = append(batch.Companies, buildCompany(record, registryCode, payload, generalData, rawPayload, annualReports, trigger))
		batch.CompanyNames = append(batch.CompanyNames, buildCompanyNames(record, registryCode, generalData, metadata)...)
		batch.CompanyStatuses = append(batch.CompanyStatuses, buildCompanyStatuses(record, registryCode, generalData, metadata)...)
		batch.LegalForms = append(batch.LegalForms, buildLegalForms(record, registryCode, generalData, metadata)...)
		batch.Addresses = append(batch.Addresses, buildAddresses(record, registryCode, generalData, metadata)...)
		batch.Contacts = append(batch.Contacts, contacts...)
		batch.Websites = append(batch.Websites, websites...)
		batch.Domains = append(batch.Domains, buildDomains(record, registryCode, websites, metadata)...)
		batch.Industries = append(batch.Industries, buildIndustries(record, registryCode, generalData, metadata)...)
		batch.Capital = append(batch.Capital, buildCapital(record, registryCode, generalData, metadata)...)
		batch.FinancialYearPeriods = append(batch.FinancialYearPeriods, buildFinancialYearPeriods(record, registryCode, generalData, metadata)...)
		batch.AnnualReports = append(batch.AnnualReports, annualReports...)
		batch.Articles = append(batch.Articles, buildArticles(record, registryCode, generalData, metadata)...)
		batch.RegistryNotes = append(batch.RegistryNotes, buildRegistryNotes(record, registryCode, generalData, metadata)...)
	}
	return batch, nil
}

func buildCompany(record RawRecord, registryCode string, payload, generalData map[string]any, rawPayload json.RawMessage, annualReports []AnnualReportRow, trigger string) CompanyRow {
	legalName := firstNonEmpty(stringValue(payload, "nimi"), record.LegalName, registryCode)
	activeLabel := stringValue(generalData, "tegutseb_tekstina")
	isActive := boolPtr(generalData, "tegutseb")
	if isActive == nil {
		isActive = estonianYesNoPtr(activeLabel)
	}
	lastReportYear, employeeCount := latestAnnualReportFacts(annualReports)
	return CompanyRow{
		RawRecordID:             record.ID,
		RegistryCode:            registryCode,
		SourceNativeID:          firstNonEmpty(registryCode, record.SourceNativeID),
		CountryISO2:             strings.ToUpper(firstNonEmpty(record.CountryISO2, defaultCountryISO2)),
		LegalName:               legalName,
		LegalNameNormalized:     strings.ToLower(legalName),
		RegistrationStatus:      firstNonEmpty(stringValue(generalData, "staatus"), record.RegistrationStatus),
		RegistrationStatusLabel: firstNonEmpty(stringValue(generalData, "staatus_tekstina"), record.RegistrationStatus),
		LifecycleStatus:         lifecycleStatus(stringValue(generalData, "staatus"), estonianDateText(stringValue(generalData, "kustutamise_kpv")), isActive),
		LegalFormCode:           stringValue(generalData, "oiguslik_vorm"),
		LegalFormNumber:         intPtr(generalData, "oiguslik_vorm_nr"),
		LegalFormLabel:          firstNonEmpty(stringValue(generalData, "oiguslik_vorm_tekstina"), record.LegalForm),
		LegalFormSubtype:        stringValue(generalData, "oigusliku_vormi_alaliik"),
		LegalFormSubtypeLabel:   stringValue(generalData, "oigusliku_vormi_alaliik_tekstina"),
		RegionCode:              intPtr(generalData, "piirkond"),
		RegionLabel:             stringValue(generalData, "piirkond_tekstina"),
		RegionLabelLong:         stringValue(generalData, "piirkond_tekstina_pikk"),
		ActiveLabel:             activeLabel,
		FirstRegisteredOn:       estonianDateText(stringValue(generalData, "esmaregistreerimise_kpv")),
		DeletedOn:               estonianDateText(stringValue(generalData, "kustutamise_kpv")),
		EVKSRegisteredAt:        estonianDateText(stringValue(generalData, "evks_registreeritud_kande_kpv")),
		HasMissingBeneficialOwnerDiscrepancyNotice: boolPtr(generalData, "lahknevusteade_puudumisest"),
		FoundedWithoutContribution:                 boolPtr(generalData, "asutatud_sissemakset_tegemata"),
		WaivedFormRequirements:                     boolPtr(generalData, "loobunud_vorminouetest"),
		IsAccountingRequired:                       boolPtr(generalData, "on_raamatupidamiskohustuslane"),
		ReportsBeneficialOwners:                    boolPtr(generalData, "esitab_kasusaajad"),
		IsActive:                                   isActive,
		LastAnnualReportYear:                       lastReportYear,
		EmployeeCount:                              employeeCount,
		EmployeeCountSource:                        employeeCountSource(employeeCount),
		SourceUpdatedAt:                            record.SourceUpdatedAt,
		PayloadHash:                                record.PayloadHash,
		ProfileVersion:                             sourceProfileVersion,
		RowStatus:                                  "active",
		NormalizedPayload:                          mustJSON(map[string]any{"source": "ariregister", "version": sourceProfileVersion}),
		RawCompanyPayload:                          rawPayload,
		Evidence:                                   mustJSON(map[string]any{"source": "ariregister_workflow.raw_records", "raw_record_id": record.ID.String()}),
		Metadata:                                   mustJSON(map[string]any{"trigger": trigger}),
	}
}

func buildCompanyNames(record RawRecord, registryCode string, generalData map[string]any, metadata json.RawMessage) []CompanyNameRow {
	items := objectArray(generalData, "arinimed")
	rows := make([]CompanyNameRow, 0, len(items))
	for _, item := range items {
		name := firstNonEmpty(stringValue(item, "sisu"), stringValue(item, "nimi"), stringValue(item, "arinimi"))
		if name == "" {
			continue
		}
		card := commonCardFields(item)
		rows = append(rows, CompanyNameRow{
			RegistryCode:   registryCode,
			RawRecordID:    record.ID,
			SourceEntryID:  card.SourceEntryID,
			CardRegion:     card.CardRegion,
			CardNumber:     card.CardNumber,
			CardType:       card.CardType,
			EntryNumber:    card.EntryNumber,
			Name:           name,
			StartedOn:      card.StartedOn,
			EndedOn:        card.EndedOn,
			RawNamePayload: mustJSON(item),
			Evidence:       rowEvidence(record, "arinimed", item),
			Metadata:       metadata,
		})
	}
	return rows
}

func buildCompanyStatuses(record RawRecord, registryCode string, generalData map[string]any, metadata json.RawMessage) []CompanyStatusRow {
	items := objectArray(generalData, "staatused")
	rows := make([]CompanyStatusRow, 0, len(items))
	for _, item := range items {
		statusCode := stringValue(item, "staatus")
		if statusCode == "" {
			continue
		}
		card := commonCardFields(item)
		rows = append(rows, CompanyStatusRow{
			RegistryCode:     registryCode,
			RawRecordID:      record.ID,
			CardRegion:       card.CardRegion,
			CardNumber:       card.CardNumber,
			CardType:         card.CardType,
			EntryNumber:      card.EntryNumber,
			StatusCode:       statusCode,
			StatusLabel:      stringValue(item, "staatus_tekstina"),
			StartedOn:        card.StartedOn,
			RawStatusPayload: mustJSON(item),
			Evidence:         rowEvidence(record, "staatused", item),
			Metadata:         metadata,
		})
	}
	return rows
}

func buildLegalForms(record RawRecord, registryCode string, generalData map[string]any, metadata json.RawMessage) []LegalFormRow {
	items := objectArray(generalData, "oiguslikud_vormid")
	rows := make([]LegalFormRow, 0, len(items))
	for _, item := range items {
		card := commonCardFields(item)
		rows = append(rows, LegalFormRow{
			RegistryCode:          registryCode,
			RawRecordID:           record.ID,
			SourceEntryID:         card.SourceEntryID,
			CardRegion:            card.CardRegion,
			CardNumber:            card.CardNumber,
			CardType:              card.CardType,
			EntryNumber:           card.EntryNumber,
			LegalFormCode:         firstNonEmpty(stringValue(item, "sisu"), stringValue(item, "oiguslik_vorm")),
			LegalFormNumber:       firstIntPtr(intPtr(item, "sisu_nr"), intPtr(item, "oiguslik_vorm_nr")),
			LegalFormLabel:        firstNonEmpty(stringValue(item, "sisu_tekstina"), stringValue(item, "oiguslik_vorm_tekstina")),
			LegalFormSubtype:      stringValue(item, "oigusliku_vormi_alaliik"),
			LegalFormSubtypeLabel: stringValue(item, "oigusliku_vormi_alaliik_tekstina"),
			StartedOn:             card.StartedOn,
			EndedOn:               card.EndedOn,
			RawLegalFormPayload:   mustJSON(item),
			Evidence:              rowEvidence(record, "oiguslikud_vormid", item),
			Metadata:              metadata,
		})
	}
	return rows
}

func buildAddresses(record RawRecord, registryCode string, generalData map[string]any, metadata json.RawMessage) []AddressRow {
	items := objectArray(generalData, "aadressid")
	rows := make([]AddressRow, 0, len(items))
	for _, item := range items {
		card := commonCardFields(item)
		rows = append(rows, AddressRow{
			RegistryCode:                registryCode,
			RawRecordID:                 record.ID,
			SourceEntryID:               card.SourceEntryID,
			AddressType:                 "registered",
			CountryCode:                 stringValue(item, "riik"),
			CountryLabel:                stringValue(item, "riik_tekstina"),
			EHAKCode:                    stringValue(item, "ehak"),
			EHAKName:                    stringValue(item, "ehak_nimetus"),
			StreetText:                  stringValue(item, "tanav_maja_korter"),
			PostalCode:                  stringValue(item, "postiindeks"),
			ADSOID:                      stringValue(item, "aadress_ads__ads_oid"),
			ADRID:                       int64Ptr(item, "aadress_ads__adr_id"),
			NormalizedFullAddress:       stringValue(item, "aadress_ads__ads_normaliseeritud_taisaadress"),
			NormalizedFullAddressDetail: stringValue(item, "aadress_ads__ads_normaliseeritud_taisaadress_tapsustus"),
			CodeAddress:                 stringValue(item, "aadress_ads__koodaadress"),
			ADOBID:                      stringValue(item, "aadress_ads__adob_id"),
			ADSType:                     stringValue(item, "aadress_ads__tyyp"),
			StartedOn:                   card.StartedOn,
			EndedOn:                     card.EndedOn,
			RawAddressPayload:           mustJSON(item),
			Evidence:                    rowEvidence(record, "aadressid", item),
			Metadata:                    metadata,
		})
	}
	return rows
}

func buildContacts(record RawRecord, registryCode string, generalData map[string]any, metadata json.RawMessage) []ContactRow {
	items := objectArray(generalData, "sidevahendid")
	contacts := make([]ContactRow, 0, len(items)+3)
	seen := map[string]struct{}{}
	for _, item := range items {
		value := stringValue(item, "sisu")
		if value == "" {
			continue
		}
		contactType := contactTypeFromSource(stringValue(item, "liik"))
		normalized := normalizeContactValue(contactType, value)
		if normalized == "" {
			continue
		}
		card := commonCardFields(item)
		row := ContactRow{
			RegistryCode:      registryCode,
			RawRecordID:       record.ID,
			SourceEntryID:     card.SourceEntryID,
			ContactType:       contactType,
			ContactTypeLabel:  stringValue(item, "liik_tekstina"),
			Value:             value,
			NormalizedValue:   normalized,
			Source:            "ariregister",
			Status:            "active",
			IsPrimary:         !hasContactType(contacts, contactType),
			EndedOn:           card.EndedOn,
			Evidence:          rowEvidence(record, "sidevahendid", item),
			RawContactPayload: mustJSON(item),
			Metadata:          metadata,
		}
		if appendContact(&contacts, seen, row) {
			continue
		}
	}
	appendRawContact(&contacts, seen, record, registryCode, "email", record.Email, metadata)
	appendRawContact(&contacts, seen, record, registryCode, "phone", record.Phone, metadata)
	appendRawContact(&contacts, seen, record, registryCode, "website", record.Website, metadata)
	return contacts
}

func buildWebsites(record RawRecord, registryCode string, contacts []ContactRow, metadata json.RawMessage) []WebsiteRow {
	websites := make([]WebsiteRow, 0, len(contacts))
	seen := map[string]struct{}{}
	for _, contact := range contacts {
		if contact.ContactType != "website" {
			continue
		}
		normalizedURL := normalizeURL(contact.Value)
		if normalizedURL == "" {
			continue
		}
		host := hostFromURL(normalizedURL)
		if host == "" {
			continue
		}
		normalizedTrimmed := normalizedURLText(normalizedURL)
		if _, ok := seen[normalizedTrimmed]; ok {
			continue
		}
		seen[normalizedTrimmed] = struct{}{}
		websites = append(websites, WebsiteRow{
			RegistryCode:  registryCode,
			RawRecordID:   record.ID,
			URL:           normalizedURL,
			NormalizedURL: normalizedTrimmed,
			Host:          host,
			Path:          pathFromURL(normalizedURL),
			WebsiteType:   websiteType(host),
			Source:        "ariregister_contact",
			Status:        "active",
			Confidence:    90,
			IsPrimary:     len(websites) == 0,
			Evidence:      mustJSON(map[string]any{"source": "ariregister_contact", "value": contact.Value}),
			Metadata:      metadata,
		})
	}
	return websites
}

func buildDomains(record RawRecord, registryCode string, websites []WebsiteRow, metadata json.RawMessage) []DomainRow {
	domains := make([]DomainRow, 0, len(websites))
	seen := map[string]struct{}{}
	for _, website := range websites {
		domain := strings.TrimPrefix(strings.ToLower(website.Host), "www.")
		if domain == "" || !strings.Contains(domain, ".") || excludedOfficialDomain(domain) {
			continue
		}
		if _, ok := seen[domain]; ok {
			continue
		}
		seen[domain] = struct{}{}
		domains = append(domains, DomainRow{
			RegistryCode:      registryCode,
			RawRecordID:       record.ID,
			WebsiteNormalized: website.NormalizedURL,
			Domain:            domain,
			NormalizedDomain:  domain,
			RegistrableDomain: domain,
			DomainType:        "official",
			Source:            "ariregister_website",
			Status:            "active",
			Confidence:        90,
			IsPrimary:         len(domains) == 0,
			BestSignal:        "ariregister_contact",
			Evidence:          mustJSON(map[string]any{"source": "ariregister_website", "website": website.NormalizedURL}),
			Metadata:          metadata,
		})
	}
	return domains
}

func buildIndustries(record RawRecord, registryCode string, generalData map[string]any, metadata json.RawMessage) []IndustryRow {
	items := objectArray(generalData, "teatatud_tegevusalad")
	rows := make([]IndustryRow, 0, len(items))
	for _, item := range items {
		if len(rows) >= 50 {
			break
		}
		code := stringValue(item, "emtak_kood")
		if code == "" {
			continue
		}
		card := commonCardFields(item)
		naceCode := stringValue(item, "nace_kood")
		mappingMethod := ""
		var mappingConfidence *float64
		if naceCode != "" {
			mappingMethod = "ariregister_declared_nace"
			mappingConfidence = floatPtr(1)
		}
		rows = append(rows, IndustryRow{
			RegistryCode:       registryCode,
			RawRecordID:        record.ID,
			SourceEntryID:      card.SourceEntryID,
			ClassificationType: "declared_activity",
			SourceField:        "teatatud_tegevusalad",
			Position:           int16(len(rows) + 1),
			EMTAKCode:          code,
			EMTAKLabel:         stringValue(item, "emtak_tekstina"),
			EMTAKVersion:       intPtr(item, "emtak_versioon"),
			EMTAKVersionLabel:  stringValue(item, "emtak_versioon_tekstina"),
			NACECode:           naceCode,
			MappingMethod:      mappingMethod,
			MappingConfidence:  mappingConfidence,
			IsPrimary:          boolValue(item, "on_pohitegevusala"),
			StartedOn:          card.StartedOn,
			EndedOn:            card.EndedOn,
			RawIndustryPayload: mustJSON(item),
			Evidence:           rowEvidence(record, "teatatud_tegevusalad", item),
			Metadata:           metadata,
		})
	}
	return rows
}

func buildCapital(record RawRecord, registryCode string, generalData map[string]any, metadata json.RawMessage) []CapitalRow {
	items := objectArray(generalData, "kapitalid")
	rows := make([]CapitalRow, 0, len(items))
	for _, item := range items {
		amount := numericText(stringValue(item, "kapitali_suurus"))
		if amount == "" {
			continue
		}
		card := commonCardFields(item)
		rows = append(rows, CapitalRow{
			RegistryCode:         registryCode,
			RawRecordID:          record.ID,
			SourceEntryID:        card.SourceEntryID,
			CapitalAmount:        amount,
			CapitalCurrency:      stringValue(item, "kapitali_valuuta"),
			CapitalCurrencyLabel: stringValue(item, "kapitali_valuuta_tekstina"),
			IntroducedOn:         card.StartedOn,
			EndedOn:              card.EndedOn,
			RawCapitalPayload:    mustJSON(item),
			Evidence:             rowEvidence(record, "kapitalid", item),
			Metadata:             metadata,
		})
	}
	return rows
}

func buildFinancialYearPeriods(record RawRecord, registryCode string, generalData map[string]any, metadata json.RawMessage) []FinancialYearPeriodRow {
	items := objectArray(generalData, "majandusaastad")
	rows := make([]FinancialYearPeriodRow, 0, len(items))
	for _, item := range items {
		card := commonCardFields(item)
		rows = append(rows, FinancialYearPeriodRow{
			RegistryCode:        registryCode,
			RawRecordID:         record.ID,
			SourceEntryID:       card.SourceEntryID,
			PeriodStartMonthDay: monthDayText(firstNonEmpty(stringValue(item, "maj_aasta_algus"), stringValue(item, "majandusaasta_perioodi_algus_kpv"), stringValue(item, "algus_kpv"))),
			PeriodEndMonthDay:   monthDayText(firstNonEmpty(stringValue(item, "maj_aasta_lopp"), stringValue(item, "majandusaasta_perioodi_lopp_kpv"), stringValue(item, "lopp_kpv"))),
			StartedOn:           card.StartedOn,
			EndedOn:             card.EndedOn,
			RawPeriodPayload:    mustJSON(item),
			Evidence:            rowEvidence(record, "majandusaastad", item),
			Metadata:            metadata,
		})
	}
	return rows
}

func buildAnnualReports(record RawRecord, registryCode string, generalData map[string]any, metadata json.RawMessage) []AnnualReportRow {
	items := objectArray(generalData, "info_majandusaasta_aruannetest")
	rows := make([]AnnualReportRow, 0, len(items))
	for _, item := range items {
		card := commonCardFields(item)
		periodStart := estonianDateText(stringValue(item, "majandusaasta_perioodi_algus_kpv"))
		periodEnd := estonianDateText(stringValue(item, "majandusaasta_perioodi_lopp_kpv"))
		rows = append(rows, AnnualReportRow{
			RegistryCode:         registryCode,
			RawRecordID:          record.ID,
			SourceEntryID:        card.SourceEntryID,
			FiscalYear:           yearPtr(periodEnd),
			PeriodStart:          periodStart,
			PeriodEnd:            periodEnd,
			EmployeeCount:        nonNegativeIntPtr(item, "tootajate_arv"),
			ReportAddress:        stringValue(item, "ettevotja_aadress_aruandes"),
			ActivityEMTAKCode:    stringValue(item, "tegevusala_emtak_kood"),
			ActivityLabel:        stringValue(item, "tegevusala_emtak_tekstina"),
			ActivityVersion:      stringValue(item, "tegevusala_emtak_versioon"),
			ActivityVersionLabel: stringValue(item, "tegevusala_emtak_versioon_tekstina"),
			ActivityNACECode:     stringValue(item, "tegevusala_nace_kood"),
			RawReportPayload:     mustJSON(item),
			Evidence:             rowEvidence(record, "info_majandusaasta_aruannetest", item),
			Metadata:             metadata,
		})
	}
	return rows
}

func buildArticles(record RawRecord, registryCode string, generalData map[string]any, metadata json.RawMessage) []ArticleRow {
	items := objectArray(generalData, "pohikirjad")
	rows := make([]ArticleRow, 0, len(items))
	for _, item := range items {
		card := commonCardFields(item)
		rows = append(rows, ArticleRow{
			RegistryCode:          registryCode,
			RawRecordID:           record.ID,
			SourceEntryID:         card.SourceEntryID,
			ConfirmedOn:           estonianDateText(stringValue(item, "kinnitamise_kpv")),
			ChangedOn:             estonianDateText(stringValue(item, "muutmise_kpv")),
			Explanation:           stringValue(item, "selgitus"),
			ContainsSpecialRights: boolPtr(item, "sisaldab_erioigusi"),
			StartedOn:             card.StartedOn,
			EndedOn:               card.EndedOn,
			RawArticlesPayload:    mustJSON(item),
			Evidence:              rowEvidence(record, "pohikirjad", item),
			Metadata:              metadata,
		})
	}
	return rows
}

func buildRegistryNotes(record RawRecord, registryCode string, generalData map[string]any, metadata json.RawMessage) []RegistryNoteRow {
	items := objectArray(generalData, "markused_kaardil")
	rows := make([]RegistryNoteRow, 0, len(items))
	for _, item := range items {
		card := commonCardFields(item)
		rows = append(rows, RegistryNoteRow{
			RegistryCode:   registryCode,
			RawRecordID:    record.ID,
			SourceEntryID:  card.SourceEntryID,
			CardRegion:     card.CardRegion,
			CardNumber:     card.CardNumber,
			CardType:       card.CardType,
			EntryNumber:    card.EntryNumber,
			ColumnNumber:   intPtr(item, "veerg_nr"),
			NoteType:       stringValue(item, "tyyp"),
			NoteTypeLabel:  stringValue(item, "tyyp_tekstina"),
			NoteText:       stringValue(item, "sisu"),
			StartedOn:      card.StartedOn,
			EndedOn:        card.EndedOn,
			RawNotePayload: mustJSON(item),
			Evidence:       rowEvidence(record, "markused_kaardil", item),
			Metadata:       metadata,
		})
	}
	return rows
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

func objectArray(payload map[string]any, key string) []map[string]any {
	raw, ok := payload[key]
	if !ok || raw == nil {
		return nil
	}
	values, ok := raw.([]any)
	if !ok {
		return nil
	}
	result := make([]map[string]any, 0, len(values))
	for _, value := range values {
		item, ok := value.(map[string]any)
		if ok {
			result = append(result, item)
		}
	}
	return result
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

func commonCardFields(item map[string]any) cardFields {
	return cardFields{
		SourceEntryID: int64Ptr(item, "kirje_id"),
		CardRegion:    intPtr(item, "kaardi_piirkond"),
		CardNumber:    intPtr(item, "kaardi_nr"),
		CardType:      stringValue(item, "kaardi_tyyp"),
		EntryNumber:   intPtr(item, "kande_nr"),
		StartedOn:     estonianDateText(stringValue(item, "algus_kpv")),
		EndedOn:       estonianDateText(stringValue(item, "lopp_kpv")),
	}
}

func boolPtr(payload map[string]any, key string) *bool {
	raw, ok := payload[key]
	if !ok || raw == nil {
		return nil
	}
	switch value := raw.(type) {
	case bool:
		return &value
	case string:
		return estonianYesNoPtr(value)
	default:
		return nil
	}
}

func boolValue(payload map[string]any, key string) bool {
	value := boolPtr(payload, key)
	return value != nil && *value
}

func estonianYesNoPtr(value string) *bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "jah", "true", "1":
		result := true
		return &result
	case "ei", "false", "0":
		result := false
		return &result
	default:
		return nil
	}
}

func intPtr(payload map[string]any, key string) *int {
	value, ok := parseInt(stringValue(payload, key))
	if !ok {
		return nil
	}
	return &value
}

func firstIntPtr(values ...*int) *int {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}

func nonNegativeIntPtr(payload map[string]any, key string) *int {
	value, ok := parseInt(stringValue(payload, key))
	if !ok || value < 0 {
		return nil
	}
	return &value
}

func int64Ptr(payload map[string]any, key string) *int64 {
	value, ok := parseInt64(stringValue(payload, key))
	if !ok {
		return nil
	}
	return &value
}

func yearPtr(date string) *int {
	if len(date) < 4 {
		return nil
	}
	year, ok := parseInt(date[:4])
	if !ok || year < 1800 || year > 2200 {
		return nil
	}
	return &year
}

func floatPtr(value float64) *float64 {
	return &value
}

func parseInt(value string) (int, bool) {
	if !integerTextPattern.MatchString(value) {
		return 0, false
	}
	parsed, err := strconv.Atoi(value)
	return parsed, err == nil
}

func parseInt64(value string) (int64, bool) {
	if !integerTextPattern.MatchString(value) {
		return 0, false
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	return parsed, err == nil
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

func lifecycleStatus(statusCode, deletedOn string, isActive *bool) string {
	if deletedOn != "" {
		return "deleted"
	}
	if isActive != nil {
		if *isActive {
			return "active"
		}
		return "inactive"
	}
	if strings.EqualFold(strings.TrimSpace(statusCode), "R") {
		return "active"
	}
	return "unknown"
}

func latestAnnualReportFacts(reports []AnnualReportRow) (*int, *int) {
	var latestYear *int
	var employeeCount *int
	for _, report := range reports {
		if report.FiscalYear == nil {
			continue
		}
		if latestYear == nil || *report.FiscalYear > *latestYear {
			year := *report.FiscalYear
			latestYear = &year
			if report.EmployeeCount != nil {
				count := *report.EmployeeCount
				employeeCount = &count
			} else {
				employeeCount = nil
			}
		}
	}
	return latestYear, employeeCount
}

func employeeCountSource(employeeCount *int) string {
	if employeeCount == nil {
		return ""
	}
	return "annual_report"
}

func estonianDateText(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if len(value) == len("2006-01-02") {
		if _, err := time.Parse("2006-01-02", value); err == nil {
			return value
		}
	}
	parsed, err := time.Parse("02.01.2006", value)
	if err != nil {
		return ""
	}
	return parsed.Format("2006-01-02")
}

func monthDayText(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if date := estonianDateText(value); date != "" {
		return date[5:]
	}
	parts := strings.Split(value, ".")
	if len(parts) == 2 && len(parts[0]) == 2 && len(parts[1]) == 2 {
		return parts[1] + "-" + parts[0]
	}
	if len(value) == len("01-02") {
		return value
	}
	return ""
}

func contactTypeFromSource(value string) string {
	normalized := strings.ToUpper(strings.TrimSpace(value))
	switch {
	case normalized == "WWW" || strings.Contains(normalized, "WEB"):
		return "website"
	case normalized == "EMAIL" || strings.Contains(normalized, "EPOST") || strings.Contains(normalized, "E-MAIL"):
		return "email"
	case strings.Contains(normalized, "MOB"):
		return "mobile"
	case strings.Contains(normalized, "FAX") || strings.Contains(normalized, "FAKS"):
		return "fax"
	case strings.Contains(normalized, "TEL"):
		return "phone"
	default:
		return strings.ToLower(normalized)
	}
}

func normalizeContactValue(contactType, value string) string {
	switch contactType {
	case "email":
		return strings.ToLower(strings.TrimSpace(value))
	case "phone", "mobile", "fax":
		return normalizePhone(value)
	case "website":
		normalized := normalizeURL(value)
		if normalized == "" {
			return ""
		}
		return normalizedURLText(normalized)
	default:
		return strings.TrimSpace(value)
	}
}

func appendRawContact(contacts *[]ContactRow, seen map[string]struct{}, record RawRecord, registryCode, contactType, value string, metadata json.RawMessage) {
	value = strings.TrimSpace(value)
	if value == "" {
		return
	}
	normalized := normalizeContactValue(contactType, value)
	if normalized == "" {
		return
	}
	appendContact(contacts, seen, ContactRow{
		RegistryCode:      registryCode,
		RawRecordID:       record.ID,
		ContactType:       contactType,
		ContactTypeLabel:  contactType,
		Value:             value,
		NormalizedValue:   normalized,
		Source:            "ariregister_raw_record",
		Status:            "active",
		IsPrimary:         !hasContactType(*contacts, contactType),
		Evidence:          mustJSON(map[string]any{"source": "ariregister_workflow.raw_records", "raw_record_id": record.ID.String()}),
		RawContactPayload: json.RawMessage(`{}`),
		Metadata:          metadata,
	})
}

func appendContact(contacts *[]ContactRow, seen map[string]struct{}, row ContactRow) bool {
	key := row.ContactType + ":" + row.NormalizedValue
	if _, ok := seen[key]; ok {
		return false
	}
	seen[key] = struct{}{}
	*contacts = append(*contacts, row)
	return true
}

func hasContactType(contacts []ContactRow, contactType string) bool {
	for _, contact := range contacts {
		if contact.ContactType == contactType {
			return true
		}
	}
	return false
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

func normalizedURLText(value string) string {
	return strings.TrimRight(strings.ToLower(value), "/")
}

func hostFromURL(value string) string {
	parsed, err := url.Parse(value)
	if err != nil {
		return ""
	}
	return strings.ToLower(parsed.Hostname())
}

func pathFromURL(value string) string {
	parsed, err := url.Parse(value)
	if err != nil {
		return ""
	}
	return parsed.EscapedPath()
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
		"ariregister.rik.ee", "rik.ee", "eesti.ee", "teatmik.ee", "inforegister.ee", "creditinfo.ee", "e-krediidiinfo.ee":
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

func formatFloat(value float64) string {
	data, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return string(data)
}

func rowEvidence(record RawRecord, field string, item map[string]any) json.RawMessage {
	evidence := map[string]any{
		"source":        "ariregister_raw_payload",
		"field":         field,
		"raw_record_id": record.ID.String(),
	}
	if sourceEntryID := stringValue(item, "kirje_id"); sourceEntryID != "" {
		evidence["source_entry_id"] = sourceEntryID
	}
	return mustJSON(evidence)
}

func mustJSON(value any) json.RawMessage {
	data, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return data
}
