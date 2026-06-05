package companydata

import (
	"github.com/google/uuid"

	"github.com/pulsarpoint/corpscout/scheduler/internal/sourcetranslation"
)

type CompanyData struct {
	Company    Company
	Addresses  []Address
	Industries []Industry
	Websites   []Website
	Contacts   []Contact
	Capital    []Capital
	Roles      []Role
}

type Company struct {
	ID                        uuid.UUID
	RawRecordID               uuid.UUID
	OrganizationNumber        string
	OrganizationName          string
	OrganizationNameEN        string
	ShortDescription          string
	ShortDescriptionEN        string
	Description               string
	DescriptionEN             string
	RegistrationStatusLabel   string
	RegistrationStatusLabelEN string
	OrganizationFormLabel     string
	OrganizationFormLabelEN   string
	ResponseClass             string
	ResponseClassEN           string
	ActivityDescription       string
	ActivityDescriptionEN     string
	StatutoryPurpose          string
	StatutoryPurposeEN        string
}

type Address struct {
	ID          uuid.UUID
	CompanyID   uuid.UUID
	RawRecordID uuid.UUID
	Country     string
	CountryEN   string
}

type Industry struct {
	ID            uuid.UUID
	CompanyID     uuid.UUID
	RawRecordID   uuid.UUID
	SourceLabel   string
	SourceLabelEN string
}

type Website struct {
	ID            uuid.UUID
	CompanyID     uuid.UUID
	RawRecordID   uuid.UUID
	Title         string
	TitleEN       string
	Description   string
	DescriptionEN string
}

type Contact struct {
	ID          uuid.UUID
	CompanyID   uuid.UUID
	RawRecordID uuid.UUID
	Label       string
	LabelEN     string
}

type Capital struct {
	ID            uuid.UUID
	CompanyID     uuid.UUID
	RawRecordID   uuid.UUID
	CapitalType   string
	CapitalTypeEN string
}

type Role struct {
	ID          uuid.UUID
	CompanyID   uuid.UUID
	RawRecordID uuid.UUID
	RoleLabel   string
	RoleLabelEN string
	RoleGroup   string
	RoleGroupEN string
}

type TranslationTerm struct {
	Key            string
	SourceText     string
	NormalizedText string
}

type TermTranslation struct {
	SourceText     string
	TranslatedText string
}

type TranslationTermResult = sourcetranslation.TranslationTermResult

type SaveTranslationTermsResult = sourcetranslation.SaveTermsResult

type ApplyTranslationsResult struct {
	FieldsApplied     int32
	TermsWithoutMatch int32
}
