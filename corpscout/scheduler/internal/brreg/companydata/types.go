package companydata

import (
	"encoding/json"

	"github.com/google/uuid"
)

type CompanyData struct {
	Company Company
	Capital []Capital
}

type Company struct {
	ID                      uuid.UUID
	RawRecordID             uuid.UUID
	OrganizationNumber      string
	OrganizationName        string
	OrganizationNameEN      string
	OrganizationFormLabel   string
	OrganizationFormLabelEN string
	ResponseClass           string
	ResponseClassEN         string
	ActivityDescription     string
	ActivityDescriptionEN   string
	StatutoryPurpose        string
	StatutoryPurposeEN      string
}

type Capital struct {
	ID            uuid.UUID
	CompanyID     uuid.UUID
	RawRecordID   uuid.UUID
	CapitalType   string
	CapitalTypeEN string
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

type ApplyTranslationsResult struct {
	FieldsApplied     int32
	TermsWithoutMatch int32
}

type ApplyCachedTranslationsCommand struct {
	CompanyID     uuid.UUID
	PromptVersion string
}

type ApplyCachedTranslationsResult struct {
	FieldsSeen      int32
	FieldsApplied   int32
	RemainingFields int32
}

type MarkTranslationStatusCommand struct {
	CompanyID uuid.UUID
	Metadata  json.RawMessage
}

type MarkTranslationFailedCommand struct {
	CompanyID     uuid.UUID
	Error         string
	ErrorCategory string
	ErrorCode     string
	RetryStrategy string
	MaxAttempts   int32
	Terminal      bool
	Metadata      json.RawMessage
}
