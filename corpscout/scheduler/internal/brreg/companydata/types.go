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

type TranslationTermResult struct {
	SourceText           string
	SourceTextNormalized string
	TermKey              string
	TranslatedText       string
	Status               string
	Provider             string
	Model                string
	PromptVersion        string
	Error                string
	ErrorCode            string
	Metadata             map[string]any
}

type SaveTranslationTermsResult struct {
	TermsSaved int32
}

type AutoClaimForTranslationCommand struct {
	PageSize             int32
	MaxRequestChars      int32
	MaxCompaniesPerBatch int32
	MaxParallelTasks     int32
	LeaseSeconds         int32
	MaxAttempts          int32
	WorkerID             string
}

type AutoClaimForTranslationResult struct {
	StatusRowsInserted    int32
	Companies             []ClaimedCompanyData
	EstimatedRequestChars int32
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
