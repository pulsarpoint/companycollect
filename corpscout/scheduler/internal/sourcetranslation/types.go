package sourcetranslation

import "context"

// SourceConfig identifies the source/language pair handled by a translation workset.
type SourceConfig struct {
	Source               string
	SourceLang           string
	TargetLang           string
	DefaultPromptVersion string
}

// LoadMissingFieldsCommand selects company-scoped source fields that still need target-language values.
type LoadMissingFieldsCommand struct {
	PromptVersion string
	CompanyIDs    []string
	Filters       map[string]string
	CompanyLimit  int32
	FieldLimit    int32
}

// MissingField is one source table column value that can be translated into its matching target column.
type MissingField struct {
	CompanyID            string
	SourceTable          string
	SourceRowID          string
	SourceColumn         string
	TargetColumn         string
	SourceText           string
	SourceTextNormalized string
	TermKey              string
	Priority             int32
}

// LoadCachedTermsCommand requests already translated terms for one source/language/prompt combination.
type LoadCachedTermsCommand struct {
	PromptVersion string
	TermKeys      []string
}

// CachedTerm is a reusable translation result from a persistent source cache.
type CachedTerm struct {
	TermKey        string
	TranslatedText string
}

// TranslationTerm is one deduplicated term selected for model translation.
type TranslationTerm struct {
	TermKey              string
	SourceText           string
	SourceTextNormalized string
}

// TranslationTermResult is a model or cache result for one translation term.
type TranslationTermResult struct {
	TermKey              string
	SourceText           string
	SourceTextNormalized string
	TranslatedText       string
	Status               string
	Provider             string
	Model                string
	PromptVersion        string
	Error                string
	ErrorCode            string
	Metadata             map[string]any
}

// SaveTermsCommand persists translated terms; PromptVersion is the fallback for results with a blank PromptVersion.
type SaveTermsCommand struct {
	PromptVersion string
	Terms         []TranslationTermResult
}

// SaveTermsResult reports how many source cache rows were written.
type SaveTermsResult struct {
	TermsSaved int32
}

// TranslationBinding connects a translated term back to one source row target column.
type TranslationBinding struct {
	ID             int64
	CompanyID      string
	SourceTable    string
	SourceRowID    string
	SourceColumn   string
	TargetColumn   string
	TranslatedText string
}

// ApplyCompanyTranslationsCommand applies translated bindings for one source company.
type ApplyCompanyTranslationsCommand struct {
	CompanyID string
	Bindings  []TranslationBinding
}

// ApplyCompanyTranslationsResult reports which bindings changed source data.
type ApplyCompanyTranslationsResult struct {
	BindingsApplied   int32
	AppliedBindingIDs []int64
}

// SourceStore is the source-specific boundary consumed by the shared translation workset.
// Implementations own their SQL, cache table, and supported target-column updates.
type SourceStore interface {
	LoadMissingTranslationFields(context.Context, LoadMissingFieldsCommand) ([]MissingField, error)
	LoadCachedTranslationTerms(context.Context, LoadCachedTermsCommand) (map[string]CachedTerm, error)
	SaveTranslationTerms(context.Context, SaveTermsCommand) (SaveTermsResult, error)
	ApplyCompanyTranslations(context.Context, ApplyCompanyTranslationsCommand) (ApplyCompanyTranslationsResult, error)
	RefreshTranslationStatus(context.Context) error
}
