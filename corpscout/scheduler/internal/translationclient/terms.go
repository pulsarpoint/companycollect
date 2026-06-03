package translationclient

const (
	DefaultTermTranslationRequestSubject = "brreg.translation.terms.request"
	DefaultTermTranslationResultSubject  = "brreg.translation.terms.result"
)

type TermTranslationRequest struct {
	RequestID     string                       `json:"request_id"`
	Source        string                       `json:"source"`
	SourceLang    string                       `json:"source_lang"`
	TargetLang    string                       `json:"target_lang"`
	Provider      string                       `json:"provider"`
	Model         string                       `json:"model,omitempty"`
	PromptVersion string                       `json:"prompt_version"`
	Terms         []TermTranslationRequestTerm `json:"terms"`
}

type TermTranslationRequestTerm struct {
	TermKey              string `json:"term_key"`
	SourceText           string `json:"source_text"`
	SourceTextNormalized string `json:"source_text_normalized"`
}

type TermTranslationResult struct {
	RequestID     string                         `json:"request_id"`
	Source        string                         `json:"source"`
	SourceLang    string                         `json:"source_lang"`
	TargetLang    string                         `json:"target_lang"`
	Provider      string                         `json:"provider"`
	Model         string                         `json:"model,omitempty"`
	PromptVersion string                         `json:"prompt_version"`
	Results       []TermTranslationResultItem    `json:"results"`
	Failures      []TermTranslationFailureResult `json:"failures"`
}

type TermTranslationResultItem struct {
	TermKey              string `json:"term_key"`
	SourceText           string `json:"source_text"`
	SourceTextNormalized string `json:"source_text_normalized"`
	TranslatedText       string `json:"translated_text"`
	Status               string `json:"status"`
}

type TermTranslationFailureResult struct {
	TermKey              string `json:"term_key"`
	SourceText           string `json:"source_text"`
	SourceTextNormalized string `json:"source_text_normalized"`
	Status               string `json:"status"`
	ErrorCode            string `json:"error_code,omitempty"`
	Error                string `json:"error,omitempty"`
}
