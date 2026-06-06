package translationqueue

const (
	JobsSubject    = "source.translation.jobs"
	ResultsSubject = "source.translation.results"
	StreamName     = "SOURCE_TRANSLATION"
)

type TranslationJob struct {
	JobID         string               `json:"job_id"`
	BatchID       string               `json:"batch_id"`
	Source        string               `json:"source"`
	SourceLang    string               `json:"source_lang"`
	TargetLang    string               `json:"target_lang"`
	Provider      string               `json:"provider"`
	Model         string               `json:"model,omitempty"`
	PromptVersion string               `json:"prompt_version"`
	CompanyIDs    []string             `json:"company_ids"`
	Terms         []TranslationJobTerm `json:"terms"`
}

type TranslationJobTerm struct {
	TermKey              string `json:"term_key"`
	SourceText           string `json:"source_text"`
	SourceTextNormalized string `json:"source_text_normalized"`
}

type TranslationResult struct {
	JobID         string                     `json:"job_id"`
	BatchID       string                     `json:"batch_id"`
	Source        string                     `json:"source"`
	Status        string                     `json:"status"`
	Provider      string                     `json:"provider"`
	Model         string                     `json:"model,omitempty"`
	PromptVersion string                     `json:"prompt_version"`
	CompanyIDs    []string                   `json:"company_ids"`
	DurationMS    int                        `json:"duration_ms"`
	Results       []TranslationResultTerm    `json:"results"`
	Failures      []TranslationFailureResult `json:"failures"`
}

type TranslationResultTerm struct {
	TermKey              string `json:"term_key"`
	SourceText           string `json:"source_text"`
	SourceTextNormalized string `json:"source_text_normalized"`
	TranslatedText       string `json:"translated_text"`
	Status               string `json:"status"`
}

type TranslationFailureResult struct {
	TermKey              string `json:"term_key"`
	SourceText           string `json:"source_text"`
	SourceTextNormalized string `json:"source_text_normalized"`
	Status               string `json:"status"`
	ErrorCode            string `json:"error_code,omitempty"`
	Error                string `json:"error,omitempty"`
}
