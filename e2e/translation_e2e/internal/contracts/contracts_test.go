package contracts

import (
	"encoding/json"
	"testing"
)

func TestTranslationJobJSONContract(t *testing.T) {
	body, err := json.Marshal(TranslationJob{
		JobID:         "job-1",
		BatchID:       "batch-1",
		Source:        "e2e",
		SourceLang:    "et",
		TargetLang:    "en",
		Provider:      "default",
		Model:         "default",
		PromptVersion: "v1",
		CompanyIDs:    []string{"company-1"},
		Terms: []TranslationJobTerm{{
			TermKey:              "term-key",
			SourceText:           "osaühing",
			SourceTextNormalized: "osaühing",
		}},
	})
	if err != nil {
		t.Fatalf("marshal job: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("decode job json: %v", err)
	}
	for _, key := range []string{"job_id", "batch_id", "source", "source_lang", "target_lang", "provider", "model", "prompt_version", "company_ids", "terms"} {
		if _, ok := decoded[key]; !ok {
			t.Fatalf("missing job key %q in %s", key, string(body))
		}
	}
}

func TestTranslationResultJSONContract(t *testing.T) {
	body, err := json.Marshal(TranslationResult{
		JobID:         "job-1",
		BatchID:       "batch-1",
		Source:        "e2e",
		Status:        "succeeded",
		Provider:      "local",
		Model:         "qwen3:6b",
		PromptVersion: "v1",
		CompanyIDs:    []string{"company-1"},
		DurationMS:    123,
		Results: []TranslationResultTerm{{
			TermKey:              "term-key",
			SourceText:           "osaühing",
			SourceTextNormalized: "osaühing",
			TranslatedText:       "private limited company",
			Status:               "succeeded",
		}},
		Failures: []TranslationFailureResult{},
	})
	if err != nil {
		t.Fatalf("marshal result: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("decode result json: %v", err)
	}
	for _, key := range []string{"job_id", "batch_id", "source", "status", "provider", "model", "prompt_version", "company_ids", "duration_ms", "results", "failures"} {
		if _, ok := decoded[key]; !ok {
			t.Fatalf("missing result key %q in %s", key, string(body))
		}
	}
}
