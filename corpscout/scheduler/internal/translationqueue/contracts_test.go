package translationqueue

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTranslationJobPayloadJSONShape(t *testing.T) {
	payload := TranslationJob{
		JobID:         "job-1",
		BatchID:       "workflow/batch/000001",
		Source:        "brreg",
		SourceLang:    "no",
		TargetLang:    "en",
		Provider:      "deepseek",
		Model:         "deepseek-chat",
		PromptVersion: "v1",
		CompanyIDs:    []string{"company-a"},
		Terms: []TranslationJobTerm{{
			TermKey:              "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			SourceText:           "Aksjeselskap",
			SourceTextNormalized: "aksjeselskap",
		}},
	}

	body, err := json.Marshal(payload)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"job_id":"job-1",
		"batch_id":"workflow/batch/000001",
		"source":"brreg",
		"source_lang":"no",
		"target_lang":"en",
		"provider":"deepseek",
		"model":"deepseek-chat",
		"prompt_version":"v1",
		"company_ids":["company-a"],
		"terms":[{
			"term_key":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			"source_text":"Aksjeselskap",
			"source_text_normalized":"aksjeselskap"
		}]
	}`, string(body))
}

func TestTranslationResultPayloadJSONShape(t *testing.T) {
	payload := TranslationResult{
		JobID:         "job-1",
		BatchID:       "workflow/batch/000001",
		Source:        "brreg",
		Status:        "succeeded",
		Provider:      "deepseek",
		Model:         "deepseek-chat",
		PromptVersion: "v1",
		CompanyIDs:    []string{"company-a"},
		DurationMS:    1234,
		Results: []TranslationResultTerm{{
			TermKey:              "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			SourceText:           "Aksjeselskap",
			SourceTextNormalized: "aksjeselskap",
			TranslatedText:       "Limited liability company",
			Status:               "succeeded",
		}},
		Failures: []TranslationFailureResult{},
	}

	body, err := json.Marshal(payload)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"job_id":"job-1",
		"batch_id":"workflow/batch/000001",
		"source":"brreg",
		"status":"succeeded",
		"provider":"deepseek",
		"model":"deepseek-chat",
		"prompt_version":"v1",
		"company_ids":["company-a"],
		"duration_ms":1234,
		"results":[{
			"term_key":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			"source_text":"Aksjeselskap",
			"source_text_normalized":"aksjeselskap",
			"translated_text":"Limited liability company",
			"status":"succeeded"
		}],
		"failures":[]
	}`, string(body))
}
