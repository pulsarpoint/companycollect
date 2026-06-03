package translationclient

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTermTranslationRequestPayloadRoundTrips(t *testing.T) {
	require.Equal(t, "brreg.translation.terms.request", DefaultTermTranslationRequestSubject)
	require.Equal(t, "brreg.translation.terms.result", DefaultTermTranslationResultSubject)

	request := TermTranslationRequest{
		RequestID:     "request-1",
		Source:        "brreg",
		SourceLang:    "no",
		TargetLang:    "en",
		Provider:      "mock",
		Model:         "mock-fast",
		PromptVersion: "v1",
		Terms: []TermTranslationRequestTerm{{
			TermKey:              "orgform.as",
			SourceText:           "Aksjeselskap",
			SourceTextNormalized: "aksjeselskap",
		}},
	}

	payload, err := json.Marshal(request)
	require.NoError(t, err)
	require.JSONEq(t, `{
	  "request_id": "request-1",
	  "source": "brreg",
	  "source_lang": "no",
	  "target_lang": "en",
	  "provider": "mock",
	  "model": "mock-fast",
	  "prompt_version": "v1",
	  "terms": [
	    {
	      "term_key": "orgform.as",
	      "source_text": "Aksjeselskap",
	      "source_text_normalized": "aksjeselskap"
	    }
	  ]
	}`, string(payload))

	var decoded TermTranslationRequest
	require.NoError(t, json.Unmarshal(payload, &decoded))
	require.Equal(t, "brreg", decoded.Source)
	require.Equal(t, "Aksjeselskap", decoded.Terms[0].SourceText)
}

func TestTermTranslationResultPayloadRoundTrips(t *testing.T) {
	result := TermTranslationResult{
		RequestID:     "request-1",
		Source:        "brreg",
		SourceLang:    "no",
		TargetLang:    "en",
		Provider:      "mock",
		Model:         "mock-fast",
		PromptVersion: "v1",
		Results: []TermTranslationResultItem{{
			TermKey:              "orgform.as",
			SourceText:           "Aksjeselskap",
			SourceTextNormalized: "aksjeselskap",
			TranslatedText:       "Limited liability company",
			Status:               "succeeded",
		}},
		Failures: []TermTranslationFailureResult{{
			TermKey:              "bad-term",
			SourceText:           "Ugyldig verdi",
			SourceTextNormalized: "ugyldig verdi",
			Status:               "failed_retryable",
			ErrorCode:            "llm_request_failed",
			Error:                "request timed out",
		}},
	}

	payload, err := json.Marshal(result)
	require.NoError(t, err)
	require.JSONEq(t, `{
	  "request_id": "request-1",
	  "source": "brreg",
	  "source_lang": "no",
	  "target_lang": "en",
	  "provider": "mock",
	  "model": "mock-fast",
	  "prompt_version": "v1",
	  "results": [
	    {
	      "term_key": "orgform.as",
	      "source_text": "Aksjeselskap",
	      "source_text_normalized": "aksjeselskap",
	      "translated_text": "Limited liability company",
	      "status": "succeeded"
	    }
	  ],
	  "failures": [
	    {
	      "term_key": "bad-term",
	      "source_text": "Ugyldig verdi",
	      "source_text_normalized": "ugyldig verdi",
	      "status": "failed_retryable",
	      "error_code": "llm_request_failed",
	      "error": "request timed out"
	    }
	  ]
	}`, string(payload))

	var decoded TermTranslationResult
	require.NoError(t, json.Unmarshal(payload, &decoded))
	require.Equal(t, "brreg", decoded.Source)
	require.Equal(t, "Limited liability company", decoded.Results[0].TranslatedText)
	require.Equal(t, "llm_request_failed", decoded.Failures[0].ErrorCode)
}
