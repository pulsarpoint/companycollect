package translationclient

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestTranslateBrregRecordsRequestsSubjectAndDecodesResponse(t *testing.T) {
	fake := &fakeNATSRequester{
		response: []byte(`{
			"schema_version":"translation-service.brreg.v1",
			"status":"succeeded",
			"provider":"mock",
			"model":"mock-fast",
			"prompt_version":"v1",
			"records_seen":1,
			"records_completed":1,
			"records_failed":0,
			"records_skipped":0,
			"duration_ms":12,
			"results":[{
				"record_id":"record-1",
				"organization_number":"810202572",
				"status":"succeeded",
				"translated_payload":{"name":"BORTIGARD AS"},
				"missing_terms":[],
				"duration_ms":12
			}]
		}`),
	}
	client := newClientFromRequester("brreg.translation.translate", fake)

	response, err := client.TranslateBrregRecords(context.Background(), BrregTranslateRequest{
		Records: []BrregRecord{{
			RecordID:           "record-1",
			OrganizationNumber: "810202572",
			RawPayload:         json.RawMessage(`{"navn":"BORTIGARD AS"}`),
		}},
		LLM:           LLMSelection{Provider: "mock", Model: "mock-fast", BaseURL: "https://llm.example/v1", APIKey: "secret-key"},
		PromptVersion: "v1",
		SourceLang:    "no",
		TargetLang:    "en",
		MaxRetries:    3,
	})

	require.NoError(t, err)
	require.Equal(t, "brreg.translation.translate", fake.subject)
	require.JSONEq(t, `{
		"records":[{
			"record_id":"record-1",
			"organization_number":"810202572",
			"raw_payload":{"navn":"BORTIGARD AS"}
		}],
		"llm":{"provider":"mock","model":"mock-fast","base_url":"https://llm.example/v1","api_key":"secret-key"},
		"prompt_version":"v1",
		"source_lang":"no",
		"target_lang":"en",
		"max_retries":3
	}`, string(fake.payload))
	require.Equal(t, "succeeded", response.Status)
	require.Equal(t, "BORTIGARD AS", response.Results[0].TranslatedPayload["name"])
}

func TestTranslateBrregTermsRequestsSubjectAndDecodesResponse(t *testing.T) {
	fake := &fakeNATSRequester{
		response: []byte(`{
			"request_id":"request-1",
			"source":"brreg",
			"source_lang":"no",
			"target_lang":"en",
			"provider":"mock",
			"model":"mock-fast",
			"prompt_version":"v1",
			"results":[{
				"term_key":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				"source_text":"Aksjeselskap",
				"source_text_normalized":"aksjeselskap",
				"translated_text":"Limited liability company",
				"status":"succeeded"
			}]
		}`),
	}
	client := newClientFromRequester(DefaultTermTranslationRequestSubject, fake)

	response, err := client.TranslateBrregTerms(context.Background(), TermTranslationRequest{
		RequestID:     "request-1",
		Source:        "brreg",
		SourceLang:    "no",
		TargetLang:    "en",
		Provider:      "mock",
		Model:         "mock-fast",
		PromptVersion: "v1",
		Terms: []TermTranslationRequestTerm{{
			TermKey:              "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			SourceText:           "Aksjeselskap",
			SourceTextNormalized: "aksjeselskap",
		}},
	})

	require.NoError(t, err)
	require.Equal(t, DefaultTermTranslationRequestSubject, fake.subject)
	require.JSONEq(t, `{
		"request_id":"request-1",
		"source":"brreg",
		"source_lang":"no",
		"target_lang":"en",
		"provider":"mock",
		"model":"mock-fast",
		"prompt_version":"v1",
		"terms":[{
			"term_key":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			"source_text":"Aksjeselskap",
			"source_text_normalized":"aksjeselskap"
		}]
	}`, string(fake.payload))
	require.Len(t, response.Results, 1)
	require.Equal(t, "Limited liability company", response.Results[0].TranslatedText)
}

func TestTranslateBrregRecordsWrapsRequestErrors(t *testing.T) {
	client := newClientFromRequester("brreg.translation.translate", &fakeNATSRequester{err: errors.New("nats down")})

	_, err := client.TranslateBrregRecords(context.Background(), BrregTranslateRequest{
		Records: []BrregRecord{{
			RecordID:           "record-1",
			OrganizationNumber: "810202572",
			RawPayload:         json.RawMessage(`{}`),
		}},
		LLM: LLMSelection{Provider: "mock", Model: "mock-fast"},
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "request brreg translation over nats")
}

func TestTranslateBrregRecordsWrapsDecodeErrors(t *testing.T) {
	client := newClientFromRequester("brreg.translation.translate", &fakeNATSRequester{response: []byte(`{not-json`)})

	_, err := client.TranslateBrregRecords(context.Background(), BrregTranslateRequest{
		Records: []BrregRecord{{
			RecordID:           "record-1",
			OrganizationNumber: "810202572",
			RawPayload:         json.RawMessage(`{}`),
		}},
		LLM: LLMSelection{Provider: "mock", Model: "mock-fast"},
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "decode brreg translation nats response")
}

func TestTranslateBrregRecordsAppliesRequestTimeout(t *testing.T) {
	fake := &fakeNATSRequester{response: []byte(`{"status":"succeeded","results":[]}`)}
	client := &Client{
		subject:        "brreg.translation.translate",
		requestTimeout: 2 * time.Minute,
		requester:      fake,
	}

	_, err := client.TranslateBrregRecords(context.Background(), BrregTranslateRequest{
		LLM: LLMSelection{Provider: "mock", Model: "mock-fast"},
	})

	require.NoError(t, err)
	require.True(t, fake.hasDeadline)
	remaining := time.Until(fake.deadline)
	require.Greater(t, remaining, 110*time.Second)
	require.LessOrEqual(t, remaining, 2*time.Minute)
}

func TestTranslateBrregRecordsKeepsShorterParentDeadline(t *testing.T) {
	fake := &fakeNATSRequester{response: []byte(`{"status":"succeeded","results":[]}`)}
	client := &Client{
		subject:        "brreg.translation.translate",
		requestTimeout: 2 * time.Minute,
		requester:      fake,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := client.TranslateBrregRecords(ctx, BrregTranslateRequest{
		LLM: LLMSelection{Provider: "mock", Model: "mock-fast"},
	})

	require.NoError(t, err)
	require.True(t, fake.hasDeadline)
	remaining := time.Until(fake.deadline)
	require.Greater(t, remaining, 20*time.Second)
	require.LessOrEqual(t, remaining, 30*time.Second)
}

type fakeNATSRequester struct {
	subject     string
	payload     []byte
	response    []byte
	err         error
	hasDeadline bool
	deadline    time.Time
}

func (f *fakeNATSRequester) Request(ctx context.Context, subject string, payload []byte) ([]byte, error) {
	f.subject = subject
	f.payload = append([]byte(nil), payload...)
	f.deadline, f.hasDeadline = ctx.Deadline()
	if f.err != nil {
		return nil, f.err
	}
	return f.response, nil
}
