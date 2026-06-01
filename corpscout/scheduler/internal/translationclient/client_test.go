package translationclient

import (
	"context"
	"encoding/json"
	"testing"

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

type fakeNATSRequester struct {
	subject  string
	payload  []byte
	response []byte
	err      error
}

func (f *fakeNATSRequester) Request(ctx context.Context, subject string, payload []byte) ([]byte, error) {
	f.subject = subject
	f.payload = append([]byte(nil), payload...)
	if f.err != nil {
		return nil, f.err
	}
	return f.response, nil
}
