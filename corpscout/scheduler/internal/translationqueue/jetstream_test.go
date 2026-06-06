package translationqueue

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodeTranslationJobRejectsMissingBatchID(t *testing.T) {
	_, err := encodeTranslationJob(TranslationJob{JobID: "job-1", Source: "brreg"})
	require.ErrorContains(t, err, "batch id is required")
}

func TestEncodeTranslationJobReturnsJSONPayload(t *testing.T) {
	body, err := encodeTranslationJob(TranslationJob{
		JobID:         "job-1",
		BatchID:       "batch-1",
		Source:        "brreg",
		SourceLang:    "no",
		TargetLang:    "en",
		Provider:      "default",
		PromptVersion: "v1",
		CompanyIDs:    []string{"company-a"},
		Terms: []TranslationJobTerm{{
			TermKey:              "a",
			SourceText:           "Aksjeselskap",
			SourceTextNormalized: "aksjeselskap",
		}},
	})
	require.NoError(t, err)

	var decoded TranslationJob
	require.NoError(t, json.Unmarshal(body, &decoded))
	require.Equal(t, "batch-1", decoded.BatchID)
	require.Equal(t, "brreg", decoded.Source)
	require.Len(t, decoded.Terms, 1)
}

func TestJetStreamPublisherPublishesJobSubject(t *testing.T) {
	publisher := &fakeJetStreamPublisher{}
	client := NewJetStreamClientFromPublisher(publisher)

	err := client.PublishJob(context.Background(), TranslationJob{
		JobID:         "job-1",
		BatchID:       "batch-1",
		Source:        "brreg",
		SourceLang:    "no",
		TargetLang:    "en",
		Provider:      "default",
		PromptVersion: "v1",
		CompanyIDs:    []string{"company-a"},
		Terms: []TranslationJobTerm{{
			TermKey:              "a",
			SourceText:           "Aksjeselskap",
			SourceTextNormalized: "aksjeselskap",
		}},
	})
	require.NoError(t, err)
	require.Equal(t, JobsSubject, publisher.subject)
	require.NotEmpty(t, publisher.payload)
}

type fakeJetStreamPublisher struct {
	subject string
	payload []byte
}

func (f *fakeJetStreamPublisher) Publish(_ context.Context, subject string, payload []byte) error {
	f.subject = subject
	f.payload = payload
	return nil
}
