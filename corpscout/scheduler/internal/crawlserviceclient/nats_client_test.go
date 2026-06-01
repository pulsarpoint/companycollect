package crawlserviceclient

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestNATSDiscoverBrregDomainRequestsSubjectAndDecodesResponse(t *testing.T) {
	fake := &fakeNATSRequester{
		response: []byte(`{
			"schema_version":"crawl-service.domains.v1",
			"status":"succeeded",
			"best_domain":"bortigard.no",
			"search_engine":"duckduckgo",
			"duration_ms":17
		}`),
	}
	client := newNATSClientFromRequester("brreg.domain.discover", fake)

	response, err := client.DiscoverBrregDomain(context.Background(), BrregDomainDiscoveryRequest{
		RecordID:           "raw-1",
		OrganizationNumber: "810202572",
		OrganizationName:   ptr("BORTIGARD AS"),
		RawPayload:         json.RawMessage(`{"navn":"BORTIGARD AS"}`),
		SearchProvider:     "duckduckgo",
		PromptVersion:      "v1",
	})

	require.NoError(t, err)
	require.Equal(t, "brreg.domain.discover", fake.subject)
	require.JSONEq(t, `{
		"record_id":"raw-1",
		"organization_number":"810202572",
		"organization_name":"BORTIGARD AS",
		"raw_payload":{"navn":"BORTIGARD AS"},
		"search_provider":"duckduckgo",
		"prompt_version":"v1",
		"limits":{}
	}`, string(fake.payload))
	require.Equal(t, "succeeded", response.Status)
	require.Equal(t, "bortigard.no", *response.BestDomain)
	require.JSONEq(t, `{
		"schema_version":"crawl-service.domains.v1",
		"status":"succeeded",
		"best_domain":"bortigard.no",
		"search_engine":"duckduckgo",
		"duration_ms":17
	}`, string(response.RawPayload))
}

func TestNATSDiscoverBrregDomainWrapsRequestErrors(t *testing.T) {
	client := newNATSClientFromRequester("brreg.domain.discover", &fakeNATSRequester{err: errors.New("nats down")})

	_, err := client.DiscoverBrregDomain(context.Background(), BrregDomainDiscoveryRequest{
		RecordID:           "raw-1",
		OrganizationNumber: "810202572",
		RawPayload:         json.RawMessage(`{}`),
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "request brreg domain discovery over nats")
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
