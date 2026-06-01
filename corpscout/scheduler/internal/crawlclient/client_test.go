package crawlclient

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestFetchSearchPageRequestsSubjectAndDecodesResponse(t *testing.T) {
	fake := &fakeNATSRequester{
		response: []byte(`{
			"url":"https://html.duckduckgo.com/html/?q=BORTIGARD%20AS%20NO%20website",
			"final_url":"https://html.duckduckgo.com/html/?q=BORTIGARD%20AS%20NO%20website",
			"status":"succeeded",
			"markdown":"# Search results",
			"markdown_hash":"search-hash",
			"links":["https://bortigard.no/"],
			"duration_ms":12
		}`),
	}
	client := newClientFromRequester("brreg.domain.search.fetch", "brreg.domain.search.analyze", fake)

	response, err := client.FetchSearchPage(context.Background(), SearchFetchRequest{
		SearchTerm:     "BORTIGARD AS NO website",
		SearchEngine:   "duckduckgo",
		TimeoutSeconds: 60,
	})

	require.NoError(t, err)
	require.Equal(t, "brreg.domain.search.fetch", fake.subject)
	require.JSONEq(t, `{
		"search_term":"BORTIGARD AS NO website",
		"search_engine":"duckduckgo",
		"timeout_seconds":60
	}`, string(fake.payload))
	require.Equal(t, "succeeded", response.Status)
	require.Equal(t, "search-hash", response.MarkdownHash)
	require.Equal(t, []string{"https://bortigard.no/"}, response.Links)
}

func TestAnalyzeSearchPageRequestsSubjectAndDecodesResponse(t *testing.T) {
	fake := &fakeNATSRequester{
		response: []byte(`{
			"status":"succeeded",
			"candidates":[{
				"url":"https://bortigard.no/",
				"domain":"bortigard.no",
				"normalized_domain":"bortigard.no",
				"score":88,
				"reason":"Search result matches."
			}]
		}`),
	}
	client := newClientFromRequester("brreg.domain.search.fetch", "brreg.domain.search.analyze", fake)

	response, err := client.AnalyzeSearchPage(context.Background(), SearchAnalyzeRequest{
		CompanyName:        "BORTIGARD AS",
		OrganizationNumber: "810202572",
		Country:            "NO",
		SearchEngine:       "duckduckgo",
		SearchTerm:         "BORTIGARD AS NO website",
		Links:              []string{"https://bortigard.no/"},
		Markdown:           "# Search results",
		CandidateThreshold: 50,
		MaxCandidates:      10,
		TimeoutSeconds:     60,
		LLM: LLMSelection{
			Provider: "deepseek-v4-flash",
			Model:    "deepseek-v4-flash",
			BaseURL:  "https://api.deepseek.com/v1",
			APIKey:   "secret-key",
		},
	})

	require.NoError(t, err)
	require.Equal(t, "brreg.domain.search.analyze", fake.subject)
	require.JSONEq(t, `{
		"company_name":"BORTIGARD AS",
		"organization_number":"810202572",
		"country":"NO",
		"address_lines":null,
		"business_activity":null,
		"statutory_purpose":null,
		"industry_codes":null,
		"search_engine":"duckduckgo",
		"search_term":"BORTIGARD AS NO website",
		"links":["https://bortigard.no/"],
		"markdown":"# Search results",
		"candidate_threshold":50,
		"max_candidates":10,
		"timeout_seconds":60,
		"llm":{
			"provider":"deepseek-v4-flash",
			"model":"deepseek-v4-flash",
			"base_url":"https://api.deepseek.com/v1",
			"api_key":"secret-key"
		}
	}`, string(fake.payload))
	require.Equal(t, "succeeded", response.Status)
	require.Equal(t, "bortigard.no", response.Candidates[0].NormalizedDomain)
}

func TestSearchActionsWrapRequestErrors(t *testing.T) {
	client := newClientFromRequester("fetch", "analyze", &fakeNATSRequester{err: errors.New("nats down")})

	_, err := client.FetchSearchPage(context.Background(), SearchFetchRequest{})

	require.Error(t, err)
	require.Contains(t, err.Error(), "request brreg search fetch over nats")
}

type fakeNATSRequester struct {
	subject  string
	payload  []byte
	response []byte
	err      error
}

func (f *fakeNATSRequester) Request(_ context.Context, subject string, payload []byte) ([]byte, error) {
	f.subject = subject
	f.payload = append([]byte(nil), payload...)
	if f.err != nil {
		return nil, f.err
	}
	return f.response, nil
}
