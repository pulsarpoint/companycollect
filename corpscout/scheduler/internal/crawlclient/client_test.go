package crawlclient

import (
	"context"
	"testing"
	"time"

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

func TestSearchActionsApplyRequestTimeout(t *testing.T) {
	fake := &fakeNATSRequester{response: []byte(`{"status":"succeeded","links":[]}`)}
	client := &Client{
		searchFetchSubject: "fetch",
		requestTimeout:     2 * time.Minute,
		requester:          fake,
	}

	_, err := client.FetchSearchPage(context.Background(), SearchFetchRequest{SearchTerm: "BORTIGARD AS"})

	require.NoError(t, err)
	require.True(t, fake.hasDeadline)
	remaining := time.Until(fake.deadline)
	require.Greater(t, remaining, 110*time.Second)
	require.LessOrEqual(t, remaining, 2*time.Minute)
}

func TestSearchActionsKeepShorterParentDeadline(t *testing.T) {
	fake := &fakeNATSRequester{response: []byte(`{"status":"succeeded","links":[]}`)}
	client := &Client{
		searchFetchSubject: "fetch",
		requestTimeout:     2 * time.Minute,
		requester:          fake,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := client.FetchSearchPage(ctx, SearchFetchRequest{SearchTerm: "BORTIGARD AS"})

	require.NoError(t, err)
	require.True(t, fake.hasDeadline)
	remaining := time.Until(fake.deadline)
	require.Greater(t, remaining, 20*time.Second)
	require.LessOrEqual(t, remaining, 30*time.Second)
}

func TestCrawlPageRequestsSubjectAndDecodesResponse(t *testing.T) {
	fake := &fakeNATSRequester{
		response: []byte(`{
			"url":"https://bortigard.no/",
			"final_url":"https://bortigard.no/",
			"status":"succeeded",
			"markdown":"# BORTIGARD AS",
			"markdown_hash":"site-hash",
			"links":["https://bortigard.no/contact"],
			"duration_ms":9
		}`),
	}
	client := newClientFromRequesterWithSubjects("fetch", "analyze", "brreg.domain.page.crawl", "brreg.domain.page.analyze", fake)

	response, err := client.CrawlPage(context.Background(), CrawlPageRequest{
		URL:            "https://bortigard.no/",
		TimeoutSeconds: 60,
		Metadata:       map[string]any{"candidate_score": 88},
	})

	require.NoError(t, err)
	require.Equal(t, "brreg.domain.page.crawl", fake.subject)
	require.JSONEq(t, `{
		"url":"https://bortigard.no/",
		"timeout_seconds":60,
		"metadata":{"candidate_score":88}
	}`, string(fake.payload))
	require.Equal(t, "succeeded", response.Status)
	require.Equal(t, "site-hash", response.MarkdownHash)
}

func TestAnalyzeCompanyPageRequestsSubjectAndDecodesResponse(t *testing.T) {
	fake := &fakeNATSRequester{
		response: []byte(`{
			"status":"succeeded",
			"analysis":{
				"decision":"accepted",
				"score":91,
				"site_type":"company_website",
				"relationship":"primary_web_presence",
				"owned_domain":true,
				"reason":"The page names BORTIGARD AS.",
				"evidence":["BORTIGARD AS"]
			}
		}`),
	}
	client := newClientFromRequesterWithSubjects("fetch", "analyze", "crawl", "brreg.domain.page.analyze", fake)

	response, err := client.AnalyzeCompanyPage(context.Background(), PageAnalyzeRequest{
		CompanyName:        "BORTIGARD AS",
		OrganizationNumber: "810202572",
		Country:            "NO",
		URL:                "https://bortigard.no/",
		FinalURL:           "https://bortigard.no/",
		NormalizedDomain:   "bortigard.no",
		Markdown:           "# BORTIGARD AS",
		CandidateScore:     88,
		CandidateReason:    "Search result matches.",
		TimeoutSeconds:     60,
		LLM: LLMSelection{
			Provider: "deepseek-v4-flash",
			Model:    "deepseek-v4-flash",
			BaseURL:  "https://api.deepseek.com/v1",
			APIKey:   "secret-key",
		},
	})

	require.NoError(t, err)
	require.Equal(t, "brreg.domain.page.analyze", fake.subject)
	require.JSONEq(t, `{
		"company_name":"BORTIGARD AS",
		"organization_number":"810202572",
		"country":"NO",
		"address_lines":null,
		"business_activity":null,
		"statutory_purpose":null,
		"industry_codes":null,
		"url":"https://bortigard.no/",
		"final_url":"https://bortigard.no/",
		"normalized_domain":"bortigard.no",
		"markdown":"# BORTIGARD AS",
		"candidate_score":88,
		"candidate_reason":"Search result matches.",
		"timeout_seconds":60,
		"llm":{
			"provider":"deepseek-v4-flash",
			"model":"deepseek-v4-flash",
			"base_url":"https://api.deepseek.com/v1",
			"api_key":"secret-key"
		}
	}`, string(fake.payload))
	require.Equal(t, "succeeded", response.Status)
	require.Equal(t, "accepted", response.Analysis["decision"])
	require.EqualValues(t, 91, response.Analysis["score"])
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
