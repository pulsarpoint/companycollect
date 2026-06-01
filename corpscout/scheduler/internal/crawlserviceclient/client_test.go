package crawlserviceclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDiscoverBrregDomainPostsTypedRequest(t *testing.T) {
	var capturedPath string
	var capturedRequest BrregDomainDiscoveryRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		require.Equal(t, http.MethodPost, r.Method)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&capturedRequest))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"schema_version":"crawl-service.domain-discovery.v1",
			"status":"succeeded",
			"best_domain":"bortigard.no",
			"search_engine":"duckduckgo",
			"search_term":"BORTIGARD AS Norway",
			"errors":[],
			"warnings":[],
			"duration_ms":12,
			"service_version":"test"
		}`))
	}))
	defer server.Close()

	client := New(server.URL)
	response, err := client.DiscoverBrregDomain(context.Background(), BrregDomainDiscoveryRequest{
		RecordID:           "record-1",
		OrganizationNumber: "810202572",
		OrganizationName:   ptr("BORTIGARD AS"),
		RawPayload:         json.RawMessage(`{"navn":"BORTIGARD AS"}`),
		ExistingWebsite:    ptr("https://bortigard.no"),
		Country:            "NO",
		SearchProvider:     "duckduckgo",
		PromptVersion:      "v1",
		Limits: DomainDiscoverLimits{
			MaxSearchCandidates:      5,
			MaxSiteChecks:            3,
			SearchCandidateThreshold: 50,
			DomainThreshold:          70,
		},
	})

	require.NoError(t, err)
	require.Equal(t, "/v1/brreg/domain-discovery", capturedPath)
	require.Equal(t, "record-1", capturedRequest.RecordID)
	require.Equal(t, "810202572", capturedRequest.OrganizationNumber)
	require.Equal(t, "BORTIGARD AS", *capturedRequest.OrganizationName)
	require.Equal(t, json.RawMessage(`{"navn":"BORTIGARD AS"}`), capturedRequest.RawPayload)
	require.Equal(t, "succeeded", response.Status)
	require.Equal(t, "bortigard.no", *response.BestDomain)
	require.JSONEq(t, `{"schema_version":"crawl-service.domain-discovery.v1","status":"succeeded","best_domain":"bortigard.no","search_engine":"duckduckgo","search_term":"BORTIGARD AS Norway","errors":[],"warnings":[],"duration_ms":12,"service_version":"test"}`, string(response.RawPayload))
}

func TestFetchSearchPagePostsTypedRequest(t *testing.T) {
	var capturedPath string
	var capturedRequest SearchFetchRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		require.Equal(t, http.MethodPost, r.Method)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&capturedRequest))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"url":"https://html.duckduckgo.com/html/?q=BORTIGARD%20AS%20NO%20website",
			"final_url":"https://html.duckduckgo.com/html/?q=BORTIGARD%20AS%20NO%20website",
			"status":"succeeded",
			"markdown":"# Search",
			"markdown_hash":"search-hash",
			"links":["https://bortigard.no/"],
			"duration_ms":10,
			"metadata":{}
		}`))
	}))
	defer server.Close()

	client := New(server.URL)
	response, err := client.FetchSearchPage(context.Background(), SearchFetchRequest{
		SearchTerm:     "BORTIGARD AS NO website",
		SearchEngine:   "duckduckgo",
		TimeoutSeconds: 60,
	})

	require.NoError(t, err)
	require.Equal(t, "/v1/search/fetch", capturedPath)
	require.Equal(t, "BORTIGARD AS NO website", capturedRequest.SearchTerm)
	require.Equal(t, "succeeded", response.Status)
	require.Equal(t, "search-hash", response.MarkdownHash)
	require.Equal(t, []string{"https://bortigard.no/"}, response.Links)
}

func TestAnalyzeCompanyPagePostsTypedRequest(t *testing.T) {
	var capturedPath string
	var capturedRequest PageAnalyzeRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		require.Equal(t, http.MethodPost, r.Method)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&capturedRequest))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"status":"succeeded",
			"analysis":{
				"decision":"accepted",
				"score":91,
				"site_type":"company_website",
				"relationship":"primary_web_presence",
				"owned_domain":true
			}
		}`))
	}))
	defer server.Close()

	client := New(server.URL)
	response, err := client.AnalyzeCompanyPage(context.Background(), PageAnalyzeRequest{
		CompanyName:        "BORTIGARD AS",
		OrganizationNumber: "810202572",
		URL:                "https://bortigard.no/",
		FinalURL:           "https://bortigard.no/",
		NormalizedDomain:   "bortigard.no",
		Markdown:           "# BORTIGARD AS",
		CandidateScore:     88,
		CandidateReason:    "Search result matches.",
	})

	require.NoError(t, err)
	require.Equal(t, "/v1/pages/analyze-company", capturedPath)
	require.Equal(t, "BORTIGARD AS", capturedRequest.CompanyName)
	require.Equal(t, "succeeded", response.Status)
	require.Equal(t, float64(91), response.Analysis["score"])
}

func TestDiscoverBrregDomainReturnsStructuredHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "bad gateway", http.StatusBadGateway)
	}))
	defer server.Close()

	client := New(server.URL)
	_, err := client.DiscoverBrregDomain(context.Background(), BrregDomainDiscoveryRequest{
		RecordID:           "record-1",
		OrganizationNumber: "810202572",
		RawPayload:         json.RawMessage(`{}`),
	})

	require.Error(t, err)
	require.ErrorIs(t, err, ErrUnexpectedStatus)
	require.Contains(t, err.Error(), "502")
}

func TestDiscoverBrregDomainUsesTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		time.Sleep(50 * time.Millisecond)
	}))
	defer server.Close()

	client := New(server.URL)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	_, err := client.DiscoverBrregDomain(ctx, BrregDomainDiscoveryRequest{
		RecordID:           "record-1",
		OrganizationNumber: "810202572",
		RawPayload:         json.RawMessage(`{}`),
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "discover brreg domain")
}

func ptr(value string) *string {
	return &value
}
