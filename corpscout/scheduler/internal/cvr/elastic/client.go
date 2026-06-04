package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/pulsarpoint/corpscout/scheduler/internal/cvr/cvrrecord"
)

const (
	DefaultSourceURL = "https://distribution.virk.dk/cvr-permanent/virksomhed/_search"
	DefaultScrollURL = "https://distribution.virk.dk/_search/scroll"
	DefaultScroll    = "1m"
	DefaultPageSize  = int32(100)
)

type Record = cvrrecord.Record

type Client struct {
	httpClient *http.Client
}

func NewClient(httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Client{httpClient: httpClient}
}

type StreamInput struct {
	SourceURL   string
	ScrollURL   string
	Scroll      string
	PageSize    int32
	Limit       int32
	Username    string
	Password    string
	BearerToken string
	APIKey      string
}

type StreamResult struct {
	RowsSeen int32
}

func (c *Client) StreamRecords(ctx context.Context, input StreamInput, emit func(Record) error) (StreamResult, error) {
	if emit == nil {
		return StreamResult{}, errors.New("emit callback is required")
	}
	input = normalizeStreamInput(input)
	if input.Username == "" && input.Password == "" && input.BearerToken == "" && input.APIKey == "" {
		return StreamResult{}, errors.New("cvr distribution credentials are not configured")
	}

	response, err := c.search(ctx, input, initialSearchBody(input.PageSize))
	if err != nil {
		return StreamResult{}, err
	}
	var result StreamResult
	scrollID := response.ScrollID
	for {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if len(response.Hits.Hits) == 0 {
			return result, nil
		}
		for _, hit := range response.Hits.Hits {
			if input.Limit > 0 && result.RowsSeen >= input.Limit {
				return result, nil
			}
			record, err := cvrrecord.NewRecord(hit.Source)
			if err != nil {
				return result, errors.Wrap(err, "decode cvr search hit")
			}
			result.RowsSeen++
			if err := emit(record); err != nil {
				return result, err
			}
		}
		if scrollID == "" {
			return result, nil
		}
		response, err = c.scroll(ctx, input, scrollID)
		if err != nil {
			return result, err
		}
		scrollID = response.ScrollID
	}
}

func normalizeStreamInput(input StreamInput) StreamInput {
	input.SourceURL = strings.TrimSpace(input.SourceURL)
	if input.SourceURL == "" {
		input.SourceURL = DefaultSourceURL
	}
	input.ScrollURL = strings.TrimSpace(input.ScrollURL)
	if input.ScrollURL == "" {
		input.ScrollURL = DefaultScrollURL
	}
	input.Scroll = strings.TrimSpace(input.Scroll)
	if input.Scroll == "" {
		input.Scroll = DefaultScroll
	}
	if input.PageSize <= 0 {
		input.PageSize = DefaultPageSize
	}
	input.Username = strings.TrimSpace(input.Username)
	input.Password = strings.TrimSpace(input.Password)
	input.BearerToken = strings.TrimSpace(input.BearerToken)
	input.APIKey = strings.TrimSpace(input.APIKey)
	return input
}

func initialSearchBody(pageSize int32) map[string]any {
	return map[string]any{
		"size": pageSize,
		"sort": []string{"_doc"},
		"query": map[string]any{
			"match_all": map[string]any{},
		},
	}
}

func (c *Client) search(ctx context.Context, input StreamInput, body map[string]any) (searchResponse, error) {
	searchURL, err := url.Parse(input.SourceURL)
	if err != nil {
		return searchResponse{}, errors.Wrap(err, "parse cvr search url")
	}
	query := searchURL.Query()
	query.Set("scroll", input.Scroll)
	searchURL.RawQuery = query.Encode()
	return c.doJSON(ctx, input, searchURL.String(), body)
}

func (c *Client) scroll(ctx context.Context, input StreamInput, scrollID string) (searchResponse, error) {
	return c.doJSON(ctx, input, input.ScrollURL, map[string]any{
		"scroll":    input.Scroll,
		"scroll_id": scrollID,
	})
}

func (c *Client) doJSON(ctx context.Context, input StreamInput, endpoint string, body map[string]any) (searchResponse, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return searchResponse{}, errors.Wrap(err, "encode cvr search request")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return searchResponse{}, errors.Wrap(err, "create cvr search request")
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/json")
	if input.Username != "" || input.Password != "" {
		request.SetBasicAuth(input.Username, input.Password)
	}
	if input.BearerToken != "" {
		request.Header.Set("Authorization", "Bearer "+input.BearerToken)
	}
	if input.APIKey != "" {
		request.Header.Set("X-API-Key", input.APIKey)
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		return searchResponse{}, errors.Wrap(err, "request cvr search")
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return searchResponse{}, errors.Newf("cvr search returned status %d", response.StatusCode)
	}
	var decoded searchResponse
	if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
		return searchResponse{}, errors.Wrap(err, "decode cvr search response")
	}
	return decoded, nil
}

type searchResponse struct {
	ScrollID string `json:"_scroll_id"`
	Hits     struct {
		Hits []searchHit `json:"hits"`
	} `json:"hits"`
}

type searchHit struct {
	Source json.RawMessage `json:"_source"`
}
