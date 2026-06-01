package llmproviders

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
)

type ProbeClient struct {
	httpClient *http.Client
	cipher     *Cipher
}

func NewProbeClient(httpClient *http.Client, cipher *Cipher) *ProbeClient {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &ProbeClient{httpClient: httpClient, cipher: cipher}
}

func (c *ProbeClient) TestProvider(ctx context.Context, provider ProviderConfig, command TestProviderCommand) TestProviderResult {
	start := time.Now()
	result := TestProviderResult{
		Status:       "failed",
		ProviderID:   provider.ID,
		ProviderSlug: provider.Slug,
		Model:        provider.Model,
	}
	if strings.TrimSpace(command.Prompt) == "" {
		result.Error = &TestProviderError{Code: "invalid_prompt", Message: "prompt is required"}
		result.DurationMS = time.Since(start).Milliseconds()
		return result
	}
	apiKey, err := c.apiKey(provider)
	if err != nil {
		result.Error = &TestProviderError{Code: "decrypt_failed", Message: "provider API key could not be decrypted"}
		result.DurationMS = time.Since(start).Milliseconds()
		return result
	}
	timeout := time.Duration(command.TimeoutSeconds) * time.Second
	if timeout <= 0 || timeout > 120*time.Second {
		timeout = 30 * time.Second
	}
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	responseText, err := c.chatCompletion(requestCtx, provider, apiKey, command)
	result.DurationMS = time.Since(start).Milliseconds()
	if err != nil {
		result.Error = &TestProviderError{Code: "provider_request_failed", Message: err.Error()}
		return result
	}
	result.Status = "succeeded"
	result.ResponseText = responseText
	return result
}

func (c *ProbeClient) apiKey(provider ProviderConfig) (string, error) {
	if provider.APIKey == nil {
		return "", nil
	}
	if c.cipher == nil {
		return "", errors.New("llm provider encryption key is not configured")
	}
	return c.cipher.Decrypt(*provider.APIKey)
}

func (c *ProbeClient) chatCompletion(ctx context.Context, provider ProviderConfig, apiKey string, command TestProviderCommand) (string, error) {
	maxTokens := command.MaxTokens
	if maxTokens <= 0 || maxTokens > 2048 {
		maxTokens = 128
	}
	body := map[string]any{
		"model": provider.Model,
		"messages": []map[string]string{
			{"role": "user", "content": strings.TrimSpace(command.Prompt)},
		},
		"max_tokens":  maxTokens,
		"temperature": 0,
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return "", errors.Wrap(err, "marshal provider test request")
	}
	endpoint := strings.TrimRight(provider.BaseURL, "/") + "/v1/chat/completions"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return "", errors.Wrap(err, "create provider test request")
	}
	req.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", errors.Wrap(err, "call llm provider")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return "", errors.Newf("LLM provider request failed with status %d", resp.StatusCode)
	}
	var parsed struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return "", errors.Wrap(err, "decode provider test response")
	}
	if len(parsed.Choices) == 0 || strings.TrimSpace(parsed.Choices[0].Message.Content) == "" {
		return "", errors.New("LLM provider returned an empty response")
	}
	return parsed.Choices[0].Message.Content, nil
}
