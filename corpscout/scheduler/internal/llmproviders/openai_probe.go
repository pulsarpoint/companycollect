package llmproviders

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
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
		slog.Warn("llm provider test rejected invalid prompt",
			"llm_provider_id", provider.ID,
			"llm_provider_slug", provider.Slug,
			"model", provider.Model,
		)
		return result
	}
	apiKey, err := c.apiKey(provider)
	if err != nil {
		result.Error = &TestProviderError{Code: "decrypt_failed", Message: "provider API key could not be decrypted"}
		result.DurationMS = time.Since(start).Milliseconds()
		slog.Error("llm provider test failed to decrypt api key",
			"error", err,
			"llm_provider_id", provider.ID,
			"llm_provider_slug", provider.Slug,
			"model", provider.Model,
		)
		return result
	}
	timeout := time.Duration(command.TimeoutSeconds) * time.Second
	if timeout <= 0 || timeout > 120*time.Second {
		timeout = 30 * time.Second
	}
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	slog.Debug("llm provider test started",
		"llm_provider_id", provider.ID,
		"llm_provider_slug", provider.Slug,
		"base_url", provider.BaseURL,
		"model", provider.Model,
		"timeout_seconds", int(timeout.Seconds()),
		"max_tokens", command.MaxTokens,
		"has_api_key", apiKey != "",
	)
	responseText, err := c.chatCompletion(requestCtx, provider, apiKey, command)
	result.DurationMS = time.Since(start).Milliseconds()
	if err != nil {
		result.Error = &TestProviderError{Code: "provider_request_failed", Message: err.Error()}
		slog.Warn("llm provider test failed",
			"error", err,
			"llm_provider_id", provider.ID,
			"llm_provider_slug", provider.Slug,
			"base_url", provider.BaseURL,
			"model", provider.Model,
			"duration_ms", result.DurationMS,
		)
		return result
	}
	result.Status = "succeeded"
	result.ResponseText = responseText
	slog.Debug("llm provider test succeeded",
		"llm_provider_id", provider.ID,
		"llm_provider_slug", provider.Slug,
		"model", provider.Model,
		"duration_ms", result.DurationMS,
		"response_chars", len(responseText),
	)
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
		maxTokens = 512
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
	endpoint := normalizeOpenAICompatibleBaseURL(provider.BaseURL) + "/chat/completions"
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
	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", errors.Wrap(err, "read provider test response")
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		slog.Warn("llm provider test returned non-success status",
			"llm_provider_slug", provider.Slug,
			"base_url", provider.BaseURL,
			"model", provider.Model,
			"status", resp.StatusCode,
			"response_body_bytes", len(responseBody),
			"response_body_preview", responseBodyPreview(responseBody),
		)
		return "", errors.Newf("LLM provider request failed with status %d", resp.StatusCode)
	}
	var parsed struct {
		Choices []struct {
			FinishReason string `json:"finish_reason"`
			Text         string `json:"text"`
			Message      struct {
				Role             string `json:"role"`
				Content          any    `json:"content"`
				ReasoningContent string `json:"reasoning_content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(responseBody, &parsed); err != nil {
		return "", errors.Wrap(err, "decode provider test response")
	}
	if len(parsed.Choices) == 0 {
		slog.Warn("llm provider test returned no choices",
			"llm_provider_slug", provider.Slug,
			"base_url", provider.BaseURL,
			"model", provider.Model,
			"response_body_bytes", len(responseBody),
			"response_body_preview", responseBodyPreview(responseBody),
		)
		return "", errors.New("LLM provider returned an empty response")
	}
	content := strings.TrimSpace(messageContentText(parsed.Choices[0].Message.Content))
	if content == "" {
		content = strings.TrimSpace(parsed.Choices[0].Text)
	}
	if content == "" {
		slog.Warn("llm provider test returned empty message content",
			"llm_provider_slug", provider.Slug,
			"base_url", provider.BaseURL,
			"model", provider.Model,
			"choice_count", len(parsed.Choices),
			"finish_reason", parsed.Choices[0].FinishReason,
			"message_role", parsed.Choices[0].Message.Role,
			"content_shape", fmt.Sprintf("%T", parsed.Choices[0].Message.Content),
			"reasoning_content_chars", len(parsed.Choices[0].Message.ReasoningContent),
			"response_body_bytes", len(responseBody),
			"response_body_preview", responseBodyPreview(responseBody),
		)
		return "", errors.New("LLM provider returned an empty response")
	}
	return content, nil
}

func messageContentText(content any) string {
	switch value := content.(type) {
	case string:
		return value
	case []any:
		var parts []string
		for _, item := range value {
			block, ok := item.(map[string]any)
			if !ok {
				continue
			}
			if text, ok := block["text"].(string); ok {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, "")
	default:
		return ""
	}
}

func responseBodyPreview(body []byte) string {
	const limit = 500
	preview := strings.TrimSpace(string(body))
	if len(preview) > limit {
		preview = preview[:limit] + "..."
	}
	return preview
}

func normalizeOpenAICompatibleBaseURL(baseURL string) string {
	trimmed := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	trimmed = strings.TrimSuffix(trimmed, "/v1/chat/completions")
	trimmed = strings.TrimSuffix(trimmed, "/chat/completions")
	trimmed = strings.TrimSuffix(trimmed, "/v1")
	return strings.TrimRight(trimmed, "/") + "/v1"
}
