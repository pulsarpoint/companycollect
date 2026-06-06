package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/pulsarpoint/companycollect/data-pipelines/golang-translate/internal/fixture"
)

type Client struct {
	baseURL string
	apiKey  string
	model   string
	http    *http.Client
}

type Request struct {
	SourceLang string
	TargetLang string
	Items      []fixture.Item
}

type Result struct {
	Translations map[string]string `json:"translations"`
	RawContent   string            `json:"raw_content"`
	PromptTokens int               `json:"prompt_tokens,omitempty"`
	OutputTokens int               `json:"output_tokens,omitempty"`
}

type chatRequest struct {
	Model              string        `json:"model"`
	Messages           []chatMessage `json:"messages"`
	Temperature        int           `json:"temperature"`
	MaxTokens          int           `json:"max_tokens"`
	ChatTemplateKwargs thinkingArgs  `json:"chat_template_kwargs"`
}

type thinkingArgs struct {
	EnableThinking bool `json:"enable_thinking"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatResponse struct {
	Choices []struct {
		Message chatMessage `json:"message"`
	} `json:"choices"`
	Usage struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
	} `json:"usage"`
}

func NewClient(baseURL string, apiKey string, model string, timeout time.Duration) *Client {
	return &Client{
		baseURL: NormalizeOpenAIBaseURL(baseURL),
		apiKey:  apiKey,
		model:   model,
		http:    &http.Client{Timeout: timeout},
	}
}

func (c *Client) Translate(ctx context.Context, request Request) (Result, error) {
	body, err := json.Marshal(chatRequest{
		Model:              c.model,
		Messages:           BuildTranslationMessages(request.SourceLang, request.TargetLang, request.Items),
		Temperature:        0,
		MaxTokens:          TranslationMaxTokens(len(request.Items)),
		ChatTemplateKwargs: thinkingArgs{EnableThinking: false},
	})
	if err != nil {
		return Result{}, errors.Wrap(err, "encode llm request")
	}

	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return Result{}, errors.Wrap(err, "create llm request")
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		httpRequest.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	response, err := c.http.Do(httpRequest)
	if err != nil {
		return Result{}, errors.Wrap(err, "send llm request")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return Result{}, errors.Errorf("llm request failed with status %d", response.StatusCode)
	}

	var decoded chatResponse
	if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
		return Result{}, errors.Wrap(err, "decode llm response")
	}
	if len(decoded.Choices) == 0 {
		return Result{}, errors.New("llm response contains no choices")
	}
	content := decoded.Choices[0].Message.Content
	expectedIDs := make(map[string]struct{}, len(request.Items))
	for _, item := range request.Items {
		expectedIDs[item.ID] = struct{}{}
	}
	translations, err := ParseTranslations(content, expectedIDs)
	if err != nil {
		return Result{}, err
	}
	return Result{
		Translations: translations,
		RawContent:   content,
		PromptTokens: decoded.Usage.PromptTokens,
		OutputTokens: decoded.Usage.CompletionTokens,
	}, nil
}

func BuildTranslationMessages(sourceLang string, targetLang string, items []fixture.Item) []chatMessage {
	itemsJSON, _ := json.Marshal(compactItems(items))
	return []chatMessage{
		{
			Role: "user",
			Content: "/no_think\n" +
				"Translate " + sourceLang + " business registry text to " + targetLang + ".\n" +
				"Use each item's category as context.\n" +
				"Return only JSON: {\"translations\":[{\"id\":\"...\",\"translation\":\"...\"}]}\n" +
				"Preserve every input id exactly. Include one translation per input item.\n" +
				"Items: " + string(itemsJSON),
		},
	}
}

func TranslationMaxTokens(itemCount int) int {
	tokens := itemCount * 96
	if tokens < 512 {
		return 512
	}
	if tokens > 4096 {
		return 4096
	}
	return tokens
}

func NormalizeOpenAIBaseURL(baseURL string) string {
	trimmed := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	for _, suffix := range []string{"/v1/chat/completions", "/chat/completions", "/v1"} {
		if strings.HasSuffix(trimmed, suffix) {
			trimmed = strings.TrimSuffix(trimmed, suffix)
		}
	}
	return strings.TrimRight(trimmed, "/") + "/v1"
}

func ParseTranslations(content string, expectedIDs map[string]struct{}) (map[string]string, error) {
	cleaned := cleanJSONContent(content)
	var decoded any
	if err := json.Unmarshal([]byte(cleaned), &decoded); err != nil {
		return nil, errors.Wrap(err, "parse llm translation json")
	}
	translations := translationsFromValue(decoded, expectedIDs)
	return translations, nil
}

func compactItems(items []fixture.Item) []map[string]string {
	compact := make([]map[string]string, 0, len(items))
	for _, item := range items {
		compact = append(compact, map[string]string{
			"id":       item.ID,
			"text":     item.Text,
			"category": item.Category,
		})
	}
	return compact
}

func translationsFromValue(value any, expectedIDs map[string]struct{}) map[string]string {
	result := make(map[string]string)
	switch typed := value.(type) {
	case map[string]any:
		if list, ok := typed["translations"].([]any); ok {
			return translationsFromList(list, expectedIDs)
		}
		if list, ok := typed["items"].([]any); ok {
			return translationsFromList(list, expectedIDs)
		}
		for key, value := range typed {
			if _, ok := expectedIDs[key]; ok {
				if translated := strings.TrimSpace(stringValue(value)); translated != "" {
					result[key] = translated
				}
			}
		}
	case []any:
		return translationsFromList(typed, expectedIDs)
	}
	return result
}

func translationsFromList(values []any, expectedIDs map[string]struct{}) map[string]string {
	result := make(map[string]string)
	for _, value := range values {
		item, ok := value.(map[string]any)
		if !ok {
			continue
		}
		id := stringValue(item["id"])
		if _, ok := expectedIDs[id]; !ok {
			continue
		}
		translated := strings.TrimSpace(firstString(item["translation"], item["translated_text"], item["text"]))
		if translated != "" {
			result[id] = translated
		}
	}
	return result
}

func cleanJSONContent(content string) string {
	text := strings.TrimSpace(content)
	if strings.HasPrefix(text, "```") {
		text = strings.TrimPrefix(text, "```json")
		text = strings.TrimPrefix(text, "```")
		text = strings.TrimSuffix(text, "```")
	}
	return strings.TrimSpace(text)
}

func firstString(values ...any) string {
	for _, value := range values {
		if text := stringValue(value); text != "" {
			return text
		}
	}
	return ""
}

func stringValue(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	default:
		return ""
	}
}
