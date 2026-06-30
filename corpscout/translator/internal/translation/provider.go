package translation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"text/template"
	"time"
)

const DefaultMaxTokens = 32768

const promptTemplateSource = `/no_think
Translate each {{.SourceLanguage}} text fragment to {{.TargetLanguage}}. Preserve legal and business meaning. Do not add explanations. Do not produce reasoning, chain-of-thought, thinking tags, or Markdown. Return only valid JSON with shape {"translations":[{"item_id":"...","translated_text":"..."}]}. Every item_id in the response must match an input item_id. Input JSON: {{.ItemsJSON}}`

type Config struct {
	BaseURL    string
	Model      string
	APIKey     string
	MaxTokens  int
	ExtraBody  map[string]any
	PromptData PromptData
	Logger     *slog.Logger
}

type PromptData struct {
	SourceLanguage string
	TargetLanguage string
}

type promptRenderData struct {
	SourceLanguage string
	TargetLanguage string
	ItemsJSON      string
}

type LocalOpenAICompatibleProvider struct {
	baseURL        string
	model          string
	apiKey         string
	maxTokens      int
	extraBody      map[string]any
	promptTemplate *template.Template
	promptData     PromptData
	client         *http.Client
	logger         *slog.Logger
}

func Init(config Config) (*LocalOpenAICompatibleProvider, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(config.BaseURL), "/")
	model := strings.TrimSpace(config.Model)
	apiKey := strings.TrimSpace(config.APIKey)

	if baseURL == "" {
		return nil, errors.New("translation provider base_url is required")
	}
	if model == "" {
		return nil, errors.New("translation provider model is required")
	}
	if apiKey == "" {
		return nil, errors.New("translation provider api_key is required")
	}
	if config.MaxTokens <= 0 {
		config.MaxTokens = DefaultMaxTokens
	}
	if config.ExtraBody == nil {
		config.ExtraBody = defaultExtraBody()
	}
	if strings.TrimSpace(config.PromptData.SourceLanguage) == "" {
		return nil, errors.New("translation prompt source_language is required")
	}
	if strings.TrimSpace(config.PromptData.TargetLanguage) == "" {
		return nil, errors.New("translation prompt target_language is required")
	}
	logger := config.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	logger = logger.With(
		"component", "translation_provider",
		"model", model,
		"source_language", config.PromptData.SourceLanguage,
		"target_language", config.PromptData.TargetLanguage,
	)

	promptTemplate, err := template.New("translation-prompt").Parse(promptTemplateSource)
	if err != nil {
		return nil, fmt.Errorf("parse prompt template: %w", err)
	}

	return &LocalOpenAICompatibleProvider{
		baseURL:        baseURL,
		model:          model,
		apiKey:         apiKey,
		maxTokens:      config.MaxTokens,
		extraBody:      cloneMap(config.ExtraBody),
		promptTemplate: promptTemplate,
		promptData:     config.PromptData,
		client:         http.DefaultClient,
		logger:         logger,
	}, nil
}

func (p *LocalOpenAICompatibleProvider) renderPrompt(items []TranslationInput) (string, error) {
	payload := struct {
		Items []struct {
			ItemID     string `json:"item_id"`
			SourceText string `json:"source_text"`
			SourceLang string `json:"source_lang"`
			TargetLang string `json:"target_lang"`
		} `json:"items"`
	}{
		Items: make([]struct {
			ItemID     string `json:"item_id"`
			SourceText string `json:"source_text"`
			SourceLang string `json:"source_lang"`
			TargetLang string `json:"target_lang"`
		}, 0, len(items)),
	}

	for _, item := range items {
		payload.Items = append(payload.Items, struct {
			ItemID     string `json:"item_id"`
			SourceText string `json:"source_text"`
			SourceLang string `json:"source_lang"`
			TargetLang string `json:"target_lang"`
		}{
			ItemID:     item.ItemID,
			SourceText: item.SourceText,
			SourceLang: item.SourceLang,
			TargetLang: item.TargetLang,
		})
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal translation prompt payload: %w", err)
	}

	promptData := promptRenderData{
		SourceLanguage: p.promptData.SourceLanguage,
		TargetLanguage: p.promptData.TargetLanguage,
		ItemsJSON:      string(data),
	}

	var prompt bytes.Buffer
	if err := p.promptTemplate.Execute(&prompt, promptData); err != nil {
		return "", fmt.Errorf("execute prompt template: %w", err)
	}
	return prompt.String(), nil
}

func ParseTranslationResponse(responseText string, expectedItemIDs map[string]bool) ([]TranslationResult, error) {
	var payload map[string]json.RawMessage
	if err := json.Unmarshal([]byte(stripJSONFence(responseText)), &payload); err != nil {
		return nil, fmt.Errorf("parse translation response JSON: %w", err)
	}

	rawTranslations, ok := payload["translations"]
	if !ok {
		return nil, errors.New("translation response must contain translations list")
	}

	var rows []map[string]any
	if err := json.Unmarshal(rawTranslations, &rows); err != nil {
		return nil, errors.New("translation response must contain translations list")
	}

	results := make([]TranslationResult, 0, len(rows))
	seen := make(map[string]bool, len(rows))
	for _, row := range rows {
		itemID, ok := row["item_id"].(string)
		if !ok || !expectedItemIDs[itemID] {
			return nil, fmt.Errorf("unexpected item_id: %v", row["item_id"])
		}
		if seen[itemID] {
			return nil, fmt.Errorf("duplicate item_id: %s", itemID)
		}

		translatedText, ok := row["translated_text"].(string)
		translatedText = strings.TrimSpace(translatedText)
		if !ok || translatedText == "" {
			return nil, fmt.Errorf("empty translated_text for item_id: %s", itemID)
		}

		seen[itemID] = true
		results = append(results, TranslationResult{
			ItemID:         itemID,
			TranslatedText: translatedText,
		})
	}

	missing := make([]string, 0)
	for expectedID := range expectedItemIDs {
		if !seen[expectedID] {
			missing = append(missing, expectedID)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return nil, fmt.Errorf("missing item_id values: %v", missing)
	}

	return results, nil
}

func (p *LocalOpenAICompatibleProvider) Translate(
	ctx context.Context,
	items []TranslationInput,
	timeoutSeconds int,
) ([]TranslationResult, error) {
	if len(items) == 0 {
		return []TranslationResult{}, nil
	}
	start := time.Now()
	p.logger.Info(
		"translation provider request started",
		"item_count", len(items),
		"timeout_seconds", timeoutSeconds,
		"max_tokens", p.maxTokens,
	)

	prompt, err := p.renderPrompt(items)
	if err != nil {
		p.logger.Error(
			"translation provider request failed",
			"err", err,
			"item_count", len(items),
			"duration_ms", time.Since(start).Milliseconds(),
		)
		return nil, err
	}

	requestBody := map[string]any{
		"model":       p.model,
		"messages":    []map[string]string{{"role": "user", "content": prompt}},
		"temperature": 0,
		"max_tokens":  p.maxTokens,
	}
	for key, value := range p.extraBody {
		requestBody[key] = value
	}

	body, err := json.Marshal(requestBody)
	if err != nil {
		p.logger.Error(
			"translation provider request failed",
			"err", err,
			"item_count", len(items),
			"duration_ms", time.Since(start).Milliseconds(),
		)
		return nil, fmt.Errorf("marshal translation provider request: %w", err)
	}

	if timeoutSeconds > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(timeoutSeconds)*time.Second)
		defer cancel()
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		p.baseURL+"/chat/completions",
		bytes.NewReader(body),
	)
	if err != nil {
		p.logger.Error(
			"translation provider request failed",
			"err", err,
			"item_count", len(items),
			"duration_ms", time.Since(start).Milliseconds(),
		)
		return nil, fmt.Errorf("build translation provider request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+p.apiKey)

	resp, err := p.client.Do(req)
	if err != nil {
		p.logger.Error(
			"translation provider request failed",
			"err", err,
			"item_count", len(items),
			"duration_ms", time.Since(start).Milliseconds(),
		)
		return nil, fmt.Errorf("send translation provider request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		responseBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		err := fmt.Errorf("translation provider returned %s: %s", resp.Status, strings.TrimSpace(string(responseBody)))
		p.logger.Error(
			"translation provider request failed",
			"err", err,
			"status", resp.Status,
			"status_code", resp.StatusCode,
			"item_count", len(items),
			"duration_ms", time.Since(start).Milliseconds(),
		)
		return nil, err
	}

	var providerResponse struct {
		Choices []struct {
			Message struct {
				Content *string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&providerResponse); err != nil {
		p.logger.Error(
			"translation provider request failed",
			"err", err,
			"status_code", resp.StatusCode,
			"item_count", len(items),
			"duration_ms", time.Since(start).Milliseconds(),
		)
		return nil, fmt.Errorf("decode translation provider response: %w", err)
	}
	if len(providerResponse.Choices) == 0 || providerResponse.Choices[0].Message.Content == nil {
		err := errors.New("translation response content is empty")
		p.logger.Error(
			"translation provider request failed",
			"err", err,
			"status_code", resp.StatusCode,
			"item_count", len(items),
			"duration_ms", time.Since(start).Milliseconds(),
		)
		return nil, err
	}

	expectedItemIDs := make(map[string]bool, len(items))
	for _, item := range items {
		expectedItemIDs[item.ItemID] = true
	}
	results, err := ParseTranslationResponse(*providerResponse.Choices[0].Message.Content, expectedItemIDs)
	if err != nil {
		p.logger.Error(
			"translation provider request failed",
			"err", err,
			"status_code", resp.StatusCode,
			"item_count", len(items),
			"duration_ms", time.Since(start).Milliseconds(),
		)
		return nil, err
	}
	p.logger.Info(
		"translation provider request completed",
		"item_count", len(items),
		"translated_count", len(results),
		"status_code", resp.StatusCode,
		"duration_ms", time.Since(start).Milliseconds(),
	)
	return results, nil
}

func defaultExtraBody() map[string]any {
	return map[string]any{
		"chat_template_kwargs": map[string]any{
			"enable_thinking": false,
		},
	}
}

func cloneMap(values map[string]any) map[string]any {
	cloned := make(map[string]any, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func stripJSONFence(responseText string) string {
	stripped := strings.TrimSpace(responseText)
	if strings.HasPrefix(stripped, "```json") && strings.HasSuffix(stripped, "```") {
		return strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(stripped, "```json"), "```"))
	}
	if strings.HasPrefix(stripped, "```") && strings.HasSuffix(stripped, "```") {
		return strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(stripped, "```"), "```"))
	}
	return stripped
}
