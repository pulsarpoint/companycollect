package translation_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pulsarpoint/corpscout/translator/internal/translation"
)

func TestInitRendersPackagePromptTemplateWithConfiguredLanguages(t *testing.T) {
	var requestBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"choices": [
				{
					"message": {
						"content": "{\"translations\":[{\"item_id\":\"1\",\"translated_text\":\"Holding company\"}]}"
					}
				}
			]
		}`))
	}))
	defer server.Close()

	provider, err := translation.Init(translation.Config{
		BaseURL:   server.URL + "/v1",
		Model:     "qwen3:6b",
		APIKey:    "test-key",
		MaxTokens: 123,
		ExtraBody: map[string]any{"chat_template_kwargs": map[string]any{"enable_thinking": false}},
		PromptData: translation.PromptData{
			SourceLanguage: "Danish",
			TargetLanguage: "English",
		},
	})
	if err != nil {
		t.Fatalf("init provider: %v", err)
	}

	_, err = provider.Translate(context.Background(), []translation.TranslationInput{
		{
			ItemID:     "item-1",
			SourceText: "Holdingselskab",
			SourceLang: "da",
			TargetLang: "en",
		},
	}, 30)
	if err != nil {
		t.Fatalf("translate: %v", err)
	}

	messages, ok := requestBody["messages"].([]any)
	if !ok || len(messages) != 1 {
		t.Fatalf("expected one user message, got %#v", requestBody["messages"])
	}
	message, ok := messages[0].(map[string]any)
	if !ok || message["role"] != "user" {
		t.Fatalf("expected user message, got %#v", messages[0])
	}
	content, ok := message["content"].(string)
	if !ok {
		t.Fatalf("expected string prompt content, got %#v", message["content"])
	}

	required := []string{
		"Translate each Danish text fragment to English.",
		`"item_id":"1"`,
		`"source_text":"Holdingselskab"`,
		`"source_lang":"da"`,
		`"target_lang":"en"`,
	}
	for _, fragment := range required {
		if !strings.Contains(content, fragment) {
			t.Fatalf("expected prompt to contain %q\nprompt:\n%s", fragment, content)
		}
	}
	if strings.Contains(content, "Norwegian") {
		t.Fatalf("central translation provider must not hardcode Norwegian:\n%s", content)
	}
	if strings.Contains(content, "item-1") {
		t.Fatalf("prompt must not expose canonical item id:\n%s", content)
	}
}

func TestInitRequiresPromptLanguages(t *testing.T) {
	_, err := translation.Init(translation.Config{
		BaseURL: "http://127.0.0.1:8888/v1",
		Model:   "qwen3:6b",
		APIKey:  "test-key",
		PromptData: translation.PromptData{
			TargetLanguage: "English",
		},
	})
	if err == nil {
		t.Fatal("expected missing source language to fail")
	}
	if !strings.Contains(err.Error(), "source_language") {
		t.Fatalf("expected source language error, got %v", err)
	}
}

func TestLocalOpenAICompatibleProviderMapsRequestLocalItemIDsToCanonicalItemIDs(t *testing.T) {
	const canonicalItemID = "corpscout.no_companies|activity_text_original|10012706760871717925|no|en"

	var requestBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
			t.Fatalf("decode request body: %v", err)
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"choices": [
				{
					"message": {
						"content": "{\"translations\":[{\"item_id\":\"1\",\"translated_text\":\"Business consulting\"}]}"
					}
				}
			]
		}`))
	}))
	defer server.Close()

	provider, err := translation.Init(translation.Config{
		BaseURL: server.URL + "/v1",
		Model:   "qwen3:6b",
		APIKey:  "test-key",
		PromptData: translation.PromptData{
			SourceLanguage: "Norwegian",
			TargetLanguage: "English",
		},
	})
	if err != nil {
		t.Fatalf("new provider: %v", err)
	}

	results, err := provider.Translate(context.Background(), []translation.TranslationInput{
		{
			ItemID:     canonicalItemID,
			SourceText: "Bedriftsradgivning",
			SourceLang: "no",
			TargetLang: "en",
		},
	}, 30)
	if err != nil {
		t.Fatalf("Translate(canonical item id) error = %v, want nil", err)
	}
	if len(results) != 1 {
		t.Fatalf("Translate(canonical item id) result count = %d, want 1", len(results))
	}
	if results[0].ItemID != canonicalItemID {
		t.Fatalf("Translate(canonical item id) item id = %q, want %q", results[0].ItemID, canonicalItemID)
	}

	content := requestPromptContent(t, requestBody)
	if strings.Contains(content, canonicalItemID) {
		t.Fatalf("prompt leaked canonical item id %q\nprompt:\n%s", canonicalItemID, content)
	}
	if !strings.Contains(content, `"item_id":"1"`) {
		t.Fatalf("prompt did not contain request-local item id\nprompt:\n%s", content)
	}
}

func TestParseTranslationResponseAcceptsJsonFenceAndRequiresEveryExpectedItem(t *testing.T) {
	results, err := translation.ParseTranslationResponse(
		"```json\n{\"translations\":[{\"item_id\":\"item-1\",\"translated_text\":\"Holding company\"}]}\n```",
		map[string]bool{"item-1": true},
	)
	if err != nil {
		t.Fatalf("parse response: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected one translation result, got %d", len(results))
	}
	if results[0].ItemID != "item-1" || results[0].TranslatedText != "Holding company" {
		t.Fatalf("unexpected result: %+v", results[0])
	}

	_, err = translation.ParseTranslationResponse(
		"{\"translations\":[{\"item_id\":\"item-2\",\"translated_text\":\"Other\"}]}",
		map[string]bool{"item-1": true},
	)
	if err == nil {
		t.Fatal("expected unexpected item id to fail")
	}
	if !strings.Contains(err.Error(), "unexpected item_id") {
		t.Fatalf("expected unexpected item_id error, got %v", err)
	}
}

func TestLocalOpenAICompatibleProviderSendsRequestAndParsesResponse(t *testing.T) {
	var requestBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("expected /v1/chat/completions, got %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Fatalf("expected bearer auth header, got %q", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
			t.Fatalf("decode request body: %v", err)
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"choices": [
				{
					"message": {
						"content": "{\"translations\":[{\"item_id\":\"1\",\"translated_text\":\"Holding company\"}]}"
					}
				}
			]
		}`))
	}))
	defer server.Close()

	provider, err := translation.Init(translation.Config{
		BaseURL:   server.URL + "/v1",
		Model:     "qwen3:6b",
		APIKey:    "test-key",
		MaxTokens: 123,
		ExtraBody: map[string]any{"chat_template_kwargs": map[string]any{"enable_thinking": false}},
		PromptData: translation.PromptData{
			SourceLanguage: "Norwegian",
			TargetLanguage: "English",
		},
	})
	if err != nil {
		t.Fatalf("new provider: %v", err)
	}

	results, err := provider.Translate(context.Background(), []translation.TranslationInput{
		{ItemID: "item-1", SourceText: "Holdingselskap", SourceLang: "no", TargetLang: "en"},
	}, 30)
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	if len(results) != 1 || results[0].TranslatedText != "Holding company" {
		t.Fatalf("unexpected results: %+v", results)
	}

	if requestBody["model"] != "qwen3:6b" {
		t.Fatalf("expected model qwen3:6b, got %#v", requestBody["model"])
	}
	if requestBody["max_tokens"] != float64(123) {
		t.Fatalf("expected max_tokens 123, got %#v", requestBody["max_tokens"])
	}
	if requestBody["temperature"] != float64(0) {
		t.Fatalf("expected temperature 0, got %#v", requestBody["temperature"])
	}
	if _, ok := requestBody["chat_template_kwargs"].(map[string]any); !ok {
		t.Fatalf("expected extra body chat_template_kwargs, got %#v", requestBody["chat_template_kwargs"])
	}
	messages, ok := requestBody["messages"].([]any)
	if !ok || len(messages) != 1 {
		t.Fatalf("expected one user message, got %#v", requestBody["messages"])
	}
	message, ok := messages[0].(map[string]any)
	if !ok || message["role"] != "user" {
		t.Fatalf("expected user message, got %#v", messages[0])
	}
	if content, ok := message["content"].(string); !ok || !strings.Contains(content, "Holdingselskap") {
		t.Fatalf("expected prompt content with source text, got %#v", message["content"])
	}
}

func TestLocalOpenAICompatibleProviderLogsRequestMetadataWithoutSecretsOrSourceText(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"choices": [
				{
					"message": {
						"content": "{\"translations\":[{\"item_id\":\"1\",\"translated_text\":\"Holding company\"}]}"
					}
				}
			]
		}`))
	}))
	defer server.Close()

	provider, err := translation.Init(translation.Config{
		BaseURL:   server.URL + "/v1",
		Model:     "qwen3:6b",
		APIKey:    "secret-api-key",
		MaxTokens: 123,
		Logger:    logger,
		PromptData: translation.PromptData{
			SourceLanguage: "Norwegian",
			TargetLanguage: "English",
		},
	})
	if err != nil {
		t.Fatalf("new provider: %v", err)
	}

	_, err = provider.Translate(context.Background(), []translation.TranslationInput{
		{ItemID: "item-1", SourceText: "Holdingselskap", SourceLang: "no", TargetLang: "en"},
	}, 30)
	if err != nil {
		t.Fatalf("translate: %v", err)
	}

	logText := logs.String()
	required := []string{
		`"msg":"translation provider request started"`,
		`"msg":"translation provider request completed"`,
		`"model":"qwen3:6b"`,
		`"item_count":1`,
		`"timeout_seconds":30`,
	}
	for _, fragment := range required {
		if !strings.Contains(logText, fragment) {
			t.Fatalf("expected provider logs to contain %s\nlogs:\n%s", fragment, logText)
		}
	}
	for _, forbidden := range []string{"secret-api-key", "Holdingselskap"} {
		if strings.Contains(logText, forbidden) {
			t.Fatalf("provider logs leaked %q\nlogs:\n%s", forbidden, logText)
		}
	}
}

func requestPromptContent(t *testing.T, requestBody map[string]any) string {
	t.Helper()

	messages, ok := requestBody["messages"].([]any)
	if !ok || len(messages) != 1 {
		t.Fatalf("expected one user message, got %#v", requestBody["messages"])
	}
	message, ok := messages[0].(map[string]any)
	if !ok || message["role"] != "user" {
		t.Fatalf("expected user message, got %#v", messages[0])
	}
	content, ok := message["content"].(string)
	if !ok {
		t.Fatalf("expected string prompt content, got %#v", message["content"])
	}
	return content
}
