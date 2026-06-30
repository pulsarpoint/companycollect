package translation_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pulsarpoint/corpscout/translator/internal/translation"
)

func TestBuildTranslationPromptContainsStrictJSONInstructionsAndInputItems(t *testing.T) {
	prompt, err := translation.BuildTranslationPrompt([]translation.TranslationInput{
		{ItemID: "item-1", SourceText: "Holdingselskap"},
	})
	if err != nil {
		t.Fatalf("build prompt: %v", err)
	}

	required := []string{
		"/no_think",
		"Translate each Norwegian company registry text fragment to English.",
		"Return only valid JSON",
		`"source_language":"Norwegian"`,
		`"target_language":"English"`,
		`"item_id":"item-1"`,
		`"source_text":"Holdingselskap"`,
	}
	for _, fragment := range required {
		if !strings.Contains(prompt, fragment) {
			t.Fatalf("expected prompt to contain %q\nprompt:\n%s", fragment, prompt)
		}
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
						"content": "{\"translations\":[{\"item_id\":\"item-1\",\"translated_text\":\"Holding company\"}]}"
					}
				}
			]
		}`))
	}))
	defer server.Close()

	provider, err := translation.NewLocalOpenAICompatibleProvider(
		server.URL+"/v1",
		"qwen3:6b",
		"test-key",
		123,
		map[string]any{"chat_template_kwargs": map[string]any{"enable_thinking": false}},
	)
	if err != nil {
		t.Fatalf("new provider: %v", err)
	}

	results, err := provider.Translate(context.Background(), []translation.TranslationInput{
		{ItemID: "item-1", SourceText: "Holdingselskap"},
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
