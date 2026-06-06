package llm

import (
	"strings"
	"testing"

	"github.com/pulsarpoint/companycollect/data-pipelines/golang-translate/internal/fixture"
)

func TestNormalizeOpenAIBaseURL(t *testing.T) {
	cases := map[string]string{
		"http://host:8888":                     "http://host:8888/v1",
		"http://host:8888/v1":                  "http://host:8888/v1",
		"http://host:8888/v1/chat/completions": "http://host:8888/v1",
		"http://host:8888/chat/completions":    "http://host:8888/v1",
	}
	for input, want := range cases {
		if got := NormalizeOpenAIBaseURL(input); got != want {
			t.Fatalf("normalize %q = %q, want %q", input, got, want)
		}
	}
}

func TestBuildTranslationMessagesMatchesServicePromptShape(t *testing.T) {
	messages := BuildTranslationMessages("et", "en", []fixture.Item{
		{ID: "one", Category: "activity", Text: "programmeerimine"},
	})
	if len(messages) != 1 {
		t.Fatalf("message count = %d, want 1", len(messages))
	}
	content := messages[0].Content
	for _, required := range []string{"/no_think", "Translate et business registry text to en", "Return only JSON", "\"id\":\"one\""} {
		if !strings.Contains(content, required) {
			t.Fatalf("prompt does not contain %q: %s", required, content)
		}
	}
}

func TestParseTranslationsReadsTranslationsList(t *testing.T) {
	expected := map[string]struct{}{"one": {}, "two": {}}
	got, err := ParseTranslations(`{"translations":[{"id":"one","translation":"Programming"},{"id":"two","translation":"Retail"}]}`, expected)
	if err != nil {
		t.Fatalf("parse translations: %v", err)
	}
	if got["one"] != "Programming" || got["two"] != "Retail" {
		t.Fatalf("translations = %#v", got)
	}
}

func TestTranslationMaxTokensUsesServiceBounds(t *testing.T) {
	if got := TranslationMaxTokens(1); got != 512 {
		t.Fatalf("max tokens for one item = %d, want 512", got)
	}
	if got := TranslationMaxTokens(100); got != 4096 {
		t.Fatalf("max tokens for many items = %d, want 4096", got)
	}
}
