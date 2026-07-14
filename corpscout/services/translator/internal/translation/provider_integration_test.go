package translation_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pulsarpoint/corpscout/translator/internal/config"
	"github.com/pulsarpoint/corpscout/translator/internal/translation"
)

func TestLocalLLMTranslatesNorwegianBatchToEnglish(t *testing.T) {
	if os.Getenv("TRANSLATOR_INTEGRATION_TESTS") != "true" {
		t.Skip("set TRANSLATOR_INTEGRATION_TESTS=true to run against the configured local LLM")
	}

	configPath := os.Getenv(config.ConfigFileEnv)
	if configPath == "" {
		configPath = "../../config/translator.json"
	}
	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatalf("load translator config: %v", err)
	}

	endpoint, ok := cfg.Endpoints["local_llm"]
	if !ok {
		t.Fatal("translator config must define endpoints.local_llm")
	}
	if endpoint.BaseURL == "" {
		t.Fatal("local_llm base_url is required")
	}
	if endpoint.Model == "" {
		t.Fatal("local_llm model is required")
	}

	provider, err := translation.Init(translation.Config{
		BaseURL:   endpoint.BaseURL,
		Model:     endpoint.Model,
		APIKey:    endpoint.APIKey,
		MaxTokens: endpoint.MaxTokens,
		ExtraBody: endpoint.ExtraBody,
	})
	if err != nil {
		t.Fatalf("init translation provider: %v", err)
	}

	items := []translation.TranslationInput{
		{ItemID: "item-01", SourceText: "Holdingselskap", SourceLang: "no", TargetLang: "en"},
		{ItemID: "item-02", SourceText: "Utvikling av programvare", SourceLang: "no", TargetLang: "en"},
		{ItemID: "item-03", SourceText: "Konsulentvirksomhet innen informasjonsteknologi", SourceLang: "no", TargetLang: "en"},
		{ItemID: "item-04", SourceText: "Kjøp og salg av fast eiendom", SourceLang: "no", TargetLang: "en"},
		{ItemID: "item-05", SourceText: "Investering i aksjer og andre verdipapirer", SourceLang: "no", TargetLang: "en"},
		{ItemID: "item-06", SourceText: "Regnskapsføring og økonomisk rådgivning", SourceLang: "no", TargetLang: "en"},
		{ItemID: "item-07", SourceText: "Bygging av boliger", SourceLang: "no", TargetLang: "en"},
		{ItemID: "item-08", SourceText: "Transport av gods på vei", SourceLang: "no", TargetLang: "en"},
		{ItemID: "item-09", SourceText: "Servering og drift av restaurant", SourceLang: "no", TargetLang: "en"},
		{ItemID: "item-10", SourceText: "Engroshandel med maskiner og utstyr", SourceLang: "no", TargetLang: "en"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	results, err := provider.Translate(ctx, items, 180, translation.PromptData{
		SourceLanguage: "Norwegian",
		TargetLanguage: "English",
	})
	if err != nil {
		t.Fatalf(
			"translate Norwegian batch using endpoint=%s model=%s: %v",
			endpoint.BaseURL,
			endpoint.Model,
			err,
		)
	}

	if len(results) != len(items) {
		t.Fatalf("expected %d translation results, got %d: %+v", len(items), len(results), results)
	}

	sourceByID := make(map[string]string, len(items))
	for _, item := range items {
		sourceByID[item.ItemID] = item.SourceText
	}

	seen := make(map[string]bool, len(results))
	for _, result := range results {
		sourceText, ok := sourceByID[result.ItemID]
		if !ok {
			t.Fatalf("unexpected result item_id %q", result.ItemID)
		}
		if seen[result.ItemID] {
			t.Fatalf("duplicate result item_id %q", result.ItemID)
		}
		seen[result.ItemID] = true

		translated := strings.TrimSpace(result.TranslatedText)
		if translated == "" {
			t.Fatalf("empty translation for %s", result.ItemID)
		}
		if strings.EqualFold(translated, sourceText) {
			t.Fatalf("translation for %s did not change source text %q", result.ItemID, sourceText)
		}
		if strings.Contains(translated, "```") || strings.Contains(strings.ToLower(translated), "<think") {
			t.Fatalf("translation for %s contains non-answer markup: %q", result.ItemID, translated)
		}
	}

	for _, item := range items {
		if !seen[item.ItemID] {
			t.Fatalf("missing translation for %s", item.ItemID)
		}
	}

	t.Logf("translated %d Norwegian entries using endpoint=%s model=%s", len(results), endpoint.BaseURL, endpoint.Model)
}
