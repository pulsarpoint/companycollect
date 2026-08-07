package translation_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/pulsarpoint/corpscout/translator/internal/queue"
	"github.com/pulsarpoint/corpscout/translator/internal/translation"
)

func TestTranslateItemsRetriesModelOutputFailureWithShuffledItems(t *testing.T) {
	ctx := context.Background()
	items := testQueueItems(4)
	translator := &modelOutputThenSuccessTranslator{failuresBeforeSuccess: 1}

	output, failed, err := translation.TranslateItems(
		ctx,
		translator,
		items,
		30,
		"local",
		"qwen3:6b",
		testPromptData(),
	)
	if err != nil {
		t.Fatalf("TranslateItems() error = %v, want nil", err)
	}
	if len(output) != 4 || len(failed) != 0 {
		t.Fatalf("TranslateItems() returned %d output and %d failed rows, want 4 output and 0 failed", len(output), len(failed))
	}
	if output[0].Provider != "local" || output[0].Model != "qwen3:6b" {
		t.Fatalf("translated metadata = %s/%s, want local/qwen3:6b", output[0].Provider, output[0].Model)
	}
	if got := len(translator.calls); got != 2 {
		t.Fatalf("translator calls = %d, want 2", got)
	}
	if slices.Equal(translator.calls[0], translator.calls[1]) {
		t.Fatalf("second translator call order = %v, want shuffled order different from first", translator.calls[1])
	}
}

func TestTranslateItemsSplitsBatchAfterRepeatedModelOutputFailures(t *testing.T) {
	ctx := context.Background()
	items := testQueueItems(4)
	translator := &failLargeBatchTranslator{maxSuccessfulBatchSize: 2}

	output, failed, err := translation.TranslateItems(
		ctx,
		translator,
		items,
		30,
		"local",
		"qwen3:6b",
		testPromptData(),
	)
	if err != nil {
		t.Fatalf("TranslateItems() error = %v, want nil", err)
	}
	if len(output) != 4 || len(failed) != 0 {
		t.Fatalf("TranslateItems() returned %d output and %d failed rows, want 4 output and 0 failed", len(output), len(failed))
	}

	wantSizes := []int{4, 4, 2, 2}
	if !slices.Equal(translator.callSizes, wantSizes) {
		t.Fatalf("translator call sizes = %v, want %v", translator.callSizes, wantSizes)
	}
}

func TestTranslateItemsMarksSingleItemFailedAfterRepeatedModelOutputFailures(t *testing.T) {
	ctx := context.Background()
	items := testQueueItems(1)

	output, failed, err := translation.TranslateItems(
		ctx,
		alwaysModelOutputFailureTranslator{},
		items,
		30,
		"local",
		"qwen3:6b",
		testPromptData(),
	)
	if err != nil {
		t.Fatalf("TranslateItems() error = %v, want nil", err)
	}
	if len(output) != 0 || len(failed) != 1 {
		t.Fatalf("TranslateItems() returned %d output and %d failed rows, want 0 output and 1 failed", len(output), len(failed))
	}
	if failed[0].ItemID != items[0].ItemID {
		t.Fatalf("failed item id = %q, want %q", failed[0].ItemID, items[0].ItemID)
	}
	if failed[0].ErrorMessage == "" {
		t.Fatal("expected failed item error message")
	}
}

func TestTranslateItemsReturnsTransientProviderError(t *testing.T) {
	ctx := context.Background()
	wantErr := errors.New("provider unavailable")

	output, failed, err := translation.TranslateItems(
		ctx,
		transientFailureTranslator{err: wantErr},
		testQueueItems(1),
		30,
		"local",
		"qwen3:6b",
		testPromptData(),
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("TranslateItems() error = %v, want %v", err, wantErr)
	}
	if len(output) != 0 || len(failed) != 0 {
		t.Fatalf("TranslateItems() returned %d output and %d failed rows, want none on transient error", len(output), len(failed))
	}
}

func TestTranslateItemsRecoversTruncatedSingleItemWithValidatedFragments(t *testing.T) {
	ctx := context.Background()
	sourceText := "Att äga och förvalta samfälld mark för fastigheterna Gastorp 2:184, 2:171, 2:182, 2:186, 2:199, 2:202, 9:4, 2:35, 2:40 och 2:42 jämte gemensamhetsanordningarna."
	items := testQueueItems(1)
	items[0].SourceText = sourceText
	translator := &truncateLongOutputTranslator{maxSourceRunes: 32}

	output, failed, err := translation.TranslateItems(
		ctx,
		translator,
		items,
		30,
		"local",
		"qwen3:6b",
		testPromptData(),
	)
	if err != nil {
		t.Fatalf("TranslateItems() error = %v, want nil", err)
	}
	if len(output) != 1 || len(failed) != 0 {
		t.Fatalf("TranslateItems() returned %d output and %d failed rows, want 1 output and 0 failed", len(output), len(failed))
	}
	if output[0].SourceText != sourceText {
		t.Fatalf("output source text = %q, want original source text", output[0].SourceText)
	}
	if output[0].TranslatedText != sourceText {
		t.Fatalf("reassembled translation = %q, want %q", output[0].TranslatedText, sourceText)
	}
	if translator.truncatedCalls < 2 {
		t.Fatalf("truncated calls = %d, want recursive fragment recovery", translator.truncatedCalls)
	}
	if translator.successfulCalls < 2 {
		t.Fatalf("successful fragment calls = %d, want multiple validated fragments", translator.successfulCalls)
	}
}

func TestTranslateItemsSplitsOversizedPunctuationDelimitedToken(t *testing.T) {
	ctx := context.Background()
	sourceText := "gruppnummer: 6,11,14,16,18,20,21,24,25,28,29,30,31,32."
	items := testQueueItems(1)
	items[0].SourceText = sourceText
	translator := &truncateLongOutputTranslator{maxSourceRunes: 12}

	output, failed, err := translation.TranslateItems(
		ctx,
		translator,
		items,
		30,
		"local",
		"qwen3:6b",
		testPromptData(),
	)
	if err != nil {
		t.Fatalf("TranslateItems() error = %v, want nil", err)
	}
	if len(output) != 1 || len(failed) != 0 {
		t.Fatalf("TranslateItems() returned %d output and %d failed rows, want 1 output and 0 failed", len(output), len(failed))
	}
	if strings.ReplaceAll(output[0].TranslatedText, " ", "") != strings.ReplaceAll(sourceText, " ", "") {
		t.Fatalf("reassembled translation = %q, want punctuation-delimited values preserved from %q", output[0].TranslatedText, sourceText)
	}
}

func testPromptData() translation.PromptData {
	return translation.PromptData{
		SourceLanguage: "Norwegian",
		TargetLanguage: "English",
	}
}

func testQueueItems(count int) []queue.Item {
	items := make([]queue.Item, 0, count)
	for i := range count {
		itemID := fmt.Sprintf("item-%d", i+1)
		items = append(items, queue.Item{
			ItemID:         itemID,
			SourceTable:    "corpscout.no_companies",
			SourceColumn:   "activity_text_original",
			SourceText:     fmt.Sprintf("source text %d", i+1),
			SourceTextHash: uint64(i + 1),
			SourceLang:     "no",
			TargetLang:     "en",
		})
	}
	return items
}

type modelOutputThenSuccessTranslator struct {
	failuresBeforeSuccess int
	calls                 [][]string
}

func (t *modelOutputThenSuccessTranslator) Translate(
	ctx context.Context,
	items []translation.TranslationInput,
	timeoutSeconds int,
	promptData translation.PromptData,
) ([]translation.TranslationResult, error) {
	t.calls = append(t.calls, translationItemIDs(items))
	if len(t.calls) <= t.failuresBeforeSuccess {
		return nil, fmt.Errorf("fake model output: %w", translation.ErrModelOutput)
	}
	return successfulTranslations(items), nil
}

type failLargeBatchTranslator struct {
	maxSuccessfulBatchSize int
	callSizes              []int
}

func (t *failLargeBatchTranslator) Translate(
	ctx context.Context,
	items []translation.TranslationInput,
	timeoutSeconds int,
	promptData translation.PromptData,
) ([]translation.TranslationResult, error) {
	t.callSizes = append(t.callSizes, len(items))
	if len(items) > t.maxSuccessfulBatchSize {
		return nil, fmt.Errorf("fake model output: %w", translation.ErrModelOutput)
	}
	return successfulTranslations(items), nil
}

type alwaysModelOutputFailureTranslator struct{}

func (alwaysModelOutputFailureTranslator) Translate(
	ctx context.Context,
	items []translation.TranslationInput,
	timeoutSeconds int,
	promptData translation.PromptData,
) ([]translation.TranslationResult, error) {
	return nil, fmt.Errorf("fake model output: %w", translation.ErrModelOutput)
}

type transientFailureTranslator struct {
	err error
}

type truncateLongOutputTranslator struct {
	maxSourceRunes  int
	truncatedCalls  int
	successfulCalls int
}

func (t *truncateLongOutputTranslator) Translate(
	ctx context.Context,
	items []translation.TranslationInput,
	timeoutSeconds int,
	promptData translation.PromptData,
) ([]translation.TranslationResult, error) {
	if len(items) != 1 || utf8.RuneCountInString(items[0].SourceText) > t.maxSourceRunes {
		t.truncatedCalls++
		return nil, fmt.Errorf("fake truncated output: %w: %w", translation.ErrModelOutput, translation.ErrOutputTruncated)
	}

	t.successfulCalls++
	return []translation.TranslationResult{{
		ItemID:         items[0].ItemID,
		TranslatedText: strings.TrimSpace(items[0].SourceText),
	}}, nil
}

func (t transientFailureTranslator) Translate(
	ctx context.Context,
	items []translation.TranslationInput,
	timeoutSeconds int,
	promptData translation.PromptData,
) ([]translation.TranslationResult, error) {
	return nil, t.err
}

func successfulTranslations(items []translation.TranslationInput) []translation.TranslationResult {
	results := make([]translation.TranslationResult, 0, len(items))
	for _, item := range items {
		results = append(results, translation.TranslationResult{
			ItemID:         item.ItemID,
			TranslatedText: "translated " + item.SourceText,
		})
	}
	return results
}

func translationItemIDs(items []translation.TranslationInput) []string {
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.ItemID)
	}
	return ids
}
