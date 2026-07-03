package translation_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

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
