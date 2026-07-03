package translation

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/pulsarpoint/corpscout/translator/internal/queue"
)

// TranslateItems translates queue items and applies the deterministic-output
// recovery policy for LLM responses.
//
// Temporal retries are useful for transient provider failures, but they are
// usually useless for invalid model output. With temperature=0, the same prompt
// tends to produce the same malformed JSON, missing ID, duplicate ID,
// unexpected ID, or empty translation again.
//
// The recovery order is intentionally local to this function because it already
// has the concrete queue items. First it retries the same items in a
// deterministic shuffled order to change the prompt without changing batch
// size. If that still produces a model-output error, it splits the item list in
// half and retries each half. A single item that still cannot produce a valid
// response is returned as failed so the caller can write it to failed_items and
// stop that one row from blocking the queue.
//
// Non-model-output errors are returned to the caller, usually a Temporal
// activity, to retry. Those errors represent infrastructure or provider
// failures where normal activity retry is the right behavior.
func TranslateItems(
	ctx context.Context,
	translator Translator,
	items []queue.Item,
	timeoutSeconds int,
	provider string,
	model string,
	promptData PromptData,
) ([]queue.TranslatedItem, []queue.FailedItem, error) {
	if translator == nil {
		return nil, nil, errors.New("translator is required")
	}
	if len(items) == 0 {
		return nil, nil, nil
	}

	output, err := translateItemsOnce(ctx, translator, items, timeoutSeconds, provider, model, promptData)
	if err == nil {
		return output, nil, nil
	}
	if !errors.Is(err, ErrModelOutput) {
		return nil, nil, err
	}

	shuffled := shuffledItems(items)
	output, shuffledErr := translateItemsOnce(ctx, translator, shuffled, timeoutSeconds, provider, model, promptData)
	if shuffledErr == nil {
		return output, nil, nil
	}
	if !errors.Is(shuffledErr, ErrModelOutput) {
		return nil, nil, shuffledErr
	}

	if len(items) == 1 {
		return nil, []queue.FailedItem{{
			Item:         items[0],
			ErrorMessage: shuffledErr.Error(),
		}}, nil
	}

	midpoint := len(items) / 2
	leftOutput, leftFailed, err := TranslateItems(ctx, translator, items[:midpoint], timeoutSeconds, provider, model, promptData)
	if err != nil {
		return nil, nil, err
	}
	rightOutput, rightFailed, err := TranslateItems(ctx, translator, items[midpoint:], timeoutSeconds, provider, model, promptData)
	if err != nil {
		return nil, nil, err
	}

	combinedOutput := make([]queue.TranslatedItem, 0, len(leftOutput)+len(rightOutput))
	combinedOutput = append(combinedOutput, leftOutput...)
	combinedOutput = append(combinedOutput, rightOutput...)

	combinedFailed := make([]queue.FailedItem, 0, len(leftFailed)+len(rightFailed))
	combinedFailed = append(combinedFailed, leftFailed...)
	combinedFailed = append(combinedFailed, rightFailed...)

	return combinedOutput, combinedFailed, nil
}

// translateItemsOnce performs exactly one translator request and validates that
// the response is a complete one-to-one mapping for the requested items.
// Validation errors are wrapped with ErrModelOutput so callers can distinguish
// deterministic bad model output from transient provider errors.
func translateItemsOnce(
	ctx context.Context,
	translator Translator,
	items []queue.Item,
	timeoutSeconds int,
	provider string,
	model string,
	promptData PromptData,
) ([]queue.TranslatedItem, error) {
	inputs := make([]TranslationInput, 0, len(items))
	expectedItemIDs := make(map[string]bool, len(items))
	for _, item := range items {
		inputs = append(inputs, TranslationInput{
			ItemID:     item.ItemID,
			SourceText: item.SourceText,
			SourceLang: item.SourceLang,
			TargetLang: item.TargetLang,
		})
		expectedItemIDs[item.ItemID] = true
	}

	results, err := translator.Translate(ctx, inputs, timeoutSeconds, promptData)
	if err != nil {
		return nil, fmt.Errorf("translate queue batch: %w", err)
	}

	resultsByID := make(map[string]string, len(results))
	for _, result := range results {
		if result.ItemID == "" {
			return nil, fmt.Errorf("%w: translation result item_id is required", ErrModelOutput)
		}
		if !expectedItemIDs[result.ItemID] {
			return nil, fmt.Errorf("%w: unexpected translation result item_id: %s", ErrModelOutput, result.ItemID)
		}
		if result.TranslatedText == "" {
			return nil, fmt.Errorf("%w: translation result for %s has empty translated text", ErrModelOutput, result.ItemID)
		}
		if _, exists := resultsByID[result.ItemID]; exists {
			return nil, fmt.Errorf("%w: translation result for %s is duplicated", ErrModelOutput, result.ItemID)
		}
		resultsByID[result.ItemID] = result.TranslatedText
	}

	output := make([]queue.TranslatedItem, 0, len(items))
	for _, item := range items {
		translatedText, ok := resultsByID[item.ItemID]
		if !ok {
			return nil, fmt.Errorf("%w: translation result for %s is missing", ErrModelOutput, item.ItemID)
		}
		output = append(output, queue.TranslatedItem{
			Item:           item,
			TranslatedText: translatedText,
			Provider:       provider,
			Model:          model,
		})
	}
	return output, nil
}

func shuffledItems(items []queue.Item) []queue.Item {
	shuffled := slices.Clone(items)
	slices.Reverse(shuffled)
	return shuffled
}
