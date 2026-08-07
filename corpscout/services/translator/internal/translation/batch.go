package translation

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/pulsarpoint/corpscout/translator/internal/queue"
)

const (
	truncatedOutputChunkRuneLimit = 48
	minimumOutputChunkRuneLimit   = 8
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
	if errors.Is(err, ErrOutputTruncated) {
		return recoverTruncatedItems(ctx, translator, items, timeoutSeconds, provider, model, promptData)
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

func recoverTruncatedItems(
	ctx context.Context,
	translator Translator,
	items []queue.Item,
	timeoutSeconds int,
	provider string,
	model string,
	promptData PromptData,
) ([]queue.TranslatedItem, []queue.FailedItem, error) {
	output := make([]queue.TranslatedItem, 0, len(items))
	failed := make([]queue.FailedItem, 0)

	for _, item := range items {
		singleOutput, err := translateItemsOnce(
			ctx,
			translator,
			[]queue.Item{item},
			timeoutSeconds,
			provider,
			model,
			promptData,
		)
		if errors.Is(err, ErrOutputTruncated) {
			translatedItem, recoveryErr := translateItemInFragments(
				ctx,
				translator,
				item,
				timeoutSeconds,
				provider,
				model,
				promptData,
				truncatedOutputChunkRuneLimit,
			)
			if recoveryErr == nil {
				output = append(output, translatedItem)
				continue
			}
			err = recoveryErr
		} else if errors.Is(err, ErrModelOutput) {
			singleOutput, err = translateItemsOnce(
				ctx,
				translator,
				[]queue.Item{item},
				timeoutSeconds,
				provider,
				model,
				promptData,
			)
		}

		if err == nil {
			output = append(output, singleOutput[0])
			continue
		}
		if !errors.Is(err, ErrModelOutput) {
			return nil, nil, err
		}
		failed = append(failed, queue.FailedItem{
			Item:         item,
			ErrorMessage: err.Error(),
		})
	}

	return output, failed, nil
}

func translateItemInFragments(
	ctx context.Context,
	translator Translator,
	item queue.Item,
	timeoutSeconds int,
	provider string,
	model string,
	promptData PromptData,
	chunkRuneLimit int,
) (queue.TranslatedItem, error) {
	fragments := splitTranslationText(item.SourceText, chunkRuneLimit)
	if len(fragments) < 2 {
		nextLimit := smallerChunkRuneLimit(item.SourceText, chunkRuneLimit)
		if nextLimit == 0 {
			return queue.TranslatedItem{}, fmt.Errorf(
				"%w: %w after fragmenting source text to %d runes",
				ErrModelOutput,
				ErrOutputTruncated,
				chunkRuneLimit,
			)
		}
		fragments = splitTranslationText(item.SourceText, nextLimit)
		chunkRuneLimit = nextLimit
	}

	translatedFragments := make([]string, 0, len(fragments))
	for _, fragment := range fragments {
		fragmentItem := item
		fragmentItem.SourceText = fragment

		fragmentOutput, err := translateItemsOnce(
			ctx,
			translator,
			[]queue.Item{fragmentItem},
			timeoutSeconds,
			provider,
			model,
			promptData,
		)
		if errors.Is(err, ErrOutputTruncated) {
			nextLimit := smallerChunkRuneLimit(fragment, chunkRuneLimit)
			if nextLimit == 0 {
				return queue.TranslatedItem{}, err
			}
			recoveredFragment, recoveryErr := translateItemInFragments(
				ctx,
				translator,
				fragmentItem,
				timeoutSeconds,
				provider,
				model,
				promptData,
				nextLimit,
			)
			if recoveryErr != nil {
				return queue.TranslatedItem{}, recoveryErr
			}
			translatedFragments = append(translatedFragments, recoveredFragment.TranslatedText)
			continue
		}
		if err != nil {
			return queue.TranslatedItem{}, err
		}
		translatedFragments = append(translatedFragments, fragmentOutput[0].TranslatedText)
	}

	return queue.TranslatedItem{
		Item:           item,
		TranslatedText: strings.Join(translatedFragments, " "),
		Provider:       provider,
		Model:          model,
	}, nil
}

func splitTranslationText(sourceText string, chunkRuneLimit int) []string {
	rawWords := strings.Fields(sourceText)
	if len(rawWords) == 0 || chunkRuneLimit <= 0 {
		return nil
	}
	words := make([]string, 0, len(rawWords))
	for _, word := range rawWords {
		words = append(words, splitOversizedTranslationToken(word, chunkRuneLimit)...)
	}

	fragments := make([]string, 0, (utf8.RuneCountInString(sourceText)/chunkRuneLimit)+1)
	currentWords := make([]string, 0)
	currentRunes := 0
	for _, word := range words {
		wordRunes := utf8.RuneCountInString(word)
		separatorRunes := 0
		if len(currentWords) > 0 {
			separatorRunes = 1
		}
		if len(currentWords) > 0 && currentRunes+separatorRunes+wordRunes > chunkRuneLimit {
			fragments = append(fragments, strings.Join(currentWords, " "))
			currentWords = currentWords[:0]
			currentRunes = 0
			separatorRunes = 0
		}
		currentWords = append(currentWords, word)
		currentRunes += separatorRunes + wordRunes
	}
	if len(currentWords) > 0 {
		fragments = append(fragments, strings.Join(currentWords, " "))
	}
	return fragments
}

func splitOversizedTranslationToken(token string, chunkRuneLimit int) []string {
	remaining := []rune(token)
	if len(remaining) <= chunkRuneLimit {
		return []string{token}
	}

	parts := make([]string, 0)
	for len(remaining) > chunkRuneLimit {
		splitAt := 0
		for index := chunkRuneLimit; index >= 1; index-- {
			if strings.ContainsRune(",;|/\\", remaining[index-1]) {
				splitAt = index
				break
			}
		}
		if splitAt == 0 {
			return []string{token}
		}
		parts = append(parts, string(remaining[:splitAt]))
		remaining = remaining[splitAt:]
	}
	if len(remaining) > 0 {
		parts = append(parts, string(remaining))
	}
	return parts
}

func smallerChunkRuneLimit(sourceText string, currentLimit int) int {
	sourceRunes := utf8.RuneCountInString(strings.TrimSpace(sourceText))
	if sourceRunes <= 1 || currentLimit <= minimumOutputChunkRuneLimit {
		return 0
	}

	nextLimit := min(currentLimit/2, sourceRunes/2)
	return max(minimumOutputChunkRuneLimit, nextLimit)
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
