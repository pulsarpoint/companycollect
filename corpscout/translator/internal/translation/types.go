package translation

import "context"

type TranslationInput struct {
	ItemID     string
	SourceText string
}

type TranslationResult struct {
	ItemID         string
	TranslatedText string
}

type Translator interface {
	Translate(ctx context.Context, items []TranslationInput, timeoutSeconds int) ([]TranslationResult, error)
}
