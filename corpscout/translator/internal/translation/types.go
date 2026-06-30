package translation

import (
	"context"
	"errors"
)

// ErrModelOutput marks deterministic invalid model responses, such as malformed
// JSON, missing item IDs, duplicate item IDs, unexpected item IDs, or empty
// translated text. Callers can handle this differently from transient provider
// failures because retrying the same prompt at temperature=0 usually reproduces
// the same invalid response.
var ErrModelOutput = errors.New("translation model output is invalid")

type TranslationInput struct {
	ItemID     string
	SourceText string
	SourceLang string
	TargetLang string
}

type TranslationResult struct {
	ItemID         string
	TranslatedText string
}

type Translator interface {
	Translate(ctx context.Context, items []TranslationInput, timeoutSeconds int) ([]TranslationResult, error)
}
