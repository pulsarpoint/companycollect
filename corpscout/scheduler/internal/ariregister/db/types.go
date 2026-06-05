package ariregisterdb

import (
	"encoding/json"

	"github.com/google/uuid"
)

const jsonPayloadEmptyObject = "{}"

type IngestRawRecordsResult struct {
	RowsSeen              int32
	RowsWritten           int32
	RowsInsertedNew       int32
	RowsExistingUnchanged int32
	RowsNewVersions       int32
	RawRecordIDs          []uuid.UUID
}

type TranslationTermResult struct {
	SourceLang           string
	TargetLang           string
	SourceTextNormalized string
	SourceText           string
	TermKey              string
	TranslatedText       string
	Status               string
	Provider             string
	Model                string
	PromptVersion        string
	Error                string
	ErrorCode            string
	Metadata             map[string]any
}

type UpsertTranslationTermsCommand struct {
	Terms []TranslationTermResult
}

type UpsertTranslationTermsResult struct {
	TermsUpserted int32
}

func jsonObject(value []byte) []byte {
	if len(value) == 0 {
		return []byte(jsonPayloadEmptyObject)
	}
	if !json.Valid(value) {
		return []byte(jsonPayloadEmptyObject)
	}
	return value
}
