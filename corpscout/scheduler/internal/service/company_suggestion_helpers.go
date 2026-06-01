package service

import (
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

const emptyJSONObject = "{}"

func suggestionCompanyID(suggestion db.Suggestion) (uuid.UUID, bool) {
	if suggestion.TargetCompanyID.Valid {
		return uuid.UUID(suggestion.TargetCompanyID.Bytes), true
	}
	if suggestion.CreatedCompanyID.Valid {
		return uuid.UUID(suggestion.CreatedCompanyID.Bytes), true
	}
	return uuid.Nil, false
}

func validateSuggestionChild(parentID, childParentID uuid.UUID, status string, childID uuid.UUID) error {
	if childParentID != parentID {
		return errors.Newf("child suggestion %s belongs to suggestion %s, not %s", childID, childParentID, parentID)
	}
	if status != "pending" {
		return errors.Newf("child suggestion %s is not pending (status=%s)", childID, status)
	}
	return nil
}

func missingSuggestionCompanyError(suggestionID uuid.UUID, item CompanySuggestionReviewItem) error {
	return errors.Newf("suggestion %s has no target company; apply a profile section before %s %s", suggestionID, item.Table, item.ID)
}

func firstNonEmpty(values ...*string) string {
	for _, value := range values {
		if value == nil {
			continue
		}
		trimmed := strings.TrimSpace(*value)
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func firstStringPtr(values ...*string) *string {
	value := firstNonEmpty(values...)
	if value == "" {
		return nil
	}
	return &value
}

func nonEmptyJSON(value []byte) []byte {
	if len(value) == 0 || string(value) == "null" {
		return []byte(emptyJSONObject)
	}
	return value
}
