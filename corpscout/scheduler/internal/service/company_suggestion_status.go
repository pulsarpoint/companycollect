package service

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

func updateSuggestionAggregateStatus(ctx context.Context, qtx *db.Queries, suggestionID uuid.UUID, reviewedBy, reviewNote string) error {
	counts, err := qtx.CountSuggestionReviewItemStatuses(ctx, suggestionID)
	if err != nil {
		return errors.Wrap(err, "count company suggestion review item statuses")
	}
	status := aggregateSuggestionStatus(counts.PendingCount, counts.AppliedCount, counts.RejectedCount)
	if err := qtx.UpdateSuggestionAggregateStatus(ctx, db.UpdateSuggestionAggregateStatusParams{
		ID:         suggestionID,
		Status:     status,
		ReviewedBy: &reviewedBy,
		ReviewNote: &reviewNote,
	}); err != nil {
		return errors.Wrap(err, "update suggestion aggregate status")
	}
	return nil
}

func aggregateSuggestionStatus(pending, applied, rejected int64) string {
	switch {
	case pending > 0 && (applied > 0 || rejected > 0):
		return "partially_applied"
	case pending > 0:
		return "pending"
	case applied > 0 && rejected == 0:
		return "applied"
	case rejected > 0 && applied == 0:
		return "rejected"
	case applied > 0 && rejected > 0:
		return "partially_applied"
	default:
		return "pending"
	}
}
