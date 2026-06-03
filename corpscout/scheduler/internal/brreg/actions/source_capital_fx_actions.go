package actions

import (
	"context"
	"log/slog"

	"github.com/cockroachdb/errors"

	brregdb "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/db"
)

type SourceCapitalFXActions struct {
	gateway *brregdb.Gateway
}

func NewSourceCapitalFXActions(gateway *brregdb.Gateway) *SourceCapitalFXActions {
	return &SourceCapitalFXActions{gateway: gateway}
}

type ConvertBrregSourceCapitalToUSDActivityInput struct {
	TemporalWorkflowID string            `json:"temporal_workflow_id,omitempty"`
	IDs                []string          `json:"ids,omitempty"`
	Filters            map[string]string `json:"filters,omitempty"`
	Limit              int32             `json:"limit"`
	RateDate           string            `json:"rate_date,omitempty"`
	ForceReprocess     bool              `json:"force_reprocess,omitempty"`
	Trigger            string            `json:"trigger,omitempty"`
}

type ConvertBrregSourceCapitalToUSDActivityResult struct {
	CapitalSeen                    int32  `json:"capital_seen"`
	CapitalConverted               int32  `json:"capital_converted"`
	CapitalSkippedMissingRate      int32  `json:"capital_skipped_missing_rate"`
	CapitalSkippedAlreadyConverted int32  `json:"capital_skipped_already_converted"`
	RateDate                       string `json:"rate_date"`
}

func (a *SourceCapitalFXActions) ConvertBrregSourceCapitalToUSD(
	ctx context.Context,
	input ConvertBrregSourceCapitalToUSDActivityInput,
) (ConvertBrregSourceCapitalToUSDActivityResult, error) {
	if a == nil || a.gateway == nil {
		return ConvertBrregSourceCapitalToUSDActivityResult{}, errors.New("brreg source capital fx gateway not available")
	}
	slog.DebugContext(ctx, "converting brreg source capital to usd",
		"temporal_workflow_id", input.TemporalWorkflowID,
		"ids_count", len(input.IDs),
		"filters_count", len(input.Filters),
		"limit", input.Limit,
		"rate_date", input.RateDate,
		"force_reprocess", input.ForceReprocess,
		"trigger", input.Trigger,
	)
	result, err := a.gateway.ConvertSourceCapitalToUSD(ctx, brregdb.ConvertSourceCapitalToUSDCommand{
		IDs:            input.IDs,
		Filters:        input.Filters,
		Limit:          input.Limit,
		RateDate:       input.RateDate,
		ForceReprocess: input.ForceReprocess,
		Trigger:        input.Trigger,
	})
	if err != nil {
		return ConvertBrregSourceCapitalToUSDActivityResult{}, errors.Wrap(err, "convert brreg source capital to usd")
	}
	slog.DebugContext(ctx, "converted brreg source capital to usd",
		"capital_seen", result.CapitalSeen,
		"capital_converted", result.CapitalConverted,
		"capital_skipped_missing_rate", result.CapitalSkippedMissingRate,
		"capital_skipped_already_converted", result.CapitalSkippedAlreadyConverted,
		"rate_date", result.RateDate,
	)
	return ConvertBrregSourceCapitalToUSDActivityResult{
		CapitalSeen:                    result.CapitalSeen,
		CapitalConverted:               result.CapitalConverted,
		CapitalSkippedMissingRate:      result.CapitalSkippedMissingRate,
		CapitalSkippedAlreadyConverted: result.CapitalSkippedAlreadyConverted,
		RateDate:                       result.RateDate,
	}, nil
}
