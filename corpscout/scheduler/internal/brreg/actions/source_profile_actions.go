package actions

import (
	"context"
	"log/slog"

	"github.com/cockroachdb/errors"

	brregdb "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/db"
)

type SourceProfileActions struct {
	gateway *brregdb.Gateway
}

func NewSourceProfileActions(gateway *brregdb.Gateway) *SourceProfileActions {
	return &SourceProfileActions{gateway: gateway}
}

type NormalizeBrregSourceProfilesActivityInput struct {
	TemporalWorkflowID string            `json:"temporal_workflow_id,omitempty"`
	IDs                []string          `json:"ids,omitempty"`
	Filters            map[string]string `json:"filters,omitempty"`
	Limit              int32             `json:"limit"`
	Trigger            string            `json:"trigger,omitempty"`
}

type NormalizeBrregSourceProfilesActivityResult struct {
	RecordsSeen        int32 `json:"records_seen"`
	CompaniesUpserted  int32 `json:"companies_upserted"`
	AddressesUpserted  int32 `json:"addresses_upserted"`
	IndustriesUpserted int32 `json:"industries_upserted"`
	WebsitesUpserted   int32 `json:"websites_upserted"`
	DomainsUpserted    int32 `json:"domains_upserted"`
	ContactsUpserted   int32 `json:"contacts_upserted"`
	CapitalUpserted    int32 `json:"capital_upserted"`
}

type RefreshBrregSourceExplorerActivityInput struct {
	TemporalWorkflowID string `json:"temporal_workflow_id,omitempty"`
	Trigger            string `json:"trigger,omitempty"`
}

type RefreshBrregSourceExplorerActivityResult struct {
	Refreshed             bool    `json:"refreshed"`
	UsedConcurrentRefresh bool    `json:"used_concurrent_refresh"`
	SourceEntries         int64   `json:"source_entries"`
	LatestSourceUpdatedAt *string `json:"latest_source_updated_at,omitempty"`
}

func (a *SourceProfileActions) NormalizeBrregSourceProfilesWithCopy(
	ctx context.Context,
	input NormalizeBrregSourceProfilesActivityInput,
) (NormalizeBrregSourceProfilesActivityResult, error) {
	if a == nil || a.gateway == nil {
		return NormalizeBrregSourceProfilesActivityResult{}, errors.New("brreg source profile gateway not available")
	}
	slog.DebugContext(ctx, "normalizing brreg source profiles",
		"mode", "copy",
		"temporal_workflow_id", input.TemporalWorkflowID,
		"ids_count", len(input.IDs),
		"filters_count", len(input.Filters),
		"limit", input.Limit,
		"trigger", input.Trigger,
	)
	result, err := a.gateway.NormalizeSourceProfilesWithCopy(ctx, brregdb.NormalizeSourceProfilesCommand{
		IDs:     input.IDs,
		Filters: input.Filters,
		Limit:   input.Limit,
		Trigger: input.Trigger,
	})
	if err != nil {
		return NormalizeBrregSourceProfilesActivityResult{}, errors.Wrap(err, "normalize brreg source profiles")
	}
	slog.DebugContext(ctx, "normalized brreg source profiles",
		"mode", "copy",
		"records_seen", result.RecordsSeen,
		"companies_upserted", result.CompaniesUpserted,
		"addresses_upserted", result.AddressesUpserted,
		"industries_upserted", result.IndustriesUpserted,
		"websites_upserted", result.WebsitesUpserted,
		"domains_upserted", result.DomainsUpserted,
		"contacts_upserted", result.ContactsUpserted,
		"capital_upserted", result.CapitalUpserted,
	)
	return NormalizeBrregSourceProfilesActivityResult{
		RecordsSeen:        result.RecordsSeen,
		CompaniesUpserted:  result.CompaniesUpserted,
		AddressesUpserted:  result.AddressesUpserted,
		IndustriesUpserted: result.IndustriesUpserted,
		WebsitesUpserted:   result.WebsitesUpserted,
		DomainsUpserted:    result.DomainsUpserted,
		ContactsUpserted:   result.ContactsUpserted,
		CapitalUpserted:    result.CapitalUpserted,
	}, nil
}

func (a *SourceProfileActions) RefreshBrregSourceExplorer(
	ctx context.Context,
	input RefreshBrregSourceExplorerActivityInput,
) (RefreshBrregSourceExplorerActivityResult, error) {
	if a == nil || a.gateway == nil {
		return RefreshBrregSourceExplorerActivityResult{}, errors.New("brreg source profile gateway not available")
	}
	slog.DebugContext(ctx, "refreshing brreg source explorer materialized view",
		"temporal_workflow_id", input.TemporalWorkflowID,
		"trigger", input.Trigger,
	)
	result, err := a.gateway.RefreshSourceExplorer(ctx)
	if err != nil {
		return RefreshBrregSourceExplorerActivityResult{}, errors.Wrap(err, "refresh brreg source explorer")
	}
	slog.DebugContext(ctx, "refreshed brreg source explorer materialized view",
		"source_entries", result.SourceEntries,
		"used_concurrent_refresh", result.UsedConcurrentRefresh,
	)
	return RefreshBrregSourceExplorerActivityResult{
		Refreshed:             result.Refreshed,
		UsedConcurrentRefresh: result.UsedConcurrentRefresh,
		SourceEntries:         result.SourceEntries,
		LatestSourceUpdatedAt: result.LatestSourceUpdatedAt,
	}, nil
}
