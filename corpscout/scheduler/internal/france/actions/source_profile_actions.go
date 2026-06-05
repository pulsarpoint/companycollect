package actions

import (
	"context"
	"log/slog"

	"github.com/cockroachdb/errors"

	francedb "github.com/pulsarpoint/corpscout/scheduler/internal/france/db"
)

type SourceProfileActions struct {
	gateway *francedb.Gateway
}

func NewSourceProfileActions(gateway *francedb.Gateway) *SourceProfileActions {
	return &SourceProfileActions{gateway: gateway}
}

type NormalizeFranceSourceProfilesActivityInput struct {
	TemporalWorkflowID string            `json:"temporal_workflow_id,omitempty"`
	IDs                []string          `json:"ids,omitempty"`
	Filters            map[string]string `json:"filters,omitempty"`
	Limit              int32             `json:"limit"`
	Trigger            string            `json:"trigger,omitempty"`
}

type NormalizeFranceSourceProfilesActivityResult struct {
	RecordsSeen            int32 `json:"records_seen"`
	CompaniesUpserted      int32 `json:"companies_upserted"`
	EstablishmentsUpserted int32 `json:"establishments_upserted"`
	AddressesUpserted      int32 `json:"addresses_upserted"`
	IndustriesInserted     int32 `json:"industries_inserted"`
	WebsitesUpserted       int32 `json:"websites_upserted"`
	DomainsUpserted        int32 `json:"domains_upserted"`
	ContactsUpserted       int32 `json:"contacts_upserted"`
}

func (a *SourceProfileActions) NormalizeFranceSourceProfiles(
	ctx context.Context,
	input NormalizeFranceSourceProfilesActivityInput,
) (NormalizeFranceSourceProfilesActivityResult, error) {
	if a == nil || a.gateway == nil {
		return NormalizeFranceSourceProfilesActivityResult{}, errors.New("france source profile gateway not available")
	}
	slog.DebugContext(ctx, "normalizing france source profiles",
		"temporal_workflow_id", input.TemporalWorkflowID,
		"ids_count", len(input.IDs),
		"filters_count", len(input.Filters),
		"limit", input.Limit,
		"trigger", input.Trigger,
	)
	result, err := a.gateway.NormalizeSourceProfiles(ctx, francedb.NormalizeSourceProfilesCommand{
		IDs:     input.IDs,
		Filters: input.Filters,
		Limit:   input.Limit,
		Trigger: input.Trigger,
	})
	if err != nil {
		return NormalizeFranceSourceProfilesActivityResult{}, errors.Wrap(err, "normalize france source profiles")
	}
	slog.DebugContext(ctx, "normalized france source profiles",
		"records_seen", result.RecordsSeen,
		"companies_upserted", result.CompaniesUpserted,
		"establishments_upserted", result.EstablishmentsUpserted,
		"addresses_upserted", result.AddressesUpserted,
		"industries_inserted", result.IndustriesInserted,
		"websites_upserted", result.WebsitesUpserted,
		"domains_upserted", result.DomainsUpserted,
		"contacts_upserted", result.ContactsUpserted,
	)
	return NormalizeFranceSourceProfilesActivityResult{
		RecordsSeen:            result.RecordsSeen,
		CompaniesUpserted:      result.CompaniesUpserted,
		EstablishmentsUpserted: result.EstablishmentsUpserted,
		AddressesUpserted:      result.AddressesUpserted,
		IndustriesInserted:     result.IndustriesInserted,
		WebsitesUpserted:       result.WebsitesUpserted,
		DomainsUpserted:        result.DomainsUpserted,
		ContactsUpserted:       result.ContactsUpserted,
	}, nil
}
