package brregdb

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

const defaultNACERevision = "2.1"

func (g *Gateway) NormalizeSourceProfiles(
	ctx context.Context,
	command NormalizeSourceProfilesCommand,
) (NormalizeSourceProfilesResult, error) {
	if g.pool == nil {
		return NormalizeSourceProfilesResult{}, errors.New("brreg workflow database pool not available")
	}
	limit := command.Limit
	if limit < 0 {
		return NormalizeSourceProfilesResult{}, errors.New("brreg source profile limit cannot be negative")
	}
	trigger := strings.TrimSpace(command.Trigger)
	if trigger == "" {
		trigger = "manual"
	}
	naceRevision := defaultNACERevision
	row, err := db.New(g.pool).NormalizeBrregSourceProfiles(ctx, db.NormalizeBrregSourceProfilesParams{
		SelectedIds:       command.IDs,
		Query:             textFilter(command.Filters, "query"),
		LifecycleState:    textFilter(command.Filters, "state", "lifecycle_state"),
		TranslationStatus: textFilter(command.Filters, "translation_status"),
		DomainStatus:      textFilter(command.Filters, "domain_status"),
		FinancialStatus:   textFilter(command.Filters, "financial_status"),
		EnhancedStatus:    textFilter(command.Filters, "enhanced_status"),
		Limit:             limit,
		Trigger:           &trigger,
		NaceRevision:      &naceRevision,
	})
	if err != nil {
		return NormalizeSourceProfilesResult{}, errors.Wrap(err, "normalize brreg source profiles")
	}
	return NormalizeSourceProfilesResult{
		RecordsSeen:        row.RecordsSeen,
		CompaniesUpserted:  row.CompaniesUpserted,
		AddressesUpserted:  row.AddressesUpserted,
		IndustriesUpserted: row.IndustriesUpserted,
		WebsitesUpserted:   row.WebsitesUpserted,
		DomainsUpserted:    row.DomainsUpserted,
		ContactsUpserted:   row.ContactsUpserted,
		CapitalUpserted:    row.CapitalUpserted,
	}, nil
}

func textFilter(filters map[string]string, keys ...string) *string {
	if filters == nil {
		return nil
	}
	for _, key := range keys {
		value := strings.TrimSpace(filters[key])
		if value != "" {
			return &value
		}
	}
	return nil
}
