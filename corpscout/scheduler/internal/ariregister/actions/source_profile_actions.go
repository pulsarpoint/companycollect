package actions

import (
	"context"
	"log/slog"

	"github.com/cockroachdb/errors"

	ariregisterdb "github.com/pulsarpoint/corpscout/scheduler/internal/ariregister/db"
)

type SourceProfileActions struct {
	gateway *ariregisterdb.Gateway
}

func NewSourceProfileActions(gateway *ariregisterdb.Gateway) *SourceProfileActions {
	return &SourceProfileActions{gateway: gateway}
}

type NormalizeAriregisterSourceProfilesActivityInput struct {
	TemporalWorkflowID string            `json:"temporal_workflow_id,omitempty"`
	IDs                []string          `json:"ids,omitempty"`
	Filters            map[string]string `json:"filters,omitempty"`
	Limit              int32             `json:"limit"`
	Trigger            string            `json:"trigger,omitempty"`
}

type NormalizeAriregisterSourceProfilesActivityResult struct {
	RecordsSeen                  int32 `json:"records_seen"`
	CompaniesUpserted            int32 `json:"companies_upserted"`
	CompanyNamesUpserted         int32 `json:"company_names_upserted"`
	CompanyStatusesUpserted      int32 `json:"company_statuses_upserted"`
	LegalFormsUpserted           int32 `json:"legal_forms_upserted"`
	AddressesUpserted            int32 `json:"addresses_upserted"`
	ContactsUpserted             int32 `json:"contacts_upserted"`
	WebsitesUpserted             int32 `json:"websites_upserted"`
	DomainsUpserted              int32 `json:"domains_upserted"`
	IndustriesUpserted           int32 `json:"industries_upserted"`
	CapitalUpserted              int32 `json:"capital_upserted"`
	FinancialYearPeriodsUpserted int32 `json:"financial_year_periods_upserted"`
	AnnualReportsUpserted        int32 `json:"annual_reports_upserted"`
	ArticlesUpserted             int32 `json:"articles_upserted"`
	RegistryNotesUpserted        int32 `json:"registry_notes_upserted"`
}

type RefreshAriregisterSourceExplorerActivityInput struct {
	TemporalWorkflowID string `json:"temporal_workflow_id,omitempty"`
	Trigger            string `json:"trigger,omitempty"`
}

type RefreshAriregisterSourceExplorerActivityResult struct {
	Refreshed             bool    `json:"refreshed"`
	UsedConcurrentRefresh bool    `json:"used_concurrent_refresh"`
	SourceEntries         int64   `json:"source_entries"`
	LatestSourceUpdatedAt *string `json:"latest_source_updated_at,omitempty"`
}

func (a *SourceProfileActions) NormalizeAriregisterSourceProfilesWithCopy(
	ctx context.Context,
	input NormalizeAriregisterSourceProfilesActivityInput,
) (NormalizeAriregisterSourceProfilesActivityResult, error) {
	if a == nil || a.gateway == nil {
		return NormalizeAriregisterSourceProfilesActivityResult{}, errors.New("ariregister source profile gateway not available")
	}
	slog.DebugContext(ctx, "normalizing ariregister source profiles",
		"mode", "copy",
		"temporal_workflow_id", input.TemporalWorkflowID,
		"ids_count", len(input.IDs),
		"filters_count", len(input.Filters),
		"limit", input.Limit,
		"trigger", input.Trigger,
	)
	result, err := a.gateway.NormalizeSourceProfilesWithCopy(ctx, ariregisterdb.NormalizeSourceProfilesCommand{
		IDs:     input.IDs,
		Filters: input.Filters,
		Limit:   input.Limit,
		Trigger: input.Trigger,
	})
	if err != nil {
		return NormalizeAriregisterSourceProfilesActivityResult{}, errors.Wrap(err, "normalize ariregister source profiles")
	}
	slog.DebugContext(ctx, "normalized ariregister source profiles",
		"mode", "copy",
		"records_seen", result.RecordsSeen,
		"companies_upserted", result.CompaniesUpserted,
		"company_names_upserted", result.CompanyNamesUpserted,
		"company_statuses_upserted", result.CompanyStatusesUpserted,
		"legal_forms_upserted", result.LegalFormsUpserted,
		"addresses_upserted", result.AddressesUpserted,
		"contacts_upserted", result.ContactsUpserted,
		"websites_upserted", result.WebsitesUpserted,
		"domains_upserted", result.DomainsUpserted,
		"industries_upserted", result.IndustriesUpserted,
		"capital_upserted", result.CapitalUpserted,
		"financial_year_periods_upserted", result.FinancialYearPeriodsUpserted,
		"annual_reports_upserted", result.AnnualReportsUpserted,
		"articles_upserted", result.ArticlesUpserted,
		"registry_notes_upserted", result.RegistryNotesUpserted,
	)
	return NormalizeAriregisterSourceProfilesActivityResult{
		RecordsSeen:                  result.RecordsSeen,
		CompaniesUpserted:            result.CompaniesUpserted,
		CompanyNamesUpserted:         result.CompanyNamesUpserted,
		CompanyStatusesUpserted:      result.CompanyStatusesUpserted,
		LegalFormsUpserted:           result.LegalFormsUpserted,
		AddressesUpserted:            result.AddressesUpserted,
		ContactsUpserted:             result.ContactsUpserted,
		WebsitesUpserted:             result.WebsitesUpserted,
		DomainsUpserted:              result.DomainsUpserted,
		IndustriesUpserted:           result.IndustriesUpserted,
		CapitalUpserted:              result.CapitalUpserted,
		FinancialYearPeriodsUpserted: result.FinancialYearPeriodsUpserted,
		AnnualReportsUpserted:        result.AnnualReportsUpserted,
		ArticlesUpserted:             result.ArticlesUpserted,
		RegistryNotesUpserted:        result.RegistryNotesUpserted,
	}, nil
}

func (a *SourceProfileActions) RefreshAriregisterSourceExplorer(
	ctx context.Context,
	input RefreshAriregisterSourceExplorerActivityInput,
) (RefreshAriregisterSourceExplorerActivityResult, error) {
	if a == nil || a.gateway == nil {
		return RefreshAriregisterSourceExplorerActivityResult{}, errors.New("ariregister source profile gateway not available")
	}
	slog.DebugContext(ctx, "refreshing ariregister source explorer materialized view",
		"temporal_workflow_id", input.TemporalWorkflowID,
		"trigger", input.Trigger,
	)
	result, err := a.gateway.RefreshSourceExplorer(ctx)
	if err != nil {
		return RefreshAriregisterSourceExplorerActivityResult{}, errors.Wrap(err, "refresh ariregister source explorer")
	}
	slog.DebugContext(ctx, "refreshed ariregister source explorer materialized view",
		"source_entries", result.SourceEntries,
		"used_concurrent_refresh", result.UsedConcurrentRefresh,
	)
	return RefreshAriregisterSourceExplorerActivityResult{
		Refreshed:             result.Refreshed,
		UsedConcurrentRefresh: result.UsedConcurrentRefresh,
		SourceEntries:         result.SourceEntries,
		LatestSourceUpdatedAt: result.LatestSourceUpdatedAt,
	}, nil
}
