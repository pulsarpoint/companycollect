package service

import (
	"context"
	"sort"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

const (
	SuggestionCompanyProfilesTable      = "suggestion_company_profiles"
	SuggestionCompanyDomainsTable       = "suggestion_company_domains"
	SuggestionCompanyLocationsTable     = "suggestion_company_locations"
	SuggestionCompanyEmailsTable        = "suggestion_company_emails"
	SuggestionCompanyPhonesTable        = "suggestion_company_phones"
	SuggestionCompanyFinancialsTable    = "suggestion_company_financials"
	SuggestionCompanyIndustriesTable    = "suggestion_company_industries"
	SuggestionCompanyMarketsTable       = "suggestion_company_markets"
	SuggestionCompanyServicesTable      = "suggestion_company_services"
	SuggestionCompanyRelationshipsTable = "suggestion_company_relationships"
)

// CompanySuggestionReviewItem identifies one reviewable child row under a company suggestion.
type CompanySuggestionReviewItem struct {
	Table string    `json:"table"`
	ID    uuid.UUID `json:"id"`
}

// ApplyCompanySuggestionSections applies selected child rows from the new company
// suggestion review model. Profile rows are applied first so new-company suggestions
// can create the target company before dependent sections are written.
func ApplyCompanySuggestionSections(ctx context.Context, pool TxPool, suggestionID uuid.UUID, items []CompanySuggestionReviewItem, reviewedBy, reviewNote string) error {
	if len(items) == 0 {
		return errors.New("no suggestion sections selected")
	}

	orderedItems := append([]CompanySuggestionReviewItem(nil), items...)
	sort.SliceStable(orderedItems, func(i, j int) bool {
		return orderedItems[i].Table == SuggestionCompanyProfilesTable && orderedItems[j].Table != SuggestionCompanyProfilesTable
	})

	tx, err := pool.Begin(ctx)
	if err != nil {
		return errors.Wrap(err, "begin company suggestion review transaction")
	}
	defer func() { _ = tx.Rollback(ctx) }()

	qtx := db.New(tx)
	suggestion, err := qtx.GetSuggestionByID(ctx, suggestionID)
	if err != nil {
		return errors.Wrap(err, "get company suggestion")
	}
	if suggestion.Status == "applied" || suggestion.Status == "rejected" || suggestion.Status == "superseded" {
		return errors.Newf("suggestion %s is not reviewable (status=%s)", suggestionID, suggestion.Status)
	}

	companyID, hasCompany := suggestionCompanyID(suggestion)
	for _, item := range orderedItems {
		switch item.Table {
		case SuggestionCompanyProfilesTable:
			appliedCompanyID, err := applySuggestionCompanyProfile(ctx, qtx, suggestion, item.ID, reviewedBy, reviewNote)
			if err != nil {
				return errors.Wrapf(err, "apply %s %s", item.Table, item.ID)
			}
			companyID = appliedCompanyID
			hasCompany = true
			suggestion.CreatedCompanyID = pgtype.UUID{Bytes: appliedCompanyID, Valid: true}
		case SuggestionCompanyDomainsTable:
			if !hasCompany {
				return missingSuggestionCompanyError(suggestionID, item)
			}
			if err := applySuggestionCompanyDomain(ctx, qtx, suggestionID, companyID, item.ID, reviewedBy, reviewNote); err != nil {
				return errors.Wrapf(err, "apply %s %s", item.Table, item.ID)
			}
		case SuggestionCompanyLocationsTable:
			if !hasCompany {
				return missingSuggestionCompanyError(suggestionID, item)
			}
			if err := applySuggestionCompanyLocation(ctx, qtx, suggestionID, companyID, item.ID, reviewedBy, reviewNote); err != nil {
				return errors.Wrapf(err, "apply %s %s", item.Table, item.ID)
			}
		case SuggestionCompanyEmailsTable:
			if !hasCompany {
				return missingSuggestionCompanyError(suggestionID, item)
			}
			if err := applySuggestionCompanyEmail(ctx, qtx, suggestionID, companyID, item.ID, reviewedBy, reviewNote); err != nil {
				return errors.Wrapf(err, "apply %s %s", item.Table, item.ID)
			}
		case SuggestionCompanyPhonesTable:
			if !hasCompany {
				return missingSuggestionCompanyError(suggestionID, item)
			}
			if err := applySuggestionCompanyPhone(ctx, qtx, suggestionID, companyID, item.ID, reviewedBy, reviewNote); err != nil {
				return errors.Wrapf(err, "apply %s %s", item.Table, item.ID)
			}
		case SuggestionCompanyFinancialsTable:
			if !hasCompany {
				return missingSuggestionCompanyError(suggestionID, item)
			}
			if err := applySuggestionCompanyFinancial(ctx, qtx, suggestionID, companyID, item.ID, reviewedBy, reviewNote); err != nil {
				return errors.Wrapf(err, "apply %s %s", item.Table, item.ID)
			}
		case SuggestionCompanyIndustriesTable:
			if !hasCompany {
				return missingSuggestionCompanyError(suggestionID, item)
			}
			if err := applySuggestionCompanyIndustry(ctx, qtx, suggestionID, companyID, item.ID, reviewedBy, reviewNote); err != nil {
				return errors.Wrapf(err, "apply %s %s", item.Table, item.ID)
			}
		case SuggestionCompanyMarketsTable:
			if !hasCompany {
				return missingSuggestionCompanyError(suggestionID, item)
			}
			if err := applySuggestionCompanyMarket(ctx, qtx, suggestionID, companyID, item.ID, reviewedBy, reviewNote); err != nil {
				return errors.Wrapf(err, "apply %s %s", item.Table, item.ID)
			}
		case SuggestionCompanyServicesTable:
			if !hasCompany {
				return missingSuggestionCompanyError(suggestionID, item)
			}
			if err := applySuggestionCompanyService(ctx, qtx, suggestionID, companyID, item.ID, reviewedBy, reviewNote); err != nil {
				return errors.Wrapf(err, "apply %s %s", item.Table, item.ID)
			}
		case SuggestionCompanyRelationshipsTable:
			if !hasCompany {
				return missingSuggestionCompanyError(suggestionID, item)
			}
			if err := applySuggestionCompanyRelationship(ctx, qtx, suggestionID, companyID, item.ID, reviewedBy, reviewNote); err != nil {
				return errors.Wrapf(err, "apply %s %s", item.Table, item.ID)
			}
		default:
			return errors.Newf("unknown company suggestion section table: %s", item.Table)
		}
	}

	if err := updateSuggestionAggregateStatus(ctx, qtx, suggestionID, reviewedBy, reviewNote); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return errors.Wrap(err, "commit company suggestion review transaction")
	}
	return nil
}

// RejectCompanySuggestionSections marks selected child rows rejected and then
// recomputes the parent aggregate review status.
func RejectCompanySuggestionSections(ctx context.Context, pool TxPool, suggestionID uuid.UUID, items []CompanySuggestionReviewItem, reviewedBy, reviewNote string) error {
	if len(items) == 0 {
		return errors.New("no suggestion sections selected")
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return errors.Wrap(err, "begin company suggestion reject transaction")
	}
	defer func() { _ = tx.Rollback(ctx) }()

	qtx := db.New(tx)
	suggestion, err := qtx.GetSuggestionByID(ctx, suggestionID)
	if err != nil {
		return errors.Wrap(err, "get company suggestion")
	}
	if suggestion.Status == "applied" || suggestion.Status == "rejected" || suggestion.Status == "superseded" {
		return errors.Newf("suggestion %s is not reviewable (status=%s)", suggestionID, suggestion.Status)
	}

	for _, item := range items {
		if err := rejectSuggestionCompanySection(ctx, qtx, suggestionID, item, reviewedBy, reviewNote); err != nil {
			return errors.Wrapf(err, "reject %s %s", item.Table, item.ID)
		}
	}
	if err := updateSuggestionAggregateStatus(ctx, qtx, suggestionID, reviewedBy, reviewNote); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return errors.Wrap(err, "commit company suggestion reject transaction")
	}
	return nil
}
