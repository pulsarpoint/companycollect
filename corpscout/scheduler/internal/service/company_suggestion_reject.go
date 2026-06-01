package service

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

func rejectSuggestionCompanySection(ctx context.Context, qtx *db.Queries, suggestionID uuid.UUID, item CompanySuggestionReviewItem, reviewedBy, reviewNote string) error {
	switch item.Table {
	case SuggestionCompanyProfilesTable:
		child, err := qtx.GetSuggestionCompanyProfileByID(ctx, item.ID)
		if err != nil {
			return errors.Wrap(err, "get profile suggestion")
		}
		if err := validateSuggestionChild(suggestionID, child.SuggestionID, child.Status, child.ID); err != nil {
			return err
		}
		return qtx.MarkSuggestionCompanyProfileRejected(ctx, db.MarkSuggestionCompanyProfileRejectedParams{ID: item.ID, ReviewedBy: &reviewedBy, ReviewNote: &reviewNote})
	case SuggestionCompanyDomainsTable:
		child, err := qtx.GetSuggestionCompanyDomainByID(ctx, item.ID)
		if err != nil {
			return errors.Wrap(err, "get domain suggestion")
		}
		if err := validateSuggestionChild(suggestionID, child.SuggestionID, child.Status, child.ID); err != nil {
			return err
		}
		return qtx.MarkSuggestionCompanyDomainRejected(ctx, db.MarkSuggestionCompanyDomainRejectedParams{ID: item.ID, ReviewedBy: &reviewedBy, ReviewNote: &reviewNote})
	case SuggestionCompanyLocationsTable:
		child, err := qtx.GetSuggestionCompanyLocationByID(ctx, item.ID)
		if err != nil {
			return errors.Wrap(err, "get location suggestion")
		}
		if err := validateSuggestionChild(suggestionID, child.SuggestionID, child.Status, child.ID); err != nil {
			return err
		}
		return qtx.MarkSuggestionCompanyLocationRejected(ctx, db.MarkSuggestionCompanyLocationRejectedParams{ID: item.ID, ReviewedBy: &reviewedBy, ReviewNote: &reviewNote})
	case SuggestionCompanyEmailsTable:
		child, err := qtx.GetSuggestionCompanyEmailByID(ctx, item.ID)
		if err != nil {
			return errors.Wrap(err, "get email suggestion")
		}
		if err := validateSuggestionChild(suggestionID, child.SuggestionID, child.Status, child.ID); err != nil {
			return err
		}
		return qtx.MarkSuggestionCompanyEmailRejected(ctx, db.MarkSuggestionCompanyEmailRejectedParams{ID: item.ID, ReviewedBy: &reviewedBy, ReviewNote: &reviewNote})
	case SuggestionCompanyPhonesTable:
		child, err := qtx.GetSuggestionCompanyPhoneByID(ctx, item.ID)
		if err != nil {
			return errors.Wrap(err, "get phone suggestion")
		}
		if err := validateSuggestionChild(suggestionID, child.SuggestionID, child.Status, child.ID); err != nil {
			return err
		}
		return qtx.MarkSuggestionCompanyPhoneRejected(ctx, db.MarkSuggestionCompanyPhoneRejectedParams{ID: item.ID, ReviewedBy: &reviewedBy, ReviewNote: &reviewNote})
	case SuggestionCompanyFinancialsTable:
		child, err := qtx.GetSuggestionCompanyFinancialByID(ctx, item.ID)
		if err != nil {
			return errors.Wrap(err, "get financial suggestion")
		}
		if err := validateSuggestionChild(suggestionID, child.SuggestionID, child.Status, child.ID); err != nil {
			return err
		}
		return qtx.MarkSuggestionCompanyFinancialRejected(ctx, db.MarkSuggestionCompanyFinancialRejectedParams{ID: item.ID, ReviewedBy: &reviewedBy, ReviewNote: &reviewNote})
	case SuggestionCompanyIndustriesTable:
		child, err := qtx.GetSuggestionCompanyIndustryByID(ctx, item.ID)
		if err != nil {
			return errors.Wrap(err, "get industry suggestion")
		}
		if err := validateSuggestionChild(suggestionID, child.SuggestionID, child.Status, child.ID); err != nil {
			return err
		}
		return qtx.MarkSuggestionCompanyIndustryRejected(ctx, db.MarkSuggestionCompanyIndustryRejectedParams{ID: item.ID, ReviewedBy: &reviewedBy, ReviewNote: &reviewNote})
	case SuggestionCompanyMarketsTable:
		child, err := qtx.GetSuggestionCompanyMarketByID(ctx, item.ID)
		if err != nil {
			return errors.Wrap(err, "get market suggestion")
		}
		if err := validateSuggestionChild(suggestionID, child.SuggestionID, child.Status, child.ID); err != nil {
			return err
		}
		return qtx.MarkSuggestionCompanyMarketRejected(ctx, db.MarkSuggestionCompanyMarketRejectedParams{ID: item.ID, ReviewedBy: &reviewedBy, ReviewNote: &reviewNote})
	case SuggestionCompanyServicesTable:
		child, err := qtx.GetSuggestionCompanyServiceByID(ctx, item.ID)
		if err != nil {
			return errors.Wrap(err, "get service suggestion")
		}
		if err := validateSuggestionChild(suggestionID, child.SuggestionID, child.Status, child.ID); err != nil {
			return err
		}
		return qtx.MarkSuggestionCompanyServiceRejected(ctx, db.MarkSuggestionCompanyServiceRejectedParams{ID: item.ID, ReviewedBy: &reviewedBy, ReviewNote: &reviewNote})
	case SuggestionCompanyRelationshipsTable:
		child, err := qtx.GetSuggestionCompanyRelationshipByID(ctx, item.ID)
		if err != nil {
			return errors.Wrap(err, "get relationship suggestion")
		}
		if err := validateSuggestionChild(suggestionID, child.SuggestionID, child.Status, child.ID); err != nil {
			return err
		}
		return qtx.MarkSuggestionCompanyRelationshipRejected(ctx, db.MarkSuggestionCompanyRelationshipRejectedParams{ID: item.ID, ReviewedBy: &reviewedBy, ReviewNote: &reviewNote})
	default:
		return errors.Newf("unknown company suggestion section table: %s", item.Table)
	}
}
