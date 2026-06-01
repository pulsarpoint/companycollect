package service

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

func applySuggestionCompanyFinancial(ctx context.Context, qtx *db.Queries, suggestionID, companyID, childID uuid.UUID, reviewedBy, reviewNote string) error {
	child, err := qtx.GetSuggestionCompanyFinancialByID(ctx, childID)
	if err != nil {
		return errors.Wrap(err, "get financial suggestion")
	}
	if err := validateSuggestionChild(suggestionID, child.SuggestionID, child.Status, child.ID); err != nil {
		return err
	}
	financial, err := qtx.CreateCompanyFinancial(ctx, db.CreateCompanyFinancialParams{
		CompanyID:       companyID,
		Year:            child.Year,
		SourceName:      child.SourceName,
		EmployeeCount:   child.EmployeeCount,
		RevenueAmount:   child.RevenueAmount,
		RevenueCurrency: child.RevenueCurrency,
		RevenueUsd:      child.RevenueUsd,
		ProfitAmount:    child.ProfitAmount,
		ProfitUsd:       child.ProfitUsd,
		Evidence:        nonEmptyJSON(child.Evidence),
	})
	if err != nil {
		return errors.Wrap(err, "upsert company financial")
	}
	if financial.Status == "suggested" {
		if err := qtx.ApproveCompanyFinancial(ctx, db.ApproveCompanyFinancialParams{
			ID:         financial.ID,
			ReviewedBy: &reviewedBy,
		}); err != nil {
			return errors.Wrap(err, "approve company financial")
		}
	}
	return qtx.MarkSuggestionCompanyFinancialApplied(ctx, db.MarkSuggestionCompanyFinancialAppliedParams{
		ID: child.ID, ReviewedBy: &reviewedBy, ReviewNote: &reviewNote,
	})
}

func applySuggestionCompanyIndustry(ctx context.Context, qtx *db.Queries, suggestionID, companyID, childID uuid.UUID, reviewedBy, reviewNote string) error {
	child, err := qtx.GetSuggestionCompanyIndustryByID(ctx, childID)
	if err != nil {
		return errors.Wrap(err, "get industry suggestion")
	}
	if err := validateSuggestionChild(suggestionID, child.SuggestionID, child.Status, child.ID); err != nil {
		return err
	}
	if _, err := qtx.UpsertCompanyIndustry(ctx, db.UpsertCompanyIndustryParams{
		CompanyID:  companyID,
		Industry:   child.Industry,
		Source:     child.Source,
		Confidence: child.Confidence,
		Evidence:   nonEmptyJSON(child.Evidence),
	}); err != nil {
		return errors.Wrap(err, "upsert company industry")
	}
	return qtx.MarkSuggestionCompanyIndustryApplied(ctx, db.MarkSuggestionCompanyIndustryAppliedParams{
		ID: child.ID, ReviewedBy: &reviewedBy, ReviewNote: &reviewNote,
	})
}

func applySuggestionCompanyMarket(ctx context.Context, qtx *db.Queries, suggestionID, companyID, childID uuid.UUID, reviewedBy, reviewNote string) error {
	child, err := qtx.GetSuggestionCompanyMarketByID(ctx, childID)
	if err != nil {
		return errors.Wrap(err, "get market suggestion")
	}
	if err := validateSuggestionChild(suggestionID, child.SuggestionID, child.Status, child.ID); err != nil {
		return err
	}
	if _, err := qtx.UpsertCompanyMarket(ctx, db.UpsertCompanyMarketParams{
		CompanyID:  companyID,
		Market:     child.Market,
		Source:     child.Source,
		Confidence: child.Confidence,
		Evidence:   nonEmptyJSON(child.Evidence),
	}); err != nil {
		return errors.Wrap(err, "upsert company market")
	}
	return qtx.MarkSuggestionCompanyMarketApplied(ctx, db.MarkSuggestionCompanyMarketAppliedParams{
		ID: child.ID, ReviewedBy: &reviewedBy, ReviewNote: &reviewNote,
	})
}

func applySuggestionCompanyService(ctx context.Context, qtx *db.Queries, suggestionID, companyID, childID uuid.UUID, reviewedBy, reviewNote string) error {
	child, err := qtx.GetSuggestionCompanyServiceByID(ctx, childID)
	if err != nil {
		return errors.Wrap(err, "get service suggestion")
	}
	if err := validateSuggestionChild(suggestionID, child.SuggestionID, child.Status, child.ID); err != nil {
		return err
	}
	if _, err := qtx.UpsertCompanyService(ctx, db.UpsertCompanyServiceParams{
		CompanyID:   companyID,
		Service:     child.Service,
		Description: child.Description,
		Source:      child.Source,
		Confidence:  child.Confidence,
		Evidence:    nonEmptyJSON(child.Evidence),
	}); err != nil {
		return errors.Wrap(err, "upsert company service")
	}
	return qtx.MarkSuggestionCompanyServiceApplied(ctx, db.MarkSuggestionCompanyServiceAppliedParams{
		ID: child.ID, ReviewedBy: &reviewedBy, ReviewNote: &reviewNote,
	})
}

func applySuggestionCompanyRelationship(ctx context.Context, qtx *db.Queries, suggestionID, companyID, childID uuid.UUID, reviewedBy, reviewNote string) error {
	child, err := qtx.GetSuggestionCompanyRelationshipByID(ctx, childID)
	if err != nil {
		return errors.Wrap(err, "get relationship suggestion")
	}
	if err := validateSuggestionChild(suggestionID, child.SuggestionID, child.Status, child.ID); err != nil {
		return err
	}
	if !child.RelatedCompanyID.Valid {
		return errors.Newf("relationship suggestion %s has no related_company_id", child.ID)
	}
	if _, err := qtx.UpsertCompanyRelationship(ctx, db.UpsertCompanyRelationshipParams{
		SubjectCompanyID:    companyID,
		RelatedCompanyID:    uuid.UUID(child.RelatedCompanyID.Bytes),
		RelationshipType:    child.RelationshipType,
		Source:              child.Source,
		Confidence:          child.Confidence,
		Evidence:            nonEmptyJSON(child.Evidence),
		OwnershipPercentage: child.OwnershipPercentage,
		ValidFrom:           child.ValidFrom,
		ValidTo:             child.ValidTo,
	}); err != nil {
		return errors.Wrap(err, "upsert company relationship")
	}
	return qtx.MarkSuggestionCompanyRelationshipApplied(ctx, db.MarkSuggestionCompanyRelationshipAppliedParams{
		ID: child.ID, ReviewedBy: &reviewedBy, ReviewNote: &reviewNote,
	})
}
