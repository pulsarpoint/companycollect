package service

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

func applySuggestionCompanyDomain(ctx context.Context, qtx *db.Queries, suggestionID, companyID, childID uuid.UUID, reviewedBy, reviewNote string) error {
	child, err := qtx.GetSuggestionCompanyDomainByID(ctx, childID)
	if err != nil {
		return errors.Wrap(err, "get domain suggestion")
	}
	if err := validateSuggestionChild(suggestionID, child.SuggestionID, child.Status, child.ID); err != nil {
		return err
	}
	domain, err := qtx.UpsertDomainWithSource(ctx, db.UpsertDomainWithSourceParams{
		Domain:       child.Domain,
		ImportSource: child.Signal,
	})
	if err != nil {
		return errors.Wrap(err, "upsert domain")
	}
	if _, err := qtx.UpsertCompanyDomain(ctx, db.UpsertCompanyDomainParams{
		CompanyID:        companyID,
		DomainID:         domain.ID,
		RelationshipType: child.RelationshipType,
		Status:           child.DomainStatus,
		Signal:           child.Signal,
		Confidence:       child.SignalConfidence,
		Evidence:         nonEmptyJSON(child.Evidence),
	}); err != nil {
		return errors.Wrap(err, "upsert company domain")
	}
	return qtx.MarkSuggestionCompanyDomainApplied(ctx, db.MarkSuggestionCompanyDomainAppliedParams{
		ID: child.ID, ReviewedBy: &reviewedBy, ReviewNote: &reviewNote,
	})
}

func applySuggestionCompanyLocation(ctx context.Context, qtx *db.Queries, suggestionID, companyID, childID uuid.UUID, reviewedBy, reviewNote string) error {
	child, err := qtx.GetSuggestionCompanyLocationByID(ctx, childID)
	if err != nil {
		return errors.Wrap(err, "get location suggestion")
	}
	if err := validateSuggestionChild(suggestionID, child.SuggestionID, child.Status, child.ID); err != nil {
		return err
	}
	if _, err := qtx.UpsertCompanyLocation(ctx, db.UpsertCompanyLocationParams{
		CompanyID:    companyID,
		LocationType: child.LocationType,
		Label:        child.Label,
		AddressLine1: child.AddressLine1,
		AddressLine2: child.AddressLine2,
		City:         child.City,
		Region:       child.Region,
		PostalCode:   child.PostalCode,
		Country:      child.Country,
		CountryCode:  child.CountryCode,
		Latitude:     child.Latitude,
		Longitude:    child.Longitude,
		Source:       child.Source,
		Confidence:   child.Confidence,
		Evidence:     nonEmptyJSON(child.Evidence),
	}); err != nil {
		return errors.Wrap(err, "upsert company location")
	}
	return qtx.MarkSuggestionCompanyLocationApplied(ctx, db.MarkSuggestionCompanyLocationAppliedParams{
		ID: child.ID, ReviewedBy: &reviewedBy, ReviewNote: &reviewNote,
	})
}

func applySuggestionCompanyEmail(ctx context.Context, qtx *db.Queries, suggestionID, companyID, childID uuid.UUID, reviewedBy, reviewNote string) error {
	child, err := qtx.GetSuggestionCompanyEmailByID(ctx, childID)
	if err != nil {
		return errors.Wrap(err, "get email suggestion")
	}
	if err := validateSuggestionChild(suggestionID, child.SuggestionID, child.Status, child.ID); err != nil {
		return err
	}
	if _, err := qtx.UpsertCompanyEmail(ctx, db.UpsertCompanyEmailParams{
		CompanyID:   companyID,
		Email:       child.Email,
		Description: child.Description,
		Purpose:     child.Purpose,
		Name:        child.Name,
		Source:      child.Source,
		Confidence:  child.Confidence,
		Evidence:    nonEmptyJSON(child.Evidence),
	}); err != nil {
		return errors.Wrap(err, "upsert company email")
	}
	return qtx.MarkSuggestionCompanyEmailApplied(ctx, db.MarkSuggestionCompanyEmailAppliedParams{
		ID: child.ID, ReviewedBy: &reviewedBy, ReviewNote: &reviewNote,
	})
}

func applySuggestionCompanyPhone(ctx context.Context, qtx *db.Queries, suggestionID, companyID, childID uuid.UUID, reviewedBy, reviewNote string) error {
	child, err := qtx.GetSuggestionCompanyPhoneByID(ctx, childID)
	if err != nil {
		return errors.Wrap(err, "get phone suggestion")
	}
	if err := validateSuggestionChild(suggestionID, child.SuggestionID, child.Status, child.ID); err != nil {
		return err
	}
	if _, err := qtx.UpsertCompanyPhone(ctx, db.UpsertCompanyPhoneParams{
		CompanyID:   companyID,
		Phone:       child.Phone,
		Description: child.Description,
		Purpose:     child.Purpose,
		Source:      child.Source,
		Confidence:  child.Confidence,
		Evidence:    nonEmptyJSON(child.Evidence),
	}); err != nil {
		return errors.Wrap(err, "upsert company phone")
	}
	return qtx.MarkSuggestionCompanyPhoneApplied(ctx, db.MarkSuggestionCompanyPhoneAppliedParams{
		ID: child.ID, ReviewedBy: &reviewedBy, ReviewNote: &reviewNote,
	})
}
