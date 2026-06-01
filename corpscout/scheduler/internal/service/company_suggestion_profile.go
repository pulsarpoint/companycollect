package service

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/slug"
)

func applySuggestionCompanyProfile(ctx context.Context, qtx *db.Queries, suggestion db.Suggestion, profileID uuid.UUID, reviewedBy, reviewNote string) (uuid.UUID, error) {
	profile, err := qtx.GetSuggestionCompanyProfileByID(ctx, profileID)
	if err != nil {
		return uuid.Nil, errors.Wrap(err, "get profile suggestion")
	}
	if err := validateSuggestionChild(suggestion.ID, profile.SuggestionID, profile.Status, profile.ID); err != nil {
		return uuid.Nil, err
	}

	companyID, exists := suggestionCompanyID(suggestion)
	if exists {
		if _, err := qtx.UpdateCompanyRegistryProfile(ctx, db.UpdateCompanyRegistryProfileParams{
			ID:                 companyID,
			RegistrationNumber: profile.RegistrationNumber,
			Lei:                profile.Lei,
			Status:             profile.LifecycleStatus,
			ParentLei:          profile.ParentLei,
			UltimateParentLei:  profile.UltimateParentLei,
		}); err != nil {
			return uuid.Nil, errors.Wrap(err, "update company registry profile fields")
		}
		if _, err := qtx.UpdateCompanyInfo(ctx, db.UpdateCompanyInfoParams{
			ID:               companyID,
			Name:             firstStringPtr(profile.Name, profile.DisplayName),
			ShortName:        profile.ShortName,
			ShortDescription: profile.ShortDescription,
			Description:      profile.Description,
			Website:          profile.Website,
			FoundedYear:      profile.FoundedYear,
		}); err != nil {
			return uuid.Nil, errors.Wrap(err, "update company profile fields")
		}
		if _, err := qtx.UpdateCompanyEnrichment(ctx, db.UpdateCompanyEnrichmentParams{
			ID:               companyID,
			ShortName:        profile.ShortName,
			ShortDescription: profile.ShortDescription,
			Description:      profile.Description,
			Website:          profile.Website,
			FoundedYear:      profile.FoundedYear,
			EmployeeEstimate: nonEmptyJSON(profile.EmployeeEstimate),
			RevenueEstimate:  nonEmptyJSON(profile.RevenueEstimate),
			Ownership:        nonEmptyJSON(profile.Ownership),
			EmployeeCount:    profile.EmployeeCount,
			RevenueUsd:       profile.RevenueUsd,
		}); err != nil {
			return uuid.Nil, errors.Wrap(err, "update company enrichment fields")
		}
	} else {
		createdCompany, err := createCompanyFromProfileSuggestion(ctx, qtx, suggestion, profile)
		if err != nil {
			return uuid.Nil, err
		}
		companyID = createdCompany.ID
		if err := qtx.UpdateSuggestionCreatedCompany(ctx, db.UpdateSuggestionCreatedCompanyParams{
			ID:               suggestion.ID,
			CreatedCompanyID: pgtype.UUID{Bytes: companyID, Valid: true},
		}); err != nil {
			return uuid.Nil, errors.Wrap(err, "store created company on suggestion")
		}
	}

	if err := qtx.MarkSuggestionCompanyProfileApplied(ctx, db.MarkSuggestionCompanyProfileAppliedParams{
		ID:         profile.ID,
		ReviewedBy: &reviewedBy,
		ReviewNote: &reviewNote,
	}); err != nil {
		return uuid.Nil, errors.Wrap(err, "mark profile suggestion applied")
	}
	return companyID, nil
}

func createCompanyFromProfileSuggestion(ctx context.Context, qtx *db.Queries, suggestion db.Suggestion, profile db.SuggestionCompanyProfile) (db.Company, error) {
	if !profile.CountryID.Valid {
		return db.Company{}, errors.Newf("profile suggestion %s has no country_id; cannot create company", profile.ID)
	}

	companyName := firstNonEmpty(profile.Name, profile.DisplayName, profile.RegistrationNumber, profile.Lei)
	if companyName == "" {
		return db.Company{}, errors.Newf("profile suggestion %s has no usable company name", profile.ID)
	}

	canonicalSlug := ""
	if profile.CanonicalSlug != nil {
		canonicalSlug = strings.TrimSpace(*profile.CanonicalSlug)
	}
	if canonicalSlug == "" {
		canonicalSlug = slug.Generate(companyName)
	}
	if canonicalSlug == "" {
		canonicalSlug = "company-" + suggestion.ID.String()[:12]
	}
	if _, err := qtx.GetCompanyBySlug(ctx, canonicalSlug); err == nil {
		canonicalSlug = canonicalSlug + "-" + suggestion.ID.String()[:12]
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return db.Company{}, errors.Wrap(err, "check slug collision")
	}

	status := "active"
	if profile.LifecycleStatus != nil && strings.TrimSpace(*profile.LifecycleStatus) != "" {
		status = strings.TrimSpace(*profile.LifecycleStatus)
	}

	company, err := qtx.InsertCompanyFromRawInput(ctx, db.InsertCompanyFromRawInputParams{
		CanonicalSlug:      canonicalSlug,
		Name:               companyName,
		CountryID:          uuid.UUID(profile.CountryID.Bytes),
		RegistrationNumber: profile.RegistrationNumber,
		Lei:                profile.Lei,
		Status:             status,
		Website:            profile.Website,
		PrimarySourceID:    pgtype.UUID{Bytes: suggestion.SourceID, Valid: true},
		ParentLei:          profile.ParentLei,
		UltimateParentLei:  profile.UltimateParentLei,
		Evidence:           nonEmptyJSON(profile.Evidence),
	})
	if err != nil {
		return db.Company{}, errors.Wrap(err, "insert company from profile suggestion")
	}
	return company, nil
}
