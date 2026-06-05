package francedb

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

const defaultFranceSourceProfileLimit int32 = 1000

func (g *Gateway) NormalizeSourceProfiles(
	ctx context.Context,
	command NormalizeSourceProfilesCommand,
) (NormalizeSourceProfilesResult, error) {
	if g == nil || g.pool == nil {
		return NormalizeSourceProfilesResult{}, errors.New("france workflow database pool not available")
	}
	if command.Limit < 0 {
		return NormalizeSourceProfilesResult{}, errors.New("france source profile limit cannot be negative")
	}
	limit := command.Limit
	if limit == 0 {
		limit = defaultFranceSourceProfileLimit
	}
	trigger := strings.TrimSpace(command.Trigger)
	if trigger == "" {
		trigger = "manual"
	}
	ids := command.IDs
	if ids == nil {
		ids = []string{}
	}

	queries := db.New(g.pool)
	rawLegalUnitIDs, err := queries.SelectFranceSourceProfileLegalUnitIDs(ctx, db.SelectFranceSourceProfileLegalUnitIDsParams{
		Ids:   ids,
		Query: textFilter(command.Filters, "query", "q"),
		Limit: limit,
	})
	if err != nil {
		return NormalizeSourceProfilesResult{}, errors.Wrap(err, "select france source profile legal units")
	}
	if len(rawLegalUnitIDs) == 0 {
		return NormalizeSourceProfilesResult{}, nil
	}

	result := NormalizeSourceProfilesResult{RecordsSeen: int32(len(rawLegalUnitIDs))}
	if err := g.mergeSourceProfiles(ctx, rawLegalUnitIDs, trigger, &result); err != nil {
		return NormalizeSourceProfilesResult{}, err
	}
	return result, nil
}

func (g *Gateway) mergeSourceProfiles(
	ctx context.Context,
	rawLegalUnitIDs []uuid.UUID,
	trigger string,
	result *NormalizeSourceProfilesResult,
) error {
	tx, err := g.pool.Begin(ctx)
	if err != nil {
		return errors.Wrap(err, "begin france source profile transaction")
	}
	defer func() { _ = tx.Rollback(ctx) }()

	queries := db.New(tx)
	if err := queries.SupersedeFranceSourceCompaniesForLegalUnits(ctx, rawLegalUnitIDs); err != nil {
		return errors.Wrap(err, "supersede changed france source companies")
	}
	companiesUpserted, err := queries.UpsertFranceSourceCompanies(ctx, db.UpsertFranceSourceCompaniesParams{
		RawLegalUnitIds: rawLegalUnitIDs,
		Trigger:         trigger,
	})
	if err != nil {
		return errors.Wrap(err, "upsert france source companies")
	}
	result.CompaniesUpserted = companiesUpserted

	if err := queries.SupersedeFranceSourceEstablishmentsForLegalUnits(ctx, rawLegalUnitIDs); err != nil {
		return errors.Wrap(err, "supersede changed france source establishments")
	}
	if err := queries.DemoteFranceSourceHeadquartersForLegalUnits(ctx, rawLegalUnitIDs); err != nil {
		return errors.Wrap(err, "clear france source active headquarters markers")
	}
	establishmentsUpserted, err := queries.UpsertFranceSourceEstablishments(ctx, db.UpsertFranceSourceEstablishmentsParams{
		RawLegalUnitIds: rawLegalUnitIDs,
		Trigger:         trigger,
	})
	if err != nil {
		return errors.Wrap(err, "upsert france source establishments")
	}
	result.EstablishmentsUpserted = establishmentsUpserted

	addressesUpserted, err := queries.UpsertFranceSourceAddresses(ctx, db.UpsertFranceSourceAddressesParams{
		RawLegalUnitIds: rawLegalUnitIDs,
		Trigger:         trigger,
	})
	if err != nil {
		return errors.Wrap(err, "upsert france source addresses")
	}
	result.AddressesUpserted = addressesUpserted

	if err := queries.DeleteFranceSourceIndustriesForLegalUnits(ctx, rawLegalUnitIDs); err != nil {
		return errors.Wrap(err, "delete stale france source industries")
	}
	industriesInserted, err := queries.InsertFranceSourceIndustries(ctx, db.InsertFranceSourceIndustriesParams{
		RawLegalUnitIds: rawLegalUnitIDs,
		Trigger:         trigger,
	})
	if err != nil {
		return errors.Wrap(err, "insert france source industries")
	}
	result.IndustriesInserted = industriesInserted

	if err := tx.Commit(ctx); err != nil {
		return errors.Wrap(err, "commit france source profile transaction")
	}
	return nil
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
