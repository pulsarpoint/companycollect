package francedb

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

func (g *Gateway) IngestLegalUnits(ctx context.Context, records []db.UpsertFranceWorkflowRawLegalUnitParams) (IngestRawRecordsResult, error) {
	result := IngestRawRecordsResult{RowsSeen: int32(len(records))}
	if len(records) == 0 {
		return result, nil
	}
	if err := g.withTx(ctx, func(q *db.Queries) error {
		for _, record := range records {
			record.Metadata = jsonObject(record.Metadata)
			current, hasCurrent, err := currentLegalUnit(ctx, q, record.Siren)
			if err != nil {
				return err
			}
			if hasCurrent && current.PayloadHash != record.PayloadHash {
				if err := q.SupersedeCurrentFranceWorkflowRawLegalUnit(ctx, db.SupersedeCurrentFranceWorkflowRawLegalUnitParams{
					Siren:       record.Siren,
					PayloadHash: record.PayloadHash,
				}); err != nil {
					return errors.Wrap(err, "supersede current france workflow raw legal unit")
				}
			}
			row, err := q.UpsertFranceWorkflowRawLegalUnit(ctx, record)
			if err != nil {
				return errors.Wrap(err, "upsert france workflow raw legal unit")
			}
			result.RowsWritten += row.RowsWritten
			switch {
			case !hasCurrent:
				result.RowsInsertedNew++
			case current.PayloadHash == record.PayloadHash:
				result.RowsExistingUnchanged++
			default:
				result.RowsNewVersions++
			}
			result.RawRecordIDs = append(result.RawRecordIDs, row.ID)
		}
		return nil
	}); err != nil {
		return IngestRawRecordsResult{}, err
	}
	return result, nil
}

func (g *Gateway) IngestEstablishments(ctx context.Context, records []db.UpsertFranceWorkflowRawEstablishmentParams) (IngestRawRecordsResult, error) {
	result := IngestRawRecordsResult{RowsSeen: int32(len(records))}
	if len(records) == 0 {
		return result, nil
	}
	if err := g.withTx(ctx, func(q *db.Queries) error {
		for _, record := range records {
			record.Metadata = jsonObject(record.Metadata)
			current, hasCurrent, err := currentEstablishment(ctx, q, record.Siret)
			if err != nil {
				return err
			}
			if hasCurrent && current.PayloadHash != record.PayloadHash {
				if err := q.SupersedeCurrentFranceWorkflowRawEstablishment(ctx, db.SupersedeCurrentFranceWorkflowRawEstablishmentParams{
					Siret:       record.Siret,
					PayloadHash: record.PayloadHash,
				}); err != nil {
					return errors.Wrap(err, "supersede current france workflow raw establishment")
				}
			}
			row, err := q.UpsertFranceWorkflowRawEstablishment(ctx, record)
			if err != nil {
				return errors.Wrap(err, "upsert france workflow raw establishment")
			}
			result.RowsWritten += row.RowsWritten
			switch {
			case !hasCurrent:
				result.RowsInsertedNew++
			case current.PayloadHash == record.PayloadHash:
				result.RowsExistingUnchanged++
			default:
				result.RowsNewVersions++
			}
			result.RawRecordIDs = append(result.RawRecordIDs, row.ID)
		}
		return nil
	}); err != nil {
		return IngestRawRecordsResult{}, err
	}
	return result, nil
}

func currentLegalUnit(ctx context.Context, q *db.Queries, siren string) (db.GetCurrentFranceWorkflowRawLegalUnitRow, bool, error) {
	current, err := q.GetCurrentFranceWorkflowRawLegalUnit(ctx, siren)
	if errors.Is(err, pgx.ErrNoRows) {
		return db.GetCurrentFranceWorkflowRawLegalUnitRow{}, false, nil
	}
	if err != nil {
		return db.GetCurrentFranceWorkflowRawLegalUnitRow{}, false, errors.Wrap(err, "get current france workflow raw legal unit")
	}
	return current, true, nil
}

func currentEstablishment(ctx context.Context, q *db.Queries, siret string) (db.GetCurrentFranceWorkflowRawEstablishmentRow, bool, error) {
	current, err := q.GetCurrentFranceWorkflowRawEstablishment(ctx, siret)
	if errors.Is(err, pgx.ErrNoRows) {
		return db.GetCurrentFranceWorkflowRawEstablishmentRow{}, false, nil
	}
	if err != nil {
		return db.GetCurrentFranceWorkflowRawEstablishmentRow{}, false, errors.Wrap(err, "get current france workflow raw establishment")
	}
	return current, true, nil
}
