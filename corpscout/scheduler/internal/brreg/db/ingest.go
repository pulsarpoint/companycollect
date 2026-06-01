package brregdb

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

func (g *Gateway) IngestRawRecords(ctx context.Context, records []db.UpsertBrregWorkflowRawRecordParams) (IngestRawRecordsResult, error) {
	result := IngestRawRecordsResult{RowsSeen: int32(len(records))}
	if len(records) == 0 {
		return result, nil
	}
	if err := g.withTx(ctx, func(q *db.Queries) error {
		for _, record := range records {
			record.Metadata = jsonObject(record.Metadata)
			current, hasCurrent, err := currentRawRecord(ctx, q, record.OrganizationNumber)
			if err != nil {
				return err
			}
			if hasCurrent && current.PayloadHash != record.PayloadHash {
				if err := q.SupersedeCurrentBrregWorkflowRawRecord(ctx, db.SupersedeCurrentBrregWorkflowRawRecordParams{
					OrganizationNumber: record.OrganizationNumber,
					PayloadHash:        record.PayloadHash,
				}); err != nil {
					return errors.Wrap(err, "supersede current brreg workflow raw record")
				}
			}
			row, err := q.UpsertBrregWorkflowRawRecord(ctx, record)
			if err != nil {
				return errors.Wrap(err, "upsert brreg workflow raw record")
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

func currentRawRecord(ctx context.Context, q *db.Queries, organizationNumber string) (db.GetCurrentBrregWorkflowRawRecordRow, bool, error) {
	current, err := q.GetCurrentBrregWorkflowRawRecord(ctx, organizationNumber)
	if errors.Is(err, pgx.ErrNoRows) {
		return db.GetCurrentBrregWorkflowRawRecordRow{}, false, nil
	}
	if err != nil {
		return db.GetCurrentBrregWorkflowRawRecordRow{}, false, errors.Wrap(err, "get current brreg workflow raw record")
	}
	return current, true, nil
}
