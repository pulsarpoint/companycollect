package ariregisterdb

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

func (g *Gateway) IngestRawRecords(ctx context.Context, records []db.UpsertAriregisterWorkflowRawRecordParams) (IngestRawRecordsResult, error) {
	result := IngestRawRecordsResult{RowsSeen: int32(len(records))}
	if len(records) == 0 {
		return result, nil
	}
	if err := g.withTx(ctx, func(q *db.Queries) error {
		for _, record := range records {
			record.Metadata = jsonObject(record.Metadata)
			current, hasCurrent, err := currentRawRecord(ctx, q, record.RegistryCode)
			if err != nil {
				return err
			}
			if hasCurrent && current.PayloadHash != record.PayloadHash {
				if err := q.SupersedeCurrentAriregisterWorkflowRawRecord(ctx, db.SupersedeCurrentAriregisterWorkflowRawRecordParams{
					RegistryCode: record.RegistryCode,
					PayloadHash:  record.PayloadHash,
				}); err != nil {
					return errors.Wrap(err, "supersede current ariregister workflow raw record")
				}
			}
			row, err := q.UpsertAriregisterWorkflowRawRecord(ctx, record)
			if err != nil {
				return errors.Wrap(err, "upsert ariregister workflow raw record")
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

func currentRawRecord(ctx context.Context, q *db.Queries, registryCode string) (db.GetCurrentAriregisterWorkflowRawRecordRow, bool, error) {
	current, err := q.GetCurrentAriregisterWorkflowRawRecord(ctx, registryCode)
	if errors.Is(err, pgx.ErrNoRows) {
		return db.GetCurrentAriregisterWorkflowRawRecordRow{}, false, nil
	}
	if err != nil {
		return db.GetCurrentAriregisterWorkflowRawRecordRow{}, false, errors.Wrap(err, "get current ariregister workflow raw record")
	}
	return current, true, nil
}
