package sedb

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func (g *Gateway) IngestRawRecords(ctx context.Context, records []RawRecord) (IngestRawRecordsResult, error) {
	result := IngestRawRecordsResult{RowsSeen: int32(len(records))}
	if len(records) == 0 {
		return result, nil
	}
	if err := g.withTx(ctx, func(tx pgx.Tx) error {
		for _, record := range records {
			record.SNICodes = jsonArray(record.SNICodes)
			record.PostalAddress = jsonObject(record.PostalAddress)
			record.RawPayload = jsonObject(record.RawPayload)
			record.Metadata = jsonObject(record.Metadata)

			current, hasCurrent, err := currentRecord(ctx, tx, record.OrganizationNumber)
			if err != nil {
				return err
			}
			if hasCurrent && current.PayloadHash != record.PayloadHash {
				if _, err := tx.Exec(ctx, `
					UPDATE se_workflow.raw_records
					SET is_current = false, last_seen_at = now()
					WHERE organization_number = $1
					  AND payload_hash <> $2
					  AND is_current = true
				`, record.OrganizationNumber, record.PayloadHash); err != nil {
					return errors.Wrap(err, "supersede current se workflow raw record")
				}
			}

			var id uuid.UUID
			if err := tx.QueryRow(ctx, `
				INSERT INTO se_workflow.raw_records (
					source_file_id,
					source_native_id,
					organization_number,
					organization_name,
					registration_status,
					legal_form,
					business_description,
					country_iso2,
					sni_codes,
					postal_address,
					raw_payload,
					payload_hash,
					is_current,
					run_id,
					metadata
				)
				VALUES ($1, $2, $3, $4, $5, $6, $7, 'SE', $8, $9, $10, $11, true, $12, COALESCE($13::jsonb, '{}'::jsonb))
				ON CONFLICT (organization_number, payload_hash) DO UPDATE
				SET
					source_file_id = EXCLUDED.source_file_id,
					organization_name = EXCLUDED.organization_name,
					registration_status = EXCLUDED.registration_status,
					legal_form = EXCLUDED.legal_form,
					business_description = EXCLUDED.business_description,
					sni_codes = EXCLUDED.sni_codes,
					postal_address = EXCLUDED.postal_address,
					raw_payload = EXCLUDED.raw_payload,
					is_current = true,
					last_seen_at = now(),
					run_id = EXCLUDED.run_id,
					metadata = EXCLUDED.metadata
				RETURNING id
			`, nullableUUID(record.SourceFileID), record.SourceNativeID, record.OrganizationNumber,
				nullableString(record.OrganizationName), nullableString(record.RegistrationStatus),
				nullableString(record.LegalForm), nullableString(record.BusinessDescription),
				record.SNICodes, record.PostalAddress, record.RawPayload, record.PayloadHash,
				nullableString(record.RunID), record.Metadata).Scan(&id); err != nil {
				return errors.Wrap(err, "upsert se workflow raw record")
			}

			result.RowsWritten++
			switch {
			case !hasCurrent:
				result.RowsInsertedNew++
			case current.PayloadHash == record.PayloadHash:
				result.RowsExistingUnchanged++
			default:
				result.RowsNewVersions++
			}
			result.RawRecordIDs = append(result.RawRecordIDs, id)
		}
		return nil
	}); err != nil {
		return IngestRawRecordsResult{}, err
	}
	return result, nil
}

func currentRecord(ctx context.Context, tx pgx.Tx, organizationNumber string) (currentRawRecord, bool, error) {
	var current currentRawRecord
	err := tx.QueryRow(ctx, `
		SELECT id, payload_hash
		FROM se_workflow.raw_records
		WHERE organization_number = $1
		  AND is_current = true
	`, organizationNumber).Scan(&current.ID, &current.PayloadHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return currentRawRecord{}, false, nil
	}
	if err != nil {
		return currentRawRecord{}, false, errors.Wrap(err, "get current se workflow raw record")
	}
	return current, true, nil
}

func nullableUUID(value uuid.UUID) *uuid.UUID {
	if value == uuid.Nil {
		return nil
	}
	return &value
}

func nullableString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}
