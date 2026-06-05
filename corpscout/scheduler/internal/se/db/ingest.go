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

func (g *Gateway) IngestBolagsverketRawRecords(ctx context.Context, records []BolagsverketRawRecord) (IngestRawRecordsResult, error) {
	result := IngestRawRecordsResult{RowsSeen: int32(len(records))}
	if len(records) == 0 {
		return result, nil
	}
	if err := g.withTx(ctx, func(tx pgx.Tx) error {
		for _, record := range records {
			record.PostalAddress = jsonObject(record.PostalAddress)
			record.RawPayload = jsonObject(record.RawPayload)
			record.Metadata = jsonObject(record.Metadata)

			current, hasCurrent, err := currentBolagsverketRecord(ctx, tx, record.SourceRecordKey)
			if err != nil {
				return err
			}
			if hasCurrent && current.PayloadHash != record.PayloadHash {
				if _, err := tx.Exec(ctx, `
					UPDATE se_workflow.bolagsverket_raw_records
					SET is_current = false, last_seen_at = now()
					WHERE source_record_key = $1
					  AND payload_hash <> $2
					  AND is_current = true
				`, record.SourceRecordKey, record.PayloadHash); err != nil {
					return errors.Wrap(err, "supersede current se workflow Bolagsverket raw record")
				}
			}

			var id uuid.UUID
			if err := tx.QueryRow(ctx, `
				INSERT INTO se_workflow.bolagsverket_raw_records (
					bulk_snapshot_id,
					source_file_id,
					source_record_key,
					row_number,
					organisationsidentitet,
					organization_number,
					namnskyddslopnummer,
					registreringsland,
					organisationsnamn,
					organization_name,
					organisationsform,
					avregistreringsdatum,
					avregistreringsorsak,
					pagande_avvecklings_eller_omstruktureringsforfarande,
					registreringsdatum,
					verksamhetsbeskrivning,
					postadress,
					postal_address,
					raw_payload,
					payload_hash,
					is_current,
					run_id,
					metadata
				)
				VALUES (
					(SELECT bulk_snapshot_id FROM se_workflow.source_files WHERE id = $1),
					$1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
					$11, $12, $13, $14, $15, $16, $17, $18, $19,
					true, $20, COALESCE($21::jsonb, '{}'::jsonb)
				)
				ON CONFLICT (source_record_key, payload_hash) DO UPDATE
				SET
					source_file_id = EXCLUDED.source_file_id,
					bulk_snapshot_id = EXCLUDED.bulk_snapshot_id,
					row_number = EXCLUDED.row_number,
					organisationsidentitet = EXCLUDED.organisationsidentitet,
					organization_number = EXCLUDED.organization_number,
					namnskyddslopnummer = EXCLUDED.namnskyddslopnummer,
					registreringsland = EXCLUDED.registreringsland,
					organisationsnamn = EXCLUDED.organisationsnamn,
					organization_name = EXCLUDED.organization_name,
					organisationsform = EXCLUDED.organisationsform,
					avregistreringsdatum = EXCLUDED.avregistreringsdatum,
					avregistreringsorsak = EXCLUDED.avregistreringsorsak,
					pagande_avvecklings_eller_omstruktureringsforfarande = EXCLUDED.pagande_avvecklings_eller_omstruktureringsforfarande,
					registreringsdatum = EXCLUDED.registreringsdatum,
					verksamhetsbeskrivning = EXCLUDED.verksamhetsbeskrivning,
					postadress = EXCLUDED.postadress,
					postal_address = EXCLUDED.postal_address,
					raw_payload = EXCLUDED.raw_payload,
					is_current = true,
					last_seen_at = now(),
					run_id = EXCLUDED.run_id,
					metadata = EXCLUDED.metadata
				RETURNING id
			`,
				nullableUUID(record.SourceFileID),
				record.SourceRecordKey,
				nullableInt32(record.RowNumber),
				record.Organisationsidentitet,
				record.OrganizationNumber,
				nullableString(record.Namnskyddslopnummer),
				nullableString(record.Registreringsland),
				record.Organisationsnamn,
				nullableString(record.OrganizationName),
				nullableString(record.Organisationsform),
				nullableString(record.Avregistreringsdatum),
				nullableString(record.Avregistreringsorsak),
				nullableString(record.PagandeAvvecklingsEllerOmstruktureringsforfarande),
				nullableString(record.Registreringsdatum),
				nullableString(record.Verksamhetsbeskrivning),
				nullableString(record.Postadress),
				record.PostalAddress,
				record.RawPayload,
				record.PayloadHash,
				nullableString(record.RunID),
				record.Metadata,
			).Scan(&id); err != nil {
				return errors.Wrap(err, "upsert se workflow Bolagsverket raw record")
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

func (g *Gateway) IngestSCBRawRecords(ctx context.Context, records []SCBRawRecord) (IngestRawRecordsResult, error) {
	result := IngestRawRecordsResult{RowsSeen: int32(len(records))}
	if len(records) == 0 {
		return result, nil
	}
	if err := g.withTx(ctx, func(tx pgx.Tx) error {
		for _, record := range records {
			record.MaskColumns = jsonObject(record.MaskColumns)
			record.SNICodes = jsonArray(record.SNICodes)
			record.PostalAddress = jsonObject(record.PostalAddress)
			record.RawPayload = jsonObject(record.RawPayload)
			record.Metadata = jsonObject(record.Metadata)

			current, hasCurrent, err := currentSCBRecord(ctx, tx, record.SourceRecordKey)
			if err != nil {
				return err
			}
			if hasCurrent && current.PayloadHash != record.PayloadHash {
				if _, err := tx.Exec(ctx, `
					UPDATE se_workflow.scb_raw_records
					SET is_current = false, last_seen_at = now()
					WHERE source_record_key = $1
					  AND payload_hash <> $2
					  AND is_current = true
				`, record.SourceRecordKey, record.PayloadHash); err != nil {
					return errors.Wrap(err, "supersede current se workflow SCB raw record")
				}
			}

			var id uuid.UUID
			if err := tx.QueryRow(ctx, `
				INSERT INTO se_workflow.scb_raw_records (
					bulk_snapshot_id,
					source_file_id,
					source_record_key,
					row_number,
					for_andr_typ,
					co_adress,
					foretagsnamn,
					ftg_stat,
					gatuadress,
					je_stat,
					jur_form,
					namn,
					ng1,
					ng2,
					ng3,
					ng4,
					ng5,
					pe_org_nr,
					organization_number,
					post_nr,
					post_ort,
					reg_dat_ktid,
					reklamsparrtyp,
					m_co_adress,
					m_foretagsnamn,
					m_ftg_stat,
					m_gatuadress,
					m_je_stat,
					m_jur_form,
					m_namn,
					m_ng1,
					m_ng2,
					m_ng3,
					m_ng4,
					m_ng5,
					m_post_nr,
					m_post_ort,
					m_reg_dat_ktid,
					m_reklamsparrtyp,
					mask_columns,
					sni_codes,
					postal_address,
					raw_payload,
					payload_hash,
					is_current,
					run_id,
					metadata
				)
				VALUES (
					(SELECT bulk_snapshot_id FROM se_workflow.source_files WHERE id = $1),
					$1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
					$11, $12, $13, $14, $15, $16, $17, $18, $19,
					$20, $21, $22, $23, $24, $25, $26, $27, $28,
					$29, $30, $31, $32, $33, $34, $35, $36, $37,
					$38, $39, $40, $41, $42, $43, true, $44,
					COALESCE($45::jsonb, '{}'::jsonb)
				)
				ON CONFLICT (source_record_key, payload_hash) DO UPDATE
				SET
					source_file_id = EXCLUDED.source_file_id,
					bulk_snapshot_id = EXCLUDED.bulk_snapshot_id,
					row_number = EXCLUDED.row_number,
					for_andr_typ = EXCLUDED.for_andr_typ,
					co_adress = EXCLUDED.co_adress,
					foretagsnamn = EXCLUDED.foretagsnamn,
					ftg_stat = EXCLUDED.ftg_stat,
					gatuadress = EXCLUDED.gatuadress,
					je_stat = EXCLUDED.je_stat,
					jur_form = EXCLUDED.jur_form,
					namn = EXCLUDED.namn,
					ng1 = EXCLUDED.ng1,
					ng2 = EXCLUDED.ng2,
					ng3 = EXCLUDED.ng3,
					ng4 = EXCLUDED.ng4,
					ng5 = EXCLUDED.ng5,
					pe_org_nr = EXCLUDED.pe_org_nr,
					organization_number = EXCLUDED.organization_number,
					post_nr = EXCLUDED.post_nr,
					post_ort = EXCLUDED.post_ort,
					reg_dat_ktid = EXCLUDED.reg_dat_ktid,
					reklamsparrtyp = EXCLUDED.reklamsparrtyp,
					m_co_adress = EXCLUDED.m_co_adress,
					m_foretagsnamn = EXCLUDED.m_foretagsnamn,
					m_ftg_stat = EXCLUDED.m_ftg_stat,
					m_gatuadress = EXCLUDED.m_gatuadress,
					m_je_stat = EXCLUDED.m_je_stat,
					m_jur_form = EXCLUDED.m_jur_form,
					m_namn = EXCLUDED.m_namn,
					m_ng1 = EXCLUDED.m_ng1,
					m_ng2 = EXCLUDED.m_ng2,
					m_ng3 = EXCLUDED.m_ng3,
					m_ng4 = EXCLUDED.m_ng4,
					m_ng5 = EXCLUDED.m_ng5,
					m_post_nr = EXCLUDED.m_post_nr,
					m_post_ort = EXCLUDED.m_post_ort,
					m_reg_dat_ktid = EXCLUDED.m_reg_dat_ktid,
					m_reklamsparrtyp = EXCLUDED.m_reklamsparrtyp,
					mask_columns = EXCLUDED.mask_columns,
					sni_codes = EXCLUDED.sni_codes,
					postal_address = EXCLUDED.postal_address,
					raw_payload = EXCLUDED.raw_payload,
					is_current = true,
					last_seen_at = now(),
					run_id = EXCLUDED.run_id,
					metadata = EXCLUDED.metadata
				RETURNING id
			`,
				nullableUUID(record.SourceFileID),
				record.SourceRecordKey,
				nullableInt32(record.RowNumber),
				nullableString(record.ForAndrTyp),
				nullableString(record.COAdress),
				nullableString(record.Foretagsnamn),
				nullableString(record.FtgStat),
				nullableString(record.Gatuadress),
				nullableString(record.JEStat),
				nullableString(record.JurForm),
				nullableString(record.Namn),
				nullableString(record.Ng1),
				nullableString(record.Ng2),
				nullableString(record.Ng3),
				nullableString(record.Ng4),
				nullableString(record.Ng5),
				record.PeOrgNr,
				nullableString(record.OrganizationNumber),
				nullableString(record.PostNr),
				nullableString(record.PostOrt),
				nullableString(record.RegDatKtid),
				nullableString(record.Reklamsparrtyp),
				nullableString(record.MCOAdress),
				nullableString(record.MForetagsnamn),
				nullableString(record.MFtgStat),
				nullableString(record.MGatuadress),
				nullableString(record.MJEStat),
				nullableString(record.MJurForm),
				nullableString(record.MNamn),
				nullableString(record.MNg1),
				nullableString(record.MNg2),
				nullableString(record.MNg3),
				nullableString(record.MNg4),
				nullableString(record.MNg5),
				nullableString(record.MPostNr),
				nullableString(record.MPostOrt),
				nullableString(record.MRegDatKtid),
				nullableString(record.MReklamsparrtyp),
				record.MaskColumns,
				record.SNICodes,
				record.PostalAddress,
				record.RawPayload,
				record.PayloadHash,
				nullableString(record.RunID),
				record.Metadata,
			).Scan(&id); err != nil {
				return errors.Wrap(err, "upsert se workflow SCB raw record")
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

func currentBolagsverketRecord(ctx context.Context, tx pgx.Tx, sourceRecordKey string) (currentRawRecord, bool, error) {
	var current currentRawRecord
	err := tx.QueryRow(ctx, `
		SELECT id, payload_hash
		FROM se_workflow.bolagsverket_raw_records
		WHERE source_record_key = $1
		  AND is_current = true
	`, sourceRecordKey).Scan(&current.ID, &current.PayloadHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return currentRawRecord{}, false, nil
	}
	if err != nil {
		return currentRawRecord{}, false, errors.Wrap(err, "get current se workflow Bolagsverket raw record")
	}
	return current, true, nil
}

func currentSCBRecord(ctx context.Context, tx pgx.Tx, sourceRecordKey string) (currentRawRecord, bool, error) {
	var current currentRawRecord
	err := tx.QueryRow(ctx, `
		SELECT id, payload_hash
		FROM se_workflow.scb_raw_records
		WHERE source_record_key = $1
		  AND is_current = true
	`, sourceRecordKey).Scan(&current.ID, &current.PayloadHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return currentRawRecord{}, false, nil
	}
	if err != nil {
		return currentRawRecord{}, false, errors.Wrap(err, "get current se workflow SCB raw record")
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

func nullableInt32(value int32) *int32 {
	if value <= 0 {
		return nil
	}
	return &value
}
