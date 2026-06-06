package countrydata

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pulsarpoint/corpscout/countrydata/finland/prhytj"
	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

type FinlandPRHYTJTxPool interface {
	db.DBTX
	Begin(context.Context) (pgx.Tx, error)
}

type FinlandPRHYTJDBStore struct {
	pool                FinlandPRHYTJTxPool
	latestSourceID      uuid.UUID
	latestDownloadRunID *uuid.UUID
}

func NewFinlandPRHYTJDBStore(pool FinlandPRHYTJTxPool) *FinlandPRHYTJDBStore {
	return &FinlandPRHYTJDBStore{pool: pool}
}

func (s *FinlandPRHYTJDBStore) SaveDownload(ctx context.Context, metadata countryimport.DownloadMetadata) error {
	var sourceID uuid.UUID
	var downloadRunID uuid.UUID
	if err := s.withTx(ctx, func(tx pgx.Tx) error {
		q := db.New(tx)

		id, err := upsertFinlandPRHYTJSource(ctx, q, metadata.BaseURL)
		if err != nil {
			return err
		}
		sourceID = id

		runID, err := q.RecordFinlandPRHYTJDownloadRun(ctx, db.RecordFinlandPRHYTJDownloadRunParams{
			SourceID:             sourceID,
			Status:               "succeeded",
			BaseUrl:              resolveFinlandPRHYTJBaseURL(metadata.BaseURL),
			SnapshotPath:         optionalString(metadata.SnapshotPath),
			SnapshotSha256:       optionalString(metadata.SHA256),
			StartedAt:            nonZeroTime(metadata.StartedAt),
			FinishedAt:           optionalTimestamp(metadata.FinishedAt),
			DurationMs:           optionalPositiveInt64(metadata.DurationMS),
			BytesDownloaded:      metadata.BytesDownloaded,
			RecordsSeen:          metadata.RecordsSeen,
			PagesDownloaded:      int32(metadata.PagesDownloaded),
			FirstPage:            optionalPositiveInt32(metadata.FirstPage),
			LastPage:             optionalPositiveInt32(metadata.LastPage),
			TotalResultsReported: metadata.TotalResultsReported,
			Metadata:             jsonObject(metadata),
		})
		if err != nil {
			return errors.Wrap(err, "record Finland PRH YTJ download run")
		}
		downloadRunID = runID
		return nil
	}); err != nil {
		return err
	}

	s.latestSourceID = sourceID
	s.latestDownloadRunID = &downloadRunID
	return nil
}

func (s *FinlandPRHYTJDBStore) SaveProcess(ctx context.Context, metadata countryimport.ProcessMetadata) error {
	if s == nil || s.latestDownloadRunID == nil {
		return nil
	}
	downloadRunID := *s.latestDownloadRunID
	return s.withTx(ctx, func(tx pgx.Tx) error {
		if err := db.New(tx).UpdateFinlandPRHYTJDownloadProcessStats(ctx, db.UpdateFinlandPRHYTJDownloadProcessStatsParams{
			RecordsProcessed: metadata.RecordsProcessed,
			RecordsStored:    metadata.RecordsStored,
			DecodeErrors:     metadata.DecodeErrors,
			ChunksProcessed:  metadata.ChunksProcessed,
			FinishedAt:       optionalTimestamp(metadata.FinishedAt),
			ID:               downloadRunID,
		}); err != nil {
			return errors.Wrap(err, "update Finland PRH YTJ process stats")
		}
		return nil
	})
}

func (s *FinlandPRHYTJDBStore) StoreCompanies(ctx context.Context, records []prhytj.CompanyRecord) (countryimport.StoreResult, error) {
	result := countryimport.StoreResult{RecordsReceived: int64(len(records))}
	if len(records) == 0 {
		return result, nil
	}

	var recordsStored int64
	if err := s.withTx(ctx, func(tx pgx.Tx) error {
		q := db.New(tx)
		sourceID := s.latestSourceID
		if sourceID == uuid.Nil {
			id, err := upsertFinlandPRHYTJSource(ctx, q, "")
			if err != nil {
				return err
			}
			sourceID = id
		}

		downloadRunID := s.latestDownloadRunID
		for _, record := range records {
			params, err := finlandPRHYTJRawRecordParams(sourceID, downloadRunID, record)
			if err != nil {
				return err
			}

			current, err := q.GetCurrentFinlandPRHYTJRawRecord(ctx, params.BusinessID)
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return errors.Wrap(err, "get current Finland PRH YTJ raw record")
			}
			if err == nil && current.PayloadHash != params.PayloadHash {
				if err := q.SupersedeCurrentFinlandPRHYTJRawRecord(ctx, db.SupersedeCurrentFinlandPRHYTJRawRecordParams{
					BusinessID:  params.BusinessID,
					PayloadHash: params.PayloadHash,
				}); err != nil {
					return errors.Wrap(err, "supersede current Finland PRH YTJ raw record")
				}
			}

			if _, err := q.UpsertFinlandPRHYTJRawRecord(ctx, params); err != nil {
				return errors.Wrap(err, "upsert Finland PRH YTJ raw record")
			}
			recordsStored++
		}
		s.latestSourceID = sourceID
		return nil
	}); err != nil {
		return countryimport.StoreResult{}, err
	}

	result.RecordsStored = recordsStored
	return result, nil
}

func (s *FinlandPRHYTJDBStore) withTx(ctx context.Context, fn func(pgx.Tx) error) error {
	if s == nil || s.pool == nil {
		return countryimport.WrapSourceError(
			countryimport.ErrorKindState,
			prhytj.SourceSlug,
			"",
			"",
			0,
			errors.New("Finland PRH YTJ database pool not available"),
		)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return countryimport.WrapSourceError(
			countryimport.ErrorKindState,
			prhytj.SourceSlug,
			"",
			"",
			0,
			errors.Wrap(err, "begin Finland PRH YTJ transaction"),
		)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := fn(tx); err != nil {
		kind := countryimport.Classify(err)
		if kind == countryimport.ErrorKindUnknown {
			kind = countryimport.ErrorKindState
		}
		return countryimport.WrapSourceError(
			kind,
			prhytj.SourceSlug,
			"",
			"",
			0,
			err,
		)
	}
	if err := tx.Commit(ctx); err != nil {
		return countryimport.WrapSourceError(
			countryimport.ErrorKindState,
			prhytj.SourceSlug,
			"",
			"",
			0,
			errors.Wrap(err, "commit Finland PRH YTJ transaction"),
		)
	}
	return nil
}

func upsertFinlandPRHYTJSource(ctx context.Context, q *db.Queries, baseURL string) (uuid.UUID, error) {
	sourceID, err := q.UpsertFinlandPRHYTJSource(ctx, db.UpsertFinlandPRHYTJSourceParams{
		SourceSlug:          prhytj.SourceSlug,
		SourceName:          prhytj.SourceName,
		BaseUrl:             resolveFinlandPRHYTJBaseURL(baseURL),
		SupportsIncremental: false,
		Metadata: jsonObject(map[string]any{
			"schema":               "countrydata_finland_prh_ytj",
			"raw_table":            "countrydata_finland_prh_ytj.raw_records",
			"supports_incremental": false,
		}),
	})
	if err != nil {
		return uuid.Nil, errors.Wrap(err, "upsert Finland PRH YTJ source")
	}
	return sourceID, nil
}

func finlandPRHYTJRawRecordParams(sourceID uuid.UUID, downloadRunID *uuid.UUID, record prhytj.CompanyRecord) (db.UpsertFinlandPRHYTJRawRecordParams, error) {
	businessID := strings.TrimSpace(record.BusinessID.Value)
	if businessID == "" {
		return db.UpsertFinlandPRHYTJRawRecordParams{}, countryimport.WrapSourceError(
			countryimport.ErrorKindLineDecode,
			prhytj.SourceSlug,
			"",
			"",
			0,
			errors.New("missing Finland PRH YTJ business ID"),
		)
	}
	if len(record.RawPayload) == 0 {
		return db.UpsertFinlandPRHYTJRawRecordParams{}, countryimport.WrapSourceError(
			countryimport.ErrorKindLineDecode,
			prhytj.SourceSlug,
			"",
			"",
			0,
			errors.New("missing Finland PRH YTJ raw payload"),
		)
	}
	if !isJSONObjectPayload(record.RawPayload) {
		return db.UpsertFinlandPRHYTJRawRecordParams{}, countryimport.WrapSourceError(
			countryimport.ErrorKindLineDecode,
			prhytj.SourceSlug,
			"",
			"",
			0,
			errors.New("invalid Finland PRH YTJ raw payload JSON"),
		)
	}
	payloadHash := strings.TrimSpace(record.PayloadHash)
	if payloadHash == "" {
		return db.UpsertFinlandPRHYTJRawRecordParams{}, countryimport.WrapSourceError(
			countryimport.ErrorKindLineDecode,
			prhytj.SourceSlug,
			"",
			"",
			0,
			errors.New("missing Finland PRH YTJ payload hash"),
		)
	}

	registrationDate, err := optionalDate(record.RegistrationDate)
	if err != nil {
		return db.UpsertFinlandPRHYTJRawRecordParams{}, countryimport.WrapSourceError(
			countryimport.ErrorKindLineDecode,
			prhytj.SourceSlug,
			"",
			"",
			0,
			errors.Wrap(err, "parse Finland PRH YTJ registration date"),
		)
	}
	endDate, err := optionalDate(record.EndDate)
	if err != nil {
		return db.UpsertFinlandPRHYTJRawRecordParams{}, countryimport.WrapSourceError(
			countryimport.ErrorKindLineDecode,
			prhytj.SourceSlug,
			"",
			"",
			0,
			errors.Wrap(err, "parse Finland PRH YTJ end date"),
		)
	}
	sourceUpdatedAt, err := optionalPRHTimestamp(record.LastModified)
	if err != nil {
		return db.UpsertFinlandPRHYTJRawRecordParams{}, countryimport.WrapSourceError(
			countryimport.ErrorKindLineDecode,
			prhytj.SourceSlug,
			"",
			"",
			0,
			errors.Wrap(err, "parse Finland PRH YTJ last modified time"),
		)
	}

	profile := record.ToProfile()
	return db.UpsertFinlandPRHYTJRawRecordParams{
		SourceID:             sourceID,
		DownloadRunID:        optionalUUID(downloadRunID),
		SourceNativeID:       businessID,
		BusinessID:           businessID,
		VatID:                optionalString(profile.VATID),
		Euid:                 optionalString(profile.EUID),
		LegalName:            optionalString(profile.LegalName),
		TradeRegisterStatus:  optionalString(record.TradeRegisterStatus),
		Status:               optionalString(record.Status),
		IsActive:             optionalBool(profile.IsActive),
		LegalForm:            optionalString(profile.LegalForm),
		LegalFormCode:        optionalString(profile.LegalFormCode),
		MainBusinessLine:     optionalString(profile.MainBusinessLine),
		MainBusinessLineCode: optionalString(profile.MainBusinessLineCode),
		Website:              optionalString(profile.Website),
		CountryIso2:          optionalString("FI"),
		RegistrationDate:     registrationDate,
		EndDate:              endDate,
		SourceUpdatedAt:      sourceUpdatedAt,
		RawPayload:           append([]byte(nil), record.RawPayload...),
		PayloadHash:          payloadHash,
		Metadata:             []byte(`{}`),
	}, nil
}

func resolveFinlandPRHYTJBaseURL(baseURL string) string {
	if trimmed := strings.TrimSpace(baseURL); trimmed != "" {
		return trimmed
	}
	return prhytj.DefaultBaseURL
}

func isJSONObjectPayload(payload []byte) bool {
	trimmed := bytes.TrimSpace(payload)
	return len(trimmed) > 0 && trimmed[0] == '{' && json.Valid(trimmed)
}

func nonZeroTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Now().UTC()
	}
	return value
}

func optionalString(value string) *string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func optionalBool(value bool) *bool {
	return &value
}

func optionalPositiveInt32(value int) *int32 {
	if value <= 0 {
		return nil
	}
	converted := int32(value)
	return &converted
}

func optionalPositiveInt64(value int64) *int64 {
	if value <= 0 {
		return nil
	}
	return &value
}

func optionalUUID(value *uuid.UUID) pgtype.UUID {
	if value == nil || *value == uuid.Nil {
		return pgtype.UUID{}
	}
	return pgtype.UUID{Bytes: [16]byte(*value), Valid: true}
}

func optionalDate(value string) (pgtype.Date, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return pgtype.Date{}, nil
	}
	if len(trimmed) > len(time.DateOnly) {
		trimmed = trimmed[:len(time.DateOnly)]
	}
	parsed, err := time.Parse(time.DateOnly, trimmed)
	if err != nil {
		return pgtype.Date{}, err
	}
	return pgtype.Date{Time: parsed, Valid: true}, nil
}

func optionalPRHTimestamp(value string) (pgtype.Timestamptz, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return pgtype.Timestamptz{}, nil
	}
	if parsed, err := time.Parse(time.RFC3339Nano, trimmed); err == nil {
		return pgtype.Timestamptz{Time: parsed, Valid: true}, nil
	}
	parsed, err := time.Parse(time.DateOnly, trimmed)
	if err != nil {
		return pgtype.Timestamptz{}, err
	}
	return pgtype.Timestamptz{Time: parsed, Valid: true}, nil
}

func optionalTimestamp(value time.Time) pgtype.Timestamptz {
	if value.IsZero() {
		return pgtype.Timestamptz{}
	}
	return pgtype.Timestamptz{Time: value, Valid: true}
}

func jsonObject(value any) []byte {
	if value == nil {
		return []byte(`{}`)
	}
	payload, err := json.Marshal(value)
	if err != nil || !json.Valid(payload) {
		return []byte(`{}`)
	}
	return payload
}
