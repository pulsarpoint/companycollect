package countrydata

import (
	"context"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
	"github.com/pulsarpoint/corpscout/countrydata/united_states/secedgar"
	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

type USSECEDGARTxPool interface {
	db.DBTX
	Begin(context.Context) (pgx.Tx, error)
}

type USSECEDGARDBStore struct {
	pool                USSECEDGARTxPool
	latestSourceID      uuid.UUID
	latestDownloadRunID *uuid.UUID
}

func NewUSSECEDGARDBStore(pool USSECEDGARTxPool) *USSECEDGARDBStore {
	return &USSECEDGARDBStore{pool: pool}
}

func (s *USSECEDGARDBStore) SaveDownload(ctx context.Context, metadata countryimport.DownloadMetadata) error {
	var sourceID uuid.UUID
	var downloadRunID uuid.UUID
	if err := s.withTx(ctx, func(tx pgx.Tx) error {
		q := db.New(tx)

		id, err := upsertUSSECEDGARSource(ctx, q, metadata.BaseURL)
		if err != nil {
			return err
		}
		sourceID = id

		runID, err := q.RecordUSSECEDGARDownloadRun(ctx, db.RecordUSSECEDGARDownloadRunParams{
			SourceID:             sourceID,
			Status:               "succeeded",
			BaseUrl:              resolveUSSECEDGARBaseURL(metadata.BaseURL),
			SnapshotPath:         optionalString(metadata.SnapshotPath),
			SnapshotSha256:       optionalString(metadata.SHA256),
			StartedAt:            nonZeroTime(metadata.StartedAt),
			FinishedAt:           optionalTimestamp(metadata.FinishedAt),
			DurationMs:           optionalPositiveInt64(metadata.DurationMS),
			BytesDownloaded:      metadata.BytesDownloaded,
			RecordsSeen:          metadata.RecordsSeen,
			FilesDownloaded:      int32(metadata.PagesDownloaded),
			TotalResultsReported: metadata.TotalResultsReported,
			Metadata:             jsonObject(metadata),
		})
		if err != nil {
			return errors.Wrap(err, "record SEC EDGAR download run")
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

func (s *USSECEDGARDBStore) SaveProcess(ctx context.Context, metadata countryimport.ProcessMetadata) error {
	if s == nil || s.latestDownloadRunID == nil {
		return nil
	}
	downloadRunID := *s.latestDownloadRunID
	return s.withTx(ctx, func(tx pgx.Tx) error {
		if err := db.New(tx).UpdateUSSECEDGARDownloadProcessStats(ctx, db.UpdateUSSECEDGARDownloadProcessStatsParams{
			RecordsProcessed: metadata.RecordsProcessed,
			RecordsStored:    metadata.RecordsStored,
			DecodeErrors:     metadata.DecodeErrors,
			ChunksProcessed:  metadata.ChunksProcessed,
			FinishedAt:       optionalTimestamp(metadata.FinishedAt),
			ID:               downloadRunID,
		}); err != nil {
			return errors.Wrap(err, "update SEC EDGAR process stats")
		}
		return nil
	})
}

func (s *USSECEDGARDBStore) StoreCompanies(ctx context.Context, records []secedgar.SecTickerRecord) (countryimport.StoreResult, error) {
	result := countryimport.StoreResult{RecordsReceived: int64(len(records))}
	if len(records) == 0 {
		return result, nil
	}

	var recordsStored int64
	if err := s.withTx(ctx, func(tx pgx.Tx) error {
		q := db.New(tx)
		sourceID := s.latestSourceID
		if sourceID == uuid.Nil {
			id, err := upsertUSSECEDGARSource(ctx, q, "")
			if err != nil {
				return err
			}
			sourceID = id
		}

		downloadRunID := s.latestDownloadRunID
		for _, record := range records {
			params, err := usSECEDGARRawRecordParams(sourceID, downloadRunID, record)
			if err != nil {
				return err
			}

			current, err := q.GetCurrentUSSECEDGARRawRecord(ctx, params.Cik)
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return errors.Wrap(err, "get current SEC EDGAR raw record")
			}
			if err == nil && current.PayloadHash != params.PayloadHash {
				if err := q.SupersedeCurrentUSSECEDGARRawRecord(ctx, db.SupersedeCurrentUSSECEDGARRawRecordParams{
					Cik:         params.Cik,
					PayloadHash: params.PayloadHash,
				}); err != nil {
					return errors.Wrap(err, "supersede current SEC EDGAR raw record")
				}
			}

			if _, err := q.UpsertUSSECEDGARRawRecord(ctx, params); err != nil {
				return errors.Wrap(err, "upsert SEC EDGAR raw record")
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

func (s *USSECEDGARDBStore) withTx(ctx context.Context, fn func(pgx.Tx) error) error {
	if s == nil || s.pool == nil {
		return countryimport.WrapSourceError(
			countryimport.ErrorKindState,
			secedgar.SourceSlug,
			"",
			"",
			0,
			errors.New("SEC EDGAR database pool not available"),
		)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return countryimport.WrapSourceError(
			countryimport.ErrorKindState,
			secedgar.SourceSlug,
			"",
			"",
			0,
			errors.Wrap(err, "begin SEC EDGAR transaction"),
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
			secedgar.SourceSlug,
			"",
			"",
			0,
			err,
		)
	}
	if err := tx.Commit(ctx); err != nil {
		return countryimport.WrapSourceError(
			countryimport.ErrorKindState,
			secedgar.SourceSlug,
			"",
			"",
			0,
			errors.Wrap(err, "commit SEC EDGAR transaction"),
		)
	}
	return nil
}

func upsertUSSECEDGARSource(ctx context.Context, q *db.Queries, baseURL string) (uuid.UUID, error) {
	sourceID, err := q.UpsertUSSECEDGARSource(ctx, db.UpsertUSSECEDGARSourceParams{
		SourceSlug:          secedgar.SourceSlug,
		SourceName:          secedgar.SourceName,
		BaseUrl:             resolveUSSECEDGARBaseURL(baseURL),
		SupportsIncremental: false,
		Metadata: jsonObject(map[string]any{
			"schema":               "countrydata_united_states_sec_edgar",
			"raw_table":            "countrydata_united_states_sec_edgar.raw_records",
			"supports_incremental": false,
		}),
	})
	if err != nil {
		return uuid.Nil, errors.Wrap(err, "upsert SEC EDGAR source")
	}
	return sourceID, nil
}

func usSECEDGARRawRecordParams(sourceID uuid.UUID, downloadRunID *uuid.UUID, record secedgar.SecTickerRecord) (db.UpsertUSSECEDGARRawRecordParams, error) {
	if record.CIKStr <= 0 {
		return db.UpsertUSSECEDGARRawRecordParams{}, countryimport.WrapSourceError(
			countryimport.ErrorKindLineDecode,
			secedgar.SourceSlug,
			"",
			"",
			0,
			errors.New("missing or non-positive SEC EDGAR CIK"),
		)
	}
	if len(record.RawPayload) == 0 {
		return db.UpsertUSSECEDGARRawRecordParams{}, countryimport.WrapSourceError(
			countryimport.ErrorKindLineDecode,
			secedgar.SourceSlug,
			"",
			"",
			0,
			errors.New("missing SEC EDGAR raw payload"),
		)
	}
	if !isJSONObjectPayload(record.RawPayload) {
		return db.UpsertUSSECEDGARRawRecordParams{}, countryimport.WrapSourceError(
			countryimport.ErrorKindLineDecode,
			secedgar.SourceSlug,
			"",
			"",
			0,
			errors.New("invalid SEC EDGAR raw payload JSON"),
		)
	}
	payloadHash := strings.TrimSpace(record.PayloadHash)
	if payloadHash == "" {
		return db.UpsertUSSECEDGARRawRecordParams{}, countryimport.WrapSourceError(
			countryimport.ErrorKindLineDecode,
			secedgar.SourceSlug,
			"",
			"",
			0,
			errors.New("missing SEC EDGAR payload hash"),
		)
	}

	profile := record.ToProfile()
	cik := strconv.FormatInt(record.CIKStr, 10)
	cikNumber := record.CIKStr

	return db.UpsertUSSECEDGARRawRecordParams{
		SourceID:        sourceID,
		DownloadRunID:   optionalUUID(downloadRunID),
		SourceNativeID:  cik,
		Cik:             cik,
		PrimaryID:       optionalString(profile.PrimaryID),
		CikNumber:       &cikNumber,
		Ticker:          optionalString(profile.Ticker),
		LegalName:       optionalString(profile.LegalName),
		IsPublicCompany: optionalBool(profile.IsPublicCompany),
		CountryIso2:     optionalString("US"),
		RawPayload:      append([]byte(nil), record.RawPayload...),
		PayloadHash:     payloadHash,
		Metadata:        []byte(`{}`),
	}, nil
}

func resolveUSSECEDGARBaseURL(baseURL string) string {
	if trimmed := strings.TrimSpace(baseURL); trimmed != "" {
		return trimmed
	}
	return secedgar.DefaultDownloadURL
}
