package countrydata

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
	"github.com/pulsarpoint/corpscout/countrydata/united_states/irseobmf"
	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

type USIRSEoBmfTxPool interface {
	db.DBTX
	Begin(context.Context) (pgx.Tx, error)
}

type USIRSEoBmfDBStore struct {
	pool                USIRSEoBmfTxPool
	latestSourceID      uuid.UUID
	latestDownloadRunID *uuid.UUID
}

func NewUSIRSEoBmfDBStore(pool USIRSEoBmfTxPool) *USIRSEoBmfDBStore {
	return &USIRSEoBmfDBStore{pool: pool}
}

func (s *USIRSEoBmfDBStore) SaveDownload(ctx context.Context, metadata countryimport.DownloadMetadata) error {
	var sourceID uuid.UUID
	var downloadRunID uuid.UUID
	if err := s.withTx(ctx, func(tx pgx.Tx) error {
		q := db.New(tx)

		id, err := upsertUSIRSEoBmfSource(ctx, q, metadata.BaseURL)
		if err != nil {
			return err
		}
		sourceID = id

		runID, err := q.RecordUSIRSEoBmfDownloadRun(ctx, db.RecordUSIRSEoBmfDownloadRunParams{
			SourceID:             sourceID,
			Status:               "succeeded",
			BaseUrl:              resolveUSIRSEoBmfBaseURL(metadata.BaseURL),
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
			return errors.Wrap(err, "record IRS EO BMF download run")
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

func (s *USIRSEoBmfDBStore) SaveProcess(ctx context.Context, metadata countryimport.ProcessMetadata) error {
	if s == nil || s.latestDownloadRunID == nil {
		return nil
	}
	downloadRunID := *s.latestDownloadRunID
	return s.withTx(ctx, func(tx pgx.Tx) error {
		if err := db.New(tx).UpdateUSIRSEoBmfDownloadProcessStats(ctx, db.UpdateUSIRSEoBmfDownloadProcessStatsParams{
			RecordsProcessed: metadata.RecordsProcessed,
			RecordsStored:    metadata.RecordsStored,
			DecodeErrors:     metadata.DecodeErrors,
			ChunksProcessed:  metadata.ChunksProcessed,
			FinishedAt:       optionalTimestamp(metadata.FinishedAt),
			ID:               downloadRunID,
		}); err != nil {
			return errors.Wrap(err, "update IRS EO BMF process stats")
		}
		return nil
	})
}

func (s *USIRSEoBmfDBStore) StoreCompanies(ctx context.Context, records []irseobmf.IrsEoBmfRecord) (countryimport.StoreResult, error) {
	result := countryimport.StoreResult{RecordsReceived: int64(len(records))}
	if len(records) == 0 {
		return result, nil
	}

	var recordsStored int64
	if err := s.withTx(ctx, func(tx pgx.Tx) error {
		q := db.New(tx)
		sourceID := s.latestSourceID
		if sourceID == uuid.Nil {
			id, err := upsertUSIRSEoBmfSource(ctx, q, "")
			if err != nil {
				return err
			}
			sourceID = id
		}

		downloadRunID := s.latestDownloadRunID
		for _, record := range records {
			params, err := usIRSEoBmfRawRecordParams(sourceID, downloadRunID, record)
			if err != nil {
				return err
			}

			current, err := q.GetCurrentUSIRSEoBmfRawRecord(ctx, params.Ein)
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return errors.Wrap(err, "get current IRS EO BMF raw record")
			}
			if err == nil && current.PayloadHash != params.PayloadHash {
				if err := q.SupersedeCurrentUSIRSEoBmfRawRecord(ctx, db.SupersedeCurrentUSIRSEoBmfRawRecordParams{
					Ein:         params.Ein,
					PayloadHash: params.PayloadHash,
				}); err != nil {
					return errors.Wrap(err, "supersede current IRS EO BMF raw record")
				}
			}

			if _, err := q.UpsertUSIRSEoBmfRawRecord(ctx, params); err != nil {
				return errors.Wrap(err, "upsert IRS EO BMF raw record")
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

func (s *USIRSEoBmfDBStore) withTx(ctx context.Context, fn func(pgx.Tx) error) error {
	if s == nil || s.pool == nil {
		return countryimport.WrapSourceError(
			countryimport.ErrorKindState,
			irseobmf.SourceSlug,
			"",
			"",
			0,
			errors.New("IRS EO BMF database pool not available"),
		)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return countryimport.WrapSourceError(
			countryimport.ErrorKindState,
			irseobmf.SourceSlug,
			"",
			"",
			0,
			errors.Wrap(err, "begin IRS EO BMF transaction"),
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
			irseobmf.SourceSlug,
			"",
			"",
			0,
			err,
		)
	}
	if err := tx.Commit(ctx); err != nil {
		return countryimport.WrapSourceError(
			countryimport.ErrorKindState,
			irseobmf.SourceSlug,
			"",
			"",
			0,
			errors.Wrap(err, "commit IRS EO BMF transaction"),
		)
	}
	return nil
}

func upsertUSIRSEoBmfSource(ctx context.Context, q *db.Queries, baseURL string) (uuid.UUID, error) {
	sourceID, err := q.UpsertUSIRSEoBmfSource(ctx, db.UpsertUSIRSEoBmfSourceParams{
		SourceSlug:          irseobmf.SourceSlug,
		SourceName:          irseobmf.SourceName,
		BaseUrl:             resolveUSIRSEoBmfBaseURL(baseURL),
		SupportsIncremental: false,
		Metadata: jsonObject(map[string]any{
			"schema":               "countrydata_united_states_irs_eo_bmf",
			"raw_table":            "countrydata_united_states_irs_eo_bmf.raw_records",
			"supports_incremental": false,
		}),
	})
	if err != nil {
		return uuid.Nil, errors.Wrap(err, "upsert IRS EO BMF source")
	}
	return sourceID, nil
}

func usIRSEoBmfRawRecordParams(sourceID uuid.UUID, downloadRunID *uuid.UUID, record irseobmf.IrsEoBmfRecord) (db.UpsertUSIRSEoBmfRawRecordParams, error) {
	profile := record.ToProfile()
	ein := strings.TrimSpace(profile.EIN)
	if ein == "" {
		return db.UpsertUSIRSEoBmfRawRecordParams{}, countryimport.WrapSourceError(
			countryimport.ErrorKindLineDecode,
			irseobmf.SourceSlug,
			"",
			"",
			0,
			errors.New("missing IRS EO BMF EIN"),
		)
	}
	if len(record.RawPayload) == 0 {
		return db.UpsertUSIRSEoBmfRawRecordParams{}, countryimport.WrapSourceError(
			countryimport.ErrorKindLineDecode,
			irseobmf.SourceSlug,
			"",
			"",
			0,
			errors.New("missing IRS EO BMF raw payload"),
		)
	}
	if !isJSONObjectPayload(record.RawPayload) {
		return db.UpsertUSIRSEoBmfRawRecordParams{}, countryimport.WrapSourceError(
			countryimport.ErrorKindLineDecode,
			irseobmf.SourceSlug,
			"",
			"",
			0,
			errors.New("invalid IRS EO BMF raw payload JSON"),
		)
	}
	payloadHash := strings.TrimSpace(record.PayloadHash)
	if payloadHash == "" {
		return db.UpsertUSIRSEoBmfRawRecordParams{}, countryimport.WrapSourceError(
			countryimport.ErrorKindLineDecode,
			irseobmf.SourceSlug,
			"",
			"",
			0,
			errors.New("missing IRS EO BMF payload hash"),
		)
	}

	var sortName string
	if len(profile.AlternateNames) > 0 {
		sortName = profile.AlternateNames[0]
	}

	return db.UpsertUSIRSEoBmfRawRecordParams{
		SourceID:             sourceID,
		DownloadRunID:        optionalUUID(downloadRunID),
		SourceNativeID:       ein,
		Ein:                  ein,
		PrimaryID:            optionalString(profile.PrimaryID),
		LegalName:            optionalString(profile.LegalName),
		SortName:             optionalString(sortName),
		ExemptStatusCode:     optionalString(profile.ExemptStatusCode),
		IsExemptStatusActive: optionalBool(profile.IsExemptStatusActive),
		Subsection:           optionalString(profile.Subsection),
		OrganizationCode:     optionalString(profile.OrganizationCode),
		FoundationCode:       optionalString(profile.FoundationCode),
		NteeCode:             optionalString(profile.NTEECode),
		IrsRulingDate:        optionalString(profile.IRSRulingDate),
		TaxPeriod:            optionalString(profile.Financials.TaxPeriod),
		AssetAmount:          profile.Financials.AssetAmount,
		IncomeAmount:         profile.Financials.IncomeAmount,
		RevenueAmount:        profile.Financials.RevenueAmount,
		City:                 optionalString(profile.Address.City),
		StateCode:            optionalString(profile.Address.State),
		CountryIso2:          optionalString("US"),
		RawPayload:           append([]byte(nil), record.RawPayload...),
		PayloadHash:          payloadHash,
		Metadata:             []byte(`{}`),
	}, nil
}

func resolveUSIRSEoBmfBaseURL(baseURL string) string {
	if trimmed := strings.TrimSpace(baseURL); trimmed != "" {
		return trimmed
	}
	return "https://www.irs.gov/pub/irs-soi/"
}
