package countrydata

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
	"github.com/pulsarpoint/corpscout/countrydata/united_states/coloradoentities"
	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

type USColoradoEntitiesTxPool interface {
	db.DBTX
	Begin(context.Context) (pgx.Tx, error)
}

type USColoradoEntitiesDBStore struct {
	pool                USColoradoEntitiesTxPool
	latestSourceID      uuid.UUID
	latestDownloadRunID *uuid.UUID
}

func NewUSColoradoEntitiesDBStore(pool USColoradoEntitiesTxPool) *USColoradoEntitiesDBStore {
	return &USColoradoEntitiesDBStore{pool: pool}
}

func (s *USColoradoEntitiesDBStore) SaveDownload(ctx context.Context, metadata countryimport.DownloadMetadata) error {
	var sourceID uuid.UUID
	var downloadRunID uuid.UUID
	if err := s.withTx(ctx, func(tx pgx.Tx) error {
		q := db.New(tx)

		id, err := upsertUSColoradoEntitiesSource(ctx, q, metadata.BaseURL)
		if err != nil {
			return err
		}
		sourceID = id

		runID, err := q.RecordUSColoradoEntitiesDownloadRun(ctx, db.RecordUSColoradoEntitiesDownloadRunParams{
			SourceID:             sourceID,
			Status:               "succeeded",
			BaseUrl:              resolveUSColoradoEntitiesBaseURL(metadata.BaseURL),
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
			return errors.Wrap(err, "record Colorado entities download run")
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

func (s *USColoradoEntitiesDBStore) SaveProcess(ctx context.Context, metadata countryimport.ProcessMetadata) error {
	if s == nil || s.latestDownloadRunID == nil {
		return nil
	}
	downloadRunID := *s.latestDownloadRunID
	return s.withTx(ctx, func(tx pgx.Tx) error {
		if err := db.New(tx).UpdateUSColoradoEntitiesDownloadProcessStats(ctx, db.UpdateUSColoradoEntitiesDownloadProcessStatsParams{
			RecordsProcessed: metadata.RecordsProcessed,
			RecordsStored:    metadata.RecordsStored,
			DecodeErrors:     metadata.DecodeErrors,
			ChunksProcessed:  metadata.ChunksProcessed,
			FinishedAt:       optionalTimestamp(metadata.FinishedAt),
			ID:               downloadRunID,
		}); err != nil {
			return errors.Wrap(err, "update Colorado entities process stats")
		}
		return nil
	})
}

func (s *USColoradoEntitiesDBStore) StoreCompanies(ctx context.Context, records []coloradoentities.ColoradoEntityRecord) (countryimport.StoreResult, error) {
	result := countryimport.StoreResult{RecordsReceived: int64(len(records))}
	if len(records) == 0 {
		return result, nil
	}

	var recordsStored int64
	if err := s.withTx(ctx, func(tx pgx.Tx) error {
		q := db.New(tx)
		sourceID := s.latestSourceID
		if sourceID == uuid.Nil {
			id, err := upsertUSColoradoEntitiesSource(ctx, q, "")
			if err != nil {
				return err
			}
			sourceID = id
		}

		downloadRunID := s.latestDownloadRunID
		for _, record := range records {
			params, err := usColoradoEntitiesRawRecordParams(sourceID, downloadRunID, record)
			if err != nil {
				return err
			}

			current, err := q.GetCurrentUSColoradoEntitiesRawRecord(ctx, params.EntityID)
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return errors.Wrap(err, "get current Colorado entities raw record")
			}
			if err == nil && current.PayloadHash != params.PayloadHash {
				if err := q.SupersedeCurrentUSColoradoEntitiesRawRecord(ctx, db.SupersedeCurrentUSColoradoEntitiesRawRecordParams{
					EntityID:    params.EntityID,
					PayloadHash: params.PayloadHash,
				}); err != nil {
					return errors.Wrap(err, "supersede current Colorado entities raw record")
				}
			}

			if _, err := q.UpsertUSColoradoEntitiesRawRecord(ctx, params); err != nil {
				return errors.Wrap(err, "upsert Colorado entities raw record")
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

func (s *USColoradoEntitiesDBStore) withTx(ctx context.Context, fn func(pgx.Tx) error) error {
	if s == nil || s.pool == nil {
		return countryimport.WrapSourceError(
			countryimport.ErrorKindState,
			coloradoentities.SourceSlug,
			"",
			"",
			0,
			errors.New("Colorado entities database pool not available"),
		)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return countryimport.WrapSourceError(
			countryimport.ErrorKindState,
			coloradoentities.SourceSlug,
			"",
			"",
			0,
			errors.Wrap(err, "begin Colorado entities transaction"),
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
			coloradoentities.SourceSlug,
			"",
			"",
			0,
			err,
		)
	}
	if err := tx.Commit(ctx); err != nil {
		return countryimport.WrapSourceError(
			countryimport.ErrorKindState,
			coloradoentities.SourceSlug,
			"",
			"",
			0,
			errors.Wrap(err, "commit Colorado entities transaction"),
		)
	}
	return nil
}

func upsertUSColoradoEntitiesSource(ctx context.Context, q *db.Queries, baseURL string) (uuid.UUID, error) {
	sourceID, err := q.UpsertUSColoradoEntitiesSource(ctx, db.UpsertUSColoradoEntitiesSourceParams{
		SourceSlug:          coloradoentities.SourceSlug,
		SourceName:          coloradoentities.SourceName,
		BaseUrl:             resolveUSColoradoEntitiesBaseURL(baseURL),
		SupportsIncremental: false,
		Metadata: jsonObject(map[string]any{
			"schema":               "countrydata_united_states_colorado_entities",
			"raw_table":            "countrydata_united_states_colorado_entities.raw_records",
			"supports_incremental": false,
		}),
	})
	if err != nil {
		return uuid.Nil, errors.Wrap(err, "upsert Colorado entities source")
	}
	return sourceID, nil
}

func usColoradoEntitiesRawRecordParams(sourceID uuid.UUID, downloadRunID *uuid.UUID, record coloradoentities.ColoradoEntityRecord) (db.UpsertUSColoradoEntitiesRawRecordParams, error) {
	entityID := strings.TrimSpace(record.EntityID)
	if entityID == "" {
		return db.UpsertUSColoradoEntitiesRawRecordParams{}, countryimport.WrapSourceError(
			countryimport.ErrorKindLineDecode,
			coloradoentities.SourceSlug,
			"",
			"",
			0,
			errors.New("missing Colorado entities entity ID"),
		)
	}
	if len(record.RawPayload) == 0 {
		return db.UpsertUSColoradoEntitiesRawRecordParams{}, countryimport.WrapSourceError(
			countryimport.ErrorKindLineDecode,
			coloradoentities.SourceSlug,
			"",
			"",
			0,
			errors.New("missing Colorado entities raw payload"),
		)
	}
	if !isJSONObjectPayload(record.RawPayload) {
		return db.UpsertUSColoradoEntitiesRawRecordParams{}, countryimport.WrapSourceError(
			countryimport.ErrorKindLineDecode,
			coloradoentities.SourceSlug,
			"",
			"",
			0,
			errors.New("invalid Colorado entities raw payload JSON"),
		)
	}
	payloadHash := strings.TrimSpace(record.PayloadHash)
	if payloadHash == "" {
		return db.UpsertUSColoradoEntitiesRawRecordParams{}, countryimport.WrapSourceError(
			countryimport.ErrorKindLineDecode,
			coloradoentities.SourceSlug,
			"",
			"",
			0,
			errors.New("missing Colorado entities payload hash"),
		)
	}

	profile := record.ToProfile()
	formationDate, err := optionalDate(profile.FormationDate)
	if err != nil {
		return db.UpsertUSColoradoEntitiesRawRecordParams{}, countryimport.WrapSourceError(
			countryimport.ErrorKindLineDecode,
			coloradoentities.SourceSlug,
			"",
			"",
			0,
			errors.Wrap(err, "parse Colorado entities formation date"),
		)
	}

	var alternateName string
	if len(profile.AlternateNames) > 0 {
		alternateName = profile.AlternateNames[0]
	}

	return db.UpsertUSColoradoEntitiesRawRecordParams{
		SourceID:                sourceID,
		DownloadRunID:           optionalUUID(downloadRunID),
		SourceNativeID:          entityID,
		EntityID:                entityID,
		PrimaryID:               optionalString(profile.PrimaryID),
		LegalName:               optionalString(profile.LegalName),
		AlternateName:           optionalString(alternateName),
		Status:                  optionalString(profile.StatusRaw),
		IsActive:                optionalBool(profile.IsActive),
		LegalForm:               optionalString(profile.LegalForm),
		LegalFormCode:           optionalString(profile.LegalFormCode),
		JurisdictionOfFormation: optionalString(profile.JurisdictionOfFormation),
		IsForeign:               optionalBool(profile.IsForeign),
		FormationDate:           formationDate,
		CountryIso2:             optionalString("US"),
		RawPayload:              append([]byte(nil), record.RawPayload...),
		PayloadHash:             payloadHash,
		Metadata:                []byte(`{}`),
	}, nil
}

func resolveUSColoradoEntitiesBaseURL(baseURL string) string {
	if trimmed := strings.TrimSpace(baseURL); trimmed != "" {
		return trimmed
	}
	return coloradoentities.DefaultBaseURL
}
