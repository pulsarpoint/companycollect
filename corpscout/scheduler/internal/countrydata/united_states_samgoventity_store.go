package countrydata

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
	"github.com/pulsarpoint/corpscout/countrydata/united_states/samgoventity"
	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

type USSamGovEntityTxPool interface {
	db.DBTX
	Begin(context.Context) (pgx.Tx, error)
}

type USSamGovEntityDBStore struct {
	pool                USSamGovEntityTxPool
	latestSourceID      uuid.UUID
	latestDownloadRunID *uuid.UUID
}

func NewUSSamGovEntityDBStore(pool USSamGovEntityTxPool) *USSamGovEntityDBStore {
	return &USSamGovEntityDBStore{pool: pool}
}

func (s *USSamGovEntityDBStore) SaveDownload(ctx context.Context, metadata countryimport.DownloadMetadata) error {
	var sourceID uuid.UUID
	var downloadRunID uuid.UUID
	if err := s.withTx(ctx, func(tx pgx.Tx) error {
		q := db.New(tx)

		id, err := upsertUSSamGovEntitySource(ctx, q, metadata.BaseURL)
		if err != nil {
			return err
		}
		sourceID = id

		runID, err := q.RecordUSSamGovEntityDownloadRun(ctx, db.RecordUSSamGovEntityDownloadRunParams{
			SourceID:             sourceID,
			Status:               "succeeded",
			BaseUrl:              resolveUSSamGovEntityBaseURL(metadata.BaseURL),
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
			return errors.Wrap(err, "record SAM.gov entity download run")
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

func (s *USSamGovEntityDBStore) SaveProcess(ctx context.Context, metadata countryimport.ProcessMetadata) error {
	if s == nil || s.latestDownloadRunID == nil {
		return nil
	}
	downloadRunID := *s.latestDownloadRunID
	return s.withTx(ctx, func(tx pgx.Tx) error {
		if err := db.New(tx).UpdateUSSamGovEntityDownloadProcessStats(ctx, db.UpdateUSSamGovEntityDownloadProcessStatsParams{
			RecordsProcessed: metadata.RecordsProcessed,
			RecordsStored:    metadata.RecordsStored,
			DecodeErrors:     metadata.DecodeErrors,
			ChunksProcessed:  metadata.ChunksProcessed,
			FinishedAt:       optionalTimestamp(metadata.FinishedAt),
			ID:               downloadRunID,
		}); err != nil {
			return errors.Wrap(err, "update SAM.gov entity process stats")
		}
		return nil
	})
}

func (s *USSamGovEntityDBStore) StoreCompanies(ctx context.Context, records []samgoventity.SamEntityRecord) (countryimport.StoreResult, error) {
	result := countryimport.StoreResult{RecordsReceived: int64(len(records))}
	if len(records) == 0 {
		return result, nil
	}

	var recordsStored int64
	if err := s.withTx(ctx, func(tx pgx.Tx) error {
		q := db.New(tx)
		sourceID := s.latestSourceID
		if sourceID == uuid.Nil {
			id, err := upsertUSSamGovEntitySource(ctx, q, "")
			if err != nil {
				return err
			}
			sourceID = id
		}

		downloadRunID := s.latestDownloadRunID
		for _, record := range records {
			params, err := usSamGovEntityRawRecordParams(sourceID, downloadRunID, record)
			if err != nil {
				return err
			}

			current, err := q.GetCurrentUSSamGovEntityRawRecord(ctx, params.UeiSam)
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return errors.Wrap(err, "get current SAM.gov entity raw record")
			}
			if err == nil && current.PayloadHash != params.PayloadHash {
				if err := q.SupersedeCurrentUSSamGovEntityRawRecord(ctx, db.SupersedeCurrentUSSamGovEntityRawRecordParams{
					UeiSam:      params.UeiSam,
					PayloadHash: params.PayloadHash,
				}); err != nil {
					return errors.Wrap(err, "supersede current SAM.gov entity raw record")
				}
			}

			if _, err := q.UpsertUSSamGovEntityRawRecord(ctx, params); err != nil {
				return errors.Wrap(err, "upsert SAM.gov entity raw record")
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

func (s *USSamGovEntityDBStore) withTx(ctx context.Context, fn func(pgx.Tx) error) error {
	if s == nil || s.pool == nil {
		return countryimport.WrapSourceError(
			countryimport.ErrorKindState,
			samgoventity.SourceSlug,
			"",
			"",
			0,
			errors.New("SAM.gov entity database pool not available"),
		)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return countryimport.WrapSourceError(
			countryimport.ErrorKindState,
			samgoventity.SourceSlug,
			"",
			"",
			0,
			errors.Wrap(err, "begin SAM.gov entity transaction"),
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
			samgoventity.SourceSlug,
			"",
			"",
			0,
			err,
		)
	}
	if err := tx.Commit(ctx); err != nil {
		return countryimport.WrapSourceError(
			countryimport.ErrorKindState,
			samgoventity.SourceSlug,
			"",
			"",
			0,
			errors.Wrap(err, "commit SAM.gov entity transaction"),
		)
	}
	return nil
}

func upsertUSSamGovEntitySource(ctx context.Context, q *db.Queries, baseURL string) (uuid.UUID, error) {
	sourceID, err := q.UpsertUSSamGovEntitySource(ctx, db.UpsertUSSamGovEntitySourceParams{
		SourceSlug:          samgoventity.SourceSlug,
		SourceName:          samgoventity.SourceName,
		BaseUrl:             resolveUSSamGovEntityBaseURL(baseURL),
		SupportsIncremental: false,
		Metadata: jsonObject(map[string]any{
			"schema":               "countrydata_united_states_sam_gov_entity",
			"raw_table":            "countrydata_united_states_sam_gov_entity.raw_records",
			"supports_incremental": false,
		}),
	})
	if err != nil {
		return uuid.Nil, errors.Wrap(err, "upsert SAM.gov entity source")
	}
	return sourceID, nil
}

func usSamGovEntityRawRecordParams(sourceID uuid.UUID, downloadRunID *uuid.UUID, record samgoventity.SamEntityRecord) (db.UpsertUSSamGovEntityRawRecordParams, error) {
	profile := record.ToProfile()
	uei := strings.TrimSpace(profile.UeiSAM)
	if uei == "" {
		return db.UpsertUSSamGovEntityRawRecordParams{}, countryimport.WrapSourceError(
			countryimport.ErrorKindLineDecode,
			samgoventity.SourceSlug,
			"",
			"",
			0,
			errors.New("missing SAM.gov entity ueiSAM"),
		)
	}
	if len(record.RawPayload) == 0 {
		return db.UpsertUSSamGovEntityRawRecordParams{}, countryimport.WrapSourceError(
			countryimport.ErrorKindLineDecode,
			samgoventity.SourceSlug,
			"",
			"",
			0,
			errors.New("missing SAM.gov entity raw payload"),
		)
	}
	if !isJSONObjectPayload(record.RawPayload) {
		return db.UpsertUSSamGovEntityRawRecordParams{}, countryimport.WrapSourceError(
			countryimport.ErrorKindLineDecode,
			samgoventity.SourceSlug,
			"",
			"",
			0,
			errors.New("invalid SAM.gov entity raw payload JSON"),
		)
	}
	payloadHash := strings.TrimSpace(record.PayloadHash)
	if payloadHash == "" {
		return db.UpsertUSSamGovEntityRawRecordParams{}, countryimport.WrapSourceError(
			countryimport.ErrorKindLineDecode,
			samgoventity.SourceSlug,
			"",
			"",
			0,
			errors.New("missing SAM.gov entity payload hash"),
		)
	}

	registrationDate, err := optionalDate(profile.RegistrationDate)
	if err != nil {
		return db.UpsertUSSamGovEntityRawRecordParams{}, countryimport.WrapSourceError(
			countryimport.ErrorKindLineDecode,
			samgoventity.SourceSlug,
			"",
			"",
			0,
			errors.Wrap(err, "parse SAM.gov entity registration date"),
		)
	}
	expirationDate, err := optionalDate(profile.RegistrationExpirationDate)
	if err != nil {
		return db.UpsertUSSamGovEntityRawRecordParams{}, countryimport.WrapSourceError(
			countryimport.ErrorKindLineDecode,
			samgoventity.SourceSlug,
			"",
			"",
			0,
			errors.Wrap(err, "parse SAM.gov entity registration expiration date"),
		)
	}
	sourceUpdatedAt, err := optionalPRHTimestamp(record.EntityRegistration.LastUpdateDate)
	if err != nil {
		return db.UpsertUSSamGovEntityRawRecordParams{}, countryimport.WrapSourceError(
			countryimport.ErrorKindLineDecode,
			samgoventity.SourceSlug,
			"",
			"",
			0,
			errors.Wrap(err, "parse SAM.gov entity last update date"),
		)
	}

	var alternateName string
	if len(profile.AlternateNames) > 0 {
		alternateName = profile.AlternateNames[0]
	}

	return db.UpsertUSSamGovEntityRawRecordParams{
		SourceID:                   sourceID,
		DownloadRunID:              optionalUUID(downloadRunID),
		SourceNativeID:             uei,
		UeiSam:                     uei,
		CageCode:                   optionalString(profile.CageCode),
		PrimaryID:                  optionalString(profile.PrimaryID),
		LegalName:                  optionalString(profile.LegalName),
		AlternateName:              optionalString(alternateName),
		SamRegistrationStatus:      optionalString(profile.SamRegistrationStatus),
		IsSamActive:                optionalBool(profile.IsSamActive),
		RegistrationDate:           registrationDate,
		RegistrationExpirationDate: expirationDate,
		EntityStructure:            optionalString(profile.EntityStructure),
		ProfitStructure:            optionalString(profile.ProfitStructure),
		StateOfIncorporation:       optionalString(profile.StateOfIncorporation),
		CountryOfIncorporation:     optionalString(profile.CountryOfIncorporation),
		Website:                    optionalString(profile.EntityURL),
		PrimaryNaics:               optionalString(profile.PrimaryNaics),
		City:                       optionalString(profile.PhysicalAddress.City),
		StateCode:                  optionalString(profile.PhysicalAddress.State),
		CountryIso2:                optionalString("US"),
		SourceUpdatedAt:            sourceUpdatedAt,
		RawPayload:                 append([]byte(nil), record.RawPayload...),
		PayloadHash:                payloadHash,
		Metadata:                   []byte(`{}`),
	}, nil
}

func resolveUSSamGovEntityBaseURL(baseURL string) string {
	if trimmed := strings.TrimSpace(baseURL); trimmed != "" {
		return trimmed
	}
	return samgoventity.DefaultBaseURL
}
