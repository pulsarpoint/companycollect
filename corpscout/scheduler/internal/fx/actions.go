package fx

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

const (
	SyncStatusSucceeded = "succeeded"
	SyncStatusSkipped   = "skipped"
	SyncStatusFailed    = "failed"

	sourceFileStatusProcessed = "processed"
)

type Actions struct {
	pool             *pgxpool.Pool
	httpClient       *http.Client
	defaultSourceURL string
}

func NewActions(pool *pgxpool.Pool, httpClient *http.Client, defaultSourceURL string) *Actions {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Actions{
		pool:             pool,
		httpClient:       httpClient,
		defaultSourceURL: strings.TrimSpace(defaultSourceURL),
	}
}

type SyncExchangeRatesActivityInput struct {
	TemporalWorkflowID string `json:"temporal_workflow_id"`
	Provider           string `json:"provider"`
	SourceURL          string `json:"source_url"`
	Trigger            string `json:"trigger"`
	ForceReprocess     bool   `json:"force_reprocess"`
}

type SyncExchangeRatesActivityResult struct {
	Status             string `json:"status"`
	SyncRunID          string `json:"sync_run_id"`
	SourceFileID       string `json:"source_file_id"`
	SheetID            string `json:"sheet_id"`
	ContentSHA256      string `json:"content_sha256"`
	RateDate           string `json:"rate_date"`
	CurrenciesSeen     int32  `json:"currencies_seen"`
	CurrenciesImported int32  `json:"currencies_imported"`
	Message            string `json:"message"`
}

func (a *Actions) SyncExchangeRatesActivity(ctx context.Context, input SyncExchangeRatesActivityInput) (SyncExchangeRatesActivityResult, error) {
	input = a.normalizeInput(input)
	if err := validateSyncInput(input); err != nil {
		return SyncExchangeRatesActivityResult{}, err
	}
	if a == nil || a.pool == nil {
		return SyncExchangeRatesActivityResult{}, errors.New("exchange rate database is not available")
	}

	queries := db.New(a.pool)
	existingRun, err := queries.GetExchangeRateSyncRunByWorkflowID(ctx, input.TemporalWorkflowID)
	if err == nil && terminalSyncRunStatus(existingRun.Status) {
		slog.DebugContext(ctx, "returning terminal exchange rate sync run",
			"sync_run_id", existingRun.ID.String(),
			"temporal_workflow_id", input.TemporalWorkflowID,
			"status", existingRun.Status,
		)
		return syncResultFromRun(existingRun, existingRun.SourceFileID, existingRun.SheetID, syncRunMessage(existingRun.Status)), nil
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return SyncExchangeRatesActivityResult{}, errors.Wrap(err, "get exchange rate sync run by workflow id")
	}

	run, err := queries.BeginExchangeRateSyncRun(ctx, db.BeginExchangeRateSyncRunParams{
		TemporalWorkflowID: input.TemporalWorkflowID,
		Provider:           input.Provider,
		SourceUrl:          input.SourceURL,
		Metadata:           mustJSON(map[string]any{"trigger": input.Trigger, "force_reprocess": input.ForceReprocess}),
	})
	if err != nil {
		return SyncExchangeRatesActivityResult{}, errors.Wrap(err, "begin exchange rate sync run")
	}
	result := SyncExchangeRatesActivityResult{Status: "running", SyncRunID: run.ID.String()}

	slog.DebugContext(ctx, "downloading exchange rate source",
		"sync_run_id", run.ID.String(),
		"provider", input.Provider,
		"source_url", input.SourceURL,
		"force_reprocess", input.ForceReprocess,
	)
	downloaded, err := DownloadRateFile(ctx, a.httpClient, input.SourceURL, DefaultMaxSourceFileBytes)
	if err != nil {
		_ = a.finishRunFailed(ctx, run.ID, nil, nil, "", err)
		return result, errors.Wrap(err, "download exchange rate source")
	}
	result.ContentSHA256 = downloaded.ContentSHA256

	sheet, err := ParseECBDailyRateSheet(downloaded)
	if err != nil {
		_ = a.finishRunFailed(ctx, run.ID, nil, nil, downloaded.ContentSHA256, err)
		return result, errors.Wrap(err, "parse exchange rate source")
	}
	result.RateDate = sheet.RateDate
	result.CurrenciesSeen = int32(len(sheet.Rates))

	if !input.ForceReprocess {
		existingFile, err := queries.GetProcessedExchangeRateSourceFileByHash(ctx, db.GetProcessedExchangeRateSourceFileByHashParams{
			Provider:      input.Provider,
			SourceUrl:     input.SourceURL,
			ContentSha256: downloaded.ContentSHA256,
		})
		if err == nil {
			message := "source file hash already processed"
			finished, finishErr := queries.FinishExchangeRateSyncRun(ctx, db.FinishExchangeRateSyncRunParams{
				SourceFileID:       uuidToPgtype(existingFile.ID),
				SheetID:            pgtype.UUID{},
				Status:             SyncStatusSkipped,
				RateDate:           dateToPgtype(sheet.RateDate),
				ContentSha256:      &downloaded.ContentSHA256,
				CurrenciesSeen:     int32(len(sheet.Rates)),
				CurrenciesImported: 0,
				Error:              "",
				Metadata:           mustJSON(map[string]any{"message": message}),
				ID:                 run.ID,
			})
			if finishErr != nil {
				return result, errors.Wrap(finishErr, "finish skipped exchange rate sync run")
			}
			return syncResultFromRun(finished, uuidToPgtype(existingFile.ID), pgtype.UUID{}, message), nil
		}
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			_ = a.finishRunFailed(ctx, run.ID, nil, nil, downloaded.ContentSHA256, err)
			return result, errors.Wrap(err, "check processed exchange rate source file hash")
		}
	}

	sourceFile, err := queries.UpsertDownloadedExchangeRateSourceFile(ctx, db.UpsertDownloadedExchangeRateSourceFileParams{
		Provider:           input.Provider,
		SourceUrl:          input.SourceURL,
		RateDate:           dateToPgtype(sheet.RateDate),
		ContentSha256:      downloaded.ContentSHA256,
		ContentLengthBytes: downloaded.ContentLengthBytes,
		ContentType:        optionalString(downloaded.ContentType),
		Etag:               optionalString(downloaded.ETag),
		LastModified:       optionalString(downloaded.LastModified),
		Metadata:           mustJSON(map[string]any{"trigger": input.Trigger}),
	})
	if err != nil {
		_ = a.finishRunFailed(ctx, run.ID, nil, nil, downloaded.ContentSHA256, err)
		return result, errors.Wrap(err, "register downloaded exchange rate source file")
	}
	result.SourceFileID = sourceFile.ID.String()

	sourceFileAlreadyProcessed := sourceFile.Status == sourceFileStatusProcessed
	if sourceFileAlreadyProcessed && !input.ForceReprocess {
		message := "source file hash already processed"
		finished, finishErr := queries.FinishExchangeRateSyncRun(ctx, db.FinishExchangeRateSyncRunParams{
			SourceFileID:       uuidToPgtype(sourceFile.ID),
			SheetID:            pgtype.UUID{},
			Status:             SyncStatusSkipped,
			RateDate:           dateToPgtype(sheet.RateDate),
			ContentSha256:      &downloaded.ContentSHA256,
			CurrenciesSeen:     int32(len(sheet.Rates)),
			CurrenciesImported: 0,
			Error:              "",
			Metadata:           mustJSON(map[string]any{"message": message}),
			ID:                 run.ID,
		})
		if finishErr != nil {
			return result, errors.Wrap(finishErr, "finish skipped exchange rate sync run")
		}
		return syncResultFromRun(finished, uuidToPgtype(sourceFile.ID), pgtype.UUID{}, message), nil
	}

	if !sourceFileAlreadyProcessed {
		if _, err := queries.MarkExchangeRateSourceFileProcessing(ctx, sourceFile.ID); err != nil {
			_ = a.finishRunFailed(ctx, run.ID, &sourceFile.ID, nil, downloaded.ContentSHA256, err)
			return result, errors.Wrap(err, "mark exchange rate source file processing")
		}
	}

	imported, sheetID, err := a.importRateSheet(ctx, run.ID, sourceFile.ID, downloaded, sheet)
	if err != nil {
		_ = a.finishRunFailed(ctx, run.ID, &sourceFile.ID, nil, downloaded.ContentSHA256, err)
		return result, errors.Wrap(err, "import exchange rate sheet")
	}

	result.Status = SyncStatusSucceeded
	result.SheetID = sheetID.String()
	result.CurrenciesImported = imported
	result.Message = "exchange rates imported"
	return result, nil
}

func (a *Actions) importRateSheet(
	ctx context.Context,
	runID uuid.UUID,
	sourceFileID uuid.UUID,
	downloaded DownloadedRateFile,
	sheet RateSheet,
) (int32, uuid.UUID, error) {
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return 0, uuid.UUID{}, errors.Wrap(err, "begin exchange rate import transaction")
	}
	defer func() { _ = tx.Rollback(ctx) }()

	queries := db.New(tx)
	upsertedSheet, err := queries.UpsertExchangeRateSheet(ctx, db.UpsertExchangeRateSheetParams{
		Provider:      sheet.Provider,
		RateDate:      dateToPgtype(sheet.RateDate),
		BaseCurrency:  sheet.BaseCurrency,
		SourceFileID:  sourceFileID,
		ContentSha256: downloaded.ContentSHA256,
		Metadata:      mustJSON(map[string]any{"source_url": downloaded.SourceURL}),
	})
	if err != nil {
		return 0, uuid.UUID{}, errors.Wrap(err, "upsert exchange rate sheet")
	}

	currencies := sheet.Currencies()
	var imported int32
	for _, currency := range currencies {
		if _, err := queries.UpsertExchangeRate(ctx, db.UpsertExchangeRateParams{
			SheetID:     upsertedSheet.ID,
			Currency:    currency,
			RatePerBase: sheet.Rates[currency],
			Metadata:    mustJSON(map[string]any{"provider": sheet.Provider}),
		}); err != nil {
			return 0, uuid.UUID{}, errors.Wrapf(err, "upsert exchange rate %s", currency)
		}
		imported++
	}
	if _, err := queries.DeleteExchangeRatesNotInCurrencies(ctx, db.DeleteExchangeRatesNotInCurrenciesParams{
		SheetID:    upsertedSheet.ID,
		Currencies: currencies,
	}); err != nil {
		return 0, uuid.UUID{}, errors.Wrap(err, "delete exchange rates missing from source")
	}
	if _, err := queries.MarkExchangeRateSourceFileProcessed(ctx, sourceFileID); err != nil {
		return 0, uuid.UUID{}, errors.Wrap(err, "mark exchange rate source file processed")
	}
	finished, err := queries.FinishExchangeRateSyncRun(ctx, db.FinishExchangeRateSyncRunParams{
		SourceFileID:       uuidToPgtype(sourceFileID),
		SheetID:            uuidToPgtype(upsertedSheet.ID),
		Status:             SyncStatusSucceeded,
		RateDate:           dateToPgtype(sheet.RateDate),
		ContentSha256:      &downloaded.ContentSHA256,
		CurrenciesSeen:     int32(len(sheet.Rates)),
		CurrenciesImported: imported,
		Error:              "",
		Metadata:           mustJSON(map[string]any{"message": "exchange rates imported"}),
		ID:                 runID,
	})
	if err != nil {
		return 0, uuid.UUID{}, errors.Wrap(err, "finish exchange rate sync run")
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, uuid.UUID{}, errors.Wrap(err, "commit exchange rate import transaction")
	}
	return finished.CurrenciesImported, upsertedSheet.ID, nil
}

func (a *Actions) normalizeInput(input SyncExchangeRatesActivityInput) SyncExchangeRatesActivityInput {
	input.Provider = strings.ToLower(strings.TrimSpace(input.Provider))
	input.SourceURL = strings.TrimSpace(input.SourceURL)
	input.Trigger = strings.TrimSpace(input.Trigger)
	if input.Provider == "" {
		input.Provider = DefaultProvider
	}
	if input.SourceURL == "" && a != nil {
		input.SourceURL = a.defaultSourceURL
	}
	if input.Trigger == "" {
		input.Trigger = "manual"
	}
	return input
}

func validateSyncInput(input SyncExchangeRatesActivityInput) error {
	if strings.TrimSpace(input.TemporalWorkflowID) == "" {
		return errors.New("temporal workflow id is required")
	}
	if input.Provider != DefaultProvider {
		return errors.New("provider must be ecb")
	}
	if strings.TrimSpace(input.SourceURL) == "" {
		return errors.New("exchange rate source url is required")
	}
	return nil
}

func terminalSyncRunStatus(status string) bool {
	return status == SyncStatusSucceeded || status == SyncStatusSkipped || status == SyncStatusFailed
}

func syncRunMessage(status string) string {
	switch status {
	case SyncStatusSkipped:
		return "source file hash already processed"
	case SyncStatusSucceeded:
		return "exchange rates imported"
	case SyncStatusFailed:
		return "exchange rate sync failed"
	default:
		return ""
	}
}

func (a *Actions) finishRunFailed(
	ctx context.Context,
	runID uuid.UUID,
	sourceFileID *uuid.UUID,
	sheetID *uuid.UUID,
	contentSHA256 string,
	cause error,
) error {
	queries := db.New(a.pool)
	var sourceFileParam pgtype.UUID
	if sourceFileID != nil {
		sourceFileParam = uuidToPgtype(*sourceFileID)
		_, _ = queries.MarkExchangeRateSourceFileFailed(ctx, db.MarkExchangeRateSourceFileFailedParams{
			Error: optionalString(safeError(cause)),
			ID:    *sourceFileID,
		})
	}
	var sheetParam pgtype.UUID
	if sheetID != nil {
		sheetParam = uuidToPgtype(*sheetID)
	}
	var hashParam *string
	if contentSHA256 != "" {
		hashParam = &contentSHA256
	}
	_, err := queries.FinishExchangeRateSyncRun(ctx, db.FinishExchangeRateSyncRunParams{
		SourceFileID:       sourceFileParam,
		SheetID:            sheetParam,
		Status:             SyncStatusFailed,
		RateDate:           pgtype.Date{},
		ContentSha256:      hashParam,
		CurrenciesSeen:     0,
		CurrenciesImported: 0,
		Error:              safeError(cause),
		Metadata:           mustJSON(map[string]any{"message": "exchange rate sync failed"}),
		ID:                 runID,
	})
	return err
}

func syncResultFromRun(row db.ExchangeRateSyncRun, sourceFileID pgtype.UUID, sheetID pgtype.UUID, message string) SyncExchangeRatesActivityResult {
	result := SyncExchangeRatesActivityResult{
		Status:             row.Status,
		SyncRunID:          row.ID.String(),
		CurrenciesSeen:     row.CurrenciesSeen,
		CurrenciesImported: row.CurrenciesImported,
		Message:            message,
	}
	if row.ContentSha256 != nil {
		result.ContentSHA256 = *row.ContentSha256
	}
	if row.RateDate.Valid {
		result.RateDate = row.RateDate.Time.Format("2006-01-02")
	}
	if sourceFileID.Valid {
		result.SourceFileID = uuid.UUID(sourceFileID.Bytes).String()
	}
	if sheetID.Valid {
		result.SheetID = uuid.UUID(sheetID.Bytes).String()
	}
	return result
}

func uuidToPgtype(id uuid.UUID) pgtype.UUID {
	return pgtype.UUID{Bytes: id, Valid: true}
}

func dateToPgtype(value string) pgtype.Date {
	parsed, err := time.Parse("2006-01-02", value)
	if err != nil {
		return pgtype.Date{}
	}
	return pgtype.Date{Time: parsed, Valid: true}
}

func optionalString(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}

func mustJSON(value map[string]any) []byte {
	body, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return body
}

func safeError(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
