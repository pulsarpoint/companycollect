package nace

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	domainnace "github.com/pulsarpoint/corpscout/scheduler/internal/nacetaxonomy"
	naceworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/temporal/workflow/nace"
)

const (
	sourceFileStatusProcessed = "processed"
)

type Actions struct {
	pool                *pgxpool.Pool
	httpClient          *http.Client
	clickHouseNativeURL string
}

func NewActions(pool *pgxpool.Pool, httpClient *http.Client, clickHouseNativeURL ...string) *Actions {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	var nativeURL string
	if len(clickHouseNativeURL) > 0 {
		nativeURL = strings.TrimSpace(clickHouseNativeURL[0])
	}
	return &Actions{pool: pool, httpClient: httpClient, clickHouseNativeURL: nativeURL}
}

type SyncNACETaxonomyActivityInput = naceworkflow.SyncNACETaxonomyActivityInput
type SyncNACETaxonomyActivityResult = naceworkflow.SyncNACETaxonomyActivityResult
type SyncNACEToClickHouseActivityInput = naceworkflow.SyncNACEToClickHouseActivityInput
type SyncNACEToClickHouseActivityResult = naceworkflow.SyncNACEToClickHouseActivityResult

func (a *Actions) SyncNACETaxonomyActivity(ctx context.Context, input SyncNACETaxonomyActivityInput) (SyncNACETaxonomyActivityResult, error) {
	input = normalizeSyncNACEInput(input)
	if err := validateSyncNACEInput(input); err != nil {
		return SyncNACETaxonomyActivityResult{}, err
	}
	if a == nil || a.pool == nil {
		return SyncNACETaxonomyActivityResult{}, errors.New("nace taxonomy database is not available")
	}

	queries := db.New(a.pool)
	existingRun, err := queries.GetNACEImportRunByWorkflowID(ctx, input.TemporalWorkflowID)
	if err == nil && terminalImportRunStatus(existingRun.Status) {
		slog.DebugContext(ctx, "returning terminal nace taxonomy import run",
			"import_run_id", existingRun.ID.String(),
			"temporal_workflow_id", input.TemporalWorkflowID,
			"status", existingRun.Status,
		)
		return syncResultFromImportRun(existingRun, pgtypeUUIDString(existingRun.SourceFileID), importRunMessage(existingRun.Status)), nil
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return SyncNACETaxonomyActivityResult{}, errors.Wrap(err, "get nace import run by workflow id")
	}

	importRun, err := queries.BeginNACEImportRun(ctx, db.BeginNACEImportRunParams{
		TemporalWorkflowID: input.TemporalWorkflowID,
		Revision:           input.Revision,
		SourceUrl:          input.SourceURL,
		Metadata:           mustJSON(map[string]any{"trigger": input.Trigger, "force_reprocess": input.ForceReprocess}),
	})
	if err != nil {
		return SyncNACETaxonomyActivityResult{}, errors.Wrap(err, "begin nace import run")
	}
	result := SyncNACETaxonomyActivityResult{
		Status:      "running",
		ImportRunID: importRun.ID.String(),
	}

	slog.DebugContext(ctx, "downloading nace taxonomy source file",
		"import_run_id", importRun.ID.String(),
		"revision", input.Revision,
		"source_url", input.SourceURL,
		"force_reprocess", input.ForceReprocess,
	)
	downloaded, err := domainnace.DownloadSourceFile(ctx, a.httpClient, input.SourceURL, domainnace.DefaultMaxSourceFileBytes)
	if err != nil {
		_ = a.finishImportRunFailed(ctx, importRun.ID, nil, "", err)
		return result, errors.Wrap(err, "download nace taxonomy source")
	}
	result.ContentSHA256 = downloaded.SHA256

	if !input.ForceReprocess {
		existing, err := queries.GetProcessedNACESourceFileByHash(ctx, db.GetProcessedNACESourceFileByHashParams{
			Revision:      input.Revision,
			SourceUrl:     input.SourceURL,
			ContentSha256: downloaded.SHA256,
		})
		if err == nil {
			message := "source file hash already processed"
			finished, finishErr := queries.FinishNACEImportRun(ctx, db.FinishNACEImportRunParams{
				SourceFileID:       uuidToPgtype(existing.ID),
				Status:             naceworkflow.SyncStatusSkipped,
				ContentSha256:      &downloaded.SHA256,
				RecordsSeen:        0,
				RecordsImported:    0,
				RecordsDeactivated: 0,
				Error:              "",
				Metadata:           mustJSON(map[string]any{"message": message}),
				ID:                 importRun.ID,
			})
			if finishErr != nil {
				return result, errors.Wrap(finishErr, "finish skipped nace import run")
			}
			return syncResultFromImportRun(finished, existing.ID.String(), message), nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			_ = a.finishImportRunFailed(ctx, importRun.ID, nil, downloaded.SHA256, err)
			return result, errors.Wrap(err, "check processed nace source file hash")
		}
	}

	sourceFile, err := queries.UpsertDownloadedNACESourceFile(ctx, db.UpsertDownloadedNACESourceFileParams{
		Revision:           input.Revision,
		SourceUrl:          input.SourceURL,
		ContentSha256:      downloaded.SHA256,
		ContentLengthBytes: downloaded.ContentLengthBytes,
		ContentType:        optionalString(downloaded.ContentType),
		Etag:               optionalString(downloaded.ETag),
		LastModified:       optionalString(downloaded.LastModified),
		Metadata:           mustJSON(map[string]any{"trigger": input.Trigger}),
	})
	if err != nil {
		_ = a.finishImportRunFailed(ctx, importRun.ID, nil, downloaded.SHA256, err)
		return result, errors.Wrap(err, "register downloaded nace source file")
	}
	result.SourceFileID = sourceFile.ID.String()

	sourceFileAlreadyProcessed := sourceFile.Status == sourceFileStatusProcessed
	if sourceFileAlreadyProcessed && !input.ForceReprocess {
		message := "source file hash already processed"
		finished, finishErr := queries.FinishNACEImportRun(ctx, db.FinishNACEImportRunParams{
			SourceFileID:       uuidToPgtype(sourceFile.ID),
			Status:             naceworkflow.SyncStatusSkipped,
			ContentSha256:      &downloaded.SHA256,
			RecordsSeen:        0,
			RecordsImported:    0,
			RecordsDeactivated: 0,
			Error:              "",
			Metadata:           mustJSON(map[string]any{"message": message}),
			ID:                 importRun.ID,
		})
		if finishErr != nil {
			return result, errors.Wrap(finishErr, "finish skipped nace import run")
		}
		return syncResultFromImportRun(finished, sourceFile.ID.String(), message), nil
	}

	if !sourceFileAlreadyProcessed {
		if _, err := queries.MarkNACESourceFileProcessing(ctx, sourceFile.ID); err != nil {
			_ = a.finishImportRunFailed(ctx, importRun.ID, &sourceFile.ID, downloaded.SHA256, err)
			return result, errors.Wrap(err, "mark nace source file processing")
		}
	}

	codes, err := domainnace.ParseRDFXMLNACECodes(downloaded.Body)
	if err != nil {
		_ = a.markSourceAndRunFailed(ctx, importRun.ID, sourceFile.ID, downloaded.SHA256, sourceFileAlreadyProcessed, err)
		return result, errors.Wrap(err, "parse nace taxonomy source")
	}
	result.RecordsSeen = int32(len(codes))

	imported, deactivated, err := a.importCodes(ctx, input, importRun.ID, sourceFile.ID, downloaded, codes)
	if err != nil {
		_ = a.markSourceAndRunFailed(ctx, importRun.ID, sourceFile.ID, downloaded.SHA256, sourceFileAlreadyProcessed, err)
		return result, errors.Wrap(err, "import nace taxonomy codes")
	}

	result.Status = naceworkflow.SyncStatusSucceeded
	result.RecordsImported = imported
	result.RecordsDeactivated = deactivated
	result.Message = "nace taxonomy imported"
	return result, nil
}

func (a *Actions) importCodes(
	ctx context.Context,
	input SyncNACETaxonomyActivityInput,
	importRunID uuid.UUID,
	sourceFileID uuid.UUID,
	downloaded domainnace.DownloadedSourceFile,
	codes []domainnace.ParsedCode,
) (int32, int32, error) {
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return 0, 0, errors.Wrap(err, "begin nace taxonomy import transaction")
	}
	defer func() { _ = tx.Rollback(ctx) }()

	queries := db.New(tx)
	classification, err := queries.UpsertNACEClassification(ctx, db.UpsertNACEClassificationParams{
		Revision:       input.Revision,
		Name:           "NACE Rev. " + input.Revision,
		ValidFrom:      pgtype.Date{},
		ValidTo:        pgtype.Date{},
		SourceUrl:      &input.SourceURL,
		SourceMetadata: mustJSON(map[string]any{"content_sha256": downloaded.SHA256, "content_length_bytes": downloaded.ContentLengthBytes}),
	})
	if err != nil {
		return 0, 0, errors.Wrap(err, "upsert nace classification")
	}

	activeCodes := make([]string, 0, len(codes))
	var imported int32
	for _, parsed := range codes {
		if parsed.Level == 0 || parsed.LevelName == "" {
			return 0, 0, errors.Newf("nace code %q has unsupported level", parsed.Code)
		}
		upserted, err := queries.UpsertNACECode(ctx, db.UpsertNACECodeParams{
			ClassificationID: classification.ID,
			Code:             parsed.Code,
			NormalizedCode:   parsed.NormalizedCode,
			Level:            parsed.Level,
			LevelName:        parsed.LevelName,
			ParentCode:       optionalString(parsed.ParentCode),
			Title:            parsed.Title,
			Description:      optionalString(parsed.Description),
			Includes:         optionalString(parsed.Includes),
			Excludes:         optionalString(parsed.Excludes),
			Notes:            jsonObject(parsed.Notes),
			SourcePayload:    jsonObject(parsed.SourcePayload),
			SourceHash:       parsed.SourceHash,
		})
		if err != nil {
			return 0, 0, errors.Wrapf(err, "upsert nace code %s", parsed.Code)
		}
		if err := upsertCodeAliases(ctx, queries, upserted.ID, parsed); err != nil {
			return 0, 0, err
		}
		activeCodes = append(activeCodes, parsed.Code)
		imported++
	}

	if err := queries.LinkNACECodeParents(ctx, classification.ID); err != nil {
		return 0, 0, errors.Wrap(err, "link nace code parents")
	}
	if err := queries.ClearRootNACECodeParents(ctx, classification.ID); err != nil {
		return 0, 0, errors.Wrap(err, "clear root nace code parents")
	}
	deactivated, err := queries.DeactivateMissingNACECodes(ctx, db.DeactivateMissingNACECodesParams{
		ActiveCodes:      activeCodes,
		ClassificationID: classification.ID,
	})
	if err != nil {
		return 0, 0, errors.Wrap(err, "deactivate missing nace codes")
	}
	if _, err := queries.MarkNACESourceFileProcessed(ctx, db.MarkNACESourceFileProcessedParams{
		Metadata: mustJSON(map[string]any{"records_imported": imported, "records_deactivated": deactivated}),
		ID:       sourceFileID,
	}); err != nil {
		return 0, 0, errors.Wrap(err, "mark nace source file processed")
	}
	if _, err := queries.FinishNACEImportRun(ctx, db.FinishNACEImportRunParams{
		SourceFileID:       uuidToPgtype(sourceFileID),
		Status:             naceworkflow.SyncStatusSucceeded,
		ContentSha256:      &downloaded.SHA256,
		RecordsSeen:        int32(len(codes)),
		RecordsImported:    imported,
		RecordsDeactivated: deactivated,
		Error:              "",
		Metadata:           mustJSON(map[string]any{"message": "nace taxonomy imported"}),
		ID:                 importRunID,
	}); err != nil {
		return 0, 0, errors.Wrap(err, "finish nace import run")
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, 0, errors.Wrap(err, "commit nace taxonomy import")
	}
	return imported, deactivated, nil
}

func upsertCodeAliases(ctx context.Context, queries *db.Queries, codeID uuid.UUID, parsed domainnace.ParsedCode) error {
	exact := strings.TrimSpace(parsed.Code)
	normalized := strings.TrimSpace(parsed.NormalizedCode)
	if exact != "" {
		if err := queries.UpsertNACECodeAlias(ctx, db.UpsertNACECodeAliasParams{
			NaceCodeID:          codeID,
			AliasType:           "exact",
			AliasCode:           exact,
			NormalizedAliasCode: domainnace.NormalizeCode(exact),
			Metadata:            mustJSON(map[string]any{}),
		}); err != nil {
			return errors.Wrapf(err, "upsert exact alias for nace code %s", parsed.Code)
		}
	}
	if normalized != "" {
		if err := queries.UpsertNACECodeAlias(ctx, db.UpsertNACECodeAliasParams{
			NaceCodeID:          codeID,
			AliasType:           "normalized",
			AliasCode:           normalized,
			NormalizedAliasCode: normalized,
			Metadata:            mustJSON(map[string]any{}),
		}); err != nil {
			return errors.Wrapf(err, "upsert normalized alias for nace code %s", parsed.Code)
		}
	}
	return nil
}

func (a *Actions) markSourceAndRunFailed(
	ctx context.Context,
	importRunID uuid.UUID,
	sourceFileID uuid.UUID,
	hash string,
	sourceFileAlreadyProcessed bool,
	err error,
) error {
	queries := db.New(a.pool)
	safeMessage := safeNACEImportError()
	if !sourceFileAlreadyProcessed {
		if _, markErr := queries.MarkNACESourceFileFailed(ctx, db.MarkNACESourceFileFailedParams{
			Error:    &safeMessage,
			Metadata: mustJSON(map[string]any{}),
			ID:       sourceFileID,
		}); markErr != nil {
			return errors.Wrap(markErr, "mark nace source file failed")
		}
	}
	return a.finishImportRunFailed(ctx, importRunID, &sourceFileID, hash, err)
}

func (a *Actions) finishImportRunFailed(ctx context.Context, importRunID uuid.UUID, sourceFileID *uuid.UUID, hash string, cause error) error {
	slog.WarnContext(ctx, "nace taxonomy import failed",
		"import_run_id", importRunID.String(),
		"has_source_file", sourceFileID != nil,
		"content_sha256", hash,
		"error", cause,
	)
	queries := db.New(a.pool)
	var hashPtr *string
	if strings.TrimSpace(hash) != "" {
		hashPtr = &hash
	}
	_, err := queries.FinishNACEImportRun(ctx, db.FinishNACEImportRunParams{
		SourceFileID:       optionalUUID(sourceFileID),
		Status:             naceworkflow.SyncStatusFailed,
		ContentSha256:      hashPtr,
		RecordsSeen:        0,
		RecordsImported:    0,
		RecordsDeactivated: 0,
		Error:              safeNACEImportError(),
		Metadata:           mustJSON(map[string]any{}),
		ID:                 importRunID,
	})
	return err
}

func syncResultFromImportRun(run db.NaceImportRun, sourceFileID string, message string) SyncNACETaxonomyActivityResult {
	contentHash := ""
	if run.ContentSha256 != nil {
		contentHash = *run.ContentSha256
	}
	return SyncNACETaxonomyActivityResult{
		Status:             run.Status,
		ImportRunID:        run.ID.String(),
		SourceFileID:       sourceFileID,
		ContentSHA256:      contentHash,
		RecordsSeen:        run.RecordsSeen,
		RecordsImported:    run.RecordsImported,
		RecordsDeactivated: run.RecordsDeactivated,
		Message:            message,
	}
}

func terminalImportRunStatus(status string) bool {
	return status == naceworkflow.SyncStatusSucceeded || status == naceworkflow.SyncStatusSkipped
}

func importRunMessage(status string) string {
	switch status {
	case naceworkflow.SyncStatusSkipped:
		return "source file hash already processed"
	case naceworkflow.SyncStatusSucceeded:
		return "nace taxonomy imported"
	default:
		return ""
	}
}

func normalizeSyncNACEInput(input SyncNACETaxonomyActivityInput) SyncNACETaxonomyActivityInput {
	input.TemporalWorkflowID = strings.TrimSpace(input.TemporalWorkflowID)
	input.Revision = strings.TrimSpace(input.Revision)
	input.SourceURL = strings.TrimSpace(input.SourceURL)
	input.Trigger = strings.TrimSpace(input.Trigger)
	if input.Revision == "" {
		input.Revision = domainnace.DefaultRevision
	}
	if input.Trigger == "" {
		input.Trigger = "manual"
	}
	return input
}

func validateSyncNACEInput(input SyncNACETaxonomyActivityInput) error {
	if input.TemporalWorkflowID == "" {
		return errors.New("temporal workflow id is required")
	}
	if input.Revision == "" {
		return errors.New("nace revision is required")
	}
	if input.SourceURL == "" {
		return errors.New("nace source url is required")
	}
	return nil
}

func optionalString(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}

func optionalUUID(value *uuid.UUID) pgtype.UUID {
	if value == nil {
		return pgtype.UUID{}
	}
	return uuidToPgtype(*value)
}

func pgtypeUUIDString(value pgtype.UUID) string {
	if !value.Valid {
		return ""
	}
	return uuid.UUID(value.Bytes).String()
}

func uuidToPgtype(value uuid.UUID) pgtype.UUID {
	return pgtype.UUID{Bytes: value, Valid: true}
}

func safeNACEImportError() string {
	return "nace taxonomy import failed"
}

func jsonObject(value []byte) json.RawMessage {
	if len(value) == 0 {
		return json.RawMessage(`{}`)
	}
	return json.RawMessage(value)
}

func mustJSON(value any) json.RawMessage {
	body, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return body
}
