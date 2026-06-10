package companysources

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/activity"

	sourcecore "github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources/finland/prhxbrl"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources/finland/prhytj"
	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	sourceworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/temporal/workflow/companysources"
)

type SyncSourceDownloadInput = sourceworkflow.SyncSourceDownloadInput
type ImportSourceToClickHouseInput = sourceworkflow.ImportSourceToClickHouseInput
type RefreshSourceExplorerCacheInput = sourceworkflow.RefreshSourceExplorerCacheInput
type MapSourceIndustriesToNACEInput = sourceworkflow.MapSourceIndustriesToNACEInput
type DownloadSourceFileInput = sourceworkflow.DownloadSourceFileInput
type FinishSourceDownloadInput = sourceworkflow.FinishSourceDownloadInput
type PrepareSourceDownloadResult = sourceworkflow.PrepareSourceDownloadResult
type DownloadSourceFileResult = sourceworkflow.DownloadSourceFileResult
type ImportSourceToClickHouseResult = sourceworkflow.ImportSourceToClickHouseResult
type RefreshSourceExplorerCacheResult = sourceworkflow.RefreshSourceExplorerCacheResult
type MapSourceIndustriesToNACEResult = sourceworkflow.MapSourceIndustriesToNACEResult

type Actions struct {
	pool                *pgxpool.Pool
	registry            sourcecore.Registry
	sourceRunsRoot      string
	clickHouseNativeURL string
}

const failedRunFinishTimeout = 10 * time.Second

func NewActions(pool *pgxpool.Pool, registry sourcecore.Registry, sourceRunsRoot string, clickHouseNativeURL string) *Actions {
	return &Actions{
		pool:                pool,
		registry:            registry,
		sourceRunsRoot:      strings.TrimSpace(sourceRunsRoot),
		clickHouseNativeURL: strings.TrimSpace(clickHouseNativeURL),
	}
}

func (a *Actions) PrepareSourceDownloadActivity(ctx context.Context, input SyncSourceDownloadInput) (PrepareSourceDownloadResult, error) {
	if a == nil || a.pool == nil {
		return PrepareSourceDownloadResult{}, errors.New("company source database is not available")
	}

	actionRunID, err := uuid.Parse(input.ActionRunID)
	if err != nil {
		return PrepareSourceDownloadResult{}, errors.Wrap(err, "parse action run id")
	}

	queries := db.New(a.pool)
	workflowID, runID := workflowExecutionFromContext(ctx)

	actionRun, err := queries.GetSourceActionRun(ctx, actionRunID)
	if err != nil {
		return PrepareSourceDownloadResult{}, errors.Wrap(err, "load source action run")
	}
	if actionRun.Action != sourceworkflow.ActionPullSource {
		return PrepareSourceDownloadResult{}, errors.Errorf("action run %s has action %q", actionRun.ID, actionRun.Action)
	}
	action, err := queries.GetSourceActionByName(ctx, db.GetSourceActionByNameParams{
		Name:   input.SourceName,
		Action: sourceworkflow.ActionPullSource,
	})
	if err != nil {
		return PrepareSourceDownloadResult{}, errors.Wrap(err, "load pull source action")
	}
	if actionRun.SourceID != action.SourceID || actionRun.ActionID != action.ID {
		return PrepareSourceDownloadResult{}, errors.Errorf("action run %s does not belong to %s pull action", actionRun.ID, input.SourceName)
	}
	if err := validateStoredWorkflowID(actionRun.TemporalWorkflowID, workflowID, "source action run", actionRun.ID); err != nil {
		return PrepareSourceDownloadResult{}, err
	}
	if runID != "" {
		if err := queries.UpdateSourceActionRunTemporalRunID(ctx, db.UpdateSourceActionRunTemporalRunIDParams{
			TemporalRunID: optionalStringPointer(runID),
			ID:            actionRunID,
		}); err != nil {
			return PrepareSourceDownloadResult{}, errors.Wrap(err, "update source action temporal run id")
		}
	}

	files, err := queries.ListSourceFilesWithLatestRun(ctx, input.SourceName)
	if err != nil {
		return PrepareSourceDownloadResult{}, errors.Wrap(err, "list source files")
	}

	result := PrepareSourceDownloadResult{ActionRunID: input.ActionRunID}
	for _, file := range files {
		if !file.Enabled {
			continue
		}
		fileRunID := deterministicSourceFileRunID(actionRun.ID, file.ID)
		workflowID := sourceworkflow.FileRunWorkflowID(fileRunID.String())
		_, err := queries.CreateSourceFileRun(ctx, db.CreateSourceFileRunParams{
			ID:                 fileRunID,
			ParentActionRunID:  pgUUID(actionRun.ID),
			TemporalWorkflowID: &workflowID,
			SourceFileID:       file.ID,
		})
		if err != nil && !isUniqueViolation(err) {
			return PrepareSourceDownloadResult{}, errors.Wrapf(err, "create file run %s", file.FileKey)
		}
		result.Files = append(result.Files, sourceworkflow.DownloadSourceFileInput{
			FileRunID:           fileRunID.String(),
			SourceName:          input.SourceName,
			FileKey:             file.FileKey,
			Trigger:             input.Trigger,
			ParentActionRunID:   input.ActionRunID,
			RegisteredDateStart: input.RegisteredDateStart,
			RegisteredDateEnd:   input.RegisteredDateEnd,
			MaxStatements:       input.MaxStatements,
			RetryFailed:         input.RetryFailed,
		})
	}
	return result, nil
}

func (a *Actions) DownloadSourceFileActivity(ctx context.Context, input DownloadSourceFileInput) (DownloadSourceFileResult, error) {
	if a == nil || a.pool == nil {
		return DownloadSourceFileResult{}, errors.New("company source database is not available")
	}
	if a.sourceRunsRoot == "" {
		return DownloadSourceFileResult{}, errors.New("company source run root is required")
	}

	fileRunID, err := uuid.Parse(input.FileRunID)
	if err != nil {
		return DownloadSourceFileResult{}, errors.Wrap(err, "parse file run id")
	}

	queries := db.New(a.pool)
	workflowID, runID := workflowExecutionFromContext(ctx)

	fileRun, err := queries.GetSourceFileRunWithDefinition(ctx, fileRunID)
	if err != nil {
		return DownloadSourceFileResult{}, errors.Wrap(err, "load source file run")
	}
	if fileRun.SourceName != input.SourceName || fileRun.FileKey != input.FileKey {
		return DownloadSourceFileResult{}, errors.Errorf("file run %s does not match %s/%s", fileRunID, input.SourceName, input.FileKey)
	}
	if fileRun.Status != "running" {
		return DownloadSourceFileResult{}, errors.Errorf("source file run %s has status %q", fileRun.ID, fileRun.Status)
	}
	if err := validateStoredWorkflowID(fileRun.TemporalWorkflowID, workflowID, "source file run", fileRun.ID); err != nil {
		return DownloadSourceFileResult{}, err
	}
	if runID != "" {
		if err := queries.UpdateSourceFileRunTemporalRunID(ctx, db.UpdateSourceFileRunTemporalRunIDParams{
			TemporalRunID: optionalStringPointer(runID),
			ID:            fileRunID,
		}); err != nil {
			return DownloadSourceFileResult{}, errors.Wrap(err, "update source file temporal run id")
		}
	}

	fileDir, err := fileRunDirectoryName(fileRun.FileKey, input.FileRunID)
	if err != nil {
		return DownloadSourceFileResult{}, err
	}
	runDir := filepath.Join(a.sourceRunsRoot, fileRun.Country, fileRun.Source, "files", fileRun.FileKey, fileDir)

	var downloaded sourcecore.DownloadedFile
	if fileRun.SourceName == prhxbrl.SourceName && fileRun.FileKey == "statements_manifest" {
		var windowInput prhxbrlWindowInput
		windowInput, err = validatePRHXBRLWindowInput(input)
		if err == nil {
			parentActionRunID, parseErr := uuid.Parse(input.ParentActionRunID)
			if parseErr != nil {
				err = errors.Wrap(parseErr, "parse parent action run id")
			} else {
				downloaded, err = prhxbrl.Download(ctx, prhxbrl.DownloadOptions{
					Queries:              queries,
					HTTPClient:           http.DefaultClient,
					SourceID:             fileRun.SourceID,
					ActionRunID:          parentActionRunID,
					TemporalWorkflowID:   workflowID,
					TemporalRunID:        runID,
					RunDir:               runDir,
					ManifestRelativePath: fileRun.RelativePath,
					SourceURL:            fileRun.SourceUrl,
					UserAgentRequired:    fileRun.UserAgentRequired,
					RegisteredDateStart:  windowInput.RegisteredDateStart,
					RegisteredDateEnd:    windowInput.RegisteredDateEnd,
					MaxStatements:        windowInput.MaxStatements,
					RetryFailed:          windowInput.RetryFailed,
				})
			}
		}
	} else {
		downloaded, err = sourcecore.DownloadFile(ctx, a.registry, sourcecore.DownloadFileRequest{
			Country:           fileRun.Country,
			Source:            fileRun.Source,
			FileKey:           fileRun.FileKey,
			FileKind:          fileRun.Kind,
			RunDir:            runDir,
			RelativePath:      fileRun.RelativePath,
			SourceURL:         fileRun.SourceUrl,
			UserAgentRequired: fileRun.UserAgentRequired,
			Config:            decodeObject(fileRun.Config),
		})
	}
	if err != nil {
		finishErr := a.finishFileRunFailed(fileRun.ID, err)
		return DownloadSourceFileResult{FileRunID: input.FileRunID, SourceName: input.SourceName, FileKey: input.FileKey}, combineWithFinishError(errors.Wrap(err, "download source file"), finishErr)
	}
	if err := validateDownloadedFile(downloaded, runDir, fileRun.RelativePath); err != nil {
		finishErr := a.finishFileRunFailed(fileRun.ID, err)
		return DownloadSourceFileResult{FileRunID: input.FileRunID, SourceName: input.SourceName, FileKey: input.FileKey}, combineWithFinishError(errors.Wrap(err, "validate downloaded source file"), finishErr)
	}

	contentLengthBytes := downloaded.ContentLengthBytes
	recordsWritten := downloaded.RecordsWritten
	if _, err := queries.FinishSourceFileRun(ctx, db.FinishSourceFileRunParams{
		Status:             sourceworkflow.StatusSucceeded,
		Path:               downloaded.Path,
		ContentSha256:      downloaded.ContentSHA256,
		ContentLengthBytes: &contentLengthBytes,
		RecordsWritten:     &recordsWritten,
		ErrorMessage:       "",
		Log:                marshalActionResult([]map[string]any{{"level": "info", "message": "file written", "path": downloaded.Path}}),
		ID:                 fileRun.ID,
	}); err != nil {
		return DownloadSourceFileResult{}, errors.Wrap(err, "finish source file run")
	}

	return DownloadSourceFileResult{
		FileRunID:          input.FileRunID,
		SourceName:         input.SourceName,
		FileKey:            input.FileKey,
		Path:               downloaded.Path,
		ContentSHA256:      downloaded.ContentSHA256,
		ContentLengthBytes: downloaded.ContentLengthBytes,
		RecordsWritten:     downloaded.RecordsWritten,
	}, nil
}

func (a *Actions) FinishSourceDownloadActivity(ctx context.Context, input FinishSourceDownloadInput) error {
	if a == nil || a.pool == nil {
		return errors.New("company source database is not available")
	}
	actionRunID, err := uuid.Parse(input.ActionRunID)
	if err != nil {
		return errors.Wrap(err, "parse action run id")
	}
	workflowID, _ := workflowExecutionFromContext(ctx)
	status := strings.TrimSpace(input.Status)
	if status == "" {
		status = sourceworkflow.StatusSucceeded
	}
	queries := db.New(a.pool)
	actionRun, err := queries.GetSourceActionRun(ctx, actionRunID)
	if err != nil {
		return errors.Wrap(err, "load source action run")
	}
	if actionRun.Action != sourceworkflow.ActionPullSource {
		return errors.Errorf("action run %s has action %q", actionRun.ID, actionRun.Action)
	}
	if err := validateStoredWorkflowID(actionRun.TemporalWorkflowID, workflowID, "source action run", actionRun.ID); err != nil {
		return err
	}
	_, err = queries.FinishSourceActionRun(ctx, db.FinishSourceActionRunParams{
		Status:       status,
		Result:       marshalActionResult(map[string]any{"action_run_id": input.ActionRunID, "files": input.Files}),
		ErrorMessage: input.ErrorMessage,
		ID:           actionRunID,
	})
	return errors.Wrap(err, "finish source download action run")
}

func (a *Actions) ImportSourceToClickHouseActivity(ctx context.Context, input ImportSourceToClickHouseInput) (ImportSourceToClickHouseResult, error) {
	if a == nil || a.pool == nil {
		return ImportSourceToClickHouseResult{}, errors.New("company source database is not available")
	}
	actionRunID, err := uuid.Parse(input.ActionRunID)
	if err != nil {
		return ImportSourceToClickHouseResult{}, errors.Wrap(err, "parse action run id")
	}

	queries := db.New(a.pool)
	workflowID, runID := workflowExecutionFromContext(ctx)

	actionRun, err := queries.GetSourceActionRun(ctx, actionRunID)
	if err != nil {
		return ImportSourceToClickHouseResult{}, errors.Wrap(err, "load import action run")
	}
	if actionRun.Action != sourceworkflow.ActionImportClickHouse {
		return ImportSourceToClickHouseResult{}, errors.Errorf("action run %s has action %q", actionRun.ID, actionRun.Action)
	}

	action, err := queries.GetSourceActionByName(ctx, db.GetSourceActionByNameParams{
		Name:   input.SourceName,
		Action: sourceworkflow.ActionImportClickHouse,
	})
	if err != nil {
		return ImportSourceToClickHouseResult{}, errors.Wrap(err, "load import clickhouse action")
	}
	if actionRun.SourceID != action.SourceID || actionRun.ActionID != action.ID {
		return ImportSourceToClickHouseResult{}, errors.Errorf("action run %s does not belong to %s import action", actionRun.ID, input.SourceName)
	}
	if err := validateStoredWorkflowID(actionRun.TemporalWorkflowID, workflowID, "source action run", actionRun.ID); err != nil {
		return ImportSourceToClickHouseResult{}, err
	}
	if runID != "" {
		if err := queries.UpdateSourceActionRunTemporalRunID(ctx, db.UpdateSourceActionRunTemporalRunIDParams{
			TemporalRunID: optionalStringPointer(runID),
			ID:            actionRunID,
		}); err != nil {
			return ImportSourceToClickHouseResult{}, errors.Wrap(err, "update import action temporal run id")
		}
	}

	result := ImportSourceToClickHouseResult{ActionRunID: actionRun.ID.String()}
	selectedFiles, err := a.selectedImportFiles(ctx, queries, input, action.SourceID)
	if err != nil {
		finishErr := a.finishActionRunFailed(actionRun.ID, err)
		return result, combineWithFinishError(errors.Wrap(err, "select import files"), finishErr)
	}
	runDir, err := selectedImportRunDir(selectedFiles)
	if err != nil {
		finishErr := a.finishActionRunFailed(actionRun.ID, err)
		return result, combineWithFinishError(errors.Wrap(err, "select import run directory"), finishErr)
	}

	imported, err := sourcecore.ImportRun(ctx, a.registry, sourcecore.ImportRunRequest{
		Country:             action.Country,
		Source:              action.Source,
		RunDir:              runDir,
		Files:               selectedFiles,
		ClickHouseNativeURL: a.clickHouseNativeURL,
		BatchSize:           input.BatchSize,
		Limit:               input.Limit,
		Truncate:            true,
	})
	if err != nil {
		finishErr := a.finishActionRunFailed(actionRun.ID, err)
		return result, combineWithFinishError(errors.Wrap(err, "import company source to clickhouse"), finishErr)
	}

	result.ImportedTables = imported.ImportedTables
	result.ImportedRows = imported.ImportedRows
	if _, err := queries.FinishSourceActionRun(ctx, db.FinishSourceActionRunParams{
		Status:       sourceworkflow.StatusSucceeded,
		Result:       marshalActionResult(result),
		ErrorMessage: "",
		ID:           actionRun.ID,
	}); err != nil {
		return result, errors.Wrap(err, "finish import clickhouse action run")
	}
	return result, nil
}

func (a *Actions) RefreshSourceExplorerCacheActivity(ctx context.Context, input RefreshSourceExplorerCacheInput) (RefreshSourceExplorerCacheResult, error) {
	if a == nil || a.pool == nil {
		return RefreshSourceExplorerCacheResult{}, errors.New("company source database is not available")
	}
	actionRunID, err := uuid.Parse(input.ActionRunID)
	if err != nil {
		return RefreshSourceExplorerCacheResult{}, errors.Wrap(err, "parse action run id")
	}

	queries := db.New(a.pool)
	workflowID, runID := workflowExecutionFromContext(ctx)

	actionRun, err := queries.GetSourceActionRun(ctx, actionRunID)
	if err != nil {
		return RefreshSourceExplorerCacheResult{}, errors.Wrap(err, "load explorer cache action run")
	}
	if actionRun.Action != sourceworkflow.ActionRefreshExplorerCache {
		return RefreshSourceExplorerCacheResult{}, errors.Errorf("action run %s has action %q", actionRun.ID, actionRun.Action)
	}
	action, err := queries.GetSourceActionByName(ctx, db.GetSourceActionByNameParams{
		Name:   input.SourceName,
		Action: sourceworkflow.ActionRefreshExplorerCache,
	})
	if err != nil {
		return RefreshSourceExplorerCacheResult{}, errors.Wrap(err, "load explorer cache action")
	}
	if actionRun.SourceID != action.SourceID || actionRun.ActionID != action.ID {
		return RefreshSourceExplorerCacheResult{}, errors.Errorf("action run %s does not belong to %s explorer cache action", actionRun.ID, input.SourceName)
	}
	if err := validateStoredWorkflowID(actionRun.TemporalWorkflowID, workflowID, "source action run", actionRun.ID); err != nil {
		return RefreshSourceExplorerCacheResult{}, err
	}
	if runID != "" {
		if err := queries.UpdateSourceActionRunTemporalRunID(ctx, db.UpdateSourceActionRunTemporalRunIDParams{
			TemporalRunID: optionalStringPointer(runID),
			ID:            actionRunID,
		}); err != nil {
			return RefreshSourceExplorerCacheResult{}, errors.Wrap(err, "update explorer cache action temporal run id")
		}
	}

	result := RefreshSourceExplorerCacheResult{
		ActionRunID: actionRun.ID.String(),
		SourceName:  input.SourceName,
	}
	refreshed, err := a.refreshSourceExplorerCache(ctx, input.SourceName)
	if err != nil {
		finishErr := a.finishActionRunFailed(actionRun.ID, err)
		return result, combineWithFinishError(errors.Wrap(err, "refresh source explorer cache"), finishErr)
	}
	result.CacheTable = refreshed.CacheTable
	result.Rows = refreshed.Rows
	result.RefreshedAt = refreshed.RefreshedAt.Format(time.RFC3339Nano)

	if _, err := queries.FinishSourceActionRun(ctx, db.FinishSourceActionRunParams{
		Status:       sourceworkflow.StatusSucceeded,
		Result:       marshalActionResult(result),
		ErrorMessage: "",
		ID:           actionRun.ID,
	}); err != nil {
		return result, errors.Wrap(err, "finish explorer cache action run")
	}
	return result, nil
}

func (a *Actions) MapSourceIndustriesToNACEActivity(ctx context.Context, input MapSourceIndustriesToNACEInput) (MapSourceIndustriesToNACEResult, error) {
	if a == nil || a.pool == nil {
		return MapSourceIndustriesToNACEResult{}, errors.New("company source database is not available")
	}
	actionRunID, err := uuid.Parse(input.ActionRunID)
	if err != nil {
		return MapSourceIndustriesToNACEResult{}, errors.Wrap(err, "parse action run id")
	}

	queries := db.New(a.pool)
	workflowID, runID := workflowExecutionFromContext(ctx)

	actionRun, err := queries.GetSourceActionRun(ctx, actionRunID)
	if err != nil {
		return MapSourceIndustriesToNACEResult{}, errors.Wrap(err, "load industry NACE mapping action run")
	}
	if actionRun.Action != sourceworkflow.ActionMapIndustriesToNACE {
		return MapSourceIndustriesToNACEResult{}, errors.Errorf("action run %s has action %q", actionRun.ID, actionRun.Action)
	}
	action, err := queries.GetSourceActionByName(ctx, db.GetSourceActionByNameParams{
		Name:   input.SourceName,
		Action: sourceworkflow.ActionMapIndustriesToNACE,
	})
	if err != nil {
		return MapSourceIndustriesToNACEResult{}, errors.Wrap(err, "load industry NACE mapping action")
	}
	if actionRun.SourceID != action.SourceID || actionRun.ActionID != action.ID {
		return MapSourceIndustriesToNACEResult{}, errors.Errorf("action run %s does not belong to %s industry NACE mapping action", actionRun.ID, input.SourceName)
	}
	if err := validateStoredWorkflowID(actionRun.TemporalWorkflowID, workflowID, "source action run", actionRun.ID); err != nil {
		return MapSourceIndustriesToNACEResult{}, err
	}
	if runID != "" {
		if err := queries.UpdateSourceActionRunTemporalRunID(ctx, db.UpdateSourceActionRunTemporalRunIDParams{
			TemporalRunID: optionalStringPointer(runID),
			ID:            actionRunID,
		}); err != nil {
			return MapSourceIndustriesToNACEResult{}, errors.Wrap(err, "update industry NACE mapping action temporal run id")
		}
	}

	result := MapSourceIndustriesToNACEResult{
		ActionRunID: actionRun.ID.String(),
		SourceName:  input.SourceName,
	}
	mapped, err := a.mapSourceIndustriesToNACE(ctx, input.SourceName)
	if err != nil {
		finishErr := a.finishActionRunFailed(actionRun.ID, err)
		return result, combineWithFinishError(errors.Wrap(err, "map source industries to NACE"), finishErr)
	}
	result.MappingTable = mapped.MappingTable
	result.Rows = mapped.Rows
	result.MappedRows = mapped.MappedRows
	result.UnmappedRows = mapped.UnmappedRows
	result.MappedAt = mapped.MappedAt.Format(time.RFC3339Nano)

	if _, err := queries.FinishSourceActionRun(ctx, db.FinishSourceActionRunParams{
		Status:       sourceworkflow.StatusSucceeded,
		Result:       marshalActionResult(result),
		ErrorMessage: "",
		ID:           actionRun.ID,
	}); err != nil {
		return result, errors.Wrap(err, "finish industry NACE mapping action run")
	}
	return result, nil
}

func (a *Actions) refreshSourceExplorerCache(ctx context.Context, sourceName string) (prhytj.CompanyExplorerCacheRefreshResult, error) {
	switch sourceName {
	case "finland_prhytj":
		return prhytj.RefreshCompanyExplorerCache(ctx, a.clickHouseNativeURL)
	default:
		return prhytj.CompanyExplorerCacheRefreshResult{}, errors.Errorf("source explorer cache refresh is not implemented for %s", sourceName)
	}
}

func (a *Actions) mapSourceIndustriesToNACE(ctx context.Context, sourceName string) (prhytj.IndustryNACEMappingRefreshResult, error) {
	switch sourceName {
	case "finland_prhytj":
		return prhytj.RefreshIndustryNACEMappings(ctx, a.clickHouseNativeURL)
	default:
		return prhytj.IndustryNACEMappingRefreshResult{}, errors.Errorf("source industry nace mapping is not implemented for %s", sourceName)
	}
}

func (a *Actions) selectedImportFiles(ctx context.Context, queries *db.Queries, input ImportSourceToClickHouseInput, sourceID uuid.UUID) ([]sourcecore.SelectedSourceFile, error) {
	if len(input.FileRunIDs) > 0 {
		files := make([]sourcecore.SelectedSourceFile, 0, len(input.FileRunIDs))
		for _, id := range input.FileRunIDs {
			fileRunID, err := uuid.Parse(id)
			if err != nil {
				return nil, errors.Wrap(err, "parse file run id")
			}
			row, err := queries.GetSourceFileRunWithDefinition(ctx, fileRunID)
			if err != nil {
				return nil, errors.Wrap(err, "load source file run")
			}
			if row.SourceName != input.SourceName {
				return nil, errors.Errorf("source file run %s belongs to source %s, not %s", row.ID, row.SourceName, input.SourceName)
			}
			if row.SourceID != sourceID {
				return nil, errors.Errorf("source file run %s does not belong to source %s", row.ID, sourceID)
			}
			file, err := selectedSourceFileFromRun(row.FileKey, row.Kind, row.RelativePath, row.Config, row.Path, row.Status, row.ID.String())
			if err != nil {
				return nil, err
			}
			files = append(files, file)
		}
		return files, nil
	}

	if input.DownloadActionRunID != "" {
		actionRunID, err := uuid.Parse(input.DownloadActionRunID)
		if err != nil {
			return nil, errors.Wrap(err, "parse download action run id")
		}
		actionRun, err := queries.GetSourceActionRun(ctx, actionRunID)
		if err != nil {
			return nil, errors.Wrap(err, "load download action run")
		}
		if actionRun.Action != sourceworkflow.ActionPullSource {
			return nil, errors.Errorf("download action run %s has action %q", actionRun.ID, actionRun.Action)
		}
		if actionRun.SourceID != sourceID {
			return nil, errors.Errorf("download action run %s does not belong to source %s", actionRun.ID, sourceID)
		}
		if actionRun.Status != sourceworkflow.StatusSucceeded {
			return nil, errors.Errorf("download action run %s has status %q", actionRun.ID, actionRun.Status)
		}
		rows, err := queries.ListSuccessfulSourceFileRunsForAction(ctx, pgUUID(actionRunID))
		if err != nil {
			return nil, errors.Wrap(err, "list successful source file runs for action")
		}
		definitions, err := queries.ListSourceFilesWithLatestRun(ctx, input.SourceName)
		if err != nil {
			return nil, errors.Wrap(err, "list source files")
		}
		return selectedSourceFilesFromActionRows(rows, definitions)
	}

	rows, err := queries.ListLatestSuccessfulRequiredSourceFileRuns(ctx, input.SourceName)
	if err != nil {
		return nil, errors.Wrap(err, "list latest successful required source file runs")
	}
	return selectedSourceFilesFromLatestRows(rows)
}

func selectedSourceFilesFromActionRows(rows []db.ListSuccessfulSourceFileRunsForActionRow, definitions []db.ListSourceFilesWithLatestRunRow) ([]sourcecore.SelectedSourceFile, error) {
	if len(rows) == 0 {
		return nil, errors.New("no successful source file runs found")
	}
	if err := requireEnabledRequiredFiles(rows, definitions); err != nil {
		return nil, err
	}
	files := make([]sourcecore.SelectedSourceFile, 0, len(rows))
	for _, row := range rows {
		file, err := selectedSourceFileFromRun(row.FileKey, row.Kind, row.RelativePath, row.Config, row.Path, row.Status, row.ID.String())
		if err != nil {
			return nil, err
		}
		files = append(files, file)
	}
	return files, nil
}

func selectedSourceFilesFromLatestRows(rows []db.ListLatestSuccessfulRequiredSourceFileRunsRow) ([]sourcecore.SelectedSourceFile, error) {
	if len(rows) == 0 {
		return nil, errors.New("no required source files are configured")
	}
	files := make([]sourcecore.SelectedSourceFile, 0, len(rows))
	for _, row := range rows {
		if !row.ID.Valid {
			return nil, errors.Errorf("required source file %s has no successful run", row.FileKey)
		}
		if row.Status == nil {
			return nil, errors.Errorf("required source file %s has no successful run", row.FileKey)
		}
		fileRunID := uuid.UUID(row.ID.Bytes).String()
		file, err := selectedSourceFileFromRun(row.FileKey, row.Kind, row.RelativePath, row.Config, row.Path, *row.Status, fileRunID)
		if err != nil {
			return nil, err
		}
		files = append(files, file)
	}
	return files, nil
}

func requireEnabledRequiredFiles(rows []db.ListSuccessfulSourceFileRunsForActionRow, definitions []db.ListSourceFilesWithLatestRunRow) error {
	downloaded := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		downloaded[row.FileKey] = struct{}{}
	}
	for _, definition := range definitions {
		if !definition.Enabled || !definition.Required {
			continue
		}
		if _, ok := downloaded[definition.FileKey]; !ok {
			return errors.Errorf("required source file %s has no successful run for download action", definition.FileKey)
		}
	}
	return nil
}

func selectedSourceFileFromRun(fileKey string, kind string, relativePath string, config json.RawMessage, path *string, status string, fileRunID string) (sourcecore.SelectedSourceFile, error) {
	if status != sourceworkflow.StatusSucceeded {
		return sourcecore.SelectedSourceFile{}, errors.Errorf("source file run %s has status %q", fileRunID, status)
	}
	if path == nil || strings.TrimSpace(*path) == "" {
		return sourcecore.SelectedSourceFile{}, errors.Errorf("source file run %s has no path", fileRunID)
	}
	cleanPath := strings.TrimSpace(*path)
	if _, err := os.Stat(cleanPath); err != nil {
		return sourcecore.SelectedSourceFile{}, errors.Wrapf(err, "stat source file %s", cleanPath)
	}
	return sourcecore.SelectedSourceFile{
		FileRunID:    fileRunID,
		FileKey:      fileKey,
		Kind:         kind,
		Path:         cleanPath,
		RelativePath: relativePath,
		Config:       sourceFileConfigMap(config),
	}, nil
}

func sourceFileConfigMap(config json.RawMessage) map[string]any {
	if len(config) == 0 {
		return nil
	}
	var decoded map[string]any
	if err := json.Unmarshal(config, &decoded); err != nil {
		return nil
	}
	return decoded
}

func validateDownloadedFile(downloaded sourcecore.DownloadedFile, runDir string, relativePath string) error {
	if strings.TrimSpace(downloaded.Path) == "" {
		return errors.New("downloaded source file path is required")
	}
	expectedPath, err := sourcecore.SafeRunRelativePath(runDir, relativePath)
	if err != nil {
		return errors.Wrap(err, "resolve expected downloaded source file path")
	}
	if filepath.Clean(downloaded.Path) != filepath.Clean(expectedPath) {
		return errors.Errorf("downloaded source file path %q does not match expected path %q", downloaded.Path, expectedPath)
	}
	info, err := os.Stat(downloaded.Path)
	if err != nil {
		return errors.Wrapf(err, "stat downloaded source file %s", downloaded.Path)
	}
	if info.IsDir() {
		return errors.Errorf("downloaded source file path %q is a directory", downloaded.Path)
	}
	return nil
}

type prhxbrlWindowInput struct {
	RegisteredDateStart string
	RegisteredDateEnd   string
	MaxStatements       int32
	RetryFailed         bool
}

func validatePRHXBRLWindowInput(input DownloadSourceFileInput) (prhxbrlWindowInput, error) {
	start := strings.TrimSpace(input.RegisteredDateStart)
	end := strings.TrimSpace(input.RegisteredDateEnd)
	if start == "" {
		return prhxbrlWindowInput{}, errors.New("registered_date_start is required")
	}
	if end == "" {
		return prhxbrlWindowInput{}, errors.New("registered_date_end is required")
	}
	startDate, err := time.Parse(time.DateOnly, start)
	if err != nil {
		return prhxbrlWindowInput{}, errors.Wrap(err, "parse registered_date_start")
	}
	endDate, err := time.Parse(time.DateOnly, end)
	if err != nil {
		return prhxbrlWindowInput{}, errors.Wrap(err, "parse registered_date_end")
	}
	if startDate.After(endDate) {
		return prhxbrlWindowInput{}, errors.New("registered_date_start must be on or before registered_date_end")
	}
	maxStatements := input.MaxStatements
	if maxStatements <= 0 {
		maxStatements = 50
	}
	if maxStatements > 1000 {
		return prhxbrlWindowInput{}, errors.New("max_statements must be 1000 or less")
	}
	return prhxbrlWindowInput{
		RegisteredDateStart: start,
		RegisteredDateEnd:   end,
		MaxStatements:       maxStatements,
		RetryFailed:         input.RetryFailed,
	}, nil
}

func selectedImportRunDir(files []sourcecore.SelectedSourceFile) (string, error) {
	for _, file := range files {
		if file.FileKey == "source" && strings.TrimSpace(file.Path) != "" {
			return filepath.Dir(file.Path), nil
		}
	}
	for _, file := range files {
		if strings.TrimSpace(file.Path) != "" {
			return filepath.Dir(file.Path), nil
		}
	}
	return "", errors.New("selected import files have no paths")
}

func (a *Actions) finishActionRunFailed(actionRunID uuid.UUID, runErr error) error {
	finishCtx, cancel := context.WithTimeout(context.Background(), failedRunFinishTimeout)
	defer cancel()

	queries := db.New(a.pool)
	_, err := queries.FinishSourceActionRun(finishCtx, db.FinishSourceActionRunParams{
		Status:       sourceworkflow.StatusFailed,
		Result:       marshalJSONObject(map[string]any{}),
		ErrorMessage: runErr.Error(),
		ID:           actionRunID,
	})
	return errors.Wrap(err, "finish failed source action run")
}

func (a *Actions) finishFileRunFailed(fileRunID uuid.UUID, runErr error) error {
	finishCtx, cancel := context.WithTimeout(context.Background(), failedRunFinishTimeout)
	defer cancel()

	queries := db.New(a.pool)
	_, err := queries.FinishSourceFileRun(finishCtx, db.FinishSourceFileRunParams{
		Status:       sourceworkflow.StatusFailed,
		ErrorMessage: runErr.Error(),
		Log:          marshalActionResult([]map[string]any{{"level": "error", "message": runErr.Error()}}),
		ID:           fileRunID,
	})
	return errors.Wrap(err, "finish failed source file run")
}

func marshalActionResult(value any) json.RawMessage {
	data, err := json.Marshal(value)
	if err != nil {
		return marshalJSONObject(map[string]any{"marshal_error": err.Error()})
	}
	return data
}

func marshalJSONObject(value map[string]any) json.RawMessage {
	data, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return data
}

func decodeObject(raw json.RawMessage) map[string]any {
	if len(raw) == 0 {
		return map[string]any{}
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return map[string]any{}
	}
	if out == nil {
		return map[string]any{}
	}
	return out
}

func workflowExecutionFromContext(ctx context.Context) (workflowID string, runID string) {
	defer func() {
		_ = recover()
	}()
	info := activity.GetInfo(ctx)
	return info.WorkflowExecution.ID, info.WorkflowExecution.RunID
}

func fileRunDirectoryName(fileKey string, fileRunID string) (string, error) {
	fileKey = strings.TrimSpace(fileKey)
	if fileKey == "" {
		return "", errors.New("file key is required")
	}
	if filepath.IsAbs(fileKey) || strings.ContainsAny(fileKey, `/\`) || fileKey == "." || fileKey == ".." {
		return "", errors.Errorf("unsafe source file key %q", fileKey)
	}
	return time.Now().UTC().Format("20060102T150405Z") + "-" + fileKey + "-" + fileRunID, nil
}

func deterministicSourceFileRunID(actionRunID uuid.UUID, sourceFileID uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("corpscout/source-file-run/"+actionRunID.String()+"/"+sourceFileID.String()))
}

func validateStoredWorkflowID(stored *string, current string, label string, id uuid.UUID) error {
	if current == "" || stored == nil || strings.TrimSpace(*stored) == "" {
		return nil
	}
	if *stored != current {
		return errors.Errorf("%s %s workflow id mismatch: stored %q, current %q", label, id, *stored, current)
	}
	return nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func optionalStringPointer(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func pgUUID(value uuid.UUID) pgtype.UUID {
	return pgtype.UUID{Bytes: value, Valid: true}
}

func combineWithFinishError(runErr error, finishErr error) error {
	if finishErr == nil {
		return runErr
	}
	return errors.WithSecondaryError(runErr, finishErr)
}
