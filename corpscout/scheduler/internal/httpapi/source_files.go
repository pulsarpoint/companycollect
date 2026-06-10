package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"go.temporal.io/sdk/client"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	companysourceworkflows "github.com/pulsarpoint/corpscout/scheduler/internal/temporal/workflow/companysources"
)

const defaultSourceFileRunsLimit = 20
const maxSourceFileRunsLimit = 100

func (h *Handlers) handleListSourceFiles(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	rows, err := h.db.ListSourceFilesWithLatestRun(r.Context(), name)
	if err != nil {
		slog.ErrorContext(r.Context(), "list source files", "source", name, "error", err)
		writeError(w, http.StatusInternalServerError, "list source files failed")
		return
	}
	items := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		missing := !row.LatestSuccessfulRunID.Valid || row.LatestSuccessfulPath == nil || !pathExists(*row.LatestSuccessfulPath)
		items = append(items, map[string]any{
			"id":                          row.ID,
			"source_id":                   row.SourceID,
			"source_name":                 row.SourceName,
			"file_key":                    row.FileKey,
			"display_name":                row.DisplayName,
			"description":                 row.Description,
			"kind":                        row.Kind,
			"required":                    row.Required,
			"relative_path":               row.RelativePath,
			"enabled":                     row.Enabled,
			"sort_order":                  row.SortOrder,
			"latest_status":               row.LatestStatus,
			"missing":                     missing,
			"latest_run_id":               pgUUIDString(row.LatestRunID),
			"latest_started_at":           timestampFromPG(row.LatestStartedAt),
			"latest_finished_at":          timestampFromPG(row.LatestFinishedAt),
			"latest_path":                 row.LatestPath,
			"latest_content_sha256":       row.LatestContentSha256,
			"latest_content_length_bytes": row.LatestContentLengthBytes,
			"latest_records_written":      row.LatestRecordsWritten,
			"latest_error_message":        row.LatestErrorMessage,
			"latest_successful_run_id":    pgUUIDString(row.LatestSuccessfulRunID),
			"latest_successful_path":      row.LatestSuccessfulPath,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (h *Handlers) handleListSourceFileRuns(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	fileKey := chi.URLParam(r, "file_key")
	limit := parseBoundedLimit(r.URL.Query().Get("limit"), defaultSourceFileRunsLimit, maxSourceFileRunsLimit)
	rows, err := h.db.ListSourceFileRuns(r.Context(), db.ListSourceFileRunsParams{
		SourceName: name,
		FileKey:    fileKey,
		RowLimit:   int32(limit),
	})
	if err != nil {
		slog.ErrorContext(r.Context(), "list source file runs", "source", name, "file_key", fileKey, "error", err)
		writeError(w, http.StatusInternalServerError, "list source file runs failed")
		return
	}
	if rows == nil {
		rows = []db.ListSourceFileRunsRow{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": rows})
}

func (h *Handlers) handleTriggerSourceFileDownload(w http.ResponseWriter, r *http.Request) {
	if h.temporal == nil {
		writeError(w, http.StatusServiceUnavailable, "temporal client not available")
		return
	}
	sourceName := chi.URLParam(r, "name")
	fileKey := chi.URLParam(r, "file_key")
	req, err := decodeSourceActionTriggerRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	file, err := h.db.GetSourceFileBySourceNameAndKey(r.Context(), db.GetSourceFileBySourceNameAndKeyParams{
		Name:    sourceName,
		FileKey: fileKey,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "source file not found")
			return
		}
		slog.ErrorContext(r.Context(), "get source file", "source", sourceName, "file_key", fileKey, "error", err)
		writeError(w, http.StatusInternalServerError, "get source file failed")
		return
	}
	if isPRHXBRLStatementsManifest(file.SourceName, file.FileKey) {
		writeError(w, http.StatusBadRequest, "finland_prh_xbrl statements_manifest downloads must be started through pull_source with registered_date_start and registered_date_end")
		return
	}

	fileRunID := uuid.New()
	workflowID := companysourceworkflows.FileRunWorkflowID(fileRunID.String())
	fileRun, err := h.db.CreateSourceFileRun(r.Context(), db.CreateSourceFileRunParams{
		ID:                 fileRunID,
		TemporalWorkflowID: optionalStringPointer(workflowID),
		SourceFileID:       file.ID,
	})
	if err != nil {
		slog.ErrorContext(r.Context(), "create source file run", "source", sourceName, "file_key", fileKey, "error", err)
		writeError(w, http.StatusInternalServerError, "create source file run failed")
		return
	}
	run, err := h.temporal.ExecuteWorkflow(r.Context(), client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: companysourceworkflows.SourceTaskQueue,
	}, companysourceworkflows.DownloadSourceFileWorkflowName, companysourceworkflows.DownloadSourceFileInput{
		FileRunID:  fileRun.ID.String(),
		SourceName: sourceName,
		FileKey:    fileKey,
		Trigger:    req.Trigger,
	})
	if err != nil {
		_, _ = h.db.FinishSourceFileRun(r.Context(), db.FinishSourceFileRunParams{
			Status:       companysourceworkflows.StatusFailed,
			ErrorMessage: "failed to start workflow",
			Log:          json.RawMessage(`[]`),
			ID:           fileRun.ID,
		})
		slog.ErrorContext(r.Context(), "start source file download workflow", "source", sourceName, "file_key", fileKey, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to start workflow")
		return
	}
	_ = h.db.UpdateSourceFileRunTemporalRunID(r.Context(), db.UpdateSourceFileRunTemporalRunIDParams{
		ID:            fileRun.ID,
		TemporalRunID: optionalStringPointer(run.GetRunID()),
	})
	writeJSON(w, http.StatusAccepted, startWorkflowResponse{
		Status:        "started",
		Workflow:      companysourceworkflows.DownloadSourceFileWorkflowName,
		TaskQueue:     companysourceworkflows.SourceTaskQueue,
		WorkflowID:    workflowID,
		WorkflowRunID: run.GetRunID(),
		RunID:         fileRun.ID.String(),
	})
}

func (h *Handlers) handleTriggerSourceFileImport(w http.ResponseWriter, r *http.Request) {
	if h.temporal == nil {
		writeError(w, http.StatusServiceUnavailable, "temporal client not available")
		return
	}
	sourceName := chi.URLParam(r, "name")
	fileKey := chi.URLParam(r, "file_key")
	req, err := decodeSourceActionTriggerRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.BatchSize <= 0 {
		req.BatchSize = 1000
	}

	action, err := h.db.GetSourceActionByName(r.Context(), db.GetSourceActionByNameParams{
		Name:   sourceName,
		Action: companysourceworkflows.ActionImportClickHouse,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "import clickhouse action not found")
			return
		}
		slog.ErrorContext(r.Context(), "get import source action", "source", sourceName, "file_key", fileKey, "error", err)
		writeError(w, http.StatusInternalServerError, "get import source action failed")
		return
	}
	if !action.Enabled {
		writeError(w, http.StatusUnprocessableEntity, "import clickhouse action is disabled")
		return
	}

	fileRun, err := h.db.GetLatestSuccessfulSourceFileRun(r.Context(), db.GetLatestSuccessfulSourceFileRunParams{
		SourceName: sourceName,
		FileKey:    fileKey,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "successful source file run not found")
			return
		}
		slog.ErrorContext(r.Context(), "get latest successful source file run", "source", sourceName, "file_key", fileKey, "error", err)
		writeError(w, http.StatusInternalServerError, "get latest successful source file run failed")
		return
	}

	actionRunID := uuid.New()
	workflowID := companysourceworkflows.ActionRunWorkflowID(actionRunID.String())
	input := companysourceworkflows.ImportSourceToClickHouseInput{
		ActionRunID: actionRunID.String(),
		SourceName:  sourceName,
		Trigger:     req.Trigger,
		FileRunIDs:  []string{fileRun.ID.String()},
		BatchSize:   req.BatchSize,
		Limit:       req.Limit,
	}
	actionRun, err := h.db.CreateSourceActionRun(r.Context(), db.CreateSourceActionRunParams{
		ID:                 actionRunID,
		TemporalWorkflowID: optionalStringPointer(workflowID),
		Input:              marshalJSON(input),
		ActionID:           action.ID,
	})
	if err != nil {
		slog.ErrorContext(r.Context(), "create source file import action run", "source", sourceName, "file_key", fileKey, "error", err)
		writeError(w, http.StatusInternalServerError, "create import action run failed")
		return
	}
	run, err := h.temporal.ExecuteWorkflow(r.Context(), client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: companysourceworkflows.SourceTaskQueue,
	}, companysourceworkflows.ImportSourceToClickHouseWorkflowName, input)
	if err != nil {
		_, _ = h.db.FinishSourceActionRun(r.Context(), db.FinishSourceActionRunParams{
			Status:       companysourceworkflows.StatusFailed,
			Result:       json.RawMessage(`{}`),
			ErrorMessage: "failed to start workflow",
			ID:           actionRun.ID,
		})
		slog.ErrorContext(r.Context(), "start source file import workflow", "source", sourceName, "file_key", fileKey, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to start workflow")
		return
	}
	_ = h.db.UpdateSourceActionRunTemporalRunID(r.Context(), db.UpdateSourceActionRunTemporalRunIDParams{
		ID:            actionRun.ID,
		TemporalRunID: optionalStringPointer(run.GetRunID()),
	})
	writeJSON(w, http.StatusAccepted, startWorkflowResponse{
		Status:        "started",
		Workflow:      companysourceworkflows.ImportSourceToClickHouseWorkflowName,
		TaskQueue:     companysourceworkflows.SourceTaskQueue,
		WorkflowID:    workflowID,
		WorkflowRunID: run.GetRunID(),
		RunID:         actionRun.ID.String(),
	})
}

func (h *Handlers) handleGetSourceFileRunTemporalStatus(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid file run id")
		return
	}
	run, err := h.db.GetSourceFileRunWithDefinition(r.Context(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "source file run not found")
			return
		}
		slog.ErrorContext(r.Context(), "get source file run", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "get source file run failed")
		return
	}
	h.writeTemporalStatus(w, r, run.ID.String(), run.Status, run.TemporalWorkflowID, run.TemporalRunID, run.StartedAt, timestampFromPG(run.FinishedAt))
}

func pathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func parseBoundedLimit(value string, fallback int, maximum int) int {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	limit, err := strconv.Atoi(value)
	if err != nil || limit < 1 {
		return fallback
	}
	if limit > maximum {
		return maximum
	}
	return limit
}

func pgUUIDString(value pgtype.UUID) *string {
	if !value.Valid {
		return nil
	}
	id := uuid.UUID(value.Bytes).String()
	return &id
}

func timestampFromPG(value pgtype.Timestamptz) *time.Time {
	if !value.Valid {
		return nil
	}
	t := value.Time
	return &t
}
