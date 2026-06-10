package httpapi

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.temporal.io/sdk/client"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	companysourceworkflows "github.com/pulsarpoint/corpscout/scheduler/internal/temporal/workflow/companysources"
)

const defaultSourceActionRunsLimit = 20
const maxSourceActionRunsLimit = 100

type sourceActionTriggerRequest struct {
	Trigger             string `json:"trigger"`
	DownloadActionRunID string `json:"download_action_run_id,omitempty"`
	BatchSize           int    `json:"batch_size,omitempty"`
	Limit               int64  `json:"limit,omitempty"`
}

type sourceActionRunResponse struct {
	db.DataSourceActionRun
	SourceName string `json:"source_name"`
}

func (h *Handlers) handleListSourceActions(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	rows, err := h.db.ListSourceActions(r.Context(), name)
	if err != nil {
		slog.ErrorContext(r.Context(), "list source actions", "source", name, "error", err)
		writeError(w, http.StatusInternalServerError, "list source actions failed")
		return
	}
	if rows == nil {
		rows = []db.ListSourceActionsRow{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": rows})
}

func (h *Handlers) handleListSourceActionRuns(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	limit, err := parseSourceActionRunsLimit(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	rows, err := h.db.ListSourceActionRuns(r.Context(), db.ListSourceActionRunsParams{
		Name:  name,
		Limit: limit,
	})
	if err != nil {
		slog.ErrorContext(r.Context(), "list source action runs", "source", name, "error", err)
		writeError(w, http.StatusInternalServerError, "list source action runs failed")
		return
	}
	if rows == nil {
		rows = []db.ListSourceActionRunsRow{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": rows})
}

func (h *Handlers) handleGetLatestSuccessfulSourceDownload(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	run, err := h.db.GetLatestSuccessfulSourceActionRun(r.Context(), db.GetLatestSuccessfulSourceActionRunParams{
		Name:   name,
		Action: companysourceworkflows.ActionPullSource,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "successful download not found")
			return
		}
		slog.ErrorContext(r.Context(), "get latest successful source download", "source", name, "error", err)
		writeError(w, http.StatusInternalServerError, "get latest successful source download failed")
		return
	}
	writeJSON(w, http.StatusOK, sourceActionRunResponse{
		DataSourceActionRun: run,
		SourceName:          name,
	})
}

func (h *Handlers) handleTriggerSourceAction(w http.ResponseWriter, r *http.Request) {
	if h.temporal == nil {
		writeError(w, http.StatusServiceUnavailable, "temporal client not available")
		return
	}

	sourceName := chi.URLParam(r, "name")
	actionKey := chi.URLParam(r, "action")
	req, err := decodeSourceActionTriggerRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	action, err := h.db.GetSourceActionByName(r.Context(), db.GetSourceActionByNameParams{
		Name:   sourceName,
		Action: actionKey,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "source action not found")
			return
		}
		slog.ErrorContext(r.Context(), "get source action", "source", sourceName, "action", actionKey, "error", err)
		writeError(w, http.StatusInternalServerError, "get source action failed")
		return
	}
	if !action.Enabled {
		writeError(w, http.StatusUnprocessableEntity, "source action is disabled")
		return
	}

	workflowName, err := sourceActionWorkflowName(action.Action, action.TemporalWorkflowType)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	actionRunID := uuid.New()
	workflowID := companysourceworkflows.ActionRunWorkflowID(actionRunID.String())
	input, err := sourceActionWorkflowInput(action.Action, sourceName, actionRunID.String(), req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	actionRun, err := h.db.CreateSourceActionRun(r.Context(), db.CreateSourceActionRunParams{
		ID:                 actionRunID,
		TemporalWorkflowID: optionalStringPointer(workflowID),
		Input:              marshalJSON(input),
		ActionID:           action.ID,
	})
	if err != nil {
		slog.ErrorContext(r.Context(), "create source action run", "source", sourceName, "action", actionKey, "error", err)
		writeError(w, http.StatusInternalServerError, "create source action run failed")
		return
	}
	run, err := h.temporal.ExecuteWorkflow(r.Context(), client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: companysourceworkflows.SourceTaskQueue,
	}, workflowName, input)
	if err != nil {
		_, _ = h.db.FinishSourceActionRun(r.Context(), db.FinishSourceActionRunParams{
			Status:       companysourceworkflows.StatusFailed,
			Result:       json.RawMessage(`{}`),
			ErrorMessage: "failed to start workflow",
			ID:           actionRun.ID,
		})
		slog.ErrorContext(r.Context(), "start source action workflow", "source", sourceName, "action", actionKey, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to start workflow")
		return
	}
	_ = h.db.UpdateSourceActionRunTemporalRunID(r.Context(), db.UpdateSourceActionRunTemporalRunIDParams{
		ID:            actionRun.ID,
		TemporalRunID: optionalStringPointer(run.GetRunID()),
	})

	writeJSON(w, http.StatusAccepted, startWorkflowResponse{
		Status:        "started",
		Workflow:      workflowName,
		TaskQueue:     companysourceworkflows.SourceTaskQueue,
		WorkflowID:    workflowID,
		WorkflowRunID: run.GetRunID(),
		RunID:         actionRun.ID.String(),
	})
}

func (h *Handlers) handleTriggerSourceSyncClickHouse(w http.ResponseWriter, r *http.Request) {
	if h.temporal == nil {
		writeError(w, http.StatusServiceUnavailable, "temporal client not available")
		return
	}

	sourceName := chi.URLParam(r, "name")
	req, err := decodeSourceActionTriggerRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.BatchSize <= 0 {
		req.BatchSize = 1000
	}

	downloadAction, err := h.db.GetSourceActionByName(r.Context(), db.GetSourceActionByNameParams{
		Name:   sourceName,
		Action: companysourceworkflows.ActionPullSource,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "pull source action not found")
			return
		}
		slog.ErrorContext(r.Context(), "get pull source action", "source", sourceName, "error", err)
		writeError(w, http.StatusInternalServerError, "get pull source action failed")
		return
	}
	importAction, err := h.db.GetSourceActionByName(r.Context(), db.GetSourceActionByNameParams{
		Name:   sourceName,
		Action: companysourceworkflows.ActionImportClickHouse,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "import clickhouse action not found")
			return
		}
		slog.ErrorContext(r.Context(), "get import clickhouse action", "source", sourceName, "error", err)
		writeError(w, http.StatusInternalServerError, "get import clickhouse action failed")
		return
	}
	downloadActionRunID := uuid.New()
	importActionRunID := uuid.New()
	workflowID := newWorkflowID(strings.ReplaceAll(sourceName+"-sync-clickhouse", "_", "-"))
	downloadWorkflowID := companysourceworkflows.ActionRunWorkflowID(downloadActionRunID.String())
	importWorkflowID := companysourceworkflows.ActionRunWorkflowID(importActionRunID.String())
	syncInput := companysourceworkflows.SyncSourceToClickHouseInput{
		DownloadActionRunID: downloadActionRunID.String(),
		ImportActionRunID:   importActionRunID.String(),
		SourceName:          sourceName,
		Trigger:             req.Trigger,
		BatchSize:           req.BatchSize,
		Limit:               req.Limit,
	}
	downloadActionRun, err := h.db.CreateSourceActionRun(r.Context(), db.CreateSourceActionRunParams{
		ID:                 downloadActionRunID,
		TemporalWorkflowID: optionalStringPointer(downloadWorkflowID),
		Input:              marshalJSON(syncInput),
		ActionID:           downloadAction.ID,
	})
	if err != nil {
		slog.ErrorContext(r.Context(), "create sync download action run", "source", sourceName, "error", err)
		writeError(w, http.StatusInternalServerError, "create download action run failed")
		return
	}
	importActionRun, err := h.db.CreateSourceActionRun(r.Context(), db.CreateSourceActionRunParams{
		ID:                 importActionRunID,
		TemporalWorkflowID: optionalStringPointer(importWorkflowID),
		Input:              marshalJSON(syncInput),
		ActionID:           importAction.ID,
	})
	if err != nil {
		_, _ = h.db.FinishSourceActionRun(r.Context(), db.FinishSourceActionRunParams{
			Status:       companysourceworkflows.StatusFailed,
			Result:       json.RawMessage(`{}`),
			ErrorMessage: "failed to create import action run",
			ID:           downloadActionRun.ID,
		})
		slog.ErrorContext(r.Context(), "create sync import action run", "source", sourceName, "error", err)
		writeError(w, http.StatusInternalServerError, "create import action run failed")
		return
	}
	run, err := h.temporal.ExecuteWorkflow(r.Context(), client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: companysourceworkflows.SourceTaskQueue,
	}, companysourceworkflows.SyncSourceToClickHouseWorkflowName, syncInput)
	if err != nil {
		for _, actionRunID := range []uuid.UUID{downloadActionRun.ID, importActionRun.ID} {
			_, _ = h.db.FinishSourceActionRun(r.Context(), db.FinishSourceActionRunParams{
				Status:       companysourceworkflows.StatusFailed,
				Result:       json.RawMessage(`{}`),
				ErrorMessage: "failed to start workflow",
				ID:           actionRunID,
			})
		}
		slog.ErrorContext(r.Context(), "start source sync workflow", "source", sourceName, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to start workflow")
		return
	}

	writeJSON(w, http.StatusAccepted, startWorkflowResponse{
		Status:        "started",
		Workflow:      companysourceworkflows.SyncSourceToClickHouseWorkflowName,
		TaskQueue:     companysourceworkflows.SourceTaskQueue,
		WorkflowID:    workflowID,
		WorkflowRunID: run.GetRunID(),
		RunID:         downloadActionRun.ID.String(),
	})
}

func decodeSourceActionTriggerRequest(r *http.Request) (sourceActionTriggerRequest, error) {
	var req sourceActionTriggerRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		return sourceActionTriggerRequest{}, errors.New("invalid request body")
	}
	req.Trigger = strings.TrimSpace(req.Trigger)
	req.DownloadActionRunID = strings.TrimSpace(req.DownloadActionRunID)
	if req.Trigger == "" {
		req.Trigger = "manual"
	}
	return req, nil
}

func sourceActionWorkflowName(action string, configuredWorkflow string) (string, error) {
	switch action {
	case companysourceworkflows.ActionPullSource:
		return nonEmptyString(configuredWorkflow, companysourceworkflows.DownloadSourceWorkflowName), nil
	case companysourceworkflows.ActionImportClickHouse:
		return nonEmptyString(configuredWorkflow, companysourceworkflows.ImportSourceToClickHouseWorkflowName), nil
	case companysourceworkflows.ActionRefreshExplorerCache:
		return nonEmptyString(configuredWorkflow, companysourceworkflows.RefreshSourceExplorerCacheWorkflowName), nil
	default:
		return "", errors.New("unsupported source action")
	}
}

func sourceActionWorkflowInput(action string, sourceName string, actionRunID string, req sourceActionTriggerRequest) (any, error) {
	switch action {
	case companysourceworkflows.ActionPullSource:
		return companysourceworkflows.SyncSourceDownloadInput{
			ActionRunID: actionRunID,
			SourceName:  sourceName,
			Trigger:     req.Trigger,
		}, nil
	case companysourceworkflows.ActionImportClickHouse:
		if req.BatchSize <= 0 {
			req.BatchSize = 1000
		}
		return companysourceworkflows.ImportSourceToClickHouseInput{
			ActionRunID:         actionRunID,
			SourceName:          sourceName,
			Trigger:             req.Trigger,
			DownloadActionRunID: req.DownloadActionRunID,
			BatchSize:           req.BatchSize,
			Limit:               req.Limit,
		}, nil
	case companysourceworkflows.ActionRefreshExplorerCache:
		return companysourceworkflows.RefreshSourceExplorerCacheInput{
			ActionRunID: actionRunID,
			SourceName:  sourceName,
			Trigger:     req.Trigger,
		}, nil
	default:
		return nil, errors.New("unsupported source action")
	}
}

func parseSourceActionRunsLimit(r *http.Request) (int32, error) {
	value := strings.TrimSpace(r.URL.Query().Get("limit"))
	if value == "" {
		return defaultSourceActionRunsLimit, nil
	}
	limit, err := strconv.Atoi(value)
	if err != nil || limit < 1 {
		return 0, errors.New("limit must be a positive integer")
	}
	if limit > maxSourceActionRunsLimit {
		limit = maxSourceActionRunsLimit
	}
	return int32(limit), nil
}

func (h *Handlers) handleGetSourceActionRunTemporalStatus(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid action run id")
		return
	}
	run, err := h.db.GetSourceActionRun(r.Context(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "source action run not found")
			return
		}
		slog.ErrorContext(r.Context(), "get source action run", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "get source action run failed")
		return
	}
	h.writeTemporalStatus(w, r, run.ID.String(), run.Status, run.TemporalWorkflowID, run.TemporalRunID, run.StartedAt, timestampFromPG(run.FinishedAt))
}

func (h *Handlers) writeTemporalStatus(w http.ResponseWriter, r *http.Request, id string, status string, workflowID *string, runID *string, startedAt time.Time, finishedAt *time.Time) {
	response := map[string]any{
		"id":          id,
		"db_status":   status,
		"started_at":  startedAt,
		"finished_at": finishedAt,
	}
	if workflowID != nil {
		response["workflow_id"] = *workflowID
	}
	if runID != nil {
		response["workflow_run_id"] = *runID
	}
	if h.temporal == nil || workflowID == nil || strings.TrimSpace(*workflowID) == "" {
		writeJSON(w, http.StatusOK, response)
		return
	}
	describeRunID := ""
	if runID != nil {
		describeRunID = *runID
	}
	description, err := h.temporal.DescribeWorkflowExecution(r.Context(), *workflowID, describeRunID)
	if err != nil {
		slog.ErrorContext(r.Context(), "describe source temporal workflow", "workflow_id", *workflowID, "error", err)
		response["temporal_status_error"] = "describe workflow failed"
		writeJSON(w, http.StatusOK, response)
		return
	}
	if executionInfo := description.GetWorkflowExecutionInfo(); executionInfo != nil {
		response["temporal_status"] = executionInfo.GetStatus().String()
	}
	writeJSON(w, http.StatusOK, response)
}

func marshalJSON(value any) json.RawMessage {
	data, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return data
}

func optionalStringPointer(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func nonEmptyString(value string, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
