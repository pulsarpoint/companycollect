package httpapi

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/workflowschedules"
)

type workflowScheduleSpecRequest struct {
	Timezone             string `json:"timezone"`
	CronExpression       string `json:"cron_expression"`
	OverlapPolicy        string `json:"overlap_policy"`
	CatchupWindowSeconds int    `json:"catchup_window_seconds"`
}

type workflowScheduleRequest struct {
	TemporalScheduleID string                      `json:"temporal_schedule_id"`
	WorkflowKey        string                      `json:"workflow_key"`
	DisplayName        string                      `json:"display_name"`
	Description        string                      `json:"description"`
	Enabled            bool                        `json:"enabled"`
	Tags               []string                    `json:"tags"`
	Metadata           map[string]any              `json:"metadata"`
	Spec               workflowScheduleSpecRequest `json:"spec"`
	ActionInput        json.RawMessage             `json:"action_input"`
}

type workflowScheduleTemporalState struct {
	Exists    bool   `json:"exists"`
	Paused    bool   `json:"paused"`
	Note      string `json:"note"`
	NextRunAt string `json:"next_run_at,omitempty"`
}

type workflowScheduleResponse struct {
	ID                 string                        `json:"id"`
	TemporalScheduleID string                        `json:"temporal_schedule_id"`
	WorkflowKey        string                        `json:"workflow_key"`
	WorkflowName       string                        `json:"workflow_name"`
	TaskQueue          string                        `json:"task_queue"`
	Domain             string                        `json:"domain"`
	Purpose            string                        `json:"purpose"`
	DisplayName        string                        `json:"display_name"`
	Description        string                        `json:"description,omitempty"`
	Enabled            bool                          `json:"enabled"`
	Tags               []string                      `json:"tags"`
	Metadata           map[string]any                `json:"metadata"`
	Spec               workflowScheduleSpecRequest   `json:"spec"`
	ActionInput        map[string]any                `json:"action_input"`
	Temporal           workflowScheduleTemporalState `json:"temporal"`
	CreatedAt          string                        `json:"created_at"`
	UpdatedAt          string                        `json:"updated_at"`
}

type workflowScheduleListResponse struct {
	Items []workflowScheduleResponse `json:"items"`
}

type workflowScheduleStatusResponse struct {
	Status             string `json:"status"`
	TemporalScheduleID string `json:"temporal_schedule_id"`
	TemporalMissing    bool   `json:"temporal_missing,omitempty"`
}

type workflowScheduleNoteRequest struct {
	Note string `json:"note"`
}

func (h *Handlers) handleListWorkflowSchedules(w http.ResponseWriter, r *http.Request) {
	if h.db == nil {
		writeError(w, http.StatusServiceUnavailable, "workflow schedule metadata is not configured")
		return
	}
	rows, err := h.db.ListTemporalScheduleMetadata(r.Context(), db.ListTemporalScheduleMetadataParams{
		WorkflowKey: strings.TrimSpace(r.URL.Query().Get("workflow_key")),
		Domain:      strings.TrimSpace(r.URL.Query().Get("domain")),
	})
	if err != nil {
		slog.ErrorContext(r.Context(), "list temporal schedule metadata", "error", err)
		writeError(w, http.StatusInternalServerError, "list workflow schedules failed")
		return
	}

	items := make([]workflowScheduleResponse, 0, len(rows))
	for _, row := range rows {
		item, describeErr := h.workflowScheduleResponse(r.Context(), row)
		if describeErr != nil {
			slog.WarnContext(r.Context(), "describe temporal schedule for list", "error", describeErr, "schedule_id", row.TemporalScheduleID)
		}
		items = append(items, item)
	}
	writeJSON(w, http.StatusOK, workflowScheduleListResponse{Items: items})
}

func (h *Handlers) handleGetWorkflowSchedule(w http.ResponseWriter, r *http.Request) {
	if h.db == nil {
		writeError(w, http.StatusServiceUnavailable, "workflow schedule metadata is not configured")
		return
	}
	scheduleID := strings.TrimSpace(chi.URLParam(r, "schedule_id"))
	row, err := h.db.GetTemporalScheduleMetadata(r.Context(), scheduleID)
	if err != nil {
		if stderrors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "workflow schedule not found")
			return
		}
		slog.ErrorContext(r.Context(), "get temporal schedule metadata", "error", err, "schedule_id", scheduleID)
		writeError(w, http.StatusInternalServerError, "get workflow schedule failed")
		return
	}
	item, describeErr := h.workflowScheduleResponse(r.Context(), row)
	if describeErr != nil {
		slog.WarnContext(r.Context(), "describe temporal schedule for get", "error", describeErr, "schedule_id", row.TemporalScheduleID)
	}
	writeJSON(w, http.StatusOK, item)
}

func (h *Handlers) handleCreateWorkflowSchedule(w http.ResponseWriter, r *http.Request) {
	if h.temporal == nil {
		writeError(w, http.StatusServiceUnavailable, "temporal client not available")
		return
	}
	if h.db == nil {
		writeError(w, http.StatusServiceUnavailable, "workflow schedule metadata is not configured")
		return
	}

	req, err := decodeWorkflowScheduleRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	def, options, err := buildWorkflowScheduleOptions(req, h.naceSourceURL)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	scheduleClient := h.temporal.ScheduleClient()
	if _, err := scheduleClient.Create(r.Context(), options); err != nil {
		slog.ErrorContext(r.Context(), "create temporal schedule", "error", err, "schedule_id", req.TemporalScheduleID, "workflow_key", req.WorkflowKey)
		writeError(w, http.StatusBadGateway, "create temporal schedule failed")
		return
	}

	row, err := h.db.CreateTemporalScheduleMetadata(r.Context(), db.CreateTemporalScheduleMetadataParams{
		TemporalScheduleID: req.TemporalScheduleID,
		WorkflowKey:        def.Key,
		WorkflowName:       def.WorkflowName,
		TaskQueue:          def.TaskQueue,
		Domain:             def.Domain,
		Purpose:            def.Purpose,
		DisplayName:        req.DisplayName,
		Description:        optionalString(req.Description),
		Enabled:            req.Enabled,
		Tags:               normalizeScheduleTags(req.Tags),
		Metadata:           marshalJSONObject(req.Metadata),
	})
	if err != nil {
		_ = scheduleClient.GetHandle(r.Context(), req.TemporalScheduleID).Delete(r.Context())
		slog.ErrorContext(r.Context(), "store temporal schedule metadata", "error", err, "schedule_id", req.TemporalScheduleID)
		writeError(w, http.StatusInternalServerError, "store schedule metadata failed")
		return
	}

	item, describeErr := h.workflowScheduleResponse(r.Context(), row)
	if describeErr != nil {
		slog.WarnContext(r.Context(), "describe created temporal schedule", "error", describeErr, "schedule_id", row.TemporalScheduleID)
	}
	writeJSON(w, http.StatusCreated, item)
}

func (h *Handlers) handleUpdateWorkflowSchedule(w http.ResponseWriter, r *http.Request) {
	if h.temporal == nil {
		writeError(w, http.StatusServiceUnavailable, "temporal client not available")
		return
	}
	if h.db == nil {
		writeError(w, http.StatusServiceUnavailable, "workflow schedule metadata is not configured")
		return
	}

	scheduleID := strings.TrimSpace(chi.URLParam(r, "schedule_id"))
	req, err := decodeWorkflowScheduleUpdateRequest(r, scheduleID)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	def, options, err := buildWorkflowScheduleOptions(req, h.naceSourceURL)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	var currentMetadata db.TemporalScheduleMetadatum
	currentMetadata, err = h.db.GetTemporalScheduleMetadata(r.Context(), scheduleID)
	if err != nil {
		if stderrors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "workflow schedule not found")
			return
		}
		slog.ErrorContext(r.Context(), "get temporal schedule metadata before update", "error", err, "schedule_id", scheduleID)
		writeError(w, http.StatusInternalServerError, "get workflow schedule failed")
		return
	}

	handle := h.temporal.ScheduleClient().GetHandle(r.Context(), scheduleID)
	previousDescription, err := handle.Describe(r.Context())
	if err != nil {
		slog.ErrorContext(r.Context(), "describe temporal schedule before update", "error", err, "schedule_id", scheduleID)
		writeError(w, http.StatusBadGateway, "update temporal schedule failed")
		return
	}
	if err := handle.Update(r.Context(), client.ScheduleUpdateOptions{
		DoUpdate: func(input client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
			paused := schedulePaused(input.Description)
			note := scheduleNote(input.Description)
			if !req.Enabled {
				paused = true
				note = "Paused by Corpscout because schedule is disabled"
			} else if !currentMetadata.Enabled && req.Enabled {
				paused = false
				note = ""
			}
			return &client.ScheduleUpdate{
				Schedule: &client.Schedule{
					Action: options.Action,
					Spec:   &options.Spec,
					Policy: &client.SchedulePolicies{
						Overlap:       options.Overlap,
						CatchupWindow: options.CatchupWindow,
					},
					State: &client.ScheduleState{
						Paused: paused,
						Note:   note,
					},
				},
			}, nil
		},
	}); err != nil {
		slog.ErrorContext(r.Context(), "update temporal schedule", "error", err, "schedule_id", scheduleID)
		writeError(w, http.StatusBadGateway, "update temporal schedule failed")
		return
	}

	row, err := h.db.UpdateTemporalScheduleMetadata(r.Context(), db.UpdateTemporalScheduleMetadataParams{
		TemporalScheduleID: scheduleID,
		WorkflowKey:        def.Key,
		WorkflowName:       def.WorkflowName,
		TaskQueue:          def.TaskQueue,
		Domain:             def.Domain,
		Purpose:            def.Purpose,
		DisplayName:        req.DisplayName,
		Description:        optionalString(req.Description),
		Enabled:            req.Enabled,
		Tags:               normalizeScheduleTags(req.Tags),
		Metadata:           marshalJSONObject(req.Metadata),
	})
	if err != nil {
		if restoreErr := restoreTemporalSchedule(r.Context(), handle, previousDescription); restoreErr != nil {
			slog.ErrorContext(r.Context(), "restore temporal schedule after metadata update failure", "error", restoreErr, "schedule_id", scheduleID)
		}
		slog.ErrorContext(r.Context(), "update temporal schedule metadata", "error", err, "schedule_id", scheduleID)
		writeError(w, http.StatusInternalServerError, "update schedule metadata failed")
		return
	}

	item, describeErr := h.workflowScheduleResponse(r.Context(), row)
	if describeErr != nil {
		slog.WarnContext(r.Context(), "describe updated temporal schedule", "error", describeErr, "schedule_id", row.TemporalScheduleID)
	}
	writeJSON(w, http.StatusOK, item)
}

func (h *Handlers) handleTriggerWorkflowSchedule(w http.ResponseWriter, r *http.Request) {
	if h.temporal == nil {
		writeError(w, http.StatusServiceUnavailable, "temporal client not available")
		return
	}
	if h.db == nil {
		writeError(w, http.StatusServiceUnavailable, "workflow schedule metadata is not configured")
		return
	}
	scheduleID := strings.TrimSpace(chi.URLParam(r, "schedule_id"))
	if scheduleID == "" {
		writeError(w, http.StatusBadRequest, "schedule id is required")
		return
	}
	if _, err := h.db.GetTemporalScheduleMetadata(r.Context(), scheduleID); err != nil {
		if stderrors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "workflow schedule not found")
			return
		}
		slog.ErrorContext(r.Context(), "get temporal schedule metadata before trigger", "error", err, "schedule_id", scheduleID)
		writeError(w, http.StatusInternalServerError, "get workflow schedule failed")
		return
	}
	if err := h.temporal.ScheduleClient().GetHandle(r.Context(), scheduleID).Trigger(r.Context(), client.ScheduleTriggerOptions{}); err != nil {
		slog.ErrorContext(r.Context(), "trigger temporal schedule", "error", err, "schedule_id", scheduleID)
		writeError(w, http.StatusBadGateway, "trigger workflow schedule failed")
		return
	}
	writeJSON(w, http.StatusOK, workflowScheduleStatusResponse{Status: "triggered", TemporalScheduleID: scheduleID})
}

func (h *Handlers) handlePauseWorkflowSchedule(w http.ResponseWriter, r *http.Request) {
	if h.temporal == nil {
		writeError(w, http.StatusServiceUnavailable, "temporal client not available")
		return
	}
	if h.db == nil {
		writeError(w, http.StatusServiceUnavailable, "workflow schedule metadata is not configured")
		return
	}
	scheduleID := strings.TrimSpace(chi.URLParam(r, "schedule_id"))
	note := decodeWorkflowScheduleNote(r)
	if _, err := h.db.GetTemporalScheduleMetadata(r.Context(), scheduleID); err != nil {
		if stderrors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "workflow schedule not found")
			return
		}
		slog.ErrorContext(r.Context(), "get temporal schedule metadata before pause", "error", err, "schedule_id", scheduleID)
		writeError(w, http.StatusInternalServerError, "update schedule metadata failed")
		return
	}
	if err := h.temporal.ScheduleClient().GetHandle(r.Context(), scheduleID).Pause(r.Context(), client.SchedulePauseOptions{Note: note}); err != nil {
		slog.ErrorContext(r.Context(), "pause temporal schedule", "error", err, "schedule_id", scheduleID)
		writeError(w, http.StatusBadGateway, "pause workflow schedule failed")
		return
	}
	writeJSON(w, http.StatusOK, workflowScheduleStatusResponse{Status: "paused", TemporalScheduleID: scheduleID})
}

func (h *Handlers) handleResumeWorkflowSchedule(w http.ResponseWriter, r *http.Request) {
	if h.temporal == nil {
		writeError(w, http.StatusServiceUnavailable, "temporal client not available")
		return
	}
	if h.db == nil {
		writeError(w, http.StatusServiceUnavailable, "workflow schedule metadata is not configured")
		return
	}
	scheduleID := strings.TrimSpace(chi.URLParam(r, "schedule_id"))
	note := decodeWorkflowScheduleNote(r)
	if _, err := h.db.GetTemporalScheduleMetadata(r.Context(), scheduleID); err != nil {
		if stderrors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "workflow schedule not found")
			return
		}
		slog.ErrorContext(r.Context(), "get temporal schedule metadata before resume", "error", err, "schedule_id", scheduleID)
		writeError(w, http.StatusInternalServerError, "update schedule metadata failed")
		return
	}
	if err := h.temporal.ScheduleClient().GetHandle(r.Context(), scheduleID).Unpause(r.Context(), client.ScheduleUnpauseOptions{Note: note}); err != nil {
		slog.ErrorContext(r.Context(), "resume temporal schedule", "error", err, "schedule_id", scheduleID)
		writeError(w, http.StatusBadGateway, "resume workflow schedule failed")
		return
	}
	writeJSON(w, http.StatusOK, workflowScheduleStatusResponse{Status: "resumed", TemporalScheduleID: scheduleID})
}

func (h *Handlers) handleDeleteWorkflowSchedule(w http.ResponseWriter, r *http.Request) {
	if h.temporal == nil {
		writeError(w, http.StatusServiceUnavailable, "temporal client not available")
		return
	}
	if h.db == nil {
		writeError(w, http.StatusServiceUnavailable, "workflow schedule metadata is not configured")
		return
	}
	scheduleID := strings.TrimSpace(chi.URLParam(r, "schedule_id"))
	if _, err := h.db.GetTemporalScheduleMetadata(r.Context(), scheduleID); err != nil {
		if stderrors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "workflow schedule not found")
			return
		}
		slog.ErrorContext(r.Context(), "get temporal schedule metadata before delete", "error", err, "schedule_id", scheduleID)
		writeError(w, http.StatusInternalServerError, "get workflow schedule failed")
		return
	}
	temporalMissing := false
	if err := h.temporal.ScheduleClient().GetHandle(r.Context(), scheduleID).Delete(r.Context()); err != nil {
		var notFound *serviceerror.NotFound
		if stderrors.As(err, &notFound) {
			temporalMissing = true
		} else {
			slog.ErrorContext(r.Context(), "delete temporal schedule", "error", err, "schedule_id", scheduleID)
			writeError(w, http.StatusBadGateway, "delete temporal schedule failed")
			return
		}
	}
	if err := h.db.DeleteTemporalScheduleMetadata(r.Context(), scheduleID); err != nil {
		slog.ErrorContext(r.Context(), "delete temporal schedule metadata", "error", err, "schedule_id", scheduleID)
		writeError(w, http.StatusInternalServerError, "delete schedule metadata failed")
		return
	}
	writeJSON(w, http.StatusOK, workflowScheduleStatusResponse{
		Status:             "deleted",
		TemporalScheduleID: scheduleID,
		TemporalMissing:    temporalMissing,
	})
}

func decodeWorkflowScheduleRequest(r *http.Request) (workflowScheduleRequest, error) {
	return decodeWorkflowScheduleRequestBody(r, true)
}

func decodeWorkflowScheduleUpdateRequest(r *http.Request, scheduleID string) (workflowScheduleRequest, error) {
	req, err := decodeWorkflowScheduleRequestBody(r, false)
	if err != nil {
		return workflowScheduleRequest{}, err
	}
	if req.TemporalScheduleID == "" {
		req.TemporalScheduleID = scheduleID
	}
	if req.TemporalScheduleID != scheduleID {
		return workflowScheduleRequest{}, errors.New("temporal schedule id cannot be changed")
	}
	return req, nil
}

func decodeWorkflowScheduleRequestBody(r *http.Request, requireScheduleID bool) (workflowScheduleRequest, error) {
	var req workflowScheduleRequest
	if err := decodeJSON(r, &req); err != nil {
		return workflowScheduleRequest{}, errors.New("invalid request body")
	}
	req.TemporalScheduleID = strings.TrimSpace(req.TemporalScheduleID)
	req.WorkflowKey = strings.TrimSpace(req.WorkflowKey)
	req.DisplayName = strings.TrimSpace(req.DisplayName)
	req.Description = strings.TrimSpace(req.Description)
	req.Spec.CronExpression = strings.TrimSpace(req.Spec.CronExpression)
	req.Spec.Timezone = strings.TrimSpace(req.Spec.Timezone)
	req.Spec.OverlapPolicy = strings.TrimSpace(req.Spec.OverlapPolicy)
	if requireScheduleID && req.TemporalScheduleID == "" {
		return workflowScheduleRequest{}, errors.New("temporal schedule id is required")
	}
	if strings.ContainsAny(req.TemporalScheduleID, " \t\r\n/") {
		return workflowScheduleRequest{}, errors.New("temporal schedule id cannot contain whitespace or slash")
	}
	if req.WorkflowKey == "" {
		return workflowScheduleRequest{}, errors.New("workflow key is required")
	}
	if req.DisplayName == "" {
		return workflowScheduleRequest{}, errors.New("display name is required")
	}
	return req, nil
}

func buildWorkflowScheduleOptions(req workflowScheduleRequest, defaultNACESourceURL string) (workflowschedules.Definition, client.ScheduleOptions, error) {
	def, ok := workflowschedules.DefinitionByKey(req.WorkflowKey)
	if !ok {
		return workflowschedules.Definition{}, client.ScheduleOptions{}, errors.New("workflow key is not schedulable")
	}
	spec, err := workflowschedules.BuildScheduleSpec(workflowschedules.ScheduleSpecInput{
		CronExpression:       req.Spec.CronExpression,
		Timezone:             req.Spec.Timezone,
		OverlapPolicy:        req.Spec.OverlapPolicy,
		CatchupWindowSeconds: req.Spec.CatchupWindowSeconds,
	})
	if err != nil {
		return workflowschedules.Definition{}, client.ScheduleOptions{}, err
	}
	overlap, err := workflowschedules.OverlapPolicy(req.Spec.OverlapPolicy)
	if err != nil {
		return workflowschedules.Definition{}, client.ScheduleOptions{}, err
	}
	req, err = applyWorkflowScheduleActionDefaults(req, defaultNACESourceURL)
	if err != nil {
		return workflowschedules.Definition{}, client.ScheduleOptions{}, err
	}
	actionInput, err := def.DecodeActionInput(req.ActionInput)
	if err != nil {
		return workflowschedules.Definition{}, client.ScheduleOptions{}, err
	}
	note := ""
	if !req.Enabled {
		note = "Paused by Corpscout because schedule is disabled"
	}
	return def, client.ScheduleOptions{
		ID:            req.TemporalScheduleID,
		Spec:          spec,
		Overlap:       overlap,
		CatchupWindow: time.Duration(req.Spec.CatchupWindowSeconds) * time.Second,
		Paused:        !req.Enabled,
		Note:          note,
		Action: &client.ScheduleWorkflowAction{
			ID:        req.TemporalScheduleID + "-workflow",
			Workflow:  def.WorkflowName,
			TaskQueue: def.TaskQueue,
			Args:      []interface{}{actionInput},
		},
	}, nil
}

func applyWorkflowScheduleActionDefaults(req workflowScheduleRequest, defaultNACESourceURL string) (workflowScheduleRequest, error) {
	if req.WorkflowKey != "nace_taxonomy_sync" {
		return req, nil
	}
	input := map[string]any{}
	if len(req.ActionInput) > 0 && strings.TrimSpace(string(req.ActionInput)) != "null" {
		if err := json.Unmarshal(req.ActionInput, &input); err != nil {
			return workflowScheduleRequest{}, errors.New("invalid nace taxonomy sync action input")
		}
		if input == nil {
			input = map[string]any{}
		}
	}
	if strings.TrimSpace(stringValue(input["source_url"])) == "" {
		sourceURL := strings.TrimSpace(defaultNACESourceURL)
		if sourceURL == "" {
			return workflowScheduleRequest{}, errors.New("nace source url is required")
		}
		input["source_url"] = sourceURL
	}
	sourceURL := strings.TrimSpace(stringValue(input["source_url"]))
	parsedURL, err := url.Parse(sourceURL)
	if err != nil || parsedURL.Scheme == "" || parsedURL.Host == "" {
		return workflowScheduleRequest{}, errors.New("nace source url must be http or https")
	}
	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		return workflowScheduleRequest{}, errors.New("nace source url must be http or https")
	}
	if strings.TrimSpace(stringValue(input["trigger"])) == "" {
		input["trigger"] = "schedule"
	}
	body, err := json.Marshal(input)
	if err != nil {
		return workflowScheduleRequest{}, errors.New("invalid nace taxonomy sync action input")
	}
	req.ActionInput = body
	return req, nil
}

func (h *Handlers) workflowScheduleResponse(ctx context.Context, row db.TemporalScheduleMetadatum) (workflowScheduleResponse, error) {
	response := workflowScheduleResponse{
		ID:                 row.ID.String(),
		TemporalScheduleID: row.TemporalScheduleID,
		WorkflowKey:        row.WorkflowKey,
		WorkflowName:       row.WorkflowName,
		TaskQueue:          row.TaskQueue,
		Domain:             row.Domain,
		Purpose:            row.Purpose,
		DisplayName:        row.DisplayName,
		Description:        valueString(row.Description),
		Enabled:            row.Enabled,
		Tags:               row.Tags,
		Metadata:           unmarshalJSONObject(row.Metadata),
		Spec: workflowScheduleSpecRequest{
			Timezone:             workflowschedules.DefaultTimezone,
			CronExpression:       "",
			OverlapPolicy:        "skip",
			CatchupWindowSeconds: 0,
		},
		ActionInput: map[string]any{},
		Temporal:    workflowScheduleTemporalState{Exists: false},
		CreatedAt:   row.CreatedAt.Format(time.RFC3339),
		UpdatedAt:   row.UpdatedAt.Format(time.RFC3339),
	}
	if h.temporal == nil {
		return response, nil
	}
	description, err := h.temporal.ScheduleClient().GetHandle(ctx, row.TemporalScheduleID).Describe(ctx)
	if err != nil {
		return response, err
	}
	response.Temporal = temporalStateFromDescription(description)
	response.Spec = scheduleSpecRequestFromDescription(description)
	response.ActionInput = actionInputFromDescription(description)
	return response, nil
}

func temporalStateFromDescription(description *client.ScheduleDescription) workflowScheduleTemporalState {
	if description == nil {
		return workflowScheduleTemporalState{Exists: false}
	}
	state := workflowScheduleTemporalState{Exists: true}
	if description.Schedule.State != nil {
		state.Paused = description.Schedule.State.Paused
		state.Note = description.Schedule.State.Note
	}
	if len(description.Info.NextActionTimes) > 0 {
		state.NextRunAt = description.Info.NextActionTimes[0].Format(time.RFC3339)
	}
	return state
}

func schedulePaused(description client.ScheduleDescription) bool {
	if description.Schedule.State == nil {
		return false
	}
	return description.Schedule.State.Paused
}

func scheduleNote(description client.ScheduleDescription) string {
	if description.Schedule.State == nil {
		return ""
	}
	return description.Schedule.State.Note
}

func scheduleSpecRequestFromDescription(description *client.ScheduleDescription) workflowScheduleSpecRequest {
	spec := workflowScheduleSpecRequest{
		Timezone:      workflowschedules.DefaultTimezone,
		OverlapPolicy: "skip",
	}
	if description == nil {
		return spec
	}
	if description.Schedule.Spec != nil {
		spec.CronExpression = strings.Join(description.Schedule.Spec.CronExpressions, " ")
		if description.Schedule.Spec.TimeZoneName != "" {
			spec.Timezone = description.Schedule.Spec.TimeZoneName
		}
	}
	if description.Schedule.Policy != nil {
		spec.OverlapPolicy = overlapPolicyString(description.Schedule.Policy.Overlap)
		spec.CatchupWindowSeconds = int(description.Schedule.Policy.CatchupWindow.Seconds())
	}
	return spec
}

func actionInputFromDescription(description *client.ScheduleDescription) map[string]any {
	if description == nil {
		return map[string]any{}
	}
	action, ok := description.Schedule.Action.(*client.ScheduleWorkflowAction)
	if !ok || len(action.Args) == 0 {
		return map[string]any{}
	}
	firstArg := action.Args[0]
	if payload, ok := firstArg.(*commonpb.Payload); ok {
		var decoded map[string]any
		if err := converter.GetDefaultDataConverter().FromPayload(payload, &decoded); err == nil {
			return decoded
		}
		return map[string]any{}
	}
	return mapFromValue(firstArg)
}

func overlapPolicyString(policy enumspb.ScheduleOverlapPolicy) string {
	switch policy {
	case enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE:
		return "buffer_one"
	case enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL:
		return "allow_all"
	case enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER:
		return "cancel_other"
	case enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER:
		return "terminate_other"
	default:
		return "skip"
	}
}

func decodeWorkflowScheduleNote(r *http.Request) string {
	var req workflowScheduleNoteRequest
	if err := decodeJSON(r, &req); err != nil {
		return ""
	}
	return strings.TrimSpace(req.Note)
}

func optionalString(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}

func valueString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func normalizeScheduleTags(tags []string) []string {
	seen := make(map[string]struct{}, len(tags))
	normalized := make([]string, 0, len(tags))
	for _, tag := range tags {
		tag = strings.TrimSpace(tag)
		if tag == "" {
			continue
		}
		if _, ok := seen[tag]; ok {
			continue
		}
		seen[tag] = struct{}{}
		normalized = append(normalized, tag)
	}
	return normalized
}

func marshalJSONObject(value map[string]any) json.RawMessage {
	if value == nil {
		value = map[string]any{}
	}
	body, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return body
}

func unmarshalJSONObject(body json.RawMessage) map[string]any {
	if len(body) == 0 {
		return map[string]any{}
	}
	var value map[string]any
	if err := json.Unmarshal(body, &value); err != nil || value == nil {
		return map[string]any{}
	}
	return value
}

func mapFromValue(value any) map[string]any {
	body, err := json.Marshal(value)
	if err != nil {
		return map[string]any{}
	}
	var result map[string]any
	if err := json.Unmarshal(body, &result); err != nil || result == nil {
		return map[string]any{}
	}
	return result
}

func stringValue(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	return ""
}

func restoreTemporalSchedule(ctx context.Context, handle client.ScheduleHandle, description *client.ScheduleDescription) error {
	if description == nil {
		return nil
	}
	return handle.Update(ctx, client.ScheduleUpdateOptions{
		DoUpdate: func(client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
			schedule := description.Schedule
			return &client.ScheduleUpdate{Schedule: &schedule}, nil
		},
	})
}
