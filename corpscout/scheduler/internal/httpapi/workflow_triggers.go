package httpapi

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"go.temporal.io/sdk/client"

	brregworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/workflow"
)

type startWorkflowResponse struct {
	Status        string `json:"status"`
	Workflow      string `json:"workflow"`
	WorkflowID    string `json:"workflow_id"`
	WorkflowRunID string `json:"workflow_run_id"`
}

type startBrregTranslationWorkflowRequest struct {
	IDs         []string          `json:"ids,omitempty"`
	Filters     map[string]string `json:"filters,omitempty"`
	Limit       int               `json:"limit,omitempty"`
	BatchSize   int               `json:"batch_size,omitempty"`
	MaxAttempts int               `json:"max_attempts,omitempty"`
	Trigger     string            `json:"trigger,omitempty"`
}

func (h *Handlers) handleStartBrregTranslationWorkflow(w http.ResponseWriter, r *http.Request) {
	if h.temporal == nil {
		writeError(w, http.StatusServiceUnavailable, "temporal client not available")
		return
	}

	req, err := decodeStartBrregTranslationWorkflowRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	input := brregworkflow.TranslateBrregRawInputsInput{
		IDs:         req.IDs,
		Filters:     req.Filters,
		Limit:       req.Limit,
		BatchSize:   req.BatchSize,
		MaxAttempts: req.MaxAttempts,
		Trigger:     req.Trigger,
	}
	workflowID := newWorkflowID("brreg-translation")
	slog.Debug("starting brreg translation workflow",
		"workflow_id", workflowID,
		"task_queue", brregworkflow.TranslateBrregRawInputsTaskQueue,
		"ids_count", len(req.IDs),
		"filters_count", len(req.Filters),
		"limit", req.Limit,
		"batch_size", req.BatchSize,
		"max_attempts", req.MaxAttempts,
		"trigger", req.Trigger,
	)
	run, err := h.temporal.ExecuteWorkflow(
		r.Context(),
		client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: brregworkflow.TranslateBrregRawInputsTaskQueue,
		},
		brregworkflow.TranslateBrregRawInputs,
		input,
	)
	if err != nil {
		slog.Error("start brreg translation workflow", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to start workflow")
		return
	}
	slog.Debug("brreg translation workflow started", "workflow_id", workflowID, "run_id", run.GetRunID())

	writeJSON(w, http.StatusAccepted, startWorkflowResponse{
		Status:        "started",
		Workflow:      brregworkflow.TranslateBrregRawInputsWorkflowName,
		WorkflowID:    workflowID,
		WorkflowRunID: run.GetRunID(),
	})
}

func decodeStartBrregTranslationWorkflowRequest(r *http.Request) (startBrregTranslationWorkflowRequest, error) {
	var req startBrregTranslationWorkflowRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		return startBrregTranslationWorkflowRequest{}, errors.New("invalid request body")
	}
	if req.Trigger == "" {
		req.Trigger = "manual"
	}
	if req.Limit < 0 {
		return startBrregTranslationWorkflowRequest{}, errors.New("limit must be greater than zero when provided")
	}
	if req.BatchSize < 0 {
		return startBrregTranslationWorkflowRequest{}, errors.New("batch_size must be greater than zero when provided")
	}
	if req.MaxAttempts < 0 {
		return startBrregTranslationWorkflowRequest{}, errors.New("max_attempts must be greater than zero when provided")
	}
	for _, id := range req.IDs {
		if _, err := uuid.Parse(id); err != nil {
			return startBrregTranslationWorkflowRequest{}, errors.New("ids must contain valid UUID values")
		}
	}
	return req, nil
}

func newWorkflowID(prefix string) string {
	return prefix + "-" + time.Now().UTC().Format("20060102-150405.000000000")
}
