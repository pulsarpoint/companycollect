package httpapi

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"go.temporal.io/sdk/client"

	brregworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/workflow"
)

const defaultBrregTranslationProvider = "default"
const defaultBrregDomainSearchProvider = "default"
const defaultBrregDomainSearchEngine = "duckduckgo"

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

	MaxParallelTasks  int    `json:"max_parallel_tasks,omitempty"`
	LeaseSeconds      int    `json:"lease_seconds,omitempty"`
	Provider          string `json:"provider,omitempty"`
	Model             string `json:"model,omitempty"`
	PromptVersion     string `json:"prompt_version,omitempty"`
	SourceLang        string `json:"source_lang,omitempty"`
	TargetLang        string `json:"target_lang,omitempty"`
	MaxServiceRetries int    `json:"max_service_retries,omitempty"`
}

type startBrregDomainSearchWorkflowRequest struct {
	IDs         []string          `json:"ids,omitempty"`
	Filters     map[string]string `json:"filters,omitempty"`
	Limit       int               `json:"limit,omitempty"`
	BatchSize   int               `json:"batch_size,omitempty"`
	MaxAttempts int               `json:"max_attempts,omitempty"`
	Trigger     string            `json:"trigger,omitempty"`

	MaxParallelTasks   int    `json:"max_parallel_tasks,omitempty"`
	LeaseSeconds       int    `json:"lease_seconds,omitempty"`
	SearchEngine       string `json:"search_engine,omitempty"`
	Provider           string `json:"provider,omitempty"`
	Model              string `json:"model,omitempty"`
	CandidateThreshold int    `json:"candidate_threshold,omitempty"`
	DomainThreshold    int    `json:"domain_threshold,omitempty"`
	MaxCandidates      int    `json:"max_candidates,omitempty"`
	MaxSiteChecks      int    `json:"max_site_checks,omitempty"`
	TimeoutSeconds     int    `json:"timeout_seconds,omitempty"`
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
		IDs:               req.IDs,
		Filters:           req.Filters,
		Limit:             req.Limit,
		BatchSize:         req.BatchSize,
		MaxAttempts:       req.MaxAttempts,
		Trigger:           req.Trigger,
		MaxParallelTasks:  req.MaxParallelTasks,
		LeaseSeconds:      req.LeaseSeconds,
		Provider:          req.Provider,
		Model:             req.Model,
		PromptVersion:     req.PromptVersion,
		SourceLang:        req.SourceLang,
		TargetLang:        req.TargetLang,
		MaxServiceRetries: req.MaxServiceRetries,
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
		"max_parallel_tasks", req.MaxParallelTasks,
		"lease_seconds", req.LeaseSeconds,
		"provider", req.Provider,
		"model", req.Model,
		"prompt_version", req.PromptVersion,
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

func (h *Handlers) handleStartBrregDomainSearchWorkflow(w http.ResponseWriter, r *http.Request) {
	if h.temporal == nil {
		writeError(w, http.StatusServiceUnavailable, "temporal client not available")
		return
	}

	req, err := decodeStartBrregDomainSearchWorkflowRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	input := brregworkflow.SearchBrregDomainsInput{
		IDs:                req.IDs,
		Filters:            req.Filters,
		Limit:              req.Limit,
		BatchSize:          req.BatchSize,
		MaxAttempts:        req.MaxAttempts,
		Trigger:            req.Trigger,
		MaxParallelTasks:   req.MaxParallelTasks,
		LeaseSeconds:       req.LeaseSeconds,
		SearchEngine:       req.SearchEngine,
		Provider:           req.Provider,
		Model:              req.Model,
		CandidateThreshold: req.CandidateThreshold,
		DomainThreshold:    req.DomainThreshold,
		MaxCandidates:      req.MaxCandidates,
		MaxSiteChecks:      req.MaxSiteChecks,
		TimeoutSeconds:     req.TimeoutSeconds,
	}
	workflowID := newWorkflowID("brreg-domain-search")
	slog.Debug("starting brreg domain search workflow",
		"workflow_id", workflowID,
		"task_queue", brregworkflow.SearchBrregDomainsTaskQueue,
		"ids_count", len(req.IDs),
		"filters_count", len(req.Filters),
		"limit", req.Limit,
		"batch_size", req.BatchSize,
		"max_attempts", req.MaxAttempts,
		"max_parallel_tasks", req.MaxParallelTasks,
		"lease_seconds", req.LeaseSeconds,
		"search_engine", req.SearchEngine,
		"provider", req.Provider,
		"model", req.Model,
		"candidate_threshold", req.CandidateThreshold,
		"domain_threshold", req.DomainThreshold,
		"max_candidates", req.MaxCandidates,
		"max_site_checks", req.MaxSiteChecks,
		"timeout_seconds", req.TimeoutSeconds,
		"trigger", req.Trigger,
	)
	run, err := h.temporal.ExecuteWorkflow(
		r.Context(),
		client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: brregworkflow.SearchBrregDomainsTaskQueue,
		},
		brregworkflow.SearchBrregDomains,
		input,
	)
	if err != nil {
		slog.Error("start brreg domain search workflow", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to start workflow")
		return
	}
	slog.Debug("brreg domain search workflow started", "workflow_id", workflowID, "run_id", run.GetRunID())

	writeJSON(w, http.StatusAccepted, startWorkflowResponse{
		Status:        "started",
		Workflow:      brregworkflow.SearchBrregDomainsWorkflowName,
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
	req.Trigger = strings.TrimSpace(req.Trigger)
	req.Provider = strings.TrimSpace(req.Provider)
	req.Model = strings.TrimSpace(req.Model)
	req.PromptVersion = strings.TrimSpace(req.PromptVersion)
	req.SourceLang = strings.TrimSpace(req.SourceLang)
	req.TargetLang = strings.TrimSpace(req.TargetLang)
	if req.Trigger == "" {
		req.Trigger = "manual"
	}
	if req.Provider == "" {
		req.Provider = defaultBrregTranslationProvider
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
	if req.MaxParallelTasks < 0 {
		return startBrregTranslationWorkflowRequest{}, errors.New("max_parallel_tasks must be greater than zero when provided")
	}
	if req.LeaseSeconds < 0 {
		return startBrregTranslationWorkflowRequest{}, errors.New("lease_seconds must be greater than zero when provided")
	}
	if req.MaxServiceRetries < 0 {
		return startBrregTranslationWorkflowRequest{}, errors.New("max_service_retries must be greater than zero when provided")
	}
	for _, id := range req.IDs {
		if _, err := uuid.Parse(id); err != nil {
			return startBrregTranslationWorkflowRequest{}, errors.New("ids must contain valid UUID values")
		}
	}
	return req, nil
}

func decodeStartBrregDomainSearchWorkflowRequest(r *http.Request) (startBrregDomainSearchWorkflowRequest, error) {
	var req startBrregDomainSearchWorkflowRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		return startBrregDomainSearchWorkflowRequest{}, errors.New("invalid request body")
	}
	req.Trigger = strings.TrimSpace(req.Trigger)
	req.SearchEngine = strings.ToLower(strings.TrimSpace(req.SearchEngine))
	req.Provider = strings.TrimSpace(req.Provider)
	req.Model = strings.TrimSpace(req.Model)
	if req.Trigger == "" {
		req.Trigger = "manual"
	}
	if req.SearchEngine == "" {
		req.SearchEngine = defaultBrregDomainSearchEngine
	}
	if req.Provider == "" {
		req.Provider = defaultBrregDomainSearchProvider
	}
	if req.SearchEngine != "duckduckgo" && req.SearchEngine != "yandex" {
		return startBrregDomainSearchWorkflowRequest{}, errors.New("search_engine must be duckduckgo or yandex")
	}
	if req.Limit < 0 {
		return startBrregDomainSearchWorkflowRequest{}, errors.New("limit must be greater than zero when provided")
	}
	if req.BatchSize < 0 {
		return startBrregDomainSearchWorkflowRequest{}, errors.New("batch_size must be greater than zero when provided")
	}
	if req.MaxAttempts < 0 {
		return startBrregDomainSearchWorkflowRequest{}, errors.New("max_attempts must be greater than zero when provided")
	}
	if req.MaxParallelTasks < 0 {
		return startBrregDomainSearchWorkflowRequest{}, errors.New("max_parallel_tasks must be greater than zero when provided")
	}
	if req.LeaseSeconds < 0 {
		return startBrregDomainSearchWorkflowRequest{}, errors.New("lease_seconds must be greater than zero when provided")
	}
	if req.CandidateThreshold < 0 || req.CandidateThreshold > 100 {
		return startBrregDomainSearchWorkflowRequest{}, errors.New("candidate_threshold must be between 0 and 100 when provided")
	}
	if req.DomainThreshold < 0 || req.DomainThreshold > 100 {
		return startBrregDomainSearchWorkflowRequest{}, errors.New("domain_threshold must be between 0 and 100 when provided")
	}
	if req.MaxCandidates < 0 {
		return startBrregDomainSearchWorkflowRequest{}, errors.New("max_candidates must be greater than zero when provided")
	}
	if req.MaxSiteChecks < 0 {
		return startBrregDomainSearchWorkflowRequest{}, errors.New("max_site_checks must be greater than zero when provided")
	}
	if req.TimeoutSeconds < 0 {
		return startBrregDomainSearchWorkflowRequest{}, errors.New("timeout_seconds must be greater than zero when provided")
	}
	for _, id := range req.IDs {
		if _, err := uuid.Parse(id); err != nil {
			return startBrregDomainSearchWorkflowRequest{}, errors.New("ids must contain valid UUID values")
		}
	}
	return req, nil
}

func newWorkflowID(prefix string) string {
	return prefix + "-" + time.Now().UTC().Format("20060102-150405.000000000")
}
