package httpapi

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	workflowservicepb "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	brregworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/workflow"
	"github.com/pulsarpoint/corpscout/scheduler/internal/nacetaxonomy"
)

const defaultBrregTranslationProvider = "default"
const defaultBrregDomainSearchProvider = "default"
const defaultBrregDomainSearchEngine = "duckduckgo"
const defaultNACETaxonomyWorkflowRunsLimit = 10
const maxNACETaxonomyWorkflowRunsLimit = 50

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

type startBrregSourceProfileNormalizationWorkflowRequest struct {
	IDs     []string          `json:"ids,omitempty"`
	Filters map[string]string `json:"filters,omitempty"`
	Limit   int               `json:"limit,omitempty"`
	Trigger string            `json:"trigger,omitempty"`
}

type startBrregBulkRawIngestWorkflowRequest struct {
	SourceURL string `json:"source_url,omitempty"`
	Limit     int    `json:"limit,omitempty"`
	BatchSize int    `json:"batch_size,omitempty"`
	Trigger   string `json:"trigger,omitempty"`
}

type startNACETaxonomySyncWorkflowRequest struct {
	Revision       string `json:"revision,omitempty"`
	SourceURL      string `json:"source_url,omitempty"`
	Trigger        string `json:"trigger,omitempty"`
	ForceReprocess bool   `json:"force_reprocess,omitempty"`
}

type naceTaxonomyWorkflowRunListResponse struct {
	Items []naceTaxonomyWorkflowRunResponse `json:"items"`
}

type naceTaxonomyWorkflowRunResponse struct {
	WorkflowID    string     `json:"workflow_id"`
	RunID         string     `json:"run_id"`
	WorkflowType  string     `json:"workflow_type"`
	Status        string     `json:"status"`
	StartTime     *time.Time `json:"start_time,omitempty"`
	CloseTime     *time.Time `json:"close_time,omitempty"`
	ExecutionTime *time.Time `json:"execution_time,omitempty"`
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
		IDs:     req.IDs,
		Filters: req.Filters,
		Limit:   req.Limit,
		Trigger: req.Trigger,
	}
	workflowID := newWorkflowID("brreg-translation")
	slog.Debug("starting brreg translation workflow",
		"workflow_id", workflowID,
		"task_queue", brregworkflow.TranslateBrregRawInputsTaskQueue,
		"ids_count", len(req.IDs),
		"filters_count", len(req.Filters),
		"limit", req.Limit,
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

func (h *Handlers) handleStartBrregSourceProfileNormalizationWorkflow(w http.ResponseWriter, r *http.Request) {
	if h.temporal == nil {
		writeError(w, http.StatusServiceUnavailable, "temporal client not available")
		return
	}

	req, err := decodeStartBrregSourceProfileNormalizationWorkflowRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	input := brregworkflow.NormalizeBrregSourceProfilesInput{
		IDs:     req.IDs,
		Filters: req.Filters,
		Limit:   req.Limit,
		Trigger: req.Trigger,
	}
	workflowID := newWorkflowID("brreg-source-profile")
	slog.Debug("starting brreg source profile normalization workflow",
		"workflow_id", workflowID,
		"task_queue", brregworkflow.NormalizeBrregSourceProfilesTaskQueue,
		"ids_count", len(req.IDs),
		"filters_count", len(req.Filters),
		"limit", req.Limit,
		"trigger", req.Trigger,
	)
	run, err := h.temporal.ExecuteWorkflow(
		r.Context(),
		client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: brregworkflow.NormalizeBrregSourceProfilesTaskQueue,
		},
		brregworkflow.NormalizeBrregSourceProfiles,
		input,
	)
	if err != nil {
		slog.Error("start brreg source profile normalization workflow", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to start workflow")
		return
	}
	slog.Debug("brreg source profile normalization workflow started", "workflow_id", workflowID, "run_id", run.GetRunID())

	writeJSON(w, http.StatusAccepted, startWorkflowResponse{
		Status:        "started",
		Workflow:      brregworkflow.NormalizeBrregSourceProfilesWorkflowName,
		WorkflowID:    workflowID,
		WorkflowRunID: run.GetRunID(),
	})
}

func (h *Handlers) handleStartBrregBulkRawIngestWorkflow(w http.ResponseWriter, r *http.Request) {
	if h.temporal == nil {
		writeError(w, http.StatusServiceUnavailable, "temporal client not available")
		return
	}

	req, err := decodeStartBrregBulkRawIngestWorkflowRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	input := brregworkflow.LoadBrregBulkRawRecordsInput{
		SourceURL: req.SourceURL,
		Limit:     req.Limit,
		BatchSize: req.BatchSize,
		Trigger:   req.Trigger,
	}
	workflowID := newWorkflowID("brreg-bulk-ingest")
	slog.Debug("starting brreg bulk raw ingest workflow",
		"workflow_id", workflowID,
		"task_queue", brregworkflow.LoadBrregBulkRawRecordsTaskQueue,
		"source_url", req.SourceURL,
		"limit", req.Limit,
		"batch_size", req.BatchSize,
		"trigger", req.Trigger,
	)
	run, err := h.temporal.ExecuteWorkflow(
		r.Context(),
		client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: brregworkflow.LoadBrregBulkRawRecordsTaskQueue,
		},
		brregworkflow.LoadBrregBulkRawRecords,
		input,
	)
	if err != nil {
		slog.Error("start brreg bulk raw ingest workflow", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to start workflow")
		return
	}
	slog.Debug("brreg bulk raw ingest workflow started", "workflow_id", workflowID, "run_id", run.GetRunID())

	writeJSON(w, http.StatusAccepted, startWorkflowResponse{
		Status:        "started",
		Workflow:      brregworkflow.LoadBrregBulkRawRecordsWorkflowName,
		WorkflowID:    workflowID,
		WorkflowRunID: run.GetRunID(),
	})
}

func (h *Handlers) handleStartNACETaxonomySyncWorkflow(w http.ResponseWriter, r *http.Request) {
	if h.temporal == nil {
		writeError(w, http.StatusServiceUnavailable, "temporal client not available")
		return
	}

	req, err := h.decodeStartNACETaxonomySyncWorkflowRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	input := nacetaxonomy.SyncNACETaxonomyInput{
		Revision:       req.Revision,
		SourceURL:      req.SourceURL,
		Trigger:        req.Trigger,
		ForceReprocess: req.ForceReprocess,
	}
	workflowID := newWorkflowID("nace-taxonomy-sync")
	slog.Debug("starting nace taxonomy sync workflow",
		"workflow_id", workflowID,
		"task_queue", nacetaxonomy.SyncTaskQueue,
		"revision", req.Revision,
		"source_url", req.SourceURL,
		"trigger", req.Trigger,
		"force_reprocess", req.ForceReprocess,
	)
	run, err := h.temporal.ExecuteWorkflow(
		r.Context(),
		client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: nacetaxonomy.SyncTaskQueue,
		},
		nacetaxonomy.SyncNACETaxonomy,
		input,
	)
	if err != nil {
		slog.Error("start nace taxonomy sync workflow", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to start workflow")
		return
	}
	slog.Debug("nace taxonomy sync workflow started", "workflow_id", workflowID, "run_id", run.GetRunID())

	writeJSON(w, http.StatusAccepted, startWorkflowResponse{
		Status:        "started",
		Workflow:      nacetaxonomy.SyncWorkflowName,
		WorkflowID:    workflowID,
		WorkflowRunID: run.GetRunID(),
	})
}

func (h *Handlers) handleListNACETaxonomySyncWorkflowRuns(w http.ResponseWriter, r *http.Request) {
	if h.temporal == nil {
		writeError(w, http.StatusServiceUnavailable, "temporal client not available")
		return
	}

	limit, err := parseNACETaxonomyWorkflowRunsLimit(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	response, err := h.temporal.ListWorkflow(r.Context(), &workflowservicepb.ListWorkflowExecutionsRequest{
		PageSize: limit,
		Query:    "WorkflowType = 'SyncNACETaxonomy'",
	})
	if err != nil {
		slog.Error("list nace taxonomy sync workflow runs", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list workflow runs")
		return
	}

	items := make([]naceTaxonomyWorkflowRunResponse, 0, len(response.GetExecutions()))
	for _, execution := range response.GetExecutions() {
		item := naceTaxonomyWorkflowRunResponse{
			WorkflowID:   execution.GetExecution().GetWorkflowId(),
			RunID:        execution.GetExecution().GetRunId(),
			WorkflowType: execution.GetType().GetName(),
			Status:       workflowExecutionStatusString(execution.GetStatus()),
		}
		if startTime := execution.GetStartTime(); startTime != nil {
			value := startTime.AsTime()
			item.StartTime = &value
		}
		if closeTime := execution.GetCloseTime(); closeTime != nil {
			value := closeTime.AsTime()
			item.CloseTime = &value
		}
		if executionTime := execution.GetExecutionTime(); executionTime != nil {
			value := executionTime.AsTime()
			item.ExecutionTime = &value
		}
		items = append(items, item)
	}

	writeJSON(w, http.StatusOK, naceTaxonomyWorkflowRunListResponse{Items: items})
}

func decodeStartBrregTranslationWorkflowRequest(r *http.Request) (startBrregTranslationWorkflowRequest, error) {
	var req startBrregTranslationWorkflowRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		return startBrregTranslationWorkflowRequest{}, errors.New("invalid request body")
	}
	req.Trigger = strings.TrimSpace(req.Trigger)
	if req.Trigger == "" {
		req.Trigger = "manual"
	}
	if req.Limit < 0 {
		return startBrregTranslationWorkflowRequest{}, errors.New("limit must be greater than zero when provided")
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

func decodeStartBrregSourceProfileNormalizationWorkflowRequest(r *http.Request) (startBrregSourceProfileNormalizationWorkflowRequest, error) {
	var req startBrregSourceProfileNormalizationWorkflowRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		return startBrregSourceProfileNormalizationWorkflowRequest{}, errors.New("invalid request body")
	}
	req.Trigger = strings.TrimSpace(req.Trigger)
	if req.Trigger == "" {
		req.Trigger = "manual"
	}
	if req.Limit < 0 {
		return startBrregSourceProfileNormalizationWorkflowRequest{}, errors.New("limit cannot be negative")
	}
	for _, id := range req.IDs {
		if _, err := uuid.Parse(id); err != nil {
			return startBrregSourceProfileNormalizationWorkflowRequest{}, errors.New("ids must contain valid UUID values")
		}
	}
	return req, nil
}

func decodeStartBrregBulkRawIngestWorkflowRequest(r *http.Request) (startBrregBulkRawIngestWorkflowRequest, error) {
	var req startBrregBulkRawIngestWorkflowRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		return startBrregBulkRawIngestWorkflowRequest{}, errors.New("invalid request body")
	}
	req.SourceURL = strings.TrimSpace(req.SourceURL)
	req.Trigger = strings.TrimSpace(req.Trigger)
	if req.Trigger == "" {
		req.Trigger = "manual"
	}
	if req.Limit < 0 {
		return startBrregBulkRawIngestWorkflowRequest{}, errors.New("limit cannot be negative")
	}
	if req.BatchSize < 0 {
		return startBrregBulkRawIngestWorkflowRequest{}, errors.New("batch_size cannot be negative")
	}
	if req.SourceURL != "" {
		parsed, err := url.Parse(req.SourceURL)
		if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			return startBrregBulkRawIngestWorkflowRequest{}, errors.New("source_url must be http or https")
		}
	}
	return req, nil
}

func (h *Handlers) decodeStartNACETaxonomySyncWorkflowRequest(r *http.Request) (startNACETaxonomySyncWorkflowRequest, error) {
	var req startNACETaxonomySyncWorkflowRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		return startNACETaxonomySyncWorkflowRequest{}, errors.New("invalid request body")
	}
	req.Revision = strings.TrimSpace(req.Revision)
	req.SourceURL = strings.TrimSpace(req.SourceURL)
	req.Trigger = strings.TrimSpace(req.Trigger)
	if req.Revision == "" {
		req.Revision = nacetaxonomy.DefaultRevision
	}
	if req.SourceURL == "" {
		req.SourceURL = strings.TrimSpace(h.naceSourceURL)
	}
	if req.Trigger == "" {
		req.Trigger = "manual"
	}
	if req.SourceURL == "" {
		return startNACETaxonomySyncWorkflowRequest{}, errors.New("nace source url is required")
	}
	parsed, err := url.Parse(req.SourceURL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return startNACETaxonomySyncWorkflowRequest{}, errors.New("nace source url must be http or https")
	}
	return req, nil
}

func parseNACETaxonomyWorkflowRunsLimit(r *http.Request) (int32, error) {
	rawLimit := strings.TrimSpace(r.URL.Query().Get("limit"))
	if rawLimit == "" {
		return defaultNACETaxonomyWorkflowRunsLimit, nil
	}
	limit, err := strconv.Atoi(rawLimit)
	if err != nil || limit <= 0 {
		return 0, errors.New("limit must be a positive integer")
	}
	if limit > maxNACETaxonomyWorkflowRunsLimit {
		limit = maxNACETaxonomyWorkflowRunsLimit
	}
	return int32(limit), nil
}

func workflowExecutionStatusString(status enumspb.WorkflowExecutionStatus) string {
	switch status {
	case enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING:
		return "running"
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		return "completed"
	case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:
		return "failed"
	case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
		return "canceled"
	case enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		return "terminated"
	case enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
		return "continued_as_new"
	case enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		return "timed_out"
	default:
		return "unspecified"
	}
}

func newWorkflowID(prefix string) string {
	return prefix + "-" + time.Now().UTC().Format("20060102-150405.000000000")
}
