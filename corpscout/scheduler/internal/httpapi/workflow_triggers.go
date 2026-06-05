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

	ariregisterworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/ariregister/workflow"
	brregworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/workflow"
	cvrworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/cvr/workflow"
	franceworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/france/workflow"
	"github.com/pulsarpoint/corpscout/scheduler/internal/fx"
	"github.com/pulsarpoint/corpscout/scheduler/internal/nacetaxonomy"
	seworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/se/workflow"
)

const defaultBrregDomainSearchProvider = "default"
const defaultBrregDomainSearchEngine = "duckduckgo"
const defaultBrregWorkflowRunsLimit = 50
const maxBrregWorkflowRunsLimit = 100
const defaultNACETaxonomyWorkflowRunsLimit = 10
const maxNACETaxonomyWorkflowRunsLimit = 50
const defaultFXWorkflowRunsLimit = 10
const maxFXWorkflowRunsLimit = 50

type startWorkflowResponse struct {
	Status        string `json:"status"`
	Workflow      string `json:"workflow"`
	TaskQueue     string `json:"task_queue,omitempty"`
	WorkflowID    string `json:"workflow_id"`
	WorkflowRunID string `json:"workflow_run_id"`
}

type startBrregCompanyTranslationWorkflowRequest struct {
	AllRecords           bool              `json:"all_records,omitempty"`
	IDs                  []string          `json:"ids,omitempty"`
	Filters              map[string]string `json:"filters,omitempty"`
	Limit                int               `json:"limit,omitempty"`
	BatchSize            int               `json:"batch_size,omitempty"`
	ClaimMode            string            `json:"claim_mode,omitempty"`
	MaxRequestChars      int               `json:"max_request_chars,omitempty"`
	MaxTerms             int               `json:"max_terms,omitempty"`
	MaxCompaniesPerBatch int               `json:"max_companies_per_batch,omitempty"`
	MaxBatches           int               `json:"max_batches,omitempty"`
	MaxParallelTasks     int               `json:"max_parallel_tasks,omitempty"`
	LeaseSeconds         int               `json:"lease_seconds,omitempty"`
	MaxAttempts          int               `json:"max_attempts,omitempty"`
	BatchDelaySeconds    int               `json:"batch_delay_seconds,omitempty"`
	Provider             string            `json:"provider,omitempty"`
	Model                string            `json:"model,omitempty"`
	PromptVersion        string            `json:"prompt_version,omitempty"`
	Trigger              string            `json:"trigger,omitempty"`
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
	IDs       []string          `json:"ids,omitempty"`
	Filters   map[string]string `json:"filters,omitempty"`
	Limit     int               `json:"limit,omitempty"`
	BatchSize int               `json:"batch_size,omitempty"`
	Trigger   string            `json:"trigger,omitempty"`
}

type startBrregSourceExplorerRefreshWorkflowRequest struct {
	Trigger string `json:"trigger,omitempty"`
}

type startBrregSourceCapitalFXWorkflowRequest struct {
	IDs            []string          `json:"ids,omitempty"`
	Filters        map[string]string `json:"filters,omitempty"`
	Limit          int               `json:"limit,omitempty"`
	RateDate       string            `json:"rate_date,omitempty"`
	ForceReprocess bool              `json:"force_reprocess,omitempty"`
	Trigger        string            `json:"trigger,omitempty"`
}

type startBrregSourceFinancialWorkflowRequest struct {
	Limit            int    `json:"limit,omitempty"`
	BatchSize        int    `json:"batch_size,omitempty"`
	MaxParallelTasks int    `json:"max_parallel_tasks,omitempty"`
	LeaseSeconds     int    `json:"lease_seconds,omitempty"`
	MaxAttempts      int    `json:"max_attempts,omitempty"`
	Trigger          string `json:"trigger,omitempty"`
}

type startBrregBulkRawIngestWorkflowRequest struct {
	SourceURL string `json:"source_url,omitempty"`
	Limit     int    `json:"limit,omitempty"`
	BatchSize int    `json:"batch_size,omitempty"`
	Trigger   string `json:"trigger,omitempty"`
}

type startAriregisterBulkRawIngestWorkflowRequest struct {
	SourceURL string `json:"source_url,omitempty"`
	Limit     int    `json:"limit,omitempty"`
	BatchSize int    `json:"batch_size,omitempty"`
	Trigger   string `json:"trigger,omitempty"`
}

type startAriregisterCompanyTranslationWorkflowRequest struct {
	AllRecords           bool              `json:"all_records,omitempty"`
	IDs                  []string          `json:"ids,omitempty"`
	Filters              map[string]string `json:"filters,omitempty"`
	Limit                int               `json:"limit,omitempty"`
	BatchSize            int               `json:"batch_size,omitempty"`
	ClaimMode            string            `json:"claim_mode,omitempty"`
	MaxRequestChars      int               `json:"max_request_chars,omitempty"`
	MaxTerms             int               `json:"max_terms,omitempty"`
	MaxCompaniesPerBatch int               `json:"max_companies_per_batch,omitempty"`
	MaxBatches           int               `json:"max_batches,omitempty"`
	MaxParallelTasks     int               `json:"max_parallel_tasks,omitempty"`
	LeaseSeconds         int               `json:"lease_seconds,omitempty"`
	MaxAttempts          int               `json:"max_attempts,omitempty"`
	BatchDelaySeconds    int               `json:"batch_delay_seconds,omitempty"`
	Provider             string            `json:"provider,omitempty"`
	Model                string            `json:"model,omitempty"`
	PromptVersion        string            `json:"prompt_version,omitempty"`
	Trigger              string            `json:"trigger,omitempty"`
}

type startAriregisterSourceProfileWorkflowRequest struct {
	IDs       []string          `json:"ids,omitempty"`
	Filters   map[string]string `json:"filters,omitempty"`
	Limit     int               `json:"limit,omitempty"`
	BatchSize int               `json:"batch_size,omitempty"`
	Trigger   string            `json:"trigger,omitempty"`
}

type startCVRRawIngestWorkflowRequest struct {
	SourceURL string `json:"source_url,omitempty"`
	ScrollURL string `json:"scroll_url,omitempty"`
	Scroll    string `json:"scroll,omitempty"`
	Limit     int    `json:"limit,omitempty"`
	PageSize  int    `json:"page_size,omitempty"`
	BatchSize int    `json:"batch_size,omitempty"`
	Trigger   string `json:"trigger,omitempty"`
}

type startFranceBulkRawIngestWorkflowRequest struct {
	LegalUnitsURL     string `json:"legal_units_url,omitempty"`
	EstablishmentsURL string `json:"establishments_url,omitempty"`
	Limit             int    `json:"limit,omitempty"`
	BatchSize         int    `json:"batch_size,omitempty"`
	Trigger           string `json:"trigger,omitempty"`
}

type startFranceSourceProfileWorkflowRequest struct {
	IDs       []string          `json:"ids,omitempty"`
	Filters   map[string]string `json:"filters,omitempty"`
	Limit     int               `json:"limit,omitempty"`
	BatchSize int               `json:"batch_size,omitempty"`
	Trigger   string            `json:"trigger,omitempty"`
}

type startSEBulkRawIngestWorkflowRequest struct {
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

type startExchangeRateSyncWorkflowRequest struct {
	Provider       string `json:"provider,omitempty"`
	SourceURL      string `json:"source_url,omitempty"`
	Trigger        string `json:"trigger,omitempty"`
	ForceReprocess bool   `json:"force_reprocess,omitempty"`
}

type brregWorkflowRunListResponse struct {
	Prefixes []brregWorkflowPrefixResponse `json:"prefixes"`
	Items    []brregWorkflowRunResponse    `json:"items"`
}

type brregWorkflowPrefixResponse struct {
	Prefix       string `json:"prefix"`
	Label        string `json:"label"`
	WorkflowType string `json:"workflow_type"`
}

type brregWorkflowRunResponse struct {
	WorkflowID    string     `json:"workflow_id"`
	RunID         string     `json:"run_id"`
	WorkflowType  string     `json:"workflow_type"`
	Prefix        string     `json:"prefix"`
	Action        string     `json:"action"`
	Status        string     `json:"status"`
	StartTime     *time.Time `json:"start_time,omitempty"`
	CloseTime     *time.Time `json:"close_time,omitempty"`
	ExecutionTime *time.Time `json:"execution_time,omitempty"`
}

type naceTaxonomyWorkflowRunListResponse struct {
	Items []naceTaxonomyWorkflowRunResponse `json:"items"`
}

type exchangeRateSyncWorkflowRunListResponse struct {
	Items []exchangeRateSyncWorkflowRunResponse `json:"items"`
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

type exchangeRateSyncWorkflowRunResponse struct {
	WorkflowID    string     `json:"workflow_id"`
	RunID         string     `json:"run_id"`
	WorkflowType  string     `json:"workflow_type"`
	Status        string     `json:"status"`
	StartTime     *time.Time `json:"start_time,omitempty"`
	CloseTime     *time.Time `json:"close_time,omitempty"`
	ExecutionTime *time.Time `json:"execution_time,omitempty"`
}

func (h *Handlers) handleStartBrregCompanyTranslationWorkflow(w http.ResponseWriter, r *http.Request) {
	if h.temporal == nil {
		writeError(w, http.StatusServiceUnavailable, "temporal client not available")
		return
	}

	req, err := decodeStartBrregCompanyTranslationWorkflowRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	input := brregworkflow.TranslateBrregSourceCompaniesInput{
		AllRecords:           req.AllRecords,
		IDs:                  req.IDs,
		Filters:              req.Filters,
		Limit:                req.Limit,
		BatchSize:            req.BatchSize,
		ClaimMode:            req.ClaimMode,
		MaxRequestChars:      req.MaxRequestChars,
		MaxTerms:             req.MaxTerms,
		MaxCompaniesPerBatch: req.MaxCompaniesPerBatch,
		MaxBatches:           req.MaxBatches,
		MaxParallelTasks:     req.MaxParallelTasks,
		LeaseSeconds:         req.LeaseSeconds,
		MaxAttempts:          req.MaxAttempts,
		BatchDelaySeconds:    req.BatchDelaySeconds,
		Provider:             req.Provider,
		Model:                req.Model,
		PromptVersion:        req.PromptVersion,
		Trigger:              req.Trigger,
	}
	workflowID := newWorkflowID("brreg-company-translation")
	slog.Debug("starting brreg company translation workflow",
		"workflow_id", workflowID,
		"task_queue", brregworkflow.TranslateBrregSourceCompaniesTaskQueue,
		"all_records", req.AllRecords,
		"ids_count", len(req.IDs),
		"filters_count", len(req.Filters),
		"limit", req.Limit,
		"batch_size", req.BatchSize,
		"claim_mode", req.ClaimMode,
		"max_request_chars", req.MaxRequestChars,
		"max_terms", req.MaxTerms,
		"max_companies_per_batch", req.MaxCompaniesPerBatch,
		"max_batches", req.MaxBatches,
		"max_parallel_tasks", req.MaxParallelTasks,
		"lease_seconds", req.LeaseSeconds,
		"max_attempts", req.MaxAttempts,
		"batch_delay_seconds", req.BatchDelaySeconds,
		"provider", req.Provider,
		"model", req.Model,
		"prompt_version", req.PromptVersion,
		"trigger", req.Trigger,
	)
	run, err := h.temporal.ExecuteWorkflow(
		r.Context(),
		client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: brregworkflow.TranslateBrregSourceCompaniesTaskQueue,
		},
		brregworkflow.TranslateBrregSourceCompanies,
		input,
	)
	if err != nil {
		slog.Error("start brreg company translation workflow", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to start workflow")
		return
	}
	slog.Debug("brreg company translation workflow started", "workflow_id", workflowID, "run_id", run.GetRunID())

	writeJSON(w, http.StatusAccepted, startWorkflowResponse{
		Status:        "started",
		Workflow:      brregworkflow.TranslateBrregSourceCompaniesWorkflowName,
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

func (h *Handlers) handleStartAriregisterCompanyTranslationWorkflow(w http.ResponseWriter, r *http.Request) {
	if h.temporal == nil {
		writeError(w, http.StatusServiceUnavailable, "temporal client not available")
		return
	}

	req, err := decodeStartAriregisterCompanyTranslationWorkflowRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	input := ariregisterworkflow.TranslateAriregisterSourceCompaniesInput{
		AllRecords:           req.AllRecords,
		IDs:                  req.IDs,
		Filters:              req.Filters,
		Limit:                req.Limit,
		BatchSize:            req.BatchSize,
		ClaimMode:            req.ClaimMode,
		MaxRequestChars:      req.MaxRequestChars,
		MaxTerms:             req.MaxTerms,
		MaxCompaniesPerBatch: req.MaxCompaniesPerBatch,
		MaxBatches:           req.MaxBatches,
		MaxParallelTasks:     req.MaxParallelTasks,
		LeaseSeconds:         req.LeaseSeconds,
		MaxAttempts:          req.MaxAttempts,
		BatchDelaySeconds:    req.BatchDelaySeconds,
		Provider:             req.Provider,
		Model:                req.Model,
		PromptVersion:        req.PromptVersion,
		Trigger:              req.Trigger,
	}
	workflowID := newWorkflowID("ariregister-company-translation")
	slog.Debug("starting ariregister company translation workflow",
		"workflow_id", workflowID,
		"task_queue", ariregisterworkflow.TranslateAriregisterSourceCompaniesTaskQueue,
		"all_records", req.AllRecords,
		"ids_count", len(req.IDs),
		"filters_count", len(req.Filters),
		"limit", req.Limit,
		"batch_size", req.BatchSize,
		"claim_mode", req.ClaimMode,
		"max_request_chars", req.MaxRequestChars,
		"max_terms", req.MaxTerms,
		"max_companies_per_batch", req.MaxCompaniesPerBatch,
		"max_batches", req.MaxBatches,
		"max_parallel_tasks", req.MaxParallelTasks,
		"lease_seconds", req.LeaseSeconds,
		"max_attempts", req.MaxAttempts,
		"batch_delay_seconds", req.BatchDelaySeconds,
		"provider", req.Provider,
		"model", req.Model,
		"prompt_version", req.PromptVersion,
		"trigger", req.Trigger,
	)
	run, err := h.temporal.ExecuteWorkflow(
		r.Context(),
		client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: ariregisterworkflow.TranslateAriregisterSourceCompaniesTaskQueue,
		},
		ariregisterworkflow.TranslateAriregisterSourceCompanies,
		input,
	)
	if err != nil {
		slog.Error("start ariregister company translation workflow", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to start workflow")
		return
	}
	slog.Debug("ariregister company translation workflow started", "workflow_id", workflowID, "run_id", run.GetRunID())

	writeJSON(w, http.StatusAccepted, startWorkflowResponse{
		Status:        "started",
		Workflow:      ariregisterworkflow.TranslateAriregisterSourceCompaniesWorkflowName,
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
		IDs:       req.IDs,
		Filters:   req.Filters,
		Limit:     req.Limit,
		BatchSize: req.BatchSize,
		Trigger:   req.Trigger,
	}
	workflowID := newWorkflowID("brreg-source-profile")
	slog.Debug("starting brreg source profile normalization workflow",
		"workflow_id", workflowID,
		"task_queue", brregworkflow.NormalizeBrregSourceProfilesTaskQueue,
		"workflow", brregworkflow.NormalizeBrregSourceProfilesWithCopyWorkflowName,
		"ids_count", len(req.IDs),
		"filters_count", len(req.Filters),
		"limit", req.Limit,
		"batch_size", req.BatchSize,
		"trigger", req.Trigger,
	)
	run, err := h.temporal.ExecuteWorkflow(
		r.Context(),
		client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: brregworkflow.NormalizeBrregSourceProfilesTaskQueue,
		},
		brregworkflow.NormalizeBrregSourceProfilesWithCopy,
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
		Workflow:      brregworkflow.NormalizeBrregSourceProfilesWithCopyWorkflowName,
		WorkflowID:    workflowID,
		WorkflowRunID: run.GetRunID(),
	})
}

func (h *Handlers) handleStartBrregSourceExplorerRefreshWorkflow(w http.ResponseWriter, r *http.Request) {
	if h.temporal == nil {
		writeError(w, http.StatusServiceUnavailable, "temporal client not available")
		return
	}

	req, err := decodeStartBrregSourceExplorerRefreshWorkflowRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	input := brregworkflow.RefreshBrregSourceExplorerInput{
		Trigger: req.Trigger,
	}
	workflowID := newWorkflowID("brreg-source-explorer-refresh")
	slog.Debug("starting brreg source explorer refresh workflow",
		"workflow_id", workflowID,
		"task_queue", brregworkflow.RefreshBrregSourceExplorerTaskQueue,
		"trigger", req.Trigger,
	)
	run, err := h.temporal.ExecuteWorkflow(
		r.Context(),
		client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: brregworkflow.RefreshBrregSourceExplorerTaskQueue,
		},
		brregworkflow.RefreshBrregSourceExplorer,
		input,
	)
	if err != nil {
		slog.Error("start brreg source explorer refresh workflow", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to start workflow")
		return
	}
	slog.Debug("brreg source explorer refresh workflow started", "workflow_id", workflowID, "run_id", run.GetRunID())

	writeJSON(w, http.StatusAccepted, startWorkflowResponse{
		Status:        "started",
		Workflow:      brregworkflow.RefreshBrregSourceExplorerWorkflowName,
		WorkflowID:    workflowID,
		WorkflowRunID: run.GetRunID(),
	})
}

func (h *Handlers) handleStartBrregSourceCapitalFXWorkflow(w http.ResponseWriter, r *http.Request) {
	if h.temporal == nil {
		writeError(w, http.StatusServiceUnavailable, "temporal client not available")
		return
	}

	req, err := decodeStartBrregSourceCapitalFXWorkflowRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	input := brregworkflow.ConvertBrregSourceCapitalToUSDInput{
		IDs:            req.IDs,
		Filters:        req.Filters,
		Limit:          req.Limit,
		RateDate:       req.RateDate,
		ForceReprocess: req.ForceReprocess,
		Trigger:        req.Trigger,
	}
	workflowID := newWorkflowID("brreg-source-capital-fx")
	slog.Debug("starting brreg source capital fx workflow",
		"workflow_id", workflowID,
		"task_queue", brregworkflow.ConvertBrregSourceCapitalToUSDTaskQueue,
		"ids_count", len(req.IDs),
		"filters_count", len(req.Filters),
		"limit", req.Limit,
		"rate_date", req.RateDate,
		"force_reprocess", req.ForceReprocess,
		"trigger", req.Trigger,
	)
	run, err := h.temporal.ExecuteWorkflow(
		r.Context(),
		client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: brregworkflow.ConvertBrregSourceCapitalToUSDTaskQueue,
		},
		brregworkflow.ConvertBrregSourceCapitalToUSD,
		input,
	)
	if err != nil {
		slog.Error("start brreg source capital fx workflow", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to start workflow")
		return
	}
	slog.Debug("brreg source capital fx workflow started", "workflow_id", workflowID, "run_id", run.GetRunID())

	writeJSON(w, http.StatusAccepted, startWorkflowResponse{
		Status:        "started",
		Workflow:      brregworkflow.ConvertBrregSourceCapitalToUSDWorkflowName,
		WorkflowID:    workflowID,
		WorkflowRunID: run.GetRunID(),
	})
}

func (h *Handlers) handleStartBrregSourceFinancialWorkflow(w http.ResponseWriter, r *http.Request) {
	if h.temporal == nil {
		writeError(w, http.StatusServiceUnavailable, "temporal client not available")
		return
	}

	req, err := decodeStartBrregSourceFinancialWorkflowRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	input := brregworkflow.FetchBrregSourceFinancialStatementsInput{
		Limit:            req.Limit,
		BatchSize:        req.BatchSize,
		MaxParallelTasks: req.MaxParallelTasks,
		LeaseSeconds:     req.LeaseSeconds,
		MaxAttempts:      req.MaxAttempts,
		Trigger:          req.Trigger,
	}
	workflowID := newWorkflowID("brreg-source-financial")
	slog.Debug("starting brreg source financial workflow",
		"workflow_id", workflowID,
		"task_queue", brregworkflow.FetchBrregSourceFinancialStatementsTaskQueue,
		"limit", req.Limit,
		"batch_size", req.BatchSize,
		"max_parallel_tasks", req.MaxParallelTasks,
		"lease_seconds", req.LeaseSeconds,
		"max_attempts", req.MaxAttempts,
		"trigger", req.Trigger,
	)
	run, err := h.temporal.ExecuteWorkflow(
		r.Context(),
		client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: brregworkflow.FetchBrregSourceFinancialStatementsTaskQueue,
		},
		brregworkflow.FetchBrregSourceFinancialStatements,
		input,
	)
	if err != nil {
		slog.Error("start brreg source financial workflow", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to start workflow")
		return
	}
	slog.Debug("brreg source financial workflow started", "workflow_id", workflowID, "run_id", run.GetRunID())

	writeJSON(w, http.StatusAccepted, startWorkflowResponse{
		Status:        "started",
		Workflow:      brregworkflow.FetchBrregSourceFinancialStatementsWorkflowName,
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

func (h *Handlers) handleStartAriregisterBulkRawIngestWorkflow(w http.ResponseWriter, r *http.Request) {
	if h.temporal == nil {
		writeError(w, http.StatusServiceUnavailable, "temporal client not available")
		return
	}

	req, err := decodeStartAriregisterBulkRawIngestWorkflowRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	input := ariregisterworkflow.LoadAriregisterBulkRawRecordsInput{
		SourceURL: req.SourceURL,
		Limit:     req.Limit,
		BatchSize: req.BatchSize,
		Trigger:   req.Trigger,
	}
	workflowID := newWorkflowID("ariregister-bulk-ingest")
	slog.Debug("starting ariregister bulk raw ingest workflow",
		"workflow_id", workflowID,
		"task_queue", ariregisterworkflow.LoadAriregisterBulkRawRecordsTaskQueue,
		"source_url", req.SourceURL,
		"limit", req.Limit,
		"batch_size", req.BatchSize,
		"trigger", req.Trigger,
	)
	run, err := h.temporal.ExecuteWorkflow(
		r.Context(),
		client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: ariregisterworkflow.LoadAriregisterBulkRawRecordsTaskQueue,
		},
		ariregisterworkflow.LoadAriregisterBulkRawRecords,
		input,
	)
	if err != nil {
		slog.Error("start ariregister bulk raw ingest workflow", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to start workflow")
		return
	}
	slog.Debug("ariregister bulk raw ingest workflow started", "workflow_id", workflowID, "run_id", run.GetRunID())

	writeJSON(w, http.StatusAccepted, startWorkflowResponse{
		Status:        "started",
		Workflow:      ariregisterworkflow.LoadAriregisterBulkRawRecordsWorkflowName,
		WorkflowID:    workflowID,
		WorkflowRunID: run.GetRunID(),
	})
}

func (h *Handlers) handleStartAriregisterSourceProfileWorkflow(w http.ResponseWriter, r *http.Request) {
	if h.temporal == nil {
		writeError(w, http.StatusServiceUnavailable, "temporal client not available")
		return
	}

	req, err := decodeStartAriregisterSourceProfileWorkflowRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	input := ariregisterworkflow.NormalizeAriregisterSourceProfilesInput{
		IDs:       req.IDs,
		Filters:   req.Filters,
		Limit:     req.Limit,
		BatchSize: req.BatchSize,
		Trigger:   req.Trigger,
	}
	workflowID := newWorkflowID("ariregister-source-profile")
	slog.Debug("starting ariregister source profile workflow",
		"workflow_id", workflowID,
		"task_queue", ariregisterworkflow.NormalizeAriregisterSourceProfilesTaskQueue,
		"workflow", ariregisterworkflow.NormalizeAriregisterSourceProfilesWithCopyWorkflowName,
		"ids_count", len(req.IDs),
		"filters_count", len(req.Filters),
		"limit", req.Limit,
		"batch_size", req.BatchSize,
		"trigger", req.Trigger,
	)
	run, err := h.temporal.ExecuteWorkflow(
		r.Context(),
		client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: ariregisterworkflow.NormalizeAriregisterSourceProfilesTaskQueue,
		},
		ariregisterworkflow.NormalizeAriregisterSourceProfilesWithCopy,
		input,
	)
	if err != nil {
		slog.Error("start ariregister source profile workflow", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to start workflow")
		return
	}
	slog.Debug("ariregister source profile workflow started", "workflow_id", workflowID, "run_id", run.GetRunID())

	writeJSON(w, http.StatusAccepted, startWorkflowResponse{
		Status:        "started",
		Workflow:      ariregisterworkflow.NormalizeAriregisterSourceProfilesWithCopyWorkflowName,
		TaskQueue:     ariregisterworkflow.NormalizeAriregisterSourceProfilesTaskQueue,
		WorkflowID:    workflowID,
		WorkflowRunID: run.GetRunID(),
	})
}

func (h *Handlers) handleStartFranceSourceProfileWorkflow(w http.ResponseWriter, r *http.Request) {
	if h.temporal == nil {
		writeError(w, http.StatusServiceUnavailable, "temporal client not available")
		return
	}

	req, err := decodeStartFranceSourceProfileWorkflowRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	input := franceworkflow.NormalizeFranceSourceProfilesInput{
		IDs:       req.IDs,
		Filters:   req.Filters,
		Limit:     req.Limit,
		BatchSize: req.BatchSize,
		Trigger:   req.Trigger,
	}
	workflowID := newWorkflowID("france-source-profile")
	slog.Debug("starting france source profile workflow",
		"workflow_id", workflowID,
		"task_queue", franceworkflow.NormalizeFranceSourceProfilesTaskQueue,
		"workflow", franceworkflow.NormalizeFranceSourceProfilesWorkflowName,
		"ids_count", len(req.IDs),
		"filters_count", len(req.Filters),
		"limit", req.Limit,
		"batch_size", req.BatchSize,
		"trigger", req.Trigger,
	)
	run, err := h.temporal.ExecuteWorkflow(
		r.Context(),
		client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: franceworkflow.NormalizeFranceSourceProfilesTaskQueue,
		},
		franceworkflow.NormalizeFranceSourceProfiles,
		input,
	)
	if err != nil {
		slog.Error("start france source profile workflow", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to start workflow")
		return
	}
	slog.Debug("france source profile workflow started", "workflow_id", workflowID, "run_id", run.GetRunID())

	writeJSON(w, http.StatusAccepted, startWorkflowResponse{
		Status:        "started",
		Workflow:      franceworkflow.NormalizeFranceSourceProfilesWorkflowName,
		TaskQueue:     franceworkflow.NormalizeFranceSourceProfilesTaskQueue,
		WorkflowID:    workflowID,
		WorkflowRunID: run.GetRunID(),
	})
}

func (h *Handlers) handleStartFranceBulkRawIngestWorkflow(w http.ResponseWriter, r *http.Request) {
	if h.temporal == nil {
		writeError(w, http.StatusServiceUnavailable, "temporal client not available")
		return
	}

	req, err := decodeStartFranceBulkRawIngestWorkflowRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	input := franceworkflow.LoadFranceBulkRawRecordsInput{
		LegalUnitsURL:     req.LegalUnitsURL,
		EstablishmentsURL: req.EstablishmentsURL,
		Limit:             req.Limit,
		BatchSize:         req.BatchSize,
		Trigger:           req.Trigger,
	}
	workflowID := newWorkflowID("france-bulk-ingest")
	slog.Debug("starting france bulk raw ingest workflow",
		"workflow_id", workflowID,
		"task_queue", franceworkflow.LoadFranceBulkRawRecordsTaskQueue,
		"legal_units_url", req.LegalUnitsURL,
		"establishments_url", req.EstablishmentsURL,
		"limit", req.Limit,
		"batch_size", req.BatchSize,
		"trigger", req.Trigger,
	)
	run, err := h.temporal.ExecuteWorkflow(
		r.Context(),
		client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: franceworkflow.LoadFranceBulkRawRecordsTaskQueue,
		},
		franceworkflow.LoadFranceBulkRawRecords,
		input,
	)
	if err != nil {
		slog.Error("start france bulk raw ingest workflow", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to start workflow")
		return
	}
	slog.Debug("france bulk raw ingest workflow started", "workflow_id", workflowID, "run_id", run.GetRunID())

	writeJSON(w, http.StatusAccepted, startWorkflowResponse{
		Status:        "started",
		Workflow:      franceworkflow.LoadFranceBulkRawRecordsWorkflowName,
		WorkflowID:    workflowID,
		WorkflowRunID: run.GetRunID(),
	})
}

func (h *Handlers) handleStartSEBulkRawIngestWorkflow(w http.ResponseWriter, r *http.Request) {
	if h.temporal == nil {
		writeError(w, http.StatusServiceUnavailable, "temporal client not available")
		return
	}

	req, err := decodeStartSEBulkRawIngestWorkflowRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	input := seworkflow.LoadSEBulkRawRecordsInput{
		Limit:     req.Limit,
		BatchSize: req.BatchSize,
		Trigger:   req.Trigger,
	}
	workflowID := newWorkflowID("se-bulk-ingest")
	slog.Debug("starting se bulk raw ingest workflow",
		"workflow_id", workflowID,
		"task_queue", seworkflow.LoadSEBulkRawRecordsTaskQueue,
		"limit", req.Limit,
		"batch_size", req.BatchSize,
		"trigger", req.Trigger,
	)
	run, err := h.temporal.ExecuteWorkflow(
		r.Context(),
		client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: seworkflow.LoadSEBulkRawRecordsTaskQueue,
		},
		seworkflow.LoadSEBulkRawRecords,
		input,
	)
	if err != nil {
		slog.Error("start se bulk raw ingest workflow", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to start workflow")
		return
	}
	slog.Debug("se bulk raw ingest workflow started", "workflow_id", workflowID, "run_id", run.GetRunID())

	writeJSON(w, http.StatusAccepted, startWorkflowResponse{
		Status:        "started",
		Workflow:      seworkflow.LoadSEBulkRawRecordsWorkflowName,
		WorkflowID:    workflowID,
		WorkflowRunID: run.GetRunID(),
	})
}

func (h *Handlers) handleStartCVRRawIngestWorkflow(w http.ResponseWriter, r *http.Request) {
	if h.temporal == nil {
		writeError(w, http.StatusServiceUnavailable, "temporal client not available")
		return
	}

	req, err := decodeStartCVRRawIngestWorkflowRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	input := cvrworkflow.LoadCVRRawRecordsInput{
		SourceURL: req.SourceURL,
		ScrollURL: req.ScrollURL,
		Scroll:    req.Scroll,
		Limit:     req.Limit,
		PageSize:  req.PageSize,
		BatchSize: req.BatchSize,
		Trigger:   req.Trigger,
	}
	workflowID := newWorkflowID("cvr-raw-ingest")
	slog.Debug("starting cvr raw ingest workflow",
		"workflow_id", workflowID,
		"task_queue", cvrworkflow.LoadCVRRawRecordsTaskQueue,
		"source_url", req.SourceURL,
		"scroll_url", req.ScrollURL,
		"scroll", req.Scroll,
		"limit", req.Limit,
		"page_size", req.PageSize,
		"batch_size", req.BatchSize,
		"trigger", req.Trigger,
	)
	run, err := h.temporal.ExecuteWorkflow(
		r.Context(),
		client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: cvrworkflow.LoadCVRRawRecordsTaskQueue,
		},
		cvrworkflow.LoadCVRRawRecords,
		input,
	)
	if err != nil {
		slog.Error("start cvr raw ingest workflow", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to start workflow")
		return
	}
	slog.Debug("cvr raw ingest workflow started", "workflow_id", workflowID, "run_id", run.GetRunID())

	writeJSON(w, http.StatusAccepted, startWorkflowResponse{
		Status:        "started",
		Workflow:      cvrworkflow.LoadCVRRawRecordsWorkflowName,
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

func (h *Handlers) handleStartExchangeRateSyncWorkflow(w http.ResponseWriter, r *http.Request) {
	if h.temporal == nil {
		writeError(w, http.StatusServiceUnavailable, "temporal client not available")
		return
	}

	req, err := h.decodeStartExchangeRateSyncWorkflowRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	input := fx.SyncExchangeRatesInput{
		Provider:       req.Provider,
		SourceURL:      req.SourceURL,
		Trigger:        req.Trigger,
		ForceReprocess: req.ForceReprocess,
	}
	workflowID := newWorkflowID("fx-rate-sync")
	slog.Debug("starting exchange rate sync workflow",
		"workflow_id", workflowID,
		"task_queue", fx.SyncTaskQueue,
		"provider", req.Provider,
		"source_url", req.SourceURL,
		"trigger", req.Trigger,
		"force_reprocess", req.ForceReprocess,
	)
	run, err := h.temporal.ExecuteWorkflow(
		r.Context(),
		client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: fx.SyncTaskQueue,
		},
		fx.SyncExchangeRates,
		input,
	)
	if err != nil {
		slog.Error("start exchange rate sync workflow", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to start workflow")
		return
	}
	slog.Debug("exchange rate sync workflow started", "workflow_id", workflowID, "run_id", run.GetRunID())

	writeJSON(w, http.StatusAccepted, startWorkflowResponse{
		Status:        "started",
		Workflow:      fx.SyncWorkflowName,
		WorkflowID:    workflowID,
		WorkflowRunID: run.GetRunID(),
	})
}

func (h *Handlers) handleListBrregWorkflowRuns(w http.ResponseWriter, r *http.Request) {
	if h.temporal == nil {
		writeError(w, http.StatusServiceUnavailable, "temporal client not available")
		return
	}

	limit, err := parseBrregWorkflowRunsLimit(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	visibilityQuery, err := brregWorkflowRunsVisibilityQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	response, err := h.temporal.ListWorkflow(r.Context(), &workflowservicepb.ListWorkflowExecutionsRequest{
		PageSize: limit,
		Query:    visibilityQuery,
	})
	if err != nil {
		slog.Error("list brreg workflow runs", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list workflow runs")
		return
	}

	search := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("q")))
	items := make([]brregWorkflowRunResponse, 0, len(response.GetExecutions()))
	for _, execution := range response.GetExecutions() {
		item := brregWorkflowRunResponse{
			WorkflowID:   execution.GetExecution().GetWorkflowId(),
			RunID:        execution.GetExecution().GetRunId(),
			WorkflowType: execution.GetType().GetName(),
			Status:       workflowExecutionStatusString(execution.GetStatus()),
		}
		prefix := brregWorkflowPrefixForExecution(item.WorkflowID, item.WorkflowType)
		if prefix == nil {
			continue
		}
		item.Prefix = prefix.Prefix
		item.Action = prefix.Label
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
		if !brregWorkflowRunMatchesSearch(item, search) {
			continue
		}
		items = append(items, item)
	}

	writeJSON(w, http.StatusOK, brregWorkflowRunListResponse{
		Prefixes: brregWorkflowPrefixResponses(),
		Items:    items,
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

func (h *Handlers) handleListExchangeRateSyncWorkflowRuns(w http.ResponseWriter, r *http.Request) {
	if h.temporal == nil {
		writeError(w, http.StatusServiceUnavailable, "temporal client not available")
		return
	}

	limit, err := parseFXWorkflowRunsLimit(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	response, err := h.temporal.ListWorkflow(r.Context(), &workflowservicepb.ListWorkflowExecutionsRequest{
		PageSize: limit,
		Query:    "WorkflowType = 'SyncExchangeRates'",
	})
	if err != nil {
		slog.Error("list exchange rate sync workflow runs", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list workflow runs")
		return
	}

	items := make([]exchangeRateSyncWorkflowRunResponse, 0, len(response.GetExecutions()))
	for _, execution := range response.GetExecutions() {
		item := exchangeRateSyncWorkflowRunResponse{
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

	writeJSON(w, http.StatusOK, exchangeRateSyncWorkflowRunListResponse{Items: items})
}

func decodeStartBrregCompanyTranslationWorkflowRequest(r *http.Request) (startBrregCompanyTranslationWorkflowRequest, error) {
	var req startBrregCompanyTranslationWorkflowRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		return startBrregCompanyTranslationWorkflowRequest{}, errors.New("invalid request body")
	}
	req.Provider = strings.TrimSpace(req.Provider)
	req.Model = strings.TrimSpace(req.Model)
	req.PromptVersion = strings.TrimSpace(req.PromptVersion)
	req.ClaimMode = strings.ToLower(strings.TrimSpace(req.ClaimMode))
	req.Trigger = strings.TrimSpace(req.Trigger)
	req.IDs = compactRequestStrings(req.IDs)
	req.Filters = compactRequestFilters(req.Filters)
	if req.Trigger == "" {
		req.Trigger = "manual"
	}
	for _, id := range req.IDs {
		if _, err := uuid.Parse(id); err != nil {
			return startBrregCompanyTranslationWorkflowRequest{}, errors.New("ids must contain valid UUID values")
		}
	}
	if req.ClaimMode != "" && req.ClaimMode != "auto" && req.ClaimMode != "fixed" {
		return startBrregCompanyTranslationWorkflowRequest{}, errors.New("claim_mode must be auto or fixed")
	}
	if req.Limit < 0 {
		return startBrregCompanyTranslationWorkflowRequest{}, errors.New("limit cannot be negative")
	}
	if req.BatchSize < 0 {
		return startBrregCompanyTranslationWorkflowRequest{}, errors.New("batch_size cannot be negative")
	}
	if req.MaxRequestChars < 0 {
		return startBrregCompanyTranslationWorkflowRequest{}, errors.New("max_request_chars cannot be negative")
	}
	if req.MaxTerms < 0 {
		return startBrregCompanyTranslationWorkflowRequest{}, errors.New("max_terms cannot be negative")
	}
	if req.MaxCompaniesPerBatch < 0 {
		return startBrregCompanyTranslationWorkflowRequest{}, errors.New("max_companies_per_batch cannot be negative")
	}
	if req.MaxBatches < 0 {
		return startBrregCompanyTranslationWorkflowRequest{}, errors.New("max_batches cannot be negative")
	}
	if req.MaxParallelTasks < 0 {
		return startBrregCompanyTranslationWorkflowRequest{}, errors.New("max_parallel_tasks cannot be negative")
	}
	if req.LeaseSeconds < 0 {
		return startBrregCompanyTranslationWorkflowRequest{}, errors.New("lease_seconds cannot be negative")
	}
	if req.MaxAttempts < 0 {
		return startBrregCompanyTranslationWorkflowRequest{}, errors.New("max_attempts cannot be negative")
	}
	if req.BatchDelaySeconds < 0 {
		return startBrregCompanyTranslationWorkflowRequest{}, errors.New("batch_delay_seconds cannot be negative")
	}
	return req, nil
}

func decodeStartAriregisterCompanyTranslationWorkflowRequest(r *http.Request) (startAriregisterCompanyTranslationWorkflowRequest, error) {
	var req startAriregisterCompanyTranslationWorkflowRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		return startAriregisterCompanyTranslationWorkflowRequest{}, errors.New("invalid request body")
	}
	req.Provider = strings.TrimSpace(req.Provider)
	req.Model = strings.TrimSpace(req.Model)
	req.PromptVersion = strings.TrimSpace(req.PromptVersion)
	req.ClaimMode = strings.ToLower(strings.TrimSpace(req.ClaimMode))
	req.Trigger = strings.TrimSpace(req.Trigger)
	req.IDs = compactRequestStrings(req.IDs)
	req.Filters = compactRequestFilters(req.Filters)
	if req.Trigger == "" {
		req.Trigger = "manual"
	}
	for _, id := range req.IDs {
		if _, err := uuid.Parse(id); err != nil {
			return startAriregisterCompanyTranslationWorkflowRequest{}, errors.New("ids must contain valid UUID values")
		}
	}
	if req.ClaimMode != "" && req.ClaimMode != "auto" && req.ClaimMode != "fixed" {
		return startAriregisterCompanyTranslationWorkflowRequest{}, errors.New("claim_mode must be auto or fixed")
	}
	if req.Limit < 0 {
		return startAriregisterCompanyTranslationWorkflowRequest{}, errors.New("limit cannot be negative")
	}
	if req.BatchSize < 0 {
		return startAriregisterCompanyTranslationWorkflowRequest{}, errors.New("batch_size cannot be negative")
	}
	if req.MaxRequestChars < 0 {
		return startAriregisterCompanyTranslationWorkflowRequest{}, errors.New("max_request_chars cannot be negative")
	}
	if req.MaxTerms < 0 {
		return startAriregisterCompanyTranslationWorkflowRequest{}, errors.New("max_terms cannot be negative")
	}
	if req.MaxCompaniesPerBatch < 0 {
		return startAriregisterCompanyTranslationWorkflowRequest{}, errors.New("max_companies_per_batch cannot be negative")
	}
	if req.MaxBatches < 0 {
		return startAriregisterCompanyTranslationWorkflowRequest{}, errors.New("max_batches cannot be negative")
	}
	if req.MaxParallelTasks < 0 {
		return startAriregisterCompanyTranslationWorkflowRequest{}, errors.New("max_parallel_tasks cannot be negative")
	}
	if req.LeaseSeconds < 0 {
		return startAriregisterCompanyTranslationWorkflowRequest{}, errors.New("lease_seconds cannot be negative")
	}
	if req.MaxAttempts < 0 {
		return startAriregisterCompanyTranslationWorkflowRequest{}, errors.New("max_attempts cannot be negative")
	}
	if req.BatchDelaySeconds < 0 {
		return startAriregisterCompanyTranslationWorkflowRequest{}, errors.New("batch_delay_seconds cannot be negative")
	}
	return req, nil
}

func compactRequestStrings(values []string) []string {
	compact := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			compact = append(compact, value)
		}
	}
	return compact
}

func compactRequestFilters(filters map[string]string) map[string]string {
	if len(filters) == 0 {
		return nil
	}
	compact := make(map[string]string, len(filters))
	for key, value := range filters {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key != "" && value != "" {
			compact[key] = value
		}
	}
	if len(compact) == 0 {
		return nil
	}
	return compact
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
	if req.BatchSize < 0 {
		return startBrregSourceProfileNormalizationWorkflowRequest{}, errors.New("batch size cannot be negative")
	}
	for _, id := range req.IDs {
		if _, err := uuid.Parse(id); err != nil {
			return startBrregSourceProfileNormalizationWorkflowRequest{}, errors.New("ids must contain valid UUID values")
		}
	}
	return req, nil
}

func decodeStartBrregSourceExplorerRefreshWorkflowRequest(r *http.Request) (startBrregSourceExplorerRefreshWorkflowRequest, error) {
	var req startBrregSourceExplorerRefreshWorkflowRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		return startBrregSourceExplorerRefreshWorkflowRequest{}, errors.New("invalid request body")
	}
	req.Trigger = strings.TrimSpace(req.Trigger)
	if req.Trigger == "" {
		req.Trigger = "manual"
	}
	return req, nil
}

func decodeStartBrregSourceCapitalFXWorkflowRequest(r *http.Request) (startBrregSourceCapitalFXWorkflowRequest, error) {
	var req startBrregSourceCapitalFXWorkflowRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		return startBrregSourceCapitalFXWorkflowRequest{}, errors.New("invalid request body")
	}
	req.RateDate = strings.TrimSpace(req.RateDate)
	req.Trigger = strings.TrimSpace(req.Trigger)
	if req.Trigger == "" {
		req.Trigger = "manual"
	}
	if req.Limit < 0 {
		return startBrregSourceCapitalFXWorkflowRequest{}, errors.New("limit cannot be negative")
	}
	if req.RateDate != "" {
		if _, err := time.Parse("2006-01-02", req.RateDate); err != nil {
			return startBrregSourceCapitalFXWorkflowRequest{}, errors.New("rate_date must use YYYY-MM-DD format")
		}
	}
	for _, id := range req.IDs {
		if _, err := uuid.Parse(id); err != nil {
			return startBrregSourceCapitalFXWorkflowRequest{}, errors.New("ids must contain valid UUID values")
		}
	}
	return req, nil
}

func decodeStartBrregSourceFinancialWorkflowRequest(r *http.Request) (startBrregSourceFinancialWorkflowRequest, error) {
	var req startBrregSourceFinancialWorkflowRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		return startBrregSourceFinancialWorkflowRequest{}, errors.New("invalid request body")
	}
	req.Trigger = strings.TrimSpace(req.Trigger)
	if req.Trigger == "" {
		req.Trigger = "manual"
	}
	if req.Limit < 0 {
		return startBrregSourceFinancialWorkflowRequest{}, errors.New("limit cannot be negative")
	}
	if req.BatchSize < 0 {
		return startBrregSourceFinancialWorkflowRequest{}, errors.New("batch_size cannot be negative")
	}
	if req.MaxParallelTasks < 0 {
		return startBrregSourceFinancialWorkflowRequest{}, errors.New("max_parallel_tasks cannot be negative")
	}
	if req.LeaseSeconds < 0 {
		return startBrregSourceFinancialWorkflowRequest{}, errors.New("lease_seconds cannot be negative")
	}
	if req.MaxAttempts < 0 {
		return startBrregSourceFinancialWorkflowRequest{}, errors.New("max_attempts cannot be negative")
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

func decodeStartAriregisterBulkRawIngestWorkflowRequest(r *http.Request) (startAriregisterBulkRawIngestWorkflowRequest, error) {
	var req startAriregisterBulkRawIngestWorkflowRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		return startAriregisterBulkRawIngestWorkflowRequest{}, errors.New("invalid request body")
	}
	req.SourceURL = strings.TrimSpace(req.SourceURL)
	req.Trigger = strings.TrimSpace(req.Trigger)
	if req.Trigger == "" {
		req.Trigger = "manual"
	}
	if req.Limit < 0 {
		return startAriregisterBulkRawIngestWorkflowRequest{}, errors.New("limit cannot be negative")
	}
	if req.BatchSize < 0 {
		return startAriregisterBulkRawIngestWorkflowRequest{}, errors.New("batch_size cannot be negative")
	}
	if req.SourceURL != "" {
		parsed, err := url.Parse(req.SourceURL)
		if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			return startAriregisterBulkRawIngestWorkflowRequest{}, errors.New("source_url must be http or https")
		}
	}
	return req, nil
}

func decodeStartAriregisterSourceProfileWorkflowRequest(r *http.Request) (startAriregisterSourceProfileWorkflowRequest, error) {
	var req startAriregisterSourceProfileWorkflowRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		return startAriregisterSourceProfileWorkflowRequest{}, errors.New("invalid request body")
	}
	req.Trigger = strings.TrimSpace(req.Trigger)
	if req.Limit < 0 {
		return startAriregisterSourceProfileWorkflowRequest{}, errors.New("limit cannot be negative")
	}
	if req.BatchSize < 0 {
		return startAriregisterSourceProfileWorkflowRequest{}, errors.New("batch_size cannot be negative")
	}
	return req, nil
}

func decodeStartFranceSourceProfileWorkflowRequest(r *http.Request) (startFranceSourceProfileWorkflowRequest, error) {
	var req startFranceSourceProfileWorkflowRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		return startFranceSourceProfileWorkflowRequest{}, errors.New("invalid request body")
	}
	req.Trigger = strings.TrimSpace(req.Trigger)
	if req.Trigger == "" {
		req.Trigger = "manual"
	}
	if req.Limit < 0 {
		return startFranceSourceProfileWorkflowRequest{}, errors.New("limit cannot be negative")
	}
	if req.BatchSize < 0 {
		return startFranceSourceProfileWorkflowRequest{}, errors.New("batch_size cannot be negative")
	}
	return req, nil
}

func decodeStartFranceBulkRawIngestWorkflowRequest(r *http.Request) (startFranceBulkRawIngestWorkflowRequest, error) {
	var req startFranceBulkRawIngestWorkflowRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		return startFranceBulkRawIngestWorkflowRequest{}, errors.New("invalid request body")
	}
	req.LegalUnitsURL = strings.TrimSpace(req.LegalUnitsURL)
	req.EstablishmentsURL = strings.TrimSpace(req.EstablishmentsURL)
	req.Trigger = strings.TrimSpace(req.Trigger)
	if req.Trigger == "" {
		req.Trigger = "manual"
	}
	if req.Limit < 0 {
		return startFranceBulkRawIngestWorkflowRequest{}, errors.New("limit cannot be negative")
	}
	if req.BatchSize < 0 {
		return startFranceBulkRawIngestWorkflowRequest{}, errors.New("batch_size cannot be negative")
	}
	if err := validateOptionalHTTPURL(req.LegalUnitsURL, "legal_units_url"); err != nil {
		return startFranceBulkRawIngestWorkflowRequest{}, err
	}
	if err := validateOptionalHTTPURL(req.EstablishmentsURL, "establishments_url"); err != nil {
		return startFranceBulkRawIngestWorkflowRequest{}, err
	}
	return req, nil
}

func decodeStartSEBulkRawIngestWorkflowRequest(r *http.Request) (startSEBulkRawIngestWorkflowRequest, error) {
	var req startSEBulkRawIngestWorkflowRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		return startSEBulkRawIngestWorkflowRequest{}, errors.New("invalid request body")
	}
	req.Trigger = strings.TrimSpace(req.Trigger)
	if req.Trigger == "" {
		req.Trigger = "manual"
	}
	if req.Limit < 0 {
		return startSEBulkRawIngestWorkflowRequest{}, errors.New("limit cannot be negative")
	}
	if req.BatchSize < 0 {
		return startSEBulkRawIngestWorkflowRequest{}, errors.New("batch_size cannot be negative")
	}
	return req, nil
}

func decodeStartCVRRawIngestWorkflowRequest(r *http.Request) (startCVRRawIngestWorkflowRequest, error) {
	var req startCVRRawIngestWorkflowRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		return startCVRRawIngestWorkflowRequest{}, errors.New("invalid request body")
	}
	req.SourceURL = strings.TrimSpace(req.SourceURL)
	req.ScrollURL = strings.TrimSpace(req.ScrollURL)
	req.Scroll = strings.TrimSpace(req.Scroll)
	req.Trigger = strings.TrimSpace(req.Trigger)
	if req.Trigger == "" {
		req.Trigger = "manual"
	}
	if req.Limit < 0 {
		return startCVRRawIngestWorkflowRequest{}, errors.New("limit cannot be negative")
	}
	if req.PageSize < 0 {
		return startCVRRawIngestWorkflowRequest{}, errors.New("page_size cannot be negative")
	}
	if req.BatchSize < 0 {
		return startCVRRawIngestWorkflowRequest{}, errors.New("batch_size cannot be negative")
	}
	if err := validateOptionalHTTPURL(req.SourceURL, "source_url"); err != nil {
		return startCVRRawIngestWorkflowRequest{}, err
	}
	if err := validateOptionalHTTPURL(req.ScrollURL, "scroll_url"); err != nil {
		return startCVRRawIngestWorkflowRequest{}, err
	}
	return req, nil
}

func validateOptionalHTTPURL(value string, fieldName string) error {
	if value == "" {
		return nil
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return errors.Newf("%s must be http or https", fieldName)
	}
	return nil
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

func (h *Handlers) decodeStartExchangeRateSyncWorkflowRequest(r *http.Request) (startExchangeRateSyncWorkflowRequest, error) {
	var req startExchangeRateSyncWorkflowRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		return startExchangeRateSyncWorkflowRequest{}, errors.New("invalid request body")
	}
	req.Provider = strings.ToLower(strings.TrimSpace(req.Provider))
	req.SourceURL = strings.TrimSpace(req.SourceURL)
	req.Trigger = strings.TrimSpace(req.Trigger)
	if req.Provider == "" {
		req.Provider = fx.DefaultProvider
	}
	if req.SourceURL == "" {
		req.SourceURL = strings.TrimSpace(h.fxSourceURL)
	}
	if req.SourceURL == "" {
		req.SourceURL = fx.DefaultDailySourceURL
	}
	if req.Trigger == "" {
		req.Trigger = "manual"
	}
	if req.Provider != fx.DefaultProvider {
		return startExchangeRateSyncWorkflowRequest{}, errors.New("provider must be ecb")
	}
	parsed, err := url.Parse(req.SourceURL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return startExchangeRateSyncWorkflowRequest{}, errors.New("source_url must be http or https")
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

func parseFXWorkflowRunsLimit(r *http.Request) (int32, error) {
	rawLimit := strings.TrimSpace(r.URL.Query().Get("limit"))
	if rawLimit == "" {
		return defaultFXWorkflowRunsLimit, nil
	}
	limit, err := strconv.Atoi(rawLimit)
	if err != nil || limit <= 0 {
		return 0, errors.New("limit must be a positive integer")
	}
	if limit > maxFXWorkflowRunsLimit {
		limit = maxFXWorkflowRunsLimit
	}
	return int32(limit), nil
}

func parseBrregWorkflowRunsLimit(r *http.Request) (int32, error) {
	rawLimit := strings.TrimSpace(r.URL.Query().Get("limit"))
	if rawLimit == "" {
		return defaultBrregWorkflowRunsLimit, nil
	}
	limit, err := strconv.Atoi(rawLimit)
	if err != nil || limit <= 0 {
		return 0, errors.New("limit must be a positive integer")
	}
	if limit > maxBrregWorkflowRunsLimit {
		limit = maxBrregWorkflowRunsLimit
	}
	return int32(limit), nil
}

type brregWorkflowPrefix struct {
	Prefix       string
	Label        string
	WorkflowType string
}

var brregWorkflowPrefixes = []brregWorkflowPrefix{
	{
		Prefix:       "brreg-company-translation",
		Label:        "Company translation",
		WorkflowType: brregworkflow.TranslateBrregSourceCompaniesWorkflowName,
	},
	{
		Prefix:       "brreg-domain-search",
		Label:        "Domain discovery",
		WorkflowType: brregworkflow.SearchBrregDomainsWorkflowName,
	},
	{
		Prefix:       "brreg-source-profile",
		Label:        "Source profile sync",
		WorkflowType: brregworkflow.NormalizeBrregSourceProfilesWithCopyWorkflowName,
	},
	{
		Prefix:       "brreg-source-explorer-refresh",
		Label:        "Source explorer refresh",
		WorkflowType: brregworkflow.RefreshBrregSourceExplorerWorkflowName,
	},
	{
		Prefix:       "brreg-source-capital-fx",
		Label:        "Capital FX conversion",
		WorkflowType: brregworkflow.ConvertBrregSourceCapitalToUSDWorkflowName,
	},
	{
		Prefix:       "brreg-source-financial",
		Label:        "Source financial records",
		WorkflowType: brregworkflow.FetchBrregSourceFinancialStatementsWorkflowName,
	},
	{
		Prefix:       "brreg-bulk-ingest",
		Label:        "Bulk raw ingest",
		WorkflowType: brregworkflow.LoadBrregBulkRawRecordsWorkflowName,
	},
}

func brregWorkflowPrefixResponses() []brregWorkflowPrefixResponse {
	responses := make([]brregWorkflowPrefixResponse, 0, len(brregWorkflowPrefixes))
	for _, prefix := range brregWorkflowPrefixes {
		responses = append(responses, brregWorkflowPrefixResponse{
			Prefix:       prefix.Prefix,
			Label:        prefix.Label,
			WorkflowType: prefix.WorkflowType,
		})
	}
	return responses
}

func brregWorkflowRunsVisibilityQuery(r *http.Request) (string, error) {
	prefixes, err := selectedBrregWorkflowPrefixes(r.URL.Query().Get("prefix"))
	if err != nil {
		return "", err
	}

	parts := []string{workflowTypeVisibilityQuery(prefixes)}
	if len(prefixes) == 1 {
		parts = append(parts, "WorkflowId STARTS_WITH '"+prefixes[0].Prefix+"-'")
	}
	if status := strings.TrimSpace(r.URL.Query().Get("status")); status != "" {
		statusValue, err := brregWorkflowExecutionStatusQueryValue(status)
		if err != nil {
			return "", err
		}
		parts = append(parts, "ExecutionStatus = '"+statusValue+"'")
	}

	return strings.Join(parts, " AND "), nil
}

func selectedBrregWorkflowPrefixes(prefix string) ([]brregWorkflowPrefix, error) {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return brregWorkflowPrefixes, nil
	}
	for _, candidate := range brregWorkflowPrefixes {
		if candidate.Prefix == prefix {
			return []brregWorkflowPrefix{candidate}, nil
		}
	}
	return nil, errors.New("unsupported brreg workflow prefix")
}

func workflowTypeVisibilityQuery(prefixes []brregWorkflowPrefix) string {
	if len(prefixes) == 1 {
		return "WorkflowType = '" + prefixes[0].WorkflowType + "'"
	}
	values := make([]string, 0, len(prefixes))
	for _, prefix := range prefixes {
		values = append(values, "'"+prefix.WorkflowType+"'")
	}
	return "WorkflowType IN (" + strings.Join(values, ", ") + ")"
}

func brregWorkflowExecutionStatusQueryValue(status string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "running":
		return "Running", nil
	case "completed":
		return "Completed", nil
	case "failed":
		return "Failed", nil
	case "canceled":
		return "Canceled", nil
	case "terminated":
		return "Terminated", nil
	case "continued_as_new":
		return "ContinuedAsNew", nil
	case "timed_out":
		return "TimedOut", nil
	default:
		return "", errors.New("unsupported brreg workflow status")
	}
}

func brregWorkflowPrefixForExecution(workflowID string, workflowType string) *brregWorkflowPrefix {
	for _, prefix := range brregWorkflowPrefixes {
		if strings.HasPrefix(workflowID, prefix.Prefix+"-") || workflowType == prefix.WorkflowType {
			return &prefix
		}
	}
	return nil
}

func brregWorkflowRunMatchesSearch(item brregWorkflowRunResponse, search string) bool {
	if search == "" {
		return true
	}
	haystack := strings.ToLower(strings.Join([]string{
		item.WorkflowID,
		item.RunID,
		item.WorkflowType,
		item.Prefix,
		item.Action,
		item.Status,
	}, " "))
	return strings.Contains(haystack, search)
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
