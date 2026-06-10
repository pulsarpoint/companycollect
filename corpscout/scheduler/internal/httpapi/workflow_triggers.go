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
	enumspb "go.temporal.io/api/enums/v1"
	workflowservicepb "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	"github.com/pulsarpoint/corpscout/scheduler/internal/fx"
	domainnace "github.com/pulsarpoint/corpscout/scheduler/internal/nacetaxonomy"
	naceworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/temporal/workflow/nace"
)

const defaultNACETaxonomyWorkflowRunsLimit = 10
const maxNACETaxonomyWorkflowRunsLimit = 50
const defaultNACEClickHouseWorkflowRunsLimit = 10
const maxNACEClickHouseWorkflowRunsLimit = 50
const defaultFXWorkflowRunsLimit = 10
const maxFXWorkflowRunsLimit = 50

type startWorkflowResponse struct {
	Status        string `json:"status"`
	Workflow      string `json:"workflow"`
	TaskQueue     string `json:"task_queue,omitempty"`
	WorkflowID    string `json:"workflow_id"`
	WorkflowRunID string `json:"workflow_run_id"`
	RunID         string `json:"run_id,omitempty"`
}

type startNACETaxonomySyncWorkflowRequest struct {
	Revision       string `json:"revision,omitempty"`
	SourceURL      string `json:"source_url,omitempty"`
	Trigger        string `json:"trigger,omitempty"`
	ForceReprocess bool   `json:"force_reprocess,omitempty"`
}

type startNACEClickHouseSyncWorkflowRequest struct {
	Trigger string `json:"trigger,omitempty"`
}

type startExchangeRateSyncWorkflowRequest struct {
	Provider       string `json:"provider,omitempty"`
	SourceURL      string `json:"source_url,omitempty"`
	Trigger        string `json:"trigger,omitempty"`
	ForceReprocess bool   `json:"force_reprocess,omitempty"`
}

type naceTaxonomyWorkflowRunListResponse struct {
	Items []naceTaxonomyWorkflowRunResponse `json:"items"`
}

type naceClickHouseWorkflowRunListResponse struct {
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

	input := naceworkflow.SyncNACETaxonomyInput{
		Revision:       req.Revision,
		SourceURL:      req.SourceURL,
		Trigger:        req.Trigger,
		ForceReprocess: req.ForceReprocess,
	}
	workflowID := newWorkflowID("nace-taxonomy-sync")
	slog.Debug("starting nace taxonomy sync workflow",
		"workflow_id", workflowID,
		"task_queue", naceworkflow.SyncTaskQueue,
		"revision", req.Revision,
		"source_url", req.SourceURL,
		"trigger", req.Trigger,
		"force_reprocess", req.ForceReprocess,
	)
	run, err := h.temporal.ExecuteWorkflow(
		r.Context(),
		client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: naceworkflow.SyncTaskQueue,
		},
		naceworkflow.SyncNACETaxonomy,
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
		Workflow:      naceworkflow.SyncWorkflowName,
		WorkflowID:    workflowID,
		WorkflowRunID: run.GetRunID(),
	})
}

func (h *Handlers) handleStartNACEClickHouseSyncWorkflow(w http.ResponseWriter, r *http.Request) {
	if h.temporal == nil {
		writeError(w, http.StatusServiceUnavailable, "temporal client not available")
		return
	}

	req := startNACEClickHouseSyncWorkflowRequest{Trigger: "manual"}
	if r.Body != nil {
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}
	}
	req.Trigger = strings.TrimSpace(req.Trigger)
	if req.Trigger == "" {
		req.Trigger = "manual"
	}

	workflowID := newWorkflowID("nace-clickhouse-sync")
	slog.Debug("starting nace clickhouse sync workflow",
		"workflow_id", workflowID,
		"task_queue", naceworkflow.SyncTaskQueue,
		"trigger", req.Trigger,
	)
	run, err := h.temporal.ExecuteWorkflow(
		r.Context(),
		client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: naceworkflow.SyncTaskQueue,
		},
		naceworkflow.SyncNACEToClickHouse,
		naceworkflow.SyncNACEToClickHouseInput{Trigger: req.Trigger},
	)
	if err != nil {
		slog.Error("start nace clickhouse sync workflow", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to start workflow")
		return
	}
	slog.Debug("nace clickhouse sync workflow started", "workflow_id", workflowID, "run_id", run.GetRunID())

	writeJSON(w, http.StatusAccepted, startWorkflowResponse{
		Status:        "started",
		Workflow:      naceworkflow.SyncToClickHouseWorkflowName,
		TaskQueue:     naceworkflow.SyncTaskQueue,
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

func (h *Handlers) handleListNACEClickHouseSyncWorkflowRuns(w http.ResponseWriter, r *http.Request) {
	if h.temporal == nil {
		writeError(w, http.StatusServiceUnavailable, "temporal client not available")
		return
	}

	limit, err := parseNACEClickHouseWorkflowRunsLimit(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	response, err := h.temporal.ListWorkflow(r.Context(), &workflowservicepb.ListWorkflowExecutionsRequest{
		PageSize: limit,
		Query:    "WorkflowType = 'SyncNACEToClickHouse'",
	})
	if err != nil {
		slog.Error("list nace clickhouse sync workflow runs", "error", err)
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

	writeJSON(w, http.StatusOK, naceClickHouseWorkflowRunListResponse{Items: items})
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
		req.Revision = domainnace.DefaultRevision
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

func parseNACEClickHouseWorkflowRunsLimit(r *http.Request) (int32, error) {
	rawLimit := strings.TrimSpace(r.URL.Query().Get("limit"))
	if rawLimit == "" {
		return defaultNACEClickHouseWorkflowRunsLimit, nil
	}
	limit, err := strconv.Atoi(rawLimit)
	if err != nil || limit <= 0 {
		return 0, errors.New("limit must be a positive integer")
	}
	if limit > maxNACEClickHouseWorkflowRunsLimit {
		limit = maxNACEClickHouseWorkflowRunsLimit
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
