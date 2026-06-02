package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	workflowservicepb "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/types/known/timestamppb"

	brregworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/workflow"
	"github.com/pulsarpoint/corpscout/scheduler/internal/httpapi"
	"github.com/pulsarpoint/corpscout/scheduler/internal/nacetaxonomy"
)

func TestStartBrregTranslationWorkflowStartsTemporalWorkflow(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/brreg/translation", bytes.NewBufferString(`{
		"ids": ["7ffd5bf3-f96e-4907-9ef3-096eb4056ab8"],
		"filters": {"state": "raw"},
		"limit": 1000,
		"batch_size": 50,
		"max_attempts": 3,
		"trigger": "manual",
		"max_parallel_tasks": 25,
		"lease_seconds": 1200,
		"provider": "deepseek",
		"model": "deepseek-chat",
		"prompt_version": "v2",
		"source_lang": "no",
		"target_lang": "en",
		"max_service_retries": 4
	}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	require.Contains(t, w.Body.String(), `"status":"started"`)
	require.Contains(t, w.Body.String(), `"workflow":"TranslateBrregRawInputs"`)
	require.Contains(t, w.Body.String(), `"workflow_run_id":"run-1"`)
	require.NotEmpty(t, tc.options.ID)
	require.Equal(t, "brreg-translation", tc.options.TaskQueue)
	require.Equal(t, reflect.ValueOf(brregworkflow.TranslateBrregRawInputs).Pointer(), reflect.ValueOf(tc.workflow).Pointer())
	require.Len(t, tc.args, 1)

	input := tc.args[0].(brregworkflow.TranslateBrregRawInputsInput)
	require.Equal(t, []string{"7ffd5bf3-f96e-4907-9ef3-096eb4056ab8"}, input.IDs)
	require.Equal(t, map[string]string{"state": "raw"}, input.Filters)
	require.Equal(t, 1000, input.Limit)
	require.Zero(t, input.BatchSize)
	require.Zero(t, input.MaxAttempts)
	require.Equal(t, "manual", input.Trigger)
	require.Zero(t, input.MaxParallelTasks)
	require.Zero(t, input.LeaseSeconds)
	require.Empty(t, input.Provider)
	require.Empty(t, input.Model)
	require.Empty(t, input.PromptVersion)
	require.Empty(t, input.SourceLang)
	require.Empty(t, input.TargetLang)
	require.Zero(t, input.MaxServiceRetries)
}

func TestStartBrregTranslationWorkflowIgnoresAdvancedOptionsAtHTTPBoundary(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/brreg/translation", bytes.NewBufferString(`{
		"provider": "  ",
		"model": "  ",
		"prompt_version": " v1 ",
		"source_lang": " no ",
		"target_lang": " en ",
		"max_service_retries": -1
	}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	require.Len(t, tc.args, 1)

	input := tc.args[0].(brregworkflow.TranslateBrregRawInputsInput)
	require.Empty(t, input.Provider)
	require.Empty(t, input.Model)
	require.Empty(t, input.PromptVersion)
	require.Empty(t, input.SourceLang)
	require.Empty(t, input.TargetLang)
	require.Zero(t, input.MaxServiceRetries)
	require.Equal(t, "manual", input.Trigger)
}

func TestStartBrregTranslationWorkflowRejectsInvalidLimit(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/brreg/translation", bytes.NewBufferString(`{"limit": -1}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), `"error":"limit must be greater than zero when provided"`)
	require.Nil(t, tc.workflow)
}

func TestStartBrregTranslationWorkflowRejectsInvalidID(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/brreg/translation", bytes.NewBufferString(`{"ids":["not-a-uuid"]}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), `"error":"ids must contain valid UUID values"`)
	require.Nil(t, tc.workflow)
}

func TestStartBrregTranslationWorkflowRequiresTemporalClient(t *testing.T) {
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", nil, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/brreg/translation", bytes.NewBufferString(`{}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.Contains(t, w.Body.String(), `"error":"temporal client not available"`)
}

func TestStartBrregDomainSearchWorkflowStartsTemporalWorkflow(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/brreg/domain-search", bytes.NewBufferString(`{
		"ids": ["7ffd5bf3-f96e-4907-9ef3-096eb4056ab8"],
		"filters": {"domain_status": "not_started"},
		"limit": 1000,
		"batch_size": 10,
		"max_attempts": 3,
		"trigger": "manual",
		"max_parallel_tasks": 5,
		"lease_seconds": 1200,
		"search_engine": "yandex",
		"provider": "deepseek",
		"model": "deepseek-chat",
		"candidate_threshold": 60,
		"domain_threshold": 75,
		"max_candidates": 12,
		"max_site_checks": 4,
		"timeout_seconds": 90
	}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	require.Contains(t, w.Body.String(), `"status":"started"`)
	require.Contains(t, w.Body.String(), `"workflow":"SearchBrregDomains"`)
	require.Equal(t, "brreg-domain-search", tc.options.TaskQueue)
	require.Equal(t, reflect.ValueOf(brregworkflow.SearchBrregDomains).Pointer(), reflect.ValueOf(tc.workflow).Pointer())
	require.Len(t, tc.args, 1)

	input := tc.args[0].(brregworkflow.SearchBrregDomainsInput)
	require.Equal(t, []string{"7ffd5bf3-f96e-4907-9ef3-096eb4056ab8"}, input.IDs)
	require.Equal(t, map[string]string{"domain_status": "not_started"}, input.Filters)
	require.Equal(t, 1000, input.Limit)
	require.Equal(t, 10, input.BatchSize)
	require.Equal(t, 3, input.MaxAttempts)
	require.Equal(t, "manual", input.Trigger)
	require.Equal(t, 5, input.MaxParallelTasks)
	require.Equal(t, 1200, input.LeaseSeconds)
	require.Equal(t, "yandex", input.SearchEngine)
	require.Equal(t, "deepseek", input.Provider)
	require.Equal(t, "deepseek-chat", input.Model)
	require.Equal(t, 60, input.CandidateThreshold)
	require.Equal(t, 75, input.DomainThreshold)
	require.Equal(t, 12, input.MaxCandidates)
	require.Equal(t, 4, input.MaxSiteChecks)
	require.Equal(t, 90, input.TimeoutSeconds)
}

func TestStartBrregDomainSearchWorkflowDefaultsProviderAndSearchEngine(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/brreg/domain-search", bytes.NewBufferString(`{
		"search_engine": "  ",
		"provider": "  ",
		"model": "  "
	}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	input := tc.args[0].(brregworkflow.SearchBrregDomainsInput)
	require.Equal(t, "duckduckgo", input.SearchEngine)
	require.Equal(t, "default", input.Provider)
	require.Empty(t, input.Model)
	require.Equal(t, "manual", input.Trigger)
}

func TestStartBrregDomainSearchWorkflowRejectsInvalidSearchEngine(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/brreg/domain-search", bytes.NewBufferString(`{"search_engine":"google"}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), `"error":"search_engine must be duckduckgo or yandex"`)
	require.Nil(t, tc.workflow)
}

func TestStartBrregDomainSearchWorkflowRejectsInvalidID(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/brreg/domain-search", bytes.NewBufferString(`{"ids":["not-a-uuid"]}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), `"error":"ids must contain valid UUID values"`)
	require.Nil(t, tc.workflow)
}

func TestStartBrregSourceProfileNormalizationWorkflowStartsTemporalWorkflow(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/brreg/source-profile-normalization", bytes.NewBufferString(`{
		"ids": ["7ffd5bf3-f96e-4907-9ef3-096eb4056ab8"],
		"filters": {"query": "BORTIGARD"},
		"limit": 1000,
		"trigger": "manual"
	}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	require.Contains(t, w.Body.String(), `"status":"started"`)
	require.Contains(t, w.Body.String(), `"workflow":"NormalizeBrregSourceProfiles"`)
	require.Equal(t, "brreg-source-profile", tc.options.TaskQueue)
	require.Equal(t, reflect.ValueOf(brregworkflow.NormalizeBrregSourceProfiles).Pointer(), reflect.ValueOf(tc.workflow).Pointer())
	require.Len(t, tc.args, 1)

	input := tc.args[0].(brregworkflow.NormalizeBrregSourceProfilesInput)
	require.Equal(t, []string{"7ffd5bf3-f96e-4907-9ef3-096eb4056ab8"}, input.IDs)
	require.Equal(t, map[string]string{"query": "BORTIGARD"}, input.Filters)
	require.Equal(t, 1000, input.Limit)
	require.Equal(t, "manual", input.Trigger)
}

func TestStartBrregSourceProfileNormalizationWorkflowRejectsInvalidID(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/brreg/source-profile-normalization", bytes.NewBufferString(`{"ids":["not-a-uuid"]}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), `"error":"ids must contain valid UUID values"`)
	require.Nil(t, tc.workflow)
}

func TestStartBrregBulkRawIngestWorkflowStartsTemporalWorkflowAndAllowsAll(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/brreg/bulk-raw-ingest", bytes.NewBufferString(`{
		"limit": 0,
		"source_url": "https://example.test/enheter_alle.json.gz",
		"trigger": "manual"
	}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	require.Contains(t, w.Body.String(), `"status":"started"`)
	require.Contains(t, w.Body.String(), `"workflow":"LoadBrregBulkRawRecords"`)
	require.Equal(t, "brreg-bulk-ingest", tc.options.TaskQueue)
	require.Equal(t, reflect.ValueOf(brregworkflow.LoadBrregBulkRawRecords).Pointer(), reflect.ValueOf(tc.workflow).Pointer())
	require.Len(t, tc.args, 1)

	input := tc.args[0].(brregworkflow.LoadBrregBulkRawRecordsInput)
	require.Equal(t, 0, input.Limit)
	require.Equal(t, "https://example.test/enheter_alle.json.gz", input.SourceURL)
	require.Equal(t, "manual", input.Trigger)
}

func TestStartBrregBulkRawIngestWorkflowRejectsInvalidSourceURL(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/brreg/bulk-raw-ingest", bytes.NewBufferString(`{"source_url":"file:///tmp/brreg.json.gz"}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), `"error":"source_url must be http or https"`)
	require.Nil(t, tc.workflow)
}

func TestStartNACETaxonomySyncWorkflowStartsTemporalWorkflow(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/nace/taxonomy-sync", bytes.NewBufferString(`{
		"revision": "2.1",
		"source_url": "https://example.test/nace.rdf",
		"trigger": "manual",
		"force_reprocess": true
	}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	require.Contains(t, w.Body.String(), `"status":"started"`)
	require.Contains(t, w.Body.String(), `"workflow":"SyncNACETaxonomy"`)
	require.Equal(t, "nace-taxonomy-sync", tc.options.TaskQueue)
	require.Equal(t, reflect.ValueOf(nacetaxonomy.SyncNACETaxonomy).Pointer(), reflect.ValueOf(tc.workflow).Pointer())
	require.Len(t, tc.args, 1)

	input := tc.args[0].(nacetaxonomy.SyncNACETaxonomyInput)
	require.Equal(t, "2.1", input.Revision)
	require.Equal(t, "https://example.test/nace.rdf", input.SourceURL)
	require.Equal(t, "manual", input.Trigger)
	require.True(t, input.ForceReprocess)
}

func TestStartNACETaxonomySyncWorkflowUsesConfiguredSourceURL(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	handlers := httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, "").
		ConfigureNACE("https://example.test/configured-nace.rdf")
	r := routerFor(handlers)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/nace/taxonomy-sync", bytes.NewBufferString(`{
		"revision": "  ",
		"trigger": " scheduled "
	}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	require.Len(t, tc.args, 1)

	input := tc.args[0].(nacetaxonomy.SyncNACETaxonomyInput)
	require.Equal(t, nacetaxonomy.DefaultRevision, input.Revision)
	require.Equal(t, "https://example.test/configured-nace.rdf", input.SourceURL)
	require.Equal(t, "scheduled", input.Trigger)
}

func TestStartNACETaxonomySyncWorkflowRejectsMissingSourceURL(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/nace/taxonomy-sync", bytes.NewBufferString(`{}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), `"error":"nace source url is required"`)
	require.Nil(t, tc.workflow)
}

func TestStartNACETaxonomySyncWorkflowRejectsInvalidURL(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/nace/taxonomy-sync", bytes.NewBufferString(`{"source_url":"file:///tmp/nace.rdf"}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), `"error":"nace source url must be http or https"`)
	require.Nil(t, tc.workflow)
}

func TestListNACETaxonomySyncWorkflowRunsReturnsRecentTemporalExecutions(t *testing.T) {
	startedAt := time.Date(2026, 6, 2, 11, 30, 0, 0, time.UTC)
	closedAt := startedAt.Add(2 * time.Minute)
	tc := &temporalExecuteRecorder{
		listWorkflowResponse: &workflowservicepb.ListWorkflowExecutionsResponse{
			Executions: []*workflowpb.WorkflowExecutionInfo{{
				Execution:     &commonpb.WorkflowExecution{WorkflowId: "nace-taxonomy-sync-20260602", RunId: "run-123"},
				Type:          &commonpb.WorkflowType{Name: nacetaxonomy.SyncWorkflowName},
				Status:        enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
				StartTime:     timestamppb.New(startedAt),
				CloseTime:     timestamppb.New(closedAt),
				ExecutionTime: timestamppb.New(startedAt),
			}},
		},
	}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/workflows/nace/taxonomy-sync/runs?limit=5", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, tc.listWorkflowRequest)
	require.Equal(t, int32(5), tc.listWorkflowRequest.PageSize)
	require.Equal(t, "WorkflowType = 'SyncNACETaxonomy'", tc.listWorkflowRequest.Query)

	var body struct {
		Items []struct {
			WorkflowID    string `json:"workflow_id"`
			RunID         string `json:"run_id"`
			WorkflowType  string `json:"workflow_type"`
			Status        string `json:"status"`
			StartTime     string `json:"start_time"`
			CloseTime     string `json:"close_time"`
			ExecutionTime string `json:"execution_time"`
		} `json:"items"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Len(t, body.Items, 1)
	require.Equal(t, "nace-taxonomy-sync-20260602", body.Items[0].WorkflowID)
	require.Equal(t, "run-123", body.Items[0].RunID)
	require.Equal(t, nacetaxonomy.SyncWorkflowName, body.Items[0].WorkflowType)
	require.Equal(t, "completed", body.Items[0].Status)
	require.Equal(t, startedAt.Format(time.RFC3339), body.Items[0].StartTime)
	require.Equal(t, closedAt.Format(time.RFC3339), body.Items[0].CloseTime)
	require.Equal(t, startedAt.Format(time.RFC3339), body.Items[0].ExecutionTime)
}

func TestListNACETaxonomySyncWorkflowRunsRequiresTemporalClient(t *testing.T) {
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", nil, ""))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/workflows/nace/taxonomy-sync/runs", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.Contains(t, w.Body.String(), `"error":"temporal client not available"`)
}

func TestListNACETaxonomySyncWorkflowRunsRejectsInvalidLimit(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/workflows/nace/taxonomy-sync/runs?limit=0", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), `"error":"limit must be a positive integer"`)
	require.Nil(t, tc.listWorkflowRequest)
}

var _ client.Client = (*temporalExecuteRecorder)(nil)

func (t *temporalExecuteRecorder) ListWorkflow(ctx context.Context, request *workflowservicepb.ListWorkflowExecutionsRequest) (*workflowservicepb.ListWorkflowExecutionsResponse, error) {
	t.listWorkflowRequest = request
	if t.listWorkflowError != nil {
		return nil, t.listWorkflowError
	}
	if t.listWorkflowResponse == nil {
		return &workflowservicepb.ListWorkflowExecutionsResponse{}, nil
	}
	return t.listWorkflowResponse, nil
}
