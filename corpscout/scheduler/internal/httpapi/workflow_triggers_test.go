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

	ariregisterworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/ariregister/workflow"
	brregworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/workflow"
	cvrworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/cvr/workflow"
	franceworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/france/workflow"
	"github.com/pulsarpoint/corpscout/scheduler/internal/fx"
	"github.com/pulsarpoint/corpscout/scheduler/internal/httpapi"
	"github.com/pulsarpoint/corpscout/scheduler/internal/nacetaxonomy"
	seworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/se/workflow"
)

func TestStartBrregCompanyTranslationWorkflowStartsTemporalWorkflow(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/brreg/company-translation", bytes.NewBufferString(`{
		"all_records": true,
		"ids": ["7ffd5bf3-f96e-4907-9ef3-096eb4056ab8"],
		"filters": {"translation_status": "missing", "q": "BORTIGARD"},
		"limit": 25,
		"batch_size": 10,
		"claim_mode": "auto",
		"max_request_chars": 16000,
		"max_terms": 250,
		"max_companies_per_batch": 250,
		"max_batches": 50,
		"max_parallel_tasks": 5,
		"lease_seconds": 900,
		"max_attempts": 3,
		"provider": "deepseek",
		"model": "deepseek-v4-flash",
		"prompt_version": "v2",
		"trigger": "manual"
	}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	require.Contains(t, w.Body.String(), `"status":"started"`)
	require.Contains(t, w.Body.String(), `"workflow":"TranslateBrregSourceCompanies"`)
	require.Equal(t, "brreg-company-translation", tc.options.TaskQueue)
	require.Equal(t, reflect.ValueOf(brregworkflow.TranslateBrregSourceCompanies).Pointer(), reflect.ValueOf(tc.workflow).Pointer())
	require.Len(t, tc.args, 1)

	input := tc.args[0].(brregworkflow.TranslateBrregSourceCompaniesInput)
	require.True(t, input.AllRecords)
	require.Equal(t, []string{"7ffd5bf3-f96e-4907-9ef3-096eb4056ab8"}, input.IDs)
	require.Equal(t, map[string]string{"translation_status": "missing", "q": "BORTIGARD"}, input.Filters)
	require.Equal(t, 25, input.Limit)
	require.Equal(t, 10, input.BatchSize)
	require.Equal(t, "auto", input.ClaimMode)
	require.Equal(t, 16000, input.MaxRequestChars)
	require.Equal(t, 250, input.MaxTerms)
	require.Equal(t, 250, input.MaxCompaniesPerBatch)
	require.Equal(t, 50, input.MaxBatches)
	require.Equal(t, 5, input.MaxParallelTasks)
	require.Equal(t, 900, input.LeaseSeconds)
	require.Equal(t, 3, input.MaxAttempts)
	require.Equal(t, "deepseek", input.Provider)
	require.Equal(t, "deepseek-v4-flash", input.Model)
	require.Equal(t, "v2", input.PromptVersion)
	require.Equal(t, "manual", input.Trigger)
}

func TestStartAriregisterCompanyTranslationWorkflowStartsTemporalWorkflow(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/ariregister/company-translation", bytes.NewBufferString(`{
		"all_records": true,
		"ids": ["7ffd5bf3-f96e-4907-9ef3-096eb4056ab8"],
		"filters": {"translation_status": "missing", "q": "TALLINN"},
		"limit": 25,
		"batch_size": 10,
		"claim_mode": "auto",
		"max_request_chars": 12000,
		"max_terms": 250,
		"max_companies_per_batch": 500,
		"max_batches": 50,
		"max_parallel_tasks": 5,
		"lease_seconds": 900,
		"max_attempts": 3,
		"provider": "deepseek",
		"model": "deepseek-v4-flash",
		"prompt_version": "v2",
		"trigger": "manual"
	}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	require.Contains(t, w.Body.String(), `"status":"started"`)
	require.Contains(t, w.Body.String(), `"workflow":"TranslateAriregisterSourceCompanies"`)
	require.Equal(t, "ariregister-company-translation", tc.options.TaskQueue)
	require.Equal(t, reflect.ValueOf(ariregisterworkflow.TranslateAriregisterSourceCompanies).Pointer(), reflect.ValueOf(tc.workflow).Pointer())
	require.Len(t, tc.args, 1)

	input := tc.args[0].(ariregisterworkflow.TranslateAriregisterSourceCompaniesInput)
	require.True(t, input.AllRecords)
	require.Equal(t, []string{"7ffd5bf3-f96e-4907-9ef3-096eb4056ab8"}, input.IDs)
	require.Equal(t, map[string]string{"translation_status": "missing", "q": "TALLINN"}, input.Filters)
	require.Equal(t, 25, input.Limit)
	require.Equal(t, 10, input.BatchSize)
	require.Equal(t, "auto", input.ClaimMode)
	require.Equal(t, 12000, input.MaxRequestChars)
	require.Equal(t, 250, input.MaxTerms)
	require.Equal(t, 500, input.MaxCompaniesPerBatch)
	require.Equal(t, 50, input.MaxBatches)
	require.Equal(t, 5, input.MaxParallelTasks)
	require.Equal(t, 900, input.LeaseSeconds)
	require.Equal(t, 3, input.MaxAttempts)
	require.Equal(t, "deepseek", input.Provider)
	require.Equal(t, "deepseek-v4-flash", input.Model)
	require.Equal(t, "v2", input.PromptVersion)
	require.Equal(t, "manual", input.Trigger)
}

func TestStartBrregCompanyTranslationWorkflowRejectsInvalidClaimMode(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/brreg/company-translation", bytes.NewBufferString(`{"claim_mode":"huge"}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), `"error":"claim_mode must be auto or fixed"`)
}

func TestStartBrregCompanyTranslationWorkflowRejectsInvalidBatchSize(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/brreg/company-translation", bytes.NewBufferString(`{"batch_size": -1}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), `"error":"batch_size cannot be negative"`)
	require.Nil(t, tc.workflow)
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
	require.Contains(t, w.Body.String(), `"workflow":"NormalizeBrregSourceProfilesWithCopy"`)
	require.Equal(t, "brreg-source-profile", tc.options.TaskQueue)
	require.Equal(t, reflect.ValueOf(brregworkflow.NormalizeBrregSourceProfilesWithCopy).Pointer(), reflect.ValueOf(tc.workflow).Pointer())
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

func TestStartBrregSourceExplorerRefreshWorkflowStartsTemporalWorkflow(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/brreg/source-explorer-refresh", bytes.NewBufferString(`{
		"trigger": "manual"
	}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	require.Contains(t, w.Body.String(), `"status":"started"`)
	require.Contains(t, w.Body.String(), `"workflow":"RefreshBrregSourceExplorer"`)
	require.Equal(t, "brreg-source-explorer-refresh", tc.options.TaskQueue)
	require.Equal(t, reflect.ValueOf(brregworkflow.RefreshBrregSourceExplorer).Pointer(), reflect.ValueOf(tc.workflow).Pointer())
	require.Len(t, tc.args, 1)

	input := tc.args[0].(brregworkflow.RefreshBrregSourceExplorerInput)
	require.Equal(t, "manual", input.Trigger)
}

func TestStartBrregSourceCapitalFXWorkflowStartsTemporalWorkflow(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/brreg/source-capital-fx", bytes.NewBufferString(`{
		"ids": ["7ffd5bf3-f96e-4907-9ef3-096eb4056ab8"],
		"filters": {"website_status": "with_website"},
		"limit": 250,
		"rate_date": "2026-06-02",
		"force_reprocess": true,
		"trigger": "manual"
	}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	require.Contains(t, w.Body.String(), `"status":"started"`)
	require.Contains(t, w.Body.String(), `"workflow":"ConvertBrregSourceCapitalToUSD"`)
	require.Equal(t, "brreg-source-capital-fx", tc.options.TaskQueue)
	require.Equal(t, reflect.ValueOf(brregworkflow.ConvertBrregSourceCapitalToUSD).Pointer(), reflect.ValueOf(tc.workflow).Pointer())
	require.Len(t, tc.args, 1)

	input := tc.args[0].(brregworkflow.ConvertBrregSourceCapitalToUSDInput)
	require.Equal(t, []string{"7ffd5bf3-f96e-4907-9ef3-096eb4056ab8"}, input.IDs)
	require.Equal(t, map[string]string{"website_status": "with_website"}, input.Filters)
	require.Equal(t, 250, input.Limit)
	require.Equal(t, "2026-06-02", input.RateDate)
	require.True(t, input.ForceReprocess)
	require.Equal(t, "manual", input.Trigger)
}

func TestStartBrregSourceCapitalFXWorkflowRejectsInvalidRateDate(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/brreg/source-capital-fx", bytes.NewBufferString(`{"rate_date":"06/02/2026"}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), `"error":"rate_date must use YYYY-MM-DD format"`)
	require.Nil(t, tc.workflow)
}

func TestStartBrregSourceFinancialWorkflowStartsTemporalWorkflow(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/brreg/source-financials", bytes.NewBufferString(`{
		"limit": 250,
		"batch_size": 10,
		"max_parallel_tasks": 25,
		"lease_seconds": 900,
		"max_attempts": 3,
		"trigger": "manual"
	}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	require.Contains(t, w.Body.String(), `"status":"started"`)
	require.Contains(t, w.Body.String(), `"workflow":"FetchBrregSourceFinancialStatements"`)
	require.Equal(t, "brreg-source-financial", tc.options.TaskQueue)
	require.Equal(t, reflect.ValueOf(brregworkflow.FetchBrregSourceFinancialStatements).Pointer(), reflect.ValueOf(tc.workflow).Pointer())
	require.Len(t, tc.args, 1)

	input := tc.args[0].(brregworkflow.FetchBrregSourceFinancialStatementsInput)
	require.Equal(t, 250, input.Limit)
	require.Equal(t, 10, input.BatchSize)
	require.Equal(t, 25, input.MaxParallelTasks)
	require.Equal(t, 900, input.LeaseSeconds)
	require.Equal(t, 3, input.MaxAttempts)
	require.Equal(t, "manual", input.Trigger)
}

func TestStartBrregSourceFinancialWorkflowRejectsNegativeBatchSize(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/brreg/source-financials", bytes.NewBufferString(`{"batch_size":-1}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), `"error":"batch_size cannot be negative"`)
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

func TestStartAriregisterBulkRawIngestWorkflowStartsTemporalWorkflowAndAllowsAll(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/ariregister/bulk-raw-ingest", bytes.NewBufferString(`{
		"limit": 0,
		"source_url": "https://example.test/ettevotjad.csv.zip",
		"trigger": "manual"
	}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	require.Contains(t, w.Body.String(), `"status":"started"`)
	require.Contains(t, w.Body.String(), `"workflow":"LoadAriregisterBulkRawRecords"`)
	require.Equal(t, "ariregister-bulk-ingest", tc.options.TaskQueue)
	require.Equal(t, reflect.ValueOf(ariregisterworkflow.LoadAriregisterBulkRawRecords).Pointer(), reflect.ValueOf(tc.workflow).Pointer())
	require.Len(t, tc.args, 1)

	input := tc.args[0].(ariregisterworkflow.LoadAriregisterBulkRawRecordsInput)
	require.Equal(t, 0, input.Limit)
	require.Equal(t, "https://example.test/ettevotjad.csv.zip", input.SourceURL)
	require.Equal(t, "manual", input.Trigger)
}

func TestStartAriregisterSourceProfileWorkflowStartsTemporalWorkflow(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/ariregister/source-profile", bytes.NewBufferString(`{
		"limit": 1000,
		"batch_size": 500,
		"trigger": "manual"
	}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	require.Contains(t, w.Body.String(), `"status":"started"`)
	require.Contains(t, w.Body.String(), `"workflow":"NormalizeAriregisterSourceProfilesWithCopy"`)
	require.Contains(t, w.Body.String(), `"task_queue":"ariregister-source-profile"`)
	require.Contains(t, tc.options.ID, "ariregister-source-profile-")
	require.Equal(t, "ariregister-source-profile", tc.options.TaskQueue)
	require.Equal(t, reflect.ValueOf(ariregisterworkflow.NormalizeAriregisterSourceProfilesWithCopy).Pointer(), reflect.ValueOf(tc.workflow).Pointer())
	require.Len(t, tc.args, 1)

	input := tc.args[0].(ariregisterworkflow.NormalizeAriregisterSourceProfilesInput)
	require.Equal(t, 1000, input.Limit)
	require.Equal(t, 500, input.BatchSize)
	require.Equal(t, "manual", input.Trigger)
}

func TestStartAriregisterSourceProfileWorkflowRejectsNegativeLimits(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "limit",
			body: `{"limit":-1}`,
			want: `"error":"limit cannot be negative"`,
		},
		{
			name: "batch size",
			body: `{"batch_size":-1}`,
			want: `"error":"batch_size cannot be negative"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc := &temporalExecuteRecorder{}
			r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

			req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/ariregister/source-profile", bytes.NewBufferString(tt.body))
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			require.Equal(t, http.StatusBadRequest, w.Code)
			require.Contains(t, w.Body.String(), tt.want)
			require.Nil(t, tc.workflow)
		})
	}
}

func TestStartAriregisterBulkRawIngestWorkflowRejectsInvalidSourceURL(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/ariregister/bulk-raw-ingest", bytes.NewBufferString(`{"source_url":"file:///tmp/ettevotjad.csv.zip"}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), `"error":"source_url must be http or https"`)
	require.Nil(t, tc.workflow)
}

func TestStartFranceBulkRawIngestWorkflowStartsTemporalWorkflowAndAllowsAll(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/france/bulk-raw-ingest", bytes.NewBufferString(`{
		"limit": 0,
		"batch_size": 250,
		"legal_units_url": "https://example.test/StockUniteLegale.parquet",
		"establishments_url": "https://example.test/StockEtablissement.parquet",
		"trigger": "manual"
	}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	require.Contains(t, w.Body.String(), `"status":"started"`)
	require.Contains(t, w.Body.String(), `"workflow":"LoadFranceBulkRawRecords"`)
	require.Equal(t, "france-bulk-ingest", tc.options.TaskQueue)
	require.Equal(t, reflect.ValueOf(franceworkflow.LoadFranceBulkRawRecords).Pointer(), reflect.ValueOf(tc.workflow).Pointer())
	require.Len(t, tc.args, 1)

	input := tc.args[0].(franceworkflow.LoadFranceBulkRawRecordsInput)
	require.Equal(t, 0, input.Limit)
	require.Equal(t, 250, input.BatchSize)
	require.Equal(t, "https://example.test/StockUniteLegale.parquet", input.LegalUnitsURL)
	require.Equal(t, "https://example.test/StockEtablissement.parquet", input.EstablishmentsURL)
	require.Equal(t, "manual", input.Trigger)
}

func TestStartFranceBulkRawIngestWorkflowRejectsInvalidLegalUnitsURL(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/france/bulk-raw-ingest", bytes.NewBufferString(`{"legal_units_url":"file:///tmp/StockUniteLegale.parquet"}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), `"error":"legal_units_url must be http or https"`)
	require.Nil(t, tc.workflow)
}

func TestStartSEBulkRawIngestWorkflowStartsTemporalWorkflow(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/se/bulk-raw-ingest", bytes.NewBufferString(`{
		"limit": 0,
		"batch_size": 250,
		"trigger": "manual"
	}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	require.Contains(t, w.Body.String(), `"status":"started"`)
	require.Contains(t, w.Body.String(), `"workflow":"LoadSEBulkRawRecords"`)
	require.Equal(t, "se-bulk-ingest", tc.options.TaskQueue)
	require.Equal(t, reflect.ValueOf(seworkflow.LoadSEBulkRawRecords).Pointer(), reflect.ValueOf(tc.workflow).Pointer())
	require.Len(t, tc.args, 1)

	input := tc.args[0].(seworkflow.LoadSEBulkRawRecordsInput)
	require.Equal(t, 0, input.Limit)
	require.Equal(t, 250, input.BatchSize)
	require.Empty(t, input.Datasets)
	require.Empty(t, input.DatasetsJSON)
	require.Equal(t, "manual", input.Trigger)
}

func TestStartSEBulkRawIngestWorkflowRejectsDatasetOverride(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/se/bulk-raw-ingest", bytes.NewBufferString(`{
		"datasets": [
			{"dataset":"organisationer","url":"file:///tmp/organisationer.json","format":"json"}
		]
	}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), `"error":"invalid request body"`)
	require.Nil(t, tc.workflow)
}

func TestStartCVRRawIngestWorkflowStartsTemporalWorkflow(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/cvr/raw-ingest", bytes.NewBufferString(`{
		"limit": 100,
		"page_size": 25,
		"batch_size": 50,
		"source_url": "https://example.test/cvr-permanent/virksomhed/_search",
		"scroll_url": "https://example.test/_search/scroll",
		"scroll": "100",
		"trigger": "manual"
	}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	require.Contains(t, w.Body.String(), `"status":"started"`)
	require.Contains(t, w.Body.String(), `"workflow":"LoadCVRRawRecords"`)
	require.Equal(t, "cvr-raw-ingest", tc.options.TaskQueue)
	require.Equal(t, reflect.ValueOf(cvrworkflow.LoadCVRRawRecords).Pointer(), reflect.ValueOf(tc.workflow).Pointer())
	require.Len(t, tc.args, 1)

	input := tc.args[0].(cvrworkflow.LoadCVRRawRecordsInput)
	require.Equal(t, 100, input.Limit)
	require.Equal(t, 25, input.PageSize)
	require.Equal(t, 50, input.BatchSize)
	require.Equal(t, "https://example.test/cvr-permanent/virksomhed/_search", input.SourceURL)
	require.Equal(t, "https://example.test/_search/scroll", input.ScrollURL)
	require.Equal(t, "100", input.Scroll)
	require.Equal(t, "manual", input.Trigger)
}

func TestStartCVRRawIngestWorkflowRejectsInvalidSourceURL(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/cvr/raw-ingest", bytes.NewBufferString(`{"source_url":"file:///tmp/cvr.json"}`))
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

func TestStartExchangeRateSyncWorkflowStartsTemporalWorkflow(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/fx/rate-sync", bytes.NewBufferString(`{
		"provider": "ecb",
		"source_url": "https://example.test/ecb.xml",
		"trigger": "manual",
		"force_reprocess": true
	}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	require.Contains(t, w.Body.String(), `"status":"started"`)
	require.Contains(t, w.Body.String(), `"workflow":"SyncExchangeRates"`)
	require.Equal(t, "fx-rate-sync", tc.options.TaskQueue)
	require.Equal(t, reflect.ValueOf(fx.SyncExchangeRates).Pointer(), reflect.ValueOf(tc.workflow).Pointer())
	require.Len(t, tc.args, 1)

	input := tc.args[0].(fx.SyncExchangeRatesInput)
	require.Equal(t, "ecb", input.Provider)
	require.Equal(t, "https://example.test/ecb.xml", input.SourceURL)
	require.Equal(t, "manual", input.Trigger)
	require.True(t, input.ForceReprocess)
}

func TestStartExchangeRateSyncWorkflowUsesDefaultValues(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/fx/rate-sync", bytes.NewBufferString(`{}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	input := tc.args[0].(fx.SyncExchangeRatesInput)
	require.Equal(t, fx.DefaultProvider, input.Provider)
	require.Equal(t, fx.DefaultDailySourceURL, input.SourceURL)
	require.Equal(t, "manual", input.Trigger)
}

func TestStartExchangeRateSyncWorkflowUsesConfiguredSourceURL(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	handlers := httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, "").
		ConfigureFX("https://example.test/configured-ecb.xml")
	r := routerFor(handlers)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/fx/rate-sync", bytes.NewBufferString(`{}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	input := tc.args[0].(fx.SyncExchangeRatesInput)
	require.Equal(t, "https://example.test/configured-ecb.xml", input.SourceURL)
}

func TestStartExchangeRateSyncWorkflowRejectsUnsupportedProvider(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/fx/rate-sync", bytes.NewBufferString(`{"provider":"other"}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), `"error":"provider must be ecb"`)
	require.Nil(t, tc.workflow)
}

func TestStartExchangeRateSyncWorkflowRejectsInvalidSourceURL(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/fx/rate-sync", bytes.NewBufferString(`{"source_url":"file:///tmp/ecb.xml"}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), `"error":"source_url must be http or https"`)
	require.Nil(t, tc.workflow)
}

func TestListExchangeRateSyncWorkflowRunsReturnsRecentTemporalExecutions(t *testing.T) {
	startedAt := time.Date(2026, 6, 3, 10, 0, 0, 0, time.UTC)
	tc := &temporalExecuteRecorder{
		listWorkflowResponse: &workflowservicepb.ListWorkflowExecutionsResponse{
			Executions: []*workflowpb.WorkflowExecutionInfo{{
				Execution: &commonpb.WorkflowExecution{WorkflowId: "fx-rate-sync-20260603", RunId: "run-123"},
				Type:      &commonpb.WorkflowType{Name: fx.SyncWorkflowName},
				Status:    enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
				StartTime: timestamppb.New(startedAt),
			}},
		},
	}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/workflows/fx/rate-sync/runs?limit=5", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, tc.listWorkflowRequest)
	require.Equal(t, int32(5), tc.listWorkflowRequest.PageSize)
	require.Equal(t, "WorkflowType = 'SyncExchangeRates'", tc.listWorkflowRequest.Query)
	require.Contains(t, w.Body.String(), `"workflow_id":"fx-rate-sync-20260603"`)
	require.Contains(t, w.Body.String(), `"status":"completed"`)
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

func TestListBrregWorkflowRunsReturnsFilteredTemporalExecutions(t *testing.T) {
	startedAt := time.Date(2026, 6, 2, 11, 30, 0, 0, time.UTC)
	closedAt := startedAt.Add(2 * time.Minute)
	tc := &temporalExecuteRecorder{
		listWorkflowResponse: &workflowservicepb.ListWorkflowExecutionsResponse{
			Executions: []*workflowpb.WorkflowExecutionInfo{
				{
					Execution:     &commonpb.WorkflowExecution{WorkflowId: "brreg-company-translation-20260602-113000", RunId: "run-123"},
					Type:          &commonpb.WorkflowType{Name: brregworkflow.TranslateBrregSourceCompaniesWorkflowName},
					Status:        enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
					StartTime:     timestamppb.New(startedAt),
					ExecutionTime: timestamppb.New(startedAt),
				},
				{
					Execution:     &commonpb.WorkflowExecution{WorkflowId: "brreg-company-translation-other", RunId: "run-456"},
					Type:          &commonpb.WorkflowType{Name: brregworkflow.TranslateBrregSourceCompaniesWorkflowName},
					Status:        enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
					StartTime:     timestamppb.New(startedAt.Add(-time.Hour)),
					CloseTime:     timestamppb.New(closedAt),
					ExecutionTime: timestamppb.New(startedAt.Add(-time.Hour)),
				},
			},
		},
	}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/workflows/brreg/runs?limit=5&prefix=brreg-company-translation&status=running&q=20260602", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, tc.listWorkflowRequest)
	require.Equal(t, int32(5), tc.listWorkflowRequest.PageSize)
	require.Equal(t, "WorkflowType = 'TranslateBrregSourceCompanies' AND WorkflowId STARTS_WITH 'brreg-company-translation-' AND ExecutionStatus = 'Running'", tc.listWorkflowRequest.Query)

	var body struct {
		Prefixes []struct {
			Prefix       string `json:"prefix"`
			Label        string `json:"label"`
			WorkflowType string `json:"workflow_type"`
		} `json:"prefixes"`
		Items []struct {
			WorkflowID    string `json:"workflow_id"`
			RunID         string `json:"run_id"`
			WorkflowType  string `json:"workflow_type"`
			Prefix        string `json:"prefix"`
			Action        string `json:"action"`
			Status        string `json:"status"`
			StartTime     string `json:"start_time"`
			CloseTime     string `json:"close_time"`
			ExecutionTime string `json:"execution_time"`
		} `json:"items"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Len(t, body.Prefixes, 7)
	require.Equal(t, "brreg-company-translation", body.Prefixes[0].Prefix)
	require.Equal(t, brregworkflow.TranslateBrregSourceCompaniesWorkflowName, body.Prefixes[0].WorkflowType)
	require.Equal(t, "brreg-source-financial", body.Prefixes[5].Prefix)
	require.Equal(t, brregworkflow.FetchBrregSourceFinancialStatementsWorkflowName, body.Prefixes[5].WorkflowType)
	require.Len(t, body.Items, 1)
	require.Equal(t, "brreg-company-translation-20260602-113000", body.Items[0].WorkflowID)
	require.Equal(t, "run-123", body.Items[0].RunID)
	require.Equal(t, brregworkflow.TranslateBrregSourceCompaniesWorkflowName, body.Items[0].WorkflowType)
	require.Equal(t, "brreg-company-translation", body.Items[0].Prefix)
	require.Equal(t, "Company translation", body.Items[0].Action)
	require.Equal(t, "running", body.Items[0].Status)
	require.Equal(t, startedAt.Format(time.RFC3339), body.Items[0].StartTime)
	require.Empty(t, body.Items[0].CloseTime)
	require.Equal(t, startedAt.Format(time.RFC3339), body.Items[0].ExecutionTime)
}

func TestListBrregWorkflowRunsRejectsInvalidPrefix(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/workflows/brreg/runs?prefix=nace-taxonomy-sync", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), `"error":"unsupported brreg workflow prefix"`)
	require.Nil(t, tc.listWorkflowRequest)
}

func TestListBrregWorkflowRunsRequiresTemporalClient(t *testing.T) {
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", nil, ""))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/workflows/brreg/runs", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.Contains(t, w.Body.String(), `"error":"temporal client not available"`)
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
