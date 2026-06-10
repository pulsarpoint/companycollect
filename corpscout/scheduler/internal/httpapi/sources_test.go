package httpapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	workflowservicepb "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

type temporalExecuteRecorder struct {
	client.Client
	options              client.StartWorkflowOptions
	workflow             interface{}
	args                 []interface{}
	listWorkflowRequest  *workflowservicepb.ListWorkflowExecutionsRequest
	listWorkflowResponse *workflowservicepb.ListWorkflowExecutionsResponse
	listWorkflowError    error
}

func (t *temporalExecuteRecorder) ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflow interface{}, args ...interface{}) (client.WorkflowRun, error) {
	t.options = options
	t.workflow = workflow
	t.args = args
	return workflowRunStub{workflowID: options.ID, runID: "run-1"}, nil
}

type workflowRunStub struct {
	client.WorkflowRun
	workflowID string
	runID      string
}

func (w workflowRunStub) GetWorkflowID() string {
	return w.workflowID
}

func (w workflowRunStub) GetRunID() string {
	return w.runID
}

func (w workflowRunStub) Get(context.Context, interface{}) error {
	return nil
}

func TestListSourcesReturnsReadOnlySourceMetadata(t *testing.T) {
	q := &stubQuerier{}
	q.On("ListSources", mock.Anything).Return([]db.ListSourcesRow{{
		ID:                    uuid.New(),
		Name:                  "finland_prhytj",
		Country:               "finland",
		Source:                "prhytj",
		RegistryKey:           "finland/prhytj",
		Enabled:               true,
		AuthRequired:          false,
		InputTableName:        "corpscout_sources.fi_prhytj_*",
		StorageKind:           "clickhouse",
		ClickhouseDatabase:    "corpscout_sources",
		ClickhouseTablePrefix: "fi_prhytj",
		SourceUrl:             "https://avoindata.prh.fi/opendata-ytj-api/v3/companies",
		DocsUrl:               "https://www.prh.fi/en/kaupparekisteri/tietopalvelut/open_data.html",
		RawSourceRetention:    "filesystem_run_directory",
		SourceFileName:        "source.ndjson",
		UserAgentRequired:     false,
	}}, nil)

	r := routerFor(newTestHandlers(q))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sources", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body []map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Len(t, body, 1)
	require.Equal(t, "finland_prhytj", body[0]["name"])
	require.Equal(t, "finland", body[0]["country"])
	require.Equal(t, "prhytj", body[0]["source"])
	require.Equal(t, "finland/prhytj", body[0]["registry_key"])
	require.Equal(t, false, body[0]["auth_required"])
	require.Equal(t, "clickhouse", body[0]["storage_kind"])
	require.Equal(t, "corpscout_sources", body[0]["clickhouse_database"])
	require.Equal(t, "fi_prhytj", body[0]["clickhouse_table_prefix"])
	require.Equal(t, "https://avoindata.prh.fi/opendata-ytj-api/v3/companies", body[0]["source_url"])
	require.Equal(t, "https://www.prh.fi/en/kaupparekisteri/tietopalvelut/open_data.html", body[0]["docs_url"])
	require.Equal(t, "filesystem_run_directory", body[0]["raw_source_retention"])
	require.Equal(t, "source.ndjson", body[0]["source_file_name"])
	require.Equal(t, false, body[0]["user_agent_required"])
	require.Equal(t, false, body[0]["download_workflow_registered"])
	require.Equal(t, false, body[0]["manual_trigger_available"])
	require.NotContains(t, body[0], "config")
	q.AssertExpectations(t)
}

func TestGetSourceReturnsReadOnlySourceMetadata(t *testing.T) {
	q := &stubQuerier{}
	sourceID := uuid.New()
	q.On("GetSourceByName", mock.Anything, "brreg").Return(db.GetSourceByNameRow{
		ID:                    sourceID,
		Name:                  "brreg",
		Country:               "norway",
		Source:                "brreg",
		RegistryKey:           "norway/brreg",
		InputTableName:        "corpscout_sources.brreg_*",
		AuthRequired:          true,
		StorageKind:           "clickhouse",
		ClickhouseDatabase:    "corpscout_sources",
		ClickhouseTablePrefix: "brreg",
		SourceUrl:             "https://data.brreg.no/",
		DocsUrl:               "https://data.brreg.no/",
		RawSourceRetention:    "filesystem_run_directory",
		SourceFileName:        "source.ndjson",
		UserAgentRequired:     true,
		RequiresTranslation:   true,
	}, nil)

	r := routerFor(newTestHandlers(q))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sources/brreg", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Equal(t, sourceID.String(), body["id"])
	require.Equal(t, "brreg", body["name"])
	require.Equal(t, "norway/brreg", body["registry_key"])
	require.Equal(t, true, body["auth_required"])
	require.Equal(t, "https://data.brreg.no/", body["source_url"])
	require.Equal(t, "https://data.brreg.no/", body["docs_url"])
	require.Equal(t, "filesystem_run_directory", body["raw_source_retention"])
	require.Equal(t, "source.ndjson", body["source_file_name"])
	require.Equal(t, true, body["user_agent_required"])
	require.Equal(t, true, body["requires_translation"])
	require.Equal(t, false, body["download_workflow_registered"])
	require.Equal(t, false, body["manual_trigger_available"])
	require.NotContains(t, body, "config")
	q.AssertExpectations(t)
}

func TestGetAriregisterSourceExposesTemporalBulkIngestMetadata(t *testing.T) {
	q := &stubQuerier{}
	sourceID := uuid.New()
	q.On("GetSourceByName", mock.Anything, "ariregister").Return(db.GetSourceByNameRow{
		ID:             sourceID,
		Name:           "ariregister",
		InputTableName: "ariregister_company_raw_inputs",
	}, nil)

	r := routerFor(newTestHandlers(q))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sources/ariregister", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Equal(t, sourceID.String(), body["id"])
	require.Equal(t, "ariregister", body["name"])
	require.Equal(t, true, body["download_workflow_registered"])
	require.Equal(t, true, body["manual_trigger_available"])
	q.AssertExpectations(t)
}

func TestGetSESourceExposesTemporalBulkIngestMetadata(t *testing.T) {
	q := &stubQuerier{}
	sourceID := uuid.New()
	q.On("GetSourceByName", mock.Anything, "se").Return(db.GetSourceByNameRow{
		ID:             sourceID,
		Name:           "se",
		InputTableName: "se_workflow.raw_records",
	}, nil)

	r := routerFor(newTestHandlers(q))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sources/se", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Equal(t, sourceID.String(), body["id"])
	require.Equal(t, "se", body["name"])
	require.Equal(t, true, body["download_workflow_registered"])
	require.Equal(t, true, body["manual_trigger_available"])
	q.AssertExpectations(t)
}

func TestGetAriregisterSourceExposesSourceEntries(t *testing.T) {
	q := &stubQuerier{}
	sourceID := uuid.New()
	q.On("GetSourceByName", mock.Anything, "ariregister").Return(db.GetSourceByNameRow{
		ID:             sourceID,
		Name:           "ariregister",
		DisplayName:    ptrString("Ariregister (Estonia)"),
		InputTableName: "ariregister_workflow.raw_records",
		SourceGroup:    "registry",
		Enabled:        true,
		Config:         json.RawMessage(`{"dataset_key":"general"}`),
	}, nil)

	r := routerFor(newTestHandlers(q))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sources/ariregister", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"source_entries_available":true`)
	q.AssertExpectations(t)
}

func ptrString(value string) *string {
	return &value
}

func TestPatchSource_enabled_returns_422(t *testing.T) {
	q := &stubQuerier{}

	r := routerFor(newTestHandlers(q))

	body := strings.NewReader(`{"enabled": false}`)
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/sources/gleif", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusUnprocessableEntity, w.Code)
}

func TestPatchSource_unknown_field_returns_400(t *testing.T) {
	q := &stubQuerier{}

	r := routerForHandlers(q)

	body := strings.NewReader(`{"crawl_interval_hours": 24}`)
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/sources/gleif", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestPatchSource_empty_object_returns_400(t *testing.T) {
	q := &stubQuerier{}

	r := routerForHandlers(q)

	body := strings.NewReader(`{}`)
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/sources/gleif", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestPatchSource_config_patch_returns_422_without_writing(t *testing.T) {
	q := &stubQuerier{}

	r := routerFor(newTestHandlers(q))

	body := strings.NewReader(`{"enabled": false, "config": {"api_token": "secret"}}`)
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/sources/gleif", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusUnprocessableEntity, w.Code)
}
