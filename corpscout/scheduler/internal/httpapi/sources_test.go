package httpapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	workflowservicepb "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

type sourceConfigStubQuerier struct {
	stubQuerier
	updateSourceConfigParams *db.UpdateSourceConfigParams
}

func (s *sourceConfigStubQuerier) UpdateSourceConfig(_ context.Context, arg db.UpdateSourceConfigParams) error {
	s.updateSourceConfigParams = &arg
	return nil
}

type sourcePatchWriteRecorder struct {
	stubQuerier
	updateSourceEnabledCalled bool
}

func (s *sourcePatchWriteRecorder) UpdateSourceEnabled(_ context.Context, _ db.UpdateSourceEnabledParams) error {
	s.updateSourceEnabledCalled = true
	return nil
}

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
	interval := "24h"
	lastStarted := time.Date(2026, 5, 21, 10, 15, 0, 0, time.UTC)
	q.On("ListSources", mock.Anything).Return([]db.DataSource{{
		ID:                 uuid.New(),
		Name:               "brreg",
		Enabled:            true,
		ScheduleEnabled:    true,
		ScheduleKind:       "interval",
		ScheduleExpression: &interval,
		LastStartedAt:      pgtype.Timestamptz{Time: lastStarted, Valid: true},
		InputTableName:     "brreg_workflow.raw_records",
	}}, nil)

	r := routerFor(newTestHandlers(q))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sources", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body []map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Len(t, body, 1)
	require.Equal(t, "brreg", body[0]["name"])
	require.Equal(t, false, body[0]["download_workflow_registered"])
	require.Equal(t, false, body[0]["manual_trigger_available"])
	require.Equal(t, lastStarted.Add(24*time.Hour).Format(time.RFC3339Nano), body[0]["next_scheduled_at"])
	q.AssertExpectations(t)
}

func TestGetSourceReturnsReadOnlySourceMetadata(t *testing.T) {
	q := &stubQuerier{}
	sourceID := uuid.New()
	q.On("GetSourceByName", mock.Anything, "brreg").Return(db.DataSource{
		ID:                  sourceID,
		Name:                "brreg",
		InputTableName:      "brreg_workflow.raw_records",
		RequiresTranslation: true,
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
	require.Equal(t, true, body["requires_translation"])
	require.Equal(t, false, body["download_workflow_registered"])
	require.Equal(t, false, body["manual_trigger_available"])
	q.AssertExpectations(t)
}

func TestGetAriregisterSourceExposesTemporalBulkIngestMetadata(t *testing.T) {
	q := &stubQuerier{}
	sourceID := uuid.New()
	q.On("GetSourceByName", mock.Anything, "ariregister").Return(db.DataSource{
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

func TestGetAriregisterSourceExposesSourceEntries(t *testing.T) {
	q := &stubQuerier{}
	sourceID := uuid.New()
	q.On("GetSourceByName", mock.Anything, "ariregister").Return(db.DataSource{
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

func TestPatchSourceResponseUsesNamedType(t *testing.T) {
	source, err := os.ReadFile("source_patch.go")
	require.NoError(t, err)
	body := string(source)

	require.NotContains(t, body, `map[string]string{"status": "ok"}`)
	require.Contains(t, body, "type patchSourceResponse struct")
	require.Contains(t, body, `patchSourceResponse{Status: "ok"}`)
}

func TestPatchSource_updates_enabled(t *testing.T) {
	q := &stubQuerier{}

	q.On("GetSourceByName", mock.Anything, "gleif").Return(db.DataSource{
		Name: "gleif",
	}, nil)
	q.On("UpdateSourceEnabled", mock.Anything, db.UpdateSourceEnabledParams{
		Name: "gleif", Enabled: false,
	}).Return(nil)

	r := routerFor(newTestHandlers(q))

	body := strings.NewReader(`{"enabled": false}`)
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/sources/gleif", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	q.AssertExpectations(t)
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

func TestPatchSource_enabled_only_missing_source_returns_404_without_update(t *testing.T) {
	q := &sourcePatchWriteRecorder{}

	q.On("GetSourceByName", mock.Anything, "missing").Return(db.DataSource{}, errors.New("not found"))

	r := routerFor(newTestHandlers(q))

	body := strings.NewReader(`{"enabled": false}`)
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/sources/missing", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusNotFound, w.Code)
	require.False(t, q.updateSourceEnabledCalled)
}

func TestPatchSource_updates_schedule(t *testing.T) {
	q := &stubQuerier{}

	expr := "24h"
	q.On("GetSourceByName", mock.Anything, "gleif").Return(db.DataSource{
		Name:         "gleif",
		ScheduleKind: "interval",
	}, nil)
	q.On("UpdateSourceSchedule", mock.Anything, db.UpdateSourceScheduleParams{
		Name:               "gleif",
		ScheduleKind:       "interval",
		ScheduleExpression: &expr,
	}).Return(nil)

	r := routerForHandlers(q)

	body := strings.NewReader(`{"schedule_expression": "24h"}`)
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/sources/gleif", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	q.AssertExpectations(t)
}

func TestPatchSource_updates_schedule_enabled(t *testing.T) {
	q := &stubQuerier{}

	q.On("GetSourceByName", mock.Anything, "gleif").Return(db.DataSource{
		Name: "gleif",
	}, nil)
	q.On("UpdateSourceScheduleEnabled", mock.Anything, db.UpdateSourceScheduleEnabledParams{
		Name: "gleif", ScheduleEnabled: false,
	}).Return(nil)

	r := routerForHandlers(q)

	body := strings.NewReader(`{"schedule_enabled": false}`)
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/sources/gleif", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	q.AssertExpectations(t)
}

func TestPatchSource_invalid_schedule_expression_returns_422(t *testing.T) {
	q := &stubQuerier{}

	q.On("GetSourceByName", mock.Anything, "gleif").Return(db.DataSource{
		Name:         "gleif",
		ScheduleKind: "interval",
	}, nil)

	r := routerForHandlers(q)

	body := strings.NewReader(`{"schedule_expression": "daily"}`)
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/sources/gleif", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusUnprocessableEntity, w.Code)
	q.AssertExpectations(t)
}

func TestPatchSource_invalid_cron_expression_returns_422(t *testing.T) {
	q := &stubQuerier{}

	q.On("GetSourceByName", mock.Anything, "gleif").Return(db.DataSource{
		Name:         "gleif",
		ScheduleKind: "cron",
	}, nil)

	r := routerForHandlers(q)

	body := strings.NewReader(`{"schedule_expression": "daily"}`)
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/sources/gleif", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusUnprocessableEntity, w.Code)
	q.AssertExpectations(t)
}

func TestPatchSource_invalid_schedule_kind_returns_422(t *testing.T) {
	q := &stubQuerier{}

	q.On("GetSourceByName", mock.Anything, "gleif").Return(db.DataSource{
		Name:         "gleif",
		ScheduleKind: "interval",
	}, nil)

	r := routerForHandlers(q)

	body := strings.NewReader(`{"schedule_kind": "daily"}`)
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/sources/gleif", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusUnprocessableEntity, w.Code)
	q.AssertExpectations(t)
}

func TestPatchSource_non_positive_schedule_expression_returns_422(t *testing.T) {
	for _, expr := range []string{"0s", "-1h"} {
		t.Run(expr, func(t *testing.T) {
			q := &stubQuerier{}

			q.On("GetSourceByName", mock.Anything, "gleif").Return(db.DataSource{
				Name:         "gleif",
				ScheduleKind: "interval",
			}, nil)
			q.On("UpdateSourceSchedule", mock.Anything, mock.Anything).Return(nil)

			r := routerForHandlers(q)

			body := strings.NewReader(`{"schedule_expression": "` + expr + `"}`)
			req := httptest.NewRequest(http.MethodPatch, "/api/v1/sources/gleif", body)
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			require.Equal(t, http.StatusUnprocessableEntity, w.Code)
		})
	}
}

func TestPatchSource_validates_all_fields_before_writing(t *testing.T) {
	q := &sourcePatchWriteRecorder{}

	q.On("GetSourceByName", mock.Anything, "gleif").Return(db.DataSource{
		Name:   "gleif",
		Config: json.RawMessage(`{}`),
	}, nil)

	r := routerFor(newTestHandlers(q))

	body := strings.NewReader(`{"enabled": false, "config": {"api_token": "secret"}}`)
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/sources/gleif", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusUnprocessableEntity, w.Code)
	require.False(t, q.updateSourceEnabledCalled)
}

func TestPatchSource_config_merge_preserves_json_numeric_type(t *testing.T) {
	q := &sourceConfigStubQuerier{}

	q.On("GetSourceByName", mock.Anything, "gleif").Return(db.DataSource{
		Name:   "gleif",
		Config: json.RawMessage(`{"limit":10,"nested":{"threshold":0.5},"unchanged":true}`),
	}, nil)

	r := routerFor(newTestHandlers(q))

	body := strings.NewReader(`{"config":{"limit":25,"nested":{"threshold":0.75}}}`)
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/sources/gleif", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, q.updateSourceConfigParams)
	require.Equal(t, "gleif", q.updateSourceConfigParams.Name)
	require.JSONEq(t, `{"limit":25,"nested":{"threshold":0.75},"unchanged":true}`, string(q.updateSourceConfigParams.Config))
	require.Equal(t, `{"limit":25,"nested":{"threshold":0.75},"unchanged":true}`, string(q.updateSourceConfigParams.Config))
	q.AssertExpectations(t)
}

func TestPatchSource_config_secret_key_returns_422(t *testing.T) {
	q := &stubQuerier{}

	q.On("GetSourceByName", mock.Anything, "gleif").Return(db.DataSource{
		Name:   "gleif",
		Config: json.RawMessage(`{}`),
	}, nil)

	r := routerForHandlers(q)

	body := strings.NewReader(`{"config":{"api_token":"secret"}}`)
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/sources/gleif", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusUnprocessableEntity, w.Code)
}

func TestPatchSource_config_secret_key_inside_array_returns_422(t *testing.T) {
	q := &sourceConfigStubQuerier{}

	q.On("GetSourceByName", mock.Anything, "gleif").Return(db.DataSource{
		Name:   "gleif",
		Config: json.RawMessage(`{}`),
	}, nil)

	r := routerFor(newTestHandlers(q))

	body := strings.NewReader(`{"config":{"providers":[{"api_token":"secret"}]}}`)
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/sources/gleif", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusUnprocessableEntity, w.Code)
	require.Nil(t, q.updateSourceConfigParams)
}
