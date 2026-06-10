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
	"go.temporal.io/sdk/client"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/httpapi"
	companysourceworkflows "github.com/pulsarpoint/corpscout/scheduler/internal/temporal/workflow/companysources"
)

func TestListSourceActionsReturnsConfiguredActions(t *testing.T) {
	q := &stubQuerier{}
	sourceID := uuid.New()
	actionID := uuid.New()
	q.On("ListSourceActions", mock.Anything, "finland_prhytj").Return([]db.ListSourceActionsRow{{
		ID:                   actionID,
		SourceID:             sourceID,
		SourceName:           "finland_prhytj",
		Action:               "pull_source",
		DisplayName:          "Pull source data",
		TemporalWorkflowType: companysourceworkflows.DownloadSourceWorkflowName,
		TemporalTaskQueue:    ptrString(companysourceworkflows.SourceTaskQueue),
		Enabled:              true,
		Config:               json.RawMessage(`{}`),
	}}, nil)

	r := routerFor(newTestHandlers(q))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sources/finland_prhytj/actions", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"pull_source"`)
	require.Contains(t, w.Body.String(), companysourceworkflows.DownloadSourceWorkflowName)
	q.AssertExpectations(t)
}

func TestTriggerSourceActionStartsTemporalWorkflow(t *testing.T) {
	q := &stubQuerier{}
	sourceID := uuid.New()
	actionID := uuid.New()
	q.On("GetSourceActionByName", mock.Anything, db.GetSourceActionByNameParams{
		Name: "finland_prhytj", Action: "pull_source",
	}).Return(db.GetSourceActionByNameRow{
		ID:                   actionID,
		SourceID:             sourceID,
		SourceName:           "finland_prhytj",
		Country:              "finland",
		Source:               "prhytj",
		RegistryKey:          "finland/prhytj",
		SourceUrl:            "https://example.test/source",
		SourceFileName:       "source.ndjson",
		Action:               "pull_source",
		DisplayName:          "Pull source data",
		TemporalWorkflowType: companysourceworkflows.DownloadSourceWorkflowName,
		TemporalTaskQueue:    ptrString(companysourceworkflows.SourceTaskQueue),
		Enabled:              true,
		Config:               json.RawMessage(`{}`),
	}, nil)
	var createdActionRunID uuid.UUID
	q.On("CreateSourceActionRun", mock.Anything, mock.MatchedBy(func(arg db.CreateSourceActionRunParams) bool {
		createdActionRunID = arg.ID
		return arg.ActionID == actionID &&
			arg.TemporalWorkflowID != nil &&
			*arg.TemporalWorkflowID == companysourceworkflows.ActionRunWorkflowID(arg.ID.String()) &&
			json.Valid(arg.Input)
	})).Return(func(_ context.Context, arg db.CreateSourceActionRunParams) db.DataSourceActionRun {
		return db.DataSourceActionRun{
			ID:       arg.ID,
			SourceID: sourceID,
			ActionID: actionID,
			Action:   companysourceworkflows.ActionPullSource,
			Status:   "running",
		}
	}, nil)

	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(q, nil, nil, nil, "", tc, ""))
	body := strings.NewReader(`{"trigger":"manual"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sources/finland_prhytj/actions/pull_source/trigger", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	require.Equal(t, companysourceworkflows.SourceTaskQueue, tc.options.TaskQueue)
	require.Equal(t, companysourceworkflows.DownloadSourceWorkflowName, tc.workflow)
	require.Equal(t, companysourceworkflows.ActionRunWorkflowID(createdActionRunID.String()), tc.options.ID)
	require.Equal(t, []interface{}{companysourceworkflows.SyncSourceDownloadInput{
		ActionRunID: createdActionRunID.String(),
		SourceName:  "finland_prhytj",
		Trigger:     "manual",
	}}, tc.args)
}

func TestTriggerSourceExplorerCacheActionStartsTemporalWorkflow(t *testing.T) {
	q := &stubQuerier{}
	sourceID := uuid.New()
	actionID := uuid.New()
	q.On("GetSourceActionByName", mock.Anything, db.GetSourceActionByNameParams{
		Name: "finland_prhytj", Action: companysourceworkflows.ActionRefreshExplorerCache,
	}).Return(db.GetSourceActionByNameRow{
		ID:                   actionID,
		SourceID:             sourceID,
		SourceName:           "finland_prhytj",
		Country:              "finland",
		Source:               "prhytj",
		RegistryKey:          "finland/prhytj",
		Action:               companysourceworkflows.ActionRefreshExplorerCache,
		DisplayName:          "Refresh explorer cache",
		TemporalWorkflowType: companysourceworkflows.RefreshSourceExplorerCacheWorkflowName,
		TemporalTaskQueue:    ptrString(companysourceworkflows.SourceTaskQueue),
		Enabled:              true,
		Config:               json.RawMessage(`{}`),
	}, nil)
	var createdActionRunID uuid.UUID
	q.On("CreateSourceActionRun", mock.Anything, mock.MatchedBy(func(arg db.CreateSourceActionRunParams) bool {
		createdActionRunID = arg.ID
		return arg.ActionID == actionID &&
			arg.TemporalWorkflowID != nil &&
			*arg.TemporalWorkflowID == companysourceworkflows.ActionRunWorkflowID(arg.ID.String()) &&
			json.Valid(arg.Input)
	})).Return(func(_ context.Context, arg db.CreateSourceActionRunParams) db.DataSourceActionRun {
		return db.DataSourceActionRun{
			ID:       arg.ID,
			SourceID: sourceID,
			ActionID: actionID,
			Action:   companysourceworkflows.ActionRefreshExplorerCache,
			Status:   "running",
		}
	}, nil)

	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(q, nil, nil, nil, "", tc, ""))
	body := strings.NewReader(`{"trigger":"manual"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sources/finland_prhytj/actions/refresh_explorer_cache/trigger", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	require.Equal(t, companysourceworkflows.SourceTaskQueue, tc.options.TaskQueue)
	require.Equal(t, companysourceworkflows.RefreshSourceExplorerCacheWorkflowName, tc.workflow)
	require.Equal(t, companysourceworkflows.ActionRunWorkflowID(createdActionRunID.String()), tc.options.ID)
	require.Equal(t, []interface{}{companysourceworkflows.RefreshSourceExplorerCacheInput{
		ActionRunID: createdActionRunID.String(),
		SourceName:  "finland_prhytj",
		Trigger:     "manual",
	}}, tc.args)
}

func TestTriggerSourceIndustryNACEMappingActionStartsTemporalWorkflow(t *testing.T) {
	q := &stubQuerier{}
	sourceID := uuid.New()
	actionID := uuid.New()
	q.On("GetSourceActionByName", mock.Anything, db.GetSourceActionByNameParams{
		Name: "finland_prhytj", Action: companysourceworkflows.ActionMapIndustriesToNACE,
	}).Return(db.GetSourceActionByNameRow{
		ID:                   actionID,
		SourceID:             sourceID,
		SourceName:           "finland_prhytj",
		Country:              "finland",
		Source:               "prhytj",
		RegistryKey:          "finland/prhytj",
		Action:               companysourceworkflows.ActionMapIndustriesToNACE,
		DisplayName:          "Map industries to NACE",
		TemporalWorkflowType: companysourceworkflows.MapSourceIndustriesToNACEWorkflowName,
		TemporalTaskQueue:    ptrString(companysourceworkflows.SourceTaskQueue),
		Enabled:              true,
		Config:               json.RawMessage(`{}`),
	}, nil)
	var createdActionRunID uuid.UUID
	q.On("CreateSourceActionRun", mock.Anything, mock.MatchedBy(func(arg db.CreateSourceActionRunParams) bool {
		createdActionRunID = arg.ID
		return arg.ActionID == actionID &&
			arg.TemporalWorkflowID != nil &&
			*arg.TemporalWorkflowID == companysourceworkflows.ActionRunWorkflowID(arg.ID.String()) &&
			json.Valid(arg.Input)
	})).Return(func(_ context.Context, arg db.CreateSourceActionRunParams) db.DataSourceActionRun {
		return db.DataSourceActionRun{
			ID:       arg.ID,
			SourceID: sourceID,
			ActionID: actionID,
			Action:   companysourceworkflows.ActionMapIndustriesToNACE,
			Status:   "running",
		}
	}, nil)

	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(q, nil, nil, nil, "", tc, ""))
	body := strings.NewReader(`{"trigger":"manual"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sources/finland_prhytj/actions/map_industries_to_nace/trigger", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	require.Equal(t, companysourceworkflows.SourceTaskQueue, tc.options.TaskQueue)
	require.Equal(t, companysourceworkflows.MapSourceIndustriesToNACEWorkflowName, tc.workflow)
	require.Equal(t, companysourceworkflows.ActionRunWorkflowID(createdActionRunID.String()), tc.options.ID)
	require.Equal(t, []interface{}{companysourceworkflows.MapSourceIndustriesToNACEInput{
		ActionRunID: createdActionRunID.String(),
		SourceName:  "finland_prhytj",
		Trigger:     "manual",
	}}, tc.args)
}

func TestListSourceActionRunsClampsLimit(t *testing.T) {
	q := &stubQuerier{}
	q.On("ListSourceActionRuns", mock.Anything, db.ListSourceActionRunsParams{
		Name:  "finland_prhytj",
		Limit: 100,
	}).Return([]db.ListSourceActionRunsRow{}, nil)

	r := routerFor(newTestHandlers(q))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sources/finland_prhytj/action-runs?limit=9999999999", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	q.AssertExpectations(t)
}

func TestListSourceActionRunsReturnsEmptyItemsArray(t *testing.T) {
	q := &stubQuerier{}
	q.On("ListSourceActionRuns", mock.Anything, db.ListSourceActionRunsParams{
		Name:  "finland_prhytj",
		Limit: 20,
	}).Return([]db.ListSourceActionRunsRow(nil), nil)

	r := routerFor(newTestHandlers(q))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sources/finland_prhytj/action-runs", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `{"items":[]}`, w.Body.String())
	q.AssertExpectations(t)
}

func TestGetLatestSuccessfulSourceDownloadUsesPullSourceAction(t *testing.T) {
	q := &stubQuerier{}
	runID := uuid.New()
	q.On("GetLatestSuccessfulSourceActionRun", mock.Anything, db.GetLatestSuccessfulSourceActionRunParams{
		Name:   "finland_prhytj",
		Action: companysourceworkflows.ActionPullSource,
	}).Return(db.DataSourceActionRun{
		ID:     runID,
		Action: companysourceworkflows.ActionPullSource,
		Status: companysourceworkflows.StatusSucceeded,
		Result: json.RawMessage(`{"run_dir":"/runs/finland/prhytj/1"}`),
	}, nil)

	r := routerFor(newTestHandlers(q))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sources/finland_prhytj/latest-successful-download", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), runID.String())
	require.Contains(t, w.Body.String(), `"source_name":"finland_prhytj"`)
	require.Contains(t, w.Body.String(), `"run_dir"`)
	q.AssertExpectations(t)
}

func TestTriggerSourceSyncStartsCompositeWorkflow(t *testing.T) {
	q := &stubQuerier{}
	sourceID := uuid.New()
	pullActionID := uuid.New()
	importActionID := uuid.New()
	q.On("GetSourceActionByName", mock.Anything, db.GetSourceActionByNameParams{
		Name: "finland_prhytj", Action: companysourceworkflows.ActionPullSource,
	}).Return(db.GetSourceActionByNameRow{
		ID:                   pullActionID,
		SourceID:             sourceID,
		SourceName:           "finland_prhytj",
		Action:               companysourceworkflows.ActionPullSource,
		TemporalWorkflowType: companysourceworkflows.DownloadSourceWorkflowName,
		Enabled:              true,
	}, nil)
	q.On("GetSourceActionByName", mock.Anything, db.GetSourceActionByNameParams{
		Name: "finland_prhytj", Action: companysourceworkflows.ActionImportClickHouse,
	}).Return(db.GetSourceActionByNameRow{
		ID:                   importActionID,
		SourceID:             sourceID,
		SourceName:           "finland_prhytj",
		Action:               companysourceworkflows.ActionImportClickHouse,
		TemporalWorkflowType: companysourceworkflows.ImportSourceToClickHouseWorkflowName,
		Enabled:              true,
	}, nil)
	created := make([]db.CreateSourceActionRunParams, 0, 2)
	q.On("CreateSourceActionRun", mock.Anything, mock.AnythingOfType("db.CreateSourceActionRunParams")).Return(func(_ context.Context, arg db.CreateSourceActionRunParams) db.DataSourceActionRun {
		created = append(created, arg)
		return db.DataSourceActionRun{ID: arg.ID, SourceID: sourceID, ActionID: arg.ActionID, Status: "running"}
	}, nil)
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(q, nil, nil, nil, "", tc, ""))

	body := strings.NewReader(`{"trigger":"manual","batch_size":1000}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sources/finland_prhytj/sync-clickhouse", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	require.Equal(t, companysourceworkflows.SourceTaskQueue, tc.options.TaskQueue)
	require.Equal(t, companysourceworkflows.SyncSourceToClickHouseWorkflowName, tc.workflow)
	require.Len(t, created, 2)
	input := tc.args[0].(companysourceworkflows.SyncSourceToClickHouseInput)
	require.Equal(t, "finland_prhytj", input.SourceName)
	require.Equal(t, "manual", input.Trigger)
	require.Equal(t, 1000, input.BatchSize)
	require.NotEmpty(t, input.DownloadActionRunID)
	require.NotEmpty(t, input.ImportActionRunID)
}

var _ client.Client = (*temporalExecuteRecorder)(nil)
