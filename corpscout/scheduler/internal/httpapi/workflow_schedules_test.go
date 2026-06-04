package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"

	brregworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/workflow"
	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/fx"
	"github.com/pulsarpoint/corpscout/scheduler/internal/httpapi"
	"github.com/pulsarpoint/corpscout/scheduler/internal/nacetaxonomy"
)

func TestCreateWorkflowScheduleRejectsUnknownWorkflowKey(t *testing.T) {
	tc := newTemporalScheduleClientRecorder()
	r := routerFor(httpapi.NewHandlers(&scheduleMetadataQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflow-schedules", bytes.NewBufferString(`{
		"temporal_schedule_id": "nace-taxonomy-sync-nightly",
		"workflow_key": "unknown",
		"display_name": "Nightly NACE sync",
		"enabled": true,
		"spec": {
			"timezone": "Europe/Belgrade",
			"cron_expression": "0 3 * * *",
			"overlap_policy": "skip",
			"catchup_window_seconds": 3600
		},
		"action_input": {}
	}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), `"error":"workflow key is not schedulable"`)
	require.False(t, tc.schedule.createCalled)
}

func TestCreateWorkflowScheduleRejectsInvalidCron(t *testing.T) {
	tc := newTemporalScheduleClientRecorder()
	r := routerFor(httpapi.NewHandlers(&scheduleMetadataQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflow-schedules", bytes.NewBufferString(`{
		"temporal_schedule_id": "nace-taxonomy-sync-nightly",
		"workflow_key": "nace_taxonomy_sync",
		"display_name": "Nightly NACE sync",
		"enabled": true,
		"spec": {
			"timezone": "Europe/Belgrade",
			"cron_expression": "invalid cron",
			"overlap_policy": "skip",
			"catchup_window_seconds": 3600
		},
		"action_input": {}
	}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), `"error":"cron expression must contain 5 fields"`)
	require.False(t, tc.schedule.createCalled)
}

func TestCreateWorkflowScheduleRejectsInvalidNACESourceURL(t *testing.T) {
	tc := newTemporalScheduleClientRecorder()
	r := routerFor(httpapi.NewHandlers(&scheduleMetadataQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflow-schedules", bytes.NewBufferString(`{
		"temporal_schedule_id": "nace-taxonomy-sync-nightly",
		"workflow_key": "nace_taxonomy_sync",
		"display_name": "Nightly NACE sync",
		"enabled": true,
		"spec": {
			"timezone": "Europe/Belgrade",
			"cron_expression": "0 3 * * *",
			"overlap_policy": "skip",
			"catchup_window_seconds": 3600
		},
		"action_input": {"source_url": "file:///tmp/nace.rdf"}
	}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), `"error":"nace source url must be http or https"`)
	require.False(t, tc.schedule.createCalled)
}

func TestCreateWorkflowScheduleCreatesTemporalScheduleAndMetadata(t *testing.T) {
	tc := newTemporalScheduleClientRecorder()
	q := &scheduleMetadataQuerier{}
	r := routerFor(httpapi.NewHandlers(q, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflow-schedules", bytes.NewBufferString(`{
		"temporal_schedule_id": "nace-taxonomy-sync-nightly",
		"workflow_key": "nace_taxonomy_sync",
		"display_name": "Nightly NACE sync",
		"description": "Keep NACE taxonomy fresh",
		"enabled": true,
		"tags": ["taxonomy", "nace", "nace"],
		"metadata": {"owner": "ops"},
		"spec": {
			"timezone": "Europe/Belgrade",
			"cron_expression": "0 3 * * *",
			"overlap_policy": "skip",
			"catchup_window_seconds": 3600
		},
		"action_input": {
			"revision": "2.1",
			"source_url": "https://example.test/nace.rdf",
			"trigger": "schedule",
			"force_reprocess": false
		}
	}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusCreated, w.Code)
	require.True(t, tc.schedule.createCalled)
	require.Equal(t, "nace-taxonomy-sync-nightly", tc.schedule.createOptions.ID)
	require.Equal(t, []string{"0 3 * * *"}, tc.schedule.createOptions.Spec.CronExpressions)
	require.Equal(t, "Europe/Belgrade", tc.schedule.createOptions.Spec.TimeZoneName)
	require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, tc.schedule.createOptions.Overlap)
	require.Equal(t, time.Hour, tc.schedule.createOptions.CatchupWindow)
	require.False(t, tc.schedule.createOptions.Paused)
	action, ok := tc.schedule.createOptions.Action.(*client.ScheduleWorkflowAction)
	require.True(t, ok)
	require.Len(t, action.Args, 1)
	actionInput, ok := action.Args[0].(nacetaxonomy.SyncNACETaxonomyInput)
	require.True(t, ok)
	require.Equal(t, nacetaxonomy.DefaultRevision, actionInput.Revision)
	require.Equal(t, "nace-taxonomy-sync-nightly", q.createParams.TemporalScheduleID)
	require.Equal(t, "nace_taxonomy_sync", q.createParams.WorkflowKey)
	require.Equal(t, []string{"taxonomy", "nace"}, q.createParams.Tags)

	var body map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Equal(t, "nace-taxonomy-sync-nightly", body["temporal_schedule_id"])
	require.Equal(t, "Nightly NACE sync", body["display_name"])
	require.Equal(t, true, body["enabled"])
	require.Equal(t, map[string]any{"owner": "ops"}, body["metadata"])
	require.Equal(t, map[string]any{
		"exists": true,
		"paused": false,
		"note":   "",
	}, body["temporal"])
}

func TestCreateWorkflowScheduleCreatesFXTemporalScheduleAndMetadata(t *testing.T) {
	tc := newTemporalScheduleClientRecorder()
	q := &scheduleMetadataQuerier{}
	r := routerFor(httpapi.NewHandlers(q, nil, nil, nil, "", tc, "").ConfigureFX("https://example.test/ecb.xml"))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflow-schedules", bytes.NewBufferString(`{
		"temporal_schedule_id": "fx-rate-sync-daily",
		"workflow_key": "fx_rate_sync",
		"display_name": "Daily FX rate sync",
		"description": "Keep exchange rates fresh",
		"enabled": true,
		"tags": ["fx", "exchange-rates", "fx"],
		"metadata": {"owner": "ops"},
		"spec": {
			"timezone": "Europe/Belgrade",
			"cron_expression": "15 4 * * *",
			"overlap_policy": "skip",
			"catchup_window_seconds": 3600
		},
		"action_input": {
			"trigger": "schedule",
			"force_reprocess": false
		}
	}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusCreated, w.Code)
	require.True(t, tc.schedule.createCalled)
	require.Equal(t, "fx-rate-sync-daily", tc.schedule.createOptions.ID)
	action, ok := tc.schedule.createOptions.Action.(*client.ScheduleWorkflowAction)
	require.True(t, ok)
	require.Equal(t, fx.SyncWorkflowName, action.Workflow)
	require.Equal(t, fx.SyncTaskQueue, action.TaskQueue)
	require.Len(t, action.Args, 1)
	actionInput, ok := action.Args[0].(fx.SyncExchangeRatesInput)
	require.True(t, ok)
	require.Equal(t, fx.DefaultProvider, actionInput.Provider)
	require.Equal(t, "https://example.test/ecb.xml", actionInput.SourceURL)
	require.Equal(t, "schedule", actionInput.Trigger)
	require.False(t, actionInput.ForceReprocess)
	require.Equal(t, "fx-rate-sync-daily", q.createParams.TemporalScheduleID)
	require.Equal(t, "fx_rate_sync", q.createParams.WorkflowKey)
	require.Equal(t, fx.SyncWorkflowName, q.createParams.WorkflowName)
	require.Equal(t, fx.SyncTaskQueue, q.createParams.TaskQueue)
	require.Equal(t, []string{"fx", "exchange-rates"}, q.createParams.Tags)
}

func TestCreateWorkflowScheduleCreatesBrregSourceExplorerRefreshScheduleAndMetadata(t *testing.T) {
	tc := newTemporalScheduleClientRecorder()
	q := &scheduleMetadataQuerier{}
	r := routerFor(httpapi.NewHandlers(q, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflow-schedules", bytes.NewBufferString(`{
		"temporal_schedule_id": "brreg-source-explorer-refresh-hourly",
		"workflow_key": "brreg_source_explorer_refresh",
		"display_name": "Hourly BRREG source explorer refresh",
		"description": "Keep source entries reads fast",
		"enabled": true,
		"tags": ["brreg", "source-explorer"],
		"metadata": {"owner": "ops"},
		"spec": {
			"timezone": "Europe/Belgrade",
			"cron_expression": "10 * * * *",
			"overlap_policy": "skip",
			"catchup_window_seconds": 3600
		},
		"action_input": {}
	}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusCreated, w.Code)
	require.True(t, tc.schedule.createCalled)
	require.Equal(t, "brreg-source-explorer-refresh-hourly", tc.schedule.createOptions.ID)
	action, ok := tc.schedule.createOptions.Action.(*client.ScheduleWorkflowAction)
	require.True(t, ok)
	require.Equal(t, brregworkflow.RefreshBrregSourceExplorerWorkflowName, action.Workflow)
	require.Equal(t, brregworkflow.RefreshBrregSourceExplorerTaskQueue, action.TaskQueue)
	require.Len(t, action.Args, 1)
	actionInput, ok := action.Args[0].(brregworkflow.RefreshBrregSourceExplorerInput)
	require.True(t, ok)
	require.Equal(t, "schedule", actionInput.Trigger)
	require.Equal(t, "brreg-source-explorer-refresh-hourly", q.createParams.TemporalScheduleID)
	require.Equal(t, "brreg_source_explorer_refresh", q.createParams.WorkflowKey)
	require.Equal(t, brregworkflow.RefreshBrregSourceExplorerWorkflowName, q.createParams.WorkflowName)
	require.Equal(t, brregworkflow.RefreshBrregSourceExplorerTaskQueue, q.createParams.TaskQueue)
	require.Equal(t, "brreg", q.createParams.Domain)
	require.Equal(t, "source_explorer_refresh", q.createParams.Purpose)
	require.Equal(t, []string{"brreg", "source-explorer"}, q.createParams.Tags)
}

func TestCreateWorkflowScheduleRejectsInvalidFXSourceURL(t *testing.T) {
	tc := newTemporalScheduleClientRecorder()
	r := routerFor(httpapi.NewHandlers(&scheduleMetadataQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflow-schedules", bytes.NewBufferString(`{
		"temporal_schedule_id": "fx-rate-sync-daily",
		"workflow_key": "fx_rate_sync",
		"display_name": "Daily FX rate sync",
		"enabled": true,
		"spec": {
			"timezone": "Europe/Belgrade",
			"cron_expression": "15 4 * * *",
			"overlap_policy": "skip",
			"catchup_window_seconds": 3600
		},
		"action_input": {"source_url": "file:///tmp/ecb.xml"}
	}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), `"error":"fx source url must be http or https"`)
	require.False(t, tc.schedule.createCalled)
}

func TestListWorkflowSchedulesReturnsMetadataAndTemporalState(t *testing.T) {
	tc := newTemporalScheduleClientRecorder()
	now := time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC)
	row := testWorkflowScheduleRow("nace-taxonomy-sync-nightly", now)
	tc.schedule.handles[row.TemporalScheduleID] = &scheduleHandleRecorder{
		id: row.TemporalScheduleID,
		description: scheduleDescriptionForOptions(client.ScheduleOptions{
			ID: row.TemporalScheduleID,
			Spec: client.ScheduleSpec{
				CronExpressions: []string{"0 3 * * *"},
				TimeZoneName:    "Europe/Belgrade",
			},
			Overlap:       enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
			CatchupWindow: time.Hour,
			Action: &client.ScheduleWorkflowAction{
				Workflow:  "SyncNACETaxonomy",
				TaskQueue: "nace-taxonomy-sync",
				Args:      []interface{}{map[string]any{"trigger": "schedule"}},
			},
		}),
	}
	q := &scheduleMetadataQuerier{listRows: []db.TemporalScheduleMetadatum{row}}
	r := routerFor(httpapi.NewHandlers(q, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/workflow-schedules", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"temporal_schedule_id":"nace-taxonomy-sync-nightly"`)
	require.Contains(t, w.Body.String(), `"exists":true`)
	require.Contains(t, w.Body.String(), `"cron_expression":"0 3 * * *"`)
}

func TestUpdateWorkflowScheduleUsesURLScheduleID(t *testing.T) {
	tc := newTemporalScheduleClientRecorder()
	row := testWorkflowScheduleRow("nace-taxonomy-sync-nightly", time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC))
	tc.schedule.handles[row.TemporalScheduleID] = &scheduleHandleRecorder{
		id: row.TemporalScheduleID,
		description: scheduleDescriptionForOptions(client.ScheduleOptions{
			ID: row.TemporalScheduleID,
			Spec: client.ScheduleSpec{
				CronExpressions: []string{"0 3 * * *"},
				TimeZoneName:    "Europe/Belgrade",
			},
			Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
			Action: &client.ScheduleWorkflowAction{
				Workflow:  "SyncNACETaxonomy",
				TaskQueue: "nace-taxonomy-sync",
				Args:      []interface{}{map[string]any{"trigger": "schedule"}},
			},
		}),
	}
	q := &scheduleMetadataQuerier{getRow: row}
	r := routerFor(httpapi.NewHandlers(q, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPatch, "/api/v1/workflow-schedules/nace-taxonomy-sync-nightly", bytes.NewBufferString(`{
		"workflow_key": "nace_taxonomy_sync",
		"display_name": "Updated NACE sync",
		"enabled": true,
		"tags": ["taxonomy"],
		"metadata": {},
		"spec": {
			"timezone": "Europe/Belgrade",
			"cron_expression": "0 4 * * *",
			"overlap_policy": "skip",
			"catchup_window_seconds": 120
		},
		"action_input": {
			"source_url": "https://example.test/nace.rdf",
			"trigger": "schedule"
		}
	}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "nace-taxonomy-sync-nightly", q.updateParams.TemporalScheduleID)
	require.Equal(t, "Updated NACE sync", q.updateParams.DisplayName)
	require.Equal(t, "0 4 * * *", tc.schedule.handles[row.TemporalScheduleID].description.Schedule.Spec.CronExpressions[0])
}

func TestUpdateWorkflowSchedulePreservesPausedStateWhenEnabledUnchanged(t *testing.T) {
	tc := newTemporalScheduleClientRecorder()
	row := testWorkflowScheduleRow("nace-taxonomy-sync-nightly", time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC))
	row.Enabled = true
	description := scheduleDescriptionForOptions(client.ScheduleOptions{
		ID: row.TemporalScheduleID,
		Spec: client.ScheduleSpec{
			CronExpressions: []string{"0 3 * * *"},
			TimeZoneName:    "Europe/Belgrade",
		},
		Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		Action: &client.ScheduleWorkflowAction{
			Workflow:  "SyncNACETaxonomy",
			TaskQueue: "nace-taxonomy-sync",
			Args:      []interface{}{map[string]any{"source_url": "https://example.test/nace.rdf", "trigger": "schedule"}},
		},
	})
	description.Schedule.State.Paused = true
	description.Schedule.State.Note = "Paused for maintenance"
	tc.schedule.handles[row.TemporalScheduleID] = &scheduleHandleRecorder{
		id:          row.TemporalScheduleID,
		description: description,
	}
	q := &scheduleMetadataQuerier{getRow: row}
	r := routerFor(httpapi.NewHandlers(q, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPatch, "/api/v1/workflow-schedules/nace-taxonomy-sync-nightly", bytes.NewBufferString(`{
		"workflow_key": "nace_taxonomy_sync",
		"display_name": "Updated NACE sync",
		"enabled": true,
		"tags": ["taxonomy"],
		"metadata": {},
		"spec": {
			"timezone": "Europe/Belgrade",
			"cron_expression": "0 4 * * *",
			"overlap_policy": "skip",
			"catchup_window_seconds": 120
		},
		"action_input": {
			"source_url": "https://example.test/nace.rdf",
			"trigger": "schedule"
		}
	}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	state := tc.schedule.handles[row.TemporalScheduleID].description.Schedule.State
	require.True(t, state.Paused)
	require.Equal(t, "Paused for maintenance", state.Note)
	require.True(t, q.updateParams.Enabled)
}

func TestTriggerWorkflowScheduleCallsTemporal(t *testing.T) {
	tc := newTemporalScheduleClientRecorder()
	tc.schedule.handles["nace-taxonomy-sync-nightly"] = &scheduleHandleRecorder{id: "nace-taxonomy-sync-nightly"}
	q := &scheduleMetadataQuerier{
		getRow: testWorkflowScheduleRow("nace-taxonomy-sync-nightly", time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC)),
	}
	r := routerFor(httpapi.NewHandlers(q, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflow-schedules/nace-taxonomy-sync-nightly/trigger", bytes.NewBufferString(`{}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.True(t, tc.schedule.handles["nace-taxonomy-sync-nightly"].triggerCalled)
	require.Contains(t, w.Body.String(), `"status":"triggered"`)
}

func TestTriggerWorkflowScheduleReturnsNotFoundWhenMetadataMissing(t *testing.T) {
	tc := newTemporalScheduleClientRecorder()
	tc.schedule.handles["nace-taxonomy-sync-nightly"] = &scheduleHandleRecorder{id: "nace-taxonomy-sync-nightly"}
	q := &scheduleMetadataQuerier{getErr: pgx.ErrNoRows}
	r := routerFor(httpapi.NewHandlers(q, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflow-schedules/nace-taxonomy-sync-nightly/trigger", bytes.NewBufferString(`{}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusNotFound, w.Code)
	require.False(t, tc.schedule.handles["nace-taxonomy-sync-nightly"].triggerCalled)
	require.Contains(t, w.Body.String(), `"error":"workflow schedule not found"`)
}

func TestDeleteWorkflowScheduleRemovesMetadataWhenTemporalMissing(t *testing.T) {
	tc := newTemporalScheduleClientRecorder()
	tc.schedule.handles["nace-taxonomy-sync-nightly"] = &scheduleHandleRecorder{
		id:        "nace-taxonomy-sync-nightly",
		deleteErr: serviceerror.NewNotFound("missing schedule"),
	}
	q := &scheduleMetadataQuerier{
		getRow: testWorkflowScheduleRow("nace-taxonomy-sync-nightly", time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC)),
	}
	r := routerFor(httpapi.NewHandlers(q, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/workflow-schedules/nace-taxonomy-sync-nightly", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "nace-taxonomy-sync-nightly", q.deletedScheduleID)
	require.Contains(t, w.Body.String(), `"status":"deleted"`)
	require.Contains(t, w.Body.String(), `"temporal_missing":true`)
}

func TestDeleteWorkflowScheduleReturnsNotFoundWhenMetadataMissing(t *testing.T) {
	tc := newTemporalScheduleClientRecorder()
	tc.schedule.handles["nace-taxonomy-sync-nightly"] = &scheduleHandleRecorder{id: "nace-taxonomy-sync-nightly"}
	q := &scheduleMetadataQuerier{getErr: pgx.ErrNoRows}
	r := routerFor(httpapi.NewHandlers(q, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/workflow-schedules/nace-taxonomy-sync-nightly", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusNotFound, w.Code)
	require.Empty(t, q.deletedScheduleID)
	require.Contains(t, w.Body.String(), `"error":"workflow schedule not found"`)
}

func TestPauseWorkflowScheduleReturnsNotFoundWhenMetadataMissing(t *testing.T) {
	tc := newTemporalScheduleClientRecorder()
	q := &scheduleMetadataQuerier{getErr: pgx.ErrNoRows}
	r := routerFor(httpapi.NewHandlers(q, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflow-schedules/nace-taxonomy-sync-nightly/pause", bytes.NewBufferString(`{}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusNotFound, w.Code)
	require.Contains(t, w.Body.String(), `"error":"workflow schedule not found"`)
}

type scheduleMetadataQuerier struct {
	stubQuerier
	createParams      db.CreateTemporalScheduleMetadataParams
	updateParams      db.UpdateTemporalScheduleMetadataParams
	deletedScheduleID string
	listRows          []db.TemporalScheduleMetadatum
	getRow            db.TemporalScheduleMetadatum
	getErr            error
}

func (s *scheduleMetadataQuerier) CreateTemporalScheduleMetadata(_ context.Context, arg db.CreateTemporalScheduleMetadataParams) (db.TemporalScheduleMetadatum, error) {
	s.createParams = arg
	return scheduleRowFromCreateParams(arg), nil
}

func (s *scheduleMetadataQuerier) UpdateTemporalScheduleMetadata(_ context.Context, arg db.UpdateTemporalScheduleMetadataParams) (db.TemporalScheduleMetadatum, error) {
	s.updateParams = arg
	return scheduleRowFromUpdateParams(arg), nil
}

func (s *scheduleMetadataQuerier) GetTemporalScheduleMetadata(_ context.Context, temporalScheduleID string) (db.TemporalScheduleMetadatum, error) {
	if s.getErr != nil {
		return db.TemporalScheduleMetadatum{}, s.getErr
	}
	if s.getRow.TemporalScheduleID != "" {
		return s.getRow, nil
	}
	return testWorkflowScheduleRow(temporalScheduleID, time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC)), nil
}

func (s *scheduleMetadataQuerier) ListTemporalScheduleMetadata(_ context.Context, _ db.ListTemporalScheduleMetadataParams) ([]db.TemporalScheduleMetadatum, error) {
	return s.listRows, nil
}

func (s *scheduleMetadataQuerier) DeleteTemporalScheduleMetadata(_ context.Context, temporalScheduleID string) error {
	s.deletedScheduleID = temporalScheduleID
	return nil
}

type temporalScheduleClientRecorder struct {
	client.Client
	schedule *scheduleClientRecorder
}

func newTemporalScheduleClientRecorder() *temporalScheduleClientRecorder {
	return &temporalScheduleClientRecorder{schedule: &scheduleClientRecorder{handles: map[string]*scheduleHandleRecorder{}}}
}

func (t *temporalScheduleClientRecorder) ScheduleClient() client.ScheduleClient {
	return t.schedule
}

type scheduleClientRecorder struct {
	createCalled  bool
	createOptions client.ScheduleOptions
	handles       map[string]*scheduleHandleRecorder
}

func (s *scheduleClientRecorder) Create(_ context.Context, options client.ScheduleOptions) (client.ScheduleHandle, error) {
	s.createCalled = true
	s.createOptions = options
	handle := &scheduleHandleRecorder{
		id:          options.ID,
		description: scheduleDescriptionForOptions(options),
	}
	s.handles[options.ID] = handle
	return handle, nil
}

func (s *scheduleClientRecorder) List(context.Context, client.ScheduleListOptions) (client.ScheduleListIterator, error) {
	return nil, nil
}

func (s *scheduleClientRecorder) GetHandle(_ context.Context, scheduleID string) client.ScheduleHandle {
	if handle, ok := s.handles[scheduleID]; ok {
		return handle
	}
	return &scheduleHandleRecorder{id: scheduleID, describeErr: serviceerror.NewNotFound("missing schedule")}
}

type scheduleHandleRecorder struct {
	id            string
	description   *client.ScheduleDescription
	describeErr   error
	deleteErr     error
	triggerCalled bool
	pauseCalled   bool
	unpauseCalled bool
}

func (s *scheduleHandleRecorder) GetID() string {
	return s.id
}

func (s *scheduleHandleRecorder) Delete(context.Context) error {
	return s.deleteErr
}

func (s *scheduleHandleRecorder) Backfill(context.Context, client.ScheduleBackfillOptions) error {
	return nil
}

func (s *scheduleHandleRecorder) Update(_ context.Context, options client.ScheduleUpdateOptions) error {
	update, err := options.DoUpdate(client.ScheduleUpdateInput{Description: *s.description})
	if err != nil {
		return err
	}
	s.description.Schedule = *update.Schedule
	return nil
}

func (s *scheduleHandleRecorder) Describe(context.Context) (*client.ScheduleDescription, error) {
	if s.describeErr != nil {
		return nil, s.describeErr
	}
	if s.description == nil {
		s.description = scheduleDescriptionForOptions(client.ScheduleOptions{ID: s.id})
	}
	return s.description, nil
}

func (s *scheduleHandleRecorder) Trigger(context.Context, client.ScheduleTriggerOptions) error {
	s.triggerCalled = true
	return nil
}

func (s *scheduleHandleRecorder) Pause(_ context.Context, _ client.SchedulePauseOptions) error {
	s.pauseCalled = true
	return nil
}

func (s *scheduleHandleRecorder) Unpause(_ context.Context, _ client.ScheduleUnpauseOptions) error {
	s.unpauseCalled = true
	return nil
}

func scheduleDescriptionForOptions(options client.ScheduleOptions) *client.ScheduleDescription {
	return &client.ScheduleDescription{
		Schedule: client.Schedule{
			Action: options.Action,
			Spec:   &options.Spec,
			Policy: &client.SchedulePolicies{
				Overlap:       options.Overlap,
				CatchupWindow: options.CatchupWindow,
			},
			State: &client.ScheduleState{
				Paused: options.Paused,
				Note:   options.Note,
			},
		},
	}
}

func scheduleRowFromCreateParams(arg db.CreateTemporalScheduleMetadataParams) db.TemporalScheduleMetadatum {
	now := time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC)
	return db.TemporalScheduleMetadatum{
		ID:                 uuid.MustParse("9d7fe042-6d43-4b8a-8f3f-7454c09b0661"),
		TemporalScheduleID: arg.TemporalScheduleID,
		WorkflowKey:        arg.WorkflowKey,
		WorkflowName:       arg.WorkflowName,
		TaskQueue:          arg.TaskQueue,
		Domain:             arg.Domain,
		Purpose:            arg.Purpose,
		DisplayName:        arg.DisplayName,
		Description:        arg.Description,
		Enabled:            arg.Enabled,
		Tags:               arg.Tags,
		Metadata:           arg.Metadata,
		CreatedAt:          now,
		UpdatedAt:          now,
	}
}

func scheduleRowFromUpdateParams(arg db.UpdateTemporalScheduleMetadataParams) db.TemporalScheduleMetadatum {
	create := db.CreateTemporalScheduleMetadataParams{
		TemporalScheduleID: arg.TemporalScheduleID,
		WorkflowKey:        arg.WorkflowKey,
		WorkflowName:       arg.WorkflowName,
		TaskQueue:          arg.TaskQueue,
		Domain:             arg.Domain,
		Purpose:            arg.Purpose,
		DisplayName:        arg.DisplayName,
		Description:        arg.Description,
		Enabled:            arg.Enabled,
		Tags:               arg.Tags,
		Metadata:           arg.Metadata,
	}
	return scheduleRowFromCreateParams(create)
}

func testWorkflowScheduleRow(scheduleID string, now time.Time) db.TemporalScheduleMetadatum {
	description := "Keep NACE taxonomy fresh"
	return db.TemporalScheduleMetadatum{
		ID:                 uuid.MustParse("9d7fe042-6d43-4b8a-8f3f-7454c09b0661"),
		TemporalScheduleID: scheduleID,
		WorkflowKey:        "nace_taxonomy_sync",
		WorkflowName:       "SyncNACETaxonomy",
		TaskQueue:          "nace-taxonomy-sync",
		Domain:             "taxonomy",
		Purpose:            "nace_taxonomy_sync",
		DisplayName:        "Nightly NACE sync",
		Description:        &description,
		Enabled:            true,
		Tags:               []string{"taxonomy", "nace"},
		Metadata:           json.RawMessage(`{"owner":"ops"}`),
		CreatedAt:          now,
		UpdatedAt:          now,
	}
}
