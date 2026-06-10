package httpapi_test

import (
	"context"
	"encoding/json"
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

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/httpapi"
	companysourceworkflows "github.com/pulsarpoint/corpscout/scheduler/internal/temporal/workflow/companysources"
)

func TestListSourceFilesMarksMissingFromSuccessfulPath(t *testing.T) {
	q := &stubQuerier{}
	sourceID := uuid.New()
	missingID := uuid.New()
	presentID := uuid.New()
	presentFile, err := os.CreateTemp(t.TempDir(), "source-*.ndjson")
	require.NoError(t, err)
	require.NoError(t, presentFile.Close())
	presentPath := presentFile.Name()
	missingPath := presentPath + ".missing"
	description := "Primary company feed"
	successRunID := pgtype.UUID{Bytes: presentID, Valid: true}
	q.On("ListSourceFilesWithLatestRun", mock.Anything, "finland_prhytj").Return([]db.ListSourceFilesWithLatestRunRow{
		{
			ID:                    missingID,
			SourceID:              sourceID,
			SourceName:            "finland_prhytj",
			FileKey:               "companies",
			DisplayName:           "Companies",
			Description:           &description,
			Kind:                  "snapshot",
			Required:              true,
			RelativePath:          "companies/source.ndjson",
			Enabled:               true,
			SortOrder:             10,
			LatestSuccessfulRunID: successRunID,
			LatestSuccessfulPath:  &missingPath,
		},
		{
			ID:                    uuid.New(),
			SourceID:              sourceID,
			SourceName:            "finland_prhytj",
			FileKey:               "rek",
			DisplayName:           "REK",
			Kind:                  "snapshot",
			Required:              true,
			RelativePath:          "rek/source.ndjson",
			Enabled:               true,
			SortOrder:             20,
			LatestSuccessfulRunID: successRunID,
			LatestSuccessfulPath:  &presentPath,
		},
	}, nil)

	r := routerFor(newTestHandlers(q))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sources/finland_prhytj/files", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body struct {
		Items []map[string]any `json:"items"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Len(t, body.Items, 2)
	require.Equal(t, true, body.Items[0]["missing"])
	require.Equal(t, false, body.Items[1]["missing"])
	require.Equal(t, "companies", body.Items[0]["file_key"])
	require.Equal(t, "rek", body.Items[1]["file_key"])
	q.AssertExpectations(t)
}

func TestListSourceFileRunsClampsLimit(t *testing.T) {
	q := &stubQuerier{}
	q.On("ListSourceFileRuns", mock.Anything, db.ListSourceFileRunsParams{
		SourceName: "finland_prhytj",
		FileKey:    "rek",
		RowLimit:   100,
	}).Return([]db.ListSourceFileRunsRow{}, nil)

	r := routerFor(newTestHandlers(q))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sources/finland_prhytj/files/rek/runs?limit=1000", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `{"items":[]}`, w.Body.String())
	q.AssertExpectations(t)
}

func TestTriggerSourceFileDownloadStartsTemporalWorkflow(t *testing.T) {
	q := &stubQuerier{}
	sourceID := uuid.New()
	fileID := uuid.New()
	q.On("GetSourceFileBySourceNameAndKey", mock.Anything, db.GetSourceFileBySourceNameAndKeyParams{
		Name:    "finland_prhytj",
		FileKey: "rek",
	}).Return(db.GetSourceFileBySourceNameAndKeyRow{
		ID:           fileID,
		SourceID:     sourceID,
		FileKey:      "rek",
		DisplayName:  "REK",
		Kind:         "snapshot",
		Required:     true,
		RelativePath: "rek/source.ndjson",
		Enabled:      true,
		SourceName:   "finland_prhytj",
		Country:      "finland",
		Source:       "prhytj",
		RegistryKey:  "finland/prhytj",
	}, nil)
	var createdFileRunID uuid.UUID
	q.On("CreateSourceFileRun", mock.Anything, mock.MatchedBy(func(arg db.CreateSourceFileRunParams) bool {
		createdFileRunID = arg.ID
		return arg.SourceFileID == fileID &&
			arg.TemporalWorkflowID != nil &&
			*arg.TemporalWorkflowID == companysourceworkflows.FileRunWorkflowID(arg.ID.String())
	})).Return(func(_ context.Context, arg db.CreateSourceFileRunParams) db.DataSourceFileRun {
		return db.DataSourceFileRun{
			ID:           arg.ID,
			SourceID:     sourceID,
			SourceFileID: fileID,
			Status:       "running",
		}
	}, nil)
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(q, nil, nil, nil, "", tc, ""))

	body := strings.NewReader(`{"trigger":"manual"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sources/finland_prhytj/files/rek/download", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	require.Equal(t, companysourceworkflows.SourceTaskQueue, tc.options.TaskQueue)
	require.Equal(t, companysourceworkflows.DownloadSourceFileWorkflowName, tc.workflow)
	require.Equal(t, companysourceworkflows.FileRunWorkflowID(createdFileRunID.String()), tc.options.ID)
	require.Equal(t, []interface{}{companysourceworkflows.DownloadSourceFileInput{
		FileRunID:  createdFileRunID.String(),
		SourceName: "finland_prhytj",
		FileKey:    "rek",
		Trigger:    "manual",
	}}, tc.args)
	require.Contains(t, w.Body.String(), createdFileRunID.String())
	q.AssertExpectations(t)
}

func TestTriggerFinlandPRHXBRLManifestDownloadRejectsDirectFileRun(t *testing.T) {
	q := &stubQuerier{}
	sourceID := uuid.New()
	fileID := uuid.New()
	q.On("GetSourceFileBySourceNameAndKey", mock.Anything, db.GetSourceFileBySourceNameAndKeyParams{
		Name:    "finland_prh_xbrl",
		FileKey: "statements_manifest",
	}).Return(db.GetSourceFileBySourceNameAndKeyRow{
		ID:           fileID,
		SourceID:     sourceID,
		FileKey:      "statements_manifest",
		DisplayName:  "Statements manifest",
		Kind:         "source_manifest",
		Required:     true,
		RelativePath: "statements.ndjson",
		Enabled:      true,
		SourceName:   "finland_prh_xbrl",
		Country:      "finland",
		Source:       "prh_xbrl",
		RegistryKey:  "finland/prh_xbrl",
	}, nil)
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(q, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/sources/finland_prh_xbrl/files/statements_manifest/download", strings.NewReader(`{"trigger":"manual"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "must be started through pull_source")
	require.Nil(t, tc.workflow)
	q.AssertNotCalled(t, "CreateSourceFileRun", mock.Anything, mock.Anything)
	q.AssertExpectations(t)
}

func TestTriggerSourceFileImportStartsTemporalWorkflow(t *testing.T) {
	q := &stubQuerier{}
	sourceID := uuid.New()
	actionID := uuid.New()
	fileRunID := uuid.New()
	q.On("GetSourceActionByName", mock.Anything, db.GetSourceActionByNameParams{
		Name: "finland_prhytj", Action: companysourceworkflows.ActionImportClickHouse,
	}).Return(db.GetSourceActionByNameRow{
		ID:                   actionID,
		SourceID:             sourceID,
		SourceName:           "finland_prhytj",
		Action:               companysourceworkflows.ActionImportClickHouse,
		TemporalWorkflowType: companysourceworkflows.ImportSourceToClickHouseWorkflowName,
		Enabled:              true,
	}, nil)
	q.On("GetLatestSuccessfulSourceFileRun", mock.Anything, db.GetLatestSuccessfulSourceFileRunParams{
		SourceName: "finland_prhytj",
		FileKey:    "source",
	}).Return(db.GetLatestSuccessfulSourceFileRunRow{
		ID:         fileRunID,
		SourceID:   sourceID,
		Status:     companysourceworkflows.StatusSucceeded,
		FileKey:    "source",
		SourceName: "finland_prhytj",
	}, nil)
	var createdActionRunID uuid.UUID
	q.On("CreateSourceActionRun", mock.Anything, mock.MatchedBy(func(arg db.CreateSourceActionRunParams) bool {
		createdActionRunID = arg.ID
		var input companysourceworkflows.ImportSourceToClickHouseInput
		if err := json.Unmarshal(arg.Input, &input); err != nil {
			return false
		}
		return arg.ActionID == actionID &&
			arg.TemporalWorkflowID != nil &&
			*arg.TemporalWorkflowID == companysourceworkflows.ActionRunWorkflowID(arg.ID.String()) &&
			input.ActionRunID == arg.ID.String() &&
			input.SourceName == "finland_prhytj" &&
			input.Trigger == "manual" &&
			input.BatchSize == 500 &&
			len(input.FileRunIDs) == 1 &&
			input.FileRunIDs[0] == fileRunID.String()
	})).Return(func(_ context.Context, arg db.CreateSourceActionRunParams) db.DataSourceActionRun {
		return db.DataSourceActionRun{
			ID:       arg.ID,
			SourceID: sourceID,
			ActionID: actionID,
			Action:   companysourceworkflows.ActionImportClickHouse,
			Status:   "running",
		}
	}, nil)

	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(q, nil, nil, nil, "", tc, ""))
	body := strings.NewReader(`{"trigger":"manual","batch_size":500}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sources/finland_prhytj/files/source/import", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	require.Equal(t, companysourceworkflows.SourceTaskQueue, tc.options.TaskQueue)
	require.Equal(t, companysourceworkflows.ImportSourceToClickHouseWorkflowName, tc.workflow)
	require.Equal(t, companysourceworkflows.ActionRunWorkflowID(createdActionRunID.String()), tc.options.ID)
	require.Equal(t, []interface{}{companysourceworkflows.ImportSourceToClickHouseInput{
		ActionRunID: createdActionRunID.String(),
		SourceName:  "finland_prhytj",
		Trigger:     "manual",
		FileRunIDs:  []string{fileRunID.String()},
		BatchSize:   500,
	}}, tc.args)
	require.Contains(t, w.Body.String(), createdActionRunID.String())
	q.AssertExpectations(t)
}

func TestGetSourceFileRunTemporalStatusReturnsStoredStatusWithoutTemporalClient(t *testing.T) {
	q := &stubQuerier{}
	runID := uuid.New()
	workflowID := companysourceworkflows.FileRunWorkflowID(runID.String())
	temporalRunID := "temporal-run-1"
	startedAt := time.Date(2026, 6, 10, 9, 0, 0, 0, time.UTC)
	q.On("GetSourceFileRunWithDefinition", mock.Anything, runID).Return(db.GetSourceFileRunWithDefinitionRow{
		ID:                 runID,
		Status:             companysourceworkflows.StatusSucceeded,
		TemporalWorkflowID: &workflowID,
		TemporalRunID:      &temporalRunID,
		StartedAt:          startedAt,
		FileKey:            "rek",
		SourceName:         "finland_prhytj",
	}, nil)

	r := routerFor(newTestHandlers(q))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/source-file-runs/"+runID.String()+"/temporal-status", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"db_status":"succeeded"`)
	require.Contains(t, w.Body.String(), workflowID)
	require.Contains(t, w.Body.String(), temporalRunID)
	q.AssertExpectations(t)
}
