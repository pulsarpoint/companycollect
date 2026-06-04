package httpapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/httpapi"
)

func TestBrregTaskStateEndpointUsesTaskStateNamingInternally(t *testing.T) {
	_, err := os.Stat("source_workflow.go")
	require.True(t, os.IsNotExist(err), "BRREG task-state endpoint should not live in source_workflow.go")

	source, err := os.ReadFile("brreg_task_state.go")
	require.NoError(t, err)
	handlers, err := os.ReadFile("handlers.go")
	require.NoError(t, err)

	require.Contains(t, string(source), "brregTaskStateResponse")
	require.Contains(t, string(source), "handleGetBrregTaskState")
	require.Contains(t, string(handlers), "h.handleGetBrregTaskState")
	require.Contains(t, string(handlers), `"/sources/brreg/task-state"`)
	require.NotContains(t, string(handlers), `"/sources/brreg/workflow"`)
	require.NotContains(t, string(source), "brregWorkflowResponse")
	require.NotContains(t, string(source), "handleGetBrregWorkflow")
}

func TestGetBrregTaskStateReturnsTaskActionsAndResultState(t *testing.T) {
	q := &stubQuerier{}
	q.On("GetBrregSourceTranslationAssetState", mock.Anything).Return(db.GetBrregSourceTranslationAssetStateRow{
		Asset:               "mv_company_translation_status",
		RawRecordsCurrent:   1000,
		TaskPending:         20,
		TaskRunningActive:   3,
		TaskFailedRetryable: 4,
		TaskFailedTerminal:  5,
		TaskSucceeded:       800,
		TaskSkipped:         68,
		TaskEligibleNow:     27,
		ArtifactSucceeded:   800,
		ArtifactSkipped:     68,
		ArtifactFailed:      5,
		ArtifactMissing:     127,
	}, nil)
	q.On("GetBrregSourceDomainAssetState", mock.Anything).Return(db.GetBrregSourceDomainAssetStateRow{
		Asset:             "brreg_source.domains",
		RawRecordsCurrent: 1000,
		TaskNoState:       900,
		TaskSucceeded:     100,
		TaskEligibleNow:   900,
		ArtifactSucceeded: 100,
		ArtifactMissing:   900,
	}, nil)
	q.On("GetBrregSourceFinancialAssetState", mock.Anything).Return(db.GetBrregSourceFinancialAssetStateRow{
		Asset:               "brreg_source.financial_statements",
		RawRecordsCurrent:   1000,
		TaskPending:         900,
		TaskRunningActive:   1,
		TaskFailedRetryable: 2,
		TaskSucceeded:       90,
		TaskSkipped:         7,
		TaskEligibleNow:     902,
		ArtifactSucceeded:   90,
		ArtifactSkipped:     7,
		ArtifactFailed:      2,
		ArtifactMissing:     901,
	}, nil)
	q.On("GetBrregSourceResultTableCounts", mock.Anything).Return(db.GetBrregSourceResultTableCountsRow{
		Companies:           1000,
		TranslationStatus:   1000,
		Websites:            120,
		Domains:             100,
		FinancialStatements: 90,
	}, nil)
	r := routerFor(httpapi.NewHandlers(q, nil, nil, nil, "", nil, ""))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/sources/brreg/task-state", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)

	var body struct {
		Source  string `json:"source"`
		Actions []struct {
			Key         string `json:"key"`
			Label       string `json:"label"`
			Description string `json:"description"`
			Asset       string `json:"asset"`
			State       struct {
				ArtifactSucceeded   int64 `json:"artifact_succeeded"`
				ArtifactMissing     int64 `json:"artifact_missing"`
				TaskRunningActive   int64 `json:"task_running_active"`
				TaskFailedRetryable int64 `json:"task_failed_retryable"`
				TaskFailedTerminal  int64 `json:"task_failed_terminal"`
				TaskEligibleNow     int64 `json:"task_eligible_now"`
			} `json:"state"`
		} `json:"actions"`
		ResultTables []struct {
			Name  string `json:"name"`
			Count int64  `json:"count"`
		} `json:"result_tables"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	var raw struct {
		Actions []map[string]any `json:"actions"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &raw))

	require.Equal(t, "brreg", body.Source)
	require.Len(t, body.Actions, 4)
	for _, action := range raw.Actions {
		require.NotContains(t, action, "action_url")
	}
	require.Equal(t, "ingest_raw", body.Actions[0].Key)
	require.Equal(t, "translate", body.Actions[1].Key)
	require.Contains(t, body.Actions[1].Description, "source tables")
	require.NotContains(t, body.Actions[1].Description, "workflow artifacts")
	require.Equal(t, int64(800), body.Actions[1].State.ArtifactSucceeded)
	require.Equal(t, int64(127), body.Actions[1].State.ArtifactMissing)
	require.Equal(t, int64(3), body.Actions[1].State.TaskRunningActive)
	require.Equal(t, int64(4), body.Actions[1].State.TaskFailedRetryable)
	require.Equal(t, int64(5), body.Actions[1].State.TaskFailedTerminal)
	require.Equal(t, int64(27), body.Actions[1].State.TaskEligibleNow)
	require.Equal(t, "discover_domains", body.Actions[2].Key)
	require.Equal(t, "brreg_source.domains", body.Actions[2].Asset)
	require.Equal(t, int64(100), body.Actions[2].State.ArtifactSucceeded)
	require.Equal(t, int64(900), body.Actions[2].State.ArtifactMissing)
	require.Equal(t, "convert_financials", body.Actions[3].Key)
	require.Equal(t, "brreg_source.financial_statements", body.Actions[3].Asset)
	require.Equal(t, int64(90), body.Actions[3].State.ArtifactSucceeded)
	require.Equal(t, int64(901), body.Actions[3].State.ArtifactMissing)
	for _, action := range body.Actions {
		require.NotEqual(t, "build_enhanced", action.Key)
	}

	require.ElementsMatch(t, []struct {
		Name  string `json:"name"`
		Count int64  `json:"count"`
	}{
		{Name: "brreg_workflow.raw_records", Count: 1000},
		{Name: "brreg_source.companies", Count: 1000},
		{Name: "brreg_source.mv_company_translation_status", Count: 1000},
		{Name: "brreg_source.websites", Count: 120},
		{Name: "brreg_source.domains", Count: 100},
		{Name: "brreg_source.financial_statements", Count: 90},
	}, body.ResultTables)
}

func TestGetBrregTaskStateDoesNotExposeWorkflowRoute(t *testing.T) {
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", nil, ""))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/sources/brreg/workflow", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusNotFound, w.Code)
}
