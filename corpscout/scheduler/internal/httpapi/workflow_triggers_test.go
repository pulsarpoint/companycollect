package httpapi_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	brregworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/workflow"
	"github.com/pulsarpoint/corpscout/scheduler/internal/httpapi"
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
		"trigger": "manual"
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
	require.Equal(t, 50, input.BatchSize)
	require.Equal(t, 3, input.MaxAttempts)
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
