package httpapi_test

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/corpscout/scheduler/internal/httpapi"
	naceworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/temporal/workflow/nace"
)

func TestStartNACEClickHouseSyncWorkflow(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))
	body := strings.NewReader(`{"trigger":"manual"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/nace/clickhouse-sync", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	require.Equal(t, naceworkflow.SyncTaskQueue, tc.options.TaskQueue)
	require.Equal(t, reflect.ValueOf(naceworkflow.SyncNACEToClickHouse).Pointer(), reflect.ValueOf(tc.workflow).Pointer())
	require.Equal(t, []interface{}{naceworkflow.SyncNACEToClickHouseInput{
		Trigger: "manual",
	}}, tc.args)
	require.Contains(t, w.Body.String(), naceworkflow.SyncToClickHouseWorkflowName)
}
