package httpapi_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/corpscout/scheduler/internal/httpapi"
)

func TestCancelJobRejectsPartiallyNumericID(t *testing.T) {
	rv := &fakeRiverCanceller{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, rv, nil, nil, "", nil, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/123abc/cancel", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Zero(t, rv.cancelCalls)
}

func TestJobCancelResponsesUseNamedTypes(t *testing.T) {
	source, err := os.ReadFile("jobs.go")
	require.NoError(t, err)
	body := string(source)

	require.NotContains(t, body, `map[string]any{"status": "cancelled", "id": id}`)
	require.NotContains(t, body, `map[string]any{"cancelled": cancelled}`)
	require.Contains(t, body, "type cancelJobResponse struct")
	require.Contains(t, body, "type cancelBulkResponse struct")
	require.Contains(t, body, `Status: "cancelled"`)
	require.True(t, strings.Contains(body, "cancelJobResponse{") && strings.Contains(body, "cancelBulkResponse{"))
}

type fakeRiverCanceller struct {
	cancelCalls int
}

func (f *fakeRiverCanceller) Insert(context.Context, river.JobArgs, *river.InsertOpts) (*rivertype.JobInsertResult, error) {
	return nil, nil
}

func (f *fakeRiverCanceller) JobCancel(context.Context, int64) (*rivertype.JobRow, error) {
	f.cancelCalls++
	return &rivertype.JobRow{}, nil
}
