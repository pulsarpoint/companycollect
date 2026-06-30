package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSourceActionStartsWorkflow(t *testing.T) {
	starter := &fakeWorkflowStarter{
		result: WorkflowActionResult{
			WorkflowID: "translator/norway_brreg",
			RunID:      "run-1",
		},
	}
	router := NewRouter(starter)

	req := httptest.NewRequest(http.MethodPost, "/v1/sources/norway_brreg/run", nil)
	resp := httptest.NewRecorder()

	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d body=%s", resp.Code, resp.Body.String())
	}
	if starter.source != "norway_brreg" || starter.action != "run" {
		t.Fatalf("expected starter call norway_brreg/run, got %s/%s", starter.source, starter.action)
	}

	var body map[string]any
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["workflow_id"] != "translator/norway_brreg" {
		t.Fatalf("expected workflow id response, got %#v", body)
	}
	if body["run_id"] != "run-1" {
		t.Fatalf("expected run id response, got %#v", body)
	}
	if body["status"] != "accepted" {
		t.Fatalf("expected accepted response, got %#v", body)
	}
}

func TestSourceActionRejectsUnknownSource(t *testing.T) {
	router := NewRouter(&fakeWorkflowStarter{})

	req := httptest.NewRequest(http.MethodPost, "/v1/sources/unknown/run", nil)
	resp := httptest.NewRecorder()

	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d body=%s", resp.Code, resp.Body.String())
	}
}

type fakeWorkflowStarter struct {
	source string
	action string
	result WorkflowActionResult
	err    error
}

func (f *fakeWorkflowStarter) StartSourceAction(
	ctx context.Context,
	source string,
	action string,
) (WorkflowActionResult, error) {
	f.source = source
	f.action = action
	return f.result, f.err
}
