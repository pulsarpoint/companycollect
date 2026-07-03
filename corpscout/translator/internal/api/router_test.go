package api

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pulsarpoint/corpscout/translator/internal/engine"
	"github.com/pulsarpoint/corpscout/translator/internal/orchestration"
)

func TestSourceActionStartsWorkflow(t *testing.T) {
	starter := &fakeWorkflowStarter{
		result: orchestration.WorkflowActionResult{
			WorkflowID: "translator/norway_brreg",
			RunID:      "run-1",
		},
	}
	router := NewRouter(starter, []string{"norway_brreg"})

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

func TestSourceActionStartsLoadAndRunWorkflow(t *testing.T) {
	starter := &fakeWorkflowStarter{
		result: orchestration.WorkflowActionResult{
			WorkflowID: "translator/norway_brreg",
			RunID:      "run-1",
		},
	}
	router := NewRouter(starter, []string{"norway_brreg"})

	req := httptest.NewRequest(http.MethodPost, "/v1/sources/norway_brreg/load-and-run", nil)
	resp := httptest.NewRecorder()

	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d body=%s", resp.Code, resp.Body.String())
	}
	if starter.source != "norway_brreg" || starter.action != engine.ActionLoadAndRun {
		t.Fatalf("expected starter call %s/%s, got %s/%s", "norway_brreg", engine.ActionLoadAndRun, starter.source, starter.action)
	}
}

func TestSourceActionRejectsUnknownSource(t *testing.T) {
	router := NewRouter(&fakeWorkflowStarter{}, []string{"norway_brreg"})

	req := httptest.NewRequest(http.MethodPost, "/v1/sources/unknown/run", nil)
	resp := httptest.NewRecorder()

	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d body=%s", resp.Code, resp.Body.String())
	}
}

func TestSourceActionLogsAcceptedWorkflowTrigger(t *testing.T) {
	var logs bytes.Buffer
	router := NewRouterWithLogger(&fakeWorkflowStarter{
		result: orchestration.WorkflowActionResult{
			WorkflowID: "translator/norway_brreg",
			RunID:      "run-1",
		},
	}, []string{"norway_brreg"}, slog.New(slog.NewJSONHandler(&logs, nil)))

	req := httptest.NewRequest(http.MethodPost, "/v1/sources/norway_brreg/run", nil)
	resp := httptest.NewRecorder()

	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d body=%s", resp.Code, resp.Body.String())
	}
	logText := logs.String()
	required := []string{
		`"msg":"source workflow trigger accepted"`,
		`"source":"norway_brreg"`,
		`"action":"run"`,
		`"workflow_id":"translator/norway_brreg"`,
		`"run_id":"run-1"`,
	}
	for _, fragment := range required {
		if !strings.Contains(logText, fragment) {
			t.Fatalf("expected router logs to contain %s\nlogs:\n%s", fragment, logText)
		}
	}
}

func TestSourceActionAcceptsAnyConfiguredSource(t *testing.T) {
	starter := &fakeWorkflowStarter{
		result: orchestration.WorkflowActionResult{
			WorkflowID: "translator/sweden_scb",
			RunID:      "run-2",
		},
	}
	router := NewRouter(starter, []string{"norway_brreg", "sweden_scb"})

	req := httptest.NewRequest(http.MethodPost, "/v1/sources/sweden_scb/run", nil)
	resp := httptest.NewRecorder()

	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d body=%s", resp.Code, resp.Body.String())
	}
	if starter.source != "sweden_scb" {
		t.Fatalf("expected starter call for sweden_scb, got %s", starter.source)
	}
}

type fakeWorkflowStarter struct {
	source string
	action string
	result orchestration.WorkflowActionResult
	err    error
}

func (f *fakeWorkflowStarter) StartSourceAction(
	ctx context.Context,
	source string,
	action string,
) (orchestration.WorkflowActionResult, error) {
	f.source = source
	f.action = action
	return f.result, f.err
}
