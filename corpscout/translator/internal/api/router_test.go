package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pulsarpoint/corpscout/translator/internal/engine"
	"github.com/pulsarpoint/corpscout/translator/internal/orchestration"
)

const validEnqueueBody = `{
	"source_lang": "no",
	"target_lang": "en",
	"source_language_name": "Norwegian",
	"target_language_name": "English",
	"items": [
		{"source_table": "companies", "source_column": "name", "source_text": "Foo AS", "source_text_hash": "123"},
		{"source_table": "companies", "source_column": "name", "source_text": "Bar AS", "source_text_hash": "456"}
	]
}`

func TestEnqueueAcceptsValidRequestAndStartsWorkflow(t *testing.T) {
	queue := &fakeQueueAPI{enqueueResult: engine.EnqueueResult{Received: 2, Inserted: 2}}
	starter := &fakeProcessStarter{result: orchestration.WorkflowActionResult{WorkflowID: "translator/process", RunID: "run-1"}}
	router := NewRouter(queue, starter)

	req := httptest.NewRequest(http.MethodPost, "/v1/queue/items", strings.NewReader(validEnqueueBody))
	resp := httptest.NewRecorder()

	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d body=%s", resp.Code, resp.Body.String())
	}

	var body map[string]any
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["received"] != float64(2) {
		t.Fatalf("expected received=2, got %#v", body)
	}
	if body["inserted"] != float64(2) {
		t.Fatalf("expected inserted=2, got %#v", body)
	}
	if body["workflow_id"] != "translator/process" {
		t.Fatalf("expected workflow_id in response, got %#v", body)
	}
	if body["run_id"] != "run-1" {
		t.Fatalf("expected run_id in response, got %#v", body)
	}
	if starter.calls != 1 {
		t.Fatalf("expected starter called once, got %d", starter.calls)
	}
}

func TestEnqueueSkipsWorkflowStartWhenNothingInserted(t *testing.T) {
	queue := &fakeQueueAPI{enqueueResult: engine.EnqueueResult{Received: 2, Inserted: 0}}
	starter := &fakeProcessStarter{result: orchestration.WorkflowActionResult{WorkflowID: "translator/process", RunID: "run-1"}}
	router := NewRouter(queue, starter)

	req := httptest.NewRequest(http.MethodPost, "/v1/queue/items", strings.NewReader(validEnqueueBody))
	resp := httptest.NewRecorder()

	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d body=%s", resp.Code, resp.Body.String())
	}

	var body map[string]any
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if _, ok := body["workflow_id"]; ok {
		t.Fatalf("expected no workflow_id key, got %#v", body)
	}
	if starter.calls != 0 {
		t.Fatalf("expected starter not called, got %d calls", starter.calls)
	}
}

func TestEnqueueRejectsInvalidRequest(t *testing.T) {
	queue := &fakeQueueAPI{}
	starter := &fakeProcessStarter{}
	router := NewRouter(queue, starter)

	body := `{
		"target_lang": "en",
		"source_language_name": "Norwegian",
		"target_language_name": "English",
		"items": [
			{"source_table": "companies", "source_column": "name", "source_text": "Foo AS", "source_text_hash": "123"}
		]
	}`
	req := httptest.NewRequest(http.MethodPost, "/v1/queue/items", strings.NewReader(body))
	resp := httptest.NewRecorder()

	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d body=%s", resp.Code, resp.Body.String())
	}
	if !strings.Contains(resp.Body.String(), "source_lang is required") {
		t.Fatalf("expected body to contain source_lang is required, got %s", resp.Body.String())
	}
	if starter.calls != 0 {
		t.Fatalf("expected starter not called, got %d calls", starter.calls)
	}
}

func TestEnqueueReportsWarningWhenWorkflowStartFails(t *testing.T) {
	queue := &fakeQueueAPI{enqueueResult: engine.EnqueueResult{Received: 2, Inserted: 2}}
	starter := &fakeProcessStarter{err: errors.New("temporal unavailable")}
	router := NewRouter(queue, starter)

	req := httptest.NewRequest(http.MethodPost, "/v1/queue/items", strings.NewReader(validEnqueueBody))
	resp := httptest.NewRecorder()

	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d body=%s", resp.Code, resp.Body.String())
	}

	var body map[string]any
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if _, ok := body["warning"]; !ok {
		t.Fatalf("expected warning key in response, got %#v", body)
	}
}

func TestStatsReturnsCounts(t *testing.T) {
	queue := &fakeQueueAPI{statsResult: engine.QueueStats{Input: 5, Pending: 3, Output: 1, Failed: 1}}
	router := NewRouter(queue, &fakeProcessStarter{})

	req := httptest.NewRequest(http.MethodGet, "/v1/queue/stats", nil)
	resp := httptest.NewRecorder()

	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d body=%s", resp.Code, resp.Body.String())
	}

	var body map[string]any
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	want := map[string]any{"input": float64(5), "pending": float64(3), "output": float64(1), "failed": float64(1)}
	for key, value := range want {
		if body[key] != value {
			t.Fatalf("expected %s=%v, got %#v", key, value, body)
		}
	}
}

func TestProcessTriggersWorkflow(t *testing.T) {
	starter := &fakeProcessStarter{result: orchestration.WorkflowActionResult{WorkflowID: "translator/process", RunID: "run-1"}}
	router := NewRouter(&fakeQueueAPI{}, starter)

	req := httptest.NewRequest(http.MethodPost, "/v1/queue/process", nil)
	resp := httptest.NewRecorder()

	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d body=%s", resp.Code, resp.Body.String())
	}

	var body map[string]any
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["workflow_id"] != "translator/process" {
		t.Fatalf("expected workflow_id in response, got %#v", body)
	}
	if starter.calls != 1 {
		t.Fatalf("expected starter called once, got %d", starter.calls)
	}
}

func TestOldSourceRoutesAreGone(t *testing.T) {
	router := NewRouter(&fakeQueueAPI{}, &fakeProcessStarter{})

	req := httptest.NewRequest(http.MethodPost, "/v1/sources/norway_brreg/run", nil)
	resp := httptest.NewRecorder()

	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d body=%s", resp.Code, resp.Body.String())
	}
}

type fakeQueueAPI struct {
	enqueueRequest engine.EnqueueRequest
	enqueueResult  engine.EnqueueResult
	enqueueErr     error
	statsResult    engine.QueueStats
	statsErr       error
}

func (f *fakeQueueAPI) Enqueue(ctx context.Context, req engine.EnqueueRequest) (engine.EnqueueResult, error) {
	f.enqueueRequest = req
	return f.enqueueResult, f.enqueueErr
}

func (f *fakeQueueAPI) Stats(ctx context.Context) (engine.QueueStats, error) {
	return f.statsResult, f.statsErr
}

type fakeProcessStarter struct {
	calls  int
	result orchestration.WorkflowActionResult
	err    error
}

func (f *fakeProcessStarter) StartProcess(ctx context.Context) (orchestration.WorkflowActionResult, error) {
	f.calls++
	return f.result, f.err
}
