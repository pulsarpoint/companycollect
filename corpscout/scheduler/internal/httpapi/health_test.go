package httpapi_test

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/pulsarpoint/corpscout/scheduler/internal/httpapi"
)

func TestHandleHealth(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	httpapi.HandleHealth(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}
	body := w.Body.String()
	if body == "" {
		t.Fatal("expected non-empty body")
	}
}

func TestHandleHealthUsesSharedJSONResponse(t *testing.T) {
	source, err := os.ReadFile("health.go")
	if err != nil {
		t.Fatal(err)
	}
	body := string(source)

	if strings.Contains(body, "json.NewEncoder") {
		t.Fatal("health handler should use shared writeJSON helper")
	}
	if strings.Contains(body, "map[string]string") {
		t.Fatal("health response should use a named response type")
	}
	if !strings.Contains(body, "type healthResponse struct") {
		t.Fatal("health response type should be named")
	}
	if !strings.Contains(body, "writeJSON(w, http.StatusOK") {
		t.Fatal("health handler should write through shared JSON helper")
	}
}

func TestWriteErrorUsesNamedResponse(t *testing.T) {
	source, err := os.ReadFile("handlers.go")
	if err != nil {
		t.Fatal(err)
	}
	body := string(source)

	if strings.Contains(body, `map[string]string{"error": msg}`) {
		t.Fatal("writeError should use a named response type")
	}
	if !strings.Contains(body, "type errorResponse struct") {
		t.Fatal("error response type should be named")
	}
	if !strings.Contains(body, "writeJSON(w, status, errorResponse{Error: msg})") {
		t.Fatal("writeError should write the named error response")
	}
}
