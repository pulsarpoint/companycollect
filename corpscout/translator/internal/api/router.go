package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

type Router struct {
	startedAt       time.Time
	workflowStarter WorkflowStarter
}

type WorkflowStarter interface {
	StartSourceAction(ctx context.Context, source string, action string) (WorkflowActionResult, error)
}

type WorkflowActionResult struct {
	WorkflowID string
	RunID      string
}

func NewRouter(workflowStarter WorkflowStarter) *Router {
	return &Router{startedAt: time.Now().UTC(), workflowStarter: workflowStarter}
}

func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch {
	case req.URL.Path == "/healthz":
		r.healthz(w, req)
	case strings.HasPrefix(req.URL.Path, "/v1/sources/"):
		r.sourceAction(w, req)
	default:
		writeError(w, http.StatusNotFound, "not found")
	}
}

func (r *Router) healthz(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"service":    "translator",
		"status":     "ok",
		"started_at": r.startedAt,
	})
}

func (r *Router) sourceAction(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	source, action, ok := parseSourceAction(req.URL.Path)
	if !ok {
		writeError(w, http.StatusNotFound, "not found")
		return
	}

	if source != "norway_brreg" {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	if action != "load-queue" && action != "run" {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	if r.workflowStarter == nil {
		writeError(w, http.StatusServiceUnavailable, "workflow starter is not configured")
		return
	}

	result, err := r.workflowStarter.StartSourceAction(req.Context(), source, action)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"source":      source,
		"action":      action,
		"workflow_id": result.WorkflowID,
		"run_id":      result.RunID,
		"status":      "accepted",
		"accepted_at": time.Now().UTC(),
	})
}

func parseSourceAction(path string) (string, string, bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 4 {
		return "", "", false
	}
	if parts[0] != "v1" || parts[1] != "sources" {
		return "", "", false
	}
	if parts[2] == "" || parts[3] == "" {
		return "", "", false
	}
	return parts[2], parts[3], true
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
