package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/pulsarpoint/corpscout/translator/internal/engine"
	"github.com/pulsarpoint/corpscout/translator/internal/orchestration"
)

type Router struct {
	startedAt       time.Time
	workflowStarter WorkflowStarter
	sources         map[string]bool
	logger          *slog.Logger
}

type WorkflowStarter interface {
	StartSourceAction(ctx context.Context, source string, action string) (orchestration.WorkflowActionResult, error)
}

func NewRouter(workflowStarter WorkflowStarter, sources []string) *Router {
	return NewRouterWithLogger(workflowStarter, sources, nil)
}

func NewRouterWithLogger(workflowStarter WorkflowStarter, sources []string, logger *slog.Logger) *Router {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	known := make(map[string]bool, len(sources))
	for _, source := range sources {
		known[source] = true
	}
	return &Router{
		startedAt:       time.Now().UTC(),
		workflowStarter: workflowStarter,
		sources:         known,
		logger:          logger.With("component", "api"),
	}
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
	start := time.Now()
	if req.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	source, action, ok := parseSourceAction(req.URL.Path)
	if !ok {
		writeError(w, http.StatusNotFound, "not found")
		return
	}

	if !r.sources[source] {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	if !isSourceAction(action) {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	if r.workflowStarter == nil {
		writeError(w, http.StatusServiceUnavailable, "workflow starter is not configured")
		return
	}

	result, err := r.workflowStarter.StartSourceAction(req.Context(), source, action)
	if err != nil {
		r.logger.Error(
			"source workflow trigger failed",
			"err", err,
			"source", source,
			"action", action,
			"duration_ms", time.Since(start).Milliseconds(),
		)
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	r.logger.Info(
		"source workflow trigger accepted",
		"source", source,
		"action", action,
		"workflow_id", result.WorkflowID,
		"run_id", result.RunID,
		"duration_ms", time.Since(start).Milliseconds(),
	)

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

func isSourceAction(action string) bool {
	switch action {
	case engine.ActionLoadAndRun, engine.ActionLoadQueue, engine.ActionRun:
		return true
	default:
		return false
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
