package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/pulsarpoint/corpscout/translator/internal/engine"
	"github.com/pulsarpoint/corpscout/translator/internal/orchestration"
)

// QueueAPI is the queue surface the router exposes; *engine.Runtime satisfies it.
type QueueAPI interface {
	Enqueue(ctx context.Context, req engine.EnqueueRequest) (engine.EnqueueResult, error)
	Stats(ctx context.Context) (engine.QueueStats, error)
}

// ProcessStarter starts (or wakes) the translation workflow.
type ProcessStarter interface {
	StartProcess(ctx context.Context) (orchestration.WorkflowActionResult, error)
}

type Router struct {
	startedAt time.Time
	queue     QueueAPI
	starter   ProcessStarter
	logger    *slog.Logger
}

func NewRouter(queue QueueAPI, starter ProcessStarter) *Router {
	return NewRouterWithLogger(queue, starter, nil)
}

func NewRouterWithLogger(queue QueueAPI, starter ProcessStarter, logger *slog.Logger) *Router {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Router{
		startedAt: time.Now().UTC(),
		queue:     queue,
		starter:   starter,
		logger:    logger.With("component", "api"),
	}
}

func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch {
	case req.URL.Path == "/healthz":
		r.healthz(w, req)
	case req.URL.Path == "/v1/queue/items":
		r.enqueue(w, req)
	case req.URL.Path == "/v1/queue/stats":
		r.stats(w, req)
	case req.URL.Path == "/v1/queue/process":
		r.process(w, req)
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

func (r *Router) enqueue(w http.ResponseWriter, req *http.Request) {
	start := time.Now()
	if req.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if r.queue == nil {
		writeError(w, http.StatusServiceUnavailable, "queue is not configured")
		return
	}

	var enqueueRequest engine.EnqueueRequest
	decoder := json.NewDecoder(req.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&enqueueRequest); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if err := enqueueRequest.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	result, err := r.queue.Enqueue(req.Context(), enqueueRequest)
	if err != nil {
		r.logger.Error("enqueue failed", "err", err, "received", len(enqueueRequest.Items), "duration_ms", time.Since(start).Milliseconds())
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	response := map[string]any{
		"received": result.Received,
		"inserted": result.Inserted,
	}
	if r.starter != nil {
		workflowResult, err := r.starter.StartProcess(req.Context())
		if err != nil {
			r.logger.Error("enqueue accepted but workflow start failed", "err", err)
			response["warning"] = "items queued but workflow start failed: " + err.Error()
		} else {
			response["workflow_id"] = workflowResult.WorkflowID
			response["run_id"] = workflowResult.RunID
		}
	}
	r.logger.Info(
		"enqueue accepted",
		"received", result.Received,
		"inserted", result.Inserted,
		"source_lang", enqueueRequest.SourceLang,
		"target_lang", enqueueRequest.TargetLang,
		"duration_ms", time.Since(start).Milliseconds(),
	)
	writeJSON(w, http.StatusAccepted, response)
}

func (r *Router) stats(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if r.queue == nil {
		writeError(w, http.StatusServiceUnavailable, "queue is not configured")
		return
	}
	stats, err := r.queue.Stats(req.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

func (r *Router) process(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if r.starter == nil {
		writeError(w, http.StatusServiceUnavailable, "workflow starter is not configured")
		return
	}
	result, err := r.starter.StartProcess(req.Context())
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"workflow_id": result.WorkflowID,
		"run_id":      result.RunID,
		"status":      "accepted",
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
