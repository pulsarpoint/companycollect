// Package server exposes collect and restore over HTTP for Dagster and the
// backoffice. One operation runs at a time; collects run in the background
// and are polled, restores are synchronous.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"

	"provider_recon/internal/assemble"
	"provider_recon/internal/publish"
	"provider_recon/internal/runner"
)

// Run statuses.
const (
	StatusRunning   = "running"
	StatusSucceeded = "succeeded"
	StatusFailed    = "failed"
)

const maxRuns = 50

// Run is one operation as the API reports it. RunID is the manifest run id.
type Run struct {
	RunID          string                   `json:"run_id"`
	Command        string                   `json:"command"`
	Providers      []string                 `json:"providers,omitempty"`
	Status         string                   `json:"status"`
	StartedAt      time.Time                `json:"started_at"`
	FinishedAt     *time.Time               `json:"finished_at,omitempty"`
	Changed        []string                 `json:"changed"`
	UnchangedCount int                      `json:"unchanged_count"`
	Issues         []publish.CollectorIssue `json:"issues"`
	Restored       int                      `json:"restored,omitempty"`
	Error          string                   `json:"error,omitempty"`
}

// Server holds the run registry and the single-operation lock.
type Server struct {
	cfg runner.Config
	// Now is the clock (tests pin it).
	Now func() time.Time

	mu     sync.Mutex
	active string
	runs   map[string]*Run
	order  []string
	wg     sync.WaitGroup
}

// New returns a server for cfg. There is no authentication (owner decision,
// 2026-09-28); the deployment listens only on the Tailscale address.
func New(cfg runner.Config) *Server {
	return &Server{cfg: cfg, Now: time.Now, runs: map[string]*Run{}}
}

// Wait blocks until background collects have finished.
func (s *Server) Wait() { s.wg.Wait() }

// Handler routes the API.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	})
	mux.HandleFunc("POST /v1/collect", s.collect)
	mux.HandleFunc("GET /v1/runs/{id}", s.getRun)
	mux.HandleFunc("POST /v1/restore", s.restore)
	return mux
}

// begin takes the lock and registers a running operation with a run id no
// earlier run has used. It returns nil and the active id when busy.
func (s *Server) begin(command string, providers []string) (*Run, time.Time, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active != "" {
		return nil, time.Time{}, s.active
	}
	now := s.Now().UTC().Truncate(time.Second)
	for {
		if _, taken := s.runs[publish.RunID(now, command)]; !taken {
			break
		}
		now = now.Add(time.Second)
	}
	run := &Run{RunID: publish.RunID(now, command), Command: command, Providers: providers,
		Status: StatusRunning, StartedAt: now, Changed: []string{}, Issues: []publish.CollectorIssue{}}
	s.runs[run.RunID] = run
	s.order = append(s.order, run.RunID)
	s.active = run.RunID
	for len(s.order) > maxRuns {
		delete(s.runs, s.order[0])
		s.order = s.order[1:]
	}
	return run, now, ""
}

func (s *Server) finish(run *Run, m publish.Manifest, restored int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	done := s.Now().UTC()
	run.FinishedAt = &done
	if err != nil {
		run.Status, run.Error = StatusFailed, err.Error()
	} else {
		run.Status = StatusSucceeded
		for _, c := range m.Changed {
			run.Changed = append(run.Changed, c.Slug)
		}
		run.UnchangedCount = len(m.Unchanged)
		run.Issues = append(run.Issues, m.CollectorIssues...)
		run.Restored = restored
	}
	if s.active == run.RunID {
		s.active = ""
	}
}

func (s *Server) snapshot(run *Run) Run {
	s.mu.Lock()
	defer s.mu.Unlock()
	return *run
}

func (s *Server) collect(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Providers []string `json:"providers"`
	}
	if err := decode(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	defs, err := runner.LoadDefinitions(s.cfg.DefinitionsDir, s.cfg.Registry)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if _, err := runner.SelectProviders(defs, body.Providers); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	run, now, busy := s.begin("collect", body.Providers)
	if run == nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "an operation is already running", "run_id": busy})
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		// Detached from the request: the client polls; shutdown waits via Wait.
		m, err := runner.Collect(context.Background(), s.cfg, body.Providers, now)
		s.finish(run, m, 0, err)
	}()
	writeJSON(w, http.StatusAccepted, s.snapshot(run))
}

func (s *Server) getRun(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	run, ok := s.runs[r.PathValue("id")]
	s.mu.Unlock()
	if !ok {
		writeError(w, http.StatusNotFound, "no such run")
		return
	}
	writeJSON(w, http.StatusOK, s.snapshot(run))
}

func (s *Server) restore(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Provider     string `json:"provider"`
		Collector    string `json:"collector"`
		RemovedSince string `json:"removed_since"`
	}
	if err := decode(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if body.Provider == "" || body.Collector == "" || body.RemovedSince == "" {
		writeError(w, http.StatusBadRequest, "provider, collector and removed_since are required")
		return
	}
	run, now, busy := s.begin("restore", []string{body.Provider})
	if run == nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "an operation is already running", "run_id": busy})
		return
	}
	m, n, err := runner.Restore(r.Context(), s.cfg, body.Provider, body.Collector, body.RemovedSince, now)
	s.finish(run, m, n, err)
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, s.snapshot(run))
	case errors.Is(err, runner.ErrNoDocument), errors.Is(err, assemble.ErrUnknownCollector):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, assemble.ErrBadDate):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, assemble.ErrNothingToRestore):
		writeError(w, http.StatusUnprocessableEntity, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

// decode reads an optional JSON body strictly; an empty body is allowed.
func decode(r *http.Request, v any) error {
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil && !errors.Is(err, io.EOF) {
		return errors.New("invalid JSON body: " + err.Error())
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
