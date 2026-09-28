// Package server is the dns-detect HTTP service: a stateless wrapper around
// the per-record resolver. It keeps the compiled knowledge in memory, reloads
// it when provider-recon publishes, and never reads ClickHouse: callers send
// batches of records and get the results back in the same response.
package server

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"provider_recon/internal/detect/knowledge"
	"provider_recon/internal/detect/resolve"
	"provider_recon/internal/publish"
)

// MaxRecords is the largest batch POST /v1/resolve accepts.
const MaxRecords = 50_000

// maxBody bounds a request body (records are a few hundred bytes each).
const maxBody = 256 << 20

type state struct {
	kb       *knowledge.Index
	digest   string
	loadedAt time.Time
}

// Server serves the resolver over HTTP.
type Server struct {
	store publish.Store
	log   *slog.Logger
	now   func() time.Time

	loaded   atomic.Pointer[state]
	reloadMu sync.Mutex // one reload at a time

	errMu     sync.Mutex
	reloadErr string
	errAt     time.Time
}

// New builds a server reading provider documents from store. Call Reload
// before serving.
func New(store publish.Store, log *slog.Logger) *Server {
	return &Server{store: store, log: log, now: time.Now}
}

func (s *Server) current() *state { return s.loaded.Load() }

// Reload loads the published documents and swaps the knowledge in when the
// run index changed since the last load. A failure keeps the current
// knowledge and is reported by /healthz.
func (s *Server) Reload(ctx context.Context) error {
	s.reloadMu.Lock()
	defer s.reloadMu.Unlock()
	kb, digest, err := knowledge.LoadStore(ctx, s.store)
	s.errMu.Lock()
	defer s.errMu.Unlock()
	if err != nil {
		s.reloadErr, s.errAt = err.Error(), s.now().UTC()
		return err
	}
	s.reloadErr, s.errAt = "", time.Time{}
	if cur := s.current(); cur != nil && cur.digest == digest {
		return nil
	}
	s.loaded.Store(&state{kb: kb, digest: digest, loadedAt: s.now().UTC()})
	s.log.Info("knowledge loaded", "documents", kb.Documents(), "rules_version", kb.RulesVersion(), "ip_version", kb.IPVersion())
	return nil
}

// Run reloads every interval until ctx ends, logging failures.
func (s *Server) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := s.Reload(ctx); err != nil {
				s.log.Warn("knowledge reload failed; keeping the loaded knowledge", "error", err)
			}
		}
	}
}

type health struct {
	OK            bool   `json:"ok"`
	RulesVersion  string `json:"rules_version,omitempty"`
	IPVersion     string `json:"ip_version,omitempty"`
	LoadedAt      string `json:"loaded_at,omitempty"`
	ReloadError   string `json:"reload_error,omitempty"`
	ReloadErrorAt string `json:"reload_error_at,omitempty"`
}

func (s *Server) health() health {
	var h health
	if cur := s.current(); cur != nil {
		h.OK, h.RulesVersion, h.IPVersion, h.LoadedAt = true, cur.kb.RulesVersion(), cur.kb.IPVersion(), cur.loadedAt.Format(time.RFC3339)
	}
	s.errMu.Lock()
	defer s.errMu.Unlock()
	if s.reloadErr != "" {
		h.ReloadError, h.ReloadErrorAt = s.reloadErr, s.errAt.Format(time.RFC3339)
	}
	return h
}

// Handler routes the service's endpoints.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /v1/knowledge", s.handleKnowledge)
	mux.HandleFunc("POST /v1/resolve", s.handleResolve)
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	h := s.health()
	code := http.StatusOK
	if !h.OK {
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, h)
}

func (s *Server) handleKnowledge(w http.ResponseWriter, _ *http.Request) {
	cur := s.current()
	if cur == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "knowledge not loaded"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"rules_version": cur.kb.RulesVersion(), "ip_version": cur.kb.IPVersion(), "version": cur.kb.Version(),
		"documents": cur.kb.Documents(), "loaded_at": cur.loadedAt.Format(time.RFC3339),
	})
}

var errTooMany = errors.New("too many records")

// handleResolve reads the whole batch first, so a bad record answers 400
// without any partial output; results come back in input order.
func (s *Server) handleResolve(w http.ResponseWriter, r *http.Request) {
	cur := s.current()
	if cur == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "knowledge not loaded"})
		return
	}
	records, err := readRecords(http.MaxBytesReader(w, r.Body, maxBody))
	switch {
	case errors.Is(err, errTooMany):
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": fmt.Sprintf("at most %d records per request", MaxRecords)})
		return
	case err != nil:
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("X-Rules-Version", cur.kb.RulesVersion())
	w.Header().Set("X-IP-Version", cur.kb.IPVersion())
	out := bufio.NewWriter(w)
	enc := json.NewEncoder(out)
	for _, rec := range records {
		if err := enc.Encode(resolve.Resolve(rec, cur.kb)); err != nil {
			s.log.Warn("writing a resolve response failed", "error", err)
			return
		}
	}
	if err := out.Flush(); err != nil {
		s.log.Warn("writing a resolve response failed", "error", err)
	}
}

func readRecords(body io.Reader) ([]resolve.Record, error) {
	dec := json.NewDecoder(bufio.NewReader(body))
	var records []resolve.Record
	for n := 1; ; n++ {
		var rec resolve.Record
		if err := dec.Decode(&rec); errors.Is(err, io.EOF) {
			return records, nil
		} else if err != nil {
			return nil, fmt.Errorf("record %d: %w", n, err)
		}
		if n > MaxRecords {
			return nil, errTooMany
		}
		if err := rec.Validate(); err != nil {
			return nil, fmt.Errorf("record %d: %w", n, err)
		}
		records = append(records, rec)
	}
}
