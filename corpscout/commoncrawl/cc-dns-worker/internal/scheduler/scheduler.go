// Package scheduler paces outbound work per target server IP. Every DNS query passes through Do(),
// which grants a token from that server's bucket and a per-server in-flight slot before running fn.
// Single-process, in-memory; see the spec's shard-by-server note for the distributed path.
package scheduler

import (
	"context"
	"sync"

	"golang.org/x/time/rate"
)

// Config holds the per-server pacing knobs.
type Config struct {
	PerServerQPS float64
	Burst        int
	MaxInFlight  int
}

// Scheduler owns one limiter + one in-flight semaphore per server IP, created lazily.
type Scheduler struct {
	cfg  Config
	mu   sync.Mutex
	lims map[string]*server
}

type server struct {
	lim  *rate.Limiter
	slot chan struct{}
}

// New returns a Scheduler; zero/negative knobs fall back to safe defaults.
func New(cfg Config) *Scheduler {
	if cfg.PerServerQPS <= 0 {
		cfg.PerServerQPS = 10
	}
	if cfg.Burst <= 0 {
		cfg.Burst = 10
	}
	if cfg.MaxInFlight <= 0 {
		cfg.MaxInFlight = 3
	}
	return &Scheduler{cfg: cfg, lims: make(map[string]*server)}
}

func (s *Scheduler) forServer(ip string) *server {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sv, ok := s.lims[ip]; ok {
		return sv
	}
	sv := &server{
		lim:  rate.NewLimiter(rate.Limit(s.cfg.PerServerQPS), s.cfg.Burst),
		slot: make(chan struct{}, s.cfg.MaxInFlight),
	}
	s.lims[ip] = sv
	return sv
}

// Do waits for a token and an in-flight slot for serverIP, then runs fn.
func (s *Scheduler) Do(ctx context.Context, serverIP string, fn func() error) error {
	sv := s.forServer(serverIP)
	select {
	case sv.slot <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-sv.slot }()
	if err := sv.lim.Wait(ctx); err != nil {
		return err
	}
	return fn()
}
