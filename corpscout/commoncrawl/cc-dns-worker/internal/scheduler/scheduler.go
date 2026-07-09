// Package scheduler paces outbound work per target server IP. Every DNS query passes through Do(),
// which grants a token from that server's bucket and a per-server in-flight slot before running fn.
// Do() also drives a per-server circuit breaker: after BreakerThreshold consecutive transport
// failures (fn returning an error) to one IP, that IP's circuit opens and Do() fast-fails with
// ErrCircuitOpen for BreakerCooldown, so a dead server stops wasting the query timeout for every
// domain that shares it. Single-process, in-memory; see the spec's shard-by-server note for the
// distributed path.
package scheduler

import (
	"context"
	"errors"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// ErrCircuitOpen is returned by Do when the target server IP's circuit is open. Callers treat it
// like any other error and rotate to the next server.
var ErrCircuitOpen = errors.New("scheduler: circuit open")

// Config holds the per-server pacing and circuit-breaker knobs.
type Config struct {
	PerServerQPS float64
	Burst        int
	MaxInFlight  int
	// HyperscalerQPS / HyperscalerInFlight override PerServerQPS / MaxInFlight for server IPs in a
	// known large anycast DNS provider range (see providers.go). <= 0 disables the override (every
	// server paced at the default). These providers safely absorb far more than a small nameserver,
	// so this stops Cloudflare/Google/Route53 from being the throughput long-pole.
	HyperscalerQPS      float64
	HyperscalerInFlight int
	// BreakerThreshold is the number of CONSECUTIVE transport failures (fn errors) to one server IP
	// that opens its circuit. <= 0 disables the breaker (Do never fast-fails).
	BreakerThreshold int
	// BreakerCooldown is how long a circuit stays open before the next call is allowed through as a
	// half-open probe.
	BreakerCooldown time.Duration
}

// Scheduler owns one limiter + in-flight semaphore + breaker per server IP, created lazily.
type Scheduler struct {
	cfg  Config
	now  func() time.Time // injectable clock for deterministic breaker tests
	mu   sync.Mutex
	lims map[string]*server
}

type server struct {
	lim  *rate.Limiter
	slot chan struct{}

	bmu       sync.Mutex // guards the breaker fields below
	fails     int        // consecutive fn errors since the last success
	openUntil time.Time  // zero = closed; a future time = open
}

// New returns a Scheduler; zero/negative pacing knobs fall back to safe defaults. BreakerThreshold
// <= 0 leaves the breaker off.
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
	return &Scheduler{cfg: cfg, now: time.Now, lims: make(map[string]*server)}
}

func (s *Scheduler) forServer(ip string) *server {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sv, ok := s.lims[ip]; ok {
		return sv
	}
	qps, burst, inflight := s.cfg.PerServerQPS, s.cfg.Burst, s.cfg.MaxInFlight
	if s.cfg.HyperscalerQPS > 0 && IsHyperscaler(ip) {
		qps = s.cfg.HyperscalerQPS
		if s.cfg.HyperscalerInFlight > 0 {
			inflight = s.cfg.HyperscalerInFlight
		}
		if int(qps) > burst { // let the elevated rate actually burst
			burst = int(qps)
		}
	}
	sv := &server{
		lim:  rate.NewLimiter(rate.Limit(qps), burst),
		slot: make(chan struct{}, inflight),
	}
	s.lims[ip] = sv
	return sv
}

// allow reports whether a request may proceed: true when the circuit is closed or the cooldown has
// elapsed (half-open); false while open.
func (sv *server) allow(now time.Time) bool {
	sv.bmu.Lock()
	defer sv.bmu.Unlock()
	return sv.openUntil.IsZero() || !now.Before(sv.openUntil)
}

// record folds one outcome into the breaker: a success closes the circuit; threshold consecutive
// failures (re)open it for cooldown.
func (sv *server) record(now time.Time, ok bool, threshold int, cooldown time.Duration) {
	sv.bmu.Lock()
	defer sv.bmu.Unlock()
	if ok {
		sv.fails = 0
		sv.openUntil = time.Time{}
		return
	}
	sv.fails++
	if sv.fails >= threshold {
		sv.openUntil = now.Add(cooldown)
	}
}

// Do waits for a token and an in-flight slot for serverIP, then runs fn. If the breaker is enabled
// and serverIP's circuit is open, Do returns ErrCircuitOpen immediately — no slot, no token, no fn.
func (s *Scheduler) Do(ctx context.Context, serverIP string, fn func() error) error {
	sv := s.forServer(serverIP)
	breaker := s.cfg.BreakerThreshold > 0
	if breaker && !sv.allow(s.now()) {
		return ErrCircuitOpen
	}
	select {
	case sv.slot <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-sv.slot }()
	if err := sv.lim.Wait(ctx); err != nil {
		return err
	}
	err := fn()
	if breaker {
		sv.record(s.now(), err == nil, s.cfg.BreakerThreshold, s.cfg.BreakerCooldown)
	}
	return err
}
