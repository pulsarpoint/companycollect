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
	"sync/atomic"
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

// BreakerMetrics is a point-in-time snapshot of aggregate circuit-breaker transition counts across
// every server IP a Scheduler paces. These are transition counts, not per-request rejection counts:
// a server IP that is fast-failing thousands of queued calls against an open circuit contributes
// nothing further to Opened until it actually reopens (e.g. after a failed probe).
type BreakerMetrics struct {
	Opened     int64 // closed -> open, or half-open (failed probe) -> open
	HalfOpened int64 // open -> half-open: a single probe admitted after cooldown
	Closed     int64 // half-open (successful probe) -> closed
}

// breakerMetrics holds the atomic counters backing BreakerMetrics. Shared by every per-server
// breaker on a Scheduler so callers get one aggregate view instead of per-request log noise.
type breakerMetrics struct {
	opened     atomic.Int64
	halfOpened atomic.Int64
	closed     atomic.Int64
}

func (m *breakerMetrics) snapshot() BreakerMetrics {
	return BreakerMetrics{
		Opened:     m.opened.Load(),
		HalfOpened: m.halfOpened.Load(),
		Closed:     m.closed.Load(),
	}
}

// Scheduler owns one limiter + in-flight semaphore + breaker per server IP, created lazily.
type Scheduler struct {
	cfg     Config
	now     func() time.Time // injectable clock for deterministic breaker tests
	mu      sync.Mutex
	lims    map[string]*server
	metrics breakerMetrics
}

type server struct {
	lim  *rate.Limiter
	slot chan struct{}

	bmu       sync.Mutex // guards the breaker fields below
	fails     int        // consecutive fn errors since the last success (closed-state counting)
	openUntil time.Time  // zero = closed; a future time = open (cooldown elapsed = half-open)
	probing   bool       // true while a single half-open probe is outstanding
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

// BreakerMetrics returns a snapshot of the aggregate circuit-breaker transition counters across all
// server IPs this Scheduler paces. Safe for concurrent use.
func (s *Scheduler) BreakerMetrics() BreakerMetrics {
	return s.metrics.snapshot()
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

// isOpenNow is a cheap, best-effort check for whether the circuit is definitely open. It performs
// no state transition — it never grants or consumes the single half-open probe — so it's safe to
// call before queueing for a slot/token purely to avoid wasted queueing. It is NOT authoritative:
// the circuit may open (or a probe may already be outstanding) between this check and when the
// caller actually executes, which is why Do rechecks via admit immediately before running fn.
func (sv *server) isOpenNow(now time.Time) bool {
	sv.bmu.Lock()
	defer sv.bmu.Unlock()
	return !sv.openUntil.IsZero() && now.Before(sv.openUntil)
}

// admit is the AUTHORITATIVE breaker admission check. It reports whether the caller may proceed to
// run fn right now, and if so, whether the caller is the single half-open probe.
//
//   - Closed (openUntil zero): always admitted, never a probe.
//   - Open (cooldown not yet elapsed): never admitted.
//   - Half-open (cooldown elapsed): admits exactly ONE caller as the probe (guarded by the
//     `probing` flag under bmu); every other concurrent caller is rejected until that probe calls
//     record.
func (sv *server) admit(now time.Time, m *breakerMetrics) (allowed, isProbe bool) {
	sv.bmu.Lock()
	defer sv.bmu.Unlock()
	if sv.openUntil.IsZero() {
		return true, false
	}
	if now.Before(sv.openUntil) {
		return false, false
	}
	if sv.probing {
		return false, false // a probe is already outstanding; everyone else fast-fails
	}
	sv.probing = true
	m.halfOpened.Add(1)
	return true, true
}

// record folds one fn outcome into the breaker.
//
//   - isProbe: this is the half-open probe's outcome. The probe slot is always released. Success
//     closes the circuit; failure reopens it and restarts the cooldown.
//   - otherwise: a closed-state call. Success resets the consecutive-failure count; reaching
//     threshold consecutive failures opens the circuit for cooldown.
func (sv *server) record(now time.Time, ok, isProbe bool, threshold int, cooldown time.Duration, m *breakerMetrics) {
	sv.bmu.Lock()
	defer sv.bmu.Unlock()
	if isProbe {
		sv.probing = false
		if ok {
			sv.fails = 0
			sv.openUntil = time.Time{}
			m.closed.Add(1)
		} else {
			sv.openUntil = now.Add(cooldown)
			m.opened.Add(1)
		}
		return
	}
	if ok {
		sv.fails = 0
		sv.openUntil = time.Time{}
		return
	}
	sv.fails++
	if sv.fails >= threshold {
		sv.openUntil = now.Add(cooldown)
		m.opened.Add(1)
	}
}

// Do waits for a token and an in-flight slot for serverIP, then runs fn.
//
// The breaker's decisive admission check runs LAST: after the in-flight slot is acquired and after
// the rate-token wait returns, immediately before fn executes. A call that acquired its slot/token
// before the circuit opened is still fast-failed with ErrCircuitOpen if the circuit is open (or a
// half-open probe is already outstanding) by the time it would actually run — queued callers can't
// drain through a circuit that opened while they waited. A cheap, non-authoritative early check
// runs first purely to avoid needlessly queueing against an already-open circuit.
//
// When the cooldown elapses, exactly one caller — whichever reaches the decisive check first — is
// admitted as the half-open probe; every other caller fast-fails with ErrCircuitOpen until that
// probe records success (closing the circuit) or failure (reopening it and restarting cooldown).
func (s *Scheduler) Do(ctx context.Context, serverIP string, fn func() error) error {
	sv := s.forServer(serverIP)
	breaker := s.cfg.BreakerThreshold > 0

	// Cheap early fast-fail: avoid queueing for a slot/token when the circuit is already known
	// open. Not authoritative and never grants/consumes the half-open probe.
	if breaker && sv.isOpenNow(s.now()) {
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

	// Decisive admission check: the rate-token wait can be long, so recheck breaker state here —
	// the authoritative gate is the last thing before fn runs.
	isProbe := false
	if breaker {
		allowed, probe := sv.admit(s.now(), &s.metrics)
		if !allowed {
			return ErrCircuitOpen
		}
		isProbe = probe
	}

	err := fn()
	if breaker {
		sv.record(s.now(), err == nil, isProbe, s.cfg.BreakerThreshold, s.cfg.BreakerCooldown, &s.metrics)
	}
	return err
}
