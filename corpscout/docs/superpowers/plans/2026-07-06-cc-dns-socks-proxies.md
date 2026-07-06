# cc-dns-worker SOCKS Proxy Pool Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Distribute the scanner's source IP across many SOCKS5 proxies so a long full-corpus run isn't throttled/blocked per-source, while keeping each target exactly as polite as today (per-target rate + circuit breaker unchanged; only the source rotates).

**Architecture:** A new `internal/socks` package owns a health-aware, round-robin pool of SOCKS5 proxies and a `Dial` that opens a TCP tunnel to a target via SOCKS5 CONNECT (rotating past dead proxies). The resolver's `Exchange`, when a pool is configured, does DNS-over-TCP through a rotated proxy (`dns.Client.ExchangeWithConn`); with no pool it's the unchanged direct path. Failure attribution keeps proxy failures on the pool's per-proxy breaker and only post-connect outcomes on the target breaker; pool exhaustion is signalled via `scheduler.ErrUnavailable` (excluded from the target breaker) and aborts the run if systemic.

**Tech Stack:** Go 1.25 (existing module), `golang.org/x/net/proxy` (already a dep), `github.com/miekg/dns`.

**Spec:** `docs/superpowers/specs/2026-07-06-cc-dns-socks-proxies-design.md`

## Global Constraints
- Module `cc-dns-worker`; branch `main` (shared workspace switches branches unreliably; the controller scopes review diffs to cc-dns-worker paths). Work from `commoncrawl/cc-dns-worker/`.
- go.mod floor `go 1.25.0` — do NOT change. **Do NOT run `go mod tidy`**; do not edit go.mod/go.sum. All required deps (`x/net/proxy`, `miekg/dns`) are already present — no new dependency is needed (the SOCKS test server is hand-rolled in-repo).
- Follow Conventional Commits; `go fmt ./...` + `go vet ./...` before each commit. Commit only the paths named per task. Do not commit a binary/`.db`.
- Politeness invariant (verbatim from spec): the per-target scheduler (rate limit + circuit breaker) is UNCHANGED — same key (target IP), same rate. Proxies rotate the SOURCE per query; a target still sees ≤ its configured rate total. Direct (no-pool) mode is byte-for-byte the current transport.
- Failure attribution (verbatim): proxy-dial failures update the pool's per-proxy breaker and never reach the target breaker; only post-connect exchange outcomes reach the target breaker; `pool.Dial` returning `socks.ErrNoProxy` is mapped by the resolver to `scheduler.ErrUnavailable`, which `scheduler.Do` excludes from the breaker.
- Defaults: `--socks-max-attempts 3`, `--socks-fail-threshold 5`, `--socks-cooldown 60s`. No proxies configured → direct mode (unchanged).

---

### Task 1: `internal/socks` pool core (parse, health, round-robin)

**Files:**
- Create: `commoncrawl/cc-dns-worker/internal/socks/pool.go`
- Test: `commoncrawl/cc-dns-worker/internal/socks/pool_test.go`

**Interfaces:**
- Produces:
  - `socks.ErrNoProxy error`.
  - `socks.Config{ MaxAttempts, FailThreshold int; Cooldown time.Duration }`.
  - `socks.Proxy` (exported fields `Addr, User, Pass`; unexported `dialer`, health).
  - `socks.Load(entries []string, cfg Config) (*Pool, error)` — nil pool (no error) for empty list.
  - `(*Pool).Next() *Proxy` — next healthy proxy round-robin, or nil if all benched.
  - Internal `markFail`/`markOK` (per-proxy consecutive-failure breaker) + injectable `now` (tests).

- [ ] **Step 1: Write the failing tests**

Create `commoncrawl/cc-dns-worker/internal/socks/pool_test.go`:
```go
package socks

import (
	"testing"
	"time"
)

func TestLoadParsesFormats(t *testing.T) {
	pl, err := Load([]string{
		"1.2.3.4:1080",
		"user:pass@5.6.7.8:1080",
		"socks5://u2:p2@9.9.9.9:1080",
		"  # a comment",
		"",
	}, Config{})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(pl.proxies) != 3 {
		t.Fatalf("got %d proxies, want 3", len(pl.proxies))
	}
	if pl.proxies[0].Addr != "1.2.3.4:1080" || pl.proxies[0].User != "" {
		t.Errorf("proxy0 = %+v", pl.proxies[0])
	}
	if pl.proxies[1].User != "user" || pl.proxies[1].Pass != "pass" || pl.proxies[1].Addr != "5.6.7.8:1080" {
		t.Errorf("proxy1 = %+v", pl.proxies[1])
	}
	if pl.proxies[2].User != "u2" || pl.proxies[2].Addr != "9.9.9.9:1080" {
		t.Errorf("proxy2 = %+v", pl.proxies[2])
	}
}

func TestLoadEmptyIsNilPool(t *testing.T) {
	pl, err := Load([]string{"", "  ", "# only comments"}, Config{})
	if err != nil || pl != nil {
		t.Fatalf("empty list must give nil pool, no error; got pl=%v err=%v", pl, err)
	}
}

func TestLoadBadEntryErrors(t *testing.T) {
	if _, err := Load([]string{"not-a-host-port"}, Config{}); err == nil {
		t.Fatal("want error for malformed proxy entry")
	}
}

func TestNextRoundRobinAndHealth(t *testing.T) {
	pl, _ := Load([]string{"1.1.1.1:1080", "2.2.2.2:1080", "3.3.3.3:1080"}, Config{FailThreshold: 2, Cooldown: 30 * time.Second})
	clk := time.Unix(0, 0).UTC()
	pl.now = func() time.Time { return clk }

	// round-robin across the three
	got := []string{pl.Next().Addr, pl.Next().Addr, pl.Next().Addr, pl.Next().Addr}
	if got[0] != "1.1.1.1:1080" || got[1] != "2.2.2.2:1080" || got[2] != "3.3.3.3:1080" || got[3] != "1.1.1.1:1080" {
		t.Fatalf("round-robin wrong: %v", got)
	}
	// bench proxy 2.2.2.2 (2 consecutive fails)
	p2 := pl.proxies[1]
	pl.markFail(p2)
	pl.markFail(p2)
	// now Next never returns 2.2.2.2 while benched
	for i := 0; i < 6; i++ {
		if pl.Next().Addr == "2.2.2.2:1080" {
			t.Fatal("benched proxy 2.2.2.2 should be skipped")
		}
	}
	// after cooldown it comes back
	clk = clk.Add(31 * time.Second)
	seen := false
	for i := 0; i < 6; i++ {
		if pl.Next().Addr == "2.2.2.2:1080" {
			seen = true
		}
	}
	if !seen {
		t.Error("2.2.2.2 should be available after cooldown")
	}
}

func TestMarkOKResetsFails(t *testing.T) {
	pl, _ := Load([]string{"1.1.1.1:1080"}, Config{FailThreshold: 3, Cooldown: time.Second})
	pl.now = func() time.Time { return time.Unix(0, 0).UTC() }
	p := pl.proxies[0]
	pl.markFail(p)
	pl.markFail(p)
	pl.markOK(p) // reset
	pl.markFail(p)
	pl.markFail(p)
	// only 2 consecutive since reset -> not benched
	if pl.Next() == nil {
		t.Error("proxy should not be benched after markOK reset")
	}
}

func TestAllBenchedNextNil(t *testing.T) {
	pl, _ := Load([]string{"1.1.1.1:1080"}, Config{FailThreshold: 1, Cooldown: time.Hour})
	pl.now = func() time.Time { return time.Unix(0, 0).UTC() }
	pl.markFail(pl.proxies[0]) // benched (threshold 1)
	if pl.Next() != nil {
		t.Error("Next() must be nil when all proxies are benched")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/socks/`
Expected: FAIL — package/`Load`/`Config`/`Pool` undefined.

- [ ] **Step 3: Write the pool**

Create `commoncrawl/cc-dns-worker/internal/socks/pool.go`:
```go
// Package socks manages a pool of SOCKS5 proxies for source-IP distribution: it parses a proxy
// list, hands out proxies round-robin skipping unhealthy ones, tracks per-proxy health with a
// consecutive-failure breaker, and (pool.Dial, in dial.go) opens TCP tunnels via SOCKS5 CONNECT.
// The per-target DNS rate limit + breaker live in package scheduler and are unchanged — this only
// rotates the SOURCE.
package socks

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/proxy"
)

// ErrNoProxy is returned by Dial when every proxy in the pool is currently benched.
var ErrNoProxy = errors.New("socks: no healthy proxy")

// Config tunes pool behavior. Zero values fall back to defaults.
type Config struct {
	MaxAttempts   int           // distinct proxies to try per Dial (default 3)
	FailThreshold int           // consecutive dial failures that bench a proxy (default 5)
	Cooldown      time.Duration // how long a benched proxy stays out (default 60s)
}

// Proxy is one SOCKS5 endpoint plus its health state (health guarded by Pool.mu).
type Proxy struct {
	Addr string // host:port
	User string
	Pass string

	dialer       proxy.Dialer // built once in Load; read-only after
	fails        int
	benchedUntil time.Time
}

// Pool is a round-robin, health-aware fleet of SOCKS5 proxies.
type Pool struct {
	cfg     Config
	now     func() time.Time
	mu      sync.Mutex
	proxies []*Proxy
	cursor  int
}

// Load parses proxy entries — "host:port", "user:pass@host:port", or "socks5://user:pass@host:port"
// (blank lines and #-comments ignored) — and builds a Pool. An empty effective list returns
// (nil, nil): the caller runs in direct (no-proxy) mode.
func Load(entries []string, cfg Config) (*Pool, error) {
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.FailThreshold <= 0 {
		cfg.FailThreshold = 5
	}
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = 60 * time.Second
	}
	var ps []*Proxy
	for _, raw := range entries {
		e := strings.TrimSpace(raw)
		if e == "" || strings.HasPrefix(e, "#") {
			continue
		}
		p, err := parseProxy(e)
		if err != nil {
			return nil, err
		}
		ps = append(ps, p)
	}
	if len(ps) == 0 {
		return nil, nil
	}
	return &Pool{cfg: cfg, now: time.Now, proxies: ps}, nil
}

func parseProxy(e string) (*Proxy, error) {
	e = strings.TrimPrefix(e, "socks5://")
	e = strings.TrimPrefix(e, "socks5h://")
	var user, pass string
	if at := strings.LastIndex(e, "@"); at >= 0 {
		creds := e[:at]
		e = e[at+1:]
		if c := strings.IndexByte(creds, ':'); c >= 0 {
			user, pass = creds[:c], creds[c+1:]
		} else {
			user = creds
		}
	}
	if _, _, err := net.SplitHostPort(e); err != nil {
		return nil, fmt.Errorf("bad proxy %q: %w", e, err)
	}
	var auth *proxy.Auth
	if user != "" {
		auth = &proxy.Auth{User: user, Password: pass}
	}
	d, err := proxy.SOCKS5("tcp", e, auth, proxy.Direct)
	if err != nil {
		return nil, fmt.Errorf("proxy %q: %w", e, err)
	}
	return &Proxy{Addr: e, User: user, Pass: pass, dialer: d}, nil
}

// Next returns the next healthy proxy round-robin, or nil if all are currently benched.
func (pl *Pool) Next() *Proxy {
	pl.mu.Lock()
	defer pl.mu.Unlock()
	now := pl.now()
	n := len(pl.proxies)
	for i := 0; i < n; i++ {
		p := pl.proxies[pl.cursor%n]
		pl.cursor++
		if p.benchedUntil.IsZero() || !now.Before(p.benchedUntil) {
			return p
		}
	}
	return nil
}

// markFail records a proxy dial failure; FailThreshold consecutive failures bench it for Cooldown.
func (pl *Pool) markFail(p *Proxy) {
	pl.mu.Lock()
	defer pl.mu.Unlock()
	p.fails++
	if p.fails >= pl.cfg.FailThreshold {
		p.benchedUntil = pl.now().Add(pl.cfg.Cooldown)
	}
}

// markOK records a proxy dial success, clearing its failure count and bench.
func (pl *Pool) markOK(p *Proxy) {
	pl.mu.Lock()
	defer pl.mu.Unlock()
	p.fails = 0
	p.benchedUntil = time.Time{}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/socks/ && go test -race ./internal/socks/`
Expected: PASS, no races.

- [ ] **Step 5: Commit**

```bash
cd commoncrawl/cc-dns-worker && go fmt ./... && go vet ./internal/socks/
git add commoncrawl/cc-dns-worker/internal/socks/pool.go commoncrawl/cc-dns-worker/internal/socks/pool_test.go
git commit -m "feat(dns): socks proxy pool — parse, round-robin, per-proxy health"
```

---

### Task 2: `pool.Dial` (SOCKS5 CONNECT) + in-process SOCKS5 test server

**Files:**
- Create: `commoncrawl/cc-dns-worker/internal/socks/dial.go`
- Create: `commoncrawl/cc-dns-worker/internal/socks/sockstest/server.go` (reusable in-process SOCKS5 CONNECT server — used by this task's tests AND the resolve integration test in Task 3)
- Test: `commoncrawl/cc-dns-worker/internal/socks/dial_test.go`

**Interfaces:**
- Consumes: `Pool`/`Proxy`/`ErrNoProxy` (Task 1).
- Produces:
  - `(*Pool).Dial(ctx context.Context, targetAddr string) (net.Conn, *Proxy, error)` — used by Task 3's Exchanger. Rotates past dial failures up to `MaxAttempts`; returns `ErrNoProxy` when `Next()` yields nil (pool exhausted); returns the last dial error when `MaxAttempts` proxies were tried and all failed.
  - `sockstest.Start(user, pass string) (addr string, stop func(), err error)` — a minimal SOCKS5 CONNECT proxy (no-auth or username/password) that relays tunnels to the requested target.

- [ ] **Step 1: Write the in-process SOCKS5 CONNECT server (reusable test support)**

Create `commoncrawl/cc-dns-worker/internal/socks/sockstest/server.go`:
```go
// Package sockstest is a minimal in-process SOCKS5 CONNECT server for tests: it relays a CONNECT
// tunnel to the requested target so DNS-over-TCP-through-SOCKS can be exercised without a network.
// Supports no-auth and username/password. Not for production use.
package sockstest

import (
	"bufio"
	"io"
	"net"
	"strconv"
)

// Start listens on 127.0.0.1:0 and serves SOCKS5 CONNECT. If user != "", username/password auth is
// required and validated against user/pass. Returns the listen addr and a stop func.
func Start(user, pass string) (string, func(), error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, err
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go serve(c, user, pass)
		}
	}()
	return ln.Addr().String(), func() { _ = ln.Close() }, nil
}

func serve(c net.Conn, user, pass string) {
	defer c.Close()
	br := bufio.NewReader(c)
	hdr := make([]byte, 2) // ver, nmethods
	if _, err := io.ReadFull(br, hdr); err != nil || hdr[0] != 0x05 {
		return
	}
	methods := make([]byte, int(hdr[1]))
	if _, err := io.ReadFull(br, methods); err != nil {
		return
	}
	if user != "" {
		if _, err := c.Write([]byte{0x05, 0x02}); err != nil { // choose username/password
			return
		}
		a := make([]byte, 2) // ver, ulen
		if _, err := io.ReadFull(br, a); err != nil {
			return
		}
		u := make([]byte, int(a[1]))
		io.ReadFull(br, u)
		pl := make([]byte, 1)
		io.ReadFull(br, pl)
		p := make([]byte, int(pl[0]))
		io.ReadFull(br, p)
		if string(u) != user || string(p) != pass {
			c.Write([]byte{0x01, 0x01}) // auth failure
			return
		}
		c.Write([]byte{0x01, 0x00}) // auth success
	} else {
		c.Write([]byte{0x05, 0x00}) // no-auth
	}
	req := make([]byte, 4) // ver, cmd, rsv, atyp
	if _, err := io.ReadFull(br, req); err != nil || req[1] != 0x01 {
		c.Write([]byte{0x05, 0x07, 0x00, 0x01, 0, 0, 0, 0, 0, 0}) // command not supported
		return
	}
	var host string
	switch req[3] {
	case 0x01:
		b := make([]byte, 4)
		io.ReadFull(br, b)
		host = net.IP(b).String()
	case 0x03:
		l := make([]byte, 1)
		io.ReadFull(br, l)
		b := make([]byte, int(l[0]))
		io.ReadFull(br, b)
		host = string(b)
	case 0x04:
		b := make([]byte, 16)
		io.ReadFull(br, b)
		host = net.IP(b).String()
	default:
		c.Write([]byte{0x05, 0x08, 0x00, 0x01, 0, 0, 0, 0, 0, 0}) // address type not supported
		return
	}
	pb := make([]byte, 2)
	io.ReadFull(br, pb)
	target := net.JoinHostPort(host, strconv.Itoa(int(pb[0])<<8|int(pb[1])))
	up, err := net.Dial("tcp", target)
	if err != nil {
		c.Write([]byte{0x05, 0x05, 0x00, 0x01, 0, 0, 0, 0, 0, 0}) // connection refused
		return
	}
	defer up.Close()
	c.Write([]byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0}) // success, BND 0.0.0.0:0
	go io.Copy(up, br)
	io.Copy(c, up)
}
```

- [ ] **Step 2: Write the failing Dial tests**

Create `commoncrawl/cc-dns-worker/internal/socks/dial_test.go`:
```go
package socks

import (
	"bufio"
	"context"
	"net"
	"testing"
	"time"

	"cc-dns-worker/internal/socks/sockstest"
)

// echoLine starts a TCP server that writes "hello\n" to any client that connects, so a test can
// prove the SOCKS tunnel reached the target.
func echoLine(t *testing.T) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			c.Write([]byte("hello\n"))
			c.Close()
		}
	}()
	return ln.Addr().String(), func() { ln.Close() }
}

func TestDialThroughProxy(t *testing.T) {
	target, stopT := echoLine(t)
	defer stopT()
	proxyAddr, stopP, err := sockstest.Start("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer stopP()

	pl, _ := Load([]string{proxyAddr}, Config{})
	conn, p, err := pl.Dial(context.Background(), target)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if p.Addr != proxyAddr {
		t.Errorf("used proxy %q, want %q", p.Addr, proxyAddr)
	}
	line, _ := bufio.NewReader(conn).ReadString('\n')
	if line != "hello\n" {
		t.Errorf("tunnel payload = %q, want hello", line)
	}
}

func TestDialRotatesPastDeadProxy(t *testing.T) {
	target, stopT := echoLine(t)
	defer stopT()
	good, stopP, err := sockstest.Start("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer stopP()
	// 127.0.0.1:1 is a closed port -> dead proxy, must be rotated past.
	pl, _ := Load([]string{"127.0.0.1:1", good}, Config{MaxAttempts: 3, FailThreshold: 5})
	conn, p, err := pl.Dial(context.Background(), target)
	if err != nil {
		t.Fatalf("dial should have rotated to the good proxy: %v", err)
	}
	conn.Close()
	if p.Addr != good {
		t.Errorf("expected the good proxy, got %q", p.Addr)
	}
}

func TestDialAllBenchedIsErrNoProxy(t *testing.T) {
	pl, _ := Load([]string{"127.0.0.1:1"}, Config{MaxAttempts: 3, FailThreshold: 1, Cooldown: time.Hour})
	pl.now = func() time.Time { return time.Unix(0, 0).UTC() }
	// first dial fails and (threshold 1) benches the only proxy
	_, _, _ = pl.Dial(context.Background(), "127.0.0.1:9")
	// now the pool is exhausted -> ErrNoProxy
	_, _, err := pl.Dial(context.Background(), "127.0.0.1:9")
	if err != ErrNoProxy {
		t.Fatalf("want ErrNoProxy when all benched, got %v", err)
	}
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/socks/ -run TestDial`
Expected: FAIL — `Dial` undefined.

- [ ] **Step 4: Write Dial**

Create `commoncrawl/cc-dns-worker/internal/socks/dial.go`:
```go
package socks

import (
	"context"
	"net"

	"golang.org/x/net/proxy"
)

// Dial picks a healthy proxy and opens a TCP conn to targetAddr through its SOCKS5 CONNECT, rotating
// to the next healthy proxy on failure up to MaxAttempts. It returns ErrNoProxy when the pool is
// exhausted (Next() yields nil); if MaxAttempts distinct proxies were tried and all failed, it
// returns the last dial error (the caller treats that as a target-side failure). The returned
// *Proxy is the one that succeeded.
func (pl *Pool) Dial(ctx context.Context, targetAddr string) (net.Conn, *Proxy, error) {
	var lastErr error
	for attempt := 0; attempt < pl.cfg.MaxAttempts; attempt++ {
		p := pl.Next()
		if p == nil {
			return nil, nil, ErrNoProxy
		}
		conn, err := dialContext(ctx, p.dialer, targetAddr)
		if err != nil {
			pl.markFail(p)
			lastErr = err
			continue
		}
		pl.markOK(p)
		return conn, p, nil
	}
	if lastErr != nil {
		return nil, nil, lastErr
	}
	return nil, nil, ErrNoProxy
}

func dialContext(ctx context.Context, d proxy.Dialer, addr string) (net.Conn, error) {
	if cd, ok := d.(proxy.ContextDialer); ok {
		return cd.DialContext(ctx, "tcp", addr)
	}
	return d.Dial("tcp", addr)
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/socks/... && go test -race ./internal/socks/...`
Expected: PASS (pool + dial + sockstest), no races.

- [ ] **Step 6: Commit**

```bash
cd commoncrawl/cc-dns-worker && go fmt ./... && go vet ./internal/socks/...
git add commoncrawl/cc-dns-worker/internal/socks/dial.go commoncrawl/cc-dns-worker/internal/socks/sockstest commoncrawl/cc-dns-worker/internal/socks/dial_test.go
git commit -m "feat(dns): pool.Dial via SOCKS5 CONNECT + in-process SOCKS5 test server"
```

---

### Task 3: proxy-aware Exchanger + `scheduler.ErrUnavailable`

**Files:**
- Modify: `commoncrawl/cc-dns-worker/internal/scheduler/scheduler.go` (add `ErrUnavailable` + exclude it from the breaker)
- Modify: `commoncrawl/cc-dns-worker/internal/resolve/exchange.go` (optional pool; proxied DNS-over-TCP path)
- Test: `commoncrawl/cc-dns-worker/internal/resolve/proxy_test.go` (build-tag-free, in-process)

**Interfaces:**
- Consumes: `socks.Pool`/`Dial`/`ErrNoProxy` (Task 1-2), `socks/sockstest` (Task 2), `scheduler`.
- Produces:
  - `scheduler.ErrUnavailable error`; `Do` skips breaker-recording when `errors.Is(fnErr, ErrUnavailable)`.
  - `resolve.NewExchanger(sched *scheduler.Scheduler, timeout time.Duration, pool *socks.Pool) Exchanger` — pool may be nil (direct mode). Used by Task 4's scan.

- [ ] **Step 1: Add `ErrUnavailable` to scheduler + failing test**

Append to `commoncrawl/cc-dns-worker/internal/scheduler/scheduler_test.go`:
```go
func TestBreakerIgnoresErrUnavailable(t *testing.T) {
	s := New(Config{PerServerQPS: 1000, Burst: 1000, MaxInFlight: 10, BreakerThreshold: 2, BreakerCooldown: time.Hour})
	clk := time.Unix(0, 0).UTC()
	frozen(s, &clk)
	ctx := context.Background()
	// Many ErrUnavailable outcomes must NOT open the circuit (infra failures, not the target's).
	for i := 0; i < 10; i++ {
		_ = s.Do(ctx, "7.7.7.7", func() error { return ErrUnavailable })
	}
	ran := false
	if err := s.Do(ctx, "7.7.7.7", func() error { ran = true; return nil }); err != nil {
		t.Fatalf("circuit should be closed (ErrUnavailable is not a target failure), got %v", err)
	}
	if !ran {
		t.Error("fn should run — circuit must not have opened from ErrUnavailable")
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/scheduler/ -run TestBreakerIgnoresErrUnavailable`
Expected: FAIL — `ErrUnavailable` undefined.

- [ ] **Step 3: Implement `ErrUnavailable` in scheduler.go**

In `commoncrawl/cc-dns-worker/internal/scheduler/scheduler.go`, add next to `ErrCircuitOpen`:
```go
// ErrUnavailable marks an infrastructure failure (e.g. no usable SOCKS proxy) that must NOT count
// against the target server's circuit breaker. Callers wrap such failures with it via fmt.Errorf.
var ErrUnavailable = errors.New("scheduler: unavailable")
```
Then in `Do`, change the record call so `ErrUnavailable` is excluded. Replace:
```go
	err := fn()
	if breaker {
		sv.record(s.now(), err == nil, s.cfg.BreakerThreshold, s.cfg.BreakerCooldown)
	}
	return err
```
with:
```go
	err := fn()
	if breaker && !errors.Is(err, ErrUnavailable) {
		sv.record(s.now(), err == nil, s.cfg.BreakerThreshold, s.cfg.BreakerCooldown)
	}
	return err
```
(`errors` is already imported for `ErrCircuitOpen`.)

- [ ] **Step 4: Run scheduler tests green**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/scheduler/`
Expected: PASS (new test + all existing breaker/pacing tests).

- [ ] **Step 5: Write the failing proxied-exchange test**

Create `commoncrawl/cc-dns-worker/internal/resolve/proxy_test.go`:
```go
package resolve

import (
	"context"
	"net"
	"testing"
	"time"

	"cc-dns-worker/internal/scheduler"
	"cc-dns-worker/internal/socks"
	"cc-dns-worker/internal/socks/sockstest"

	"github.com/miekg/dns"
)

// startAuthTCP serves the crafted zone over TCP (the SOCKS tunnel is TCP), mirroring startAuth (UDP).
func startAuthTCP(t *testing.T, z zone) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	mux := dns.NewServeMux()
	mux.HandleFunc(".", func(w dns.ResponseWriter, r *dns.Msg) {
		m := new(dns.Msg)
		m.SetReply(r)
		q := r.Question[0]
		if rrs, ok := z[q.Name+"/"+dns.TypeToString[q.Qtype]]; ok {
			m.Answer = append(m.Answer, rrs...)
		}
		_ = w.WriteMsg(m)
	})
	srv := &dns.Server{Listener: ln, Handler: mux}
	go func() { _ = srv.ActivateAndServe() }()
	return ln.Addr().String(), func() { _ = srv.Shutdown() }
}

func newProxiedExchanger(t *testing.T, proxies []string, sched *scheduler.Scheduler) Exchanger {
	t.Helper()
	pool, err := socks.Load(proxies, socks.Config{MaxAttempts: 3, FailThreshold: 5, Cooldown: time.Second})
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	return NewExchanger(sched, 3*time.Second, pool)
}

func TestExchangeThroughProxy(t *testing.T) {
	dnsAddr, stopD := startAuthTCP(t, zone{
		"example.com./A": {mustRR(t, "example.com. 300 IN A 93.184.216.34")},
	})
	defer stopD()
	proxyAddr, stopP, err := sockstest.Start("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer stopP()

	sched := scheduler.New(scheduler.Config{PerServerQPS: 1000, Burst: 1000, MaxInFlight: 10})
	ex := newProxiedExchanger(t, []string{proxyAddr}, sched)

	m := new(dns.Msg)
	m.SetQuestion("example.com.", dns.TypeA)
	resp, err := ex.Exchange(context.Background(), m, dnsAddr)
	if err != nil {
		t.Fatalf("proxied exchange: %v", err)
	}
	if len(resp.Answer) != 1 {
		t.Fatalf("answers = %d, want 1 (routed through proxy)", len(resp.Answer))
	}
	if a, ok := resp.Answer[0].(*dns.A); !ok || a.A.String() != "93.184.216.34" {
		t.Errorf("bad answer %v", resp.Answer[0])
	}
}

// A pool where every proxy is dead (closed port) — after benching, Exchange must NOT open the target
// breaker (the failure is ErrNoProxy -> ErrUnavailable, infra not target).
func TestProxyExhaustionDoesNotTripTargetBreaker(t *testing.T) {
	sched := scheduler.New(scheduler.Config{PerServerQPS: 1000, Burst: 1000, MaxInFlight: 10, BreakerThreshold: 2, BreakerCooldown: time.Hour})
	ex := newProxiedExchanger(t, []string{"127.0.0.1:1"}, sched)
	ctx := context.Background()
	m := new(dns.Msg)
	m.SetQuestion("example.com.", dns.TypeA)
	// Several attempts against a dead proxy pool; all should be infra failures, not target failures.
	for i := 0; i < 6; i++ {
		_, _ = ex.Exchange(ctx, m.Copy(), "203.0.113.9")
	}
	// The target's circuit must still be closed: a direct-style Do runs fn.
	ran := false
	if err := sched.Do(ctx, "203.0.113.9", func() error { ran = true; return nil }); err != nil {
		t.Fatalf("target breaker should be closed (proxy failures are infra), got %v", err)
	}
	if !ran {
		t.Error("target breaker wrongly opened from proxy-exhaustion failures")
	}
}
```

- [ ] **Step 6: Run it to verify it fails**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/resolve/ -run 'Proxy|ThroughProxy'`
Expected: FAIL — `NewExchanger` takes 2 args, not 3.

- [ ] **Step 7: Make the Exchanger proxy-aware**

Replace `commoncrawl/cc-dns-worker/internal/resolve/exchange.go` with:
```go
// Package resolve queries DNS in two tiers: Tier-1 discovery via recursive resolvers (discover.go)
// and Tier-2 record queries directly against authoritative servers (query.go). exchange.go is the
// shared transport: paced through a per-server scheduler. Without a SOCKS pool it sends UDP (TCP on
// truncation) directly; with a pool it sends DNS-over-TCP through a rotated SOCKS5 proxy so the
// source IP is spread across the fleet (the per-target rate/breaker are unchanged either way).
package resolve

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"cc-dns-worker/internal/scheduler"
	"cc-dns-worker/internal/socks"

	"github.com/miekg/dns"
)

// Exchanger sends one DNS message to one server IP and returns the reply.
type Exchanger interface {
	Exchange(ctx context.Context, m *dns.Msg, serverIP string) (*dns.Msg, error)
}

type client struct {
	sched   *scheduler.Scheduler
	pool    *socks.Pool // nil = direct mode
	udp     *dns.Client
	tcp     *dns.Client
	timeout time.Duration
}

// NewExchanger returns an Exchanger paced through sched. If pool is non-nil, every query is sent as
// DNS-over-TCP through a rotated SOCKS5 proxy; if nil, queries go direct (UDP first, TCP on
// truncation). serverIP may be a bare IP (port 53 assumed) or ip:port.
func NewExchanger(sched *scheduler.Scheduler, timeout time.Duration, pool *socks.Pool) Exchanger {
	return &client{
		sched:   sched,
		pool:    pool,
		udp:     &dns.Client{Net: "udp", Timeout: timeout, UDPSize: 1232},
		tcp:     &dns.Client{Net: "tcp", Timeout: timeout},
		timeout: timeout,
	}
}

func withPort(ip string) string {
	if _, _, err := net.SplitHostPort(ip); err == nil {
		return ip
	}
	return net.JoinHostPort(ip, "53")
}

func hostOnly(ip string) string {
	if h, _, err := net.SplitHostPort(ip); err == nil {
		return h
	}
	return ip
}

func (c *client) Exchange(ctx context.Context, m *dns.Msg, serverIP string) (*dns.Msg, error) {
	m.SetEdns0(1232, true) // DO bit + large UDP buffer on every query
	addr := withPort(serverIP)
	var resp *dns.Msg
	err := c.sched.Do(ctx, hostOnly(serverIP), func() error {
		if c.pool == nil {
			r, _, err := c.udp.ExchangeContext(ctx, m, addr)
			if err != nil {
				return err
			}
			if r.Truncated {
				r, _, err = c.tcp.ExchangeContext(ctx, m, addr)
				if err != nil {
					return err
				}
			}
			resp = r
			return nil
		}
		// Proxied: DNS-over-TCP through a rotated SOCKS proxy. A pool exhaustion (ErrNoProxy) is
		// infrastructure, not a target failure, so it's wrapped as scheduler.ErrUnavailable which
		// Do excludes from the target breaker.
		conn, _, derr := c.pool.Dial(ctx, addr)
		if derr != nil {
			if errors.Is(derr, socks.ErrNoProxy) {
				return fmt.Errorf("socks pool exhausted: %w", scheduler.ErrUnavailable)
			}
			return derr
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(c.timeout))
		r, _, xerr := c.tcp.ExchangeWithConn(m, &dns.Conn{Conn: conn})
		if xerr != nil {
			return xerr
		}
		resp = r
		return nil
	})
	return resp, err
}
```

- [ ] **Step 8: Run tests green (resolve + whole module)**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/resolve/ && go build ./... && go test ./... && go test -race ./internal/resolve/ ./internal/scheduler/ ./internal/socks/...`
Expected: PASS everywhere. (The existing resolve tests call `NewExchanger(s, timeout)` with 2 args — they are in the SAME package and will now fail to compile. Fix them: every existing `NewExchanger(s, 2*time.Second)` / `NewExchanger(..., 5*time.Second)` call in `resolve_test.go`, `resolve_extra_test.go`, `query_test.go`(if any), and `smoke_test.go` must pass a third arg `nil`. Update those call sites to `NewExchanger(s, 2*time.Second, nil)` etc. — direct mode, behavior unchanged.)

- [ ] **Step 9: Commit**

```bash
cd commoncrawl/cc-dns-worker && go fmt ./...
git add commoncrawl/cc-dns-worker/internal/scheduler commoncrawl/cc-dns-worker/internal/resolve
git commit -m "feat(dns): proxy-aware Exchanger (DNS-over-TCP via SOCKS) + scheduler.ErrUnavailable"
```

---

### Task 4: wire SOCKS flags into `scan` + README

**Files:**
- Modify: `commoncrawl/cc-dns-worker/cmd/cc-dns-worker/scan.go`
- Modify: `commoncrawl/cc-dns-worker/README.md`

**Interfaces:**
- Consumes: `socks.Load`/`Config` (Task 1), `resolve.NewExchanger(...)` 3-arg (Task 3).
- Produces: `--socks`, `--socks-file`, `--socks-max-attempts`, `--socks-fail-threshold`, `--socks-cooldown` flags; a pool passed to BOTH exchangers; a pool-exhaustion abort in the dispatch loop.

- [ ] **Step 1: Add the flags + build the pool**

In `commoncrawl/cc-dns-worker/cmd/cc-dns-worker/scan.go`, add these flags after the existing `breaker-cooldown` flag:
```go
	socksList := fs.String("socks", "", "comma-separated SOCKS5 proxies (host:port | user:pass@host:port) to spread source IPs; empty = direct")
	socksFile := fs.String("socks-file", "", "file with one SOCKS5 proxy per line (# comments ok); merged with --socks")
	socksMaxAttempts := fs.Int("socks-max-attempts", 3, "distinct proxies to try per query before giving up")
	socksFailThreshold := fs.Int("socks-fail-threshold", 5, "consecutive dial failures that bench a proxy")
	socksCooldown := fs.Duration("socks-cooldown", 60*time.Second, "how long a benched proxy stays out")
```
Then, after flag parsing/validation and before building the schedulers (right after `ctx := context.Background()` and store open, near where `resolverList` is built), assemble the proxy entries and load the pool:
```go
	var socksEntries []string
	if *socksList != "" {
		socksEntries = append(socksEntries, strings.Split(*socksList, ",")...)
	}
	if *socksFile != "" {
		data, ferr := os.ReadFile(*socksFile)
		if ferr != nil {
			return fmt.Errorf("read --socks-file: %w", ferr)
		}
		socksEntries = append(socksEntries, strings.Split(string(data), "\n")...)
	}
	pool, err := socks.Load(socksEntries, socks.Config{MaxAttempts: *socksMaxAttempts, FailThreshold: *socksFailThreshold, Cooldown: *socksCooldown})
	if err != nil {
		return fmt.Errorf("load socks proxies: %w", err)
	}
	if pool != nil {
		log.Printf("scan_id=%s: routing through SOCKS proxy pool", *scanID)
	}
```
Add imports: `"os"` and `"cc-dns-worker/internal/socks"` to scan.go's import block (`strings`, `fmt`, `log`, `time` are already imported).

- [ ] **Step 2: Pass the pool to both exchangers**

Replace the two `resolve.NewExchanger(...)` calls:
```go
	disc := resolve.NewDiscoverer(resolve.NewExchanger(discSched, *timeout), resolverList)
	rec := resolve.NewResolver(resolve.NewExchanger(authSched, *timeout))
```
with:
```go
	disc := resolve.NewDiscoverer(resolve.NewExchanger(discSched, *timeout, pool), resolverList)
	rec := resolve.NewResolver(resolve.NewExchanger(authSched, *timeout, pool))
```

- [ ] **Step 3: Abort if the proxy pool is systemically exhausted**

The design (§4.1): if a whole dispatch batch produces only proxy-exhaustion failures, abort rather than write bogus errors. `resolveBatch` already returns `(committed, err)`; add a systemic-exhaustion guard by detecting an all-`ErrNoProxy` batch. The simplest robust check: after `resolveBatch` returns, if a pool is configured and the batch's domains ALL ended as `error` with the no-proxy reason, abort. To keep it simple and within the existing structure, thread a check into `resolveDomain`: when `disc.DiscoverNS` fails because the pool is exhausted, that domain's error message will contain "socks pool exhausted". In `runScan`'s loop, after `resolveBatch`, query the store for how many of this batch's domains have the exhaustion error is heavier than needed — instead, have `resolveBatch` count results whose `Status=="error"` and `Error` contains `"socks pool exhausted"`, and return that count; if it equals `len(batch)` and `len(batch) > 0`, `runScan` aborts.

Concretely, change `resolveBatch`'s signature to also return the exhaustion count. Replace the `resolveBatch` return type `(int, error)` usage: after the commit loop, compute:
```go
	exhausted := 0
	for _, r := range collected {
		if r.Status == "error" && strings.Contains(r.Error, "socks pool exhausted") {
			exhausted++
		}
	}
	return committed, exhausted, nil
```
Update `resolveBatch`'s signature to `(int, int, error)` and its two early `return committed, err` / `return committed, fmt.Errorf(...)` sites to `return committed, 0, err`. In `runScan`'s loop, change the call and add the guard:
```go
		committed, exhausted, err := resolveBatch(ctx, st, disc, rec, cfg, batch, *scanID, *runID, *workers, *batchN)
		if err != nil {
			return err
		}
		if pool != nil && exhausted == len(batch) && len(batch) > 0 {
			return fmt.Errorf("socks pool exhausted for an entire batch of %d domains — check proxies", len(batch))
		}
		if committed == 0 {
			return fmt.Errorf("no progress: batch of %d domains committed 0", len(batch))
		}
```
(`strings` is already imported in scan.go.)

- [ ] **Step 4: Build + full suite**

Run: `cd commoncrawl/cc-dns-worker && go build ./... && go vet ./... && go test ./...`
Expected: build/vet clean; all tests PASS.

- [ ] **Step 5: Confirm flags + a direct-mode sanity run (no proxies)**

Run:
```bash
cd commoncrawl/cc-dns-worker && go build -o bin/cc-dns-worker ./cmd/cc-dns-worker
./bin/cc-dns-worker scan -h 2>&1 | grep -E 'socks'
```
Expected: the five `-socks*` flags print with their defaults.

Optional live direct-mode sanity (no `--socks`, CH env set — `CLICKHOUSE_ADDR=companycollect:9002 CLICKHOUSE_USER=default CLICKHOUSE_PASSWORD=password123 CLICKHOUSE_DB=corpscout`): a small `scan --limit 10 --db /tmp/dns-sx.db --scan-id sxchk` should complete normally (proxies off by default → unchanged behavior), then `rm -f /tmp/dns-sx.db*`. (A live *proxied* run needs real SOCKS proxies, which this environment lacks — the proxied path is covered by the in-process integration tests in Task 3.)

- [ ] **Step 6: Update the README**

In `commoncrawl/cc-dns-worker/README.md`:
- Add the five `--socks*` flags to the `scan` flags table with accurate descriptions matching the flag help.
- Add a short "SOCKS proxy pool (source distribution)" subsection near the rate-limiting/scheduler section: when `--socks`/`--socks-file` is set, every query is sent **DNS-over-TCP through a rotated SOCKS5 proxy**, spreading the source IP across the fleet so no single source is throttled/blocked by a target over a long run; the **per-target rate limit and circuit breaker are unchanged** (a target still sees ≤ its configured rate total). Note: proxy failures are tracked on a **per-proxy** health breaker (bench after `--socks-fail-threshold` consecutive dial failures for `--socks-cooldown`) and never trip a target's breaker; with no proxies configured the worker runs in unchanged direct mode.
- Keep tone consistent; do not invent flags (every documented flag must exist in scan.go).

- [ ] **Step 7: Commit**

```bash
cd commoncrawl/cc-dns-worker && go fmt ./...
git add commoncrawl/cc-dns-worker/cmd/cc-dns-worker/scan.go commoncrawl/cc-dns-worker/README.md
git commit -m "feat(dns): scan --socks proxy-pool flags (source distribution) + README"
```

---

## Self-review notes
- Spec §2 transport (DNS-over-TCP via SOCKS5 CONNECT, x/net/proxy) → Task 2 `Dial` + Task 3 Exchange proxied path. ✓
- Spec §2 rate model (per-target unchanged; source rotates) → scheduler untouched except the ErrUnavailable exclusion; source rotation in Task 2 `Dial`/Task 3 Exchange. ✓
- Spec §3.1 pool (parse/Next/health/ErrNoProxy) → Task 1; §3.2 proxy-aware exchange → Task 3. ✓
- Spec §4 failure attribution (proxy failures → pool breaker only; post-connect → target breaker) → Task 2 `Dial` markFail/markOK inside dial; Task 3 Exchange returns only post-connect error; `TestProxyExhaustionDoesNotTripTargetBreaker`. ✓
- Spec §4.1 ErrNoProxy→ErrUnavailable exclusion + systemic-exhaustion abort → Task 3 (scheduler.ErrUnavailable + Exchange mapping) + Task 4 Step 3. ✓
- Spec §5 flags → Task 4. §6 direct mode unchanged → Task 3 (nil-pool branch is the old code) + Task 3 Step 8 fixes existing call sites to `nil`. ✓
- Spec §7 tests (pool units, in-process SOCKS5 + TCP-DNS integration, failure attribution, direct-mode green) → Tasks 1/2/3. ✓
- Type consistency: `socks.Load(entries, Config) (*Pool, error)`, `(*Pool).Dial(ctx, addr) (net.Conn, *Proxy, error)`, `(*Pool).Next() *Proxy`, `scheduler.ErrUnavailable`, `resolve.NewExchanger(sched, timeout, *socks.Pool)` — defined in Tasks 1-3 and consumed with matching signatures in Tasks 3-4; `resolveBatch` return widened to `(int, int, error)` consistently in Task 4. ✓
```
