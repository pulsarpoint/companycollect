# CommonCrawl DNS Scanner Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Go worker (`cc-dns-worker`) that reads domains from ClickHouse, resolves a rich DNS record set directly from each domain's authoritative nameservers under strict per-server rate limits, writes one row per domain to Parquet, and loads it into a historical ClickHouse table.

**Architecture:** Iterative DNS resolution with `miekg/dns`. Every outbound query targets a specific server IP; a single token-bucket limiter per server IP (default ~10 qps) paces both the TLD tier (NS discovery) and the authoritative-NS tier (record queries). Single-process, in-memory for v1. Output mirrors `cc-enrich-worker`: rolling Parquet with dual `parquet`/`ch`-tagged row structs, plus a `load` subcommand that batch-inserts via the ClickHouse native protocol into `corpscout.commoncrawl_domain_dns` (ReplacingMergeTree, historical by `scan_id`).

**Tech Stack:** Go 1.24, `github.com/miekg/dns`, `golang.org/x/time/rate`, `golang.org/x/net/publicsuffix`, `github.com/parquet-go/parquet-go`, `github.com/ClickHouse/clickhouse-go/v2`.

**Spec:** `docs/superpowers/specs/2026-07-05-commoncrawl-dns-scanner-design.md`

## Global Constraints

- Go module name: `cc-dns-worker`; all internal imports are `cc-dns-worker/internal/...`.
- Go version floor: `go 1.24` in `go.mod`.
- Every Parquet row struct field carries BOTH a `parquet:"col"` and `ch:"col"` tag naming the same column; a struct-tag test pins them equal (mirrors `cc-enrich-worker`).
- Default per-server rate: `10` qps, burst `10`; default per-server in-flight cap `3`. All configurable via flags/env.
- Default hostnames (5, apex always added separately, apex stored under key `@`): `www, mail, webmail, smtp, autodiscover`.
- Default DKIM selectors (10): `default, google, selector1, selector2, k1, dkim, s1, s2, mail, mandrill`.
- EDNS0 UDP buffer size `1232`, DO bit set on every query.
- ClickHouse database `corpscout`; new table `corpscout.commoncrawl_domain_dns`.
- Follow Conventional Commits. Run `go fmt ./...` and `go vet ./...` before each commit.
- Working directory for the module: `commoncrawl/cc-dns-worker/`.

---

### Task 1: Scaffold the Go module and CLI skeleton

**Files:**
- Create: `commoncrawl/cc-dns-worker/go.mod`
- Create: `commoncrawl/cc-dns-worker/cmd/cc-dns-worker/main.go`
- Create: `commoncrawl/cc-dns-worker/Makefile`

**Interfaces:**
- Consumes: nothing.
- Produces: a buildable binary with `scan` and `load` subcommands (stubs that print usage).

- [ ] **Step 1: Create the module**

Run:
```bash
cd commoncrawl/cc-dns-worker && go mod init cc-dns-worker && go get github.com/miekg/dns@latest golang.org/x/time/rate@latest golang.org/x/net/publicsuffix@latest github.com/parquet-go/parquet-go@latest github.com/ClickHouse/clickhouse-go/v2@latest
```
Expected: `go.mod` and `go.sum` created with those requires; `go 1.24` line present (add/adjust if the toolchain writes a different floor).

- [ ] **Step 2: Write the CLI skeleton**

Create `commoncrawl/cc-dns-worker/cmd/cc-dns-worker/main.go`:
```go
// Command cc-dns-worker resolves DNS for corpscout domains directly from authoritative
// nameservers and loads the results into ClickHouse.
//
// Usage:
//   cc-dns-worker scan  [flags]   resolve domains -> dns.parquet
//   cc-dns-worker load  [flags]   load dns.parquet -> corpscout.commoncrawl_domain_dns
package main

import (
	"fmt"
	"os"
)

func usage() {
	fmt.Fprint(os.Stderr, `cc-dns-worker — resolve corpscout domains directly from authoritative DNS

Usage:
  cc-dns-worker <command> [flags]

Commands:
  scan   resolve domains from ClickHouse (or a worklist parquet) into dns.parquet
  load   load an already-produced dns.parquet into corpscout.commoncrawl_domain_dns

Run "cc-dns-worker <command> -h" for that command's flags.
`)
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "scan":
		if err := runScan(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "scan:", err)
			os.Exit(1)
		}
	case "load":
		if err := runLoad(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "load:", err)
			os.Exit(1)
		}
	default:
		usage()
		os.Exit(2)
	}
}
```

Create `commoncrawl/cc-dns-worker/cmd/cc-dns-worker/scan.go`:
```go
package main

import "flag"

func runScan(args []string) error {
	fs := flag.NewFlagSet("scan", flag.ExitOnError)
	_ = fs.Parse(args)
	return nil // wired up in Task 8
}
```

Create `commoncrawl/cc-dns-worker/cmd/cc-dns-worker/load.go`:
```go
package main

import "flag"

func runLoad(args []string) error {
	fs := flag.NewFlagSet("load", flag.ExitOnError)
	_ = fs.Parse(args)
	return nil // wired up in Task 9
}
```

- [ ] **Step 3: Write the Makefile**

Create `commoncrawl/cc-dns-worker/Makefile`:
```make
.PHONY: build test vet fmt
build:
	go build -o bin/cc-dns-worker ./cmd/cc-dns-worker
test:
	go test ./...
vet:
	go vet ./...
fmt:
	go fmt ./...
```

- [ ] **Step 4: Verify it builds**

Run: `cd commoncrawl/cc-dns-worker && go build ./... && go vet ./...`
Expected: no output, exit 0.

- [ ] **Step 5: Commit**

```bash
cd commoncrawl/cc-dns-worker && go fmt ./...
git add commoncrawl/cc-dns-worker
git commit -m "feat(dns): scaffold cc-dns-worker module and CLI skeleton"
```

---

### Task 2: The DomainDNSRow model with pinned parquet/ch tags

**Files:**
- Create: `commoncrawl/cc-dns-worker/internal/model/model.go`
- Test: `commoncrawl/cc-dns-worker/internal/model/model_test.go`

**Interfaces:**
- Produces: `model.DomainDNSRow` — the one struct written to Parquet and inserted into ClickHouse. Fields consumed by output (Task 7), load (Task 9), and worker assembly (Task 8).

- [ ] **Step 1: Write the failing test**

Create `commoncrawl/cc-dns-worker/internal/model/model_test.go`:
```go
package model

import (
	"reflect"
	"testing"
)

// The load path INSERTs using ch tags; the file is written using parquet tags. They must name the
// same column or the loaded data lands in the wrong column silently.
func TestDomainDNSRowTagsMatch(t *testing.T) {
	rt := reflect.TypeOf(DomainDNSRow{})
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		pq := f.Tag.Get("parquet")
		ch := f.Tag.Get("ch")
		if pq == "" || ch == "" {
			t.Fatalf("field %s missing a tag: parquet=%q ch=%q", f.Name, pq, ch)
		}
		// parquet tag may carry options after a comma (e.g. "resolved_at,timestamp").
		pqCol := pq
		if idx := indexByte(pq, ','); idx >= 0 {
			pqCol = pq[:idx]
		}
		if pqCol != ch {
			t.Errorf("field %s: parquet col %q != ch col %q", f.Name, pqCol, ch)
		}
	}
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/model/`
Expected: FAIL — `DomainDNSRow` undefined.

- [ ] **Step 3: Write the model**

Create `commoncrawl/cc-dns-worker/internal/model/model.go`:
```go
// Package model defines DomainDNSRow, the single row written to Parquet and inserted into
// corpscout.commoncrawl_domain_dns. Every field carries matching parquet and ch tags; the two
// must name the same column (pinned by TestDomainDNSRowTagsMatch).
package model

import "time"

// DomainDNSRow is one domain's DNS posture for one scan. Records are stored verbatim; SPF/DMARC/
// DKIM parsing and scoring are derived later in SQL.
type DomainDNSRow struct {
	ScanID     string `parquet:"scan_id" ch:"scan_id"`
	RootDomain string `parquet:"root_domain" ch:"root_domain"`
	ETLD       string `parquet:"etld" ch:"etld"`

	Nameservers []string `parquet:"nameservers" ch:"nameservers"`
	NSIPs       []string `parquet:"ns_ips" ch:"ns_ips"`

	A    map[string][]string `parquet:"a" ch:"a"`
	AAAA map[string][]string `parquet:"aaaa" ch:"aaaa"`

	MX     []string          `parquet:"mx" ch:"mx"`
	TXT    []string          `parquet:"txt" ch:"txt"`
	DMARC  string            `parquet:"dmarc" ch:"dmarc"`
	DKIM   map[string]string `parquet:"dkim" ch:"dkim"`
	MTASTS string            `parquet:"mta_sts" ch:"mta_sts"`
	TLSRPT string            `parquet:"tls_rpt" ch:"tls_rpt"`
	BIMI   string            `parquet:"bimi" ch:"bimi"`

	CAA []string `parquet:"caa" ch:"caa"`
	SOA string   `parquet:"soa" ch:"soa"`

	DNSSECSigned uint8    `parquet:"dnssec_signed" ch:"dnssec_signed"`
	DSPresent    uint8    `parquet:"ds_present" ch:"ds_present"`
	DNSKEY       []string `parquet:"dnskey" ch:"dnskey"`
	DS           []string `parquet:"ds" ch:"ds"`

	QueryStatus  map[string]string `parquet:"query_status" ch:"query_status"`
	ResolverPath string            `parquet:"resolver_path" ch:"resolver_path"`
	SourceRunID  string            `parquet:"source_run_id" ch:"source_run_id"`
	ResolvedAt   time.Time         `parquet:"resolved_at,timestamp" ch:"resolved_at"`
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/model/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd commoncrawl/cc-dns-worker && go fmt ./... && go vet ./...
git add commoncrawl/cc-dns-worker/internal/model
git commit -m "feat(dns): DomainDNSRow model with pinned parquet/ch tags"
```

---

### Task 3: Query planning (hostnames, DKIM selectors, qnames)

**Files:**
- Create: `commoncrawl/cc-dns-worker/internal/records/plan.go`
- Test: `commoncrawl/cc-dns-worker/internal/records/plan_test.go`

**Interfaces:**
- Produces:
  - `records.Config{ Hostnames []string; DKIMSelectors []string }`
  - `records.DefaultConfig() Config`
  - `records.Query{ Name string; Type uint16; Slot string }` — `Slot` is the map key used when
    storing the answer (e.g. `www`, `@`, a DKIM selector).
  - `records.Plan(domain string, cfg Config) []Query` — the full per-domain query list (Tier 2;
    NS discovery is separate, in Task 5).
- Consumes: `github.com/miekg/dns` type constants.

- [ ] **Step 1: Write the failing test**

Create `commoncrawl/cc-dns-worker/internal/records/plan_test.go`:
```go
package records

import (
	"testing"

	"github.com/miekg/dns"
)

func TestPlanCoversAllRecordFamilies(t *testing.T) {
	cfg := DefaultConfig()
	qs := Plan("example.com", cfg)

	// Index by "name/type" for assertions.
	got := map[string]bool{}
	for _, q := range qs {
		got[q.Name+"/"+dns.TypeToString[q.Type]] = true
	}

	want := []string{
		"example.com./A", "example.com./AAAA", // apex host
		"www.example.com./A", "mail.example.com./A", // sample subdomains
		"example.com./MX", "example.com./TXT",
		"_dmarc.example.com./TXT",
		"default._domainkey.example.com./TXT",   // first DKIM selector
		"mandrill._domainkey.example.com./TXT",  // last DKIM selector
		"_mta-sts.example.com./TXT",
		"_smtp._tls.example.com./TXT",
		"default._bimi.example.com./TXT",
		"example.com./CAA", "example.com./SOA", "example.com./NS",
		"example.com./DNSKEY",
	}
	for _, w := range want {
		if !got[w] {
			t.Errorf("plan missing query %q", w)
		}
	}
}

func TestPlanApexSlotIsAt(t *testing.T) {
	for _, q := range Plan("example.com", DefaultConfig()) {
		if q.Name == "example.com." && q.Type == dns.TypeA && q.Slot != "@" {
			t.Errorf("apex A slot = %q, want @", q.Slot)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/records/`
Expected: FAIL — `DefaultConfig`, `Plan`, `Query` undefined.

- [ ] **Step 3: Write the plan**

Create `commoncrawl/cc-dns-worker/internal/records/plan.go`:
```go
// Package records builds the per-domain list of DNS queries (Tier 2 — sent to the domain's own
// authoritative nameservers). NS discovery (Tier 1) lives in package resolve.
package records

import (
	"github.com/miekg/dns"
)

// Config controls which hostnames get A/AAAA queries and which DKIM selectors are brute-forced.
type Config struct {
	Hostnames     []string // subdomains, apex is always added separately
	DKIMSelectors []string
}

// DefaultConfig is the spec's default hostname (5) and DKIM selector (10) lists.
func DefaultConfig() Config {
	return Config{
		Hostnames:     []string{"www", "mail", "webmail", "smtp", "autodiscover"},
		DKIMSelectors: []string{"default", "google", "selector1", "selector2", "k1", "dkim", "s1", "s2", "mail", "mandrill"},
	}
}

// Query is one DNS question plus the map key ("slot") under which its answer is stored.
type Query struct {
	Name string // FQDN with trailing dot
	Type uint16
	Slot string // "@" for apex; hostname for A/AAAA; selector for DKIM; "" when N/A
}

// Plan returns every Tier-2 query for a domain. domain has no trailing dot.
func Plan(domain string, cfg Config) []Query {
	fqdn := dns.Fqdn(domain)
	qs := []Query{
		// apex host
		{fqdn, dns.TypeA, "@"},
		{fqdn, dns.TypeAAAA, "@"},
		// zone/infra
		{fqdn, dns.TypeMX, ""},
		{fqdn, dns.TypeTXT, ""},
		{fqdn, dns.TypeNS, ""},
		{fqdn, dns.TypeSOA, ""},
		{fqdn, dns.TypeCAA, ""},
		{fqdn, dns.TypeDNSKEY, ""},
		// mail policy
		{"_dmarc." + fqdn, dns.TypeTXT, "dmarc"},
		{"_mta-sts." + fqdn, dns.TypeTXT, "mta_sts"},
		{"_smtp._tls." + fqdn, dns.TypeTXT, "tls_rpt"},
		{"default._bimi." + fqdn, dns.TypeTXT, "bimi"},
	}
	for _, h := range cfg.Hostnames {
		hn := dns.Fqdn(h + "." + domain)
		qs = append(qs, Query{hn, dns.TypeA, h}, Query{hn, dns.TypeAAAA, h})
	}
	for _, sel := range cfg.DKIMSelectors {
		qs = append(qs, Query{dns.Fqdn(sel + "._domainkey." + domain), dns.TypeTXT, sel})
	}
	return qs
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/records/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd commoncrawl/cc-dns-worker && go fmt ./... && go vet ./...
git add commoncrawl/cc-dns-worker/internal/records
git commit -m "feat(dns): per-domain query plan (hosts, DKIM, mail policy, DNSSEC)"
```

---

### Task 4: Per-server-IP scheduler (token-bucket limiter + in-flight cap)

**Files:**
- Create: `commoncrawl/cc-dns-worker/internal/scheduler/scheduler.go`
- Test: `commoncrawl/cc-dns-worker/internal/scheduler/scheduler_test.go`

**Interfaces:**
- Produces:
  - `scheduler.Config{ PerServerQPS float64; Burst int; MaxInFlight int }`
  - `scheduler.New(cfg Config) *Scheduler`
  - `(*Scheduler).Do(ctx context.Context, serverIP string, fn func() error) error` — acquires the
    server's token + in-flight slot, runs `fn`, releases the slot. This is the single choke point
    every outbound query passes through (Task 5 calls it).
- Consumes: `golang.org/x/time/rate`.

- [ ] **Step 1: Write the failing test**

Create `commoncrawl/cc-dns-worker/internal/scheduler/scheduler_test.go`:
```go
package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// With 5 qps and burst 1, 6 calls to one server must take >= ~1s (5 tokens/sec after the first).
func TestPerServerPacing(t *testing.T) {
	s := New(Config{PerServerQPS: 5, Burst: 1, MaxInFlight: 100})
	ctx := context.Background()
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.Do(ctx, "1.2.3.4", func() error { return nil })
		}()
	}
	wg.Wait()
	if elapsed := time.Since(start); elapsed < 900*time.Millisecond {
		t.Errorf("6 calls at 5qps/burst1 took %v, want >= ~1s", elapsed)
	}
}

// Two different servers are independent: 6 calls split across two servers finish fast.
func TestServersAreIndependent(t *testing.T) {
	s := New(Config{PerServerQPS: 5, Burst: 3, MaxInFlight: 100})
	ctx := context.Background()
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		ip := "1.1.1.1"
		if i%2 == 0 {
			ip = "2.2.2.2"
		}
		wg.Add(1)
		go func(ip string) {
			defer wg.Done()
			_ = s.Do(ctx, ip, func() error { return nil })
		}(ip)
	}
	wg.Wait()
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Errorf("independent servers took %v, want fast", elapsed)
	}
}

// MaxInFlight caps concurrent fn execution per server.
func TestMaxInFlightCap(t *testing.T) {
	s := New(Config{PerServerQPS: 1000, Burst: 1000, MaxInFlight: 2})
	ctx := context.Background()
	var cur, max int32
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.Do(ctx, "9.9.9.9", func() error {
				n := atomic.AddInt32(&cur, 1)
				mu.Lock()
				if n > max {
					max = n
				}
				mu.Unlock()
				time.Sleep(20 * time.Millisecond)
				atomic.AddInt32(&cur, -1)
				return nil
			})
		}()
	}
	wg.Wait()
	if max > 2 {
		t.Errorf("max in-flight = %d, want <= 2", max)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/scheduler/`
Expected: FAIL — `New`, `Config` undefined.

- [ ] **Step 3: Write the scheduler**

Create `commoncrawl/cc-dns-worker/internal/scheduler/scheduler.go`:
```go
// Package scheduler paces outbound work per target server IP. Every DNS query passes through
// Do(), which grants a token from that server's bucket and a per-server in-flight slot before
// running fn. This is the whole rate-limiting model: hundreds of thousands of independent
// server buckets, each gentle, run in parallel. Single-process only; see the spec's shard-by-
// server scale-out note for the distributed path.
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

// New returns a Scheduler. Zero/negative knobs fall back to safe defaults.
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

// Do waits for a token and an in-flight slot for serverIP, then runs fn. It returns ctx.Err() if
// the context is cancelled while waiting, otherwise fn's error.
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/scheduler/`
Expected: PASS (all three tests).

- [ ] **Step 5: Commit**

```bash
cd commoncrawl/cc-dns-worker && go fmt ./... && go vet ./...
git add commoncrawl/cc-dns-worker/internal/scheduler
git commit -m "feat(dns): per-server-IP token-bucket scheduler with in-flight cap"
```

---

### Task 5: Iterative resolver — a low-level exchange + NS discovery

**Files:**
- Create: `commoncrawl/cc-dns-worker/internal/resolve/exchange.go`
- Create: `commoncrawl/cc-dns-worker/internal/resolve/discover.go`
- Test: `commoncrawl/cc-dns-worker/internal/resolve/resolve_test.go`
- Test helper: `commoncrawl/cc-dns-worker/internal/resolve/testserver_test.go`

**Interfaces:**
- Produces:
  - `resolve.Exchanger` interface: `Exchange(ctx, m *dns.Msg, serverIP string) (*dns.Msg, error)`
  - `resolve.NewExchanger(sched *scheduler.Scheduler, timeout time.Duration) Exchanger` — the real
    UDP-with-TCP-fallback client, every send routed through `sched.Do`.
  - `resolve.Resolver{ Ex Exchanger; Roots []string }`
  - `resolve.NewResolver(ex Exchanger) *Resolver` — seeds `Roots` with the 13 root server IPs.
  - `(*Resolver).DiscoverNS(ctx, domain string) (Delegation, error)` where
    `Delegation{ ETLD string; NS []string; NSIPs []string; DS []string }`.
- Consumes: `scheduler.Scheduler` (Task 4), `github.com/miekg/dns`, `golang.org/x/net/publicsuffix`.

- [ ] **Step 1: Write the in-process authoritative test server helper**

Create `commoncrawl/cc-dns-worker/internal/resolve/testserver_test.go`:
```go
package resolve

import (
	"net"
	"testing"

	"github.com/miekg/dns"
)

// zone maps an exact "qname/qtype" to the RRs an authoritative server returns in ANSWER.
type zone map[string][]dns.RR

// startAuth spins a UDP miekg/dns server on 127.0.0.1:0 answering from z. It returns the server's
// ip:port host and a cleanup func.
func startAuth(t *testing.T, z zone) (string, func()) {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	mux := dns.NewServeMux()
	mux.HandleFunc(".", func(w dns.ResponseWriter, r *dns.Msg) {
		m := new(dns.Msg)
		m.SetReply(r)
		q := r.Question[0]
		key := q.Name + "/" + dns.TypeToString[q.Qtype]
		if rrs, ok := z[key]; ok {
			m.Answer = append(m.Answer, rrs...)
		}
		_ = w.WriteMsg(m)
	})
	srv := &dns.Server{PacketConn: pc, Handler: mux}
	go func() { _ = srv.ActivateAndServe() }()
	return pc.LocalAddr().String(), func() { _ = srv.Shutdown() }
}

func mustRR(t *testing.T, s string) dns.RR {
	t.Helper()
	rr, err := dns.NewRR(s)
	if err != nil {
		t.Fatalf("bad RR %q: %v", s, err)
	}
	return rr
}
```

- [ ] **Step 2: Write the failing exchange + discovery test**

Create `commoncrawl/cc-dns-worker/internal/resolve/resolve_test.go`:
```go
package resolve

import (
	"context"
	"testing"
	"time"

	"cc-dns-worker/internal/scheduler"

	"github.com/miekg/dns"
)

func newTestExchanger() Exchanger {
	s := scheduler.New(scheduler.Config{PerServerQPS: 1000, Burst: 1000, MaxInFlight: 10})
	return NewExchanger(s, 2*time.Second)
}

func TestExchangeRoundTrip(t *testing.T) {
	addr, stop := startAuth(t, zone{
		"example.com./A": {mustRR(t, "example.com. 300 IN A 93.184.216.34")},
	})
	defer stop()

	ex := newTestExchanger()
	m := new(dns.Msg)
	m.SetQuestion("example.com.", dns.TypeA)
	resp, err := ex.Exchange(context.Background(), m, addr)
	if err != nil {
		t.Fatalf("exchange: %v", err)
	}
	if len(resp.Answer) != 1 {
		t.Fatalf("answers = %d, want 1", len(resp.Answer))
	}
	if a, ok := resp.Answer[0].(*dns.A); !ok || a.A.String() != "93.184.216.34" {
		t.Errorf("bad answer %v", resp.Answer[0])
	}
}
```

Note: `DiscoverNS` is exercised end-to-end in the real-DNS smoke test (Task 10) because a faithful
root→TLD→auth walk against fake in-process servers requires wiring referral responses; the unit
layer here pins the exchange primitive and pacing. Keep `DiscoverNS` logic small and covered by the
smoke test.

- [ ] **Step 3: Run test to verify it fails**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/resolve/`
Expected: FAIL — `Exchanger`, `NewExchanger` undefined.

- [ ] **Step 4: Write the exchange primitive**

Create `commoncrawl/cc-dns-worker/internal/resolve/exchange.go`:
```go
// Package resolve does iterative DNS resolution directly against authoritative servers. exchange.go
// is the transport primitive: a single UDP query (TCP on truncation) routed through the per-server
// scheduler so no server is ever hit too fast.
package resolve

import (
	"context"
	"net"
	"time"

	"cc-dns-worker/internal/scheduler"

	"github.com/miekg/dns"
)

// Exchanger sends one DNS message to one server IP and returns the reply.
type Exchanger interface {
	Exchange(ctx context.Context, m *dns.Msg, serverIP string) (*dns.Msg, error)
}

type client struct {
	sched   *scheduler.Scheduler
	udp     *dns.Client
	tcp     *dns.Client
	timeout time.Duration
}

// NewExchanger returns an Exchanger that paces every send through sched. serverIP may be a bare IP
// (port 53 assumed) or ip:port.
func NewExchanger(sched *scheduler.Scheduler, timeout time.Duration) Exchanger {
	return &client{
		sched:   sched,
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

func (c *client) Exchange(ctx context.Context, m *dns.Msg, serverIP string) (*dns.Msg, error) {
	// Request DNSSEC records (DO bit) and a large UDP buffer on every query.
	m.SetEdns0(1232, true)
	addr := withPort(serverIP)
	var resp *dns.Msg
	err := c.sched.Do(ctx, hostOnly(serverIP), func() error {
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
	})
	return resp, err
}

func hostOnly(ip string) string {
	if h, _, err := net.SplitHostPort(ip); err == nil {
		return h
	}
	return ip
}
```

- [ ] **Step 5: Write NS discovery (iterative walk)**

Create `commoncrawl/cc-dns-worker/internal/resolve/discover.go`:
```go
package resolve

import (
	"context"
	"errors"
	"strings"

	"github.com/miekg/dns"
	"golang.org/x/net/publicsuffix"
)

// rootServers are the 13 IANA root server IPv4 addresses (a–m.root-servers.net).
var rootServers = []string{
	"198.41.0.4", "170.247.170.2", "192.33.4.12", "199.7.91.13",
	"192.203.230.10", "192.5.5.241", "192.112.36.4", "198.97.190.53",
	"192.36.148.17", "192.58.128.30", "193.0.14.129", "199.7.83.42", "202.12.27.33",
}

// Delegation is the authoritative delegation learned for a domain.
type Delegation struct {
	ETLD  string   // public suffix (registrable-domain suffix), e.g. "com" or "co.uk"
	NS    []string // authoritative nameserver hostnames
	NSIPs []string // resolved NS IPs (glue where present, else A/AAAA)
	DS    []string // DS records at the parent (DNSSEC), verbatim
}

// Resolver performs iterative resolution using an Exchanger. Roots is the starting server set.
type Resolver struct {
	Ex    Exchanger
	Roots []string
}

// NewResolver seeds the root server list.
func NewResolver(ex Exchanger) *Resolver {
	return &Resolver{Ex: ex, Roots: append([]string(nil), rootServers...)}
}

// DiscoverNS walks root -> TLD -> authoritative to learn a domain's NS set and parent DS. It follows
// referrals (NS in AUTHORITY, glue A in ADDITIONAL). domain has no trailing dot.
func (r *Resolver) DiscoverNS(ctx context.Context, domain string) (Delegation, error) {
	etld, _ := publicsuffix.PublicSuffix(domain)
	del := Delegation{ETLD: etld}
	fqdn := dns.Fqdn(domain)

	servers := append([]string(nil), r.Roots...)
	// Walk down at most a handful of referral hops (root, TLD, sld, safety margin).
	for hop := 0; hop < 8; hop++ {
		m := new(dns.Msg)
		m.SetQuestion(fqdn, dns.TypeNS)
		resp, err := r.exchangeAny(ctx, m, servers)
		if err != nil {
			return del, err
		}

		nsNames, glue := extractDelegation(resp)
		for _, rr := range resp.Ns {
			if ds, ok := rr.(*dns.DS); ok {
				del.DS = append(del.DS, ds.String())
			}
		}

		// Authoritative answer: NS records for the domain in ANSWER.
		if authoritativeNS := answerNS(resp, fqdn); len(authoritativeNS) > 0 {
			del.NS = authoritativeNS
			del.NSIPs = resolveNSIPs(ctx, r, authoritativeNS, glue)
			return del, nil
		}
		if len(nsNames) == 0 {
			return del, errors.New("no delegation and no authoritative NS")
		}
		// Descend: prefer glue IPs; else resolve one NS name via the roots.
		next := ipsFor(nsNames, glue)
		if len(next) == 0 {
			next = resolveNSIPs(ctx, r, nsNames, nil)
		}
		if len(next) == 0 {
			// Last referral gave us the auth NS names even without an ANSWER section.
			del.NS = nsNames
			del.NSIPs = resolveNSIPs(ctx, r, nsNames, glue)
			return del, nil
		}
		servers = next
	}
	return del, errors.New("referral loop exceeded")
}

func (r *Resolver) exchangeAny(ctx context.Context, m *dns.Msg, servers []string) (*dns.Msg, error) {
	var lastErr error
	for _, s := range servers {
		resp, err := r.Ex.Exchange(ctx, m.Copy(), s)
		if err == nil && resp != nil {
			return resp, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = errors.New("all servers failed")
	}
	return nil, lastErr
}

func extractDelegation(resp *dns.Msg) (ns []string, glue map[string][]string) {
	glue = map[string][]string{}
	for _, rr := range resp.Ns {
		if n, ok := rr.(*dns.NS); ok {
			ns = append(ns, strings.ToLower(n.Ns))
		}
	}
	for _, rr := range resp.Extra {
		switch a := rr.(type) {
		case *dns.A:
			glue[strings.ToLower(a.Hdr.Name)] = append(glue[strings.ToLower(a.Hdr.Name)], a.A.String())
		case *dns.AAAA:
			glue[strings.ToLower(a.Hdr.Name)] = append(glue[strings.ToLower(a.Hdr.Name)], a.AAAA.String())
		}
	}
	return ns, glue
}

func answerNS(resp *dns.Msg, fqdn string) []string {
	var out []string
	for _, rr := range resp.Answer {
		if n, ok := rr.(*dns.NS); ok && strings.EqualFold(n.Hdr.Name, fqdn) {
			out = append(out, strings.ToLower(n.Ns))
		}
	}
	return out
}

func ipsFor(names []string, glue map[string][]string) []string {
	var out []string
	for _, n := range names {
		out = append(out, glue[strings.ToLower(n)]...)
	}
	return out
}

// resolveNSIPs resolves NS hostnames to IPs, using glue when available, else an A query via roots.
func resolveNSIPs(ctx context.Context, r *Resolver, names []string, glue map[string][]string) []string {
	seen := map[string]bool{}
	var out []string
	add := func(ip string) {
		if ip != "" && !seen[ip] {
			seen[ip] = true
			out = append(out, ip)
		}
	}
	for _, n := range names {
		if g := glue[strings.ToLower(n)]; len(g) > 0 {
			for _, ip := range g {
				add(ip)
			}
			continue
		}
		m := new(dns.Msg)
		m.SetQuestion(dns.Fqdn(n), dns.TypeA)
		if resp, err := r.exchangeAny(ctx, m, r.Roots); err == nil && resp != nil {
			for _, rr := range resp.Answer {
				if a, ok := rr.(*dns.A); ok {
					add(a.A.String())
				}
			}
		}
	}
	return out
}
```

Note on the walk: querying roots/TLDs for `NS domain` returns a referral (NS in AUTHORITY + glue in
ADDITIONAL) until we reach a server authoritative for the domain, which answers with NS in ANSWER.
Root/TLD responses are cached per run in Task 8 to avoid re-walking the same TLD for every domain.

- [ ] **Step 6: Run tests to verify they pass**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/resolve/`
Expected: PASS (`TestExchangeRoundTrip`).

- [ ] **Step 7: Commit**

```bash
cd commoncrawl/cc-dns-worker && go fmt ./... && go vet ./...
git add commoncrawl/cc-dns-worker/internal/resolve
git commit -m "feat(dns): iterative resolver — scheduled exchange + NS discovery walk"
```

---

### Task 6: Record querying + row assembly

**Files:**
- Create: `commoncrawl/cc-dns-worker/internal/resolve/query.go`
- Test: `commoncrawl/cc-dns-worker/internal/resolve/query_test.go`

**Interfaces:**
- Consumes: `Resolver` + `Exchanger` (Task 5), `records.Plan`/`records.Config` (Task 3),
  `model.DomainDNSRow` (Task 2).
- Produces:
  - `(*Resolver).QueryRecords(ctx, domain string, del Delegation, cfg records.Config) *ResultSet`
    where `ResultSet` holds every answer keyed by slot + a `Status map[string]string`.
  - `resolve.AssembleRow(domain, scanID, runID string, del Delegation, rs *ResultSet, now time.Time) model.DomainDNSRow`.

- [ ] **Step 1: Write the failing test**

Create `commoncrawl/cc-dns-worker/internal/resolve/query_test.go`:
```go
package resolve

import (
	"context"
	"testing"
	"time"

	"cc-dns-worker/internal/records"

	"github.com/miekg/dns"
)

// A stub Exchanger that answers from an in-memory table keyed by "qname/qtype".
type stubEx struct{ z map[string][]dns.RR }

func (s stubEx) Exchange(_ context.Context, m *dns.Msg, _ string) (*dns.Msg, error) {
	r := new(dns.Msg)
	r.SetReply(m)
	q := m.Question[0]
	r.Answer = append(r.Answer, s.z[q.Name+"/"+dns.TypeToString[q.Qtype]]...)
	return r, nil
}

func TestQueryRecordsAndAssemble(t *testing.T) {
	z := map[string][]dns.RR{
		"example.com./MX":                    {mx(t, "example.com. 300 IN MX 10 mail.example.com.")},
		"example.com./TXT":                   {txt(t, `example.com. 300 IN TXT "v=spf1 include:_spf.example.com ~all"`)},
		"_dmarc.example.com./TXT":            {txt(t, `_dmarc.example.com. 300 IN TXT "v=DMARC1; p=reject"`)},
		"www.example.com./A":                 {mustRR(t, "www.example.com. 300 IN A 1.2.3.4")},
		"google._domainkey.example.com./TXT": {txt(t, `google._domainkey.example.com. 300 IN TXT "v=DKIM1; k=rsa; p=MII"`)},
	}
	r := &Resolver{Ex: stubEx{z: z}}
	del := Delegation{ETLD: "com", NS: []string{"ns1.example.com."}, NSIPs: []string{"9.9.9.9"}}

	rs := r.QueryRecords(context.Background(), "example.com", del, records.DefaultConfig())
	row := AssembleRow("example.com", "2026-07-05", "run1", del, rs, time.Unix(0, 0).UTC())

	if len(row.MX) != 1 || row.MX[0] != "10 mail.example.com." {
		t.Errorf("MX = %v", row.MX)
	}
	if len(row.TXT) != 1 || row.DMARC == "" {
		t.Errorf("TXT=%v DMARC=%q", row.TXT, row.DMARC)
	}
	if got := row.A["www"]; len(got) != 1 || got[0] != "1.2.3.4" {
		t.Errorf("A[www] = %v", got)
	}
	if row.DKIM["google"] == "" {
		t.Errorf("missing DKIM google")
	}
	if row.RootDomain != "example.com" || row.ScanID != "2026-07-05" {
		t.Errorf("identity fields wrong: %+v", row)
	}
}

func mx(t *testing.T, s string) dns.RR   { return mustRR(t, s) }
func txt(t *testing.T, s string) dns.RR  { return mustRR(t, s) }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/resolve/ -run TestQueryRecords`
Expected: FAIL — `QueryRecords`, `AssembleRow`, `ResultSet` undefined.

- [ ] **Step 3: Write query + assembly**

Create `commoncrawl/cc-dns-worker/internal/resolve/query.go`:
```go
package resolve

import (
	"context"
	"strconv"
	"strings"

	"cc-dns-worker/internal/model"
	"cc-dns-worker/internal/records"

	"github.com/miekg/dns"
)

// ResultSet collects every Tier-2 answer for a domain, plus a per-query status map.
type ResultSet struct {
	A      map[string][]string // slot -> IPv4s
	AAAA   map[string][]string // slot -> IPv6s
	MX     []string
	TXT    []string
	NS     []string
	SOA    string
	CAA    []string
	DNSKEY []string
	DMARC  string
	DKIM   map[string]string
	MTASTS string
	TLSRPT string
	BIMI   string
	Status map[string]string
}

// QueryRecords runs every Tier-2 query against the domain's authoritative NS IPs (round-robin),
// recording each answer and its rcode/error. Falls back across NS IPs on failure.
func (r *Resolver) QueryRecords(ctx context.Context, domain string, del Delegation, cfg records.Config) *ResultSet {
	rs := &ResultSet{
		A: map[string][]string{}, AAAA: map[string][]string{}, DKIM: map[string]string{},
		Status: map[string]string{},
	}
	servers := del.NSIPs
	i := 0
	for _, q := range records.Plan(domain, cfg) {
		m := new(dns.Msg)
		m.SetQuestion(q.Name, q.Type)
		var resp *dns.Msg
		var err error
		// Round-robin start, fall back across the rest.
		for attempt := 0; attempt < len(servers); attempt++ {
			srv := servers[(i+attempt)%len(servers)]
			resp, err = r.Ex.Exchange(ctx, m.Copy(), srv)
			if err == nil && resp != nil {
				break
			}
		}
		i++
		key := q.Name + "/" + dns.TypeToString[q.Type]
		if err != nil || resp == nil {
			rs.Status[key] = "error"
			continue
		}
		rs.Status[key] = dns.RcodeToString[resp.Rcode]
		collect(rs, q, resp)
	}
	return rs
}

func collect(rs *ResultSet, q records.Query, resp *dns.Msg) {
	for _, rr := range resp.Answer {
		switch v := rr.(type) {
		case *dns.A:
			rs.A[q.Slot] = append(rs.A[q.Slot], v.A.String())
		case *dns.AAAA:
			rs.AAAA[q.Slot] = append(rs.AAAA[q.Slot], v.AAAA.String())
		case *dns.MX:
			rs.MX = append(rs.MX, strconv.Itoa(int(v.Preference))+" "+v.Mx)
		case *dns.NS:
			rs.NS = append(rs.NS, strings.ToLower(v.Ns))
		case *dns.SOA:
			rs.SOA = v.String()
		case *dns.CAA:
			rs.CAA = append(rs.CAA, v.String())
		case *dns.DNSKEY:
			rs.DNSKEY = append(rs.DNSKEY, v.String())
		case *dns.TXT:
			joined := strings.Join(v.Txt, "")
			switch q.Slot {
			case "dmarc":
				rs.DMARC = joined
			case "mta_sts":
				rs.MTASTS = joined
			case "tls_rpt":
				rs.TLSRPT = joined
			case "bimi":
				rs.BIMI = joined
			case "": // apex TXT
				rs.TXT = append(rs.TXT, joined)
			default: // DKIM selector
				rs.DKIM[q.Slot] = joined
			}
		}
	}
}

// AssembleRow flattens a Delegation + ResultSet into the stored row. now must be passed in (the
// caller stamps time; deterministic in tests).
func AssembleRow(domain, scanID, runID string, del Delegation, rs *ResultSet, now interface{ }) model.DomainDNSRow {
	panic("replaced below") // see real signature in Step 3b
}
```

- [ ] **Step 3b: Fix the AssembleRow signature (real code)**

Replace the placeholder `AssembleRow` at the bottom of `query.go` with:
```go
// AssembleRow flattens a Delegation + ResultSet into the stored row.
func AssembleRow(domain, scanID, runID string, del Delegation, rs *ResultSet, now time.Time) model.DomainDNSRow {
	b2u := func(b bool) uint8 {
		if b {
			return 1
		}
		return 0
	}
	return model.DomainDNSRow{
		ScanID:       scanID,
		RootDomain:   domain,
		ETLD:         del.ETLD,
		Nameservers:  del.NS,
		NSIPs:        del.NSIPs,
		A:            rs.A,
		AAAA:         rs.AAAA,
		MX:           rs.MX,
		TXT:          rs.TXT,
		DMARC:        rs.DMARC,
		DKIM:         rs.DKIM,
		MTASTS:       rs.MTASTS,
		TLSRPT:       rs.TLSRPT,
		BIMI:         rs.BIMI,
		CAA:          rs.CAA,
		SOA:          rs.SOA,
		DNSSECSigned: b2u(len(rs.DNSKEY) > 0),
		DSPresent:    b2u(len(del.DS) > 0),
		DNSKEY:       rs.DNSKEY,
		DS:           del.DS,
		QueryStatus:  rs.Status,
		ResolverPath: "iterative",
		SourceRunID:  runID,
		ResolvedAt:   now,
	}
}
```
Add `"time"` to the imports of `query.go` and delete the placeholder `AssembleRow` (the one that
panics). The `interface{}` version was scaffolding only.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/resolve/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd commoncrawl/cc-dns-worker && go fmt ./... && go vet ./...
git add commoncrawl/cc-dns-worker/internal/resolve
git commit -m "feat(dns): Tier-2 record querying and row assembly"
```

---

### Task 7: Rolling Parquet output writer

**Files:**
- Create: `commoncrawl/cc-dns-worker/internal/output/output.go`
- Test: `commoncrawl/cc-dns-worker/internal/output/output_test.go`

**Interfaces:**
- Consumes: `model.DomainDNSRow` (Task 2).
- Produces:
  - `output.NewWriter(path string) (*Writer, error)` — writes `dns.parquet` at path (dir or file;
    if dir, filename is `dns.parquet`).
  - `(*Writer).Write(row model.DomainDNSRow) error`
  - `(*Writer).Close() error`

- [ ] **Step 1: Write the failing test**

Create `commoncrawl/cc-dns-worker/internal/output/output_test.go`:
```go
package output

import (
	"path/filepath"
	"testing"
	"time"

	"cc-dns-worker/internal/model"

	"github.com/parquet-go/parquet-go"
)

func TestWriteAndReadBack(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	row := model.DomainDNSRow{
		ScanID: "2026-07-05", RootDomain: "example.com", ETLD: "com",
		A:      map[string][]string{"@": {"1.2.3.4"}},
		MX:     []string{"10 mail.example.com."},
		DKIM:   map[string]string{"google": "v=DKIM1"},
		Status: nil,
		ResolvedAt: time.Unix(0, 0).UTC(),
	}
	row.QueryStatus = map[string]string{"example.com./MX": "NOERROR"}
	if err := w.Write(row); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	rows, err := parquet.ReadFile[model.DomainDNSRow](filepath.Join(dir, "dns.parquet"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(rows) != 1 || rows[0].RootDomain != "example.com" || rows[0].A["@"][0] != "1.2.3.4" {
		t.Fatalf("round-trip mismatch: %+v", rows)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/output/`
Expected: FAIL — `NewWriter` undefined.

- [ ] **Step 3: Write the writer**

Create `commoncrawl/cc-dns-worker/internal/output/output.go`:
```go
// Package output writes DomainDNSRow records to a single dns.parquet file (one row per domain).
package output

import (
	"os"
	"path/filepath"

	"cc-dns-worker/internal/model"

	"github.com/parquet-go/parquet-go"
)

// Writer streams rows into dns.parquet.
type Writer struct {
	f  *os.File
	pw *parquet.GenericWriter[model.DomainDNSRow]
}

// NewWriter creates dns.parquet. If path is an existing dir (or has no .parquet suffix), the file
// is path/dns.parquet; otherwise path is used verbatim.
func NewWriter(path string) (*Writer, error) {
	target := path
	if filepath.Ext(path) != ".parquet" {
		target = filepath.Join(path, "dns.parquet")
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return nil, err
	}
	f, err := os.Create(target)
	if err != nil {
		return nil, err
	}
	return &Writer{f: f, pw: parquet.NewGenericWriter[model.DomainDNSRow](f)}, nil
}

// Write appends one row.
func (w *Writer) Write(row model.DomainDNSRow) error {
	_, err := w.pw.Write([]model.DomainDNSRow{row})
	return err
}

// Close flushes the parquet footer and closes the file.
func (w *Writer) Close() error {
	if err := w.pw.Close(); err != nil {
		_ = w.f.Close()
		return err
	}
	return w.f.Close()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/output/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd commoncrawl/cc-dns-worker && go fmt ./... && go vet ./...
git add commoncrawl/cc-dns-worker/internal/output
git commit -m "feat(dns): dns.parquet output writer"
```

---

### Task 8: Domain input reader + scan orchestration wiring

**Files:**
- Create: `commoncrawl/cc-dns-worker/internal/input/input.go`
- Test: `commoncrawl/cc-dns-worker/internal/input/input_test.go`
- Modify: `commoncrawl/cc-dns-worker/cmd/cc-dns-worker/scan.go`

**Interfaces:**
- Consumes: everything above; `github.com/ClickHouse/clickhouse-go/v2`.
- Produces:
  - `input.FromClickHouse(ctx, conn driver.Conn, query string, limit int) ([]string, error)` —
    returns distinct root_domains.
  - `input.DefaultQuery = "SELECT DISTINCT root_domain FROM corpscout.commoncrawl_domains"`.
  - Wired `runScan` that resolves all domains and writes `dns.parquet`.

- [ ] **Step 1: Write the failing test (query builder is pure/testable)**

Create `commoncrawl/cc-dns-worker/internal/input/input_test.go`:
```go
package input

import "testing"

func TestApplyLimit(t *testing.T) {
	got := applyLimit("SELECT DISTINCT root_domain FROM corpscout.commoncrawl_domains", 100)
	want := "SELECT DISTINCT root_domain FROM corpscout.commoncrawl_domains LIMIT 100"
	if got != want {
		t.Errorf("got %q want %q", got, want)
	}
	if applyLimit("SELECT 1", 0) != "SELECT 1" {
		t.Errorf("limit 0 should be a no-op")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/input/`
Expected: FAIL — `applyLimit` undefined.

- [ ] **Step 3: Write the input reader**

Create `commoncrawl/cc-dns-worker/internal/input/input.go`:
```go
// Package input loads the list of root domains to scan from ClickHouse.
package input

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// DefaultQuery selects every distinct domain known to the commoncrawl pipeline.
const DefaultQuery = "SELECT DISTINCT root_domain FROM corpscout.commoncrawl_domains"

func applyLimit(q string, limit int) string {
	if limit <= 0 {
		return q
	}
	return fmt.Sprintf("%s LIMIT %d", q, limit)
}

// FromClickHouse runs query (default DefaultQuery) and returns the root_domain column.
func FromClickHouse(ctx context.Context, conn driver.Conn, query string, limit int) ([]string, error) {
	if query == "" {
		query = DefaultQuery
	}
	rows, err := conn.Query(ctx, applyLimit(query, limit))
	if err != nil {
		return nil, fmt.Errorf("query domains: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			return nil, err
		}
		if d != "" {
			out = append(out, d)
		}
	}
	return out, rows.Err()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/input/`
Expected: PASS.

- [ ] **Step 5: Wire runScan**

Replace `commoncrawl/cc-dns-worker/cmd/cc-dns-worker/scan.go` with:
```go
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"cc-dns-worker/internal/input"
	"cc-dns-worker/internal/output"
	"cc-dns-worker/internal/records"
	"cc-dns-worker/internal/resolve"
	"cc-dns-worker/internal/scheduler"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func runScan(args []string) error {
	fs := flag.NewFlagSet("scan", flag.ExitOnError)
	scanID := fs.String("scan-id", time.Now().UTC().Format("2006-01-02"), "scan batch id (default today, UTC)")
	runID := fs.String("run-id", "", "source run id (defaults to scan-id)")
	out := fs.String("out", ".", "output dir or dns.parquet path")
	query := fs.String("query", input.DefaultQuery, "ClickHouse query returning root_domain")
	limit := fs.Int("limit", 0, "cap number of domains (0 = all)")
	qps := fs.Float64("per-server-qps", 10, "max queries/sec per server IP")
	inflight := fs.Int("per-server-inflight", 3, "max concurrent queries per server IP")
	domainConc := fs.Int("domain-concurrency", 5000, "max domains resolved concurrently")
	timeout := fs.Duration("query-timeout", 5*time.Second, "per-query timeout")
	_ = fs.Parse(args)
	if *runID == "" {
		*runID = *scanID
	}

	ctx := context.Background()
	conn, err := chConn()
	if err != nil {
		return err
	}
	defer conn.Close()

	domains, err := input.FromClickHouse(ctx, conn, *query, *limit)
	if err != nil {
		return err
	}
	log.Printf("scanning %d domains (scan_id=%s)", len(domains), *scanID)

	sched := scheduler.New(scheduler.Config{PerServerQPS: *qps, Burst: int(*qps), MaxInFlight: *inflight})
	ex := resolve.NewExchanger(sched, *timeout)
	res := resolve.NewResolver(ex)
	cfg := records.DefaultConfig()

	w, err := output.NewWriter(*out)
	if err != nil {
		return err
	}
	var wmu sync.Mutex

	sem := make(chan struct{}, *domainConc)
	var wg sync.WaitGroup
	var done int64
	var dmu sync.Mutex
	for _, d := range domains {
		sem <- struct{}{}
		wg.Add(1)
		go func(domain string) {
			defer wg.Done()
			defer func() { <-sem }()
			del, err := res.DiscoverNS(ctx, domain)
			var rs *resolve.ResultSet
			if err == nil && len(del.NSIPs) > 0 {
				rs = res.QueryRecords(ctx, domain, del, cfg)
			} else {
				rs = &resolve.ResultSet{
					A: map[string][]string{}, AAAA: map[string][]string{},
					DKIM: map[string]string{}, Status: map[string]string{"_discover": errString(err)},
				}
			}
			row := resolve.AssembleRow(domain, *scanID, *runID, del, rs, time.Now().UTC())
			wmu.Lock()
			werr := w.Write(row)
			wmu.Unlock()
			if werr != nil {
				log.Printf("write %s: %v", domain, werr)
			}
			dmu.Lock()
			done++
			if done%1000 == 0 {
				log.Printf("resolved %d/%d", done, len(domains))
			}
			dmu.Unlock()
		}(d)
	}
	wg.Wait()
	if err := w.Close(); err != nil {
		return err
	}
	log.Printf("done: %d domains -> %s", len(domains), *out)
	return nil
}

func errString(err error) string {
	if err == nil {
		return "ok"
	}
	return err.Error()
}

// chConn connects to ClickHouse from CLICKHOUSE_* env (ADDR host:port, DB, USER, PASSWORD).
func chConn() (driver.Conn, error) {
	addr := os.Getenv("CLICKHOUSE_ADDR")
	if addr == "" {
		addr = "localhost:9000"
	}
	return clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: envOr("CLICKHOUSE_DB", "corpscout"),
			Username: envOr("CLICKHOUSE_USER", "default"),
			Password: os.Getenv("CLICKHOUSE_PASSWORD"),
		},
	})
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

var _ = fmt.Sprint
```

- [ ] **Step 6: Verify build**

Run: `cd commoncrawl/cc-dns-worker && go build ./... && go vet ./...`
Expected: exit 0.

- [ ] **Step 7: Commit**

```bash
cd commoncrawl/cc-dns-worker && go fmt ./...
git add commoncrawl/cc-dns-worker
git commit -m "feat(dns): domain input reader and scan orchestration"
```

---

### Task 9: ClickHouse migration + load subcommand

**Files:**
- Create: `clickhouse/migrations/0000NN_corpscout_commoncrawl_domain_dns.up.sql` (use the next free
  migration number — check `ls clickhouse/migrations/ | tail`)
- Create: `clickhouse/migrations/0000NN_corpscout_commoncrawl_domain_dns.down.sql`
- Create: `commoncrawl/cc-dns-worker/internal/load/load.go`
- Modify: `commoncrawl/cc-dns-worker/cmd/cc-dns-worker/load.go`
- Test: `commoncrawl/cc-dns-worker/internal/load/load_test.go`

**Interfaces:**
- Consumes: `model.DomainDNSRow`, the CH connection helper from Task 8.
- Produces:
  - `load.FromFile(ctx, conn driver.Conn, path string) (int, error)` — reads `dns.parquet`, inserts
    into `corpscout.commoncrawl_domain_dns`.
  - Wired `runLoad`.

- [ ] **Step 1: Write the migration**

Create `clickhouse/migrations/0000NN_corpscout_commoncrawl_domain_dns.up.sql` (copy the schema from
the spec §5 verbatim):
```sql
CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_dns
(
    scan_id        LowCardinality(String),
    root_domain    String,
    etld           LowCardinality(String),
    nameservers    Array(String),
    ns_ips         Array(String),
    a              Map(LowCardinality(String), Array(String)),
    aaaa           Map(LowCardinality(String), Array(String)),
    mx             Array(String),
    txt            Array(String),
    dmarc          String,
    dkim           Map(LowCardinality(String), String),
    mta_sts        String,
    tls_rpt        String,
    bimi           String,
    caa            Array(String),
    soa            String,
    dnssec_signed  UInt8,
    ds_present     UInt8,
    dnskey         Array(String),
    ds             Array(String),
    query_status   Map(LowCardinality(String), String),
    resolver_path  LowCardinality(String),
    source_run_id  String,
    resolved_at    DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (root_domain, scan_id);
```

Create `clickhouse/migrations/0000NN_corpscout_commoncrawl_domain_dns.down.sql`:
```sql
DROP TABLE IF EXISTS corpscout.commoncrawl_domain_dns;
```

- [ ] **Step 2: Write the failing load test (column list is pure/testable)**

Create `commoncrawl/cc-dns-worker/internal/load/load_test.go`:
```go
package load

import (
	"strings"
	"testing"

	"cc-dns-worker/internal/model"
)

func TestInsertColumnsMatchModel(t *testing.T) {
	cols := chColumns[model.DomainDNSRow]()
	joined := strings.Join(cols, ",")
	for _, must := range []string{"scan_id", "root_domain", "a", "dkim", "query_status", "resolved_at"} {
		if !strings.Contains(joined, must) {
			t.Errorf("column list missing %q: %s", must, joined)
		}
	}
	if len(cols) < 20 {
		t.Errorf("expected all model columns, got %d", len(cols))
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/load/`
Expected: FAIL — `chColumns` undefined.

- [ ] **Step 4: Write the loader (mirror cc-enrich-worker/internal/load)**

Create `commoncrawl/cc-dns-worker/internal/load/load.go`:
```go
// Package load inserts dns.parquet into corpscout.commoncrawl_domain_dns over the native protocol.
package load

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"cc-dns-worker/internal/model"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/parquet-go/parquet-go"
)

const table = "corpscout.commoncrawl_domain_dns"

// chColumns returns the ch tag of each field of T in declaration order.
func chColumns[T any]() []string {
	rt := reflect.TypeOf(*new(T))
	cols := make([]string, 0, rt.NumField())
	for i := 0; i < rt.NumField(); i++ {
		if c := rt.Field(i).Tag.Get("ch"); c != "" {
			cols = append(cols, c)
		}
	}
	return cols
}

// FromFile reads dns.parquet and batch-inserts every row.
func FromFile(ctx context.Context, conn driver.Conn, path string) (int, error) {
	rows, err := parquet.ReadFile[model.DomainDNSRow](path)
	if err != nil {
		return 0, fmt.Errorf("read %s: %w", path, err)
	}
	if len(rows) == 0 {
		return 0, nil
	}
	query := "INSERT INTO " + table + " (" + strings.Join(chColumns[model.DomainDNSRow](), ", ") + ")"
	batch, err := conn.PrepareBatch(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("prepare: %w", err)
	}
	for i := range rows {
		if err := batch.AppendStruct(&rows[i]); err != nil {
			_ = batch.Abort()
			return 0, fmt.Errorf("append row %d: %w", i, err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send: %w", err)
	}
	return len(rows), nil
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/load/`
Expected: PASS.

- [ ] **Step 6: Wire runLoad**

Replace `commoncrawl/cc-dns-worker/cmd/cc-dns-worker/load.go` with:
```go
package main

import (
	"context"
	"flag"
	"fmt"

	"cc-dns-worker/internal/load"
)

func runLoad(args []string) error {
	fs := flag.NewFlagSet("load", flag.ExitOnError)
	path := fs.String("file", "dns.parquet", "path to dns.parquet")
	_ = fs.Parse(args)

	ctx := context.Background()
	conn, err := chConn()
	if err != nil {
		return err
	}
	defer conn.Close()

	n, err := load.FromFile(ctx, conn, *path)
	if err != nil {
		return err
	}
	fmt.Printf("loaded %d rows into corpscout.commoncrawl_domain_dns\n", n)
	return nil
}
```

- [ ] **Step 7: Verify build + tests**

Run: `cd commoncrawl/cc-dns-worker && go build ./... && go test ./... && go vet ./...`
Expected: build + all unit tests pass.

- [ ] **Step 8: Commit**

```bash
cd commoncrawl/cc-dns-worker && go fmt ./...
git add commoncrawl/cc-dns-worker clickhouse/migrations
git commit -m "feat(dns): commoncrawl_domain_dns migration and load subcommand"
```

---

### Task 10: Real-DNS smoke test + ClickHouse load integration test

**Files:**
- Create: `commoncrawl/cc-dns-worker/internal/resolve/smoke_test.go` (build tag `//go:build integration`)
- Create: `commoncrawl/cc-dns-worker/internal/load/integration_test.go` (build tag `//go:build integration`)

**Interfaces:**
- Consumes: the full stack; a reachable network (smoke) and a real ClickHouse (load).

- [ ] **Step 1: Write the real-DNS smoke test**

Create `commoncrawl/cc-dns-worker/internal/resolve/smoke_test.go`:
```go
//go:build integration

package resolve

import (
	"context"
	"testing"
	"time"

	"cc-dns-worker/internal/records"
	"cc-dns-worker/internal/scheduler"
)

// Resolves real, stable domains straight from authoritative servers. Requires outbound UDP/53.
func TestSmokeRealDomains(t *testing.T) {
	s := scheduler.New(scheduler.Config{PerServerQPS: 10, Burst: 10, MaxInFlight: 3})
	r := NewResolver(NewExchanger(s, 5*time.Second))
	ctx := context.Background()

	del, err := r.DiscoverNS(ctx, "cloudflare.com")
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if len(del.NS) == 0 || len(del.NSIPs) == 0 {
		t.Fatalf("no NS learned: %+v", del)
	}
	rs := r.QueryRecords(ctx, "cloudflare.com", del, records.DefaultConfig())
	if len(rs.MX) == 0 {
		t.Errorf("expected MX for cloudflare.com")
	}
	if len(rs.A["@"]) == 0 && len(rs.A["www"]) == 0 {
		t.Errorf("expected some A records")
	}
}
```

- [ ] **Step 2: Run the smoke test**

Run: `cd commoncrawl/cc-dns-worker && go test -tags=integration ./internal/resolve/ -run TestSmokeRealDomains -v`
Expected: PASS when network is available. (If the environment blocks UDP/53, document it as skipped —
do NOT weaken the assertions.)

- [ ] **Step 3: Write the ClickHouse load integration test**

Create `commoncrawl/cc-dns-worker/internal/load/integration_test.go`:
```go
//go:build integration

package load

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cc-dns-worker/internal/model"
	"cc-dns-worker/internal/output"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func testConn(t *testing.T) driver.Conn {
	t.Helper()
	addr := os.Getenv("CLICKHOUSE_ADDR")
	if addr == "" {
		addr = "localhost:9000"
	}
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{Database: "corpscout", Username: envOr("CLICKHOUSE_USER", "default"), Password: os.Getenv("CLICKHOUSE_PASSWORD")},
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	return conn
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

// Writes a row to parquet, loads it, and reads it back from ClickHouse. Requires the migration
// applied to a reachable ClickHouse.
func TestLoadRoundTrip(t *testing.T) {
	ctx := context.Background()
	conn := testConn(t)
	defer conn.Close()

	dir := t.TempDir()
	w, err := output.NewWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	row := model.DomainDNSRow{
		ScanID: "itest", RootDomain: "example.test", ETLD: "test",
		A:      map[string][]string{"@": {"1.2.3.4"}},
		DKIM:   map[string]string{"google": "v=DKIM1"},
		QueryStatus: map[string]string{"example.test./MX": "NOERROR"},
		ResolverPath: "iterative", ResolvedAt: time.Now().UTC(),
	}
	if err := w.Write(row); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	n, err := FromFile(ctx, conn, filepath.Join(dir, "dns.parquet"))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if n != 1 {
		t.Fatalf("loaded %d rows, want 1", n)
	}

	var got string
	if err := conn.QueryRow(ctx,
		"SELECT root_domain FROM corpscout.commoncrawl_domain_dns FINAL WHERE scan_id = 'itest' LIMIT 1",
	).Scan(&got); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if got != "example.test" {
		t.Errorf("got %q", got)
	}
	_ = conn.Exec(ctx, "DELETE FROM corpscout.commoncrawl_domain_dns WHERE scan_id = 'itest'")
}
```

- [ ] **Step 4: Apply the migration and run the load integration test**

Run:
```bash
# apply the migration (adjust to the repo's migration runner / clickhouse-client)
clickhouse-client --host "${CLICKHOUSE_HOST:-localhost}" --multiquery < clickhouse/migrations/0000NN_corpscout_commoncrawl_domain_dns.up.sql
cd commoncrawl/cc-dns-worker && go test -tags=integration ./internal/load/ -run TestLoadRoundTrip -v
```
Expected: PASS (1 row loaded and read back).

- [ ] **Step 5: Commit**

```bash
cd commoncrawl/cc-dns-worker && go fmt ./...
git add commoncrawl/cc-dns-worker
git commit -m "test(dns): real-DNS smoke and ClickHouse load integration tests"
```

---

### Task 11: End-to-end scan smoke + README

**Files:**
- Create: `commoncrawl/cc-dns-worker/README.md`

**Interfaces:** none new — this validates the wired binary against a tiny real slice.

- [ ] **Step 1: Run a bounded real scan against ClickHouse**

Run:
```bash
cd commoncrawl/cc-dns-worker && go build -o bin/cc-dns-worker ./cmd/cc-dns-worker
CLICKHOUSE_ADDR=localhost:9000 ./bin/cc-dns-worker scan --limit 20 --out /tmp/dnsout --scan-id smoke
```
Expected: log `scanning 20 domains`, then `done: 20 domains -> /tmp/dnsout`, and
`/tmp/dnsout/dns.parquet` exists.

- [ ] **Step 2: Load the smoke output and verify**

Run:
```bash
CLICKHOUSE_ADDR=localhost:9000 ./bin/cc-dns-worker load --file /tmp/dnsout/dns.parquet
clickhouse-client -q "SELECT count(), uniqExact(root_domain) FROM corpscout.commoncrawl_domain_dns FINAL WHERE scan_id='smoke'"
```
Expected: `loaded 20 rows`; count and uniqExact both ~20 (some domains may share; count == rows).

- [ ] **Step 3: Write the README**

Create `commoncrawl/cc-dns-worker/README.md` documenting: purpose, the two-tier per-server rate
model, `scan`/`load` usage and flags, the CH table, env vars (`CLICKHOUSE_*`), and the deferred
items (Redis scale-out, CT-log hostnames, DNSSEC validation, raw record table). Keep it aligned with
`commoncrawl/cc-enrich-worker/README.md` in tone.

- [ ] **Step 4: Clean up smoke data and commit**

```bash
clickhouse-client -q "DELETE FROM corpscout.commoncrawl_domain_dns WHERE scan_id='smoke'"
cd commoncrawl/cc-dns-worker
git add README.md
git commit -m "docs(dns): cc-dns-worker README and end-to-end smoke notes"
```

---

## Deferred (documented, not built in v1)
- **Redis-backed distributed scheduler** via shard-by-server-IP (spec §3.3) — only when one box is
  too slow.
- **CT-log hostname discovery** feeding the hostname list (spec §2) — replaces the static 5.
- **DNSSEC chain validation** — v1 only captures DNSKEY/DS/RRSIG presence.
- **Raw long record table** `commoncrawl_domain_dns_records` (spec §5 open decision) — add if
  per-record TTL/RRSIG fidelity is needed.
- **dagster asset** wrapping `scan`+`load` on an every-few-days partitioned schedule (spec §8).

## Self-review notes
- Spec §2 record families → Task 3 (plan) + Task 6 (collect) + smoke Task 10. ✓
- Spec §3 rate model → Task 4 (scheduler) + Task 5 (exchange routes through it). ✓
- Spec §3.4 iterative walk → Task 5 (discover). ✓
- Spec §5 schema → Task 9 migration + Task 2 model (columns match). ✓
- Spec §7 testing (deterministic in-process server, real-DNS smoke, CH load) → Tasks 5/10. ✓
- Global constraint parquet==ch tag pin → Task 2 test. ✓
