# CommonCrawl DNS Scanner Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Go worker (`cc-dns-worker`) that reads domains from ClickHouse, resolves a rich DNS record set directly from each domain's authoritative nameservers under strict per-server rate limits, stages results in a durable embedded SQLite database (crash-resumable), and loads them into two normalized, historical ClickHouse tables.

**Architecture:** Two-tier DNS with `miekg/dns`. **Tier 1 (NS discovery)** sends recursive (RD=1) queries to configured recursive resolvers (default public: 1.1.1.1/8.8.8.8/9.9.9.9; override with a local `unbound` at 127.0.0.1:53) to learn each domain's authoritative NS names and IPs and its parent DS — the resolver's cache absorbs the root/TLD load, so we never walk roots or hammer TLD servers. **Tier 2 (record queries)** goes **directly to the domain's authoritative NS IPs** (RD=0), where a token-bucket limiter per NS-IP (default ~10 qps) enforces politeness — this is where per-server rate limiting matters (small domains on their own nameserver). Single node, no Redis: limiters are in-memory; the work queue + per-domain status + staged records live in an embedded SQLite `scan.db` written by one dedicated writer goroutine, so a killed run resumes the remaining `pending` domains. A `load` subcommand bulk-copies SQLite → ClickHouse over the native protocol into `commoncrawl_domain_dns_records` (one row per record) and `commoncrawl_domain_dns_scan` (per-domain summary/status).

> **Reuse note:** `pulsarprotectrunner2/pkg/dns` already implements a recursive DNS engine with health tracking, racing, TCP fallback, and `GetAuthoritativeNameservers`. We model the lean discovery client below on it but do **not** import it — that module (`github.com/pulsarpoint/pulsarprotectrunner2.git`) pulls a heavy dep tree (geoip, otel, ip_analysis) unsuitable for this standalone worker. If reuse is later preferred over the ~80-line lean client, extract `pkg/dns` into a shared module first.

**Tech Stack:** Go 1.24, `github.com/miekg/dns`, `golang.org/x/time/rate`, `golang.org/x/net/publicsuffix`, `modernc.org/sqlite` (pure-Go, no cgo), `github.com/ClickHouse/clickhouse-go/v2`.

**Spec:** `docs/superpowers/specs/2026-07-05-commoncrawl-dns-scanner-design.md`

## Global Constraints

- Go module name: `cc-dns-worker`; all internal imports are `cc-dns-worker/internal/...`.
- Go version floor: `go 1.25.0` in `go.mod` (raised from 1.24 during Task 4 — `golang.org/x/time v0.15.0`, the pinned rate-limiter dep, declares `go 1.25.0`, which the toolchain propagates to the module; verified via `go list -m`).
- SQLite driver: `modernc.org/sqlite` (pure Go, no cgo), registered `database/sql` driver name `"sqlite"`. Open with `?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)`.
- CH load structs carry a `ch:"col"` tag on every stored field naming the exact CH column; a struct-tag test pins the expected column set.
- **Two tiers:** Tier-1 NS discovery uses recursive resolvers (`--resolvers`, default `1.1.1.1:53,8.8.8.8:53,9.9.9.9:53`; set a local `unbound` like `127.0.0.1:53` for large runs). Tier-2 record queries go directly to the domain's authoritative NS IPs with `RecursionDesired=false`.
- Default per-authoritative-NS rate: `10` qps, burst `10`; per-NS in-flight cap `3`. Default discovery rate: `50` qps per resolver (bump high for a local unbound). All configurable via flags.
- Retries: each query is retried across the available servers (Tier-1: rotate resolvers, 2 attempts each; Tier-2: rotate the domain's NS IPs); the per-server limiter spaces retries to the same server. No explicit sleep-backoff in v1 (a per-server circuit breaker is deferred — see end).
- Default hostnames (5; apex always resolved separately, apex stored under slot `@`): `www, mail, webmail, smtp, autodiscover`.
- Default DKIM selectors (10): `default, google, selector1, selector2, k1, dkim, s1, s2, mail, mandrill`.
- EDNS0 UDP buffer size `1232`, DO bit set on every query (needed for DS/DNSKEY capture).
- ClickHouse database `corpscout`; new tables `corpscout.commoncrawl_domain_dns_records` and `corpscout.commoncrawl_domain_dns_scan`.
- Resume contract: a domain is flipped to `done`/`error` in one SQLite transaction only after its results are written; a crash leaves unfinished domains `pending`, and re-running `scan` processes exactly the not-`done`/not-`error` set.
- Follow Conventional Commits. Run `go fmt ./...` and `go vet ./...` before each commit. Working dir for the module: `commoncrawl/cc-dns-worker/`.

---

### Task 1: Scaffold the Go module and CLI skeleton

**Files:**
- Create: `commoncrawl/cc-dns-worker/go.mod`
- Create: `commoncrawl/cc-dns-worker/cmd/cc-dns-worker/main.go`
- Create: `commoncrawl/cc-dns-worker/cmd/cc-dns-worker/scan.go`
- Create: `commoncrawl/cc-dns-worker/cmd/cc-dns-worker/load.go`
- Create: `commoncrawl/cc-dns-worker/Makefile`

**Interfaces:**
- Produces: a buildable binary with `scan` and `load` subcommand stubs.

- [ ] **Step 1: Create the module + deps**

Run:
```bash
cd commoncrawl/cc-dns-worker && go mod init cc-dns-worker && \
go get github.com/miekg/dns@latest golang.org/x/time/rate@latest golang.org/x/net/publicsuffix@latest \
       modernc.org/sqlite@latest github.com/ClickHouse/clickhouse-go/v2@latest
```
Expected: `go.mod`/`go.sum` created; ensure the `go` line reads `go 1.24` (edit if the toolchain wrote a higher floor).

- [ ] **Step 2: Write the CLI skeleton**

Create `commoncrawl/cc-dns-worker/cmd/cc-dns-worker/main.go`:
```go
// Command cc-dns-worker resolves DNS for corpscout domains directly from authoritative
// nameservers, stages results in SQLite (resumable), and loads them into ClickHouse.
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
  scan   resolve domains from ClickHouse into a durable SQLite stage (resumable)
  load   bulk-copy the SQLite stage into corpscout ClickHouse tables

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
Expected: exit 0, no output.

- [ ] **Step 5: Commit**

```bash
cd commoncrawl/cc-dns-worker && go fmt ./...
git add commoncrawl/cc-dns-worker
git commit -m "feat(dns): scaffold cc-dns-worker module and CLI skeleton"
```

---

### Task 2: Domain-result and CH-load models

**Files:**
- Create: `commoncrawl/cc-dns-worker/internal/model/model.go`
- Test: `commoncrawl/cc-dns-worker/internal/model/model_test.go`

**Interfaces:**
- Produces:
  - `model.DNSRecord{ Name, RecordType, Slot, Value, Rcode string; TTL uint32; Priority uint16 }`
  - `model.DomainResult{ ScanID, RootDomain, ETLD string; Nameservers, NSIPs []string;
    DNSSECSigned, DSPresent bool; Status, Error string; QueriesTotal, QueriesOK int;
    Records []DNSRecord; SourceRunID string; ResolvedAt time.Time }` — what a resolver emits and the
    store commits (Tasks 6, 7, 8).
  - `model.RecordRow` / `model.ScanRow` — CH-load structs (ch tags), used by load (Task 9).

- [ ] **Step 1: Write the failing test**

Create `commoncrawl/cc-dns-worker/internal/model/model_test.go`:
```go
package model

import (
	"reflect"
	"testing"
)

func chCols(v any) map[string]bool {
	rt := reflect.TypeOf(v)
	out := map[string]bool{}
	for i := 0; i < rt.NumField(); i++ {
		if c := rt.Field(i).Tag.Get("ch"); c != "" {
			out[c] = true
		}
	}
	return out
}

func TestRecordRowColumns(t *testing.T) {
	cols := chCols(RecordRow{})
	for _, c := range []string{"scan_id", "root_domain", "name", "record_type", "slot", "value", "ttl", "priority", "rcode", "source_run_id", "resolved_at"} {
		if !cols[c] {
			t.Errorf("RecordRow missing ch column %q", c)
		}
	}
}

func TestScanRowColumns(t *testing.T) {
	cols := chCols(ScanRow{})
	for _, c := range []string{"scan_id", "root_domain", "etld", "nameservers", "ns_ips", "dnssec_signed", "ds_present", "status", "error", "queries_total", "queries_ok", "source_run_id", "resolved_at"} {
		if !cols[c] {
			t.Errorf("ScanRow missing ch column %q", c)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/model/`
Expected: FAIL — `RecordRow`, `ScanRow` undefined.

- [ ] **Step 3: Write the model**

Create `commoncrawl/cc-dns-worker/internal/model/model.go`:
```go
// Package model holds the in-memory result a resolver emits (DomainResult/DNSRecord) and the
// ClickHouse-load row structs (RecordRow/ScanRow) whose ch tags name the target columns.
package model

import "time"

// DNSRecord is one resolved resource record, stored verbatim.
type DNSRecord struct {
	Name       string // qname queried (FQDN without trailing dot)
	RecordType string // A, AAAA, MX, TXT, NS, SOA, CAA, DNSKEY, DS
	Slot       string // "@", hostname, DKIM selector, "dmarc"/"mta_sts"/"tls_rpt"/"bimi", or ""
	Value      string // rdata verbatim
	Rcode      string // query rcode for the query that produced this record
	TTL        uint32
	Priority   uint16 // MX preference; 0 otherwise
}

// DomainResult is everything learned for one domain in one scan.
type DomainResult struct {
	ScanID       string
	RootDomain   string
	ETLD         string
	Nameservers  []string
	NSIPs        []string
	DNSSECSigned bool
	DSPresent    bool
	Status       string // "done" | "error"
	Error        string
	QueriesTotal int
	QueriesOK    int
	Records      []DNSRecord
	SourceRunID  string
	ResolvedAt   time.Time
}

// RecordRow mirrors corpscout.commoncrawl_domain_dns_records.
type RecordRow struct {
	ScanID      string    `ch:"scan_id"`
	RootDomain  string    `ch:"root_domain"`
	Name        string    `ch:"name"`
	RecordType  string    `ch:"record_type"`
	Slot        string    `ch:"slot"`
	Value       string    `ch:"value"`
	TTL         uint32    `ch:"ttl"`
	Priority    uint16    `ch:"priority"`
	Rcode       string    `ch:"rcode"`
	SourceRunID string    `ch:"source_run_id"`
	ResolvedAt  time.Time `ch:"resolved_at"`
}

// ScanRow mirrors corpscout.commoncrawl_domain_dns_scan.
type ScanRow struct {
	ScanID       string    `ch:"scan_id"`
	RootDomain   string    `ch:"root_domain"`
	ETLD         string    `ch:"etld"`
	Nameservers  []string  `ch:"nameservers"`
	NSIPs        []string  `ch:"ns_ips"`
	DNSSECSigned uint8     `ch:"dnssec_signed"`
	DSPresent    uint8     `ch:"ds_present"`
	Status       string    `ch:"status"`
	Error        string    `ch:"error"`
	QueriesTotal uint16    `ch:"queries_total"`
	QueriesOK    uint16    `ch:"queries_ok"`
	SourceRunID  string    `ch:"source_run_id"`
	ResolvedAt   time.Time `ch:"resolved_at"`
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/model/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd commoncrawl/cc-dns-worker && go fmt ./... && go vet ./...
git add commoncrawl/cc-dns-worker/internal/model
git commit -m "feat(dns): DomainResult + normalized RecordRow/ScanRow models"
```

---

### Task 3: Query planning (hostnames, DKIM selectors, qnames)

**Files:**
- Create: `commoncrawl/cc-dns-worker/internal/records/plan.go`
- Test: `commoncrawl/cc-dns-worker/internal/records/plan_test.go`

**Interfaces:**
- Produces:
  - `records.Config{ Hostnames []string; DKIMSelectors []string }`, `records.DefaultConfig() Config`
  - `records.Query{ Name string; Type uint16; Slot string }`
  - `records.Plan(domain string, cfg Config) []Query` — the Tier-2 query list (NS discovery is Task 5).

- [ ] **Step 1: Write the failing test**

Create `commoncrawl/cc-dns-worker/internal/records/plan_test.go`:
```go
package records

import (
	"testing"

	"github.com/miekg/dns"
)

func TestPlanCoversAllRecordFamilies(t *testing.T) {
	qs := Plan("example.com", DefaultConfig())
	got := map[string]bool{}
	for _, q := range qs {
		got[q.Name+"/"+dns.TypeToString[q.Type]] = true
	}
	want := []string{
		"example.com./A", "example.com./AAAA",
		"www.example.com./A", "mail.example.com./A",
		"example.com./MX", "example.com./TXT",
		"_dmarc.example.com./TXT",
		"default._domainkey.example.com./TXT",
		"mandrill._domainkey.example.com./TXT",
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
Expected: FAIL — undefined `Plan`/`DefaultConfig`/`Query`.

- [ ] **Step 3: Write the plan**

Create `commoncrawl/cc-dns-worker/internal/records/plan.go`:
```go
// Package records builds the per-domain list of Tier-2 DNS queries (sent to the domain's own
// authoritative nameservers). NS discovery (Tier 1) lives in package resolve.
package records

import "github.com/miekg/dns"

// Config controls A/AAAA hostnames and brute-forced DKIM selectors.
type Config struct {
	Hostnames     []string // subdomains; apex is added separately
	DKIMSelectors []string
}

// DefaultConfig is the spec's default 5 hostnames and 10 DKIM selectors.
func DefaultConfig() Config {
	return Config{
		Hostnames:     []string{"www", "mail", "webmail", "smtp", "autodiscover"},
		DKIMSelectors: []string{"default", "google", "selector1", "selector2", "k1", "dkim", "s1", "s2", "mail", "mandrill"},
	}
}

// Query is one DNS question plus the semantic slot its answers are tagged with.
type Query struct {
	Name string // FQDN with trailing dot
	Type uint16
	Slot string // "@" apex host; hostname; DKIM selector; "dmarc"/"mta_sts"/"tls_rpt"/"bimi"; "" infra
}

// Plan returns every Tier-2 query for a domain (no trailing dot on input).
func Plan(domain string, cfg Config) []Query {
	fqdn := dns.Fqdn(domain)
	qs := []Query{
		{fqdn, dns.TypeA, "@"},
		{fqdn, dns.TypeAAAA, "@"},
		{fqdn, dns.TypeMX, ""},
		{fqdn, dns.TypeTXT, ""},
		{fqdn, dns.TypeNS, ""},
		{fqdn, dns.TypeSOA, ""},
		{fqdn, dns.TypeCAA, ""},
		{fqdn, dns.TypeDNSKEY, ""},
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
  - `scheduler.Config{ PerServerQPS float64; Burst int; MaxInFlight int }`, `scheduler.New(cfg) *Scheduler`
  - `(*Scheduler).Do(ctx context.Context, serverIP string, fn func() error) error` — the single choke
    point every outbound query passes through (Task 5).

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

func TestPerServerPacing(t *testing.T) {
	s := New(Config{PerServerQPS: 5, Burst: 1, MaxInFlight: 100})
	ctx := context.Background()
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = s.Do(ctx, "1.2.3.4", func() error { return nil }) }()
	}
	wg.Wait()
	if elapsed := time.Since(start); elapsed < 900*time.Millisecond {
		t.Errorf("6 calls at 5qps/burst1 took %v, want >= ~1s", elapsed)
	}
}

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
		go func(ip string) { defer wg.Done(); _ = s.Do(ctx, ip, func() error { return nil }) }(ip)
	}
	wg.Wait()
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Errorf("independent servers took %v, want fast", elapsed)
	}
}

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
Expected: FAIL — undefined `New`/`Config`.

- [ ] **Step 3: Write the scheduler**

Create `commoncrawl/cc-dns-worker/internal/scheduler/scheduler.go`:
```go
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/scheduler/`
Expected: PASS (all three).

- [ ] **Step 5: Commit**

```bash
cd commoncrawl/cc-dns-worker && go fmt ./... && go vet ./...
git add commoncrawl/cc-dns-worker/internal/scheduler
git commit -m "feat(dns): per-server-IP token-bucket scheduler with in-flight cap"
```

---

### Task 5: Scheduled exchange + recursive NS discovery

**Files:**
- Create: `commoncrawl/cc-dns-worker/internal/resolve/exchange.go`
- Create: `commoncrawl/cc-dns-worker/internal/resolve/discover.go`
- Test: `commoncrawl/cc-dns-worker/internal/resolve/resolve_test.go`
- Test helper: `commoncrawl/cc-dns-worker/internal/resolve/testserver_test.go`

**Interfaces:**
- Produces:
  - `resolve.Exchanger` interface: `Exchange(ctx, m *dns.Msg, serverIP string) (*dns.Msg, error)`
  - `resolve.NewExchanger(sched *scheduler.Scheduler, timeout time.Duration) Exchanger`
  - `resolve.Delegation{ ETLD string; NS []string; NSIPs []string; DS []string }`
  - `resolve.DefaultResolvers []string`
  - `resolve.Discoverer{ Ex Exchanger; Resolvers []string }`, `resolve.NewDiscoverer(ex Exchanger, resolvers []string) *Discoverer`
  - `(*Discoverer).DiscoverNS(ctx, domain string) (Delegation, error)`
- Consumes: `scheduler.Scheduler`, `miekg/dns`, `golang.org/x/net/publicsuffix`.

**Design:** Tier-1 discovery sends **recursive** (RD=1) queries to configured resolvers — `NS` (auth
nameservers), `A`/`AAAA` per NS name (their IPs; the resolver transparently handles glue and
cross-TLD nameservers), and `DS` (parent DNSSEC). No root walk, no manual glue resolution — that is
the whole point of this direction. Because a recursive resolver returns answers in the ANSWER
section, the in-process test server can serve them directly, so `DiscoverNS` is **unit-tested here**.

- [ ] **Step 1: Write the in-process authoritative test server helper**

Create `commoncrawl/cc-dns-worker/internal/resolve/testserver_test.go`:
```go
package resolve

import (
	"net"
	"testing"

	"github.com/miekg/dns"
)

// zone maps "qname/qtype" to the RRs the server returns in ANSWER — exactly what a recursive
// resolver returns, so it doubles as a stand-in recursive resolver for discovery tests.
type zone map[string][]dns.RR

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
		if rrs, ok := z[q.Name+"/"+dns.TypeToString[q.Qtype]]; ok {
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

- [ ] **Step 2: Write the failing exchange + discovery tests**

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

// DiscoverNS against a stand-in recursive resolver: NS names, their IPs (including a cross-TLD NS
// the recursive resolver resolves for us), and the parent DS.
func TestDiscoverNS(t *testing.T) {
	addr, stop := startAuth(t, zone{
		"example.com./NS": {
			mustRR(t, "example.com. 300 IN NS ns1.example.com."),
			mustRR(t, "example.com. 300 IN NS ns2.example.net."), // cross-TLD, no glue needed
		},
		"ns1.example.com./A": {mustRR(t, "ns1.example.com. 300 IN A 1.1.1.1")},
		"ns2.example.net./A": {mustRR(t, "ns2.example.net. 300 IN A 2.2.2.2")},
		"example.com./DS":    {mustRR(t, "example.com. 3600 IN DS 12345 13 2 E2D3C916F6DEEAC73294E8268FB5885044A833FC5459588F4A9184CFC41A5766")},
	})
	defer stop()

	d := NewDiscoverer(newTestExchanger(), []string{addr})
	del, err := d.DiscoverNS(context.Background(), "example.com")
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if del.ETLD != "com" {
		t.Errorf("etld = %q, want com", del.ETLD)
	}
	if len(del.NS) != 2 {
		t.Errorf("NS = %v, want 2", del.NS)
	}
	hasIP := func(ip string) bool {
		for _, x := range del.NSIPs {
			if x == ip {
				return true
			}
		}
		return false
	}
	if !hasIP("1.1.1.1") || !hasIP("2.2.2.2") {
		t.Errorf("NSIPs = %v, want both 1.1.1.1 and 2.2.2.2", del.NSIPs)
	}
	if len(del.DS) == 0 {
		t.Errorf("expected DS captured")
	}
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/resolve/`
Expected: FAIL — undefined `Exchanger`/`NewExchanger`/`NewDiscoverer`.

- [ ] **Step 4: Write the exchange primitive**

Create `commoncrawl/cc-dns-worker/internal/resolve/exchange.go`:
```go
// Package resolve queries DNS in two tiers: Tier-1 discovery via recursive resolvers (discover.go)
// and Tier-2 record queries directly against authoritative servers (query.go). exchange.go is the
// shared transport: one UDP query (TCP on truncation) routed through a per-server scheduler so no
// server is hit too fast.
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
// (port 53 assumed) or ip:port. The caller sets RecursionDesired on m (true for discovery, false
// for direct-authoritative record queries).
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
```

- [ ] **Step 5: Write recursive NS discovery**

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

// DefaultResolvers are the recursive resolvers used for NS discovery. Override with a local unbound
// ("127.0.0.1:53") to remove any dependence on public resolvers and get natural TLD caching.
var DefaultResolvers = []string{"1.1.1.1:53", "8.8.8.8:53", "9.9.9.9:53"}

// Delegation is what discovery learned for a domain.
type Delegation struct {
	ETLD  string
	NS    []string
	NSIPs []string
	DS    []string
}

// Discoverer finds a domain's authoritative NS (+ IPs) and parent DS via recursive resolvers. It
// does NOT walk roots: the recursive resolver's cache absorbs the root/TLD load (polite + fast) and
// transparently resolves cross-TLD / glue-less nameservers. Record queries (query.go) then go
// directly to the discovered NS IPs.
type Discoverer struct {
	Ex        Exchanger
	Resolvers []string
}

// NewDiscoverer returns a Discoverer; empty resolvers falls back to DefaultResolvers.
func NewDiscoverer(ex Exchanger, resolvers []string) *Discoverer {
	if len(resolvers) == 0 {
		resolvers = DefaultResolvers
	}
	return &Discoverer{Ex: ex, Resolvers: append([]string(nil), resolvers...)}
}

// DiscoverNS resolves NS names, their IPs, and the parent DS for a domain (no trailing dot).
func (d *Discoverer) DiscoverNS(ctx context.Context, domain string) (Delegation, error) {
	etld, _ := publicsuffix.PublicSuffix(domain)
	del := Delegation{ETLD: etld}
	fqdn := dns.Fqdn(domain)

	nsResp, err := d.query(ctx, fqdn, dns.TypeNS)
	if err != nil {
		return del, err
	}
	for _, rr := range nsResp.Answer {
		if ns, ok := rr.(*dns.NS); ok {
			del.NS = append(del.NS, strings.ToLower(ns.Ns))
		}
	}
	if len(del.NS) == 0 {
		return del, errors.New("no NS records")
	}

	if dsResp, err := d.query(ctx, fqdn, dns.TypeDS); err == nil && dsResp != nil {
		for _, rr := range dsResp.Answer {
			if ds, ok := rr.(*dns.DS); ok {
				del.DS = append(del.DS, ds.String())
			}
		}
	}

	seen := map[string]bool{}
	add := func(ip string) {
		if ip != "" && !seen[ip] {
			seen[ip] = true
			del.NSIPs = append(del.NSIPs, ip)
		}
	}
	for _, ns := range del.NS {
		for _, qt := range []uint16{dns.TypeA, dns.TypeAAAA} {
			resp, err := d.query(ctx, dns.Fqdn(ns), qt)
			if err != nil || resp == nil {
				continue
			}
			for _, rr := range resp.Answer {
				switch a := rr.(type) {
				case *dns.A:
					add(a.A.String())
				case *dns.AAAA:
					add(a.AAAA.String())
				}
			}
		}
	}
	if len(del.NSIPs) == 0 {
		return del, errors.New("no NS IPs resolved")
	}
	return del, nil
}

// query sends a recursive (RD=1) query, rotating across resolvers with 2 attempts each. SetQuestion
// sets RecursionDesired=true by default, which is what we want here.
func (d *Discoverer) query(ctx context.Context, name string, qtype uint16) (*dns.Msg, error) {
	var lastErr error
	for _, srv := range d.Resolvers {
		for attempt := 0; attempt < 2; attempt++ {
			m := new(dns.Msg)
			m.SetQuestion(name, qtype)
			resp, err := d.Ex.Exchange(ctx, m, srv)
			if err == nil && resp != nil && resp.Rcode != dns.RcodeServerFailure {
				return resp, nil
			}
			lastErr = err
		}
	}
	if lastErr == nil {
		lastErr = errors.New("all resolvers failed")
	}
	return nil, lastErr
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/resolve/`
Expected: PASS (`TestExchangeRoundTrip`, `TestDiscoverNS`).

- [ ] **Step 7: Commit**

```bash
cd commoncrawl/cc-dns-worker && go fmt ./... && go vet ./...
git add commoncrawl/cc-dns-worker/internal/resolve
git commit -m "feat(dns): scheduled exchange + recursive NS discovery"
```

---

### Task 6: Tier-2 record querying → DomainResult

**Files:**
- Create: `commoncrawl/cc-dns-worker/internal/resolve/query.go`
- Test: `commoncrawl/cc-dns-worker/internal/resolve/query_test.go`

**Interfaces:**
- Consumes: `Exchanger` (Task 5), `records.Plan`/`Config` (Task 3), `model` (Task 2).
- Produces:
  - `resolve.Resolver{ Ex Exchanger }`, `resolve.NewResolver(ex Exchanger) *Resolver`
  - `(*Resolver).Resolve(ctx, domain, scanID, runID string, del Delegation, cfg records.Config, now time.Time) model.DomainResult`.

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

type stubEx struct{ z map[string][]dns.RR }

func (s stubEx) Exchange(_ context.Context, m *dns.Msg, _ string) (*dns.Msg, error) {
	r := new(dns.Msg)
	r.SetReply(m)
	q := m.Question[0]
	r.Answer = append(r.Answer, s.z[q.Name+"/"+dns.TypeToString[q.Qtype]]...)
	return r, nil
}

func TestResolveProducesRecords(t *testing.T) {
	z := map[string][]dns.RR{
		"example.com./MX":                    {mustRR(t, "example.com. 300 IN MX 10 mail.example.com.")},
		"example.com./TXT":                   {mustRR(t, `example.com. 300 IN TXT "v=spf1 include:_spf.example.com ~all"`)},
		"_dmarc.example.com./TXT":            {mustRR(t, `_dmarc.example.com. 300 IN TXT "v=DMARC1; p=reject"`)},
		"www.example.com./A":                 {mustRR(t, "www.example.com. 300 IN A 1.2.3.4")},
		"google._domainkey.example.com./TXT": {mustRR(t, `google._domainkey.example.com. 300 IN TXT "v=DKIM1; k=rsa; p=MII"`)},
	}
	r := &Resolver{Ex: stubEx{z: z}}
	del := Delegation{ETLD: "com", NS: []string{"ns1.example.com."}, NSIPs: []string{"9.9.9.9"}}

	res := r.Resolve(context.Background(), "example.com", "2026-07-05", "run1", del, records.DefaultConfig(), time.Unix(0, 0).UTC())

	if res.RootDomain != "example.com" || res.ScanID != "2026-07-05" || res.Status != "done" {
		t.Fatalf("identity/status wrong: %+v", res)
	}
	find := func(rt, slot, wantVal string) bool {
		for _, rec := range res.Records {
			if rec.RecordType == rt && rec.Slot == slot {
				if wantVal == "" || rec.Value == wantVal {
					return true
				}
			}
		}
		return false
	}
	if !find("MX", "", "") {
		t.Errorf("missing MX record; records=%+v", res.Records)
	}
	if !find("TXT", "", "") {
		t.Errorf("missing apex TXT (SPF)")
	}
	if !find("TXT", "dmarc", "") {
		t.Errorf("missing DMARC")
	}
	if !find("A", "www", "1.2.3.4") {
		t.Errorf("missing www A")
	}
	if !find("TXT", "google", "") {
		t.Errorf("missing DKIM google")
	}
	// MX preference is captured in both the priority column and the value (so the ReplacingMergeTree
	// sort key, which includes value but not priority, never collapses two MX at different prefs).
	if !find("MX", "", "10 mail.example.com") {
		t.Errorf("MX value should be full rdata '10 mail.example.com'; records=%+v", res.Records)
	}
	for _, rec := range res.Records {
		if rec.RecordType == "MX" && rec.Priority != 10 {
			t.Errorf("MX priority = %d, want 10", rec.Priority)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/resolve/ -run TestResolveProducesRecords`
Expected: FAIL — `Resolve` undefined.

- [ ] **Step 3: Write query + assembly**

Create `commoncrawl/cc-dns-worker/internal/resolve/query.go`:
```go
package resolve

import (
	"context"
	"strconv"
	"strings"
	"time"

	"cc-dns-worker/internal/model"
	"cc-dns-worker/internal/records"

	"github.com/miekg/dns"
)

// Resolver runs Tier-2 record queries directly against a domain's authoritative NS IPs.
type Resolver struct{ Ex Exchanger }

// NewResolver wraps an Exchanger (configured with the authoritative-NS scheduler).
func NewResolver(ex Exchanger) *Resolver { return &Resolver{Ex: ex} }

// Resolve runs every Tier-2 query for a domain directly against its authoritative NS IPs, rotating
// across them with retry, and assembles a DomainResult. Delegation must already be discovered.
func (r *Resolver) Resolve(ctx context.Context, domain, scanID, runID string, del Delegation, cfg records.Config, now time.Time) model.DomainResult {
	res := model.DomainResult{
		ScanID: scanID, RootDomain: domain, ETLD: del.ETLD,
		Nameservers: del.NS, NSIPs: del.NSIPs,
		DSPresent: len(del.DS) > 0, Status: "done",
		SourceRunID: runID, ResolvedAt: now,
	}
	for _, ds := range del.DS {
		res.Records = append(res.Records, model.DNSRecord{Name: domain, RecordType: "DS", Slot: "", Value: ds, Rcode: "NOERROR"})
	}

	servers := del.NSIPs
	i := 0
	for _, q := range records.Plan(domain, cfg) {
		res.QueriesTotal++
		resp, err := r.queryAuth(ctx, q, servers, i)
		i++
		rcode := "error"
		if err == nil && resp != nil {
			rcode = dns.RcodeToString[resp.Rcode]
			res.QueriesOK++
			recs := collect(q, resp, rcode)
			res.Records = append(res.Records, recs...)
			for _, rec := range recs {
				if rec.RecordType == "DNSKEY" {
					res.DNSSECSigned = true
				}
			}
		}
	}
	return res
}

// queryAuth sends one authoritative query (RecursionDesired=false), rotating across the domain's NS
// IPs so each is tried once per pass and twice overall; the per-server limiter spaces retries.
func (r *Resolver) queryAuth(ctx context.Context, q records.Query, servers []string, start int) (*dns.Msg, error) {
	var lastErr error
	for attempt := 0; attempt < len(servers)*2; attempt++ {
		m := new(dns.Msg)
		m.SetQuestion(q.Name, q.Type)
		m.RecursionDesired = false // authoritative servers don't recurse
		resp, err := r.Ex.Exchange(ctx, m, servers[(start+attempt)%len(servers)])
		if err == nil && resp != nil && resp.Rcode != dns.RcodeServerFailure {
			return resp, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

// collect turns one query's ANSWER RRs into DNSRecords, tagging them with the query's slot.
func collect(q records.Query, resp *dns.Msg, rcode string) []model.DNSRecord {
	name := strings.TrimSuffix(q.Name, ".")
	var out []model.DNSRecord
	for _, rr := range resp.Answer {
		rec := model.DNSRecord{Name: name, Slot: q.Slot, Rcode: rcode, TTL: rr.Header().Ttl}
		switch v := rr.(type) {
		case *dns.A:
			rec.RecordType, rec.Value = "A", v.A.String()
		case *dns.AAAA:
			rec.RecordType, rec.Value = "AAAA", v.AAAA.String()
		case *dns.MX:
			// value = full rdata "<pref> <host>" so the ReplacingMergeTree sort key (which includes
			// value but not the priority column) can't collapse two MX at different preferences.
			rec.RecordType = "MX"
			rec.Priority = v.Preference
			rec.Value = strconv.Itoa(int(v.Preference)) + " " + strings.TrimSuffix(strings.ToLower(v.Mx), ".")
		case *dns.NS:
			rec.RecordType, rec.Value = "NS", strings.TrimSuffix(strings.ToLower(v.Ns), ".")
		case *dns.SOA:
			rec.RecordType, rec.Value = "SOA", strings.TrimSpace(v.String()[len(v.Hdr.String()):])
		case *dns.CAA:
			rec.RecordType, rec.Value = "CAA", strings.TrimSpace(v.String()[len(v.Hdr.String()):])
		case *dns.DNSKEY:
			rec.RecordType, rec.Value = "DNSKEY", strings.TrimSpace(v.String()[len(v.Hdr.String()):])
		case *dns.TXT:
			rec.RecordType, rec.Value = "TXT", strings.Join(v.Txt, "")
		default:
			continue
		}
		out = append(out, rec)
	}
	return out
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/resolve/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd commoncrawl/cc-dns-worker && go fmt ./... && go vet ./...
git add commoncrawl/cc-dns-worker/internal/resolve
git commit -m "feat(dns): Tier-2 record querying assembling DomainResult"
```

---

### Task 7: Durable SQLite store (queue + status + staged records)

**Files:**
- Create: `commoncrawl/cc-dns-worker/internal/store/store.go`
- Test: `commoncrawl/cc-dns-worker/internal/store/store_test.go`

**Interfaces:**
- Consumes: `model.DomainResult` (Task 2), `modernc.org/sqlite`.
- Produces:
  - `store.Open(path string) (*Store, error)` — opens WAL SQLite, creates `scan_domains` + `scan_records`.
  - `(*Store).Seed(ctx, scanID string, domains []string) (int, error)` — INSERT OR IGNORE pending rows; returns count newly added.
  - `(*Store).Pending(ctx, scanID string) ([]string, error)` — domains whose status is neither `done` nor `error` (the resume set).
  - `(*Store).CommitBatch(ctx, results []model.DomainResult) error` — one transaction: for each result, delete its prior `scan_records`, insert new records, upsert its `scan_domains` summary/status.
  - `(*Store).StagedDomains(ctx, scanID string) ([]model.ScanRow, error)` and `(*Store).StagedRecords(ctx, scanID string) ([]model.RecordRow, error)` — read the stage for load (Task 9).
  - `(*Store).Close() error`

- [ ] **Step 1: Write the failing test**

Create `commoncrawl/cc-dns-worker/internal/store/store_test.go`:
```go
package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"cc-dns-worker/internal/model"
)

func openTemp(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "scan.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestSeedAndPending(t *testing.T) {
	ctx := context.Background()
	s := openTemp(t)
	n, err := s.Seed(ctx, "sc", []string{"a.com", "b.com", "c.com"})
	if err != nil || n != 3 {
		t.Fatalf("seed n=%d err=%v", n, err)
	}
	// Re-seeding is idempotent.
	n2, _ := s.Seed(ctx, "sc", []string{"a.com", "d.com"})
	if n2 != 1 {
		t.Errorf("re-seed added %d, want 1 (only d.com)", n2)
	}
	pend, _ := s.Pending(ctx, "sc")
	if len(pend) != 4 {
		t.Fatalf("pending = %d, want 4", len(pend))
	}
}

func TestCommitBatchAndResume(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "scan.db")

	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Seed(ctx, "sc", []string{"a.com", "b.com"}); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(0, 0).UTC()
	err = s.CommitBatch(ctx, []model.DomainResult{{
		ScanID: "sc", RootDomain: "a.com", ETLD: "com", Status: "done",
		Nameservers: []string{"ns1.a.com"}, NSIPs: []string{"1.1.1.1"},
		QueriesTotal: 10, QueriesOK: 9, ResolvedAt: now,
		Records: []model.DNSRecord{{Name: "a.com", RecordType: "MX", Value: "mail.a.com", Priority: 10, Rcode: "NOERROR"}},
	}})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: a.com is done, so only b.com remains pending (the resume contract).
	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	pend, _ := s2.Pending(ctx, "sc")
	if len(pend) != 1 || pend[0] != "b.com" {
		t.Fatalf("pending after resume = %v, want [b.com]", pend)
	}
	recs, _ := s2.StagedRecords(ctx, "sc")
	if len(recs) != 1 || recs[0].RecordType != "MX" || recs[0].Priority != 10 {
		t.Fatalf("staged records = %+v", recs)
	}
	rows, _ := s2.StagedDomains(ctx, "sc")
	if len(rows) != 1 || rows[0].RootDomain != "a.com" || len(rows[0].Nameservers) != 1 {
		t.Fatalf("staged domains = %+v", rows)
	}
}

func TestCommitBatchIsIdempotentPerDomain(t *testing.T) {
	ctx := context.Background()
	s := openTemp(t)
	_, _ = s.Seed(ctx, "sc", []string{"a.com"})
	now := time.Unix(0, 0).UTC()
	res := model.DomainResult{ScanID: "sc", RootDomain: "a.com", Status: "done", ResolvedAt: now,
		Records: []model.DNSRecord{{Name: "a.com", RecordType: "A", Slot: "@", Value: "1.2.3.4", Rcode: "NOERROR"}}}
	_ = s.CommitBatch(ctx, []model.DomainResult{res})
	_ = s.CommitBatch(ctx, []model.DomainResult{res}) // re-commit must not duplicate
	recs, _ := s.StagedRecords(ctx, "sc")
	if len(recs) != 1 {
		t.Fatalf("records after double-commit = %d, want 1", len(recs))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/store/`
Expected: FAIL — `Open` undefined.

- [ ] **Step 3: Write the store**

Create `commoncrawl/cc-dns-worker/internal/store/store.go`:
```go
// Package store is the durable local stage: an embedded SQLite DB holding the domain work queue
// (via scan_domains.status), the per-domain summary, and the resolved records. It is written by one
// dedicated goroutine (CommitBatch) so SQLite's single-writer lock is never contended, and it makes
// scan resumable — a crash leaves unfinished domains not-'done', which Pending returns.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"cc-dns-worker/internal/model"

	_ "modernc.org/sqlite"
)

// Store wraps the SQLite stage.
type Store struct{ db *sql.DB }

const schema = `
CREATE TABLE IF NOT EXISTS scan_domains (
  scan_id       TEXT NOT NULL,
  root_domain   TEXT NOT NULL,
  status        TEXT NOT NULL DEFAULT 'pending',
  etld          TEXT DEFAULT '',
  nameservers   TEXT DEFAULT '[]',
  ns_ips        TEXT DEFAULT '[]',
  dnssec_signed INTEGER DEFAULT 0,
  ds_present    INTEGER DEFAULT 0,
  queries_total INTEGER DEFAULT 0,
  queries_ok    INTEGER DEFAULT 0,
  error         TEXT DEFAULT '',
  resolved_at   TEXT DEFAULT '',
  PRIMARY KEY (scan_id, root_domain)
);
CREATE TABLE IF NOT EXISTS scan_records (
  scan_id      TEXT NOT NULL,
  root_domain  TEXT NOT NULL,
  name         TEXT NOT NULL,
  record_type  TEXT NOT NULL,
  slot         TEXT DEFAULT '',
  value        TEXT NOT NULL,
  ttl          INTEGER DEFAULT 0,
  priority     INTEGER DEFAULT 0,
  rcode        TEXT DEFAULT '',
  resolved_at  TEXT DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_records_domain ON scan_records (scan_id, root_domain);
`

// Open opens (creating if needed) the SQLite stage in WAL mode and ensures the schema.
func Open(path string) (*Store, error) {
	dsn := path + "?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// One writer goroutine calls CommitBatch, but reads may be concurrent; a single open conn keeps
	// writes serialized deterministically.
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(context.Background(), schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("schema: %w", err)
	}
	return &Store{db: db}, nil
}

// Seed inserts pending rows for domains, ignoring any already present. Returns rows newly added.
func (s *Store) Seed(ctx context.Context, scanID string, domains []string) (int, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `INSERT OR IGNORE INTO scan_domains (scan_id, root_domain) VALUES (?, ?)`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	added := 0
	for _, d := range domains {
		r, err := stmt.ExecContext(ctx, scanID, d)
		if err != nil {
			return 0, err
		}
		if n, _ := r.RowsAffected(); n > 0 {
			added++
		}
	}
	return added, tx.Commit()
}

// Pending returns domains for scanID whose status is neither 'done' nor 'error'.
func (s *Store) Pending(ctx context.Context, scanID string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT root_domain FROM scan_domains WHERE scan_id = ? AND status NOT IN ('done','error') ORDER BY root_domain`, scanID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// CommitBatch writes a batch of results in one transaction, replacing each domain's records so a
// re-commit is idempotent.
func (s *Store) CommitBatch(ctx context.Context, results []model.DomainResult) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	del, err := tx.PrepareContext(ctx, `DELETE FROM scan_records WHERE scan_id = ? AND root_domain = ?`)
	if err != nil {
		return err
	}
	defer del.Close()
	insR, err := tx.PrepareContext(ctx, `INSERT INTO scan_records
		(scan_id, root_domain, name, record_type, slot, value, ttl, priority, rcode, resolved_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer insR.Close()
	upD, err := tx.PrepareContext(ctx, `UPDATE scan_domains SET
		status=?, etld=?, nameservers=?, ns_ips=?, dnssec_signed=?, ds_present=?,
		queries_total=?, queries_ok=?, error=?, resolved_at=?
		WHERE scan_id=? AND root_domain=?`)
	if err != nil {
		return err
	}
	defer upD.Close()

	for _, res := range results {
		if _, err := del.ExecContext(ctx, res.ScanID, res.RootDomain); err != nil {
			return err
		}
		ts := res.ResolvedAt.UTC().Format(time.RFC3339Nano)
		for _, rec := range res.Records {
			if _, err := insR.ExecContext(ctx, res.ScanID, res.RootDomain, rec.Name, rec.RecordType,
				rec.Slot, rec.Value, rec.TTL, rec.Priority, rec.Rcode, ts); err != nil {
				return err
			}
		}
		ns, _ := json.Marshal(res.Nameservers)
		nsips, _ := json.Marshal(res.NSIPs)
		if _, err := upD.ExecContext(ctx, res.Status, res.ETLD, string(ns), string(nsips),
			b2i(res.DNSSECSigned), b2i(res.DSPresent), res.QueriesTotal, res.QueriesOK,
			res.Error, ts, res.ScanID, res.RootDomain); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// StagedRecords reads the record stage for a scan into CH RecordRow shape.
func (s *Store) StagedRecords(ctx context.Context, scanID string) ([]model.RecordRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT scan_id, root_domain, name, record_type, slot, value,
		ttl, priority, rcode, resolved_at FROM scan_records WHERE scan_id = ?`, scanID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.RecordRow
	for rows.Next() {
		var r model.RecordRow
		var ts string
		if err := rows.Scan(&r.ScanID, &r.RootDomain, &r.Name, &r.RecordType, &r.Slot, &r.Value,
			&r.TTL, &r.Priority, &r.Rcode, &ts); err != nil {
			return nil, err
		}
		r.ResolvedAt = parseTS(ts)
		r.SourceRunID = scanID
		out = append(out, r)
	}
	return out, rows.Err()
}

// StagedDomains reads finished domain summaries for a scan into CH ScanRow shape.
func (s *Store) StagedDomains(ctx context.Context, scanID string) ([]model.ScanRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT scan_id, root_domain, etld, nameservers, ns_ips,
		dnssec_signed, ds_present, status, error, queries_total, queries_ok, resolved_at
		FROM scan_domains WHERE scan_id = ? AND status IN ('done','error')`, scanID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.ScanRow
	for rows.Next() {
		var r model.ScanRow
		var ns, nsips, ts string
		var dnssec, ds int
		if err := rows.Scan(&r.ScanID, &r.RootDomain, &r.ETLD, &ns, &nsips, &dnssec, &ds,
			&r.Status, &r.Error, &r.QueriesTotal, &r.QueriesOK, &ts); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(ns), &r.Nameservers)
		_ = json.Unmarshal([]byte(nsips), &r.NSIPs)
		r.DNSSECSigned = uint8(dnssec)
		r.DSPresent = uint8(ds)
		r.ResolvedAt = parseTS(ts)
		r.SourceRunID = scanID
		out = append(out, r)
	}
	return out, rows.Err()
}

// Close closes the DB.
func (s *Store) Close() error { return s.db.Close() }

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

func parseTS(s string) time.Time {
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UTC()
	}
	return time.Unix(0, 0).UTC()
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/store/`
Expected: PASS (seed idempotency, resume, per-domain idempotency).

- [ ] **Step 5: Commit**

```bash
cd commoncrawl/cc-dns-worker && go fmt ./... && go vet ./...
git add commoncrawl/cc-dns-worker/internal/store
git commit -m "feat(dns): durable SQLite stage — queue, status, staged records, resume"
```

---

### Task 8: Domain input reader + scan orchestration (resumable)

**Files:**
- Create: `commoncrawl/cc-dns-worker/internal/input/input.go`
- Test: `commoncrawl/cc-dns-worker/internal/input/input_test.go`
- Create: `commoncrawl/cc-dns-worker/cmd/cc-dns-worker/ch.go`
- Modify: `commoncrawl/cc-dns-worker/cmd/cc-dns-worker/scan.go`

**Interfaces:**
- Consumes: `store`, `resolve`, `records`, `scheduler`, `input`, `clickhouse-go/v2`.
- Produces:
  - `input.DefaultQuery` and `input.FromClickHouse(ctx, conn driver.Conn, query string, limit int) ([]string, error)`.
  - `chConn()` helper (in `ch.go`, shared by scan + load).
  - Wired `runScan`: seed queue from CH → resolver pool → single writer goroutine batches into SQLite.

- [ ] **Step 1: Write the failing input test**

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

- [ ] **Step 5: Write the shared CH connection helper**

Create `commoncrawl/cc-dns-worker/cmd/cc-dns-worker/ch.go`:
```go
package main

import (
	"os"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

// chConn connects to ClickHouse from CLICKHOUSE_* env.
func chConn() (driver.Conn, error) {
	return clickhouse.Open(&clickhouse.Options{
		Addr: []string{envOr("CLICKHOUSE_ADDR", "localhost:9000")},
		Auth: clickhouse.Auth{
			Database: envOr("CLICKHOUSE_DB", "corpscout"),
			Username: envOr("CLICKHOUSE_USER", "default"),
			Password: os.Getenv("CLICKHOUSE_PASSWORD"),
		},
	})
}
```

- [ ] **Step 6: Wire runScan**

Replace `commoncrawl/cc-dns-worker/cmd/cc-dns-worker/scan.go` with:
```go
package main

import (
	"context"
	"flag"
	"log"
	"strings"
	"sync"
	"time"

	"cc-dns-worker/internal/input"
	"cc-dns-worker/internal/model"
	"cc-dns-worker/internal/records"
	"cc-dns-worker/internal/resolve"
	"cc-dns-worker/internal/scheduler"
	"cc-dns-worker/internal/store"
)

func runScan(args []string) error {
	fs := flag.NewFlagSet("scan", flag.ExitOnError)
	scanID := fs.String("scan-id", time.Now().UTC().Format("2006-01-02"), "scan batch id (default today, UTC)")
	runID := fs.String("run-id", "", "source run id (defaults to scan-id)")
	dbPath := fs.String("db", "scan.db", "SQLite stage path")
	query := fs.String("query", input.DefaultQuery, "ClickHouse query returning root_domain")
	limit := fs.Int("limit", 0, "cap number of domains (0 = all)")
	resolvers := fs.String("resolvers", strings.Join(resolve.DefaultResolvers, ","), "comma-separated recursive resolvers for NS discovery (use 127.0.0.1:53 for a local unbound)")
	discoveryQPS := fs.Float64("discovery-qps", 50, "max queries/sec per recursive resolver (bump high for local unbound)")
	qps := fs.Float64("per-server-qps", 10, "max queries/sec per authoritative NS IP")
	inflight := fs.Int("per-server-inflight", 3, "max concurrent queries per NS IP")
	workers := fs.Int("workers", 4000, "max domains resolved concurrently")
	batchN := fs.Int("commit-batch", 200, "domains per SQLite commit")
	seedChunk := fs.Int("seed-chunk", 5000, "domains per SQLite seed transaction")
	timeout := fs.Duration("query-timeout", 5*time.Second, "per-query timeout")
	_ = fs.Parse(args)
	if *runID == "" {
		*runID = *scanID
	}
	ctx := context.Background()

	// 1) Seed the durable queue from ClickHouse (idempotent; resumes if scan.db already has rows).
	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()

	conn, err := chConn()
	if err != nil {
		return err
	}
	domains, err := input.FromClickHouse(ctx, conn, *query, *limit)
	conn.Close()
	if err != nil {
		return err
	}
	added := 0
	for i := 0; i < len(domains); i += *seedChunk {
		end := i + *seedChunk
		if end > len(domains) {
			end = len(domains)
		}
		n, err := st.Seed(ctx, *scanID, domains[i:end])
		if err != nil {
			return err
		}
		added += n
	}
	pending, err := st.Pending(ctx, *scanID)
	if err != nil {
		return err
	}
	log.Printf("scan_id=%s: %d domains from CH (%d new); %d pending to resolve", *scanID, len(domains), added, len(pending))

	// 2) Two schedulers: discovery (recursive resolvers) and authoritative (per-NS-IP politeness).
	discSched := scheduler.New(scheduler.Config{PerServerQPS: *discoveryQPS, Burst: int(*discoveryQPS), MaxInFlight: *inflight})
	authSched := scheduler.New(scheduler.Config{PerServerQPS: *qps, Burst: int(*qps), MaxInFlight: *inflight})
	disc := resolve.NewDiscoverer(resolve.NewExchanger(discSched, *timeout), strings.Split(*resolvers, ","))
	rec := resolve.NewResolver(resolve.NewExchanger(authSched, *timeout))
	cfg := records.DefaultConfig()

	// 3) Resolver pool -> results channel -> single writer goroutine (batched SQLite commits).
	results := make(chan model.DomainResult, *batchN*2)
	var writerWG sync.WaitGroup
	writerWG.Add(1)
	go func() {
		defer writerWG.Done()
		batch := make([]model.DomainResult, 0, *batchN)
		flush := func() {
			if len(batch) == 0 {
				return
			}
			if err := st.CommitBatch(ctx, batch); err != nil {
				log.Printf("commit batch: %v", err)
			}
			batch = batch[:0]
		}
		for r := range results {
			batch = append(batch, r)
			if len(batch) >= *batchN {
				flush()
			}
		}
		flush()
	}()

	sem := make(chan struct{}, *workers)
	var wg sync.WaitGroup
	for _, d := range pending {
		sem <- struct{}{}
		wg.Add(1)
		go func(domain string) {
			defer wg.Done()
			defer func() { <-sem }()
			now := time.Now().UTC()
			del, derr := disc.DiscoverNS(ctx, domain)
			if derr != nil || len(del.NSIPs) == 0 {
				msg := "no authoritative NS IPs"
				if derr != nil {
					msg = derr.Error()
				}
				results <- model.DomainResult{
					ScanID: *scanID, RootDomain: domain, ETLD: del.ETLD,
					Nameservers: del.NS, DSPresent: len(del.DS) > 0,
					Status: "error", Error: msg, SourceRunID: *runID, ResolvedAt: now,
				}
				return
			}
			results <- rec.Resolve(ctx, domain, *scanID, *runID, del, cfg, now)
		}(d)
	}
	wg.Wait()
	close(results)
	writerWG.Wait()
	log.Printf("scan_id=%s: done (%d domains resolved this run)", *scanID, len(pending))
	return nil
}
```

- [ ] **Step 7: Verify build**

Run: `cd commoncrawl/cc-dns-worker && go build ./... && go vet ./...`
Expected: exit 0.

- [ ] **Step 8: Commit**

```bash
cd commoncrawl/cc-dns-worker && go fmt ./...
git add commoncrawl/cc-dns-worker
git commit -m "feat(dns): resumable scan orchestration (CH seed, resolver pool, batched writer)"
```

---

### Task 9: ClickHouse migrations + load subcommand (SQLite → CH)

**Files:**
- Create: `clickhouse/migrations/0000NN_corpscout_commoncrawl_domain_dns_records.up.sql` (next free number — `ls clickhouse/migrations/ | tail`)
- Create: `clickhouse/migrations/0000NN_corpscout_commoncrawl_domain_dns_records.down.sql`
- Create: `clickhouse/migrations/0000MM_corpscout_commoncrawl_domain_dns_scan.up.sql` (next number after records)
- Create: `clickhouse/migrations/0000MM_corpscout_commoncrawl_domain_dns_scan.down.sql`
- Create: `commoncrawl/cc-dns-worker/internal/load/load.go`
- Test: `commoncrawl/cc-dns-worker/internal/load/load_test.go`
- Modify: `commoncrawl/cc-dns-worker/cmd/cc-dns-worker/load.go`

**Interfaces:**
- Consumes: `store.StagedRecords`/`StagedDomains`, `model.RecordRow`/`ScanRow`, `chConn()`.
- Produces: `load.FromStore(ctx, conn driver.Conn, st *store.Store, scanID string) (records int, domains int, err error)`; wired `runLoad`.

- [ ] **Step 1: Write the migrations**

Create `clickhouse/migrations/0000NN_corpscout_commoncrawl_domain_dns_records.up.sql`:
```sql
CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_dns_records
(
    scan_id       LowCardinality(String),
    root_domain   String,
    name          String,
    record_type   LowCardinality(String),
    slot          LowCardinality(String),
    value         String,                     -- rdata verbatim; MX value is "<pref> <host>" so the
                                              -- sort key below can't collapse two MX at different prefs
    ttl           UInt32,
    priority      UInt16,                     -- MX preference (convenience; also embedded in value)
    rcode         LowCardinality(String),
    source_run_id String,
    resolved_at   DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (root_domain, scan_id, record_type, name, value);
```

Create `clickhouse/migrations/0000NN_corpscout_commoncrawl_domain_dns_records.down.sql`:
```sql
DROP TABLE IF EXISTS corpscout.commoncrawl_domain_dns_records;
```

Create `clickhouse/migrations/0000MM_corpscout_commoncrawl_domain_dns_scan.up.sql`:
```sql
CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_dns_scan
(
    scan_id       LowCardinality(String),
    root_domain   String,
    etld          LowCardinality(String),
    nameservers   Array(String),
    ns_ips        Array(String),
    dnssec_signed UInt8,
    ds_present    UInt8,
    status        LowCardinality(String),
    error         String,
    queries_total UInt16,
    queries_ok    UInt16,
    source_run_id String,
    resolved_at   DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (root_domain, scan_id);
```

Create `clickhouse/migrations/0000MM_corpscout_commoncrawl_domain_dns_scan.down.sql`:
```sql
DROP TABLE IF EXISTS corpscout.commoncrawl_domain_dns_scan;
```

- [ ] **Step 2: Write the failing load test**

Create `commoncrawl/cc-dns-worker/internal/load/load_test.go`:
```go
package load

import (
	"strings"
	"testing"

	"cc-dns-worker/internal/model"
)

func TestColumnLists(t *testing.T) {
	rc := chColumns[model.RecordRow]()
	if !strings.Contains(strings.Join(rc, ","), "record_type") || len(rc) < 11 {
		t.Errorf("RecordRow columns wrong: %v", rc)
	}
	sc := chColumns[model.ScanRow]()
	if !strings.Contains(strings.Join(sc, ","), "nameservers") || len(sc) < 13 {
		t.Errorf("ScanRow columns wrong: %v", sc)
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/load/`
Expected: FAIL — `chColumns` undefined.

- [ ] **Step 4: Write the loader**

Create `commoncrawl/cc-dns-worker/internal/load/load.go`:
```go
// Package load bulk-copies the SQLite stage into the two corpscout ClickHouse tables over the
// native protocol.
package load

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"cc-dns-worker/internal/model"
	"cc-dns-worker/internal/store"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const (
	recordsTable = "corpscout.commoncrawl_domain_dns_records"
	scanTable    = "corpscout.commoncrawl_domain_dns_scan"
)

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

func insert[T any](ctx context.Context, conn driver.Conn, table string, rows []T) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	q := "INSERT INTO " + table + " (" + strings.Join(chColumns[T](), ", ") + ")"
	batch, err := conn.PrepareBatch(ctx, q)
	if err != nil {
		return 0, fmt.Errorf("prepare %s: %w", table, err)
	}
	for i := range rows {
		if err := batch.AppendStruct(&rows[i]); err != nil {
			_ = batch.Abort()
			return 0, fmt.Errorf("append %s row %d: %w", table, i, err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send %s: %w", table, err)
	}
	return len(rows), nil
}

// FromStore reads the stage for scanID and inserts records + domain summaries into ClickHouse.
func FromStore(ctx context.Context, conn driver.Conn, st *store.Store, scanID string) (int, int, error) {
	recs, err := st.StagedRecords(ctx, scanID)
	if err != nil {
		return 0, 0, err
	}
	nr, err := insert(ctx, conn, recordsTable, recs)
	if err != nil {
		return 0, 0, err
	}
	doms, err := st.StagedDomains(ctx, scanID)
	if err != nil {
		return nr, 0, err
	}
	nd, err := insert(ctx, conn, scanTable, doms)
	return nr, nd, err
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
	"time"

	"cc-dns-worker/internal/load"
	"cc-dns-worker/internal/store"
)

func runLoad(args []string) error {
	fs := flag.NewFlagSet("load", flag.ExitOnError)
	dbPath := fs.String("db", "scan.db", "SQLite stage path")
	scanID := fs.String("scan-id", time.Now().UTC().Format("2006-01-02"), "scan id to load")
	_ = fs.Parse(args)

	ctx := context.Background()
	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()

	conn, err := chConn()
	if err != nil {
		return err
	}
	defer conn.Close()

	nr, nd, err := load.FromStore(ctx, conn, st, *scanID)
	if err != nil {
		return err
	}
	fmt.Printf("loaded %d records and %d domain summaries for scan_id=%s\n", nr, nd, *scanID)
	return nil
}
```

- [ ] **Step 7: Verify build + all unit tests**

Run: `cd commoncrawl/cc-dns-worker && go build ./... && go test ./... && go vet ./...`
Expected: build + unit tests pass.

- [ ] **Step 8: Commit**

```bash
cd commoncrawl/cc-dns-worker && go fmt ./...
git add commoncrawl/cc-dns-worker clickhouse/migrations
git commit -m "feat(dns): CH migrations (records + scan) and SQLite->CH load subcommand"
```

---

### Task 10: Real-DNS smoke + ClickHouse load integration tests

**Files:**
- Create: `commoncrawl/cc-dns-worker/internal/resolve/smoke_test.go` (`//go:build integration`)
- Create: `commoncrawl/cc-dns-worker/internal/load/integration_test.go` (`//go:build integration`)

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

func TestSmokeRealDomains(t *testing.T) {
	discSched := scheduler.New(scheduler.Config{PerServerQPS: 50, Burst: 50, MaxInFlight: 3})
	authSched := scheduler.New(scheduler.Config{PerServerQPS: 10, Burst: 10, MaxInFlight: 3})
	disc := NewDiscoverer(NewExchanger(discSched, 5*time.Second), nil) // nil -> DefaultResolvers
	r := NewResolver(NewExchanger(authSched, 5*time.Second))
	ctx := context.Background()

	del, err := disc.DiscoverNS(ctx, "cloudflare.com")
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if len(del.NS) == 0 || len(del.NSIPs) == 0 {
		t.Fatalf("no NS learned: %+v", del)
	}
	res := r.Resolve(ctx, "cloudflare.com", "smoke", "smoke", del, records.DefaultConfig(), time.Now().UTC())
	var haveMX, haveA bool
	for _, rec := range res.Records {
		if rec.RecordType == "MX" {
			haveMX = true
		}
		if rec.RecordType == "A" {
			haveA = true
		}
	}
	if !haveMX {
		t.Errorf("expected MX for cloudflare.com")
	}
	if !haveA {
		t.Errorf("expected some A records")
	}
}
```

- [ ] **Step 2: Run the smoke test**

Run: `cd commoncrawl/cc-dns-worker && go test -tags=integration ./internal/resolve/ -run TestSmokeRealDomains -v`
Expected: PASS when outbound UDP/53 is available. If the environment blocks it, record it as skipped —
do NOT weaken assertions.

- [ ] **Step 3: Write the CH load integration test**

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
	"cc-dns-worker/internal/store"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func testConn(t *testing.T) driver.Conn {
	t.Helper()
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{envOr("CLICKHOUSE_ADDR", "localhost:9000")},
		Auth: clickhouse.Auth{Database: "corpscout", Username: envOr("CLICKHOUSE_USER", "default"), Password: os.Getenv("CLICKHOUSE_PASSWORD")},
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	return conn
}

// Stages one domain + records in a temp scan.db, loads to CH, reads back. Requires the migrations
// applied to a reachable ClickHouse.
func TestLoadFromStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "scan.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err := st.Seed(ctx, "itest", []string{"example.test"}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := st.CommitBatch(ctx, []model.DomainResult{{
		ScanID: "itest", RootDomain: "example.test", ETLD: "test", Status: "done",
		Nameservers: []string{"ns1.example.test"}, NSIPs: []string{"1.1.1.1"},
		QueriesTotal: 2, QueriesOK: 2, ResolvedAt: now,
		Records: []model.DNSRecord{{Name: "example.test", RecordType: "MX", Value: "mail.example.test", Priority: 10, Rcode: "NOERROR", TTL: 300}},
	}}); err != nil {
		t.Fatal(err)
	}

	conn := testConn(t)
	defer conn.Close()
	nr, nd, err := FromStore(ctx, conn, st, "itest")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if nr != 1 || nd != 1 {
		t.Fatalf("loaded records=%d domains=%d, want 1/1", nr, nd)
	}

	var rt string
	if err := conn.QueryRow(ctx,
		"SELECT record_type FROM corpscout.commoncrawl_domain_dns_records FINAL WHERE scan_id='itest' LIMIT 1").Scan(&rt); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if rt != "MX" {
		t.Errorf("record_type = %q, want MX", rt)
	}
	_ = conn.Exec(ctx, "DELETE FROM corpscout.commoncrawl_domain_dns_records WHERE scan_id='itest'")
	_ = conn.Exec(ctx, "DELETE FROM corpscout.commoncrawl_domain_dns_scan WHERE scan_id='itest'")
}
```

- [ ] **Step 4: Apply migrations and run the load integration test**

Run:
```bash
clickhouse-client --host "${CLICKHOUSE_HOST:-localhost}" --multiquery < clickhouse/migrations/0000NN_corpscout_commoncrawl_domain_dns_records.up.sql
clickhouse-client --host "${CLICKHOUSE_HOST:-localhost}" --multiquery < clickhouse/migrations/0000MM_corpscout_commoncrawl_domain_dns_scan.up.sql
cd commoncrawl/cc-dns-worker && go test -tags=integration ./internal/load/ -run TestLoadFromStoreRoundTrip -v
```
Expected: PASS (1 record + 1 domain loaded and read back).

- [ ] **Step 5: Commit**

```bash
cd commoncrawl/cc-dns-worker && go fmt ./...
git add commoncrawl/cc-dns-worker
git commit -m "test(dns): real-DNS smoke and SQLite->CH load integration tests"
```

---

### Task 11: End-to-end scan+resume smoke + README

**Files:**
- Create: `commoncrawl/cc-dns-worker/README.md`

- [ ] **Step 1: Run a bounded real scan into SQLite**

Run:
```bash
cd commoncrawl/cc-dns-worker && go build -o bin/cc-dns-worker ./cmd/cc-dns-worker
CLICKHOUSE_ADDR=localhost:9000 ./bin/cc-dns-worker scan --limit 20 --db /tmp/dns-smoke.db --scan-id smoke
```
Expected: log `20 domains from CH ... 20 pending`, then `done (20 domains resolved this run)`; `/tmp/dns-smoke.db` exists.

- [ ] **Step 2: Verify the resume contract**

Run:
```bash
CLICKHOUSE_ADDR=localhost:9000 ./bin/cc-dns-worker scan --limit 20 --db /tmp/dns-smoke.db --scan-id smoke
```
Expected: log shows `0 pending to resolve` (all already done/error) — proving resume skips finished domains.

- [ ] **Step 3: Load and verify in ClickHouse**

Run:
```bash
CLICKHOUSE_ADDR=localhost:9000 ./bin/cc-dns-worker load --db /tmp/dns-smoke.db --scan-id smoke
clickhouse-client -q "SELECT count() FROM corpscout.commoncrawl_domain_dns_records FINAL WHERE scan_id='smoke'"
clickhouse-client -q "SELECT count(), countIf(status='done') FROM corpscout.commoncrawl_domain_dns_scan FINAL WHERE scan_id='smoke'"
```
Expected: nonzero record count; ~20 domain rows.

- [ ] **Step 4: Write the README**

Create `commoncrawl/cc-dns-worker/README.md` documenting: purpose; the two-tier model (recursive
discovery via `--resolvers`, incl. the recommended local `unbound` for large runs; direct-to-authoritative
record queries with per-NS-IP rate limiting); the SQLite stage + resume contract; `scan`/`load` usage
and flags; the two CH tables and how history works (`scan_id`); env vars (`CLICKHOUSE_*`); and deferred
items (circuit breaker, streaming input, Redis/shard-by-server scale-out, CT-log hostnames, DNSSEC
validation, dagster scheduling). Match `cc-enrich-worker/README.md` tone.

- [ ] **Step 5: Clean up smoke data and commit**

```bash
clickhouse-client -q "DELETE FROM corpscout.commoncrawl_domain_dns_records WHERE scan_id='smoke'"
clickhouse-client -q "DELETE FROM corpscout.commoncrawl_domain_dns_scan WHERE scan_id='smoke'"
rm -f /tmp/dns-smoke.db*
cd commoncrawl/cc-dns-worker
git add README.md
git commit -m "docs(dns): cc-dns-worker README and end-to-end smoke/resume notes"
```

---

## Deferred (documented, not built in v1)
- **Per-server circuit breaker** — after N consecutive timeouts to a server IP, short-circuit its queries so a dead nameserver stops burning per-query timeout budget. v1 relies on the per-server limiter + bounded retries only.
- **Local `unbound` for discovery** — recommended for large runs: run `unbound` on the scan box and pass `--resolvers 127.0.0.1:53 --discovery-qps 2000`. Removes public-resolver dependence and gives natural TLD caching. (The worker already supports it via flags; deploying unbound is the deferred part.)
- **Streaming domain input** — `input.FromClickHouse` currently materializes all root_domains before seeding; for full-corpus runs switch to a streaming reader that seeds in chunks without holding the whole list in memory. (Seeding is already chunked; the read is not.)
- **Redis-backed distributed scheduler** via shard-by-server-IP (spec §3.3) — only when one box is too slow.
- **CT-log hostname discovery** feeding the hostname list (spec §2) — replaces the static 5.
- **DNSSEC chain validation** — v1 only captures DNSKEY/DS presence.
- **dagster asset** wrapping `scan`+`load` on an every-few-days partitioned schedule (spec §8).

## Self-review notes
- Spec §2 record families → Task 3 (plan) + Task 6 (collect) + smoke Task 10. ✓
- Rate model + single-node → Task 4 (scheduler); two schedulers (discovery + authoritative) wired in Task 8. ✓
- Recursive discovery (no root walk; cross-TLD/glue handled by the resolver) → Task 5 (`Discoverer`), deterministically unit-tested by `TestDiscoverNS`. ✓
- Direct-to-authoritative record queries with per-NS-IP limiting + retry → Task 6 (`Resolver.queryAuth`, RD=false). ✓
- Durable queue + SQLite stage + resume → Task 7 (store) + Task 8 (chunked seed/pending/writer). ✓
- Two normalized tables → Task 9 migrations; SQLite stage → Task 7; column match → Task 2/9 tests. ✓
- MX ReplacingMergeTree-key collision fixed: MX `value` is full rdata `"<pref> <host>"` (Task 6 `collect`), asserted in Task 6 test. ✓
- Resume-on-crash → Task 7 `TestCommitBatchAndResume` + Task 11 Step 2. ✓
- Testing (discovery unit test, store resume, real-DNS smoke, CH load) → Tasks 5/7/10. ✓
- Type consistency: `DomainResult`/`DNSRecord` (Task 2) used by `Resolve` (Task 6), `store` (Task 7), `scan` (Task 8); `Discoverer` (Task 5) + `Resolver` (Task 6) both consume `Exchanger` (Task 5); `RecordRow`/`ScanRow` (Task 2) used by `store` (Task 7) and `load` (Task 9). ✓
