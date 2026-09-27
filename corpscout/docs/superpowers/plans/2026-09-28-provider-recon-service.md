# provider-recon HTTP service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn `provider-recon` into an HTTP service, `provider-recon serve`, running under systemd on companycollect. Dagster can then start and poll collect runs, and the backoffice can restore removals. The same Ansible playbook creates the ClickHouse named collection `provider_recon`, which plan B's S3 table needs.

**Architecture:**
- A new `internal/runner` package holds the collect and restore operations. The CLI and the service both call it, so nothing is duplicated.
- A new `internal/server` package:
  - an HTTP API without authentication for now (owner, 2026-09-28); it listens only on companycollect's Tailscale address
  - a one-operation-at-a-time lock
  - background collects with an in-memory run registry (the last 50 runs)
  - synchronous restores
- `assemble.Restore` gains sentinel errors so the service can map them to HTTP status codes.
- An Ansible role (modelled on `services/translator/ansible`) builds `linux/amd64`, installs the binary and definitions, writes a root-only env file, installs `provider-recon.service`, and creates the named collection from stdin with query logging off.

**Tech Stack:** Go 1.25 stdlib `net/http` (1.22+ method/path patterns), stdlib `testing` + `httptest`, Ansible built-in modules only.

**Spec:** `docs/superpowers/specs/2026-09-27-provider-recon-service-design.md`, section "Stage 3: service, ClickHouse mapping and Dagster".

## Global Constraints

- Module `provider_recon`, in `corpscout/services/provider_recon`.
  - Run `go`/`make` commands from there, and `git` commands from the `corpscout` root.
  - Commit by explicit path. Conventional Commits, each ending with `Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>`.
- API contract (from the spec):
  - `GET /healthz`: no auth.
  - `POST /v1/collect` with `{"providers":[...]}` (optional): returns 202 `{run_id,status}`. 409 while any operation runs; 400 for an unknown provider or bad JSON.
  - `GET /v1/runs/{run_id}`: 200 with the run, or 404.
  - `POST /v1/restore` with `{provider,collector,removed_since}`: synchronous. 200 `{run_id,restored,…}`. 404 when there is no document or no such collector, 422 when there is nothing to restore, 400 for a bad date or missing fields, 409 while an operation runs.
  - No authentication for now (owner decision, 2026-09-28). Exposure is limited by listening only on companycollect's Tailscale address, `100.85.212.113:8095`.
- A service run id equals the manifest run id (`<YYYYMMDDTHHMMSSZ>-collect` / `-restore`). Two runs of the same command never share an id.
- `serve` configuration comes from the environment:
  - `PROVIDER_RECON_LISTEN`: default `:8095`; the deployment sets `100.85.212.113:8095`
  - `PROVIDER_RECON_DEFINITIONS`: default `definitions`
  - `CORPSCOUT_S3_*`, and `PROVIDER_RECON_BUCKET` (default `provider-recon`)
- Deployment target: host `companycollect` (Linux x86_64). Ansible connects as `graovic` with passwordless sudo, and the service runs as the unprivileged `provider-recon` account.
  - Binary: `/opt/companycollect/corpscout/provider_recon/bin/provider-recon`
  - Definitions: `/opt/companycollect/corpscout/provider_recon/definitions/`
  - Env file: `/etc/corpscout-provider-recon/provider-recon.env` (root 0600)
  - Unit: `/etc/systemd/system/provider-recon.service`
- S3 endpoint for the service and for ClickHouse: `http://rustfs.taileb086.ts.net:9000` (full hostname, per the host-name rule).
- The S3 keys never go into git, logs, argv or a migration. Tasks that touch them set `no_log: true`. The named collection is created from stdin with `--log_queries=0`.

## Review Focus

1. **A collect request while a collect runs, and a restore while a collect runs.** Expected: 409 naming the active run id; nothing started. *(Task 2: `TestCollectWhileRunningIs409`.)*
2. **Two runs finishing in the same second.** Expected: distinct run ids; the second never overwrites the first run's registry entry or manifest. *(Task 2: `TestSequentialRunsGetDistinctIDs`.)*
3. **A collect that fails** (store write error). Expected: the run ends `failed` with the error text, and the lock is released so the next collect starts. *(Task 2: `TestFailedCollectReleasesTheLock`.)*
4. **Exposure without auth.** Expected: the unit listens only on the Tailscale address, never on `0.0.0.0` or the LAN. *(Task 4 Step 4: `ss -ltn` shows `100.85.212.113:8095` only.)*
5. **Restore error mapping.** No document → 404, unknown collector → 404, nothing to restore → 422, bad date → 400. *(Task 2: `TestRestoreErrorMapping`.)*

---

## File Structure

```
internal/assemble/assemble.go          + ErrUnknownCollector, ErrBadDate, ErrNothingToRestore (wrapped)
internal/runner/runner.go              LoadDefinitions, SelectProviders, UniqueFeeds, CollectFeeds, Collect, Restore
internal/runner/runner_test.go
internal/server/server.go              API, run registry, single-operation lock
internal/server/server_test.go
cmd/provider-recon/main.go             uses runner; + serve command
cmd/provider-recon/serve.go            serve command
cmd/provider-recon/serve_test.go
ansible/                               ansible.cfg, inventory.ini, site.yml, group_vars, roles/provider_recon/{tasks,templates,handlers}
ansible/README.md
.gitignore                             + .env
```

---

### Task 1: Shared runner and typed restore errors

**Files:**
- Create: `internal/runner/runner.go`, `internal/runner/runner_test.go`
- Modify: `internal/assemble/assemble.go` (Restore errors), `cmd/provider-recon/main.go` (use runner)

**Interfaces:**
- Produces (package `provider_recon/internal/runner`):
  - `var ErrUnknownProvider, ErrNoDocument error`
  - `type Config struct{ DefinitionsDir string; Registry func() map[string]feeds.Collector; Fetcher *feeds.Fetcher; Store publish.Store; Concurrency int; Logger *slog.Logger }`
  - `func LoadDefinitions(dir string, registry func() map[string]feeds.Collector) ([]definitions.Definition, error)`
  - `func SelectProviders(defs []definitions.Definition, slugs []string) ([]definitions.Definition, error)`
  - `func Collect(ctx context.Context, cfg Config, providers []string, now time.Time) (publish.Manifest, error)`
  - `func Restore(ctx context.Context, cfg Config, slug, collector, removedSince string, now time.Time) (publish.Manifest, int, error)`
- Produces (package `assemble`): `ErrUnknownCollector`, `ErrBadDate`, `ErrNothingToRestore`, wrapped by `Restore`'s errors.

- [ ] **Step 1: Write the failing tests** — `internal/runner/runner_test.go`

```go
package runner

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"provider_recon/internal/assemble"
	"provider_recon/internal/feeds"
	"provider_recon/internal/publish"
)

const awsYAML = `slug: aws
display_name: Amazon Web Services
category: cloud
services:
  - key: aws.cloudfront
    display_name: CloudFront
    service_types: [cdn]
feeds:
  - collector: aws_ip_ranges
    tag_map: {CLOUDFRONT: aws.cloudfront}
`

const awsBody = `{"syncToken":"1","prefixes":[{"ip_prefix":"52.84.0.0/15","region":"GLOBAL","service":"CLOUDFRONT"}],"ipv6_prefixes":[{"ipv6_prefix":"2600:9000::/28","region":"GLOBAL","service":"CLOUDFRONT"}]}`

func testConfig(t *testing.T) Config {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(awsBody)) }))
	t.Cleanup(srv.Close)
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "aws.yaml"), []byte(awsYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	return Config{
		DefinitionsDir: dir,
		Registry:       func() map[string]feeds.Collector { return map[string]feeds.Collector{"aws_ip_ranges": &feeds.AWS{URL: srv.URL}} },
		Fetcher:        &feeds.Fetcher{Client: srv.Client(), Retries: 0},
		Store:          publish.FSStore{Root: t.TempDir()},
		Concurrency:    2,
	}
}

func TestCollectPublishesWithTheGivenRunTime(t *testing.T) {
	cfg := testConfig(t)
	now := time.Date(2026, 9, 28, 5, 0, 0, 0, time.UTC)
	m, err := Collect(context.Background(), cfg, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	if m.RunID != "20260928T050000Z-collect" || len(m.Changed) != 1 || m.Changed[0].Slug != "aws" {
		t.Fatalf("manifest = %+v", m)
	}
	if _, ok, _ := cfg.Store.Get(context.Background(), publish.ChangesKey(m.RunID)); !ok {
		t.Fatal("manifest not written")
	}
}

func TestCollectRejectsUnknownProvider(t *testing.T) {
	_, err := Collect(context.Background(), testConfig(t), []string{"nope"}, time.Now())
	if !errors.Is(err, ErrUnknownProvider) {
		t.Fatalf("err = %v, want ErrUnknownProvider", err)
	}
}

func TestRestoreErrors(t *testing.T) {
	cfg := testConfig(t)
	ctx := context.Background()
	now := time.Date(2026, 9, 28, 5, 0, 0, 0, time.UTC)
	if _, _, err := Restore(ctx, cfg, "aws", "aws_ip_ranges", "2026-09-28", now); !errors.Is(err, ErrNoDocument) {
		t.Fatalf("no document: err = %v", err)
	}
	if _, err := Collect(ctx, cfg, nil, now); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		collector, since string
		want             error
	}{
		{"nope", "2026-09-28", assemble.ErrUnknownCollector},
		{"aws_ip_ranges", "2026/09/28", assemble.ErrBadDate},
		{"aws_ip_ranges", "2026-09-28", assemble.ErrNothingToRestore},
	}
	for _, c := range cases {
		if _, _, err := Restore(ctx, cfg, "aws", c.collector, c.since, now.Add(time.Second)); !errors.Is(err, c.want) {
			t.Errorf("Restore(%q,%q) err = %v, want %v", c.collector, c.since, err, c.want)
		}
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/runner/`
Expected: FAIL (`undefined: Config`, `assemble.ErrUnknownCollector` …).

- [ ] **Step 3: Implement**

In `internal/assemble/assemble.go`:
- Add after the `bgpCollectors` var:

  ```go
  // Restore failures, wrapped so callers can map them (the HTTP service does).
  var (
  	ErrUnknownCollector = errors.New("unknown collector")
  	ErrBadDate          = errors.New("invalid date")
  	ErrNothingToRestore = errors.New("nothing to restore")
  )
  ```
- Change the three `Restore` error returns to wrap them:
  - `fmt.Errorf("%s has no collector %q: %w", doc.Slug, collectorID, ErrUnknownCollector)`
  - `fmt.Errorf("removed-since %q is not a YYYY-MM-DD date: %w", removedSince, ErrBadDate)`
  - `fmt.Errorf("%s %s has no grace-expired removals on or after %s: %w", doc.Slug, collectorID, removedSince, ErrNothingToRestore)`

`internal/runner/runner.go`:
```go
// Package runner holds the collect and restore operations shared by the CLI
// and the HTTP service.
package runner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
	"time"

	"provider_recon/internal/assemble"
	"provider_recon/internal/definitions"
	"provider_recon/internal/feeds"
	"provider_recon/internal/model"
	"provider_recon/internal/publish"
)

// ErrUnknownProvider means a requested slug has no definition.
var ErrUnknownProvider = errors.New("unknown provider")

// ErrNoDocument means the provider has never been published.
var ErrNoDocument = errors.New("no published document")

// Config wires one run.
type Config struct {
	DefinitionsDir string
	Registry       func() map[string]feeds.Collector
	Fetcher        *feeds.Fetcher
	Store          publish.Store
	Concurrency    int
	Logger         *slog.Logger
}

func (c Config) logger() *slog.Logger {
	if c.Logger != nil {
		return c.Logger
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// LoadDefinitions loads and validates every definition against the
// registry's collectors.
func LoadDefinitions(dir string, registry func() map[string]feeds.Collector) ([]definitions.Definition, error) {
	defs, err := definitions.LoadDir(dir)
	if err != nil {
		return nil, err
	}
	validators := map[string]definitions.ParamValidator{}
	for name, c := range registry() {
		validators[name] = c
	}
	if err := definitions.Validate(defs, validators); err != nil {
		return nil, err
	}
	return defs, nil
}

// SelectProviders returns the definitions for slugs, or all when slugs is empty.
func SelectProviders(defs []definitions.Definition, slugs []string) ([]definitions.Definition, error) {
	if len(slugs) == 0 {
		return defs, nil
	}
	bySlug := make(map[string]definitions.Definition, len(defs))
	for _, d := range defs {
		bySlug[d.Slug] = d
	}
	out := make([]definitions.Definition, 0, len(slugs))
	for _, s := range slugs {
		d, ok := bySlug[s]
		if !ok {
			return nil, fmt.Errorf("%w: %q", ErrUnknownProvider, s)
		}
		out = append(out, d)
	}
	return out, nil
}

// Collect runs the selected providers' feeds and publishes at now; the
// manifest's run id is publish.RunID(now, "collect").
func Collect(ctx context.Context, cfg Config, providers []string, now time.Time) (publish.Manifest, error) {
	defs, err := LoadDefinitions(cfg.DefinitionsDir, cfg.Registry)
	if err != nil {
		return publish.Manifest{}, err
	}
	if defs, err = SelectProviders(defs, providers); err != nil {
		return publish.Manifest{}, err
	}
	outcomes := CollectFeeds(ctx, UniqueFeeds(defs), cfg.Registry(), cfg.Fetcher, cfg.Concurrency, cfg.logger())
	docs := make([]model.Document, 0, len(defs))
	for _, d := range defs {
		prev, err := publish.LoadLatest(ctx, cfg.Store, d.Slug)
		if err != nil {
			return publish.Manifest{}, err
		}
		doc, err := assemble.Build(d, outcomes, prev, now)
		if err != nil {
			return publish.Manifest{}, fmt.Errorf("%s: %w", d.Slug, err)
		}
		docs = append(docs, doc)
	}
	return publish.PublishScoped(ctx, cfg.Store, docs, now, publish.Scope{Command: "collect", Providers: providers})
}

// Restore undoes grace-expired removals of one feed (see assemble.Restore)
// and publishes the result as a restore run.
func Restore(ctx context.Context, cfg Config, slug, collector, removedSince string, now time.Time) (publish.Manifest, int, error) {
	prev, err := publish.LoadLatest(ctx, cfg.Store, slug)
	if err != nil {
		return publish.Manifest{}, 0, err
	}
	if prev == nil {
		return publish.Manifest{}, 0, fmt.Errorf("%w for %q", ErrNoDocument, slug)
	}
	doc, n, err := assemble.Restore(*prev, collector, removedSince, now)
	if err != nil {
		return publish.Manifest{}, 0, err
	}
	m, err := publish.PublishScoped(ctx, cfg.Store, []model.Document{doc}, now, publish.Scope{Command: "restore", Providers: []string{slug}})
	return m, n, err
}

// UniqueFeeds returns each feed reference once (by ID), sorted by ID.
func UniqueFeeds(defs []definitions.Definition) []definitions.FeedRef {
	seen := map[string]definitions.FeedRef{}
	for _, d := range defs {
		for _, f := range d.Feeds {
			seen[f.ID()] = f
		}
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]definitions.FeedRef, len(ids))
	for i, id := range ids {
		out[i] = seen[id]
	}
	return out
}

// CollectFeeds runs every feed with bounded parallelism; a failure is an
// outcome, never an abort.
func CollectFeeds(ctx context.Context, refs []definitions.FeedRef, collectors map[string]feeds.Collector, f *feeds.Fetcher, limit int, logger *slog.Logger) map[string]assemble.FeedOutcome {
	out := make(map[string]assemble.FeedOutcome, len(refs))
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, max(limit, 1))
	for _, ref := range refs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			start := time.Now()
			res, err := collectors[ref.Collector].Collect(ctx, f, ref.Params)
			if err != nil {
				logger.Warn("collector failed", "collector", ref.ID(), "err", err)
			} else {
				logger.Info("collected", "collector", ref.ID(), "ranges", len(res.Ranges), "skipped", res.Skipped,
					"version", res.SourceVersion, "took", time.Since(start).Round(time.Millisecond))
			}
			mu.Lock()
			out[ref.ID()] = assemble.FeedOutcome{Result: res, Err: err}
			mu.Unlock()
		}()
	}
	wg.Wait()
	return out
}
```

In `cmd/provider-recon/main.go`:
- Delete `loadDefinitions`, `uniqueFeeds` and `collectAll`.
- Make `cmdValidate` call `runner.LoadDefinitions(*dir, registry)`.
- Replace the body of `cmdCollect` after flag parsing with:

```go
	logger := slog.New(slog.NewTextHandler(stderr, nil))
	store, err := openStore(ctx, *outDir)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	var providers []string
	if *only != "" {
		providers = []string{*only}
	}
	cfg := runner.Config{DefinitionsDir: *dir, Registry: registry, Fetcher: feeds.NewFetcher(), Store: store, Concurrency: *concurrency, Logger: logger}
	m, err := runner.Collect(ctx, cfg, providers, time.Now().UTC().Truncate(time.Second))
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
```
  keeping the existing output lines and the exit code 2 logic after it.
- Validation of the definitions now happens inside `runner.Collect`, before any store write. Keep `TestCollectRefusesInvalidDefinitionsAndWritesNothing` green: `openStore` with `-out` only creates the directory lazily on `Put`, so nothing is written.
- Replace the body of `cmdRestore` after `openStore` with:

```go
	m, n, err := runner.Restore(ctx, runner.Config{Store: store}, *slug, *collector, *since, time.Now().UTC().Truncate(time.Second))
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintf(stdout, "restored %d ranges for %s %s (run %s)\n", n, *slug, *collector, m.RunID)
	return 0
```
- Drop imports that are no longer used (`sort`, `sync`, `assemble`, `model` if unused) and add `provider_recon/internal/runner`.

- [ ] **Step 4: Run to verify pass**

Run: `go test ./... && go vet ./... && test -z "$(gofmt -l .)"`
Expected: PASS. Every existing CLI test (collect twice, collector failure exit 2, invalid definitions, restore + collect) still passes unchanged.

- [ ] **Step 5: Commit**

```bash
git add services/provider_recon/internal/runner services/provider_recon/internal/assemble/assemble.go services/provider_recon/cmd/provider-recon/main.go
git commit -m "refactor(provider_recon): shared runner for collect and restore, typed restore errors"
```

---

### Task 2: HTTP API

**Files:**
- Create: `internal/server/server.go`, `internal/server/server_test.go`

**Interfaces:**
- Consumes: `runner.Config`, `runner.Collect`, `runner.Restore`, `runner.LoadDefinitions`, `runner.SelectProviders`, the runner and assemble sentinel errors (Task 1); `publish.RunID`, `publish.CollectorIssue`.
- Produces (package `provider_recon/internal/server`):
  - `type Run struct{…}` (JSON as below)
  - `func New(cfg runner.Config) *Server`
  - `func (s *Server) Handler() http.Handler`
  - `func (s *Server) Wait()`, which blocks until background runs finish
  - `Server.Now func() time.Time`, exported for tests

- [ ] **Step 1: Write the failing tests** — `internal/server/server_test.go`

```go
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"testing"
	"time"

	"provider_recon/internal/assemble"
	"provider_recon/internal/definitions"
	"provider_recon/internal/feeds"
	"provider_recon/internal/model"
	"provider_recon/internal/publish"
	"provider_recon/internal/runner"
)

const awsYAML = `slug: aws
display_name: Amazon Web Services
category: cloud
services:
  - key: aws.cloudfront
    display_name: CloudFront
    service_types: [cdn]
feeds:
  - collector: aws_ip_ranges
    tag_map: {CLOUDFRONT: aws.cloudfront}
`

func awsBody(v4 int) string {
	var b bytes.Buffer
	b.WriteString(`{"syncToken":"1","prefixes":[`)
	for i := 0; i < v4; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, `{"ip_prefix":"10.0.%d.0/24","region":"GLOBAL","service":"CLOUDFRONT"}`, i)
	}
	b.WriteString(`],"ipv6_prefixes":[{"ipv6_prefix":"2600:9000::/28","region":"GLOBAL","service":"CLOUDFRONT"}]}`)
	return b.String()
}

type harness struct {
	api     *httptest.Server
	srv     *Server
	store   publish.Store
	release chan struct{} // when non-nil, the feed blocks until closed
}

func newHarness(t *testing.T, store publish.Store, block bool) *harness {
	t.Helper()
	h := &harness{store: store}
	if block {
		h.release = make(chan struct{})
	}
	feed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h.release != nil {
			<-h.release
		}
		w.Write([]byte(awsBody(10)))
	}))
	t.Cleanup(feed.Close)
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "aws.yaml"), []byte(awsYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := runner.Config{
		DefinitionsDir: dir,
		Registry:       func() map[string]feeds.Collector { return map[string]feeds.Collector{"aws_ip_ranges": &feeds.AWS{URL: feed.URL}} },
		Fetcher:        &feeds.Fetcher{Client: feed.Client()},
		Store:          store,
		Concurrency:    2,
	}
	h.srv = New(cfg)
	h.api = httptest.NewServer(h.srv.Handler())
	t.Cleanup(func() {
		if h.release != nil {
			select {
			case <-h.release:
			default:
				close(h.release)
			}
		}
		h.api.Close()
		h.srv.Wait()
	})
	return h
}

func (h *harness) do(t *testing.T, method, path string, body any) (*http.Response, map[string]any) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	req, _ := http.NewRequest(method, h.api.URL+path, &buf)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	out := map[string]any{}
	json.NewDecoder(res.Body).Decode(&out)
	return res, out
}

func (h *harness) waitFor(t *testing.T, runID string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		_, run := h.do(t, "GET", "/v1/runs/"+runID, nil)
		if run["status"] != "running" {
			return run
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("run %s did not finish", runID)
	return nil
}

func TestHealthz(t *testing.T) {
	h := newHarness(t, publish.FSStore{Root: t.TempDir()}, false)
	if res, body := h.do(t, "GET", "/healthz", nil); res.StatusCode != 200 || body["ok"] != true {
		t.Fatalf("healthz = %d %v", res.StatusCode, body)
	}
}

func TestCollectRunsInTheBackgroundAndReportsTheManifest(t *testing.T) {
	store := publish.FSStore{Root: t.TempDir()}
	h := newHarness(t, store, false)
	res, started := h.do(t, "POST", "/v1/collect", map[string]any{})
	if res.StatusCode != 202 || started["status"] != "running" {
		t.Fatalf("start = %d %v", res.StatusCode, started)
	}
	runID := started["run_id"].(string)
	run := h.waitFor(t, runID)
	if run["status"] != "succeeded" || fmt.Sprint(run["changed"]) != "[aws]" {
		t.Fatalf("run = %v", run)
	}
	if _, ok, _ := store.Get(context.Background(), publish.ChangesKey(runID)); !ok {
		t.Fatal("service run id is not the manifest run id")
	}
}

func TestCollectRejectsUnknownProviderAndBadJSON(t *testing.T) {
	h := newHarness(t, publish.FSStore{Root: t.TempDir()}, false)
	if res, body := h.do(t, "POST", "/v1/collect", map[string]any{"providers": []string{"nope"}}); res.StatusCode != 400 {
		t.Fatalf("unknown provider = %d %v", res.StatusCode, body)
	}
	req, _ := http.NewRequest("POST", h.api.URL+"/v1/collect", bytes.NewBufferString(`{"providers":`))
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 400 {
		t.Fatalf("bad json = %d", res.StatusCode)
	}
}

func TestCollectWhileRunningIs409(t *testing.T) {
	h := newHarness(t, publish.FSStore{Root: t.TempDir()}, true)
	_, started := h.do(t, "POST", "/v1/collect", nil)
	res, body := h.do(t, "POST", "/v1/collect", nil)
	if res.StatusCode != 409 || body["run_id"] != started["run_id"] {
		t.Fatalf("second collect = %d %v", res.StatusCode, body)
	}
	res, _ = h.do(t, "POST", "/v1/restore", map[string]any{"provider": "aws", "collector": "aws_ip_ranges", "removed_since": "2026-09-28"})
	if res.StatusCode != 409 {
		t.Fatalf("restore during collect = %d", res.StatusCode)
	}
	close(h.release)
	if run := h.waitFor(t, started["run_id"].(string)); run["status"] != "succeeded" {
		t.Fatalf("run = %v", run)
	}
}

func TestSequentialRunsGetDistinctIDs(t *testing.T) {
	h := newHarness(t, publish.FSStore{Root: t.TempDir()}, false)
	fixed := time.Date(2026, 9, 28, 5, 0, 0, 0, time.UTC)
	h.srv.Now = func() time.Time { return fixed }
	_, a := h.do(t, "POST", "/v1/collect", nil)
	h.waitFor(t, a["run_id"].(string))
	_, b := h.do(t, "POST", "/v1/collect", nil)
	h.waitFor(t, b["run_id"].(string))
	if a["run_id"] == b["run_id"] {
		t.Fatalf("two runs share id %v", a["run_id"])
	}
	if _, first := h.do(t, "GET", "/v1/runs/"+a["run_id"].(string), nil); first["status"] != "succeeded" {
		t.Fatalf("first run entry overwritten: %v", first)
	}
}

type failingStore struct{ publish.FSStore }

func (failingStore) Put(context.Context, string, []byte, string) error {
	return errors.New("injected put failure")
}

func TestFailedCollectReleasesTheLock(t *testing.T) {
	h := newHarness(t, failingStore{publish.FSStore{Root: t.TempDir()}}, false)
	_, started := h.do(t, "POST", "/v1/collect", nil)
	run := h.waitFor(t, started["run_id"].(string))
	if run["status"] != "failed" || run["error"] == "" {
		t.Fatalf("run = %v", run)
	}
	if res, _ := h.do(t, "POST", "/v1/collect", nil); res.StatusCode != 202 {
		t.Fatalf("lock not released: %d", res.StatusCode)
	}
}

func TestUnknownRunIs404(t *testing.T) {
	h := newHarness(t, publish.FSStore{Root: t.TempDir()}, false)
	if res, _ := h.do(t, "GET", "/v1/runs/20260928T050000Z-collect", nil); res.StatusCode != 404 {
		t.Fatalf("status = %d", res.StatusCode)
	}
}

// seedRemoval publishes an aws document whose 10.0.9.0/24 was removed by grace
// expiry on removedDay, using the real Build and Publish.
func seedRemoval(t *testing.T, store publish.Store) string {
	t.Helper()
	def := definitions.Definition{Slug: "aws", DisplayName: "Amazon Web Services", Category: "cloud",
		Services: []definitions.ServiceDef{{Key: "aws.cloudfront", DisplayName: "CloudFront", ServiceTypes: []string{"cdn"}}},
		Feeds:    []definitions.FeedRef{{Collector: "aws_ip_ranges", TagMap: map[string]string{"CLOUDFRONT": "aws.cloudfront"}}}}
	outcome := func(n int) map[string]assemble.FeedOutcome {
		var rs []feeds.Range
		for i := 0; i < n; i++ {
			rs = append(rs, feeds.Range{Prefix: netip.MustParsePrefix(fmt.Sprintf("10.0.%d.0/24", i)), Tag: "CLOUDFRONT"})
		}
		return map[string]assemble.FeedOutcome{"aws_ip_ranges": {Result: feeds.Result{Ranges: rs}}}
	}
	start := time.Now().UTC().AddDate(0, 0, -10)
	var prev *model.Document
	for _, step := range []struct{ day, ranges int }{{0, 10}, {1, 9}, {8, 9}} {
		at := start.AddDate(0, 0, step.day)
		doc, err := assemble.Build(def, outcome(step.ranges), prev, at)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := publish.Publish(context.Background(), store, []model.Document{doc}, at); err != nil {
			t.Fatal(err)
		}
		prev = &doc
	}
	return start.AddDate(0, 0, 8).Format(model.DateLayout)
}

func TestRestoreSucceeds(t *testing.T) {
	store := publish.FSStore{Root: t.TempDir()}
	removedOn := seedRemoval(t, store)
	h := newHarness(t, store, false)
	res, body := h.do(t, "POST", "/v1/restore", map[string]any{"provider": "aws", "collector": "aws_ip_ranges", "removed_since": removedOn})
	if res.StatusCode != 200 || body["status"] != "succeeded" || body["restored"] != float64(1) {
		t.Fatalf("restore = %d %v", res.StatusCode, body)
	}
	if _, ok, _ := store.Get(context.Background(), publish.ChangesKey(body["run_id"].(string))); !ok {
		t.Fatal("restore manifest not written under the returned run id")
	}
}

func TestRestoreErrorMapping(t *testing.T) {
	store := publish.FSStore{Root: t.TempDir()}
	h := newHarness(t, store, false)
	req := func(provider, collector, since string) int {
		res, _ := h.do(t, "POST", "/v1/restore", map[string]any{"provider": provider, "collector": collector, "removed_since": since})
		return res.StatusCode
	}
	if got := req("aws", "aws_ip_ranges", "2026-09-28"); got != 404 {
		t.Fatalf("no document = %d", got)
	}
	seedRemoval(t, store)
	cases := []struct {
		collector, since string
		want             int
	}{
		{"nope", "2026-09-01", 404},
		{"aws_ip_ranges", "2026/09/01", 400},
		{"aws_ip_ranges", "2099-01-01", 422},
		{"", "2026-09-01", 400},
	}
	for _, c := range cases {
		if got := req("aws", c.collector, c.since); got != c.want {
			t.Errorf("restore(%q,%q) = %d, want %d", c.collector, c.since, got, c.want)
		}
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/server/`
Expected: FAIL (`undefined: New`).

- [ ] **Step 3: Implement** — `internal/server/server.go`

```go
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
```

- [ ] **Step 4: Run to verify pass**

Run: `go test -race ./internal/server/ && go test ./... && go vet ./... && test -z "$(gofmt -l .)"`
Expected: PASS, including under `-race`.

- [ ] **Step 5: Commit**

```bash
git add services/provider_recon/internal/server
git commit -m "feat(provider_recon): HTTP API for collect, run status and restore (no auth for now)"
```

---

### Task 3: `serve` command

**Files:**
- Create: `cmd/provider-recon/serve.go`, `cmd/provider-recon/serve_test.go`
- Modify: `cmd/provider-recon/main.go` (dispatch `serve`, usage)

**Interfaces:**
- Consumes: `server.New`, `runner.LoadDefinitions`, `openStore`, `registry` (Tasks 1–2).
- Produces: `provider-recon serve [-definitions dir] [-listen addr] [-out dir]`; the flags default to the env vars in Global Constraints.

- [ ] **Step 1: Write the failing tests** — `cmd/provider-recon/serve_test.go`

```go
package main

import (
	"bytes"
	"context"
	"testing"
)

func TestServeRefusesInvalidDefinitions(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"serve", "-out", t.TempDir(), "-definitions", t.TempDir()}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit %d: %s", code, stderr.String())
	}
}

func TestServeStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	var stdout, stderr bytes.Buffer
	go func() {
		done <- run(ctx, []string{"serve", "-out", t.TempDir(), "-definitions", "../../definitions", "-listen", "127.0.0.1:0"}, &stdout, &stderr)
	}()
	cancel()
	if code := <-done; code != 0 {
		t.Fatalf("exit %d: %s", code, stderr.String())
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./cmd/provider-recon/ -run TestServe`
Expected: FAIL (`serve` is unknown → exit 64, not 1/0).

- [ ] **Step 3: Implement**

`cmd/provider-recon/serve.go`:
```go
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"provider_recon/internal/feeds"
	"provider_recon/internal/runner"
	"provider_recon/internal/server"
)

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

// cmdServe runs the HTTP API until SIGINT/SIGTERM (or ctx is cancelled), then
// stops accepting requests and waits for a running collect to finish.
func cmdServe(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dir := fs.String("definitions", envOr("PROVIDER_RECON_DEFINITIONS", "definitions"), "definitions directory")
	listen := fs.String("listen", envOr("PROVIDER_RECON_LISTEN", ":8095"), "listen address")
	outDir := fs.String("out", "", "use this local directory instead of S3 (testing)")
	if err := fs.Parse(args); err != nil {
		return 64
	}
	if _, err := runner.LoadDefinitions(*dir, registry); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	store, err := openStore(ctx, *outDir)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	logger := slog.New(slog.NewTextHandler(stderr, nil))
	api := server.New(runner.Config{DefinitionsDir: *dir, Registry: registry, Fetcher: feeds.NewFetcher(),
		Store: store, Concurrency: 4, Logger: logger})

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	httpServer := &http.Server{Addr: *listen, Handler: api.Handler(), ReadHeaderTimeout: 10 * time.Second}
	errs := make(chan error, 1)
	go func() { errs <- httpServer.ListenAndServe() }()
	logger.Info("provider-recon serving", "listen", *listen, "definitions", *dir)

	select {
	case err := <-errs:
		if !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintln(stderr, err)
			return 1
		}
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdown); err != nil {
		fmt.Fprintln(stderr, err)
	}
	api.Wait()
	fmt.Fprintln(stdout, "provider-recon stopped")
	return 0
}
```

In `main.go`:
- Add `case "serve": return cmdServe(ctx, args[1:], stdout, stderr)`.
- Change the usage text to `validate|schema|collect|restore|serve`.


- [ ] **Step 4: Run to verify pass**

Run: `go test -race ./... && go vet ./... && test -z "$(gofmt -l .)" && make build`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add services/provider_recon/cmd
git commit -m "feat(provider_recon): serve command with graceful shutdown"
```

---

### Task 4: Ansible deployment to companycollect and the ClickHouse named collection

**Files:**
- Create under `services/provider_recon/ansible/`:
  - `ansible.cfg`, `inventory.ini`, `site.yml`
  - `group_vars/provider_recon_hosts/vars.yml`
  - `roles/provider_recon/tasks/main.yml`
  - `roles/provider_recon/handlers/main.yml`
  - `roles/provider_recon/templates/provider-recon.service.j2`
  - `roles/provider_recon/templates/provider-recon.env.j2`
  - `README.md`

- [ ] **Step 1: Check the target**

```bash
ssh companycollect 'uname -sm; getent hosts rustfs.taileb086.ts.net; ss -ltn | grep -c ":8095 " || true; docker ps --format "{{.Names}}" | grep clickhouse'
```
Expected:
- `Linux x86_64`
- the RustFS address resolves
- `0` listeners on :8095
- `clickhouse-clickhouse-1` is running

If the ClickHouse container name differs, set `provider_recon_clickhouse_container` in `vars.yml` accordingly.

- [ ] **Step 2: Write the playbook**

`ansible.cfg`:
```ini
[defaults]
inventory = ./inventory.ini
roles_path = ./roles
host_key_checking = True
retry_files_enabled = False
callback_result_format = yaml

[ssh_connection]
pipelining = True
```

`inventory.ini`:
```ini
[provider_recon_hosts]
# `companycollect` resolves through the operator's SSH configuration.
companycollect ansible_host=companycollect ansible_user=graovic ansible_python_interpreter=/usr/bin/python3
```

`site.yml`:
```yaml
---
- name: Deploy the provider-recon service
  hosts: provider_recon_hosts
  become: true
  gather_facts: true
  roles:
    - provider_recon
```

`group_vars/provider_recon_hosts/vars.yml`:
```yaml
---
provider_recon_service_name: provider-recon
provider_recon_service_user: provider-recon
provider_recon_service_group: provider-recon

provider_recon_source_dir: "{{ playbook_dir }}/.."
provider_recon_local_binary: "{{ provider_recon_source_dir }}/bin/provider-recon-linux-amd64"

provider_recon_deploy_dir: /opt/companycollect/corpscout/provider_recon
provider_recon_binary: "{{ provider_recon_deploy_dir }}/bin/provider-recon"
provider_recon_definitions_dir: "{{ provider_recon_deploy_dir }}/definitions"
provider_recon_config_dir: /etc/corpscout-provider-recon
provider_recon_env_file: "{{ provider_recon_config_dir }}/provider-recon.env"

# companycollect's Tailscale address (`tailscale ip -4`): the API has no auth
# for now, so it must not listen on the LAN or 0.0.0.0.
provider_recon_listen: "100.85.212.113:8095"
provider_recon_bucket: provider-recon
provider_recon_s3_endpoint: http://rustfs.taileb086.ts.net:9000

# ClickHouse runs in Docker on the same host; the named collection lets the
# S3-engine table read the bucket without credentials in migrations.
provider_recon_clickhouse_container: clickhouse-clickhouse-1
provider_recon_named_collection: provider_recon

# S3 keys come from the control machine's environment, never from git.
provider_recon_s3_access_key: "{{ lookup('ansible.builtin.env', 'CORPSCOUT_S3_ACCESS_KEY') }}"
provider_recon_s3_secret_key: "{{ lookup('ansible.builtin.env', 'CORPSCOUT_S3_SECRET_KEY') }}"
```

`roles/provider_recon/templates/provider-recon.env.j2`:
```
# MANAGED BY ANSIBLE (roles/provider_recon). Root-only: contains the S3 keys.
PROVIDER_RECON_LISTEN={{ provider_recon_listen }}
PROVIDER_RECON_DEFINITIONS={{ provider_recon_definitions_dir }}
PROVIDER_RECON_BUCKET={{ provider_recon_bucket }}
CORPSCOUT_S3_ENDPOINT={{ provider_recon_s3_endpoint }}
CORPSCOUT_S3_ACCESS_KEY={{ provider_recon_s3_access_key }}
CORPSCOUT_S3_SECRET_KEY={{ provider_recon_s3_secret_key }}
```

`roles/provider_recon/templates/provider-recon.service.j2`:
```ini
# MANAGED BY ANSIBLE (roles/provider_recon).
[Unit]
Description=Corpscout provider-recon API (provider IP ranges and evidence)
Wants=network-online.target
After=network-online.target
StartLimitIntervalSec=0

[Service]
Type=exec
User={{ provider_recon_service_user }}
Group={{ provider_recon_service_group }}
WorkingDirectory={{ provider_recon_deploy_dir }}
EnvironmentFile={{ provider_recon_env_file }}
ExecStart={{ provider_recon_binary }} serve

Restart=on-failure
RestartSec=10s
# A running collect gets time to finish before the unit is killed.
TimeoutStopSec=120s
KillMode=control-group
UMask=0077

NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
PrivateDevices=true
ProtectControlGroups=true
ProtectKernelModules=true
ProtectKernelTunables=true
ProtectKernelLogs=true
ProtectClock=true
ProtectHostname=true
RestrictSUIDSGID=true
RestrictRealtime=true
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6
SystemCallArchitectures=native

[Install]
WantedBy=multi-user.target
```

`roles/provider_recon/handlers/main.yml`:
```yaml
---
- name: Restart provider-recon
  ansible.builtin.systemd_service:
    name: "{{ provider_recon_service_name }}"
    enabled: true
    state: restarted
    daemon_reload: true
  when: not ansible_check_mode
```

`roles/provider_recon/tasks/main.yml`:
```yaml
---
- name: Require the S3 keys on the control machine
  ansible.builtin.assert:
    that:
      - provider_recon_s3_access_key | length > 0
      - provider_recon_s3_secret_key | length > 0
      - (provider_recon_s3_access_key ~ provider_recon_s3_secret_key) is not search("[\\x00-\\x1f\\x7f'\\\\]")
    fail_msg: >-
      Export CORPSCOUT_S3_ACCESS_KEY and CORPSCOUT_S3_SECRET_KEY (no control
      characters, quotes or backslashes) before running this playbook; see README.md.
  delegate_to: localhost
  become: false
  run_once: true
  no_log: true

- name: Require Linux x86_64
  ansible.builtin.assert:
    that:
      - ansible_facts.system == "Linux"
      - ansible_facts.architecture == "x86_64"

- name: Test provider-recon on the control machine
  ansible.builtin.command:
    argv: [go, test, ./...]
    chdir: "{{ provider_recon_source_dir }}"
  environment:
    CGO_ENABLED: "0"
  delegate_to: localhost
  become: false
  run_once: true
  check_mode: false
  changed_when: false

- name: Build provider-recon for linux/amd64
  ansible.builtin.command:
    argv: [go, build, -trimpath, -o, "{{ provider_recon_local_binary }}", ./cmd/provider-recon]
    chdir: "{{ provider_recon_source_dir }}"
  environment:
    CGO_ENABLED: "0"
    GOOS: linux
    GOARCH: amd64
  delegate_to: localhost
  become: false
  run_once: true
  check_mode: false
  changed_when: false

- name: Validate the definitions that will be deployed
  ansible.builtin.command:
    argv: [go, run, ./cmd/provider-recon, validate, -definitions, definitions]
    chdir: "{{ provider_recon_source_dir }}"
  delegate_to: localhost
  become: false
  run_once: true
  check_mode: false
  changed_when: false

- name: Create the service group
  ansible.builtin.group:
    name: "{{ provider_recon_service_group }}"
    system: true

- name: Create the service user
  ansible.builtin.user:
    name: "{{ provider_recon_service_user }}"
    group: "{{ provider_recon_service_group }}"
    system: true
    shell: /usr/sbin/nologin
    create_home: false
    home: "{{ provider_recon_deploy_dir }}"

- name: Create the deployment directories
  ansible.builtin.file:
    path: "{{ item.path }}"
    state: directory
    owner: root
    group: "{{ item.group }}"
    mode: "{{ item.mode }}"
  loop:
    - { path: "{{ provider_recon_deploy_dir }}", group: root, mode: "0755" }
    - { path: "{{ provider_recon_deploy_dir }}/bin", group: root, mode: "0755" }
    - { path: "{{ provider_recon_definitions_dir }}", group: root, mode: "0755" }
    - { path: "{{ provider_recon_config_dir }}", group: "{{ provider_recon_service_group }}", mode: "0750" }

- name: Install the binary
  ansible.builtin.copy:
    src: "{{ provider_recon_local_binary }}"
    dest: "{{ provider_recon_binary }}"
    owner: root
    group: root
    mode: "0755"
  notify: Restart provider-recon

- name: Install the definitions
  ansible.builtin.copy:
    src: "{{ item }}"
    dest: "{{ provider_recon_definitions_dir }}/"
    owner: root
    group: root
    mode: "0644"
  loop: "{{ query('ansible.builtin.fileglob', provider_recon_source_dir ~ '/definitions/*.yaml') }}"
  notify: Restart provider-recon

- name: Find deployed definitions
  ansible.builtin.find:
    paths: "{{ provider_recon_definitions_dir }}"
    patterns: "*.yaml"
  register: provider_recon_deployed_definitions

- name: Remove definitions deleted from the repository
  ansible.builtin.file:
    path: "{{ item.path }}"
    state: absent
  loop: "{{ provider_recon_deployed_definitions.files }}"
  loop_control:
    label: "{{ item.path | basename }}"
  when: >-
    (item.path | basename) not in
    (query('ansible.builtin.fileglob', provider_recon_source_dir ~ '/definitions/*.yaml') | map('basename') | list)
  notify: Restart provider-recon

- name: Write the environment file
  ansible.builtin.template:
    src: provider-recon.env.j2
    dest: "{{ provider_recon_env_file }}"
    owner: root
    group: root
    mode: "0600"
  diff: false
  no_log: true
  notify: Restart provider-recon

- name: Install the systemd unit
  ansible.builtin.template:
    src: provider-recon.service.j2
    dest: "/etc/systemd/system/{{ provider_recon_service_name }}.service"
    owner: root
    group: root
    mode: "0644"
  notify: Restart provider-recon

- name: Check whether the ClickHouse named collection exists
  ansible.builtin.command:
    argv:
      - docker
      - exec
      - "{{ provider_recon_clickhouse_container }}"
      - clickhouse-client
      - --query
      - "SELECT count() FROM system.named_collections WHERE name = '{{ provider_recon_named_collection }}'"
  register: provider_recon_collection_count
  check_mode: false
  changed_when: false

- name: Create the ClickHouse named collection (credentials via stdin, query log off)
  ansible.builtin.command:
    argv:
      - docker
      - exec
      - -i
      - "{{ provider_recon_clickhouse_container }}"
      - clickhouse-client
      - --log_queries=0
      - --log_query_threads=0
    stdin: >-
      CREATE NAMED COLLECTION {{ provider_recon_named_collection }} AS
      url = '{{ provider_recon_s3_endpoint }}/{{ provider_recon_bucket }}/' NOT OVERRIDABLE,
      access_key_id = '{{ provider_recon_s3_access_key }}' NOT OVERRIDABLE,
      secret_access_key = '{{ provider_recon_s3_secret_key }}' NOT OVERRIDABLE
  when: provider_recon_collection_count.stdout | trim == "0"
  no_log: true

- name: Start provider-recon
  ansible.builtin.meta: flush_handlers

- name: Enable provider-recon
  ansible.builtin.systemd_service:
    name: "{{ provider_recon_service_name }}"
    enabled: true
    state: started
    daemon_reload: true
  when: not ansible_check_mode

- name: Wait for the health check
  ansible.builtin.uri:
    url: "http://{{ provider_recon_listen }}/healthz"
    status_code: 200
  register: provider_recon_health
  retries: 15
  delay: 2
  until: provider_recon_health.status == 200
  when: not ansible_check_mode
```

- [ ] **Step 3: README** — `ansible/README.md`

````markdown
# provider-recon Ansible deployment

Builds `provider-recon` for linux/amd64 and runs `provider-recon serve` as the
systemd unit `provider-recon.service` on `companycollect` (user
`provider-recon`). The API has no authentication for now, so it listens only on
companycollect's Tailscale address, `100.85.212.113:8095`. It also creates the ClickHouse named collection
`provider_recon`, which the S3-engine table `provider_recon_documents_s3`
reads through.

Installed:
- `/opt/companycollect/corpscout/provider_recon/bin/provider-recon`
- `/opt/companycollect/corpscout/provider_recon/definitions/*.yaml`, with
  deleted definitions removed
- `/etc/corpscout-provider-recon/provider-recon.env` (root, 0600)
- `/etc/systemd/system/provider-recon.service`

## Deploy

```bash
cd services/provider_recon/ansible
export CORPSCOUT_S3_ACCESS_KEY="$(sed -n 's/^CORPSCOUT_S3_ACCESS_KEY=//p' ../../backoffice/.env)"
export CORPSCOUT_S3_SECRET_KEY="$(sed -n 's/^CORPSCOUT_S3_SECRET_KEY=//p' ../../backoffice/.env)"
export LC_ALL=en_US.UTF-8 LANG=en_US.UTF-8
ansible-playbook site.yml --check --diff
ansible-playbook site.yml
```

The named collection is created only when it does not exist. To rotate its
credentials, use `ALTER NAMED COLLECTION provider_recon SET …` by hand.

## Check

```bash
curl -s http://companycollect.taileb086.ts.net:8095/healthz
curl -s -X POST http://companycollect.taileb086.ts.net:8095/v1/collect
curl -s http://companycollect.taileb086.ts.net:8095/v1/runs/<run_id>
journalctl -u provider-recon -n 50
```
````

- [ ] **Step 4: Dry run, deploy, verify**

Run the Deploy block in the README: `--check --diff` first, then the real run.
Expected:
- the dry run shows the planned creations
- the real run ends with the health check passing

Then, from the control machine:
```bash
curl -s http://companycollect.taileb086.ts.net:8095/healthz
RUN=$(curl -s -X POST http://companycollect.taileb086.ts.net:8095/v1/collect | jq -r .run_id); echo $RUN
sleep 20; curl -s "http://companycollect.taileb086.ts.net:8095/v1/runs/$RUN" | jq '{status, changed: (.changed|length), unchanged_count, issues: (.issues|length)}'
ssh companycollect 'ss -ltnH | grep ":8095 "'
ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT name FROM system.named_collections WHERE name='provider_recon'; SELECT count() FROM s3(provider_recon, filename='providers/*/latest.json', format='JSONAsString')\""
```
Expected:
- `{"ok":true}`
- the run `succeeded` with 0 issues, and `changed + unchanged_count = 37`
- `ss` shows exactly one listener, `100.85.212.113:8095`
- ClickHouse lists `provider_recon` and counts `37` documents through the collection

- [ ] **Step 5: Commit**

```bash
git add services/provider_recon/ansible
git commit -m "feat(provider_recon): Ansible deployment as a systemd service on companycollect with the ClickHouse named collection"
```
