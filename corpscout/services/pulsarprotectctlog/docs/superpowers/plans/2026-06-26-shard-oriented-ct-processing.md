# ctlog-Oriented Processing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Process CT data **per source, per ctlog**: a `list` command that returns the ctlogs in a source with all their metadata (JSON), and a `process` command that drains *everything* in one ctlog (`0…head`) into ClickHouse.

**Architecture:** A *source* (configured provider, e.g. `le-sycamore`) resolves via the published log list to a set of *ctlogs* (one CT log = one append-only Merkle tree = a temporal shard, e.g. `sycamore-2025h2d`). Each ctlog is identified by a **friendly id** and carries metadata (canonical LogID, MMD, state, expiry interval, endpoints) plus our stored **processing metadata** (cursor, status, counts). `process` drains a ctlog's leaves `0…head` via tiles (or get-entries); per-ctlog progress lives in the local SQLite control DB keyed by the friendly id; dedup by `(issuer, serial)` in ClickHouse unifies overlap. The date-window mode and SCT binary-search are removed.

**Tech Stack:** Go 1.25 (pure-Go, `CGO_ENABLED=0`), ClickHouse (`clickhouse-go/v2`), SQLite (`modernc.org/sqlite`), static-ct-api tiled logs + RFC 6962 logs.

## Vocabulary (native CT terms)

- **source** — a configured provider: one operator + a log-name prefix (`le-sycamore` = Let's Encrypt / Sycamore).
- **ctlog** — one CT log = one append-only Merkle tree (a temporal shard, `sycamore-2025h2d`). The unit we enumerate, drain, and track. Identified by a **friendly id**; canonical **LogID** kept in metadata.
- **checkpoint** — a ctlog's signed tree head `(size, root)`; polled to read the current **head** (entry count). Many checkpoints over a ctlog's life.
- **leaf** — one certificate (precert/cert) in the tree, at index `0…head-1`. **tiles** serve 256 leaves each.

## Global Constraints

- Module: `github.com/pulsarpoint/pulsarprotectctlog`; Go 1.25; pure-Go deps (`CGO_ENABLED=0 GOOS=linux GOARCH=amd64` for server `ctlogs`, Ubuntu 25.10 x86_64).
- Every task ends green on: `gofmt -l .` (empty), `go vet ./...`, `go test ./...`.
- Data plane = ClickHouse `companycollect:9002` db `ctlogs`; control plane = local SQLite at `CTLOG_CONTROL_DB_PATH`.
- Dedup identity = `(issuer_ca_id, serial_number)` (ReplacingMergeTree).
- Retention: SANs always written; cert metadata unless `not_after < now AND not_before < now-1y`.
- **No `-from`/`-to`, no SCT binary search.** ctlogs are processed `0…head`.
- **ctlog id = friendly name** (e.g. `sycamore-2025h2d`); canonical LogID exposed in metadata. `list` API = CLI subcommand with `--json`.

---

## File Structure

- `cmd/ctlog/main.go` — MODIFY: replace flag-soup `run()` with **subcommand dispatch** (`list`, `process`); remove date-window + single-`-shard` flags.
- `internal/loglist/loglist.go` — MODIFY: parse full ctlog metadata (`log_id`, `mmd`, `state`, urls, interval) for both `tiled_logs[]` and `logs[]`; type `CTLog` (rename from `Shard`); `CTLog.Phase(now)`.
- `internal/loglist/source.go` — CREATE: `Source` type + `LoadSources(path)`.
- `internal/loglist/status.go` — CREATE: `Head` (reachability+head), `CTLogStatus`, `PercentDone`, `BuildStatus`.
- `internal/source/source.go`,`tile.go`,`rfc6962.go`; `internal/ctclient/client.go` — MODIFY: drop `EntryTimestamp`.
- `internal/search/` — DELETE.
- `internal/store/control/store.go` — DONE (has `ListWorkUnits`, `OpenReadOnly`).
- `internal/config/config.go` — MODIFY: add `SourcesFile`, `ShardListURL` (the all-logs list); drop single-log `Source`/`LogName`/`LogURL`.
- `sources.json` — CREATE at repo root.

---

## Task 1: Remove date-window mode; convert main to subcommand dispatch

**Files:**
- Delete: `internal/search/timeindex.go`, `internal/search/timeindex_test.go`
- Modify: `internal/source/source.go` (drop `EntryTimestamp`), `internal/source/tile.go`, `internal/source/rfc6962.go`, `internal/ctclient/client.go` (drop `EntryTimestamp`)
- Modify: `cmd/ctlog/main.go` (subcommand skeleton)

**Interfaces:**
- Produces: `source.Source = { Name() string; TreeSize(ctx)(uint64,error); FetchRange(ctx,start,end)([]model.CertMeta,int64,int,error) }`.
- Produces: `main()` dispatches on `os.Args[1]` ∈ {`list`,`process`}; unknown/empty prints usage.

- [ ] **Step 1: Delete search package + EntryTimestamp**

```bash
git rm internal/search/timeindex.go internal/search/timeindex_test.go
```
In `internal/source/source.go` remove the `EntryTimestamp` line and unused `time` import. In `tile.go`/`rfc6962.go`/`ctclient/client.go` delete the `EntryTimestamp` methods (and `tile.go`'s search-only cache fields + `ensureTreeSize` if unused elsewhere — keep what `FetchRange` needs; `ctclient`'s `msToTime` if now unused).

- [ ] **Step 2: Replace main run() with subcommand dispatch**

```go
func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "list":
		err = cmdList(os.Args[2:])
	case "process":
		err = cmdProcess(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		slog.Error("ctlog failed", "error", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  ctlog list    [--source NAME] [--json]
  ctlog process  --source NAME --ctlog ID [--watch] [--watch-interval D] [--dry-run] [--limit N]`)
}
```

Stub `cmdList`/`cmdProcess` to `return fmt.Errorf("not implemented")` using `flag.NewFlagSet` for their args (filled in Tasks 5–6). Keep `tunedHTTPClient`, `deriveCTLogID` (rename of `deriveShardName`), retention/source wiring helpers.

- [ ] **Step 3: Build + tests**

Run: `go build ./... && go vet ./... && go test ./...`
Expected: PASS (no `search`; main compiles with stubs).

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "refactor: remove date-window mode; subcommand dispatch skeleton"
```

---

## Task 2: Source config (`sources.json`)

**Files:**
- Create: `internal/loglist/source.go`, `internal/loglist/source_test.go`
- Create: `sources.json`
- Modify: `internal/config/config.go`

**Interfaces:**
- Produces: `loglist.Source{ Name, Type, Operator, LogPrefix string }`, `loglist.LoadSources(path)([]Source,error)`, config `SourcesFile` (env `CTLOG_SOURCES_FILE`, default `./sources.json`), config `ShardListURL` (env `CTLOG_SHARD_LIST_URL`, default `https://www.gstatic.com/ct/log_list/v3/all_logs_list.json`).

- [ ] **Step 1: Failing test**

`internal/loglist/source_test.go`:

```go
package loglist

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadSources(t *testing.T) {
	p := filepath.Join(t.TempDir(), "sources.json")
	os.WriteFile(p, []byte(`[
	  {"name":"le-sycamore","type":"tiled","operator":"Let's Encrypt","log_prefix":"Sycamore"},
	  {"name":"google-xenon-2025","type":"rfc6962","operator":"Google","log_prefix":"Xenon2025"}
	]`), 0o644)
	got, err := LoadSources(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Name != "le-sycamore" || got[1].Type != "rfc6962" {
		t.Fatalf("got %+v", got)
	}
}

func TestLoadSourcesRejectsBadType(t *testing.T) {
	p := filepath.Join(t.TempDir(), "b.json")
	os.WriteFile(p, []byte(`[{"name":"x","type":"bogus","operator":"o","log_prefix":"p"}]`), 0o644)
	if _, err := LoadSources(p); err == nil {
		t.Fatal("want error")
	}
}
```

- [ ] **Step 2: Run → FAIL** (`go test ./internal/loglist/ -run TestLoadSources -v`) → "undefined: LoadSources".

- [ ] **Step 3: Implement `source.go`**

```go
package loglist

import (
	"encoding/json"
	"fmt"
	"os"
)

// Source is a configured CT provider: one operator's logs selected by a
// description prefix, read via the given protocol.
type Source struct {
	Name      string `json:"name"`
	Type      string `json:"type"`       // "tiled" | "rfc6962"
	Operator  string `json:"operator"`
	LogPrefix string `json:"log_prefix"`
}

// LoadSources reads and validates the sources JSON file.
func LoadSources(path string) ([]Source, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read sources file %s: %w", path, err)
	}
	var ss []Source
	if err := json.Unmarshal(b, &ss); err != nil {
		return nil, fmt.Errorf("parse sources file: %w", err)
	}
	for i, s := range ss {
		if s.Name == "" {
			return nil, fmt.Errorf("source %d: name required", i)
		}
		if s.Type != "tiled" && s.Type != "rfc6962" {
			return nil, fmt.Errorf("source %q: type must be tiled or rfc6962, got %q", s.Name, s.Type)
		}
	}
	return ss, nil
}

// Find returns the source with the given name.
func Find(sources []Source, name string) (Source, bool) {
	for _, s := range sources {
		if s.Name == name {
			return s, true
		}
	}
	return Source{}, false
}
```

- [ ] **Step 4: Run → PASS.**

- [ ] **Step 5: Config + sources.json**

Add to `Config`:
```go
	SourcesFile  string `env:"CTLOG_SOURCES_FILE" envDefault:"./sources.json"`
	ShardListURL string `env:"CTLOG_SHARD_LIST_URL" envDefault:"https://www.gstatic.com/ct/log_list/v3/all_logs_list.json"`
```
Create `sources.json`:
```json
[
  {"name": "le-sycamore", "type": "tiled", "operator": "Let's Encrypt", "log_prefix": "Sycamore"},
  {"name": "le-willow",   "type": "tiled", "operator": "Let's Encrypt", "log_prefix": "Willow"}
]
```

- [ ] **Step 6: Commit** — `go test ./... && gofmt -w . && git add -A && git commit -m "feat: source config (sources.json)"`

---

## Task 3: Enumerate ctlogs with full metadata

**Files:**
- Modify: `internal/loglist/loglist.go` (rename `Shard`→`CTLog`; parse `log_id`, `mmd`, urls; add `CTLogs(source)`)
- Create: `internal/loglist/loglist_ctlogs_test.go`

**Interfaces:**
- Produces:
  - `loglist.CTLog{ ID, Description, LogID, Type, Source, MonitoringURL, SubmissionURL, URL, State string; MMD int; Start, End time.Time }`
  - `CTLog.Phase(now time.Time) Phase` (`PhaseFrozen` if `!now.Before(End)` else `PhaseActive`)
  - `loglist.CTLogs(ctx, hc, listURL string, s Source, idFn func(CTLog) string) ([]CTLog, error)` — reads `tiled_logs[]` or `logs[]` per `s.Type`, sets `Type`/`Source`, computes `ID` via `idFn`.
- Consumes: `Source` (Task 2).

- [ ] **Step 1: Failing test (httptest fixture)**

`internal/loglist/loglist_ctlogs_test.go`:

```go
package loglist

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

const fixture = `{"operators":[
 {"name":"Let's Encrypt",
  "logs":[{"description":"LE OldRFC2025","log_id":"AAA=","url":"https://old.example/","mmd":86400,"temporal_interval":{"start_inclusive":"2025-01-01T00:00:00Z","end_exclusive":"2025-07-01T00:00:00Z"}}],
  "tiled_logs":[{"description":"Let's Encrypt 'Sycamore2025h2d'","log_id":"BBB=","submission_url":"https://sub.example/2025h2d/","monitoring_url":"https://mon.example/2025h2d/","mmd":60,"state":{"usable":{}},"temporal_interval":{"start_inclusive":"2025-06-19T00:00:00Z","end_exclusive":"2025-12-18T00:00:00Z"}}]},
 {"name":"Google",
  "logs":[{"description":"Google 'Xenon2025h2'","log_id":"CCC=","url":"https://ct.googleapis.com/logs/xenon2025h2/","mmd":86400,"temporal_interval":{"start_inclusive":"2025-07-01T00:00:00Z","end_exclusive":"2026-01-01T00:00:00Z"}}]}
]}`

func serve(t *testing.T) (*http.Client, string) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte(fixture)) }))
	t.Cleanup(s.Close)
	return s.Client(), s.URL
}

func id(c CTLog) string { return "ID:" + c.Description }

func TestCTLogsTiled(t *testing.T) {
	hc, url := serve(t)
	got, err := CTLogs(context.Background(), hc, url, Source{Type: "tiled", Operator: "Let's Encrypt", LogPrefix: "Sycamore"}, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("len=%d", len(got))
	}
	c := got[0]
	if c.MonitoringURL != "https://mon.example/2025h2d/" || c.LogID != "BBB=" || c.MMD != 60 || c.Type != "tiled" {
		t.Fatalf("ctlog=%+v", c)
	}
	if c.ID != "ID:Let's Encrypt 'Sycamore2025h2d'" {
		t.Errorf("id=%q", c.ID)
	}
}

func TestCTLogsRFC6962(t *testing.T) {
	hc, url := serve(t)
	got, err := CTLogs(context.Background(), hc, url, Source{Type: "rfc6962", Operator: "Google", LogPrefix: "Xenon2025"}, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].MonitoringURL != "https://ct.googleapis.com/logs/xenon2025h2/" || got[0].Type != "rfc6962" {
		t.Fatalf("ctlog=%+v", got)
	}
}
```

- [ ] **Step 2: Run → FAIL** (`go test ./internal/loglist/ -run TestCTLogs -v`).

- [ ] **Step 3: Implement** — rename `Shard`→`CTLog`, add fields, parse both arrays, refactor HTTP/unmarshal into `fetchList`. For rfc6962 set `MonitoringURL=URL`, `State="rfc6962"`. `CTLogs` sets `Type=s.Type`, `Source=s.Name`, `ID=idFn(c)`, sorts by `Start`. Update existing `runListShards`/`runShardDrain` callers (removed/replaced in Tasks 5–6, so update or delete now to keep build green).

```go
type CTLog struct {
	ID            string    // friendly id (set by caller via idFn)
	Description   string
	LogID         string    // canonical base64 SHA-256(pubkey)
	Type          string    // tiled | rfc6962
	Source        string    // source name
	MonitoringURL string
	SubmissionURL string
	URL           string
	State         string
	MMD           int
	Start         time.Time
	End           time.Time
}

func (c CTLog) Phase(now time.Time) Phase {
	if !now.Before(c.End) {
		return PhaseFrozen
	}
	return PhaseActive
}
```

- [ ] **Step 4: Run → PASS.**

- [ ] **Step 5: Commit** — `go test ./... && gofmt -w . && git add -A && git commit -m "feat: enumerate ctlogs with full metadata"`

---

## Task 4: ctlog status (reachability + head + processing metadata)

**Files:**
- Create: `internal/loglist/status.go`, `internal/loglist/status_test.go`

**Interfaces:**
- Consumes: `CTLog` (Task 3), `ctclient.New`, `tileclient.New`, `control.WorkUnitStatus` (already in control).
- Produces:
  - `loglist.PercentDone(cursor, head int64) float64`
  - `loglist.Head(ctx, hc, c CTLog, retries int) (head uint64, reachable bool)`
  - `loglist.CTLogStatus{ CTLog; Phase string; Reachable bool; Head int64; Tracked bool; Status string; Cursor, CertsWritten, SANsWritten int64; PercentDone float64 }` (JSON-tagged)

- [ ] **Step 1: Failing test** — `status_test.go` with the `PercentDone` table from before (`{0,100→0},{50,100→50},{100,100→100},{0,0→0}`).

- [ ] **Step 2: Run → FAIL.**

- [ ] **Step 3: Implement `status.go`**

```go
package loglist

import (
	"context"
	"net/http"

	"github.com/pulsarpoint/pulsarprotectctlog/internal/ctclient"
	"github.com/pulsarpoint/pulsarprotectctlog/internal/tileclient"
)

type CTLogStatus struct {
	CTLog
	Phase        string  `json:"phase"`
	Reachable    bool    `json:"reachable"`
	Head         int64   `json:"head"`
	Tracked      bool    `json:"tracked"`
	Status       string  `json:"status"`
	Cursor       int64   `json:"cursor"`
	CertsWritten int64   `json:"certs_written"`
	SANsWritten  int64   `json:"sans_written"`
	PercentDone  float64 `json:"percent_done"`
}

func PercentDone(cursor, head int64) float64 {
	if head <= 0 {
		return 0
	}
	return float64(cursor) / float64(head) * 100
}

// Head probes the ctlog's current entry count and reachability.
func Head(ctx context.Context, hc *http.Client, c CTLog, retries int) (uint64, bool) {
	if c.Type == "rfc6962" {
		cl, err := ctclient.New(c.MonitoringURL, c.Description, hc, retries)
		if err != nil {
			return 0, false
		}
		n, err := cl.TreeSize(ctx)
		return n, err == nil
	}
	n, err := tileclient.New(c.MonitoringURL, c.Description, hc, retries).TreeSize(ctx)
	return n, err == nil
}
```

- [ ] **Step 4: Run → PASS.**

- [ ] **Step 5: Commit** — `gofmt -w . && git add -A && git commit -m "feat: ctlog status helpers"`

---

## Task 5: `list` subcommand (the API)

**Files:**
- Modify: `cmd/ctlog/main.go` (implement `cmdList`)

**Interfaces:**
- Consumes: `LoadSources`, `Find`, `CTLogs`, `Head`, `PercentDone`, `control.OpenReadOnly`+`ListWorkUnits`, `deriveCTLogID`.
- Produces: `cmdList(args []string) error`. Flags: `--source` (optional; empty = all), `--json`.

- [ ] **Step 1: Implement cmdList**

```go
func cmdList(args []string) error {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	src := fs.String("source", "", "source name (empty = all sources)")
	asJSON := fs.Bool("json", false, "emit JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx := context.Background()
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	hc := tunedHTTPClient(cfg.HTTPTimeout, cfg.FetchParallel)

	sources, err := loglist.LoadSources(cfg.SourcesFile)
	if err != nil {
		return err
	}
	if *src != "" {
		s, ok := loglist.Find(sources, *src)
		if !ok {
			return fmt.Errorf("unknown source %q", *src)
		}
		sources = []loglist.Source{s}
	}

	processed := map[string]control.WorkUnitStatus{}
	if st, err := control.OpenReadOnly(ctx, cfg.ControlDBPath); err != nil {
		return err
	} else if st != nil {
		defer st.Close()
		rows, err := st.ListWorkUnits(ctx)
		if err != nil {
			return err
		}
		for _, r := range rows {
			processed[r.ID] = r
		}
	}

	now := control.Now()
	var out []loglist.CTLogStatus
	for _, s := range sources {
		ctlogs, err := loglist.CTLogs(ctx, hc, cfg.ShardListURL, s, deriveCTLogIDFromLog)
		if err != nil {
			return err
		}
		for _, c := range ctlogs {
			head, ok := loglist.Head(ctx, hc, c, cfg.MaxRetries)
			wu, tracked := processed[c.ID]
			out = append(out, loglist.CTLogStatus{
				CTLog: c, Phase: string(c.Phase(now)), Reachable: ok, Head: int64(head),
				Tracked: tracked, Status: statusOr(wu, tracked), Cursor: wu.NextIndex,
				CertsWritten: wu.CertsWritten, SANsWritten: wu.SANsWritten,
				PercentDone: loglist.PercentDone(wu.NextIndex, int64(head)),
			})
		}
	}

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(out)
	}
	printCTLogTable(out)
	return nil
}

func statusOr(wu control.WorkUnitStatus, tracked bool) string {
	if !tracked {
		return "not-started"
	}
	return wu.Status
}

// deriveCTLogIDFromLog computes the friendly id from a ctlog's monitoring URL.
func deriveCTLogIDFromLog(c loglist.CTLog) string { return deriveCTLogID(c.MonitoringURL) }
```

`printCTLogTable` prints columns: `source ctlog interval phase reach head cursor done% status`. `deriveCTLogID` is the renamed `deriveShardName`.

- [ ] **Step 2: Build + manual smoke**

Run: `go build ./... && go vet ./... && go test ./...` → PASS.
Manual: `./ctlog list --source le-sycamore --json` → JSON array of ctlogs with metadata + processing state; `./ctlog list` → table for all sources.

- [ ] **Step 3: Commit** — `gofmt -w . && git add -A && git commit -m "feat: ctlog list subcommand (JSON + table)"`

---

## Task 6: `process` subcommand (drain a ctlog)

**Files:**
- Modify: `cmd/ctlog/main.go` (implement `cmdProcess`; reuse drain logic)

**Interfaces:**
- Consumes: `LoadSources`/`Find`, `CTLogs`, `Head`, `CTLog.Phase`, `source.NewTile`/`NewRFC6962`, `tileclient.New`/`ctclient.New`, `ingest.New(...).WithFinalize(...).WithLimit(...)`, `clickhouse.Open`, `control.Open`.
- Produces: `cmdProcess(args []string) error`. Flags: `--source` (required), `--ctlog` (required, friendly id), `--watch`, `--watch-interval` (default 15m), `--dry-run`, `--limit`.

- [ ] **Step 1: Implement cmdProcess**

```go
func cmdProcess(args []string) error {
	fs := flag.NewFlagSet("process", flag.ContinueOnError)
	srcName := fs.String("source", "", "source name (required)")
	ctlogID := fs.String("ctlog", "", "ctlog friendly id (required)")
	watch := fs.Bool("watch", false, "tail an active ctlog's delta after draining to head")
	interval := fs.Duration("watch-interval", 15*time.Minute, "poll interval for --watch")
	dryRun := fs.Bool("dry-run", false, "do not write to ClickHouse")
	limit := fs.Int("limit", 0, "max entries (0 = unlimited)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *srcName == "" || *ctlogID == "" {
		return fmt.Errorf("--source and --ctlog are required")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	hc := tunedHTTPClient(cfg.HTTPTimeout, cfg.FetchParallel)

	sources, err := loglist.LoadSources(cfg.SourcesFile)
	if err != nil {
		return err
	}
	s, ok := loglist.Find(sources, *srcName)
	if !ok {
		return fmt.Errorf("unknown source %q", *srcName)
	}
	ctlogs, err := loglist.CTLogs(ctx, hc, cfg.ShardListURL, s, deriveCTLogIDFromLog)
	if err != nil {
		return err
	}
	var target *loglist.CTLog
	for i := range ctlogs {
		if ctlogs[i].ID == *ctlogID {
			target = &ctlogs[i]
			break
		}
	}
	if target == nil {
		return fmt.Errorf("ctlog %q not found in source %q", *ctlogID, *srcName)
	}

	var src source.Source
	if target.Type == "rfc6962" {
		cl, err := ctclient.New(target.MonitoringURL, target.ID, hc, cfg.MaxRetries)
		if err != nil {
			return err
		}
		src = source.NewRFC6962(cl, cfg.BatchSize)
	} else {
		src = source.NewTile(tileclient.New(target.MonitoringURL, target.ID, hc, cfg.MaxRetries), cfg.FetchParallel)
	}

	var chStore *clickhouse.Store
	var ctrl *control.Store
	if !*dryRun {
		chStore, err = clickhouse.Open(ctx, cfg.ClickHouseAddr, cfg.ClickHouseDatabase, cfg.ClickHouseUser, cfg.ClickHousePassword)
		if err != nil {
			return err
		}
		defer chStore.Close()
		if err := chStore.EnsureSchema(ctx, cfg.ClickHouseStorage); err != nil {
			return err
		}
		ctrl, err = control.Open(ctx, cfg.ControlDBPath)
		if err != nil {
			return err
		}
		defer ctrl.Close()
	}

	drain := func(finalize bool) error {
		head, err := src.TreeSize(ctx)
		if err != nil {
			return err
		}
		if ctrl != nil {
			_ = ctrl.SetEnd(ctx, target.ID, int64(head))
		}
		unit := model.WorkUnit{ID: target.ID, LogName: target.ID, StartIndex: 0, EndIndex: head, WindowFrom: target.Start, WindowTo: target.End}
		began := time.Now()
		stats, err := ingest.New(src, chStore, ctrl, cfg.WriteBatchSize).WithFinalize(finalize).WithLimit(*limit).Run(ctx, unit)
		if err != nil {
			return err
		}
		slog.Info("drain cycle", "ctlog", target.ID, "head", head, "entries", stats.EntriesProcessed,
			"certs", stats.CertsWritten, "sans", stats.SANsWritten, "parse_errors", stats.ParseErrors, "elapsed", time.Since(began).Round(time.Second))
		return nil
	}

	if !*watch {
		return drain(true)
	}
	slog.Info("watch: tailing ctlog delta", "ctlog", target.ID, "interval", *interval)
	for {
		if err := drain(false); err != nil {
			return err
		}
		select {
		case <-time.After(*interval):
		case <-ctx.Done():
			return nil
		}
	}
}
```

- [ ] **Step 2: Build + dry-run smoke**

Run: `go build ./... && go vet ./... && go test ./...` → PASS.
Manual: `./ctlog process --source le-sycamore --ctlog sycamore-2025h2d --dry-run --limit 2000` → drains first 2000 leaves, no writes; `--ctlog bogus` → "ctlog \"bogus\" not found".

- [ ] **Step 3: Commit** — `gofmt -w . && git add -A && git commit -m "feat: ctlog process subcommand (drain 0..head)"`

---

## Verification

- **Unit:** `go test ./...` green (loglist sources/ctlogs/percent, parse, retention, tileclient).
- **list JSON:** `./ctlog list --source le-sycamore --json` returns an array; each element has `id`, `log_id`, `monitoring_url`, `state`, `mmd`, interval, `type`, `source`, `phase`, `reachable`, `head`, and processing fields (`tracked`,`status`,`cursor`,`certs_written`,`sans_written`,`percent_done`).
- **list table:** `./ctlog list` prints all sources' ctlogs with phase/reach/head/cursor/done%/status.
- **process:** `./ctlog process --source le-sycamore --ctlog sycamore-2025h2d` drains `0…head`; control DB row `sycamore-2025h2d` reaches `status=done`; re-run skips (already done).
- **No date flags:** `./ctlog process --from ...` → "flag provided but not defined: -from".

## Out of scope (future plans)

- HTTP API exposing the same `list` data to other platform services (CLI/JSON first, per decision).
- `process --source X` (all ctlogs in a source: drain frozen, watch active, skip retired) — natural next subcommand once per-ctlog `process` is proven.
- Multi-worker control plane (promote local SQLite → shared store).
- Cross-operator completeness audit + retired-sub-ctlog gap recovery via other sources / RFC6962.
- Per-drain progress logging; periodic log-list refresh inside long `--watch`.
