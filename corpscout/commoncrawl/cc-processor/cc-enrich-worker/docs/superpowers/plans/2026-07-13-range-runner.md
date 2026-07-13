# Range Runner Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** cc-enrich-worker natively processes WARC-part ranges in two explicit lanes (local = whole-WARC download, remote = range reads) with a marker state machine and a decoupled ClickHouse loader, per `docs/superpowers/specs/2026-07-13-range-runner-design.md`.

**Architecture:** A DuckDB classifier over the existing `catalog.duckdb` partitions parts A–B by `--remote-max-pages`. Producers run one lane each (`--mode local|remote`), produce parquet + `.produced` markers, never write to CH. A separate `load --scan` sweeps markers and writes `.loaded`. `plan` and `status` are read-only reporting.

**Tech Stack:** Go 1.26; existing deps (duckdb-go/v2, parquet-go, clickhouse-go) plus `github.com/fsnotify/fsnotify` (loader `--watch` only).

## Global Constraints

- Module: `corpscout/commoncrawl/cc-processor/cc-enrich-worker`; git root `companycollect/`. Stage commits ONLY by explicit paths under the module (shared working tree — NEVER `git add -A`).
- TDD per task: failing test first, watch it fail, implement, watch it pass. One commit per task, Conventional Commits, ending `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.
- `--mode local|remote` REQUIRED for `--parts` runs; industry/embed reject `--mode local`. The lane FORCES the fetch strategy (no open-time threshold decision in range runs).
- Split: selected pages <= `--remote-max-pages` → remote; > → local; 0 → empty (skipped, reported). Same flag value must partition disjointly.
- Producer writes no CH data. Markers: `out_<cmd>_<part>.produced` (JSON row counts) and `.loaded`, siblings of the output dir, written via temp+rename.
- Failure policy: continue+summary, abort after 5 CONSECUTIVE produce failures, exit non-zero if any part failed. Setup failures abort immediately.
- Existing single-part `--part N` behavior unchanged. All existing tests stay green; `gofmt -l` empty; `go vet` clean; `go test -race ./...` green per task.

---

### Task 1: Parts range + runner flag parsing/validation

**Files:**
- Create: `cmd/cc-enrich-worker/rangeopts.go`, `cmd/cc-enrich-worker/rangeopts_test.go`

**Interfaces (Produces):**
```go
type partsRange struct{ lo, hi uint32 } // inclusive
func parsePartsRange(s string) (partsRange, error) // "A-B" or single "N"; A<=B; fits uint32
type runnerOpts struct {
    parts          partsRange
    mode           string // "local" | "remote"
    remoteMaxPages int64  // required > 0 for tech/both; industry/embed: used only to warn
    warcParallel   int    // remote lane; default 4, >=1
    downloadParallel int  // local lane; default 2, >=1
    processParallel  int  // local lane; default 2, >=1
    maxWARCFiles     int  // local lane; REQUIRED >=1 (recommend 5); downloadParallel <= maxWARCFiles
}
func validateRunnerOpts(cmd string, o runnerOpts) error
```
`validateRunnerOpts` rules (exact): mode must be `local` or `remote`; cmd `industry`/`embed` + mode `local` → error `industry/embed selections are sparse; only --mode remote is supported`; mode local requires `maxWARCFiles >= 1` and `downloadParallel <= maxWARCFiles`; tech/both require `remoteMaxPages >= 1`.

- [ ] **Step 1: failing tests** — table test `TestParsePartsRange` (`"0-10"`→{0,10}; `"7"`→{7,7}; errors: `""`, `"5-2"`, `"a-b"`, `"-1-2"`, `"0-4294967296"`) and `TestValidateRunnerOpts` (each rule above, valid cases for all four cmds).
- [ ] **Step 2:** run `go test ./cmd/... -run 'TestParsePartsRange|TestValidateRunnerOpts'` → FAIL (undefined).
- [ ] **Step 3:** implement in `rangeopts.go` (strings.Cut on "-", strconv.ParseUint 32-bit, plain error returns via `fmt.Errorf`).
- [ ] **Step 4:** tests pass. **Step 5:** commit `feat(cc-enrich-worker): parts-range and runner flag validation`.

---

### Task 2: Classifier over catalog.duckdb

**Files:**
- Create: `internal/catalog/classify.go`, `internal/catalog/classify_test.go`

**Interfaces (Produces):**
```go
type PartStats struct {
    WarcIndex     uint32
    Pages         int64
    SelectedBytes int64
}
// LoadPartStats aggregates selected pages/bytes per part for warc_index in [lo,hi],
// from the SAME catalog.duckdb file LoadWARC reads. Parts absent from the range are
// simply not returned (they are "empty").
func LoadPartStats(ctx context.Context, path string, lo, hi uint32) ([]PartStats, error)

type Classification struct{ Local, Remote, Empty []uint32 }
// ClassifyParts partitions [lo,hi]: stats with Pages <= remoteMaxPages -> Remote,
// > -> Local; indices in [lo,hi] with no stats row -> Empty. Slices ascending.
func ClassifyParts(stats []PartStats, lo, hi uint32, remoteMaxPages int64) Classification
```

`LoadPartStats` mirrors `LoadWARC`'s connection discipline exactly (open `path+"?access_mode=read_only&threads=1"`, `SetMaxOpenConns(1)`, single `Conn`), with query:
```sql
SELECT CAST(warc_index AS UBIGINT), CAST(count(*) AS BIGINT),
       CAST(sum(warc_record_length) AS BIGINT)
FROM main.pages WHERE warc_index BETWEEN ? AND ?
GROUP BY warc_index ORDER BY warc_index
```

- [ ] **Step 1: failing tests** — build a fixture `catalog.duckdb` in `t.TempDir()` via `database/sql` + duckdb driver (`CREATE TABLE main.warcs(...); CREATE TABLE main.pages(warc_index INT, root_domain VARCHAR, url VARCHAR, domain_page_rank INT, warc_record_offset BIGINT, warc_record_length BIGINT);` + INSERTs): part 0 with 3 pages, part 1 with 1 page, part 3 with 5 pages (part 2 absent). Assert `LoadPartStats(ctx, p, 0, 3)` returns exactly those three rows with correct sums, and `ClassifyParts(stats, 0, 3, 2)` → Remote=[1], Local=[0,3], Empty=[2].
- [ ] **Step 2:** FAIL (undefined). **Step 3:** implement. **Step 4:** pass; also `go test ./internal/catalog/` all green.
- [ ] **Step 5:** commit `feat(cc-enrich-worker): DuckDB part classifier for range runs`.

---

### Task 3: `plan` subcommand

**Files:**
- Create: `cmd/cc-enrich-worker/plancmd.go`, `cmd/cc-enrich-worker/plancmd_test.go`
- Modify: `cmd/cc-enrich-worker/main.go` (add `case "plan": runPlanCmd(os.Args[2:])` to the switch; add `plan` line to `usage()`)

**Interfaces:** `runPlanCmd(args []string)` parses `--crawl-id --selection --base --parts --remote-max-pages` (reuse Task 1 parser), resolves the catalog path exactly as `run()` does today (`filepath.Join(base, crawlID, "warc-index", selection, "catalog.duckdb")` — and when the S3 catalog config env is present, reuse the existing cache-download call from main.go:~313 so `plan` works before any run). It prints via a pure, testable formatter:
```go
type planReport struct {
    Lo, Hi                 uint32
    Local, Remote, Empty   int
    LocalPages, RemotePages int64
    LocalBytes, RemoteBytes int64
    Sweep                  []sweepRow // X in {100, 500, 1000, 2500, 5000, 10000} plus the flag value
}
type sweepRow struct{ X int64; Local, Remote int }
func buildPlanReport(stats []catalog.PartStats, lo, hi uint32, x int64) planReport
func (r planReport) String() string // human table; download estimate = Local parts x ~1 GiB
```
- [ ] **Step 1: failing test** — `TestBuildPlanReport` on the Task 2 fixture stats: counts, byte totals, sweep rows monotonic (Local non-increasing as X grows), flag value included in sweep.
- [ ] **Step 2:** FAIL. **Step 3:** implement formatter + command wiring. **Step 4:** pass; `go build ./...`.
- [ ] **Step 5:** commit `feat(cc-enrich-worker): plan subcommand — lane stats and threshold sweep`.

---

### Task 4: Marker files

**Files:**
- Create: `internal/markers/markers.go`, `internal/markers/markers_test.go`

**Interfaces (Produces):**
```go
type Produced struct {
    Part        uint32            `json:"part"`
    Cmd         string            `json:"cmd"`
    Rows        map[string]int    `json:"rows"` // kind -> row count, e.g. "domains": 812
    SourceRunID string            `json:"source_run_id"`
    DurationS   float64           `json:"duration_s"`
    FinishedAt  time.Time         `json:"finished_at"`
}
func ProducedPath(outDir string) string // outDir + ".produced"
func LoadedPath(outDir string) string   // outDir + ".loaded"
func WriteProduced(outDir string, p Produced) error // json to tmp file in same dir parent, then os.Rename
func ReadProduced(outDir string) (Produced, error)
func WriteLoaded(outDir string) error   // empty file via temp+rename
func Exists(path string) bool
```
- [ ] **Step 1: failing tests** — round-trip WriteProduced/ReadProduced in t.TempDir(); WriteProduced leaves no `*.tmp*` sibling; Exists false→true transitions; WriteLoaded idempotent (second call succeeds).
- [ ] **Step 2:** FAIL. **Step 3:** implement (`os.CreateTemp(filepath.Dir(path), ".marker-*")`, write, close, rename). **Step 4:** pass.
- [ ] **Step 5:** commit `feat(cc-enrich-worker): part marker files (.produced/.loaded)`.

---

### Task 5: runPart refactor + forced-mode open

**Files:**
- Modify: `internal/warcinput/input.go` + `input_test.go`
- Modify: `cmd/cc-enrich-worker/main.go`

**Part A — warcinput.** Refactor `Plan.Open` (which currently computes whole-vs-range from the threshold) into:
```go
// OpenAs opens the plan with an EXPLICIT mode (ModeRange or ModeWholeFile), skipping the
// threshold decision — range runners force the lane's strategy. Empty plans still return
// ModeEmpty. Open(threshold...) keeps its exact behavior by computing the mode then
// delegating to OpenAs.
func (plan Plan) OpenAs(ctx context.Context, objects fetch.ObjectGetter, bucket string, mode Mode, tmpDir string) (*Input, error)
```
TDD: failing test first — `TestOpenAsForcesMode` (a 2-record fixture plan opened with ModeWholeFile downloads even though coverage is tiny; with ModeRange it doesn't create the temp file even at 100% coverage), reusing the existing fake object-getter fixtures in `input_test.go`. Then refactor; ALL existing warcinput tests must stay green unchanged.

**Part B — main.go.** Mechanical but careful refactor, no behavior change for `--part`:
- Extract the per-part body of `run()` (everything from `LoadPlan` through writing outputs, EXCLUDING flag/base validation, tech-matcher setup, embed-client + reference setup, and CH connections) into:
```go
type partDeps struct { // built once per process
    mode string; o opts
    emb classify.Embedder; ref *mdl.Reference; protos *mdl.Prototypes
    objects fetch.ObjectGetter; source string
}
type partResult struct{ Rows map[string]int; Domains, Embeds int }
func producePart(ctx context.Context, d partDeps, part uint32, forced warcinput.Mode, outDir string) (partResult, error)
```
- Every `log.Fatalf`/`fatal(...)` inside the extracted body becomes `return partResult{}, fmt.Errorf(...)` (the cleanup helper keeps running via defer). `forced == ""` means "legacy threshold decision" (single-part path passes `""`; range lanes pass ModeRange/ModeWholeFile).
- `partResult.Rows` is filled from the streamer counts (tech/both) or `len(domains)`/`len(embeddings)` etc. (industry/embed) — the numbers the `.produced` marker and the loader's verify need.
- `run()` (single-part) becomes: build deps → `res, err := producePart(...)` → on err `log.Fatalf` → done log. Its observable behavior (messages, exit codes, files) stays identical — the existing `cmd` tests and worker integration tests are the guard.
- [ ] Steps: failing warcinput test → OpenAs → green; then the main.go extraction with `go test ./cmd/... ./internal/worker/ ./internal/warcinput/` green and `go build ./...` clean after each move; commit `refactor(cc-enrich-worker): extract producePart and forced-mode open` (single commit).

---

### Task 6: Remote lane runner

**Files:**
- Create: `cmd/cc-enrich-worker/runrange.go`, `cmd/cc-enrich-worker/runrange_test.go`
- Modify: `cmd/cc-enrich-worker/main.go` (`--parts/--mode/...` flags registered per Task 1; when `--parts` set → `runRange(cmd, o, ro)` instead of `run()`; `--part` and `--parts` mutually exclusive)

**Behavior (exact):**
1. Resolve catalog path; `LoadPartStats` + `ClassifyParts`; print `mode=remote X=<flag> parts=<n_class> local=<n> remote=<n> empty=<n>`.
2. For industry/embed: class = ALL non-empty parts (spec Decisions #1); for tech/both: class = Remote list.
3. Build deps ONCE (tech matcher / embed client / reference — reference load may contact CH; that is setup, allowed).
4. Worker pool of `min(warcParallel, len(class))` goroutines consuming a parts channel. Per part:
   - `markers.Exists(ProducedPath(outDir))` → skip (count `skipped`).
   - outDir exists without marker → `os.RemoveAll(outDir)` (crashed produce), log it.
   - `producePart(ctx, deps, part, warcinput.ModeRange, outDir)`; on success `markers.WriteProduced` with the row counts; on failure record `{part, err}`.
5. Consecutive-failure breaker: a shared counter — success resets to 0, failure increments; at 5, cancel the pool context; remaining parts are neither run nor marked. Breaker message names the last 5 parts.
6. Summary: `produced=%d skipped=%d failed=%d [parts: ...] elapsed=%s`; `os.Exit(1)` iff failed>0 (breaker also exit 1).

**Tests (failing first, in `runrange_test.go`):** drive the pool with the `multiGetter`/`gzWarc` fixtures from `internal/worker` tests, 4 parts (catalog fixture from Task 2 pattern): all succeed → 4 markers + parquet dirs; one part's WARC bytes missing → that part fails, other 3 marked, summary lists it, rerun produces ONLY the failed part (others skipped); `warcParallel=2` output equals sequential run byte-for-byte on domains.parquet row sets; breaker test: getter that fails everything → stops after exactly 5 attempts.
- [ ] RED → implement → GREEN → `go test -race ./cmd/...` → commit `feat(cc-enrich-worker): remote-lane range runner with markers, breaker, resume`.

---

### Task 7: Local lane runner

**Files:**
- Modify: `cmd/cc-enrich-worker/runrange.go` (+ tests)

**Behavior (exact):** class = Local list (tech/both only — Task 1 validation already rejects industry/embed).
- `slots := make(chan struct{}, maxWARCFiles)` — ONE token per WARC file on disk (acquired before a download starts, released only after the part completes and its temp dir is removed). This single semaphore IS the `--max-warc-files` guarantee: in-flight + buffered can never exceed N.
- DOWNLOAD pool: `downloadParallel` goroutines pull parts off the class list; for each: acquire slot → skip-checks (marker/crashed dir, releasing the slot on skip) → `LoadPlan` + `plan.OpenAs(..., ModeWholeFile, tmpDir)` → send `preparedPart{part, input, outDir}` on a channel (unbuffered — the slot cap, not a channel buffer, bounds disk).
- PROCESS pool: `processParallel` goroutines receive prepared parts and run the post-open remainder of `producePart` (Task 5 splits `producePart` internally into `openInput` + `processInput` so both lanes share `processInput`), write the marker, `input.Close()` + remove temp dir, release slot.
- Failure in download stage or process stage records the part as failed (slot released, temp cleaned); same breaker and summary as remote.
- [ ] **Tests (failing first):** 3 local-class parts with `maxWARCFiles=1, downloadParallel=1, processParallel=1` → outputs identical to sequential remote-forced-whole runs; instrument with a getter wrapper counting concurrent open temp files → never exceeds maxWARCFiles even with `downloadParallel=2, maxWARCFiles=2`; temp `.warc-input` dirs all gone at exit; failed part leaves no temp file and no marker.
- [ ] RED → implement → GREEN → `-race` → commit `feat(cc-enrich-worker): local-lane runner — bounded download/process pools`.

---

### Task 8: Loader `--scan` / `--watch`

**Files:**
- Modify: `cmd/cc-enrich-worker/main.go` (`runLoad`: add `--scan <root>`, `--watch`, `--parallel K`; `--scan` mutually exclusive with `--dir/--file`)
- Create: `internal/load/scan.go`, `internal/load/scan_test.go`
- Dependency: `go get github.com/fsnotify/fsnotify@latest && go mod tidy`

**Interfaces (Produces):**
```go
type ScanResult struct{ Loaded, Pending, Failed int; FailedDirs []string }
// Sweep walks root for "*.produced" markers lacking ".loaded", loads each dir via FromDir,
// verifies per-table loaded rows >= the marker's recorded counts for every kind present,
// writes .loaded on success. K dirs load concurrently. Errors are per-dir: logged,
// counted, never abort the sweep.
func Sweep(ctx context.Context, conn driver.Conn, root string, parallel int) (ScanResult, error)
```
`--watch`: loop { Sweep; wait until fsnotify Create/Rename event under root OR 5-minute ticker; } — the ticker path is unconditional (correctness), fsnotify is latency only; watcher errors degrade to pure ticker with one warning.
- [ ] **Step 1 failing tests:** fixture tree with two produced parts (real parquet via `output.Write*` + `markers.WriteProduced` with true counts) and one already-loaded → integration against real ClickHouse when `CLICKHOUSE_HOST` env reachable (`t.Skip` otherwise, message says why): Sweep loads 2, writes `.loaded`, second Sweep loads 0; a marker whose recorded count EXCEEDS parquet rows → dir counted Failed, no `.loaded`, other dirs still load. Unit-testable without CH: the walker (finds produced-without-loaded, ignores others) via an injected loadFn — split `Sweep` into `findPending(root) []string` (pure, tested without CH) + the load loop.
- [ ] RED → implement → GREEN (incl. real-CH run locally) → commit `feat(cc-enrich-worker): marker-driven loader sweep with --watch`.

---

### Task 9: `status` command, docs, full verification

**Files:**
- Create: `cmd/cc-enrich-worker/statuscmd.go`, `cmd/cc-enrich-worker/statuscmd_test.go`
- Modify: `cmd/cc-enrich-worker/main.go` (switch + usage), `README.md` (new commands + lane flags + two-server operating model + loader deployment), spec status → implemented.

**Interfaces:** `runStatusCmd(args []string)` with `--root`; walks markers → per-cmd counts {produced, loaded, pending}; oldest pending marker age; prints one table. (`--duckdb` snapshot: OMITTED this cycle — YAGNI; the spec marks it optional.)
- [ ] Failing test on a fixture tree (3 produced / 1 loaded / 1 bare dir) → counts; implement; pass.
- [ ] Full gate: `gofmt -l internal cmd` empty; `go vet ./...`; `go test -race ./...`; `go build ./...`; `go mod tidy` no diff.
- [ ] Commit `feat(cc-enrich-worker): status command + range-runner docs`.

---

## Execution notes for the controller

- Tasks 1→2→3 and 4 are independent of 5; 6 needs 1,2,4,5; 7 needs 6; 8 needs 4; 9 last. Run sequentially anyway (shared files in cmd/).
- Task 5 is the risk concentration (large mechanical refactor of main.go with zero behavior change) — use a capable model and re-run the FULL suite, not just cmd tests.
- The `.produced` marker Rows map keys must equal the parquet kind names the loader maps to tables (`domains`, `industries`, `page_signals`, `tech`, `identifiers`, `metadata`, `contacts`, `security`, `page_meta`) — Task 4's test pins one, Task 8's verify consumes them; keep names from `load.FromDir`'s kind mapping.
