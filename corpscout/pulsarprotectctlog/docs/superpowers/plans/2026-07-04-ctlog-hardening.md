# ctlog Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the correctness/robustness issues found in the code review so the service can safely download **N ctlogs** on one host without stalling or corrupting progress, and add a source-level orchestrator.

**Architecture:** Point fixes across the existing layers — SQLite control plane (concurrency), tile parser (resilience), process control-flow (graceful cancel + watch finalize), retention (CN hygiene), control key (collision-safety), plus cleanup and a `process --source` orchestrator that drains a source's reachable frozen ctlogs.

**Tech Stack:** Go 1.26 (pure-Go, `CGO_ENABLED=0`), ClickHouse (`clickhouse-go/v2`), SQLite (`modernc.org/sqlite`), static-ct-api tiled logs + RFC 6962.

## Global Constraints

- Module `github.com/pulsarpoint/pulsarprotectctlog`; Go 1.26; pure-Go deps (`CGO_ENABLED=0 GOOS=linux GOARCH=amd64` for server `ctlogs`).
- Every task ends green on: `gofmt -l .` (empty), `go vet ./...`, `go test ./...`. Prefer `make check`.
- Data plane = ClickHouse `companycollect:9002` db `ctlogs`; control plane = local SQLite at `CTLOG_CONTROL_DB_PATH`.
- Dedup identity = `(issuer_ca_id, serial_number)` (ReplacingMergeTree). ctlogs processed `0…head`.
- Commit messages end with: `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.
- Do not touch the live server or ClickHouse from tasks; verify by build/test only.

---

## File Structure

- `internal/store/control/store.go` — MODIFY: DSN pragmas (WAL + busy_timeout), `MaxOpenConns(1)` on the writer.
- `internal/parse/tile.go` — MODIFY: `TileLeaf` computes byte length before cert parse; skip unparseable-cert leaves (count, no abort); only framing errors are fatal (partial return). `DataTile` counts skips.
- `internal/source/tile.go` — MODIFY: distinguish fetch error (abort) from parse framing error (log + keep partial + continue).
- `internal/ingest/ingest.go` — MODIFY: treat context cancellation as a clean stop (no `markFailed`, no error).
- `cmd/ctlog/main.go` — MODIFY: `--watch` auto-finalize at `End`; `workUnitID(source, ctlogID)` key; parallel `list` Head probes; `process --source` orchestrator (no `--ctlog`).
- `internal/retention/classify.go` — MODIFY: only treat CN as a hostname if it looks like one.
- `internal/config/config.go` — MODIFY: delete legacy single-log fields.
- `internal/ctclient/loglist.go` — DELETE if fully unused (`ResolveLog`, `DefaultHTTPClient`).
- `internal/parse/tile.go` — remove dead `TimestampAt`.
- `internal/store/clickhouse/writer.go` — MODIFY: enable LZ4 compression.

---

## Task 1: Control DB concurrency (WAL + busy_timeout)

**Files:**
- Modify: `internal/store/control/store.go` (`Open`, `OpenReadOnly`)
- Test: `internal/store/control/store_test.go` (create)

**Interfaces:**
- Produces: unchanged public API; `Open`/`OpenReadOnly` now open the DB in WAL mode with a 5s busy timeout so concurrent writers wait instead of erroring.

- [ ] **Step 1: Write the failing test**

Create `internal/store/control/store_test.go`:

```go
package control

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/pulsarpoint/pulsarprotectctlog/internal/model"
)

// TestConcurrentWritersDoNotLock verifies two Stores on the same file can write
// concurrently without SQLITE_BUSY (WAL + busy_timeout).
func TestConcurrentWritersDoNotLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.db")
	ctx := context.Background()

	open := func() *Store {
		s, err := Open(ctx, path)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		t.Cleanup(func() { s.Close() })
		return s
	}
	a, b := open(), open()

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	write := func(s *Store, id string) {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			w := model.WorkUnit{ID: id, LogName: id, StartIndex: 0, EndIndex: 1000}
			if _, _, err := s.EnsureWorkUnit(ctx, w); err != nil {
				errs <- err
				return
			}
			if err := s.SaveProgress(ctx, id, int64(i), 1, 1); err != nil {
				errs <- err
				return
			}
		}
	}
	wg.Add(2)
	go write(a, "log-a")
	go write(b, "log-b")
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent write failed: %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/store/control/ -run TestConcurrentWritersDoNotLock -v`
Expected: FAIL with a `database is locked` (SQLITE_BUSY) error from one goroutine.

- [ ] **Step 3: Implement the DSN pragmas**

In `internal/store/control/store.go`, add a helper and use it in both openers. Replace the `sql.Open("sqlite", path)` calls.

```go
// dsn builds a modernc.org/sqlite DSN with WAL journaling and a 5s busy
// timeout so concurrent writers wait for the lock instead of erroring.
func dsn(path string) string {
	return "file:" + path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
}
```

In `Open`, after `os.MkdirAll(...)`:

```go
	db, err := sql.Open("sqlite", dsn(path))
	if err != nil {
		return nil, fmt.Errorf("open control db: %w", err)
	}
	db.SetMaxOpenConns(1) // serialize this process's writes; WAL handles cross-process
	if _, err := db.ExecContext(ctx, schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("init control schema: %w", err)
	}
	return &Store{db: db}, nil
```

In `OpenReadOnly`, replace `sql.Open("sqlite", path)` with `sql.Open("sqlite", dsn(path))` (keep the rest).

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/store/control/ -run TestConcurrentWritersDoNotLock -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/store/control/store.go internal/store/control/store_test.go
git commit -m "fix: WAL + busy_timeout on control DB for concurrent process runs

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Tile parse resilience

**Files:**
- Modify: `internal/parse/tile.go` (`TileEntry`, `TileLeaf`, `DataTile`)
- Test: `internal/parse/tile_test.go` (add cases)

**Interfaces:**
- Produces:
  - `TileEntry struct { Meta model.CertMeta; Consumed int; HasMeta bool }`
  - `TileLeaf(data []byte, startIndex int64, logName string) (TileEntry, error)` — a non-nil error means **fatal framing** (byte boundary unknown; caller must stop the tile). A leaf whose cert can't be parsed but whose TLS framing is intact returns `(TileEntry{Consumed>0, HasMeta:false}, nil)`.
  - `DataTile(tile []byte, startIndex int64, logName string) (metas []model.CertMeta, parseErrors int, err error)` — `parseErrors` counts skipped (unparseable-cert) leaves; a non-nil `err` means framing broke partway and `metas` holds everything parsed before that point.

- [ ] **Step 1: Write the failing test**

Add to `internal/parse/tile_test.go`:

```go
func TestDataTileCleanFixtureNoParseErrors(t *testing.T) {
	t.Parallel()
	tile, err := os.ReadFile(filepath.Join("testdata", "sycamore_2025h2d_tile0.bin"))
	if err != nil {
		t.Fatal(err)
	}
	metas, parseErrors, err := DataTile(tile, 0, "t")
	if err != nil {
		t.Fatalf("clean tile err: %v", err)
	}
	if parseErrors != 0 {
		t.Errorf("parseErrors = %d, want 0", parseErrors)
	}
	if len(metas) != 256 {
		t.Errorf("metas = %d, want 256", len(metas))
	}
}

func TestDataTileTruncatedReturnsPartial(t *testing.T) {
	t.Parallel()
	tile, err := os.ReadFile(filepath.Join("testdata", "sycamore_2025h2d_tile0.bin"))
	if err != nil {
		t.Fatal(err)
	}
	// Cut mid-tile so the final entry's framing is broken.
	truncated := tile[:len(tile)/2+7]
	metas, _, err := DataTile(truncated, 0, "t")
	if err == nil {
		t.Fatal("expected a framing error on truncated tile")
	}
	if len(metas) == 0 {
		t.Fatal("expected partial metas before the break, got 0")
	}
	if len(metas) >= 256 {
		t.Fatalf("expected < 256 metas, got %d", len(metas))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/parse/ -run 'TestDataTile(Clean|Truncated)' -v`
Expected: FAIL — `TestDataTileTruncatedReturnsPartial` currently gets 0 metas (old `DataTile` returns `metas` accumulated but aborts on first error; verify it fails on the `len(metas)==0` or the `HasMeta` field not existing → compile error first).

- [ ] **Step 3: Rewrite `TileLeaf` and `DataTile`**

Replace the `TileEntry` type and the `TileLeaf`/`DataTile` funcs in `internal/parse/tile.go` with:

```go
// TileEntry is one parsed entry from a static-ct data tile. Consumed is the
// entry's byte length; HasMeta is false when the certificate could not be
// parsed (the leaf is skipped but the byte boundary is still known).
type TileEntry struct {
	Meta     model.CertMeta
	Consumed int
	HasMeta  bool
}

// TileLeaf parses a single static-ct-api TileLeaf from the front of data.
// A non-nil error means the TLS framing is broken and the byte boundary is
// unknown — the caller must stop parsing the tile. If the framing is intact
// but the certificate cannot be parsed, it returns HasMeta=false with a valid
// Consumed and a nil error, so the caller can skip and continue.
func TileLeaf(data []byte, startIndex int64, logName string) (TileEntry, error) {
	var te ct.TimestampedEntry
	rest, err := tls.Unmarshal(data, &te)
	if err != nil {
		return TileEntry{}, fmt.Errorf("unmarshal timestamped entry: %w", err)
	}
	consumed := len(data) - len(rest)
	if te.EntryType == ct.PrecertLogEntryType {
		if consumed, err = skipUint24Prefixed(data, consumed); err != nil {
			return TileEntry{}, fmt.Errorf("skip pre_certificate: %w", err)
		}
	}
	if consumed, err = skipUint16Prefixed(data, consumed); err != nil {
		return TileEntry{}, fmt.Errorf("skip certificate_chain: %w", err)
	}

	cert, entryType, cerr := certFromTimestampedEntry(&te)
	if cert == nil {
		// Framing is intact (we know Consumed) but the cert is unparseable:
		// skip this leaf, do not abort the tile.
		return TileEntry{Consumed: consumed, HasMeta: false}, nil
	}
	_ = cerr // non-fatal x509 issues keep the (partial) cert
	meta := BuildMeta(cert, entryType, te.Timestamp, logName, uint64(startIndex))
	return TileEntry{Meta: meta, Consumed: consumed, HasMeta: true}, nil
}

// DataTile parses all entries in a static-ct data tile. parseErrors counts
// leaves that were skipped because their certificate could not be parsed. A
// non-nil err means the TLS framing broke partway; metas holds everything
// parsed before that point.
func DataTile(tile []byte, startIndex int64, logName string) (metas []model.CertMeta, parseErrors int, err error) {
	pos := 0
	idx := startIndex
	for pos < len(tile) {
		entry, perr := TileLeaf(tile[pos:], idx, logName)
		if perr != nil {
			return metas, parseErrors, fmt.Errorf("framing broken at index %d (offset %d): %w", idx, pos, perr)
		}
		if entry.Consumed <= 0 {
			return metas, parseErrors, fmt.Errorf("zero-length entry at index %d", idx)
		}
		if entry.HasMeta {
			metas = append(metas, entry.Meta)
		} else {
			parseErrors++
		}
		pos += entry.Consumed
		idx++
	}
	return metas, parseErrors, nil
}
```

Also change `certFromTimestampedEntry` to return the cert even when non-fatal, and never a fatal error for a nil cert (let `TileLeaf` decide). Replace its body's error returns so that a parse failure returns `(nil, entryType, err)` (cert nil) rather than a wrapped fatal — the current `if err != nil && cert == nil` checks already yield `cert == nil` on failure, which `TileLeaf` now treats as skip. Leave `certFromTimestampedEntry` returning `(*x509.Certificate, model.EntryType, error)` as-is; `TileLeaf` ignores the error when `cert != nil`.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/parse/ -v`
Expected: PASS (clean fixture → 256 metas, 0 parseErrors; truncated → partial metas + error). The existing `TestDataTileRealFixture` still passes.

- [ ] **Step 5: Commit**

```bash
git add internal/parse/tile.go internal/parse/tile_test.go
git commit -m "fix: tile parser skips unparseable leaves and returns partial on framing break

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Tile source — don't abort on a framing error

**Files:**
- Modify: `internal/source/tile.go` (`tileResult`, `FetchRange`)

**Interfaces:**
- Consumes: `DataTile` (Task 2). Produces: unchanged `FetchRange` signature; a **fetch** error still aborts (transient/retryable), but a **parse framing** error logs, keeps the tile's partial metas, and continues to the next tile so the drain cannot permanently stall.

- [ ] **Step 1: Update `tileResult` and the goroutine**

In `internal/source/tile.go`, add `import "log/slog"`. Replace `tileResult` and the goroutine body + result loop:

```go
type tileResult struct {
	metas       []model.CertMeta
	parseErrors int
	fetchErr    error // tile could not be fetched (transient) -> abort
	parseErr    error // tile framing broke partway -> log + keep partial + continue
}
```

Goroutine body (inside the `go func`):

```go
			tn := firstTile + i
			tile, err := t.client.DataTile(ctx, tn, tileclient.TileWidth(tn, treeSize))
			if err != nil {
				results[i] = tileResult{fetchErr: err}
				return
			}
			metas, perr, derr := parse.DataTile(tile, int64(tn*tileclient.EntriesPerTile), t.client.Name())
			results[i] = tileResult{metas: metas, parseErrors: perr, parseErr: derr}
```

Result-collection loop (replace the `for i` that reads results):

```go
	var all []model.CertMeta
	var parseErrors int
	for i := uint64(0); i < n; i++ {
		if results[i].fetchErr != nil {
			return nil, start, parseErrors, results[i].fetchErr
		}
		all = append(all, results[i].metas...)
		parseErrors += results[i].parseErrors
		if results[i].parseErr != nil {
			slog.Warn("tile framing error; remainder of tile skipped",
				"tile", firstTile+i, "error", results[i].parseErr)
			parseErrors++
		}
	}
```

- [ ] **Step 2: Build + vet + test**

Run: `go build ./... && go vet ./... && go test ./...`
Expected: PASS (no new unit test — behavior is exercised by Task 2's parser tests; this is integration glue).

- [ ] **Step 3: Commit**

```bash
git add internal/source/tile.go
git commit -m "fix: tile source logs+skips a framing-broken tile instead of aborting the drain

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Graceful cancellation on SIGINT

**Files:**
- Modify: `internal/ingest/ingest.go` (`Run`)
- Test: `internal/ingest/ingest_test.go` (create)

**Interfaces:**
- Consumes: `source.Source`. Produces: `Run` returns `(stats, nil)` when the context is cancelled/expired — it does **not** mark the work unit failed, leaving it resumable.

- [ ] **Step 1: Write the failing test**

Create `internal/ingest/ingest_test.go`:

```go
package ingest

import (
	"context"
	"errors"
	"testing"

	"github.com/pulsarpoint/pulsarprotectctlog/internal/model"
)

// cancelSource returns a context error from FetchRange, simulating SIGINT.
type cancelSource struct{}

func (cancelSource) Name() string                      { return "cancel" }
func (cancelSource) TreeSize(context.Context) (uint64, error) { return 100, nil }
func (cancelSource) FetchRange(ctx context.Context, start, end int64) ([]model.CertMeta, int64, int, error) {
	return nil, start, 0, context.Canceled
}

func TestRunReturnsCleanOnCancel(t *testing.T) {
	ing := New(cancelSource{}, nil, nil, 100) // dry mode (ch==nil): no control writes
	_, err := ing.Run(context.Background(), model.WorkUnit{ID: "x", StartIndex: 0, EndIndex: 100})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected error: %v", err)
	}
	if err != nil {
		t.Fatalf("cancel should return nil error, got %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/ingest/ -run TestRunReturnsCleanOnCancel -v`
Expected: FAIL — current `Run` wraps the fetch error and returns it non-nil.

- [ ] **Step 3: Handle cancellation in `Run`**

In `internal/ingest/ingest.go`, add `"errors"` to imports. In the fetch-error branch inside the loop, add a cancellation check before `markFailed`:

```go
		metas, next, parseErrors, err := i.src.FetchRange(ctx, cursor, end)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				slog.Info("ingest cancelled; stopping", "id", w.ID, "cursor", cursor)
				return stats, nil
			}
			markFailed(err.Error())
			return stats, fmt.Errorf("fetch from %d: %w", cursor, err)
		}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/ingest/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/ingest/ingest.go internal/ingest/ingest_test.go
git commit -m "fix: treat context cancellation as a clean, resumable stop

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Retention — validate CN as a hostname

**Files:**
- Modify: `internal/retention/classify.go` (`dedupeHostnames`, add `looksLikeHostname`)
- Test: `internal/retention/classify_test.go` (add a case)

**Interfaces:**
- Produces: `SANRows` no longer emits a row for a common name that isn't a hostname (e.g. `"Some CA Authority"`). DNS SANs are unaffected.

- [ ] **Step 1: Write the failing test**

Add to `internal/retention/classify_test.go`:

```go
func TestSANRowsIgnoresNonHostnameCN(t *testing.T) {
	t.Parallel()
	c := model.CertMeta{
		CommonName: "Some CA Authority X3", // not a hostname
		SANs:       []string{"www.example.com"},
	}
	rows := SANRows(c, true)
	for _, r := range rows {
		if r.FQDN == "some ca authority x3" {
			t.Fatalf("non-hostname CN leaked into SAN rows: %+v", r)
		}
	}
	if len(rows) != 1 || rows[0].FQDN != "www.example.com" {
		t.Fatalf("rows = %+v, want only www.example.com", rows)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/retention/ -run TestSANRowsIgnoresNonHostnameCN -v`
Expected: FAIL — the CN is currently added as a row.

- [ ] **Step 3: Add `looksLikeHostname` and gate the CN**

In `internal/retention/classify.go`, add:

```go
// looksLikeHostname reports whether s is plausibly a DNS name (optionally
// wildcard): it has a dot, no spaces, and only hostname characters. Used to
// keep non-hostname common names out of the subdomain index.
func looksLikeHostname(s string) bool {
	if s == "" || !strings.Contains(s, ".") {
		return false
	}
	host := strings.TrimPrefix(s, "*.")
	for _, r := range host {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
		case r == '.' || r == '-' || r == '_':
		default:
			return false
		}
	}
	return true
}
```

In `dedupeHostnames`, change the `add(commonName)` call at the end so the CN is only added when it looks like a hostname:

```go
	for _, s := range sans {
		add(s)
	}
	if looksLikeHostname(strings.ToLower(strings.TrimSpace(commonName))) {
		add(commonName)
	}
	return out
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/retention/ -v`
Expected: PASS (existing SAN tests still pass; a hostname CN is still included via `add`).

- [ ] **Step 5: Commit**

```bash
git add internal/retention/classify.go internal/retention/classify_test.go
git commit -m "fix: keep non-hostname common names out of the subdomain index

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Source-qualified control key (collision-safety) + watch auto-finalize

**Files:**
- Modify: `cmd/ctlog/main.go` (`cmdProcess`, `cmdList`, add `workUnitID`)

**Interfaces:**
- Produces: `workUnitID(source, ctlogID string) string` returning `source + "/" + ctlogID`. The control DB is keyed by this composite (unique across sources). ClickHouse `source_log` stays the friendly ctlog id (`target.ID`). `--watch` finalizes and exits once the ctlog's expiry window has passed.

- [ ] **Step 1: Add `workUnitID` and use it in `cmdProcess`**

In `cmd/ctlog/main.go` add:

```go
// workUnitID is the control-DB key for a ctlog, qualified by source so ids
// derived from different sources can never collide.
func workUnitID(source, ctlogID string) string { return source + "/" + ctlogID }
```

In `cmdProcess`, change the `drain` closure's unit id and end-tracking to the composite, and make `--watch` finalize when frozen. Replace the `drain` closure and the tail of `cmdProcess`:

```go
	wuID := workUnitID(*srcName, target.ID)
	drain := func(finalize bool) error {
		head, err := src.TreeSize(ctx)
		if err != nil {
			return err
		}
		if ctrl != nil {
			_ = ctrl.SetEnd(ctx, wuID, int64(head))
		}
		unit := model.WorkUnit{ID: wuID, LogName: target.ID, StartIndex: 0, EndIndex: head, WindowFrom: target.Start, WindowTo: target.End}
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
		frozen := !control.Now().Before(target.End)
		if err := drain(frozen); err != nil {
			return err
		}
		if frozen {
			slog.Info("ctlog window closed; finalized", "ctlog", target.ID)
			return nil
		}
		select {
		case <-time.After(*interval):
		case <-ctx.Done():
			return nil
		}
	}
```

- [ ] **Step 2: Use `workUnitID` in `cmdList`'s join**

In `cmdList`, change the processed-state lookup so the key matches. Replace the inner `for _, c := range ctlogs` body's lookup line:

```go
			head, ok := loglist.Head(ctx, hc, c, cfg.MaxRetries)
			wu, tracked := processed[workUnitID(s.Name, c.ID)]
```

- [ ] **Step 3: Build + vet + test**

Run: `go build ./... && go vet ./... && go test ./...`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add cmd/ctlog/main.go
git commit -m "fix: source-qualified control key; --watch auto-finalizes at window end

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: Parallelize `list` Head probes + dead-code cleanup + ClickHouse compression

**Files:**
- Modify: `cmd/ctlog/main.go` (`cmdList` — concurrent Head probes)
- Modify: `internal/store/clickhouse/writer.go` (`Open` — LZ4 compression)
- Delete: `internal/ctclient/loglist.go` if unused
- Modify: `internal/parse/tile.go` (remove `TimestampAt`)
- Modify: `internal/config/config.go` (remove legacy fields)

**Interfaces:**
- Produces: `list` probes all ctlogs' heads concurrently (bounded by `cfg.FetchParallel`). No public signature changes elsewhere.

- [ ] **Step 1: Confirm what is unused, then delete it**

Run: `grep -rn "ResolveLog\|DefaultHTTPClient\|TimestampAt\|cfg.Source\b\|\.LogName\b\|\.LogURL\b\|\.LogListURL\b\|CTLOG_SOURCE\b\|CTLOG_LOG_NAME\|CTLOG_LOG_URL\|CTLOG_LOG_LIST_URL" --include=*.go .`
Expected: only definitions, no callers. If `ctclient.New`/`ctclient.Client` are still used (they are, by rfc6962 source) keep `internal/ctclient/client.go`; delete only `internal/ctclient/loglist.go` (`ResolveLog`, `LogInfo`, `DefaultHTTPClient`) if it has no callers. Remove `TimestampAt` from `internal/parse/tile.go`. Remove the `Source`, `LogName`, `LogURL`, `LogListURL` fields from `internal/config/config.go`'s `Config` struct.

```bash
git rm internal/ctclient/loglist.go   # only if the grep shows no callers
```

- [ ] **Step 2: Enable ClickHouse LZ4 compression**

In `internal/store/clickhouse/writer.go` `Open`, add compression to the options:

```go
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: user,
			Password: password,
		},
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
	})
```

- [ ] **Step 3: Parallelize `cmdList` Head probes**

In `cmd/ctlog/main.go`, add `"sync"` to imports. Replace the `for _, s := range sources { ... for _, c := range ctlogs { head, ok := loglist.Head(...) ... } }` block with a two-phase build: gather all ctlogs, probe heads concurrently, then assemble rows in order.

```go
	type item struct {
		source string
		ctlog  loglist.CTLog
	}
	var items []item
	for _, s := range sources {
		ctlogs, err := loglist.CTLogs(ctx, hc, cfg.ShardListURL, s, deriveCTLogIDFromLog)
		if err != nil {
			return err
		}
		for _, c := range ctlogs {
			items = append(items, item{source: s.Name, ctlog: c})
		}
	}

	heads := make([]uint64, len(items))
	reach := make([]bool, len(items))
	sem := make(chan struct{}, max(cfg.FetchParallel, 1))
	var wg sync.WaitGroup
	for i := range items {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			heads[i], reach[i] = loglist.Head(ctx, hc, items[i].ctlog, cfg.MaxRetries)
		}(i)
	}
	wg.Wait()

	now := control.Now()
	var out []loglist.CTLogStatus
	for i, it := range items {
		c := it.ctlog
		wu, tracked := processed[workUnitID(it.source, c.ID)]
		out = append(out, loglist.CTLogStatus{
			CTLog: c, Phase: string(c.Phase(now)), Reachable: reach[i], Head: int64(heads[i]),
			Tracked: tracked, Status: statusOr(wu, tracked), Cursor: wu.NextIndex,
			CertsWritten: wu.CertsWritten, SANsWritten: wu.SANsWritten,
			PercentDone: loglist.PercentDone(wu.NextIndex, int64(heads[i])),
		})
	}
```

(Remove the old sequential `now := control.Now()` / `out` block this replaces.)

- [ ] **Step 4: Build + vet + test**

Run: `go build ./... && go vet ./... && go test ./...`
Expected: PASS. `gofmt -l .` empty.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "chore: parallelize list Head probes; LZ4 compression; remove dead code

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 8: `process --source` orchestrator (drain a source's reachable frozen ctlogs)

**Files:**
- Modify: `cmd/ctlog/main.go` (`cmdProcess` — allow no `--ctlog`; refactor drain into a reusable helper)

**Interfaces:**
- Produces: `ctlog process --source NAME` (no `--ctlog`) iterates the source's ctlogs: **reachable + frozen** → drain once (skips already-done via the control DB); **reachable + active** → drain to head once (tail requires an explicit single-ctlog `--watch`); **unreachable** → log a skip. `--ctlog ID` behaves as before (single ctlog, honoring `--watch`).

- [ ] **Step 1: Refactor the source/drain into a helper**

In `cmd/ctlog/main.go`, extract source construction + a single drain into a function used by both the single-ctlog and orchestrator paths:

```go
// buildSource constructs a Source for a ctlog.
func buildSource(target loglist.CTLog, hc *http.Client, cfg *config.Config) (source.Source, error) {
	if target.Type == "rfc6962" {
		cl, err := ctclient.New(target.MonitoringURL, target.ID, hc, cfg.MaxRetries)
		if err != nil {
			return nil, err
		}
		return source.NewRFC6962(cl, cfg.BatchSize), nil
	}
	return source.NewTile(tileclient.New(target.MonitoringURL, target.ID, hc, cfg.MaxRetries), cfg.FetchParallel), nil
}

// drainCTLog drains one ctlog to head. finalize marks it done when complete.
func drainCTLog(ctx context.Context, cfg *config.Config, hc *http.Client, ch *clickhouse.Store, ctrl *control.Store, srcName string, target loglist.CTLog, finalize bool, limit int) error {
	src, err := buildSource(target, hc, cfg)
	if err != nil {
		return err
	}
	head, err := src.TreeSize(ctx)
	if err != nil {
		return err
	}
	wuID := workUnitID(srcName, target.ID)
	if ctrl != nil {
		_ = ctrl.SetEnd(ctx, wuID, int64(head))
	}
	unit := model.WorkUnit{ID: wuID, LogName: target.ID, StartIndex: 0, EndIndex: head, WindowFrom: target.Start, WindowTo: target.End}
	began := time.Now()
	stats, err := ingest.New(src, ch, ctrl, cfg.WriteBatchSize).WithFinalize(finalize).WithLimit(limit).Run(ctx, unit)
	if err != nil {
		return err
	}
	slog.Info("drain cycle", "ctlog", target.ID, "head", head, "entries", stats.EntriesProcessed,
		"certs", stats.CertsWritten, "sans", stats.SANsWritten, "parse_errors", stats.ParseErrors, "elapsed", time.Since(began).Round(time.Second))
	return nil
}
```

- [ ] **Step 2: Rewrite `cmdProcess` control flow**

Replace `cmdProcess`'s body after config/hc/source-resolution with: open stores once, then branch on whether `--ctlog` was given.

```go
	if *ctlogID == "" {
		// Orchestrate the whole source.
		now := control.Now()
		for _, c := range ctlogs {
			head, ok := loglist.Head(ctx, hc, c, cfg.MaxRetries)
			if !ok {
				slog.Warn("ctlog unreachable; skipping", "source", *srcName, "ctlog", c.ID)
				continue
			}
			frozen := !now.Before(c.End)
			slog.Info("processing ctlog", "source", *srcName, "ctlog", c.ID, "phase", phaseName(frozen), "head", head)
			if err := drainCTLog(ctx, cfg, hc, chStore, ctrl, *srcName, c, frozen, *limit); err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return err
			}
		}
		return nil
	}

	// Single ctlog (target already resolved above).
	if !*watch {
		return drainCTLog(ctx, cfg, hc, chStore, ctrl, *srcName, *target, true, *limit)
	}
	slog.Info("watch: tailing ctlog delta", "ctlog", target.ID, "interval", *interval)
	for {
		frozen := !control.Now().Before(target.End)
		if err := drainCTLog(ctx, cfg, hc, chStore, ctrl, *srcName, *target, frozen, *limit); err != nil {
			return err
		}
		if frozen {
			slog.Info("ctlog window closed; finalized", "ctlog", target.ID)
			return nil
		}
		select {
		case <-time.After(*interval):
		case <-ctx.Done():
			return nil
		}
	}
```

Add the small helper `func phaseName(frozen bool) string { if frozen { return "frozen" }; return "active" }`. Adjust the required-flags check at the top of `cmdProcess` from `if *srcName == "" || *ctlogID == ""` to `if *srcName == ""` (only `--source` is required now). Move the single-ctlog `target` resolution so it only runs when `*ctlogID != ""` (keep the "not found" error there), and `import "errors"`.

- [ ] **Step 3: Build + vet + test + dry-run smoke**

Run: `go build ./... && go vet ./... && go test ./...` → PASS.
Manual (safe, read/skip only): `./bin/ctlog process --source le-sycamore --dry-run --limit 500` → iterates Sycamore ctlogs, drains a slice of each reachable one, logs a skip for retired ones.

- [ ] **Step 4: Commit**

```bash
git add cmd/ctlog/main.go
git commit -m "feat: process --source orchestrator (drain a source's reachable frozen ctlogs)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 9: Docs

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update README**

Update the **Usage** section to add `ctlog process --source NAME` (no `--ctlog`) as the "drain all reachable frozen ctlogs in a source" mode; note `--watch` auto-finalizes at the window end; note the control DB now runs WAL + busy_timeout so multiple `process` runs can share it on one host. Remove any lingering mention of the deleted legacy env vars from Task 7.

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: document process --source orchestrator and concurrency fixes

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Verification

- `make check` green (all unit tests: control concurrency, tile parser resilience, ingest cancel, retention CN).
- `./bin/ctlog list --source le-sycamore` still renders; `--json` unchanged.
- `./bin/ctlog process --source le-sycamore --dry-run --limit 500` iterates ctlogs, drains reachable ones, skips retired.
- Two concurrent `./bin/ctlog process` runs against different ctlogs sharing one control DB do not error with `database is locked`.
- Ctrl-C during a drain exits 0 and leaves the work unit resumable (status not `failed`).

## Out of scope (future plans)

- Postgres control plane with `FOR UPDATE SKIP LOCKED` for **multi-host** workers (Option B).
- `process --source --watch` that concurrently tails all active ctlogs and periodically re-enumerates the log list for new sub-shards.
- Per-drain incremental progress logging.
- "Be sure we see every cert" multi-operator completeness mode.
