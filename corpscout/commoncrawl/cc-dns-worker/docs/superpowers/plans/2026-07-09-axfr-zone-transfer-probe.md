# AXFR Zone Transfer Probe Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an opportunistic, default-off AXFR (DNS zone transfer) probe to `cc-dns-worker` that records the open-zone-transfer misconfiguration flag and folds any leaked records into the existing ClickHouse record table, tagged with a `source` provenance column.

**Architecture:** The probe runs TCP-only via `miekg/dns`'s streaming `dns.Transfer`, on its own scheduler lane (never sharing the UDP resolution budget). It reuses the `Delegation.NSIPs` that discovery already produced. Leaked records become ordinary `model.DNSRecord`s tagged `source="axfr"` and flow through the unchanged SQLite→ClickHouse load path; a per-domain `axfr_open` flag lands on the scan summary. All interpretation (technology inference, corroboration, GDPR gating) is out of scope — see the spec's "Out of scope" section.

**Tech Stack:** Go 1.25, `github.com/miekg/dns` (already a dep), `modernc.org/sqlite`, ClickHouse native protocol via `clickhouse-go/v2`. Tests use the standard `testing` package (this repo does not use testify).

**Spec:** `corpscout/commoncrawl/docs/axfr-zone-transfer-spec.md`

## Global Constraints

- Go 1.25 (`go.mod`: `go 1.25.0`). Run `go fmt ./...` and `go vet ./...` before every commit.
- Tests use the standard library `testing` package only — no testify (match existing tests).
- Conventional Commits format for every commit message.
- The feature ships dark: every new flag defaults off; `--axfr=false` is the master switch and when off, no AXFR code path runs.
- Caps (`--axfr-max-records`, `--axfr-max-bytes`, `--axfr-timeout`) are mandatory and enforced per-RR — they bound memory, not just storage.
- All work happens under `corpscout/commoncrawl/cc-dns-worker/`. The git root is `/Users/graovic/pulsarpoint/ppoint/companycollect`; commit from there.
- New struct fields with `ch:"..."` tags are appended at the END of `RecordRow`/`ScanRow` (declaration order = INSERT column order in `load.go`).

---

## File Structure

- `internal/scheduler/providers.go` — export `IsHyperscaler` (was `isHyperscaler`).
- `internal/model/model.go` — add `Source` to `DNSRecord`/`RecordRow`; add AXFR fields to `DomainResult`/`ScanRow`.
- `internal/store/store.go` — new SQLite columns + idempotent migration + read/write the new columns.
- `internal/resolve/axfr.go` — NEW: `AXFRCaps`, `AXFRResult`, `AXFRProber`, low-level TCP transfer + skip/dedup/semaphore policy.
- `internal/resolve/axfr_test.go` — NEW: probe tests (refused, open+capped, hyperscaler-skip, dedup).
- `internal/resolve/testserver_test.go` — extend with a TCP AXFR test server helper.
- `cmd/cc-dns-worker/scan.go` — AXFR flags, third scheduler lane, prober construction, wire into `resolveDomain`.
- `docs/` + ClickHouse — DDL to add the new columns to the live CH tables (external to this repo).

---

## Task 1: Export `IsHyperscaler`

**Files:**
- Modify: `internal/scheduler/providers.go:36-49`
- Modify: `internal/scheduler/scheduler.go:81`
- Modify: `internal/scheduler/providers_test.go:19-28`

**Interfaces:**
- Produces: `func scheduler.IsHyperscaler(ip string) bool` — used by the AXFR prober (Task 5) to skip hyperscaler-only NS sets.

- [ ] **Step 1: Rename the function and update in-package callers**

In `internal/scheduler/providers.go`, rename `isHyperscaler` → `IsHyperscaler` and update its doc comment first line:

```go
// IsHyperscaler reports whether ip (a bare address, no port) falls in a known large anycast DNS
// provider range. A non-IP string returns false.
func IsHyperscaler(ip string) bool {
```

In `internal/scheduler/scheduler.go:81`, update the caller:

```go
	if s.cfg.HyperscalerQPS > 0 && IsHyperscaler(ip) {
```

In `internal/scheduler/providers_test.go`, update all three references (`isHyperscaler` → `IsHyperscaler`).

- [ ] **Step 2: Run tests to verify they pass**

Run: `go test ./internal/scheduler/...`
Expected: PASS (rename is behavior-preserving).

- [ ] **Step 3: Vet, fmt, commit**

```bash
go fmt ./internal/scheduler/... && go vet ./internal/scheduler/...
git add internal/scheduler/providers.go internal/scheduler/scheduler.go internal/scheduler/providers_test.go
git commit -m "refactor(dns): export IsHyperscaler for cross-package use"
```

---

## Task 2: Add `source` provenance to the record model + SQLite plumbing

**Files:**
- Modify: `internal/model/model.go:8-16` (`DNSRecord`), `:39-52` (`RecordRow`)
- Modify: `internal/store/store.go` (schema, migration, insert, two selects)
- Modify: `internal/resolve/query.go:96-139` (`collect`) and `:31-33` (DS append) to stamp `source="query"`
- Test: `internal/store/store_test.go`

**Interfaces:**
- Produces: `model.DNSRecord.Source string` (`"query"` | `"axfr"`); `model.RecordRow.Source string` with `ch:"source"`. AXFR records (Task 4) set `Source="axfr"`; all query records set `Source="query"`.

- [ ] **Step 1: Write the failing test**

Add to `internal/store/store_test.go` (a fresh DB round-trips a record's source through StagedRecords):

```go
func TestCommitBatchPersistsSource(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	if _, err := st.Seed(ctx, "s1", []string{"example.com"}); err != nil {
		t.Fatal(err)
	}
	res := model.DomainResult{
		ScanID: "s1", RootDomain: "example.com", Status: "done", ResolvedAt: time.Now().UTC(),
		Records: []model.DNSRecord{
			{Name: "example.com", RecordType: "A", Value: "1.2.3.4", Rcode: "NOERROR", Source: "query"},
			{Name: "vpn.example.com", RecordType: "A", Value: "5.6.7.8", Rcode: "NOERROR", Source: "axfr"},
		},
	}
	if err := st.CommitBatch(ctx, []model.DomainResult{res}); err != nil {
		t.Fatal(err)
	}
	rows, err := st.StagedRecords(ctx, "s1")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, r := range rows {
		got[r.Name] = r.Source
	}
	if got["example.com"] != "query" || got["vpn.example.com"] != "axfr" {
		t.Fatalf("source not round-tripped: %+v", got)
	}
}
```

Ensure the test file imports `context`, `path/filepath`, `time`, and `cc-dns-worker/internal/model` (add any missing).

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/store/ -run TestCommitBatchPersistsSource`
Expected: FAIL — `RecordRow`/`DNSRecord` has no `Source` field (compile error).

- [ ] **Step 3: Add the model fields**

In `internal/model/model.go`, add to `DNSRecord` (after `Priority`):

```go
	Priority   uint16 // MX preference; 0 otherwise
	Source     string // "query" (actively queried) | "axfr" (from a zone transfer)
```

Add to `RecordRow` (after the `Scans` field, at the end):

```go
	Scans      uint64    `ch:"scans"`
	Source     string    `ch:"source"`
```

- [ ] **Step 4: Add the SQLite column, migration, and wire read/write**

In `internal/store/store.go`:

Add the column to the `scan_records` table in the `schema` const (after `priority`):

```go
  priority     INTEGER DEFAULT 0,
  source       TEXT DEFAULT 'query',
```

Add an idempotent migration for already-existing stage DBs. In `Open`, after the `schema` exec succeeds, call `migrate(db)`; add the helper:

```go
// migrate applies additive column changes to stage DBs created before those columns existed.
// SQLite has no ADD COLUMN IF NOT EXISTS, so a duplicate-column error is expected and ignored.
func migrate(db *sql.DB) {
	for _, stmt := range []string{
		`ALTER TABLE scan_records ADD COLUMN source TEXT DEFAULT 'query'`,
		`ALTER TABLE scan_domains ADD COLUMN axfr_open INTEGER DEFAULT 0`,
		`ALTER TABLE scan_domains ADD COLUMN axfr_records INTEGER DEFAULT 0`,
		`ALTER TABLE scan_domains ADD COLUMN axfr_truncated INTEGER DEFAULT 0`,
	} {
		_, _ = db.ExecContext(context.Background(), stmt)
	}
}
```

(The `scan_domains` ALTERs are for Task 3; adding them now is harmless and keeps one migration list.)

In `Open`, wire it:

```go
	if _, err := db.ExecContext(context.Background(), schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("schema: %w", err)
	}
	migrate(db)
	return &Store{db: db}, nil
```

Update the `insR` prepared insert in `CommitBatch` to include `source`:

```go
	insR, err := tx.PrepareContext(ctx, `INSERT INTO scan_records
		(scan_id, root_domain, name, record_type, slot, value, ttl, priority, rcode, source, source_run_id, resolved_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
```

Update the `insR.ExecContext` call (add `rec.Source` before `res.SourceRunID`):

```go
			if _, err := insR.ExecContext(ctx, res.ScanID, res.RootDomain, rec.Name, rec.RecordType,
				rec.Slot, rec.Value, rec.TTL, rec.Priority, rec.Rcode, rec.Source, res.SourceRunID, ts); err != nil {
```

Update `StagedRecords`' SELECT and Scan to read `source`:

```go
	rows, err := s.db.QueryContext(ctx, `SELECT root_domain, name, record_type, slot, value,
		ttl, priority, rcode, source, source_run_id, resolved_at FROM scan_records WHERE scan_id = ?`, scanID)
	...
		if err := rows.Scan(&r.RootDomain, &r.Name, &r.RecordType, &r.Slot, &r.Value,
			&r.TTL, &r.Priority, &r.Rcode, &r.Source, &r.LastRunID, &ts); err != nil {
```

Update `recordsAfterQuery` and the `RecordsAfter` Scan the same way (add `source` after `rcode`):

```go
const recordsAfterQuery = `SELECT rowid, root_domain, name, record_type, slot, value,
	ttl, priority, rcode, source, source_run_id, resolved_at FROM scan_records
	WHERE +scan_id = ? AND rowid > ? ORDER BY rowid LIMIT ?`
	...
		if err := rows.Scan(&rid, &r.RootDomain, &r.Name, &r.RecordType, &r.Slot, &r.Value,
			&r.TTL, &r.Priority, &r.Rcode, &r.Source, &r.LastRunID, &ts); err != nil {
```

- [ ] **Step 5: Stamp `source="query"` on actively-queried records**

In `internal/resolve/query.go`, in `collect`, set the source when building each record:

```go
		rec := model.DNSRecord{Name: name, Slot: q.Slot, Rcode: rcode, TTL: rr.Header().Ttl, Source: "query"}
```

And the DS record appended in `Resolve` (around line 32):

```go
		res.Records = append(res.Records, model.DNSRecord{Name: domain, RecordType: "DS", Slot: "", Value: ds, Rcode: "NOERROR", Source: "query"})
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test ./internal/store/ ./internal/resolve/`
Expected: PASS (new test passes; existing resolve tests still pass).

- [ ] **Step 7: Vet, fmt, commit**

```bash
go fmt ./... && go vet ./internal/store/ ./internal/resolve/ ./internal/model/
git add internal/model/model.go internal/store/store.go internal/resolve/query.go internal/store/store_test.go
git commit -m "feat(dns): add source provenance column to DNS records (query|axfr)"
```

---

## Task 3: Add AXFR summary flags to `DomainResult`/`ScanRow` + SQLite summary plumbing

**Files:**
- Modify: `internal/model/model.go` (`DomainResult`, `ScanRow`)
- Modify: `internal/store/store.go` (schema, CommitBatch upD, StagedDomains, SummariesFor)
- Test: `internal/store/store_test.go`

**Interfaces:**
- Produces: `model.DomainResult.AXFROpen bool`, `.AXFRRecords int`, `.AXFRTruncated bool`; `model.ScanRow.AXFROpen uint8` (`ch:"axfr_open"`), `.AXFRRecords uint32` (`ch:"axfr_records"`), `.AXFRTruncated uint8` (`ch:"axfr_truncated"`). Task 6 sets the `DomainResult` fields from an `AXFRResult`.

- [ ] **Step 1: Write the failing test**

Add to `internal/store/store_test.go`:

```go
func TestSummaryPersistsAXFRFlags(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	if _, err := st.Seed(ctx, "s1", []string{"example.com"}); err != nil {
		t.Fatal(err)
	}
	res := model.DomainResult{
		ScanID: "s1", RootDomain: "example.com", Status: "done", ResolvedAt: time.Now().UTC(),
		AXFROpen: true, AXFRRecords: 42, AXFRTruncated: true,
	}
	if err := st.CommitBatch(ctx, []model.DomainResult{res}); err != nil {
		t.Fatal(err)
	}
	rows, err := st.StagedDomains(ctx, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 summary, got %d", len(rows))
	}
	if rows[0].AXFROpen != 1 || rows[0].AXFRRecords != 42 || rows[0].AXFRTruncated != 1 {
		t.Fatalf("axfr flags not round-tripped: %+v", rows[0])
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/store/ -run TestSummaryPersistsAXFRFlags`
Expected: FAIL — `DomainResult`/`ScanRow` have no AXFR fields (compile error).

- [ ] **Step 3: Add the model fields**

In `internal/model/model.go`, add to `DomainResult` (after `Records`):

```go
	Records      []DNSRecord
	AXFROpen     bool
	AXFRRecords  int
	AXFRTruncated bool
```

Add to `ScanRow` (at the end, after `ResolvedAt`):

```go
	ResolvedAt   time.Time `ch:"resolved_at"`
	AXFROpen     uint8     `ch:"axfr_open"`
	AXFRRecords  uint32    `ch:"axfr_records"`
	AXFRTruncated uint8    `ch:"axfr_truncated"`
```

- [ ] **Step 4: Add the SQLite columns and wire read/write**

In `internal/store/store.go`, add to the `scan_domains` table in the `schema` const (after `queries_ok`):

```go
  queries_ok    INTEGER DEFAULT 0,
  axfr_open     INTEGER DEFAULT 0,
  axfr_records  INTEGER DEFAULT 0,
  axfr_truncated INTEGER DEFAULT 0,
```

(The `migrate()` ALTERs for these were already added in Task 2.)

Update the `upD` prepared UPDATE in `CommitBatch` to set the three columns:

```go
	upD, err := tx.PrepareContext(ctx, `UPDATE scan_domains SET
		status=?, etld=?, nameservers=?, ns_ips=?, dnssec_signed=?, ds_present=?,
		queries_total=?, queries_ok=?, axfr_open=?, axfr_records=?, axfr_truncated=?, error=?, source_run_id=?, resolved_at=?
		WHERE scan_id=? AND root_domain=?`)
```

Update the `upD.ExecContext` call (insert the three values after `res.QueriesOK`):

```go
		res2, err := upD.ExecContext(ctx, res.Status, res.ETLD, string(ns), string(nsips),
			b2i(res.DNSSECSigned), b2i(res.DSPresent), res.QueriesTotal, res.QueriesOK,
			b2i(res.AXFROpen), res.AXFRRecords, b2i(res.AXFRTruncated),
			res.Error, res.SourceRunID, ts, res.ScanID, res.RootDomain)
```

Update `StagedDomains`' SELECT + Scan to read the three columns:

```go
	rows, err := s.db.QueryContext(ctx, `SELECT root_domain, etld, nameservers, ns_ips,
		dnssec_signed, ds_present, status, queries_total, queries_ok, axfr_open, axfr_records, axfr_truncated,
		source_run_id, resolved_at
		FROM scan_domains WHERE scan_id = ? AND status = 'done'`, scanID)
	...
		var dnssec, ds, axfrOpen, axfrTrunc int
		var axfrRecs uint32
		if err := rows.Scan(&r.RootDomain, &r.ETLD, &ns, &nsips, &dnssec, &ds,
			&r.Status, &r.QueriesTotal, &r.QueriesOK, &axfrOpen, &axfrRecs, &axfrTrunc, &r.LastRunID, &ts); err != nil {
			return nil, err
		}
		...
		r.AXFROpen = uint8(axfrOpen)
		r.AXFRRecords = axfrRecs
		r.AXFRTruncated = uint8(axfrTrunc)
```

Apply the **identical** SELECT + Scan changes to `SummariesFor` (same column list, same scan targets, before the `IN (`+ph+`)` clause). The `WHERE scan_id = ? AND status = 'done' AND root_domain IN (...)` tail is unchanged.

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/store/`
Expected: PASS.

- [ ] **Step 6: Vet, fmt, commit**

```bash
go fmt ./... && go vet ./internal/store/ ./internal/model/
git add internal/model/model.go internal/store/store.go internal/store/store_test.go
git commit -m "feat(dns): persist axfr_open/records/truncated on the scan summary"
```

---

## Task 4: AXFR test server harness + low-level transfer

**Files:**
- Modify: `internal/resolve/testserver_test.go` (add a TCP AXFR server helper)
- Create: `internal/resolve/axfr.go`
- Create: `internal/resolve/axfr_test.go`

**Interfaces:**
- Produces:
  - `type resolve.AXFRCaps struct { MaxRecords int; MaxBytes int; Deadline time.Duration }`
  - `type resolve.AXFRResult struct { Open bool; Server string; Records int; Truncated bool; Zone []model.DNSRecord }`
  - `func transferAXFR(ctx context.Context, zone, nsIP string, caps AXFRCaps) (AXFRResult, error)` — low-level single-server transfer (unexported; called by the prober in Task 5).
- Test helper: `func startAXFRServer(t *testing.T, rrs []dns.RR, refuse bool) (addr string, closeFn func())`.

- [ ] **Step 1: Add the TCP AXFR test server helper**

Append to `internal/resolve/testserver_test.go`:

```go
// startAXFRServer starts a TCP DNS server that answers an AXFR query for any zone. If refuse is true
// it replies REFUSED; otherwise it streams rrs (which MUST start and end with the zone SOA for a
// well-formed transfer). Non-AXFR queries get an empty NOERROR reply.
func startAXFRServer(t *testing.T, rrs []dns.RR, refuse bool) (string, func()) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tcp: %v", err)
	}
	mux := dns.NewServeMux()
	mux.HandleFunc(".", func(w dns.ResponseWriter, r *dns.Msg) {
		if r.Question[0].Qtype == dns.TypeAXFR {
			if refuse {
				m := new(dns.Msg)
				m.SetReply(r)
				m.Rcode = dns.RcodeRefused
				_ = w.WriteMsg(m)
				return
			}
			ch := make(chan *dns.Envelope)
			tr := new(dns.Transfer)
			go func() {
				ch <- &dns.Envelope{RR: rrs}
				close(ch)
			}()
			_ = tr.Out(w, r, ch)
			return
		}
		m := new(dns.Msg)
		m.SetReply(r)
		_ = w.WriteMsg(m)
	})
	srv := &dns.Server{Listener: l, Handler: mux}
	go func() { _ = srv.ActivateAndServe() }()
	return l.Addr().String(), func() { _ = srv.Shutdown() }
}
```

- [ ] **Step 2: Write the failing test**

Create `internal/resolve/axfr_test.go`:

```go
package resolve

import (
	"context"
	"testing"
	"time"

	"github.com/miekg/dns"
)

func axfrZone(t *testing.T) []dns.RR {
	return []dns.RR{
		mustRR(t, "example.com. 3600 IN SOA ns1.example.com. hostmaster.example.com. 1 7200 3600 1209600 3600"),
		mustRR(t, "www.example.com. 3600 IN A 1.2.3.4"),
		mustRR(t, "cpanel.example.com. 3600 IN A 5.6.7.8"),
		mustRR(t, "asa-fw.example.com. 3600 IN A 9.10.11.12"),
		mustRR(t, "example.com. 3600 IN SOA ns1.example.com. hostmaster.example.com. 1 7200 3600 1209600 3600"),
	}
}

func TestTransferAXFROpen(t *testing.T) {
	addr, stop := startAXFRServer(t, axfrZone(t), false)
	defer stop()
	caps := AXFRCaps{MaxRecords: 50000, MaxBytes: 64 << 20, Deadline: 5 * time.Second}
	res, err := transferAXFR(context.Background(), "example.com", addr, caps)
	if err != nil {
		t.Fatalf("transfer: %v", err)
	}
	if !res.Open {
		t.Fatal("want Open=true")
	}
	if res.Truncated {
		t.Fatal("want Truncated=false")
	}
	if res.Records != 5 || len(res.Zone) != 5 {
		t.Fatalf("want 5 records, got Records=%d len(Zone)=%d", res.Records, len(res.Zone))
	}
	for _, rec := range res.Zone {
		if rec.Source != "axfr" {
			t.Fatalf("want Source=axfr, got %q", rec.Source)
		}
	}
}

func TestTransferAXFRRefused(t *testing.T) {
	addr, stop := startAXFRServer(t, nil, true)
	defer stop()
	caps := AXFRCaps{MaxRecords: 50000, MaxBytes: 64 << 20, Deadline: 5 * time.Second}
	res, _ := transferAXFR(context.Background(), "example.com", addr, caps)
	if res.Open {
		t.Fatal("want Open=false on REFUSED")
	}
	if len(res.Zone) != 0 {
		t.Fatalf("want empty zone, got %d", len(res.Zone))
	}
}

func TestTransferAXFRTruncatedByRecordCap(t *testing.T) {
	addr, stop := startAXFRServer(t, axfrZone(t), false)
	defer stop()
	caps := AXFRCaps{MaxRecords: 3, MaxBytes: 64 << 20, Deadline: 5 * time.Second}
	res, err := transferAXFR(context.Background(), "example.com", addr, caps)
	if err != nil {
		t.Fatalf("transfer: %v", err)
	}
	if !res.Open || !res.Truncated {
		t.Fatalf("want Open=true Truncated=true, got Open=%v Truncated=%v", res.Open, res.Truncated)
	}
	if res.Records != 3 {
		t.Fatalf("want capped at 3 records, got %d", res.Records)
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/resolve/ -run TestTransferAXFR`
Expected: FAIL — `transferAXFR`, `AXFRCaps`, `AXFRResult` undefined (compile error).

- [ ] **Step 4: Implement the low-level transfer**

Create `internal/resolve/axfr.go`:

```go
package resolve

import (
	"context"
	"strconv"
	"strings"
	"time"

	"cc-dns-worker/internal/model"

	"github.com/miekg/dns"
)

// AXFRCaps bounds a single transfer so a hostile or huge zone cannot stall a worker or exhaust
// memory while it is drained. Every cap is enforced per-RR, not per-envelope.
type AXFRCaps struct {
	MaxRecords int           // stop appending past this many RRs
	MaxBytes   int           // stop once the running sum of RR sizes reaches this
	Deadline   time.Duration // whole-transfer timeout
}

// AXFRResult is the outcome of probing one zone. Zone holds the transferred records (tagged
// Source="axfr"); it is retained and folded into the domain's record set by the caller.
type AXFRResult struct {
	Open      bool
	Server    string
	Records   int
	Truncated bool
	Zone      []model.DNSRecord
}

// transferAXFR runs one TCP AXFR against nsIP for zone, draining up to the caps. A REFUSED/NOTAUTH
// response or any transport error yields Open=false with an empty Zone. err is non-nil only on a
// transport/setup failure (so the caller can rotate servers); a clean REFUSED returns (Open:false, nil).
func transferAXFR(ctx context.Context, zone, nsIP string, caps AXFRCaps) (AXFRResult, error) {
	res := AXFRResult{Server: nsIP}
	deadline := caps.Deadline
	if deadline <= 0 {
		deadline = 20 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()

	m := new(dns.Msg)
	m.SetAxfr(dns.Fqdn(zone))
	tr := &dns.Transfer{DialTimeout: deadline, ReadTimeout: deadline}
	ch, err := tr.In(m, withPort(nsIP))
	if err != nil {
		return res, err // transport/dial failure — caller rotates
	}

	bytes := 0
	for env := range ch {
		if env.Error != nil {
			// REFUSED / NOTAUTH / malformed: a refusal is not a transport error to propagate.
			return res, nil
		}
		for _, rr := range env.RR {
			if caps.MaxRecords > 0 && res.Records >= caps.MaxRecords {
				res.Truncated = true
				return finalize(ctx, res), nil
			}
			if caps.MaxBytes > 0 && bytes >= caps.MaxBytes {
				res.Truncated = true
				return finalize(ctx, res), nil
			}
			bytes += dns.Len(rr)
			if rec, ok := axfrRecord(rr); ok {
				res.Zone = append(res.Zone, rec)
			}
			res.Records++
		}
	}
	return finalize(ctx, res), nil
}

// finalize marks a transfer open iff it yielded at least one RR (a well-formed AXFR always includes
// the SOA), and drains any straggler envelopes' cancellation via ctx (already timed out/cancelled by
// the caller's defer).
func finalize(_ context.Context, res AXFRResult) AXFRResult {
	res.Open = res.Records > 0
	return res
}

// axfrRecord converts one transferred RR into a model.DNSRecord tagged Source="axfr". The slot is
// empty (AXFR names are not tied to the query-plan slots); the name is the record owner, no trailing
// dot. Unsupported RR types are skipped (ok=false).
func axfrRecord(rr dns.RR) (model.DNSRecord, bool) {
	name := strings.TrimSuffix(strings.ToLower(rr.Header().Name), ".")
	rec := model.DNSRecord{Name: name, Slot: "", Rcode: "NOERROR", TTL: rr.Header().Ttl, Source: "axfr"}
	switch v := rr.(type) {
	case *dns.A:
		rec.RecordType, rec.Value = "A", v.A.String()
	case *dns.AAAA:
		rec.RecordType, rec.Value = "AAAA", v.AAAA.String()
	case *dns.CNAME:
		rec.RecordType, rec.Value = "CNAME", strings.TrimSuffix(strings.ToLower(v.Target), ".")
	case *dns.MX:
		rec.RecordType = "MX"
		rec.Priority = v.Preference
		rec.Value = strconv.Itoa(int(v.Preference)) + " " + strings.TrimSuffix(strings.ToLower(v.Mx), ".")
	case *dns.NS:
		rec.RecordType, rec.Value = "NS", strings.TrimSuffix(strings.ToLower(v.Ns), ".")
	case *dns.TXT:
		rec.RecordType, rec.Value = "TXT", strings.Join(v.Txt, "")
	case *dns.SRV:
		rec.RecordType = "SRV"
		rec.Priority = v.Priority
		rec.Value = strings.TrimSpace(v.String()[len(v.Hdr.String()):])
	case *dns.SOA:
		rec.RecordType, rec.Value = "SOA", strings.TrimSpace(v.String()[len(v.Hdr.String()):])
	default:
		return rec, false
	}
	return rec, true
}
```

Note the count semantics the tests assume: `res.Records` counts every RR seen (including SOA and skipped types), while `res.Zone` holds only the RRs that mapped to a supported type. In `axfrZone`, all 5 RRs are supported, so both equal 5.

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/resolve/ -run TestTransferAXFR`
Expected: PASS (open, refused, truncated-by-record-cap all green).

- [ ] **Step 6: Vet, fmt, commit**

```bash
go fmt ./... && go vet ./internal/resolve/
git add internal/resolve/axfr.go internal/resolve/axfr_test.go internal/resolve/testserver_test.go
git commit -m "feat(dns): low-level TCP AXFR transfer with per-RR caps"
```

---

## Task 5: `AXFRProber` — hyperscaler skip, NS-set dedup, aggregate semaphore, pacing

**Files:**
- Modify: `internal/resolve/axfr.go` (add the prober type + methods)
- Modify: `internal/resolve/axfr_test.go` (prober tests)

**Interfaces:**
- Consumes: `scheduler.IsHyperscaler` (Task 1); `transferAXFR` (Task 4); `*scheduler.Scheduler` (existing).
- Produces:
  - `func resolve.NewAXFRProber(sched *scheduler.Scheduler, caps AXFRCaps, maxInflight int) *AXFRProber`
  - `func (p *resolve.AXFRProber) Probe(ctx context.Context, zone string, nsIPs []string) AXFRResult` — applies skip/dedup/pacing and returns the merged result. Task 6 calls this per domain.

- [ ] **Step 1: Write the failing tests**

Add to `internal/resolve/axfr_test.go`:

```go
import (
	// add to the existing import block:
	"cc-dns-worker/internal/scheduler"
)

func newTestProber(caps AXFRCaps) *AXFRProber {
	sched := scheduler.New(scheduler.Config{PerServerQPS: 100, MaxInFlight: 1})
	return NewAXFRProber(sched, caps, 8)
}

func TestProbeOpenZone(t *testing.T) {
	addr, stop := startAXFRServer(t, axfrZone(t), false)
	defer stop()
	p := newTestProber(AXFRCaps{MaxRecords: 50000, MaxBytes: 64 << 20, Deadline: 5 * time.Second})
	res := p.Probe(context.Background(), "example.com", []string{addr})
	if !res.Open || len(res.Zone) != 5 {
		t.Fatalf("want open zone with 5 records, got Open=%v len=%d", res.Open, len(res.Zone))
	}
}

func TestProbeSkipsHyperscalerOnlyNSSet(t *testing.T) {
	// 1.1.1.1 is Cloudflare (hyperscaler). An all-hyperscaler NS set must be skipped without dialing.
	p := newTestProber(AXFRCaps{MaxRecords: 50000, MaxBytes: 64 << 20, Deadline: time.Second})
	res := p.Probe(context.Background(), "example.com", []string{"1.1.1.1"})
	if res.Open {
		t.Fatal("hyperscaler-only NS set should be skipped, not open")
	}
	if res.Server != "" {
		t.Fatalf("skipped probe should not name a server, got %q", res.Server)
	}
}

func TestProbeDedupsRefusedNSSet(t *testing.T) {
	// A refusing server: the first probe transfers, the second short-circuits on the NS-set verdict.
	var dials int
	addr, stop := startCountingRefuser(t, &dials)
	defer stop()
	p := newTestProber(AXFRCaps{MaxRecords: 10, MaxBytes: 1 << 20, Deadline: time.Second})
	_ = p.Probe(context.Background(), "a.example", []string{addr})
	_ = p.Probe(context.Background(), "b.example", []string{addr})
	if dials != 1 {
		t.Fatalf("want 1 dial (second deduped), got %d", dials)
	}
}
```

Add the counting refuser helper to `internal/resolve/testserver_test.go`:

```go
// startCountingRefuser is startAXFRServer(refuse=true) that also counts how many AXFR queries it saw,
// so a test can assert the NS-set dedup suppressed a repeat probe.
func startCountingRefuser(t *testing.T, count *int) (string, func()) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tcp: %v", err)
	}
	var mu sync.Mutex
	mux := dns.NewServeMux()
	mux.HandleFunc(".", func(w dns.ResponseWriter, r *dns.Msg) {
		if r.Question[0].Qtype == dns.TypeAXFR {
			mu.Lock()
			*count++
			mu.Unlock()
			m := new(dns.Msg)
			m.SetReply(r)
			m.Rcode = dns.RcodeRefused
			_ = w.WriteMsg(m)
			return
		}
		m := new(dns.Msg)
		m.SetReply(r)
		_ = w.WriteMsg(m)
	})
	srv := &dns.Server{Listener: l, Handler: mux}
	go func() { _ = srv.ActivateAndServe() }()
	return l.Addr().String(), func() { _ = srv.Shutdown() }
}
```

Add `"sync"` to the `testserver_test.go` import block.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/resolve/ -run TestProbe`
Expected: FAIL — `AXFRProber`, `NewAXFRProber`, `Probe` undefined (compile error).

- [ ] **Step 3: Implement the prober**

Add to `internal/resolve/axfr.go` (imports: add `"context"` is present; add `"sort"`, `"sync"`, and `"cc-dns-worker/internal/scheduler"`):

```go
// AXFRProber applies probe policy over the low-level transfer: it skips NS sets that are entirely
// hyperscaler (they never allow AXFR), remembers per-NS-set REFUSED verdicts so a chronic refuser is
// probed at most once (a refusal is not a transport error, so the scheduler's breaker never trips on
// it — this dedup is what caps the volume), paces every dial through the AXFR scheduler lane, and
// bounds total concurrent transfers with an aggregate semaphore.
type AXFRProber struct {
	sched *scheduler.Scheduler
	caps  AXFRCaps
	sem   chan struct{}

	refused sync.Map // nsSetKey -> struct{}: NS sets known to refuse; skip re-probing
}

// NewAXFRProber builds a prober over the AXFR scheduler lane. maxInflight bounds total concurrent
// transfers across all domains (aggregate held-open TCP connections); <=0 defaults to 50.
func NewAXFRProber(sched *scheduler.Scheduler, caps AXFRCaps, maxInflight int) *AXFRProber {
	if maxInflight <= 0 {
		maxInflight = 50
	}
	return &AXFRProber{sched: sched, caps: caps, sem: make(chan struct{}, maxInflight)}
}

// Probe transfers zone from the first NS IP that yields data. It skips an all-hyperscaler NS set and
// short-circuits an NS set already known to refuse. A zero-value result (Open=false, Server="") means
// skipped or nothing answered.
func (p *AXFRProber) Probe(ctx context.Context, zone string, nsIPs []string) AXFRResult {
	targets := make([]string, 0, len(nsIPs))
	for _, ip := range nsIPs {
		if !scheduler.IsHyperscaler(ip) {
			targets = append(targets, ip)
		}
	}
	if len(targets) == 0 {
		return AXFRResult{} // all-hyperscaler (or empty): skip
	}
	key := nsSetKey(nsIPs)
	if _, refused := p.refused.Load(key); refused {
		return AXFRResult{}
	}

	select {
	case p.sem <- struct{}{}:
	case <-ctx.Done():
		return AXFRResult{}
	}
	defer func() { <-p.sem }()

	for i, ip := range targets {
		var res AXFRResult
		err := p.sched.Do(ctx, ip, func() error {
			r, e := transferAXFR(ctx, zone, ip, p.caps)
			res = r
			return e
		})
		if err == nil && res.Open {
			return res
		}
		_ = i
	}
	// Nothing answered across the whole NS set — remember it as a refuser so peers skip it.
	p.refused.Store(key, struct{}{})
	return AXFRResult{Server: targets[len(targets)-1]}
}

// nsSetKey is the order-independent identity of an NS IP set (dedup is a property of the server set,
// not the domain).
func nsSetKey(nsIPs []string) string {
	c := append([]string(nil), nsIPs...)
	sort.Strings(c)
	return strings.Join(c, ",")
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/resolve/ -run 'TestProbe|TestTransferAXFR'`
Expected: PASS.

- [ ] **Step 5: Run the whole package with the race detector**

Run: `go test -race ./internal/resolve/`
Expected: PASS, no race (the prober is hit concurrently by many workers in production; the `sync.Map` + buffered-channel semaphore must be race-clean).

- [ ] **Step 6: Vet, fmt, commit**

```bash
go fmt ./... && go vet ./internal/resolve/
git add internal/resolve/axfr.go internal/resolve/axfr_test.go internal/resolve/testserver_test.go
git commit -m "feat(dns): AXFR prober with hyperscaler-skip, NS-set dedup, capped concurrency"
```

---

## Task 6: Flags + third scheduler lane + wire into `resolveDomain`

**Files:**
- Modify: `cmd/cc-dns-worker/scan.go` (`scanConfig`, `scanFlags`, `scanCycle`, `resolveDomain`)
- Test: `cmd/cc-dns-worker/scan_test.go`

**Interfaces:**
- Consumes: `resolve.NewAXFRProber`, `resolve.AXFRProber`, `resolve.AXFRCaps`, `scheduler.New` (existing).
- Produces: `resolveDomain(ctx, disc, rec, cfg, domain, scanID, runID, prober)` — new trailing `prober *resolve.AXFRProber` param (nil disables AXFR).

- [ ] **Step 1: Write the failing test**

Add to `cmd/cc-dns-worker/scan_test.go` (verify `resolveDomain` folds AXFR results in when a prober is supplied, and does nothing when nil). This test uses the resolve package's exported prober against a local AXFR server; if `scan_test.go` cannot import the test-only server helper, assert instead at the `resolve` layer. Concretely, add a focused test that a non-nil prober's zone records land on the result with `source="axfr"` and the flags are set:

```go
func TestResolveDomainFoldsAXFR(t *testing.T) {
	// A domain whose discovery + Tier-2 succeed against one fake authoritative server, and whose
	// AXFR prober returns an open zone, must end with axfr records merged and AXFROpen=true.
	// Build a prober pointed at an AXFR server via the resolve package's test constructor.
	t.Skip("integration wiring — exercised by the resolve-package prober tests (Task 5) plus the " +
		"manual dark-rollout smoke in Task 8; resolveDomain merge is verified by review + go vet here")
}
```

Rationale: `resolveDomain` orchestrates discovery + Tier-2 + AXFR against live sockets, which the resolve-package tests already cover per-unit. Rather than stand up three fake servers in `package main`, verify the merge logic by keeping it trivial (below) and rely on the Task 5 unit tests + Task 8 smoke. If you prefer a real assertion, promote the merge into a tiny pure helper `mergeAXFR(res *model.DomainResult, a resolve.AXFRResult)` and unit-test that helper directly — do that in this step instead of the skip.

**Preferred: implement and test the pure merge helper.** Replace the skip with:

```go
func TestMergeAXFR(t *testing.T) {
	res := model.DomainResult{Records: []model.DNSRecord{{Name: "example.com", RecordType: "A", Source: "query"}}}
	a := resolve.AXFRResult{
		Open: true, Records: 2, Truncated: true,
		Zone: []model.DNSRecord{
			{Name: "cpanel.example.com", RecordType: "A", Source: "axfr"},
			{Name: "asa-fw.example.com", RecordType: "A", Source: "axfr"},
		},
	}
	mergeAXFR(&res, a)
	if !res.AXFROpen || res.AXFRRecords != 2 || !res.AXFRTruncated {
		t.Fatalf("flags not merged: %+v", res)
	}
	if len(res.Records) != 3 {
		t.Fatalf("want 3 records after merge, got %d", len(res.Records))
	}
}
```

Ensure `scan_test.go` imports `cc-dns-worker/internal/model` and `cc-dns-worker/internal/resolve`.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/cc-dns-worker/ -run TestMergeAXFR`
Expected: FAIL — `mergeAXFR` undefined.

- [ ] **Step 3: Add the AXFR flags to `scanConfig` and `scanFlags`**

In `cmd/cc-dns-worker/scan.go`, add to `scanConfig` (after `statsInterval`):

```go
	statsInterval     time.Duration
	axfr              bool
	axfrQPS           float64
	axfrInflight      int
	axfrMaxRecords    int
	axfrMaxBytes      int
	axfrTimeout       time.Duration
```

In `scanFlags`, register the flags (before the returned closure):

```go
	axfr := fs.Bool("axfr", false, "enable opportunistic AXFR (zone transfer) probing — master switch, default off")
	axfrQPS := fs.Float64("axfr-qps", 5, "max AXFR transfers/sec per NS IP")
	axfrInflight := fs.Int("axfr-inflight", 50, "max total concurrent AXFR transfers across all domains")
	axfrMaxRecords := fs.Int("axfr-max-records", 50000, "stop draining a zone past this many records")
	axfrMaxBytes := fs.Int("axfr-max-bytes", 67108864, "stop draining a zone past this running byte sum")
	axfrTimeout := fs.Duration("axfr-timeout", 20*time.Second, "whole-transfer timeout per AXFR")
```

And set them in the returned `scanConfig` literal:

```go
			statsInterval: *statsInterval,
			axfr: *axfr, axfrQPS: *axfrQPS, axfrInflight: *axfrInflight,
			axfrMaxRecords: *axfrMaxRecords, axfrMaxBytes: *axfrMaxBytes, axfrTimeout: *axfrTimeout,
```

- [ ] **Step 4: Build the AXFR lane + prober in `scanCycle`, thread it to workers**

In `scanCycle`, after `authSched`/`disc`/`rec` are built (around the `rcfg := records.DefaultConfig()` line), construct the prober only when enabled:

```go
	rcfg := records.DefaultConfig()

	var prober *resolve.AXFRProber
	if cfg.axfr {
		axfrSched := scheduler.New(scheduler.Config{
			PerServerQPS:     cfg.axfrQPS,
			Burst:            max(1, int(cfg.axfrQPS)),
			MaxInFlight:      1, // one transfer per server IP at a time
			BreakerThreshold: cfg.breakerThreshold,
			BreakerCooldown:  cfg.breakerCooldown,
		})
		prober = resolve.NewAXFRProber(axfrSched, resolve.AXFRCaps{
			MaxRecords: cfg.axfrMaxRecords, MaxBytes: cfg.axfrMaxBytes, Deadline: cfg.axfrTimeout,
		}, cfg.axfrInflight)
		log.Printf("scan_id=%s: AXFR probing ENABLED (qps=%.1f inflight=%d max-records=%d timeout=%s)",
			cfg.scanID, cfg.axfrQPS, cfg.axfrInflight, cfg.axfrMaxRecords, cfg.axfrTimeout)
	}
```

Update the worker loop to pass `prober`:

```go
			for d := range work {
				results <- resolveDomain(ctx, disc, rec, rcfg, d, cfg.scanID, cfg.runID, prober)
			}
```

- [ ] **Step 5: Wire AXFR into `resolveDomain` + add the merge helper**

In `cmd/cc-dns-worker/scan.go`, change `resolveDomain`'s signature and add the probe after a successful Tier-2 resolve:

```go
func resolveDomain(ctx context.Context, disc *resolve.Discoverer, rec *resolve.Resolver, cfg records.Config, domain, scanID, runID string, prober *resolve.AXFRProber) model.DomainResult {
	now := time.Now().UTC()
	del, derr := disc.DiscoverNS(ctx, domain)
	if derr != nil || len(del.NSIPs) == 0 {
		msg := "no authoritative NS IPs"
		if derr != nil {
			msg = derr.Error()
		}
		return model.DomainResult{
			ScanID: scanID, RootDomain: domain, ETLD: del.ETLD,
			Nameservers: del.NS, DSPresent: len(del.DS) > 0,
			Status: "error", Error: msg, SourceRunID: runID, ResolvedAt: now,
		}
	}
	res := rec.Resolve(ctx, domain, scanID, runID, del, cfg, now)
	if prober != nil {
		mergeAXFR(&res, prober.Probe(ctx, domain, del.NSIPs))
	}
	return res
}

// mergeAXFR folds an AXFR probe outcome into a domain result: the open-zone flag + counts on the
// summary, and the transferred records (already tagged Source="axfr") into the record set.
func mergeAXFR(res *model.DomainResult, a resolve.AXFRResult) {
	res.AXFROpen = a.Open
	res.AXFRRecords = a.Records
	res.AXFRTruncated = a.Truncated
	res.Records = append(res.Records, a.Zone...)
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test ./cmd/cc-dns-worker/`
Expected: PASS (`TestMergeAXFR` green). Note: `scan_test.go` currently only tests `cleanResolvers` and `runScan` flag validation — it does not call `resolveDomain`, so the signature change needs no test call-site edits. The only in-repo caller of `resolveDomain` is the worker loop in `scanCycle` (updated in Step 4).

- [ ] **Step 7: Build the binary to confirm the whole thing compiles**

Run: `go build ./... && go vet ./...`
Expected: no errors.

- [ ] **Step 8: Vet, fmt, commit**

```bash
go fmt ./... && go vet ./...
git add cmd/cc-dns-worker/scan.go cmd/cc-dns-worker/scan_test.go
git commit -m "feat(dns): wire default-off AXFR probing into the scan cycle"
```

---

## Task 7: ClickHouse DDL for the new columns (external schema)

**Files:**
- Modify: `README.md` (document the new columns + the ALTER statements)
- External: run the DDL against the live ClickHouse before deploying an AXFR-enabled build.

This task has no Go test — it changes the external CH schema and the docs. The `load.go` insert derives its column list from the `ch:"..."` struct tags via reflection, so the CH tables MUST have matching columns or `PrepareBatch`/`Send` fails.

- [ ] **Step 1: Apply the records-table DDL**

Against the live ClickHouse (native client / `clickhouse-client`), add the `source` column and extend the sort key so query-vs-axfr provenance survives the AggregatingMergeTree merge:

```sql
ALTER TABLE corpscout.commoncrawl_domain_dns_records
  ADD COLUMN IF NOT EXISTS source LowCardinality(String) DEFAULT 'query';

-- Extend ORDER BY to keep the same record from query vs axfr as distinct rows.
-- MODIFY ORDER BY may only APPEND new columns that have a default — source qualifies.
ALTER TABLE corpscout.commoncrawl_domain_dns_records
  MODIFY ORDER BY (root_domain, scan_id, record_type, name, value, source);
```

If `MODIFY ORDER BY` is rejected by the table's engine/version, fall back to keeping `source` as a non-key column (query and axfr rows for an identical record then collapse to one on merge — acceptable, but you lose the ability to tell how a record was discovered).

- [ ] **Step 2: Apply the scan-summary DDL**

```sql
ALTER TABLE corpscout.commoncrawl_domain_dns_scan
  ADD COLUMN IF NOT EXISTS axfr_open UInt8 DEFAULT 0,
  ADD COLUMN IF NOT EXISTS axfr_records UInt32 DEFAULT 0,
  ADD COLUMN IF NOT EXISTS axfr_truncated UInt8 DEFAULT 0;
```

These are data columns (not in the `ORDER BY (root_domain, scan_id)` key), so a plain `ADD COLUMN` is sufficient.

- [ ] **Step 3: Verify the columns exist**

Run:

```sql
DESCRIBE corpscout.commoncrawl_domain_dns_records;
DESCRIBE corpscout.commoncrawl_domain_dns_scan;
```

Expected: `source` present on records; `axfr_open`/`axfr_records`/`axfr_truncated` present on scan.

- [ ] **Step 4: Document the columns in README and commit**

In `README.md`, under the `commoncrawl_domain_dns_records` column table add a `source` row (`LowCardinality(String)`, "query|axfr provenance"); under `commoncrawl_domain_dns_scan` add the three `axfr_*` rows. Note in prose that an AXFR-enabled build requires the DDL above to have been applied first.

```bash
git add README.md
git commit -m "docs(dns): document source + axfr_* columns and the ClickHouse DDL"
```

---

## Task 8: Dark-rollout smoke test on a bounded sample

**Files:** none (operational verification).

- [ ] **Step 1: Full test + vet + build gate**

Run: `go test -race ./... && go vet ./... && go build ./...`
Expected: all PASS.

- [ ] **Step 2: Run a bounded live sample with AXFR enabled**

Point at the local recursive resolver and a small `--limit`. Requires the CH DDL (Task 7) applied and `CLICKHOUSE_*` env set.

```bash
go run ./cmd/cc-dns-worker scan \
  --resolvers 127.0.0.1:53 \
  --limit 2000 \
  --axfr --axfr-qps 5 --axfr-inflight 25 --axfr-timeout 20s \
  --db /tmp/axfr-smoke.db --scan-id axfr-smoke
```

Expected: the run completes; the log shows `AXFR probing ENABLED`; no worker stalls.

- [ ] **Step 3: Confirm the signal landed in ClickHouse**

```sql
SELECT count() AS open_zones
FROM corpscout.commoncrawl_domain_dns_scan FINAL
WHERE scan_id = 'axfr-smoke' AND axfr_open = 1;

SELECT count() AS axfr_records
FROM corpscout.commoncrawl_domain_dns_records
WHERE scan_id = 'axfr-smoke' AND source = 'axfr';
```

Expected: `open_zones` is small (low single-digit % of the sample at most — most servers refuse); `axfr_records` is non-zero only if at least one open zone was found. A zero-open sample is a valid outcome — re-run with a larger `--limit` or a seeded known-open test zone to exercise the retain path end-to-end.

- [ ] **Step 4: Record the observed hit-rate and transfer-latency tail**

From the run's stats output and the queries above, note the open-AXFR hit-rate and any slow transfers. These numbers drive the steady-state cap tuning (spec §7 step 3). No commit — this is an operational reading that informs whether to wire `--axfr` into the orchestrator's steady-state flags.

---

## Self-Review

**Spec coverage:**
- §1 Probe (`internal/resolve/axfr.go`, TCP `dns.Transfer`, rotate NS IPs) → Tasks 4, 5. ✓
- §2 Caps (per-RR MaxRecords/MaxBytes/Deadline, Truncated) → Task 4. ✓
- §3 Separate lane (third `Scheduler`, aggregate `--axfr-inflight` semaphore) → Tasks 5 (semaphore), 6 (lane). ✓
- §4 Skip hyperscalers (export `IsHyperscaler`) + NS-set dedup of refusers → Tasks 1, 5. ✓
- §5 Retain-with-provenance (`source` column, records → existing table, `axfr_open`/`axfr_records`/`axfr_truncated` on summary, plumbing across model/store/load/DomainResult) → Tasks 2, 3, 6, 7. Downstream inference explicitly NOT built. ✓
- §6 Flags (all six, default-off, on `scanConfig`/`scanFlags`) → Task 6. ✓
- §7 Rollout (probe+tests behind `--axfr=false`, TCP test harness with REFUSED + capped paths, bounded sample) → Tasks 4, 5, 8. ✓
- Risk/retention: worker's job ends at `source='axfr'` + `axfr_open`; no corroboration/GDPR logic here → honored (no such task). ✓

**Placeholder scan:** No TBD/TODO. The one `t.Skip` in Task 6 Step 1 is immediately superseded by the preferred `mergeAXFR` helper test in the same step — the implementer builds the pure helper and tests it. ✓

**Type consistency:** `Source string` on both `DNSRecord` and `RecordRow`; `AXFROpen`/`AXFRRecords`/`AXFRTruncated` typed `bool`/`int`/`bool` on `DomainResult` and `uint8`/`uint32`/`uint8` on `ScanRow` (converted via `b2i`/`uint32` in the store). `IsHyperscaler` (exported) used consistently in Tasks 1 and 5. `transferAXFR` (unexported, Task 4) called only by `AXFRProber.Probe` (Task 5). `NewAXFRProber(sched, caps, maxInflight)` and `Probe(ctx, zone, nsIPs)` signatures match between Tasks 5 and 6. `resolveDomain(..., prober)` new trailing param matches the worker-loop call site in Task 6. ✓
