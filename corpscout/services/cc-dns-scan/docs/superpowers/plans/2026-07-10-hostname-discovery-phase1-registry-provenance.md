# Hostname Discovery — Phase 1: `discovery` Provenance + Durable Registry + Write-back

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Record how each hostname was discovered (`discovery` column), and make discovered hostnames durable via a ClickHouse registry that a per-cycle write-back accumulates — so an AXFR-internal host survives the zone closing.

**Architecture:** (1) a records `discovery` column (`static`|`ct`|`axfr`), a second provenance axis distinct from `source` (how the *record* was obtained); (2) a new `commoncrawl_domain_hostnames` AggregatingMergeTree registry; (3) a cycle-end write-back that reads the cycle's non-static resolved hosts from the SQLite stage and blind-`INSERT`s them into the registry (AggregatingMergeTree folds them — no read-before-write). This is Phase 1 of the [hostname-discovery spec](../../../../docs/hostname-discovery-spec.md) (§1, §2, §5 migrations 2&4). Phase 2 (seed-time CT/registry read + `Plan` union) consumes the registry; it is NOT in this plan. `discovery`/registry columns follow the migration-000107 lesson: non-key `SimpleAggregateFunction`, never a defaulted key column.

**Tech Stack:** Go 1.25, ClickHouse (golang-migrate), SQLite stage, pytest (migration validation).

**Spec:** `corpscout/commoncrawl/docs/hostname-discovery-spec.md`

## Global Constraints

- Go 1.25 module (`cc-dns-worker`). `go fmt ./...` + `go vet ./...` before Go commits; tests stdlib `testing` only (no testify); DO NOT run `go mod tidy`.
- **ClickHouse constraint (from migration 000107):** never add a defaulted column to a sorting key. New provenance/registry data columns are `SimpleAggregateFunction(anyLast|min|max, …)`, NOT in any `ORDER BY`. A registry sort key uses plain columns present at `CREATE`.
- Migration files in `corpscout/clickhouse/migrations/` (`0001NN_corpscout_<name>.up/down.sql`), registered in `EXPECTED_MIGRATIONS` in `corpscout/dagster_v3/tests/test_clickhouse_migrations.py`; validation rules: up must CREATE/ALTER/DROP, no `TRUNCATE TABLE` in up, matching `.down.sql`, and **no `;` inside `--` line comments**.
- Next-free migration numbers after `000108` are `000109`, `000110` (confirm at implementation; renumber if taken).
- The reflection-driven load path (`internal/load` `chColumns` + `AppendStruct`) inserts a Go `string` into a `SimpleAggregateFunction(…, LowCardinality(String))` column fine (verified for `source`) — so no Go type change is needed for the CH column type.
- Applying migrations to live ClickHouse is an operator step (`make clickhouse-migrate-up`) — this plan creates + validates files only.
- Conventional Commits. Git root `/Users/graovic/pulsarpoint/ppoint/companycollect`.

## File Structure

- `corpscout/clickhouse/migrations/000109_corpscout_commoncrawl_domain_dns_records_discovery.{up,down}.sql`
- `corpscout/clickhouse/migrations/000110_corpscout_commoncrawl_domain_hostnames.{up,down}.sql`
- `corpscout/dagster_v3/tests/test_clickhouse_migrations.py` — register 109, 110.
- `internal/model/model.go` — `DNSRecord.Discovery`, `RecordRow.Discovery`, new `HostnameRow`.
- `internal/resolve/query.go` — set `Discovery: "static"` on query records + DS.
- `internal/resolve/axfr.go` — set `Discovery: "axfr"` on AXFR records.
- `internal/store/store.go` — `scan_records.discovery` column + migrate + insert + `StagedRecords`/`RecordsAfter`; new `DiscoveredHostnames`.
- `internal/load/load.go` — new `WriteHostnameRegistry`.
- `internal/load/load_test.go` — RecordRow column count 13→14.
- `cmd/cc-dns-worker/run.go` — call the write-back at cycle end (flush phase).
- `cmd/cc-dns-worker/load.go` — call the write-back in the standalone `load` path.

---

## Task 1: Migrations — records `discovery` + `commoncrawl_domain_hostnames` registry

**Files:**
- Create: `000109_corpscout_commoncrawl_domain_dns_records_discovery.{up,down}.sql`
- Create: `000110_corpscout_commoncrawl_domain_hostnames.{up,down}.sql`
- Modify: `corpscout/dagster_v3/tests/test_clickhouse_migrations.py` (`EXPECTED_MIGRATIONS`)

**Interfaces:**
- Produces: CH column `discovery` on records (for Task 2's `RecordRow.Discovery`); CH table `commoncrawl_domain_hostnames` (for Task 3's `HostnameRow`).

- [ ] **Step 1: Confirm next-free numbers**

Run: `ls corpscout/clickhouse/migrations/ | sed 's/_.*//' | sort -u | tail -3`
Expected: highest `000108`. If `000109`/`000110` exist, use the next free pair and adjust names + EXPECTED_MIGRATIONS.

- [ ] **Step 2: records `discovery` migration**

`000109_corpscout_commoncrawl_domain_dns_records_discovery.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- Add hostname-discovery provenance (static, ct, axfr) as a non-key SimpleAggregateFunction, same as
-- source (migration 000107) -- ClickHouse forbids a defaulted column in the sort key. anyLast matches
-- the sibling data columns. This is a different axis from source (how the record was obtained).
ALTER TABLE corpscout.commoncrawl_domain_dns_records
    ADD COLUMN IF NOT EXISTS discovery SimpleAggregateFunction(anyLast, LowCardinality(String));
```

`000109_…discovery.down.sql`:

```sql
-- Remove the discovery column (non-key, so a plain DROP COLUMN reverses the up cleanly).
ALTER TABLE corpscout.commoncrawl_domain_dns_records
    DROP COLUMN IF EXISTS discovery;
```

- [ ] **Step 3: registry migration**

`000110_corpscout_commoncrawl_domain_hostnames.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- Durable per-domain hostname registry: the monotonic set of every hostname ever discovered for a
-- domain, so AXFR-internal hosts survive the zone closing (Phase 2 re-reads this at seed). The per-cycle
-- write-back blind-INSERTs one row per discovered label -- AggregatingMergeTree folds duplicates with
-- first_seen=min, last_seen=max, last_resolved=max, discovery_source=min (axfr precedence), so no
-- read-before-write. The sort key is the plain (root_domain, label) columns present at CREATE.
DROP TABLE IF EXISTS corpscout.commoncrawl_domain_hostnames;

CREATE TABLE corpscout.commoncrawl_domain_hostnames
(
    root_domain      String,
    label            String,
    discovery_source SimpleAggregateFunction(min, LowCardinality(String)),
    first_seen       SimpleAggregateFunction(min, DateTime64(3, 'UTC')),
    last_seen        SimpleAggregateFunction(max, DateTime64(3, 'UTC')),
    last_resolved    SimpleAggregateFunction(max, DateTime64(3, 'UTC'))
)
ENGINE = AggregatingMergeTree()
ORDER BY (root_domain, label);
```

`000110_…hostnames.down.sql`:

```sql
-- Drop the hostname registry.
DROP TABLE IF EXISTS corpscout.commoncrawl_domain_hostnames;
```

- [ ] **Step 4: Register in EXPECTED_MIGRATIONS**

Append after `"000108_corpscout_commoncrawl_domain_dns_scan_axfr"`:

```python
    "000109_corpscout_commoncrawl_domain_dns_records_discovery",
    "000110_corpscout_commoncrawl_domain_hostnames",
```

- [ ] **Step 5: Validate**

Run: `cd corpscout/dagster_v3 && timeout 200 uv run pytest tests/test_clickhouse_migrations.py -q`
Expected: PASS (files-explicit, down-files, no-`;`-in-comments, create/alter/drop structural checks all green). If a test needs live ClickHouse and none is configured, run the static checks and note skips.

- [ ] **Step 6: Commit**

```bash
git add corpscout/clickhouse/migrations/000109_* corpscout/clickhouse/migrations/000110_* corpscout/dagster_v3/tests/test_clickhouse_migrations.py
git commit -m "feat(dns): migrations for records.discovery + commoncrawl_domain_hostnames registry"
```

---

## Task 2: `discovery` provenance plumbing

**Files:**
- Modify: `internal/model/model.go` (`DNSRecord`, `RecordRow`)
- Modify: `internal/resolve/query.go` (`collect`, DS append)
- Modify: `internal/resolve/axfr.go` (`axfrRecord`)
- Modify: `internal/store/store.go` (schema, migrate, insert, `StagedRecords`, `RecordsAfter`)
- Modify: `internal/load/load_test.go` (RecordRow 13→14)
- Test: `internal/store/store_test.go`

**Interfaces:**
- Produces: `model.DNSRecord.Discovery string`; `model.RecordRow.Discovery string` (`ch:"discovery"`, appended at END). Values `static`|`ct`|`axfr`. Query records + DS = `static`; AXFR records = `axfr`. (`ct` arrives in Phase 2.)

- [ ] **Step 1: Failing test**

Add to `internal/store/store_test.go` (round-trip discovery, mirroring `TestCommitBatchPersistsSource`):

```go
func TestCommitBatchPersistsDiscovery(t *testing.T) {
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
			{Name: "www.example.com", RecordType: "A", Value: "1.2.3.4", Rcode: "NOERROR", Source: "query", Discovery: "static"},
			{Name: "jenkins.example.com", RecordType: "A", Value: "10.0.0.5", Rcode: "NOERROR", Source: "axfr", Discovery: "axfr"},
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
		got[r.Name] = r.Discovery
	}
	if got["www.example.com"] != "static" || got["jenkins.example.com"] != "axfr" {
		t.Fatalf("discovery not round-tripped: %+v", got)
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd corpscout/commoncrawl/cc-dns-worker && go test ./internal/store/ -run TestCommitBatchPersistsDiscovery`
Expected: FAIL — `Discovery` undefined (compile error).

- [ ] **Step 3: Model fields**

In `internal/model/model.go`, add to `DNSRecord` after `Source`:

```go
	Source     string // "query" (actively queried) | "axfr" (from a zone transfer)
	Discovery  string // "static" | "ct" | "axfr" — how the hostname was discovered
```

Add to `RecordRow` after `Source` (END):

```go
	Source     string    `ch:"source"`
	Discovery  string    `ch:"discovery"`
```

- [ ] **Step 4: Set discovery where records are built**

In `internal/resolve/query.go`, the `collect` per-RR construction (line ~100):

```go
		rec := model.DNSRecord{Name: name, Slot: q.Slot, Rcode: rcode, TTL: rr.Header().Ttl, Source: "query", Discovery: "static"}
```

And the DS append (line ~32):

```go
		res.Records = append(res.Records, model.DNSRecord{Name: domain, RecordType: "DS", Slot: "", Value: ds, Rcode: "NOERROR", Source: "query", Discovery: "static"})
```

In `internal/resolve/axfr.go`, `axfrRecord` (line ~111):

```go
	rec := model.DNSRecord{Name: name, Slot: "", Rcode: "NOERROR", TTL: rr.Header().Ttl, Source: "axfr", Discovery: "axfr"}
```

- [ ] **Step 5: SQLite column + read/write**

In `internal/store/store.go`:

Add to `scan_records` in the `schema` const (after `source`):

```go
  source       TEXT DEFAULT 'query',
  discovery    TEXT DEFAULT 'static',
```

Add to the `migrate()` ALTER list:

```go
		`ALTER TABLE scan_records ADD COLUMN discovery TEXT DEFAULT 'static'`,
```

Update the `insR` INSERT in `CommitBatch` to add `discovery` (column list + one more `?` + `rec.Discovery` arg after `rec.Source`).

Update `StagedRecords`' SELECT + Scan to read `discovery` (after `source` / `&r.Source`) into `&r.Discovery`.

Update `recordsAfterQuery` + `RecordsAfter`'s Scan identically (add `discovery` after `source`, `&r.Discovery` after `&r.Source`).

- [ ] **Step 6: Column-count test**

In `internal/load/load_test.go`, change the RecordRow assertion `len(rc) != 13` to `len(rc) != 14`. Leave ScanRow at `15`.

- [ ] **Step 7: Run tests**

Run: `cd corpscout/commoncrawl/cc-dns-worker && go test ./internal/store/ ./internal/load/ ./internal/resolve/ && go build ./... && go test ./...`
Expected: PASS (new test, `TestColumnLists` RecordRow=14, whole suite).

- [ ] **Step 8: Vet, fmt, commit**

```bash
cd corpscout/commoncrawl/cc-dns-worker && go fmt ./... && go vet ./...
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/commoncrawl/cc-dns-worker/internal/model/model.go corpscout/commoncrawl/cc-dns-worker/internal/resolve/query.go corpscout/commoncrawl/cc-dns-worker/internal/resolve/axfr.go corpscout/commoncrawl/cc-dns-worker/internal/store/store.go corpscout/commoncrawl/cc-dns-worker/internal/store/store_test.go corpscout/commoncrawl/cc-dns-worker/internal/load/load_test.go
git commit -m "feat(dns): add discovery provenance (static|ct|axfr) to DNS records"
```

---

## Task 3: Registry write-back (cycle-end)

**Files:**
- Modify: `internal/model/model.go` (`HostnameRow`)
- Modify: `internal/store/store.go` (`DiscoveredHostnames`)
- Modify: `internal/load/load.go` (`WriteHostnameRegistry`)
- Modify: `cmd/cc-dns-worker/run.go` (call at flush) and `cmd/cc-dns-worker/load.go` (call in standalone load)
- Test: `internal/store/store_test.go`, `internal/load/integration_test.go` (if a CH integration test exists; else a store-level test)

**Interfaces:**
- Consumes: `scan_records` rows with `discovery`, `source`, `record_type`, `name` (Task 2).
- Produces: `model.HostnameRow` (ch tags for `commoncrawl_domain_hostnames`); `store.DiscoveredHostnames(ctx, scanID) ([]HostnameRow, error)`; `load.WriteHostnameRegistry(ctx, conn, st, scanID) (int, error)`.

- [ ] **Step 1: `HostnameRow` model**

In `internal/model/model.go`:

```go
// HostnameRow mirrors corpscout.commoncrawl_domain_hostnames (AggregatingMergeTree registry). One
// blind-inserted row per hostname discovered in a cycle; the merge folds first_seen=min, last_seen=max,
// last_resolved=max, discovery_source=min. Insert a plain string into the SimpleAggregateFunction cols.
type HostnameRow struct {
	RootDomain      string    `ch:"root_domain"`
	Label           string    `ch:"label"`
	DiscoverySource string    `ch:"discovery_source"`
	FirstSeen       time.Time `ch:"first_seen"`
	LastSeen        time.Time `ch:"last_seen"`
	LastResolved    time.Time `ch:"last_resolved"`
}
```

- [ ] **Step 2: Failing test for `DiscoveredHostnames`**

Add to `internal/store/store_test.go` (a domain with an AXFR-discovered host and a static host; only the non-static host, normalized to a label, is returned):

```go
func TestDiscoveredHostnames(t *testing.T) {
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
			{Name: "example.com", RecordType: "A", Value: "1.1.1.1", Rcode: "NOERROR", Source: "query", Discovery: "static"},   // apex — excluded
			{Name: "www.example.com", RecordType: "A", Value: "1.2.3.4", Rcode: "NOERROR", Source: "query", Discovery: "static"}, // static — excluded
			{Name: "jenkins.example.com", RecordType: "A", Value: "10.0.0.5", Rcode: "NOERROR", Source: "axfr", Discovery: "axfr"}, // captured
			{Name: "vpn.example.com", RecordType: "CNAME", Value: "gw.example.net", Rcode: "NOERROR", Source: "axfr", Discovery: "axfr"}, // captured
		},
	}
	if err := st.CommitBatch(ctx, []model.DomainResult{res}); err != nil {
		t.Fatal(err)
	}
	rows, err := st.DiscoveredHostnames(ctx, "s1")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, r := range rows {
		got[r.Label] = r.DiscoverySource
	}
	if len(got) != 2 || got["jenkins"] != "axfr" || got["vpn"] != "axfr" {
		t.Fatalf("want {jenkins:axfr, vpn:axfr}, got %+v", got)
	}
}
```

- [ ] **Step 3: Run to verify it fails**

Run: `cd corpscout/commoncrawl/cc-dns-worker && go test ./internal/store/ -run TestDiscoveredHostnames`
Expected: FAIL — `DiscoveredHostnames` undefined.

- [ ] **Step 4: Implement `DiscoveredHostnames`**

In `internal/store/store.go` (reads distinct non-static A/AAAA/CNAME host records, normalizes each `name` to a label by stripping `.<root_domain>`; `first_seen`/`last_seen`/`last_resolved` are set to now by the caller — leave them zero here and let the load layer stamp them, OR stamp here with a passed clock). Stamp in the caller (load) to keep the store pure:

```go
// DiscoveredHostnames returns the distinct non-static hosts (discovery in ct/axfr) that resolved this
// scan, one per (root_domain, label), for the registry write-back. label = name minus ".<root_domain>";
// apex and non-subdomain names are skipped. Timestamps are left zero for the caller to stamp.
func (s *Store) DiscoveredHostnames(ctx context.Context, scanID string) ([]model.HostnameRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT root_domain, name, discovery
		FROM scan_records
		WHERE scan_id = ? AND discovery IN ('ct','axfr') AND record_type IN ('A','AAAA','CNAME')`, scanID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	seen := map[string]bool{}
	var out []model.HostnameRow
	for rows.Next() {
		var rd, name, disc string
		if err := rows.Scan(&rd, &name, &disc); err != nil {
			return nil, err
		}
		suffix := "." + rd
		if name == rd || !strings.HasSuffix(name, suffix) {
			continue // apex or not a subdomain of rd
		}
		label := strings.ToLower(strings.TrimSuffix(name, suffix))
		if label == "" {
			continue
		}
		key := rd + "\x00" + label
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, model.HostnameRow{RootDomain: rd, Label: label, DiscoverySource: disc})
	}
	return out, rows.Err()
}
```

(`strings` is already imported in store.go.)

- [ ] **Step 5: Run to verify it passes**

Run: `cd corpscout/commoncrawl/cc-dns-worker && go test ./internal/store/ -run TestDiscoveredHostnames`
Expected: PASS.

- [ ] **Step 6: Implement `WriteHostnameRegistry`**

In `internal/load/load.go`, add the registry table constant and the write-back (reuses the generic `insert[T]`; stamps timestamps to now):

```go
const hostnamesTable = "corpscout.commoncrawl_domain_hostnames"

// WriteHostnameRegistry upserts this cycle's non-static discovered hosts into the durable hostname
// registry. Blind INSERT — the AggregatingMergeTree folds first_seen=min, last_seen=max,
// last_resolved=max, discovery_source=min — so it is idempotent and needs no read-before-write.
func WriteHostnameRegistry(ctx context.Context, conn driver.Conn, st *store.Store, scanID string, now time.Time) (int, error) {
	rows, err := st.DiscoveredHostnames(ctx, scanID)
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}
	now = now.UTC()
	for i := range rows {
		rows[i].FirstSeen, rows[i].LastSeen, rows[i].LastResolved = now, now, now
	}
	return insert(ctx, conn, hostnamesTable, rows)
}
```

(Add `"time"` to load.go imports if missing.)

- [ ] **Step 7: Wire into the cycle end**

In `cmd/cc-dns-worker/run.go`, in `runFlushPhase`, after the final `incLoad` succeeds, run the write-back (fresh CH conn like `incLoad` uses):

```go
	n, err := incLoad(ctx, st, state.CycleID, loadBatch)
	if err != nil {
		return err
	}
	log.Printf("cycle %s: final flush loaded %d records to ClickHouse", state.CycleID, n)
	if hn, herr := regWriteBack(ctx, st, state.CycleID); herr != nil {
		log.Printf("cycle %s: hostname registry write-back error: %v", state.CycleID, herr)
	} else if hn > 0 {
		log.Printf("cycle %s: registered %d discovered hostnames", state.CycleID, hn)
	}
	return nil
```

Add the helper near `incLoad`:

```go
// regWriteBack opens a fresh ClickHouse connection and upserts the cycle's discovered hostnames into
// the durable registry.
func regWriteBack(ctx context.Context, st *store.Store, scanID string) (int, error) {
	conn, err := chConn()
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	return load.WriteHostnameRegistry(ctx, conn, st, scanID, time.Now())
}
```

(Ensure `run.go` imports `"time"` and `"cc-dns-worker/internal/load"` — it already uses `load` via `incLoad`.)

In `cmd/cc-dns-worker/load.go` (the standalone `load` subcommand), after `load.FromStore(...)` succeeds, call `load.WriteHostnameRegistry(ctx, conn, st, scanID, time.Now())` with the same conn and log the count, so a standalone load also populates the registry.

- [ ] **Step 8: Run full suite + build**

Run: `cd corpscout/commoncrawl/cc-dns-worker && go build ./... && go vet ./... && go test ./...`
Expected: PASS (the write-back is exercised at the store level by `TestDiscoveredHostnames`; the CH insert reuses the verified generic `insert[T]`).

- [ ] **Step 9: Commit**

```bash
cd corpscout/commoncrawl/cc-dns-worker && go fmt ./...
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/commoncrawl/cc-dns-worker/internal/model/model.go corpscout/commoncrawl/cc-dns-worker/internal/store/store.go corpscout/commoncrawl/cc-dns-worker/internal/store/store_test.go corpscout/commoncrawl/cc-dns-worker/internal/load/load.go corpscout/commoncrawl/cc-dns-worker/cmd/cc-dns-worker/run.go corpscout/commoncrawl/cc-dns-worker/cmd/cc-dns-worker/load.go
git commit -m "feat(dns): cycle-end hostname registry write-back (durable discovered hosts)"
```

---

## Self-Review

**Spec coverage:** Phase 1 = spec §2 (`discovery` axis), §1 (registry), §5 migrations 2 (discovery) & 4 (registry), §4 write-back. Task 1 = both migrations. Task 2 = `discovery` plumbing. Task 3 = registry write-back. Phase 2 (seed read + CT + `Plan` union) is NOT here. ✓

**Placeholder scan:** No TBD/TODO; every migration and code step has literal content. ✓

**Type/lesson consistency:** `discovery` and all registry data columns are non-key `SimpleAggregateFunction` (migration-000107 lesson); the registry sort key is the plain `(root_domain, label)` columns at `CREATE`. `DNSRecord.Discovery`/`RecordRow.Discovery` are `string` (`ch:"discovery"`); RecordRow count 13→14, ScanRow stays 15. `HostnameRow` ch tags match the 000110 columns exactly. Write-back captures `discovery IN ('ct','axfr')` (in Phase 1 only `axfr` exists; `ct` is forward-compatible for Phase 2). Query records + DS = `static`, AXFR = `axfr`. ✓

**Scale note:** `DiscoveredHostnames` scans `scan_records` once per cycle-end with a `DISTINCT` — acceptable at cycle boundary; can be made incremental later if it becomes a bottleneck (log the row count so this is observable).
