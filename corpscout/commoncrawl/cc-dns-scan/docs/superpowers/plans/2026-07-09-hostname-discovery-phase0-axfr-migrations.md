# Hostname Discovery — Phase 0: AXFR ClickHouse Migrations + `axfr_server`

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the already-merged AXFR feature deployable by turning its ClickHouse schema changes into proper migration files, and record which NS IP answered a transfer (`axfr_server`).

**Architecture:** Three focused changes: (1) golang-migrate files that add `source` (+re-key) to `commoncrawl_domain_dns_records` and the `axfr_*` + new `axfr_server` columns to `commoncrawl_domain_dns_scan`, registered in the dagster `EXPECTED_MIGRATIONS` test; (2) thread `axfr_server` through the Go worker (model → SQLite store → `mergeAXFR`); (3) fix the cc-dns-worker README's stale ORDER BY and point it at the migrations. This is the smallest, independently-shippable slice of the [hostname-discovery spec](../../../../docs/hostname-discovery-spec.md) — it only unblocks AXFR; no registry, CT, or `discovery` work here.

**Tech Stack:** Go 1.25, ClickHouse (golang-migrate `.up.sql`/`.down.sql`), SQLite stage, pytest (dagster migration-validation test).

**Spec:** `corpscout/commoncrawl/docs/hostname-discovery-spec.md` (§5 migrations 1 & 3, §8 Phase 0)

## Global Constraints

- Go 1.25 module (`cc-dns-worker`). Run `go fmt ./...` + `go vet ./...` before Go commits. Tests: stdlib `testing` only (no testify). DO NOT run `go mod tidy` (strips pre-fetched deps; pre-existing go.mod "should be direct" warnings are expected).
- The **live** records table (migration 000105) is `AggregatingMergeTree` with `ORDER BY (root_domain, record_type, slot, name, value)` — no `scan_id`. The correct re-key is `(root_domain, record_type, slot, name, value, source)`. Do NOT use the stale scan_id-based key.
- The **live** scan table (migration 000106) is `ReplacingMergeTree(resolved_at)` `ORDER BY (root_domain)`.
- Migration files live in `corpscout/clickhouse/migrations/`, named `0001NN_corpscout_<name>.up.sql` / `.down.sql`. Every migration must be listed in `EXPECTED_MIGRATIONS` in `corpscout/dagster_v3/tests/test_clickhouse_migrations.py` (an exact-match test) and satisfy its validation rules: up must CREATE/ALTER/DROP (not a no-op), no `TRUNCATE` in up, a matching `.down.sql` must exist, and **line comments must not contain semicolons**.
- Use the next free migration numbers after the highest present (currently `000106`; the ledger notes a prior `000101` collision from concurrent work — if `000107`/`000108` are taken at implementation time, use the next free pair and keep the two migrations consecutive).
- Conventional Commits. Git root is `/Users/graovic/pulsarpoint/ppoint/companycollect`.
- Applying migrations to live ClickHouse is an operator step (production DDL on a 322M-row table, incl. `MODIFY ORDER BY`) — this plan CREATES and validates the files; it does not apply them to production.
- **Pre-existing red test:** `test_clickhouse_migration_files_are_explicit` is already failing on this branch (the prior build's `000105`/`000106` files aren't in `EXPECTED_MIGRATIONS`). Task 1 reconciles this (registers 105–108) so the test ends green — an intentional, in-scope fix, not scope creep, since Task 1 already edits that file.

---

## File Structure

- `corpscout/clickhouse/migrations/000107_corpscout_commoncrawl_domain_dns_records_source.{up,down}.sql` — add `source` + re-key records.
- `corpscout/clickhouse/migrations/000108_corpscout_commoncrawl_domain_dns_scan_axfr.{up,down}.sql` — add `axfr_open/records/truncated/axfr_server` to scan summary.
- `corpscout/dagster_v3/tests/test_clickhouse_migrations.py` — append the two names to `EXPECTED_MIGRATIONS`.
- `corpscout/commoncrawl/cc-dns-worker/internal/model/model.go` — `DomainResult.AXFRServer`, `ScanRow.AXFRServer`.
- `corpscout/commoncrawl/cc-dns-worker/internal/store/store.go` — `axfr_server` column + migrate + read/write paths.
- `corpscout/commoncrawl/cc-dns-worker/internal/load/load_test.go` — ScanRow column count 14→15.
- `corpscout/commoncrawl/cc-dns-worker/cmd/cc-dns-worker/scan.go` — `mergeAXFR` sets `AXFRServer`.
- `corpscout/commoncrawl/cc-dns-worker/README.md` — fix stale ORDER BY, point to migrations.

---

## Task 1: ClickHouse migration files + register in EXPECTED_MIGRATIONS

**Files:**
- Create: `corpscout/clickhouse/migrations/000107_corpscout_commoncrawl_domain_dns_records_source.up.sql`
- Create: `corpscout/clickhouse/migrations/000107_corpscout_commoncrawl_domain_dns_records_source.down.sql`
- Create: `corpscout/clickhouse/migrations/000108_corpscout_commoncrawl_domain_dns_scan_axfr.up.sql`
- Create: `corpscout/clickhouse/migrations/000108_corpscout_commoncrawl_domain_dns_scan_axfr.down.sql`
- Modify: `corpscout/dagster_v3/tests/test_clickhouse_migrations.py` (`EXPECTED_MIGRATIONS`)

**Interfaces:**
- Produces: the CH columns the merged worker's reflection-driven INSERT already emits (`source` on records; `axfr_open/axfr_records/axfr_truncated` on scan) plus `axfr_server` (consumed by Task 2's `ScanRow.AXFRServer`).

- [ ] **Step 1: Confirm the next-free migration numbers**

Run: `ls corpscout/clickhouse/migrations/ | sed 's/_.*//' | sort -u | tail -3`
Expected: highest is `000106`. If `000107`/`000108` already exist, use the next free consecutive pair and adjust all filenames/EXPECTED_MIGRATIONS entries below accordingly.

- [ ] **Step 2: Write the records migration (up + down)**

`000107_corpscout_commoncrawl_domain_dns_records_source.up.sql`:

```sql
-- Add source provenance (query vs axfr) to the distinct records table and extend the sort key so
-- query- and axfr-discovered copies of the same record stay distinct rows through the merge.
-- The worker's reflection-driven INSERT already emits this column; this migration makes loads work.
ALTER TABLE corpscout.commoncrawl_domain_dns_records
    ADD COLUMN IF NOT EXISTS source LowCardinality(String) DEFAULT 'query';

ALTER TABLE corpscout.commoncrawl_domain_dns_records
    MODIFY ORDER BY (root_domain, record_type, slot, name, value, source);
```

`000107_corpscout_commoncrawl_domain_dns_records_source.down.sql` (MODIFY ORDER BY cannot shrink a key, so recreate the 000105 shape — same DROP+CREATE pattern the 000105 down uses):

```sql
-- Restore the 000105 distinct records table without source (MODIFY ORDER BY cannot drop key columns).
DROP TABLE IF EXISTS corpscout.commoncrawl_domain_dns_records;

CREATE TABLE corpscout.commoncrawl_domain_dns_records
(
    root_domain  String,
    record_type  LowCardinality(String),
    slot         LowCardinality(String),
    name         String,
    value        String,
    ttl          SimpleAggregateFunction(anyLast, UInt32),
    priority     SimpleAggregateFunction(anyLast, UInt16),
    rcode        SimpleAggregateFunction(anyLast, String),
    last_run_id  SimpleAggregateFunction(anyLast, String),
    first_seen   SimpleAggregateFunction(min, DateTime64(3, 'UTC')),
    last_seen    SimpleAggregateFunction(max, DateTime64(3, 'UTC')),
    scans        SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree()
ORDER BY (root_domain, record_type, slot, name, value);
```

- [ ] **Step 3: Write the scan-summary migration (up + down)**

`000108_corpscout_commoncrawl_domain_dns_scan_axfr.up.sql`:

```sql
-- Add the AXFR probe outputs to the scan summary: the open-zone-transfer flag, record/truncation
-- counts, and the NS IP that answered the transfer (empty when the zone is closed). These are plain
-- data columns, not in the ReplacingMergeTree sort key, so a simple ADD COLUMN suffices.
ALTER TABLE corpscout.commoncrawl_domain_dns_scan
    ADD COLUMN IF NOT EXISTS axfr_open UInt8 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS axfr_records UInt32 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS axfr_truncated UInt8 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS axfr_server String DEFAULT '';
```

`000108_corpscout_commoncrawl_domain_dns_scan_axfr.down.sql`:

```sql
-- Drop the AXFR columns (non-key, so removable without recreating the table).
ALTER TABLE corpscout.commoncrawl_domain_dns_scan
    DROP COLUMN IF EXISTS axfr_open,
    DROP COLUMN IF EXISTS axfr_records,
    DROP COLUMN IF EXISTS axfr_truncated,
    DROP COLUMN IF EXISTS axfr_server;
```

Note: keep every `--` line comment free of semicolons (a validation rule); the `;` only ever ends a statement.

- [ ] **Step 4: Register migrations in EXPECTED_MIGRATIONS (incl. the pre-existing 105/106 gap)**

**Pre-existing state:** `test_clickhouse_migration_files_are_explicit` is ALREADY FAILING on the current branch — the prior build's `000105`/`000106` DNS distinct-model migration files exist on disk but were never added to `EXPECTED_MIGRATIONS` (its last entry is `000104`). To leave the test green, register all four migrations (the two pre-existing + the two new), in numeric order.

In `corpscout/dagster_v3/tests/test_clickhouse_migrations.py`, the `EXPECTED_MIGRATIONS` tuple currently ends:

```python
    "000103_corpscout_br_pgfn_company_debts",
    "000104_corpscout_br_cgu_sanctions",
)
```

Change the end to:

```python
    "000103_corpscout_br_pgfn_company_debts",
    "000104_corpscout_br_cgu_sanctions",
    "000105_corpscout_commoncrawl_domain_dns_records_distinct",
    "000106_corpscout_commoncrawl_domain_dns_scan_latest",
    "000107_corpscout_commoncrawl_domain_dns_records_source",
    "000108_corpscout_commoncrawl_domain_dns_scan_axfr",
)
```

(105/106 are real, already-merged migrations; registering them is a correct reconciliation of a pre-existing omission, not new schema. If their exact filenames differ from the above, use the on-disk names — `ls corpscout/clickhouse/migrations/000105* 000106*` and strip the `.up.sql`/`.down.sql`.)

- [ ] **Step 5: Run the migration-validation tests**

Run: `cd corpscout/dagster_v3 && timeout 200 uv run pytest tests/test_clickhouse_migrations.py -q`
Expected: PASS — `test_clickhouse_migration_files_are_explicit` now green (it was RED before this task due to the 105/106 gap), plus `test_clickhouse_migrations_have_down_files`, `test_clickhouse_migration_line_comments_do_not_contain_semicolons`, and the create/alter/drop structural checks (which loop over EXPECTED_MIGRATIONS — so they now also validate 105–108).

If any test requires a live ClickHouse and none is configured, run at least the static file-structure/down-file/comment checks and note in the report which were skipped for lack of infra. Report the before/after of the explicit-files test explicitly (it should flip red→green).

- [ ] **Step 6: Commit**

```bash
git add corpscout/clickhouse/migrations/000107_* corpscout/clickhouse/migrations/000108_* corpscout/dagster_v3/tests/test_clickhouse_migrations.py
git commit -m "feat(dns): migrations for records.source + scan axfr_* columns (+axfr_server)"
```

---

## Task 2: Wire `axfr_server` through the Go worker

**Files:**
- Modify: `corpscout/commoncrawl/cc-dns-worker/internal/model/model.go` (`DomainResult`, `ScanRow`)
- Modify: `corpscout/commoncrawl/cc-dns-worker/internal/store/store.go` (schema, migrate, CommitBatch, StagedDomains, SummariesFor)
- Modify: `corpscout/commoncrawl/cc-dns-worker/cmd/cc-dns-worker/scan.go` (`mergeAXFR`)
- Modify: `corpscout/commoncrawl/cc-dns-worker/internal/load/load_test.go` (ScanRow count 14→15)
- Test: `corpscout/commoncrawl/cc-dns-worker/internal/store/store_test.go`

**Interfaces:**
- Consumes: `resolve.AXFRResult.Server` (already exists — the NS IP that answered).
- Produces: `model.DomainResult.AXFRServer string`; `model.ScanRow.AXFRServer string` with `ch:"axfr_server"`. Persisted so the scan summary records the answering IP.

- [ ] **Step 1: Write the failing test**

Add to `internal/store/store_test.go` (round-trip `axfr_server` through CommitBatch → StagedDomains). Model it on the existing `TestSummaryPersistsAXFRFlags`:

```go
func TestSummaryPersistsAXFRServer(t *testing.T) {
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
		AXFROpen: true, AXFRServer: "203.0.113.9",
	}
	if err := st.CommitBatch(ctx, []model.DomainResult{res}); err != nil {
		t.Fatal(err)
	}
	rows, err := st.StagedDomains(ctx, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].AXFRServer != "203.0.113.9" {
		t.Fatalf("axfr_server not round-tripped: %+v", rows)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd corpscout/commoncrawl/cc-dns-worker && go test ./internal/store/ -run TestSummaryPersistsAXFRServer`
Expected: FAIL — `AXFRServer` undefined on `DomainResult`/`ScanRow` (compile error).

- [ ] **Step 3: Add the model fields**

In `internal/model/model.go`, add to `DomainResult` after `AXFRTruncated`:

```go
	AXFRTruncated bool
	AXFRServer    string
```

Add to `ScanRow` after `AXFRTruncated` (appended at END — declaration order = CH INSERT column order):

```go
	AXFRTruncated uint8     `ch:"axfr_truncated"`
	AXFRServer    string    `ch:"axfr_server"`
```

- [ ] **Step 4: Wire the SQLite column + read/write**

In `internal/store/store.go`:

Add to the `scan_domains` table in the `schema` const (after `axfr_truncated`):

```go
  axfr_truncated INTEGER DEFAULT 0,
  axfr_server    TEXT DEFAULT '',
```

Add to the `migrate()` ALTER list:

```go
		`ALTER TABLE scan_domains ADD COLUMN axfr_server TEXT DEFAULT ''`,
```

In `CommitBatch`, add `axfr_server=?` to the `upD` UPDATE SET clause (after `axfr_truncated=?`) and pass `res.AXFRServer` in the matching position of the `ExecContext` args (after `b2i(res.AXFRTruncated)`):

```go
		queries_total=?, queries_ok=?, axfr_open=?, axfr_records=?, axfr_truncated=?, axfr_server=?, error=?, source_run_id=?, resolved_at=?
```
```go
			b2i(res.AXFROpen), res.AXFRRecords, b2i(res.AXFRTruncated), res.AXFRServer,
```

In `StagedDomains` and `SummariesFor`, add `axfr_server` to BOTH SELECT column lists (after `axfr_truncated`) and scan it into `&r.AXFRServer` in the matching position of BOTH `Scan` calls. The two functions share an identical column list — change both.

- [ ] **Step 5: Set `AXFRServer` in `mergeAXFR`**

In `cmd/cc-dns-worker/scan.go`, add to `mergeAXFR`:

```go
func mergeAXFR(res *model.DomainResult, a resolve.AXFRResult) {
	res.AXFROpen = a.Open
	res.AXFRRecords = a.Records
	res.AXFRTruncated = a.Truncated
	res.AXFRServer = a.Server
	res.Records = append(res.Records, a.Zone...)
}
```

- [ ] **Step 6: Update the column-count test**

In `internal/load/load_test.go`, change the ScanRow assertion from `len(sc) != 14` to `len(sc) != 15` (adding `axfr_server`). Leave the RecordRow assertion at `13`.

- [ ] **Step 7: Run tests**

Run: `cd corpscout/commoncrawl/cc-dns-worker && go test ./internal/store/ ./internal/load/ && go build ./... && go test ./...`
Expected: PASS — new test green, `TestColumnLists` green (15), whole suite green.

- [ ] **Step 8: Vet, fmt, commit**

```bash
cd corpscout/commoncrawl/cc-dns-worker && go fmt ./... && go vet ./...
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/commoncrawl/cc-dns-worker/internal/model/model.go corpscout/commoncrawl/cc-dns-worker/internal/store/store.go corpscout/commoncrawl/cc-dns-worker/internal/store/store_test.go corpscout/commoncrawl/cc-dns-worker/internal/load/load_test.go corpscout/commoncrawl/cc-dns-worker/cmd/cc-dns-worker/scan.go
git commit -m "feat(dns): persist axfr_server (answering NS IP) on the scan summary"
```

---

## Task 3: Fix the cc-dns-worker README ORDER BY + point at the migrations

**Files:**
- Modify: `corpscout/commoncrawl/cc-dns-worker/README.md`

**Interfaces:** none (docs only).

- [ ] **Step 1: Fix the stale records ORDER BY**

In `corpscout/commoncrawl/cc-dns-worker/README.md`, the records table currently documents `ORDER BY (root_domain, scan_id, record_type, name, value, source)` and the runbook DDL uses the same stale key. Replace both occurrences with the correct live key:

`ORDER BY (root_domain, record_type, slot, name, value, source)`

- [ ] **Step 2: Replace the DDL runbook with a migration pointer + add axfr_server**

In the "Schema migrations for AXFR support" section, replace the inline `ALTER`/`MODIFY ORDER BY` runbook with a pointer to the checked-in migrations, and document the new `axfr_server` column on the scan-summary table:

> The AXFR/provenance ClickHouse schema changes are checked-in migrations, not a manual runbook: `../../clickhouse/migrations/000107_*_dns_records_source.*` (adds `source` + re-key) and `000108_*_dns_scan_axfr.*` (adds `axfr_open`, `axfr_records`, `axfr_truncated`, `axfr_server`). Apply them with the project's ClickHouse migration tooling before deploying a build of this worker — the load path's INSERT column list is derived from the struct tags, so a load against a table missing these columns fails at `PrepareBatch` regardless of the `--axfr` flag.

Add `axfr_server | String | the NS IP that answered the zone transfer (`''` when closed)` to the `commoncrawl_domain_dns_scan` column table.

- [ ] **Step 3: Commit**

```bash
git add corpscout/commoncrawl/cc-dns-worker/README.md
git commit -m "docs(dns): correct records ORDER BY and point to AXFR migrations"
```

---

## Self-Review

**Spec coverage:** Phase 0 in the spec = migrations 1 (records source + re-key) & 3 (scan axfr_* + axfr_server) and wiring `axfr_server` through the store. Task 1 = the two migrations + EXPECTED_MIGRATIONS. Task 2 = `axfr_server` Go plumbing. Task 3 = README correction (the spec notes the merged README's key is wrong). Migrations 2 (`discovery`) and 4 (registry) are Phase 1 — correctly NOT here. ✓

**Placeholder scan:** No TBD/TODO; every migration and code step has literal content. ✓

**Type consistency:** `AXFRServer` is `string` on `DomainResult` and `string` with `ch:"axfr_server"` on `ScanRow`; set from `AXFRResult.Server` (string) in `mergeAXFR`; persisted as SQLite `TEXT`. `ScanRow` column count goes 14→15 (RecordRow stays 13). The records re-key `(root_domain, record_type, slot, name, value, source)` matches the live 000105 table plus `source`. ✓
