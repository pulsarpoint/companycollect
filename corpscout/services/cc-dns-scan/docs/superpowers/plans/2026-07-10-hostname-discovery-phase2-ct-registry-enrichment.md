# Hostname Discovery — Phase 2: CT + Registry Seed Enrichment + Plan Union

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Before scanning, augment each domain's hostname list with CT-log and registry-discovered subdomains, so the worker resolves A/AAAA for real public + durable-internal hosts — not just the 5 static guesses.

**Architecture:** A new **host-load phase** runs after the domain seed and before dispatch (gated by `--host-enrich`): it iterates seeded domains in batches, queries `ctlogs.hostnames` (CT) and `commoncrawl_domain_hostnames` (the Phase 1 registry) scoped to each batch, merges them (dedupe, `axfr>ct` precedence, cap 100), and writes labels into a new SQLite `scan_hostnames` table. At scan time the feeder bulk-loads each dispatch batch's labels and `records.Plan` unions them with the static list (A+AAAA), tagging each resulting record with its `discovery` source. This is Phase 2 of the [hostname-discovery spec](../../../../docs/hostname-discovery-spec.md) (§3, §4 union side). No new migration (scan_hostnames is SQLite; CT + registry tables already exist).

**Tech Stack:** Go 1.25, ClickHouse (`ctlogs.hostnames` 1.9B rows + `commoncrawl_domain_hostnames`), SQLite stage.

**Spec:** `corpscout/commoncrawl/docs/hostname-discovery-spec.md` (§3, §4)

## Global Constraints

- Go 1.25 module. `go fmt ./...` + `go vet ./...` before commits; tests stdlib `testing` only (no testify); DO NOT run `go mod tidy`. Revert incidental gofmt of unrelated pre-existing files (internal/metrics/*.go, internal/records/plan.go if untouched by the task).
- Feature ships dark: `--host-enrich` defaults **false**; when off, no host-load phase runs, the work channel carries no extra labels, and `records.Plan` behaves exactly as today.
- Per-domain cap `--host-cap` (default 100) applies to the **union** of CT + registry (not per source). Each CT hostname adds A+AAAA (2 queries); worst case +200/domain.
- CT reads are index-pruned: `WHERE registered_domain IN (batch) … LIMIT 100 BY registered_domain` on the sort-key prefix. Registry reads use `GROUP BY root_domain,label` (AggregatingMergeTree) then `LIMIT 100 BY root_domain`.
- Label normalization: `fqdn` minus `.<root_domain>`, lowercased; skip apex (`fqdn == root_domain`), non-subdomains, wildcards (label containing `*`), and empty labels — same rules as the Phase 1 write-back.
- Shared type `model.HostLabel{Label, DiscoverySource string, LiveCert bool}` avoids import cycles (`model` imports nothing; `records`/`store`/`hostsource` may import `model`).
- Conventional Commits. Git root `/Users/graovic/pulsarpoint/ppoint/companycollect`.

## File Structure

- `internal/model/model.go` — `HostLabel` type.
- `internal/store/store.go` — `scan_hostnames` table + `host_load_state` cursor + methods.
- `internal/hostsource/hostsource.go` (new) — CT + registry ClickHouse queries + `Merge` + label normalization.
- `internal/hostsource/hostsource_test.go` (new) — normalize + merge tests.
- `internal/records/plan.go` — `Query.Discovery`, `Plan(domain, cfg, extra)` union.
- `internal/resolve/query.go` — `collect` uses `q.Discovery`.
- `cmd/cc-dns-worker/scan.go` — flags, host-load phase, feeder `domainWork` threading, `resolveDomain` passes labels.

---

## Task 1: `scan_hostnames` SQLite table + store methods

**Files:**
- Modify: `internal/model/model.go` (`HostLabel`)
- Modify: `internal/store/store.go` (schema, migrate, methods)
- Test: `internal/store/store_test.go`

**Interfaces:**
- Produces: `model.HostLabel{Label, DiscoverySource string, LiveCert bool}`. Store methods: `SeededDomainsAfter(ctx, scanID, cursor string, limit int) ([]string, error)` (batch-iterate the seed queue), `InsertHostnames(ctx, scanID, rootDomain string, hosts []model.HostLabel) error`, `HostnamesForBatch(ctx, scanID string, domains []string) (map[string][]model.HostLabel, error)` (bulk-load for a dispatch batch), `HostLoadComplete`/`MarkHostLoadComplete`, `HostLoadCursor`/`SetHostLoadCursor`.

- [ ] **Step 1: `HostLabel` model**

In `internal/model/model.go`:

```go
// HostLabel is one discovered subdomain label to scan for a domain (from CT, the registry, or a
// zone transfer). Label is the name minus ".<root_domain>", lowercased. DiscoverySource is how it
// was found (ct|axfr|static); LiveCert is true when a CT source had a still-valid certificate.
type HostLabel struct {
	Label           string
	DiscoverySource string
	LiveCert        bool
}
```

- [ ] **Step 2: Failing test (round-trip a domain's labels)**

Add to `internal/store/store_test.go`:

```go
func TestHostnamesRoundTrip(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	if err := st.InsertHostnames(ctx, "s1", "example.com", []model.HostLabel{
		{Label: "jenkins", DiscoverySource: "axfr", LiveCert: false},
		{Label: "api", DiscoverySource: "ct", LiveCert: true},
	}); err != nil {
		t.Fatal(err)
	}
	m, err := st.HostnamesForBatch(ctx, "s1", []string{"example.com", "other.com"})
	if err != nil {
		t.Fatal(err)
	}
	if len(m["example.com"]) != 2 {
		t.Fatalf("want 2 labels for example.com, got %d", len(m["example.com"]))
	}
	got := map[string]string{}
	for _, h := range m["example.com"] {
		got[h.Label] = h.DiscoverySource
	}
	if got["jenkins"] != "axfr" || got["api"] != "ct" {
		t.Fatalf("labels not round-tripped: %+v", got)
	}
}
```

- [ ] **Step 3: Run to verify it fails**

Run: `cd corpscout/commoncrawl/cc-dns-worker && go test ./internal/store/ -run TestHostnamesRoundTrip`
Expected: FAIL — `InsertHostnames`/`HostnamesForBatch` undefined.

- [ ] **Step 4: Schema + migrate**

In `internal/store/store.go`, add to the `schema` const:

```sql
CREATE TABLE IF NOT EXISTS scan_hostnames (
  scan_id          TEXT NOT NULL,
  root_domain      TEXT NOT NULL,
  label            TEXT NOT NULL,
  discovery_source TEXT NOT NULL DEFAULT 'ct',
  live_cert        INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (scan_id, root_domain, label)
);
CREATE TABLE IF NOT EXISTS host_load_state (
  scan_id  TEXT PRIMARY KEY,
  cursor   TEXT NOT NULL DEFAULT '',
  complete INTEGER NOT NULL DEFAULT 0
);
```

(Both are `CREATE TABLE IF NOT EXISTS` in the schema const — new-DB safe. No `migrate()` ALTER needed since these are whole new tables the const creates on every `Open`.)

- [ ] **Step 5: Implement the methods**

Add to `internal/store/store.go`:

```go
// SeededDomainsAfter returns up to limit seeded root_domains for scanID greater than afterRootDomain,
// ordered — the cursor iterator the host-load phase walks (mirrors PendingBatch, but over ALL seeded
// domains regardless of status).
func (s *Store) SeededDomainsAfter(ctx context.Context, scanID, afterRootDomain string, limit int) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT root_domain FROM scan_domains WHERE scan_id = ? AND root_domain > ? ORDER BY root_domain LIMIT ?`,
		scanID, afterRootDomain, limit)
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

// InsertHostnames writes a domain's discovered labels for scanID (idempotent — INSERT OR IGNORE on the
// (scan_id, root_domain, label) primary key).
func (s *Store) InsertHostnames(ctx context.Context, scanID, rootDomain string, hosts []model.HostLabel) error {
	if len(hosts) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `INSERT OR IGNORE INTO scan_hostnames
		(scan_id, root_domain, label, discovery_source, live_cert) VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, h := range hosts {
		if _, err := stmt.ExecContext(ctx, scanID, rootDomain, h.Label, h.DiscoverySource, b2i(h.LiveCert)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// HostnamesForBatch bulk-loads the labels for a set of domains in one query (the feeder calls it once
// per dispatch batch, not per worker) → map[root_domain][]HostLabel.
func (s *Store) HostnamesForBatch(ctx context.Context, scanID string, domains []string) (map[string][]model.HostLabel, error) {
	if len(domains) == 0 {
		return nil, nil
	}
	ph := strings.Repeat("?,", len(domains))
	ph = ph[:len(ph)-1]
	args := make([]any, 0, len(domains)+1)
	args = append(args, scanID)
	for _, d := range domains {
		args = append(args, d)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT root_domain, label, discovery_source, live_cert
		FROM scan_hostnames WHERE scan_id = ? AND root_domain IN (`+ph+`)`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string][]model.HostLabel{}
	for rows.Next() {
		var rd string
		var h model.HostLabel
		var lc int
		if err := rows.Scan(&rd, &h.Label, &h.DiscoverySource, &lc); err != nil {
			return nil, err
		}
		h.LiveCert = lc != 0
		out[rd] = append(out[rd], h)
	}
	return out, rows.Err()
}

// HostLoadComplete reports whether the host-load phase finished for scanID (resume skips it).
func (s *Store) HostLoadComplete(ctx context.Context, scanID string) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT complete FROM host_load_state WHERE scan_id = ?`, scanID).Scan(&n)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return n == 1, err
}

// MarkHostLoadComplete records that the host-load phase finished for scanID.
func (s *Store) MarkHostLoadComplete(ctx context.Context, scanID string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO host_load_state (scan_id, complete) VALUES (?, 1)
		 ON CONFLICT(scan_id) DO UPDATE SET complete = 1`, scanID)
	return err
}

// HostLoadCursor returns the last root_domain the host-load phase processed for scanID ("" if none).
func (s *Store) HostLoadCursor(ctx context.Context, scanID string) (string, error) {
	var c string
	err := s.db.QueryRowContext(ctx, `SELECT cursor FROM host_load_state WHERE scan_id = ?`, scanID).Scan(&c)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return c, err
}

// SetHostLoadCursor advances the host-load cursor for scanID.
func (s *Store) SetHostLoadCursor(ctx context.Context, scanID, cursor string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO host_load_state (scan_id, cursor) VALUES (?, ?)
		 ON CONFLICT(scan_id) DO UPDATE SET cursor = excluded.cursor`, scanID, cursor)
	return err
}
```

- [ ] **Step 6: Run tests**

Run: `cd corpscout/commoncrawl/cc-dns-worker && go test ./internal/store/ && go build ./...`
Expected: PASS.

- [ ] **Step 7: Vet, fmt, commit**

```bash
cd corpscout/commoncrawl/cc-dns-worker && go fmt ./... && go vet ./...
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/commoncrawl/cc-dns-worker/internal/model/model.go corpscout/commoncrawl/cc-dns-worker/internal/store/store.go corpscout/commoncrawl/cc-dns-worker/internal/store/store_test.go
git commit -m "feat(dns): scan_hostnames SQLite table + host-load cursor/methods"
```

---

## Task 2: `hostsource` package — CT + registry queries + merge

**Files:**
- Create: `internal/hostsource/hostsource.go`
- Create: `internal/hostsource/hostsource_test.go`

**Interfaces:**
- Consumes: `model.HostLabel`; a ClickHouse `driver.Conn`.
- Produces: `func CTHostnames(ctx, conn, domains []string, cap int) (map[string][]model.HostLabel, error)`; `func RegistryHostnames(ctx, conn, domains []string, cap int) (map[string][]model.HostLabel, error)`; `func Merge(ct, reg map[string][]model.HostLabel, cap int) map[string][]model.HostLabel`; `func NormalizeLabel(rootDomain, fqdn string) (string, bool)`.

- [ ] **Step 1: Failing tests (normalize + merge — pure, no ClickHouse)**

Create `internal/hostsource/hostsource_test.go`:

```go
package hostsource

import (
	"testing"

	"cc-dns-worker/internal/model"
)

func TestNormalizeLabel(t *testing.T) {
	cases := []struct {
		rd, fqdn, want string
		ok             bool
	}{
		{"example.com", "mail.example.com", "mail", true},
		{"example.com", "a.b.example.com", "a.b", true},
		{"example.com", "MAIL.example.com", "mail", true},
		{"example.com", "example.com", "", false},   // apex
		{"example.com", "*.example.com", "", false},  // wildcard
		{"example.com", "other.org", "", false},      // not a subdomain
	}
	for _, c := range cases {
		got, ok := NormalizeLabel(c.rd, c.fqdn)
		if ok != c.ok || got != c.want {
			t.Errorf("NormalizeLabel(%q,%q) = (%q,%v), want (%q,%v)", c.rd, c.fqdn, got, ok, c.want, c.ok)
		}
	}
}

func TestMerge(t *testing.T) {
	ct := map[string][]model.HostLabel{"e.com": {
		{Label: "www", DiscoverySource: "ct", LiveCert: true},
		{Label: "api", DiscoverySource: "ct", LiveCert: true},
	}}
	reg := map[string][]model.HostLabel{"e.com": {
		{Label: "api", DiscoverySource: "axfr"}, // dup of ct api — axfr precedence wins
		{Label: "vpn", DiscoverySource: "axfr"},
	}}
	got := Merge(ct, reg, 100)["e.com"]
	by := map[string]string{}
	for _, h := range got {
		by[h.Label] = h.DiscoverySource
	}
	if len(got) != 3 || by["www"] != "ct" || by["api"] != "axfr" || by["vpn"] != "axfr" {
		t.Fatalf("merge wrong: %+v", by)
	}
	// cap
	capped := Merge(ct, reg, 2)["e.com"]
	if len(capped) != 2 {
		t.Fatalf("cap 2 not applied: got %d", len(capped))
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd corpscout/commoncrawl/cc-dns-worker && go test ./internal/hostsource/`
Expected: FAIL — package/functions undefined.

- [ ] **Step 3: Implement `hostsource.go`**

NOTE: rename the `cap` parameter to `capN` throughout (CTHostnames/RegistryHostnames/Merge) — `cap` shadows the Go builtin. The code below uses `cap` for readability against the spec; use `capN` in the actual file, and use `strconv.Itoa(capN)` directly in the `LIMIT … BY` string instead of the `itoa` wrapper. Drop the `sort` import (unused). Create `internal/hostsource/hostsource.go`:

```go
// Package hostsource loads discovered subdomain labels for a batch of domains from two ClickHouse
// sources — Certificate Transparency (ctlogs.hostnames) and the durable registry
// (commoncrawl_domain_hostnames) — and merges them into a capped per-domain set for scanning.
package hostsource

import (
	"context"
	"sort"
	"strings"

	"cc-dns-worker/internal/model"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// NormalizeLabel turns an fqdn into a scan label relative to rootDomain: strips the ".<rootDomain>"
// suffix and lowercases. Returns ok=false for the apex, non-subdomains, wildcards, or an empty label.
func NormalizeLabel(rootDomain, fqdn string) (string, bool) {
	fqdn = strings.ToLower(strings.TrimSuffix(fqdn, "."))
	rootDomain = strings.ToLower(rootDomain)
	suffix := "." + rootDomain
	if fqdn == rootDomain || !strings.HasSuffix(fqdn, suffix) {
		return "", false
	}
	label := strings.TrimSuffix(fqdn, suffix)
	if label == "" || strings.Contains(label, "*") {
		return "", false
	}
	return label, true
}

func inClause(n int) string {
	if n == 0 {
		return ""
	}
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

func toArgs(domains []string) []any {
	a := make([]any, len(domains))
	for i, d := range domains {
		a[i] = d
	}
	return a
}

// CTHostnames returns up to cap live-cert-first, recency-ranked non-wildcard labels per domain from
// Certificate Transparency, scoped to domains (index-pruned on the ctlogs sort key).
func CTHostnames(ctx context.Context, conn driver.Conn, domains []string, cap int) (map[string][]model.HostLabel, error) {
	if len(domains) == 0 {
		return map[string][]model.HostLabel{}, nil
	}
	q := `SELECT registered_domain, fqdn, (lna >= now()) AS live FROM (
	    SELECT registered_domain, fqdn, max(is_wildcard) is_wc, max(last_seen) ls, max(last_not_after) lna
	    FROM ctlogs.hostnames WHERE registered_domain IN (` + inClause(len(domains)) + `)
	    GROUP BY registered_domain, fqdn
	) WHERE is_wc = 0 AND fqdn != registered_domain
	ORDER BY registered_domain, (lna >= now()) DESC, ls DESC
	LIMIT ` + itoa(cap) + ` BY registered_domain`
	rows, err := conn.Query(ctx, q, toArgs(domains)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string][]model.HostLabel{}
	for rows.Next() {
		var rd, fqdn string
		var live uint8
		if err := rows.Scan(&rd, &fqdn, &live); err != nil {
			return nil, err
		}
		if label, ok := NormalizeLabel(rd, fqdn); ok {
			out[rd] = append(out[rd], model.HostLabel{Label: label, DiscoverySource: "ct", LiveCert: live != 0})
		}
	}
	return out, rows.Err()
}

// RegistryHostnames returns up to cap recency-ranked labels per domain from the durable registry,
// carrying each label's stored discovery_source (axfr precedence via min).
func RegistryHostnames(ctx context.Context, conn driver.Conn, domains []string, cap int) (map[string][]model.HostLabel, error) {
	if len(domains) == 0 {
		return map[string][]model.HostLabel{}, nil
	}
	q := `SELECT root_domain, label, min(discovery_source) AS ds FROM corpscout.commoncrawl_domain_hostnames
	    WHERE root_domain IN (` + inClause(len(domains)) + `)
	    GROUP BY root_domain, label
	    ORDER BY root_domain, max(last_seen) DESC
	    LIMIT ` + itoa(cap) + ` BY root_domain`
	rows, err := conn.Query(ctx, q, toArgs(domains)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string][]model.HostLabel{}
	for rows.Next() {
		var rd string
		var h model.HostLabel
		if err := rows.Scan(&rd, &h.Label, &h.DiscoverySource); err != nil {
			return nil, err
		}
		out[rd] = append(out[rd], h)
	}
	return out, rows.Err()
}

// Merge unions the CT and registry labels per domain, deduping by label with discovery-source
// precedence axfr > ct > static (min), and caps each domain to cap by keeping CT (live-first) then
// registry (recency) order. LiveCert is preserved from whichever source had it true.
func Merge(ct, reg map[string][]model.HostLabel, cap int) map[string][]model.HostLabel {
	out := map[string][]model.HostLabel{}
	domains := map[string]struct{}{}
	for d := range ct {
		domains[d] = struct{}{}
	}
	for d := range reg {
		domains[d] = struct{}{}
	}
	for d := range domains {
		seen := map[string]int{} // label -> index in merged
		var merged []model.HostLabel
		add := func(h model.HostLabel) {
			if i, ok := seen[h.Label]; ok {
				if minSource(h.DiscoverySource, merged[i].DiscoverySource) == h.DiscoverySource {
					merged[i].DiscoverySource = h.DiscoverySource
				}
				if h.LiveCert {
					merged[i].LiveCert = true
				}
				return
			}
			seen[h.Label] = len(merged)
			merged = append(merged, h)
		}
		for _, h := range ct[d] { // CT first (live-first order)
			add(h)
		}
		for _, h := range reg[d] { // then registry (recency)
			add(h)
		}
		if cap > 0 && len(merged) > cap {
			merged = merged[:cap]
		}
		out[d] = merged
	}
	return out
}

// minSource returns the alphabetically-smaller discovery source (axfr < ct < static — axfr precedence).
func minSource(a, b string) string {
	if a < b {
		return a
	}
	return b
}

func itoa(n int) string { return strconv.Itoa(n) }
```

Add `"strconv"` to the imports (used by `itoa`; `sort` is imported but only used if you prefer a sorted domain iteration — if `go vet`/compiler flags `sort` as unused, remove it). Run `go build` and drop any unused import the compiler reports.

- [ ] **Step 4: Run tests**

Run: `cd corpscout/commoncrawl/cc-dns-worker && go test ./internal/hostsource/`
Expected: PASS (normalize + merge). Note in the report: the CT/registry SQL is validated live in Task 3's rollout, not unit-tested against the 1.9B-row table.

- [ ] **Step 5: Vet, fmt, commit**

```bash
cd corpscout/commoncrawl/cc-dns-worker && go fmt ./... && go vet ./...
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/commoncrawl/cc-dns-worker/internal/hostsource/
git commit -m "feat(dns): hostsource — CT + registry hostname queries and capped merge"
```

---

## Task 3: Host-load phase + flags

**Files:**
- Modify: `cmd/cc-dns-worker/scan.go` (`scanConfig`, `scanFlags`, `scanCycle`)

**Interfaces:**
- Consumes: `store.SeededDomainsAfter`/`InsertHostnames`/`HostLoadComplete`/`MarkHostLoadComplete`/`HostLoadCursor`/`SetHostLoadCursor`; `hostsource.CTHostnames`/`RegistryHostnames`/`Merge`; `chConn()`.
- Produces: a populated `scan_hostnames` for the scan-id when `--host-enrich` is on.

- [ ] **Step 1: Add flags to `scanConfig`/`scanFlags`**

In `scan.go` `scanConfig`, after the axfr fields:

```go
	hostEnrich bool
	hostCap    int
	hostBatch  int
```

In `scanFlags`, register + copy into the returned literal:

```go
	hostEnrich := fs.Bool("host-enrich", false, "enable CT + registry hostname enrichment (seed-time) — master switch, default off")
	hostCap := fs.Int("host-cap", 100, "max discovered hosts per domain unioned into the scan")
	hostBatch := fs.Int("host-batch", 5000, "domains per ClickHouse hostname lookup batch")
```
```go
			hostEnrich: *hostEnrich, hostCap: *hostCap, hostBatch: *hostBatch,
```

- [ ] **Step 2: Add the host-load phase in `scanCycle`**

After the seed block (after the `if seeded { … } else { … }` that ends ~line 165) and before `stats := &metrics.Stats{}`, insert:

```go
	// 1b) Host-load phase: union CT + registry discovered hostnames into scan_hostnames (resumable,
	// skipped on resume or when disabled). Runs before dispatch so the plan can consume it.
	if cfg.hostEnrich {
		if err := hostLoadPhase(ctx, st, cfg); err != nil {
			return err
		}
	}
```

Add the function (near `scanCycle`):

```go
// hostLoadPhase populates scan_hostnames for cfg.scanID from CT (ctlogs.hostnames) and the registry
// (commoncrawl_domain_hostnames), in cursor batches of cfg.hostBatch, capped at cfg.hostCap per domain.
// Resumable via host_load_state; idempotent (InsertHostnames uses INSERT OR IGNORE).
func hostLoadPhase(ctx context.Context, st *store.Store, cfg scanConfig) error {
	done, err := st.HostLoadComplete(ctx, cfg.scanID)
	if err != nil {
		return err
	}
	if done {
		log.Printf("scan_id=%s: host-load already complete — skipping", cfg.scanID)
		return nil
	}
	cursor, err := st.HostLoadCursor(ctx, cfg.scanID)
	if err != nil {
		return err
	}
	conn, err := chConn()
	if err != nil {
		return err
	}
	defer conn.Close()
	batchN := cfg.hostBatch
	if batchN <= 0 {
		batchN = 5000
	}
	total := 0
	for {
		domains, err := st.SeededDomainsAfter(ctx, cfg.scanID, cursor, batchN)
		if err != nil {
			return err
		}
		if len(domains) == 0 {
			break
		}
		ct, err := hostsource.CTHostnames(ctx, conn, domains, cfg.hostCap)
		if err != nil {
			return fmt.Errorf("CT hostnames: %w", err)
		}
		reg, err := hostsource.RegistryHostnames(ctx, conn, domains, cfg.hostCap)
		if err != nil {
			return fmt.Errorf("registry hostnames: %w", err)
		}
		merged := hostsource.Merge(ct, reg, cfg.hostCap)
		for _, d := range domains {
			if hosts := merged[d]; len(hosts) > 0 {
				if err := st.InsertHostnames(ctx, cfg.scanID, d, hosts); err != nil {
					return err
				}
				total += len(hosts)
			}
		}
		cursor = domains[len(domains)-1]
		if err := st.SetHostLoadCursor(ctx, cfg.scanID, cursor); err != nil {
			return err
		}
	}
	if err := st.MarkHostLoadComplete(ctx, cfg.scanID); err != nil {
		return err
	}
	log.Printf("scan_id=%s: host-load complete (%d discovered hostnames)", cfg.scanID, total)
	return nil
}
```

Add `"cc-dns-worker/internal/hostsource"` to `scan.go` imports.

- [ ] **Step 3: Build + vet**

Run: `cd corpscout/commoncrawl/cc-dns-worker && go build ./... && go vet ./... && go test ./...`
Expected: PASS (compiles; host-load phase only runs under `--host-enrich`, so existing tests unaffected).

- [ ] **Step 4: Commit**

```bash
cd corpscout/commoncrawl/cc-dns-worker && go fmt ./...
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/commoncrawl/cc-dns-worker/cmd/cc-dns-worker/scan.go
git commit -m "feat(dns): seed-time host-load phase (CT + registry -> scan_hostnames), default-off"
```

---

## Task 4: `records.Plan` union + `Query.Discovery` + `collect`

**Files:**
- Modify: `internal/records/plan.go` (`Query`, `Plan`)
- Modify: `internal/resolve/query.go` (`collect`)
- Test: `internal/records/plan_test.go`

**Interfaces:**
- Consumes: `model.HostLabel`.
- Produces: `records.Query.Discovery string`; `records.Plan(domain string, cfg Config, extra []model.HostLabel) []Query` (new trailing param — union extra hosts, deduped against the static set, each A+AAAA tagged with its `DiscoverySource`).

- [ ] **Step 1: Failing test**

Add to `internal/records/plan_test.go`:

```go
func TestPlanUnionsExtraHosts(t *testing.T) {
	cfg := DefaultConfig()
	extra := []model.HostLabel{
		{Label: "jenkins", DiscoverySource: "axfr"},
		{Label: "www", DiscoverySource: "ct"}, // dup of a static hostname — must NOT double-query
	}
	qs := Plan("example.com", cfg, extra)
	var jenkinsA, wwwStatic int
	for _, q := range qs {
		if q.Name == "jenkins.example.com." && q.Type == dns.TypeA {
			jenkinsA++
			if q.Discovery != "axfr" {
				t.Errorf("jenkins A discovery = %q, want axfr", q.Discovery)
			}
		}
		if q.Name == "www.example.com." && q.Type == dns.TypeA {
			wwwStatic++
			if q.Discovery != "static" {
				t.Errorf("www A discovery = %q, want static (static wins the overlap)", q.Discovery)
			}
		}
	}
	if jenkinsA != 1 {
		t.Errorf("want exactly 1 jenkins A query, got %d", jenkinsA)
	}
	if wwwStatic != 1 {
		t.Errorf("want exactly 1 www A query (deduped), got %d", wwwStatic)
	}
}
```

(Add `"cc-dns-worker/internal/model"` and confirm `"github.com/miekg/dns"` imports in the test.)

- [ ] **Step 2: Run to verify it fails**

Run: `cd corpscout/commoncrawl/cc-dns-worker && go test ./internal/records/ -run TestPlanUnionsExtraHosts`
Expected: FAIL — `Plan` takes 2 args / `Query.Discovery` undefined.

- [ ] **Step 3: Add `Query.Discovery` and set it on the built-in queries**

In `internal/records/plan.go`, add `Discovery string` to `Query` (doc: `"static" | "ct" | "axfr"`). Every built-in query the current `Plan` builds gets `Discovery: "static"` (the apex A/AAAA/MX/TXT/NS/SOA/CAA/DNSKEY/HTTPS, the `_dmarc`/`_mta-sts`/etc slots, the static `Hostnames`, DKIM selectors, SRV services). The simplest way: build the plan as today, then set `Discovery: "static"` on each element before the extra-host union (a single loop `for i := range qs { qs[i].Discovery = "static" }`), OR set it inline in each literal. Use the single loop for brevity.

- [ ] **Step 4: Change `Plan`'s signature and union the extra hosts**

Change `func Plan(domain string, cfg Config) []Query` to `func Plan(domain string, cfg Config, extra []model.HostLabel) []Query`. After building the static `qs` (and stamping `Discovery: "static"`), union the extras, deduped against the static hostname labels (case-insensitive):

```go
	// Union discovered hosts (CT/registry/axfr), skipping any label already covered by the static set,
	// so we never double-query. Each gets A+AAAA tagged with its discovery source.
	staticLabels := map[string]bool{}
	for _, h := range cfg.Hostnames {
		staticLabels[strings.ToLower(h)] = true
	}
	for _, e := range extra {
		l := strings.ToLower(e.Label)
		if l == "" || staticLabels[l] {
			continue
		}
		staticLabels[l] = true // also dedupe extras against each other
		hn := dns.Fqdn(l + "." + domain)
		qs = append(qs,
			Query{Name: hn, Type: dns.TypeA, Slot: l, Discovery: e.DiscoverySource},
			Query{Name: hn, Type: dns.TypeAAAA, Slot: l, Discovery: e.DiscoverySource})
	}
	return qs
```

Add `"strings"` and `"cc-dns-worker/internal/model"` to `plan.go` imports.

- [ ] **Step 5: `collect` uses `q.Discovery`**

In `internal/resolve/query.go` `collect`, change the record construction from hardcoded `Discovery: "static"` to the query's discovery:

```go
		rec := model.DNSRecord{Name: name, Slot: q.Slot, Rcode: rcode, TTL: rr.Header().Ttl, Source: "query", Discovery: q.Discovery}
```

(The DS append in `Resolve` stays `Discovery: "static"` — the parent DS isn't a discovered host. AXFR records keep `Discovery: "axfr"` in axfrRecord.)

- [ ] **Step 6: Update existing `Plan` call sites**

`records.Plan` is called in `internal/resolve/query.go` `Resolve` (`plan := records.Plan(domain, cfg)`) — update to pass the resolver's per-domain hosts. `Resolve` gains an `extra []model.HostLabel` parameter (threaded from `resolveDomain` in Task 5). For THIS task, change the call to `records.Plan(domain, cfg, extra)` and add the `extra []model.HostLabel` param to `Resolver.Resolve`; the sole caller (`resolveDomain`) passes `nil` for now (Task 5 wires the real labels). Any test calling `Plan(domain, cfg)` gets a trailing `nil`.

- [ ] **Step 7: Run tests**

Run: `cd corpscout/commoncrawl/cc-dns-worker && go test ./internal/records/ ./internal/resolve/ && go build ./... && go test ./...`
Expected: PASS (union + dedup test green; full suite compiles with the new signatures).

- [ ] **Step 8: Vet, fmt, commit**

```bash
cd corpscout/commoncrawl/cc-dns-worker && go fmt ./... && go vet ./...
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/commoncrawl/cc-dns-worker/internal/records/plan.go corpscout/commoncrawl/cc-dns-worker/internal/records/plan_test.go corpscout/commoncrawl/cc-dns-worker/internal/resolve/query.go
git commit -m "feat(dns): records.Plan unions discovered hosts, tags per-host discovery"
```

---

## Task 5: Feeder threading — bulk-load labels per dispatch batch

**Files:**
- Modify: `cmd/cc-dns-worker/scan.go` (feeder, work channel, worker loop, `resolveDomain`)
- Test: `cmd/cc-dns-worker/scan_test.go`

**Interfaces:**
- Consumes: `store.HostnamesForBatch`; `Resolver.Resolve(..., extra)` (Task 4).
- Produces: the scan actually resolves the union'd hostnames — `resolveDomain(ctx, disc, rec, cfg, domain, scanID, runID, prober, extra)`.

- [ ] **Step 1: Change the work channel to carry labels**

In `scanCycle`, define a work item and change the channel:

```go
	type domainWork struct {
		domain string
		hosts  []model.HostLabel
	}
	work := make(chan domainWork, cfg.workers)
```

- [ ] **Step 2: Feeder bulk-loads labels per dispatch batch**

In the feeder goroutine, after `PendingBatch` returns `batch`, bulk-load its labels once and send `domainWork`:

```go
		hostsByDomain, herr := st.HostnamesForBatch(ctx, cfg.scanID, batch)
		if herr != nil {
			feedErr = herr
			return
		}
		for _, d := range batch {
			select {
			case work <- domainWork{domain: d, hosts: hostsByDomain[d]}:
			case <-ctx.Done():
				return
			}
		}
```

(When `--host-enrich` is off, `scan_hostnames` is empty, so `HostnamesForBatch` returns an empty map and `hosts` is nil — behaviour is unchanged.)

- [ ] **Step 3: Worker + `resolveDomain` pass the labels**

Worker loop:

```go
			for w := range work {
				results <- resolveDomain(ctx, disc, rec, rcfg, w.domain, cfg.scanID, cfg.runID, prober, w.hosts)
			}
```

`resolveDomain` gains a trailing `extra []model.HostLabel` and passes it into `rec.Resolve(ctx, domain, scanID, runID, del, cfg, now, extra)`:

```go
func resolveDomain(ctx context.Context, disc *resolve.Discoverer, rec *resolve.Resolver, cfg records.Config, domain, scanID, runID string, prober *resolve.AXFRProber, extra []model.HostLabel) model.DomainResult {
	...
	res := rec.Resolve(ctx, domain, scanID, runID, del, cfg, now, extra)
	...
}
```

Ensure `Resolver.Resolve`'s signature (from Task 4) is `Resolve(ctx, domain, scanID, runID string, del Delegation, cfg records.Config, now time.Time, extra []model.HostLabel)` and it calls `records.Plan(domain, cfg, extra)`.

- [ ] **Step 4: Test — a domain with extra hosts gets them planned**

Add to `cmd/cc-dns-worker/scan_test.go` a focused test that `resolveDomain` with a non-nil `extra` produces (via the resolver against a fake server, OR by asserting at the `records.Plan` level). Since `resolveDomain` needs live discovery, keep this at the plan level (already covered by Task 4's `TestPlanUnionsExtraHosts`) and instead assert the feeder wiring compiles + the empty-enrich path is unchanged. Concretely, add a smoke assertion that `HostnamesForBatch` on an unseeded scan returns an empty map (no panic, nil hosts):

```go
func TestHostnamesForBatchEmpty(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	m, err := st.HostnamesForBatch(context.Background(), "s1", []string{"example.com"})
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 0 {
		t.Fatalf("want empty map, got %+v", m)
	}
}
```

(Import `store`, `context`, `path/filepath` in scan_test.go if needed.)

- [ ] **Step 5: Run full suite + build**

Run: `cd corpscout/commoncrawl/cc-dns-worker && go build ./... && go vet ./... && go test ./...`
Expected: PASS.

- [ ] **Step 6: Vet, fmt, commit**

```bash
cd corpscout/commoncrawl/cc-dns-worker && go fmt ./...
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/commoncrawl/cc-dns-worker/cmd/cc-dns-worker/scan.go corpscout/commoncrawl/cc-dns-worker/cmd/cc-dns-worker/scan_test.go
git commit -m "feat(dns): feeder bulk-loads per-domain hostnames into the scan plan"
```

---

## Rollout (after merge, operator)

1. Migrations are already applied (Phase 0/1). Phase 2 has no migration.
2. Bounded live sample: `go run ./cmd/cc-dns-worker scan --resolvers 127.0.0.1:53 --limit 2000 --host-enrich --db /tmp/he-smoke.db --scan-id he-smoke`. Watch the `host-load complete (N discovered hostnames)` log and the added query volume. Confirm `scan_hostnames` populated and records for CT/registry hosts carry `discovery` ct/axfr.
3. This is also where the CT + registry SQL is validated end-to-end against the real `ctlogs.hostnames` (1.9B rows) and the registry.

## Self-Review

**Spec coverage:** §3 host-load (CT+registry query, cap, `scan_hostnames`, resumable) → Tasks 1-3; §4 union side (`Plan` union, per-host `discovery`, feeder bulk-load) → Tasks 4-5. Default-off `--host-enrich`. ✓

**Placeholder scan:** No TBD/TODO; each step has literal code. The Task 2 `sort` import note is an explicit "drop if unused" instruction, not a placeholder. ✓

**Type/lesson consistency:** `model.HostLabel{Label, DiscoverySource, LiveCert}` is the single shared type (no import cycle: model→nothing, records/store/hostsource→model). `Merge` precedence axfr<ct<static via `minSource`; Plan dedups extras against static (static wins overlaps) — resolving the Phase-1 within-cycle precedence note. Wildcard labels excluded in `NormalizeLabel` (belt with the Phase-1 write-back guard). CT/registry reads capped `LIMIT cap BY`; union re-capped in `Merge`. `collect` now tags `Discovery: q.Discovery` (static for built-ins, ct/axfr for extras) — records for CT/registry hosts flow back to the registry via the Phase-1 write-back (`discovery IN ('ct','axfr')`), closing the durability loop. ✓
