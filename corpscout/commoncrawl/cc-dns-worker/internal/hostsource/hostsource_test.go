package hostsource

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
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
		{"example.com", "*.example.com", "", false}, // wildcard
		{"example.com", "other.org", "", false},     // not a subdomain
	}
	for _, c := range cases {
		got, ok := NormalizeLabel(c.rd, c.fqdn)
		if ok != c.ok || got != c.want {
			t.Errorf("NormalizeLabel(%q,%q) = (%q,%v), want (%q,%v)", c.rd, c.fqdn, got, ok, c.want, c.ok)
		}
	}
}

// TestShardQueryScopedToSeedMembership proves the shard query joins BOTH the CT branch and the
// registry branch to corpscout.dns_scan_seed_domains filtered on scan_id — the Task 11 scope fix — and
// never references the whole-corpus corpscout.commoncrawl_domains table the old CTShard/RegistryShard
// semi-joined against.
func TestShardQueryScopedToSeedMembership(t *testing.T) {
	q := shardQuery(3, 16, 100)

	if n := strings.Count(q, "corpscout.dns_scan_seed_domains"); n != 2 {
		t.Errorf("dns_scan_seed_domains referenced %d times, want 2 (once per branch: CT and registry)", n)
	}
	if !strings.Contains(q, "WHERE scan_id = ?") {
		t.Errorf("query does not filter dns_scan_seed_domains on scan_id: %s", q)
	}
	// The old bug: semi-joining the whole commoncrawl_domains corpus instead of this scan's membership.
	// (commoncrawl_domain_hostnames — the registry table this query legitimately reads — does not match:
	// the char right after "domain" there is "_", never the "s" that would make it "...domains".)
	if strings.Contains(q, "corpscout.commoncrawl_domains") {
		t.Errorf("query still references the whole-corpus commoncrawl_domains table: %s", q)
	}
	if !strings.Contains(q, "ctlogs.hostnames") {
		t.Errorf("query must still read ctlogs.hostnames for the CT branch: %s", q)
	}
	if !strings.Contains(q, "corpscout.commoncrawl_domain_hostnames") {
		t.Errorf("query must still read the durable registry for the registry branch: %s", q)
	}
}

// TestShardQueryPriorityIsExplicitNumeric proves source precedence is encoded as an EXPLICIT numeric
// priority (multiIf mapping axfr/ct/static to descending integers), not a lexical string comparison
// like the old `min(discovery_source)`.
func TestShardQueryPriorityIsExplicitNumeric(t *testing.T) {
	q := shardQuery(0, 16, 50)

	if strings.Contains(q, "min(discovery_source)") {
		t.Errorf("query still uses lexical min(discovery_source) precedence, want an explicit numeric priority: %s", q)
	}
	wantMulti := fmt.Sprintf("multiIf(discovery_source = 'axfr', %d, discovery_source = 'ct', %d, discovery_source = 'static', %d, 0)", axfrPriority, ctPriority, staticPriority)
	if !strings.Contains(q, wantMulti) {
		t.Errorf("registry branch priority expression = missing %q in query: %s", wantMulti, q)
	}
	if axfrPriority <= ctPriority || ctPriority <= staticPriority {
		t.Fatalf("priority constants must satisfy axfr > ct > static, got axfr=%d ct=%d static=%d", axfrPriority, ctPriority, staticPriority)
	}
	// The CT branch's own rows must carry the SAME numeric ct priority as a registry row whose
	// discovery_source is 'ct' — one priority scale shared by both branches.
	if !strings.Contains(q, "AS priority, live, recency") || !strings.Contains(q, strconv.Itoa(ctPriority)+" AS priority, live, recency") {
		t.Errorf("CT branch must tag its rows with the shared ctPriority constant (%d): %s", ctPriority, q)
	}
}

// TestShardQueryCapsFinalUnionedSetOnce proves the per-domain cap (LIMIT capN BY root_domain) is
// applied exactly once, to the GROUP-BY-deduped, priority-ranked union of both sources — not
// separately per source, which is what let the old client-side Merge evict an AXFR label after
// concatenating a CT slice that alone already filled the cap.
func TestShardQueryCapsFinalUnionedSetOnce(t *testing.T) {
	q := shardQuery(5, 16, 7)

	wantLimit := "LIMIT 7 BY root_domain"
	if n := strings.Count(q, "LIMIT"); n != 1 {
		t.Errorf("query has %d LIMIT clauses, want exactly 1 (cap applied once, to the final unioned+ranked set): %s", n, q)
	}
	if !strings.HasSuffix(strings.TrimSpace(q), wantLimit) {
		t.Errorf("query does not end with %q: %s", wantLimit, q)
	}
	if !strings.Contains(q, "ORDER BY root_domain, top_priority DESC, top_live DESC, top_recency DESC") {
		t.Errorf("query missing the expected rank ordering (priority first, so AXFR always sorts ahead of CT): %s", q)
	}
}

// TestShardQueryPartitionAligned proves the shard filter is applied to both the ctlogs.hostnames scan
// AND the registry scan AND the seed-membership subquery each branch joins to, preserving partition
// pruning (CTPartitions) end to end for the given shard/numShards.
func TestShardQueryPartitionAligned(t *testing.T) {
	q := shardQuery(9, 16, 100)
	want := "cityHash64(registered_domain) % 16 = 9"
	if !strings.Contains(q, want) {
		t.Errorf("CT branch missing shard filter %q: %s", want, q)
	}
	// want2 appears 3 times: the registry table scan itself, plus once in EACH branch's own
	// seed-membership subquery (both the CT branch and the registry branch join to
	// dns_scan_seed_domains, which is keyed by root_domain, not registered_domain).
	want2 := "cityHash64(root_domain) % 16 = 9"
	if n := strings.Count(q, want2); n != 3 {
		t.Errorf("shard filter %q must appear 3 times (registry scan + both branches' seed-membership subqueries) got %d: %s", want2, n, q)
	}
}

// fakeShardRows simulates a ClickHouse Rows cursor over a fixed slice of rows for drainShardRows,
// without any network or database dependency — proving the streaming/batching contract in isolation.
type fakeShardRows struct {
	rows []model.ScanHostnameRow
	i    int
}

func (f *fakeShardRows) next() bool { return f.i < len(f.rows) }
func (f *fakeShardRows) scan(r *model.ScanHostnameRow) error {
	*r = f.rows[f.i]
	f.i++
	return nil
}
func (f *fakeShardRows) err() error { return nil }

// TestDrainShardRowsBoundedBatches proves the shard-query result is streamed in bounded batches — never
// materialized as a whole map[string][]HostLabel — by feeding a synthetic 10-row shard through a
// batchSize of 3 and asserting the callback fires with batches of 3,3,3,1 (never more than batchSize at
// once), while every row is still delivered exactly once and in order.
func TestDrainShardRowsBoundedBatches(t *testing.T) {
	const total = 10
	const batchSize = 3
	src := make([]model.ScanHostnameRow, total)
	for i := range src {
		src[i] = model.ScanHostnameRow{RootDomain: fmt.Sprintf("d%d.com", i), HostLabel: model.HostLabel{Label: "www"}}
	}
	f := &fakeShardRows{rows: src}

	var batchSizes []int
	var got []model.ScanHostnameRow
	err := drainShardRows(f.next, f.scan, f.err, batchSize, func(batch []model.ScanHostnameRow) error {
		if len(batch) > batchSize {
			t.Fatalf("batch of %d rows exceeds batchSize %d — the shard result was not bounded", len(batch), batchSize)
		}
		batchSizes = append(batchSizes, len(batch))
		got = append(got, batch...)
		return nil
	})
	if err != nil {
		t.Fatalf("drainShardRows: %v", err)
	}
	wantBatches := []int{3, 3, 3, 1}
	if fmt.Sprint(batchSizes) != fmt.Sprint(wantBatches) {
		t.Errorf("batch sizes = %v, want %v", batchSizes, wantBatches)
	}
	if len(got) != total {
		t.Fatalf("total rows delivered = %d, want %d", len(got), total)
	}
	for i, r := range got {
		if r.RootDomain != src[i].RootDomain {
			t.Errorf("row %d = %q, want %q (order not preserved)", i, r.RootDomain, src[i].RootDomain)
		}
	}
}

// TestDrainShardRowsPropagatesScanError proves a mid-stream Scan error aborts the drain immediately
// rather than silently dropping rows.
func TestDrainShardRowsPropagatesScanError(t *testing.T) {
	wantErr := errors.New("boom")
	calls := 0
	next := func() bool { calls++; return calls <= 5 }
	scan := func(r *model.ScanHostnameRow) error {
		if calls == 2 {
			return wantErr
		}
		return nil
	}
	err := drainShardRows(next, scan, func() error { return nil }, 10, func([]model.ScanHostnameRow) error { return nil })
	if !errors.Is(err, wantErr) {
		t.Errorf("err = %v, want %v", err, wantErr)
	}
}

// TestDrainShardRowsPropagatesCallbackError proves an fn error (e.g. the SQLite insert failing) stops
// the drain rather than continuing to pull more rows from ClickHouse.
func TestDrainShardRowsPropagatesCallbackError(t *testing.T) {
	wantErr := errors.New("insert failed")
	src := []model.ScanHostnameRow{{RootDomain: "a.com"}, {RootDomain: "b.com"}}
	f := &fakeShardRows{rows: src}
	fnCalls := 0
	err := drainShardRows(f.next, f.scan, f.err, 1, func(batch []model.ScanHostnameRow) error {
		fnCalls++
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Errorf("err = %v, want %v", err, wantErr)
	}
	if fnCalls != 1 {
		t.Errorf("fn called %d times, want 1 (must stop after the first failure)", fnCalls)
	}
}
