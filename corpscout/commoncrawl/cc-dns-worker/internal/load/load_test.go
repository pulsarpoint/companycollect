package load

import (
	"strings"
	"testing"
	"time"

	"cc-dns-worker/internal/model"
	"cc-dns-worker/internal/store"
)

func TestColumnLists(t *testing.T) {
	rc := chColumns[model.RecordRow]()
	if !strings.Contains(strings.Join(rc, ","), "first_seen") || len(rc) != 14 {
		t.Errorf("RecordRow columns wrong: %v", rc)
	}
	sc := chColumns[model.ScanRow]()
	if !strings.Contains(strings.Join(sc, ","), "nameservers") || len(sc) != 11 {
		t.Errorf("ScanRow columns wrong: %v", sc)
	}
	oc := chColumns[model.RecordObservationRow]()
	if !strings.Contains(strings.Join(oc, ","), "scan_id") || len(oc) != 13 {
		t.Errorf("RecordObservationRow columns wrong: %v", oc)
	}
}

// TestToObservationRowsIdentity proves the observation row's sorting-key identity (root_domain, name,
// record_type, slot, value, source, discovery, scan_id — see migration 000113) is exactly what makes a
// retried load safe: two calls to toObservationRows for the SAME staged RecordRow and the SAME scanID
// produce byte-identical rows except for LoadedAt, which is why ReplacingMergeTree(loaded_at) can
// collapse them on merge/FINAL regardless of which LoadedAt value "wins" the version — the payload is
// identical either way. This is a logic-only check; the actual merge/dedup is proven against real
// ClickHouse in TestObservationRetryDedup (integration_test.go, //go:build integration).
func TestToObservationRowsIdentity(t *testing.T) {
	rec := model.RecordRow{
		RootDomain: "example.test", RecordType: "A", Slot: "@", Name: "example.test", Value: "10.0.0.1",
		TTL: 300, Priority: 0, Rcode: "NOERROR", Source: "query", Discovery: "static",
		FirstSeen: time.Unix(100, 0).UTC(), LastSeen: time.Unix(100, 0).UTC(), Scans: 1,
	}

	first := toObservationRows([]model.RecordRow{rec}, "scan-1", time.Unix(200, 0).UTC())
	retry := toObservationRows([]model.RecordRow{rec}, "scan-1", time.Unix(300, 0).UTC())
	if len(first) != 1 || len(retry) != 1 {
		t.Fatalf("expected 1 row each, got %d and %d", len(first), len(retry))
	}

	a, b := first[0], retry[0]
	a.LoadedAt, b.LoadedAt = time.Time{}, time.Time{} // the only field a retry is allowed to differ on
	if a != b {
		t.Errorf("retried observation must be identical except LoadedAt: %+v vs %+v", first[0], retry[0])
	}
	if first[0].LoadedAt.Equal(retry[0].LoadedAt) {
		t.Errorf("test setup: retry's LoadedAt should differ from the first call's to exercise the dedup path")
	}
	if first[0].ScanID != "scan-1" || first[0].ObservedAt != rec.LastSeen {
		t.Errorf("identity fields must come from scanID param / RecordRow.LastSeen: %+v", first[0])
	}
}

// TestToObservationRowsScanIDNotLastRunID proves ScanID is taken from the scanID PARAMETER, not
// RecordRow.LastRunID (source_run_id) — they happen to be equal in production today, but the
// observation's identity must not silently depend on that always being true.
func TestToObservationRowsScanIDNotLastRunID(t *testing.T) {
	rec := model.RecordRow{RootDomain: "d.test", RecordType: "A", Name: "d.test", Value: "1.1.1.1", LastRunID: "different-run-id"}
	rows := toObservationRows([]model.RecordRow{rec}, "the-scan-id", time.Now())
	if rows[0].ScanID != "the-scan-id" {
		t.Errorf("ScanID = %q, want the scanID parameter (%q), not LastRunID (%q)", rows[0].ScanID, "the-scan-id", rec.LastRunID)
	}
}

// TestBuildAXFRLatestRowsPreservesDefinitiveOnUnknown proves an unknown probe (e.g. a timeout) updates
// only last_probe_verdict/reason/last_probed_at: the endpoint's prior DEFINITIVE state
// (has_definitive_state/axfr_open/definitive_at/definitive_scan_id) must survive untouched, since an
// unknown probe must never be treated as either open or closed.
func TestBuildAXFRLatestRowsPreservesDefinitiveOnUnknown(t *testing.T) {
	key := store.AXFREndpointKey{RootDomain: "d.com", NameServer: "ns1.d.com", NameServerIP: "9.9.9.9"}
	prior := map[store.AXFREndpointKey]store.AXFRPriorState{
		key: {HasDefinitive: true, AXFROpen: true, DefinitiveAt: time.Unix(100, 0).UTC(), DefinitiveScanID: "sc1"},
	}
	eps := []store.AXFRProbedEndpoint{
		{AXFREndpointKey: key, Verdict: "unknown", Reason: "timeout", ObservedAt: time.Unix(200, 0).UTC(),
			Definitive: false, DelegationActive: true},
	}

	rows := BuildAXFRLatestRows(eps, prior, "sc2", time.Unix(300, 0).UTC())
	if len(rows) != 1 {
		t.Fatalf("rows = %+v, want 1", rows)
	}
	r := rows[0]
	if r.HasDefinitiveState != 1 || r.AXFROpen != 1 {
		t.Errorf("definitive state not preserved across an unknown probe: %+v", r)
	}
	if !r.DefinitiveAt.Equal(prior[key].DefinitiveAt) || r.DefinitiveScanID != "sc1" {
		t.Errorf("definitive_at/definitive_scan_id must stay at the last DEFINITIVE probe, got %+v", r)
	}
	if r.LastProbeVerdict != "unknown" || r.LastProbeReason != "timeout" || !r.LastProbedAt.Equal(time.Unix(200, 0).UTC()) {
		t.Errorf("last_probe_* must reflect THIS scan's unknown probe, got %+v", r)
	}
	if r.DelegationActive != 1 {
		t.Errorf("delegation_active = %d, want 1 (still delegated)", r.DelegationActive)
	}
}

// TestBuildAXFRLatestRowsUnseenClosedLatestRow proves an endpoint with no prior state at all still gets
// a full latest row from its first (closed) probe — "no change" (see store.StageAXFRChanges) does not
// mean "no latest row".
func TestBuildAXFRLatestRowsUnseenClosedLatestRow(t *testing.T) {
	key := store.AXFREndpointKey{RootDomain: "closed.com", NameServer: "ns1.closed.com", NameServerIP: "9.9.9.9"}
	eps := []store.AXFRProbedEndpoint{
		{AXFREndpointKey: key, Verdict: "closed", Reason: "refused", ObservedAt: time.Unix(100, 0).UTC(),
			Definitive: true, DelegationActive: true},
	}

	rows := BuildAXFRLatestRows(eps, nil, "sc1", time.Unix(200, 0).UTC())
	if len(rows) != 1 {
		t.Fatalf("rows = %+v, want 1", rows)
	}
	r := rows[0]
	if r.HasDefinitiveState != 1 || r.AXFROpen != 0 {
		t.Errorf("unseen endpoint's first (closed) probe must still produce a definitive latest row: %+v", r)
	}
	if r.DefinitiveScanID != "sc1" || !r.DefinitiveAt.Equal(time.Unix(100, 0).UTC()) {
		t.Errorf("definitive_at/definitive_scan_id must be this probe's, got %+v", r)
	}
}

// TestBuildAXFRLatestRowsDelegationRemovalInactive proves an endpoint no longer in the domain's current
// delegation (store.AXFRProbedEndpoint.DelegationActive=false, no fresh probe) is written back with
// delegation_active=0 while every other field — including the AXFR definitive state and
// delegation_seen_at, the LAST time it truly was seen active — carries forward from prior untouched.
func TestBuildAXFRLatestRowsDelegationRemovalInactive(t *testing.T) {
	key := store.AXFREndpointKey{RootDomain: "moved.com", NameServer: "ns-old.moved.com", NameServerIP: "1.1.1.1"}
	prior := map[store.AXFREndpointKey]store.AXFRPriorState{
		key: {
			HasDefinitive: true, AXFROpen: true, DelegationActive: true, DelegationSeenAt: time.Unix(50, 0).UTC(),
			LastProbeVerdict: "open", LastProbeReason: "transferred", LastProbedAt: time.Unix(50, 0).UTC(),
			DefinitiveAt: time.Unix(50, 0).UTC(), DefinitiveScanID: "sc0",
		},
	}
	eps := []store.AXFRProbedEndpoint{
		{AXFREndpointKey: key, DelegationActive: false}, // no probe this scan: dropped from the delegation
	}

	rows := BuildAXFRLatestRows(eps, prior, "sc1", time.Unix(500, 0).UTC())
	if len(rows) != 1 {
		t.Fatalf("rows = %+v, want 1", rows)
	}
	r := rows[0]
	if r.DelegationActive != 0 {
		t.Errorf("delegation_active = %d, want 0 (removed from delegation)", r.DelegationActive)
	}
	if r.HasDefinitiveState != 1 || r.AXFROpen != 1 {
		t.Errorf("a delegation removal must not touch the AXFR definitive state: %+v", r)
	}
	if !r.DelegationSeenAt.Equal(time.Unix(50, 0).UTC()) {
		t.Errorf("delegation_seen_at = %v, want the prior seen time preserved (not bumped to now)", r.DelegationSeenAt)
	}
	if r.LastProbeVerdict != "open" || !r.LastProbedAt.Equal(time.Unix(50, 0).UTC()) {
		t.Errorf("last_probe_* must carry forward since no probe happened this scan: %+v", r)
	}
}
