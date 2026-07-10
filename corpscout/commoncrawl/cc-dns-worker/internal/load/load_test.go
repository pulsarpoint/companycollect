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
