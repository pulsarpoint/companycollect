package load

import (
	"testing"
	"time"

	"cc-dns-worker/internal/model"
	"cc-dns-worker/internal/resolve"
	"cc-dns-worker/internal/store"
)

func TestObservationRowsCarryUniversalRRFields(t *testing.T) {
	loadedAt := time.Unix(20, 0).UTC()
	rows := observationRows([]model.StagedDNSRecord{{
		RootDomain: "example.com", Name: "unknown.example.com", RecordType: "TYPE65400",
		TypeCode: 65400, ClassCode: 65280, Value: `\# 4 DEADBEEF`,
		RDataWire: string([]byte{0xde, 0xad, 0xbe, 0xef}), Source: "axfr", Discovery: "axfr",
		NameServer: "ns.example.com", NameServerIP: "192.0.2.53",
	}}, "scan", loadedAt)
	if len(rows) != 1 {
		t.Fatalf("rows = %d", len(rows))
	}
	row := rows[0]
	if row.TypeCode != 65400 || row.ClassCode != 65280 || row.RDataWire != string([]byte{0xde, 0xad, 0xbe, 0xef}) ||
		row.NameServer != "ns.example.com" || row.NameServerIP != "192.0.2.53" {
		t.Errorf("universal RR fields lost: %+v", row)
	}
}

func TestUnknownProbeUpdatesMetricsAndPreservesDefinitiveState(t *testing.T) {
	key := store.AXFREndpointKey{RootDomain: "a.test", NameServer: "ns.a.test", NameServerIP: "1.1.1.1"}
	prior := map[store.AXFREndpointKey]store.AXFRPriorState{
		key: {
			HasDefinitive: true, AXFROpen: true, DefinitiveAt: time.Unix(10, 0).UTC(),
			DefinitiveScanID: "old", LastProbeRecords: 20, LastProbeBytes: 2000,
		},
	}
	observedAt := time.Unix(20, 0).UTC()
	rows := BuildAXFRLatestRows([]store.AXFRProbedEndpoint{{
		AXFREndpointKey: key, Verdict: string(resolve.VerdictUnknown), Reason: "timeout",
		ObservedAt: observedAt, StateObservedAt: observedAt, Records: 7, Bytes: 800,
		Truncated: true, DelegationActive: true,
	}}, prior, "new")
	row := rows[0]
	if row.HasDefinitiveState != 1 || row.AXFROpen != 1 || row.DefinitiveScanID != "old" {
		t.Errorf("unknown probe changed definitive state: %+v", row)
	}
	if row.LastProbeRecords != 7 || row.LastProbeBytes != 800 || row.LastProbeTruncated != 1 {
		t.Errorf("unknown probe metrics not updated: %+v", row)
	}
	if !row.UpdatedAt.Equal(observedAt) || !row.DelegationSeenAt.Equal(observedAt) {
		t.Errorf("stable observation timestamps not used: %+v", row)
	}
}

func TestDelegationRemovalPreservesProbeState(t *testing.T) {
	key := store.AXFREndpointKey{RootDomain: "a.test", NameServer: "old.a.test", NameServerIP: "1.1.1.1"}
	prior := map[store.AXFREndpointKey]store.AXFRPriorState{
		key: {
			HasDefinitive: true, AXFROpen: true, LastProbeVerdict: "open",
			LastProbeRecords: 42, LastProbeBytes: 4096, LastProbeTruncated: true,
			DelegationSeenAt: time.Unix(10, 0).UTC(),
		},
	}
	removedAt := time.Unix(30, 0).UTC()
	row := BuildAXFRLatestRows([]store.AXFRProbedEndpoint{{
		AXFREndpointKey: key, StateObservedAt: removedAt,
	}}, prior, "new")[0]
	if row.DelegationActive != 0 || row.AXFROpen != 1 || row.LastProbeRecords != 42 {
		t.Errorf("delegation removal changed probe state: %+v", row)
	}
	if !row.UpdatedAt.Equal(removedAt) || !row.DelegationSeenAt.Equal(time.Unix(10, 0).UTC()) {
		t.Errorf("delegation timestamps are wrong: %+v", row)
	}
}

func TestFirstClosedHasNoChangeAndFirstOpenDoes(t *testing.T) {
	closed := store.AXFRProbedEndpoint{
		AXFREndpointKey: store.AXFREndpointKey{RootDomain: "closed.test"},
		Verdict:         string(resolve.VerdictClosed), Definitive: true, ObservedAt: time.Unix(1, 0).UTC(),
	}
	open := store.AXFRProbedEndpoint{
		AXFREndpointKey: store.AXFREndpointKey{RootDomain: "open.test"},
		Verdict:         string(resolve.VerdictOpen), Definitive: true, ObservedAt: time.Unix(2, 0).UTC(),
	}
	changes := boundedAXFRChanges([]store.AXFRProbedEndpoint{closed, open}, nil, "scan")
	if len(changes) != 1 || changes[0].RootDomain != "open.test" || changes[0].AXFROpen != 1 {
		t.Errorf("changes = %+v, want only initial open", changes)
	}
}
