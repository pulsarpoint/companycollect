package axfrscan

import (
	"testing"
	"time"

	"cc-dns-axfr/internal/axfrprobe"
	"cc-dns-axfr/internal/model"
)

func TestUnknownProbePreservesDefinitiveState(t *testing.T) {
	key := endpointKey{RootDomain: "example.com", NameServer: "ns.example.com", NameServerIP: "1.1.1.1"}
	prior := map[endpointKey]priorState{
		key: {
			HasDefinitive: true, AXFROpen: true, DefinitiveAt: time.Unix(10, 0),
			DefinitiveScanID: "old", LastProbeRecords: 20, LastProbeBytes: 2000,
		},
	}
	observedAt := time.Unix(20, 0).UTC()
	rows := buildLatestRows([]observedEndpoint{{
		endpointKey: key, Verdict: string(axfrprobe.VerdictUnknown), Reason: "timeout",
		ObservedAt: observedAt, StateObservedAt: observedAt, Records: 7, Bytes: 800,
		Truncated: true, DelegationActive: true,
	}}, prior, "new")
	row := rows[0]
	if row.HasDefinitiveState != 1 || row.AXFROpen != 1 || row.DefinitiveScanID != "old" {
		t.Fatalf("unknown probe changed definitive state: %+v", row)
	}
	if row.LastProbeRecords != 7 || row.LastProbeBytes != 800 || row.LastProbeTruncated != 1 {
		t.Fatalf("unknown probe metrics were not updated: %+v", row)
	}
}

func TestRemovedDelegationPreservesProbeState(t *testing.T) {
	key := endpointKey{RootDomain: "example.com", NameServer: "old.example.com", NameServerIP: "1.1.1.1"}
	prior := map[endpointKey]priorState{
		key: {
			HasDefinitive: true, AXFROpen: true, LastProbeVerdict: "open",
			LastProbeRecords: 42, DelegationSeenAt: time.Unix(10, 0).UTC(),
		},
	}
	removedAt := time.Unix(30, 0).UTC()
	row := buildLatestRows([]observedEndpoint{{endpointKey: key, StateObservedAt: removedAt}}, prior, "new")[0]
	if row.DelegationActive != 0 || row.AXFROpen != 1 || row.LastProbeRecords != 42 {
		t.Fatalf("delegation removal changed probe state: %+v", row)
	}
}

func TestSharedIPProbeIsAppliedToEveryNameserverIdentity(t *testing.T) {
	observedAt := time.Unix(20, 0).UTC()
	domains := []readyDomain{{
		RootDomain: "example.com", DelegationObservedAt: observedAt,
		Endpoints: []model.NameserverEndpoint{
			{Name: "ns1.example.com", IP: "1.1.1.1", Dialable: true},
			{Name: "ns2.example.com", IP: "1.1.1.1", Dialable: true},
		},
		Probes: []axfrprobe.AXFROutcome{{
			NSHost: "ns1.example.com", NSIP: "1.1.1.1", Verdict: axfrprobe.VerdictClosed,
			Reason: axfrprobe.ReasonRefused, ObservedAt: observedAt,
		}},
	}}
	endpoints := buildObservedEndpoints(domains, nil)
	if len(endpoints) != 2 || endpoints[0].NameServer == endpoints[1].NameServer {
		t.Fatalf("observed endpoints = %+v", endpoints)
	}
	for _, endpoint := range endpoints {
		if endpoint.Verdict != string(axfrprobe.VerdictClosed) || !endpoint.DelegationActive {
			t.Fatalf("probe was not applied to endpoint: %+v", endpoint)
		}
	}
}

func TestOnlyInitialOpenCreatesStateChange(t *testing.T) {
	changes := buildStateChanges([]observedEndpoint{
		{endpointKey: endpointKey{RootDomain: "closed.test"}, Verdict: string(axfrprobe.VerdictClosed), Definitive: true},
		{endpointKey: endpointKey{RootDomain: "open.test"}, Verdict: string(axfrprobe.VerdictOpen), Definitive: true},
	}, nil, "scan")
	if len(changes) != 1 || changes[0].RootDomain != "open.test" || changes[0].AXFROpen != 1 {
		t.Fatalf("changes = %+v", changes)
	}
}
