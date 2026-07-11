package main

import (
	"context"
	"testing"
	"time"

	"cc-dns-worker/internal/metrics"
	"cc-dns-worker/internal/model"
	"cc-dns-worker/internal/resolve"
	"cc-dns-worker/internal/store"
)

type countingAXFRProber struct {
	calls     int
	truncated bool
}

func (prober *countingAXFRProber) ProbeServer(_ context.Context, _, _, ip string) resolve.AXFROutcome {
	prober.calls++
	return resolve.AXFROutcome{
		Verdict: resolve.VerdictOpen, NSIP: ip, Records: 3, Bytes: 100, Truncated: prober.truncated,
		ObservedAt: time.Unix(10, 0).UTC(), Zone: []model.DNSRecord{{Name: "www.a.test"}},
	}
}

func TestProcessAXFRTargetSkipsPrivateAndSharesIPProbe(t *testing.T) {
	prober := &countingAXFRProber{truncated: true}
	var stats metrics.Stats
	result := processAXFRTarget(context.Background(), prober, store.AXFRTarget{
		RootDomain: "a.test",
		Endpoints: []model.NameserverEndpoint{
			{Name: "ns1.a.test", IP: "1.2.3.4", Dialable: true},
			{Name: "ns2.a.test", IP: "1.2.3.4", Dialable: true},
			{Name: "private.a.test", IP: "10.0.0.1", Dialable: false},
		},
	}, &stats)
	if prober.calls != 1 {
		t.Errorf("network probes = %d, want one for shared public IP", prober.calls)
	}
	if len(result.probes) != 2 || result.probes[0].NSHost == result.probes[1].NSHost {
		t.Errorf("endpoint identities not preserved: %+v", result.probes)
	}
	if len(result.zone) != 1 || !result.probes[0].Truncated {
		t.Errorf("open truncated transfer not preserved: %+v", result)
	}
	if result.zone[0].NameServer != "ns1.a.test" || result.zone[0].NameServerIP != "1.2.3.4" {
		t.Errorf("zone record endpoint provenance lost: %+v", result.zone[0])
	}
	snapshot := stats.Snapshot(time.Now().UTC())
	if snapshot.AXFRPullsTried != 1 || snapshot.AXFRPullsSuccessful != 0 {
		t.Errorf("truncated pull stats = %+v", snapshot)
	}
}

func TestProcessAXFRTargetAllSkippedStillCompletes(t *testing.T) {
	prober := &countingAXFRProber{}
	var stats metrics.Stats
	result := processAXFRTarget(context.Background(), prober, store.AXFRTarget{
		RootDomain: "a.test",
		Endpoints:  []model.NameserverEndpoint{{Name: "ns.a.test", IP: "127.0.0.1", Dialable: false}},
	}, &stats)
	if prober.calls != 0 || len(result.probes) != 0 || result.domain != "a.test" {
		t.Errorf("all-skipped result = %+v, calls = %d", result, prober.calls)
	}
	if snapshot := stats.Snapshot(time.Now().UTC()); snapshot.AXFRPullsTried != 0 {
		t.Errorf("skipped endpoint counted as a pull: %+v", snapshot)
	}
}

func TestProcessAXFRTargetCountsSuccessfulPull(t *testing.T) {
	prober := &countingAXFRProber{}
	var stats metrics.Stats
	processAXFRTarget(context.Background(), prober, store.AXFRTarget{
		RootDomain: "a.test",
		Endpoints: []model.NameserverEndpoint{{
			Name: "ns.a.test", IP: "1.2.3.4", Dialable: true,
		}},
	}, &stats)
	snapshot := stats.Snapshot(time.Now().UTC())
	if snapshot.AXFRPullsTried != 1 || snapshot.AXFRPullsSuccessful != 1 {
		t.Errorf("successful pull stats = %+v", snapshot)
	}
}
