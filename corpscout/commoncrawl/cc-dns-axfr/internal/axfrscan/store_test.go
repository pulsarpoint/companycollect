package axfrscan

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"cc-dns-axfr/internal/axfrprobe"
	"cc-dns-axfr/internal/model"
)

func TestAXFRStoreCreatesOneProbePerUniquePublicIP(t *testing.T) {
	ctx := context.Background()
	store, err := openStore(filepath.Join(t.TempDir(), "axfr.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	if err := store.begin(ctx, "scan", time.Unix(1, 0)); err != nil {
		t.Fatal(err)
	}
	_, err = store.addPage(ctx, "scan", []sourceDomain{{
		RootDomain: "example.com", ObservedAt: time.Unix(2, 0),
		Endpoints: []model.NameserverEndpoint{
			{Name: "ns1.example.com", IP: "1.1.1.1", Dialable: true},
			{Name: "ns2.example.com", IP: "1.1.1.1", Dialable: true},
			{Name: "hyperscaler.example.com", IP: "104.16.0.1", Dialable: true},
			{Name: "private.example.com", IP: "10.0.0.1", Dialable: false},
		},
	}}, true, 0)
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := store.claim(ctx, "scan", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].NameServerIP != "1.1.1.1" {
		t.Fatalf("jobs = %+v, want one public-IP job", jobs)
	}
}

func TestAXFRStoreCommitsEndpointWithoutWaitingForBatch(t *testing.T) {
	ctx := context.Background()
	store, err := openStore(filepath.Join(t.TempDir(), "axfr.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	if err := store.begin(ctx, "scan", time.Unix(1, 0)); err != nil {
		t.Fatal(err)
	}
	_, err = store.addPage(ctx, "scan", []sourceDomain{{
		RootDomain: "example.com", ObservedAt: time.Unix(2, 0),
		Endpoints: []model.NameserverEndpoint{{Name: "ns.example.com", IP: "1.1.1.1", Dialable: true}},
	}}, true, 0)
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := store.claim(ctx, "scan", 1)
	if err != nil {
		t.Fatal(err)
	}
	outcome := axfrprobe.AXFROutcome{
		Verdict: axfrprobe.VerdictOpen, Reason: axfrprobe.ReasonTransferred,
		NSHost: jobs[0].NameServer, NSIP: jobs[0].NameServerIP,
		Records: 1, ObservedAt: time.Unix(3, 0),
		Zone: []model.DNSRecord{{Name: "www.example.com", RecordType: "A", TypeCode: 1, ClassCode: 1}},
	}
	if err := store.commit(ctx, "scan", jobs[0], outcome); err != nil {
		t.Fatal(err)
	}
	ready, err := store.ready(ctx, "scan", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(ready) != 1 || len(ready[0].Probes) != 1 || len(ready[0].Zone) != 1 {
		t.Fatalf("ready domain = %+v", ready)
	}
	stats, err := store.stats(ctx, "scan")
	if err != nil {
		t.Fatal(err)
	}
	if stats.Tried != 1 || stats.Successful != 1 || stats.Open != 1 {
		t.Fatalf("stats = %+v", stats)
	}
}
