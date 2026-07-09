package resolve

import (
	"context"
	"runtime"
	"strconv"
	"testing"
	"time"

	"cc-dns-worker/internal/scheduler"

	"github.com/miekg/dns"
)

func axfrZone(t *testing.T) []dns.RR {
	return []dns.RR{
		mustRR(t, "example.com. 3600 IN SOA ns1.example.com. hostmaster.example.com. 1 7200 3600 1209600 3600"),
		mustRR(t, "www.example.com. 3600 IN A 1.2.3.4"),
		mustRR(t, "cpanel.example.com. 3600 IN A 5.6.7.8"),
		mustRR(t, "asa-fw.example.com. 3600 IN A 9.10.11.12"),
		mustRR(t, "example.com. 3600 IN SOA ns1.example.com. hostmaster.example.com. 1 7200 3600 1209600 3600"),
	}
}

func TestTransferAXFROpen(t *testing.T) {
	addr, stop := startAXFRServer(t, axfrZone(t), false)
	defer stop()
	caps := AXFRCaps{MaxRecords: 50000, MaxBytes: 64 << 20, Deadline: 5 * time.Second}
	res, err := transferAXFR(context.Background(), "example.com", addr, caps)
	if err != nil {
		t.Fatalf("transfer: %v", err)
	}
	if !res.Open {
		t.Fatal("want Open=true")
	}
	if res.Truncated {
		t.Fatal("want Truncated=false")
	}
	if res.Records != 5 || len(res.Zone) != 5 {
		t.Fatalf("want 5 records, got Records=%d len(Zone)=%d", res.Records, len(res.Zone))
	}
	for _, rec := range res.Zone {
		if rec.Source != "axfr" {
			t.Fatalf("want Source=axfr, got %q", rec.Source)
		}
	}
}

func TestTransferAXFRRefused(t *testing.T) {
	addr, stop := startAXFRServer(t, nil, true)
	defer stop()
	caps := AXFRCaps{MaxRecords: 50000, MaxBytes: 64 << 20, Deadline: 5 * time.Second}
	res, _ := transferAXFR(context.Background(), "example.com", addr, caps)
	if res.Open {
		t.Fatal("want Open=false on REFUSED")
	}
	if len(res.Zone) != 0 {
		t.Fatalf("want empty zone, got %d", len(res.Zone))
	}
}

func TestTransferAXFRMidStreamErrorNotOpen(t *testing.T) {
	// SOA-first + two A records, but NO closing SOA: the client collects these RRs and then hits a
	// read error when the server drops the connection. Even with Records>0, a mid-stream error must
	// NOT be reported as an open zone.
	rrs := []dns.RR{
		mustRR(t, "example.com. 3600 IN SOA ns1.example.com. hostmaster.example.com. 1 7200 3600 1209600 3600"),
		mustRR(t, "www.example.com. 3600 IN A 1.2.3.4"),
		mustRR(t, "mail.example.com. 3600 IN A 5.6.7.8"),
	}
	addr, stop := startAXFRServerAbrupt(t, rrs)
	defer stop()
	caps := AXFRCaps{MaxRecords: 50000, MaxBytes: 64 << 20, Deadline: 5 * time.Second}
	res, err := transferAXFR(context.Background(), "example.com", addr, caps)
	if err != nil {
		t.Fatalf("transfer: %v", err)
	}
	if res.Open {
		t.Fatalf("want Open=false on mid-stream error, got Open=true (Records=%d)", res.Records)
	}
}

func TestTransferAXFRNoLeakOnMidStreamCap(t *testing.T) {
	rrs := []dns.RR{
		mustRR(t, "example.com. 3600 IN SOA ns1.example.com. hostmaster.example.com. 1 7200 3600 1209600 3600"),
	}
	for i := 0; i < 18; i++ {
		rrs = append(rrs, mustRR(t, "host"+strconv.Itoa(i)+".example.com. 3600 IN A 10.0.0."+strconv.Itoa(i+1)))
	}
	rrs = append(rrs, mustRR(t, "example.com. 3600 IN SOA ns1.example.com. hostmaster.example.com. 1 7200 3600 1209600 3600"))

	addr, stop := startAXFRServerMulti(t, rrs)
	defer stop()

	before := runtime.NumGoroutine()
	caps := AXFRCaps{MaxRecords: 5, MaxBytes: 1 << 30, Deadline: 5 * time.Second}
	res, err := transferAXFR(context.Background(), "example.com", addr, caps)
	if err != nil {
		t.Fatalf("transfer: %v", err)
	}
	if !res.Truncated {
		t.Fatal("want Truncated=true (cap fired mid-stream)")
	}
	if res.Records != 5 {
		t.Fatalf("want capped at 5 records, got %d", res.Records)
	}

	// Poll for the producer goroutine (and its socket) to be reclaimed rather than parked forever.
	for i := 0; i < 200; i++ {
		if runtime.NumGoroutine() <= before {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if after := runtime.NumGoroutine(); after > before+2 {
		t.Fatalf("goroutine leak: before=%d after=%d", before, after)
	}
}

func newTestProber(caps AXFRCaps) *AXFRProber {
	sched := scheduler.New(scheduler.Config{PerServerQPS: 100, MaxInFlight: 1})
	return NewAXFRProber(sched, caps, 8)
}

func TestProbeOpenZone(t *testing.T) {
	addr, stop := startAXFRServer(t, axfrZone(t), false)
	defer stop()
	p := newTestProber(AXFRCaps{MaxRecords: 50000, MaxBytes: 64 << 20, Deadline: 5 * time.Second})
	res := p.Probe(context.Background(), "example.com", []string{addr})
	if !res.Open || len(res.Zone) != 5 {
		t.Fatalf("want open zone with 5 records, got Open=%v len=%d", res.Open, len(res.Zone))
	}
}

func TestProbeSkipsHyperscalerOnlyNSSet(t *testing.T) {
	// 104.16.1.1 is a Cloudflare anycast NS IP (hyperscaler). An all-hyperscaler NS set must be
	// skipped without dialing.
	//
	// NOTE: the task brief for this test used "1.1.1.1", asserting it is "Cloudflare (hyperscaler)".
	// That is incorrect against the already-committed scheduler.IsHyperscaler: internal/scheduler
	// /providers_test.go (Task 1) explicitly asserts IsHyperscaler("1.1.1.1") == false, with the
	// comment "public resolvers, not in the auth ranges" — 1.1.1.1 is Cloudflare's public recursive
	// resolver service, not part of the authoritative anycast NS CIDR ranges an AXFR target would use.
	// Adding 1.1.1.1 to hyperscalerCIDRs would break that pre-existing, deliberately-authored test, so
	// this test uses 104.16.1.1 instead (confirmed hyperscaler in the same providers_test.go) to
	// exercise the identical skip-logic path without touching Task 1's contract.
	p := newTestProber(AXFRCaps{MaxRecords: 50000, MaxBytes: 64 << 20, Deadline: time.Second})
	res := p.Probe(context.Background(), "example.com", []string{"104.16.1.1"})
	if res.Open {
		t.Fatal("hyperscaler-only NS set should be skipped, not open")
	}
	if res.Server != "" {
		t.Fatalf("skipped probe should not name a server, got %q", res.Server)
	}
}

func TestProbeDedupsRefusedNSSet(t *testing.T) {
	// A refusing server: the first probe transfers, the second short-circuits on the NS-set verdict.
	var dials int
	addr, stop := startCountingRefuser(t, &dials)
	defer stop()
	p := newTestProber(AXFRCaps{MaxRecords: 10, MaxBytes: 1 << 20, Deadline: time.Second})
	_ = p.Probe(context.Background(), "a.example", []string{addr})
	_ = p.Probe(context.Background(), "b.example", []string{addr})
	if dials != 1 {
		t.Fatalf("want 1 dial (second deduped), got %d", dials)
	}
}

func TestTransferAXFRTruncatedByRecordCap(t *testing.T) {
	addr, stop := startAXFRServer(t, axfrZone(t), false)
	defer stop()
	caps := AXFRCaps{MaxRecords: 3, MaxBytes: 64 << 20, Deadline: 5 * time.Second}
	res, err := transferAXFR(context.Background(), "example.com", addr, caps)
	if err != nil {
		t.Fatalf("transfer: %v", err)
	}
	if !res.Open || !res.Truncated {
		t.Fatalf("want Open=true Truncated=true, got Open=%v Truncated=%v", res.Open, res.Truncated)
	}
	if res.Records != 3 {
		t.Fatalf("want capped at 3 records, got %d", res.Records)
	}
}
