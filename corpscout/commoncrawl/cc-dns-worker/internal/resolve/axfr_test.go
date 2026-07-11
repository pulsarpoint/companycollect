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

// assertNoGoroutineLeak polls for runtime.NumGoroutine() to settle back to at most before+slack, so a
// deferred watchdog/cleanup goroutine that hasn't unwound yet doesn't flake the assertion.
func assertNoGoroutineLeak(t *testing.T, before int) {
	t.Helper()
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

func TestTransferAXFROpen(t *testing.T) {
	addr, stop := startAXFRServer(t, axfrZone(t), false)
	defer stop()
	caps := AXFRCaps{MaxRecords: 50000, MaxBytes: 64 << 20, Deadline: 5 * time.Second}

	before := runtime.NumGoroutine()
	res := transferAXFR(context.Background(), "example.com", addr, caps)
	assertNoGoroutineLeak(t, before)

	if res.Verdict != VerdictOpen {
		t.Fatalf("want Verdict=open, got %v (reason=%v)", res.Verdict, res.Reason)
	}
	if res.Reason != ReasonTransferred {
		t.Fatalf("want Reason=transferred, got %v", res.Reason)
	}
	if res.Truncated {
		t.Fatal("want Truncated=false")
	}
	if res.Records != 5 || len(res.Zone) != 5 {
		t.Fatalf("want 5 records, got Records=%d len(Zone)=%d", res.Records, len(res.Zone))
	}
	if res.ObservedAt.IsZero() {
		t.Fatal("want ObservedAt set")
	}
	for _, rec := range res.Zone {
		if rec.Source != "axfr" {
			t.Fatalf("want Source=axfr, got %q", rec.Source)
		}
	}
}

func TestTransferAXFRStoresMixedAndUnknownRecordTypes(t *testing.T) {
	rrs := []dns.RR{
		mustRR(t, "example.com. 3600 IN SOA ns1.example.com. hostmaster.example.com. 1 7200 3600 1209600 3600"),
		mustRR(t, `example.com. 300 IN CAA 0 issue "letsencrypt.org"`),
		mustRR(t, `example.com. 300 IN NAPTR 10 20 "S" "SIP+D2U" "" _sip._udp.example.com.`),
		mustRR(t, `unknown.example.com. 60 IN TYPE65400 \# 4 DEADBEEF`),
		mustRR(t, "example.com. 3600 IN SOA ns1.example.com. hostmaster.example.com. 1 7200 3600 1209600 3600"),
	}
	addr, stop := startAXFRServer(t, rrs, false)
	defer stop()

	result := transferAXFR(context.Background(), "example.com", addr, AXFRCaps{
		MaxRecords: 100, MaxBytes: 1 << 20, Deadline: 5 * time.Second,
	})
	if result.Verdict != VerdictOpen || result.Records != len(rrs) || len(result.Zone) != len(rrs) {
		t.Fatalf("mixed AXFR lost records: verdict=%s records=%d zone=%d", result.Verdict, result.Records, len(result.Zone))
	}
	if result.Zone[3].TypeCode != 65400 || result.Zone[3].RDataWire == "" {
		t.Errorf("unknown AXFR RR lost: %+v", result.Zone[3])
	}
}

func TestTransferAXFRRefused(t *testing.T) {
	addr, stop := startAXFRServer(t, nil, true)
	defer stop()
	caps := AXFRCaps{MaxRecords: 50000, MaxBytes: 64 << 20, Deadline: 5 * time.Second}

	res := transferAXFR(context.Background(), "example.com", addr, caps)
	if res.Verdict != VerdictClosed || res.Reason != ReasonRefused {
		t.Fatalf("want closed/refused, got %v/%v", res.Verdict, res.Reason)
	}
	if len(res.Zone) != 0 {
		t.Fatalf("want empty zone, got %d", len(res.Zone))
	}
}

func TestTransferAXFRNotAuth(t *testing.T) {
	addr, stop := startAXFRServerRcode(t, dns.RcodeNotAuth)
	defer stop()
	caps := AXFRCaps{Deadline: 5 * time.Second}

	res := transferAXFR(context.Background(), "example.com", addr, caps)
	if res.Verdict != VerdictClosed || res.Reason != ReasonNotAuth {
		t.Fatalf("want closed/notauth, got %v/%v", res.Verdict, res.Reason)
	}
}

func TestTransferAXFRServfail(t *testing.T) {
	addr, stop := startAXFRServerRcode(t, dns.RcodeServerFailure)
	defer stop()
	caps := AXFRCaps{Deadline: 5 * time.Second}

	res := transferAXFR(context.Background(), "example.com", addr, caps)
	if res.Verdict != VerdictUnknown || res.Reason != ReasonServfail {
		t.Fatalf("want unknown/servfail, got %v/%v", res.Verdict, res.Reason)
	}
}

func TestTransferAXFRMalformed(t *testing.T) {
	addr, stop := startAXFRServerMalformed(t)
	defer stop()
	caps := AXFRCaps{Deadline: 5 * time.Second}

	res := transferAXFR(context.Background(), "example.com", addr, caps)
	if res.Verdict != VerdictUnknown || res.Reason != ReasonMalformed {
		t.Fatalf("want unknown/malformed, got %v/%v", res.Verdict, res.Reason)
	}
}

func TestTransferAXFREarlyDisconnect(t *testing.T) {
	addr, stop := startAXFRServerEarlyDisconnect(t)
	defer stop()
	caps := AXFRCaps{Deadline: 5 * time.Second}

	before := runtime.NumGoroutine()
	res := transferAXFR(context.Background(), "example.com", addr, caps)
	assertNoGoroutineLeak(t, before)

	if res.Verdict != VerdictUnknown || res.Reason != ReasonTransportError {
		t.Fatalf("want unknown/transport_error, got %v/%v", res.Verdict, res.Reason)
	}
}

func TestTransferAXFRTimeout(t *testing.T) {
	addr, stop := startAXFRServerNoReply(t)
	caps := AXFRCaps{Deadline: 150 * time.Millisecond}

	before := runtime.NumGoroutine()
	start := time.Now()
	res := transferAXFR(context.Background(), "example.com", addr, caps)
	elapsed := time.Since(start)
	stop() // release the held connection before checking for leaks
	assertNoGoroutineLeak(t, before)

	if res.Verdict != VerdictUnknown || res.Reason != ReasonTimeout {
		t.Fatalf("want unknown/timeout, got %v/%v", res.Verdict, res.Reason)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("preflight did not respect its deadline: took %s", elapsed)
	}
}

func TestTransferAXFRContextCancelled(t *testing.T) {
	addr, stop := startAXFRServerNoReply(t)
	caps := AXFRCaps{Deadline: 5 * time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled before the dial ever starts

	before := runtime.NumGoroutine()
	start := time.Now()
	res := transferAXFR(ctx, "example.com", addr, caps)
	elapsed := time.Since(start)
	stop()
	assertNoGoroutineLeak(t, before)

	if res.Verdict != VerdictUnknown || res.Reason != ReasonCancelled {
		t.Fatalf("want unknown/cancelled, got %v/%v", res.Verdict, res.Reason)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("cancellation not honored promptly: took %s", elapsed)
	}
}

func TestProbeServerBlocksNonPublicTargetAtSocketBoundary(t *testing.T) {
	p := newTestProber(AXFRCaps{Deadline: time.Second})
	for _, target := range []string{"127.0.0.1:53", "10.0.0.1", "0.0.0.1", "100::1"} {
		res := p.ProbeServer(context.Background(), "example.com", "ns1.example.com", target)
		if res.Verdict != VerdictUnknown || res.Reason != ReasonSkipped {
			t.Errorf("ProbeServer(%q) = %v/%v, want unknown/skipped", target, res.Verdict, res.Reason)
		}
		if res.NSHost != "ns1.example.com" || res.NSIP != target {
			t.Errorf("ProbeServer(%q) lost endpoint identity: host=%q ip=%q", target, res.NSHost, res.NSIP)
		}
	}
}

func TestTransferAXFROpenTruncatedByMidStreamDisconnect(t *testing.T) {
	// SOA-first + two A records, but NO closing SOA: the preflight sees the leading SOA (Open), then
	// the collection connection collects those RRs and hits a read error when the server drops the
	// connection. That must report Verdict=open (the preflight already proved the transfer opens) with
	// Truncated=true — a partial pull of an open zone, not a different verdict.
	rrs := []dns.RR{
		mustRR(t, "example.com. 3600 IN SOA ns1.example.com. hostmaster.example.com. 1 7200 3600 1209600 3600"),
		mustRR(t, "www.example.com. 3600 IN A 1.2.3.4"),
		mustRR(t, "mail.example.com. 3600 IN A 5.6.7.8"),
	}
	addr, stop := startAXFRServerAbrupt(t, rrs)
	defer stop()
	caps := AXFRCaps{MaxRecords: 50000, MaxBytes: 64 << 20, Deadline: 5 * time.Second}

	before := runtime.NumGoroutine()
	res := transferAXFR(context.Background(), "example.com", addr, caps)
	assertNoGoroutineLeak(t, before)

	if res.Verdict != VerdictOpen {
		t.Fatalf("want Verdict=open despite the mid-stream drop, got %v (reason=%v)", res.Verdict, res.Reason)
	}
	if !res.Truncated {
		t.Fatal("want Truncated=true on a mid-stream drop")
	}
	if res.Records == 0 {
		t.Fatal("want at least the records collected before the drop")
	}
}

func TestTransferAXFRTruncatedByRecordCap(t *testing.T) {
	addr, stop := startAXFRServer(t, axfrZone(t), false)
	defer stop()
	caps := AXFRCaps{MaxRecords: 3, MaxBytes: 64 << 20, Deadline: 5 * time.Second}

	res := transferAXFR(context.Background(), "example.com", addr, caps)
	if res.Verdict != VerdictOpen || !res.Truncated {
		t.Fatalf("want Verdict=open Truncated=true, got Verdict=%v Truncated=%v", res.Verdict, res.Truncated)
	}
	if res.Records != 3 {
		t.Fatalf("want capped at 3 records, got %d", res.Records)
	}
}

func TestTransferAXFRTruncatedByByteCap(t *testing.T) {
	addr, stop := startAXFRServer(t, axfrZone(t), false)
	defer stop()
	// MaxBytes=1 caps after the first RR's bytes are folded in, regardless of its exact wire size.
	caps := AXFRCaps{MaxRecords: 50000, MaxBytes: 1, Deadline: 5 * time.Second}

	res := transferAXFR(context.Background(), "example.com", addr, caps)
	if res.Verdict != VerdictOpen || !res.Truncated {
		t.Fatalf("want Verdict=open Truncated=true, got Verdict=%v Truncated=%v", res.Verdict, res.Truncated)
	}
	if res.Records != 1 {
		t.Fatalf("want capped at 1 record, got %d", res.Records)
	}
	if res.Bytes == 0 {
		t.Fatal("want Bytes to reflect the one collected record")
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
	res := transferAXFR(context.Background(), "example.com", addr, caps)

	if res.Verdict != VerdictOpen {
		t.Fatalf("want Verdict=open, got %v (reason=%v)", res.Verdict, res.Reason)
	}
	if !res.Truncated {
		t.Fatal("want Truncated=true (cap fired mid-stream)")
	}
	if res.Records != 5 {
		t.Fatalf("want capped at 5 records, got %d", res.Records)
	}

	// Poll for the producer goroutines (preflight + collection connections) and their sockets to be
	// reclaimed rather than parked forever.
	assertNoGoroutineLeak(t, before)
}

func newTestProber(caps AXFRCaps) *AXFRProber {
	sched := scheduler.New(scheduler.Config{PerServerQPS: 100, MaxInFlight: 1})
	return NewAXFRProber(sched, caps, 8)
}
