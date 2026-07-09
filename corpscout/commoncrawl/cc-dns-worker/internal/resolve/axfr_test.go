package resolve

import (
	"context"
	"runtime"
	"strconv"
	"testing"
	"time"

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
