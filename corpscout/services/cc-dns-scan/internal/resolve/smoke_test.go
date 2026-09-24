//go:build integration

package resolve

import (
	"context"
	"os"
	"testing"
	"time"

	"cc-dns-scan/internal/records"
	"cc-dns-scan/internal/scheduler"
)

func TestSmokeRealDomains(t *testing.T) {
	discSched := scheduler.New(scheduler.Config{PerServerQPS: 50, Burst: 50, MaxInFlight: 3})
	authSched := scheduler.New(scheduler.Config{PerServerQPS: 10, Burst: 10, MaxInFlight: 3})
	// Production mandates a local recursive resolver; this real-DNS smoke just needs any working
	// recursive resolver — default to a public one, override via SMOKE_RESOLVER (e.g. 127.0.0.1:53).
	resolver := "1.1.1.1:53"
	if v := os.Getenv("SMOKE_RESOLVER"); v != "" {
		resolver = v
	}
	disc := NewDiscoverer(NewExchanger(discSched, 5*time.Second), []string{resolver})
	r := NewResolver(NewExchanger(authSched, 5*time.Second))
	ctx := context.Background()

	del, err := disc.DiscoverNS(ctx, "cloudflare.com")
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if len(del.NS) == 0 || len(del.NSIPs) == 0 {
		t.Fatalf("no NS learned: %+v", del)
	}
	res := r.Resolve(ctx, "cloudflare.com", "smoke", "smoke", del, records.DefaultConfig(), time.Now().UTC(), nil)
	var haveMX, haveA bool
	for _, rec := range res.Records {
		if rec.RecordType == "MX" {
			haveMX = true
		}
		if rec.RecordType == "A" {
			haveA = true
		}
	}
	if !haveMX {
		t.Errorf("expected MX for cloudflare.com")
	}
	if !haveA {
		t.Errorf("expected some A records")
	}
}
