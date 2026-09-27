//go:build live

package feeds

import (
	"context"
	"testing"
	"time"
)

// TestLive hits every real publication. Run with `make live` when a feed
// format may have changed; never in CI.
func TestLive(t *testing.T) {
	params := map[string]map[string]string{
		"geofeed":            {"url": "https://digitalocean.com/geo/google.csv"},
		"ripestat_announced": {"asn": "24940"},
	}
	minimum := map[string]int{
		"aws_ip_ranges": 1000, "azure_service_tags": 1000, "google_cloud": 100, "google_goog": 50,
		"cloudflare_ips": 10, "fastly_public_ips": 10, "bunny_edge_servers": 50,
		"oracle_public_ip_ranges": 100, "github_meta": 10, "geofeed": 100, "ripestat_announced": 10,
	}
	f := NewFetcher()
	for name, c := range Registry() {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			res, err := c.Collect(ctx, f, params[name])
			if err != nil {
				t.Fatal(err)
			}
			if len(res.Ranges) < minimum[name] {
				t.Fatalf("%d ranges, want >= %d", len(res.Ranges), minimum[name])
			}
			t.Logf("%s: %d ranges, %d skipped, version %s", name, len(res.Ranges), res.Skipped, res.SourceVersion)
		})
	}
}
