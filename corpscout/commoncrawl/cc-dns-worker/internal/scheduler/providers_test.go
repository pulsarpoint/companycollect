package scheduler

import "testing"

func TestIsHyperscaler(t *testing.T) {
	// Confirmed live-scan Cloudflare NS IPs + representative Google/AWS ranges.
	yes := []string{
		"172.64.53.25", "162.159.48.223", "172.64.52.239", // Cloudflare (observed)
		"104.16.1.1", "173.245.49.10", // Cloudflare
		"216.239.32.10", "216.239.38.10", // Google ns-cloud
		"205.251.195.10",                     // AWS Route 53
		"156.154.132.200", "156.154.133.200", // UltraDNS (Namecheap, observed jamming)
		"204.74.108.1", // UltraDNS
	}
	no := []string{
		"8.8.8.8", "1.1.1.1", // public resolvers, not in the auth ranges
		"192.0.2.10", "203.0.113.5", // TEST-NET (a self-hosted NS)
		"", "not-an-ip",
	}
	for _, ip := range yes {
		if !IsHyperscaler(ip) {
			t.Errorf("IsHyperscaler(%q) = false, want true", ip)
		}
	}
	for _, ip := range no {
		if IsHyperscaler(ip) {
			t.Errorf("IsHyperscaler(%q) = true, want false", ip)
		}
	}
}

func TestHyperscalerGetsElevatedLimits(t *testing.T) {
	s := New(Config{PerServerQPS: 10, Burst: 10, MaxInFlight: 3, HyperscalerQPS: 200, HyperscalerInFlight: 40})

	cf := s.forServer("172.64.53.25") // Cloudflare
	if got := float64(cf.lim.Limit()); got != 200 {
		t.Errorf("cloudflare rate = %v, want 200", got)
	}
	if got := cap(cf.slot); got != 40 {
		t.Errorf("cloudflare in-flight = %d, want 40", got)
	}
	if got := cf.lim.Burst(); got != 200 {
		t.Errorf("cloudflare burst = %d, want 200 (elevated so the rate can actually burst)", got)
	}

	small := s.forServer("192.0.2.10") // self-hosted NS keeps the polite default
	if got := float64(small.lim.Limit()); got != 10 {
		t.Errorf("small NS rate = %v, want 10", got)
	}
	if got := cap(small.slot); got != 3 {
		t.Errorf("small NS in-flight = %d, want 3", got)
	}
}

func TestHyperscalerOverrideDisabledByDefault(t *testing.T) {
	// HyperscalerQPS unset (0) => even Cloudflare paced at the default.
	s := New(Config{PerServerQPS: 10, Burst: 10, MaxInFlight: 3})
	cf := s.forServer("172.64.53.25")
	if got := float64(cf.lim.Limit()); got != 10 {
		t.Errorf("with override off, cloudflare rate = %v, want 10", got)
	}
}
