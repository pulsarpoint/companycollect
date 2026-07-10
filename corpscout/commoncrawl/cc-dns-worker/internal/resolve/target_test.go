package resolve

import (
	"net/netip"
	"testing"
)

// TestClassifyAddr is the table-driven core of the classification contract: every named AddrScope
// must land on the address ranges the spec assigns it, for both IPv4 and IPv6 where applicable.
func TestClassifyAddr(t *testing.T) {
	tests := []struct {
		name string
		ip   string
		want AddrScope
	}{
		// allow: public.
		{"public v4", "8.8.8.8", ScopePublic},
		{"public v4 other", "93.184.216.34", ScopePublic},
		{"public v6", "2001:4860:4860::8888", ScopePublic},

		// deny: loopback.
		{"loopback v4", "127.0.0.1", ScopeLoopback},
		{"loopback v6", "::1", ScopeLoopback},

		// deny: RFC1918 private + IPv6 ULA.
		{"private v4 10/8", "10.1.2.3", ScopePrivate},
		{"private v4 172.16/12", "172.16.5.6", ScopePrivate},
		{"private v4 192.168/16", "192.168.1.1", ScopePrivate},
		{"private v6 ULA fc00::", "fc00::1", ScopePrivate},

		// deny: link-local.
		{"link-local v4", "169.254.1.1", ScopeLinkLocal},
		{"link-local v6", "fe80::1", ScopeLinkLocal},

		// deny: CGNAT (RFC6598).
		{"cgnat v4", "100.64.0.1", ScopeCGNAT},
		{"cgnat v4 upper end", "100.127.255.254", ScopeCGNAT},

		// deny: documentation ranges (RFC5737 + RFC3849).
		{"documentation TEST-NET-1", "192.0.2.1", ScopeDocumentation},
		{"documentation TEST-NET-2", "198.51.100.7", ScopeDocumentation},
		{"documentation TEST-NET-3", "203.0.113.9", ScopeDocumentation},
		{"documentation v6", "2001:db8::1", ScopeDocumentation},

		// deny: benchmarking (RFC2544).
		{"benchmark v4", "198.18.0.1", ScopeBenchmark},
		{"benchmark v4 upper end", "198.19.255.254", ScopeBenchmark},

		// deny: multicast (includes link-local multicast — it's still multicast, not link_local).
		{"multicast v4", "224.0.0.1", ScopeMulticast},
		{"multicast v6", "ff02::1", ScopeMulticast},

		// deny: unspecified.
		{"unspecified v4", "0.0.0.0", ScopeUnspecified},
		{"unspecified v6", "::", ScopeUnspecified},

		// deny: reserved catch-all (Class E "future use", not covered by any named range above).
		{"reserved v4 240/4", "240.0.0.1", ScopeReserved},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			addr, err := netip.ParseAddr(tc.ip)
			if err != nil {
				t.Fatalf("ParseAddr(%q): %v", tc.ip, err)
			}
			if got := ClassifyAddr(addr); got != tc.want {
				t.Errorf("ClassifyAddr(%s) = %s, want %s", tc.ip, got, tc.want)
			}
		})
	}
}

// TestClassifyAddrUnmapsIPv4in6 proves an IPv4-in-IPv6 address (::ffff:a.b.c.d) classifies the same as
// its bare IPv4 form — Unmap must run before any range check, not after.
func TestClassifyAddrUnmapsIPv4in6(t *testing.T) {
	tests := []struct {
		mapped string
		want   AddrScope
	}{
		{"::ffff:8.8.8.8", ScopePublic},
		{"::ffff:127.0.0.1", ScopeLoopback},
		{"::ffff:10.0.0.1", ScopePrivate},
		{"::ffff:169.254.1.1", ScopeLinkLocal},
		{"::ffff:100.64.0.1", ScopeCGNAT},
	}
	for _, tc := range tests {
		addr, err := netip.ParseAddr(tc.mapped)
		if err != nil {
			t.Fatalf("ParseAddr(%q): %v", tc.mapped, err)
		}
		if got := ClassifyAddr(addr); got != tc.want {
			t.Errorf("ClassifyAddr(%s) = %s, want %s", tc.mapped, got, tc.want)
		}
	}
}

// TestClassifyAddrInvalid proves the zero-value (invalid) Addr classifies as ScopeReserved rather than
// panicking or falling through to ScopePublic.
func TestClassifyAddrInvalid(t *testing.T) {
	var zero netip.Addr
	if got := ClassifyAddr(zero); got != ScopeReserved {
		t.Errorf("ClassifyAddr(invalid) = %s, want %s", got, ScopeReserved)
	}
}

// TestClassifyString covers the string-parsing entry point: valid IPs classify exactly like
// ClassifyAddr, and anything that isn't a bare IP address reports ok=false.
func TestClassifyString(t *testing.T) {
	if scope, ok := ClassifyString("8.8.8.8"); !ok || scope != ScopePublic {
		t.Errorf("ClassifyString(8.8.8.8) = (%s, %v), want (%s, true)", scope, ok, ScopePublic)
	}
	if scope, ok := ClassifyString("10.0.0.1"); !ok || scope != ScopePrivate {
		t.Errorf("ClassifyString(10.0.0.1) = (%s, %v), want (%s, true)", scope, ok, ScopePrivate)
	}
	for _, bad := range []string{"", "not-an-ip", "ns1.example.com", "8.8.8.8:53", "2001:db8::1/32"} {
		if scope, ok := ClassifyString(bad); ok {
			t.Errorf("ClassifyString(%q) = (%s, true), want ok=false", bad, scope)
		}
	}
}

// TestDialable proves ONLY ScopePublic is dialable — every other named scope, including ones not yet
// invented, must default to non-dialable.
func TestDialable(t *testing.T) {
	tests := []struct {
		scope AddrScope
		want  bool
	}{
		{ScopePublic, true},
		{ScopePrivate, false},
		{ScopeLoopback, false},
		{ScopeLinkLocal, false},
		{ScopeCGNAT, false},
		{ScopeDocumentation, false},
		{ScopeBenchmark, false},
		{ScopeMulticast, false},
		{ScopeUnspecified, false},
		{ScopeReserved, false},
		{AddrScope("made_up"), false},
	}
	for _, tc := range tests {
		if got := Dialable(tc.scope); got != tc.want {
			t.Errorf("Dialable(%s) = %v, want %v", tc.scope, got, tc.want)
		}
	}
}
