package resolve

import (
	"context"
	"testing"

	"cc-dns-scan/internal/model"
)

// TestDiscoverNSEndpointsMultipleIPsOneName covers one NS hostname that resolves to several addresses:
// each (hostname, ip) pair must land as its own Endpoints entry, not collapse into a single mapping.
func TestDiscoverNSEndpointsMultipleIPsOneName(t *testing.T) {
	addr, stop := startAuth(t, zone{
		"example.com./NS": {mustRR(t, "example.com. 300 IN NS ns1.example.com.")},
		"ns1.example.com./A": {
			mustRR(t, "ns1.example.com. 300 IN A 8.8.8.8"),
			mustRR(t, "ns1.example.com. 300 IN A 8.8.4.4"),
		},
	})
	defer stop()

	d := NewDiscoverer(newTestExchanger(), []string{addr})
	del, err := d.DiscoverNS(context.Background(), "example.com")
	if err != nil {
		t.Fatalf("discover: %v", err)
	}

	if len(del.Endpoints) != 2 {
		t.Fatalf("Endpoints = %+v, want 2 entries (one per IP)", del.Endpoints)
	}
	wantIPs := map[string]bool{"8.8.8.8": false, "8.8.4.4": false}
	for _, ep := range del.Endpoints {
		if ep.Name != "ns1.example.com" {
			t.Errorf("endpoint %+v: Name = %q, want ns1.example.com", ep, ep.Name)
		}
		if _, ok := wantIPs[ep.IP]; !ok {
			t.Errorf("endpoint %+v: unexpected IP", ep)
			continue
		}
		wantIPs[ep.IP] = true
		if ep.Scope != string(ScopePublic) || !ep.Dialable {
			t.Errorf("endpoint %+v: want Scope=public Dialable=true", ep)
		}
	}
	for ip, seen := range wantIPs {
		if !seen {
			t.Errorf("missing endpoint for IP %s", ip)
		}
	}
	if len(del.NSIPs) != 2 {
		t.Errorf("NSIPs = %v, want both addresses (derived from Endpoints)", del.NSIPs)
	}
}

// TestDiscoverNSEndpointsSharedIPAcrossNames covers two distinct NS hostnames that resolve to the SAME
// address (e.g. shared/anycast infrastructure): both (hostname, ip) pairs must be preserved as distinct
// endpoints, while the derived NSIPs view dedups down to the single distinct address.
func TestDiscoverNSEndpointsSharedIPAcrossNames(t *testing.T) {
	addr, stop := startAuth(t, zone{
		"example.com./NS": {
			mustRR(t, "example.com. 300 IN NS ns1.example.com."),
			mustRR(t, "example.com. 300 IN NS ns2.example.com."),
		},
		"ns1.example.com./A": {mustRR(t, "ns1.example.com. 300 IN A 9.9.9.9")},
		"ns2.example.com./A": {mustRR(t, "ns2.example.com. 300 IN A 9.9.9.9")},
	})
	defer stop()

	d := NewDiscoverer(newTestExchanger(), []string{addr})
	del, err := d.DiscoverNS(context.Background(), "example.com")
	if err != nil {
		t.Fatalf("discover: %v", err)
	}

	if len(del.Endpoints) != 2 {
		t.Fatalf("Endpoints = %+v, want 2 distinct (name, ip) pairs sharing one IP", del.Endpoints)
	}
	wantNames := map[string]bool{"ns1.example.com": false, "ns2.example.com": false}
	for _, ep := range del.Endpoints {
		if ep.IP != "9.9.9.9" {
			t.Errorf("endpoint %+v: IP = %q, want 9.9.9.9 (shared)", ep, ep.IP)
		}
		if _, ok := wantNames[ep.Name]; !ok {
			t.Errorf("endpoint %+v: unexpected Name", ep)
			continue
		}
		wantNames[ep.Name] = true
	}
	for name, seen := range wantNames {
		if !seen {
			t.Errorf("missing endpoint for NS name %s", name)
		}
	}
	// The derived IP view dedups by address alone: one shared IP counts once, even though it backs two
	// distinct NS names.
	if len(del.NSIPs) != 1 || del.NSIPs[0] != "9.9.9.9" {
		t.Errorf("NSIPs = %v, want exactly [9.9.9.9] (deduped across NS names)", del.NSIPs)
	}
	if len(del.DialableNSIPs) != 1 || del.DialableNSIPs[0] != "9.9.9.9" {
		t.Errorf("DialableNSIPs = %v, want exactly [9.9.9.9]", del.DialableNSIPs)
	}
}

// TestDiscoverNSDerivedFieldsMatchOldSemanticsAllPublic is the compatibility test: for an ordinary
// all-public delegation, NS/NSIPs/DialableNSIPs — now derived from Endpoints — must come out exactly as
// the old flat-list algorithm produced them, in the same order, so no downstream consumer of Delegation
// (query.go's Resolve, cmd/scan.go's resolveDomain) observes any behavior change.
func TestDiscoverNSDerivedFieldsMatchOldSemanticsAllPublic(t *testing.T) {
	addr, stop := startAuth(t, zone{
		"example.com./NS": {
			mustRR(t, "example.com. 300 IN NS ns1.example.com."),
			mustRR(t, "example.com. 300 IN NS ns2.example.net."),
		},
		"ns1.example.com./A": {mustRR(t, "ns1.example.com. 300 IN A 1.1.1.1")},
		"ns2.example.net./A": {mustRR(t, "ns2.example.net. 300 IN A 2.2.2.2")},
	})
	defer stop()

	d := NewDiscoverer(newTestExchanger(), []string{addr})
	del, err := d.DiscoverNS(context.Background(), "example.com")
	if err != nil {
		t.Fatalf("discover: %v", err)
	}

	wantNS := []string{"ns1.example.com", "ns2.example.net"}
	if len(del.NS) != len(wantNS) {
		t.Fatalf("NS = %v, want %v", del.NS, wantNS)
	}
	for i, name := range wantNS {
		if del.NS[i] != name {
			t.Errorf("NS[%d] = %q, want %q", i, del.NS[i], name)
		}
	}

	wantIPs := []string{"1.1.1.1", "2.2.2.2"}
	if len(del.NSIPs) != len(wantIPs) {
		t.Fatalf("NSIPs = %v, want %v", del.NSIPs, wantIPs)
	}
	for i, ip := range wantIPs {
		if del.NSIPs[i] != ip {
			t.Errorf("NSIPs[%d] = %q, want %q", i, del.NSIPs[i], ip)
		}
	}
	// All-public: DialableNSIPs must equal NSIPs exactly (same old-semantics invariant).
	if len(del.DialableNSIPs) != len(del.NSIPs) {
		t.Fatalf("DialableNSIPs = %v, want it to equal NSIPs %v (all-public delegation)", del.DialableNSIPs, del.NSIPs)
	}
	for i := range del.NSIPs {
		if del.DialableNSIPs[i] != del.NSIPs[i] {
			t.Errorf("DialableNSIPs[%d] = %q, want %q", i, del.DialableNSIPs[i], del.NSIPs[i])
		}
	}

	// Endpoints is the new field: it must carry exactly the same identity the derived fields summarize.
	want := []model.NameserverEndpoint{
		{Name: "ns1.example.com", IP: "1.1.1.1", Scope: string(ScopePublic), Dialable: true},
		{Name: "ns2.example.net", IP: "2.2.2.2", Scope: string(ScopePublic), Dialable: true},
	}
	if len(del.Endpoints) != len(want) {
		t.Fatalf("Endpoints = %+v, want %+v", del.Endpoints, want)
	}
	for i, w := range want {
		if del.Endpoints[i] != w {
			t.Errorf("Endpoints[%d] = %+v, want %+v", i, del.Endpoints[i], w)
		}
	}
}
