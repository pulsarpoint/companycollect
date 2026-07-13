package resolve

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/miekg/dns"
)

// startAuthCapture is a variant of startAuth that additionally invokes onReq with every incoming
// request before the zone-driven reply is built, so tests can inspect what the client actually sent
// (e.g. EDNS0/DO) rather than just what came back.
func startAuthCapture(t *testing.T, z zone, onReq func(*dns.Msg)) (string, func()) {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	mux := dns.NewServeMux()
	mux.HandleFunc(".", func(w dns.ResponseWriter, r *dns.Msg) {
		if onReq != nil {
			onReq(r)
		}
		m := new(dns.Msg)
		m.SetReply(r)
		q := r.Question[0]
		if rrs, ok := z[q.Name+"/"+dns.TypeToString[q.Qtype]]; ok {
			m.Answer = append(m.Answer, rrs...)
		}
		_ = w.WriteMsg(m)
	})
	srv := &dns.Server{PacketConn: pc, Handler: mux}
	go func() { _ = srv.ActivateAndServe() }()
	return pc.LocalAddr().String(), func() { _ = srv.Shutdown() }
}

// startServfail starts an in-process server that answers every query with SERVFAIL, regardless of
// question name/type, to exercise the retryable-error/rotation path in Discoverer.query.
func startServfail(t *testing.T) (string, func()) {
	return startRcode(t, dns.RcodeServerFailure)
}

func startRcode(t *testing.T, rcode int) (string, func()) {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	mux := dns.NewServeMux()
	mux.HandleFunc(".", func(w dns.ResponseWriter, r *dns.Msg) {
		m := new(dns.Msg)
		m.SetReply(r)
		m.Rcode = rcode
		_ = w.WriteMsg(m)
	})
	srv := &dns.Server{PacketConn: pc, Handler: mux}
	go func() { _ = srv.ActivateAndServe() }()
	return pc.LocalAddr().String(), func() { _ = srv.Shutdown() }
}

func startNoReply(t *testing.T) (string, func()) {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	mux := dns.NewServeMux()
	mux.HandleFunc(".", func(dns.ResponseWriter, *dns.Msg) {})
	srv := &dns.Server{PacketConn: pc, Handler: mux}
	go func() { _ = srv.ActivateAndServe() }()
	return pc.LocalAddr().String(), func() { _ = srv.Shutdown() }
}

// TestExchangeSetsEDNSAndDO verifies every outgoing query carries an EDNS0 OPT record with the DO
// (DNSSEC OK) bit set, per exchange.go's m.SetEdns0(1232, true) on every Exchange call.
func TestExchangeSetsEDNSAndDO(t *testing.T) {
	var (
		mu       sync.Mutex
		captured *dns.Msg
	)
	addr, stop := startAuthCapture(t, zone{
		"example.com./A": {mustRR(t, "example.com. 300 IN A 93.184.216.34")},
	}, func(r *dns.Msg) {
		mu.Lock()
		defer mu.Unlock()
		captured = r.Copy()
	})
	defer stop()

	ex := newTestExchanger()
	m := new(dns.Msg)
	m.SetQuestion("example.com.", dns.TypeA)
	if _, err := ex.Exchange(context.Background(), m, addr); err != nil {
		t.Fatalf("exchange: %v", err)
	}

	mu.Lock()
	req := captured
	mu.Unlock()
	if req == nil {
		t.Fatal("server never captured a request")
	}
	opt := req.IsEdns0()
	if opt == nil {
		t.Fatal("request has no EDNS0 OPT record")
	}
	if !opt.Do() {
		t.Error("EDNS0 OPT present but DO bit not set")
	}
}

// TestDiscoverNSNoNS covers the negative path where the resolver has no NS records at all for the
// domain: DiscoverNS must return a non-nil "no NS records" error and leave Delegation.NS empty.
func TestDiscoverNSNoNS(t *testing.T) {
	addr, stop := startAuth(t, zone{})
	defer stop()

	d := NewDiscoverer(newTestExchanger(), []string{addr})
	del, err := d.DiscoverNS(context.Background(), "example.com")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "no NS records") {
		t.Errorf("error = %q, want to contain %q", err.Error(), "no NS records")
	}
	if len(del.NS) != 0 {
		t.Errorf("NS = %v, want empty", del.NS)
	}
}

// TestDiscoverNSNoNSIPs covers the negative path where NS records resolve but none of the NS
// hostnames have any A/AAAA: DiscoverNS must return a non-nil "no NS IPs resolved" error, with NS
// populated but NSIPs empty.
func TestDiscoverNSNoNSIPs(t *testing.T) {
	addr, stop := startAuth(t, zone{
		"example.com./NS": {
			mustRR(t, "example.com. 300 IN NS ns1.example.com."),
		},
	})
	defer stop()

	d := NewDiscoverer(newTestExchanger(), []string{addr})
	del, err := d.DiscoverNS(context.Background(), "example.com")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "no NS IPs resolved") {
		t.Errorf("error = %q, want to contain %q", err.Error(), "no NS IPs resolved")
	}
	if len(del.NS) == 0 {
		t.Errorf("NS = %v, want populated", del.NS)
	}
	if len(del.NSIPs) != 0 {
		t.Errorf("NSIPs = %v, want empty", del.NSIPs)
	}
}

// TestDiscoverNSRotatesOnServfail proves Discoverer.query treats SERVFAIL as retryable: the first
// resolver always answers SERVFAIL, the second is a healthy zone, and discovery must still succeed
// by rotating past the failing resolver.
func TestDiscoverNSRotatesOnServfail(t *testing.T) {
	servfailAddr, stopServfail := startServfail(t)
	defer stopServfail()

	healthyAddr, stopHealthy := startAuth(t, zone{
		"example.com./NS":    {mustRR(t, "example.com. 300 IN NS ns1.example.com.")},
		"ns1.example.com./A": {mustRR(t, "ns1.example.com. 300 IN A 5.5.5.5")},
	})
	defer stopHealthy()

	d := NewDiscoverer(newTestExchanger(), []string{servfailAddr, healthyAddr})
	del, err := d.DiscoverNS(context.Background(), "example.com")
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if len(del.NS) == 0 {
		t.Errorf("NS = %v, want populated", del.NS)
	}
	if len(del.NSIPs) == 0 {
		t.Errorf("NSIPs = %v, want populated", del.NSIPs)
	}
}

// TestDiscoverNSAAAA covers the *dns.AAAA branch in discover.go's NS-IP resolution loop: an NS
// hostname with only an AAAA record must still contribute its IPv6 address to Delegation.NSIPs. Uses a
// public IPv6 address (Cloudflare's 2606:4700:4700::1111) rather than a documentation-range one so this
// test only exercises AAAA plumbing, not the target-classification path covered in target_test.go.
func TestDiscoverNSAAAA(t *testing.T) {
	addr, stop := startAuth(t, zone{
		"example.com./NS":       {mustRR(t, "example.com. 300 IN NS ns1.example.com.")},
		"ns1.example.com./AAAA": {mustRR(t, "ns1.example.com. 300 IN AAAA 2606:4700:4700::1111")},
	})
	defer stop()

	d := NewDiscoverer(newTestExchanger(), []string{addr})
	del, err := d.DiscoverNS(context.Background(), "example.com")
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	hasIP := func(ip string) bool {
		for _, x := range del.NSIPs {
			if x == ip {
				return true
			}
		}
		return false
	}
	if !hasIP("2606:4700:4700::1111") {
		t.Errorf("NSIPs = %v, want to include 2606:4700:4700::1111", del.NSIPs)
	}
}

// TestDiscoverNSMixedScopesPreservesEvidence is the core-invariant test: a delegation whose NS set
// resolves to a mix of public and non-public addresses (loopback, RFC1918, link-local, CGNAT) must keep
// EVERY address in NSIPs (evidence is never discarded for being non-public) while DialableNSIPs holds
// only the public subset.
func TestDiscoverNSMixedScopesPreservesEvidence(t *testing.T) {
	addr, stop := startAuth(t, zone{
		"example.com./NS": {
			mustRR(t, "example.com. 300 IN NS ns1.example.com."),
			mustRR(t, "example.com. 300 IN NS ns2.example.com."),
			mustRR(t, "example.com. 300 IN NS ns3.example.com."),
			mustRR(t, "example.com. 300 IN NS ns4.example.com."),
			mustRR(t, "example.com. 300 IN NS ns5.example.com."),
		},
		"ns1.example.com./A": {mustRR(t, "ns1.example.com. 300 IN A 8.8.8.8")},     // public
		"ns2.example.com./A": {mustRR(t, "ns2.example.com. 300 IN A 127.0.0.1")},   // loopback
		"ns3.example.com./A": {mustRR(t, "ns3.example.com. 300 IN A 10.0.0.1")},    // private
		"ns4.example.com./A": {mustRR(t, "ns4.example.com. 300 IN A 169.254.1.1")}, // link-local
		"ns5.example.com./A": {mustRR(t, "ns5.example.com. 300 IN A 100.64.0.1")},  // cgnat
	})
	defer stop()

	d := NewDiscoverer(newTestExchanger(), []string{addr})
	del, err := d.DiscoverNS(context.Background(), "example.com")
	if err != nil {
		t.Fatalf("discover: %v", err)
	}

	wantAll := []string{"8.8.8.8", "127.0.0.1", "10.0.0.1", "169.254.1.1", "100.64.0.1"}
	if len(del.NSIPs) != len(wantAll) {
		t.Fatalf("NSIPs = %v, want all %d addresses (full evidence): %v", del.NSIPs, len(wantAll), wantAll)
	}
	for _, ip := range wantAll {
		found := false
		for _, x := range del.NSIPs {
			if x == ip {
				found = true
			}
		}
		if !found {
			t.Errorf("NSIPs = %v, missing evidence for %s", del.NSIPs, ip)
		}
	}

	if len(del.DialableNSIPs) != 1 || del.DialableNSIPs[0] != "8.8.8.8" {
		t.Errorf("DialableNSIPs = %v, want only the public address [8.8.8.8]", del.DialableNSIPs)
	}
}

// TestDiscoverNSAllPrivateReturnsErrNoPublicNSEndpoints covers a fully-private delegation: NS resolution
// succeeds and NSIPs is non-empty (evidence preserved), but every address is non-public, so DiscoverNS
// must return the distinguishable ErrNoPublicNSEndpoints sentinel rather than the generic "no NS IPs
// resolved" transport-failure error.
func TestDiscoverNSAllPrivateReturnsErrNoPublicNSEndpoints(t *testing.T) {
	addr, stop := startAuth(t, zone{
		"example.com./NS": {
			mustRR(t, "example.com. 300 IN NS ns1.example.com."),
			mustRR(t, "example.com. 300 IN NS ns2.example.com."),
		},
		"ns1.example.com./A": {mustRR(t, "ns1.example.com. 300 IN A 10.0.0.1")},
		"ns2.example.com./A": {mustRR(t, "ns2.example.com. 300 IN A 192.168.1.1")},
	})
	defer stop()

	d := NewDiscoverer(newTestExchanger(), []string{addr})
	del, err := d.DiscoverNS(context.Background(), "example.com")

	if !errors.Is(err, ErrNoPublicNSEndpoints) {
		t.Fatalf("err = %v, want ErrNoPublicNSEndpoints", err)
	}
	if len(del.NSIPs) != 2 {
		t.Errorf("NSIPs = %v, want both private addresses preserved as evidence", del.NSIPs)
	}
	if len(del.DialableNSIPs) != 0 {
		t.Errorf("DialableNSIPs = %v, want empty", del.DialableNSIPs)
	}

	// Distinguishable from the "no NS IPs resolved" transport-failure case (TestDiscoverNSNoNSIPs):
	// that one carries a different message and leaves NSIPs empty, whereas this one preserves evidence.
	if err.Error() == "no NS IPs resolved" {
		t.Errorf("ErrNoPublicNSEndpoints must not read as the generic no-NS-IPs-resolved error")
	}
}
