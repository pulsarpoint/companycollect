package resolve

import (
	"net"
	"testing"

	"github.com/miekg/dns"
)

// zone maps "qname/qtype" to the RRs the server returns in ANSWER — exactly what a recursive
// resolver returns, so it doubles as a stand-in recursive resolver for discovery tests.
type zone map[string][]dns.RR

func startAuth(t *testing.T, z zone) (string, func()) {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	mux := dns.NewServeMux()
	mux.HandleFunc(".", func(w dns.ResponseWriter, r *dns.Msg) {
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

func mustRR(t *testing.T, s string) dns.RR {
	t.Helper()
	rr, err := dns.NewRR(s)
	if err != nil {
		t.Fatalf("bad RR %q: %v", s, err)
	}
	return rr
}
