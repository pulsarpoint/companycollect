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

// startAXFRServer starts a TCP DNS server that answers an AXFR query for any zone. If refuse is true
// it replies REFUSED; otherwise it streams rrs (which MUST start and end with the zone SOA for a
// well-formed transfer). Non-AXFR queries get an empty NOERROR reply.
func startAXFRServer(t *testing.T, rrs []dns.RR, refuse bool) (string, func()) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tcp: %v", err)
	}
	mux := dns.NewServeMux()
	mux.HandleFunc(".", func(w dns.ResponseWriter, r *dns.Msg) {
		if r.Question[0].Qtype == dns.TypeAXFR {
			if refuse {
				m := new(dns.Msg)
				m.SetReply(r)
				m.Rcode = dns.RcodeRefused
				_ = w.WriteMsg(m)
				return
			}
			ch := make(chan *dns.Envelope)
			tr := new(dns.Transfer)
			go func() {
				ch <- &dns.Envelope{RR: rrs}
				close(ch)
			}()
			_ = tr.Out(w, r, ch)
			return
		}
		m := new(dns.Msg)
		m.SetReply(r)
		_ = w.WriteMsg(m)
	})
	srv := &dns.Server{Listener: l, Handler: mux}
	go func() { _ = srv.ActivateAndServe() }()
	return l.Addr().String(), func() { _ = srv.Shutdown() }
}

// startAXFRServerMulti is startAXFRServer (never refusing) but streams EACH rr as its own envelope, so
// a low cap fires on a non-final envelope — exercising the mid-stream early-exit/drain path. rrs MUST
// start and end with the zone SOA for a well-formed transfer.
func startAXFRServerMulti(t *testing.T, rrs []dns.RR) (string, func()) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tcp: %v", err)
	}
	mux := dns.NewServeMux()
	mux.HandleFunc(".", func(w dns.ResponseWriter, r *dns.Msg) {
		if r.Question[0].Qtype == dns.TypeAXFR {
			ch := make(chan *dns.Envelope)
			tr := new(dns.Transfer)
			go func() {
				for _, rr := range rrs {
					ch <- &dns.Envelope{RR: []dns.RR{rr}}
				}
				close(ch)
			}()
			_ = tr.Out(w, r, ch)
			return
		}
		m := new(dns.Msg)
		m.SetReply(r)
		_ = w.WriteMsg(m)
	})
	srv := &dns.Server{Listener: l, Handler: mux}
	go func() { _ = srv.ActivateAndServe() }()
	return l.Addr().String(), func() { _ = srv.Shutdown() }
}
