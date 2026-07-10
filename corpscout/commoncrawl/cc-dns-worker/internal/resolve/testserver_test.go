package resolve

import (
	"encoding/binary"
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

// startAXFRServerAbrupt writes rrs as a single AXFR answer message (SOA first, NO closing SOA) then
// drops the connection, so the client collects those records and then hits a read error mid-transfer.
// It exercises the "error after ≥1 RR" path that must still report Open=false.
func startAXFRServerAbrupt(t *testing.T, rrs []dns.RR) (string, func()) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tcp: %v", err)
	}
	mux := dns.NewServeMux()
	mux.HandleFunc(".", func(w dns.ResponseWriter, r *dns.Msg) {
		if r.Question[0].Qtype == dns.TypeAXFR {
			m := new(dns.Msg)
			m.SetReply(r)
			m.Answer = rrs // SOA first, some records, NO closing SOA
			_ = w.WriteMsg(m)
			_ = w.Close() // drop the conn mid-transfer -> client read error
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
//
// The channel is sized to hold every rr up front, so the producer goroutine never blocks on a send: our
// definitive-preflight design dials this server twice per probe (a preflight connection that reads only
// the first envelope, then a second connection to collect), and either connection can stop reading
// before the producer is done. An unbuffered channel would leave the producer parked forever on the
// next send once nobody (Out included, after a write error on the now-closed socket) is left to receive
// it — a leak in the test double, not the code under test.
func startAXFRServerMulti(t *testing.T, rrs []dns.RR) (string, func()) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tcp: %v", err)
	}
	mux := dns.NewServeMux()
	mux.HandleFunc(".", func(w dns.ResponseWriter, r *dns.Msg) {
		if r.Question[0].Qtype == dns.TypeAXFR {
			ch := make(chan *dns.Envelope, len(rrs))
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

// startAXFRServerRcode answers every AXFR query with rcode and an empty answer section — used to
// exercise a definitive or indefinite preflight RCODE beyond REFUSED (e.g. NOTAUTH, SERVFAIL).
func startAXFRServerRcode(t *testing.T, rcode int) (string, func()) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tcp: %v", err)
	}
	mux := dns.NewServeMux()
	mux.HandleFunc(".", func(w dns.ResponseWriter, r *dns.Msg) {
		m := new(dns.Msg)
		m.SetReply(r)
		m.Rcode = rcode
		_ = w.WriteMsg(m)
	})
	srv := &dns.Server{Listener: l, Handler: mux}
	go func() { _ = srv.ActivateAndServe() }()
	return l.Addr().String(), func() { _ = srv.Shutdown() }
}

// startAXFRServerMalformed answers an AXFR query with a complete, well-formed NOERROR message whose
// Answer section does NOT start with a SOA — exercising the "NOERROR but no leading SOA" malformed
// branch, as distinct from a raw framing/transport failure (startAXFRServerEarlyDisconnect).
func startAXFRServerMalformed(t *testing.T) (string, func()) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tcp: %v", err)
	}
	mux := dns.NewServeMux()
	mux.HandleFunc(".", func(w dns.ResponseWriter, r *dns.Msg) {
		m := new(dns.Msg)
		m.SetReply(r)
		m.Answer = []dns.RR{mustRR(t, "www.example.com. 3600 IN A 1.2.3.4")} // NOERROR, non-SOA first RR
		_ = w.WriteMsg(m)
	})
	srv := &dns.Server{Listener: l, Handler: mux}
	go func() { _ = srv.ActivateAndServe() }()
	return l.Addr().String(), func() { _ = srv.Shutdown() }
}

// startAXFRServerNoReply accepts the TCP connection, drains the query, and then holds the connection
// open without ever answering — the fake-server analogue of an unresponsive/black-holed nameserver, so
// a probe against it must resolve via its own deadline (a "timeout") rather than hang forever. Call
// stop() (not just defer it) before asserting on goroutine counts: it releases the held connection so
// the assertion reflects only the code under test, not this helper's own bookkeeping goroutine.
func startAXFRServerNoReply(t *testing.T) (string, func()) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tcp: %v", err)
	}
	done := make(chan struct{})
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 4096)
				_, _ = c.Read(buf) // drain the query; never respond
				<-done
			}(conn)
		}
	}()
	return l.Addr().String(), func() {
		close(done)
		_ = l.Close()
	}
}

// startAXFRServerEarlyDisconnect accepts one connection, reads the query, writes an incomplete
// length-prefixed frame (declares more bytes than it actually sends), then hangs up — a raw connection
// drop that fails at the framing/read layer before any dns.Msg can be parsed, distinct from a
// well-formed-but-wrong-shape "malformed" reply (startAXFRServerMalformed).
func startAXFRServerEarlyDisconnect(t *testing.T) (string, func()) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tcp: %v", err)
	}
	go func() {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 4096)
		_, _ = conn.Read(buf) // drain the query
		var hdr [2]byte
		binary.BigEndian.PutUint16(hdr[:], 100) // claim a 100-byte message
		_, _ = conn.Write(hdr[:])
		_, _ = conn.Write([]byte{0, 0, 0, 0}) // then send only 4 of them and hang up
	}()
	return l.Addr().String(), func() { _ = l.Close() }
}
