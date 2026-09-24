package resolve

import (
	"net"
	"testing"

	"github.com/miekg/dns"
)

// zone maps "qname/qtype" to the RRs the server returns in ANSWER — exactly what a recursive
// resolver returns, so it doubles as a stand-in recursive resolver for discovery tests.
type zone map[string][]dns.RR

func startAuth(t *testing.T, zone zone) (string, func()) {
	t.Helper()
	packetConnection, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	mux := dns.NewServeMux()
	mux.HandleFunc(".", func(writer dns.ResponseWriter, request *dns.Msg) {
		response := new(dns.Msg)
		response.SetReply(request)
		question := request.Question[0]
		if records, ok := zone[question.Name+"/"+dns.TypeToString[question.Qtype]]; ok {
			response.Answer = append(response.Answer, records...)
		}
		_ = writer.WriteMsg(response)
	})
	server := &dns.Server{PacketConn: packetConnection, Handler: mux}
	go func() { _ = server.ActivateAndServe() }()
	return packetConnection.LocalAddr().String(), func() { _ = server.Shutdown() }
}

func mustRR(t *testing.T, value string) dns.RR {
	t.Helper()
	record, err := dns.NewRR(value)
	if err != nil {
		t.Fatalf("bad RR %q: %v", value, err)
	}
	return record
}
