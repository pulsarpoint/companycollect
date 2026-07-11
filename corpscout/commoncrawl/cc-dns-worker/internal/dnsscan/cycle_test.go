package dnsscan

import (
	"context"
	"testing"
	"time"

	"cc-dns-worker/internal/model"
	"cc-dns-worker/internal/resolve"

	"github.com/miekg/dns"
)

type privateDelegationExchanger struct{}

func (privateDelegationExchanger) Exchange(_ context.Context, request *dns.Msg, _ string) (*dns.Msg, error) {
	response := new(dns.Msg)
	response.SetReply(request)
	question := request.Question[0]
	if question.Qtype == dns.TypeNS {
		response.Answer = []dns.RR{&dns.NS{
			Hdr: dns.RR_Header{Name: question.Name, Rrtype: dns.TypeNS, Class: dns.ClassINET, Ttl: 300},
			Ns:  "ns1.example.com.",
		}}
	} else if question.Qtype == dns.TypeA {
		response.Answer = []dns.RR{&dns.A{
			Hdr: dns.RR_Header{Name: question.Name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 300},
			A:   []byte{10, 0, 0, 53},
		}}
	}
	return response, nil
}

func TestResolveDomainPreservesPrivateDelegationWithoutDialingIt(t *testing.T) {
	discoverer := resolve.NewDiscoverer(privateDelegationExchanger{}, []string{"operator-resolver"})
	result := resolveDomain(context.Background(), discoverer, nil, "example.com", "scan", "run", nil)
	if result.Status != model.DomainStatusNoPublicNSEndpoints {
		t.Fatalf("status = %q", result.Status)
	}
	if len(result.Endpoints) != 1 || result.Endpoints[0].IP != "10.0.0.53" || result.Endpoints[0].Dialable {
		t.Fatalf("private delegation evidence was not preserved: %+v", result.Endpoints)
	}
	if result.ResolvedAt.IsZero() || time.Since(result.ResolvedAt) > time.Minute {
		t.Fatalf("unexpected resolution timestamp: %v", result.ResolvedAt)
	}
}
