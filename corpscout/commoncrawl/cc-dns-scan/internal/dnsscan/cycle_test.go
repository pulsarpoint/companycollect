package dnsscan

import (
	"context"
	"testing"
	"time"

	"cc-dns-scan/internal/model"
	"cc-dns-scan/internal/resolve"

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

func TestSourcePageSizeWaitsForOneFullBlock(t *testing.T) {
	config := Config{DomainPageSize: 5000, WorkCapacity: 20000}
	if got := sourcePageSize(config, 15001, -1); got != 0 {
		t.Fatalf("page size with 4,999 free slots = %d, want 0", got)
	}
	if got := sourcePageSize(config, 15000, -1); got != 5000 {
		t.Fatalf("page size with 5,000 free slots = %d, want 5000", got)
	}
}

func TestSourcePageSizeHandlesBoundedTailAndSmallCapacity(t *testing.T) {
	config := Config{DomainPageSize: 5000, WorkCapacity: 20000}
	if got := sourcePageSize(config, 19900, 100); got != 100 {
		t.Fatalf("bounded final page size = %d, want 100", got)
	}
	config.WorkCapacity = 2000
	if got := sourcePageSize(config, 0, -1); got != 2000 {
		t.Fatalf("page size with capacity below configured page = %d, want 2000", got)
	}
}
