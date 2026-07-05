package records

import (
	"testing"

	"github.com/miekg/dns"
)

func TestPlanCoversAllRecordFamilies(t *testing.T) {
	qs := Plan("example.com", DefaultConfig())
	got := map[string]bool{}
	for _, q := range qs {
		got[q.Name+"/"+dns.TypeToString[q.Type]] = true
	}
	want := []string{
		"example.com./A", "example.com./AAAA",
		"www.example.com./A", "mail.example.com./A",
		"example.com./MX", "example.com./TXT",
		"_dmarc.example.com./TXT",
		"default._domainkey.example.com./TXT",
		"mandrill._domainkey.example.com./TXT",
		"_mta-sts.example.com./TXT",
		"_smtp._tls.example.com./TXT",
		"default._bimi.example.com./TXT",
		"example.com./CAA", "example.com./SOA", "example.com./NS",
		"example.com./DNSKEY",
	}
	for _, w := range want {
		if !got[w] {
			t.Errorf("plan missing query %q", w)
		}
	}
}

func TestPlanApexSlotIsAt(t *testing.T) {
	for _, q := range Plan("example.com", DefaultConfig()) {
		if q.Name == "example.com." && q.Type == dns.TypeA && q.Slot != "@" {
			t.Errorf("apex A slot = %q, want @", q.Slot)
		}
	}
}
