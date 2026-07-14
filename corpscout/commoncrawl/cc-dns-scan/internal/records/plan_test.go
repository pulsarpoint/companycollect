package records

import (
	"testing"

	"cc-dns-scan/internal/model"

	"github.com/miekg/dns"
)

func TestPlanCoversAllRecordFamilies(t *testing.T) {
	qs := Plan("example.com", DefaultConfig(), nil)
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
		"example.com./HTTPS", "www.example.com./HTTPS",
		"_autodiscover._tcp.example.com./SRV", "_sip._tcp.example.com./SRV",
	}
	for _, w := range want {
		if !got[w] {
			t.Errorf("plan missing query %q", w)
		}
	}
}

func TestPlanApexSlotIsAt(t *testing.T) {
	for _, q := range Plan("example.com", DefaultConfig(), nil) {
		if q.Name == "example.com." && q.Type == dns.TypeA && q.Slot != "@" {
			t.Errorf("apex A slot = %q, want @", q.Slot)
		}
	}
}

func TestPlanSlots(t *testing.T) {
	cfg := DefaultConfig()
	qs := Plan("example.com", cfg, nil)

	// Build lookup from "name/type" to slot for quick verification
	lookup := make(map[string]string)
	for _, q := range qs {
		key := q.Name + "/" + dns.TypeToString[q.Type]
		lookup[key] = q.Slot
	}

	// Check apex A/AAAA with slot "@"
	if got := lookup["example.com./A"]; got != "@" {
		t.Errorf("example.com./A slot = %q, want @", got)
	}
	if got := lookup["example.com./AAAA"]; got != "@" {
		t.Errorf("example.com./AAAA slot = %q, want @", got)
	}

	// Check hostnames A/AAAA with slot == hostname (driven from DefaultConfig)
	for _, h := range cfg.Hostnames {
		name := h + ".example.com."
		if got := lookup[name+"/A"]; got != h {
			t.Errorf("%s/A slot = %q, want %q", name, got, h)
		}
		if got := lookup[name+"/AAAA"]; got != h {
			t.Errorf("%s/AAAA slot = %q, want %q", name, got, h)
		}
	}

	// Check infrastructure records with slot ""
	infraQueries := []string{
		"example.com./MX",
		"example.com./TXT",
		"example.com./NS",
		"example.com./SOA",
		"example.com./CAA",
		"example.com./DNSKEY",
	}
	for _, q := range infraQueries {
		if got := lookup[q]; got != "" {
			t.Errorf("%s slot = %q, want empty string", q, got)
		}
	}

	// Check special mail/security subdomains
	specialQueries := map[string]string{
		"_dmarc.example.com./TXT":        "dmarc",
		"_mta-sts.example.com./TXT":      "mta_sts",
		"_smtp._tls.example.com./TXT":    "tls_rpt",
		"default._bimi.example.com./TXT": "bimi",
	}
	for q, expectedSlot := range specialQueries {
		if got := lookup[q]; got != expectedSlot {
			t.Errorf("%s slot = %q, want %q", q, got, expectedSlot)
		}
	}

	// Check DKIM selectors with slot == selector (driven from DefaultConfig)
	for _, sel := range cfg.DKIMSelectors {
		q := sel + "._domainkey.example.com./TXT"
		if got := lookup[q]; got != sel {
			t.Errorf("%s slot = %q, want %q", q, got, sel)
		}
	}

	// HTTPS at apex ("@") and www
	if got := lookup["example.com./HTTPS"]; got != "@" {
		t.Errorf("example.com./HTTPS slot = %q, want @", got)
	}
	if got := lookup["www.example.com./HTTPS"]; got != "www" {
		t.Errorf("www.example.com./HTTPS slot = %q, want www", got)
	}

	// Check SRV services with slot == service (driven from DefaultConfig)
	for _, s := range cfg.SRVServices {
		q := s + ".example.com./SRV"
		if got := lookup[q]; got != s {
			t.Errorf("%s slot = %q, want %q", q, got, s)
		}
	}
}

func TestPlanUnionsExtraHosts(t *testing.T) {
	cfg := DefaultConfig()
	extra := []model.HostLabel{
		{Label: "jenkins", DiscoverySource: "axfr"},
		{Label: "www", DiscoverySource: "ct"}, // dup of a static hostname — must NOT double-query
	}
	qs := Plan("example.com", cfg, extra)
	var jenkinsA, jenkinsAAAA, wwwStatic int
	for _, q := range qs {
		if q.Name == "jenkins.example.com." && q.Type == dns.TypeA {
			jenkinsA++
			if q.Discovery != "axfr" {
				t.Errorf("jenkins A discovery = %q, want axfr", q.Discovery)
			}
		}
		if q.Name == "jenkins.example.com." && q.Type == dns.TypeAAAA {
			jenkinsAAAA++
			if q.Discovery != "axfr" {
				t.Errorf("jenkins AAAA discovery = %q, want axfr", q.Discovery)
			}
		}
		if q.Name == "www.example.com." && q.Type == dns.TypeA {
			wwwStatic++
			if q.Discovery != "static" {
				t.Errorf("www A discovery = %q, want static (static wins the overlap)", q.Discovery)
			}
		}
	}
	if jenkinsA != 1 {
		t.Errorf("want exactly 1 jenkins A query, got %d", jenkinsA)
	}
	if jenkinsAAAA != 1 {
		t.Errorf("want exactly 1 jenkins AAAA query, got %d", jenkinsAAAA)
	}
	if wwwStatic != 1 {
		t.Errorf("want exactly 1 www A query (deduped), got %d", wwwStatic)
	}
}
