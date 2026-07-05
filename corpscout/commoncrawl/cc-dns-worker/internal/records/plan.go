// Package records builds the per-domain list of Tier-2 DNS queries (sent to the domain's own
// authoritative nameservers). NS discovery (Tier 1) lives in package resolve.
package records

import "github.com/miekg/dns"

// Config controls A/AAAA hostnames and brute-forced DKIM selectors.
type Config struct {
	Hostnames     []string // subdomains; apex is added separately
	DKIMSelectors []string
}

// DefaultConfig is the spec's default 5 hostnames and 10 DKIM selectors.
func DefaultConfig() Config {
	return Config{
		Hostnames:     []string{"www", "mail", "webmail", "smtp", "autodiscover"},
		DKIMSelectors: []string{"default", "google", "selector1", "selector2", "k1", "dkim", "s1", "s2", "mail", "mandrill"},
	}
}

// Query is one DNS question plus the semantic slot its answers are tagged with.
type Query struct {
	Name string // FQDN with trailing dot
	Type uint16
	Slot string // "@" apex host; hostname; DKIM selector; "dmarc"/"mta_sts"/"tls_rpt"/"bimi"; "" infra
}

// Plan returns every Tier-2 query for a domain (no trailing dot on input).
func Plan(domain string, cfg Config) []Query {
	fqdn := dns.Fqdn(domain)
	qs := []Query{
		{Name: fqdn, Type: dns.TypeA, Slot: "@"},
		{Name: fqdn, Type: dns.TypeAAAA, Slot: "@"},
		{Name: fqdn, Type: dns.TypeMX, Slot: ""},
		{Name: fqdn, Type: dns.TypeTXT, Slot: ""},
		{Name: fqdn, Type: dns.TypeNS, Slot: ""},
		{Name: fqdn, Type: dns.TypeSOA, Slot: ""},
		{Name: fqdn, Type: dns.TypeCAA, Slot: ""},
		{Name: fqdn, Type: dns.TypeDNSKEY, Slot: ""},
		{Name: "_dmarc." + fqdn, Type: dns.TypeTXT, Slot: "dmarc"},
		{Name: "_mta-sts." + fqdn, Type: dns.TypeTXT, Slot: "mta_sts"},
		{Name: "_smtp._tls." + fqdn, Type: dns.TypeTXT, Slot: "tls_rpt"},
		{Name: "default._bimi." + fqdn, Type: dns.TypeTXT, Slot: "bimi"},
	}
	for _, h := range cfg.Hostnames {
		hn := dns.Fqdn(h + "." + domain)
		qs = append(qs, Query{Name: hn, Type: dns.TypeA, Slot: h}, Query{Name: hn, Type: dns.TypeAAAA, Slot: h})
	}
	for _, sel := range cfg.DKIMSelectors {
		qs = append(qs, Query{Name: dns.Fqdn(sel + "._domainkey." + domain), Type: dns.TypeTXT, Slot: sel})
	}
	return qs
}
