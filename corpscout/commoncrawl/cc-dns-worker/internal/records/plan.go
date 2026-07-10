// Package records builds the per-domain list of Tier-2 DNS queries (sent to the domain's own
// authoritative nameservers). NS discovery (Tier 1) lives in package resolve.
package records

import (
	"strings"

	"cc-dns-worker/internal/model"

	"github.com/miekg/dns"
)

// Config controls A/AAAA hostnames, brute-forced DKIM selectors, and brute-forced SRV services.
type Config struct {
	Hostnames     []string // subdomains; apex is added separately
	DKIMSelectors []string // brute-forced <selector>._domainkey TXT
	SRVServices   []string // brute-forced "_service._proto" SRV names (service discovery)
}

// DefaultConfig is the default hostname / DKIM-selector / SRV-service lists.
func DefaultConfig() Config {
	return Config{
		Hostnames:     []string{"www", "mail", "webmail", "smtp", "autodiscover"},
		DKIMSelectors: []string{"default", "google", "selector1", "selector2", "k1", "dkim", "s1", "s2", "mail", "mandrill"},
		SRVServices: []string{
			"_sip._tcp", "_sip._udp", "_sips._tcp",
			"_sipfederationtls._tcp",                 // Teams / Skype for Business
			"_autodiscover._tcp",                     // Exchange / Microsoft 365 mail
			"_xmpp-server._tcp", "_xmpp-client._tcp", // XMPP / Jabber
			"_ldap._tcp", "_kerberos._tcp", "_kerberos._udp", "_gc._tcp", "_kpasswd._tcp", // Active Directory
			"_vlmcs._tcp",                                                      // Windows KMS activation
			"_caldav._tcp", "_caldavs._tcp", "_carddav._tcp", "_carddavs._tcp", // calendar / contacts
			"_imap._tcp", "_imaps._tcp", "_pop3._tcp", "_pop3s._tcp", "_submission._tcp", // mail discovery
			"_matrix._tcp",             // Matrix
			"_stun._udp", "_turn._udp", // WebRTC
			"_minecraft._tcp", "_ts3._udp", // game servers
		},
	}
}

// Query is one DNS question plus the semantic slot its answers are tagged with.
type Query struct {
	Name      string // FQDN with trailing dot
	Type      uint16
	Slot      string // "@" apex host; hostname; DKIM selector; SRV service; "dmarc"/"mta_sts"/"tls_rpt"/"bimi"; "" infra
	Discovery string // "static" | "ct" | "axfr" — how the queried hostname was discovered
}

// Plan returns every Tier-2 query for a domain (no trailing dot on input), unioning the static
// hostname/DKIM/SRV lists in cfg with any extra discovered hostnames (from CT/registry/axfr). An
// extra hostname already covered by the static set is skipped so it's never double-queried; the
// static set wins the overlap, so its Discovery stays "static".
func Plan(domain string, cfg Config, extra []model.HostLabel) []Query {
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
		{Name: fqdn, Type: dns.TypeHTTPS, Slot: "@"},                        // SVCB/HTTPS: ALPN, ECH, IP hints, CDN
		{Name: dns.Fqdn("www." + domain), Type: dns.TypeHTTPS, Slot: "www"}, // HTTPS is meaningful at apex + www
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
	for _, s := range cfg.SRVServices {
		qs = append(qs, Query{Name: dns.Fqdn(s + "." + domain), Type: dns.TypeSRV, Slot: s})
	}
	for i := range qs {
		qs[i].Discovery = "static"
	}

	// Union discovered hosts (CT/registry/axfr), skipping any label already covered by the static set,
	// so we never double-query. Each gets A+AAAA tagged with its discovery source.
	staticLabels := map[string]bool{}
	for _, h := range cfg.Hostnames {
		staticLabels[strings.ToLower(h)] = true
	}
	for _, e := range extra {
		l := strings.ToLower(e.Label)
		if l == "" || staticLabels[l] {
			continue
		}
		staticLabels[l] = true // also dedupe extras against each other
		hn := dns.Fqdn(l + "." + domain)
		qs = append(qs,
			Query{Name: hn, Type: dns.TypeA, Slot: l, Discovery: e.DiscoverySource},
			Query{Name: hn, Type: dns.TypeAAAA, Slot: l, Discovery: e.DiscoverySource})
	}
	return qs
}
