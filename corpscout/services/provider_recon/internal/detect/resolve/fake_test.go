package resolve

import (
	"net/netip"
	"reflect"
	"slices"
	"testing"

	"provider_recon/internal/detect/knowledge"
)

// fakeKB is injected knowledge: exact subjects per kind, and provider keys.
type fakeKB struct {
	rules map[knowledge.Kind]map[string]knowledge.Match
	keys  map[string]knowledge.Provider
	ips   []knowledge.IPRange
}

func (f fakeKB) Match(kind knowledge.Kind, subject string) (knowledge.Match, bool) {
	m, ok := f.rules[kind][subject]
	return m, ok
}

func (f fakeKB) ProviderForKey(key string) (knowledge.Provider, bool) {
	p, ok := f.keys[key]
	return p, ok
}

func (fakeKB) Version() string { return "fake" }

// LookupIP returns the fake's ranges containing p, longest prefix first.
func (f fakeKB) LookupIP(p netip.Prefix) []knowledge.IPRange {
	var out []knowledge.IPRange
	for _, r := range f.ips {
		if r.Prefix.Bits() <= p.Bits() && r.Prefix.Contains(p.Addr()) {
			out = append(out, r)
		}
	}
	slices.SortStableFunc(out, func(a, b knowledge.IPRange) int { return b.Prefix.Bits() - a.Prefix.Bits() })
	return out
}

var kb = fakeKB{
	rules: map[knowledge.Kind]map[string]knowledge.Match{
		knowledge.NSTarget: {
			"abby.ns.cloudflare.com": {ProviderSlug: "cloudflare", ServiceKey: "cloudflare.dns", ServiceTypes: []string{"dns"}, RuleID: "cloudflare/ns", Confidence: 1},
		},
		knowledge.MXTarget: {
			"aspmx.l.google.com": {ProviderSlug: "google", ServiceKey: "google.workspace-mail", ServiceTypes: []string{"email"}, RuleID: "google/mx", Confidence: 1},
		},
		knowledge.TXTValue: {
			"apple-domain-verification=xyz": {ProviderSlug: "apple", ServiceKey: "apple.domain-verification", ServiceTypes: []string{"saas_verification"}, RuleID: "apple/txt", Confidence: 0.9},
		},
		knowledge.SPFInclude: {
			"_spf.google.com": {ProviderSlug: "google", ServiceKey: "google.workspace-sending", ServiceTypes: []string{"email_sending"}, RuleID: "google/spf", Confidence: 1},
		},
		knowledge.DKIMSelector: {
			"selector1": {ProviderSlug: "microsoft", ServiceKey: "microsoft.365-sending", ServiceTypes: []string{"email_sending"}, RuleID: "microsoft/dkim-selector", Confidence: 0.6},
			"google":    {ProviderSlug: "google", ServiceKey: "google.workspace-sending", ServiceTypes: []string{"email_sending"}, RuleID: "google/dkim-selector", Confidence: 0.7},
		},
		knowledge.DKIMTarget: {
			"selector1-example-se._domainkey.contoso.onmicrosoft.com": {ProviderSlug: "microsoft", ServiceKey: "microsoft.365-sending", ServiceTypes: []string{"email_sending"}, RuleID: "microsoft/dkim-target", Confidence: 1},
		},
		knowledge.DMARCReport: {
			"rua.dmarcian.com": {ProviderSlug: "dmarcian", ServiceKey: "dmarcian.reporting", ServiceTypes: []string{"dmarc_reporting"}, RuleID: "dmarcian/rua", Confidence: 1},
		},
		knowledge.TXTName: {
			"_amazonses": {ProviderSlug: "aws", ServiceKey: "aws.ses", ServiceTypes: []string{"email_sending"}, RuleID: "aws/txt-name", Confidence: 0.9},
		},
		knowledge.CNAMETarget: {
			"example.se.cdn.cloudflare.net": {ProviderSlug: "cloudflare", ServiceKey: "cloudflare.edge", ServiceTypes: []string{"cdn", "ddos_protection", "waf"}, RuleID: "cloudflare/cname", Confidence: 1},
		},
	},
	ips: []knowledge.IPRange{
		{Prefix: netip.MustParsePrefix("52.84.0.0/14"), ProviderSlug: "aws", ServiceKey: "aws.other", ServiceTypes: []string{"iaas"}, Confidence: 0.9, RuleID: "aws/aws.other/IP 52.84.0.0/14"},
		{Prefix: netip.MustParsePrefix("52.84.0.0/15"), ProviderSlug: "aws", ServiceKey: "aws.cloudfront", ServiceTypes: []string{"cdn"}, Confidence: 1, From: "2026-03-01", RuleID: "aws/aws.cloudfront/IP 52.84.0.0/15"},
		{Prefix: netip.MustParsePrefix("2600:9000::/28"), ProviderSlug: "aws", ServiceKey: "aws.cloudfront", ServiceTypes: []string{"cdn"}, Confidence: 1, RuleID: "aws/aws.cloudfront/IP 2600:9000::/28"},
		{Prefix: netip.MustParsePrefix("198.51.100.0/24"), ProviderSlug: "mailchimp", ServiceKey: "mailchimp.sending", ServiceTypes: []string{"email_sending"}, Confidence: 1, RuleID: "mailchimp/mailchimp.sending/IP 198.51.100.0/24"},
		{Prefix: netip.MustParsePrefix("198.18.0.0/24"), ProviderSlug: "sendy", ServiceKey: "sendy.mail", ServiceTypes: []string{"email_sending"}, Confidence: 1, To: "2026-08-20", RuleID: "sendy/sendy.mail/IP 198.18.0.0/24"},
		{Prefix: netip.MustParsePrefix("198.18.0.0/24"), ProviderSlug: "sendy", ServiceKey: "sendy.mail", ServiceTypes: []string{"email_sending"}, Confidence: 1, From: "2026-09-01", RuleID: "sendy/sendy.mail/IP 198.18.0.0/24"},
		{Prefix: netip.MustParsePrefix("203.0.113.0/24"), ProviderSlug: "hosty", ServiceKey: "hosty.web", ServiceTypes: []string{"hosting"}, Confidence: 1, RuleID: "hosty/hosty.web/IP 203.0.113.0/24"},
	},
	keys: map[string]knowledge.Provider{
		"binero.se": {Slug: "binero", Services: []knowledge.ProviderService{{Key: "binero.web", Types: []string{"hosting"}}, {Key: "binero.dns", Types: []string{"dns"}}}},
	},
}

func rec(name, typ, value string) Record {
	return Record{RecordID: "r1", RootDomain: "example.se", Name: name, Type: typ, Value: value, FirstSeen: "2026-08-10", LastSeen: "2026-09-19"}
}

// short renders results as [service_type provider_key provider_slug service_key subject confidence fallback].
func short(rs []Result) [][]any {
	out := [][]any{}
	for _, r := range rs {
		out = append(out, []any{r.ServiceType, r.ProviderKey, r.ProviderSlug, r.ServiceKey, r.Subject, r.Confidence, r.Fallback})
	}
	return out
}

func assertResults(t *testing.T, got []Result, want [][]any) {
	t.Helper()
	if g := short(got); !reflect.DeepEqual(g, want) {
		t.Fatalf("results\n got %v\nwant %v", g, want)
	}
}
