package resolve

import (
	"reflect"
	"testing"

	"provider_recon/internal/detect/knowledge"
)

// fakeKB is injected knowledge: exact subjects per kind, and provider keys.
type fakeKB struct {
	rules map[knowledge.Kind]map[string]knowledge.Match
	keys  map[string]knowledge.Provider
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
		knowledge.TXTName: {
			"_amazonses": {ProviderSlug: "aws", ServiceKey: "aws.ses", ServiceTypes: []string{"email_sending"}, RuleID: "aws/txt-name", Confidence: 0.9},
		},
		knowledge.CNAMETarget: {
			"example.se.cdn.cloudflare.net": {ProviderSlug: "cloudflare", ServiceKey: "cloudflare.edge", ServiceTypes: []string{"cdn", "ddos_protection", "waf"}, RuleID: "cloudflare/cname", Confidence: 1},
		},
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
