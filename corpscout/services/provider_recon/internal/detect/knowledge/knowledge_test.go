package knowledge

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"provider_recon/internal/model"
)

func rule(recordType, field, matcherType, pattern string, priority int, confidence float64) model.DNSRule {
	return model.DNSRule{RecordType: recordType, MatchField: field, MatcherType: matcherType, Pattern: pattern,
		Priority: priority, Confidence: confidence, Lifecycle: model.Lifecycle{Status: model.StatusActive}}
}

func svc(key string, types []string, rules ...model.DNSRule) model.Service {
	return model.Service{Key: key, ServiceTypes: types, Evidence: model.Evidence{DNSRules: rules}}
}

func doc(slug string, keys []string, services ...model.Service) model.Document {
	return model.Document{Version: model.ContractVersion, Slug: slug, DisplayName: strings.ToUpper(slug), ProviderKeys: keys, Services: services}
}

func TestLoadDirCompilesRealDocuments(t *testing.T) {
	idx, err := LoadDir("testdata/providers")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		kind    Kind
		subject string
		service string
	}{
		{NSTarget, "ns1.loopia.se.", "loopia.dns"},
		{NSTarget, "abby.ns.cloudflare.com", "cloudflare.dns"},
		{NSTarget, "NS51.DOMAINCONTROL.COM.", "godaddy.dns"},
		{MXTarget, "route1.mx.cloudflare.net", "cloudflare.email-routing"},
		{CNAMETarget, "example.com.cdn.cloudflare.net", "cloudflare.edge"},
	} {
		m, ok := idx.Match(c.kind, c.subject)
		if !ok || m.ServiceKey != c.service {
			t.Errorf("Match(%s, %q) = %+v %v, want %s", c.kind, c.subject, m, ok, c.service)
		}
	}
	if p, ok := idx.ProviderForKey("domaincontrol.com"); !ok || p.Slug != "godaddy" || p.FirstService("dns") != "godaddy.dns" {
		t.Fatalf("ProviderForKey(domaincontrol.com) = %+v %v", p, ok)
	}
	if !strings.HasPrefix(idx.Version(), "sha256:") {
		t.Fatalf("version = %q", idx.Version())
	}
	again, err := LoadDir("testdata/providers")
	if err != nil || again.Version() != idx.Version() {
		t.Fatalf("version not stable: %v %q vs %q", err, again.Version(), idx.Version())
	}
}

func TestLoadDirRefusesBrokenFiles(t *testing.T) {
	if _, err := LoadDir(t.TempDir()); err == nil {
		t.Error("empty directory loaded")
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "broken.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadDir(dir); err == nil {
		t.Error("broken JSON loaded")
	}
}

func TestMatchPrefersPriorityThenConfidenceThenRuleID(t *testing.T) {
	idx, err := Compile([]model.Document{
		doc("a", nil, svc("a.low", []string{"dns"}, rule("NS", "target", "suffix", "example.net", 0, 1))),
		doc("b", nil, svc("b.high", []string{"dns"}, rule("NS", "target", "suffix", "ns.example.net", 10, 0.5))),
		doc("c", nil, svc("c.tie", []string{"dns"}, rule("NS", "target", "suffix", "ns.example.net", 10, 0.5))),
	})
	if err != nil {
		t.Fatal(err)
	}
	m, ok := idx.Match(NSTarget, "NS1.ns.example.net")
	if !ok || m.ServiceKey != "b.high" {
		t.Fatalf("got %+v; want b.high (priority beats confidence, rule id breaks the tie)", m)
	}
	if m, _ := idx.Match(NSTarget, "ns1.other.example.net"); m.ServiceKey != "a.low" {
		t.Fatalf("got %+v; want a.low", m)
	}
	if _, ok := idx.Match(NSTarget, "notexample.net"); ok {
		t.Fatal("suffix matched without a label boundary")
	}
}

func TestMatcherTypesComeFromTheSharedMatcher(t *testing.T) {
	idx, err := Compile([]model.Document{doc("p", nil,
		svc("p.exact", []string{"email"}, rule("MX", "target", "exact", "mx.p.com", 0, 1)),
		svc("p.prefix", []string{"saas_verification"}, rule("TXT", "value", "prefix", "p-verification=", 0, 1)),
		svc("p.contains", []string{"email_sending"}, rule("TXT", "value", "contains", "include:spf.p.com", 0, 1)),
		svc("p.regex", []string{"dns"}, rule("NS", "target", "regex", `^ns[0-9]+\.p\.com$`, 0, 1)),
		svc("p.glob", []string{"hosting"}, rule("CNAME", "target", "glob", "*.edge-*.p.net", 0, 1)),
	)})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		kind    Kind
		subject string
		want    string
	}{
		{MXTarget, "MX.P.COM.", "p.exact"},
		{MXTarget, "a.mx.p.com", ""},
		{TXTValue, "p-verification=abc", "p.prefix"},
		{TXTValue, "v=spf1 include:spf.p.com ~all", "p.contains"},
		{NSTarget, "ns12.p.com.", "p.regex"},
		{NSTarget, "xns12.p.com", ""},
		{CNAMETarget, "site.edge-eu.p.net", "p.glob"},
	} {
		m, _ := idx.Match(c.kind, c.subject)
		if m.ServiceKey != c.want {
			t.Errorf("Match(%s, %q) = %q, want %q", c.kind, c.subject, m.ServiceKey, c.want)
		}
	}
}

func TestProviderKeysExactAndGlob(t *testing.T) {
	idx, err := Compile([]model.Document{
		doc("aws", []string{"amazonaws.com", "awsdns-*"}, svc("aws.route53", []string{"dns"}), svc("aws.ses", []string{"email_sending"})),
	})
	if err != nil {
		t.Fatal(err)
	}
	p, ok := idx.ProviderForKey("awsdns-01.co.uk")
	if !ok || p.Slug != "aws" || p.FirstService("dns") != "aws.route53" || p.FirstService("cdn") != "" {
		t.Fatalf("glob key: %+v %v", p, ok)
	}
	if _, ok := idx.ProviderForKey("example.com"); ok {
		t.Fatal("unknown key matched")
	}
}

func TestRemovedRulesAndServicesAreSkipped(t *testing.T) {
	removedRule := rule("NS", "target", "suffix", "old.example.net", 0, 1)
	removedRule.Status = model.StatusRemoved
	removedSvc := svc("p.gone", []string{"dns"}, rule("NS", "target", "suffix", "gone.example.net", 0, 1))
	removedSvc.RemovedAt = "2026-09-01"
	idx, err := Compile([]model.Document{doc("p", []string{"p.com"}, svc("p.dns", []string{"dns"}, removedRule), removedSvc)})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := idx.Match(NSTarget, "ns.old.example.net"); ok {
		t.Fatal("removed rule matched")
	}
	if _, ok := idx.Match(NSTarget, "ns.gone.example.net"); ok {
		t.Fatal("rule of a removed service matched")
	}
	if p, _ := idx.ProviderForKey("p.com"); p.FirstService("dns") != "p.dns" {
		t.Fatal("removed service offered as the provider's dns service")
	}
}

func TestCompileRefusesBadKnowledge(t *testing.T) {
	other := doc("p", nil)
	other.Version = "provider-recon/v2"
	for name, d := range map[string]model.Document{
		"unknown kind":    doc("p", nil, svc("p.a", []string{"cdn"}, rule("A", "value", "exact", "192.0.2.1", 0, 1))),
		"unknown matcher": doc("p", nil, svc("p.a", []string{"dns"}, rule("NS", "target", "wildcard", "*.p.com", 0, 1))),
		"invalid regex":   doc("p", nil, svc("p.a", []string{"dns"}, rule("NS", "target", "regex", "(", 0, 1))),
		"other contract":  other,
	} {
		if _, err := Compile([]model.Document{d}); err == nil {
			t.Errorf("%s: compiled without error", name)
		}
	}
	if _, err := Compile([]model.Document{doc("a", []string{"shared.com"}), doc("b", []string{"shared.com"})}); err == nil {
		t.Error("a provider key claimed by two providers compiled")
	}
}
