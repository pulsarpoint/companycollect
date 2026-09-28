package knowledge

import (
	"net/netip"
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
		"exists rule":     doc("p", nil, svc("p.a", []string{"dns"}, rule("NS", "target", "exists", "", 0, 1))),
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

func TestVersionTracksOnlyWhatTheIndexUses(t *testing.T) {
	base := func() model.Document {
		return doc("p", []string{"p.com"}, svc("p.dns", []string{"dns"}, rule("NS", "target", "suffix", "ns.p.com", 0, 1)))
	}
	version := func(d model.Document) string {
		idx, err := Compile([]model.Document{d})
		if err != nil {
			t.Fatal(err)
		}
		return idx.Version()
	}
	v0 := version(base())

	seen := base()
	seen.Services[0].Evidence.DNSRules[0].LastSeen = "2026-12-31"
	if version(seen) != v0 {
		t.Error("a last_seen bump moved the version")
	}
	for name, mutate := range map[string]func(*model.Document){
		"rule pattern":  func(d *model.Document) { d.Services[0].Evidence.DNSRules[0].Pattern = "ns2.p.com" },
		"rule priority": func(d *model.Document) { d.Services[0].Evidence.DNSRules[0].Priority = 5 },
		"service types": func(d *model.Document) { d.Services[0].ServiceTypes = []string{"dns", "hosting"} },
		"provider key":  func(d *model.Document) { d.ProviderKeys = []string{"p.net"} },
		"rule removed":  func(d *model.Document) { d.Services[0].Evidence.DNSRules[0].Status = model.StatusRemoved },
	} {
		d := base()
		mutate(&d)
		if version(d) == v0 {
			t.Errorf("%s: version unchanged", name)
		}
	}
}

func ipRange(cidr, status, first, last string) model.IPRange {
	return model.IPRange{CIDR: cidr, Confidence: 1, Lifecycle: model.Lifecycle{Status: status, FirstSeen: first, LastSeen: last}}
}

func withRanges(slug, key string, types []string, ranges ...model.IPRange) model.Document {
	s := svc(key, types)
	s.Evidence.IPRanges = ranges
	return doc(slug, nil, s)
}

func mustPrefix(t *testing.T, s string) netip.Prefix {
	t.Helper()
	p, err := netip.ParsePrefix(s)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLookupIPReturnsContainingRangesWithWindows(t *testing.T) {
	idx, err := Compile([]model.Document{
		withRanges("aws", "aws.other", []string{"iaas"}, ipRange("52.84.0.0/14", model.StatusActive, "2026-09-27", "2026-09-28")),
		withRanges("aws2", "aws2.cloudfront", []string{"cdn"},
			ipRange("52.84.0.0/15", model.StatusRemoved, "2026-10-01", "2026-11-15"),
			ipRange("2600:9000::/28", model.StatusActive, "2026-10-01", "2026-12-01")),
	})
	if err != nil {
		t.Fatal(err)
	}
	got := idx.LookupIP(mustPrefix(t, "52.84.1.1/32"))
	if len(got) != 2 {
		t.Fatalf("ranges = %+v", got)
	}
	// Longest prefix first.
	// aws2's feed first ran on 2026-10-01: its first-run ranges open at the beginning.
	if got[0].Prefix.String() != "52.84.0.0/15" || got[0].ServiceKey != "aws2.cloudfront" || got[0].From != "" || got[0].To != "2026-11-15" ||
		got[0].RuleID != "aws2/aws2.cloudfront/IP 52.84.0.0/15" || got[0].ServiceTypes[0] != "cdn" {
		t.Fatalf("nested range = %+v", got[0])
	}
	// First seen on the timeline's first day: open start; active: open end.
	if got[1].Prefix.String() != "52.84.0.0/14" || got[1].From != "" || got[1].To != "" {
		t.Fatalf("outer range = %+v", got[1])
	}
	if v6 := idx.LookupIP(mustPrefix(t, "2600:9000:1::1/128")); len(v6) != 1 || v6[0].From != "" || v6[0].To != "" {
		t.Fatalf("v6 = %+v", v6)
	}
	if none := idx.LookupIP(mustPrefix(t, "192.0.2.1/32")); len(none) != 0 {
		t.Fatalf("outside = %+v", none)
	}
	// A /24 inside the /15 matches both; a /13 containing them matches nothing.
	if len(idx.LookupIP(mustPrefix(t, "52.85.10.0/24"))) != 2 || len(idx.LookupIP(mustPrefix(t, "52.80.0.0/13"))) != 0 {
		t.Fatal("prefix containment wrong")
	}
}

func TestRangesOfRemovedServicesStayInTheHistory(t *testing.T) {
	d := withRanges("p", "p.old", []string{"cdn"}, ipRange("198.51.100.0/24", model.StatusRemoved, "2026-09-27", "2026-10-05"))
	d.Services[0].RemovedAt = "2026-10-06"
	idx, err := Compile([]model.Document{d})
	if err != nil {
		t.Fatal(err)
	}
	if got := idx.LookupIP(mustPrefix(t, "198.51.100.7/32")); len(got) != 1 || got[0].To != "2026-10-05" {
		t.Fatalf("got %+v", got)
	}
}

func TestCompileRefusesAnInvalidCIDR(t *testing.T) {
	if _, err := Compile([]model.Document{withRanges("p", "p.a", []string{"cdn"}, ipRange("10.0.0/8", model.StatusActive, "2026-09-27", "2026-09-28"))}); err == nil {
		t.Fatal("invalid CIDR compiled")
	}
}

func TestVersionTracksRanges(t *testing.T) {
	v := func(ranges ...model.IPRange) string {
		idx, err := Compile([]model.Document{withRanges("p", "p.a", []string{"cdn"}, ranges...)})
		if err != nil {
			t.Fatal(err)
		}
		return idx.Version()
	}
	base := v(ipRange("198.51.100.0/24", model.StatusActive, "2026-09-27", "2026-09-28"))
	if v(ipRange("198.51.100.0/24", model.StatusActive, "2026-09-27", "2026-12-31")) != base {
		t.Error("a last_seen bump of an active range moved the version")
	}
	if v(ipRange("198.51.100.0/24", model.StatusRemoved, "2026-09-27", "2026-12-31")) == base {
		t.Error("a removal did not move the version")
	}
	if v(ipRange("198.51.100.0/24", model.StatusActive, "2026-09-27", "2026-09-28"), ipRange("203.0.113.0/24", model.StatusActive, "2026-09-27", "2026-09-28")) == base {
		t.Error("an added range did not move the version")
	}
}

func TestRangesOpenAtTheirOwnFeedsFirstRun(t *testing.T) {
	collected := func(cidr, collector, first string) model.IPRange {
		r := ipRange(cidr, model.StatusActive, first, "2026-12-01")
		r.Collector = collector
		return r
	}
	old := withRanges("aws", "aws.cloudfront", []string{"cdn"}, collected("52.84.0.0/15", "aws_ip_ranges", "2026-09-27"))
	// A provider whose feed first ran two months later: its first-run ranges
	// are just as old as anything else, so they open at the beginning too.
	late := withRanges("vercel", "vercel.edge", []string{"cdn"},
		collected("76.76.21.0/24", "vercel_ranges", "2026-11-01"),
		collected("76.76.22.0/24", "vercel_ranges", "2026-11-20"))
	idx, err := Compile([]model.Document{old, late})
	if err != nil {
		t.Fatal(err)
	}
	if got := idx.LookupIP(mustPrefix(t, "76.76.21.9/32")); len(got) != 1 || got[0].From != "" {
		t.Fatalf("first-run range of a later feed = %+v", got)
	}
	if got := idx.LookupIP(mustPrefix(t, "76.76.22.9/32")); len(got) != 1 || got[0].From != "2026-11-20" {
		t.Fatalf("range added after that feed's first run = %+v", got)
	}
	// Loading another provider's document must not change these windows.
	alone, err := Compile([]model.Document{late})
	if err != nil {
		t.Fatal(err)
	}
	if got := alone.LookupIP(mustPrefix(t, "76.76.21.9/32")); got[0].From != "" {
		t.Fatalf("window depends on which documents are loaded: %+v", got)
	}
}
