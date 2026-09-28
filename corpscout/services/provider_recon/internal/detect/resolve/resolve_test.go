package resolve

import (
	"encoding/json"
	"testing"
)

func TestNSRuleProviderKeySelfHostedUnmappedAndIP(t *testing.T) {
	for _, c := range []struct {
		value string
		want  [][]any
	}{
		{"ABBY.ns.cloudflare.com.", [][]any{{"dns", "cloudflare", "cloudflare", "cloudflare.dns", "abby.ns.cloudflare.com", 1.0, false}}},
		{"ns1.binero.se.", [][]any{{"dns", "binero", "binero", "binero.dns", "ns1.binero.se", KeyMatchConfidence, false}}},
		{"ns1.example.se.", [][]any{{"dns", SelfHosted, "", "", "ns1.example.se", SelfHostedConfidence, false}}},
		{"dns1.p07.nsone.net.", [][]any{{"dns", "nsone.net", "", "", "dns1.p07.nsone.net", UnmappedConfidence, false}}},
		{"192.0.2.53", [][]any{}},
		{".", [][]any{}},
	} {
		t.Run(c.value, func(t *testing.T) {
			assertResults(t, Resolve(rec("example.se", "NS", c.value), kb).Results, c.want)
		})
	}
}

func TestSOAIsFallbackEvidence(t *testing.T) {
	out := Resolve(rec("example.se.", "SOA", "ns1.binero.se. hostmaster.binero.se. 1 2 3 4 5"), kb)
	assertResults(t, out.Results, [][]any{{"dns", "binero", "binero", "binero.dns", "ns1.binero.se", KeyMatchConfidence, true}})
	if out.Results[0].Analyzer != "soa" {
		t.Fatalf("analyzer = %q", out.Results[0].Analyzer)
	}
}

func TestMXHostNullMXAndPlaceholders(t *testing.T) {
	assertResults(t, Resolve(rec("example.se", "MX", "1 ASPMX.L.GOOGLE.COM."), kb).Results,
		[][]any{{"email", "google", "google", "google.workspace-mail", "aspmx.l.google.com", 1.0, false}})
	assertResults(t, Resolve(rec("example.se", "MX", "10 mail.example.se."), kb).Results,
		[][]any{{"email", SelfHosted, "", "", "mail.example.se", SelfHostedConfidence, false}})
	// Binero is known but has no email service: named provider, empty service key.
	assertResults(t, Resolve(rec("example.se", "MX", "10 mx.binero.se."), kb).Results,
		[][]any{{"email", "binero", "binero", "", "mx.binero.se", KeyMatchConfidence, false}})
	null := Resolve(rec("example.se", "MX", "0 ."), kb)
	if len(null.Results) != 0 || len(null.Findings) != 1 || null.Findings[0].Code != "null_mx" || null.Findings[0].RecordID != "r1" {
		t.Fatalf("null MX = %+v", null)
	}
	if out := Resolve(rec("example.se", "MX", "10 localhost."), kb); len(out.Results)+len(out.Findings) != 0 {
		t.Fatalf("localhost MX = %+v", out)
	}
}

func TestCNAMEEdgeGivesOneResultPerServiceType(t *testing.T) {
	assertResults(t, Resolve(rec("www.example.se", "CNAME", "example.se.cdn.cloudflare.net."), kb).Results, [][]any{
		{"cdn", "cloudflare", "cloudflare", "cloudflare.edge", "example.se.cdn.cloudflare.net", 1.0, false},
		{"ddos_protection", "cloudflare", "cloudflare", "cloudflare.edge", "example.se.cdn.cloudflare.net", 1.0, false},
		{"waf", "cloudflare", "cloudflare", "cloudflare.edge", "example.se.cdn.cloudflare.net", 1.0, false},
	})
	assertResults(t, Resolve(rec("example.se", "CNAME", "example.github.io."), kb).Results,
		[][]any{{"hosting", "example.github.io", "", "", "example.github.io", UnmappedConfidence, false}})
}

func TestRoutingIgnoresRecordsNoAnalyzerHandles(t *testing.T) {
	for _, r := range []Record{
		rec("sub.example.se", "NS", "ns.elsewhere.net."),
		rec("shop.example.se", "CNAME", "shops.myshopify.com."),
		rec("www.example.se", "MX", "10 mx.elsewhere.net."),
		rec("example.se", "CAA", `0 issue "letsencrypt.org"`),
		rec("example.se", "A", "192.0.2.1"),
	} {
		if out := Resolve(r, kb); len(out.Results)+len(out.Findings) != 0 {
			t.Errorf("%s %s routed: %+v", r.Name, r.Type, out)
		}
	}
}

func TestResultsCarryTheRecordAndItsWindow(t *testing.T) {
	r := Record{RecordID: "abc", RootDomain: "Example.SE.", Name: "EXAMPLE.se.", Type: "ns", Value: "ns1.binero.se.",
		FirstSeen: "2026-08-10 00:00:00.000", LastSeen: "2026-09-19"}
	got := Resolve(r, kb).Results
	if len(got) != 1 {
		t.Fatalf("results = %+v", got)
	}
	g := got[0]
	if g.RecordID != "abc" || g.RootDomain != "example.se" || g.RecordName != "example.se" || g.RecordType != "NS" ||
		g.Analyzer != "ns" || g.ValidFrom != "2026-08-10" || g.ValidTo != "2026-09-19" {
		t.Fatalf("result = %+v", g)
	}
}

func TestResolveIsDeterministicAndSerialisesEmptyLists(t *testing.T) {
	r := rec("www.example.se", "CNAME", "example.se.cdn.cloudflare.net.")
	first, _ := json.Marshal(Resolve(r, kb))
	for range 20 {
		again, _ := json.Marshal(Resolve(r, kb))
		if string(again) != string(first) {
			t.Fatalf("output changed:\n%s\n%s", again, first)
		}
	}
	empty, _ := json.Marshal(Resolve(rec("example.se", "A", "192.0.2.1"), kb))
	if string(empty) != `{"record_id":"r1","analyzer":"ip","results":[],"findings":[]}` {
		t.Fatalf("empty output = %s", empty)
	}
}

func TestCNAMEToTheDomainItselfIsNotHostingEvidence(t *testing.T) {
	for _, target := range []string{"example.se.", "cdn.example.se."} {
		out := Resolve(rec("www.example.se", "CNAME", target), kb)
		if len(out.Results) != 0 {
			t.Fatalf("CNAME to %s gave results %v", target, short(out.Results))
		}
		if len(out.Findings) != 1 || out.Findings[0].Code != "cname_within_domain" {
			t.Fatalf("CNAME to %s findings = %+v", target, out.Findings)
		}
	}
}

func TestValidateRecord(t *testing.T) {
	good := rec("example.se", "NS", "ns1.binero.se.")
	if err := good.Validate(); err != nil {
		t.Fatalf("valid record refused: %v", err)
	}
	open := good
	open.FirstSeen, open.LastSeen = "", "2026-09-19 00:00:00.000"
	if err := open.Validate(); err != nil {
		t.Fatalf("open window / timestamp refused: %v", err)
	}
	for name, mutate := range map[string]func(*Record){
		"no record_id":   func(r *Record) { r.RecordID = "" },
		"no root_domain": func(r *Record) { r.RootDomain = "" },
		"no name":        func(r *Record) { r.Name = " " },
		"no type":        func(r *Record) { r.Type = "" },
		"no value":       func(r *Record) { r.Value = "" },
		"bad first_seen": func(r *Record) { r.FirstSeen = "garbage" },
		"bad last_seen":  func(r *Record) { r.LastSeen = "2026-13-40" },
		"reversed":       func(r *Record) { r.FirstSeen, r.LastSeen = "2026-09-20", "2026-09-19" },
	} {
		r := good
		mutate(&r)
		if err := r.Validate(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestOutputNamesTheAnalyzerEvenWithoutResults(t *testing.T) {
	if got := Resolve(rec("example.se", "A", "192.0.2.1"), kb).Analyzer; got != "ip" {
		t.Fatalf("routed record analyzer = %q", got)
	}
	if got := Resolve(rec("example.se", "CAA", `0 issue "letsencrypt.org"`), kb).Analyzer; got != "" {
		t.Fatalf("unrouted record analyzer = %q", got)
	}
}
