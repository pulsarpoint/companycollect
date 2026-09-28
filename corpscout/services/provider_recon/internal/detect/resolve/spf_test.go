package resolve

import (
	"strings"
	"testing"
)

func spf(value string) Output { return Resolve(rec("example.se", "TXT", value), kb) }

func TestSPFIncludesSplitStringsRulesAndFallback(t *testing.T) {
	out := spf(`"v=spf1 include:_spf.google.com" " include:sendgrid.net ~all"`)
	assertResults(t, out.Results, [][]any{
		{"email_sending", "google", "google", "google.workspace-sending", "_spf.google.com", 1.0, false},
		{"email_sending", "sendgrid.net", "", "", "sendgrid.net", UnmappedConfidence, false},
	})
	if out.Results[0].Analyzer != "spf" || out.Results[0].RuleID != "google/spf" {
		t.Fatalf("result = %+v", out.Results[0])
	}
}

func TestSPFQualifiersRedirectAndSelfHosted(t *testing.T) {
	assertResults(t, spf(`"v=spf1 +include:_spf.google.com -a ~mx a/24 mx/24 ?all"`).Results, [][]any{
		{"email_sending", "google", "google", "google.workspace-sending", "_spf.google.com", 1.0, false},
		{"email_sending", SelfHosted, "", "", "example.se", SelfHostedConfidence, false},
	})
	assertResults(t, spf(`"v=spf1 redirect=_spf.google.com"`).Results, [][]any{
		{"email_sending", "google", "google", "google.workspace-sending", "_spf.google.com", 1.0, false},
	})
	assertResults(t, spf(`"v=spf1 a:mail.other.net mx:mx.example.se -all"`).Results, [][]any{
		{"email_sending", "other.net", "", "", "mail.other.net", UnmappedConfidence, false},
		{"email_sending", SelfHosted, "", "", "mx.example.se", SelfHostedConfidence, false},
	})
}

func TestSPFIgnoresIPsPTRExistsAndAll(t *testing.T) {
	out := spf(`"v=spf1 ip4:192.0.2.0/24 ip6:2001:db8::/32 ptr exists:%{i}.x.example.net -all"`)
	if len(out.Results) != 0 {
		t.Fatalf("results = %v", short(out.Results))
	}
	if out := spf(`"v=spf1"`); len(out.Results)+len(out.Findings) != 0 {
		t.Fatalf("bare v=spf1 = %+v", out)
	}
}

func TestSPFMacroLabelsItsLiteralTailAndIsAFinding(t *testing.T) {
	// Owner ruling 2026-09-28: the labels after the last macro are written in
	// the record, so they are labelled; the macro itself is still a finding.
	out := spf(`"v=spf1 include:%{i}._ip.%{h}._ehlo.%{d}._spf.vali.email ~all"`)
	assertResults(t, out.Results, [][]any{
		{"email_sending", "vali.email", "", "", "_spf.vali.email", UnmappedConfidence, false},
	})
	if len(out.Findings) != 1 || out.Findings[0].Code != "spf_macro" {
		t.Fatalf("findings = %+v", out.Findings)
	}
	// A macro with no literal tail names nothing.
	out = spf(`"v=spf1 include:%{d} -all"`)
	if len(out.Results) != 0 || len(out.Findings) != 1 || out.Findings[0].Code != "spf_macro" {
		t.Fatalf("tail-less macro = %+v", out)
	}
}

func TestSPFLookupBudget(t *testing.T) {
	parts := []string{"v=spf1"}
	for i := range 11 {
		parts = append(parts, "include:s"+string(rune('a'+i))+".sender.net")
	}
	out := spf(`"` + strings.Join(parts, " ") + ` -all"`)
	if len(out.Findings) != 1 || out.Findings[0].Code != "spf_lookup_budget_exceeded" || out.Findings[0].Detail != "11 lookups" {
		t.Fatalf("findings = %+v", out.Findings)
	}
	// Eleven includes of one provider collapse to one row per subject; all are unmapped sender.net.
	if len(out.Results) != 11 {
		t.Fatalf("results = %d, want 11 (one per include host)", len(out.Results))
	}
}

func TestSPFIsCaseInsensitive(t *testing.T) {
	assertResults(t, spf(`"V=SPF1 INCLUDE:_SPF.GOOGLE.COM -ALL"`).Results, [][]any{
		{"email_sending", "google", "google", "google.workspace-sending", "_spf.google.com", 1.0, false},
	})
}
