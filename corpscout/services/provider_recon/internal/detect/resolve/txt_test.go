package resolve

import "testing"

func TestTXTValueJoinsAndUnquotesStrings(t *testing.T) {
	for in, want := range map[string]string{
		`"a" "b"`:                        "ab",
		`"v=spf1 include:x" " ~all"`:     "v=spf1 include:x ~all",
		`"say \"hi\""`:                   `say "hi"`,
		`"back\\slash"`:                  `back\slash`,
		`plain`:                          "plain",
		`  "spaced"  `:                   "spaced",
		``:                               "",
		`"google-site-verification=abc"`: "google-site-verification=abc",
		`"unterminated`:                  "unterminated",
	} {
		if got := txtValue(in); got != want {
			t.Errorf("txtValue(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestTXTApexValueRulesOnly(t *testing.T) {
	assertResults(t, Resolve(rec("example.se", "TXT", `"apple-domain-verification=xyz"`), kb).Results,
		[][]any{{"saas_verification", "apple", "apple", "apple.domain-verification", "apple-domain-verification=xyz", 0.9, false}})
	// No rule: no provider-key fallback for TXT.
	if out := Resolve(rec("example.se", "TXT", `"some-unknown-token=1"`), kb); len(out.Results)+len(out.Findings) != 0 {
		t.Fatalf("unknown TXT gave %+v", out)
	}
}

func TestTXTUnderscoreNameMatchedByName(t *testing.T) {
	out := Resolve(rec("_amazonses.example.se.", "TXT", `"abcdef=="`), kb)
	assertResults(t, out.Results, [][]any{{"email_sending", "aws", "aws", "aws.ses", "_amazonses", 0.9, false}})
	if out.Results[0].Analyzer != "txt" {
		t.Fatalf("analyzer = %q", out.Results[0].Analyzer)
	}
	if out := Resolve(rec("_acme-challenge.www.example.se", "TXT", `"token"`), kb); len(out.Results) != 0 {
		t.Fatalf("unmatched _name TXT gave %v", short(out.Results))
	}
}

func TestTXTRoutingLeavesSPFDMARCDKIMAndPlainSubdomainsAlone(t *testing.T) {
	for _, r := range []Record{
		rec("www.example.se", "TXT", `"apple-domain-verification=xyz"`),
		rec("_dmarc.example.se", "TXT", `"v=DMARC1; p=none"`),
		rec("google._domainkey.example.se", "TXT", `"v=DKIM1; k=rsa; p=MIGf"`),
	} {
		if a := Route(normalise(r)); a != nil && a.Name() == "txt" {
			t.Errorf("%s routed to txt", r.Name)
		}
	}
	if a := Route(normalise(rec("example.se", "TXT", `"v=spf1 -all"`))); a != nil && a.Name() == "txt" {
		t.Error("SPF record routed to txt")
	}
}
