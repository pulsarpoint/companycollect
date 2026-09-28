package resolve

import "testing"

func TestDKIMCNAMEUsesTargetAndSelectorRules(t *testing.T) {
	out := Resolve(rec("selector1._domainkey.example.se.", "CNAME", "selector1-example-se._domainkey.contoso.onmicrosoft.com."), kb)
	assertResults(t, out.Results, [][]any{
		{"email_sending", "microsoft", "microsoft", "microsoft.365-sending", "selector1", 0.6, false},
		{"email_sending", "microsoft", "microsoft", "microsoft.365-sending", "selector1-example-se._domainkey.contoso.onmicrosoft.com", 1.0, false},
	})
	if out.Results[0].Analyzer != "dkim" {
		t.Fatalf("analyzer = %q", out.Results[0].Analyzer)
	}
}

func TestDKIMCNAMEFallsBackToTheTargetsProviderKey(t *testing.T) {
	assertResults(t, Resolve(rec("k1._domainkey.example.se", "CNAME", "dkim.mcsv.net."), kb).Results, [][]any{
		{"email_sending", "mcsv.net", "", "", "dkim.mcsv.net", UnmappedConfidence, false},
	})
}

func TestDKIMTXTUsesSelectorRulesOnly(t *testing.T) {
	assertResults(t, Resolve(rec("google._domainkey.example.se", "TXT", `"v=DKIM1; k=rsa; p=MIGfMA0G"`), kb).Results, [][]any{
		{"email_sending", "google", "google", "google.workspace-sending", "google", 0.7, false},
	})
	if out := Resolve(rec("s2024._domainkey.example.se", "TXT", `"v=DKIM1; p=MIGf"`), kb); len(out.Results) != 0 {
		t.Fatalf("unknown selector gave %v", short(out.Results))
	}
}

func TestDKIMNamesOutsideTheDomainAreNotRouted(t *testing.T) {
	r := rec("selector1._domainkey.other.se", "CNAME", "x.onmicrosoft.com.")
	if a := Route(normalise(r)); a != nil {
		t.Fatalf("routed to %s", a.Name())
	}
}
