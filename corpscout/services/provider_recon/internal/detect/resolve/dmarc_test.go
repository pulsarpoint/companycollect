package resolve

import "testing"

func dmarc(value string) Output { return Resolve(rec("_dmarc.example.se.", "TXT", value), kb) }

func TestDMARCReportingProviders(t *testing.T) {
	out := dmarc(`"v=DMARC1; p=reject; rua=mailto:a@rua.dmarcian.com,mailto:dmarc@example.se; ruf=MAILTO:x@ruf.agari.com!10m"`)
	assertResults(t, out.Results, [][]any{
		{"dmarc_reporting", "agari.com", "", "", "ruf.agari.com", UnmappedConfidence, false},
		{"dmarc_reporting", "dmarcian", "dmarcian", "dmarcian.reporting", "rua.dmarcian.com", 1.0, false},
	})
	if out.Results[0].Analyzer != "dmarc" || len(out.Findings) != 0 {
		t.Fatalf("out = %+v", out)
	}
}

func TestDMARCWithoutReportsGivesNothing(t *testing.T) {
	if out := dmarc(`"v=DMARC1; p=none"`); len(out.Results)+len(out.Findings) != 0 {
		t.Fatalf("out = %+v", out)
	}
	// Repeated and spaced addresses collapse to one row.
	assertResults(t, dmarc(`"v=DMARC1;p=quarantine; rua = mailto:a@rua.dmarcian.com , mailto:b@rua.dmarcian.com"`).Results, [][]any{
		{"dmarc_reporting", "dmarcian", "dmarcian", "dmarcian.reporting", "rua.dmarcian.com", 1.0, false},
	})
}

func TestDMARCInvalidRecordIsAFinding(t *testing.T) {
	out := dmarc(`"hello world"`)
	if len(out.Results) != 0 || len(out.Findings) != 1 || out.Findings[0].Code != "dmarc_invalid" {
		t.Fatalf("out = %+v", out)
	}
}

func TestDMARCOnlyAtTheDomainsOwnDmarcName(t *testing.T) {
	a := Route(normalise(rec("_dmarc.sub.example.se", "TXT", `"v=DMARC1; rua=mailto:a@rua.dmarcian.com"`)))
	if a != nil && a.Name() == "dmarc" {
		t.Fatal("_dmarc of a subdomain routed to dmarc")
	}
}
