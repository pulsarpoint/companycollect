package resolve

import (
	"fmt"
	"net/netip"
	"reflect"
	"testing"

	"provider_recon/internal/detect/knowledge"
)

func rng(cidr, service, from, to string) knowledge.IPRange {
	p := netip.MustParsePrefix(cidr)
	return knowledge.IPRange{Prefix: p, ProviderSlug: "p", ServiceKey: service, ServiceTypes: []string{"cdn"}, Confidence: 1,
		From: from, To: to, RuleID: "p/" + service + "/IP " + p.String()}
}

func pieceStrings(ps []ipPiece) []string {
	out := []string{}
	for _, p := range ps {
		out = append(out, fmt.Sprintf("%s..%s %s", p.From, p.To, p.Range.ServiceKey))
	}
	return out
}

func TestIPPieces(t *testing.T) {
	r15 := rng("52.84.0.0/15", "cloudfront", "2026-03-01", "2026-06-30")
	r14 := rng("52.84.0.0/14", "other", "", "")
	for _, c := range []struct {
		name     string
		from, to string
		ranges   []knowledge.IPRange
		want     []string
	}{
		{"longer prefix wins while valid", "2026-01-01", "2026-12-31", []knowledge.IPRange{r15, r14},
			[]string{"2026-01-01..2026-02-28 other", "2026-03-01..2026-06-30 cloudfront", "2026-07-01..2026-12-31 other"}},
		{"only a mid-window range", "2026-01-01", "2026-12-31", []knowledge.IPRange{r15},
			[]string{"2026-03-01..2026-06-30 cloudfront"}},
		{"range outside the window", "2026-07-01", "2026-08-01", []knowledge.IPRange{r15}, []string{}},
		{"open window, open range", "", "", []knowledge.IPRange{r14}, []string{".. other"}},
		{"open window cut by a dated range", "", "", []knowledge.IPRange{r15},
			[]string{"2026-03-01..2026-06-30 cloudfront"}},
		{"record inside the range", "2026-04-01", "2026-04-10", []knowledge.IPRange{r15, r14},
			[]string{"2026-04-01..2026-04-10 cloudfront"}},
		{"re-added instance of the same range merges", "2026-01-01", "2026-12-31", []knowledge.IPRange{
			rng("52.84.0.0/15", "cloudfront", "", "2026-05-01"), rng("52.84.0.0/15", "cloudfront", "2026-05-02", ""),
		}, []string{"2026-01-01..2026-12-31 cloudfront"}},
		{"gap between instances stays a gap", "2026-01-01", "2026-12-31", []knowledge.IPRange{
			rng("52.84.0.0/15", "cloudfront", "", "2026-05-01"), rng("52.84.0.0/15", "cloudfront", "2026-06-01", ""),
		}, []string{"2026-01-01..2026-05-01 cloudfront", "2026-06-01..2026-12-31 cloudfront"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := pieceStrings(ipPieces(c.from, c.to, c.ranges)); !reflect.DeepEqual(got, c.want) {
				t.Fatalf("got %v, want %v", got, c.want)
			}
		})
	}
}

type windowed struct {
	ServiceType, ProviderKey, ServiceKey, Subject, From, To string
}

func windows(rs []Result) []windowed {
	out := []windowed{}
	for _, r := range rs {
		out = append(out, windowed{r.ServiceType, r.ProviderKey, r.ServiceKey, r.Subject, r.ValidFrom, r.ValidTo})
	}
	return out
}

func TestIPAnalyzerCutsTheRecordWindowAtRangeChanges(t *testing.T) {
	r := Record{RecordID: "a1", RootDomain: "example.se", Name: "example.se", Type: "A", Value: "52.84.1.1", FirstSeen: "2026-01-01", LastSeen: "2026-09-19"}
	out := Resolve(r, kb)
	want := []windowed{
		{"cdn", "aws", "aws.cloudfront", "52.84.1.1", "2026-03-01", "2026-09-19"},
		{"iaas", "aws", "aws.other", "52.84.1.1", "2026-01-01", "2026-02-28"},
	}
	if got := windows(out.Results); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}
	if out.Results[0].Analyzer != "ip" || out.Results[0].RuleID != "aws/aws.cloudfront/IP 52.84.0.0/15" {
		t.Fatalf("result = %+v", out.Results[0])
	}
}

func TestIPAnalyzerRoutingAndValues(t *testing.T) {
	if got := windows(Resolve(rec("www.example.se", "AAAA", "2600:9000:1::1"), kb).Results); !reflect.DeepEqual(got, []windowed{
		{"cdn", "aws", "aws.cloudfront", "2600:9000:1::1", "2026-08-10", "2026-09-19"},
	}) {
		t.Fatalf("www AAAA = %+v", got)
	}
	if got := windows(Resolve(rec("example.se", "A", "::ffff:52.84.1.1"), kb).Results); len(got) != 1 || got[0].Subject != "52.84.1.1" {
		t.Fatalf("mapped address = %+v", got)
	}
	for _, r := range []Record{rec("shop.example.se", "A", "52.84.1.1"), rec("example.se", "A", "not-an-ip"), rec("example.se", "A", "192.0.2.1")} {
		if out := Resolve(r, kb); len(out.Results) != 0 {
			t.Errorf("%s %s gave %+v", r.Name, r.Value, windows(out.Results))
		}
	}
}

func TestSPFIPMechanismsUseTheRangeIndex(t *testing.T) {
	got := windows(spf(`"v=spf1 ip4:198.51.100.0/24 ip4:203.0.113.5 ip4:192.0.2.0/24 ip6:2600:9000::1 -all"`).Results)
	want := []windowed{
		{"email_sending", "aws", "", "2600:9000::1/128", "2026-08-10", "2026-09-19"},
		{"email_sending", "hosty", "", "203.0.113.5/32", "2026-08-10", "2026-09-19"},
		{"email_sending", "mailchimp", "mailchimp.sending", "198.51.100.0/24", "2026-08-10", "2026-09-19"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}
}

func TestResultsAreChronologicalAndRepeatedTermsDeduplicated(t *testing.T) {
	got := windows(spf(`"v=spf1 ip4:198.18.0.0/24 ip4:198.18.0.0/24 -all"`).Results)
	want := []windowed{
		{"email_sending", "sendy", "sendy.mail", "198.18.0.0/24", "2026-08-10", "2026-08-20"},
		{"email_sending", "sendy", "sendy.mail", "198.18.0.0/24", "2026-09-01", "2026-09-19"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}
}
