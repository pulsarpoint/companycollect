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
