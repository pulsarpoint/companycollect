package matcher

import (
	"net/netip"
	"testing"

	"provider_recon/internal/model"
)

func rng(cidr, tag string) model.IPRange {
	return model.IPRange{CIDR: cidr, FeedTag: tag, Confidence: 1, Provenance: model.Provenance{Source: model.SourceOfficialFeed}}
}

func TestIPTableLongestPrefixWins(t *testing.T) {
	docs := []model.Document{
		{Slug: "aws", Services: []model.Service{
			{Key: "aws.other", Evidence: model.Evidence{IPRanges: []model.IPRange{rng("52.0.0.0/10", "AMAZON")}}},
			{Key: "aws.cloudfront", Evidence: model.Evidence{IPRanges: []model.IPRange{rng("52.84.0.0/15", "CLOUDFRONT"), rng("2600:9000::/28", "CLOUDFRONT")}}},
		}},
		{Slug: "other", Services: []model.Service{
			{Key: "other.x", Evidence: model.Evidence{IPRanges: []model.IPRange{rng("52.84.0.0/15", "X")}}},
		}},
	}
	table, err := NewIPTable(docs)
	if err != nil {
		t.Fatal(err)
	}

	p, entries, ok := table.Lookup(netip.MustParseAddr("52.84.1.1"))
	if !ok || p.String() != "52.84.0.0/15" || len(entries) != 2 {
		t.Fatalf("52.84.1.1 → %v %v %v", p, entries, ok)
	}
	if entries[0].ServiceKey != "aws.cloudfront" || entries[1].ProviderSlug != "other" {
		t.Fatalf("entries in unexpected order: %+v", entries)
	}

	_, entries, ok = table.Lookup(netip.MustParseAddr("52.1.1.1"))
	if !ok || entries[0].ServiceKey != "aws.other" {
		t.Fatalf("52.1.1.1 → %+v %v", entries, ok)
	}

	_, entries, ok = table.Lookup(netip.MustParseAddr("2600:9000::1"))
	if !ok || entries[0].ServiceKey != "aws.cloudfront" {
		t.Fatalf("v6 lookup → %+v %v", entries, ok)
	}

	if _, _, ok := table.Lookup(netip.MustParseAddr("10.0.0.1")); ok {
		t.Fatal("10.0.0.1 unexpectedly matched")
	}
}

func TestIPTableRejectsBadCIDR(t *testing.T) {
	_, err := NewIPTable([]model.Document{{Slug: "x", Services: []model.Service{
		{Key: "x.a", Evidence: model.Evidence{IPRanges: []model.IPRange{rng("nope", "")}}},
	}}})
	if err == nil {
		t.Fatal("expected error for invalid CIDR")
	}
}
