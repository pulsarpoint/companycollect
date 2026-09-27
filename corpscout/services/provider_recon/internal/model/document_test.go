package model

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func sampleDoc() Document {
	return Document{
		Version:      ContractVersion,
		Slug:         "aws",
		DisplayName:  "Amazon Web Services",
		Category:     "cloud",
		Aliases:      []string{"Amazon", "AWS"},
		ProviderKeys: []string{"cloudfront.net", "amazonaws.com"},
		Services: []Service{
			{Key: "aws.ec2", DisplayName: "EC2", ServiceTypes: []string{"iaas"}},
			{Key: "aws.cloudfront", DisplayName: "CloudFront", ServiceTypes: []string{"cdn"},
				Evidence: Evidence{IPRanges: []IPRange{
					{CIDR: "52.84.0.0/15", FeedTag: "CLOUDFRONT", Confidence: 1,
						Provenance: Provenance{Source: SourceOfficialFeed, Collector: "aws_ip_ranges", SourceVersion: "syncToken=1"}},
					{CIDR: "13.32.0.0/15", FeedTag: "CLOUDFRONT", Confidence: 1,
						Provenance: Provenance{Source: SourceOfficialFeed, Collector: "aws_ip_ranges", SourceVersion: "syncToken=1"}},
				}}},
		},
		Collection: Collection{
			CollectedAt: time.Date(2026, 9, 27, 6, 0, 0, 0, time.UTC),
			Collectors: map[string]CollectorStatus{
				"aws_ip_ranges": {Status: "ok", SourceVersion: "syncToken=1", Items: 2,
					FetchedAt: time.Date(2026, 9, 27, 6, 0, 0, 0, time.UTC)},
			},
		},
	}
}

func TestMarshalIsDeterministicRegardlessOfInputOrder(t *testing.T) {
	a := sampleDoc()
	b := sampleDoc()
	b.Aliases = []string{"AWS", "Amazon"}
	b.Services[0], b.Services[1] = b.Services[1], b.Services[0]
	r := b.Services[0].Evidence.IPRanges
	r[0], r[1] = r[1], r[0]

	ja, err := Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	jb, err := Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ja, jb) {
		t.Fatalf("marshal differs:\n%s\n---\n%s", ja, jb)
	}
}

func TestMarshalEmitsEmptyArraysNotNull(t *testing.T) {
	out, err := Marshal(Document{Version: ContractVersion, Slug: "x", Services: []Service{{Key: "x.a"}}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(out), "null") {
		t.Fatalf("marshalled document contains null:\n%s", out)
	}
}

func TestMarshalDoesNotReorderCallerSlices(t *testing.T) {
	doc := sampleDoc()
	if _, err := Marshal(doc); err != nil {
		t.Fatal(err)
	}
	if doc.Services[0].Key != "aws.ec2" {
		t.Fatalf("Marshal reordered the caller's services: %q first", doc.Services[0].Key)
	}
}

func TestContentHashIgnoresRunMetadataAndSourceVersions(t *testing.T) {
	base, err := ContentHash(sampleDoc())
	if err != nil {
		t.Fatal(err)
	}

	rerun := sampleDoc()
	rerun.Collection.CollectedAt = rerun.Collection.CollectedAt.Add(24 * time.Hour)
	rerun.Collection.Collectors["aws_ip_ranges"] = CollectorStatus{Status: "stale", SourceVersion: "syncToken=2"}
	for i := range rerun.Services[1].Evidence.IPRanges {
		rerun.Services[1].Evidence.IPRanges[i].SourceVersion = "syncToken=2"
	}
	got, err := ContentHash(rerun)
	if err != nil {
		t.Fatal(err)
	}
	if got != base {
		t.Fatalf("hash changed on metadata-only change: %s vs %s", got, base)
	}

	changed := sampleDoc()
	changed.Services[1].Evidence.IPRanges[0].CIDR = "52.86.0.0/15"
	got, err = ContentHash(changed)
	if err != nil {
		t.Fatal(err)
	}
	if got == base {
		t.Fatal("hash did not change when a CIDR changed")
	}
	if !strings.HasPrefix(base, "sha256:") {
		t.Fatalf("hash %q lacks sha256: prefix", base)
	}
}

func TestContentHashIgnoresFeedSourceURLButNotCuratedOnes(t *testing.T) {
	base, err := ContentHash(sampleDoc())
	if err != nil {
		t.Fatal(err)
	}
	moved := sampleDoc()
	for i := range moved.Services[1].Evidence.IPRanges {
		moved.Services[1].Evidence.IPRanges[i].SourceURL = "https://download.example/ServiceTags_Public_20260928.json"
	}
	if got, _ := ContentHash(moved); got != base {
		t.Fatal("hash changed when only a feed item's source URL changed (weekly Azure file name)")
	}

	curated := sampleDoc()
	curated.Services[0].Evidence.DNSRules = []DNSRule{{RecordType: "NS", MatchField: "target", MatcherType: "suffix", Pattern: "x.com",
		Confidence: 1, Priority: 100, Provenance: Provenance{Source: SourceCurated, SourceURL: "https://a.example"}}}
	h1, _ := ContentHash(curated)
	curated.Services[0].Evidence.DNSRules[0].SourceURL = "https://b.example"
	h2, _ := ContentHash(curated)
	if h1 == h2 {
		t.Fatal("a curated rule's reference URL is definition content and must change the hash")
	}
}
