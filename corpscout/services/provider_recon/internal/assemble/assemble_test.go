package assemble

import (
	"errors"
	"net/netip"
	"testing"
	"time"

	"provider_recon/internal/definitions"
	"provider_recon/internal/feeds"
	"provider_recon/internal/model"
)

var t0 = time.Date(2026, 9, 27, 6, 0, 0, 0, time.UTC)

func awsDef() definitions.Definition {
	return definitions.Definition{
		Slug: "aws", DisplayName: "Amazon Web Services", Category: "cloud",
		ProviderKeys: []string{"amazonaws.com"},
		Services: []definitions.ServiceDef{
			{Key: "aws.cloudfront", DisplayName: "CloudFront", ServiceTypes: []string{"cdn"},
				DNSRules: []definitions.DNSRuleDef{{RecordType: "CNAME", MatchField: "target", MatcherType: "suffix", Pattern: "cloudfront.net", Priority: 100}}},
			{Key: "aws.ec2", DisplayName: "EC2", ServiceTypes: []string{"iaas"}},
			{Key: "aws.other", DisplayName: "Other", ServiceTypes: []string{"iaas"}},
		},
		Feeds: []definitions.FeedRef{{
			Collector:   "aws_ip_ranges",
			TagMap:      map[string]string{"CLOUDFRONT": "aws.cloudfront", "EC2": "aws.ec2", "AMAZON": "aws.other", "ROUTE53_HEALTHCHECKS": ""},
			GenericTags: []string{"AMAZON"},
		}},
	}
}

func r(cidr, tag, region string) feeds.Range {
	return feeds.Range{Prefix: netip.MustParsePrefix(cidr), Tag: tag, Region: region}
}

func okOutcome(version string, ranges ...feeds.Range) map[string]FeedOutcome {
	return map[string]FeedOutcome{"aws_ip_ranges": {Result: feeds.Result{
		SourceURL: "https://ip-ranges.amazonaws.com/ip-ranges.json", SourceVersion: version, Ranges: ranges,
	}}}
}

func service(t *testing.T, doc model.Document, key string) model.Service {
	t.Helper()
	for _, s := range doc.Services {
		if s.Key == key {
			return s
		}
	}
	t.Fatalf("service %s missing", key)
	return model.Service{}
}

func TestBuildMapsTagsAndDropsGenericDuplicates(t *testing.T) {
	doc, err := Build(awsDef(), okOutcome("syncToken=1",
		r("52.84.0.0/15", "AMAZON", "GLOBAL"),
		r("52.84.0.0/15", "CLOUDFRONT", "GLOBAL"),
		r("3.0.0.0/9", "AMAZON", "GLOBAL"),
		r("15.177.0.0/18", "ROUTE53_HEALTHCHECKS", "GLOBAL"),
	), nil, t0)
	if err != nil {
		t.Fatal(err)
	}
	cf := service(t, doc, "aws.cloudfront").Evidence.IPRanges
	other := service(t, doc, "aws.other").Evidence.IPRanges
	if len(cf) != 1 || cf[0].CIDR != "52.84.0.0/15" || cf[0].Source != model.SourceOfficialFeed || cf[0].Collector != "aws_ip_ranges" {
		t.Fatalf("cloudfront ranges = %+v", cf)
	}
	if len(other) != 1 || other[0].CIDR != "3.0.0.0/9" {
		t.Fatalf("AMAZON duplicate of a specific range was kept, or unique AMAZON dropped: %+v", other)
	}
	st := doc.Collection.Collectors["aws_ip_ranges"]
	if st.Status != "ok" || st.Items != 2 || len(st.UnmappedTags) != 0 || st.LastSuccessAt == nil {
		t.Fatalf("status = %+v", st)
	}
	if rule := service(t, doc, "aws.cloudfront").Evidence.DNSRules; len(rule) != 1 || rule[0].Source != model.SourceCurated || rule[0].Confidence != 1 {
		t.Fatalf("curated dns rule = %+v", rule)
	}
	if doc.Version != model.ContractVersion || doc.Collection.ContentHash == "" {
		t.Fatalf("version=%q hash=%q", doc.Version, doc.Collection.ContentHash)
	}
}

func TestBuildReportsUnmappedTags(t *testing.T) {
	doc, err := Build(awsDef(), okOutcome("syncToken=1",
		r("52.84.0.0/15", "CLOUDFRONT", ""),
		r("18.0.0.0/16", "NEW_SERVICE", ""),
		r("18.1.0.0/16", "NEW_SERVICE", ""),
	), nil, t0)
	if err != nil {
		t.Fatal(err)
	}
	st := doc.Collection.Collectors["aws_ip_ranges"]
	if len(st.UnmappedTags) != 1 || st.UnmappedTags[0] != "NEW_SERVICE" {
		t.Fatalf("unmapped = %v", st.UnmappedTags)
	}
	for _, s := range doc.Services {
		for _, ip := range s.Evidence.IPRanges {
			if ip.FeedTag == "NEW_SERVICE" {
				t.Fatalf("unmapped tag attached to %s", s.Key)
			}
		}
	}
}

func TestBuildKeepsPreviousRangesWhenFeedFails(t *testing.T) {
	first, err := Build(awsDef(), okOutcome("syncToken=1", r("52.84.0.0/15", "CLOUDFRONT", "")), nil, t0)
	if err != nil {
		t.Fatal(err)
	}
	failed := map[string]FeedOutcome{"aws_ip_ranges": {Err: errors.New("status 503")}}
	second, err := Build(awsDef(), failed, &first, t0.Add(24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	st := second.Collection.Collectors["aws_ip_ranges"]
	if st.Status != "stale" || st.Error != "status 503" || st.LastSuccessAt == nil || !st.LastSuccessAt.Equal(t0) || st.Items != 1 {
		t.Fatalf("status = %+v", st)
	}
	if len(service(t, second, "aws.cloudfront").Evidence.IPRanges) != 1 {
		t.Fatal("previous ranges not carried forward")
	}
	if second.Collection.ContentHash != first.Collection.ContentHash {
		t.Fatal("a stale carry-forward must not change the content hash")
	}
}

func TestBuildFailsWithoutHistoryButKeepsCuratedEvidence(t *testing.T) {
	doc, err := Build(awsDef(), map[string]FeedOutcome{"aws_ip_ranges": {Err: feeds.ErrEmptyFeed}}, nil, t0)
	if err != nil {
		t.Fatal(err)
	}
	if st := doc.Collection.Collectors["aws_ip_ranges"]; st.Status != "failed" || st.LastSuccessAt != nil {
		t.Fatalf("status = %+v", st)
	}
	if len(service(t, doc, "aws.cloudfront").Evidence.DNSRules) != 1 {
		t.Fatal("curated evidence lost when the feed failed")
	}
}

func TestBuildMissingOutcomeCountsAsFailure(t *testing.T) {
	doc, err := Build(awsDef(), map[string]FeedOutcome{}, nil, t0)
	if err != nil {
		t.Fatal(err)
	}
	if st := doc.Collection.Collectors["aws_ip_ranges"]; st.Status != "failed" || st.Error == "" {
		t.Fatalf("status = %+v", st)
	}
}

func TestBuildHashStableAcrossRunsWithNewSyncToken(t *testing.T) {
	a, err := Build(awsDef(), okOutcome("syncToken=1", r("52.84.0.0/15", "CLOUDFRONT", "")), nil, t0)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Build(awsDef(), okOutcome("syncToken=2", r("52.84.0.0/15", "CLOUDFRONT", "")), &a, t0.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if a.Collection.ContentHash != b.Collection.ContentHash {
		t.Fatal("hash changed although only the sync token moved")
	}
}

func TestBuildDedupesRegionalDuplicatesKeepingRegion(t *testing.T) {
	def := definitions.Definition{
		Slug: "microsoft", DisplayName: "Microsoft", Category: "cloud",
		Services: []definitions.ServiceDef{{Key: "microsoft.azure-cloud", DisplayName: "Azure", ServiceTypes: []string{"iaas"}}},
		Feeds:    []definitions.FeedRef{{Collector: "azure_service_tags", TagMap: map[string]string{"AzureCloud*": "microsoft.azure-cloud"}}},
	}
	outcomes := map[string]FeedOutcome{"azure_service_tags": {Result: feeds.Result{SourceVersion: "changeNumber=1", Ranges: []feeds.Range{
		r("20.50.0.0/16", "AzureCloud", ""),
		r("20.50.0.0/16", "AzureCloud.westeurope", "westeurope"),
	}}}}
	doc, err := Build(def, outcomes, nil, t0)
	if err != nil {
		t.Fatal(err)
	}
	got := doc.Services[0].Evidence.IPRanges
	if len(got) != 1 || got[0].Region != "westeurope" {
		t.Fatalf("ranges = %+v", got)
	}
}

func TestBuildBGPSourceAndConfidence(t *testing.T) {
	def := definitions.Definition{
		Slug: "hetzner", DisplayName: "Hetzner", Category: "hosting",
		Services: []definitions.ServiceDef{{Key: "hetzner.cloud", DisplayName: "Hetzner", ServiceTypes: []string{"hosting"}}},
		Feeds:    []definitions.FeedRef{{Collector: "ripestat_announced", Params: map[string]string{"asn": "24940"}, TagMap: map[string]string{"BGP": "hetzner.cloud"}}},
	}
	outcomes := map[string]FeedOutcome{"ripestat_announced:asn=24940": {Result: feeds.Result{Ranges: []feeds.Range{r("88.198.0.0/16", "BGP", "")}}}}
	doc, err := Build(def, outcomes, nil, t0)
	if err != nil {
		t.Fatal(err)
	}
	ip := doc.Services[0].Evidence.IPRanges[0]
	if ip.Source != model.SourceBGP || ip.Confidence != 0.8 || ip.Collector != "ripestat_announced:asn=24940" {
		t.Fatalf("%+v", ip)
	}
}

func TestBuildKeepsGenericRangeWhenOnlyAnIgnoredTagRepeatsIt(t *testing.T) {
	def := definitions.Definition{
		Slug: "microsoft", DisplayName: "Microsoft", Category: "cloud",
		Services: []definitions.ServiceDef{{Key: "microsoft.azure-cloud", DisplayName: "Azure", ServiceTypes: []string{"iaas"}}},
		Feeds: []definitions.FeedRef{{
			Collector:   "azure_service_tags",
			TagMap:      map[string]string{"AzureCloud*": "microsoft.azure-cloud", "*": ""},
			GenericTags: []string{"AzureCloud*"},
		}},
	}
	outcomes := map[string]FeedOutcome{"azure_service_tags": {Result: feeds.Result{Ranges: []feeds.Range{
		r("20.50.0.0/24", "AzureCloud.westeurope", "westeurope"),
		r("20.50.0.0/24", "AzureMonitor", ""),
	}}}}
	doc, err := Build(def, outcomes, nil, t0)
	if err != nil {
		t.Fatal(err)
	}
	if got := doc.Services[0].Evidence.IPRanges; len(got) != 1 || got[0].CIDR != "20.50.0.0/24" {
		t.Fatalf("umbrella range dropped because an ignored tag repeated it: %+v", got)
	}
}
