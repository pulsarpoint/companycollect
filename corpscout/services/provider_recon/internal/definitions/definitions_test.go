package definitions

import (
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var update = flag.Bool("update", false, "rewrite definitions/schema.json")

type fakeFeed struct{}

func (fakeFeed) ValidateParams(p map[string]string) error {
	if len(p) > 0 {
		return errors.New("takes no params")
	}
	return nil
}

func collectors() map[string]ParamValidator {
	return map[string]ParamValidator{"fake_feed": fakeFeed{}}
}

func writeYAML(t *testing.T, dir, name, body string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestValidExampleLoadsAndNormalizes(t *testing.T) {
	defs, err := LoadDir("testdata/valid")
	if err != nil {
		t.Fatal(err)
	}
	if err := Validate(defs, collectors()); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	d := defs[0]
	if d.Category != "cloud" || d.Country != "SE" {
		t.Errorf("category/country not normalized: %q %q", d.Category, d.Country)
	}
	if strings.Join(d.ProviderKeys, ",") != "example-dns-*,example.com" {
		t.Errorf("provider keys = %v", d.ProviderKeys)
	}
	if strings.Join(d.Aliases, ",") != "Example,Example AB" {
		t.Errorf("aliases = %v (case-insensitive dedupe, sorted)", d.Aliases)
	}
	r := d.Services[0].DNSRules[0]
	if r.RecordType != "CNAME" || r.MatchField != "target" || r.MatcherType != "suffix" || r.Pattern != "cdn.example.com" || r.Priority != 100 {
		t.Errorf("dns rule not normalized: %+v", r)
	}
	if got := d.Services[1].DNSRules[0].Pattern; got != `^ns[0-9]+\.Example\.com$` {
		t.Errorf("regex pattern must be kept verbatim, got %q", got)
	}
	if h := d.Services[0].HTTPRules[0].HeaderName; h != "x-example-id" {
		t.Errorf("header name = %q", h)
	}
}

func TestLoadFileRejectsUnknownField(t *testing.T) {
	p := writeYAML(t, t.TempDir(), "x.yaml", "slug: x\ndisplay_name: X\ncategory: cloud\nservces: []\n")
	_, err := LoadFile(p)
	if err == nil || !strings.Contains(err.Error(), "servces") {
		t.Fatalf("err = %v, want unknown field servces", err)
	}
}

func TestLoadFileRejectsSecondDocument(t *testing.T) {
	p := writeYAML(t, t.TempDir(), "x.yaml", "slug: x\n---\nslug: y\n")
	_, err := LoadFile(p)
	if err == nil || !strings.Contains(err.Error(), "one definition per file") {
		t.Fatalf("err = %v", err)
	}
}

func base() Definition {
	return Definition{
		Slug: "acme", DisplayName: "Acme", Category: "hosting", File: "defs/acme.yaml",
		Services: []ServiceDef{{Key: "acme.web", DisplayName: "Web", ServiceTypes: []string{"hosting"}}},
	}
}

func TestValidateReportsProblems(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Definition)
		want   string
	}{
		{"service type typo", func(d *Definition) { d.Services[0].ServiceTypes = []string{"hostng"} }, `services[0].service_types: unknown service type "hostng"`},
		{"host bits", func(d *Definition) { d.Services[0].IPRanges = []IPRangeDef{{CIDR: "10.0.0.1/8"}} }, "did you mean 10.0.0.0/8"},
		{"bad cidr", func(d *Definition) { d.Services[0].IPRanges = []IPRangeDef{{CIDR: "10.0.0/8"}} }, "services[0].ip_ranges[0]"},
		{"service key prefix", func(d *Definition) { d.Services[0].Key = "web" }, `must start with "acme."`},
		{"duplicate service", func(d *Definition) { d.Services = append(d.Services, d.Services[0]) }, `duplicate service key "acme.web"`},
		{"bad regex", func(d *Definition) {
			d.Services[0].DNSRules = []DNSRuleDef{{RecordType: "NS", MatchField: "target", MatcherType: "regex", Pattern: "("}}
		}, "invalid regex"},
		{"bad record type", func(d *Definition) {
			d.Services[0].DNSRules = []DNSRuleDef{{RecordType: "SOA", MatchField: "target", MatcherType: "suffix", Pattern: "x"}}
		}, `unsupported record_type "SOA"`},
		{"header without name", func(d *Definition) {
			d.Services[0].HTTPRules = []HTTPRuleDef{{HTTPPart: "header", MatcherType: "exists"}}
		}, "header_name is required"},
		{"unknown collector", func(d *Definition) {
			d.Feeds = []FeedRef{{Collector: "nope", TagMap: map[string]string{"X": "acme.web"}}}
		}, `unknown collector "nope"`},
		{"tag to missing service", func(d *Definition) {
			d.Feeds = []FeedRef{{Collector: "fake_feed", TagMap: map[string]string{"X": "acme.cdn"}}}
		}, `tag_map["X"] points to unknown service "acme.cdn"`},
		{"collector params", func(d *Definition) {
			d.Feeds = []FeedRef{{Collector: "fake_feed", Params: map[string]string{"a": "b"}, TagMap: map[string]string{"X": "acme.web"}}}
		}, "takes no params"},
		{"file name", func(d *Definition) { d.File = "defs/other.yaml" }, "file must be named acme.yaml"},
		{"confidence range", func(d *Definition) {
			c := 1.5
			d.Services[0].ASNs = []ASNDef{{ASN: 1, Confidence: &c}}
		}, "confidence must be between 0 and 1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := base()
			c.mutate(&d)
			err := Validate([]Definition{d}, collectors())
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v, want substring %q", err, c.want)
			}
			if !strings.Contains(err.Error(), "defs/") {
				t.Fatalf("error does not name the file: %v", err)
			}
		})
	}
}

func TestValidateRejectsProviderKeyClaimedTwice(t *testing.T) {
	a, b := base(), base()
	b.Slug, b.File, b.Services[0].Key = "other", "defs/other.yaml", "other.web"
	a.ProviderKeys = []string{"shared.net"}
	b.ProviderKeys = []string{"shared.net"}
	err := Validate([]Definition{a, b}, collectors())
	if err == nil || !strings.Contains(err.Error(), `key "shared.net" also claimed by acme`) {
		t.Fatalf("err = %v", err)
	}
}

func TestResolveTag(t *testing.T) {
	f := FeedRef{TagMap: map[string]string{
		"EC2": "aws.ec2", "AzureCloud*": "az.cloud", "AzureCloud.west*": "az.west", "*": "aws.other", "HEALTH": "",
	}}
	cases := map[string]struct {
		svc string
		ok  bool
	}{
		"EC2":                   {"aws.ec2", true},
		"AzureCloud.westeurope": {"az.west", true}, // longest glob wins
		"AzureCloud":            {"az.cloud", true},
		"NEW_SERVICE":           {"aws.other", true},
		"HEALTH":                {"", true}, // explicitly ignored
	}
	for tag, want := range cases {
		svc, ok := f.ResolveTag(tag)
		if svc != want.svc || ok != want.ok {
			t.Errorf("ResolveTag(%q) = %q,%v want %q,%v", tag, svc, ok, want.svc, want.ok)
		}
	}
	if _, ok := (FeedRef{TagMap: map[string]string{"EC2": "x"}}).ResolveTag("S3"); ok {
		t.Error("unmapped tag resolved")
	}
}

func TestFeedRefID(t *testing.T) {
	if got := (FeedRef{Collector: "aws_ip_ranges"}).ID(); got != "aws_ip_ranges" {
		t.Errorf("ID = %q", got)
	}
	f := FeedRef{Collector: "ripestat_announced", Params: map[string]string{"asn": "24940"}}
	if got := f.ID(); got != "ripestat_announced:asn=24940" {
		t.Errorf("ID = %q", got)
	}
}

func TestSchemaIsCurrent(t *testing.T) {
	got, err := Schema()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join("..", "..", "definitions", "schema.json")
	if *update {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (run: go test ./internal/definitions -run TestSchemaIsCurrent -update)", err)
	}
	if string(got) != string(want) {
		t.Fatal("definitions/schema.json is stale; run: go test ./internal/definitions -run TestSchemaIsCurrent -update")
	}
}

func TestFeedGraceDefault(t *testing.T) {
	if g := (FeedRef{}).Grace(); g != 7 {
		t.Fatalf("default grace = %d", g)
	}
	if g := (FeedRef{RemovalGraceDays: 14}).Grace(); g != 14 {
		t.Fatalf("explicit grace = %d", g)
	}
}

func TestValidateFeedLifecycleSettings(t *testing.T) {
	feed := func(mut func(*FeedRef)) Definition {
		d := base()
		f := FeedRef{Collector: "fake_feed", TagMap: map[string]string{"X": "acme.web"}}
		mut(&f)
		d.Feeds = []FeedRef{f}
		return d
	}
	cases := []struct {
		name string
		def  Definition
		want string
	}{
		{"negative grace", feed(func(f *FeedRef) { f.RemovalGraceDays = -1 }), "removal_grace_days: must be between 1 and 365"},
		{"huge grace", feed(func(f *FeedRef) { f.RemovalGraceDays = 400 }), "removal_grace_days: must be between 1 and 365"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := Validate([]Definition{c.def}, collectors())
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v, want %q", err, c.want)
			}
		})
	}

	d := base()
	d.Feeds = []FeedRef{
		{Collector: "fake_feed", TagMap: map[string]string{"X": "acme.web"}},
		{Collector: "fake_feed", TagMap: map[string]string{"Y": "acme.web"}},
	}
	err := Validate([]Definition{d}, collectors())
	if err == nil || !strings.Contains(err.Error(), `duplicate feed "fake_feed"`) {
		t.Fatalf("duplicate feed: err = %v", err)
	}
}
