package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"provider_recon/internal/assemble"
	"provider_recon/internal/definitions"
	"provider_recon/internal/feeds"
	"provider_recon/internal/model"
	"provider_recon/internal/publish"
)

const awsYAML = `slug: aws
display_name: Amazon Web Services
category: cloud
services:
  - key: aws.cloudfront
    display_name: CloudFront
    service_types: [cdn]
feeds:
  - collector: aws_ip_ranges
    tag_map: {CLOUDFRONT: aws.cloudfront}
`

func withAWSServer(t *testing.T, body string) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(body)) }))
	t.Cleanup(srv.Close)
	orig := registry
	registry = func() map[string]feeds.Collector {
		return map[string]feeds.Collector{"aws_ip_ranges": &feeds.AWS{URL: srv.URL}}
	}
	t.Cleanup(func() { registry = orig })
}

func defsDir(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestCollectTwiceIsIdempotent(t *testing.T) {
	withAWSServer(t, `{"syncToken":"1","prefixes":[{"ip_prefix":"52.84.0.0/15","region":"GLOBAL","service":"CLOUDFRONT"}],"ipv6_prefixes":[{"ipv6_prefix":"2600:9000::/28","region":"GLOBAL","service":"CLOUDFRONT"}]}`)
	defs := defsDir(t, map[string]string{"aws.yaml": awsYAML})
	out := t.TempDir()

	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"collect", "-definitions", defs, "-out", out}, &stdout, &stderr); code != 0 {
		t.Fatalf("first collect exit %d: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "1 changed, 0 unchanged") {
		t.Fatalf("stdout = %s", stdout.String())
	}
	stdout.Reset()
	if code := run(context.Background(), []string{"collect", "-definitions", defs, "-out", out}, &stdout, &stderr); code != 0 {
		t.Fatalf("second collect exit %d: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "0 changed, 1 unchanged") {
		t.Fatalf("stdout = %s", stdout.String())
	}
	if _, err := os.Stat(filepath.Join(out, "providers", "aws", "latest.json")); err != nil {
		t.Fatal(err)
	}
}

func TestCollectExitsTwoWhenACollectorFails(t *testing.T) {
	withAWSServer(t, `<html>down</html>`)
	defs := defsDir(t, map[string]string{"aws.yaml": awsYAML})
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"collect", "-definitions", defs, "-out", t.TempDir()}, &stdout, &stderr)
	if code != 2 || !strings.Contains(stdout.String(), "issue aws aws_ip_ranges failed") {
		t.Fatalf("exit %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
}

func TestCollectRefusesInvalidDefinitionsAndWritesNothing(t *testing.T) {
	withAWSServer(t, `{}`)
	defs := defsDir(t, map[string]string{"aws.yaml": strings.Replace(awsYAML, "[cdn]", "[cnd]", 1)})
	out := t.TempDir()
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"collect", "-definitions", defs, "-out", out}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit %d", code)
	}
	if !strings.Contains(stderr.String(), `unknown service type "cnd"`) {
		t.Fatalf("stderr = %s", stderr.String())
	}
	if entries, _ := os.ReadDir(out); len(entries) != 0 {
		t.Fatalf("collect wrote %d entries despite invalid definitions", len(entries))
	}
}

func TestCollectUnknownProvider(t *testing.T) {
	withAWSServer(t, `{}`)
	defs := defsDir(t, map[string]string{"aws.yaml": awsYAML})
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"collect", "-definitions", defs, "-provider", "nope", "-out", t.TempDir()}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit %d", code)
	}
}

func TestUsage(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), nil, &stdout, &stderr); code != 64 {
		t.Fatalf("exit %d", code)
	}
}

func TestRepoDefinitionsValidate(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"validate", "-definitions", "../../definitions"}, &stdout, &stderr); code != 0 {
		t.Fatalf("repo definitions invalid:\n%s", stderr.String())
	}
}

func awsBody(v4 int) string {
	var b strings.Builder
	b.WriteString(`{"syncToken":"1","prefixes":[`)
	for i := 0; i < v4; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, `{"ip_prefix":"10.0.%d.0/24","region":"GLOBAL","service":"CLOUDFRONT"}`, i)
	}
	b.WriteString(`],"ipv6_prefixes":[{"ipv6_prefix":"2600:9000::/28","region":"GLOBAL","service":"CLOUDFRONT"}]}`)
	return b.String()
}

// seedRemoval publishes, into out, an aws document whose range 10.0.9.0/24
// was removed by grace expiry two days ago, using the real Build and Publish.
func seedRemoval(t *testing.T, out string) string {
	t.Helper()
	def := definitions.Definition{Slug: "aws", DisplayName: "Amazon Web Services", Category: "cloud",
		Services: []definitions.ServiceDef{{Key: "aws.cloudfront", DisplayName: "CloudFront", ServiceTypes: []string{"cdn"}}},
		Feeds:    []definitions.FeedRef{{Collector: "aws_ip_ranges", TagMap: map[string]string{"CLOUDFRONT": "aws.cloudfront"}}}}
	outcome := func(n int) map[string]assemble.FeedOutcome {
		var rs []feeds.Range
		for i := 0; i < n; i++ {
			rs = append(rs, feeds.Range{Prefix: netip.MustParsePrefix(fmt.Sprintf("10.0.%d.0/24", i)), Tag: "CLOUDFRONT"})
		}
		rs = append(rs, feeds.Range{Prefix: netip.MustParsePrefix("2600:9000::/28"), Tag: "CLOUDFRONT"})
		return map[string]assemble.FeedOutcome{"aws_ip_ranges": {Result: feeds.Result{Ranges: rs}}}
	}
	ctx := context.Background()
	store := publish.FSStore{Root: out}
	start := time.Now().UTC().AddDate(0, 0, -10)
	var prev *model.Document
	for _, step := range []struct{ day, ranges int }{{0, 10}, {1, 9}, {8, 9}} {
		at := start.AddDate(0, 0, step.day)
		doc, err := assemble.Build(def, outcome(step.ranges), prev, at)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := publish.Publish(ctx, store, []model.Document{doc}, at); err != nil {
			t.Fatal(err)
		}
		prev = &doc
	}
	return start.AddDate(0, 0, 8).Format(model.DateLayout)
}

func TestRestoreThenCollectContinuesTheTimeline(t *testing.T) {
	withAWSServer(t, awsBody(10))
	defs := defsDir(t, map[string]string{"aws.yaml": awsYAML})
	out := t.TempDir()
	removedOn := seedRemoval(t, out)

	var stdout, stderr bytes.Buffer
	args := []string{"restore", "-provider", "aws", "-collector", "aws_ip_ranges", "-removed-since", removedOn, "-out", out}
	if code := run(context.Background(), args, &stdout, &stderr); code != 0 {
		t.Fatalf("restore exit %d: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "restored 1 ranges for aws aws_ip_ranges") {
		t.Fatalf("restore stdout = %s", stdout.String())
	}

	stdout.Reset()
	if code := run(context.Background(), []string{"collect", "-definitions", defs, "-out", out}, &stdout, &stderr); code != 0 {
		t.Fatalf("collect after restore exit %d: %s", code, stderr.String())
	}
	doc, err := publish.LoadLatest(context.Background(), publish.FSStore{Root: out}, "aws")
	if err != nil || doc == nil {
		t.Fatalf("latest: %v %v", doc, err)
	}
	var found []model.IPRange
	for _, s := range doc.Services {
		for _, r := range s.Evidence.IPRanges {
			if r.CIDR == "10.0.9.0/24" {
				found = append(found, r)
			}
		}
	}
	if len(found) != 1 || found[0].Status != model.StatusActive || found[0].RestoredAt == "" ||
		found[0].FirstSeen != time.Now().UTC().AddDate(0, 0, -10).Format(model.DateLayout) {
		t.Fatalf("10.0.9.0/24 after restore + collect = %+v", found)
	}
}

func TestRestoreUsageAndErrors(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"restore", "-provider", "aws", "-collector", "x"}, &stdout, &stderr); code != 64 {
		t.Fatalf("missing -removed-since: exit %d", code)
	}
	args := []string{"restore", "-provider", "aws", "-collector", "x", "-removed-since", "2026-09-27", "-out", t.TempDir()}
	if code := run(context.Background(), args, &stdout, &stderr); code != 1 {
		t.Fatalf("no published document: exit %d", code)
	}
}
