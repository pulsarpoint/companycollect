package runner

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"provider_recon/internal/assemble"
	"provider_recon/internal/feeds"
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

const awsBody = `{"syncToken":"1","prefixes":[{"ip_prefix":"52.84.0.0/15","region":"GLOBAL","service":"CLOUDFRONT"}],"ipv6_prefixes":[{"ipv6_prefix":"2600:9000::/28","region":"GLOBAL","service":"CLOUDFRONT"}]}`

func testConfig(t *testing.T) Config {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(awsBody)) }))
	t.Cleanup(srv.Close)
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "aws.yaml"), []byte(awsYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	return Config{
		DefinitionsDir: dir,
		Registry: func() map[string]feeds.Collector {
			return map[string]feeds.Collector{"aws_ip_ranges": &feeds.AWS{URL: srv.URL}}
		},
		Fetcher:     &feeds.Fetcher{Client: srv.Client(), Retries: 0},
		Store:       publish.FSStore{Root: t.TempDir()},
		Concurrency: 2,
	}
}

func TestCollectPublishesWithTheGivenRunTime(t *testing.T) {
	cfg := testConfig(t)
	now := time.Date(2026, 9, 28, 5, 0, 0, 0, time.UTC)
	m, err := Collect(context.Background(), cfg, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	if m.RunID != "20260928T050000Z-collect" || len(m.Changed) != 1 || m.Changed[0].Slug != "aws" {
		t.Fatalf("manifest = %+v", m)
	}
	if _, ok, _ := cfg.Store.Get(context.Background(), publish.ChangesKey(m.RunID)); !ok {
		t.Fatal("manifest not written")
	}
}

func TestCollectRejectsUnknownProvider(t *testing.T) {
	_, err := Collect(context.Background(), testConfig(t), []string{"nope"}, time.Now())
	if !errors.Is(err, ErrUnknownProvider) {
		t.Fatalf("err = %v, want ErrUnknownProvider", err)
	}
}

func TestRestoreErrors(t *testing.T) {
	cfg := testConfig(t)
	ctx := context.Background()
	now := time.Date(2026, 9, 28, 5, 0, 0, 0, time.UTC)
	if _, _, err := Restore(ctx, cfg, "aws", "aws_ip_ranges", "2026-09-28", now); !errors.Is(err, ErrNoDocument) {
		t.Fatalf("no document: err = %v", err)
	}
	if _, err := Collect(ctx, cfg, nil, now); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		collector, since string
		want             error
	}{
		{"nope", "2026-09-28", assemble.ErrUnknownCollector},
		{"aws_ip_ranges", "2026/09/28", assemble.ErrBadDate},
		{"aws_ip_ranges", "2026-09-28", assemble.ErrNothingToRestore},
	}
	for _, c := range cases {
		if _, _, err := Restore(ctx, cfg, "aws", c.collector, c.since, now.Add(time.Second)); !errors.Is(err, c.want) {
			t.Errorf("Restore(%q,%q) err = %v, want %v", c.collector, c.since, err, c.want)
		}
	}
}

func TestCollectDeduplicatesProviders(t *testing.T) {
	m, err := Collect(context.Background(), testConfig(t), []string{"aws", "aws"}, time.Date(2026, 9, 28, 5, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Changed) != 1 || len(m.Feeds) != 1 || len(m.Scope.Providers) != 1 {
		t.Fatalf("changed=%d feeds=%d scope=%v", len(m.Changed), len(m.Feeds), m.Scope.Providers)
	}
}
