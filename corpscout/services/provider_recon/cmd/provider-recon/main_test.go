package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"provider_recon/internal/feeds"
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
