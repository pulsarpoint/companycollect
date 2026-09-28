package feeds

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func serveFile(t *testing.T, path string) *httptest.Server {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write(body) }))
}

func serveBody(body string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(body)) }))
}

func TestAWSCollect(t *testing.T) {
	srv := serveFile(t, "testdata/aws.json")
	defer srv.Close()

	res, err := (&AWS{URL: srv.URL}).Collect(context.Background(), testFetcher(srv), nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.SourceVersion != "syncToken=1727400000" || res.SourceURL != srv.URL {
		t.Fatalf("version=%q url=%q", res.SourceVersion, res.SourceURL)
	}
	if len(res.Ranges) != 4 || res.Skipped != 1 {
		t.Fatalf("ranges=%d skipped=%d, want 4 and 1", len(res.Ranges), res.Skipped)
	}
	ec2 := res.Ranges[2]
	if ec2.Prefix.String() != "3.5.140.0/22" || ec2.Tag != "EC2" || ec2.Region != "ap-northeast-2" {
		t.Fatalf("host bits not masked or fields lost: %+v", ec2)
	}
	if res.Ranges[3].Prefix.String() != "2600:9000::/28" {
		t.Fatalf("ipv6 range = %v", res.Ranges[3].Prefix)
	}
}

func TestAWSEmptyFeedIsAnError(t *testing.T) {
	srv := serveBody(`{"syncToken":"1","prefixes":[],"ipv6_prefixes":[]}`)
	defer srv.Close()
	_, err := (&AWS{URL: srv.URL}).Collect(context.Background(), testFetcher(srv), nil)
	if !errors.Is(err, ErrShape) {
		t.Fatalf("err = %v, want ErrShape", err)
	}
}

func TestAWSRejectsHTML(t *testing.T) {
	srv := serveBody(`<html>maintenance</html>`)
	defer srv.Close()
	if _, err := (&AWS{URL: srv.URL}).Collect(context.Background(), testFetcher(srv), nil); err == nil {
		t.Fatal("expected decode error")
	}
}

func TestRegistryNamesMatchCollectors(t *testing.T) {
	for name, c := range Registry() {
		if c.Name() != name {
			t.Errorf("registry key %q holds collector %q", name, c.Name())
		}
	}
}

func TestRegistryCollectorsDescribeTheirFormat(t *testing.T) {
	want := map[string]string{
		"aws_ip_ranges": "JSON", "google_cloud": "JSON", "google_goog": "JSON", "oracle_public_ip_ranges": "JSON",
		"cloudflare_ips": "JSON API", "fastly_public_ips": "JSON API", "bunny_edge_servers": "JSON API", "github_meta": "JSON API",
		"azure_service_tags": "HTML → JSON", "geofeed": "CSV (RFC 8805 geofeed)", "ripestat_announced": "RIPEstat JSON API",
	}
	reg := Registry()
	if len(reg) != len(want) {
		t.Fatalf("registry has %d collectors, test knows %d", len(reg), len(want))
	}
	for name, c := range reg {
		if c.Format() != want[name] {
			t.Errorf("%s.Format() = %q, want %q", name, c.Format(), want[name])
		}
	}
}
