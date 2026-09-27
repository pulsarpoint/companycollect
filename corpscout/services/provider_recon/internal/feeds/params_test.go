package feeds

import (
	"context"
	"strings"
	"testing"
)

func TestGeofeedCollect(t *testing.T) {
	srv := serveBody("# DigitalOcean geofeed\n5.101.96.0/21,NL,NL-NH,Amsterdam,\n\n2a03:b0c0::/32,NL,,Amsterdam,\nnonsense,SE,,,\n")
	defer srv.Close()
	res, err := NewGeofeed().Collect(context.Background(), testFetcher(srv), map[string]string{"url": srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Ranges) != 2 || res.Skipped != 1 {
		t.Fatalf("ranges=%d skipped=%d", len(res.Ranges), res.Skipped)
	}
	if res.Ranges[0].Region != "NL-NH" || res.Ranges[1].Region != "NL" || res.Ranges[0].Tag != "GEOFEED" {
		t.Fatalf("%+v", res.Ranges)
	}
	if res.SourceURL != srv.URL {
		t.Fatalf("source url = %q", res.SourceURL)
	}
}

func TestGeofeedParams(t *testing.T) {
	g := NewGeofeed()
	if err := g.ValidateParams(map[string]string{"url": "https://digitalocean.com/geo/google.csv"}); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []map[string]string{nil, {"url": "http://x"}, {"url": "https://x", "extra": "1"}} {
		if err := g.ValidateParams(bad); err == nil {
			t.Errorf("params %v accepted", bad)
		}
	}
}

func TestRIPEstatCollect(t *testing.T) {
	srv := serveBody(`{"status":"ok","data":{"prefixes":[{"prefix":"88.198.0.0/16"},{"prefix":"2a01:4f8::/32"}]}}`)
	defer srv.Close()
	c := &RIPEstat{URLFormat: srv.URL + "/?resource=AS%s"}
	res, err := c.Collect(context.Background(), testFetcher(srv), map[string]string{"asn": "24940"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Ranges) != 2 || res.Ranges[0].Tag != "BGP" || !strings.HasPrefix(res.SourceVersion, "sha256:") {
		t.Fatalf("%+v", res)
	}
	if !strings.Contains(res.SourceURL, "AS24940") {
		t.Fatalf("source url = %q", res.SourceURL)
	}
}

func TestRIPEstatStatusNotOK(t *testing.T) {
	srv := serveBody(`{"status":"error","messages":[["error","bad resource"]],"data":{}}`)
	defer srv.Close()
	c := &RIPEstat{URLFormat: srv.URL + "/?resource=AS%s"}
	if _, err := c.Collect(context.Background(), testFetcher(srv), map[string]string{"asn": "1"}); err == nil {
		t.Fatal("expected error")
	}
}

func TestRIPEstatParams(t *testing.T) {
	r := NewRIPEstat()
	if err := r.ValidateParams(map[string]string{"asn": "24940"}); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []map[string]string{nil, {"asn": "AS24940"}, {"asn": "1", "x": "y"}} {
		if err := r.ValidateParams(bad); err == nil {
			t.Errorf("params %v accepted", bad)
		}
	}
}
