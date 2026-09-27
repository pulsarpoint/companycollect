package feeds

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGoogleCloudCollect(t *testing.T) {
	srv := serveBody(`{"syncToken":"1727","creationTime":"x","prefixes":[
		{"ipv4Prefix":"34.1.208.0/20","service":"Google Cloud","scope":"africa-south1"},
		{"ipv6Prefix":"2600:1900:8000::/44","service":"Google Cloud","scope":"us-east4"}]}`)
	defer srv.Close()
	res, err := (&GoogleCloud{URL: srv.URL}).Collect(context.Background(), testFetcher(srv), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Ranges) != 2 || res.Ranges[0].Tag != "Google Cloud" || res.Ranges[0].Region != "africa-south1" || res.SourceVersion != "syncToken=1727" {
		t.Fatalf("%+v", res)
	}
}

func TestGoogleGoogCollect(t *testing.T) {
	srv := serveBody(`{"syncToken":"9","prefixes":[{"ipv4Prefix":"8.8.4.0/24"},{"ipv6Prefix":"2001:4860::/32"}]}`)
	defer srv.Close()
	res, err := (&GoogleGoog{URL: srv.URL}).Collect(context.Background(), testFetcher(srv), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Ranges) != 2 || res.Ranges[1].Tag != "GOOG" {
		t.Fatalf("%+v", res)
	}
}

func TestCloudflareCollect(t *testing.T) {
	srv := serveBody(`{"success":true,"result":{"ipv4_cidrs":["173.245.48.0/20"],"ipv6_cidrs":["2400:cb00::/32"],"etag":"38f79d050aa027e3be3865e495dcc9bc"}}`)
	defer srv.Close()
	res, err := (&Cloudflare{URL: srv.URL}).Collect(context.Background(), testFetcher(srv), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Ranges) != 2 || res.Ranges[0].Tag != "CLOUDFLARE" || res.SourceVersion != "etag=38f79d050aa027e3be3865e495dcc9bc" {
		t.Fatalf("%+v", res)
	}
}

func TestCloudflareUnsuccessfulResponseIsAnError(t *testing.T) {
	srv := serveBody(`{"success":false,"errors":[{"message":"rate limited"}],"result":null}`)
	defer srv.Close()
	if _, err := (&Cloudflare{URL: srv.URL}).Collect(context.Background(), testFetcher(srv), nil); err == nil {
		t.Fatal("expected error")
	}
}

func TestFastlyCollect(t *testing.T) {
	srv := serveBody(`{"addresses":["23.235.32.0/20"],"ipv6_addresses":["2a04:4e40::/32"]}`)
	defer srv.Close()
	res, err := (&Fastly{URL: srv.URL}).Collect(context.Background(), testFetcher(srv), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Ranges) != 2 || res.Ranges[0].Tag != "FASTLY" || !strings.HasPrefix(res.SourceVersion, "sha256:") {
		t.Fatalf("%+v", res)
	}
}

func TestBunnyCollectsBareIPs(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v4", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Accept") != "application/json" {
			w.Write([]byte(`<?xml version="1.0"?><ArrayOfString/>`))
			return
		}
		w.Write([]byte(`["89.187.188.227","bogus"]`))
	})
	mux.HandleFunc("/v6", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`["2400:52e0:1a00::1"]`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	res, err := (&Bunny{URLv4: srv.URL + "/v4", URLv6: srv.URL + "/v6"}).Collect(context.Background(), testFetcher(srv), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Ranges) != 2 || res.Skipped != 1 {
		t.Fatalf("ranges=%d skipped=%d", len(res.Ranges), res.Skipped)
	}
	if res.Ranges[0].Prefix.String() != "89.187.188.227/32" || res.Ranges[1].Prefix.String() != "2400:52e0:1a00::1/128" {
		t.Fatalf("%+v", res.Ranges)
	}
}

func TestBunnyXMLIsAnError(t *testing.T) {
	srv := serveBody(`<?xml version="1.0"?><ArrayOfString><string>1.2.3.4</string></ArrayOfString>`)
	defer srv.Close()
	_, err := (&Bunny{URLv4: srv.URL, URLv6: srv.URL}).Collect(context.Background(), testFetcher(srv), nil)
	if err == nil || !strings.Contains(err.Error(), "bunny") {
		t.Fatalf("err = %v", err)
	}
}
