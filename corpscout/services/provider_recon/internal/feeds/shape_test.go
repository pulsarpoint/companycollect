package feeds

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"
)

func TestShapeChecks(t *testing.T) {
	cases := []struct {
		name string
		body string
		make func(url string) Collector
	}{
		{"aws without ipv6", `{"syncToken":"1","prefixes":[{"ip_prefix":"1.0.0.0/24","region":"x","service":"EC2"}],"ipv6_prefixes":[]}`,
			func(u string) Collector { return &AWS{URL: u} }},
		{"google cloud without ipv6", `{"syncToken":"1","prefixes":[{"ipv4Prefix":"1.0.0.0/24","service":"Google Cloud","scope":"x"}]}`,
			func(u string) Collector { return &GoogleCloud{URL: u} }},
		{"cloudflare without ipv6", `{"success":true,"result":{"ipv4_cidrs":["1.0.0.0/24"],"ipv6_cidrs":[],"etag":"e"}}`,
			func(u string) Collector { return &Cloudflare{URL: u} }},
		{"fastly without ipv6", `{"addresses":["1.0.0.0/24"],"ipv6_addresses":[]}`,
			func(u string) Collector { return &Fastly{URL: u} }},
		{"oracle without OCI", `{"last_updated_timestamp":"t","regions":[{"region":"r","cidrs":[{"cidr":"1.0.0.0/24","tags":["OSN"]}]}]}`,
			func(u string) Collector { return &Oracle{URL: u} }},
		{"github without pages", `{"hooks":["1.0.0.0/24"]}`,
			func(u string) Collector { return &GitHub{URL: u} }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv := serveBody(c.body)
			defer srv.Close()
			_, err := c.make(srv.URL).Collect(context.Background(), testFetcher(srv), nil)
			if !errors.Is(err, ErrShape) {
				t.Fatalf("err = %v, want ErrShape", err)
			}
		})
	}
}

func TestBunnyEmptyIPv6ListIsAShapeError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v4", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`["1.2.3.4"]`)) })
	mux.HandleFunc("/v6", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`[]`)) })
	srv := httptest.NewServer(mux)
	defer srv.Close()
	_, err := (&Bunny{URLv4: srv.URL + "/v4", URLv6: srv.URL + "/v6"}).Collect(context.Background(), testFetcher(srv), nil)
	if !errors.Is(err, ErrShape) {
		t.Fatalf("err = %v, want ErrShape", err)
	}
}

func TestAzureWithoutAzureCloudIsAShapeError(t *testing.T) {
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()
	mux.HandleFunc("/page", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `<a href="%s/download/x/ServiceTags_Public_20260921.json">x</a>`, srv.URL)
	})
	mux.HandleFunc("/download/x/ServiceTags_Public_20260921.json", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"changeNumber":1,"values":[{"name":"AzureFrontDoor.Frontend","properties":{"addressPrefixes":["13.107.246.0/24"]}}]}`))
	})
	c := &Azure{PageURL: srv.URL + "/page",
		LinkPattern: regexp.MustCompile(regexp.QuoteMeta(srv.URL) + `/download/[^"]+/ServiceTags_Public_\d{8}\.json`)}
	_, err := c.Collect(context.Background(), testFetcher(srv), nil)
	if !errors.Is(err, ErrShape) {
		t.Fatalf("err = %v, want ErrShape", err)
	}
}
