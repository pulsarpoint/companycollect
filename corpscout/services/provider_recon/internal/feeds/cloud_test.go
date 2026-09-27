package feeds

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
)

func TestAzureCollectFollowsDownloadLink(t *testing.T) {
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()
	mux.HandleFunc("/page", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `<html><a href="%s/download/7/1/d/x/ServiceTags_Public_20260921.json">Download</a></html>`, srv.URL)
	})
	mux.HandleFunc("/download/7/1/d/x/ServiceTags_Public_20260921.json", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"changeNumber":412,"cloud":"Public","values":[
			{"name":"AzureFrontDoor.Frontend","properties":{"region":"","addressPrefixes":["13.107.246.0/24","2620:1ec:bdf::/48"]}},
			{"name":"AppService.WestEurope","properties":{"region":"westeurope","addressPrefixes":["20.50.2.0/24"]}},{"name":"AzureCloud","properties":{"region":"","addressPrefixes":["20.50.3.0/24"]}}]}`))
	})

	c := &Azure{
		PageURL:     srv.URL + "/page",
		LinkPattern: regexp.MustCompile(regexp.QuoteMeta(srv.URL) + `/download/[^"'\s<>]+/ServiceTags_Public_\d{8}\.json`),
	}
	res, err := c.Collect(context.Background(), testFetcher(srv), nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.SourceVersion != "changeNumber=412" || !strings.HasSuffix(res.SourceURL, "ServiceTags_Public_20260921.json") {
		t.Fatalf("version=%q url=%q", res.SourceVersion, res.SourceURL)
	}
	if len(res.Ranges) != 4 || res.Ranges[2].Tag != "AppService.WestEurope" || res.Ranges[2].Region != "westeurope" {
		t.Fatalf("%+v", res.Ranges)
	}
}

func TestAzureMissingLinkIsAnError(t *testing.T) {
	srv := serveBody(`<html>we moved things around</html>`)
	defer srv.Close()
	_, err := (&Azure{PageURL: srv.URL, LinkPattern: azureLinkRE}).Collect(context.Background(), testFetcher(srv), nil)
	if err == nil || !strings.Contains(err.Error(), "download link not found") {
		t.Fatalf("err = %v", err)
	}
}

func TestOracleCollectOneRangePerTag(t *testing.T) {
	srv := serveBody(`{"last_updated_timestamp":"2026-09-20T12:00:00.000000","regions":[
		{"region":"eu-stockholm-1","cidrs":[{"cidr":"138.2.0.0/16","tags":["OCI"]},{"cidr":"134.70.96.0/22","tags":["OSN","OBJECT_STORAGE"]}]}]}`)
	defer srv.Close()
	res, err := (&Oracle{URL: srv.URL}).Collect(context.Background(), testFetcher(srv), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Ranges) != 3 || res.Ranges[2].Tag != "OBJECT_STORAGE" || res.Ranges[0].Region != "eu-stockholm-1" {
		t.Fatalf("%+v", res.Ranges)
	}
	if res.SourceVersion != "last_updated=2026-09-20T12:00:00.000000" {
		t.Fatalf("version = %q", res.SourceVersion)
	}
}

func TestGitHubCollectUsesOnlyCIDRLists(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `W/"e1"`)
		w.Write([]byte(`{"verifiable_password_authentication":false,
			"ssh_keys":["ssh-ed25519 AAAAC3Nza"],
			"pages":["185.199.108.0/22","2606:50c0:8000::/64"],
			"hooks":["192.30.252.0/22","bad"],
			"domains":{"website":["*.github.com"]}}`))
	}))
	defer srv.Close()
	res, err := (&GitHub{URL: srv.URL}).Collect(context.Background(), testFetcher(srv), nil)
	if err != nil {
		t.Fatal(err)
	}
	tags := map[string]int{}
	for _, r := range res.Ranges {
		tags[r.Tag]++
	}
	if tags["pages"] != 2 || tags["hooks"] != 1 || len(tags) != 2 || res.Skipped != 1 {
		t.Fatalf("tags=%v skipped=%d", tags, res.Skipped)
	}
	if res.SourceVersion != `etag=W/"e1` {
		t.Fatalf("version = %q", res.SourceVersion)
	}
}
