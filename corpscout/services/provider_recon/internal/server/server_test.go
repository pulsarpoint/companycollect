package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"testing"
	"time"

	"provider_recon/internal/assemble"
	"provider_recon/internal/definitions"
	"provider_recon/internal/feeds"
	"provider_recon/internal/model"
	"provider_recon/internal/publish"
	"provider_recon/internal/runner"
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

func awsBody(v4 int) string {
	var b bytes.Buffer
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

type harness struct {
	api     *httptest.Server
	srv     *Server
	store   publish.Store
	release chan struct{} // when non-nil, the feed blocks until closed
}

func newHarness(t *testing.T, store publish.Store, block bool) *harness {
	t.Helper()
	h := &harness{store: store}
	if block {
		h.release = make(chan struct{})
	}
	feed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h.release != nil {
			<-h.release
		}
		w.Write([]byte(awsBody(10)))
	}))
	t.Cleanup(feed.Close)
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "aws.yaml"), []byte(awsYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := runner.Config{
		DefinitionsDir: dir,
		Registry: func() map[string]feeds.Collector {
			return map[string]feeds.Collector{"aws_ip_ranges": &feeds.AWS{URL: feed.URL}}
		},
		Fetcher:     &feeds.Fetcher{Client: feed.Client()},
		Store:       store,
		Concurrency: 2,
	}
	h.srv = New(cfg)
	h.api = httptest.NewServer(h.srv.Handler())
	t.Cleanup(func() {
		if h.release != nil {
			select {
			case <-h.release:
			default:
				close(h.release)
			}
		}
		h.api.Close()
		h.srv.Wait()
	})
	return h
}

func (h *harness) do(t *testing.T, method, path string, body any) (*http.Response, map[string]any) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	req, _ := http.NewRequest(method, h.api.URL+path, &buf)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	out := map[string]any{}
	json.NewDecoder(res.Body).Decode(&out)
	return res, out
}

func (h *harness) waitFor(t *testing.T, runID string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		_, run := h.do(t, "GET", "/v1/runs/"+runID, nil)
		if run["status"] != "running" {
			return run
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("run %s did not finish", runID)
	return nil
}

func TestHealthz(t *testing.T) {
	h := newHarness(t, publish.FSStore{Root: t.TempDir()}, false)
	if res, body := h.do(t, "GET", "/healthz", nil); res.StatusCode != 200 || body["ok"] != true {
		t.Fatalf("healthz = %d %v", res.StatusCode, body)
	}
}

func TestCollectRunsInTheBackgroundAndReportsTheManifest(t *testing.T) {
	store := publish.FSStore{Root: t.TempDir()}
	h := newHarness(t, store, false)
	res, started := h.do(t, "POST", "/v1/collect", map[string]any{})
	if res.StatusCode != 202 || started["status"] != "running" {
		t.Fatalf("start = %d %v", res.StatusCode, started)
	}
	runID := started["run_id"].(string)
	run := h.waitFor(t, runID)
	if run["status"] != "succeeded" || fmt.Sprint(run["changed"]) != "[aws]" {
		t.Fatalf("run = %v", run)
	}
	if _, ok, _ := store.Get(context.Background(), publish.ChangesKey(runID)); !ok {
		t.Fatal("service run id is not the manifest run id")
	}
}

func TestCollectRejectsUnknownProviderAndBadJSON(t *testing.T) {
	h := newHarness(t, publish.FSStore{Root: t.TempDir()}, false)
	if res, body := h.do(t, "POST", "/v1/collect", map[string]any{"providers": []string{"nope"}}); res.StatusCode != 400 {
		t.Fatalf("unknown provider = %d %v", res.StatusCode, body)
	}
	req, _ := http.NewRequest("POST", h.api.URL+"/v1/collect", bytes.NewBufferString(`{"providers":`))
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 400 {
		t.Fatalf("bad json = %d", res.StatusCode)
	}
}

func TestCollectWhileRunningIs409(t *testing.T) {
	h := newHarness(t, publish.FSStore{Root: t.TempDir()}, true)
	_, started := h.do(t, "POST", "/v1/collect", nil)
	res, body := h.do(t, "POST", "/v1/collect", nil)
	if res.StatusCode != 409 || body["run_id"] != started["run_id"] {
		t.Fatalf("second collect = %d %v", res.StatusCode, body)
	}
	res, _ = h.do(t, "POST", "/v1/restore", map[string]any{"provider": "aws", "collector": "aws_ip_ranges", "removed_since": "2026-09-28"})
	if res.StatusCode != 409 {
		t.Fatalf("restore during collect = %d", res.StatusCode)
	}
	close(h.release)
	if run := h.waitFor(t, started["run_id"].(string)); run["status"] != "succeeded" {
		t.Fatalf("run = %v", run)
	}
}

func TestSequentialRunsGetDistinctIDs(t *testing.T) {
	h := newHarness(t, publish.FSStore{Root: t.TempDir()}, false)
	fixed := time.Date(2026, 9, 28, 5, 0, 0, 0, time.UTC)
	h.srv.Now = func() time.Time { return fixed }
	_, a := h.do(t, "POST", "/v1/collect", nil)
	h.waitFor(t, a["run_id"].(string))
	_, b := h.do(t, "POST", "/v1/collect", nil)
	h.waitFor(t, b["run_id"].(string))
	if a["run_id"] == b["run_id"] {
		t.Fatalf("two runs share id %v", a["run_id"])
	}
	if _, first := h.do(t, "GET", "/v1/runs/"+a["run_id"].(string), nil); first["status"] != "succeeded" {
		t.Fatalf("first run entry overwritten: %v", first)
	}
}

type failingStore struct{ publish.FSStore }

func (failingStore) Put(context.Context, string, []byte, string) error {
	return errors.New("injected put failure")
}

func TestFailedCollectReleasesTheLock(t *testing.T) {
	h := newHarness(t, failingStore{publish.FSStore{Root: t.TempDir()}}, false)
	_, started := h.do(t, "POST", "/v1/collect", nil)
	run := h.waitFor(t, started["run_id"].(string))
	if run["status"] != "failed" || run["error"] == "" {
		t.Fatalf("run = %v", run)
	}
	if res, _ := h.do(t, "POST", "/v1/collect", nil); res.StatusCode != 202 {
		t.Fatalf("lock not released: %d", res.StatusCode)
	}
}

func TestUnknownRunIs404(t *testing.T) {
	h := newHarness(t, publish.FSStore{Root: t.TempDir()}, false)
	if res, _ := h.do(t, "GET", "/v1/runs/20260928T050000Z-collect", nil); res.StatusCode != 404 {
		t.Fatalf("status = %d", res.StatusCode)
	}
}

// seedRemoval publishes an aws document whose 10.0.9.0/24 was removed by grace
// expiry on removedDay, using the real Build and Publish.
func seedRemoval(t *testing.T, store publish.Store) string {
	t.Helper()
	def := definitions.Definition{Slug: "aws", DisplayName: "Amazon Web Services", Category: "cloud",
		Services: []definitions.ServiceDef{{Key: "aws.cloudfront", DisplayName: "CloudFront", ServiceTypes: []string{"cdn"}}},
		Feeds:    []definitions.FeedRef{{Collector: "aws_ip_ranges", TagMap: map[string]string{"CLOUDFRONT": "aws.cloudfront"}}}}
	outcome := func(n int) map[string]assemble.FeedOutcome {
		var rs []feeds.Range
		for i := 0; i < n; i++ {
			rs = append(rs, feeds.Range{Prefix: netip.MustParsePrefix(fmt.Sprintf("10.0.%d.0/24", i)), Tag: "CLOUDFRONT"})
		}
		return map[string]assemble.FeedOutcome{"aws_ip_ranges": {Result: feeds.Result{Ranges: rs}}}
	}
	start := time.Now().UTC().AddDate(0, 0, -10)
	var prev *model.Document
	for _, step := range []struct{ day, ranges int }{{0, 10}, {1, 9}, {8, 9}} {
		at := start.AddDate(0, 0, step.day)
		doc, err := assemble.Build(def, outcome(step.ranges), prev, at)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := publish.Publish(context.Background(), store, []model.Document{doc}, at); err != nil {
			t.Fatal(err)
		}
		prev = &doc
	}
	return start.AddDate(0, 0, 8).Format(model.DateLayout)
}

func TestRestoreSucceeds(t *testing.T) {
	store := publish.FSStore{Root: t.TempDir()}
	removedOn := seedRemoval(t, store)
	h := newHarness(t, store, false)
	res, body := h.do(t, "POST", "/v1/restore", map[string]any{"provider": "aws", "collector": "aws_ip_ranges", "removed_since": removedOn})
	if res.StatusCode != 200 || body["status"] != "succeeded" || body["restored"] != float64(1) {
		t.Fatalf("restore = %d %v", res.StatusCode, body)
	}
	if _, ok, _ := store.Get(context.Background(), publish.ChangesKey(body["run_id"].(string))); !ok {
		t.Fatal("restore manifest not written under the returned run id")
	}
}

func TestRestoreErrorMapping(t *testing.T) {
	store := publish.FSStore{Root: t.TempDir()}
	h := newHarness(t, store, false)
	req := func(provider, collector, since string) int {
		res, _ := h.do(t, "POST", "/v1/restore", map[string]any{"provider": provider, "collector": collector, "removed_since": since})
		return res.StatusCode
	}
	if got := req("aws", "aws_ip_ranges", "2026-09-28"); got != 404 {
		t.Fatalf("no document = %d", got)
	}
	seedRemoval(t, store)
	cases := []struct {
		collector, since string
		want             int
	}{
		{"nope", "2026-09-01", 404},
		{"aws_ip_ranges", "2026/09/01", 400},
		{"aws_ip_ranges", "2099-01-01", 422},
		{"", "2026-09-01", 400},
	}
	for _, c := range cases {
		if got := req("aws", c.collector, c.since); got != c.want {
			t.Errorf("restore(%q,%q) = %d, want %d", c.collector, c.since, got, c.want)
		}
	}
}
