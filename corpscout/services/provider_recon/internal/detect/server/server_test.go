package server

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"provider_recon/internal/model"
	"provider_recon/internal/publish"
)

func document(slug, nsPattern string) model.Document {
	return model.Document{Version: model.ContractVersion, Slug: slug, ProviderKeys: []string{slug + ".com"}, Services: []model.Service{{
		Key: slug + ".dns", ServiceTypes: []string{"dns"},
		Evidence: model.Evidence{DNSRules: []model.DNSRule{{RecordType: "NS", MatchField: "target", MatcherType: "suffix", Pattern: nsPattern,
			Confidence: 1, Priority: 100, Lifecycle: model.Lifecycle{Status: model.StatusActive}}}},
	}}}
}

func publishDocs(t *testing.T, store publish.Store, runs int, docs ...model.Document) {
	t.Helper()
	ctx := context.Background()
	var slugs []string
	for _, d := range docs {
		b, _ := json.Marshal(d)
		if err := store.Put(ctx, publish.LatestKey(d.Slug), b, "application/json"); err != nil {
			t.Fatal(err)
		}
		slugs = append(slugs, d.Slug)
	}
	summaries := make([]publish.RunSummary, runs)
	for i := range summaries {
		summaries[i].RunID = fmt.Sprintf("run-%d", i)
	}
	idx, _ := json.Marshal(publish.RunIndex{Runs: summaries, Providers: slugs})
	if err := store.Put(ctx, publish.IndexKey, idx, "application/json"); err != nil {
		t.Fatal(err)
	}
}

func started(t *testing.T) (*Server, publish.FSStore, *httptest.Server) {
	t.Helper()
	store := publish.FSStore{Root: t.TempDir()}
	publishDocs(t, store, 1, document("loopia", "loopia.se"))
	s := New(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := s.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	return s, store, ts
}

func getJSON(t *testing.T, url string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	return resp.StatusCode, body
}

func TestHealthAndKnowledge(t *testing.T) {
	_, _, ts := started(t)
	code, h := getJSON(t, ts.URL+"/healthz")
	if code != 200 || h["ok"] != true || !strings.HasPrefix(h["rules_version"].(string), "sha256:") {
		t.Fatalf("healthz %d %v", code, h)
	}
	code, k := getJSON(t, ts.URL+"/v1/knowledge")
	if code != 200 || k["documents"].(float64) != 1 || k["ip_version"] == "" || k["loaded_at"] == "" {
		t.Fatalf("knowledge %d %v", code, k)
	}
}

func TestHealthBeforeTheFirstLoad(t *testing.T) {
	s := New(publish.FSStore{Root: t.TempDir()}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := s.Reload(context.Background()); err == nil {
		t.Fatal("reload of an empty store succeeded")
	}
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	if code, h := getJSON(t, ts.URL+"/healthz"); code != 503 || h["ok"] != false || h["reload_error"] == "" {
		t.Fatalf("healthz %d %v", code, h)
	}
	resp, _ := http.Post(ts.URL+"/v1/resolve", "application/x-ndjson", strings.NewReader(""))
	if resp.StatusCode != 503 {
		t.Fatalf("resolve before load = %d", resp.StatusCode)
	}
}

func post(t *testing.T, ts *httptest.Server, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(ts.URL+"/v1/resolve", "application/x-ndjson", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestResolveKeepsInputOrderAndStampsVersions(t *testing.T) {
	s, _, ts := started(t)
	resp := post(t, ts, `{"record_id":"a","root_domain":"a.se","name":"a.se.","type":"NS","value":"ns1.loopia.se."}
{"record_id":"b","root_domain":"b.se","name":"b.se.","type":"A","value":"192.0.2.1"}
{"record_id":"c","root_domain":"c.se","name":"c.se.","type":"MX","value":"0 ."}`)
	if resp.StatusCode != 200 || resp.Header.Get("X-Rules-Version") != s.current().kb.RulesVersion() || resp.Header.Get("X-IP-Version") == "" {
		t.Fatalf("status %d headers %v", resp.StatusCode, resp.Header)
	}
	var ids []string
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		var line struct {
			RecordID string            `json:"record_id"`
			Results  []json.RawMessage `json:"results"`
			Findings []json.RawMessage `json:"findings"`
		}
		if err := json.Unmarshal(sc.Bytes(), &line); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, line.RecordID)
		if line.RecordID == "a" && len(line.Results) != 1 {
			t.Fatalf("a results = %d", len(line.Results))
		}
	}
	if strings.Join(ids, ",") != "a,b,c" {
		t.Fatalf("order = %v", ids)
	}
}

func TestResolveRejectsBadInputWithoutPartialOutput(t *testing.T) {
	_, _, ts := started(t)
	resp := post(t, ts, `{"record_id":"a","root_domain":"a.se","name":"a.se.","type":"NS","value":"ns1.loopia.se."}
{"record_id":"","root_domain":"b.se","name":"b.se.","type":"NS","value":"x."}`)
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 400 || !strings.Contains(string(body), "record 2") || strings.Contains(string(body), `"results"`) {
		t.Fatalf("status %d body %s", resp.StatusCode, body)
	}
	if resp := post(t, ts, `{"record_id":`); resp.StatusCode != 400 {
		t.Fatalf("broken JSON = %d", resp.StatusCode)
	}
	if resp, _ := http.Get(ts.URL + "/v1/resolve"); resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET resolve = %d", resp.StatusCode)
	}
}

func TestResolveRefusesOversizedBatches(t *testing.T) {
	_, _, ts := started(t)
	var b strings.Builder
	for i := range MaxRecords + 1 {
		fmt.Fprintf(&b, `{"record_id":"r%d","root_domain":"a.se","name":"a.se","type":"NS","value":"ns1.loopia.se."}`+"\n", i)
	}
	if resp := post(t, ts, b.String()); resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestReloadSwapsOnlyOnChangeAndKeepsKnowledgeOnFailure(t *testing.T) {
	s, store, _ := started(t)
	first := s.current()
	if err := s.Reload(context.Background()); err != nil || s.current() != first {
		t.Fatal("unchanged store swapped the knowledge")
	}
	publishDocs(t, store, 2, document("loopia", "loopia.se"), document("glesys", "glesys.se"))
	if err := s.Reload(context.Background()); err != nil || s.current() == first || s.current().kb.Documents() != 2 {
		t.Fatalf("changed store not reloaded: %v", err)
	}
	loaded := s.current()
	if err := store.Put(context.Background(), publish.IndexKey, []byte("{broken"), "application/json"); err != nil {
		t.Fatal(err)
	}
	err := s.Reload(context.Background())
	if err == nil || s.current() != loaded {
		t.Fatalf("failed reload replaced the knowledge: %v", err)
	}
	if h := s.health(); h.OK != true || h.ReloadError == "" {
		t.Fatalf("health after failed reload = %+v", h)
	}
}

func TestReloadRecoversFromAPublishItRaced(t *testing.T) {
	s, store, _ := started(t)
	// Publish writes the run index before the documents: a reload in between
	// sees the new index with the old document.
	publishDocs(t, store, 2, document("loopia", "loopia.se"))
	if err := s.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	raced := s.current()
	// The documents land; the index does not change again.
	b, _ := json.Marshal(document("loopia", "loopia.net"))
	if err := store.Put(context.Background(), publish.LatestKey("loopia"), b, "application/json"); err != nil {
		t.Fatal(err)
	}
	if err := s.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s.current() == raced || s.current().kb.RulesVersion() == raced.kb.RulesVersion() {
		t.Fatal("the knowledge compiled during the race stayed pinned")
	}
}
