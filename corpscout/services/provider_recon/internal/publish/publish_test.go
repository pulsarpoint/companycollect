package publish

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"provider_recon/internal/model"
)

var t0 = time.Date(2026, 9, 27, 6, 0, 0, 0, time.UTC)

func doc(t *testing.T, cidrs ...string) model.Document {
	t.Helper()
	d := model.Document{Version: model.ContractVersion, Slug: "aws", DisplayName: "AWS", Category: "cloud",
		Services: []model.Service{{Key: "aws.cloudfront", ServiceTypes: []string{"cdn"}}},
		Collection: model.Collection{Collectors: map[string]model.CollectorStatus{
			"aws_ip_ranges": {Status: "ok", SourceVersion: "syncToken=1"},
		}}}
	for _, c := range cidrs {
		d.Services[0].Evidence.IPRanges = append(d.Services[0].Evidence.IPRanges, model.IPRange{CIDR: c, Confidence: 1,
			Provenance: model.Provenance{Source: model.SourceOfficialFeed, Collector: "aws_ip_ranges"}})
	}
	h, err := model.ContentHash(d)
	if err != nil {
		t.Fatal(err)
	}
	d.Collection.ContentHash = h
	return d
}

func historyCount(t *testing.T, root string) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(root, "providers", "aws", "history"))
	if err != nil {
		t.Fatal(err)
	}
	return len(entries)
}

func TestPublishLifecycle(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := FSStore{Root: root}

	m1, err := Publish(ctx, store, []model.Document{doc(t, "52.84.0.0/15")}, t0)
	if err != nil {
		t.Fatal(err)
	}
	if len(m1.Changed) != 1 || !m1.Changed[0].Created || m1.Changed[0].Evidence["ip_ranges"].AddedCount != 1 {
		t.Fatalf("first run manifest = %+v", m1)
	}
	if len(m1.Changed[0].Evidence["ip_ranges"].Added) != 0 {
		t.Fatal("created providers must list counts only, not every item")
	}
	if _, ok, _ := store.Get(ctx, ChangesKey(m1.RunID)); !ok {
		t.Fatal("changes manifest not written")
	}

	second := doc(t, "52.84.0.0/15")
	second.Collection.CollectedAt = t0.Add(24 * time.Hour)
	m2, err := Publish(ctx, store, []model.Document{second}, t0.Add(24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(m2.Changed) != 0 || len(m2.Unchanged) != 1 || historyCount(t, root) != 1 {
		t.Fatalf("unchanged run wrote history or reported change: %+v", m2)
	}
	latest, err := LoadLatest(ctx, store, "aws")
	if err != nil || latest == nil || !latest.Collection.CollectedAt.Equal(t0.Add(24*time.Hour)) {
		t.Fatalf("latest not rewritten on unchanged run: %+v %v", latest, err)
	}

	m3, err := Publish(ctx, store, []model.Document{doc(t, "52.84.0.0/15", "13.32.0.0/15")}, t0.Add(48*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	d := m3.Changed[0].Evidence["ip_ranges"]
	if len(m3.Changed) != 1 || len(d.Added) != 1 || d.Added[0] != "aws.cloudfront 13.32.0.0/15" || historyCount(t, root) != 2 {
		t.Fatalf("third run manifest = %+v", m3)
	}
}

func TestPublishReportsCollectorIssues(t *testing.T) {
	d := doc(t, "52.84.0.0/15")
	d.Collection.Collectors["aws_ip_ranges"] = model.CollectorStatus{Status: "stale", Error: "status 503"}
	m, err := Publish(context.Background(), FSStore{Root: t.TempDir()}, []model.Document{d}, t0)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.CollectorIssues) != 1 || m.CollectorIssues[0].Status != "stale" || m.CollectorIssues[0].Slug != "aws" {
		t.Fatalf("issues = %+v", m.CollectorIssues)
	}
}

func TestDiffTruncatesLongLists(t *testing.T) {
	old := doc(t)
	cidrs := make([]string, 1500)
	for i := range cidrs {
		cidrs[i] = fmt.Sprintf("10.%d.%d.0/24", i/256, i%256)
	}
	ch := Diff(&old, doc(t, cidrs...))
	d := ch.Evidence["ip_ranges"]
	if d.AddedCount != 1500 || len(d.Added) != maxListed || !d.Truncated {
		t.Fatalf("count=%d listed=%d truncated=%v", d.AddedCount, len(d.Added), d.Truncated)
	}
}

func TestDiffFeedVersions(t *testing.T) {
	old := doc(t, "52.84.0.0/15")
	cur := doc(t, "52.84.0.0/15")
	cur.Collection.Collectors["aws_ip_ranges"] = model.CollectorStatus{Status: "ok", SourceVersion: "syncToken=2"}
	ch := Diff(&old, cur)
	if v := ch.FeedVersions["aws_ip_ranges"]; v.Old != "syncToken=1" || v.New != "syncToken=2" {
		t.Fatalf("feed versions = %+v", ch.FeedVersions)
	}
	b, _ := json.Marshal(ch)
	if len(b) == 0 {
		t.Fatal("change does not marshal")
	}
}

// failingStore fails every Put whose key contains failOn.
type failingStore struct {
	FSStore
	failOn string
}

func (s failingStore) Put(ctx context.Context, key string, body []byte, ct string) error {
	if s.failOn != "" && strings.Contains(key, s.failOn) {
		return errors.New("injected put failure")
	}
	return s.FSStore.Put(ctx, key, body, ct)
}

func TestInterruptedPublishIsRedetectedForEveryProvider(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	a := doc(t, "52.84.0.0/15")
	b := doc(t, "13.32.0.0/15")
	b.Slug = "bws"

	if _, err := Publish(ctx, failingStore{FSStore{Root: root}, "providers/bws/"}, []model.Document{a, b}, t0); err == nil {
		t.Fatal("expected the injected failure to surface")
	}
	m, err := Publish(ctx, FSStore{Root: root}, []model.Document{a, b}, t0.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Changed) != 2 {
		t.Fatalf("after an interrupted run, changes were lost from the manifest stream: changed=%d unchanged=%v", len(m.Changed), m.Unchanged)
	}
}
