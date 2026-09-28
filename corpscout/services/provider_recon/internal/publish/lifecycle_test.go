package publish

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"provider_recon/internal/model"
)

func lcDoc(t *testing.T, items ...model.IPRange) model.Document {
	t.Helper()
	d := model.Document{Version: model.ContractVersion, Slug: "aws", DisplayName: "AWS", Category: "cloud",
		Services: []model.Service{{Key: "aws.cloudfront", ServiceTypes: []string{"cdn"}, Evidence: model.Evidence{IPRanges: items}}},
		Collection: model.Collection{Collectors: map[string]model.CollectorStatus{
			"aws_ip_ranges": {Status: "ok", Items: len(items), Churn: model.Churn{Added: 1}},
		}}}
	h, err := model.ContentHash(d)
	if err != nil {
		t.Fatal(err)
	}
	d.Collection.ContentHash = h
	return d
}

func ip(cidr, status, first string) model.IPRange {
	return model.IPRange{CIDR: cidr, Confidence: 1,
		Provenance: model.Provenance{Source: model.SourceOfficialFeed, Collector: "aws_ip_ranges"},
		Lifecycle:  model.Lifecycle{Status: status, FirstSeen: first, LastSeen: first}}
}

func TestDiffListsLifecycleTransitions(t *testing.T) {
	old := lcDoc(t, ip("10.0.1.0/24", "active", "2026-09-01"), ip("10.0.2.0/24", "missing", "2026-09-01"),
		ip("10.0.3.0/24", "active", "2026-09-01"), ip("10.0.4.0/24", "removed", "2026-06-01"),
		ip("10.0.6.0/24", "removed", "2026-09-01"))
	gone := ip("10.0.3.0/24", "removed", "2026-09-01")
	gone.RemovalAction = model.ActionGraceExpired
	back := ip("10.0.6.0/24", "active", "2026-09-01")
	back.RestoredAt = "2026-09-27"
	cur := lcDoc(t, ip("10.0.1.0/24", "missing", "2026-09-01"), ip("10.0.2.0/24", "active", "2026-09-01"),
		gone, ip("10.0.5.0/24", "active", "2026-09-27"), back)
	d := Diff(&old, cur).Evidence["ip_ranges"]
	check := func(name string, got []string, want string) {
		t.Helper()
		if len(got) != 1 || got[0] != want {
			t.Errorf("%s = %v, want [%s]", name, got, want)
		}
	}
	check("missing", d.Missing, "aws.cloudfront 10.0.1.0/24")
	check("reappeared", d.Reappeared, "aws.cloudfront 10.0.2.0/24")
	check("removed", d.Removed, "aws.cloudfront 10.0.3.0/24 (grace_expired)")
	check("purged", d.Purged, "aws.cloudfront 10.0.4.0/24")
	check("added", d.Added, "aws.cloudfront 10.0.5.0/24")
	check("restored", d.Restored, "aws.cloudfront 10.0.6.0/24")
}

func TestDiffLastSeenAloneIsNotAnUpdate(t *testing.T) {
	a := ip("10.0.1.0/24", "active", "2026-09-01")
	b := a
	b.LastSeen = "2026-09-27"
	old, cur := lcDoc(t, a), lcDoc(t, b)
	if d, ok := Diff(&old, cur).Evidence["ip_ranges"]; ok {
		t.Fatalf("last_seen-only change reported: %+v", d)
	}
	c := a
	c.Region = "eu-north-1"
	if d := Diff(&old, lcDoc(t, c)).Evidence["ip_ranges"]; len(d.Updated) != 1 {
		t.Fatalf("region change not reported as updated: %+v", d)
	}
}

func TestPublishMaintainsRunIndex(t *testing.T) {
	ctx := context.Background()
	store := FSStore{Root: t.TempDir()}
	t1 := time.Date(2026, 9, 27, 6, 0, 0, 0, time.UTC)
	a := lcDoc(t, ip("10.0.1.0/24", "active", "2026-09-27"))
	b := lcDoc(t, ip("10.0.2.0/24", "active", "2026-09-27"))
	b.Slug = "bws"
	if _, err := Publish(ctx, store, []model.Document{a}, t1); err != nil {
		t.Fatal(err)
	}
	if _, err := PublishScoped(ctx, store, []model.Document{b}, t1.Add(time.Hour), Scope{Command: "collect", Providers: []string{"bws"}}); err != nil {
		t.Fatal(err)
	}
	raw, ok, err := store.Get(ctx, IndexKey)
	if err != nil || !ok {
		t.Fatalf("index missing: %v", err)
	}
	var idx RunIndex
	if err := json.Unmarshal(raw, &idx); err != nil {
		t.Fatal(err)
	}
	if len(idx.Runs) != 2 || idx.Runs[0].RunID != "20260927T070000Z-collect" || idx.Runs[1].RunID != "20260927T060000Z-collect" {
		t.Fatalf("runs = %+v", idx.Runs)
	}
	if strings.Join(idx.Providers, ",") != "aws,bws" {
		t.Fatalf("providers = %v", idx.Providers)
	}
	if r := idx.Runs[0]; len(r.Changed) != 1 || r.Changed[0] != "bws" || len(r.Feeds) != 1 || r.Scope.Providers[0] != "bws" {
		t.Fatalf("summary = %+v", r)
	}
}

func TestPublishScopedManifestHasScopeAndFeeds(t *testing.T) {
	ctx := context.Background()
	store := FSStore{Root: t.TempDir()}
	now := time.Date(2026, 9, 27, 6, 0, 0, 0, time.UTC)
	d := lcDoc(t, ip("10.0.1.0/24", "active", "2026-09-27"))
	d.Collection.Collectors["aws_ip_ranges"] = model.CollectorStatus{Status: "stale", Items: 1, Error: "status 503"}
	m, err := PublishScoped(ctx, store, []model.Document{d}, now, Scope{Command: "collect", Providers: []string{"aws"}})
	if err != nil {
		t.Fatal(err)
	}
	if m.RunID != "20260927T060000Z-collect" || m.Scope.Command != "collect" || len(m.Scope.Providers) != 1 {
		t.Fatalf("manifest scope = %+v run=%s", m.Scope, m.RunID)
	}
	if len(m.Feeds) != 1 || m.Feeds[0].Status != "stale" || m.Feeds[0].Collector != "aws_ip_ranges" {
		t.Fatalf("feeds = %+v", m.Feeds)
	}
	if len(m.CollectorIssues) != 1 || m.CollectorIssues[0].Status != "stale" || m.CollectorIssues[0].Error != "status 503" {
		t.Fatalf("issues = %+v", m.CollectorIssues)
	}
	if _, ok, _ := store.Get(ctx, HistoryKey("aws", m.RunID)); !ok {
		t.Fatal("history not keyed by run id")
	}
	a, err := PublishScoped(ctx, store, []model.Document{d}, now, Scope{Command: "restore", Providers: []string{"aws"}})
	if err != nil {
		t.Fatal(err)
	}
	if a.RunID == m.RunID || !strings.HasSuffix(a.RunID, "-restore") {
		t.Fatalf("restore run id %q collides with collect run %q", a.RunID, m.RunID)
	}
}

func TestDiffKeepsOverlappingFeedsApart(t *testing.T) {
	goog := ip("34.0.0.0/15", "active", "2026-09-27")
	goog.Collector = "google_goog"
	cloud := ip("34.0.0.0/15", "active", "2026-09-27")
	cloud.Collector = "google_cloud"
	old := lcDoc(t, goog, cloud)
	gone := goog
	gone.Status, gone.MissingSince = "missing", "2026-09-28"
	d := Diff(&old, lcDoc(t, gone, cloud)).Evidence["ip_ranges"]
	if d.MissingCount != 1 || len(d.Missing) != 1 {
		t.Fatalf("one of two overlapping instances went missing but the diff shows %+v", d)
	}
}

func TestFeedRunCarriesSourceDetails(t *testing.T) {
	ctx := context.Background()
	store := FSStore{Root: t.TempDir()}
	now := time.Date(2026, 9, 28, 6, 0, 0, 0, time.UTC)
	d := lcDoc(t, ip("10.0.1.0/24", "active", "2026-09-28"))
	d.Collection.Collectors["aws_ip_ranges"] = model.CollectorStatus{Status: "ok", Items: 1, SourceURL: "https://ip-ranges.amazonaws.com/ip-ranges.json",
		SourceVersion: "syncToken=1", Format: "JSON", FetchedAt: now, LastSuccessAt: &now, SkippedLines: 2}
	m, err := Publish(ctx, store, []model.Document{d}, now)
	if err != nil {
		t.Fatal(err)
	}
	f := m.Feeds[0]
	if f.SourceURL != "https://ip-ranges.amazonaws.com/ip-ranges.json" || f.SourceVersion != "syncToken=1" || f.Format != "JSON" ||
		!f.FetchedAt.Equal(now) || f.LastSuccessAt == nil || f.SkippedLines != 2 {
		t.Fatalf("feed run = %+v", f)
	}
}
