package assemble

import (
	"fmt"
	"testing"
	"time"

	"provider_recon/internal/definitions"
	"provider_recon/internal/feeds"
	"provider_recon/internal/model"
)

func day(n int) time.Time { return t0.AddDate(0, 0, n) }

func dstr(n int) string { return day(n).Format(model.DateLayout) }

func lcDef() definitions.Definition {
	return definitions.Definition{
		Slug: "aws", DisplayName: "AWS", Category: "cloud",
		Services: []definitions.ServiceDef{{Key: "aws.cloudfront", DisplayName: "CloudFront", ServiceTypes: []string{"cdn"}}},
		Feeds:    []definitions.FeedRef{{Collector: "aws_ip_ranges", TagMap: map[string]string{"CLOUDFRONT": "aws.cloudfront"}}},
	}
}

// cidrs returns n distinct CLOUDFRONT ranges, skipping the indexes in drop.
func cidrs(n int, drop ...int) map[string]FeedOutcome {
	skip := map[int]bool{}
	for _, d := range drop {
		skip[d] = true
	}
	var rs []feeds.Range
	for i := 0; i < n; i++ {
		if !skip[i] {
			rs = append(rs, r(fmt.Sprintf("10.0.%d.0/24", i), "CLOUDFRONT", ""))
		}
	}
	return map[string]FeedOutcome{"aws_ip_ranges": {Result: feeds.Result{Ranges: rs}}}
}

func failed() map[string]FeedOutcome {
	return map[string]FeedOutcome{"aws_ip_ranges": {Err: fmt.Errorf("status 503")}}
}

// run builds day n on top of prev (nil for the first run).
func run(t *testing.T, def definitions.Definition, outcomes map[string]FeedOutcome, prev *model.Document, n int) model.Document {
	t.Helper()
	doc, err := Build(def, outcomes, prev, day(n))
	if err != nil {
		t.Fatal(err)
	}
	return doc
}

func ranges(doc model.Document, cidr string) []model.IPRange {
	var out []model.IPRange
	for _, s := range doc.Services {
		for _, ip := range s.Evidence.IPRanges {
			if ip.CIDR == cidr {
				out = append(out, ip)
			}
		}
	}
	return out
}

func one(t *testing.T, doc model.Document, cidr string) model.IPRange {
	t.Helper()
	rs := ranges(doc, cidr)
	if len(rs) != 1 {
		t.Fatalf("%s: %d instances, want 1: %+v", cidr, len(rs), rs)
	}
	return rs[0]
}

func TestLifecycleNewRangeIsActiveFromToday(t *testing.T) {
	d := run(t, lcDef(), cidrs(1), nil, 0)
	got := one(t, d, "10.0.0.0/24").Lifecycle
	want := model.Lifecycle{Status: model.StatusActive, FirstSeen: dstr(0), LastSeen: dstr(0)}
	if got != want {
		t.Fatalf("lifecycle = %+v, want %+v", got, want)
	}
	if c := d.Collection.Collectors["aws_ip_ranges"].Churn; c.Added != 1 {
		t.Fatalf("churn = %+v", c)
	}
}

func TestLifecycleCarriedRangeKeepsFirstSeen(t *testing.T) {
	d0 := run(t, lcDef(), cidrs(1), nil, 0)
	d3 := run(t, lcDef(), cidrs(1), &d0, 3)
	got := one(t, d3, "10.0.0.0/24").Lifecycle
	if got.FirstSeen != dstr(0) || got.LastSeen != dstr(3) || got.Status != model.StatusActive {
		t.Fatalf("lifecycle = %+v", got)
	}
	if c := d3.Collection.Collectors["aws_ip_ranges"].Churn; c != (model.Churn{}) {
		t.Fatalf("churn = %+v, want zero", c)
	}
	if d0.Collection.ContentHash != d3.Collection.ContentHash {
		t.Fatal("hash changed although only last_seen advanced")
	}
}

func TestLifecycleMissingThenRemovedAfterGrace(t *testing.T) {
	d0 := run(t, lcDef(), cidrs(10), nil, 0)
	d1 := run(t, lcDef(), cidrs(10, 3), &d0, 1)
	m := one(t, d1, "10.0.3.0/24")
	if m.Status != model.StatusMissing || m.MissingSince != dstr(1) || m.LastSeen != dstr(0) {
		t.Fatalf("day1 = %+v", m.Lifecycle)
	}
	st := d1.Collection.Collectors["aws_ip_ranges"]
	if st.Status != "ok" || st.Churn.Missing != 1 || st.Items != 10 {
		t.Fatalf("day1 status = %+v", st)
	}
	d5 := run(t, lcDef(), cidrs(10, 3), &d1, 5)
	if one(t, d5, "10.0.3.0/24").Status != model.StatusMissing {
		t.Fatal("removed before the 7-day grace period ended")
	}
	d8 := run(t, lcDef(), cidrs(10, 3), &d5, 8)
	rm := one(t, d8, "10.0.3.0/24")
	if rm.Status != model.StatusRemoved || rm.RemovedAt != dstr(8) || rm.RemovalAction != model.ActionGraceExpired {
		t.Fatalf("day8 = %+v", rm.Lifecycle)
	}
	if st := d8.Collection.Collectors["aws_ip_ranges"]; st.Churn.Removed != 1 || st.Items != 9 {
		t.Fatalf("day8 status = %+v", st)
	}
}

func TestLifecycleReappearWithinGrace(t *testing.T) {
	d0 := run(t, lcDef(), cidrs(10), nil, 0)
	d1 := run(t, lcDef(), cidrs(10, 3), &d0, 1)
	d2 := run(t, lcDef(), cidrs(10), &d1, 2)
	got := one(t, d2, "10.0.3.0/24").Lifecycle
	if got.Status != model.StatusActive || got.FirstSeen != dstr(0) || got.MissingSince != "" || got.LastSeen != dstr(2) {
		t.Fatalf("lifecycle = %+v", got)
	}
	if c := d2.Collection.Collectors["aws_ip_ranges"].Churn; c.Reappeared != 1 {
		t.Fatalf("churn = %+v", c)
	}
}

func TestLifecycleReaddAfterRemovalStartsNewInstance(t *testing.T) {
	d0 := run(t, lcDef(), cidrs(10), nil, 0)
	d1 := run(t, lcDef(), cidrs(10, 3), &d0, 1)
	d8 := run(t, lcDef(), cidrs(10, 3), &d1, 8)
	d9 := run(t, lcDef(), cidrs(10), &d8, 9)
	rs := ranges(d9, "10.0.3.0/24")
	if len(rs) != 2 {
		t.Fatalf("%d instances, want the removed one plus a new one: %+v", len(rs), rs)
	}
	var removed, active int
	for _, x := range rs {
		switch {
		case x.Status == model.StatusRemoved && x.FirstSeen == dstr(0):
			removed++
		case x.Status == model.StatusActive && x.FirstSeen == dstr(9):
			active++
		}
	}
	if removed != 1 || active != 1 {
		t.Fatalf("instances = %+v", rs)
	}
}

func TestLifecycleNoExpiryWhileFeedFails(t *testing.T) {
	d0 := run(t, lcDef(), cidrs(10), nil, 0)
	d1 := run(t, lcDef(), cidrs(10, 3), &d0, 1)
	prev := d1
	for n := 2; n <= 12; n++ {
		prev = run(t, lcDef(), failed(), &prev, n)
		if st := prev.Collection.Collectors["aws_ip_ranges"]; st.Status != "stale" {
			t.Fatalf("day%d status = %q", n, st.Status)
		}
		if one(t, prev, "10.0.3.0/24").Status != model.StatusMissing {
			t.Fatalf("day%d: expired while the feed was failing", n)
		}
		if one(t, prev, "10.0.4.0/24").Status != model.StatusActive {
			t.Fatalf("day%d: a present range changed status while the feed was failing", n)
		}
	}
	d13 := run(t, lcDef(), cidrs(10, 3), &prev, 13)
	if rm := one(t, d13, "10.0.3.0/24"); rm.Status != model.StatusRemoved || rm.RemovalAction != model.ActionGraceExpired {
		t.Fatalf("first successful run after the outage did not expire it: %+v", rm.Lifecycle)
	}
}

func TestLargeDropIsNotBlockedAndFollowsGrace(t *testing.T) {
	d0 := run(t, lcDef(), cidrs(10), nil, 0)
	d1 := run(t, lcDef(), cidrs(10, 0, 1, 2, 3, 4), &d0, 1)
	st := d1.Collection.Collectors["aws_ip_ranges"]
	if st.Status != "ok" || st.Churn.Missing != 5 || st.Items != 10 {
		t.Fatalf("day1 status = %+v", st)
	}
	d8 := run(t, lcDef(), cidrs(10, 0, 1, 2, 3, 4), &d1, 8)
	if st := d8.Collection.Collectors["aws_ip_ranges"]; st.Churn.Removed != 5 || st.Items != 5 {
		t.Fatalf("day8 status = %+v", st)
	}
}

func TestRestoreUndoesWrongRemovals(t *testing.T) {
	d0 := run(t, lcDef(), cidrs(10), nil, 0)
	d1 := run(t, lcDef(), cidrs(10, 0, 1, 2, 3, 4), &d0, 1)
	d8 := run(t, lcDef(), cidrs(10, 0, 1, 2, 3, 4), &d1, 8)
	restored, n, err := Restore(d8, "aws_ip_ranges", dstr(8), day(8))
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Fatalf("restored %d, want 5", n)
	}
	got := one(t, restored, "10.0.0.0/24").Lifecycle
	want := model.Lifecycle{Status: model.StatusActive, FirstSeen: dstr(0), LastSeen: dstr(0), RestoredAt: dstr(8)}
	if got != want {
		t.Fatalf("restored lifecycle = %+v, want %+v", got, want)
	}
	if st := restored.Collection.Collectors["aws_ip_ranges"]; st.Items != 10 {
		t.Fatalf("items after restore = %d", st.Items)
	}
	if restored.Collection.ContentHash == d8.Collection.ContentHash {
		t.Fatal("restore must change the content hash")
	}
	// The collector is fixed and the feed lists the ranges again: the
	// timeline continues from the original first_seen, nothing is "added".
	d9 := run(t, lcDef(), cidrs(10), &restored, 9)
	if st := d9.Collection.Collectors["aws_ip_ranges"]; st.Churn.Added != 0 || st.Churn.Missing != 0 {
		t.Fatalf("run after restore churn = %+v", st.Churn)
	}
	if l := one(t, d9, "10.0.0.0/24").Lifecycle; l.FirstSeen != dstr(0) || l.LastSeen != dstr(9) || l.RestoredAt != dstr(8) {
		t.Fatalf("lifecycle after restore + collect = %+v", l)
	}
}

func TestRestoreRespectsSinceReasonAndSuccessors(t *testing.T) {
	d0 := run(t, lcDef(), cidrs(10), nil, 0)
	d1 := run(t, lcDef(), cidrs(10, 0), &d0, 1)
	d8 := run(t, lcDef(), cidrs(10, 0, 1), &d1, 8) // 10.0.0 removed on day 8, 10.0.1 missing since day 8
	d15 := run(t, lcDef(), cidrs(10, 0, 1), &d8, 15)
	// 10.0.1 was removed on day 15. 10.0.0 came back as a new instance on day 16.
	d16 := run(t, lcDef(), cidrs(10, 1), &d15, 16)

	if _, _, err := Restore(d16, "aws_ip_ranges", dstr(16), day(16)); err == nil {
		t.Fatal("nothing was removed on or after day 16; restore must fail")
	}
	restored, n, err := Restore(d16, "aws_ip_ranges", dstr(8), day(16))
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("restored %d, want only 10.0.1 (10.0.0 already has a live successor)", n)
	}
	if l := one(t, restored, "10.0.1.0/24").Lifecycle; l.Status != model.StatusActive || l.RestoredAt != dstr(16) {
		t.Fatalf("10.0.1 = %+v", l)
	}
	for _, x := range ranges(restored, "10.0.0.0/24") {
		if x.FirstSeen == dstr(0) && x.Status != model.StatusRemoved {
			t.Fatalf("the old 10.0.0 instance must stay removed next to its successor: %+v", x.Lifecycle)
		}
	}

	withRule := lcDef()
	withRule.Services[0].DNSRules = []definitions.DNSRuleDef{{RecordType: "CNAME", MatchField: "target", MatcherType: "suffix", Pattern: "cloudfront.net", Priority: 100}}
	r0 := run(t, withRule, cidrs(1), nil, 0)
	r1 := run(t, lcDef(), cidrs(1), &r0, 1) // the rule is definition_removed
	if _, _, err := Restore(r1, "aws_ip_ranges", dstr(0), day(1)); err == nil {
		t.Fatal("definition removals are never restored; nothing else was removed, so restore must fail")
	}
}

func TestRestoreErrors(t *testing.T) {
	d0 := run(t, lcDef(), cidrs(3), nil, 0)
	if _, _, err := Restore(d0, "nope", dstr(0), day(0)); err == nil {
		t.Fatal("unknown collector must fail")
	}
	if _, _, err := Restore(d0, "aws_ip_ranges", "2026/09/27", day(0)); err == nil {
		t.Fatal("a malformed date must fail")
	}
	if _, _, err := Restore(d0, "aws_ip_ranges", dstr(0), day(0)); err == nil {
		t.Fatal("a clean feed has nothing to restore")
	}
}

func TestCuratedRuleRemovedFromDefinition(t *testing.T) {
	withRule := lcDef()
	withRule.Services[0].DNSRules = []definitions.DNSRuleDef{{RecordType: "CNAME", MatchField: "target", MatcherType: "suffix", Pattern: "cloudfront.net", Priority: 100}}
	d0 := run(t, withRule, cidrs(1), nil, 0)
	d1 := run(t, lcDef(), cidrs(1), &d0, 1)
	rules := d1.Services[0].Evidence.DNSRules
	if len(rules) != 1 || rules[0].Status != model.StatusRemoved || rules[0].RemovalAction != model.ActionDefinitionRemoved || rules[0].RemovedAt != dstr(1) {
		t.Fatalf("rules = %+v", rules)
	}
	d2 := run(t, withRule, cidrs(1), &d1, 2)
	if rules := d2.Services[0].Evidence.DNSRules; len(rules) != 2 {
		t.Fatalf("re-added rule should be a new instance next to the removed one: %+v", rules)
	}
}

func TestServiceRemovedFromDefinitionThenPurged(t *testing.T) {
	two := lcDef()
	two.Services = append(two.Services, definitions.ServiceDef{Key: "aws.ec2", DisplayName: "EC2", ServiceTypes: []string{"iaas"}})
	two.Feeds[0].TagMap["EC2"] = "aws.ec2"
	out := map[string]FeedOutcome{"aws_ip_ranges": {Result: feeds.Result{Ranges: []feeds.Range{
		r("10.0.0.0/24", "CLOUDFRONT", ""), r("3.5.140.0/22", "EC2", ""),
	}}}}
	d0 := run(t, two, out, nil, 0)
	d1 := run(t, lcDef(), out, &d0, 1)
	ec2 := one(t, d1, "3.5.140.0/22")
	if ec2.Status != model.StatusRemoved || ec2.RemovalAction != model.ActionDefinitionRemoved {
		t.Fatalf("ec2 range = %+v", ec2.Lifecycle)
	}
	var svc *model.Service
	for i := range d1.Services {
		if d1.Services[i].Key == "aws.ec2" {
			svc = &d1.Services[i]
		}
	}
	if svc == nil || svc.RemovedAt != dstr(1) {
		t.Fatalf("removed service not kept with removed_at: %+v", svc)
	}
	if tags := d1.Collection.Collectors["aws_ip_ranges"].UnmappedTags; len(tags) != 1 || tags[0] != "EC2" {
		t.Fatalf("unmapped = %v", tags)
	}
	d93 := run(t, lcDef(), out, &d1, 93)
	for _, s := range d93.Services {
		if s.Key == "aws.ec2" {
			t.Fatal("service with only purged items should disappear")
		}
	}
	if len(ranges(d93, "3.5.140.0/22")) != 0 {
		t.Fatal("removed range not purged after 90 days")
	}
}

func TestRetentionPurgesRemovedAfter90Days(t *testing.T) {
	d0 := run(t, lcDef(), cidrs(10), nil, 0)
	d1 := run(t, lcDef(), cidrs(10, 3), &d0, 1)
	d8 := run(t, lcDef(), cidrs(10, 3), &d1, 8)
	d98 := run(t, lcDef(), cidrs(10, 3), &d8, 98)
	if len(ranges(d98, "10.0.3.0/24")) != 1 {
		t.Fatal("purged before the 90-day retention ended")
	}
	d99 := run(t, lcDef(), cidrs(10, 3), &d98, 99)
	if len(ranges(d99, "10.0.3.0/24")) != 0 {
		t.Fatal("not purged after 90 days")
	}
	if c := d99.Collection.Collectors["aws_ip_ranges"].Churn; c.Purged != 1 {
		t.Fatalf("churn = %+v", c)
	}
}

func TestFeedDroppedFromDefinitionRemovesItsRanges(t *testing.T) {
	d0 := run(t, lcDef(), cidrs(2), nil, 0)
	noFeed := lcDef()
	noFeed.Feeds = nil
	d1 := run(t, noFeed, nil, &d0, 1)
	for _, x := range ranges(d1, "10.0.0.0/24") {
		if x.Status != model.StatusRemoved || x.RemovalAction != model.ActionDefinitionRemoved {
			t.Fatalf("range = %+v", x.Lifecycle)
		}
	}
	if _, ok := d1.Collection.Collectors["aws_ip_ranges"]; ok {
		t.Fatal("a dropped feed must not report a status")
	}
}

func TestLegacyPreviousDocumentIsUpgraded(t *testing.T) {
	legacy := model.Document{Version: model.ContractVersion, Slug: "aws",
		Services: []model.Service{{Key: "aws.cloudfront", Evidence: model.Evidence{IPRanges: []model.IPRange{{
			CIDR: "10.0.0.0/24", FeedTag: "CLOUDFRONT", Confidence: 1,
			Provenance: model.Provenance{Source: model.SourceOfficialFeed, Collector: "aws_ip_ranges"},
		}}}}},
		Collection: model.Collection{CollectedAt: day(0), Collectors: map[string]model.CollectorStatus{
			"aws_ip_ranges": {Status: "ok", LastSuccessAt: func() *time.Time { x := day(0); return &x }()},
		}},
	}
	d1 := run(t, lcDef(), cidrs(1), &legacy, 1)
	got := one(t, d1, "10.0.0.0/24").Lifecycle
	if got.Status != model.StatusActive || got.FirstSeen != dstr(0) || got.LastSeen != dstr(1) {
		t.Fatalf("lifecycle = %+v", got)
	}
	if c := d1.Collection.Collectors["aws_ip_ranges"].Churn; c.Added != 0 {
		t.Fatalf("legacy range counted as new: %+v", c)
	}
}

func TestOverlappingFeedsKeepOneInstancePerCollector(t *testing.T) {
	def := definitions.Definition{
		Slug: "google", DisplayName: "Google", Category: "cloud",
		Services: []definitions.ServiceDef{{Key: "google.frontend", DisplayName: "Google", ServiceTypes: []string{"hosting"}}},
		Feeds: []definitions.FeedRef{
			{Collector: "google_goog", TagMap: map[string]string{"GOOG": "google.frontend"}},
			{Collector: "google_cloud", TagMap: map[string]string{"Google Cloud": "google.frontend"}},
		},
	}
	both := map[string]FeedOutcome{
		"google_goog":  {Result: feeds.Result{Ranges: []feeds.Range{r("34.0.0.0/15", "GOOG", "")}}},
		"google_cloud": {Result: feeds.Result{Ranges: []feeds.Range{r("34.0.0.0/15", "Google Cloud", "us-east1")}}},
	}
	d0 := run(t, def, both, nil, 0)
	if n := len(ranges(d0, "34.0.0.0/15")); n != 2 {
		t.Fatalf("%d instances, want one per collector", n)
	}
	cloudFails := map[string]FeedOutcome{"google_goog": both["google_goog"], "google_cloud": {Err: fmt.Errorf("timeout")}}
	d1 := run(t, def, cloudFails, &d0, 1)
	rs := ranges(d1, "34.0.0.0/15")
	if len(rs) != 2 {
		t.Fatalf("a failing collector lost its overlapping range: %+v", rs)
	}
	for _, x := range rs {
		if x.Status != model.StatusActive {
			t.Fatalf("range = %+v", x)
		}
	}
}

func TestRestoreResetsChurnAndCountsRestored(t *testing.T) {
	def := lcDef()
	def.Feeds = append(def.Feeds, definitions.FeedRef{Collector: "other_feed", TagMap: map[string]string{"X": "aws.cloudfront"}})
	with := func(drop ...int) map[string]FeedOutcome {
		out := cidrs(10, drop...)
		out["other_feed"] = FeedOutcome{Result: feeds.Result{Ranges: []feeds.Range{r("192.0.2.0/24", "X", "")}}}
		return out
	}
	d0 := run(t, def, with(), nil, 0)
	d1 := run(t, def, with(0, 1), &d0, 1)
	d8 := run(t, def, with(0, 1), &d1, 8)
	restored, n, err := Restore(d8, "aws_ip_ranges", dstr(8), day(8))
	if err != nil || n != 2 {
		t.Fatalf("restore: n=%d err=%v", n, err)
	}
	if c := restored.Collection.Collectors["aws_ip_ranges"].Churn; c != (model.Churn{Restored: 2}) {
		t.Fatalf("restored feed churn = %+v, want only Restored=2", c)
	}
	if c := restored.Collection.Collectors["other_feed"].Churn; c != (model.Churn{}) {
		t.Fatalf("untouched feed churn = %+v, want zero", c)
	}
}

func TestRestoreRevivesOnlyTheLatestRemovedInstance(t *testing.T) {
	d0 := run(t, lcDef(), cidrs(10), nil, 0)
	d1 := run(t, lcDef(), cidrs(10, 0), &d0, 1)
	d8 := run(t, lcDef(), cidrs(10, 0), &d1, 8)    // instance A (first day 0) removed on day 8
	d9 := run(t, lcDef(), cidrs(10), &d8, 9)       // instance B starts on day 9
	d10 := run(t, lcDef(), cidrs(10, 0), &d9, 10)  // B missing since day 10
	d17 := run(t, lcDef(), cidrs(10, 0), &d10, 17) // B removed on day 17
	restored, n, err := Restore(d17, "aws_ip_ranges", dstr(8), day(17))
	if err != nil || n != 1 {
		t.Fatalf("restore: n=%d err=%v", n, err)
	}
	for _, x := range ranges(restored, "10.0.0.0/24") {
		switch x.FirstSeen {
		case dstr(0):
			if x.Status != model.StatusRemoved {
				t.Fatalf("older instance A was revived across the day 8→9 gap: %+v", x.Lifecycle)
			}
		case dstr(9):
			if x.Status != model.StatusActive || x.RestoredAt != dstr(17) {
				t.Fatalf("latest instance B not restored: %+v", x.Lifecycle)
			}
		default:
			t.Fatalf("unexpected instance %+v", x.Lifecycle)
		}
	}
}

func TestSameDayCuratedReaddRevivesTheInstance(t *testing.T) {
	withRule := lcDef()
	withRule.Services[0].DNSRules = []definitions.DNSRuleDef{{RecordType: "CNAME", MatchField: "target", MatcherType: "suffix", Pattern: "cloudfront.net", Priority: 100}}
	a := run(t, withRule, cidrs(1), nil, 0)
	b := run(t, lcDef(), cidrs(1), &a, 0)  // removed the same day it first appeared
	c := run(t, withRule, cidrs(1), &b, 0) // re-added the same day
	rules := c.Services[0].Evidence.DNSRules
	if len(rules) != 1 || rules[0].Status != model.StatusActive || rules[0].RemovedAt != "" || rules[0].FirstSeen != dstr(0) {
		t.Fatalf("same-day re-add must revive the one instance, got %+v", rules)
	}
}

func TestSameDayFeedReaddRevivesTheInstance(t *testing.T) {
	a := run(t, lcDef(), cidrs(1), nil, 0)
	noFeed := lcDef()
	noFeed.Feeds = nil
	b := run(t, noFeed, nil, &a, 0) // feed dropped the same day: range definition_removed
	c := run(t, lcDef(), cidrs(1), &b, 0)
	rs := ranges(c, "10.0.0.0/24")
	if len(rs) != 1 || rs[0].Status != model.StatusActive || rs[0].FirstSeen != dstr(0) {
		t.Fatalf("same-day re-add must revive the one instance, got %+v", rs)
	}
}
