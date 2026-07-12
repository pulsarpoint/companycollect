package worker

import (
	"context"
	"testing"

	"cc-enrich-worker/internal/model"
)

// mergePageResults must be deterministic in WORKLIST order (slice index = rank), regardless of the
// (parallel) completion order that produced the results: primary = lowest-rank SURVIVOR, dedup is
// first-rank-wins, profile is the first non-empty.
func TestMergePageResults(t *testing.T) {
	results := make([]pageResult, 4)
	ok := make([]bool, 4)
	// rank 0 (the true primary) FAILED — ok[0] stays false.
	results[1] = pageResult{
		source: model.WorklistItem{URL: "https://acme.com/about"}, sub: "",
		tech:   []model.Technology{{Name: "WordPress", Version: "6.4"}, {Name: "Nginx"}},
		emails: []string{"info@acme.com"},
		ids:    []model.Identifier{{Type: "vat", Value: "DE123456789", Valid: true}},
	}
	ok[1] = true
	results[2] = pageResult{
		source:  model.WorklistItem{URL: "https://shop.acme.com/"},
		sub:     "shop",
		tech:    []model.Technology{{Name: "WordPress", Version: "6.5"}, {Name: "WooCommerce"}}, // dup name, later rank
		emails:  []string{"info@acme.com", "sales@acme.com"},                                    // first email is a dup
		ids:     []model.Identifier{{Type: "lei", Value: "DE123456789"}},                        // dup value, different type
		profile: model.CompanyProfile{Name: "ACME GmbH"},
	}
	ok[2] = true
	results[3] = pageResult{
		source: model.WorklistItem{URL: "https://acme.com/imprint"}, primary: true, text: "acme imprint text",
		profile: model.CompanyProfile{Name: "Ignored — rank 2 already set one"},
	}
	ok[3] = true

	agg := mergePageResults("acme.com", results, ok)

	if agg.primaryURL != "https://acme.com/about" || agg.primarySub != "" {
		t.Fatalf("primary must be the lowest-rank SURVIVOR: got %q sub=%q", agg.primaryURL, agg.primarySub)
	}
	if len(agg.emails) != 2 || agg.emails[0] != "info@acme.com" || agg.emails[1] != "sales@acme.com" {
		t.Fatalf("email dedup in rank order: %+v", agg.emails)
	}
	if len(agg.identifiers) != 1 || agg.identifiers[0].Type != "vat" {
		t.Fatalf("id dedup by value, lowest rank wins: %+v", agg.identifiers)
	}
	if agg.profile.Name != "ACME GmbH" {
		t.Fatalf("profile = first non-empty by rank: %+v", agg.profile)
	}
	if !agg.hasPrimary || agg.primaryText != "acme imprint text" {
		t.Fatalf("primaryText must come from the result flagged primary: hasPrimary=%v text=%q",
			agg.hasPrimary, agg.primaryText)
	}
}

// A domain whose every page failed produces an empty agg (no primaryURL) — Finalize drops it.
func TestMergePageResultsAllFailed(t *testing.T) {
	agg := mergePageResults("dead.com", make([]pageResult, 3), make([]bool, 3))
	if agg.primaryURL != "" {
		t.Fatalf("all-failed domain must yield an empty agg: %+v", agg)
	}
}

// processDomain fetches a domain's pages CONCURRENTLY (its own page pool) but must produce the
// exact same aggregate as the old sequential loop: worklist-order dedup, first-survivor primary,
// failed pages skipped and counted.
func TestProcessDomainParallelPages(t *testing.T) {
	pageHome := gzWarc("HTTP/1.1 200 OK\r\nServer: nginx\r\n\r\n<html><body>acme home info@acme.com</body></html>")
	pageAbout := gzWarc("HTTP/1.1 200 OK\r\n\r\n<html><head><meta name=\"generator\" content=\"WordPress 6.4\">" +
		"<link href=\"/wp-content/themes/x/style.css\" rel=\"stylesheet\"></head><body>about info@acme.com</body></html>")
	getter := multiGetter{
		"f.warc.gz:0":    pageHome,
		"f.warc.gz:2000": pageAbout,
		// offset 1000 missing -> fetch error for the middle page
	}
	pages := []model.WorklistItem{
		{RootDomain: "acme.com", URL: "https://acme.com/", WarcFilename: "f.warc.gz", Offset: 0, Length: int64(len(pageHome)), Primary: true},
		{RootDomain: "acme.com", URL: "https://acme.com/gone", WarcFilename: "f.warc.gz", Offset: 1000, Length: 10},
		{RootDomain: "acme.com", URL: "https://acme.com/about", WarcFilename: "f.warc.gz", Offset: 2000, Length: int64(len(pageAbout))},
	}
	cfg := ShardConfig{Mode: "tech", Concurrency: 4}

	for range 20 { // parallel pages: result must be deterministic across runs
		stats := &chunkStats{}
		agg, successful := processDomain(context.Background(), getter, cfg, "acme.com", pages, stats)
		if agg.primaryURL != "https://acme.com/" {
			t.Fatalf("primary = first surviving page: %q", agg.primaryURL)
		}
		if len(successful) != 2 || successful[0].source.URL != "https://acme.com/" ||
			successful[1].source.URL != "https://acme.com/about" {
			t.Fatalf("successful pages must retain worklist order: %+v", successful)
		}
		if !hasTech(successful[0].tech, "Nginx") || !hasTech(successful[1].tech, "WordPress") {
			t.Fatalf("per-page technologies lost: %+v", successful)
		}
		if len(agg.emails) != 1 || agg.emails[0] != "info@acme.com" {
			t.Fatalf("emails deduped across pages: %+v", agg.emails)
		}
		if stats.errs != 1 || stats.pages != 3 {
			t.Fatalf("stats: want errs=1 pages=3, got errs=%d pages=%d", stats.errs, stats.pages)
		}
	}
}

func hasTech(techs []model.Technology, name string) bool {
	for _, tc := range techs {
		if tc.Name == name {
			return true
		}
	}
	return false
}
