package publish

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"provider_recon/internal/model"
)

// RunIDLayout formats run ids and history object names (UTC).
const RunIDLayout = "20060102T150405Z"

// LatestKey is the current document of a provider.
func LatestKey(slug string) string { return "providers/" + slug + "/latest.json" }

// RunID names a run: its time plus the command, so a restore right after a
// collect never overwrites that run's objects.
func RunID(now time.Time, command string) string {
	return now.UTC().Format(RunIDLayout) + "-" + command
}

// HistoryKey is the copy written when a provider's content hash changes.
func HistoryKey(slug, runID string) string {
	return "providers/" + slug + "/history/" + runID + ".json"
}

// ChangesKey is the per-run change manifest.
func ChangesKey(runID string) string { return "changes/" + runID + ".json" }

// CollectorIssue is a collector that was not ok for a provider.
type CollectorIssue struct {
	Slug      string `json:"slug"`
	Collector string `json:"collector"`
	Status    string `json:"status"`
	Error     string `json:"error,omitempty"`
}

// Scope says what produced a run.
type Scope struct {
	Command   string   `json:"command"`             // collect | restore
	Providers []string `json:"providers,omitempty"` // empty: every definition
}

// FeedRun is one feed's outcome for one provider in a run; churn is what the
// backoffice warning rules are evaluated against.
type FeedRun struct {
	Slug         string      `json:"slug"`
	Collector    string      `json:"collector"`
	Status       string      `json:"status"`
	Items        int         `json:"items"`
	Churn        model.Churn `json:"churn"`
	UnmappedTags []string    `json:"unmapped_tags,omitempty"`
	Error        string      `json:"error,omitempty"`
}

// IndexKey is the run index the backoffice reads: recent runs, newest first,
// and every provider slug ever published.
const IndexKey = "changes/index.json"

const maxIndexedRuns = 400

// RunSummary is one run in the index.
type RunSummary struct {
	RunID          string    `json:"run_id"`
	PublishedAt    time.Time `json:"published_at"`
	Scope          Scope     `json:"scope"`
	Changed        []string  `json:"changed"`
	UnchangedCount int       `json:"unchanged_count"`
	Issues         int       `json:"issues"`
	Feeds          []FeedRun `json:"feeds"`
}

// RunIndex lists recent runs and all provider slugs.
type RunIndex struct {
	Runs      []RunSummary `json:"runs"`
	Providers []string     `json:"providers"`
}

func updateIndex(ctx context.Context, store Store, m Manifest, docs []model.Document) error {
	var idx RunIndex
	raw, ok, err := store.Get(ctx, IndexKey)
	if err != nil {
		return err
	}
	if ok {
		if err := json.Unmarshal(raw, &idx); err != nil {
			return fmt.Errorf("decode %s: %w", IndexKey, err)
		}
	}
	changed := make([]string, 0, len(m.Changed))
	for _, c := range m.Changed {
		changed = append(changed, c.Slug)
	}
	summary := RunSummary{RunID: m.RunID, PublishedAt: m.PublishedAt, Scope: m.Scope, Changed: changed,
		UnchangedCount: len(m.Unchanged), Issues: len(m.CollectorIssues), Feeds: m.Feeds}
	runs := []RunSummary{summary}
	for _, r := range idx.Runs {
		if r.RunID != m.RunID {
			runs = append(runs, r)
		}
	}
	if len(runs) > maxIndexedRuns {
		runs = runs[:maxIndexedRuns]
	}
	slugs := map[string]bool{}
	for _, s := range idx.Providers {
		slugs[s] = true
	}
	for _, d := range docs {
		slugs[d.Slug] = true
	}
	idx.Runs, idx.Providers = runs, make([]string, 0, len(slugs))
	for s := range slugs {
		idx.Providers = append(idx.Providers, s)
	}
	sort.Strings(idx.Providers)
	body, err := json.MarshalIndent(idx, "", "  ")
	if err != nil {
		return err
	}
	return store.Put(ctx, IndexKey, append(body, '\n'), "application/json")
}

// Manifest is written to changes/<run_id>.json on every run.
type Manifest struct {
	RunID           string           `json:"run_id"`
	Scope           Scope            `json:"scope"`
	PublishedAt     time.Time        `json:"published_at"`
	Changed         []ProviderChange `json:"changed"`
	Unchanged       []string         `json:"unchanged"`
	CollectorIssues []CollectorIssue `json:"collector_issues"`
	Feeds           []FeedRun        `json:"feeds"`
}

// LoadLatest returns the provider's latest document, or nil when none exists.
func LoadLatest(ctx context.Context, store Store, slug string) (*model.Document, error) {
	b, ok, err := store.Get(ctx, LatestKey(slug))
	if err != nil || !ok {
		return nil, err
	}
	var d model.Document
	if err := json.Unmarshal(b, &d); err != nil {
		return nil, fmt.Errorf("decode %s: %w", LatestKey(slug), err)
	}
	return &d, nil
}

// Publish is PublishScoped for a full collect run.
func Publish(ctx context.Context, store Store, docs []model.Document, now time.Time) (Manifest, error) {
	return PublishScoped(ctx, store, docs, now, Scope{Command: "collect"})
}

// PublishScoped runs in three phases so an interrupted run is always
// re-detected: (1) history objects for every changed provider, (2) the run's
// change manifest and the run index, (3) every latest.json. latest.json is
// what the next run compares against, so until phase 3 completes every change
// of this run is reported again next time rather than lost from the manifest
// stream.
func PublishScoped(ctx context.Context, store Store, docs []model.Document, now time.Time, scope Scope) (Manifest, error) {
	m := Manifest{RunID: RunID(now, scope.Command), Scope: scope, PublishedAt: now.UTC(),
		Changed: []ProviderChange{}, Unchanged: []string{}, CollectorIssues: []CollectorIssue{}, Feeds: []FeedRun{}}
	sorted := append([]model.Document(nil), docs...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Slug < sorted[j].Slug })

	bodies := make([][]byte, len(sorted))
	for i, doc := range sorted {
		prev, err := LoadLatest(ctx, store, doc.Slug)
		if err != nil {
			return m, err
		}
		if bodies[i], err = model.Marshal(doc); err != nil {
			return m, err
		}
		if prev == nil || prev.Collection.ContentHash != doc.Collection.ContentHash {
			m.Changed = append(m.Changed, Diff(prev, doc))
			if err := store.Put(ctx, HistoryKey(doc.Slug, m.RunID), bodies[i], "application/json"); err != nil {
				return m, err
			}
		} else {
			m.Unchanged = append(m.Unchanged, doc.Slug)
		}
		ids := make([]string, 0, len(doc.Collection.Collectors))
		for id := range doc.Collection.Collectors {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			st := doc.Collection.Collectors[id]
			m.Feeds = append(m.Feeds, FeedRun{Slug: doc.Slug, Collector: id, Status: st.Status, Items: st.Items,
				Churn: st.Churn, UnmappedTags: st.UnmappedTags, Error: st.Error})
			if st.Status != "ok" {
				m.CollectorIssues = append(m.CollectorIssues, CollectorIssue{Slug: doc.Slug, Collector: id, Status: st.Status, Error: st.Error})
			}
		}
	}

	mb, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return m, err
	}
	if err := store.Put(ctx, ChangesKey(m.RunID), append(mb, '\n'), "application/json"); err != nil {
		return m, err
	}
	if err := updateIndex(ctx, store, m, sorted); err != nil {
		return m, err
	}

	for i, doc := range sorted {
		if err := store.Put(ctx, LatestKey(doc.Slug), bodies[i], "application/json"); err != nil {
			return m, err
		}
	}
	return m, nil
}
