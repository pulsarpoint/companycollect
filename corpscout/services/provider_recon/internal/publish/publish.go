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

// HistoryKey is the copy written when a provider's content hash changes.
func HistoryKey(slug string, at time.Time) string {
	return "providers/" + slug + "/history/" + at.UTC().Format(RunIDLayout) + ".json"
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

// Manifest is written to changes/<run_id>.json on every run.
type Manifest struct {
	RunID           string           `json:"run_id"`
	PublishedAt     time.Time        `json:"published_at"`
	Changed         []ProviderChange `json:"changed"`
	Unchanged       []string         `json:"unchanged"`
	CollectorIssues []CollectorIssue `json:"collector_issues"`
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

// Publish runs in three phases so an interrupted run is always re-detected:
// (1) history objects for every changed provider, (2) the run's change
// manifest, (3) every latest.json. latest.json is what the next run compares
// against, so until phase 3 completes every change of this run is reported
// again next time rather than lost from the manifest stream.
func Publish(ctx context.Context, store Store, docs []model.Document, now time.Time) (Manifest, error) {
	m := Manifest{RunID: now.UTC().Format(RunIDLayout), PublishedAt: now.UTC(),
		Changed: []ProviderChange{}, Unchanged: []string{}, CollectorIssues: []CollectorIssue{}}
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
			if err := store.Put(ctx, HistoryKey(doc.Slug, now), bodies[i], "application/json"); err != nil {
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
			if st := doc.Collection.Collectors[id]; st.Status != "ok" {
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

	for i, doc := range sorted {
		if err := store.Put(ctx, LatestKey(doc.Slug), bodies[i], "application/json"); err != nil {
			return m, err
		}
	}
	return m, nil
}
