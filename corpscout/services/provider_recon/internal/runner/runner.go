// Package runner holds the collect and restore operations shared by the CLI
// and the HTTP service.
package runner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
	"time"

	"provider_recon/internal/assemble"
	"provider_recon/internal/definitions"
	"provider_recon/internal/feeds"
	"provider_recon/internal/model"
	"provider_recon/internal/publish"
)

// ErrUnknownProvider means a requested slug has no definition.
var ErrUnknownProvider = errors.New("unknown provider")

// ErrNoDocument means the provider has never been published.
var ErrNoDocument = errors.New("no published document")

// Config wires one run.
type Config struct {
	DefinitionsDir string
	Registry       func() map[string]feeds.Collector
	Fetcher        *feeds.Fetcher
	Store          publish.Store
	Concurrency    int
	Logger         *slog.Logger
}

func (c Config) logger() *slog.Logger {
	if c.Logger != nil {
		return c.Logger
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// LoadDefinitions loads and validates every definition against the
// registry's collectors.
func LoadDefinitions(dir string, registry func() map[string]feeds.Collector) ([]definitions.Definition, error) {
	defs, err := definitions.LoadDir(dir)
	if err != nil {
		return nil, err
	}
	validators := map[string]definitions.ParamValidator{}
	for name, c := range registry() {
		validators[name] = c
	}
	if err := definitions.Validate(defs, validators); err != nil {
		return nil, err
	}
	return defs, nil
}

// UniqueSlugs drops repeated slugs, keeping first-seen order; nil stays nil.
func UniqueSlugs(slugs []string) []string {
	if slugs == nil {
		return nil
	}
	seen := make(map[string]bool, len(slugs))
	out := make([]string, 0, len(slugs))
	for _, s := range slugs {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

// SelectProviders returns the definitions for slugs, or all when slugs is empty.
func SelectProviders(defs []definitions.Definition, slugs []string) ([]definitions.Definition, error) {
	if len(slugs) == 0 {
		return defs, nil
	}
	bySlug := make(map[string]definitions.Definition, len(defs))
	for _, d := range defs {
		bySlug[d.Slug] = d
	}
	out := make([]definitions.Definition, 0, len(slugs))
	for _, s := range UniqueSlugs(slugs) {
		d, ok := bySlug[s]
		if !ok {
			return nil, fmt.Errorf("%w: %q", ErrUnknownProvider, s)
		}
		out = append(out, d)
	}
	return out, nil
}

// Collect runs the selected providers' feeds and publishes at now; the
// manifest's run id is publish.RunID(now, "collect").
func Collect(ctx context.Context, cfg Config, providers []string, now time.Time) (publish.Manifest, error) {
	providers = UniqueSlugs(providers)
	defs, err := LoadDefinitions(cfg.DefinitionsDir, cfg.Registry)
	if err != nil {
		return publish.Manifest{}, err
	}
	if defs, err = SelectProviders(defs, providers); err != nil {
		return publish.Manifest{}, err
	}
	outcomes := CollectFeeds(ctx, UniqueFeeds(defs), cfg.Registry(), cfg.Fetcher, cfg.Concurrency, cfg.logger())
	docs := make([]model.Document, 0, len(defs))
	for _, d := range defs {
		prev, err := publish.LoadLatest(ctx, cfg.Store, d.Slug)
		if err != nil {
			return publish.Manifest{}, err
		}
		doc, err := assemble.Build(d, outcomes, prev, now)
		if err != nil {
			return publish.Manifest{}, fmt.Errorf("%s: %w", d.Slug, err)
		}
		docs = append(docs, doc)
	}
	return publish.PublishScoped(ctx, cfg.Store, docs, now, publish.Scope{Command: "collect", Providers: providers})
}

// Restore undoes grace-expired removals of one feed (see assemble.Restore)
// and publishes the result as a restore run.
func Restore(ctx context.Context, cfg Config, slug, collector, removedSince string, now time.Time) (publish.Manifest, int, error) {
	prev, err := publish.LoadLatest(ctx, cfg.Store, slug)
	if err != nil {
		return publish.Manifest{}, 0, err
	}
	if prev == nil {
		return publish.Manifest{}, 0, fmt.Errorf("%w for %q", ErrNoDocument, slug)
	}
	doc, n, err := assemble.Restore(*prev, collector, removedSince, now)
	if err != nil {
		return publish.Manifest{}, 0, err
	}
	m, err := publish.PublishScoped(ctx, cfg.Store, []model.Document{doc}, now, publish.Scope{Command: "restore", Providers: []string{slug}})
	return m, n, err
}

// UniqueFeeds returns each feed reference once (by ID), sorted by ID.
func UniqueFeeds(defs []definitions.Definition) []definitions.FeedRef {
	seen := map[string]definitions.FeedRef{}
	for _, d := range defs {
		for _, f := range d.Feeds {
			seen[f.ID()] = f
		}
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]definitions.FeedRef, len(ids))
	for i, id := range ids {
		out[i] = seen[id]
	}
	return out
}

// CollectFeeds runs every feed with bounded parallelism; a failure is an
// outcome, never an abort.
func CollectFeeds(ctx context.Context, refs []definitions.FeedRef, collectors map[string]feeds.Collector, f *feeds.Fetcher, limit int, logger *slog.Logger) map[string]assemble.FeedOutcome {
	out := make(map[string]assemble.FeedOutcome, len(refs))
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, max(limit, 1))
	for _, ref := range refs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			start := time.Now()
			res, err := collectors[ref.Collector].Collect(ctx, f, ref.Params)
			if err != nil {
				logger.Warn("collector failed", "collector", ref.ID(), "err", err)
			} else {
				logger.Info("collected", "collector", ref.ID(), "ranges", len(res.Ranges), "skipped", res.Skipped,
					"version", res.SourceVersion, "took", time.Since(start).Round(time.Millisecond))
			}
			mu.Lock()
			out[ref.ID()] = assemble.FeedOutcome{Result: res, Err: err}
			mu.Unlock()
		}()
	}
	wg.Wait()
	return out
}
