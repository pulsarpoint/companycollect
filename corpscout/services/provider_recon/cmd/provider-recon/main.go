// Command provider-recon collects provider evidence (official IP-range feeds,
// BGP announcements, curated rules) into provider-recon/v1 documents.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"sync"
	"time"

	"provider_recon/internal/assemble"
	"provider_recon/internal/definitions"
	"provider_recon/internal/feeds"
	"provider_recon/internal/model"
	"provider_recon/internal/publish"
)

// registry is overridden in tests.
var registry = feeds.Registry

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		usage(stderr)
		return 64
	}
	switch args[0] {
	case "validate":
		return cmdValidate(args[1:], stdout, stderr)
	case "schema":
		return cmdSchema(args[1:], stderr)
	case "collect":
		return cmdCollect(ctx, args[1:], stdout, stderr)
	case "restore":
		return cmdRestore(ctx, args[1:], stdout, stderr)
	default:
		usage(stderr)
		return 64
	}
}

func usage(w io.Writer) {
	fmt.Fprintln(w, "usage: provider-recon validate|schema|collect|restore [flags]")
}

func loadDefinitions(dir string) ([]definitions.Definition, error) {
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

func cmdValidate(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dir := fs.String("definitions", "definitions", "definitions directory")
	if err := fs.Parse(args); err != nil {
		return 64
	}
	defs, err := loadDefinitions(*dir)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintf(stdout, "%d definitions valid\n", len(defs))
	return 0
}

func cmdSchema(args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("schema", flag.ContinueOnError)
	fs.SetOutput(stderr)
	out := fs.String("o", "definitions/schema.json", "output path")
	if err := fs.Parse(args); err != nil {
		return 64
	}
	b, err := definitions.Schema()
	if err == nil {
		err = os.WriteFile(*out, b, 0o644)
	}
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

func cmdCollect(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("collect", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dir := fs.String("definitions", "definitions", "definitions directory")
	only := fs.String("provider", "", "collect a single provider slug")
	outDir := fs.String("out", "", "write to this local directory instead of S3")
	concurrency := fs.Int("concurrency", 4, "collectors run in parallel")
	if err := fs.Parse(args); err != nil {
		return 64
	}
	logger := slog.New(slog.NewTextHandler(stderr, nil))

	defs, err := loadDefinitions(*dir)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if *only != "" {
		var picked []definitions.Definition
		for _, d := range defs {
			if d.Slug == *only {
				picked = append(picked, d)
			}
		}
		if len(picked) == 0 {
			fmt.Fprintf(stderr, "no definition with slug %q\n", *only)
			return 1
		}
		defs = picked
	}

	store, err := openStore(ctx, *outDir)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}

	now := time.Now().UTC().Truncate(time.Second)
	outcomes := collectAll(ctx, uniqueFeeds(defs), registry(), feeds.NewFetcher(), *concurrency, logger)

	docs := make([]model.Document, 0, len(defs))
	for _, d := range defs {
		prev, err := publish.LoadLatest(ctx, store, d.Slug)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		doc, err := assemble.Build(d, outcomes, prev, now)
		if err != nil {
			fmt.Fprintf(stderr, "%s: %v\n", d.Slug, err)
			return 1
		}
		docs = append(docs, doc)
	}

	scope := publish.Scope{Command: "collect"}
	if *only != "" {
		scope.Providers = []string{*only}
	}
	m, err := publish.PublishScoped(ctx, store, docs, now, scope)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintf(stdout, "run %s: %d changed, %d unchanged, %d collector issues\n",
		m.RunID, len(m.Changed), len(m.Unchanged), len(m.CollectorIssues))
	for _, c := range m.Changed {
		fmt.Fprintf(stdout, "changed %s %s\n", c.Slug, c.NewHash)
	}
	for _, i := range m.CollectorIssues {
		fmt.Fprintf(stdout, "issue %s %s %s: %s\n", i.Slug, i.Collector, i.Status, i.Error)
	}
	if len(m.CollectorIssues) > 0 {
		return 2
	}
	return 0
}

// cmdRestore undoes wrong removals. Fix the collector first, restore, then
// collect: ranges the feed lists again continue their original timeline.
func cmdRestore(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("restore", flag.ContinueOnError)
	fs.SetOutput(stderr)
	slug := fs.String("provider", "", "provider slug")
	collector := fs.String("collector", "", "collector id as listed in the manifest, e.g. aws_ip_ranges")
	since := fs.String("removed-since", "", "restore ranges removed on or after this day (YYYY-MM-DD)")
	outDir := fs.String("out", "", "use this local directory instead of S3")
	if err := fs.Parse(args); err != nil {
		return 64
	}
	if *slug == "" || *collector == "" || *since == "" {
		fmt.Fprintln(stderr, "restore needs -provider, -collector and -removed-since")
		return 64
	}
	store, err := openStore(ctx, *outDir)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	prev, err := publish.LoadLatest(ctx, store, *slug)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if prev == nil {
		fmt.Fprintf(stderr, "no published document for %q\n", *slug)
		return 1
	}
	now := time.Now().UTC().Truncate(time.Second)
	doc, n, err := assemble.Restore(*prev, *collector, *since, now)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	m, err := publish.PublishScoped(ctx, store, []model.Document{doc}, now, publish.Scope{Command: "restore", Providers: []string{*slug}})
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintf(stdout, "restored %d ranges for %s %s (run %s)\n", n, *slug, *collector, m.RunID)
	return 0
}

func openStore(ctx context.Context, outDir string) (publish.Store, error) {
	if outDir != "" {
		return publish.FSStore{Root: outDir}, nil
	}
	cfg, err := publish.S3ConfigFromEnv()
	if err != nil {
		return nil, err
	}
	s, err := publish.NewS3Store(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return s, s.EnsureBucket(ctx)
}

// uniqueFeeds returns each feed reference once (by ID), sorted by ID.
func uniqueFeeds(defs []definitions.Definition) []definitions.FeedRef {
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

func collectAll(ctx context.Context, refs []definitions.FeedRef, collectors map[string]feeds.Collector, f *feeds.Fetcher, limit int, logger *slog.Logger) map[string]assemble.FeedOutcome {
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
