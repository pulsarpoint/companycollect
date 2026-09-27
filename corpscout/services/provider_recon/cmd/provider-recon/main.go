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
	"time"

	"provider_recon/internal/definitions"
	"provider_recon/internal/feeds"
	"provider_recon/internal/publish"
	"provider_recon/internal/runner"
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
	case "serve":
		return cmdServe(ctx, args[1:], stdout, stderr)
	default:
		usage(stderr)
		return 64
	}
}

func usage(w io.Writer) {
	fmt.Fprintln(w, "usage: provider-recon validate|schema|collect|restore|serve [flags]")
}

func cmdValidate(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dir := fs.String("definitions", "definitions", "definitions directory")
	if err := fs.Parse(args); err != nil {
		return 64
	}
	defs, err := runner.LoadDefinitions(*dir, registry)
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
	store, err := openStore(ctx, *outDir)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	var providers []string
	if *only != "" {
		providers = []string{*only}
	}
	cfg := runner.Config{DefinitionsDir: *dir, Registry: registry, Fetcher: feeds.NewFetcher(), Store: store, Concurrency: *concurrency, Logger: logger}
	m, err := runner.Collect(ctx, cfg, providers, time.Now().UTC().Truncate(time.Second))
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
	m, n, err := runner.Restore(ctx, runner.Config{Store: store}, *slug, *collector, *since, time.Now().UTC().Truncate(time.Second))
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
