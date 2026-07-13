package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dustin/go-humanize"

	"cc-enrich-worker/internal/catalog"
	"cc-enrich-worker/internal/warcinput"
)

// planReport is a sizing summary for one WARC part range: how many parts carry selected pages
// (non-empty), how many are empty, and the page/byte totals across the selected parts. Range reads
// are the only fetch strategy, so there is no lane split — this is a pure, testable value, no I/O.
type planReport struct {
	Lo, Hi     uint32
	Selected   int // parts with at least one selected page
	Empty      int // parts in [lo,hi] with no selected pages
	TotalPages int64
	TotalBytes int64
}

// buildPlanReport tallies selected/empty part counts and page/byte totals for [lo,hi]. LoadPartStats
// returns exactly one row per non-empty part in the range, so len(stats) is the selected count and
// the remaining indices in [lo,hi] are empty.
func buildPlanReport(stats []catalog.PartStats, lo, hi uint32) planReport {
	report := planReport{Lo: lo, Hi: hi, Selected: len(stats)}
	for _, stat := range stats {
		report.TotalPages += stat.Pages
		report.TotalBytes += stat.SelectedBytes
	}
	total := int64(hi) - int64(lo) + 1
	report.Empty = int(total) - len(stats)
	return report
}

// String renders a human-readable sizing report: part counts, page totals/average, byte total.
func (r planReport) String() string {
	avgPages := 0.0
	if r.Selected > 0 {
		avgPages = float64(r.TotalPages) / float64(r.Selected)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "plan: parts [%d,%d]\n", r.Lo, r.Hi)
	fmt.Fprintf(&b, "  selected: %d parts, %d pages (avg %.1f/part), %s\n",
		r.Selected, r.TotalPages, avgPages, humanize.Bytes(uint64(r.TotalBytes)))
	fmt.Fprintf(&b, "  empty:    %d parts\n", r.Empty)
	return b.String()
}

// runPlanCmd implements the `plan` command: a read-only sizing report (part counts, pages, bytes)
// for a WARC part range. It never writes parquet, markers, or ClickHouse data — it only reads the
// catalog.
func runPlanCmd(args []string) {
	fs := flag.NewFlagSet("cc-enrich-worker plan", flag.ExitOnError)
	var crawlID, selection, base, partsFlag string
	fs.StringVar(&crawlID, "crawl-id", "", "crawl id, e.g. CC-MAIN-2026-25 (required)")
	fs.StringVar(&selection, "selection", "pages25", "catalog selection identity")
	fs.StringVar(&base, "base", "", "output ROOT (required, explicit — no environment fallback)")
	fs.StringVar(&partsFlag, "parts", "", `WARC part range, e.g. "100-200" or "42" (required)`)
	_ = fs.Parse(args)

	if crawlID == "" {
		fmt.Fprintln(os.Stderr, "error: --crawl-id is required")
		fs.Usage()
		os.Exit(2)
	}
	if base == "" {
		fmt.Fprintln(os.Stderr, "error: --base is required (catalog root)")
		fs.Usage()
		os.Exit(2)
	}
	if partsFlag == "" {
		fmt.Fprintln(os.Stderr, "error: --parts is required")
		fs.Usage()
		os.Exit(2)
	}
	parts, err := parsePartsRange(partsFlag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: invalid --parts: %v\n", err)
		fs.Usage()
		os.Exit(2)
	}

	baseAbs, err := filepath.Abs(base)
	if err != nil {
		log.Fatalf("resolve base directory %s: %v", base, err)
	}

	ctx := context.Background()
	catalogS3Base := strings.TrimSpace(os.Getenv("COMMONCRAWL_CATALOG_S3_BASE"))
	if catalogS3Base != "" {
		// Mirror run()'s cache-download call (main.go) so `plan` works before any run has ever
		// synced the catalog locally. We only care about the ensureLocalCatalog side effect, not
		// the single-WARC plan it returns, so a "requested index absent from the catalog" error is
		// expected (and harmless) whenever parts.lo itself has no selected pages; any other error
		// (bad credentials, unreachable RustFS, corrupt commit) is fatal.
		_, planErr := warcinput.LoadS3Plan(
			ctx,
			catalog.S3Config{
				BaseURI:   catalogS3Base,
				Endpoint:  os.Getenv("CORPSCOUT_S3_ENDPOINT"),
				Region:    envOr("CORPSCOUT_S3_REGION", "us-east-1"),
				AccessKey: os.Getenv("CORPSCOUT_S3_ACCESS_KEY"),
				SecretKey: os.Getenv("CORPSCOUT_S3_SECRET_KEY"),
			},
			baseAbs,
			crawlID,
			selection,
			parts.lo,
			false,
		)
		if planErr != nil && !errors.Is(planErr, catalog.ErrWARCIndexAbsent) {
			log.Fatalf("sync catalog cache: %v", planErr)
		}
	}

	catalogPath := filepath.Join(baseAbs, crawlID, "warc-index", selection, "catalog.duckdb")
	stats, err := catalog.LoadPartStats(ctx, catalogPath, parts.lo, parts.hi)
	if err != nil {
		log.Fatalf("load part stats from %s: %v", catalogPath, err)
	}

	report := buildPlanReport(stats, parts.lo, parts.hi)
	fmt.Print(report.String())
}
