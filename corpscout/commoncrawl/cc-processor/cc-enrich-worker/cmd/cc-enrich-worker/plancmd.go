package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/dustin/go-humanize"

	"cc-enrich-worker/internal/catalog"
	"cc-enrich-worker/internal/warcinput"
)

// planSweepValues are the fixed --remote-max-pages thresholds always shown in a plan report's
// sweep table, regardless of the threshold the operator actually picked.
var planSweepValues = []int64{100, 500, 1000, 2500, 5000, 10000}

// sweepRow is one row of the threshold sweep: at X pages, how many parts land in each lane.
type sweepRow struct {
	X             int64
	Local, Remote int
}

// planReport summarizes lane assignment (local whole-WARC download vs remote range reads) and
// page/byte totals for one WARC part range at one --remote-max-pages threshold, plus a sweep
// showing how the split would look at other thresholds. It is a pure, testable value — no I/O.
type planReport struct {
	Lo, Hi                  uint32
	Local, Remote, Empty    int
	LocalPages, RemotePages int64
	LocalBytes, RemoteBytes int64
	Sweep                   []sweepRow // X in {100, 500, 1000, 2500, 5000, 10000} plus the flag value
}

// buildPlanReport classifies [lo,hi] at threshold x (catalog.ClassifyParts split semantics:
// Pages <= x -> remote, Pages > x -> local, no stats row -> empty) and computes a sweep over the
// fixed planSweepValues plus x itself, deduplicated and sorted ascending.
func buildPlanReport(stats []catalog.PartStats, lo, hi uint32, x int64) planReport {
	byIndex := make(map[uint32]catalog.PartStats, len(stats))
	for _, stat := range stats {
		byIndex[stat.WarcIndex] = stat
	}

	classification := catalog.ClassifyParts(stats, lo, hi, x)
	report := planReport{
		Lo:     lo,
		Hi:     hi,
		Local:  len(classification.Local),
		Remote: len(classification.Remote),
		Empty:  len(classification.Empty),
	}
	for _, index := range classification.Local {
		report.LocalPages += byIndex[index].Pages
		report.LocalBytes += byIndex[index].SelectedBytes
	}
	for _, index := range classification.Remote {
		report.RemotePages += byIndex[index].Pages
		report.RemoteBytes += byIndex[index].SelectedBytes
	}

	sweepXs := sweepThresholds(x)
	report.Sweep = make([]sweepRow, 0, len(sweepXs))
	for _, sweepX := range sweepXs {
		c := catalog.ClassifyParts(stats, lo, hi, sweepX)
		report.Sweep = append(report.Sweep, sweepRow{X: sweepX, Local: len(c.Local), Remote: len(c.Remote)})
	}
	return report
}

// sweepThresholds returns planSweepValues plus x, deduplicated and sorted ascending.
func sweepThresholds(x int64) []int64 {
	all := append(append([]int64(nil), planSweepValues...), x)
	seen := make(map[int64]struct{}, len(all))
	values := make([]int64, 0, len(all))
	for _, v := range all {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		values = append(values, v)
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return values
}

const bytesPerWholeWARCDownload = 1 << 30 // ~1 GiB — approximate uncompressed WARC part size

// String renders a human-readable plan report: lane totals, then a download-size estimate
// (local parts x ~1 GiB), then the threshold sweep table.
func (r planReport) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "plan: parts [%d,%d]\n", r.Lo, r.Hi)
	fmt.Fprintf(&b, "  local:  %d parts, %d pages, %s\n", r.Local, r.LocalPages, humanize.Bytes(uint64(r.LocalBytes)))
	fmt.Fprintf(&b, "  remote: %d parts, %d pages, %s\n", r.Remote, r.RemotePages, humanize.Bytes(uint64(r.RemoteBytes)))
	fmt.Fprintf(&b, "  empty:  %d parts\n", r.Empty)
	fmt.Fprintf(&b, "  download estimate: %d local parts x ~1 GiB = ~%s\n",
		r.Local, humanize.Bytes(uint64(r.Local)*bytesPerWholeWARCDownload))
	b.WriteString("\n  threshold sweep (--remote-max-pages -> local/remote parts):\n")
	for _, row := range r.Sweep {
		fmt.Fprintf(&b, "    X=%-8d local=%-6d remote=%-6d\n", row.X, row.Local, row.Remote)
	}
	return b.String()
}

// runPlanCmd implements the `plan` command: read-only reporting of how a WARC part range would
// split across the local (whole-WARC download) and remote (range read) lanes at a given
// --remote-max-pages threshold, plus a sweep across other thresholds. It never writes parquet,
// markers, or ClickHouse data — it only reads the catalog.
func runPlanCmd(args []string) {
	fs := flag.NewFlagSet("cc-enrich-worker plan", flag.ExitOnError)
	var crawlID, selection, base, partsFlag string
	var remoteMaxPages int64
	fs.StringVar(&crawlID, "crawl-id", "", "crawl id, e.g. CC-MAIN-2026-25 (required)")
	fs.StringVar(&selection, "selection", "pages25", "catalog selection identity")
	fs.StringVar(&base, "base", os.Getenv("OUT_BASE_DIR"), "output ROOT (or env OUT_BASE_DIR); required")
	fs.StringVar(&partsFlag, "parts", "", `WARC part range, e.g. "100-200" or "42" (required)`)
	fs.Int64Var(&remoteMaxPages, "remote-max-pages", 0, "split threshold: parts with <= this many selected pages are remote-eligible (required, >=1)")
	_ = fs.Parse(args)

	if crawlID == "" {
		fmt.Fprintln(os.Stderr, "error: --crawl-id is required")
		fs.Usage()
		os.Exit(2)
	}
	if base == "" {
		fmt.Fprintln(os.Stderr, "error: no --base / OUT_BASE_DIR — the catalog root is required")
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
	if remoteMaxPages < 1 {
		fmt.Fprintln(os.Stderr, "error: --remote-max-pages must be >= 1")
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
		if planErr != nil && !strings.Contains(planErr.Error(), "is absent from the catalog") {
			log.Fatalf("sync catalog cache: %v", planErr)
		}
	}

	catalogPath := filepath.Join(baseAbs, crawlID, "warc-index", selection, "catalog.duckdb")
	stats, err := catalog.LoadPartStats(ctx, catalogPath, parts.lo, parts.hi)
	if err != nil {
		log.Fatalf("load part stats from %s: %v", catalogPath, err)
	}

	report := buildPlanReport(stats, parts.lo, parts.hi, remoteMaxPages)
	fmt.Print(report.String())
}
