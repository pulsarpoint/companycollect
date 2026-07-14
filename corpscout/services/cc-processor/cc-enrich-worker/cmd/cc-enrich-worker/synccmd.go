package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/dustin/go-humanize"

	"cc-enrich-worker/internal/catalog"
)

// syncReadyMessage renders the sync-db success line: the resolved local catalog path plus, when the
// file could be stat'd, its human-readable size. Pure so the formatting is unit-testable without a
// live RustFS.
func syncReadyMessage(path string, sizeBytes int64, sized bool) string {
	if sized {
		return fmt.Sprintf("catalog ready: %s (%s)", path, humanize.Bytes(uint64(sizeBytes)))
	}
	return fmt.Sprintf("catalog ready: %s", path)
}

// runSyncDBCmd implements the `sync-db` command: pull the committed WARC catalog from S3/RustFS and
// cache it locally at <base>/<crawl-id>/warc-index/<selection>/catalog.duckdb. It is idempotent —
// catalog.SyncLocal validates the cached SHA and skips the download when the local copy already
// matches the commit — so operators run it once on a freshly provisioned host to pre-warm the ~17 GB
// catalog before the first range run.
func runSyncDBCmd(args []string) {
	fs := flag.NewFlagSet("cc-enrich-worker sync-db", flag.ExitOnError)
	var crawlID, selection, base string
	fs.StringVar(&crawlID, "crawl-id", "", "crawl id, e.g. CC-MAIN-2026-25 (required)")
	fs.StringVar(&selection, "selection", "pages25", "catalog selection identity")
	fs.StringVar(&base, "base", "", "output ROOT (required, explicit — no environment fallback)")
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

	baseAbs, err := filepath.Abs(base)
	if err != nil {
		log.Fatalf("resolve base directory %s: %v", base, err)
	}

	catalogS3Base := strings.TrimSpace(os.Getenv("COMMONCRAWL_CATALOG_S3_BASE"))
	if catalogS3Base == "" {
		log.Fatal("COMMONCRAWL_CATALOG_S3_BASE is required (s3://bucket/prefix)")
	}

	ctx := context.Background()
	path, err := catalog.SyncLocal(
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
	)
	if err != nil {
		log.Fatalf("sync catalog cache: %v", err)
	}

	info, statErr := os.Stat(path)
	fmt.Println(syncReadyMessage(path, sizeOr(info, statErr), statErr == nil))
}

// sizeOr returns the file size when Stat succeeded, and 0 otherwise (the size is only rendered when
// the stat succeeded, so the fallback value is never printed).
func sizeOr(info os.FileInfo, err error) int64 {
	if err != nil {
		return 0
	}
	return info.Size()
}
