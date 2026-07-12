package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"cc-download-worker/planstore"
	"github.com/parquet-go/parquet-go"
)

type analyzerWorklistRow struct {
	RootDomain       string `parquet:"root_domain"`
	URL              string `parquet:"url"`
	WARCFilename     string `parquet:"warc_filename"`
	WARCRecordOffset int64  `parquet:"warc_record_offset"`
	WARCRecordLength int64  `parquet:"warc_record_length"`
}

func TestParsePercentages(t *testing.T) {
	thresholds, err := parsePercentages("10, 25,75")
	if err != nil {
		t.Fatal(err)
	}
	if len(thresholds) != 3 || thresholds[0] != 10 || thresholds[1] != 25 || thresholds[2] != 75 {
		t.Fatalf("unexpected thresholds %v", thresholds)
	}
	for _, value := range []string{"", "-1", "101", "ten", "NaN", "+Inf"} {
		if _, err := parsePercentages(value); err == nil {
			t.Fatalf("thresholds %q unexpectedly succeeded", value)
		}
	}
}

func TestFilterLoadedParts(t *testing.T) {
	base := t.TempDir()
	markerDirectory := filepath.Join(base, "CC-MAIN-2026-25", "crawl")
	if err := os.MkdirAll(markerDirectory, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(markerDirectory, "out_tech_86.loaded"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	pending, loaded, err := filterLoadedParts(base, "CC-MAIN-2026-25", "tech", []int{85, 86, 87})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(pending, []int{85, 87}) || !reflect.DeepEqual(loaded, []int{86}) {
		t.Fatalf("pending=%v loaded=%v", pending, loaded)
	}
}

func TestRunCreatesSQLitePlanFromCachedWorklist(t *testing.T) {
	base := t.TempDir()
	crawlID := "CC-MAIN-2026-25"
	worklistDirectory := filepath.Join(base, crawlID, "download", "worklists", "pages25")
	if err := os.MkdirAll(worklistDirectory, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := parquet.WriteFile(filepath.Join(worklistDirectory, "part_000.parquet"), []analyzerWorklistRow{
		{RootDomain: "a.example", URL: "https://a.example/", WARCFilename: "a.warc.gz", WARCRecordOffset: 0, WARCRecordLength: 100},
		{RootDomain: "b.example", URL: "https://b.example/", WARCFilename: "b.warc.gz", WARCRecordOffset: 100, WARCRecordLength: 200},
	}); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	if err := run(context.Background(), logger, []string{
		"--base", base,
		"--crawl", crawlID,
		"--parts", "0",
		"--skip-loaded=false",
		"--whole-warc-sizes=false",
	}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(base, crawlID, "download", "plans", "pages25", "tech_parts_0.sqlite")
	store, err := planstore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	stats, err := store.Stats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Parts != 1 || stats.Pages != 2 || stats.PendingPages != 2 || stats.ExactWARCObjects != 2 {
		t.Fatalf("unexpected SQLite plan stats %+v", stats)
	}
	var checkOutput bytes.Buffer
	if err := run(context.Background(), slog.New(slog.NewJSONHandler(&checkOutput, nil)), []string{
		"--base", base,
		"--crawl", crawlID,
		"--parts", "0",
		"--check",
	}); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{
		`"msg":"SQLite WARC plan stats"`,
		`"whole_warc_objects":0`,
		`"whole_warc_pages":0`,
		`"individual_pages":2`,
		`"msg":"SQLite WARC plan check complete"`,
	} {
		if !strings.Contains(checkOutput.String(), field) {
			t.Fatalf("check output is missing %q: %s", field, checkOutput.String())
		}
	}
}
