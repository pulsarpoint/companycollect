package worklistbuilder

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"
)

type testWorklistRow struct {
	RootDomain       string  `parquet:"root_domain"`
	URL              string  `parquet:"url"`
	WARCFilename     string  `parquet:"warc_filename"`
	WARCRecordOffset int64   `parquet:"warc_record_offset"`
	WARCRecordLength int64   `parquet:"warc_record_length"`
	ContentLanguages *string `parquet:"content_languages,optional"`
}

func TestDerivedWorklistIdentity(t *testing.T) {
	if got := Selection(1); got != "pages1" {
		t.Fatalf("Selection(1)=%q", got)
	}
	if got := Selection(25); got != "pages25" {
		t.Fatalf("Selection(25)=%q", got)
	}
	directory := DefaultDirectory("/data", "CC-MAIN-2026-25", 25)
	if want := filepath.Join("/data", "CC-MAIN-2026-25", "download", "worklists", "pages25"); directory != want {
		t.Fatalf("directory=%q, want %q", directory, want)
	}
	if got := Path(directory, 7); got != filepath.Join(directory, "part_007.parquet") {
		t.Fatalf("path=%q", got)
	}
	if got := Key(25, 7); got != "download/worklists/pages25/part_007.parquet" {
		t.Fatalf("key=%q", got)
	}
}

func TestEnsureReusesValidCachedWorklist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "part_000.parquet")
	rows := []testWorklistRow{{
		RootDomain:       "example.com",
		URL:              "https://example.com/",
		WARCFilename:     "crawl-data/example.warc.gz",
		WARCRecordOffset: 100,
		WARCRecordLength: 200,
	}}
	if err := parquet.WriteFile(path, rows); err != nil {
		t.Fatal(err)
	}
	result, err := Ensure(context.Background(), Config{
		Python:         "does-not-exist",
		CrawlID:        "CC-MAIN-2026-25",
		PagesPerDomain: 25,
		Part:           0,
		OutputPath:     path,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Reused || result.Rows != 1 || result.Path != path {
		t.Fatalf("unexpected result %+v", result)
	}
}

func TestEnsureRejectsInvalidConfiguration(t *testing.T) {
	_, err := Ensure(context.Background(), Config{Python: "python3", CrawlID: "crawl", PagesPerDomain: 0, OutputPath: "x"})
	if err == nil {
		t.Fatal("expected pages-per-domain validation error")
	}
}

func TestTransientBuildFailureClassification(t *testing.T) {
	for _, output := range []string{
		"HTTP Error: HTTP GET error (HTTP 503 Service Unavailable)",
		"connection reset by peer",
		"request timed out",
	} {
		if !isTransientBuildFailure([]byte(output)) {
			t.Fatalf("transient output was not classified: %q", output)
		}
	}
	for _, output := range []string{
		"HTTP Error: HTTP 404 Not Found",
		"Binder Error: missing column",
	} {
		if isTransientBuildFailure([]byte(output)) {
			t.Fatalf("permanent output was classified as transient: %q", output)
		}
	}
}
