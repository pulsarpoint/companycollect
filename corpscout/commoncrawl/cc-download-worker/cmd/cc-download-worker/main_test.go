package main

import "testing"

func TestParseOptionsUsesDownloaderDefaults(t *testing.T) {
	t.Setenv("OUT_BASE_DIR", "/srv/commoncrawl")
	t.Setenv("CC_DOWNLOAD_PYTHON", "/usr/bin/python3")
	options, err := parseOptions([]string{"--crawl", "CC-MAIN-2026-25", "--parts", "0-10"})
	if err != nil {
		t.Fatal(err)
	}
	if options.baseDirectory != "/srv/commoncrawl" || options.pagesPerDomain != 25 || options.recordAttempts != 3 || options.python != "/usr/bin/python3" {
		t.Fatalf("unexpected defaults %+v", options)
	}
	if options.parts != "0-10" || options.worklistDirectory != "" {
		t.Fatalf("unexpected range/worklist options %+v", options)
	}
}

func TestParseOptionsRejectsInvalidRecordAttempts(t *testing.T) {
	for _, attempts := range []string{"0", "11"} {
		_, err := parseOptions([]string{
			"--crawl", "CC-MAIN-2026-25", "--parts", "0",
			"--record-attempts", attempts,
		})
		if err == nil {
			t.Fatalf("record-attempts=%s unexpectedly succeeded", attempts)
		}
	}
}

func TestParseOptionsRejectsRemovedPublicFlags(t *testing.T) {
	_, err := parseOptions([]string{
		"--crawl", "CC-MAIN-2026-25", "--parts", "0",
		"--worklist", "old.parquet",
	})
	if err == nil {
		t.Fatal("removed --worklist flag unexpectedly succeeded")
	}
}
