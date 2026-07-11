package main

import (
	"reflect"
	"testing"
)

func TestParseParts(t *testing.T) {
	tests := []struct {
		input string
		want  []int
	}{
		{input: "0", want: []int{0}},
		{input: "7", want: []int{7}},
		{input: "0-3", want: []int{0, 1, 2, 3}},
	}
	for _, test := range tests {
		got, err := parseParts(test.input)
		if err != nil {
			t.Fatalf("parseParts(%q): %v", test.input, err)
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Fatalf("parseParts(%q)=%v, want %v", test.input, got, test.want)
		}
	}
	for _, input := range []string{"", "-1", "3-2", "x", "1-2-3", "0-10001"} {
		if _, err := parseParts(input); err == nil {
			t.Fatalf("parseParts(%q) unexpectedly succeeded", input)
		}
	}
}

func TestParseOptionsUsesDownloaderDefaults(t *testing.T) {
	t.Setenv("OUT_BASE_DIR", "/srv/commoncrawl")
	t.Setenv("CC_DOWNLOAD_PYTHON", "/usr/bin/python3")
	options, err := parseOptions([]string{"--crawl", "CC-MAIN-2026-25", "--parts", "0-10"})
	if err != nil {
		t.Fatal(err)
	}
	if options.baseDirectory != "/srv/commoncrawl" || options.pagesPerDomain != 25 || options.python != "/usr/bin/python3" {
		t.Fatalf("unexpected defaults %+v", options)
	}
	if options.parts != "0-10" || options.worklistDirectory != "" {
		t.Fatalf("unexpected range/worklist options %+v", options)
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
