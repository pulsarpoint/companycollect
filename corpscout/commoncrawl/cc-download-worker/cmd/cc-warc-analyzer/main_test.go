package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestParseThresholds(t *testing.T) {
	thresholds, err := parseThresholds("10, 25,75")
	if err != nil {
		t.Fatal(err)
	}
	if len(thresholds) != 3 || thresholds[0] != 10 || thresholds[1] != 25 || thresholds[2] != 75 {
		t.Fatalf("unexpected thresholds %v", thresholds)
	}
	for _, value := range []string{"", "-1", "101", "ten", "NaN", "+Inf"} {
		if _, err := parseThresholds(value); err == nil {
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
