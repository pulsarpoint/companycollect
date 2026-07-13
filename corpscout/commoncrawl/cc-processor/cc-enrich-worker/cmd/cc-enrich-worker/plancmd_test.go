package main

import (
	"testing"

	"cc-enrich-worker/internal/catalog"
)

// planFixtureStats mirrors the fixture in internal/catalog/classify_test.go:
// part 0 has 3 pages / 600 bytes, part 1 has 1 page / 50 bytes, part 2 is absent, part 3 has
// 5 pages / 150 bytes.
var planFixtureStats = []catalog.PartStats{
	{WarcIndex: 0, Pages: 3, SelectedBytes: 600},
	{WarcIndex: 1, Pages: 1, SelectedBytes: 50},
	{WarcIndex: 3, Pages: 5, SelectedBytes: 150},
}

func TestBuildPlanReport(t *testing.T) {
	t.Run("counts and byte totals over the range", func(t *testing.T) {
		report := buildPlanReport(planFixtureStats, 0, 3)

		if report.Lo != 0 || report.Hi != 3 {
			t.Fatalf("Lo,Hi = %d,%d, want 0,3", report.Lo, report.Hi)
		}
		// parts 0,1,3 carry selected pages; part 2 is absent (empty).
		if report.Selected != 3 {
			t.Errorf("Selected = %d, want 3", report.Selected)
		}
		if report.Empty != 1 {
			t.Errorf("Empty = %d, want 1", report.Empty)
		}
		if report.TotalPages != 9 { // 3 + 1 + 5
			t.Errorf("TotalPages = %d, want 9", report.TotalPages)
		}
		if report.TotalBytes != 800 { // 600 + 50 + 150
			t.Errorf("TotalBytes = %d, want 800", report.TotalBytes)
		}
	})

	t.Run("all-empty range reports zero selected", func(t *testing.T) {
		report := buildPlanReport(nil, 10, 14)
		if report.Selected != 0 {
			t.Errorf("Selected = %d, want 0", report.Selected)
		}
		if report.Empty != 5 { // 10..14 inclusive
			t.Errorf("Empty = %d, want 5", report.Empty)
		}
		if report.TotalPages != 0 || report.TotalBytes != 0 {
			t.Errorf("pages=%d bytes=%d, want 0,0", report.TotalPages, report.TotalBytes)
		}
	})

	t.Run("String renders part counts and totals", func(t *testing.T) {
		report := buildPlanReport(planFixtureStats, 0, 3)
		if out := report.String(); out == "" {
			t.Fatal("String() returned empty output")
		}
	})
}
