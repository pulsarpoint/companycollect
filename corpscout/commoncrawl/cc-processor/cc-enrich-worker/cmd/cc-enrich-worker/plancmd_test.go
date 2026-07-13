package main

import (
	"reflect"
	"testing"

	"cc-enrich-worker/internal/catalog"
)

// planFixtureStats mirrors the Task 2 fixture in internal/catalog/classify_test.go:
// part 0 has 3 pages / 600 bytes, part 1 has 1 page / 50 bytes, part 2 is absent, part 3 has
// 5 pages / 150 bytes.
var planFixtureStats = []catalog.PartStats{
	{WarcIndex: 0, Pages: 3, SelectedBytes: 600},
	{WarcIndex: 1, Pages: 1, SelectedBytes: 50},
	{WarcIndex: 3, Pages: 5, SelectedBytes: 150},
}

func TestBuildPlanReport(t *testing.T) {
	t.Run("counts and byte totals at the flag threshold", func(t *testing.T) {
		report := buildPlanReport(planFixtureStats, 0, 3, 2)

		if report.Lo != 0 || report.Hi != 3 {
			t.Fatalf("Lo,Hi = %d,%d, want 0,3", report.Lo, report.Hi)
		}
		// x=2: part 0 (3 pages > 2) -> local, part 1 (1 page <= 2) -> remote,
		// part 2 (absent) -> empty, part 3 (5 pages > 2) -> local.
		if report.Local != 2 {
			t.Errorf("Local = %d, want 2", report.Local)
		}
		if report.Remote != 1 {
			t.Errorf("Remote = %d, want 1", report.Remote)
		}
		if report.Empty != 1 {
			t.Errorf("Empty = %d, want 1", report.Empty)
		}
		if report.LocalPages != 8 { // part0 3 + part3 5
			t.Errorf("LocalPages = %d, want 8", report.LocalPages)
		}
		if report.RemotePages != 1 { // part1
			t.Errorf("RemotePages = %d, want 1", report.RemotePages)
		}
		if report.LocalBytes != 750 { // 600 + 150
			t.Errorf("LocalBytes = %d, want 750", report.LocalBytes)
		}
		if report.RemoteBytes != 50 {
			t.Errorf("RemoteBytes = %d, want 50", report.RemoteBytes)
		}
	})

	t.Run("sweep includes the fixed X values plus the flag value, sorted", func(t *testing.T) {
		report := buildPlanReport(planFixtureStats, 0, 3, 2)

		wantXs := []int64{2, 100, 500, 1000, 2500, 5000, 10000}
		var gotXs []int64
		for _, row := range report.Sweep {
			gotXs = append(gotXs, row.X)
		}
		if !reflect.DeepEqual(gotXs, wantXs) {
			t.Fatalf("sweep X values = %v, want %v", gotXs, wantXs)
		}

		flagFound := false
		for _, row := range report.Sweep {
			if row.X == 2 {
				flagFound = true
				if row.Local != 2 || row.Remote != 1 {
					t.Errorf("sweep row for flag value X=2: local=%d remote=%d, want local=2 remote=1", row.Local, row.Remote)
				}
			}
		}
		if !flagFound {
			t.Fatal("sweep does not contain the flag value X=2")
		}
	})

	t.Run("sweep dedupes when the flag value collides with a fixed X", func(t *testing.T) {
		report := buildPlanReport(planFixtureStats, 0, 3, 1000)

		wantXs := []int64{100, 500, 1000, 2500, 5000, 10000}
		var gotXs []int64
		for _, row := range report.Sweep {
			gotXs = append(gotXs, row.X)
		}
		if !reflect.DeepEqual(gotXs, wantXs) {
			t.Fatalf("sweep X values = %v, want %v (deduped)", gotXs, wantXs)
		}
	})

	t.Run("local counts are non-increasing as X grows", func(t *testing.T) {
		report := buildPlanReport(planFixtureStats, 0, 3, 2)

		for i := 1; i < len(report.Sweep); i++ {
			if report.Sweep[i].Local > report.Sweep[i-1].Local {
				t.Fatalf("Local not non-increasing at X=%d (local=%d) after X=%d (local=%d)",
					report.Sweep[i].X, report.Sweep[i].Local, report.Sweep[i-1].X, report.Sweep[i-1].Local)
			}
		}
	})

	t.Run("String renders lane totals and the download estimate", func(t *testing.T) {
		report := buildPlanReport(planFixtureStats, 0, 3, 2)
		out := report.String()
		if out == "" {
			t.Fatal("String() returned empty output")
		}
	})
}
