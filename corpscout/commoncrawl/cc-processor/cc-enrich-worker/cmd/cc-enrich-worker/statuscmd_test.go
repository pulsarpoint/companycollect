package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cc-enrich-worker/internal/markers"
)

// statusProducedDir creates root/name as a directory and writes its sibling .produced marker
// stamped with cmd and finishedAt.
func statusProducedDir(t *testing.T, root, name, cmd string, finishedAt time.Time) string {
	t.Helper()
	out := filepath.Join(root, name)
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := markers.WriteProduced(out, markers.Produced{
		Cmd:        cmd,
		Rows:       map[string]int{"domains": 1},
		FinishedAt: finishedAt,
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestScanMarkersFindsProducedIgnoresBareDirs builds the fixture tree the task brief calls for: 3
// produced-but-not-loaded dirs (pending), 1 produced-and-loaded dir, and 1 bare directory with no
// marker at all. scanMarkers must find exactly the 4 marked dirs and must not surface the bare one.
func TestScanMarkersFindsProducedIgnoresBareDirs(t *testing.T) {
	root := t.TempDir()
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)

	statusProducedDir(t, root, "out_tech_0", "tech", now.Add(-3*time.Hour))
	statusProducedDir(t, root, "out_tech_1", "tech", now.Add(-1*time.Hour))
	statusProducedDir(t, root, "out_tech_2", "tech", now.Add(-30*time.Minute))

	loadedDir := statusProducedDir(t, root, "out_tech_3", "tech", now.Add(-2*time.Hour))
	if err := markers.WriteLoaded(loadedDir); err != nil {
		t.Fatal(err)
	}

	// A bare output directory: no .produced marker at all -> must be ignored entirely.
	if err := os.MkdirAll(filepath.Join(root, "out_tech_4"), 0o755); err != nil {
		t.Fatal(err)
	}

	entries, unparsable, err := scanMarkers(root)
	if err != nil {
		t.Fatalf("scanMarkers: %v", err)
	}
	if len(unparsable) != 0 {
		t.Fatalf("unparsable = %v, want none", unparsable)
	}
	if len(entries) != 4 {
		t.Fatalf("scanMarkers found %d entries, want 4 (bare dir must be excluded)", len(entries))
	}
	loaded := 0
	for _, e := range entries {
		if e.Cmd != "tech" {
			t.Errorf("entry Cmd = %q, want %q", e.Cmd, "tech")
		}
		if e.Loaded {
			loaded++
		}
	}
	if loaded != 1 {
		t.Errorf("loaded entries = %d, want 1", loaded)
	}
}

// TestScanMarkersSkipsUnparsableMarker: one corrupt .produced file (truncated JSON — e.g. a
// partial rsync copy from a producer host, where the writer's temp+rename atomicity does not
// travel) must NOT abort the scan. The healthy markers are still returned, the corrupt marker's
// path is surfaced in the unparsable list, and the report both counts healthy markers normally
// and mentions the unparsable count in its rendered output.
func TestScanMarkersSkipsUnparsableMarker(t *testing.T) {
	root := t.TempDir()
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)

	statusProducedDir(t, root, "out_tech_0", "tech", now.Add(-1*time.Hour))
	good := statusProducedDir(t, root, "out_tech_1", "tech", now.Add(-2*time.Hour))
	if err := markers.WriteLoaded(good); err != nil {
		t.Fatal(err)
	}

	// Corrupt marker: truncated JSON, exactly as a partial copy would leave it.
	corrupt := filepath.Join(root, "out_tech_2.produced")
	if err := os.WriteFile(corrupt, []byte(`{"part":2,"cmd":"te`), 0o644); err != nil {
		t.Fatal(err)
	}

	entries, unparsable, err := scanMarkers(root)
	if err != nil {
		t.Fatalf("scanMarkers must not fail on one corrupt marker: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("scanMarkers found %d healthy entries, want 2", len(entries))
	}
	if len(unparsable) != 1 || unparsable[0] != corrupt {
		t.Fatalf("unparsable = %v, want [%s]", unparsable, corrupt)
	}

	// The healthy counts still aggregate normally despite the corrupt sibling, and the rendered
	// report surfaces the unparsable count.
	report := buildStatusReport(root, entries, now)
	report.Unparsable = len(unparsable)
	got := report.ByCmd["tech"]
	if got.Produced != 2 || got.Loaded != 1 || got.Pending != 1 {
		t.Errorf("tech counts = %+v, want Produced=2 Loaded=1 Pending=1", got)
	}
	out := report.String()
	if !strings.Contains(out, "unparsable") {
		t.Errorf("String() must surface the unparsable marker count, got:\n%s", out)
	}
}

// TestStatusReportStringOmitsUnparsableWhenZero: a clean tree's report must not mention
// unparsable markers at all.
func TestStatusReportStringOmitsUnparsableWhenZero(t *testing.T) {
	now := time.Now()
	report := buildStatusReport("/root", []markerEntry{
		{Cmd: "tech", FinishedAt: now.Add(-time.Hour), Loaded: false},
	}, now)
	if strings.Contains(report.String(), "unparsable") {
		t.Errorf("String() mentions unparsable markers with none present:\n%s", report.String())
	}
}

// TestBuildStatusReportCounts is the pure-aggregation test: given the same fixture shape as above
// (as markerEntry values, no filesystem involved) it must report produced=4, loaded=1, pending=3,
// and an oldest-pending age computed against the injected `now` — never wall-clock time.Now() —
// so the test cannot flake on scheduling jitter.
func TestBuildStatusReportCounts(t *testing.T) {
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	entries := []markerEntry{
		{Cmd: "tech", FinishedAt: now.Add(-3 * time.Hour), Loaded: false},
		{Cmd: "tech", FinishedAt: now.Add(-1 * time.Hour), Loaded: false},
		{Cmd: "tech", FinishedAt: now.Add(-30 * time.Minute), Loaded: false},
		{Cmd: "tech", FinishedAt: now.Add(-2 * time.Hour), Loaded: true},
	}

	report := buildStatusReport("/fixture/root", entries, now)

	if report.Root != "/fixture/root" {
		t.Errorf("Root = %q, want /fixture/root", report.Root)
	}
	if len(report.Cmds) != 1 || report.Cmds[0] != "tech" {
		t.Fatalf("Cmds = %v, want [tech]", report.Cmds)
	}
	got, ok := report.ByCmd["tech"]
	if !ok {
		t.Fatal(`ByCmd["tech"] missing`)
	}
	if got.Produced != 4 {
		t.Errorf("Produced = %d, want 4", got.Produced)
	}
	if got.Loaded != 1 {
		t.Errorf("Loaded = %d, want 1", got.Loaded)
	}
	if got.Pending != 3 {
		t.Errorf("Pending = %d, want 3", got.Pending)
	}
	// The oldest pending marker finished 3h before now (the -1h and -30m entries are more recent
	// pending markers; the -2h entry is loaded, not pending, so it must not win oldest-pending).
	if got.OldestPendingAge != 3*time.Hour {
		t.Errorf("OldestPendingAge = %v, want %v", got.OldestPendingAge, 3*time.Hour)
	}
}

// TestBuildStatusReportMultipleCmdsSortedAndIndependent verifies per-cmd counts stay isolated
// (a pending "industry" marker must not pollute "tech"'s pending count) and that Cmds is sorted
// for deterministic printing.
func TestBuildStatusReportMultipleCmdsSortedAndIndependent(t *testing.T) {
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	entries := []markerEntry{
		{Cmd: "tech", FinishedAt: now.Add(-1 * time.Hour), Loaded: true},
		{Cmd: "industry", FinishedAt: now.Add(-5 * time.Hour), Loaded: false},
	}

	report := buildStatusReport("/root", entries, now)

	if want := []string{"industry", "tech"}; len(report.Cmds) != 2 || report.Cmds[0] != want[0] || report.Cmds[1] != want[1] {
		t.Fatalf("Cmds = %v, want %v (sorted)", report.Cmds, want)
	}
	tech := report.ByCmd["tech"]
	if tech.Produced != 1 || tech.Loaded != 1 || tech.Pending != 0 {
		t.Errorf("tech counts = %+v, want Produced=1 Loaded=1 Pending=0", tech)
	}
	industry := report.ByCmd["industry"]
	if industry.Produced != 1 || industry.Loaded != 0 || industry.Pending != 1 {
		t.Errorf("industry counts = %+v, want Produced=1 Loaded=0 Pending=1", industry)
	}
	if industry.OldestPendingAge != 5*time.Hour {
		t.Errorf("industry.OldestPendingAge = %v, want 5h", industry.OldestPendingAge)
	}
}

// TestBuildStatusReportNoPendingHasZeroAge covers the all-loaded case: OldestPendingAge must stay
// the zero value when Pending == 0, not some stale/garbage duration.
func TestBuildStatusReportNoPendingHasZeroAge(t *testing.T) {
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	entries := []markerEntry{
		{Cmd: "tech", FinishedAt: now.Add(-1 * time.Hour), Loaded: true},
	}
	report := buildStatusReport("/root", entries, now)
	got := report.ByCmd["tech"]
	if got.Pending != 0 {
		t.Fatalf("Pending = %d, want 0", got.Pending)
	}
	if got.OldestPendingAge != 0 {
		t.Errorf("OldestPendingAge = %v, want 0", got.OldestPendingAge)
	}
}

// TestStatusReportStringRenders is a smoke test on the printed table — it only guards against a
// panic/empty-output regression, matching plancmd_test.go's String() coverage style.
func TestStatusReportStringRenders(t *testing.T) {
	now := time.Now()
	report := buildStatusReport("/some/root", []markerEntry{
		{Cmd: "tech", FinishedAt: now.Add(-time.Hour), Loaded: false},
	}, now)
	out := report.String()
	if out == "" {
		t.Fatal("String() returned empty output")
	}
}

// TestStatusReportStringEmptyRoot covers the no-markers-found path.
func TestStatusReportStringEmptyRoot(t *testing.T) {
	report := buildStatusReport("/empty/root", nil, time.Now())
	if len(report.Cmds) != 0 {
		t.Fatalf("Cmds = %v, want empty", report.Cmds)
	}
	out := report.String()
	if out == "" {
		t.Fatal("String() returned empty output")
	}
}
