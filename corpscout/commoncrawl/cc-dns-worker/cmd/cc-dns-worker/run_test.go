package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPruneOldDBs(t *testing.T) {
	dir := t.TempDir()
	names := []string{
		"scan-20260101T000000Z.db", "scan-20260102T000000Z.db",
		"scan-20260103T000000Z.db", "scan-20260104T000000Z.db",
	}
	for _, n := range names {
		_ = os.WriteFile(filepath.Join(dir, n), []byte("x"), 0o644)
		_ = os.WriteFile(filepath.Join(dir, n+"-wal"), []byte("x"), 0o644)
	}
	// Also a non-cycle file that must never be touched.
	_ = os.WriteFile(filepath.Join(dir, "orchestrator-state.json"), []byte("{}"), 0o644)

	pruneOldDBs(dir, 1) // keep newest current + 1 previous = 2

	for _, n := range names[:2] { // two oldest gone (with sidecars)
		if _, err := os.Stat(filepath.Join(dir, n)); !os.IsNotExist(err) {
			t.Errorf("%s should be pruned", n)
		}
		if _, err := os.Stat(filepath.Join(dir, n+"-wal")); !os.IsNotExist(err) {
			t.Errorf("%s-wal should be pruned", n)
		}
	}
	for _, n := range names[2:] { // two newest kept
		if _, err := os.Stat(filepath.Join(dir, n)); err != nil {
			t.Errorf("%s should be kept: %v", n, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "orchestrator-state.json")); err != nil {
		t.Errorf("non-cycle file must not be pruned: %v", err)
	}
}

func TestCycleStateResumeVsNew(t *testing.T) {
	sp := filepath.Join(t.TempDir(), "state.json")

	// No state -> mint a fresh cycle in the SEEDING phase (seed + host-load run before scanning).
	s1, err := loadOrStartCycle(sp)
	if err != nil || s1.Phase != phaseSeeding || s1.CycleID == "" {
		t.Fatalf("new cycle = %+v, err=%v", s1, err)
	}

	// A persisted, not-done cycle -> resume the SAME cycle in its SAME phase (never restart seeding).
	s2, _ := loadOrStartCycle(sp)
	if s2.CycleID != s1.CycleID || s2.Phase != phaseSeeding {
		t.Errorf("resume = %+v, want same cycle as %+v", s2, s1)
	}

	// Advanced to scanning -> a resume preserves the scanning phase (does not fall back to seeding).
	s2.Phase = phaseScanning
	if err := saveState(sp, s2); err != nil {
		t.Fatal(err)
	}
	if s4, _ := loadOrStartCycle(sp); s4.CycleID != s1.CycleID || s4.Phase != phaseScanning {
		t.Errorf("resume of scanning phase = %+v, want same cycle in scanning", s4)
	}

	// Persist as done -> next call mints a NEW cycle that starts back at seeding.
	s1.Phase = phaseDone
	if err := saveState(sp, s1); err != nil {
		t.Fatal(err)
	}
	s3, _ := loadOrStartCycle(sp)
	if s3.Phase != phaseSeeding {
		t.Errorf("after done, expected a fresh seeding cycle, got %+v", s3)
	}
}
