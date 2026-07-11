package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCycleStateResumesUntilRemoved(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state.json")
	first, err := loadOrStartCycle(statePath, "dns")
	if err != nil || first.CycleID == "" {
		t.Fatalf("first cycle = %+v, err = %v", first, err)
	}
	resumed, err := loadOrStartCycle(statePath, "dns")
	if err != nil || resumed.CycleID != first.CycleID {
		t.Fatalf("resumed cycle = %+v, err = %v, want %+v", resumed, err, first)
	}
	if err := os.Remove(statePath); err != nil {
		t.Fatal(err)
	}
	newCycle, err := loadOrStartCycle(statePath, "dns")
	if err != nil || newCycle.CycleID == "" {
		t.Fatalf("new cycle = %+v, err = %v", newCycle, err)
	}
}

func TestScannersUseDifferentCycleDatabases(t *testing.T) {
	if cycleDBName("dns", "cycle") == cycleDBName("axfr", "cycle") {
		t.Fatal("DNS and AXFR cycle databases must be different")
	}
}
