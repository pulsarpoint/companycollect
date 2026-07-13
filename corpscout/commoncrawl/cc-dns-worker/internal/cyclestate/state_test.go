package cyclestate

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadOrStartResumesExistingCycle(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state.json")
	first, resumed, err := LoadOrStart(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if first.CycleID == "" || resumed {
		t.Fatalf("first cycle = %+v, resumed = %t", first, resumed)
	}

	second, resumed, err := LoadOrStart(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if second != first || !resumed {
		t.Fatalf("second cycle = %+v, resumed = %t, want %+v and resumed", second, resumed, first)
	}
}

func TestLoadOrStartReplacesInvalidState(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(statePath, []byte(`{"cycle_id":""}`), 0o644); err != nil {
		t.Fatal(err)
	}

	state, resumed, err := LoadOrStart(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if state.CycleID == "" || resumed {
		t.Fatalf("cycle = %+v, resumed = %t", state, resumed)
	}
}

func TestDatabaseNameSeparatesComponents(t *testing.T) {
	if DatabaseName("dns", "cycle") == DatabaseName("axfr", "cycle") {
		t.Fatal("DNS and AXFR cycle databases must be different")
	}
	if got := DatabaseName("dns", "cycle"); got != "dns-scan-cycle.db" {
		t.Fatalf("DatabaseName = %q, want %q", got, "dns-scan-cycle.db")
	}
}

func TestRemoveFilesRemovesSQLiteAndStateFiles(t *testing.T) {
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "scan.db")
	statePath := filepath.Join(directory, "state.json")
	paths := []string{databasePath, databasePath + "-wal", databasePath + "-shm", statePath}
	for _, path := range paths {
		if err := os.WriteFile(path, nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	RemoveFiles(databasePath, statePath)
	for _, path := range paths {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("%s still exists or cannot be inspected: %v", path, err)
		}
	}
}
