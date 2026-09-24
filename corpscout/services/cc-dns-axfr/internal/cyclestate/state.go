// Package cyclestate persists the identity of a resumable scanner cycle.
package cyclestate

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type State struct {
	CycleID string `json:"cycle_id"`
}

func LoadOrStart(path string) (State, bool, error) {
	data, err := os.ReadFile(path)
	if err == nil {
		var state State
		if json.Unmarshal(data, &state) == nil && state.CycleID != "" {
			return state, true, nil
		}
	} else if !os.IsNotExist(err) {
		return State{}, false, fmt.Errorf("read cycle state: %w", err)
	}

	state := State{CycleID: time.Now().UTC().Format("20060102T150405Z")}
	if err := save(path, state); err != nil {
		return State{}, false, err
	}
	return state, false, nil
}

func DatabaseName(cycleID string) string {
	return "axfr-scan-" + cycleID + ".db"
}

func RemoveFiles(databasePath, statePath string) {
	for _, suffix := range []string{"", "-wal", "-shm"} {
		_ = os.Remove(databasePath + suffix)
	}
	_ = os.Remove(statePath)
}

func save(path string, state State) error {
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode cycle state: %w", err)
	}
	temporaryPath := path + ".tmp"
	if err := os.WriteFile(temporaryPath, data, 0o644); err != nil {
		return fmt.Errorf("write cycle state: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("replace cycle state: %w", err)
	}
	return nil
}
