package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pulsarpoint/pulsarprotectctlog/internal/loglist"
)

func testCTLogs(ids ...string) []loglist.CTLog {
	out := make([]loglist.CTLog, len(ids))
	for i, id := range ids {
		out[i] = loglist.CTLog{ID: id, End: time.Now().Add(24 * time.Hour)}
	}
	return out
}

func TestDrainAllContinuesPastShardFailure(t *testing.T) {
	ctlogs := testCTLogs("a", "b", "c")
	var drained []string
	err := drainAll(context.Background(), ctlogs,
		func(loglist.CTLog) bool { return true },
		func(c loglist.CTLog, frozen bool) error {
			drained = append(drained, c.ID)
			if c.ID == "b" {
				return fmt.Errorf("fetch tile: %w", context.DeadlineExceeded)
			}
			return nil
		})
	if len(drained) != 3 {
		t.Fatalf("one shard failure must not starve the rest: drained %v", drained)
	}
	if err == nil {
		t.Fatal("a failed shard must surface in the run's exit status, got nil")
	}
}

func TestDrainAllStopsCleanOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ctlogs := testCTLogs("a", "b")
	var drained []string
	err := drainAll(ctx, ctlogs,
		func(loglist.CTLog) bool { return true },
		func(c loglist.CTLog, frozen bool) error {
			drained = append(drained, c.ID)
			cancel() // SIGINT arrives mid-drain of the first shard
			return context.Canceled
		})
	if err != nil {
		t.Fatalf("cancellation is a clean stop, got %v", err)
	}
	if len(drained) != 1 {
		t.Fatalf("cancelled run must not start further shards: drained %v", drained)
	}
}

func TestDrainAllSkipsUnreachable(t *testing.T) {
	ctlogs := testCTLogs("a", "b")
	var drained []string
	err := drainAll(context.Background(), ctlogs,
		func(c loglist.CTLog) bool { return c.ID != "a" },
		func(c loglist.CTLog, frozen bool) error {
			drained = append(drained, c.ID)
			return nil
		})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(drained) != 1 || drained[0] != "b" {
		t.Fatalf("unreachable shard must be skipped: drained %v", drained)
	}
}
