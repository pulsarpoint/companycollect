package metrics

import (
	"strings"
	"testing"
	"time"
)

func TestLineReportsLatestQueryAndErrorRates(t *testing.T) {
	start := time.Unix(0, 0).UTC()
	previous := Snapshot{At: start, Queries: 1000, QueryErrors: 20}
	current := Snapshot{At: start.Add(5 * time.Second), Queries: 6000, QueryErrors: 70}
	line := Line(current, previous)

	for _, want := range []string{"qps=1000.0", "errps=10.0"} {
		if !strings.Contains(line, want) {
			t.Errorf("line missing %q: %s", want, line)
		}
	}
}

func TestLineHandlesZeroInterval(t *testing.T) {
	now := time.Unix(0, 0).UTC()
	line := Line(Snapshot{At: now}, Snapshot{At: now})
	if line != "stats qps=0.0 errps=0.0" {
		t.Fatalf("line = %q", line)
	}
}

func TestSnapshotReadsNetworkCounters(t *testing.T) {
	var stats Stats
	stats.Queries.Add(7)
	stats.QueryErrors.Add(2)
	stats.BlockedTargets.Add(3)
	snapshot := stats.Snapshot(time.Unix(0, 0).UTC())
	if snapshot.Queries != 7 || snapshot.QueryErrors != 2 || snapshot.BlockedTargets != 3 {
		t.Fatalf("snapshot = %+v", snapshot)
	}
}
