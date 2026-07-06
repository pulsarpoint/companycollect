package metrics

import (
	"strings"
	"testing"
	"time"
)

func TestLineComputesRates(t *testing.T) {
	start := time.Unix(0, 0).UTC()
	prev := Snapshot{At: start.Add(10 * time.Second), Queries: 5000, Domains: 400}
	cur := Snapshot{
		At: start.Add(15 * time.Second), // 5s interval
		Queries: 10000, QueryErrors: 200, // +5000 queries in 5s => 1000/s ; 2% err
		Domains: 1000, DomainErrors: 50, // +600 domains in 5s => 120/s ; 5% err
	}
	line := Line(prev, cur, start)

	for _, want := range []string{
		"elapsed=15s",
		"domains=1000",
		"120/s",          // interval domains/sec (600/5)
		"avg 67/s",       // cumulative: 1000/15 = 66.7 -> 67
		"queries=10000",
		"1000/s",         // interval queries/sec (5000/5)
		"10.0/domain",    // 10000/1000
		"q=2.0%",
		"dom=5.0%",
	} {
		if !strings.Contains(line, want) {
			t.Errorf("line missing %q\n  got: %s", want, line)
		}
	}
}

func TestLineZeroSafe(t *testing.T) {
	start := time.Unix(0, 0).UTC()
	// No progress yet: no divide-by-zero, percentages 0.
	line := Line(Snapshot{At: start}, Snapshot{At: start}, start)
	if !strings.Contains(line, "domains=0") || !strings.Contains(line, "q=0.0%") {
		t.Errorf("zero snapshot line wrong: %s", line)
	}
}

func TestSnapshotReadsCounters(t *testing.T) {
	var s Stats
	s.Queries.Add(7)
	s.Domains.Add(3)
	s.DomainErrors.Add(1)
	snap := s.Snapshot(time.Unix(0, 0).UTC())
	if snap.Queries != 7 || snap.Domains != 3 || snap.DomainErrors != 1 {
		t.Fatalf("snapshot = %+v", snap)
	}
}
