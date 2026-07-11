package metrics

import (
	"strings"
	"testing"
	"time"
)

func TestLineComputesRates(t *testing.T) {
	start := time.Unix(0, 0).UTC()
	prev := Snapshot{At: start.Add(10 * time.Second), Queries: 5000, Domains: 400, Records: 8000}
	cur := Snapshot{
		At:      start.Add(15 * time.Second), // 5s interval
		Queries: 10000, QueryErrors: 200,     // +5000 queries in 5s => 1000/s, 2% err
		Domains: 1000, DomainErrors: 50, // +600 domains in 5s => 120/s, 5% err
		Records: 18000, // +10000 records in 5s => 2000/s
	}
	line := Line(prev, cur)

	for _, want := range []string{
		"domains=1000",
		"records=18000",
		"dns_err=2.00%",
		"axfr_try=0",
		"axfr_ok=0",
		"dps=120",
	} {
		if !strings.Contains(line, want) {
			t.Errorf("line missing %q\n  got: %s", want, line)
		}
	}
}

func TestLineZeroSafe(t *testing.T) {
	start := time.Unix(0, 0).UTC()
	// No progress yet: no divide-by-zero, percentages 0.
	line := Line(Snapshot{At: start}, Snapshot{At: start})
	if !strings.Contains(line, "domains=0") || !strings.Contains(line, "dns_err=0.00%") {
		t.Errorf("zero snapshot line wrong: %s", line)
	}
}

func TestSnapshotReadsCounters(t *testing.T) {
	var s Stats
	s.Queries.Add(7)
	s.Domains.Add(3)
	s.DomainErrors.Add(1)
	s.Records.Add(11)
	s.AXFRPullsTried.Add(5)
	s.AXFRPullsSuccessful.Add(2)
	snap := s.Snapshot(time.Unix(0, 0).UTC())
	if snap.Queries != 7 || snap.Domains != 3 || snap.DomainErrors != 1 || snap.Records != 11 ||
		snap.AXFRPullsTried != 5 || snap.AXFRPullsSuccessful != 2 {
		t.Fatalf("snapshot = %+v", snap)
	}
}
