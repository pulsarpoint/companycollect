package metrics

import (
	"strings"
	"testing"
	"time"
)

func TestLineComputesRates(t *testing.T) {
	start := time.Unix(0, 0).UTC()
	cur := Snapshot{
		At:      start.Add(15 * time.Second),
		Queries: 10000, QueryErrors: 200, QueryTimeouts: 50,
		Domains:   1000,
		Records:   18000, // +10000 records in 5s => 2000/s
		DNSChecks: 25000, DNSChecksOK: 24000,
	}
	previous := Snapshot{At: start.Add(10 * time.Second), DNSChecksOK: 19000}
	line := Line(cur, previous, start, 1.25)

	for _, want := range []string{
		"dns=24000/25000",
		"speed=1000.0 records/s",
		"avg=1600.0 records/s",
		"domains=1000",
		"answers=18000",
		"err=2.00%",
		"err10m=1.25%",
		"timeout=0.50%",
	} {
		if !strings.Contains(line, want) {
			t.Errorf("line missing %q\n  got: %s", want, line)
		}
	}
}

func TestLineZeroSafe(t *testing.T) {
	start := time.Unix(0, 0).UTC()
	// No progress yet: no divide-by-zero, percentages 0.
	line := Line(Snapshot{At: start}, Snapshot{At: start}, start, 0)
	if !strings.Contains(line, "dns=0/0") || !strings.Contains(line, "avg=0.0 records/s") {
		t.Errorf("zero snapshot line wrong: %s", line)
	}
}

func TestSnapshotReadsCounters(t *testing.T) {
	var s Stats
	s.Queries.Add(7)
	s.QueryErrors.Add(2)
	s.QueryTimeouts.Add(1)
	s.Domains.Add(3)
	s.Records.Add(11)
	s.DNSChecks.Add(20)
	s.DNSChecksOK.Add(18)
	snap := s.Snapshot(time.Unix(0, 0).UTC())
	if snap.Queries != 7 || snap.QueryErrors != 2 || snap.QueryTimeouts != 1 || snap.Domains != 3 || snap.Records != 11 ||
		snap.DNSChecks != 20 || snap.DNSChecksOK != 18 {
		t.Fatalf("snapshot = %+v", snap)
	}
}
