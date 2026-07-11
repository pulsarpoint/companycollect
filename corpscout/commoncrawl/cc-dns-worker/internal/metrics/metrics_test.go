package metrics

import (
	"strings"
	"testing"
	"time"
)

func TestLineComputesRates(t *testing.T) {
	start := time.Unix(0, 0).UTC()
	cur := Snapshot{
		At:      start.Add(15 * time.Second), // 5s interval
		Queries: 10000, QueryErrors: 200,     // +5000 queries in 5s => 1000/s, 2% err
		Domains: 1000, DomainErrors: 50, // +600 domains in 5s => 120/s, 5% err
		Records:   18000, // +10000 records in 5s => 2000/s
		DNSChecks: 25000, DNSChecksOK: 24000,
	}
	line := Line(cur, start, 1.25)

	for _, want := range []string{
		"dns=24000/25000",
		"avg=1600.0 records/s",
		"domains=1000",
		"answers=18000",
		"err=5.00%",
		"err10m=1.25%",
		"axfr=0/0",
	} {
		if !strings.Contains(line, want) {
			t.Errorf("line missing %q\n  got: %s", want, line)
		}
	}
}

func TestLineZeroSafe(t *testing.T) {
	start := time.Unix(0, 0).UTC()
	// No progress yet: no divide-by-zero, percentages 0.
	line := Line(Snapshot{At: start}, start, 0)
	if !strings.Contains(line, "dns=0/0") || !strings.Contains(line, "avg=0.0 records/s") {
		t.Errorf("zero snapshot line wrong: %s", line)
	}
}

func TestSnapshotReadsCounters(t *testing.T) {
	var s Stats
	s.Queries.Add(7)
	s.Domains.Add(3)
	s.DomainErrors.Add(1)
	s.Records.Add(11)
	s.DNSChecks.Add(20)
	s.DNSChecksOK.Add(18)
	s.AXFRPullsTried.Add(5)
	s.AXFRPullsSuccessful.Add(2)
	snap := s.Snapshot(time.Unix(0, 0).UTC())
	if snap.Queries != 7 || snap.Domains != 3 || snap.DomainErrors != 1 || snap.Records != 11 ||
		snap.DNSChecks != 20 || snap.DNSChecksOK != 18 ||
		snap.AXFRPullsTried != 5 || snap.AXFRPullsSuccessful != 2 {
		t.Fatalf("snapshot = %+v", snap)
	}
}
