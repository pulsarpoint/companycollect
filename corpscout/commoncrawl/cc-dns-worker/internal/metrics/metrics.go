// Package metrics holds live counters for a DNS scan and formats the periodic stdout stats line
// (throughput + generated DNS traffic). Counters are updated concurrently from many resolver
// goroutines via atomics; the reporter reads Snapshots and formats deltas into rates.
package metrics

import (
	"fmt"
	"sync/atomic"
	"time"
)

// Stats are the live counters for one scan, safe for concurrent updates.
type Stats struct {
	Queries atomic.Int64 // DNS queries actually sent — the "traffic generated" indicator
	// QueryErrors counts sent attempts that failed in transport, timed out, or returned an error RCODE.
	// NOERROR/NODATA and NXDOMAIN are valid outcomes and are excluded. Paired 1:1 with Queries at the
	// same per-attempt granularity, so pct(QueryErrors, Queries) is a meaningful network health rate.
	QueryErrors    atomic.Int64
	QueryTimeouts  atomic.Int64 // QueryErrors caused specifically by a network/context deadline
	Domains        atomic.Int64 // domains that reached a terminal status this run
	Records        atomic.Int64 // DNS records observed across completed domains
	DNSChecks      atomic.Int64 // logical planned DNS checks, excluding retries
	DNSChecksOK    atomic.Int64 // logical checks that reached a definitive DNS response
	BlockedTargets atomic.Int64 // authoritative dials refused because the target address was not public (see resolve.Dialable)
}

// Snapshot is a point-in-time read of Stats.
type Snapshot struct {
	At             time.Time
	Queries        int64
	QueryErrors    int64
	QueryTimeouts  int64
	Domains        int64
	Records        int64
	DNSChecks      int64
	DNSChecksOK    int64
	BlockedTargets int64
}

// Snapshot reads the counters at time now.
func (s *Stats) Snapshot(now time.Time) Snapshot {
	return Snapshot{
		At:             now,
		Queries:        s.Queries.Load(),
		QueryErrors:    s.QueryErrors.Load(),
		QueryTimeouts:  s.QueryTimeouts.Load(),
		Domains:        s.Domains.Load(),
		Records:        s.Records.Load(),
		DNSChecks:      s.DNSChecks.Load(),
		DNSChecksOK:    s.DNSChecksOK.Load(),
		BlockedTargets: s.BlockedTargets.Load(),
	}
}

// Line formats the compact operator-facing health line. Totals and average record throughput are
// cumulative for the scan and can be restored after a process restart.
func Line(cur, previous Snapshot, start time.Time, recentQueryErrorPercent float64) string {
	elapsed := cur.At.Sub(start).Seconds()
	interval := cur.At.Sub(previous.At).Seconds()
	recordsPerSecond := 0.0
	averageRecordsPerSecond := 0.0
	if interval > 0 {
		recordsPerSecond = float64(cur.DNSChecksOK-previous.DNSChecksOK) / interval
	}
	if elapsed > 0 {
		averageRecordsPerSecond = float64(cur.DNSChecksOK) / elapsed
	}
	return fmt.Sprintf(
		"stats dns=%d/%d speed=%.1f records/s avg=%.1f records/s domains=%d answers=%d err=%.2f%% err10m=%.2f%% timeout=%.2f%%",
		cur.DNSChecksOK, cur.DNSChecks, recordsPerSecond, averageRecordsPerSecond, cur.Domains, cur.Records,
		pct(cur.QueryErrors, cur.Queries), recentQueryErrorPercent, pct(cur.QueryTimeouts, cur.Queries),
	)
}

func pct(part, total int64) float64 {
	if total <= 0 {
		return 0
	}
	return 100 * float64(part) / float64(total)
}
