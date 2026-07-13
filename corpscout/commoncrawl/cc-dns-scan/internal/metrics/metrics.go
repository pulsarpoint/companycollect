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
	BlockedTargets atomic.Int64 // authoritative dials refused because the target address was not public (see resolve.Dialable)
}

// Snapshot is a point-in-time read of Stats.
type Snapshot struct {
	At             time.Time
	Queries        int64
	QueryErrors    int64
	BlockedTargets int64
}

// Snapshot reads the counters at time now.
func (s *Stats) Snapshot(now time.Time) Snapshot {
	return Snapshot{
		At:             now,
		Queries:        s.Queries.Load(),
		QueryErrors:    s.QueryErrors.Load(),
		BlockedTargets: s.BlockedTargets.Load(),
	}
}

// Line formats live sent-query and real-error throughput for the latest reporting interval.
func Line(cur, previous Snapshot) string {
	interval := cur.At.Sub(previous.At).Seconds()
	queriesPerSecond := 0.0
	errorsPerSecond := 0.0
	if interval > 0 {
		queriesPerSecond = float64(cur.Queries-previous.Queries) / interval
		errorsPerSecond = float64(cur.QueryErrors-previous.QueryErrors) / interval
	}
	return fmt.Sprintf("stats qps=%.1f errps=%.1f", queriesPerSecond, errorsPerSecond)
}
