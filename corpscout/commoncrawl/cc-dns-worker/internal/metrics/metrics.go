// Package metrics holds live counters for a scan run and formats the periodic stdout stats line
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
	Queries        atomic.Int64 // DNS queries actually sent — the "traffic generated" indicator
	QueryErrors    atomic.Int64 // queries that failed (transport error / no answer)
	Domains        atomic.Int64 // domains that reached a terminal status this run
	DomainErrors   atomic.Int64 // domains that ended in status=error
	BlockedTargets atomic.Int64 // authoritative dials refused because the target address was not public (see resolve.Dialable)
}

// Snapshot is a point-in-time read of Stats.
type Snapshot struct {
	At             time.Time
	Queries        int64
	QueryErrors    int64
	Domains        int64
	DomainErrors   int64
	BlockedTargets int64
}

// Snapshot reads the counters at time now.
func (s *Stats) Snapshot(now time.Time) Snapshot {
	return Snapshot{
		At:             now,
		Queries:        s.Queries.Load(),
		QueryErrors:    s.QueryErrors.Load(),
		Domains:        s.Domains.Load(),
		DomainErrors:   s.DomainErrors.Load(),
		BlockedTargets: s.BlockedTargets.Load(),
	}
}

// Line formats one stats line: interval rates (cur vs prev) plus cumulative context (since start).
func Line(prev, cur Snapshot, start time.Time) string {
	dt := cur.At.Sub(prev.At).Seconds()
	if dt <= 0 {
		dt = 1
	}
	elapsed := cur.At.Sub(start)
	if elapsed <= 0 {
		elapsed = time.Second
	}
	dps := float64(cur.Domains-prev.Domains) / dt // domains/sec this interval
	qps := float64(cur.Queries-prev.Queries) / dt // queries/sec this interval (traffic)
	avgDps := float64(cur.Domains) / elapsed.Seconds()
	qPerDom := 0.0
	if cur.Domains > 0 {
		qPerDom = float64(cur.Queries) / float64(cur.Domains)
	}
	return fmt.Sprintf(
		"stats: elapsed=%s domains=%d (%.0f/s, avg %.0f/s) queries=%d (%.0f/s, %.1f/domain) err: q=%.1f%% dom=%.1f%% blocked=%d",
		elapsed.Round(time.Second), cur.Domains, dps, avgDps,
		cur.Queries, qps, qPerDom, pct(cur.QueryErrors, cur.Queries), pct(cur.DomainErrors, cur.Domains),
		cur.BlockedTargets, // cumulative: non-dialable authoritative targets refused (see resolve.Dialable)
	)
}

func pct(part, total int64) float64 {
	if total <= 0 {
		return 0
	}
	return 100 * float64(part) / float64(total)
}
