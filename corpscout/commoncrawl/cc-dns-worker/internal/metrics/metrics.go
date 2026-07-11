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
	Queries atomic.Int64 // DNS queries actually sent — the "traffic generated" indicator
	// QueryErrors counts attempts that produced no usable answer: a transport error/timeout, OR a
	// well-formed SERVFAIL response (Task 9 — SERVFAIL is not a Go error but every caller retries it
	// exactly like one; see resolve.client.Exchange). Paired 1:1 with Queries at the same per-attempt
	// granularity, so pct(QueryErrors, Queries) is a meaningful per-attempt error rate.
	QueryErrors         atomic.Int64
	Domains             atomic.Int64 // domains that reached a terminal status this run
	DomainErrors        atomic.Int64 // domains that ended in status=error
	Records             atomic.Int64 // DNS records observed across completed domains
	BlockedTargets      atomic.Int64 // authoritative dials refused because the target address was not public (see resolve.Dialable)
	AXFRPullsTried      atomic.Int64 // actual unique nameserver-IP pulls attempted per domain
	AXFRPullsSuccessful atomic.Int64 // open AXFR pulls collected without truncation
}

// Snapshot is a point-in-time read of Stats.
type Snapshot struct {
	At                  time.Time
	Queries             int64
	QueryErrors         int64
	Domains             int64
	DomainErrors        int64
	Records             int64
	BlockedTargets      int64
	AXFRPullsTried      int64
	AXFRPullsSuccessful int64
}

// Snapshot reads the counters at time now.
func (s *Stats) Snapshot(now time.Time) Snapshot {
	return Snapshot{
		At:                  now,
		Queries:             s.Queries.Load(),
		QueryErrors:         s.QueryErrors.Load(),
		Domains:             s.Domains.Load(),
		DomainErrors:        s.DomainErrors.Load(),
		Records:             s.Records.Load(),
		BlockedTargets:      s.BlockedTargets.Load(),
		AXFRPullsTried:      s.AXFRPullsTried.Load(),
		AXFRPullsSuccessful: s.AXFRPullsSuccessful.Load(),
	}
}

// Line formats the compact operator-facing health line. Totals are cumulative for this process;
// domains/sec is the only interval value needed to tell whether scanning is moving.
func Line(prev, cur Snapshot) string {
	dt := cur.At.Sub(prev.At).Seconds()
	if dt <= 0 {
		dt = 1
	}
	dps := float64(cur.Domains-prev.Domains) / dt // domains/sec this interval
	return fmt.Sprintf(
		"stats domains=%d records=%d dns_err=%.2f%% axfr_try=%d axfr_ok=%d speed=%.0f domains/s",
		cur.Domains, cur.Records, pct(cur.QueryErrors, cur.Queries),
		cur.AXFRPullsTried, cur.AXFRPullsSuccessful,
		dps,
	)
}

func pct(part, total int64) float64 {
	if total <= 0 {
		return 0
	}
	return 100 * float64(part) / float64(total)
}
