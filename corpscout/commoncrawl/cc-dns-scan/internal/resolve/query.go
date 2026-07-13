package resolve

import (
	"context"
	"fmt"
	"sync"
	"time"

	"cc-dns-scan/internal/metrics"
	"cc-dns-scan/internal/model"
	"cc-dns-scan/internal/records"

	"github.com/miekg/dns"
)

// Resolver runs Tier-2 record queries directly against a domain's authoritative NS IPs.
type Resolver struct {
	Ex Exchanger

	// Stats is optional; queryAuth's defense-in-depth reclassification increments
	// Stats.BlockedTargets whenever it refuses to dial a non-public target.
	Stats *metrics.Stats
}

// NewResolver wraps an Exchanger (configured with the authoritative-NS scheduler).
func NewResolver(ex Exchanger) *Resolver { return NewResolverWithStats(ex, nil) }

// NewResolverWithStats is NewResolver plus a metrics counter: every authoritative dial that queryAuth's
// defense-in-depth reclassification refuses (target not ScopePublic) increments stats.BlockedTargets.
// stats may be nil (no metrics).
func NewResolverWithStats(ex Exchanger, stats *metrics.Stats) *Resolver {
	return &Resolver{Ex: ex, Stats: stats}
}

// Resolve runs every Tier-2 query for a domain directly against its authoritative NS IPs, rotating
// across them with retry, and assembles a DomainResult. Delegation must already be discovered. extra
// is the domain's discovered hostnames (CT/registry/axfr), unioned into the plan alongside cfg's
// static hostname list.
//
// # Status bar (Task 9)
//
// Resolve sets Status to "done" or "partial" — it never returns "error" (that status is reserved for
// a discovery failure, set by the caller before Resolve is even invoked; see e.g. cmd/cc-dns-scan's
// resolveDomain). The bar for "done" is: every query that backs one of the summary's boolean/tri-state
// fields reached a DEFINITIVE authoritative answer this scan —
//   - at least one of the apex A/AAAA queries (Slot "@"),
//   - the DNSKEY query (backs DNSSECSigned/DNSKEYOutcome),
//   - the DS outcome from discovery (backs DSPresent/DSOutcome; del.DSOutcome == "" — a Delegation not
//     produced by DiscoverNS, e.g. a test fixture — does not gate the bar, unlike a real "unknown").
//
// Falling short of that bar (but having reached authoritative contact at all, i.e. observed at least
// one record) yields "partial": every record actually observed this scan is still fully populated in
// Records — nothing is held back — but the caller must not let a partial result's zero-value booleans
// overwrite a previously-established "done" summary (see store.CommitBatch / model.ScanRow, which
// enforce this by only ever persisting a summary row for Status == "done").
func (r *Resolver) Resolve(ctx context.Context, domain, scanID, runID string, del Delegation, cfg records.Config, now time.Time, extra []model.HostLabel) model.DomainResult {
	res := model.DomainResult{
		ScanID: scanID, RootDomain: domain, ETLD: del.ETLD,
		Nameservers: del.NS, NSIPs: del.NSIPs, Endpoints: del.Endpoints,
		DSPresent: del.DSOutcome == OutcomePresent, DSOutcome: del.DSOutcome,
		SourceRunID: runID, ResolvedAt: now,
	}
	if len(del.DSRecords) > 0 {
		res.Records = append(res.Records, del.DSRecords...)
	} else {
		// Compatibility for Delegation values built by callers/tests rather than DiscoverNS.
		for _, ds := range del.DS {
			rr, err := dns.NewRR(dns.Fqdn(domain) + " 0 IN DS " + ds)
			if err == nil {
				res.Records = append(res.Records, recordFromRR(rr, "", "NOERROR", "query", "static"))
			}
		}
	}

	// Fire the plan's queries CONCURRENTLY instead of one-at-a-time. Each still passes through the
	// per-server scheduler (rate + in-flight), so a shared authoritative server stays paced — but a
	// domain no longer serializes 60+ round-trips end to end. Critically, on a rate-limited provider
	// (e.g. many domains behind one registrar's anycast NS), serial queries made every domain advance
	// one token at a time and finish in lockstep after a long freeze; firing them up front lets each
	// domain complete as its own queries drain, so throughput is steady instead of stalled. Results
	// are assembled after the barrier, so no shared state is mutated concurrently.
	servers := del.DialableNSIPs // NSIPs is full evidence; only the public subset may be dialed
	plan := records.Plan(domain, cfg, extra)
	type qResult struct {
		q    records.Query
		resp *dns.Msg
		err  error
	}
	out := make([]qResult, len(plan))
	var wg sync.WaitGroup
	for idx := range plan {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			resp, err := r.queryAuth(ctx, plan[idx], servers, idx)
			out[idx] = qResult{plan[idx], resp, err}
		}(idx)
	}
	wg.Wait()

	var apexAnswered bool
	res.DNSKEYOutcome = OutcomeUnknown // overwritten below once the plan's one DNSKEY query is processed
	for _, o := range out {
		res.QueriesTotal++
		isApexAddr := o.q.Slot == "@" && (o.q.Type == dns.TypeA || o.q.Type == dns.TypeAAAA)
		isDNSKEY := o.q.Slot == "" && o.q.Type == dns.TypeDNSKEY
		if o.err != nil || o.resp == nil {
			continue // not definitive: exhausted retries / timeout / SERVFAIL — leave outcome unresolved
		}
		res.QueriesOK++
		rcode := dns.RcodeToString[o.resp.Rcode]
		recs := collect(o.q, o.resp, rcode)
		res.Records = append(res.Records, recs...)
		if isApexAddr {
			apexAnswered = true
		}
		if isDNSKEY {
			hasKey := false
			for _, rec := range recs {
				if rec.RecordType == "DNSKEY" {
					hasKey = true
					break
				}
			}
			if hasKey {
				res.DNSSECSigned = true
				res.DNSKEYOutcome = OutcomePresent
			} else {
				res.DNSKEYOutcome = OutcomeAbsent
			}
		}
	}

	res.Status = model.DomainStatusDone
	if !apexAnswered || res.DNSKEYOutcome == OutcomeUnknown || res.DSOutcome == OutcomeUnknown {
		res.Status = model.DomainStatusPartial
	}
	return res
}

// queryAuth sends one authoritative query (RecursionDesired=false), rotating across the domain's NS
// IPs so each is tried once per pass and twice overall; the per-server limiter spaces retries.
func (r *Resolver) queryAuth(ctx context.Context, q records.Query, servers []string, start int) (*dns.Msg, error) {
	var lastErr error
	for attempt := 0; attempt < len(servers)*2; attempt++ {
		server := servers[(start+attempt)%len(servers)]
		// Defense-in-depth: servers is meant to already be DialableNSIPs (see Resolve), but reclassify
		// every dial here too so a future caller that hands queryAuth an unfiltered server list (e.g.
		// raw NSIPs) can never slip a private/loopback/etc. address past the public-only invariant.
		if scope, ok := ClassifyString(server); !ok || !Dialable(scope) {
			if r.Stats != nil {
				r.Stats.BlockedTargets.Add(1)
			}
			lastErr = fmt.Errorf("target %s not dialable (scope=%q)", server, scope)
			continue
		}
		m := new(dns.Msg)
		m.SetQuestion(q.Name, q.Type)
		m.RecursionDesired = false // authoritative servers don't recurse
		resp, err := r.Ex.Exchange(ctx, m, server)
		if err == nil && resp != nil && resp.Rcode != dns.RcodeServerFailure {
			return resp, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		// Every attempt returned a non-transport-error, non-nil response that was nonetheless
		// rejected (a SERVFAIL) — err was nil on that attempt, so without this the exhausted sequence
		// would silently return (nil, nil) instead of a diagnosable error.
		lastErr = fmt.Errorf("no dialable server gave a usable answer for %s %s", q.Name, dns.TypeToString[q.Type])
	}
	return nil, lastErr
}

// addressFinding classifies an A/AAAA rdata value and returns "public_dns_private_address" (Task 9)
// when it is anything other than ScopePublic, empty otherwise. Every A/AAAA value collect() sees was
// returned by a server queryAuth actually dialed — which only ever dials ScopePublic addresses (see
// queryAuth's own defense-in-depth check) — so a non-public VALUE here means a public authoritative
// server answered with a bogus/internal/documentation/etc. address, worth flagging even though the
// record is still stored verbatim like any other (see model.DNSRecord.Finding). This never feeds a
// dial target; it is purely a stored classification of an already-observed value.
func addressFinding(value string) string {
	if scope, ok := ClassifyString(value); ok && scope != ScopePublic {
		return "public_dns_private_address"
	}
	return ""
}

// collect turns one query's ANSWER RRs into DNSRecords, tagging them with the query's slot.
func collect(q records.Query, resp *dns.Msg, rcode string) []model.DNSRecord {
	out := make([]model.DNSRecord, 0, len(resp.Answer))
	for _, rr := range resp.Answer {
		out = append(out, recordFromRR(rr, q.Slot, rcode, "query", q.Discovery))
	}
	return out
}
