package resolve

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"cc-dns-worker/internal/metrics"
	"cc-dns-worker/internal/model"
	"cc-dns-worker/internal/records"

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
func (r *Resolver) Resolve(ctx context.Context, domain, scanID, runID string, del Delegation, cfg records.Config, now time.Time, extra []model.HostLabel) model.DomainResult {
	res := model.DomainResult{
		ScanID: scanID, RootDomain: domain, ETLD: del.ETLD,
		Nameservers: del.NS, NSIPs: del.NSIPs, Endpoints: del.Endpoints,
		DSPresent: len(del.DS) > 0, Status: "done",
		SourceRunID: runID, ResolvedAt: now,
	}
	for _, ds := range del.DS {
		res.Records = append(res.Records, model.DNSRecord{Name: domain, RecordType: "DS", Slot: "", Value: ds, Rcode: "NOERROR", Source: "query", Discovery: "static"})
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

	for _, o := range out {
		res.QueriesTotal++
		if o.err == nil && o.resp != nil {
			rcode := dns.RcodeToString[o.resp.Rcode]
			res.QueriesOK++
			recs := collect(o.q, o.resp, rcode)
			res.Records = append(res.Records, recs...)
			for _, rec := range recs {
				if rec.RecordType == "DNSKEY" {
					res.DNSSECSigned = true
				}
			}
		}
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
	return nil, lastErr
}

// collect turns one query's ANSWER RRs into DNSRecords, tagging them with the query's slot.
func collect(q records.Query, resp *dns.Msg, rcode string) []model.DNSRecord {
	name := strings.TrimSuffix(q.Name, ".")
	var out []model.DNSRecord
	for _, rr := range resp.Answer {
		rec := model.DNSRecord{Name: name, Slot: q.Slot, Rcode: rcode, TTL: rr.Header().Ttl, Source: "query", Discovery: q.Discovery}
		switch v := rr.(type) {
		case *dns.A:
			rec.RecordType, rec.Value = "A", v.A.String()
		case *dns.AAAA:
			rec.RecordType, rec.Value = "AAAA", v.AAAA.String()
		case *dns.MX:
			// value = full rdata "<pref> <host>" so the ReplacingMergeTree sort key (which includes
			// value but not the priority column) can't collapse two MX at different preferences.
			rec.RecordType = "MX"
			rec.Priority = v.Preference
			rec.Value = strconv.Itoa(int(v.Preference)) + " " + strings.TrimSuffix(strings.ToLower(v.Mx), ".")
		case *dns.NS:
			rec.RecordType, rec.Value = "NS", strings.TrimSuffix(strings.ToLower(v.Ns), ".")
		case *dns.SOA:
			rec.RecordType, rec.Value = "SOA", strings.TrimSpace(v.String()[len(v.Hdr.String()):])
		case *dns.CAA:
			rec.RecordType, rec.Value = "CAA", strings.TrimSpace(v.String()[len(v.Hdr.String()):])
		case *dns.DNSKEY:
			rec.RecordType, rec.Value = "DNSKEY", strings.TrimSpace(v.String()[len(v.Hdr.String()):])
		case *dns.TXT:
			rec.RecordType, rec.Value = "TXT", strings.Join(v.Txt, "")
		case *dns.CNAME:
			rec.RecordType, rec.Value = "CNAME", strings.TrimSuffix(strings.ToLower(v.Target), ".")
		case *dns.SRV:
			// value = full rdata "<pri> <weight> <port> <target>"; SvcPriority also in the priority col.
			rec.RecordType = "SRV"
			rec.Priority = v.Priority
			rec.Value = strings.TrimSpace(v.String()[len(v.Hdr.String()):])
		case *dns.HTTPS:
			rec.RecordType = "HTTPS"
			rec.Priority = v.Priority // SvcPriority (0 = AliasMode)
			rec.Value = strings.TrimSpace(v.String()[len(v.Hdr.String()):])
		default:
			continue
		}
		out = append(out, rec)
	}
	return out
}
