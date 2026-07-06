package resolve

import (
	"context"
	"strconv"
	"strings"
	"time"

	"cc-dns-worker/internal/model"
	"cc-dns-worker/internal/records"

	"github.com/miekg/dns"
)

// Resolver runs Tier-2 record queries directly against a domain's authoritative NS IPs.
type Resolver struct{ Ex Exchanger }

// NewResolver wraps an Exchanger (configured with the authoritative-NS scheduler).
func NewResolver(ex Exchanger) *Resolver { return &Resolver{Ex: ex} }

// Resolve runs every Tier-2 query for a domain directly against its authoritative NS IPs, rotating
// across them with retry, and assembles a DomainResult. Delegation must already be discovered.
func (r *Resolver) Resolve(ctx context.Context, domain, scanID, runID string, del Delegation, cfg records.Config, now time.Time) model.DomainResult {
	res := model.DomainResult{
		ScanID: scanID, RootDomain: domain, ETLD: del.ETLD,
		Nameservers: del.NS, NSIPs: del.NSIPs,
		DSPresent: len(del.DS) > 0, Status: "done",
		SourceRunID: runID, ResolvedAt: now,
	}
	for _, ds := range del.DS {
		res.Records = append(res.Records, model.DNSRecord{Name: domain, RecordType: "DS", Slot: "", Value: ds, Rcode: "NOERROR"})
	}

	servers := del.NSIPs
	i := 0
	for _, q := range records.Plan(domain, cfg) {
		res.QueriesTotal++
		resp, err := r.queryAuth(ctx, q, servers, i)
		i++
		rcode := "error"
		if err == nil && resp != nil {
			rcode = dns.RcodeToString[resp.Rcode]
			res.QueriesOK++
			recs := collect(q, resp, rcode)
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
		m := new(dns.Msg)
		m.SetQuestion(q.Name, q.Type)
		m.RecursionDesired = false // authoritative servers don't recurse
		resp, err := r.Ex.Exchange(ctx, m, servers[(start+attempt)%len(servers)])
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
		rec := model.DNSRecord{Name: name, Slot: q.Slot, Rcode: rcode, TTL: rr.Header().Ttl}
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
