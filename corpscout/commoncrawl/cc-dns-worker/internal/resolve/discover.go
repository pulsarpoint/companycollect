package resolve

import (
	"context"
	"errors"
	"net/netip"
	"strings"

	"cc-dns-worker/internal/model"

	"github.com/miekg/dns"
	"golang.org/x/net/publicsuffix"
)

// Outcome values (Task 9) describe whether a query that backs a stored boolean (DSPresent/
// DNSSECSigned) reached a DEFINITIVE answer, distinguishing a genuine negative from "we don't know":
//   - OutcomePresent: the record(s) were returned.
//   - OutcomeAbsent: a definitive NOERROR/NODATA or NXDOMAIN with no matching records — the
//     authoritative side answered, it just has nothing.
//   - OutcomeUnknown: the query itself never got a definitive answer (timeout, transport error,
//     SERVFAIL, or every retry attempt exhausted). The corresponding boolean must NOT be treated as a
//     confirmed false in this case — see model.DomainResult's DSOutcome/DNSKEYOutcome doc comment.
const (
	OutcomePresent = "present"
	OutcomeAbsent  = "absent"
	OutcomeUnknown = "unknown"
)

// Delegation is what discovery learned for a domain.
type Delegation struct {
	ETLD string
	DS   []string

	// DSOutcome is the tri-state outcome of the parent DS query above: OutcomePresent when DS holds at
	// least one record, OutcomeAbsent for a definitive NOERROR/NODATA or NXDOMAIN with no DS records, or
	// OutcomeUnknown when the DS query itself failed (see the Outcome* doc comment). DiscoverNS always
	// sets this to one of the three; it is empty only on a Delegation built by hand (e.g. a test
	// fixture) rather than through DiscoverNS.
	DSOutcome string

	// NS holds every authoritative NS hostname discovered in the NS RRset, lowercased/no trailing dot,
	// deduped in discovery order — kept even for a hostname that resolves to no A/AAAA at all, since
	// that absence is itself evidence (an NS name that glue can't reach). It is therefore populated
	// straight from the wire rather than being restricted to the hostnames that made it into Endpoints.
	NS []string

	// Endpoints is the (ns-hostname, ip) pairing discovery observed: one entry per distinct pair, in
	// discovery order, IP in canonical string form (see model.NameserverEndpoint). It is the source of
	// truth for hostname<->IP identity — NSIPs and DialableNSIPs below are both pure views derived from
	// it, so they can never disagree with Endpoints about which addresses exist or are dialable.
	Endpoints []model.NameserverEndpoint

	// NSIPs is every distinct Endpoints IP, in discovery order — full evidence, never filtered.
	NSIPs []string

	// DialableNSIPs is the NSIPs subset classified ScopePublic (see target.go), in the same order.
	// Tier-2 record queries and AXFR must dial only from this list — NSIPs itself is kept intact as
	// evidence even when some (or all) of it is not safe to dial.
	DialableNSIPs []string
}

// ErrNoPublicNSEndpoints is returned by DiscoverNS when NS resolution succeeded (NSIPs is non-empty)
// but none of the discovered addresses are publicly dialable — e.g. a delegation whose glue records
// only resolve to RFC1918/loopback/link-local addresses. It is distinct from "no NS IPs resolved"
// (a discovery/transport failure) so callers can tell "we found NS IPs but can't safely reach any of
// them" apart from "we found nothing at all".
var ErrNoPublicNSEndpoints = errors.New("no public NS endpoints")

// Discoverer finds a domain's authoritative NS (+ IPs) and parent DS via recursive resolvers. It
// does NOT walk roots: the recursive resolver's cache absorbs the root/TLD load (polite + fast) and
// transparently resolves cross-TLD / glue-less nameservers. Record queries (query.go) then go
// directly to the discovered NS IPs.
type Discoverer struct {
	Ex        Exchanger
	Resolvers []string
}

// NewDiscoverer returns a Discoverer over the given recursive resolvers. resolvers must be non-empty
// — there is no public-resolver default; NS discovery is meant to run against a local recursive
// resolver (e.g. unbound / PowerDNS Recursor at 127.0.0.1:53). The caller validates non-emptiness.
func NewDiscoverer(ex Exchanger, resolvers []string) *Discoverer {
	return &Discoverer{Ex: ex, Resolvers: append([]string(nil), resolvers...)}
}

// DiscoverNS resolves NS names, their IPs, and the parent DS for a domain (no trailing dot).
func (d *Discoverer) DiscoverNS(ctx context.Context, domain string) (Delegation, error) {
	etld, _ := publicsuffix.PublicSuffix(domain)
	del := Delegation{ETLD: etld}
	fqdn := dns.Fqdn(domain)

	nsResp, err := d.query(ctx, fqdn, dns.TypeNS)
	if err != nil {
		return del, err
	}
	seenNS := map[string]bool{}
	for _, rr := range nsResp.Answer {
		if ns, ok := rr.(*dns.NS); ok {
			name := strings.ToLower(strings.TrimSuffix(ns.Ns, "."))
			if !seenNS[name] {
				seenNS[name] = true
				del.NS = append(del.NS, name)
			}
		}
	}
	if len(del.NS) == 0 {
		return del, errors.New("no NS records")
	}

	if dsResp, dsErr := d.query(ctx, fqdn, dns.TypeDS); dsErr == nil && dsResp != nil {
		for _, rr := range dsResp.Answer {
			if ds, ok := rr.(*dns.DS); ok {
				del.DS = append(del.DS, strings.TrimSpace(ds.String()[len(ds.Hdr.String()):]))
			}
		}
		// The query itself got a definitive answer either way — NOERROR/NODATA and NXDOMAIN both mean
		// "asked and there is no DS", distinguishable from OutcomeUnknown below (the query failed, so we
		// never actually learned whether DS exists).
		if len(del.DS) > 0 {
			del.DSOutcome = OutcomePresent
		} else {
			del.DSOutcome = OutcomeAbsent
		}
	} else {
		del.DSOutcome = OutcomeUnknown
	}

	// Record WHICH address belongs to WHICH NS hostname: one Endpoint per distinct (ns-hostname, ip)
	// pair, so one NS name with several IPs yields several endpoints, and several NS names sharing one
	// IP (e.g. anycast) yield several endpoints too — never collapsed into a single flat address.
	seenEndpoint := map[string]bool{} // "name\x00canonical-ip"
	addEndpoint := func(ns, rawIP string) {
		addr, perr := netip.ParseAddr(rawIP)
		if perr != nil {
			return // A/AAAA rdata should always parse; never propagate a malformed address as evidence
		}
		ip := addr.String() // canonical textual form: lowercase hex, standard zero-run compression, etc.
		key := ns + "\x00" + ip
		if seenEndpoint[key] {
			return
		}
		seenEndpoint[key] = true
		scope := ClassifyAddr(addr)
		del.Endpoints = append(del.Endpoints, model.NameserverEndpoint{
			Name: ns, IP: ip, Scope: string(scope), Dialable: Dialable(scope),
		})
	}
	for _, ns := range del.NS {
		for _, qt := range []uint16{dns.TypeA, dns.TypeAAAA} {
			resp, err := d.query(ctx, dns.Fqdn(ns), qt)
			if err != nil || resp == nil {
				continue
			}
			for _, rr := range resp.Answer {
				switch a := rr.(type) {
				case *dns.A:
					addEndpoint(ns, a.A.String())
				case *dns.AAAA:
					addEndpoint(ns, a.AAAA.String())
				}
			}
		}
	}

	// NSIPs/DialableNSIPs are pure views over Endpoints, deduped by IP alone (an IP shared by multiple
	// NS names still counts once), in first-seen order — so they can never drift from what Endpoints
	// actually recorded.
	seenIP := map[string]bool{}
	for _, ep := range del.Endpoints {
		if seenIP[ep.IP] {
			continue
		}
		seenIP[ep.IP] = true
		del.NSIPs = append(del.NSIPs, ep.IP) // full evidence: keep every scope
		if ep.Dialable {
			del.DialableNSIPs = append(del.DialableNSIPs, ep.IP)
		}
	}
	if len(del.NSIPs) == 0 {
		return del, errors.New("no NS IPs resolved")
	}
	if len(del.DialableNSIPs) == 0 {
		return del, ErrNoPublicNSEndpoints // evidence (NSIPs) preserved on this path too
	}
	return del, nil
}

// query sends a recursive (RD=1) query, rotating across resolvers with 2 attempts each. SetQuestion
// sets RecursionDesired=true by default, which is what we want here.
func (d *Discoverer) query(ctx context.Context, name string, qtype uint16) (*dns.Msg, error) {
	var lastErr error
	for _, srv := range d.Resolvers {
		for attempt := 0; attempt < 2; attempt++ {
			m := new(dns.Msg)
			m.SetQuestion(name, qtype)
			resp, err := d.Ex.Exchange(ctx, m, srv)
			if err == nil && resp != nil && resp.Rcode != dns.RcodeServerFailure {
				return resp, nil
			}
			lastErr = err
		}
	}
	if lastErr == nil {
		lastErr = errors.New("all resolvers failed")
	}
	return nil, lastErr
}
