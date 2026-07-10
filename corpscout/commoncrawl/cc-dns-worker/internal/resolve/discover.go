package resolve

import (
	"context"
	"errors"
	"strings"

	"github.com/miekg/dns"
	"golang.org/x/net/publicsuffix"
)

// Delegation is what discovery learned for a domain.
type Delegation struct {
	ETLD  string
	NS    []string
	NSIPs []string // every discovered NS IP, in discovery order, deduped — full evidence, never filtered
	DS    []string

	// DialableNSIPs is the subset of NSIPs classified ScopePublic (see target.go), in the same order,
	// deduped. Tier-2 record queries and AXFR must dial only from this list — NSIPs itself is kept
	// intact as evidence even when some (or all) of it is not safe to dial.
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
	for _, rr := range nsResp.Answer {
		if ns, ok := rr.(*dns.NS); ok {
			del.NS = append(del.NS, strings.ToLower(ns.Ns))
		}
	}
	if len(del.NS) == 0 {
		return del, errors.New("no NS records")
	}

	if dsResp, err := d.query(ctx, fqdn, dns.TypeDS); err == nil && dsResp != nil {
		for _, rr := range dsResp.Answer {
			if ds, ok := rr.(*dns.DS); ok {
				del.DS = append(del.DS, strings.TrimSpace(ds.String()[len(ds.Hdr.String()):]))
			}
		}
	}

	seen := map[string]bool{}
	add := func(ip string) {
		if ip != "" && !seen[ip] {
			seen[ip] = true
			del.NSIPs = append(del.NSIPs, ip) // full evidence: keep every scope
			if scope, ok := ClassifyString(ip); ok && Dialable(scope) {
				del.DialableNSIPs = append(del.DialableNSIPs, ip)
			}
		}
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
					add(a.A.String())
				case *dns.AAAA:
					add(a.AAAA.String())
				}
			}
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
