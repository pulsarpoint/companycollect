package matcher

import (
	"fmt"
	"net/netip"

	"github.com/gaissmai/bart"

	"provider_recon/internal/model"
)

// IPEntry is one service registered on a prefix.
type IPEntry struct {
	ProviderSlug string
	ServiceKey   string
	FeedTag      string
	Source       model.Source
	Confidence   float64
}

// IPTable answers longest-prefix lookups over provider documents.
type IPTable struct {
	trie    bart.Table[netip.Prefix]
	entries map[netip.Prefix][]IPEntry
}

// NewIPTable indexes every ip_ranges item of docs. Several services may share
// a prefix; Lookup returns all of them in document order.
func NewIPTable(docs []model.Document) (*IPTable, error) {
	t := &IPTable{entries: map[netip.Prefix][]IPEntry{}}
	for _, doc := range docs {
		for _, svc := range doc.Services {
			for _, r := range svc.Evidence.IPRanges {
				p, err := netip.ParsePrefix(r.CIDR)
				if err != nil {
					return nil, fmt.Errorf("%s %s: %w", doc.Slug, svc.Key, err)
				}
				p = p.Masked()
				if _, seen := t.entries[p]; !seen {
					t.trie.Insert(p, p)
				}
				t.entries[p] = append(t.entries[p], IPEntry{
					ProviderSlug: doc.Slug, ServiceKey: svc.Key, FeedTag: r.FeedTag,
					Source: r.Source, Confidence: r.Confidence,
				})
			}
		}
	}
	return t, nil
}

// Lookup returns the most specific prefix containing addr and its entries.
func (t *IPTable) Lookup(addr netip.Addr) (netip.Prefix, []IPEntry, bool) {
	p, ok := t.trie.Lookup(addr)
	if !ok {
		return netip.Prefix{}, nil, false
	}
	return p, t.entries[p], true
}
