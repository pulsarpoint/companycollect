// Package knowledge compiles provider-recon documents into the immutable index
// the resolver's analyzers query. Analyzers get it injected (as the Knowledge
// interface) and never load anything themselves, so each can be tested with a
// fake. Patterns go through provider-recon's matcher, so the definitions
// validator and the resolver share one implementation of rule semantics.
package knowledge

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/netip"
	"slices"
	"strings"

	"github.com/gaissmai/bart"

	"provider_recon/internal/matcher"
	"provider_recon/internal/model"
)

// Kind names what a rule is matched against: a record type plus a field. It
// is the model's kind, shared with the definitions validator.
type Kind = model.DNSRuleKind

// Rule kinds (spec: "Rule kinds"). Compilation refuses any kind outside
// model.DNSRuleKinds, so a rule no analyzer handles is rejected at load time.
var (
	NSTarget     = Kind{RecordType: "NS", MatchField: "target"}
	MXTarget     = Kind{RecordType: "MX", MatchField: "target"}
	CNAMETarget  = Kind{RecordType: "CNAME", MatchField: "target"}
	TXTValue     = Kind{RecordType: "TXT", MatchField: "value"}
	TXTName      = Kind{RecordType: "TXT", MatchField: "name"}
	SPFInclude   = Kind{RecordType: "SPF", MatchField: "include"}
	DKIMSelector = Kind{RecordType: "DKIM", MatchField: "selector"}
	DKIMTarget   = Kind{RecordType: "DKIM", MatchField: "target"}
	DMARCReport  = Kind{RecordType: "DMARC", MatchField: "report"}
)

// Match is a rule that matched: the provider service it names.
type Match struct {
	ProviderSlug string
	ServiceKey   string
	ServiceTypes []string
	RuleID       string
	Confidence   float64
}

// Provider is a named provider-recon provider with its live services.
type Provider struct {
	Slug     string
	Name     string
	Country  string
	Services []ProviderService
}

// ProviderService is one live service of a provider, in document order.
type ProviderService struct {
	Key   string
	Types []string
}

// FirstService is the provider's first live service of serviceType, or "".
func (p Provider) FirstService(serviceType string) string {
	for _, s := range p.Services {
		if slices.Contains(s.Types, serviceType) {
			return s.Key
		}
	}
	return ""
}

// IPRange is one provider range instance with the window it was valid in.
// From and To are dates (YYYY-MM-DD); "" leaves that side open. From is open
// for ranges first seen on the timeline's first day (nothing earlier is
// known); To is open while the range is active, else its last_seen.
type IPRange struct {
	Prefix       netip.Prefix
	ProviderSlug string
	ServiceKey   string
	ServiceTypes []string
	FeedTag      string
	Confidence   float64
	From         string
	To           string
	RuleID       string
}

// Knowledge is what analyzers may ask.
type Knowledge interface {
	// LookupIP returns every range whose prefix contains p, longest prefix
	// first, then by rule id.
	LookupIP(p netip.Prefix) []IPRange
	// Match returns the best rule of kind matching subject: highest priority,
	// then confidence, then rule id.
	Match(kind Kind, subject string) (Match, bool)
	// ProviderForKey returns the provider owning a registrable domain, by an
	// exact provider key or a glob key such as "awsdns-*".
	ProviderForKey(key string) (Provider, bool)
	// Version identifies the provider content the index was built from.
	Version() string
}

type compiledRule struct {
	Match
	pattern  matcher.Pattern
	priority int
}

type globKey struct {
	pattern  matcher.Pattern
	raw      string
	provider *Provider
}

// Index is the compiled Knowledge.
type Index struct {
	rules     map[Kind][]compiledRule
	exactKeys map[string]*Provider
	globKeys  []globKey
	providers []*Provider
	ipTrie    bart.Table[netip.Prefix]
	ipRanges  map[netip.Prefix][]IPRange

	rulesVersion, ipVersion, version string
}

var _ Knowledge = (*Index)(nil)

// Compile builds the index. It fails on a document of another contract
// version, a rule of an unknown kind, an invalid pattern, or a provider key
// claimed by two providers. Removed rules and services are skipped.
//
// The version hashes exactly what the index uses: live services with their
// types, provider keys, active rules with priority and confidence, and IP
// range instances with their windows. Daily last_seen ticks of active ranges
// don't move it, so results are only re-resolved when they could change.
func Compile(docs []model.Document) (*Index, error) {
	idx := &Index{rules: map[Kind][]compiledRule{}, exactKeys: map[string]*Provider{}, ipRanges: map[netip.Prefix][]IPRange{}}
	var used, ipUsed []string
	for _, d := range docs {
		if d.Version != model.ContractVersion {
			return nil, fmt.Errorf("provider %q: contract %q, want %q", d.Slug, d.Version, model.ContractVersion)
		}
		if err := idx.add(d); err != nil {
			return nil, err
		}
	}
	if err := idx.addRanges(docs); err != nil {
		return nil, err
	}
	for _, rs := range idx.ipRanges {
		for _, r := range rs {
			ipUsed = append(ipUsed, fmt.Sprintf("ip %s|%s|%s|%g|%s", r.RuleID, r.From, r.To, r.Confidence, strings.Join(r.ServiceTypes, ",")))
		}
	}
	for kind, rules := range idx.rules {
		for _, r := range rules {
			used = append(used, fmt.Sprintf("rule %s|%s|%d|%g|%s", kind, r.RuleID, r.priority, r.Confidence, strings.Join(r.ServiceTypes, ",")))
		}
	}
	for key, p := range idx.exactKeys {
		used = append(used, "key "+key+"|"+p.Slug)
	}
	for _, g := range idx.globKeys {
		used = append(used, "glob "+g.raw+"|"+g.provider.Slug)
	}
	for _, p := range idx.providers {
		for _, s := range p.Services {
			used = append(used, fmt.Sprintf("service %s|%s|%s", p.Slug, s.Key, strings.Join(s.Types, ",")))
		}
	}
	for kind := range idx.rules {
		slices.SortFunc(idx.rules[kind], func(a, b compiledRule) int { return strings.Compare(a.RuleID, b.RuleID) })
	}
	slices.SortFunc(idx.globKeys, func(a, b globKey) int { return strings.Compare(a.raw, b.raw) })
	idx.rulesVersion = digest(used)
	idx.ipVersion = digest(ipUsed)
	idx.version = digest([]string{idx.rulesVersion, idx.ipVersion})
	return idx, nil
}

func (idx *Index) add(d model.Document) error {
	p := &Provider{Slug: d.Slug, Name: d.DisplayName, Country: d.Country}
	idx.providers = append(idx.providers, p)
	for _, s := range d.Services {
		if s.RemovedAt != "" {
			continue
		}
		p.Services = append(p.Services, ProviderService{Key: s.Key, Types: s.ServiceTypes})
		for _, r := range s.Evidence.DNSRules {
			if r.Status == model.StatusRemoved {
				continue
			}
			kind := Kind{RecordType: strings.ToUpper(r.RecordType), MatchField: strings.ToLower(r.MatchField)}
			if !model.IsDNSRuleKind(kind.RecordType, kind.MatchField) {
				return fmt.Errorf("provider %q service %q: unsupported rule kind %s", d.Slug, s.Key, kind)
			}
			if strings.EqualFold(strings.TrimSpace(r.MatcherType), "exists") {
				return fmt.Errorf("provider %q service %q: exists is not meaningful for dns rules", d.Slug, s.Key)
			}
			id := fmt.Sprintf("%s/%s/%s %s %s", d.Slug, s.Key, kind, r.MatcherType, r.Pattern)
			pat, err := matcher.Compile(r.MatcherType, r.Pattern, r.CaseSensitive)
			if err != nil {
				return fmt.Errorf("rule %s: %w", id, err)
			}
			idx.rules[kind] = append(idx.rules[kind], compiledRule{
				Match:    Match{ProviderSlug: d.Slug, ServiceKey: s.Key, ServiceTypes: s.ServiceTypes, RuleID: id, Confidence: r.Confidence},
				pattern:  pat,
				priority: r.Priority,
			})
		}
	}
	for _, k := range d.ProviderKeys {
		k = strings.ToLower(strings.TrimSpace(k))
		if strings.Contains(k, "*") {
			pat, err := matcher.Compile("glob", k, false)
			if err != nil {
				return fmt.Errorf("provider %q: key %q: %w", d.Slug, k, err)
			}
			idx.globKeys = append(idx.globKeys, globKey{pattern: pat, raw: k, provider: p})
			continue
		}
		if other, dup := idx.exactKeys[k]; dup && other.Slug != d.Slug {
			return fmt.Errorf("provider key %q claimed by both %q and %q", k, other.Slug, d.Slug)
		}
		idx.exactKeys[k] = p
	}
	return nil
}

// Match implements Knowledge.
func (idx *Index) Match(kind Kind, subject string) (Match, bool) {
	var best *compiledRule
	for i := range idx.rules[kind] {
		r := &idx.rules[kind][i]
		if !r.pattern.Match(subject) {
			continue
		}
		if best == nil || r.priority > best.priority || (r.priority == best.priority && r.Confidence > best.Confidence) {
			best = r
		}
	}
	if best == nil {
		return Match{}, false
	}
	return best.Match, true
}

// ProviderForKey implements Knowledge.
func (idx *Index) ProviderForKey(key string) (Provider, bool) {
	key = strings.ToLower(key)
	if p, ok := idx.exactKeys[key]; ok {
		return *p, true
	}
	for _, g := range idx.globKeys {
		if g.pattern.Match(key) {
			return *g.provider, true
		}
	}
	return Provider{}, false
}

// Version implements Knowledge: RulesVersion and IPVersion combined.
func (idx *Index) Version() string { return idx.version }

// RulesVersion hashes services, provider keys and active DNS rules. A change
// can alter any record's results.
func (idx *Index) RulesVersion() string { return idx.rulesVersion }

// IPVersion hashes the IP range instances with their windows. A change can
// only alter results of records resolved against the range index (A, AAAA,
// SPF ip4/ip6).
func (idx *Index) IPVersion() string { return idx.ipVersion }

// digest is the sha256 of the sorted lines.
func digest(lines []string) string {
	lines = slices.Clone(lines)
	slices.Sort(lines)
	sum := sha256.Sum256([]byte(strings.Join(lines, "\n")))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// addRanges indexes every range instance of every service, removed ones too:
// they are the history that older records are matched against.
//
// A range's validity opens at the beginning of time when it was first seen on
// its own feed's first run (the earliest first_seen of that provider and
// collector): nothing earlier is known about that feed, whenever it was added.
func (idx *Index) addRanges(docs []model.Document) error {
	feed := func(slug string, r model.IPRange) string { return slug + "|" + r.Collector }
	firstRun := map[string]string{}
	for _, d := range docs {
		for _, s := range d.Services {
			for _, r := range s.Evidence.IPRanges {
				k := feed(d.Slug, r)
				if r.FirstSeen != "" && (firstRun[k] == "" || r.FirstSeen < firstRun[k]) {
					firstRun[k] = r.FirstSeen
				}
			}
		}
	}
	for _, d := range docs {
		for _, s := range d.Services {
			for _, r := range s.Evidence.IPRanges {
				p, err := netip.ParsePrefix(r.CIDR)
				if err != nil {
					return fmt.Errorf("provider %q service %q: range %q: %w", d.Slug, s.Key, r.CIDR, err)
				}
				p = p.Masked()
				e := IPRange{Prefix: p, ProviderSlug: d.Slug, ServiceKey: s.Key, ServiceTypes: s.ServiceTypes, FeedTag: r.FeedTag,
					Confidence: r.Confidence, From: r.FirstSeen, RuleID: fmt.Sprintf("%s/%s/IP %s", d.Slug, s.Key, p)}
				if e.From == firstRun[feed(d.Slug, r)] {
					e.From = ""
				}
				if r.Status != model.StatusActive {
					e.To = r.LastSeen
				}
				if _, seen := idx.ipRanges[p]; !seen {
					idx.ipTrie.Insert(p, p)
				}
				idx.ipRanges[p] = append(idx.ipRanges[p], e)
			}
		}
	}
	return nil
}

// LookupIP implements Knowledge.
func (idx *Index) LookupIP(p netip.Prefix) []IPRange {
	var out []IPRange
	for sp := range idx.ipTrie.Supernets(p.Masked()) {
		out = append(out, idx.ipRanges[sp]...)
	}
	slices.SortFunc(out, func(a, b IPRange) int {
		return cmp.Or(cmp.Compare(b.Prefix.Bits(), a.Prefix.Bits()), cmp.Compare(a.RuleID, b.RuleID), cmp.Compare(a.From, b.From))
	})
	return out
}
