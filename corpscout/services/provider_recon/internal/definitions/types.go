// Package definitions loads and validates the hand-written provider files in
// services/provider_recon/definitions (one provider per <slug>.yaml).
package definitions

import (
	"sort"
	"strings"

	"provider_recon/internal/matcher"
)

// Definition is one provider's curated definition.
type Definition struct {
	Slug         string       `yaml:"slug"`
	DisplayName  string       `yaml:"display_name"`
	Category     string       `yaml:"category"`
	Website      string       `yaml:"website,omitempty"`
	Country      string       `yaml:"country,omitempty"`
	Aliases      []string     `yaml:"aliases,omitempty"`
	ProviderKeys []string     `yaml:"provider_keys,omitempty"`
	Services     []ServiceDef `yaml:"services"`
	Feeds        []FeedRef    `yaml:"feeds,omitempty"`

	// File is the path the definition was loaded from; used in error messages.
	File string `yaml:"-"`
}

// ServiceDef is one service and its curated evidence.
type ServiceDef struct {
	Key                   string            `yaml:"key"`
	DisplayName           string            `yaml:"display_name"`
	ServiceTypes          []string          `yaml:"service_types"`
	Traits                []string          `yaml:"traits,omitempty"`
	Note                  string            `yaml:"note,omitempty"`
	IPRanges              []IPRangeDef      `yaml:"ip_ranges,omitempty"`
	ASNs                  []ASNDef          `yaml:"asns,omitempty"`
	DNSRules              []DNSRuleDef      `yaml:"dns_rules,omitempty"`
	HTTPRules             []HTTPRuleDef     `yaml:"http_rules,omitempty"`
	PTRRules              []PTRRuleDef      `yaml:"ptr_rules,omitempty"`
	CertificateIdentities []CertIdentityDef `yaml:"certificate_identities,omitempty"`
}

// IPRangeDef is a curated address block (for providers without a feed).
type IPRangeDef struct {
	CIDR       string   `yaml:"cidr"`
	Region     string   `yaml:"region,omitempty"`
	Confidence *float64 `yaml:"confidence,omitempty"`
	SourceURL  string   `yaml:"source_url,omitempty"`
	Note       string   `yaml:"note,omitempty"`
}

// ASNDef is a curated autonomous system number.
type ASNDef struct {
	ASN        uint32   `yaml:"asn"`
	Confidence *float64 `yaml:"confidence,omitempty"`
	SourceURL  string   `yaml:"source_url,omitempty"`
	Note       string   `yaml:"note,omitempty"`
}

// DNSRuleDef matches one field of a DNS record.
type DNSRuleDef struct {
	RecordType    string   `yaml:"record_type"`
	MatchField    string   `yaml:"match_field"`
	MatcherType   string   `yaml:"matcher_type"`
	Pattern       string   `yaml:"pattern,omitempty"`
	CaseSensitive bool     `yaml:"case_sensitive,omitempty"`
	Confidence    *float64 `yaml:"confidence,omitempty"`
	Priority      int      `yaml:"priority,omitempty"`
	SourceURL     string   `yaml:"source_url,omitempty"`
	Note          string   `yaml:"note,omitempty"`
}

// HTTPRuleDef matches one part of an HTTP response.
type HTTPRuleDef struct {
	HTTPPart      string   `yaml:"http_part"`
	HeaderName    string   `yaml:"header_name,omitempty"`
	MatcherType   string   `yaml:"matcher_type"`
	Pattern       string   `yaml:"pattern,omitempty"`
	PathScope     string   `yaml:"path_scope,omitempty"`
	CaseSensitive bool     `yaml:"case_sensitive,omitempty"`
	Confidence    *float64 `yaml:"confidence,omitempty"`
	Priority      int      `yaml:"priority,omitempty"`
	SourceURL     string   `yaml:"source_url,omitempty"`
	Note          string   `yaml:"note,omitempty"`
}

// PTRRuleDef matches a reverse-DNS hostname.
type PTRRuleDef struct {
	MatcherType string   `yaml:"matcher_type"`
	Pattern     string   `yaml:"pattern"`
	Confidence  *float64 `yaml:"confidence,omitempty"`
	SourceURL   string   `yaml:"source_url,omitempty"`
	Note        string   `yaml:"note,omitempty"`
}

// CertIdentityDef matches a TLS certificate field.
type CertIdentityDef struct {
	IdentityType  string   `yaml:"identity_type"`
	IdentityValue string   `yaml:"identity_value"`
	Confidence    *float64 `yaml:"confidence,omitempty"`
	SourceURL     string   `yaml:"source_url,omitempty"`
	Note          string   `yaml:"note,omitempty"`
}

// FeedRef attaches a collector's output to this provider's services.
type FeedRef struct {
	Collector string            `yaml:"collector"`
	Params    map[string]string `yaml:"params,omitempty"`
	// TagMap maps feed tags (exact, or globs with '*') to service keys. An
	// empty service key deliberately ignores the tag.
	TagMap map[string]string `yaml:"tag_map"`
	// GenericTags (exact or glob) name umbrella tags whose ranges are dropped
	// when the same CIDR also appears under a specific tag (AWS AMAZON).
	GenericTags []string `yaml:"generic_tags,omitempty"`
}

// ID is the collector name plus sorted params: the key under which the
// collector's status is reported.
func (f FeedRef) ID() string {
	if len(f.Params) == 0 {
		return f.Collector
	}
	keys := make([]string, 0, len(f.Params))
	for k := range f.Params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = k + "=" + f.Params[k]
	}
	return f.Collector + ":" + strings.Join(parts, ",")
}

// ResolveTag maps a feed tag to a service key. Exact keys win over globs, and
// the longest glob wins among globs. ok is false when nothing matches; an
// empty service key with ok=true means the tag is deliberately ignored.
func (f FeedRef) ResolveTag(tag string) (string, bool) {
	if svc, ok := f.TagMap[tag]; ok {
		return svc, true
	}
	best, bestSvc, found := "", "", false
	for pattern, svc := range f.TagMap {
		if !strings.Contains(pattern, "*") || !globMatches(pattern, tag) {
			continue
		}
		if !found || len(pattern) > len(best) || (len(pattern) == len(best) && pattern < best) {
			best, bestSvc, found = pattern, svc, true
		}
	}
	return bestSvc, found
}

// IsGeneric reports whether tag is one of the feed's umbrella tags.
func (f FeedRef) IsGeneric(tag string) bool {
	for _, g := range f.GenericTags {
		if g == tag || (strings.Contains(g, "*") && globMatches(g, tag)) {
			return true
		}
	}
	return false
}

// ConfidenceOr returns *p, or def when p is nil.
func ConfidenceOr(p *float64, def float64) float64 {
	if p == nil {
		return def
	}
	return *p
}

func globMatches(pattern, value string) bool {
	p, err := matcher.Compile("glob", pattern, true)
	return err == nil && p.Match(value)
}
