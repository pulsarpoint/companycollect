// Package model defines the provider-recon/v1 per-provider document: one
// provider, its services and the evidence that identifies each service.
package model

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"time"
)

// ContractVersion is written into every document's version field.
const ContractVersion = "provider-recon/v1"

// ServiceTypes is the closed list of service types a provider service can carry.
var ServiceTypes = []string{
	"cdn", "ddos_protection", "dns", "email", "email_security", "email_sending",
	"hosting", "iaas", "paas", "saas_verification", "waf",
}

// Traits is the closed list of service traits (vocabulary ported from runner3).
var Traits = []string{"cloud_provider_ip", "origin_obscured", "rate_limit_sensitive", "shared_infrastructure"}

// Contains reports whether v is in list.
func Contains(list []string, v string) bool { return slices.Contains(list, v) }

// Source says how an evidence item was obtained.
type Source string

// Evidence sources.
const (
	SourceCurated      Source = "curated"
	SourceOfficialFeed Source = "official_feed"
	SourceBGP          Source = "bgp"
)

// Provenance records where an evidence item came from.
type Provenance struct {
	Source        Source `json:"source"`
	Collector     string `json:"collector,omitempty"`
	SourceURL     string `json:"source_url,omitempty"`
	SourceVersion string `json:"source_version,omitempty"`
}

// hashView drops the parts of provenance that move without the evidence
// changing: the source version always, and the source URL of feed-derived
// items (Azure's download file is renamed every week). A curated item's URL
// is definition content and stays.
func (p Provenance) hashView() Provenance {
	p.SourceVersion = ""
	if p.Source != SourceCurated {
		p.SourceURL = ""
	}
	return p
}

// Item statuses.
const (
	StatusActive  = "active"
	StatusMissing = "missing"
	StatusRemoved = "removed"
)

// Removal actions.
const (
	ActionGraceExpired      = "grace_expired"
	ActionDefinitionRemoved = "definition_removed"
)

// DateLayout is the day resolution of lifecycle dates (UTC).
const DateLayout = "2006-01-02"

// Lifecycle is an evidence item's observed life. An item that reappears after
// being removed is a new instance with its own FirstSeen.
type Lifecycle struct {
	Status        string `json:"status"`
	FirstSeen     string `json:"first_seen"`
	LastSeen      string `json:"last_seen"`
	MissingSince  string `json:"missing_since,omitempty"`
	RemovedAt     string `json:"removed_at,omitempty"`
	RemovalAction string `json:"removal_action,omitempty"`
	// RestoredAt records that restore undid a wrong removal of this instance.
	RestoredAt string `json:"restored_at,omitempty"`
}

// hashView drops LastSeen: it advances every day without the evidence changing.
func (l Lifecycle) hashView() Lifecycle {
	l.LastSeen = ""
	return l
}

// IPRange is an address block operated for a service.
type IPRange struct {
	CIDR       string  `json:"cidr"`
	Region     string  `json:"region,omitempty"`
	FeedTag    string  `json:"feed_tag,omitempty"`
	Confidence float64 `json:"confidence"`
	Note       string  `json:"note,omitempty"`
	Provenance
	Lifecycle
}

// Key identifies the item for sorting and diffs.
func (r IPRange) Key() string { return r.CIDR }

// ASN is an autonomous system operated for a service.
type ASN struct {
	ASN        uint32  `json:"asn"`
	Confidence float64 `json:"confidence"`
	Note       string  `json:"note,omitempty"`
	Provenance
	Lifecycle
}

// Key identifies the item for sorting and diffs.
func (a ASN) Key() string { return fmt.Sprintf("AS%d", a.ASN) }

// DNSRule matches one field of a DNS record.
type DNSRule struct {
	RecordType    string  `json:"record_type"`
	MatchField    string  `json:"match_field"`
	MatcherType   string  `json:"matcher_type"`
	Pattern       string  `json:"pattern"`
	CaseSensitive bool    `json:"case_sensitive,omitempty"`
	Confidence    float64 `json:"confidence"`
	Priority      int     `json:"priority"`
	Note          string  `json:"note,omitempty"`
	Provenance
	Lifecycle
}

// Key identifies the item for sorting and diffs.
func (r DNSRule) Key() string {
	return r.RecordType + " " + r.MatchField + " " + r.MatcherType + " " + r.Pattern
}

// HTTPRule matches one part of an HTTP response.
type HTTPRule struct {
	HTTPPart      string  `json:"http_part"`
	HeaderName    string  `json:"header_name,omitempty"`
	MatcherType   string  `json:"matcher_type"`
	Pattern       string  `json:"pattern"`
	PathScope     string  `json:"path_scope,omitempty"`
	CaseSensitive bool    `json:"case_sensitive,omitempty"`
	Confidence    float64 `json:"confidence"`
	Priority      int     `json:"priority"`
	Note          string  `json:"note,omitempty"`
	Provenance
	Lifecycle
}

// Key identifies the item for sorting and diffs.
func (r HTTPRule) Key() string {
	return r.HTTPPart + " " + r.HeaderName + " " + r.MatcherType + " " + r.Pattern
}

// PTRRule matches a reverse-DNS hostname.
type PTRRule struct {
	MatcherType string  `json:"matcher_type"`
	Pattern     string  `json:"pattern"`
	Confidence  float64 `json:"confidence"`
	Note        string  `json:"note,omitempty"`
	Provenance
	Lifecycle
}

// Key identifies the item for sorting and diffs.
func (r PTRRule) Key() string { return r.MatcherType + " " + r.Pattern }

// CertificateIdentity matches a TLS certificate field.
type CertificateIdentity struct {
	IdentityType  string  `json:"identity_type"`
	IdentityValue string  `json:"identity_value"`
	Confidence    float64 `json:"confidence"`
	Note          string  `json:"note,omitempty"`
	Provenance
	Lifecycle
}

// Key identifies the item for sorting and diffs.
func (c CertificateIdentity) Key() string { return c.IdentityType + " " + c.IdentityValue }

// Candidate is unreviewed evidence; detection never uses it.
type Candidate struct {
	Kind       string          `json:"kind"`
	ServiceKey string          `json:"service_key,omitempty"`
	Payload    json.RawMessage `json:"payload"`
	Rationale  string          `json:"rationale,omitempty"`
	SourceURLs []string        `json:"source_urls,omitempty"`
	Provenance
}

// Key identifies the item for sorting and diffs.
func (c Candidate) Key() string { return c.Kind + " " + c.ServiceKey + " " + string(c.Payload) }

// Evidence groups a service's identifying evidence by kind.
type Evidence struct {
	IPRanges              []IPRange             `json:"ip_ranges"`
	ASNs                  []ASN                 `json:"asns"`
	DNSRules              []DNSRule             `json:"dns_rules"`
	HTTPRules             []HTTPRule            `json:"http_rules"`
	PTRRules              []PTRRule             `json:"ptr_rules"`
	CertificateIdentities []CertificateIdentity `json:"certificate_identities"`
}

// Service is one thing a provider offers, typed by our closed service list.
type Service struct {
	Key          string   `json:"service_key"`
	DisplayName  string   `json:"display_name"`
	ServiceTypes []string `json:"service_types"`
	Traits       []string `json:"traits"`
	// RemovedAt is set when the service was dropped from the definition; the
	// service stays until its removed items are purged.
	RemovedAt string   `json:"removed_at,omitempty"`
	Evidence  Evidence `json:"evidence"`
}

// Churn counts one run's lifecycle transitions for one feed.
type Churn struct {
	Added      int `json:"added"`
	Reappeared int `json:"reappeared"`
	Missing    int `json:"missing"`
	Removed    int `json:"removed"`
	Purged     int `json:"purged"`
	// Restored counts ranges put back by restore; only restore runs set it.
	Restored int `json:"restored,omitempty"`
}

// CollectorStatus reports one feed's outcome for this provider in this run.
type CollectorStatus struct {
	Status        string     `json:"status"` // ok | stale | failed
	SourceURL     string     `json:"source_url,omitempty"`
	SourceVersion string     `json:"source_version,omitempty"`
	Items         int        `json:"items"`
	FetchedAt     time.Time  `json:"fetched_at"`
	LastSuccessAt *time.Time `json:"last_success_at,omitempty"`
	Error         string     `json:"error,omitempty"`
	UnmappedTags  []string   `json:"unmapped_tags,omitempty"`
	SkippedLines  int        `json:"skipped_lines,omitempty"`
	Churn         Churn      `json:"churn"`
}

// Collection is run metadata. It is excluded from the content hash.
type Collection struct {
	CollectedAt time.Time                  `json:"collected_at"`
	Collectors  map[string]CollectorStatus `json:"collectors"`
	ContentHash string                     `json:"content_hash"`
}

// Document is the provider-recon/v1 object for one provider.
type Document struct {
	Version      string      `json:"version"`
	Slug         string      `json:"slug"`
	DisplayName  string      `json:"display_name"`
	Category     string      `json:"category"`
	Website      string      `json:"website,omitempty"`
	Country      string      `json:"country,omitempty"`
	Aliases      []string    `json:"aliases"`
	ProviderKeys []string    `json:"provider_keys"`
	Services     []Service   `json:"services"`
	Collection   Collection  `json:"collection"`
	Candidates   []Candidate `json:"candidates"`
}

// Normalize sorts and de-duplicates every list and replaces nil slices and maps
// with empty ones, so equal content always marshals to identical bytes. It
// clones slices before sorting, so a caller's slices are never reordered.
func Normalize(doc *Document) {
	doc.Aliases = sortedUnique(doc.Aliases)
	doc.ProviderKeys = sortedUnique(doc.ProviderKeys)
	doc.Services = slices.Clone(doc.Services)
	if doc.Services == nil {
		doc.Services = []Service{}
	}
	sort.SliceStable(doc.Services, func(i, j int) bool { return doc.Services[i].Key < doc.Services[j].Key })
	for i := range doc.Services {
		s := &doc.Services[i]
		s.ServiceTypes = sortedUnique(s.ServiceTypes)
		s.Traits = sortedUnique(s.Traits)
		e := &s.Evidence
		e.IPRanges = sortByKey(e.IPRanges, IPRange.Key)
		e.ASNs = sortByKey(e.ASNs, ASN.Key)
		e.DNSRules = sortByKey(e.DNSRules, DNSRule.Key)
		e.HTTPRules = sortByKey(e.HTTPRules, HTTPRule.Key)
		e.PTRRules = sortByKey(e.PTRRules, PTRRule.Key)
		e.CertificateIdentities = sortByKey(e.CertificateIdentities, CertificateIdentity.Key)
	}
	doc.Candidates = sortByKey(doc.Candidates, Candidate.Key)
	if doc.Collection.Collectors == nil {
		doc.Collection.Collectors = map[string]CollectorStatus{}
	}
}

// Marshal renders the normalized document as indented JSON with a trailing newline.
func Marshal(doc Document) ([]byte, error) {
	Normalize(&doc)
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(doc); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ContentHash hashes identity and evidence only. The collection block,
// per-item source versions, the lifecycle's last_seen and feed-derived items'
// source URLs are excluded,
// so re-running collectors against unchanged feed content yields the same
// hash even when a feed's sync token or file name moved.
func ContentHash(doc Document) (string, error) {
	raw, err := json.Marshal(doc)
	if err != nil {
		return "", err
	}
	var view Document
	if err := json.Unmarshal(raw, &view); err != nil {
		return "", err
	}
	view.Collection = Collection{}
	for i := range view.Services {
		e := &view.Services[i].Evidence
		for j := range e.IPRanges {
			e.IPRanges[j].Provenance = e.IPRanges[j].Provenance.hashView()
			e.IPRanges[j].Lifecycle = e.IPRanges[j].Lifecycle.hashView()
		}
		for j := range e.ASNs {
			e.ASNs[j].Provenance = e.ASNs[j].Provenance.hashView()
			e.ASNs[j].Lifecycle = e.ASNs[j].Lifecycle.hashView()
		}
		for j := range e.DNSRules {
			e.DNSRules[j].Provenance = e.DNSRules[j].Provenance.hashView()
			e.DNSRules[j].Lifecycle = e.DNSRules[j].Lifecycle.hashView()
		}
		for j := range e.HTTPRules {
			e.HTTPRules[j].Provenance = e.HTTPRules[j].Provenance.hashView()
			e.HTTPRules[j].Lifecycle = e.HTTPRules[j].Lifecycle.hashView()
		}
		for j := range e.PTRRules {
			e.PTRRules[j].Provenance = e.PTRRules[j].Provenance.hashView()
			e.PTRRules[j].Lifecycle = e.PTRRules[j].Lifecycle.hashView()
		}
		for j := range e.CertificateIdentities {
			e.CertificateIdentities[j].Provenance = e.CertificateIdentities[j].Provenance.hashView()
			e.CertificateIdentities[j].Lifecycle = e.CertificateIdentities[j].Lifecycle.hashView()
		}
	}
	for j := range view.Candidates {
		view.Candidates[j].Provenance = view.Candidates[j].Provenance.hashView()
	}
	Normalize(&view)
	b, err := json.Marshal(view)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func sortedUnique(values []string) []string {
	out := slices.Clone(values)
	if out == nil {
		return []string{}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// sortByKey returns a sorted clone; ties on Key break on the JSON encoding so
// the order is total and deterministic.
func sortByKey[T any](items []T, key func(T) string) []T {
	out := slices.Clone(items)
	if out == nil {
		return []T{}
	}
	sort.SliceStable(out, func(i, j int) bool {
		ki, kj := key(out[i]), key(out[j])
		if ki != kj {
			return ki < kj
		}
		bi, _ := json.Marshal(out[i])
		bj, _ := json.Marshal(out[j])
		return bytes.Compare(bi, bj) < 0
	})
	return out
}
