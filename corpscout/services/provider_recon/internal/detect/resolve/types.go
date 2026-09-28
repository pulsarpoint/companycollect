// Package resolve turns one DNS record into the services it proves. Every
// record is resolved on its own, against injected knowledge: a pure function
// of (record, knowledge). A domain's history is a view over the per-record
// results (spec: docs/superpowers/specs/2026-09-28-dns-detect-service-design.md).
package resolve

// Record is one DNS record instance as the DNS store keeps it: presentation
// type and RDATA, and the window it was seen in (YYYY-MM-DD; empty = open).
type Record struct {
	RecordID   string `json:"record_id"`
	RootDomain string `json:"root_domain"`
	Name       string `json:"name"`
	Type       string `json:"type"`
	Value      string `json:"value"`
	FirstSeen  string `json:"first_seen,omitempty"`
	LastSeen   string `json:"last_seen,omitempty"`
}

// Result is one service a record proves. ProviderKey is the provider-recon
// slug for a named provider, the registrable domain for an unmapped one, and
// SelfHosted when the evidence points at the domain itself. Fallback results
// count only where no better evidence of the same service type covers the
// same time (SOA MNAME versus NS); the history view applies that.
type Result struct {
	RecordID     string  `json:"record_id"`
	RootDomain   string  `json:"root_domain"`
	RecordName   string  `json:"record_name"`
	RecordType   string  `json:"record_type"`
	Analyzer     string  `json:"analyzer"`
	Subject      string  `json:"subject"`
	ServiceType  string  `json:"service_type"`
	ProviderKey  string  `json:"provider_key"`
	ProviderSlug string  `json:"provider_slug"`
	ServiceKey   string  `json:"service_key"`
	RuleID       string  `json:"rule_id"`
	Confidence   float64 `json:"confidence"`
	Fallback     bool    `json:"fallback"`
	ValidFrom    string  `json:"valid_from"`
	ValidTo      string  `json:"valid_to"`
}

// Finding is an observation about a record that is not a service.
type Finding struct {
	RecordID string `json:"record_id"`
	Analyzer string `json:"analyzer"`
	Code     string `json:"code"`
	Detail   string `json:"detail,omitempty"`
}

// Output is everything one record resolves to.
type Output struct {
	RecordID string    `json:"record_id"`
	Results  []Result  `json:"results"`
	Findings []Finding `json:"findings"`
}

// SelfHosted is the provider key of evidence that points at the domain itself.
const SelfHosted = "self-hosted"

// Confidence of a result that no provider-recon rule produced.
const (
	KeyMatchConfidence   = 0.8
	SelfHostedConfidence = 0.8
	UnmappedConfidence   = 0.5
)
