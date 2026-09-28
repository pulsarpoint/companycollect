package resolve

import (
	"strings"

	"provider_recon/internal/detect/hosts"
	"provider_recon/internal/detect/knowledge"
)

// Analyzer resolves the records routed to it. base is the result template for
// the record (record fields and window already set).
type Analyzer interface {
	Name() string
	Analyze(rec Record, base Result, kb knowledge.Knowledge) Output
}

// NS labels an apex nameserver: service type dns.
type NS struct{}

func (NS) Name() string { return "ns" }

func (NS) Analyze(rec Record, base Result, kb knowledge.Knowledge) Output {
	return Output{Results: LabelHost(kb, base, knowledge.NSTarget, "dns", rec.Value)}
}

// SOA labels the SOA primary nameserver (MNAME) with NS rules. Its results
// are fallback: NS is the better evidence wherever it covers the same time.
type SOA struct{}

func (SOA) Name() string { return "soa" }

func (SOA) Analyze(rec Record, base Result, kb knowledge.Knowledge) Output {
	f := strings.Fields(rec.Value)
	if len(f) == 0 {
		return Output{}
	}
	base.Fallback = true
	return Output{Results: LabelHost(kb, base, knowledge.NSTarget, "dns", f[0])}
}

// MX labels an apex mail exchanger: service type email. A null MX ("0 .",
// RFC 7505) is a finding, not a provider; localhost placeholders are ignored.
type MX struct{}

func (MX) Name() string { return "mx" }

func (a MX) Analyze(rec Record, base Result, kb knowledge.Knowledge) Output {
	f := strings.Fields(rec.Value)
	if len(f) == 0 {
		return Output{}
	}
	host := strings.ToLower(f[len(f)-1])
	switch host {
	case ".":
		return Output{Findings: []Finding{{RecordID: rec.RecordID, Analyzer: a.Name(), Code: "null_mx", Detail: "the domain accepts no mail"}}}
	case "localhost", "localhost.", "~":
		return Output{}
	}
	return Output{Results: LabelHost(kb, base, knowledge.MXTarget, "email", host)}
}

// CNAME labels an apex or www CNAME target: service type hosting unless a
// rule says otherwise (a CDN edge, a PaaS). A target inside the domain itself
// (www → apex, the usual alias) proves nothing about hosting: where that name
// is served from is the A/AAAA evidence's job, so it is only a finding.
type CNAME struct{}

func (CNAME) Name() string { return "cname" }

func (a CNAME) Analyze(rec Record, base Result, kb knowledge.Knowledge) Output {
	target := hosts.Normalize(rec.Value)
	if target != "" && hosts.Under(target, rec.RootDomain) {
		return Output{Findings: []Finding{{RecordID: rec.RecordID, Analyzer: a.Name(), Code: "cname_within_domain", Detail: target}}}
	}
	return Output{Results: LabelHost(kb, base, knowledge.CNAMETarget, "hosting", rec.Value)}
}
