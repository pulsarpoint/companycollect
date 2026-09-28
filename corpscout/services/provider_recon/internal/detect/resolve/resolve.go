package resolve

import (
	"cmp"
	"slices"
	"strings"

	"provider_recon/internal/detect/hosts"
	"provider_recon/internal/detect/knowledge"
)

// Route picks the analyzer for a normalised record, or nil when no analyzer
// handles it (spec: "Routing and analyzers").
func Route(rec Record) Analyzer {
	apex := rec.Name == rec.RootDomain
	www := rec.Name == "www."+rec.RootDomain
	switch {
	case rec.Type == "NS" && apex:
		return NS{}
	case rec.Type == "SOA" && apex:
		return SOA{}
	case rec.Type == "MX" && apex:
		return MX{}
	case rec.Type == "CNAME" && (apex || www):
		return CNAME{}
	}
	return nil
}

// Resolve turns one record into the services it proves. It normalises names
// and type, routes the record, copies its window onto every result, and sorts
// the output so the same record always gives identical output.
func Resolve(rec Record, kb knowledge.Knowledge) Output {
	rec.RootDomain = hosts.Normalize(rec.RootDomain)
	rec.Name = hosts.Normalize(rec.Name)
	rec.Type = strings.ToUpper(strings.TrimSpace(rec.Type))
	out := Output{RecordID: rec.RecordID, Results: []Result{}, Findings: []Finding{}}
	a := Route(rec)
	if a == nil {
		return out
	}
	base := Result{
		RecordID: rec.RecordID, RootDomain: rec.RootDomain, RecordName: rec.Name, RecordType: rec.Type,
		Analyzer: a.Name(), ValidFrom: day(rec.FirstSeen), ValidTo: day(rec.LastSeen),
	}
	got := a.Analyze(rec, base, kb)
	out.Results = append(out.Results, got.Results...)
	out.Findings = append(out.Findings, got.Findings...)
	slices.SortFunc(out.Results, func(a, b Result) int {
		return cmp.Or(cmp.Compare(a.ServiceType, b.ServiceType), cmp.Compare(a.ProviderKey, b.ProviderKey),
			cmp.Compare(a.Subject, b.Subject), cmp.Compare(a.RuleID, b.RuleID))
	})
	return out
}

func day(date string) string { return date[:min(len(date), 10)] }
