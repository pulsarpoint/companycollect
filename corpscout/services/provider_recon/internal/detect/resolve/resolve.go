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
	below := ""
	if !apex && strings.HasSuffix(rec.Name, "."+rec.RootDomain) {
		below = strings.TrimSuffix(rec.Name, "."+rec.RootDomain)
	}
	dkim := strings.HasSuffix(below, "._domainkey")
	label, _, _ := strings.Cut(below, ".")
	switch {
	case rec.Type == "NS" && apex:
		return NS{}
	case rec.Type == "SOA" && apex:
		return SOA{}
	case rec.Type == "MX" && apex:
		return MX{}
	case (rec.Type == "CNAME" || rec.Type == "TXT") && dkim:
		return DKIM{}
	case rec.Type == "CNAME" && (apex || www):
		return CNAME{}
	case (rec.Type == "A" || rec.Type == "AAAA") && (apex || www):
		return IP{}
	case rec.Type == "TXT" && below == "_dmarc":
		return DMARC{}
	case rec.Type == "TXT" && (apex || below != "") && isSPF(rec.Value):
		return SPF{}
	case rec.Type == "TXT" && apex:
		return TXT{}
	case rec.Type == "TXT" && strings.HasPrefix(label, "_") && label != "_dmarc" && !dkim:
		return TXT{}
	}
	return nil
}

// isSPF reports whether a TXT value is an SPF record (RFC 7208 §4.5: the
// version term, case-insensitive, alone or followed by a space).
func isSPF(value string) bool {
	v := strings.ToLower(txtValue(value))
	return v == "v=spf1" || strings.HasPrefix(v, "v=spf1 ")
}

// normalise lower-cases names, drops trailing dots and upper-cases the type.
func normalise(rec Record) Record {
	rec.RootDomain = hosts.Normalize(rec.RootDomain)
	rec.Name = hosts.Normalize(rec.Name)
	rec.Type = strings.ToUpper(strings.TrimSpace(rec.Type))
	return rec
}

// Resolve turns one record into the services it proves. It normalises names
// and type, routes the record, copies its window onto every result, and sorts
// the output so the same record always gives identical output.
func Resolve(rec Record, kb knowledge.Knowledge) Output {
	rec = normalise(rec)
	out := Output{RecordID: rec.RecordID, Results: []Result{}, Findings: []Finding{}}
	a := Route(rec)
	if a == nil {
		return out
	}
	out.Analyzer = a.Name()
	base := Result{
		RecordID: rec.RecordID, RootDomain: rec.RootDomain, RecordName: rec.Name, RecordType: rec.Type,
		Analyzer: a.Name(), ValidFrom: day(rec.FirstSeen), ValidTo: day(rec.LastSeen),
	}
	got := a.Analyze(rec, base, kb)
	out.Results = append(out.Results, got.Results...)
	out.Findings = append(out.Findings, got.Findings...)
	slices.SortFunc(out.Results, func(a, b Result) int {
		return cmp.Or(cmp.Compare(a.ServiceType, b.ServiceType), cmp.Compare(a.ProviderKey, b.ProviderKey),
			cmp.Compare(a.Subject, b.Subject), cmp.Compare(a.RuleID, b.RuleID),
			cmp.Compare(a.ValidFrom, b.ValidFrom), cmp.Compare(a.ValidTo, b.ValidTo),
			cmp.Compare(a.ServiceKey, b.ServiceKey), cmp.Compare(a.ProviderSlug, b.ProviderSlug),
			cmp.Compare(a.Confidence, b.Confidence))
	})
	// One record can prove the same thing twice (SPF "a" and "mx"): keep one.
	out.Results = slices.Compact(out.Results)
	return out
}

func day(date string) string { return date[:min(len(date), 10)] }
