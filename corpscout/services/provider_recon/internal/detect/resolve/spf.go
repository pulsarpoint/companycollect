package resolve

import (
	"fmt"
	"strings"

	"provider_recon/internal/detect/knowledge"
)

// spfLookupBudget is RFC 7208 §4.6.4's limit on DNS-querying terms.
const spfLookupBudget = 10

// SPF reads an apex "v=spf1" record without any DNS lookups: only what the
// record itself names.
//   - include:host and redirect=host are labelled with SPF/include rules, then
//     the provider-key fallback (email_sending);
//   - a and mx without a host mean the domain sends itself (self-hosted);
//     a:host and mx:host label that host;
//   - a host with a macro (%{…}) is a finding, never guessed at;
//   - ip4/ip6 (slice 3), ptr, exists and all give nothing;
//   - more than ten DNS-querying terms is a finding.
type SPF struct{}

func (SPF) Name() string { return "spf" }

func (a SPF) Analyze(rec Record, base Result, kb knowledge.Knowledge) Output {
	var out Output
	terms := strings.Fields(strings.ToLower(txtValue(rec.Value)))
	lookups := 0
	for _, term := range terms[min(1, len(terms)):] {
		term = strings.TrimLeft(term, "+-~?")
		name, arg := term, ""
		if i := strings.IndexAny(term, ":="); i >= 0 {
			name, arg = term[:i], term[i+1:]
		}
		switch name {
		case "include", "redirect", "a", "mx", "ptr", "exists":
			lookups++
		default:
			continue
		}
		host := arg
		if name == "a" || name == "mx" {
			host, _, _ = strings.Cut(arg, "/")
			if strings.HasPrefix(arg, "/") {
				host = ""
			}
		}
		switch {
		case name == "ptr" || name == "exists":
			continue
		case strings.Contains(host, "%{"):
			out.Findings = append(out.Findings, Finding{RecordID: rec.RecordID, Analyzer: a.Name(), Code: "spf_macro", Detail: term})
		case host == "" && (name == "a" || name == "mx"):
			self := base
			self.Subject, self.ServiceType, self.ProviderKey, self.Confidence = rec.RootDomain, "email_sending", SelfHosted, SelfHostedConfidence
			out.Results = append(out.Results, self)
		case host != "":
			out.Results = append(out.Results, LabelHost(kb, base, knowledge.SPFInclude, "email_sending", host)...)
		}
	}
	if lookups > spfLookupBudget {
		out.Findings = append(out.Findings, Finding{RecordID: rec.RecordID, Analyzer: a.Name(), Code: "spf_lookup_budget_exceeded", Detail: fmt.Sprintf("%d lookups", lookups)})
	}
	return out
}
