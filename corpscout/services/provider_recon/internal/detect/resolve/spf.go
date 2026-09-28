package resolve

import (
	"fmt"
	"net/netip"
	"strings"

	"provider_recon/internal/detect/hosts"
	"provider_recon/internal/detect/knowledge"
)

// spfLookupBudget is RFC 7208 §4.6.4's limit on DNS-querying terms.
const spfLookupBudget = 10

// SPF reads an apex "v=spf1" record without any DNS lookups: only what the
// record itself names.
//   - include:host and redirect=host are labelled with SPF/include rules, then
//     the provider-key fallback (email_sending); a host inside the domain is
//     delegation to another of its own records, so only a finding;
//   - a and mx without a host mean the domain sends itself (self-hosted);
//     a:host and mx:host label that host;
//   - a host with a macro (%{…}) is a finding; the labels after its last
//     macro are literal text in the record, so that tail is labelled
//     (…%{d}._spf.vali.email → _spf.vali.email); a macro is never expanded;
//   - ip4/ip6 are looked up in the provider ranges valid during the record's
//     window (email_sending); ptr, exists and all give nothing;
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
		if name == "ip4" || name == "ip6" {
			if p, ok := spfPrefix(arg); ok {
				out.Results = append(out.Results, ipResults(kb, base, p, p.String(), "email_sending")...)
			}
			continue
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
			if tail := macroTail(host); tail != "" {
				out.Results = append(out.Results, LabelHost(kb, base, knowledge.SPFInclude, "email_sending", tail)...)
			}
		case (name == "include" || name == "redirect") && hosts.Under(host, rec.RootDomain):
			out.Findings = append(out.Findings, Finding{RecordID: rec.RecordID, Analyzer: a.Name(), Code: "spf_include_within_domain", Detail: host})
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

// macroTail is the part of a macro host after its last label that contains a
// macro: "%{i}._ip.%{d}._spf.vali.email" → "_spf.vali.email", "%{d}" → "".
func macroTail(host string) string {
	labels := strings.Split(host, ".")
	last := -1
	for i, l := range labels {
		if strings.Contains(l, "%") {
			last = i
		}
	}
	return strings.Join(labels[last+1:], ".")
}

// spfPrefix parses an ip4/ip6 argument: a prefix, or an address meaning its
// single-address prefix.
func spfPrefix(arg string) (netip.Prefix, bool) {
	if strings.Contains(arg, "/") {
		p, err := netip.ParsePrefix(arg)
		if err != nil {
			return netip.Prefix{}, false
		}
		return p.Masked(), true
	}
	a, err := netip.ParseAddr(arg)
	if err != nil {
		return netip.Prefix{}, false
	}
	a = a.Unmap()
	return netip.PrefixFrom(a, a.BitLen()), true
}
