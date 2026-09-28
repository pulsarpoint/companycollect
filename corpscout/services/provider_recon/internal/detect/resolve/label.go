package resolve

import (
	"provider_recon/internal/detect/hosts"
	"provider_recon/internal/detect/knowledge"
)

// LabelHost labels one host a record names. The best rule of kind wins,
// giving one result per service type of the matched service. Without a rule
// the host's registrable domain decides: the domain itself (or a host under
// it) is self-hosted; a provider key names that provider, with its first
// service of fallbackType; anything else is kept as an unmapped key. A host
// with no registrable domain (an IP literal, garbage) yields nothing.
//
// base carries the record fields and window; LabelHost fills in the rest.
func LabelHost(kb knowledge.Knowledge, base Result, kind knowledge.Kind, fallbackType, host string) []Result {
	host = hosts.Normalize(host)
	if host == "" {
		return nil
	}
	base.Subject = host
	if m, ok := kb.Match(kind, host); ok {
		out := make([]Result, 0, len(m.ServiceTypes))
		for _, t := range m.ServiceTypes {
			r := base
			r.ServiceType, r.ProviderKey, r.ProviderSlug, r.ServiceKey = t, m.ProviderSlug, m.ProviderSlug, m.ServiceKey
			r.RuleID, r.Confidence = m.RuleID, m.Confidence
			out = append(out, r)
		}
		return out
	}
	r := base
	r.ServiceType = fallbackType
	key := hosts.Registrable(host)
	switch {
	case hosts.Under(host, base.RootDomain) || (key != "" && key == base.RootDomain):
		r.ProviderKey, r.Confidence = SelfHosted, SelfHostedConfidence
	case key == "":
		return nil
	default:
		if p, ok := kb.ProviderForKey(key); ok {
			r.ProviderKey, r.ProviderSlug, r.ServiceKey, r.Confidence = p.Slug, p.Slug, p.FirstService(fallbackType), KeyMatchConfidence
		} else {
			r.ProviderKey, r.Confidence = key, UnmappedConfidence
		}
	}
	return []Result{r}
}
