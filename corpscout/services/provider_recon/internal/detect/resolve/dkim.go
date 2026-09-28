package resolve

import (
	"strings"

	"provider_recon/internal/detect/knowledge"
)

// DKIM reads <selector>._domainkey.<domain>. The selector alone can name a
// provider (DKIM/selector rules, e.g. Microsoft 365's "selector1"); a CNAME
// target is labelled with DKIM/target rules, then the provider-key fallback
// (email_sending). A TXT record publishes the key itself, so only the
// selector can speak for it.
type DKIM struct{}

func (DKIM) Name() string { return "dkim" }

func (DKIM) Analyze(rec Record, base Result, kb knowledge.Knowledge) Output {
	selector := strings.TrimSuffix(rec.Name, "._domainkey."+rec.RootDomain)
	out := Output{Results: LabelRule(kb, base, knowledge.DKIMSelector, selector)}
	if rec.Type == "CNAME" {
		out.Results = append(out.Results, LabelHost(kb, base, knowledge.DKIMTarget, "email_sending", rec.Value)...)
	}
	return out
}
