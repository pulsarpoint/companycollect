package resolve

import (
	"strings"

	"provider_recon/internal/detect/hosts"
	"provider_recon/internal/detect/knowledge"
)

// DMARC reads _dmarc.<domain>. The aggregate (rua) and forensic (ruf) report
// mailboxes name a reporting service; each mailbox host is labelled with
// DMARC/report rules, then the provider-key fallback (dmarc_reporting). A
// mailbox inside the domain itself is no service. A record that does not
// start with v=DMARC1 is a finding.
type DMARC struct{}

func (DMARC) Name() string { return "dmarc" }

func (a DMARC) Analyze(rec Record, base Result, kb knowledge.Knowledge) Output {
	var out Output
	tags := strings.Split(txtValue(rec.Value), ";")
	k, v, _ := strings.Cut(tags[0], "=")
	if !strings.EqualFold(strings.TrimSpace(k), "v") || !strings.EqualFold(strings.TrimSpace(v), "DMARC1") {
		out.Findings = append(out.Findings, Finding{RecordID: rec.RecordID, Analyzer: a.Name(), Code: "dmarc_invalid"})
		return out
	}
	for _, tag := range tags[1:] {
		k, v, _ := strings.Cut(tag, "=")
		if k = strings.ToLower(strings.TrimSpace(k)); k != "rua" && k != "ruf" {
			continue
		}
		for _, uri := range strings.Split(v, ",") {
			uri = strings.TrimSpace(uri)
			if len(uri) < 7 || !strings.EqualFold(uri[:7], "mailto:") {
				continue
			}
			addr, _, _ := strings.Cut(uri[7:], "!")
			at := strings.LastIndex(addr, "@")
			if at < 0 {
				continue
			}
			host := hosts.Normalize(addr[at+1:])
			if host == "" || hosts.Under(host, rec.RootDomain) {
				continue
			}
			out.Results = append(out.Results, LabelHost(kb, base, knowledge.DMARCReport, "dmarc_reporting", host)...)
		}
	}
	return out
}
