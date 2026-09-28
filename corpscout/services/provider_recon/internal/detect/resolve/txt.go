package resolve

import (
	"strings"

	"provider_recon/internal/detect/knowledge"
)

// txtValue turns TXT presentation RDATA into its content: the quoted
// character-strings joined without separators (RFC 7208 §3.3 for SPF) and
// unescaped. An unquoted value is returned trimmed.
func txtValue(v string) string {
	v = strings.TrimSpace(v)
	if !strings.HasPrefix(v, `"`) {
		return v
	}
	var b strings.Builder
	in, escaped := false, false
	for _, c := range v {
		switch {
		case escaped:
			b.WriteRune(c)
			escaped = false
		case in && c == '\\':
			escaped = true
		case c == '"':
			in = !in
		case in:
			b.WriteRune(c)
		}
	}
	return b.String()
}

// TXT matches TXT rules only: an apex value against TXT/value rules (e.g.
// verification tokens), the first label of an underscore name against
// TXT/name rules (e.g. "_amazonses" of _amazonses.mail.<domain>). There is no provider-key fallback: a TXT value names no host.
type TXT struct{}

func (TXT) Name() string { return "txt" }

func (TXT) Analyze(rec Record, base Result, kb knowledge.Knowledge) Output {
	if rec.Name == rec.RootDomain {
		return Output{Results: LabelRule(kb, base, knowledge.TXTValue, txtValue(rec.Value))}
	}
	label, _, _ := strings.Cut(strings.TrimSuffix(rec.Name, "."+rec.RootDomain), ".")
	return Output{Results: LabelRule(kb, base, knowledge.TXTName, label)}
}
