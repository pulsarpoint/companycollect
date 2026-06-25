package tech

import (
	"regexp/syntax"
	"strings"
)

// longestRequiredLiteral returns the longest lowercased literal substring that MUST
// appear in the input for pat to match, or "" if none can be conservatively extracted.
//
// Used to pre-gate Wappalyzer regexes with Aho-Corasick: if a pattern's required
// literal isn't present in the page, the regex cannot match, so it's skipped. The
// extraction is conservative — it only returns a literal that is genuinely required
// (literals reachable through OpConcat / OpCapture / OpPlus), never through OpStar,
// OpQuest, or OpAlternate — so a pattern is never wrongly skipped.
func longestRequiredLiteral(pat string) string {
	if i := strings.Index(pat, `\;`); i >= 0 {
		pat = pat[:i] // strip wappalyzer "\;version:..." suffix before parsing as regex
	}
	re, err := syntax.Parse(pat, syntax.Perl)
	if err != nil {
		return ""
	}
	return strings.ToLower(requiredLiteral(re.Simplify()))
}

func requiredLiteral(re *syntax.Regexp) string {
	switch re.Op {
	case syntax.OpLiteral:
		return string(re.Rune)
	case syntax.OpConcat:
		best, cur := "", ""
		flush := func() {
			if len(cur) > len(best) {
				best = cur
			}
			cur = ""
		}
		for _, sub := range re.Sub {
			if sub.Op == syntax.OpLiteral {
				cur += string(sub.Rune)
				continue
			}
			flush()
			if l := requiredLiteral(sub); len(l) > len(best) { // required sub-expression
				best = l
			}
		}
		flush()
		return best
	case syntax.OpCapture:
		return requiredLiteral(re.Sub[0])
	case syntax.OpPlus: // x+ requires x at least once
		return requiredLiteral(re.Sub[0])
	default: // OpStar/OpQuest/OpAlternate/charclass/anchors => nothing guaranteed
		return ""
	}
}
