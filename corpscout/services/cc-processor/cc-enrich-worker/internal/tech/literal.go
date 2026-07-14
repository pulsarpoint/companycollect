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

// minGateLiteral is the shortest literal (in runes) allowed to gate a pattern: shorter strings
// occur on nearly every page, so they'd fire the Aho-Corasick gate constantly and gate nothing.
const minGateLiteral = 4

// gateLiterals returns a set of lowercased literals of which AT LEAST ONE must appear in the
// input for pat to match — the any-of generalization of longestRequiredLiteral that alternation
// patterns need (every branch contributes its own required literal). nil means no usable gate:
// some way of matching requires none of the candidates, or a candidate is shorter than
// minGateLiteral and would not be selective.
func gateLiterals(pat string) []string {
	if i := strings.Index(pat, `\;`); i >= 0 {
		pat = pat[:i] // strip wappalyzer "\;version:..." suffix before parsing as regex
	}
	re, err := syntax.Parse(pat, syntax.Perl)
	if err != nil {
		return nil
	}
	lits := altLiterals(re.Simplify())
	if len(lits) == 0 {
		return nil
	}
	seen := map[string]bool{}
	out := make([]string, 0, len(lits))
	for _, l := range lits {
		l = strings.ToLower(l)
		if len([]rune(l)) < minGateLiteral {
			return nil // one unselective member and the whole any-of gate is worthless
		}
		if !seen[l] {
			seen[l] = true
			out = append(out, l)
		}
	}
	return out
}

// altLiterals returns literals of which at least one must appear for re to match, or nil when
// no such guarantee can be extracted. Same conservative traversal as requiredLiteral, plus
// OpAlternate: a match needs SOME branch, so the union of per-branch sets gates the whole —
// but only when EVERY branch yields one.
func altLiterals(re *syntax.Regexp) []string {
	switch re.Op {
	case syntax.OpLiteral:
		return []string{string(re.Rune)}
	case syntax.OpCapture, syntax.OpPlus:
		return altLiterals(re.Sub[0])
	case syntax.OpConcat:
		// Every sub must match, so any single sub's set gates the concat. Adjacent literals merge
		// into runs (each run is a one-literal candidate). Pick the candidate set whose WEAKEST
		// member is longest, so the AC gate stays selective.
		var best []string
		bestScore := 0
		consider := func(set []string) {
			if len(set) == 0 {
				return
			}
			score := len([]rune(set[0]))
			for _, l := range set[1:] {
				if n := len([]rune(l)); n < score {
					score = n
				}
			}
			if score > bestScore {
				best, bestScore = set, score
			}
		}
		cur := ""
		for _, sub := range re.Sub {
			if sub.Op == syntax.OpLiteral {
				cur += string(sub.Rune)
				continue
			}
			if cur != "" {
				consider([]string{cur})
				cur = ""
			}
			consider(altLiterals(sub))
		}
		if cur != "" {
			consider([]string{cur})
		}
		return best
	case syntax.OpAlternate:
		var out []string
		for _, sub := range re.Sub {
			branch := altLiterals(sub)
			if len(branch) == 0 {
				return nil // this branch can match with no known literal — no gate
			}
			out = append(out, branch...)
		}
		return out
	default: // OpStar/OpQuest/charclass/anchors => nothing guaranteed
		return nil
	}
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
