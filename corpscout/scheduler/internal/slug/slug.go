package slug

import (
	"strings"
	"unicode"

	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

// Generate converts a display name to a canonical slug.
// Returns empty string if the input produces no alphanumeric characters after normalization.
func Generate(input string) string {
	input = strings.TrimSpace(input)
	input = strings.ReplaceAll(input, "&", " and ")

	// NFKD decompose, strip non-spacing combining marks, keep ASCII only.
	t := transform.Chain(
		norm.NFKD,
		runes.Remove(runes.In(unicode.Mn)),
	)
	normalized, _, _ := transform.String(t, input)
	normalized = strings.ToLower(normalized)

	var b strings.Builder
	prevWasSep := true
	for _, r := range normalized {
		isAlNum := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if isAlNum {
			b.WriteRune(r)
			prevWasSep = false
		} else if !prevWasSep {
			b.WriteRune('-')
			prevWasSep = true
		}
	}
	return strings.TrimRight(b.String(), "-")
}
