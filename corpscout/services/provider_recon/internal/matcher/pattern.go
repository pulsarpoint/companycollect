// Package matcher evaluates provider evidence patterns and IP ranges.
package matcher

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// MatcherTypes lists the supported matcher types.
var MatcherTypes = []string{"contains", "exact", "exists", "glob", "prefix", "regex", "suffix"}

// Pattern is a compiled matcher.
type Pattern struct {
	kind          string
	pattern       string
	caseSensitive bool
	re            *regexp.Regexp
}

// Compile validates and prepares a matcher. Non-regex patterns are compared
// after trimming spaces and a trailing dot, lowercased unless caseSensitive.
// suffix is label-aware: "one.com" matches "mx1.one.com" but not "bone.com".
func Compile(matcherType, pattern string, caseSensitive bool) (Pattern, error) {
	kind := strings.ToLower(strings.TrimSpace(matcherType))
	p := Pattern{kind: kind, caseSensitive: caseSensitive, pattern: normalize(pattern, caseSensitive)}
	switch kind {
	case "exists":
		if strings.TrimSpace(pattern) != "" {
			return Pattern{}, errors.New("exists takes an empty pattern")
		}
		return p, nil
	case "regex":
		expr := strings.TrimSpace(pattern)
		if expr == "" {
			return Pattern{}, errors.New("pattern is required")
		}
		if !caseSensitive {
			expr = "(?i)" + expr
		}
		re, err := regexp.Compile(expr)
		if err != nil {
			return Pattern{}, fmt.Errorf("invalid regex: %w", err)
		}
		p.re = re
		return p, nil
	case "exact", "prefix", "suffix", "contains", "glob":
		if p.pattern == "" {
			return Pattern{}, errors.New("pattern is required")
		}
		return p, nil
	default:
		return Pattern{}, fmt.Errorf("unsupported matcher_type %q", matcherType)
	}
}

// Match reports whether value satisfies the pattern.
func (p Pattern) Match(value string) bool {
	switch p.kind {
	case "exists":
		return true
	case "regex":
		return p.re.MatchString(strings.TrimSuffix(strings.TrimSpace(value), "."))
	}
	v := normalize(value, p.caseSensitive)
	if v == "" {
		return false
	}
	switch p.kind {
	case "exact":
		return v == p.pattern
	case "prefix":
		return strings.HasPrefix(v, p.pattern)
	case "suffix":
		s := strings.TrimPrefix(p.pattern, ".")
		return v == s || strings.HasSuffix(v, "."+s)
	case "contains":
		return strings.Contains(v, p.pattern)
	case "glob":
		return globMatch(p.pattern, v)
	}
	return false
}

func normalize(value string, caseSensitive bool) string {
	value = strings.TrimSpace(value)
	if !caseSensitive {
		value = strings.ToLower(value)
	}
	return strings.TrimSuffix(value, ".")
}

// globMatch supports '*' only (ported from runner3).
func globMatch(pattern, value string) bool {
	parts := strings.Split(pattern, "*")
	if len(parts) == 1 {
		return value == pattern
	}
	if !strings.HasPrefix(value, parts[0]) {
		return false
	}
	cursor := len(parts[0])
	for _, part := range parts[1 : len(parts)-1] {
		idx := strings.Index(value[cursor:], part)
		if idx < 0 {
			return false
		}
		cursor += idx + len(part)
	}
	last := parts[len(parts)-1]
	return len(value)-len(last) >= cursor && strings.HasSuffix(value, last)
}
