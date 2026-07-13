// Package security extracts security-relevant facts from a page's HTTP response. v1 captures the FULL
// response-header map verbatim so nothing is skipped — hygiene scoring, version-disclosure, cookie-flag
// analysis and issue-template mapping all derive from it downstream in SQL. Static-snapshot facts only;
// no active probing.
package security

import (
	"net/http"
	"strings"
)

// HeaderMap flattens an http.Header into lowercased-name -> value. Multi-valued headers are joined with
// ", ", except Set-Cookie which is joined with "\n" (commas are significant inside cookie values). Empty
// input returns nil.
func HeaderMap(h http.Header) map[string]string {
	if len(h) == 0 {
		return nil
	}
	out := make(map[string]string, len(h))
	for name, vals := range h {
		sep := ", "
		if strings.EqualFold(name, "Set-Cookie") {
			sep = "\n"
		}
		out[strings.ToLower(name)] = strings.Join(vals, sep)
	}
	return out
}
