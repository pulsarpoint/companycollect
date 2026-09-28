// Package hosts normalises DNS host names and derives provider keys.
package hosts

import (
	"strings"

	"golang.org/x/net/publicsuffix"
)

// Normalize lower-cases a host and drops surrounding space and the trailing dot.
func Normalize(host string) string {
	return strings.TrimSuffix(strings.ToLower(strings.TrimSpace(host)), ".")
}

// Registrable is the host's registrable domain (eTLD+1) from the public
// suffix list, e.g. ns1.binero.se → binero.se, x.github.io → x.github.io.
// It returns "" for a host that has none (a bare suffix, an IP, garbage).
func Registrable(host string) string {
	host = Normalize(host)
	if host == "" || strings.Trim(host, "0123456789.:") == "" {
		return ""
	}
	key, err := publicsuffix.EffectiveTLDPlusOne(host)
	if err != nil {
		return ""
	}
	return key
}

// Under reports whether host is domain itself or a name below it.
func Under(host, domain string) bool {
	host, domain = Normalize(host), Normalize(domain)
	return host == domain || strings.HasSuffix(host, "."+domain)
}
