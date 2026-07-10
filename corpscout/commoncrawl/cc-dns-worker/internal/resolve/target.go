package resolve

import "net/netip"

// AddrScope classifies an IP address by routability. Discovery must preserve every address it finds
// regardless of scope (evidence); only ScopePublic may ever be dialed (see Dialable).
type AddrScope string

const (
	ScopePublic        AddrScope = "public"
	ScopePrivate       AddrScope = "private"       // RFC1918 (10/8, 172.16/12, 192.168/16) + IPv6 ULA fc00::/7
	ScopeLoopback      AddrScope = "loopback"      // 127/8, ::1
	ScopeLinkLocal     AddrScope = "link_local"    // 169.254/16, fe80::/10
	ScopeCGNAT         AddrScope = "cgnat"         // 100.64/10 (RFC6598 carrier-grade NAT)
	ScopeDocumentation AddrScope = "documentation" // 192.0.2/24, 198.51.100/24, 203.0.113/24, 2001:db8::/32
	ScopeBenchmark     AddrScope = "benchmark"     // 198.18/15 (RFC2544)
	ScopeMulticast     AddrScope = "multicast"     // 224/4, ff00::/8
	ScopeUnspecified   AddrScope = "unspecified"   // 0.0.0.0, ::
	ScopeReserved      AddrScope = "reserved"      // everything else not globally routable (e.g. 240/4)
)

// Explicit prefixes for ranges net/netip has no built-in predicate for. All other special ranges
// (loopback, link-local, multicast, unspecified, RFC1918/ULA private) are covered by netip's own
// IsLoopback/IsLinkLocalUnicast/IsMulticast/IsUnspecified/IsPrivate.
var (
	cgnatV4     = netip.MustParsePrefix("100.64.0.0/10")
	benchmarkV4 = netip.MustParsePrefix("198.18.0.0/15")
	documentV4a = netip.MustParsePrefix("192.0.2.0/24")
	documentV4b = netip.MustParsePrefix("198.51.100.0/24")
	documentV4c = netip.MustParsePrefix("203.0.113.0/24")
	documentV6  = netip.MustParsePrefix("2001:db8::/32")
	reservedV4e = netip.MustParsePrefix("240.0.0.0/4") // Class E "reserved for future use" (+ broadcast)
)

// ClassifyAddr returns the routability scope of ip. IPv4-in-IPv6 addresses are unmapped first so
// ::ffff:10.0.0.1 classifies the same as 10.0.0.1. An invalid (zero-value) Addr returns ScopeReserved.
func ClassifyAddr(ip netip.Addr) AddrScope {
	if !ip.IsValid() {
		return ScopeReserved
	}
	ip = ip.Unmap()

	switch {
	case ip.IsUnspecified():
		return ScopeUnspecified
	case ip.IsLoopback():
		return ScopeLoopback
	case ip.IsMulticast(): // covers link-local multicast too (224/4, ff00::/8 both fully "multicast")
		return ScopeMulticast
	case ip.IsLinkLocalUnicast():
		return ScopeLinkLocal
	}

	if ip.Is4() {
		switch {
		case cgnatV4.Contains(ip):
			return ScopeCGNAT
		case benchmarkV4.Contains(ip):
			return ScopeBenchmark
		case documentV4a.Contains(ip), documentV4b.Contains(ip), documentV4c.Contains(ip):
			return ScopeDocumentation
		case reservedV4e.Contains(ip):
			return ScopeReserved
		}
	} else if documentV6.Contains(ip) {
		return ScopeDocumentation
	}

	if ip.IsPrivate() { // RFC1918 (v4) / ULA fc00::/7 (v6)
		return ScopePrivate
	}
	// IsGlobalUnicast alone is not sufficient: it deliberately still returns true for private/CGNAT/
	// documentation/benchmark ranges (per its doc comment), all of which are excluded above. Anything
	// left that IsGlobalUnicast accepts is actually globally routable.
	if ip.IsGlobalUnicast() {
		return ScopePublic
	}
	return ScopeReserved
}

// ClassifyString parses s as a bare IP address (no port) and classifies it. ok is false if s is not a
// valid IP.
func ClassifyString(s string) (AddrScope, bool) {
	ip, err := netip.ParseAddr(s)
	if err != nil {
		return "", false
	}
	return ClassifyAddr(ip), true
}

// Dialable reports whether an authoritative DNS/AXFR socket may target an address of this scope. Only
// ScopePublic is dialable — observation (discovery) must preserve every scope, but dialing never may.
func Dialable(scope AddrScope) bool {
	return scope == ScopePublic
}
