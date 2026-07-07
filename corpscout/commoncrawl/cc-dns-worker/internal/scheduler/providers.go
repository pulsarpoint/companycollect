package scheduler

import "net"

// hyperscalerCIDRs are the published nameserver ranges of large anycast DNS providers (Cloudflare,
// Google Cloud DNS, AWS Route 53). These operators absorb orders of magnitude more query volume than
// a self-hosted nameserver, so an authoritative server IP inside one of these ranges is paced at
// Config.HyperscalerQPS instead of the polite PerServerQPS. A miss is safe: an unlisted provider is
// just paced at the default (correct, only slower), never over-driven. Extend the list as needed.
var hyperscalerCIDRs = parseCIDRs([]string{
	// Cloudflare — https://www.cloudflare.com/ips/
	"173.245.48.0/20", "103.21.244.0/22", "103.22.200.0/22", "103.31.4.0/22",
	"141.101.64.0/18", "108.162.192.0/18", "190.93.240.0/20", "188.114.96.0/20",
	"197.234.240.0/22", "198.41.128.0/17", "162.158.0.0/15", "104.16.0.0/13",
	"104.24.0.0/14", "172.64.0.0/13", "131.0.72.0/22",
	"2400:cb00::/32", "2606:4700::/32", "2803:f800::/32", "2405:b500::/32",
	"2405:8100::/32", "2a06:98c0::/29", "2c0f:f248::/32",
	// Google Cloud DNS — ns-cloud-*.googledomains.com
	"216.239.32.0/19", "2001:4860:4802::/48",
	// AWS Route 53 — ns-*.awsdns-*
	"205.251.192.0/18", "2600:9000::/28",
	// UltraDNS / Neustar / Vercara — registrar-servers.com (Namecheap), name.com, many registrars
	"156.154.0.0/16", "204.74.0.0/16", "2610:a1::/32",
})

func parseCIDRs(cidrs []string) []*net.IPNet {
	out := make([]*net.IPNet, 0, len(cidrs))
	for _, c := range cidrs {
		if _, n, err := net.ParseCIDR(c); err == nil {
			out = append(out, n)
		}
	}
	return out
}

// isHyperscaler reports whether ip (a bare address, no port) falls in a known large anycast DNS
// provider range. A non-IP string returns false.
func isHyperscaler(ip string) bool {
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return false
	}
	for _, n := range hyperscalerCIDRs {
		if n.Contains(parsed) {
			return true
		}
	}
	return false
}
