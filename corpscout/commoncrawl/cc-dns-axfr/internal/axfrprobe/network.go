// Package axfrprobe classifies nameserver addresses and probes public endpoints for AXFR exposure.
package axfrprobe

import "net"

func withPort(ip string) string {
	if _, _, err := net.SplitHostPort(ip); err == nil {
		return ip
	}
	return net.JoinHostPort(ip, "53")
}

func hostOnly(ip string) string {
	if host, _, err := net.SplitHostPort(ip); err == nil {
		return host
	}
	return ip
}
