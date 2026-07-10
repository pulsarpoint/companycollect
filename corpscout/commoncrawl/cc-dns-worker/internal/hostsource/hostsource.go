// Package hostsource loads discovered subdomain labels for a batch of domains from two ClickHouse
// sources — Certificate Transparency (ctlogs.hostnames) and the durable registry
// (commoncrawl_domain_hostnames) — and merges them into a capped per-domain set for scanning.
package hostsource

import (
	"context"
	"strconv"
	"strings"

	"cc-dns-worker/internal/model"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// NormalizeLabel turns an fqdn into a scan label relative to rootDomain: strips the ".<rootDomain>"
// suffix and lowercases. Returns ok=false for the apex, non-subdomains, wildcards, or an empty label.
func NormalizeLabel(rootDomain, fqdn string) (string, bool) {
	fqdn = strings.ToLower(strings.TrimSuffix(fqdn, "."))
	rootDomain = strings.ToLower(rootDomain)
	suffix := "." + rootDomain
	if fqdn == rootDomain || !strings.HasSuffix(fqdn, suffix) {
		return "", false
	}
	label := strings.TrimSuffix(fqdn, suffix)
	if label == "" || strings.Contains(label, "*") {
		return "", false
	}
	return label, true
}

func inClause(n int) string {
	if n == 0 {
		return ""
	}
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

func toArgs(domains []string) []any {
	a := make([]any, len(domains))
	for i, d := range domains {
		a[i] = d
	}
	return a
}

// CTHostnames returns up to capN live-cert-first, recency-ranked non-wildcard labels per domain from
// Certificate Transparency, scoped to domains (index-pruned on the ctlogs sort key).
func CTHostnames(ctx context.Context, conn driver.Conn, domains []string, capN int) (map[string][]model.HostLabel, error) {
	if len(domains) == 0 {
		return map[string][]model.HostLabel{}, nil
	}
	q := `SELECT registered_domain, fqdn, (lna >= now()) AS live FROM (
	    SELECT registered_domain, fqdn, max(is_wildcard) is_wc, max(last_seen) ls, max(last_not_after) lna
	    FROM ctlogs.hostnames WHERE registered_domain IN (` + inClause(len(domains)) + `)
	    GROUP BY registered_domain, fqdn
	) WHERE is_wc = 0 AND fqdn != registered_domain
	ORDER BY registered_domain, (lna >= now()) DESC, ls DESC
	LIMIT ` + strconv.Itoa(capN) + ` BY registered_domain`
	rows, err := conn.Query(ctx, q, toArgs(domains)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string][]model.HostLabel{}
	for rows.Next() {
		var rd, fqdn string
		var live uint8
		if err := rows.Scan(&rd, &fqdn, &live); err != nil {
			return nil, err
		}
		if label, ok := NormalizeLabel(rd, fqdn); ok {
			out[rd] = append(out[rd], model.HostLabel{Label: label, DiscoverySource: "ct", LiveCert: live != 0})
		}
	}
	return out, rows.Err()
}

// RegistryHostnames returns up to capN recency-ranked labels per domain from the durable registry,
// carrying each label's stored discovery_source (axfr precedence via min).
func RegistryHostnames(ctx context.Context, conn driver.Conn, domains []string, capN int) (map[string][]model.HostLabel, error) {
	if len(domains) == 0 {
		return map[string][]model.HostLabel{}, nil
	}
	q := `SELECT root_domain, label, min(discovery_source) AS ds FROM corpscout.commoncrawl_domain_hostnames
	    WHERE root_domain IN (` + inClause(len(domains)) + `)
	    GROUP BY root_domain, label
	    ORDER BY root_domain, max(last_seen) DESC
	    LIMIT ` + strconv.Itoa(capN) + ` BY root_domain`
	rows, err := conn.Query(ctx, q, toArgs(domains)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string][]model.HostLabel{}
	for rows.Next() {
		var rd string
		var h model.HostLabel
		if err := rows.Scan(&rd, &h.Label, &h.DiscoverySource); err != nil {
			return nil, err
		}
		out[rd] = append(out[rd], h)
	}
	return out, rows.Err()
}

// Merge unions the CT and registry labels per domain, deduping by label with discovery-source
// precedence axfr > ct > static (min), and caps each domain to capN by keeping CT (live-first) then
// registry (recency) order. LiveCert is preserved from whichever source had it true.
func Merge(ct, reg map[string][]model.HostLabel, capN int) map[string][]model.HostLabel {
	out := map[string][]model.HostLabel{}
	domains := map[string]struct{}{}
	for d := range ct {
		domains[d] = struct{}{}
	}
	for d := range reg {
		domains[d] = struct{}{}
	}
	for d := range domains {
		seen := map[string]int{} // label -> index in merged
		var merged []model.HostLabel
		add := func(h model.HostLabel) {
			if i, ok := seen[h.Label]; ok {
				if minSource(h.DiscoverySource, merged[i].DiscoverySource) == h.DiscoverySource {
					merged[i].DiscoverySource = h.DiscoverySource
				}
				if h.LiveCert {
					merged[i].LiveCert = true
				}
				return
			}
			seen[h.Label] = len(merged)
			merged = append(merged, h)
		}
		for _, h := range ct[d] { // CT first (live-first order)
			add(h)
		}
		for _, h := range reg[d] { // then registry (recency)
			add(h)
		}
		if capN > 0 && len(merged) > capN {
			merged = merged[:capN]
		}
		out[d] = merged
	}
	return out
}

// minSource returns the alphabetically-smaller discovery source (axfr < ct < static — axfr precedence).
func minSource(a, b string) string {
	if a < b {
		return a
	}
	return b
}
