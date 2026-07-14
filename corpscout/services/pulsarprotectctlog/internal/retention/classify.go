// Package retention implements the ingestion retention rule and the derivation
// of distinct-hostname rows from parsed certificate metadata.
//
// Rule (applied per certificate at ingest time):
//   - Hostnames are ALWAYS extracted into the `hostnames` store.
//   - Full cert metadata is stored only while the certs table's TTL would
//     retain it: keepMetadata = now < notAfter + 90d. Anything past that would
//     be deleted by ClickHouse moments after insert (see certsDDL in
//     store/clickhouse/schema.go), so writing it is pure churn — old shards
//     contribute subdomains only.
package retention

import (
	"strings"
	"time"

	"golang.org/x/net/publicsuffix"

	"github.com/pulsarpoint/pulsarprotectctlog/internal/model"
)

// metadataTTLGrace mirrors the certs table's TTL (`not_after + INTERVAL 90 DAY
// DELETE` in store/clickhouse/schema.go). The two MUST stay in sync: this rule
// exists so we never insert rows the TTL would immediately delete.
const metadataTTLGrace = 90 * 24 * time.Hour

// KeepMetadata reports whether full per-certificate metadata should be stored:
// only while the certs TTL would retain the row. Certificates past the TTL
// horizon contribute subdomains only.
func KeepMetadata(c model.CertMeta, now time.Time) bool {
	return now.Before(c.NotAfter.Add(metadataTTLGrace))
}

// HostnameRows derives the distinct-hostname rows for a certificate. One row is
// produced per unique hostname found across the SANs and the common name. Each
// row seeds FirstSeen and LastSeen from the cert's SCTTimestamp; the
// AggregatingMergeTree collapses repeated observations of the same hostname into
// a single (min first_seen, max last_seen) row. Hostnames are stored regardless
// of the metadata-retention decision, so there is no has-metadata flag here.
func HostnameRows(c model.CertMeta) []model.HostnameRow {
	names := dedupeHostnames(c.SANs, c.CommonName)
	rows := make([]model.HostnameRow, 0, len(names))
	for _, name := range names {
		fqdn, wildcard := normalizeHostname(name)
		if fqdn == "" {
			continue
		}
		rows = append(rows, model.HostnameRow{
			RegisteredDomain: RegisteredDomain(fqdn),
			FQDN:             fqdn,
			IsWildcard:       wildcard,
			FirstSeen:        c.SCTTimestamp,
			LastSeen:         c.SCTTimestamp,
			LastNotAfter:     c.NotAfter,
			SourceLog:        c.LogName,
		})
	}
	return rows
}

// RegisteredDomain returns the eTLD+1 (registrable domain) for a hostname using
// the public suffix list. The leading wildcard label is stripped first. If the
// registrable domain cannot be determined, the normalized host is returned.
func RegisteredDomain(host string) string {
	host = strings.TrimPrefix(host, "*.")
	if host == "" {
		return ""
	}
	rd, err := publicsuffix.EffectiveTLDPlusOne(host)
	if err != nil {
		return host
	}
	return rd
}

// normalizeHostname lowercases and trims a name, reporting whether it carried a
// leading wildcard label. The returned FQDN retains the "*." prefix so wildcard
// entries remain distinct in the index.
func normalizeHostname(name string) (string, bool) {
	n := strings.ToLower(strings.TrimSpace(name))
	n = strings.TrimSuffix(n, ".")
	if n == "" {
		return "", false
	}
	wildcard := strings.HasPrefix(n, "*.")
	return n, wildcard
}

// looksLikeHostname reports whether s is plausibly a DNS name (optionally
// wildcard): it has a dot, no spaces, and only hostname characters. Used to
// keep non-hostname common names out of the subdomain index.
func looksLikeHostname(s string) bool {
	if s == "" || !strings.Contains(s, ".") {
		return false
	}
	host := strings.TrimPrefix(s, "*.")
	for _, r := range host {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
		case r == '.' || r == '-' || r == '_':
		default:
			return false
		}
	}
	return true
}

// dedupeHostnames merges the SAN list with the common name and removes
// duplicates while preserving first-seen order.
func dedupeHostnames(sans []string, commonName string) []string {
	seen := make(map[string]struct{}, len(sans)+1)
	out := make([]string, 0, len(sans)+1)
	add := func(raw string) {
		n, _ := normalizeHostname(raw)
		if n == "" {
			return
		}
		if _, ok := seen[n]; ok {
			return
		}
		seen[n] = struct{}{}
		out = append(out, n)
	}
	for _, s := range sans {
		add(s)
	}
	if looksLikeHostname(strings.ToLower(strings.TrimSpace(commonName))) {
		add(commonName)
	}
	return out
}
