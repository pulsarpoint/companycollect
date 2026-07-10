// Package model holds the in-memory result a resolver emits (DomainResult/DNSRecord) and the
// ClickHouse-load row structs (RecordRow/ScanRow) whose ch tags name the target columns.
package model

import "time"

// DNSRecord is one resolved resource record, stored verbatim.
type DNSRecord struct {
	Name       string // qname queried (FQDN without trailing dot)
	RecordType string // A, AAAA, MX, TXT, NS, SOA, CAA, DNSKEY, DS
	Slot       string // "@", hostname, DKIM selector, "dmarc"/"mta_sts"/"tls_rpt"/"bimi", or ""
	Value      string // rdata verbatim
	Rcode      string // query rcode for the query that produced this record
	TTL        uint32
	Priority   uint16 // MX preference; 0 otherwise
	Source     string // "query" (actively queried) | "axfr" (from a zone transfer)
	Discovery  string // "static" | "ct" | "axfr" — how the hostname was discovered
}

// NameserverEndpoint pairs one authoritative NS hostname with one of its resolved IP addresses, so
// discovery's hostname<->IP identity survives storage instead of collapsing into a flat IP list — a
// flat list can't say which NS name an address came from, and (worse) has no way to note "this address
// exists but is not safe to dial" other than dropping it, which discards evidence. Scope and Dialable
// are therefore carried explicitly rather than inferred from an empty/missing IP: an empty IP must
// never be overloaded to mean "blocked". Scope mirrors resolve.AddrScope's string value (e.g.
// "public", "private", "loopback"); this package does not import package resolve (which already
// imports model) to avoid a cycle.
type NameserverEndpoint struct {
	Name     string // NS hostname, lowercased, no trailing dot
	IP       string // resolved address, canonical form (net/netip's Addr.String())
	Scope    string // resolve.AddrScope value for IP, e.g. "public", "private", "loopback"
	Dialable bool   // true only when Scope is publicly dialable (resolve.Dialable(scope))
}

// AXFRLatestRow mirrors corpscout.dns_axfr_latest: one row per AXFR-probed endpoint (root_domain,
// name_server, name_server_ip) carrying its most recent probe and its most recent DEFINITIVE state,
// tracked separately because an unknown probe must update the former without ever touching the latter.
// HasDefinitiveState/AXFROpen/DefinitiveAt/DefinitiveScanID describe the last probe that reached open
// or closed; LastProbeVerdict/LastProbeReason/LastProbedAt describe the most recent probe of any kind
// (including unknown). DelegationActive is whether this (host, ip) is still part of the domain's
// current NS delegation as of this scan; DelegationSeenAt is when it was last seen active. Field order
// matches the CREATE TABLE column order (the ch tags drive `insert`'s column list from struct order).
type AXFRLatestRow struct {
	RootDomain         string    `ch:"root_domain"`
	NameServer         string    `ch:"name_server"`
	NameServerIP       string    `ch:"name_server_ip"`
	UpdatedAt          time.Time `ch:"updated_at"`
	DelegationActive   uint8     `ch:"delegation_active"`
	DelegationSeenAt   time.Time `ch:"delegation_seen_at"`
	LastProbeVerdict   string    `ch:"last_probe_verdict"`
	LastProbeReason    string    `ch:"last_probe_reason"`
	LastProbedAt       time.Time `ch:"last_probed_at"`
	HasDefinitiveState uint8     `ch:"has_definitive_state"`
	AXFROpen           uint8     `ch:"axfr_open"`
	DefinitiveAt       time.Time `ch:"definitive_at"`
	DefinitiveScanID   string    `ch:"definitive_scan_id"`
}

// AXFRStateChangeRow mirrors corpscout.dns_axfr_state_changes: one row per DEFINITIVE AXFR state
// transition for one endpoint. ScanID is part of the table's ORDER BY key, so re-loading the same
// scan's staged changes (a retry) is idempotent, while two distinct open periods from different scans
// never collapse into one even if their ChangedAt values coincide.
type AXFRStateChangeRow struct {
	RootDomain   string    `ch:"root_domain"`
	NameServer   string    `ch:"name_server"`
	NameServerIP string    `ch:"name_server_ip"`
	AXFROpen     uint8     `ch:"axfr_open"`
	ScanID       string    `ch:"scan_id"`
	ChangedAt    time.Time `ch:"changed_at"`
}

// HostLabel is one discovered subdomain label to scan for a domain (from CT, the registry, or a
// zone transfer). Label is the name minus ".<root_domain>", lowercased. DiscoverySource is how it
// was found (ct|axfr|static); LiveCert is true when a CT source had a still-valid certificate.
type HostLabel struct {
	Label           string
	DiscoverySource string
	LiveCert        bool
}

// DomainResult is everything learned for one domain in one scan. AXFR is NOT part of this shape — it
// runs as its own post-scan phase (see cmd/cc-dns-worker/axfr.go) and is staged/loaded through the
// dns_axfr_latest/dns_axfr_state_changes tables (AXFRLatestRow/AXFRStateChangeRow), never through a
// per-domain summary field here.
type DomainResult struct {
	ScanID       string
	RootDomain   string
	ETLD         string
	Nameservers  []string
	NSIPs        []string
	Endpoints    []NameserverEndpoint // hostname<->IP identity behind Nameservers/NSIPs (see NameserverEndpoint)
	DNSSECSigned bool
	DSPresent    bool
	Status       string // "done" | "error"
	Error        string
	QueriesTotal int
	QueriesOK    int
	Records      []DNSRecord
	SourceRunID  string
	ResolvedAt   time.Time
}

// RecordRow mirrors corpscout.commoncrawl_domain_dns_records (distinct model). Each scan inserts one
// row per record with FirstSeen = LastSeen = scan time and Scans = 1; the AggregatingMergeTree merges
// duplicates to min(first_seen) / max(last_seen) / sum(scans).
type RecordRow struct {
	RootDomain string    `ch:"root_domain"`
	RecordType string    `ch:"record_type"`
	Slot       string    `ch:"slot"`
	Name       string    `ch:"name"`
	Value      string    `ch:"value"`
	TTL        uint32    `ch:"ttl"`
	Priority   uint16    `ch:"priority"`
	Rcode      string    `ch:"rcode"`
	LastRunID  string    `ch:"last_run_id"`
	FirstSeen  time.Time `ch:"first_seen"`
	LastSeen   time.Time `ch:"last_seen"`
	Scans      uint64    `ch:"scans"`
	Source     string    `ch:"source"`
	Discovery  string    `ch:"discovery"`
}

// HostnameRow mirrors corpscout.commoncrawl_domain_hostnames (AggregatingMergeTree registry). One
// blind-inserted row per hostname discovered in a cycle; the merge folds first_seen=min, last_seen=max,
// last_resolved=max, discovery_source=min. Insert a plain string into the SimpleAggregateFunction cols.
type HostnameRow struct {
	RootDomain      string    `ch:"root_domain"`
	Label           string    `ch:"label"`
	DiscoverySource string    `ch:"discovery_source"`
	FirstSeen       time.Time `ch:"first_seen"`
	LastSeen        time.Time `ch:"last_seen"`
	LastResolved    time.Time `ch:"last_resolved"`
}

// ScanRow mirrors corpscout.commoncrawl_domain_dns_scan (latest-good-state per domain). Only
// successful scans are loaded, so a failed re-scan never clobbers a domain's last-good summary.
//
// NOTE (deprecated columns, NOT a deprecated type): the table's axfr_open/axfr_records/
// axfr_truncated/axfr_server columns are deprecated — per-endpoint AXFR state now lives in
// dns_axfr_latest/dns_axfr_state_changes (AXFRLatestRow/AXFRStateChangeRow), the only source of truth
// since AXFR moved off resolveDomain into its own post-scan phase. This struct intentionally has NO
// fields for them so the loader never again writes default/false values into those columns every
// cycle. The ClickHouse columns are left in place (defaulted) for backward compatibility; a future
// audited cleanup can drop them. (ScanRow itself is fully current — do not treat it as deprecated;
// the leading "NOTE" avoids the godoc "Deprecated:" marker that would flag every use of this type.)
type ScanRow struct {
	RootDomain  string   `ch:"root_domain"`
	ETLD        string   `ch:"etld"`
	Nameservers []string `ch:"nameservers"`
	NSIPs       []string `ch:"ns_ips"`
	// Endpoints carries the hostname<->IP identity behind Nameservers/NSIPs. It is SQLite-local only
	// (no ch tag, so chColumns/insert skip it) — not yet part of the ClickHouse scan-summary schema.
	Endpoints    []NameserverEndpoint
	DNSSECSigned uint8     `ch:"dnssec_signed"`
	DSPresent    uint8     `ch:"ds_present"`
	Status       string    `ch:"status"`
	QueriesTotal uint16    `ch:"queries_total"`
	QueriesOK    uint16    `ch:"queries_ok"`
	LastRunID    string    `ch:"last_run_id"`
	ResolvedAt   time.Time `ch:"resolved_at"`
}
