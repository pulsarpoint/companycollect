// Package model holds the in-memory result a resolver emits (DomainResult/DNSRecord) and the
// ClickHouse-load row structs whose ch tags name the target columns.
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

	// Finding is a derived classification for this record, currently only ever
	// "public_dns_private_address" (Task 9): an A/AAAA Value that resolve.ClassifyString classifies as
	// non-public (private/loopback/link-local/CGNAT/ULA/documentation/reserved/etc), returned by a
	// server that Tier-2 (query.go's queryAuth) or AXFR (axfr.go) only ever dials when it is itself
	// public (see resolve.Dialable) — i.e. a public authoritative server answering with a bogus/internal
	// address, which is worth flagging even though the record is still stored verbatim like any other.
	// Empty for every other record. This is never used to pick a dial target (see resolve.Dialable's own
	// doc comment on why observation and dialing are kept strictly separate).
	Finding string
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
	LastProbeRecords   uint64    `ch:"last_probe_records"`
	LastProbeBytes     uint64    `ch:"last_probe_bytes"`
	LastProbeTruncated uint8     `ch:"last_probe_truncated"`
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

const (
	DomainStatusDone                = "done"
	DomainStatusPartial             = "partial"
	DomainStatusError               = "error"
	DomainStatusNoPublicNSEndpoints = "no_public_ns_endpoints"
)

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

	// DSOutcome/DNSKEYOutcome (Task 9) are the tri-state OUTCOME of the query that produced DSPresent/
	// DNSSECSigned: "present" | "absent" | "unknown". DSPresent/DNSSECSigned alone cannot distinguish a
	// genuine negative (the parent has no DS / the zone has no DNSKEY — a definitive NOERROR/NODATA or
	// NXDOMAIN) from "the query failed and we simply don't know" (timeout/SERVFAIL/exhausted retries) —
	// both looked identical as plain false. "unknown" is set whenever the underlying query (DS: Tier-1
	// discovery, see resolve.Discoverer.DiscoverNS; DNSKEY: Tier-2, see resolve.Resolver.Resolve) never
	// got a definitive authoritative answer, and in that case DSPresent/DNSSECSigned stay false for
	// THIS scan's DomainResult but must never be used to overwrite a prior known-good value — see
	// Status and store.CommitBatch's done-only summary-write policy, which is what actually enforces
	// that. Empty string only appears on a DomainResult/Delegation never produced by the real discovery
	// path (e.g. a hand-built test fixture); it is treated the same as "not applicable", not "unknown".
	DSOutcome     string
	DNSKEYOutcome string

	// Status is one of the DomainStatus* constants:
	//   - "error": discovery failed outright
	//     (set by the caller before Resolve is ever invoked — Resolve itself never returns "error").
	//   - "no_public_ns_endpoints": NS discovery succeeded and its full private/special-use delegation
	//     is authoritative security evidence, but Tier-2 queries were intentionally not sent. This is a
	//     terminal, summary-worthy result distinct from a discovery/transport failure.
	//   - "done": the minimum authoritative-success bar was met — see resolve.Resolver.Resolve's doc
	//     comment for the exact bar (apex A/AAAA + DNSKEY + DS all definitive). Only a "done" result may
	//     ever replace a domain's persisted last-good summary (see model.ScanRow, store.CommitBatch).
	//   - "partial": authoritative contact succeeded and some records were observed (Records is fully
	//     populated regardless of Status), but at least one query the bar above depends on never got a
	//     definitive answer. A partial result's records still flow through the normal record-load
	//     stream; its summary is simply never written, so it can never clobber a prior "done" summary.
	Status       string
	Error        string
	QueriesTotal int
	QueriesOK    int
	Records      []DNSRecord
	SourceRunID  string
	ResolvedAt   time.Time
}

// StagedDNSRecord is the bounded SQLite read shape used to build retry-safe observations and hostname
// updates. It carries one event timestamp rather than aggregate-table concepts such as scans or ranges.
type StagedDNSRecord struct {
	RootDomain string
	RecordType string
	Slot       string
	Name       string
	Value      string
	TTL        uint32
	Priority   uint16
	Rcode      string
	Source     string
	Discovery  string
	Finding    string
	ObservedAt time.Time
}

// RecordObservationRow mirrors corpscout.commoncrawl_domain_dns_record_observations (Task 7's
// retry-safe record-load path). This is not a pre-aggregated fact —
// it is one IMMUTABLE row per (identity, source, discovery, scan_id) observation, so replaying a
// scan's load (the same rows, same ScanID) always produces byte-identical rows that
// ReplacingMergeTree(loaded_at) collapses to one on merge/FINAL, however many times it is retried.
// Field order matches the CREATE TABLE column order (see migration 000113); ORDER BY is
// (root_domain, name, record_type, slot, value, source, discovery, scan_id) — the FULL logical
// observation identity, so LoadedAt only decides which physical copy of an identical row survives a
// retry, never whether two truly distinct observations collapse into one. See internal/load/load.go
// for why LoadedAt is safe to set to time.Now() on every call (rather than pinned per scan_id).
type RecordObservationRow struct {
	RootDomain string    `ch:"root_domain"`
	Name       string    `ch:"name"`
	RecordType string    `ch:"record_type"`
	Slot       string    `ch:"slot"`
	Value      string    `ch:"value"`
	Source     string    `ch:"source"`
	Discovery  string    `ch:"discovery"`
	ScanID     string    `ch:"scan_id"`
	TTL        uint32    `ch:"ttl"`
	Priority   uint16    `ch:"priority"`
	Rcode      string    `ch:"rcode"`
	ObservedAt time.Time `ch:"observed_at"` // event time: when this record was actually resolved
	LoadedAt   time.Time `ch:"loaded_at"`   // ReplacingMergeTree version: when this row was inserted
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

// ScanRow mirrors corpscout.commoncrawl_domain_dns_scan (latest trustworthy state per domain). A
// definitive private-only delegation is loaded with Status == DomainStatusNoPublicNSEndpoints because
// it is security evidence, while failed/partial scans remain excluded. A partial scan's observed records
// still reach ClickHouse through the independent record-observation stream — only this summary is gated.
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
	// Endpoints is the convenient SQLite/in-memory shape. The four parallel ch-tagged arrays persist
	// the same identity in ClickHouse without depending on driver-specific nested-struct encoding.
	Endpoints          []NameserverEndpoint
	NSEndpointNames    []string `ch:"ns_endpoint_names"`
	NSEndpointIPs      []string `ch:"ns_endpoint_ips"`
	NSEndpointScopes   []string `ch:"ns_endpoint_scopes"`
	NSEndpointDialable []uint8  `ch:"ns_endpoint_dialable"`
	DNSSECSigned       uint8    `ch:"dnssec_signed"`
	DSPresent          uint8    `ch:"ds_present"`
	// DSOutcome/DNSKEYOutcome (Task 9, migration 000116) are the tri-state "present"|"absent"|"unknown"
	// query outcome behind DSPresent/DNSSECSigned — see model.DomainResult's doc comment on the same
	// two fields for why the plain booleans alone cannot distinguish a genuine negative from a failed
	// query. A normal "done" row carries "present" or "absent". A definitive private-only delegation can
	// carry "unknown" when its independent parent DS query failed, without being confused with a partial
	// authoritative record scan because Status remains explicit.
	DSOutcome     string    `ch:"ds_outcome"`
	DNSKEYOutcome string    `ch:"dnskey_outcome"`
	Status        string    `ch:"status"`
	QueriesTotal  uint16    `ch:"queries_total"`
	QueriesOK     uint16    `ch:"queries_ok"`
	LastRunID     string    `ch:"last_run_id"`
	ResolvedAt    time.Time `ch:"resolved_at"`
}
