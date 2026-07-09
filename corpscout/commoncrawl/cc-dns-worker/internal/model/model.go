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

// DomainResult is everything learned for one domain in one scan.
type DomainResult struct {
	ScanID        string
	RootDomain    string
	ETLD          string
	Nameservers   []string
	NSIPs         []string
	DNSSECSigned  bool
	DSPresent     bool
	Status        string // "done" | "error"
	Error         string
	QueriesTotal  int
	QueriesOK     int
	Records       []DNSRecord
	AXFROpen      bool
	AXFRRecords   int
	AXFRTruncated bool
	AXFRServer    string
	SourceRunID   string
	ResolvedAt    time.Time
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
type ScanRow struct {
	RootDomain    string    `ch:"root_domain"`
	ETLD          string    `ch:"etld"`
	Nameservers   []string  `ch:"nameservers"`
	NSIPs         []string  `ch:"ns_ips"`
	DNSSECSigned  uint8     `ch:"dnssec_signed"`
	DSPresent     uint8     `ch:"ds_present"`
	Status        string    `ch:"status"`
	QueriesTotal  uint16    `ch:"queries_total"`
	QueriesOK     uint16    `ch:"queries_ok"`
	LastRunID     string    `ch:"last_run_id"`
	ResolvedAt    time.Time `ch:"resolved_at"`
	AXFROpen      uint8     `ch:"axfr_open"`
	AXFRRecords   uint32    `ch:"axfr_records"`
	AXFRTruncated uint8     `ch:"axfr_truncated"`
	AXFRServer    string    `ch:"axfr_server"`
}
