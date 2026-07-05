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
}

// DomainResult is everything learned for one domain in one scan.
type DomainResult struct {
	ScanID       string
	RootDomain   string
	ETLD         string
	Nameservers  []string
	NSIPs        []string
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

// RecordRow mirrors corpscout.commoncrawl_domain_dns_records.
type RecordRow struct {
	ScanID      string    `ch:"scan_id"`
	RootDomain  string    `ch:"root_domain"`
	Name        string    `ch:"name"`
	RecordType  string    `ch:"record_type"`
	Slot        string    `ch:"slot"`
	Value       string    `ch:"value"`
	TTL         uint32    `ch:"ttl"`
	Priority    uint16    `ch:"priority"`
	Rcode       string    `ch:"rcode"`
	SourceRunID string    `ch:"source_run_id"`
	ResolvedAt  time.Time `ch:"resolved_at"`
}

// ScanRow mirrors corpscout.commoncrawl_domain_dns_scan.
type ScanRow struct {
	ScanID       string    `ch:"scan_id"`
	RootDomain   string    `ch:"root_domain"`
	ETLD         string    `ch:"etld"`
	Nameservers  []string  `ch:"nameservers"`
	NSIPs        []string  `ch:"ns_ips"`
	DNSSECSigned uint8     `ch:"dnssec_signed"`
	DSPresent    uint8     `ch:"ds_present"`
	Status       string    `ch:"status"`
	Error        string    `ch:"error"`
	QueriesTotal uint16    `ch:"queries_total"`
	QueriesOK    uint16    `ch:"queries_ok"`
	SourceRunID  string    `ch:"source_run_id"`
	ResolvedAt   time.Time `ch:"resolved_at"`
}
