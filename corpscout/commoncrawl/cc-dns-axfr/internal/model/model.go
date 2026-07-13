// Package model contains the records and ClickHouse rows produced by an AXFR scan.
package model

import "time"

// DNSRecord preserves one resource record returned by a zone transfer.
type DNSRecord struct {
	Name         string
	RecordType   string
	TypeCode     uint16
	ClassCode    uint16
	Slot         string
	Value        string
	RDataWire    string
	Rcode        string
	TTL          uint32
	Priority     uint16
	Source       string
	Discovery    string
	NameServer   string
	NameServerIP string
	Finding      string
}

// NameserverEndpoint keeps the hostname and address of an authoritative nameserver together.
// Dialable is derived from Scope by axfrprobe when the DNS scan summary is read.
type NameserverEndpoint struct {
	Name     string
	IP       string
	Scope    string
	Dialable bool
}

// RecordObservationRow is one retry-safe AXFR record observation loaded into ClickHouse.
type RecordObservationRow struct {
	RootDomain   string    `ch:"root_domain"`
	Name         string    `ch:"name"`
	RecordType   string    `ch:"record_type"`
	TypeCode     uint16    `ch:"record_type_code"`
	ClassCode    uint16    `ch:"record_class_code"`
	Slot         string    `ch:"slot"`
	Value        string    `ch:"value"`
	RDataWire    string    `ch:"rdata_wire"`
	Source       string    `ch:"source"`
	Discovery    string    `ch:"discovery"`
	NameServer   string    `ch:"name_server"`
	NameServerIP string    `ch:"name_server_ip"`
	ScanID       string    `ch:"scan_id"`
	TTL          uint32    `ch:"ttl"`
	Priority     uint16    `ch:"priority"`
	Rcode        string    `ch:"rcode"`
	ObservedAt   time.Time `ch:"observed_at"`
	LoadedAt     time.Time `ch:"loaded_at"`
}

// HostnameRow adds a hostname learned from an AXFR transfer to the shared hostname registry.
type HostnameRow struct {
	RootDomain      string    `ch:"root_domain"`
	Label           string    `ch:"label"`
	DiscoverySource string    `ch:"discovery_source"`
	FirstSeen       time.Time `ch:"first_seen"`
	LastSeen        time.Time `ch:"last_seen"`
	LastResolved    time.Time `ch:"last_resolved"`
}
