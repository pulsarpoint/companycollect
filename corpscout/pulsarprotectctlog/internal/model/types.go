// Package model defines the core domain types shared across the CT log
// ingestion service: parsed certificate metadata, the normalized SAN rows
// written to ClickHouse, and the work units that drive ingestion.
package model

import "time"

// EntryType distinguishes a precertificate from a final certificate as logged
// in a CT log. Both share the same (issuer, serial) identity.
type EntryType uint8

const (
	// EntryTypeUnknown is the zero value and indicates an unclassified entry.
	EntryTypeUnknown EntryType = iota
	// EntryTypePrecert is an X509 precertificate (carries the poison extension).
	EntryTypePrecert
	// EntryTypeCert is a final X509 certificate.
	EntryTypeCert
)

// String returns the ClickHouse enum label for the entry type.
func (e EntryType) String() string {
	switch e {
	case EntryTypePrecert:
		return "precert"
	case EntryTypeCert:
		return "cert"
	default:
		return "unknown"
	}
}

// CertMeta is the parsed, storage-ready view of a single CT log entry. It is
// the input to the retention classifier and the ClickHouse writer. Raw DER is
// intentionally not retained (metadata-only design).
type CertMeta struct {
	IssuerCAID         string // stable issuer key (SHA-256 of issuer SPKI/DN, hex)
	IssuerName         string
	SerialNumber       string // hex-encoded
	FingerprintSHA256  string // hex-encoded SHA-256 of the leaf certificate
	CommonName         string
	SANs               []string
	NotBefore          time.Time
	NotAfter           time.Time
	SCTTimestamp       time.Time
	LogName            string
	LogIndex           uint64
	EntryType          EntryType
	SignatureAlgorithm string
	PublicKeyAlgorithm string
	KeySize            int
	IsCA               bool
	IsWildcard         bool
}

// HostnameRow is one observation of a distinct hostname, written to the
// `hostnames` AggregatingMergeTree for every certificate (including old+expired
// ones). Rows collapse on merge to one row per (RegisteredDomain, FQDN); the
// first_seen/last_seen aggregates are min/max of SCTTimestamp and are therefore
// idempotent under the at-least-once re-drain the ingester performs on resume.
// It deliberately carries no per-certificate identity (issuer/serial): the
// distinct store trades per-hostname issuance history for a bounded row count,
// and cert details are looked up on-demand from the windowed `certs` table.
type HostnameRow struct {
	RegisteredDomain string // eTLD+1
	FQDN             string
	IsWildcard       bool
	FirstSeen        time.Time // min(SCTTimestamp) after merge
	LastSeen         time.Time // max(SCTTimestamp) after merge
	LastNotAfter     time.Time // max(NotAfter) after merge — latest expiry seen for the name
	SourceLog        string
}

// WorkUnit is an idempotent ingestion task: a contiguous [StartIndex, EndIndex)
// range of a single CT log shard, derived from an SCT-time window via binary
// search. Disjoint windows yield disjoint index ranges, so units never overlap
// and may run in any order.
type WorkUnit struct {
	ID         string
	LogName    string
	StartIndex uint64 // inclusive
	EndIndex   uint64 // exclusive
	WindowFrom time.Time
	WindowTo   time.Time
}
